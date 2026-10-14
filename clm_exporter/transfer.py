"""One active transfer, with durable ready records spanning atomic promotion."""
import errno
import hashlib
import logging
import os
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from sqlite3 import Row

from .errors import (
    CapacityStop,
    FatalError,
    MetadataError,
    PathError,
    SourceChanged,
    category,
)
from .local import (
    Capacity,
    digest_file,
    footprint,
    regular_open,
    safe_target,
    sync_directory,
)
from .progress import Progress
from .remote import backoff, file_metadata
from .state import State, utcnow

LOG = logging.getLogger("clm_exporter")


class Transfers:
    def __init__(
        self,
        state: State,
        run: str,
        remote=None,
        free: Callable[[], int] | None = None,
        sleep: Callable[[int], None] = backoff,
        checkpoint: Callable[[str, Row], None] | None = None,
        progress: Progress | None = None,
    ) -> None:
        self.state = state
        self.db = state.db
        self.cfg = state.cfg
        self.run = run
        self.remote = remote
        self.sleep = sleep
        self.capacity = Capacity(self.cfg, free)
        self.progress = progress if progress is not None else Progress(enabled=False)
        # Fault-injection hook used by the crash tests, never by production CLI.
        self.checkpoint = checkpoint or (lambda event, job: None)
        self.last_progress = time.monotonic()

    def temp_path(self, job: Row) -> Path:
        name = job["temp"]
        if not name or not name.startswith(f"transfer-{job['id']}-") or not name.endswith(".part") or "/" in name or "\\" in name:
            raise FatalError("Invalid tracked temporary path in database")
        return safe_target(self.cfg.work, name)

    def remove_temp(self, job: Row) -> None:
        if not job["temp"]:
            return
        temp = self.temp_path(job)
        try:
            size = temp.lstat().st_size
        except FileNotFoundError:
            return
        temp.unlink()
        self.capacity.removed(size)

    def get_job(self, job_id: int) -> Row:
        job = self.db.execute("SELECT * FROM transfers WHERE id=?", (job_id,)).fetchone()
        if job is None:
            raise FatalError(f"Transfer record is missing: {job_id}")
        return job

    def finalize(self, job: Row, path: Row) -> None:
        with self.state.transaction():
            self.db.execute("UPDATE transfers SET state='downloaded',completion_run=?,completed_at=?,last_error=NULL WHERE id=?",
                            (self.run, utcnow(), job["id"]))
            self.state.resolve(self.run, path["remote"], ["transfer", "availability", "recovery", "local", "force"])

    def promote(self, job: Row, path: Row) -> None:
        target = safe_target(self.cfg.export, path["local"], create_parents=True)
        temp = self.temp_path(job)
        old_size = 0
        if target.exists():
            latest = self.state.latest(path["id"])
            if latest is None:
                raise PathError(f"Refusing to overwrite untracked local file: {path['local']}")
            if digest_file(target) != (latest["size"], latest["sha256"]):
                raise PathError(f"Existing local file differs from its recorded hash: {path['local']}")
            old_size = latest["size"]
            os.replace(temp, target)
        else:
            # Atomic no-clobber creation also guards a file appearing after inspection.
            try:
                os.link(temp, target, follow_symlinks=False)
            except FileExistsError as exc:
                raise PathError(f"Local target appeared during promotion: {path['local']}") from exc
            temp.unlink()
        self.capacity.removed(old_size)
        sync_directory(target.parent)
        sync_directory(self.cfg.work)
        self.checkpoint("promoted", job)
        self.finalize(job, path)

    def recover(self) -> None:
        for job in self.db.execute("SELECT * FROM transfers WHERE state IN ('downloading','ready') ORDER BY id").fetchall():
            path = self.db.execute("SELECT * FROM paths WHERE id=?", (job["path_id"],)).fetchone()
            try:
                if job["state"] == "ready":
                    try:
                        final = safe_target(self.cfg.export, path["local"])
                        matches = digest_file(final) == (job["size"], job["sha256"])
                    except FileNotFoundError:
                        matches = False
                    if matches:
                        self.remove_temp(job)
                        sync_directory(final.parent)
                        self.finalize(job, path)
                        continue
                    try:
                        valid_temp = digest_file(self.temp_path(job)) == (job["size"], job["sha256"])
                    except FileNotFoundError:
                        valid_temp = False
                    if valid_temp:
                        self.promote(job, path)
                        continue
                self.remove_temp(job)
                with self.state.transaction():
                    self.db.execute("UPDATE transfers SET state='pending',temp=NULL,received=NULL,sha256=NULL WHERE id=?", (job["id"],))
                    self.state.resolve(self.run, path["remote"], ["recovery"])
            except (OSError, PathError) as exc:
                self.state.error(self.run, path["remote"], "recovery", category(exc), exc)
                # A verified ready payload remains recoverable on the next invocation.
                LOG.error("Recovery failed for %r: %s", path["remote"], exc)

    def download(self, force_path: str | None = None) -> None:
        # Recovery and discovery can take a long time. Refresh the startup count
        # before scheduling content; subsequent writes update it chunk by chunk.
        self.capacity.staged = footprint(self.cfg)
        if force_path is not None:
            path = self.db.execute("SELECT * FROM paths WHERE remote=? AND last_seen_run=? AND selected=1", (force_path, self.run)).fetchone()
            if path is None or path["size"] is None or path["mtime"] is None:
                self.state.error(self.run, force_path, "force", "source_unavailable", "Forced path is not a current PDF with complete metadata")
            else:
                with self.state.transaction():
                    self.state.job(path, self.run, forced=True)
        paths = self.db.execute("""SELECT p.* FROM paths p JOIN transfers t ON t.path_id=p.id
            WHERE p.last_seen_run=? AND p.selected=1 AND p.local IS NOT NULL
              AND p.size IS NOT NULL AND p.mtime IS NOT NULL AND t.state IN ('pending','failed')
            ORDER BY p.remote COLLATE BINARY""", (self.run,)).fetchall()
        with self.progress.bar(desc="Files processed", total=len(paths), unit="file", position=0) as bar:
            for path in paths:
                self.download_file(path)
                # Processed includes failed/oversized candidates, not deferred files.
                # Successful completion remains a separate durable database fact.
                bar.update(1)

    def download_file(self, path: Row) -> None:
        """Retry a pending file while retaining any durable ready payload."""
        job = self.db.execute("SELECT * FROM transfers WHERE path_id=? AND state IN ('pending','failed')", (path["id"],)).fetchone()
        for attempt in range(1, self.cfg.max_attempts + 1):
            try:
                with self.state.transaction():
                    self.db.execute("UPDATE transfers SET attempts=?,attempt_run=? WHERE id=?", (attempt, self.run, job["id"]))
                size, mtime = file_metadata(self.remote.lstat(path["remote"]))
                with self.state.transaction():
                    self.db.execute("UPDATE paths SET size=?,mtime=? WHERE id=?", (size, mtime, path["id"]))
                    self.db.execute("UPDATE observations SET size=?,mtime=? WHERE run_id=? AND path_id=?",
                                    (size, mtime, self.run, path["id"]))
                    path = self.db.execute("SELECT * FROM paths WHERE id=?", (path["id"],)).fetchone()
                    latest = self.state.latest(path["id"])
                    if not job["forced"] and latest and (latest["size"], latest["mtime"]) == (size, mtime):
                        self.db.execute("UPDATE transfers SET state='superseded' WHERE id=?", (job["id"],))
                        self.state.resolve(self.run, path["remote"], ["transfer", "availability"])
                        return
                    job_id = self.state.job(path, self.run, forced=job["forced"])
                    self.db.execute("UPDATE transfers SET attempts=?,attempt_run=? WHERE id=?", (attempt, self.run, job_id))
                job = self.get_job(job_id)
                if size > self.cfg.max_staged_bytes:
                    self.state.error(self.run, path["remote"], "transfer", "file_exceeds_batch_limit",
                                     f"File has {size} bytes; batch limit is {self.cfg.max_staged_bytes}", attempt)
                    with self.state.transaction():
                        self.db.execute("UPDATE transfers SET state='failed',last_error='file_exceeds_batch_limit' WHERE id=?", (job_id,))
                    return
                target = safe_target(self.cfg.export, path["local"], create_parents=True)
                if target.exists():
                    latest = self.state.latest(path["id"])
                    if latest is None or digest_file(target) != (latest["size"], latest["sha256"]):
                        raise PathError(f"Refusing untracked or modified local target: {path['local']}")
                self.capacity.check(size)
                temp_name = f"transfer-{job_id}-{uuid.uuid4().hex}.part"
                with self.state.transaction():
                    self.db.execute("UPDATE transfers SET state='downloading',temp=?,received=NULL,sha256=NULL,last_error=NULL WHERE id=?",
                                    (temp_name, job_id))
                job = self.get_job(job_id)
                self.checkpoint("downloading", job)
                self._receive_payload(job, path, attempt, size, mtime)
                job = self.get_job(job_id)
                self.checkpoint("ready", job)
                self.promote(job, path)
                LOG.info("Downloaded %r -> %r (%d bytes)%s", path["remote"], path["local"], size, " [zero-byte PDF: review]" if size == 0 else "")
                return
            except (CapacityStop, KeyboardInterrupt):
                # An interrupt can arrive after the ready COMMIT but before
                # _receive_payload returns. Durable state governs cleanup.
                job = self.get_job(job["id"])
                if job["state"] not in ("ready", "downloaded"):
                    self.discard(job, "pending")
                raise
            except (OSError, EOFError, MetadataError, PathError, SourceChanged) as exc:
                job = self.get_job(job["id"])
                ready = job["state"] in ("ready", "downloaded")
                if isinstance(exc, OSError) and exc.errno == errno.ENOSPC:
                    if not ready:
                        self.discard(job, "pending")
                    raise CapacityStop("disk_reserve") from exc
                if ready:
                    # Never destroy a durable ready payload because promotion failed.
                    self.state.error(self.run, path["remote"], "recovery", category(exc), exc, attempt)
                    LOG.error("Promotion failed for %r: %s", path["remote"], exc)
                    return
                self.discard(job, "failed", str(exc))
                self.state.error(self.run, path["remote"], "transfer", category(exc), exc, attempt)
                LOG.warning("Transfer attempt %d failed for %r: %s", attempt, path["remote"], exc)
                self.remote.reconnect(exc)
                if (isinstance(exc, (PermissionError, FileNotFoundError, MetadataError, PathError))
                        or isinstance(exc, OSError) and exc.errno == errno.ENAMETOOLONG
                        or attempt == self.cfg.max_attempts):
                    return
                self.sleep(attempt)

    def _receive_payload(
        self, job: Row, path: Row, attempt: int, size: int, mtime: str
    ) -> None:
        """Verify and sync a temporary payload before recording it as ready."""
        received = 0
        digest = hashlib.sha256()
        with (self.progress.bar(desc=f"Receiving {path['remote']!r} (attempt {attempt})",
                                total=size, unit="B", unit_scale=True, unit_divisor=1000,
                                position=1, leave=False) as bar,
              regular_open(self.temp_path(job), os.O_WRONLY | os.O_CREAT | os.O_EXCL) as output):
            with self.remote.open(path["remote"]) as source:
                while True:
                    chunk = source.read(self.cfg.chunk_bytes)
                    if not chunk:
                        break
                    if received + len(chunk) > size:
                        raise SourceChanged("Remote data exceeds the expected size")
                    self.capacity.check(len(chunk))
                    try:
                        output.write(chunk)
                        # Flush each chunk before accounting for it; flush can fail after a write.
                        output.flush()
                    except OSError as exc:
                        if exc.errno == errno.ENOSPC:
                            raise CapacityStop("disk_reserve") from exc
                        raise
                    received += len(chunk)
                    self.capacity.written(len(chunk))
                    bar.update(len(chunk))
                    digest.update(chunk)
                    self.checkpoint("chunk", job)
                    if time.monotonic() - self.last_progress >= 5:
                        LOG.info("Transfer %r: %d/%d bytes; staged %d bytes", path["remote"], received, size, self.capacity.staged)
                        self.last_progress = time.monotonic()
            if received != size:
                raise SourceChanged(f"Expected {size} bytes, received {received}")
            if file_metadata(self.remote.lstat(path["remote"])) != (size, mtime):
                raise SourceChanged("Remote size or modification time changed during transfer")
            output.flush()
            os.fsync(output.fileno())
        sync_directory(self.cfg.work)
        with self.state.transaction():
            self.db.execute("UPDATE transfers SET state='ready',received=?,sha256=? WHERE id=?", (received, digest.hexdigest(), job["id"]))

    def discard(self, job: Row, state: str, error: str | None = None) -> None:
        # Metadata refresh can commit a replacement job just before an
        # interrupt. Do not revive the old job and collide with its successor.
        if job["state"] == "superseded":
            return
        # Recount after partial writes: the OS may have written fewer bytes than requested.
        self.remove_temp(job)
        self.capacity.staged = footprint(self.cfg)
        with self.state.transaction():
            self.db.execute("UPDATE transfers SET state=?,temp=NULL,received=NULL,sha256=NULL,last_error=? WHERE id=?",
                            (state, error, job["id"]))
