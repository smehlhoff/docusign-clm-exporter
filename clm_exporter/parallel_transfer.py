"""Coordinator-owned concurrent transfer state machines and network-only payloads."""
import errno
import hashlib
import logging
import os
import time
import uuid
from collections.abc import Callable, Generator
from dataclasses import dataclass
from functools import partial
from sqlite3 import Row
from typing import Any

from .errors import CapacityStop, MetadataError, PathError, SourceChanged, category
from .local import (
    Reservations,
    footprint,
    regular_open,
    sync_directory,
)
from .remote import file_metadata
from .transfer import Transfers

LOG = logging.getLogger("clm_exporter")


@dataclass(frozen=True)
class Reservation:
    token: int
    size: int


def receive_payload(remote, emit, cfg, capacity, job, path, temp, size, mtime):
    """Stream one reserved file; the caller alone can commit ready or promote."""
    received = 0
    digest = hashlib.sha256()
    # Unbuffered output prevents a failed flush from writing unaccounted bytes
    # later during context-manager cleanup.
    with regular_open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, buffering=0) as output:
        with remote.open(path["remote"]) as source:
            while True:
                chunk = source.read(cfg.chunk_bytes)
                if not chunk:
                    break
                if received + len(chunk) > size:
                    raise SourceChanged("Remote data exceeds the expected size")
                capacity.write(job["id"], output, chunk)
                received += len(chunk)
                digest.update(chunk)
                emit("chunk", (job, received, size))
        if received != size:
            raise SourceChanged(f"Expected {size} bytes, received {received}")
        if file_metadata(remote.lstat(path["remote"])) != (size, mtime):
            raise SourceChanged("Remote size or modification time changed during transfer")
        output.flush()
        os.fsync(output.fileno())
    sync_directory(cfg.work)
    return received, digest.hexdigest()


class ParallelTransfers(Transfers):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Reuse the callback already bound from positional or keyword arguments.
        self.capacity: Reservations = Reservations(self.cfg, self.capacity.free)

    def discard(self, job: Row, state: str, error: str | None = None) -> None:
        if job["state"] == "superseded":
            return
        # This task's writer has settled. Other writers keep their accounting;
        # never walk a changing staging tree to reconstruct a global count.
        self.remove_temp(job)
        with self.state.transaction():
            self.db.execute("UPDATE transfers SET state=?,temp=NULL,received=NULL,sha256=NULL,last_error=? WHERE id=?",
                            (state, error, job["id"]))

    def file_steps(self, path: Row) -> Generator[Callable[..., Any] | Reservation, Any, None]:
        """Yield network tasks and reservations; commit results in the coordinator."""
        job = self.db.execute("SELECT * FROM transfers WHERE path_id=? AND state IN ('pending','failed')", (path["id"],)).fetchone()
        for attempt in range(1, self.cfg.max_attempts + 1):
            try:
                with self.state.transaction():
                    self.db.execute("UPDATE transfers SET attempts=?,attempt_run=? WHERE id=?", (attempt, self.run, job["id"]))
                size, mtime = yield lambda remote, emit, relative=path["remote"]: file_metadata(remote.lstat(relative))
                path, job_id = self._refresh_job(path, job, attempt, size, mtime)
                if job_id is None:
                    return
                job = self.get_job(job_id)
                if self._reject_oversized_file(path, job_id, size, attempt):
                    return
                self._validate_target(path)
                yield Reservation(job_id, size)
                temp_name = f"transfer-{job_id}-{uuid.uuid4().hex}.part"
                with self.state.transaction():
                    self.db.execute("UPDATE transfers SET state='downloading',temp=?,received=NULL,sha256=NULL,last_error=NULL WHERE id=?",
                                    (temp_name, job_id))
                job = self.get_job(job_id)
                self.checkpoint("downloading", job)
                temp = self.temp_path(job)
                received, digest = yield partial(
                    receive_payload, cfg=self.cfg, capacity=self.capacity,
                    job=dict(job), path=dict(path), temp=temp, size=size, mtime=mtime)
                with self.state.transaction():
                    self.db.execute("UPDATE transfers SET state='ready',received=?,sha256=? WHERE id=?",
                                    (received, digest, job_id))
                job = self.get_job(job_id)
                self.checkpoint("ready", job)
                self.promote(job, path)
                LOG.info("Downloaded %r -> %r (%d bytes)%s", path["remote"], path["local"], size, " [zero-byte PDF: review]" if size == 0 else "")
                return
            except (CapacityStop, KeyboardInterrupt):
                # An interrupt can arrive after the ready COMMIT but before
                # the in-memory job is refreshed. Durable state governs cleanup.
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
                yield lambda remote, emit, error=exc: remote.reconnect(error)
                if (isinstance(exc, (PermissionError, FileNotFoundError, MetadataError, PathError))
                        or isinstance(exc, OSError) and exc.errno == errno.ENAMETOOLONG
                        or attempt == self.cfg.max_attempts):
                    return
                yield lambda remote, emit, delay=attempt: self.sleep(delay)

            finally:
                self.capacity.release(job["id"])

    def download(self, force_path: str | None = None) -> None:
        self.capacity.staged = footprint(self.cfg)
        if force_path is not None:
            self._queue_forced_path(force_path)
        # Start from the ordered path index. Joining from all pending transfers
        # makes SQLite sort the remaining queue again for every admission.
        selection = """FROM paths p
            WHERE p.last_seen_run=? AND p.selected=1 AND p.local IS NOT NULL
              AND p.size IS NOT NULL AND p.mtime IS NOT NULL
              AND EXISTS(SELECT 1 FROM transfers t WHERE t.path_id=p.id AND t.state IN ('pending','failed'))"""
        total = self.db.execute("SELECT count(*) " + selection, (self.run,)).fetchone()[0]
        pool = self.remote
        file_steps_by_slot: dict[int, Generator[Callable[..., Any] | Reservation, Any, None]] = {}
        pending_reservations: dict[int, Reservation] = {}
        running_slots: set[int] = set()
        path_order: dict[int, str] = {}
        admitted_slots: set[int] = set()
        last_remote = ""
        exhausted = False

        def advance(slot, value=None, error=None):
            try:
                steps = file_steps_by_slot[slot]
                operation = steps.throw(error) if error else steps.send(value)
            except StopIteration:
                del file_steps_by_slot[slot]
                del path_order[slot]
                admitted_slots.discard(slot)
                bar.update(1)
                return
            if isinstance(operation, Reservation):
                admitted_slots.discard(slot)
                pending_reservations[slot] = operation
            else:
                pool.submit(slot, operation)
                running_slots.add(slot)

        def apply(event):
            if event.kind == "chunk":
                job, _received, _size = event.value
                self.checkpoint("chunk", job)
                now = time.monotonic()
                if now - self.last_progress >= 5:
                    LOG.info("Transfers: %d active workers; staged %d bytes; reserved %d bytes",
                             len(running_slots), self.capacity.staged, self.capacity.reserved)
                    self.last_progress = now

        with self.progress.bar(desc="Files processed", total=total, unit="file", position=0) as bar:
            try:
                while True:
                    for slot in range(self.cfg.workers):
                        if exhausted:
                            break
                        if slot in file_steps_by_slot:
                            continue
                        path = self.db.execute("SELECT p.* " + selection + " AND p.remote>? COLLATE BINARY ORDER BY p.remote COLLATE BINARY LIMIT 1",
                                               (self.run, last_remote)).fetchone()
                        if path is None:
                            exhausted = True
                            break
                        last_remote = path["remote"]
                        path_order[slot] = path["remote"]
                        file_steps_by_slot[slot] = self.file_steps(path)
                        advance(slot)
                    blocked = None
                    for slot in sorted(pending_reservations, key=path_order.__getitem__):
                        # Metadata can return out of order; capacity admission
                        # follows the durable binary path scheduling order.
                        first = min(
                            (slot for slot in file_steps_by_slot if slot not in admitted_slots),
                            key=path_order.__getitem__,
                        )
                        if slot != first:
                            break
                        reservation = pending_reservations[slot]
                        try:
                            self.capacity.reserve(reservation.token, reservation.size)
                        except CapacityStop as exc:
                            blocked = exc
                            break
                        del pending_reservations[slot]
                        admitted_slots.add(slot)
                        advance(slot)
                    if not running_slots:
                        if pending_reservations:
                            # With no tasks running, every unfinished generator
                            # is waiting here on a rejected capacity reservation.
                            assert blocked is not None
                            raise blocked
                        if not file_steps_by_slot and exhausted:
                            break
                        continue
                    event = pool.receive(apply)
                    running_slots.remove(event.slot)
                    advance(event.slot, event.value, event.error)
            except BaseException:
                # Stop network writes before disposing state machines or reporting.
                pool.close()
                raise
            finally:
                for steps in file_steps_by_slot.values():
                    steps.close()
