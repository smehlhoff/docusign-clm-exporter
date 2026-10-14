import json
import logging
import time
from collections import deque
from collections.abc import Callable

from .errors import MetadataError, PathError, category
from .local import component
from .mapping import allocate_siblings
from .progress import Progress
from .remote import backoff, entry_kind, entry_metadata, file_metadata
from .state import State, utcnow

LOG = logging.getLogger("clm_exporter")


def scan(
    state: State,
    remote,
    run: str,
    sleep: Callable[[int], None] = backoff,
    progress: Progress | None = None,
) -> bool:
    """Persist discovery results and return whether every entry was inspected."""
    progress = progress if progress is not None else Progress(enabled=False)
    with progress.bar(desc="Scanning", unit="entry", total=None) as bar:
        return _scan(state, remote, run, sleep, bar)


def _scan(state: State, remote, run: str, sleep: Callable[[int], None], bar) -> bool:
    db, cfg = state.db, state.cfg
    with state.transaction():
        db.execute("UPDATE runs SET scan_started_at=?,scan_status='running' WHERE id=?", (utcnow(), run))
        db.execute("INSERT INTO scans VALUES(?,'','pending')", (run,))
    queue = deque([""])
    complete = True
    observed = processed = 0
    last_progress = time.monotonic()

    def report_progress():
        nonlocal last_progress
        now = time.monotonic()
        if now - last_progress >= 5:
            LOG.info("Discovery: %d entries examined, %d directories processed, %d directories queued",
                     observed, processed, len(queue))
            last_progress = now

    while queue:
        directory = queue.popleft()
        directory_ok = False
        for attempt in range(1, cfg.max_attempts + 1):
            batch = []
            try:
                attrs = remote.lstat(directory)
                if entry_kind(attrs) in ("file", "symlink"):
                    raise MetadataError("Scanned path is a file or symlink, not a directory")
                # CLM may report special/missing types even for directories.
                # A successful listing establishes that this path is a directory.
                for entry in remote.iterdir(directory):
                    observed += 1
                    bar.update(1)
                    name = getattr(entry, "filename", None)
                    relative = f"{directory}/{name}" if directory else str(name)
                    try:
                        component(name)
                    except PathError as exc:
                        state.error(run, relative, "metadata", "unsafe_remote_path", exc)
                        complete = False
                        continue
                    attrs = entry
                    attribute_error = None
                    attribute_operation = "metadata"
                    is_pdf = name.casefold().endswith(".pdf")
                    # PDF selection does not depend on an ambiguous type label.
                    # Reuse listing metadata instead of paying one round trip
                    # per PDF; transfers recheck it before and after reading.
                    if ((not is_pdf and entry_kind(attrs) in ("special", "unknown"))
                            or any(getattr(attrs, n, None) is None for n in ("st_size", "st_mtime"))):
                        try:
                            attrs = remote.lstat(relative)
                        except (OSError, EOFError, MetadataError) as exc:
                            remote.reconnect(exc)
                            attribute_error = exc
                    attributes = json.dumps({key: getattr(attrs, key, None) for key in
                                             ("st_mode", "st_size", "st_mtime", "st_atime", "st_uid", "st_gid")},
                                            ensure_ascii=True)
                    kind = entry_kind(attrs)
                    verified_non_directory = kind in ("file", "symlink")
                    if not is_pdf and kind in ("special", "unknown"):
                        # Do not silently exclude a subtree based on CLM's mode.
                        # Leave failed checks unknown so old children cannot be
                        # mistaken for deletions through this uninspected entry.
                        kind = "unknown"
                        if attribute_error is None:
                            try:
                                remote.check_directory(relative)
                                kind = "directory"
                            except (OSError, EOFError, MetadataError) as exc:
                                remote.reconnect(exc)
                                attribute_error = exc
                        if attribute_error is not None:
                            attribute_operation = "directory"
                            LOG.warning("Cannot verify possible directory %r: %s", relative, attribute_error)
                    selected = is_pdf and kind != "directory"
                    if selected:
                        # CLM can label downloadable PDFs as special or unknown.
                        # Preserve the reported mode in attributes for diagnostics.
                        kind = "file"
                    try:
                        size, mtime = file_metadata(attrs) if kind == "file" else entry_metadata(attrs)
                    except MetadataError as exc:
                        size = mtime = None
                        attribute_error = attribute_error or exc
                    if kind in ("file", "directory", "unknown") and (size is None or mtime is None or kind == "unknown"):
                        attribute_error = attribute_error or MetadataError("Required entry attributes remain unavailable")
                    batch.append((relative, directory, name, kind, size, mtime, selected, attributes, verified_non_directory))
                    if attribute_error:
                        # Persist first so saving this observation cannot resolve the new error.
                        _save_observations(state, run, batch)
                        state.error(run, relative, attribute_operation, category(attribute_error), attribute_error, attempt)
                        complete = False
                    if kind in ("symlink", "special"):
                        LOG.info("Skipping %s: %r", kind, relative)
                    if len(batch) >= 500:
                        _save_observations(state, run, batch)
                    report_progress()
                _save_observations(state, run, batch)
                directory_ok = True
                with state.transaction():
                    state.resolve(run, directory, ["directory"])
                break
            except (OSError, EOFError, MetadataError) as exc:
                _save_observations(state, run, batch)
                remote.reconnect(exc)
                permanent = isinstance(exc, (PermissionError, FileNotFoundError, MetadataError))
                state.error(run, directory, "directory", category(exc), exc, attempt)
                LOG.warning("Directory scan failed for %r: %s", directory, exc)
                if permanent or attempt == cfg.max_attempts:
                    complete = False
                    break
                sleep(attempt)
        allocate_siblings(state, run, directory)
        with state.transaction():
            db.execute("UPDATE scans SET state=? WHERE run_id=? AND remote=?",
                       ("complete" if directory_ok else "failed", run, directory))
            for child in db.execute("SELECT remote FROM paths WHERE parent=? AND kind='directory' AND last_seen_run=?",
                                    (directory, run)).fetchall():
                db.execute("INSERT OR IGNORE INTO scans VALUES(?,?,'pending')", (run, child[0]))
                queue.append(child[0])
        # Empty directories produce no entries, but still advance discovery.
        # Count each directory once, including failed scans after their retries.
        processed += 1
        report_progress()
    _record_unavailable_paths(state, run)
    with state.transaction():
        db.execute("UPDATE runs SET scan_status=?,scan_ended_at=? WHERE id=?",
                   ("complete" if complete else "incomplete", utcnow(), run))
        _schedule_transfers(state, run)
    LOG.info("Discovery %s: %d entries examined, %d directories processed, %d directories queued",
             "complete" if complete else "incomplete", observed, processed, len(queue))
    return complete


def _save_observations(state: State, run: str, batch) -> None:
    """Clear the batch only after its observations are committed."""
    db = state.db
    if not batch:
        return
    with state.transaction():
        for relative, parent, name, kind, size, mtime, selected, attributes, verified_non_directory in batch:
            prior = db.execute("SELECT kind FROM paths WHERE remote=?", (relative,)).fetchone()
            db.execute("""INSERT INTO paths(remote,parent,name,kind,last_seen_run,size,mtime,selected,attributes)
                VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(remote) DO UPDATE SET
                kind=excluded.kind,last_seen_run=excluded.last_seen_run,
                size=excluded.size,mtime=excluded.mtime,selected=excluded.selected,attributes=excluded.attributes""",
                       (relative, parent, name, kind, run, size, mtime, selected, attributes))
            db.execute("""INSERT INTO observations SELECT ?,id,kind,size,mtime,selected,attributes FROM paths WHERE remote=?
                ON CONFLICT(run_id,path_id) DO UPDATE SET kind=excluded.kind,size=excluded.size,
                mtime=excluded.mtime,selected=excluded.selected,attributes=excluded.attributes""", (run, relative))
            if kind != "unknown" and (not selected or size is not None and mtime is not None):
                state.resolve(run, relative, ["metadata"])
            if verified_non_directory:
                # Only explicit type metadata can retire a failed directory
                # check; a PDF suffix alone does not establish that it is a file.
                state.resolve(run, relative, ["directory"])
            if prior and prior["kind"] != kind and prior["kind"] in ("file", "directory"):
                # The old mapping stays reserved; ownership checks govern reuse.
                LOG.warning("Remote entry changed type: %r", relative)
    batch.clear()


def _record_unavailable_paths(state: State, run: str) -> None:
    """Report missing unfinished PDFs only when an ancestor scan proves absence."""
    db = state.db
    # Infer absence only through a fully enumerated ancestor. A failed subtree is unknown.
    scan_states = dict(db.execute("SELECT remote,state FROM scans WHERE run_id=?", (run,)))
    # The current selected flag can disappear when a PDF changes type. Retain
    # unfinished work even when an older version was successfully downloaded.
    missing = db.execute("""SELECT p.* FROM paths p
        WHERE (p.last_seen_run<>? OR (p.selected=0 AND p.kind<>'unknown'))
        AND p.id IN (SELECT path_id FROM observations WHERE selected=1)
        AND (NOT EXISTS(SELECT 1 FROM transfers t WHERE t.path_id=p.id AND t.state='downloaded')
             OR EXISTS(SELECT 1 FROM transfers t WHERE t.path_id=p.id
                       AND t.state IN ('pending','failed','downloading','ready')))""", (run,)).fetchall()
    for path in missing:
        parent = path["parent"]
        while parent not in scan_states and parent:
            ancestor = db.execute("SELECT kind,last_seen_run FROM paths WHERE remote=?", (parent,)).fetchone()
            if ancestor and ancestor["last_seen_run"] == run and ancestor["kind"] in ("directory", "unknown"):
                # A visible ancestor whose tree could not be inspected is not
                # evidence that its previously observed children disappeared.
                break
            parent = parent.rpartition("/")[0]
        if scan_states.get(parent) == "complete":
            state.error(run, path["remote"], "availability", "source_unavailable",
                        "Previously discovered PDF is unavailable with unfinished download work")


def _schedule_transfers(state: State, run: str) -> None:
    """Update jobs for current metadata inside the caller's transaction."""
    db = state.db
    for path in db.execute("SELECT * FROM paths WHERE selected=1 AND last_seen_run=? AND size IS NOT NULL AND mtime IS NOT NULL", (run,)).fetchall():
        latest = state.latest(path["id"])
        if latest is None or (latest["size"], latest["mtime"]) != (path["size"], path["mtime"]):
            state.job(path, run)
        else:
            # A pending version that reverted before completion no longer represents a gap.
            db.execute("UPDATE transfers SET state='superseded' WHERE path_id=? AND state IN ('pending','failed') AND forced=0",
                       (path["id"],))
            active = db.execute("""SELECT 1 FROM transfers WHERE path_id=?
                AND state IN ('pending','failed','downloading','ready')""", (path["id"],)).fetchone()
            # Matching older metadata cannot resolve a failed explicit repeat
            # or a transfer still waiting for crash recovery.
            if not active:
                state.resolve(run, path["remote"], ["transfer", "availability"])
