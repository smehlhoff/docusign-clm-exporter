"""Direct checks of accepted CSV paths; no directory enumeration or probing."""
import json
import logging
import time

from .discovery import _save_observations, _schedule_transfers
from .errors import MetadataError, category
from .remote import backoff, entry_kind, file_metadata
from .state import utcnow
from .workers import Workers

LOG = logging.getLogger("clm_exporter")


def _check(remote, path, ancestor, max_attempts, sleep):
    attributes = None
    for attempt in range(1, max_attempts + 1):
        try:
            attrs = remote.lstat(path)
            attributes = json.dumps({key: getattr(attrs, key, None) for key in
                                     ("st_mode", "st_size", "st_mtime", "st_atime", "st_uid", "st_gid")})
            kind = entry_kind(attrs)
            if ancestor:
                if kind in ("file", "symlink"):
                    raise MetadataError(f"Required ancestor is an explicit {kind}")
                size = mtime = None
            else:
                if kind == "directory":
                    raise MetadataError("Listed PDF is a directory")
                size, mtime = file_metadata(attrs)
            return attempt, attributes, kind, size, mtime, None
        except (OSError, EOFError, MetadataError) as exc:
            remote.reconnect(exc)
            if isinstance(exc, (PermissionError, FileNotFoundError, MetadataError)) or attempt == max_attempts:
                return attempt, attributes, None, None, None, (category(exc), str(exc))
            sleep(attempt)


def scan_csv(state, remote, run, progress, sleep=backoff):
    db, cfg = state.db, state.cfg
    imported = db.execute("SELECT * FROM csv_imports WHERE run_id=? AND state='accepted'", (run,)).fetchone()
    if imported is None:
        raise ValueError("Metadata checks require accepted input")
    with state.transaction():
        db.execute("UPDATE runs SET scan_started_at=?,scan_status='running' WHERE id=?", (utcnow(), run))
    ancestor_total = db.execute("SELECT COUNT(*) FROM csv_ancestor_checks WHERE run_id=?", (run,)).fetchone()[0]
    observed = 0
    progress_at = time.monotonic()

    def save(row, ancestor, result):
        nonlocal observed, progress_at
        attempt, attributes, kind, size, mtime, error = result
        path = row["remote"] if ancestor else row["remote_path"]
        operation = "csv_ancestor" if ancestor else "metadata"
        table = "csv_ancestor_checks" if ancestor else "csv_entries"
        key = "remote" if ancestor else "remote_path"
        if not ancestor and not error:
            _save_observations(state, run, [(path, row["parent"], row["document_name"], "file", size, mtime,
                                            True, attributes, kind in ("file", "symlink"))])
        with state.transaction():
            db.execute(f"""UPDATE {table} SET lookup_state=?,attempts=?,category=?,message=?
                WHERE run_id=? AND {key}=?""", ("failed" if error else "checked", attempt,
                                              error[0] if error else None, error[1] if error else None, run, path))
            if ancestor:
                db.execute("UPDATE csv_ancestor_checks SET attributes=? WHERE run_id=? AND remote=?", (attributes, run, path))
                # Only explicit directories are observations. Ambiguous
                # ancestors retain their attributes without claiming a type.
                if not error and path and kind == "directory":
                    db.execute("UPDATE paths SET kind='directory',last_seen_run=?,attributes=? WHERE remote=?", (run, attributes, path))
                    db.execute("""INSERT INTO observations(run_id,path_id,kind,selected,attributes)
                        SELECT ?,id,'directory',0,? FROM paths WHERE remote=?""", (run, attributes, path))
            if not error:
                state.resolve(run, path, [operation])
        if error:
            state.error(run, path, operation, *error, attempt)
        if not ancestor:
            observed += 1
        if time.monotonic() - progress_at >= 5:
            LOG.info("CSV metadata: %d/%d document checks settled", observed, imported["expected_count"])
            progress_at = time.monotonic()

    def process(rows, ancestor, bar):
        """Keyset-ordered rows, bounded to one task per connection."""
        if not isinstance(remote, Workers):
            for row in rows:
                path = row["remote"] if ancestor else row["remote_path"]
                save(row, ancestor, _check(remote, path, ancestor, cfg.max_attempts, sleep))
                bar.update(1)
            return
        active = {}
        exhausted = False
        while True:
            for slot in range(cfg.workers):
                if slot in active or exhausted:
                    continue
                row = next(rows, None)
                if row is None:
                    exhausted = True
                    break
                path = row["remote"] if ancestor else row["remote_path"]
                active[slot] = row
                remote.submit(slot, lambda session, emit, path=path: _check(session, path, ancestor, cfg.max_attempts, sleep))
            if not active:
                break
            event = remote.receive(lambda event: None)
            row = active.pop(event.slot)
            if event.error is not None:
                raise event.error
            save(row, ancestor, event.value)
            bar.update(1)

    # A depth barrier gives each descendant a settled parent. Indexed keyset
    # iteration avoids sorting or materializing the whole remaining work list.
    def ancestors(depth):
        last = ""
        while True:
            row = db.execute("""SELECT a.* FROM csv_ancestor_checks a
                WHERE a.run_id=? AND a.lookup_state='pending' AND a.depth=? AND a.remote>=?
                  AND (a.parent IS NULL OR EXISTS(SELECT 1 FROM csv_ancestor_checks p
                       WHERE p.run_id=a.run_id AND p.remote=a.parent AND p.lookup_state='checked'))
                ORDER BY a.remote LIMIT 1""", (run, depth, last)).fetchone()
            if row is None:
                return
            last = row["remote"] + "\x00"
            yield row

    def entries():
        last = 0
        while True:
            row = db.execute("""SELECT e.* FROM csv_entries e WHERE e.run_id=? AND e.lookup_state='pending'
                AND e.row_number>? AND EXISTS(SELECT 1 FROM csv_ancestor_checks a
                    WHERE a.run_id=e.run_id AND a.remote=e.parent AND a.lookup_state='checked')
                ORDER BY e.row_number LIMIT 1""", (run, last)).fetchone()
            if row is None:
                return
            last = row["row_number"]
            yield row

    with progress.bar(desc="Checking ancestors", unit="folder", total=ancestor_total) as bar:
        maximum = db.execute("SELECT MAX(depth) FROM csv_ancestor_checks WHERE run_id=?", (run,)).fetchone()[0]
        for depth in range(maximum + 1):
            process(ancestors(depth), True, bar)
    with progress.bar(desc="Checking metadata", unit="document", total=imported["expected_count"]) as bar:
        process(entries(), False, bar)
    complete = not db.execute("""SELECT 1 FROM csv_entries WHERE run_id=? AND lookup_state<>'checked'
        UNION ALL SELECT 1 FROM csv_ancestor_checks WHERE run_id=? AND lookup_state<>'checked' LIMIT 1""", (run, run)).fetchone()
    with state.transaction():
        db.execute("UPDATE runs SET scan_status=?,scan_ended_at=? WHERE id=?", ("complete" if complete else "incomplete", utcnow(), run))
        _schedule_transfers(state, run)
    LOG.info("CSV metadata %s: %d/%d documents checked", "complete" if complete else "incomplete", observed, imported["expected_count"])
    return complete
