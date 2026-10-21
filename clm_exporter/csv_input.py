"""Streaming, all-or-nothing CSV acceptance; rejected rows never touch paths."""
import csv
import hashlib
import io
import logging
import sys
import time
from pathlib import Path

from .errors import FatalError, PathError
from .local import component
from .state import utcnow

LOG = logging.getLogger("clm_exporter")
REQUIRED = ("document id", "document name", "location path")
BATCH_SIZE = 500


class _HashedInput(io.RawIOBase):
    def __init__(self, raw):
        self.raw = raw
        self.digest = hashlib.sha256()

    def readable(self):
        return True

    def readinto(self, buffer):
        count = self.raw.readinto(buffer)
        if count:
            self.digest.update(memoryview(buffer)[:count])
        return count


def _strict_lines(stream):
    """csv.reader(strict=True) still accepts quotes inside unquoted fields."""
    mode = "start"
    for line in stream:
        if "\x00" in line:
            raise ValueError("Embedded NUL in CSV input")
        for char in line:
            if mode == "quoted":
                if char == '"':
                    mode = "closed"
            elif mode == "closed":
                if char == '"':
                    mode = "quoted"
                elif char in ",\r\n":
                    mode = "start"
                else:
                    raise ValueError("Unexpected text after closing CSV quote")
            elif char == '"':
                if mode != "start":
                    raise ValueError("Quote inside an unquoted CSV field")
                mode = "quoted"
            elif char in ",\r\n":
                mode = "start"
            else:
                mode = "unquoted"
        yield line
    if mode == "quoted":
        raise ValueError("Unterminated quoted CSV field")


def resolve_path(root, name, location):
    component(name)
    if not name.casefold().endswith(".pdf"):
        raise ValueError("Document Name must end in .pdf")
    if not location.startswith("/") or "\\" in location or "\x00" in location:
        raise ValueError("Location Path must be an absolute POSIX folder path")
    if "//" in location:
        raise ValueError("Location Path must not contain repeated separators")
    folder = location[:-1] if location.endswith("/") and location != "/" else location
    parts = [] if folder == "/" else folder[1:].split("/")
    for part in parts:
        component(part)
    root_parts = [] if root == "/" else root.lstrip("/").split("/")
    if parts[:len(root_parts)] != root_parts:
        raise ValueError("CSV path is outside configured remote_root")
    return "/".join([*parts[len(root_parts):], name])


def import_csv(state, run, source, progress, checkpoint=None):
    db = state.db
    source = Path(source).expanduser().absolute()
    previous = db.execute("SELECT run_id FROM csv_imports WHERE state='accepted' ORDER BY accepted_at DESC LIMIT 1").fetchone()
    with state.transaction():
        db.execute("INSERT INTO csv_imports(run_id,source_path,previous_run) VALUES(?,?,?)",
                   (run, str(source), previous[0] if previous else None))
    batch, diagnostics = [], []
    count, row_number = 0, None
    progress_at = time.monotonic()

    def flush():
        nonlocal progress_at
        with state.transaction():
            for number, doc_id, name, location, remote in batch:
                parent = remote.rpartition("/")[0]
                db.execute("""INSERT INTO csv_entries(run_id,row_number,document_id,document_name,location_path,remote_path,parent)
                    VALUES(?,?,?,?,?,?,?)""", (run, number, doc_id, name, location, remote, parent))
                ancestor = parent
                while True:
                    db.execute("""INSERT OR IGNORE INTO csv_ancestor_checks(run_id,remote,parent,depth)
                        VALUES(?,?,?,?)""", (run, ancestor, ancestor.rpartition("/")[0] if ancestor else None,
                                            ancestor.count("/") + 1 if ancestor else 0))
                    if not ancestor:
                        break
                    ancestor = ancestor.rpartition("/")[0]
            db.executemany("INSERT INTO csv_import_errors(run_id,row_number,category,message) VALUES(?,?,?,?)",
                           ((run, *error) for error in diagnostics))
            db.execute("UPDATE csv_imports SET expected_count=? WHERE run_id=?", (count, run))
        batch.clear()
        diagnostics.clear()
        if checkpoint:
            checkpoint("csv_import_batch", None)
        if time.monotonic() - progress_at >= 5:
            LOG.info("CSV import: %d document rows read", count)
            progress_at = time.monotonic()

    previous_limit = csv.field_size_limit()
    csv.field_size_limit(sys.maxsize)
    try:
        with source.open("rb", buffering=0) as raw, progress.bar(desc="Importing CSV", unit="row") as bar:
            hashed = _HashedInput(raw)
            with io.TextIOWrapper(io.BufferedReader(hashed), encoding="utf-8-sig", newline="") as stream:
                reader = csv.reader(_strict_lines(stream), strict=True)
                header = next((row for row in reader if row), None)
                if header is None:
                    raise ValueError("CSV has no header or document rows")
                header = [name.strip().casefold() for name in header]
                if not all(header) or len(set(header)) != len(header):
                    raise ValueError("CSV headers must be named and unique after normalization")
                if not set(REQUIRED) <= set(header):
                    raise ValueError("Missing required CSV headers: DOCUMENT ID, DOCUMENT NAME, LOCATION PATH")
                indices = [header.index(name) for name in REQUIRED]
                while True:
                    row_number = reader.line_num + 1
                    row = next(reader, None)
                    if row is None:
                        break
                    if not row:
                        continue
                    count += 1
                    bar.update(1)
                    try:
                        if len(row) != len(header):
                            raise ValueError(f"Expected {len(header)} fields; found {len(row)}")
                        doc_id, name, location = (row[i] for i in indices)
                        if not doc_id.strip() or not name or not location:
                            raise ValueError("All three required values must be nonempty")
                        remote = resolve_path(state.cfg.remote_root, name, location)
                        batch.append((row_number, doc_id, name, location, remote))
                    except (ValueError, PathError) as exc:
                        diagnostics.append((row_number, "invalid_row", str(exc)))
                    if len(batch) + len(diagnostics) >= BATCH_SIZE:
                        flush()
                flush()
            with state.transaction():
                db.execute("UPDATE csv_imports SET digest=? WHERE run_id=?", (hashed.digest.hexdigest(), run))
                db.execute("""INSERT INTO csv_import_errors(run_id,row_number,category,message)
                    SELECT e.run_id,e.row_number,'path_is_ancestor','Listed PDF is also an ancestor folder: ' || e.remote_path
                    FROM csv_entries e JOIN csv_ancestor_checks a ON a.run_id=e.run_id AND a.remote=e.remote_path
                    WHERE e.run_id=?""", (run,))
                if not count:
                    db.execute("INSERT INTO csv_import_errors(run_id,category,message) VALUES(?,'empty_input','CSV has no document rows')", (run,))
        errors = db.execute("SELECT COUNT(*) FROM csv_import_errors WHERE run_id=?", (run,)).fetchone()[0]
        if errors:
            with state.transaction():
                db.execute("UPDATE csv_imports SET state='rejected' WHERE run_id=?", (run,))
            raise FatalError(f"CSV input rejected: {errors} validation error(s); see import-errors report")
        with state.transaction():
            db.execute("UPDATE csv_imports SET state='accepted',accepted_at=? WHERE run_id=?", (utcnow(), run))
        LOG.info("CSV input accepted: %d documents", count)
    except (OSError, UnicodeError, csv.Error, ValueError, KeyboardInterrupt) as exc:
        # Do not flush uncommitted rows after cancellation. The accepted snapshot
        # and live path registry have not been touched.
        # A signal can arrive immediately after the acceptance COMMIT. As with
        # transfer recovery, the durable state wins over the interrupted stack.
        if db.execute("SELECT state FROM csv_imports WHERE run_id=?", (run,)).fetchone()[0] == "accepted":
            raise
        with state.transaction():
            db.executemany("INSERT INTO csv_import_errors(run_id,row_number,category,message) VALUES(?,?,?,?)",
                           ((run, *error) for error in diagnostics))
            db.execute("UPDATE csv_imports SET state=?,expected_count=? WHERE run_id=?",
                       ("interrupted" if isinstance(exc, KeyboardInterrupt) else "rejected", count, run))
            db.execute("INSERT INTO csv_import_errors(run_id,row_number,category,message) VALUES(?,?,?,?)",
                       (run, row_number, "interrupted" if isinstance(exc, KeyboardInterrupt) else "invalid_csv",
                        str(exc) or "CSV import interrupted"))
        if isinstance(exc, KeyboardInterrupt):
            raise
        raise FatalError(f"CSV input rejected: {exc}") from exc
    finally:
        csv.field_size_limit(previous_limit)
