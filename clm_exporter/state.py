"""Durable state. Network work must happen outside transaction() blocks."""
import json
import os
import sqlite3
import uuid
from contextlib import closing, contextmanager
from datetime import UTC, datetime

from .errors import FatalError
from .local import sync_directory

SCHEMA_VERSION = 1
SCHEMA = """
CREATE TABLE source (
 id INTEGER PRIMARY KEY CHECK(id=1), identity TEXT NOT NULL, destination TEXT NOT NULL,
 host_key_fingerprint TEXT, filesystem TEXT NOT NULL
);
CREATE TABLE runs (
 id TEXT PRIMARY KEY, command TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT,
 scan_started_at TEXT, scan_ended_at TEXT, scan_status TEXT NOT NULL DEFAULT 'not_started',
 stop_reason TEXT, summary TEXT
);
CREATE TABLE paths (
 id INTEGER PRIMARY KEY, remote TEXT NOT NULL UNIQUE COLLATE BINARY,
 parent TEXT NOT NULL, name TEXT NOT NULL, kind TEXT NOT NULL,
 local TEXT UNIQUE, collision_key TEXT UNIQUE, last_seen_run TEXT REFERENCES runs(id),
 size INTEGER, mtime TEXT, selected INTEGER NOT NULL DEFAULT 0, attributes TEXT
);
CREATE INDEX paths_seen ON paths(last_seen_run, selected);
CREATE INDEX paths_parent ON paths(parent);
CREATE TABLE observations (
 run_id TEXT NOT NULL REFERENCES runs(id), path_id INTEGER NOT NULL REFERENCES paths(id),
 kind TEXT NOT NULL, size INTEGER, mtime TEXT, selected INTEGER NOT NULL, attributes TEXT,
 PRIMARY KEY(run_id,path_id)
);
CREATE TABLE scans (
 run_id TEXT REFERENCES runs(id), remote TEXT NOT NULL, state TEXT NOT NULL,
 PRIMARY KEY(run_id, remote)
);
CREATE TABLE transfers (
 id INTEGER PRIMARY KEY, path_id INTEGER NOT NULL REFERENCES paths(id),
 size INTEGER NOT NULL, mtime TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('pending','downloading','ready','downloaded','failed','superseded')),
 attempts INTEGER NOT NULL DEFAULT 0, attempt_run TEXT REFERENCES runs(id), temp TEXT UNIQUE,
 received INTEGER, sha256 TEXT, completion_run TEXT REFERENCES runs(id), completed_at TEXT,
 last_error TEXT, forced INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX active_transfer ON transfers(path_id)
 WHERE state IN ('pending','downloading','ready','failed');
CREATE INDEX transfers_pending ON transfers(state, path_id);
CREATE INDEX latest_success ON transfers(path_id, id DESC) WHERE state='downloaded';
CREATE TABLE errors (
 id INTEGER PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), timestamp TEXT NOT NULL,
 remote TEXT NOT NULL, operation TEXT NOT NULL, category TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0, message TEXT NOT NULL,
 resolved_at TEXT, resolved_run TEXT REFERENCES runs(id)
);
CREATE INDEX unresolved_errors ON errors(remote, operation) WHERE resolved_at IS NULL;
PRAGMA user_version=1;
"""


def utcnow():
    return datetime.now(UTC).isoformat(timespec="microseconds")


def metadata_time(value):
    # JSON preserves SFTP's integer seconds, and fractional precision if supplied.
    return json.dumps(value, allow_nan=False, separators=(",", ":"))


class State:
    def __init__(self, cfg, initialize=False, filesystem=None):
        self.cfg = cfg
        created = False
        if initialize:
            try:
                fd = os.open(cfg.database, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                os.close(fd)
                created = True
            except FileExistsError as exc:
                raise FatalError("State already exists; init will not replace it") from exc
        elif not cfg.database.is_file():
            raise FatalError("State database is missing; use init only for a new export")
        self.db = None
        try:
            self.db = sqlite3.connect(cfg.database.as_uri() + "?mode=rw", uri=True, isolation_level=None)
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA foreign_keys=ON")
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
            if created:
                self.db.executescript("BEGIN IMMEDIATE;" + SCHEMA)
                self.db.execute("INSERT INTO source VALUES(1,?,?,NULL,?)",
                                (json.dumps(cfg.identity), str(cfg.export), json.dumps(filesystem or {})))
                self.db.execute("COMMIT")
                sync_directory(cfg.state_dir)
            version = self.db.execute("PRAGMA user_version").fetchone()[0]
            if version != SCHEMA_VERSION:
                raise FatalError(f"Unsupported database schema {version}; expected {SCHEMA_VERSION}")
            source = self.db.execute("SELECT * FROM source WHERE id=1").fetchone()
            if source is None or json.loads(source["identity"]) != cfg.identity or source["destination"] != str(cfg.export):
                raise FatalError("Database is bound to a different source or destination")
        except BaseException:
            remove_created = created and self.db is None
            if self.db is not None:
                try:
                    self.db.rollback()
                    if created:
                        # Inspect after rollback: even an interrupt immediately
                        # after COMMIT must retain the initialized database.
                        remove_created = self.db.execute("PRAGMA user_version").fetchone()[0] == 0
                except sqlite3.Error:
                    # Preserve state when its committed status cannot be read.
                    pass
                finally:
                    self.db.close()
            if remove_created:
                cfg.database.unlink(missing_ok=True)
            raise

    def close(self):
        self.db.close()

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            # COMMIT may have finished before an interrupt is delivered, and
            # SQLite can roll back automatically after a statement fails.
            # rollback() is also safe when no transaction remains active.
            self.db.rollback()
            raise

    def run(self, command):
        run = datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:12]
        with self.transaction():
            self.db.execute("INSERT INTO runs(id,command,started_at) VALUES(?,?,?)", (run, command, utcnow()))
        return run

    def latest(self, path_id):
        return self.db.execute("SELECT * FROM transfers WHERE path_id=? AND state='downloaded' ORDER BY id DESC LIMIT 1",
                               (path_id,)).fetchone()

    def job(self, path, run, forced=False):
        active = self.db.execute("SELECT * FROM transfers WHERE path_id=? AND state IN ('pending','failed','downloading','ready')",
                                 (path["id"],)).fetchone()
        # An explicit repeat remains requested until it succeeds, even if its
        # metadata changes while the transfer is deferred or being retried.
        forced = forced or bool(active and active["forced"])
        if active and (active["size"], active["mtime"]) == (path["size"], path["mtime"]):
            if forced:
                self.db.execute("UPDATE transfers SET forced=1 WHERE id=?", (active["id"],))
            return active["id"]
        if active and active["state"] in ("ready", "downloading"):
            # A recovery conflict must not orphan the only completed temporary payload.
            return active["id"]
        if active:
            self.db.execute("UPDATE transfers SET state='superseded' WHERE id=?", (active["id"],))
        return self.db.execute("INSERT INTO transfers(path_id,size,mtime,state,forced) VALUES(?,?,?,'pending',?)",
                               (path["id"], path["size"], path["mtime"], int(forced))).lastrowid

    def error(self, run, remote, operation, category, message, attempts=0):
        # Invalid remote names must remain reportable through SQLite and UTF-8 CSV.
        remote = remote.encode("utf-8", "backslashreplace").decode("utf-8")
        message = str(message).encode("utf-8", "backslashreplace").decode("utf-8")
        with self.transaction():
            self.db.execute("INSERT INTO errors(run_id,timestamp,remote,operation,category,attempts,message) VALUES(?,?,?,?,?,?,?)",
                            (run, utcnow(), remote, operation, category, attempts, message))

    def resolve(self, run, remote, operations):
        for operation in operations:
            self.db.execute("UPDATE errors SET resolved_at=?,resolved_run=? WHERE remote=? AND operation=? AND resolved_at IS NULL",
                            (utcnow(), run, remote, operation))

    def backup(self, run):
        target = self.cfg.backups / f"export-{run}.sqlite3"
        temp = target.with_suffix(".tmp")
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        try:
            with closing(sqlite3.connect(temp)) as destination:
                self.db.backup(destination)
            with temp.open("rb") as f:
                os.fsync(f.fileno())
            os.replace(temp, target)
            sync_directory(target.parent)
        finally:
            temp.unlink(missing_ok=True)
        return target
