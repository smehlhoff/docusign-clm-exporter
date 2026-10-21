"""CSV rows share path checks and transfers without losing document identities."""
import csv
import hashlib
import io
import json
import sqlite3
import tempfile
import threading
import unittest
from collections import Counter
from contextlib import closing, redirect_stderr
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from clm_exporter.cli import execute
from clm_exporter.config import Config
from clm_exporter.errors import FatalError
from clm_exporter.local import prepare_layout
from clm_exporter.state import SCHEMA, SCHEMA_V2, SCHEMA_V3, State

from .test_concurrency import Sessions


class DuplicatePathTests(unittest.TestCase):
    def configure(self, workers=1):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.cfg = Config("unused.invalid", "operator", self.root / "key", self.root / "hosts",
                          "/Company A", self.root / "export", workers=workers, min_free_bytes=0)
        self.sessions = Sessions()
        self.remote = self.sessions.source
        self.source = self.root / "input.csv"
        self.calls = Counter()
        lock = threading.Lock()

        def count(path):
            with lock:
                self.calls[path] += 1
        self.remote.stat_hook = count
        self.invoke("init")

    def invoke(self, command, **options):
        factory = self.sessions.factory if self.cfg.workers > 1 else self.remote.factory
        with redirect_stderr(io.StringIO()):
            return execute(self.cfg, command, remote_factory=factory, progress=False,
                           sleep=lambda _: None, **options)

    def run_csv(self, command="download", **options):
        return self.invoke(command, csv_path=self.source, **options)

    def write_input(self, rows):
        with self.source.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["DOCUMENT ID", "DOCUMENT NAME", "LOCATION PATH"])
            writer.writerows(rows)

    def report(self, summary, name="documents"):
        with Path(summary["reports"][name]).open(encoding="utf-8", newline="") as stream:
            return list(csv.DictReader(stream))

    def test_shared_checks_transfers_and_counts_in_both_schedulers(self):
        for workers in (1, 3):
            with self.subTest(workers=workers):
                self.configure(workers)
                self.remote.add("a.pdf", b"abc")
                self.remote.add("A.pdf", b"defg")
                rows = [("same", "a.pdf", "/Company A/"), ("upper", "A.pdf", "/Company A"),
                        ("same", "a.pdf", "/Company A"), ("different", "a.pdf", "/Company A/")]
                self.write_input(rows)
                code, inventory = self.run_csv("inventory")
                self.assertEqual(code, 0)
                self.assertEqual(self.calls, {"": 1, "a.pdf": 1, "A.pdf": 1})
                self.assertEqual(self.remote.opens, [])
                self.assertEqual(inventory["expected_document_count"], 4)
                self.assertEqual(inventory["unique_path_count"], 2)
                self.assertEqual(inventory["duplicate_row_count"], 2)
                self.assertEqual(inventory["document_outcomes"]["pending"], 4)
                self.assertEqual(inventory["pending_deferred_count"], 2)
                self.assertEqual(inventory["pending_deferred_bytes"], 7)
                self.assertEqual(len(self.report(inventory, "gaps")), 2)

                self.calls.clear()
                code, downloaded = self.run_csv()
                self.assertEqual(code, 0)
                self.assertTrue(downloaded["local_download_complete"])
                self.assertEqual(self.calls, {"": 1, "a.pdf": 3, "A.pdf": 3})
                self.assertCountEqual(self.remote.opens, ["a.pdf", "A.pdf"])
                self.assertEqual(downloaded["document_outcomes"]["downloaded_this_run"], 4)
                self.assertEqual(downloaded["new_successes"], 2)
                self.assertEqual(downloaded["downloaded_bytes"], 7)
                self.assertEqual(downloaded["bytes_discovered"], 7)
                self.assertEqual(downloaded["pdfs_discovered"], 2)
                self.assertEqual(downloaded["latest_version_coverage"], 2)
                self.assertEqual(downloaded["metadata_checked"], 4)
                self.assertEqual(downloaded["recovered_outside_scope"], 0)
                docs = self.report(downloaded)
                self.assertEqual([(d["document_id"], d["document_name"], d["location_path"]) for d in docs], rows)
                shared = [docs[i] for i in (0, 2, 3)]
                for field in ("transfer_id", "sha256", "mapped_path", "completion_run"):
                    self.assertEqual(len({d[field] for d in shared}), 1)
                self.assertNotEqual(docs[0]["mapped_path"], docs[1]["mapped_path"])

                for command in ("reconcile", "status"):
                    code, current = self.run_csv(command) if command == "reconcile" else self.invoke(command)
                    self.assertEqual(code, 0)
                    self.assertTrue(current["local_download_complete"])
                    self.assertEqual(current["document_outcomes"]["previously_downloaded"], 4)
                    self.assertEqual(current["unchanged_skips"], 2)
                self.assertEqual(len(self.remote.opens), 2)

                code, forced = self.run_csv(force_path="a.pdf")
                self.assertEqual(code, 0)
                self.assertEqual(forced["document_outcomes"]["downloaded_this_run"], 3)
                self.assertEqual(forced["forced_successes"], 1)
                self.assertEqual(forced["changed_or_repeated_successes"], 1)
                self.assertEqual(forced["downloaded_bytes"], 3)
                self.remote.add("a.pdf", b"changed", mtime=2)
                code, changed = self.run_csv()
                self.assertEqual(code, 0)
                self.assertEqual(changed["changed_or_repeated_successes"], 1)
                self.assertEqual(changed["downloaded_bytes"], 7)
                self.assertEqual(Counter(self.remote.opens), {"a.pdf": 3, "A.pdf": 1})
                _, rebuilt = self.invoke("status", report_run=downloaded["run_id"])
                historical = self.report(rebuilt)
                self.assertEqual([d["transfer_id"] for d in historical], [d["transfer_id"] for d in docs])
                self.assertEqual([d["outcome"] for d in historical], ["downloaded_this_run"] * 4)
                self.assertEqual(historical[0]["local_payload_status"], "replaced")
                self.assertEqual(self.sessions.active, 0)

    def test_duplicate_failures_are_shared_without_duplicate_requests_or_errors(self):
        for workers in (1, 3):
            with self.subTest(workers=workers):
                self.configure(workers)
                self.remote.add("a.pdf")
                self.remote.add("b.pdf")
                self.remote.denied.add("a.pdf")
                self.write_input([(str(i), name, "/Company A") for i, name in enumerate(
                    ["a.pdf", "b.pdf", "a.pdf", "b.pdf"])])
                code, failed = self.run_csv()
                self.assertEqual(code, 2)
                self.assertEqual(self.calls["a.pdf"], 1)
                self.assertEqual(self.remote.opens, ["b.pdf"])
                self.assertEqual(failed["document_outcomes"]["failed"], 2)
                self.assertEqual(failed["metadata_failed"], 2)
                self.assertEqual(failed["unresolved_file_errors"], 1)
                self.assertEqual(failed["pending_deferred_count"], 1)
                self.assertEqual(len(self.report(failed, "gaps")), 1)
                docs = self.report(failed)
                self.assertEqual(docs[0]["error_message"], docs[2]["error_message"])
                self.remote.denied.clear()
                code, resumed = self.run_csv()
                self.assertEqual(code, 0)
                self.assertTrue(resumed["local_download_complete"])
                self.assertEqual(resumed["new_successes"], 1)
                self.assertCountEqual(self.remote.opens, ["b.pdf", "a.pdf"])

    def test_duplicates_across_import_batches_are_checked_once_with_shared_retry(self):
        self.configure(3)
        self.remote.add("a.pdf")
        self.write_input([(str(i), "a.pdf", "/Company A/") for i in range(1001)])
        counted = self.remote.stat_hook

        def transient(path):
            counted(path)
            if path == "a.pdf" and self.calls[path] == 1:
                raise ConnectionError("transient metadata failure")
        self.remote.stat_hook = transient
        code, summary = self.run_csv("inventory")
        self.assertEqual(code, 0)
        self.assertEqual(self.calls["a.pdf"], 2)
        self.assertEqual(summary["expected_document_count"], 1001)
        self.assertEqual(summary["duplicate_row_count"], 1000)
        self.assertEqual(summary["document_outcomes"]["pending"], 1001)
        self.assertEqual(summary["pending_deferred_count"], 1)
        self.assertEqual(summary["pending_deferred_bytes"], 3)
        with closing(sqlite3.connect(self.cfg.database)) as db:
            self.assertEqual(db.execute("SELECT DISTINCT attempts,lookup_state FROM csv_entries").fetchall(), [(2, "checked")])
        self.assertEqual(len(self.report(summary)), 1001)

    def test_capacity_offload_and_absence_are_path_based(self):
        self.configure(3)
        self.cfg = replace(self.cfg, max_staged_bytes=3)
        self.remote.add("a.pdf")
        self.remote.add("b.pdf")
        self.write_input([(str(i), name, "/Company A") for i, name in enumerate(
            ["a.pdf", "b.pdf", "a.pdf", "b.pdf"])])
        code, first = self.run_csv()
        self.assertEqual(code, 0)
        self.assertEqual(first["stop_reason"], "batch_capacity")
        self.assertEqual(first["document_outcomes"]["downloaded_this_run"], 2)
        self.assertEqual(first["document_outcomes"]["pending"], 2)
        self.assertEqual(first["pending_deferred_count"], 1)
        self.assertEqual(first["pending_deferred_bytes"], 3)
        (self.cfg.export / "a.pdf").unlink()
        code, second = self.run_csv()
        self.assertEqual(code, 0)
        self.assertTrue(second["local_download_complete"])
        self.assertEqual(second["unchanged_skips"], 1)
        self.assertEqual(second["downloaded_bytes"], 3)
        self.assertEqual(self.remote.opens, ["a.pdf", "b.pdf"])
        self.write_input([("only", "a.pdf", "/Company A")])
        code, third = self.run_csv("reconcile")
        self.assertEqual(code, 0)
        absent = self.report(third, "absent_from_report")
        self.assertEqual([d["remote_path"] for d in absent], ["b.pdf", "b.pdf"])
        self.assertEqual([d["document_id"] for d in absent], ["1", "3"])

    def test_ready_recovery_and_invalid_rows_preserve_duplicate_identities(self):
        for workers in (1, 3):
            with self.subTest(workers=workers):
                self.configure(workers)
                self.remote.add("a.pdf")
                rows = [("one", "a.pdf", "/Company A"), ("two", "a.pdf", "/Company A/")]
                self.write_input(rows + [("bad", "../escape.pdf", "/Company A")])
                code, rejected = self.run_csv()
                self.assertEqual(code, 1)
                self.assertEqual(rejected["import_status"], "rejected")
                self.assertEqual(self.calls, {})
                self.assertEqual(self.remote.opens, [])
                self.write_input(rows)

                def stop(event, job):
                    if event == "ready":
                        raise KeyboardInterrupt()
                code, stopped = self.run_csv(checkpoint=stop)
                self.assertEqual(code, 130)
                self.assertEqual(stopped["document_outcomes"]["pending"], 2)
                code, recovered = self.run_csv("inventory")
                self.assertEqual(code, 0)
                self.assertTrue(recovered["local_download_complete"])
                self.assertEqual(recovered["document_outcomes"]["downloaded_this_run"], 2)
                self.assertEqual(recovered["new_successes"], 1)
                self.assertEqual(recovered["recovered_outside_scope"], 0)
                self.assertEqual(self.remote.opens, ["a.pdf"])
                self.assertEqual(len({d["transfer_id"] for d in self.report(recovered)}), 1)


class DuplicateMigrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.cfg = Config("unused.invalid", "operator", self.root / "key", self.root / "hosts",
                          "/", self.root / "export", min_free_bytes=0)
        prepare_layout(self.cfg)
        with closing(sqlite3.connect(self.cfg.database)) as db:
            db.executescript(SCHEMA + SCHEMA_V2)
            db.execute("INSERT INTO source VALUES(1,?,?,NULL,'{}')", (json.dumps(self.cfg.identity), str(self.cfg.export)))
            db.execute("""INSERT INTO runs(id,command,started_at,ended_at,stop_reason,discovery_mode)
                VALUES('old','download','2020','2021','completed','csv')""")
            db.execute("""INSERT INTO paths(id,remote,parent,name,kind,local,collision_key,size,mtime,selected,last_seen_run)
                VALUES(42,'a.pdf','','a.pdf','file','a.pdf','a.pdf',3,'1',1,'old')""")
            db.execute("""INSERT INTO transfers(id,path_id,size,mtime,state,sha256,completion_run)
                VALUES(77,42,3,'1','downloaded',?,'old')""", (hashlib.sha256(b"pdf").hexdigest(),))
            db.execute("""INSERT INTO csv_imports(run_id,source_path,state,expected_count,accepted_at,mapping_complete)
                VALUES('old','old.csv','accepted',1,'2020',1)""")
            db.execute("""INSERT INTO csv_entries VALUES('old',2,'original-id','a.pdf','/','a.pdf','',42,'checked',1,NULL,NULL)""")
            db.execute("INSERT INTO run_results VALUES('old','2021',?)", (json.dumps({"local_download_complete": True}),))
            db.execute("INSERT INTO result_rows VALUES('old','documents',0,?)", (json.dumps({"document_id": "original-id"}),))
            db.commit()
            self.old_rows = db.execute("SELECT * FROM csv_entries").fetchall()

    def test_v2_upgrade_preserves_rows_ids_results_and_prevents_redownload(self):
        state = State(self.cfg)
        try:
            self.assertEqual(state.db.execute("PRAGMA user_version").fetchone()[0], 3)
            self.assertEqual([tuple(row) for row in state.db.execute("SELECT * FROM csv_entries")], self.old_rows)
            self.assertEqual(state.db.execute("PRAGMA foreign_key_check").fetchall(), [])
            self.assertEqual(state.db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        finally:
            state.close()
        backup = next(self.cfg.backups.glob("*pre-migration-v3*"))
        with closing(sqlite3.connect(backup)) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT * FROM csv_entries").fetchall(), self.old_rows)
        source = self.root / "new.csv"
        source.write_text("DOCUMENT ID,DOCUMENT NAME,LOCATION PATH\none,a.pdf,/\ntwo,a.pdf,/\n")
        remote = Sessions().source
        remote.add("a.pdf")
        with redirect_stderr(io.StringIO()):
            code, summary = execute(self.cfg, "download", csv_path=source, remote_factory=remote.factory, progress=False)
        self.assertEqual(code, 0)
        self.assertEqual(remote.opens, [])
        self.assertEqual(summary["document_outcomes"]["previously_downloaded"], 2)
        self.assertEqual(summary["unchanged_skips"], 1)
        with closing(sqlite3.connect(self.cfg.database)) as db:
            self.assertEqual(db.execute("SELECT id,path_id FROM transfers").fetchall(), [(77, 42)])
            self.assertEqual(db.execute("SELECT * FROM csv_entries WHERE run_id='old'").fetchall(), self.old_rows)
            self.assertEqual(json.loads(db.execute("SELECT data FROM result_rows WHERE run_id='old'").fetchone()[0]),
                             {"document_id": "original-id"})

    def test_v2_migration_failure_rolls_back_and_checks_binding_before_backup(self):
        with patch("clm_exporter.state.SCHEMA_V3", SCHEMA_V3 + "INVALID SQL;"), self.assertRaises(sqlite3.Error):
            State(self.cfg)
        with closing(sqlite3.connect(self.cfg.database)) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT * FROM csv_entries").fetchall(), self.old_rows)
            self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("INSERT INTO csv_entries SELECT run_id,3,document_id,document_name,location_path,remote_path,parent,path_id,lookup_state,attempts,category,message FROM csv_entries")
        backups = list(self.cfg.backups.iterdir())
        with self.assertRaises(FatalError):
            State(replace(self.cfg, remote_root="/other"))
        self.assertEqual(list(self.cfg.backups.iterdir()), backups)
        with patch.object(State, "backup", side_effect=OSError("backup unavailable")), self.assertRaises(OSError):
            State(self.cfg)
        with closing(sqlite3.connect(self.cfg.database)) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT * FROM csv_entries").fetchall(), self.old_rows)
