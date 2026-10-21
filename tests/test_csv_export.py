"""CSV acceptance, scoped execution, immutable results, and migration contracts."""
import csv
import hashlib
import io
import json
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from collections import Counter
from contextlib import redirect_stderr
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from clm_exporter.cli import execute, parser
from clm_exporter.config import Config
from clm_exporter.errors import FatalError
from clm_exporter.local import prepare_layout
from clm_exporter.state import SCHEMA, SCHEMA_V2, SCHEMA_VERSION, State

from .fakes import FakeRemote
from .test_concurrency import Sessions


class CsvExportTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.cfg = Config("test", "user", self.root / "key", self.root / "hosts", "/Main", self.root / "export",
                          min_free_bytes=0, max_staged_bytes=10000, chunk_bytes=2)
        self.remote = FakeRemote()
        self.source = self.root / "report.csv"
        self.invoke("init")

    def invoke(self, command="download", **kwargs):
        with redirect_stderr(io.StringIO()):
            return execute(self.cfg, command, remote_factory=kwargs.pop("remote_factory", self.remote.factory),
                           progress=False, sleep=lambda _: None, **kwargs)

    def run_csv(self, command="download", **kwargs):
        return self.invoke(command, csv_path=self.source, **kwargs)

    def input(self, paths, ids=None):
        with self.source.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["DOCUMENT ID", "DOCUMENT NAME", "LOCATION PATH"])
            for index, path in enumerate(paths):
                parent, _, name = path.rpartition("/")
                writer.writerow([ids[index] if ids else f"id-{index}", name, self.cfg.remote_root.rstrip("/") + "/" + parent])

    def rows(self, query, params=()):
        with sqlite3.connect(self.cfg.database) as db:
            db.row_factory = sqlite3.Row
            return db.execute(query, params).fetchall()

    def report(self, summary, name="documents"):
        with Path(summary["reports"][name]).open(newline="") as stream:
            return list(csv.DictReader(stream))

    def test_parser_exposes_csv_only_on_online_commands(self):
        for command in ("download", "inventory", "reconcile"):
            self.assertEqual(parser().parse_args([command, "--config", "cfg", "--csv", "input.csv"]).csv, Path("input.csv"))
        for command in ("init", "status"):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parser().parse_args([command, "--config", "cfg", "--csv", "input.csv"])

    def test_sample_roots_and_no_enumeration(self):
        for root, relative in (("/", "Main/Contracts/test.pdf"), ("/Main", "Contracts/test.pdf"), ("/Main/Contracts", "test.pdf")):
            with self.subTest(root=root):
                self.cfg = replace(self.cfg, remote_root=root, working_directory=self.root / f"export-{len(root)}")
                self.invoke("init")
                self.remote = FakeRemote()
                self.remote.add(relative)
                self.source.write_text("DOCUMENT ID,DOCUMENT NAME,LOCATION PATH\nD02DC5F5-466A-43C0-95A8-A6B9F05C5F1F,test.pdf,/Main/Contracts/\n")
                with patch.object(self.remote, "iterdir", side_effect=AssertionError("No listing")), patch.object(self.remote, "check_directory", side_effect=AssertionError("No probe")):
                    code, summary = self.run_csv()
                self.assertEqual(code, 0)
                self.assertTrue(summary["local_download_complete"])
                self.assertEqual(self.remote.opens, [relative])
                self.assertEqual(self.report(summary)[0]["remote_path"], relative)
                self.assertEqual((self.cfg.export / relative).read_bytes(), b"pdf")

    def test_bom_reordered_columns_extra_values_unicode_quotes_and_digest(self):
        data = '\ufeff EXTRA , location path ,Document Name,document id\r\nignored,/Main/é/,"a, β.PdF", ID  \r\n\r\n'
        self.source.write_bytes(data.encode())
        self.remote.add("é/a, β.PdF")
        code, summary = self.run_csv()
        self.assertEqual(code, 0)
        self.assertEqual(summary["input_digest"], hashlib.sha256(data.encode()).hexdigest())
        doc = self.report(summary)[0]
        self.assertEqual((doc["document_id"], doc["document_name"], doc["location_path"]), (" ID  ", "a, β.PdF", "/Main/é/"))

    def test_large_extra_column_is_ignored_without_changing_csv_reader_limit(self):
        self.source.write_text("DOCUMENT ID,DOCUMENT NAME,LOCATION PATH,DETAILS\nid,a.pdf,/Main," + "x" * 200000 + "\n")
        self.remote.add("a.pdf")
        previous = csv.field_size_limit()
        code, summary = self.run_csv()
        self.assertEqual(code, 0)
        self.assertEqual(csv.field_size_limit(), previous)
        self.assertEqual(summary["expected_document_count"], 1)

    def test_parse_failure_preserves_earlier_buffered_row_diagnostics(self):
        self.source.write_text('DOCUMENT ID,DOCUMENT NAME,LOCATION PATH\nid,a.txt,/Main\nid,"broken.pdf,/Main\n')
        code, summary = self.run_csv()
        self.assertEqual(code, 1)
        self.assertEqual([row["category"] for row in self.report(summary, "import_errors")], ["invalid_row", "invalid_csv"])

    def test_repeated_root_separators_reject_before_network_or_recovery(self):
        self.cfg = replace(self.cfg, remote_root="/", working_directory=self.root / "root-export")
        self.invoke("init")
        self.remote.add("a.pdf")
        for location in ("//", "///", "//folder", "/folder//", "/folder//child"):
            with self.subTest(location=location):
                self.source.write_text(f"DOCUMENT ID,DOCUMENT NAME,LOCATION PATH\nid,a.pdf,{location}\n")
                with patch("clm_exporter.transfer.Transfers.recover", side_effect=AssertionError("No recovery")):
                    code, summary = self.run_csv()
                self.assertEqual(code, 1)
                self.assertEqual(summary["import_status"], "rejected")
                self.assertEqual(self.report(summary, "import_errors")[0]["row_number"], "2")
                self.assertEqual(self.rows("SELECT * FROM paths"), [])
        self.assertEqual(self.remote.connections, 0)
        self.source.write_text("DOCUMENT ID,DOCUMENT NAME,LOCATION PATH\nid,a.pdf,/\n")
        code, summary = self.run_csv()
        self.assertEqual(code, 0)
        self.assertTrue(summary["local_download_complete"])

    def test_invalid_inputs_reject_before_network_recovery_or_mapping(self):
        header = "DOCUMENT ID,DOCUMENT NAME,LOCATION PATH\n"
        cases = [
            "", header, "ID,NAME,PATH\na,a.pdf,/Main\n", "DOCUMENT ID,DOCUMENT NAME,LOCATION PATH,document id\na,a.pdf,/Main,b\n",
            header + "id,a.pdf,/Main,extra\n",
            header + 'id,"a.pdf,/Main\n', header + 'id,a".pdf,/Main\n', header + 'id,"a.pdf"bad,/Main\n',
            header + "id,a.pdf,/MainArchive\n", header + "id,a.txt,/Main\n", header + " ,a.pdf,/Main\n",
            header + "id,../a.pdf,/Main\n", header + "id,a.pdf,Main\n", header + "id,a.pdf,/Main//X\n",
            header + "id,a.pdf,/Main/./X\n", header + "id,a.pdf,/Main/../Main\n", header + "id,a.pdf,/Main\\X\n",
            header + "id,a.pdf,/Main///\n", header + "id,a.pdf,/Main\x00\n",
            header + "id,a.pdf,/Main\nid,b.pdf,/Main/a.pdf\n",
            (header + "id,a.pdf,/Main\n").encode() + b"\xff",
        ]
        for content in cases:
            with self.subTest(content=repr(content)):
                self.source.write_bytes(content.encode() if isinstance(content, str) else content)
                with patch("clm_exporter.transfer.Transfers.recover", side_effect=AssertionError("No recovery")):
                    code, summary = self.run_csv()
                self.assertEqual(code, 1)
                self.assertEqual(summary["import_status"], "rejected")
                self.assertFalse(summary["local_download_complete"])
                self.assertTrue(self.report(summary, "import_errors"))
                self.assertEqual(self.rows("SELECT * FROM paths"), [])
                self.assertEqual(self.report(summary), [])
        self.assertEqual(self.remote.connections, 0)

    def test_ids_are_metadata_and_history_is_shared_with_recursive_mode(self):
        self.remote.add("a.pdf")
        self.remote.add("folder/b.pdf")
        self.invoke()
        self.input(["a.pdf", "folder/b.pdf"], ["same", "same"])
        _, first = self.run_csv()
        self.assertEqual(first["unchanged_skips"], 2)
        self.input(["a.pdf", "folder/b.pdf"], ["changed", "changed"])
        _, second = self.run_csv()
        self.assertEqual(second["unchanged_skips"], 2)
        self.assertEqual(second["absence_count"], 0)
        self.assertEqual(len(self.remote.opens), 2)
        self.invoke("status", report_run=first["run_id"])
        self.assertEqual({row["document_id"] for row in self.report(first)}, {"same"})

    def test_missing_denied_invalid_metadata_and_directory_keep_healthy_files(self):
        self.input(["missing.pdf", "denied.pdf", "unknown.pdf", "folder.pdf", "ok.pdf"])
        for name in ("denied.pdf", "unknown.pdf", "ok.pdf"):
            self.remote.add(name)
        self.remote.add("folder.pdf", kind="directory")
        self.remote.denied.add("denied.pdf")
        self.remote.missing_attrs.add("unknown.pdf")
        code, summary = self.run_csv()
        self.assertEqual(code, 2)
        self.assertEqual(summary["document_outcomes"], {"failed": 4, "unchecked": 0, "pending": 0, "downloaded_this_run": 1, "previously_downloaded": 0})
        self.assertEqual(summary["metadata_failed"], 4)
        self.assertEqual(self.remote.opens, ["ok.pdf"])

    def test_shared_symlink_ancestor_blocks_only_descendants(self):
        for workers in (1, 3):
            with self.subTest(workers=workers):
                self.cfg = replace(self.cfg, workers=workers)
                sessions = Sessions()
                self.remote = sessions.source
                self.remote.add("bad", kind="symlink")
                self.remote.add("bad/a.pdf")
                self.remote.add("bad/b.pdf")
                self.remote.add("badly/ok.pdf")
                calls = Counter()
                self.remote.stat_hook = lambda path, calls=calls: calls.update([path])
                self.input(["bad/a.pdf", "bad/b.pdf", "badly/ok.pdf"])
                factory = sessions.factory if workers > 1 else self.remote.factory
                code, summary = self.run_csv(remote_factory=factory)
                self.assertEqual(code, 2)
                self.assertEqual(calls["bad"], 1)
                self.assertEqual(calls["bad/a.pdf"], 0)
                self.assertEqual(summary["document_outcomes"]["failed"], 2)
                self.assertEqual(summary["ancestor_checks"]["failed"], 1)
                self.assertEqual(summary["directories_discovered"], 1)
                self.assertTrue((self.cfg.export / "badly/ok.pdf").exists())

    def test_ambiguous_ancestor_allows_checks_without_false_directory_count(self):
        self.remote.add("folder/ok.pdf")
        self.remote.reported_modes.update({"": None, "folder": stat.S_IFIFO})
        self.input(["folder/ok.pdf"])
        with patch.object(self.remote, "iterdir", side_effect=AssertionError), patch.object(self.remote, "check_directory", side_effect=AssertionError):
            code, summary = self.run_csv()
        self.assertEqual(code, 0)
        self.assertEqual(summary["directories_discovered"], 0)
        self.assertTrue(summary["local_download_complete"])
        self.assertIn('"st_mode": 4096', self.rows("SELECT attributes FROM csv_ancestor_checks WHERE remote='folder'")[0][0])

    def test_recursive_scan_resolves_csv_ancestor_errors_only_after_success(self):
        for workers in (1, 2):
            for ancestor in ("", "folder"):
                with self.subTest(workers=workers, ancestor=ancestor):
                    self.cfg = replace(self.cfg, workers=workers,
                                       working_directory=self.root / f"repaired-{workers}-{ancestor or 'root'}")
                    self.invoke("init")
                    sessions = Sessions()
                    self.remote = sessions.source
                    factory = sessions.factory if workers > 1 else self.remote.factory
                    self.remote.add("folder/a.pdf")
                    self.remote.denied.add(ancestor)
                    self.input(["folder/a.pdf"])
                    code, failed = self.run_csv(remote_factory=factory)
                    self.assertEqual(code, 2)
                    error_id = self.rows("SELECT id FROM errors WHERE operation='csv_ancestor'")[0][0]
                    code, _ = self.invoke(remote_factory=factory)
                    self.assertEqual(code, 2)
                    self.assertIsNone(self.rows("SELECT resolved_run FROM errors WHERE id=?", (error_id,))[0][0])
                    self.remote.denied.clear()
                    code, repaired = self.invoke(remote_factory=factory)
                    self.assertEqual(code, 0)
                    self.assertTrue(repaired["local_download_complete"])
                    self.assertEqual(self.rows("SELECT resolved_run FROM errors WHERE id=?", (error_id,))[0][0], repaired["run_id"])
                    self.invoke("status", report_run=failed["run_id"])
                    self.assertEqual(self.report(failed)[0]["outcome"], "failed")

    def test_recursive_scan_retires_obsolete_csv_ancestor_errors(self):
        for workers in (1, 2):
            for replacement in ("absent", "file"):
                with self.subTest(workers=workers, replacement=replacement):
                    self.cfg = replace(self.cfg, workers=workers,
                                       working_directory=self.root / f"obsolete-{workers}-{replacement}")
                    self.invoke("init")
                    sessions = Sessions()
                    self.remote = sessions.source
                    factory = sessions.factory if workers > 1 else self.remote.factory
                    self.input(["folder/a.pdf"])
                    code, _ = self.run_csv(remote_factory=factory)
                    self.assertEqual(code, 2)
                    if replacement == "file":
                        self.remote.add("folder")
                    code, summary = self.invoke(remote_factory=factory)
                    self.assertEqual(code, 0)
                    self.assertTrue(summary["local_download_complete"])
                    self.assertEqual(self.report(summary, "unresolved"), [])

    def test_interrupted_ancestor_error_logging_keeps_document_diagnostics(self):
        original_error = State.error

        def interrupt(state, run, remote, operation, *args, **kwargs):
            if operation == "csv_ancestor":
                raise KeyboardInterrupt
            return original_error(state, run, remote, operation, *args, **kwargs)

        for workers in (1, 2):
            with self.subTest(workers=workers):
                self.cfg = replace(self.cfg, workers=workers,
                                   working_directory=self.root / f"interrupted-ancestor-{workers}")
                self.invoke("init")
                sessions = Sessions()
                self.remote = sessions.source
                factory = sessions.factory if workers > 1 else self.remote.factory
                self.remote.add("folder/nested/a.pdf")
                self.remote.denied.add("folder")
                self.input(["folder/nested/a.pdf"])
                with patch.object(State, "error", interrupt):
                    code, summary = self.run_csv(remote_factory=factory)
                self.assertEqual(code, 130)
                self.assertEqual(summary["ancestor_checks"]["failed"], 1)
                self.assertEqual(summary["document_outcomes"]["failed"], 1)
                self.assertEqual(summary["document_outcomes"]["unchecked"], 0)
                doc = self.report(summary)[0]
                self.assertEqual((doc["error_category"], doc["error_message"]), ("permission_denied", "denied"))
                self.assertEqual(self.remote.opens, [])
                self.assertEqual(self.rows("SELECT * FROM errors WHERE operation='csv_ancestor'"), [])
                code, rebuilt = self.invoke("status", report_run=summary["run_id"])
                self.assertEqual(code, 130)
                self.assertEqual(self.report(rebuilt)[0], doc)

    def test_metadata_concurrency_and_shared_ancestor_single_check(self):
        self.cfg = replace(self.cfg, workers=2)
        sessions = Sessions()
        for name in ("folder/a.pdf", "folder/b.pdf"):
            sessions.source.add(name)
        barrier = threading.Barrier(2, timeout=5)
        calls = Counter()
        lock = threading.Lock()

        def stat_hook(path):
            with lock:
                calls[path] += 1
            if path.endswith(".pdf"):
                barrier.wait()
        sessions.source.stat_hook = stat_hook
        self.input(["folder/a.pdf", "folder/b.pdf"])
        code, summary = self.run_csv("inventory", remote_factory=sessions.factory)
        self.assertEqual(code, 0)
        self.assertEqual(calls, Counter({"": 1, "folder": 1, "folder/a.pdf": 1, "folder/b.pdf": 1}))
        self.assertEqual(sessions.peak, 2)
        self.assertEqual(sessions.active, 0)
        self.assertEqual(summary["metadata_checked"], 2)

    def test_unrelated_errors_and_old_directory_error_do_not_block_csv(self):
        self.remote.add("bad/a.pdf")
        self.remote.add("folder/ok.pdf")
        self.remote.denied.add("bad")
        self.invoke("inventory")
        with sqlite3.connect(self.cfg.database) as db:
            # Retain an old listing error even after successful direct checks.
            run = db.execute("SELECT id FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()[0]
            db.execute("INSERT INTO errors(run_id,timestamp,remote,operation,category,message) VALUES(?,'now','folder','directory','io_error','old listing failure')", (run,))
        self.input(["folder/ok.pdf"])
        code, summary = self.run_csv()
        self.assertEqual(code, 0)
        self.assertTrue(summary["local_download_complete"])
        self.assertGreaterEqual(summary["historical_unresolved_errors"], 2)
        self.assertTrue(all(row["in_current_scope"] == "False" for row in self.report(summary, "unresolved")))

    def test_force_outside_scope_is_command_error_without_extra_work(self):
        self.remote.add("a.pdf")
        self.remote.add("b.pdf")
        self.invoke("inventory")
        self.input(["a.pdf"])
        code, summary = self.run_csv(force_path="b.pdf")
        self.assertEqual(code, 2)
        self.assertEqual(summary["applicable_command_errors"], 1)
        self.assertFalse(summary["local_download_complete"])
        self.assertEqual(len(self.report(summary)), 1)
        self.assertEqual(self.remote.opens, ["a.pdf"])
        self.assertEqual(sum(summary["document_outcomes"].values()), 1)
        self.assertEqual(self.run_csv()[0], 0)  # Prior command errors do not poison future invocations.

    def test_absence_pending_scope_offload_and_changed_version(self):
        self.remote.add("a.pdf", b"aaa")
        self.remote.add("b.pdf", b"bbb")
        self.input(["a.pdf", "b.pdf"])
        self.run_csv("inventory")
        self.input(["a.pdf"])
        code, first = self.run_csv()
        self.assertEqual(code, 0)
        self.assertEqual(first["absence_count"], 1)
        self.assertEqual(self.report(first, "absent_from_report")[0]["remote_path"], "b.pdf")
        self.assertEqual(self.report(first, "gaps"), [])
        (self.cfg.export / "a.pdf").unlink()
        _, skipped = self.run_csv()
        self.assertTrue(skipped["local_download_complete"])
        self.assertEqual(skipped["unchanged_skips"], 1)
        self.remote.add("a.pdf", b"new", mtime=2)
        _, changed = self.run_csv()
        self.assertEqual(changed["changed_or_repeated_successes"], 1)
        self.assertEqual(self.remote.opens, ["a.pdf", "a.pdf"])
        self.assertTrue(self.rows("SELECT * FROM transfers t JOIN paths p ON p.id=t.path_id WHERE p.remote='b.pdf' AND t.state='pending'"))

    def test_aliases_reserved_before_failures_and_deterministic_under_row_order(self):
        names = ["agreement.pdf", "Agreement.pdf", "agreement-1.pdf", "é.pdf", "é.pdf", "Folder/x.pdf", "folder/x.pdf"]
        self.input(names)
        for name in names:
            self.remote.add(name)
        self.remote.denied.add("Agreement.pdf")
        _, first = self.run_csv()
        aliases = {r["remote_path"]: r["mapped_path"] for r in self.report(first)}
        self.assertEqual(aliases["agreement.pdf"], "agreement-2.pdf")
        self.assertEqual(aliases["agreement-1.pdf"], "agreement-1.pdf")
        self.assertEqual(len(set(aliases.values())), len(names))
        self.input(list(reversed(names)))
        self.remote.denied.clear()
        _, second = self.run_csv()
        self.assertEqual({r["remote_path"]: r["mapped_path"] for r in self.report(second)}, aliases)
        self.assertTrue(second["local_download_complete"])

    def test_interrupted_mapping_finishes_prior_reservations_before_new_input(self):
        self.input(["folder/Agreement.pdf", "folder/agreement.pdf"])
        for name in ("folder/Agreement.pdf", "folder/agreement.pdf", "folder/agreement-1.pdf"):
            self.remote.add(name)

        def interrupt(event, job):
            if event == "csv_mapping_siblings":
                raise KeyboardInterrupt
        code, first = self.run_csv(checkpoint=interrupt)
        self.assertEqual(code, 130)
        self.assertEqual(self.remote.connections, 0)
        self.input(["folder/agreement-1.pdf"])
        code, second = self.run_csv()
        self.assertEqual(code, 0)
        self.assertEqual(self.rows("SELECT mapping_complete FROM csv_imports WHERE run_id=?", (first["run_id"],))[0][0], 1)
        self.assertEqual(self.rows("SELECT local FROM paths WHERE remote='folder/agreement.pdf'")[0][0], "folder/agreement-1.pdf")
        self.assertEqual(self.report(second)[0]["mapped_path"], "folder/agreement-1-1.pdf")

    def test_fatal_connection_keeps_aliases_and_unchecked_results(self):
        self.input(["a.pdf", "A.pdf"])
        with patch.object(self.remote, "connect", side_effect=FatalError("offline")):
            code, summary = self.run_csv()
        self.assertEqual(code, 1)
        self.assertEqual(summary["document_outcomes"]["unchecked"], 2)
        self.assertEqual(summary["metadata_unchecked"], 2)
        self.assertTrue(all(row["mapped_path"] for row in self.report(summary)))
        code, offline = self.invoke("status")
        self.assertEqual(code, 1)  # Preserve the fatal attempt, including before any metadata checks.
        self.assertEqual(offline["metadata_unchecked"], 2)
        self.assertEqual(offline["latest_online_attempt"]["export_exit_code"], 1)

    def test_parallel_fatal_and_interrupt_stop_admission_and_preserve_unchecked(self):
        for failure, expected in ((FatalError("connection failed"), 1), (KeyboardInterrupt(), 130)):
            with self.subTest(expected=expected):
                self.cfg = replace(self.cfg, workers=2)
                sessions = Sessions()
                for name in ("a.pdf", "b.pdf", "c.pdf", "d.pdf"):
                    sessions.source.add(name)
                sessions.connect_error = failure
                self.input(["a.pdf", "b.pdf", "c.pdf", "d.pdf"])
                code, summary = self.run_csv(remote_factory=sessions.factory)
                self.assertEqual(code, expected)
                self.assertEqual(summary["document_outcomes"]["unchecked"], 4)
                self.assertEqual(sessions.connections, 1)
                self.assertEqual(sessions.active, 0)

    def test_retry_transient_metadata_and_recheck_identical_csv_bytes(self):
        self.remote.add("folder/a.pdf")
        self.input(["folder/a.pdf"])
        calls = Counter()

        def lookup(path):
            calls[path] += 1
            if path in ("folder", "folder/a.pdf") and calls[path] == 1:
                raise ConnectionError("transient")
        self.remote.stat_hook = lookup
        code, first = self.run_csv("inventory")
        self.assertEqual(code, 0)
        self.assertEqual(calls["folder"], 2)
        self.assertEqual(calls["folder/a.pdf"], 2)
        self.assertEqual(self.rows("SELECT attempts FROM csv_entries WHERE run_id=?", (first["run_id"],))[0][0], 2)
        _, second = self.run_csv("inventory")
        self.assertEqual(first["input_digest"], second["input_digest"])
        self.assertNotEqual(first["run_id"], second["run_id"])
        self.assertEqual(calls["folder/a.pdf"], 3)

    def test_source_mutation_retries_and_forced_repeat_remains_pending(self):
        self.remote.add("a.pdf", b"old")
        self.input(["a.pdf"])

        def mutate(path):
            self.remote.entries[path]["mtime"] = 2
        self.remote.open_hook = mutate
        code, summary = self.run_csv()
        self.assertEqual(code, 0)
        self.assertEqual(self.remote.opens, ["a.pdf", "a.pdf"])
        self.assertEqual(self.report(summary)[0]["mtime"], "2")
        self.cfg = replace(self.cfg, max_staged_bytes=3)
        code, pending = self.run_csv(force_path="a.pdf")
        self.assertEqual(code, 0)
        self.assertEqual(pending["document_outcomes"]["pending"], 1)
        self.assertFalse(pending["local_download_complete"])
        (self.cfg.export / "a.pdf").unlink()
        _, repeated = self.run_csv()
        self.assertTrue(repeated["local_download_complete"])
        self.assertEqual(repeated["forced_successes"], 1)

    def test_local_integrity_scope_and_ancestor_mapping_failure(self):
        for name in ("other.pdf", "a.pdf"):
            self.remote.add(name)
        self.invoke()
        (self.cfg.export / "other.pdf").write_bytes(b"bad")
        self.input(["a.pdf"])
        code, okay = self.run_csv("reconcile")
        self.assertEqual(code, 0)
        self.assertTrue(okay["local_download_complete"])
        (self.cfg.export / "a.pdf").write_bytes(b"bad")
        code, failed = self.run_csv("reconcile")
        self.assertEqual(code, 2)
        self.assertEqual(failed["document_outcomes"]["failed"], 1)
        directory = "x" * 256
        self.remote.add(directory + "/test.pdf")
        self.input([directory + "/test.pdf"])
        code, conflict = self.run_csv("inventory")
        self.assertEqual(code, 2)
        self.assertEqual(conflict["document_outcomes"]["failed"], 1)
        self.assertEqual(self.report(conflict)[0]["error_category"], "unrepresentable_path")

    def test_known_file_ancestor_is_not_replaced_by_inferred_directory(self):
        self.remote.add("old.pdf")
        self.invoke()
        self.input(["old.pdf/child.pdf"])
        code, summary = self.run_csv()
        self.assertEqual(code, 2)
        self.assertEqual(self.rows("SELECT kind FROM paths WHERE remote='old.pdf'")[0][0], "file")
        self.assertEqual(summary["document_outcomes"]["failed"], 1)
        self.assertEqual((self.cfg.export / "old.pdf").read_bytes(), b"pdf")

    def test_recovery_is_global_but_only_after_valid_input(self):
        self.remote.add("a.pdf")
        self.input(["a.pdf"])

        def interrupt(event, job):
            if event == "ready":
                raise KeyboardInterrupt
        self.run_csv(checkpoint=interrupt)
        self.assertEqual(self.rows("SELECT state FROM transfers")[0][0], "ready")
        self.source.write_text("invalid")
        self.run_csv()
        self.assertEqual(self.rows("SELECT state FROM transfers")[0][0], "ready")
        self.remote.add("b.pdf")
        self.input(["b.pdf"])
        code, summary = self.run_csv("inventory")
        self.assertEqual(code, 0)
        self.assertEqual(summary["recovered_outside_scope"], 1)
        self.assertEqual(summary["document_outcomes"]["pending"], 1)
        self.assertEqual(len(self.report(summary, "downloads")), 1)

    def test_zero_byte_and_capacity_stop(self):
        self.remote.add("a.pdf", b"")
        self.remote.add("b.pdf", b"abc")
        self.input(["a.pdf", "b.pdf"])
        self.cfg = replace(self.cfg, min_free_bytes=1)
        code, summary = self.run_csv(free=lambda: 1)
        self.assertEqual(code, 0)
        self.assertEqual(summary["stop_reason"], "disk_reserve")
        self.assertEqual(summary["zero_byte_pdfs"], 1)
        self.assertEqual(summary["document_outcomes"]["downloaded_this_run"], 1)
        self.assertEqual(summary["document_outcomes"]["pending"], 1)
        self.assertEqual(self.run_csv("reconcile")[0], 2)

    def test_immutable_results_and_current_version_presence(self):
        self.remote.add("a.pdf", b"old")
        self.input(["a.pdf"], ["old-id"])
        _, original = self.run_csv()
        first_doc = self.report(original)[0]
        self.remote.add("a.pdf", b"new", mtime=2)
        self.input(["a.pdf"], ["new-id"])
        self.run_csv()
        self.source.unlink()
        code, rebuilt = self.invoke("status", report_run=original["run_id"])
        self.assertEqual(code, 0)
        self.assertTrue(rebuilt["local_download_complete"])
        doc = self.report(rebuilt)[0]
        for key in ("document_id", "outcome", "transfer_id", "sha256", "completion_run"):
            self.assertEqual(doc[key], first_doc[key])
        self.assertEqual(doc["local_payload_status"], "replaced")
        (self.cfg.export / "a.pdf").write_bytes(b"bad")
        code, damaged = self.invoke("status", report_run=original["run_id"])
        self.assertEqual(code, 0)
        self.assertTrue(damaged["local_download_complete"])
        self.assertTrue(damaged["current_staging"]["integrity_errors"])
        self.assertEqual(self.report(damaged)[0]["local_payload_status"], "unverified")
        (self.cfg.export / "a.pdf").unlink()
        _, offloaded = self.invoke("status", report_run=original["run_id"])
        self.assertEqual(self.report(offloaded)[0]["local_payload_status"], "absent")

    def test_failed_import_status_and_repeated_status_do_not_hide_attempt(self):
        for has_accepted in (False, True):
            if has_accepted:
                self.remote.add("a.pdf")
                self.input(["a.pdf"])
                _, accepted = self.run_csv()
            self.source.write_text("invalid")
            _, failed = self.run_csv()
            for _ in range(2):
                code, status = self.invoke("status")
                self.assertEqual(code, 1)
                self.assertEqual(status["latest_online_attempt"]["id"], failed["run_id"])
                self.assertEqual(status["latest_online_attempt"]["import_status"], "rejected")
                self.assertFalse(status["local_download_complete"])
                if has_accepted:
                    self.assertEqual(status["last_accepted_csv_run"]["id"], accepted["run_id"])
                    self.assertTrue(status["last_accepted_csv_run"]["local_download_complete"])
                else:
                    self.assertIsNone(status["last_accepted_csv_run"])

    def test_hard_exit_during_import_leaves_unaccepted_snapshot(self):
        self.input([f"{i}.pdf" for i in range(501)])
        script = """
import os,sys
from pathlib import Path
from clm_exporter.cli import execute
from clm_exporter.config import Config
root=Path(sys.argv[1])
cfg=Config('test','user',root/'key',root/'hosts','/Main',root/'export',min_free_bytes=0)
def stop(event, job):
    if event=='csv_import_batch': os._exit(17)
execute(cfg,'download',csv_path=root/'report.csv',checkpoint=stop,progress=False)
"""
        result = subprocess.run([sys.executable, "-c", script, str(self.root)], capture_output=True, check=False)
        self.assertEqual(result.returncode, 17, result.stderr)
        code, status = self.invoke("status")
        self.assertEqual(code, 130)
        self.assertEqual(status["latest_online_attempt"]["import_status"], "importing")
        self.assertIsNone(status["last_accepted_csv_run"])
        self.assertEqual(self.rows("SELECT * FROM paths"), [])
        code, historical = self.invoke("status", report_run=status["latest_online_attempt"]["id"])
        self.assertEqual(code, 130)
        self.assertFalse(historical["original_results_available"])
        self.assertEqual(historical["import_status"], "importing")

    def test_unfinalized_accepted_run_keeps_entries_without_inventing_outcomes(self):
        self.remote.add("a.pdf")
        self.input(["a.pdf"])
        _, summary = self.run_csv()
        with sqlite3.connect(self.cfg.database) as db:
            # Model a crash before finalization, after durable transfer success.
            db.execute("DELETE FROM run_results WHERE run_id=?", (summary["run_id"],))
            db.execute("DELETE FROM publication_attempts WHERE run_id=?", (summary["run_id"],))
            db.execute("UPDATE runs SET summary=NULL,ended_at=NULL WHERE id=?", (summary["run_id"],))
        code, rebuilt = self.invoke("status", report_run=summary["run_id"])
        self.assertEqual(code, 0)
        self.assertIsNone(rebuilt["local_download_complete"])
        self.assertIsNone(rebuilt["original_exit_code"])
        self.assertEqual(len(self.report(rebuilt)), 1)
        self.assertEqual(self.report(rebuilt)[0]["outcome"], "")
        self.assertEqual(self.report(rebuilt)[0]["result_status"], "Original completion result unavailable")
        self.assertEqual(len(self.report(rebuilt, "downloads")), 1)

    def test_offline_status_does_not_certify_an_unfinished_accepted_run(self):
        self.remote.add("a.pdf")
        self.input(["a.pdf"])
        _, summary = self.run_csv()
        with sqlite3.connect(self.cfg.database) as db:
            db.execute("DELETE FROM run_results WHERE run_id=?", (summary["run_id"],))
            db.execute("UPDATE runs SET summary=NULL,ended_at=NULL,stop_reason=NULL WHERE id=?", (summary["run_id"],))
        for _ in range(2):
            code, status = self.invoke("status")
            self.assertEqual(code, 130)
            self.assertFalse(status["local_download_complete"])
            self.assertEqual(status["currently_staged_completed_pdfs"], 1)
            self.assertEqual(status["latest_online_attempt"]["id"], summary["run_id"])

    def test_publication_failures_preserve_coverage_and_retry_history(self):
        for stage in ("report", "backup", "interrupt"):
            with self.subTest(stage=stage):
                self.remote.add("a.pdf")
                self.input(["a.pdf"])
                target = "clm_exporter.cli.write_reports" if stage != "backup" else "clm_exporter.state.State.backup"
                error = KeyboardInterrupt() if stage == "interrupt" else OSError("publication fixture")
                with patch(target, side_effect=error):
                    code, original = self.run_csv()
                self.assertEqual(code, 130 if stage == "interrupt" else 1)
                self.assertTrue(original["local_download_complete"])
                self.assertEqual(original["export_exit_code"], 0)
                original_code = code
                opens = list(self.remote.opens)
                code, retry = self.invoke("status", report_run=original["run_id"])
                self.assertEqual(code, 0)
                self.assertEqual(retry["original_exit_code"], original_code)
                self.assertEqual(retry["publication"]["reports_status"], "complete")
                self.assertEqual(retry["publication"]["backup_status"], "complete")
                self.assertEqual(self.remote.opens, opens)
                self.assertEqual(len(self.rows("SELECT * FROM publication_attempts WHERE run_id=?", (original["run_id"],))), 2)

    def test_pending_publication_is_not_rewritten_by_retry(self):
        self.remote.add("a.pdf")
        self.input(["a.pdf"])
        _, summary = self.run_csv()
        with sqlite3.connect(self.cfg.database) as db:
            db.execute("UPDATE publication_attempts SET reports_status='pending',backup_status='pending',ended_at=NULL,exit_code=NULL WHERE run_id=?", (summary["run_id"],))
        _, rebuilt = self.invoke("status", report_run=summary["run_id"])
        self.assertIsNone(rebuilt["original_exit_code"])
        self.assertEqual(rebuilt["original_publication"]["reports_status"], "pending")
        self.assertEqual(rebuilt["publication"]["reports_status"], "complete")


class MigrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.cfg = Config("test", "user", self.root / "key", self.root / "hosts", "/", self.root / "export", min_free_bytes=0)
        prepare_layout(self.cfg)
        with sqlite3.connect(self.cfg.database) as db:
            db.executescript(SCHEMA)
            db.execute("INSERT INTO source VALUES(1,?,?,NULL,'{}')", (json.dumps(self.cfg.identity), str(self.cfg.export)))
            db.execute("INSERT INTO runs(id,command,started_at,ended_at,stop_reason) VALUES('legacy','download','2020','2021','completed')")
            db.execute("INSERT INTO paths(id,remote,parent,name,kind,local,collision_key,size,mtime,selected,last_seen_run) VALUES(42,'a.pdf','','a.pdf','file','a.pdf','a.pdf',3,'1',1,'legacy')")
            db.execute("INSERT INTO transfers(id,path_id,size,mtime,state,sha256,completion_run) VALUES(77,42,3,'1','downloaded',?,'legacy')", (hashlib.sha256(b"pdf").hexdigest(),))

    def test_upgrade_preserves_ids_and_history_and_legacy_unknown_outcome(self):
        remote = FakeRemote()
        remote.add("a.pdf")
        source = self.root / "input.csv"
        source.write_text("DOCUMENT ID,DOCUMENT NAME,LOCATION PATH\nid,a.pdf,/\n")
        with redirect_stderr(io.StringIO()):
            code, summary = execute(self.cfg, "download", csv_path=source, remote_factory=remote.factory, progress=False)
            _, legacy = execute(self.cfg, "status", report_run="legacy", progress=False)
        self.assertEqual(code, 0)
        self.assertEqual(summary["unchanged_skips"], 1)
        self.assertEqual(remote.opens, [])
        self.assertIsNone(legacy["local_download_complete"])
        self.assertEqual(legacy["original_result_note"], "Original completion result unavailable")
        with sqlite3.connect(self.cfg.database) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
            self.assertEqual(db.execute("SELECT id,path_id FROM transfers").fetchall(), [(77, 42)])
            self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(), [])
        backup = next(self.cfg.backups.glob("*pre-migration*"))
        with sqlite3.connect(backup) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 1)

    def test_migration_rollback_and_binding_check_before_backup(self):
        with patch("clm_exporter.state.SCHEMA_V2", SCHEMA_V2 + "INVALID SQL;"), self.assertRaises(sqlite3.Error):
            State(self.cfg)
        with sqlite3.connect(self.cfg.database) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE name='csv_entries'").fetchall(), [])
            self.assertEqual(db.execute("SELECT id FROM transfers").fetchall(), [(77,)])
        backups = list(self.cfg.backups.iterdir())
        with self.assertRaises(FatalError):
            State(replace(self.cfg, remote_root="/elsewhere"))
        self.assertEqual(list(self.cfg.backups.iterdir()), backups)


if __name__ == "__main__":
    unittest.main()
