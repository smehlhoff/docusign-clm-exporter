import csv
import errno
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing, contextmanager, redirect_stderr
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

from clm_exporter.cli import execute, main
from clm_exporter.config import Config, load
from clm_exporter.errors import FatalError, PathError
from clm_exporter.local import working_lock

from .fakes import FakeRemote, attrs


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cfg = Config("sftp.test", "operator", self.root / "key", self.root / "known_hosts", "/root", self.root / "exporter",
                          max_staged_bytes=1000, min_free_bytes=10, chunk_bytes=2)
        self.remote = FakeRemote()
        self.invoke("init")

    def invoke(self, command="download", **kwargs):
        with redirect_stderr(io.StringIO()):
            return execute(self.cfg, command, remote_factory=self.remote.factory,
                           free=kwargs.pop("free", lambda: 10000), sleep=lambda _: None, **kwargs)

    def rows(self, sql, values=()):
        with closing(sqlite3.connect(self.cfg.database)) as db:
            db.row_factory = sqlite3.Row
            return db.execute(sql, values).fetchall()

    def csv(self, summary, name):
        with open(summary["reports"][name], newline="") as stream:
            return list(csv.DictReader(stream))

    def test_invalid_numeric_metadata_does_not_abort_healthy_siblings(self):
        invalid = [
            ("large.pdf", "file", 2**63, 1),
            ("huge.pdf", "file", 1, 10**400),
            ("negative-dir", "directory", -1, 1),
            ("large-dir", "directory", 2**63, 1),
            ("nan-dir", "directory", 0, float("nan")),
            ("infinite-dir", "directory", 0, float("inf")),
            ("text-dir", "directory", "invalid", 1),
            ("bool-dir", "directory", 0, True),
        ]
        self.remote.add("healthy.pdf")
        for name, kind, _, _ in invalid:
            self.remote.add(name, kind=kind)
        original = self.remote.iterdir
        overrides = {name: (size, mtime) for name, _, size, mtime in invalid}

        def malformed(directory):
            for info in original(directory):
                if not directory and info.filename in overrides:
                    info.st_size, info.st_mtime = overrides[info.filename]
                yield info

        self.remote.iterdir = malformed
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertFalse(summary["scan_complete"])
        self.assertEqual(self.remote.opens, ["healthy.pdf"])
        errors = self.csv(summary, "unresolved")
        self.assertEqual(
            {e["remote_path"] for e in errors if e["operation"] == "metadata"},
            {item[0] for item in invalid},
        )
        self.assertTrue(Path(summary["reports"]["backup"]).exists())
        self.remote.iterdir = original
        code, recovered = self.invoke()
        self.assertEqual(code, 0)
        self.assertTrue(recovered["local_download_complete"])

    def test_unrepresentable_remote_name_is_reported_without_aborting(self):
        self.remote.add("healthy.pdf")
        original = self.remote.iterdir

        def malformed(directory):
            yield attrs("bad\udcff.pdf")
            yield from original(directory)

        self.remote.iterdir = malformed
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertFalse(summary["scan_complete"])
        self.assertEqual(self.remote.opens, ["healthy.pdf"])
        errors = self.csv(summary, "unresolved")
        self.assertEqual(errors[0]["category"], "unsafe_remote_path")
        self.assertIn(r"\udcff", errors[0]["remote_path"])
        self.assertTrue(Path(summary["reports"]["backup"]).exists())

    def test_mixed_extensions_nesting_and_zero_bytes(self):
        for p in ["Customers/Acme/A.pdf", "Customers/Acme/B.PDF", "x.pdf/nested/C.pDf"]:
            self.remote.add(p)
        self.remote.add("empty.pdf", b"")
        self.remote.add("not.txt")
        self.remote.add("link.pdf", kind="symlink")
        self.remote.add("pipe.pdf", kind="special")
        code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(summary["pdfs_discovered"], 4)
        self.assertEqual(summary["zero_byte_pdfs"], 1)
        self.assertEqual(len(self.remote.opens), 4)
        self.assertEqual((self.cfg.export / "x.pdf/nested/C.pDf").read_bytes(), b"pdf")
        self.assertFalse((self.cfg.export / "not.txt").exists())
        self.assertTrue(summary["local_download_complete"])
        self.assertEqual(summary["cloud_delivery"], "not_assessed")

    def test_cleanup_keeps_history_and_new_files_are_found(self):
        self.remote.add("old/A.pdf")
        self.invoke()
        (self.cfg.export / "old/A.pdf").unlink()
        self.remote.add("old/B.pdf")
        self.remote.opens.clear()
        code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(self.remote.opens, ["old/B.pdf"])
        self.assertEqual(summary["unchanged_skips"], 1)
        self.assertEqual(summary["historically_downloaded_absent_or_unverified"], 1)
        self.assertEqual(len(self.csv(summary, "staging")), 1)
        self.assertEqual(len(self.csv(summary, "history")), 2)

    def test_size_mtime_and_a_b_a_changes(self):
        for data, mtime in [(b"aa", 1), (b"bbb", 1), (b"bbb", 2), (b"aa", 1)]:
            self.remote.add("a.pdf", data, mtime)
            code, summary = self.invoke()
            self.assertEqual(code, 0)
            self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), data)
        self.assertEqual(summary["historical_successful_transfers"], 4)
        self.assertEqual(len(self.remote.opens), 4)
        self.assertEqual([r["payload_status"] for r in self.csv(summary, "history")], ["superseded"] * 3 + ["staged"])

    def test_changed_failure_preserves_older_file_and_gap(self):
        self.remote.add("a.pdf", b"old")
        self.invoke()
        self.remote.add("a.pdf", b"newer", 2)
        self.remote.open_hook = lambda _: (_ for _ in ()).throw(PermissionError("no"))
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"old")
        self.assertEqual(summary["pending_deferred_count"], 1)
        self.assertFalse(summary["local_download_complete"])
        self.remote.open_hook = None
        self.assertEqual(self.invoke()[0], 0)

    def test_repeated_batch_cap_and_cleanup(self):
        self.cfg = replace(self.cfg, max_staged_bytes=6)
        self.remote.add("a.pdf", b"aaaa")
        self.remote.add("b.pdf", b"bbbb")
        code, first = self.invoke()
        self.assertEqual((code, first["stop_reason"]), (0, "batch_capacity"))
        code, second = self.invoke()
        self.assertEqual(second["new_successes"], 0)
        self.assertEqual(second["current_staged_bytes"], 4)
        (self.cfg.export / "a.pdf").unlink()
        code, third = self.invoke()
        self.assertEqual((code, third["new_successes"]), (0, 1))
        self.assertEqual(self.remote.opens, ["a.pdf", "b.pdf"])

    def test_old_new_coexistence_and_capacity_equality(self):
        self.cfg = replace(self.cfg, max_staged_bytes=6)
        self.remote.add("a.pdf", b"abc")
        self.invoke()
        self.remote.add("a.pdf", b"1234", 2)
        self.assertEqual(self.invoke()[1]["stop_reason"], "batch_capacity")
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"abc")
        self.cfg = replace(self.cfg, max_staged_bytes=7)
        code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(summary["current_staged_bytes"], 4)

    def test_oversized_continues_and_error_precedence(self):
        self.cfg = replace(self.cfg, max_staged_bytes=5)
        self.remote.add("a.pdf", b"oversized")
        self.remote.add("b.pdf", b"1234")
        self.remote.add("c.pdf", b"1234")
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual(summary["stop_reason"], "batch_capacity")
        self.assertEqual(self.remote.opens, ["b.pdf"])
        self.assertIn("file_exceeds_batch_limit", [e["category"] for e in self.csv(summary, "unresolved")])

    def test_unknown_staged_and_temp_files_count(self):
        (self.cfg.work / "unknown.part").write_bytes(b"1234")
        (self.cfg.export / "operator.txt").write_bytes(b"1234")
        self.cfg = replace(self.cfg, max_staged_bytes=10)
        self.remote.add("a.pdf", b"123")
        code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(summary["current_staged_bytes"], 8)
        self.assertEqual(summary["stop_reason"], "batch_capacity")
        self.assertTrue((self.cfg.work / "unknown.part").exists())

    def test_staging_growth_during_discovery_counts_before_transfer(self):
        self.cfg = replace(self.cfg, max_staged_bytes=5)
        self.remote.add("a.pdf", b"pdf")
        self.remote.list_hook = lambda _: (self.cfg.work / "other.part").write_bytes(b"1234")
        code, summary = self.invoke()
        self.assertEqual((code, summary["stop_reason"]), (0, "batch_capacity"))
        self.assertEqual(summary["current_staged_bytes"], 4)
        self.assertFalse(self.remote.opens)

    def test_disk_full_during_sync_is_a_resumable_planned_stop(self):
        self.remote.add("a.pdf", b"pdf")
        original = os.fsync

        def full_on_payload(fd):
            if os.fstat(fd).st_size == 3:
                raise OSError(errno.ENOSPC, "Disk full")
            return original(fd)

        with patch("clm_exporter.transfer.os.fsync", side_effect=full_on_payload):
            code, summary = self.invoke()
        self.assertEqual((code, summary["stop_reason"]), (0, "disk_reserve"))
        self.assertEqual(summary["unresolved_file_errors"], 0)
        self.assertEqual(self.rows("SELECT state FROM transfers")[0][0], "pending")
        self.assertFalse(list(self.cfg.work.iterdir()))
        self.assertEqual(self.invoke()[0], 0)

    def test_disk_reserve_before_and_during_transfer(self):
        self.remote.add("a.pdf", b"1234")
        self.assertEqual(self.invoke(free=lambda: 13)[1]["stop_reason"], "disk_reserve")
        self.assertFalse(self.remote.opens)
        readings = iter([14, 14, 10])
        code, summary = self.invoke(free=lambda: next(readings, 10))
        self.assertEqual(code, 0)
        self.assertEqual(summary["stop_reason"], "disk_reserve")
        self.assertFalse(list(self.cfg.work.iterdir()))
        self.assertEqual(summary["historical_successful_transfers"], 0)
        self.assertEqual(self.invoke(free=lambda: 14)[0], 0)

    def test_case_unicode_directory_and_generated_collisions(self):
        names = ["Acme/A.pdf", "Acme/a.pdf", "Acme/a-1.pdf", "acme/B.PDF", "Caf\u00e9/X.pdf", "Cafe\u0301/X.pdf", "Thing.pdf/y.pdf", "thing.pdf"]
        for p in names:
            self.remote.add(p, p.encode())
        self.remote.reverse = True
        self.assertEqual(self.invoke()[0], 0)
        mapping = {r["remote"]: r["local"] for r in self.rows("SELECT * FROM paths WHERE selected=1")}
        self.assertEqual(mapping["Acme/a.pdf"], "Acme/a-2.pdf")
        self.assertEqual(mapping["Acme/a-1.pdf"], "Acme/a-1.pdf")
        self.assertEqual(mapping["acme/B.PDF"], "acme-1/B.PDF")
        self.assertEqual(mapping["thing.pdf"], "thing-1.pdf")
        for p in self.cfg.export.rglob("*"):
            if p.is_file():
                p.unlink()
        self.remote.reverse = False
        self.remote.add("Acme/A.PDF")
        self.assertEqual(self.invoke()[0], 0)
        later = {r["remote"]: r["local"] for r in self.rows("SELECT * FROM paths WHERE selected=1")}
        for name, local in mapping.items():
            self.assertEqual(later[name], local)

    def test_disappearance_reserves_aliases(self):
        self.remote.add("A.pdf")
        self.invoke()
        del self.remote.entries["A.pdf"]
        (self.cfg.export / "A.pdf").unlink()
        self.remote.add("a.pdf")
        self.invoke()
        self.assertTrue((self.cfg.export / "a-1.pdf").exists())

    def test_ctrl_c_restarts_only_unfinished(self):
        self.remote.add("a.pdf", b"1234")
        self.remote.add("b.pdf", b"5678")
        def interrupt(path, offset):
            if path == "b.pdf" and offset == 2:
                raise KeyboardInterrupt
        self.remote.read_hook = interrupt
        code, summary = self.invoke()
        self.assertEqual(code, 130)
        self.assertEqual(summary["historical_successful_transfers"], 1)
        self.assertFalse(list(self.cfg.work.iterdir()))
        self.remote.read_hook = None
        self.invoke()
        self.assertEqual(self.remote.opens, ["a.pdf", "b.pdf", "b.pdf"])
        self.assertEqual((self.cfg.export / "b.pdf").read_bytes(), b"5678")

    def test_promotion_crash_before_and_after_recovers_without_network_content(self):
        for checkpoint in ("ready", "promoted"):
            with self.subTest(checkpoint=checkpoint):
                name = checkpoint + ".pdf"
                self.remote.add(name, b"abcdef")
                def crash(event, job, checkpoint=checkpoint):
                    if event == checkpoint:
                        raise SystemExit("simulated crash")
                with self.assertRaises(SystemExit):
                    self.invoke(checkpoint=crash)
                self.assertEqual(self.rows("SELECT state FROM transfers ORDER BY id DESC LIMIT 1")[0][0], "ready")
                opens = len(self.remote.opens)
                code, _ = self.invoke()
                self.assertEqual(code, 0)
                self.assertEqual(len(self.remote.opens), opens)
                self.assertEqual((self.cfg.export / name).read_bytes(), b"abcdef")

    def test_abrupt_downloading_restarts_from_zero(self):
        self.remote.add("a.pdf", b"abcdef")
        def crash(event, job):
            if event == "chunk":
                raise SystemExit("power loss")
        with self.assertRaises(SystemExit):
            self.invoke(checkpoint=crash)
        self.assertEqual(len(list(self.cfg.work.iterdir())), 1)
        self.assertEqual(self.invoke()[0], 0)
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"abcdef")
        self.assertEqual(self.remote.opens, ["a.pdf", "a.pdf"])

    def test_recovery_never_adopts_size_alone(self):
        self.remote.add("a.pdf", b"good")
        def crash(event, job):
            if event == "promoted":
                raise SystemExit
        with self.assertRaises(SystemExit):
            self.invoke(checkpoint=crash)
        (self.cfg.export / "a.pdf").write_bytes(b"evil")
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual(summary["historical_successful_transfers"], 0)
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"evil")

    def test_changing_remote_retries_new_metadata(self):
        self.remote.add("a.pdf", b"old")
        def change(path, offset):
            if offset == 2:
                self.remote.add(path, b"new-version", 2)
                self.remote.read_hook = None
        self.remote.read_hook = change
        code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(self.remote.opens, ["a.pdf", "a.pdf"])
        self.assertEqual(summary["historical_successful_transfers"], 1)
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"new-version")

    def test_growth_before_transfer_rechecks_capacity(self):
        self.cfg = replace(self.cfg, max_staged_bytes=8)
        self.remote.add("a.pdf", b"123")
        def grow(relative):
            if relative == "a.pdf":
                self.remote.add(relative, b"123456789", 2)
        self.remote.stat_hook = grow
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertFalse(self.remote.opens)
        self.assertEqual(summary["bytes_discovered"], 9)

    def test_file_directory_errors_and_missing_metadata(self):
        self.remote.add("blocked/hidden.pdf")
        self.remote.add("ok/a.pdf")
        self.remote.add("missing.pdf")
        self.remote.denied.add("blocked")
        self.remote.missing_attrs.add("missing.pdf")
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertFalse(summary["scan_complete"])
        self.assertEqual(self.remote.opens, ["ok/a.pdf"])
        self.assertEqual(summary["pdfs_with_unknown_size"], 1)
        self.remote.denied.clear()
        self.remote.missing_attrs.clear()
        code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertTrue(summary["local_download_complete"])

    def test_missing_listing_attributes_are_queried(self):
        self.remote.add("a.pdf")
        self.remote.list_missing_attrs.add("a.pdf")
        self.assertEqual(self.invoke()[0], 0)

    def test_invalid_remote_type_is_reported_without_aborting_siblings(self):
        self.remote.add("a.pdf")
        self.remote.add("z.pdf")
        original = self.remote.lstat

        def malformed(path):
            info = original(path)
            if path == "a.pdf":
                info.st_mode = "invalid"
            return info

        self.remote.lstat = malformed
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual(self.remote.opens, ["z.pdf"])
        self.assertIn("missing_metadata", [e["category"] for e in self.csv(summary, "unresolved")])

    def test_permissions_without_file_type_are_queried_and_reported(self):
        self.remote.add("a.pdf")
        original_list = self.remote.iterdir
        original_stat = self.remote.lstat

        def missing_type(directory):
            for info in original_list(directory):
                info.st_mode = 0o600
                yield info

        self.remote.iterdir = missing_type
        self.assertEqual(self.invoke("inventory")[1]["pdfs_discovered"], 1)

        def stat_missing_type(path):
            info = original_stat(path)
            if path == "a.pdf":
                info.st_mode = 0o600
            return info

        self.remote.lstat = stat_missing_type
        code, summary = self.invoke("reconcile")
        self.assertEqual(code, 2)
        self.assertFalse(summary["scan_complete"])
        self.assertFalse(summary["local_download_complete"])
        self.assertIn("missing_metadata", [e["category"] for e in self.csv(summary, "unresolved")])

    def test_unavailable_before_success_is_not_hidden(self):
        self.remote.add("dir/a.pdf")
        self.invoke("inventory")
        del self.remote.entries["dir/a.pdf"]
        del self.remote.entries["dir"]
        code, summary = self.invoke("reconcile")
        self.assertEqual(code, 2)
        self.assertEqual(summary["pdfs_discovered"], 0)
        self.assertEqual(summary["unresolved_file_errors"], 1)
        self.remote.add("dir/a.pdf")
        self.assertEqual(self.invoke()[0], 0)

    def test_pdf_replaced_by_another_type_remains_unresolved(self):
        for kind in ("directory", "symlink", "special"):
            with self.subTest(kind=kind):
                name = kind + ".pdf"
                self.remote.add(name)
                self.invoke("inventory")
                self.remote.add(name, kind=kind)
                code, summary = self.invoke("reconcile")
                self.assertEqual(code, 2)
                self.assertFalse(summary["local_download_complete"])
                self.assertIn(name, [e["remote_path"] for e in self.csv(summary, "unresolved")
                                     if e["operation"] == "availability"])
                del self.remote.entries[name]
                self.assertEqual(self.invoke("reconcile")[0], 2)
                self.remote.add(name)
                self.assertEqual(self.invoke()[0], 0)

    def test_inaccessible_directory_does_not_imply_deletion(self):
        self.remote.add("dir/a.pdf")
        self.invoke("inventory")
        self.remote.denied.add("dir")
        code, summary = self.invoke("reconcile")
        self.assertEqual(code, 2)
        errors = self.csv(summary, "unresolved")
        self.assertFalse(any(e["operation"] == "availability" for e in errors))

    def test_unknown_directory_type_does_not_imply_child_deletion(self):
        self.remote.add("dir/a.pdf")
        self.invoke("inventory")
        original_list = self.remote.iterdir
        original_stat = self.remote.lstat

        def missing_type(directory):
            for info in original_list(directory):
                if directory == "" and info.filename == "dir":
                    info.st_mode = None
                yield info

        def stat_missing_type(path):
            info = original_stat(path)
            if path == "dir":
                info.st_mode = None
            return info

        self.remote.iterdir, self.remote.lstat = missing_type, stat_missing_type
        code, summary = self.invoke("reconcile")
        self.assertEqual(code, 2)
        self.assertFalse(summary["scan_complete"])
        self.assertFalse(any(e["operation"] == "availability" for e in self.csv(summary, "unresolved")))

    def test_transient_retry_budget_and_next_invocation(self):
        self.remote.add("a.pdf")
        self.remote.open_hook = lambda _: (_ for _ in ()).throw(ConnectionError("dropped"))
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual(len(self.remote.opens), 3)
        self.assertEqual(summary["historical_successful_transfers"], 0)
        self.remote.open_hook = None
        code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(len(self.rows("SELECT * FROM transfers")), 1)
        self.assertEqual(len(self.rows("SELECT * FROM errors")), 3)
        self.assertEqual(summary["unresolved_file_errors"], 0)

    def test_permanent_file_failure_not_retried_and_csv_quotes(self):
        name = 'a,"line\nbreak.pdf'
        self.remote.add(name)
        self.remote.add("z.pdf")
        self.remote.open_hook = lambda p: (_ for _ in ()).throw(PermissionError("denied\nmessage")) if p == name else None
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual(self.remote.opens, [name, "z.pdf"])
        row = self.csv(summary, "failures")[0]
        self.assertEqual(row["remote_path"], name)
        self.assertEqual(row["message"], "denied\nmessage")

    def test_force_path_repeats_only_target_and_obeys_capacity(self):
        self.remote.add("a.pdf")
        self.remote.add("b.pdf")
        self.invoke()
        code, summary = self.invoke(force_path="a.pdf")
        self.assertEqual(code, 0)
        self.assertEqual(summary["forced_successes"], 1)
        self.assertEqual(self.remote.opens, ["a.pdf", "b.pdf", "a.pdf"])
        self.cfg = replace(self.cfg, max_staged_bytes=6)
        self.assertEqual(self.invoke(force_path="b.pdf")[1]["stop_reason"], "batch_capacity")
        self.assertEqual(self.invoke("reconcile")[0], 2)
        with self.assertRaises(PathError):
            self.invoke(force_path="../a.pdf")
        with self.assertRaises(PathError):
            self.invoke(force_path="")

    def test_scan_does_not_resolve_failed_explicit_repeat(self):
        self.remote.add("a.pdf")
        self.invoke()
        self.remote.open_hook = lambda _: (_ for _ in ()).throw(PermissionError("denied"))
        self.assertEqual(self.invoke(force_path="a.pdf")[0], 2)
        self.remote.open_hook = None
        code, summary = self.invoke("inventory")
        self.assertEqual(code, 2)
        self.assertEqual(summary["unresolved_file_errors"], 1)
        self.cfg = replace(self.cfg, max_staged_bytes=3)
        code, summary = self.invoke()
        self.assertEqual((code, summary["stop_reason"]), (2, "batch_capacity"))
        self.cfg = replace(self.cfg, max_staged_bytes=6)
        self.assertEqual(self.invoke()[0], 0)

    def test_untracked_overwrite_and_modified_owned_file_refused(self):
        self.remote.add("a.pdf", b"same")
        (self.cfg.export / "a.pdf").write_bytes(b"same")
        self.assertEqual(self.invoke()[0], 2)
        self.assertFalse(self.remote.opens)
        (self.cfg.export / "a.pdf").unlink()
        self.assertEqual(self.invoke()[0], 0)
        (self.cfg.export / "a.pdf").write_bytes(b"evil")
        self.remote.add("a.pdf", b"newer", 2)
        self.assertEqual(self.invoke()[0], 2)
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"evil")

    def test_remote_traversal_rejected(self):
        self.remote.iterdir = lambda _: iter([attrs("../escape.pdf"), attrs("/absolute.pdf")])
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertFalse(summary["scan_complete"])
        self.assertFalse(self.remote.opens)

    def test_local_symlinks_and_unrepresentable_names(self):
        outside = self.root / "outside"
        outside.mkdir()
        (self.cfg.export / "dir").symlink_to(outside, target_is_directory=True)
        self.remote.add("dir/a.pdf")
        code, summary = self.invoke()
        self.assertEqual(code, 1)
        self.assertFalse((outside / "a.pdf").exists())
        (self.cfg.export / "dir").unlink()
        self.remote.add("a" * 256 + ".pdf")
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertTrue(any(e["category"] == "unrepresentable_path" for e in self.csv(summary, "unresolved")))

    def test_status_offline_backup_and_reports_reconstruct(self):
        self.remote.add("a.pdf")
        _, summary = self.invoke()
        count = self.remote.connections
        code, status = self.invoke("status")
        self.assertEqual(code, 0)
        self.assertEqual(count, self.remote.connections)
        backup = status["reports"]["backup"]
        with closing(sqlite3.connect(backup)) as db:
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM transfers WHERE state='downloaded'").fetchone()[0], 1)
        Path(summary["reports"]["downloads"]).unlink()
        self.invoke("status", report_run=summary["run_id"])
        self.assertEqual(len(self.csv(summary, "downloads")), 1)

    def test_missing_state_binding_and_schema_fail_closed(self):
        with self.assertRaises(FatalError):
            self.invoke("init")
        self.cfg = replace(self.cfg, username="different")
        with self.assertRaises(FatalError):
            self.invoke("status")
        self.cfg = replace(self.cfg, username="operator")
        with closing(sqlite3.connect(self.cfg.database)) as db:
            db.execute("PRAGMA user_version=99")
        with self.assertRaises(FatalError):
            self.invoke("status")
        self.cfg.database.unlink()
        with self.assertRaises(FatalError):
            self.invoke()

    def test_working_directory_lock_excludes_other_process(self):
        script = "from clm_exporter.config import Config; from clm_exporter.local import working_lock; from pathlib import Path; import sys; c=Config('h','u',Path('k'),Path('h'),'/',Path(sys.argv[1]));\nwith working_lock(c): pass"
        with working_lock(self.cfg):
            result = subprocess.run([sys.executable, "-c", script, str(self.cfg.working_directory)], capture_output=True, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b"Another exporter", result.stderr)
        with working_lock(self.cfg):
            pass

    def test_reconciliation_gaps_and_complete_after_download(self):
        self.remote.add("a.pdf")
        self.assertEqual(self.invoke("inventory")[0], 0)
        self.assertFalse(self.remote.opens)
        self.assertEqual(self.invoke("reconcile")[0], 2)
        self.assertEqual(len(self.rows("SELECT * FROM transfers")), 1)
        self.invoke()
        (self.cfg.export / "a.pdf").unlink()
        code, summary = self.invoke("reconcile")
        self.assertEqual(code, 0)
        self.assertTrue(summary["local_download_complete"])

    def test_unavailable_pending_version_is_not_covered_by_older_success(self):
        for replacement in (None, "directory", "symlink"):
            with self.subTest(replacement=replacement):
                name = f"{replacement}.pdf"
                self.remote.add(name, b"old")
                self.assertEqual(self.invoke()[0], 0)
                self.remote.add(name, b"new", mtime=2)
                self.assertEqual(self.invoke("inventory")[0], 0)
                if replacement is None:
                    del self.remote.entries[name]
                else:
                    self.remote.add(name, kind=replacement)
                code, summary = self.invoke("reconcile")
                self.assertEqual(code, 2)
                self.assertFalse(summary["local_download_complete"])
                self.assertIn(name, [e["remote_path"] for e in self.csv(summary, "unresolved")
                                     if e["operation"] == "availability"])
                self.assertIn(name, [gap["remote_path"] for gap in self.csv(summary, "gaps")])
                self.assertEqual((self.cfg.export / name).read_bytes(), b"old")
                self.remote.add(name, b"new", mtime=2)
                self.assertEqual(self.invoke()[0], 0)

    def test_unavailable_forced_repeat_remains_a_gap(self):
        self.remote.add("a.pdf")
        self.invoke()
        self.cfg = replace(self.cfg, max_staged_bytes=3)
        self.assertEqual(self.invoke(force_path="a.pdf")[1]["stop_reason"], "batch_capacity")
        del self.remote.entries["a.pdf"]
        code, summary = self.invoke("reconcile")
        self.assertEqual(code, 2)
        self.assertFalse(summary["local_download_complete"])
        self.assertEqual(summary["pending_deferred_count"], 1)

    def test_forced_repeat_survives_metadata_changes_and_reversion(self):
        self.remote.add("a.pdf", b"old")
        self.invoke()
        self.cfg = replace(self.cfg, max_staged_bytes=3)
        self.assertEqual(self.invoke(force_path="a.pdf")[1]["stop_reason"], "batch_capacity")
        self.remote.add("a.pdf", b"new", mtime=2)
        self.invoke("inventory")
        self.assertEqual(self.rows("SELECT forced FROM transfers WHERE state='pending'")[0][0], 1)
        self.remote.add("a.pdf", b"old", mtime=1)
        (self.cfg.export / "a.pdf").unlink()
        code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(summary["forced_successes"], 1)
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"old")

    def test_reconstructed_report_names_its_new_backup(self):
        self.remote.add("a.pdf")
        _, original = self.invoke()
        original_backup = Path(original["reports"]["backup"])
        original_bytes = original_backup.read_bytes()
        code, rebuilt = self.invoke("status", report_run=original["run_id"])
        self.assertEqual(code, 0)
        backup = Path(rebuilt["reports"]["backup"])
        self.assertNotEqual(backup, original_backup)
        self.assertTrue(backup.is_file())
        self.assertEqual(original_backup.read_bytes(), original_bytes)
        with closing(sqlite3.connect(backup)) as db:
            saved = json.loads(db.execute("SELECT summary FROM runs WHERE id=?", (original["run_id"],)).fetchone()[0])
        self.assertEqual(saved["reports"]["backup"], str(backup))

    def test_reconstructed_status_keeps_its_original_scan(self):
        self.remote.add("a.pdf")
        _, inventory = self.invoke("inventory")
        _, status = self.invoke("status")
        self.remote.add("b.pdf")
        self.invoke("inventory")
        _, rebuilt = self.invoke("status", report_run=status["run_id"])
        self.assertEqual(rebuilt["scan_run"], inventory["run_id"])
        self.assertEqual(rebuilt["pdfs_discovered"], 1)

    def test_reconstructed_failed_run_preserves_exit_code(self):
        for failure, expected in ((FatalError("connection unavailable"), 1), (KeyboardInterrupt(), 130)):
            with self.subTest(expected=expected):
                self.invoke("inventory")
                with patch.object(self.remote, "connect", side_effect=failure):
                    code, failed = self.invoke()
                self.assertEqual(code, expected)
                code, rebuilt = self.invoke("status", report_run=failed["run_id"])
                self.assertEqual(code, expected)
                self.assertEqual(rebuilt["stop_reason"], failed["stop_reason"])

    def test_reconstructed_reconciliation_still_reports_gaps(self):
        self.remote.add("a.pdf")
        code, original = self.invoke("reconcile")
        self.assertEqual(code, 2)
        code, rebuilt = self.invoke("status", report_run=original["run_id"])
        self.assertEqual(code, 2)
        self.assertEqual(rebuilt["pending_deferred_count"], 1)

    def test_reconstructed_report_failure_still_backs_up_state(self):
        _, original = self.invoke("inventory")
        before = set(self.cfg.backups.iterdir())
        with (
            patch("clm_exporter.cli.write_reports", side_effect=OSError("report write failed")),
            self.assertRaisesRegex(OSError, "report write failed"),
        ):
            self.invoke("status", report_run=original["run_id"])
        backups = set(self.cfg.backups.iterdir()) - before
        self.assertEqual(len(backups), 1)
        with closing(sqlite3.connect(backups.pop())) as db:
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def test_staging_manifest_includes_forced_and_payload_status(self):
        self.remote.add("a.pdf")
        self.invoke()
        _, summary = self.invoke(force_path="a.pdf")
        row = self.csv(summary, "staging")[0]
        self.assertEqual(row["forced"], "1")
        self.assertEqual(row["payload_status"], "staged")

    def test_50000_entry_inventory(self):
        for number in range(50_000):
            self.remote.add(f"folder-{number // 1000:02}/document-{number:05}.PDF", b"", mtime=number)
        code, summary = self.invoke("inventory")
        self.assertEqual(code, 0)
        self.assertEqual(summary["pdfs_discovered"], 50_000)
        self.assertEqual(summary["pending_deferred_count"], 50_000)
        self.assertEqual(len(self.rows("SELECT id FROM transfers")), 50_000)
        self.assertFalse(self.remote.opens)
        self.assertFalse(list(self.cfg.export.iterdir()))

    def test_ready_temp_preserves_old_version_until_recovery(self):
        self.remote.add("a.pdf", b"old")
        self.invoke()
        self.remote.add("a.pdf", b"new", 2)
        def crash(event, job):
            if event == "ready":
                raise SystemExit
        with self.assertRaises(SystemExit):
            self.invoke(checkpoint=crash)
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"old")
        self.assertEqual(self.invoke()[0], 0)
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"new")
        self.assertEqual(len(self.remote.opens), 2)

    def test_lost_ready_copies_return_pending(self):
        self.remote.add("a.pdf", b"pdf")
        def crash(event, job):
            if event == "ready":
                raise SystemExit
        with self.assertRaises(SystemExit):
            self.invoke(checkpoint=crash)
        for part in self.cfg.work.iterdir():
            part.unlink()
        self.assertEqual(self.invoke()[0], 0)
        self.assertEqual(self.remote.opens, ["a.pdf", "a.pdf"])

    def test_resolved_recovery_conflict_does_not_block_reverted_source(self):
        self.remote.add("a.pdf", b"old")
        self.invoke()
        self.remote.add("a.pdf", b"new", mtime=2)

        def crash(event, job):
            if event == "ready":
                raise SystemExit

        with self.assertRaises(SystemExit):
            self.invoke(checkpoint=crash)
        target = self.cfg.export / "a.pdf"
        target.write_bytes(b"conflict")
        self.assertEqual(self.invoke("inventory")[0], 2)
        target.write_bytes(b"old")
        for part in self.cfg.work.iterdir():
            part.unlink()
        self.remote.add("a.pdf", b"old")
        opens = len(self.remote.opens)
        code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertTrue(summary["local_download_complete"])
        self.assertEqual(summary["unresolved_file_errors"], 0)
        self.assertEqual(len(self.remote.opens), opens)
        self.assertEqual(target.read_bytes(), b"old")

    def test_ready_conflict_does_not_orphan_temp_for_new_version(self):
        self.remote.add("a.pdf", b"pdf")
        def crash(event, job):
            if event == "ready":
                raise SystemExit
        with self.assertRaises(SystemExit):
            self.invoke(checkpoint=crash)
        (self.cfg.export / "a.pdf").write_bytes(b"untracked")
        self.remote.add("a.pdf", b"changed", 2)
        self.assertEqual(self.invoke()[0], 2)
        self.assertEqual(self.rows("SELECT state FROM transfers")[0][0], "ready")
        self.assertEqual(len(list(self.cfg.work.iterdir())), 1)
        (self.cfg.export / "a.pdf").unlink()
        self.assertEqual(self.invoke()[0], 0)
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"changed")

    def test_disappears_at_open_and_never_claims_success(self):
        self.remote.add("a.pdf")
        self.remote.add("b.pdf")
        def vanish(path):
            if path == "a.pdf":
                del self.remote.entries[path]
        self.remote.open_hook = vanish
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual(self.remote.opens, ["a.pdf", "b.pdf"])
        self.assertEqual(summary["historical_successful_transfers"], 1)

    def test_source_keeps_changing_exhausts_budget(self):
        self.remote.add("a.pdf", b"1234")
        def change(path, offset):
            if offset == 2:
                self.remote.entries[path]["mtime"] += 1
        self.remote.read_hook = change
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual(len(self.remote.opens), 3)
        self.assertEqual(summary["historical_successful_transfers"], 0)
        self.assertFalse(list(self.cfg.work.iterdir()))

    def test_database_failure_never_marks_transfer_success(self):
        self.remote.add("a.pdf")
        def fail(event, job):
            if event == "ready":
                raise sqlite3.OperationalError("simulated full database")
        code, summary = self.invoke(checkpoint=fail)
        self.assertEqual(code, 1)
        self.assertEqual(summary["historical_successful_transfers"], 0)
        self.assertEqual(self.rows("SELECT state FROM transfers")[0][0], "ready")
        self.assertEqual(self.invoke()[0], 0)

    def test_write_failure_never_succeeds(self):
        self.remote.add("a.pdf")
        from clm_exporter import transfer
        original = transfer.regular_open
        def fail(path, flags=0):
            if flags:
                raise OSError("write failed")
            return original(path)
        with patch("clm_exporter.transfer.regular_open", side_effect=fail):
            code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual(summary["historical_successful_transfers"], 0)
        self.assertFalse(list(self.cfg.work.iterdir()))

    def test_dropped_connection_abort_does_not_fail_every_remaining_file(self):
        self.remote.add("a.pdf")
        self.remote.add("b.pdf")
        self.remote.open_hook = lambda _: (_ for _ in ()).throw(ConnectionError("dropped"))
        self.remote.reconnect = lambda _: (_ for _ in ()).throw(FatalError("reconnect budget exhausted"))
        code, _ = self.invoke()
        self.assertEqual(code, 1)
        self.assertEqual(self.remote.opens, ["a.pdf"])
        states = [row[0] for row in self.rows("SELECT state FROM transfers ORDER BY id")]
        self.assertEqual(states, ["failed", "pending"])

    def test_symlinked_ancestor_is_canonicalized_but_managed_symlinks_rejected(self):
        alias = self.root / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        cfg = replace(self.cfg, working_directory=alias / "exporter")
        self.assertEqual(cfg.working_directory, self.cfg.working_directory)
        with redirect_stderr(io.StringIO()):
            self.assertEqual(execute(cfg, "status")[0], 0)
        managed_alias = self.root / "managed-alias"
        managed_alias.symlink_to(self.cfg.working_directory, target_is_directory=True)
        cfg = replace(self.cfg, working_directory=managed_alias)
        with self.assertRaises(PathError):
            execute(cfg, "status")

    def test_source_binding_includes_root_and_destination(self):
        original = self.cfg
        for field, value in [("remote_root", "/other"), ("host", "other.test"),
                             ("port", 2222), ("export_folder_name", "Other-export")]:
            self.cfg = replace(original, **{field: value})
            with self.assertRaises(FatalError):
                self.invoke("status")
        self.cfg = original

    def test_reports_refuse_symlinked_state_directories(self):
        # Move only this fixture's generated reports, then replace its directory with a link.
        saved = self.root / "saved-reports"
        self.cfg.reports.rename(saved)
        self.cfg.reports.symlink_to(saved, target_is_directory=True)
        with self.assertRaises(PathError):
            self.invoke("status")

    def test_partial_directory_listing_is_saved_and_retried_without_duplicates(self):
        self.remote.add("a.pdf")
        self.remote.add("z.pdf")
        original_iterdir = self.remote.iterdir
        attempts = []

        def interrupted_listing(directory):
            attempts.append(directory)
            for entry in original_iterdir(directory):
                yield entry
                if len(attempts) == 1:
                    raise ConnectionError("listing interrupted")

        self.remote.iterdir = interrupted_listing
        code, summary = self.invoke()

        self.assertEqual(code, 0)
        self.assertEqual(attempts, ["", ""])
        self.assertEqual(self.remote.reconnects, 1)
        self.assertEqual(self.remote.opens, ["a.pdf", "z.pdf"])
        self.assertEqual(summary["pdfs_discovered"], 2)
        self.assertEqual(len(self.rows("SELECT * FROM observations")), 2)
        errors = self.rows("SELECT * FROM errors WHERE operation='directory'")
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["attempts"], 1)
        self.assertEqual(errors[0]["resolved_run"], summary["run_id"])

    def test_permanent_listing_failure_retains_already_observed_files(self):
        self.remote.add("a.pdf")
        self.remote.add("z.pdf")
        original_iterdir = self.remote.iterdir
        attempts = []

        def denied_listing(directory):
            attempts.append(directory)
            yield next(iter(original_iterdir(directory)))
            raise PermissionError("listing denied after first entry")

        self.remote.iterdir = denied_listing
        code, summary = self.invoke()

        self.assertEqual(code, 2)
        self.assertEqual(attempts, [""])
        self.assertFalse(summary["scan_complete"])
        self.assertEqual(summary["pdfs_discovered"], 1)
        self.assertEqual(summary["unresolved_directory_errors"], 1)
        self.assertEqual(self.remote.opens, ["a.pdf"])
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"pdf")

    def test_metadata_lookup_error_survives_batch_save_until_next_scan(self):
        self.remote.add("dir/a.pdf")
        self.remote.list_missing_attrs.add("dir")

        def fail_first_directory_stat(relative):
            if relative == "dir":
                self.remote.stat_hook = None
                raise PermissionError("metadata lookup denied")

        self.remote.stat_hook = fail_first_directory_stat
        code, summary = self.invoke("inventory")

        self.assertEqual(code, 2)
        self.assertFalse(summary["scan_complete"])
        self.assertEqual(summary["pdfs_discovered"], 1)
        errors = self.csv(summary, "unresolved")
        self.assertEqual(
            [(error["remote_path"], error["operation"], error["category"]) for error in errors],
            [("dir", "metadata", "permission_denied")],
        )

        code, summary = self.invoke("inventory")
        self.assertEqual(code, 0)
        self.assertTrue(summary["scan_complete"])
        self.assertEqual(self.csv(summary, "unresolved"), [])

    def test_disk_full_during_flush_discards_payload_and_preserves_staging(self):
        from clm_exporter import transfer

        self.remote.add("a.pdf", b"abcdef")
        (self.cfg.work / "other.part").write_bytes(b"seed")
        original_open = transfer.regular_open
        outputs = []

        @contextmanager
        def full_on_flush(path, flags=0):
            with original_open(path, flags) as output:
                outputs.append(output)
                wrapped = Mock(wraps=output)

                def flush():
                    output.flush()
                    raise OSError(errno.ENOSPC, "disk full during flush")

                wrapped.flush.side_effect = flush
                yield wrapped

        with patch("clm_exporter.transfer.regular_open", side_effect=full_on_flush):
            code, summary = self.invoke()

        self.assertEqual((code, summary["stop_reason"]), (0, "disk_reserve"))
        self.assertEqual(summary["historical_successful_transfers"], 0)
        self.assertEqual(summary["unresolved_file_errors"], 0)
        self.assertEqual(summary["current_staged_bytes"], 4)
        self.assertEqual(self.rows("SELECT state FROM transfers")[0]["state"], "pending")
        self.assertEqual([path.name for path in self.cfg.work.iterdir()], ["other.part"])
        self.assertTrue(all(output.closed for output in outputs))

        self.assertEqual(self.invoke()[0], 0)
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"abcdef")


class ConfigTests(unittest.TestCase):
    def test_example_and_rejections(self):
        cfg = load(Path(__file__).parents[1] / "config.example.toml")
        self.assertEqual(cfg.max_staged_bytes, 350_000_000_000)
        self.assertEqual(cfg.min_free_bytes, 50_000_000_000)
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "config.toml"
            example = (Path(__file__).parents[1] / "config.example.toml").read_text()
            for bad in [example.replace('"CLM-export"', '"state"'), example + '\npassphrase = "secret"\n',
                        example.replace("chunk_bytes = 1048576", "chunk_bytes = 0"),
                        example.replace('remote_root = "/your-remote-root"', 'remote_root = "relative"')]:
                path.write_text(bad)
                with self.assertRaises(FatalError):
                    load(path)

    def test_actual_cli_missing_state(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "config.toml"
            example = (Path(__file__).parents[1] / "config.example.toml").read_text().replace('"~/clm-export"', json.dumps(temp + "/new"))
            path.write_text(example)
            with redirect_stderr(io.StringIO()):
                self.assertEqual(main(["status", "--config", str(path)]), 1)


if __name__ == "__main__":
    unittest.main()
