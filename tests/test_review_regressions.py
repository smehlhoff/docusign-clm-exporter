"""Adversarial checks for discovery recovery and filesystem commit ordering."""
import csv
import io
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing, redirect_stderr
from pathlib import Path
from unittest.mock import patch

from clm_exporter import transfer
from clm_exporter.cli import execute
from clm_exporter.config import Config

from .fakes import FakeRemote, attrs


class ReviewRegressionTests(unittest.TestCase):
    def configure(self, workers):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.cfg = Config("test", "operator", root / "key", root / "hosts", "/",
                          root / "export", workers=workers, min_free_bytes=0)
        self.remote = FakeRemote()
        self.assertEqual(self.invoke("init")[0], 0)

    def invoke(self, command, **kwargs):
        with redirect_stderr(io.StringIO()):
            return execute(self.cfg, command, remote_factory=self.remote.factory,
                           sleep=lambda _: None, progress=False, **kwargs)

    def test_removed_entry_retires_metadata_errors_only_after_proven_absence(self):
        for workers in (1, 2):
            with self.subTest(workers=workers):
                self.configure(workers)
                self.remote.add("parent/folder", kind="directory")
                self.remote.missing_attrs.add("parent/folder")
                self.assertEqual(self.invoke("inventory")[0], 2)

                del self.remote.entries["parent/folder"]
                self.remote.denied.add("parent")
                self.assertEqual(self.invoke("reconcile")[0], 2)
                with closing(sqlite3.connect(self.cfg.database)) as db:
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM errors WHERE operation='metadata' AND resolved_at IS NULL").fetchone()[0], 1)

                self.remote.denied.clear()
                code, summary = self.invoke("reconcile")
                self.assertEqual(code, 0)
                self.assertTrue(summary["local_download_complete"])
                self.assertEqual(summary["unresolved_file_errors"], 0)

    def test_missing_unfinished_pdf_still_blocks_after_metadata_error_retires(self):
        for workers in (1, 2):
            with self.subTest(workers=workers):
                self.configure(workers)
                self.remote.add("a.pdf")
                self.remote.missing_attrs.add("a.pdf")
                self.assertEqual(self.invoke("inventory")[0], 2)
                del self.remote.entries["a.pdf"]
                code, summary = self.invoke("reconcile")
                self.assertEqual(code, 2)
                self.assertFalse(summary["local_download_complete"])
                with closing(sqlite3.connect(self.cfg.database)) as db:
                    self.assertEqual(db.execute("SELECT operation FROM errors WHERE resolved_at IS NULL").fetchall(), [("availability",)])

    def test_removed_unmappable_directory_no_longer_blocks_reconciliation(self):
        for workers in (1, 2):
            with self.subTest(workers=workers):
                self.configure(workers)
                name = "a" * 300
                self.remote.add(name, kind="directory")
                self.assertEqual(self.invoke("inventory")[0], 2)
                del self.remote.entries[name]
                code, summary = self.invoke("reconcile")
                self.assertEqual(code, 0)
                self.assertTrue(summary["local_download_complete"])

    def test_corrected_unsafe_listing_requires_a_complete_scan(self):
        for workers in (1, 2):
            with self.subTest(workers=workers):
                self.configure(workers)
                with patch.object(self.remote, "iterdir", return_value=iter([attrs("../escape.pdf")])):
                    self.assertEqual(self.invoke("inventory")[0], 2)
                self.remote.denied.add("")
                self.assertEqual(self.invoke("reconcile")[0], 2)
                with closing(sqlite3.connect(self.cfg.database)) as db:
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM errors WHERE category='unsafe_remote_path' AND resolved_at IS NULL").fetchone()[0], 1)
                self.remote.denied.clear()
                code, summary = self.invoke("reconcile")
                self.assertEqual(code, 0)
                self.assertTrue(summary["local_download_complete"])

    def test_invalid_force_request_does_not_poison_future_runs(self):
        for workers in (1, 2):
            with self.subTest(workers=workers):
                self.configure(workers)
                self.remote.add("a.pdf")
                code, failed = self.invoke("download", force_path="typo.pdf")
                self.assertEqual(code, 2)
                self.assertFalse(failed["local_download_complete"])
                self.assertEqual(self.invoke("status")[0], 2)

                code, repaired = self.invoke("reconcile")
                self.assertEqual(code, 0)
                self.assertTrue(repaired["local_download_complete"])
                self.assertEqual(repaired["historical_unresolved_errors"], 1)
                with open(repaired["reports"]["unresolved"], newline="") as stream:
                    errors = list(csv.DictReader(stream))
                self.assertEqual([(e["remote_path"], e["in_current_scope"]) for e in errors], [("typo.pdf", "False")])
                self.assertEqual(self.invoke("status")[0], 0)
                code, original = self.invoke("status", report_run=failed["run_id"])
                self.assertEqual(code, 2)
                self.assertFalse(original["local_download_complete"])

    def test_temp_deletion_must_be_durable_before_recovery_forgets_it(self):
        for workers in (1, 2):
            with self.subTest(workers=workers):
                self.configure(workers)
                self.remote.add("a.pdf")

                # Model a crash during receive, with a tracked partial payload.
                class Crash(BaseException):
                    pass

                def crash(event, job):
                    if event == "chunk":
                        raise Crash()

                with self.assertRaises(Crash):
                    self.invoke("download", checkpoint=crash)
                with closing(sqlite3.connect(self.cfg.database)) as db:
                    name = db.execute("SELECT temp FROM transfers").fetchone()[0]
                self.assertTrue((self.cfg.work / name).exists())
                sync = transfer.sync_directory

                def fail_unlink_sync(path, name=name, sync=sync):
                    if path == self.cfg.work and not (path / name).exists():
                        raise OSError("cannot persist temporary-file deletion")
                    sync(path)

                with patch.object(transfer, "sync_directory", side_effect=fail_unlink_sync):
                    code, _ = self.invoke("inventory")
                self.assertEqual(code, 2)
                with closing(sqlite3.connect(self.cfg.database)) as db:
                    self.assertEqual(db.execute("SELECT state,temp FROM transfers").fetchone(), ("downloading", name))

                # Even an already absent path needs a successful directory sync
                # before retrying the database transition after a failed sync.
                with patch.object(transfer, "sync_directory", wraps=sync) as synced:
                    self.assertEqual(self.invoke("inventory")[0], 0)
                synced.assert_any_call(self.cfg.work)
                with closing(sqlite3.connect(self.cfg.database)) as db:
                    self.assertEqual(db.execute("SELECT state,temp FROM transfers").fetchone(), ("pending", None))
                self.assertEqual(self.invoke("download")[0], 0)
                self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"pdf")

    def test_ready_recovery_preserves_tracking_when_unlink_sync_fails(self):
        for workers in (1, 2):
            with self.subTest(workers=workers):
                self.configure(workers)
                self.remote.add("a.pdf")

                def interrupt_ready(event, job):
                    if event == "ready":
                        raise KeyboardInterrupt()

                self.assertEqual(self.invoke("download", checkpoint=interrupt_ready)[0], 130)
                with closing(sqlite3.connect(self.cfg.database)) as db:
                    name = db.execute("SELECT temp FROM transfers").fetchone()[0]
                # The first-promotion crash window has both hard links present.
                os.link(self.cfg.work / name, self.cfg.export / "a.pdf")

                def fail_work_sync(path, sync=transfer.sync_directory):
                    if path == self.cfg.work:
                        raise OSError("cannot persist temporary-file deletion")
                    sync(path)

                with patch.object(transfer, "sync_directory", side_effect=fail_work_sync):
                    self.assertEqual(self.invoke("inventory")[0], 2)
                with closing(sqlite3.connect(self.cfg.database)) as db:
                    self.assertEqual(db.execute("SELECT state,temp FROM transfers").fetchone(), ("ready", name))
                self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"pdf")
                self.assertEqual(self.invoke("inventory")[0], 0)
                self.assertEqual(self.remote.opens, ["a.pdf"])
                with closing(sqlite3.connect(self.cfg.database)) as db:
                    self.assertEqual(db.execute("SELECT state FROM transfers").fetchone()[0], "downloaded")
