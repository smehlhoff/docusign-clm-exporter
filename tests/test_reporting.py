import csv
import io
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from clm_exporter.cli import execute
from clm_exporter.config import Config
from clm_exporter.errors import FatalError

from .fakes import FakeRemote


class ReportBehaviorTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        self.cfg = Config("sftp.test", "operator", root / "key", root / "known_hosts",
                          "/root", root / "export", min_free_bytes=10)
        self.remote = FakeRemote()
        self.invoke("init")

    def invoke(self, command, **kwargs):
        with redirect_stderr(io.StringIO()):
            return execute(self.cfg, command, remote_factory=self.remote.factory,
                           sleep=lambda _: None, progress=False, **kwargs)

    def rows(self, summary, report):
        with open(summary["reports"][report], newline="") as stream:
            return list(csv.DictReader(stream))

    def test_recursive_status_does_not_reuse_scan_before_connection_failure(self):
        self.remote.add("a.pdf")
        _, successful = self.invoke("download")
        for error, expected in ((FatalError("offline"), 1), (KeyboardInterrupt(), 130)):
            with self.subTest(error=type(error).__name__):
                with patch.object(self.remote, "connect", side_effect=error):
                    code, failed = self.invoke("reconcile")
                self.assertEqual(code, expected)
                self.assertEqual(failed["scan_run"], failed["run_id"])
                self.assertEqual(failed["scan_status"], "not_started")
                for _ in range(2):
                    code, status = self.invoke("status")
                    self.assertEqual(code, expected)
                    self.assertFalse(status["local_download_complete"])
                    self.assertEqual(status["scan_run"], failed["run_id"])
                    self.assertEqual(status["pdfs_discovered"], 0)
                    self.assertEqual(status["currently_staged_completed_pdfs"], 1)
                code, historical = self.invoke("status", report_run=successful["run_id"])
                self.assertEqual(code, 0)
                self.assertTrue(historical["local_download_complete"])

    def test_recursive_status_does_not_claim_completion_after_hard_exit(self):
        self.remote.add("a.pdf")
        _, successful = self.invoke("download")
        # Model a process killed after scan completion, before command completion.
        with sqlite3.connect(self.cfg.database) as db:
            db.execute("UPDATE runs SET ended_at=NULL,stop_reason=NULL WHERE id=?", (successful["run_id"],))
            db.execute("DELETE FROM run_results WHERE run_id=?", (successful["run_id"],))
        code, status = self.invoke("status")
        self.assertEqual(code, 130)
        self.assertFalse(status["local_download_complete"])
        self.assertEqual(status["scan_run"], successful["run_id"])

    def test_recursive_status_recovers_after_a_new_successful_scan(self):
        self.remote.add("a.pdf")
        self.invoke("download")
        with patch.object(self.remote, "connect", side_effect=FatalError("offline")):
            self.invoke("reconcile")
        _, latest = self.invoke("reconcile")
        code, status = self.invoke("status")
        self.assertEqual(code, 0)
        self.assertTrue(status["local_download_complete"])
        self.assertEqual(status["scan_run"], latest["run_id"])

    def test_gaps_keep_observed_order_then_missing_work_and_count_pending_repeats(self):
        self.remote.add("a.pdf")
        _, downloaded = self.invoke("download")
        _, stopped = self.invoke("download", force_path="a.pdf", free=lambda: 10)
        self.assertEqual(stopped["stop_reason"], "disk_reserve")
        self.remote.add("b.pdf")
        self.remote.add("c.pdf")
        self.invoke("inventory")
        del self.remote.entries["c.pdf"]

        code, summary = self.invoke("reconcile")
        self.assertEqual(code, 2)
        gaps = self.rows(summary, "gaps")
        self.assertEqual([(row["remote_path"], row["reason"], row["state"]) for row in gaps], [
            ("a.pdf", "explicit_repeat_pending", "pending"),
            ("b.pdf", "latest_success_does_not_match", "pending"),
            ("c.pdf", "pending_path_not_observed", "pending"),
        ])
        self.assertEqual(summary["latest_version_coverage"], 1)
        self.assertEqual(summary["unchanged_skips"], 1)
        self.assertEqual(summary["pending_deferred_count"], 3)
        self.assertEqual(summary["pending_deferred_bytes"], 9)

        _, rebuilt = self.invoke("status", report_run=downloaded["run_id"])
        self.assertEqual(rebuilt["latest_version_coverage"], 1)
        self.assertEqual(rebuilt["unchanged_skips"], 0)
        self.assertEqual(self.rows(rebuilt, "gaps"), [])
        self.assertTrue(rebuilt["local_download_complete"])

    def test_status_resolves_local_conflicts_after_restoration_or_offload(self):
        self.remote.add("a.pdf")
        self.invoke("download")
        target = self.cfg.export / "a.pdf"
        for restored in (True, False):
            with self.subTest(restored=restored):
                target.write_bytes(b"bad")  # Same size: verification must check the hash.
                code, damaged = self.invoke("status")
                self.assertEqual(code, 2)
                self.assertEqual(self.rows(damaged, "staging"), [])
                self.assertEqual(damaged["unresolved_file_errors"], 1)
                self.assertEqual(self.rows(damaged, "history")[0]["payload_status"], "absent_or_unverified")
                if restored:
                    target.write_bytes(b"pdf")
                else:
                    target.unlink()
                code, summary = self.invoke("status")
                self.assertEqual(code, 0)
                self.assertEqual(self.rows(summary, "unresolved"), [])
                self.assertEqual(summary["currently_staged_completed_pdfs"], int(restored))
                self.assertEqual(self.rows(summary, "history")[0]["payload_status"],
                                 "staged" if restored else "absent_or_unverified")
        self.assertEqual(self.remote.opens, ["a.pdf"])

    def test_history_versions_and_saved_downloads_keep_their_payload_labels(self):
        self.remote.add("a.pdf", b"old", mtime=1)
        _, first = self.invoke("download")
        original_downloads = self.rows(first, "downloads")
        self.remote.add("a.pdf", b"new version", mtime=2)
        _, second = self.invoke("download")
        new_downloads = self.rows(second, "downloads")
        history = self.rows(second, "history")
        self.assertEqual([row["payload_status"] for row in history], ["superseded", "staged"])
        self.assertEqual([row["transfer_id"] for row in history],
                         [original_downloads[0]["transfer_id"], new_downloads[0]["transfer_id"]])
        self.assertEqual([row["payload_status"] for row in new_downloads], ["staged"])

        (self.cfg.export / "a.pdf").unlink()
        code, rebuilt = self.invoke("status", report_run=first["run_id"])
        self.assertEqual(code, 0)
        self.assertEqual(self.rows(rebuilt, "downloads"), original_downloads)
        self.assertEqual([row["payload_status"] for row in self.rows(rebuilt, "history")],
                         ["superseded", "absent_or_unverified"])
