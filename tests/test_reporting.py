import csv
import io
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from clm_exporter.cli import execute
from clm_exporter.config import Config

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
        self.assertEqual([row["reason"] for row in self.rows(rebuilt, "gaps")], [
            "explicit_repeat_pending", "pending_path_not_observed", "pending_path_not_observed",
        ])

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
