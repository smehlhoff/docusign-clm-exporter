"""Failure-path regressions from the adversarial review; no live services."""
import importlib.util
import io
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from clm_exporter.cli import execute
from clm_exporter.config import Config
from clm_exporter.errors import FatalError

from .fakes import FakeRemote


class AdversarialReviewTests(unittest.TestCase):
    def configure(self, workers=1):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.cfg = Config("unused.invalid", "operator", self.root / "key", self.root / "hosts",
                          "/", self.root / "export", workers=workers, min_free_bytes=0)
        self.remote = FakeRemote()
        self.source = self.root / "documents.csv"
        self.source.write_text("DOCUMENT ID,DOCUMENT NAME,LOCATION PATH\nid,a.pdf,/\n", encoding="utf-8")
        self.assertEqual(self.invoke("init")[0], 0)

    def invoke(self, command, **kwargs):
        with redirect_stderr(io.StringIO()):
            return execute(self.cfg, command, remote_factory=self.remote.factory,
                           progress=False, sleep=lambda _: None, **kwargs)

    @unittest.skipUnless(importlib.util.find_spec("paramiko"), "Paramiko is not installed")
    def test_unavailable_passphrase_finalizes_failure_and_preserves_backup(self):
        import paramiko

        for workers in (1, 2):
            for failure, expected in ((EOFError("stdin is closed"), 1), (KeyboardInterrupt(), 130)):
                with self.subTest(workers=workers, failure=type(failure).__name__):
                    self.configure(workers)
                    with (
                        patch("paramiko.RSAKey.from_private_key_file", side_effect=paramiko.PasswordRequiredException()),
                        patch("clm_exporter.remote.getpass.getpass", side_effect=failure),
                        patch("paramiko.SSHClient") as client,
                        redirect_stderr(io.StringIO()),
                    ):
                        code, summary = execute(self.cfg, "download", progress=False)
                    self.assertEqual(code, expected)
                    self.assertEqual(summary["export_exit_code"], expected)
                    self.assertEqual(summary["stop_reason"], "fatal_error" if expected == 1 else "interrupted")
                    self.assertFalse(summary["local_download_complete"])
                    self.assertTrue(summary["original_results_available"])
                    self.assertTrue(Path(summary["reports"]["backup"]).is_file())
                    client.assert_not_called()
                    self.assertEqual(self.invoke("status")[0], expected)

    def test_csv_status_reassesses_repaired_integrity_without_rewriting_history(self):
        for restored in (True, False):
            with self.subTest(restored=restored):
                self.configure()
                self.remote.add("a.pdf")
                self.assertEqual(self.invoke("download", csv_path=self.source)[0], 0)
                target = self.cfg.export / "a.pdf"
                target.write_bytes(b"bad")
                code, damaged = self.invoke("reconcile", csv_path=self.source)
                self.assertEqual(code, 2)
                self.assertFalse(damaged["local_download_complete"])
                if restored:
                    target.write_bytes(b"pdf")
                else:
                    target.unlink()
                for _ in range(2):
                    code, current = self.invoke("status")
                    self.assertEqual(code, 0)
                    self.assertTrue(current["local_download_complete"])
                    self.assertEqual(current["document_outcomes"]["previously_downloaded"], 1)
                    self.assertEqual(current["current_staging"]["integrity_errors"], [])
                    self.assertFalse(current["latest_online_attempt"]["local_download_complete"])
                code, historical = self.invoke("status", report_run=damaged["run_id"])
                self.assertEqual(code, 2)
                self.assertFalse(historical["local_download_complete"])
                self.assertEqual(self.remote.opens, ["a.pdf"])

    def test_csv_status_preserves_fatal_or_interrupted_online_outcome(self):
        for failure, expected in ((FatalError("fixture failure after scan"), 1), (KeyboardInterrupt(), 130)):
            with self.subTest(failure=type(failure).__name__):
                self.configure()
                self.remote.add("a.pdf")
                self.assertEqual(self.invoke("download", csv_path=self.source)[0], 0)
                with patch("clm_exporter.transfer.Transfers.download", side_effect=failure):
                    code, stopped = self.invoke("download", csv_path=self.source)
                self.assertEqual(code, expected)
                self.assertEqual(stopped["scan_status"], "complete")
                for _ in range(2):
                    code, current = self.invoke("status")
                    self.assertEqual(code, expected)
                    self.assertFalse(current["local_download_complete"])
                    self.assertEqual(current["scan_run"], stopped["run_id"])
