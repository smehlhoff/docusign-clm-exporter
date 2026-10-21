"""Process startup and restoration of a resumable export from its own backup."""
import csv
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from clm_exporter.cli import execute
from clm_exporter.config import load

from .fakes import FakeRemote


class CoreWorkflowTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        config_dir = self.root / "configuration"
        config_dir.mkdir()
        self.config = config_dir / "export.toml"
        self.config.write_text('''
[sftp]
host = "unused.invalid"
username = "operator"
private_key_path = "missing-key"
known_hosts_path = "missing-known-hosts"
remote_root = "/Main"
[local]
working_directory = "./export"
[limits]
max_staged_bytes = 4
min_free_bytes = 0
''', encoding="utf-8")
        self.cfg = load(self.config)

    def cli(self, command, *options, expected=0):
        result = subprocess.run(
            [sys.executable, "-m", "clm_exporter", command,
             "--config", str(self.config), "--no-progress", *options],
            cwd=self.root,
            env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])},
            capture_output=True, text=True, timeout=15, check=False,
        )
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        return result

    def report(self, summary, name):
        with Path(summary["reports"][name]).open(encoding="utf-8", newline="") as stream:
            return list(csv.DictReader(stream))

    def test_module_cli_initializes_and_reports_offline(self):
        self.assertIn("Initialized", self.cli("init").stdout)
        self.assertTrue(self.cfg.database.is_file())
        self.assertFalse((self.root / "export").exists())

        # A fresh process must load the persisted state without SFTP credentials.
        summary = json.loads(self.cli("status").stdout)
        self.assertEqual(summary["current_staging"]["completed_pdfs"], 0)
        self.assertNotIn("historical_successful_transfers", summary)
        self.assertEqual(self.report(summary, "staging"), [])
        self.assertTrue(Path(summary["reports"]["backup"]).is_file())
        saved = json.loads(Path(summary["reports"]["summary"]).read_text())
        self.assertEqual(saved, summary)

        # An accidental second init must leave the existing export usable.
        rejected = self.cli("init", expected=1)
        self.assertIn("State already exists", rejected.stderr)
        self.assertNotIn("Initialized", rejected.stdout)
        self.cli("status", "--run-id", summary["run_id"])

    def test_restored_backup_resumes_pending_csv_without_redownloading_offloaded_files(self):
        self.cli("init")
        remote = FakeRemote()
        remote.add("Contracts/first.pdf", b"paid")
        remote.add("Contracts/second.pdf", b"term")
        source = self.root / "documents.csv"
        content = ("DOCUMENT ID,DOCUMENT NAME,LOCATION PATH\n"
                   "contract-1,first.pdf,/Main/Contracts\n"
                   "contract-2,second.pdf,/Main/Contracts\n")
        source.write_text(content, encoding="utf-8")
        with redirect_stderr(io.StringIO()):
            code, first = execute(self.cfg, "download", csv_path=source,
                                  remote_factory=remote.factory, progress=False,
                                  free=lambda: 10000, sleep=lambda _: None)
        self.assertEqual((code, first["stop_reason"]), (0, "batch_capacity"))
        self.assertEqual(remote.opens, ["Contracts/first.pdf"])
        self.assertEqual((self.cfg.export / "Contracts/first.pdf").read_bytes(), b"paid")
        saved_documents = self.report(first, "documents")

        # Model offloading and loss of the active DB, using only this fixture's files.
        shutil.rmtree(self.cfg.export)
        self.cfg.database.replace(self.root / "previous.sqlite3")
        shutil.copyfile(first["reports"]["backup"], self.cfg.database)
        source.unlink()

        restored = json.loads(self.cli("status", "--run-id", first["run_id"]).stdout)
        self.assertEqual(restored["document_outcomes"], first["document_outcomes"])
        documents = self.report(restored, "documents")
        self.assertEqual(
            [(row["document_id"], row["outcome"], row["transfer_id"]) for row in documents],
            [(row["document_id"], row["outcome"], row["transfer_id"]) for row in saved_documents],
        )
        self.assertEqual(documents[0]["local_payload_status"], "absent")
        self.assertEqual(self.report(restored, "staging"), [])

        source.write_text(content, encoding="utf-8")
        remote.opens.clear()
        with redirect_stderr(io.StringIO()):
            code, resumed = execute(self.cfg, "download", csv_path=source,
                                    remote_factory=remote.factory, progress=False,
                                    free=lambda: 10000, sleep=lambda _: None)
        self.assertEqual(code, 0)
        self.assertTrue(resumed["local_download_complete"])
        self.assertEqual(remote.opens, ["Contracts/second.pdf"])
        self.assertFalse((self.cfg.export / "Contracts/first.pdf").exists())
        self.assertEqual((self.cfg.export / "Contracts/second.pdf").read_bytes(), b"term")
        self.assertEqual(resumed["historical_successful_transfers"], 2)
        self.assertEqual(resumed["document_outcomes"], {
            "failed": 0, "unchecked": 0, "pending": 0,
            "downloaded_this_run": 1, "previously_downloaded": 1,
        })
