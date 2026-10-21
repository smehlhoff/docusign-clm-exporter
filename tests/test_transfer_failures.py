"""Serial and parallel downloads must preserve the same failure contracts."""
import errno
import io
import sqlite3
import tempfile
import unittest
from contextlib import closing, redirect_stderr
from pathlib import Path
from unittest.mock import Mock

from clm_exporter.cli import execute
from clm_exporter.config import Config
from clm_exporter.errors import MetadataError, PathError, SourceChanged

from .test_concurrency import Sessions


class TransferFailureTests(unittest.TestCase):
    def configure(self, workers):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        self.cfg = Config("test", "user", root / "key", root / "hosts", "/",
                          root / "export", workers=workers, min_free_bytes=0)
        self.sessions = Sessions()
        self.sessions.source.add("a.pdf")
        self.sleep = Mock()
        self.recovery_checks = Mock(side_effect=lambda exc: isinstance(exc, (ConnectionError, EOFError, TimeoutError)))
        self.assertEqual(self.invoke("init")[0], 0)

    def invoke(self, command="download", **kwargs):
        # The fixture's clients enforce connection ownership in both modes.
        def factory(cfg, state):
            client = self.sessions.factory(cfg, None)
            client.reconnect = self.recovery_checks
            client.is_connection_error = self.recovery_checks
            return client

        with redirect_stderr(io.StringIO()):
            return execute(self.cfg, command, remote_factory=factory,
                           free=lambda: 10000, sleep=self.sleep, progress=False, **kwargs)

    def test_failure_classification_and_reconnect_before_retry_decision(self):
        cases = [
            (PermissionError("denied"), 1),
            (FileNotFoundError("missing"), 1),
            (MetadataError("invalid metadata"), 1),
            (PathError("unsafe target"), 1),
            (OSError(errno.ENAMETOOLONG, "name too long"), 1),
            (ConnectionError("dropped"), 3),
            (SourceChanged("changed"), 3),
        ]
        for workers in (1, 2):
            for error, attempts in cases:
                with self.subTest(workers=workers, error=error):
                    self.configure(workers)
                    self.sessions.source.open_hook = Mock(side_effect=error)
                    code, summary = self.invoke()
                    self.assertEqual(code, 2)
                    self.assertEqual(self.sessions.source.open_hook.call_count, attempts)
                    self.assertTrue(all(call.args == ("a.pdf",)
                                        for call in self.sessions.source.open_hook.call_args_list))
                    # Even a terminal failure reaches the connection's recovery hook.
                    self.assertEqual(self.recovery_checks.call_count, attempts)
                    self.assertTrue(all(call.args[0] is error for call in self.recovery_checks.call_args_list))
                    self.assertEqual([call.args[0] for call in self.sleep.call_args_list],
                                     list(range(1, attempts)))
                    self.assertEqual(summary["historical_successful_transfers"], 0)
                    self.assertFalse(list(self.cfg.work.iterdir()))
                    with closing(sqlite3.connect(self.cfg.database)) as db:
                        self.assertEqual(db.execute("SELECT state,attempts,last_error FROM transfers").fetchall(),
                                         [("failed", attempts, str(error))])
                        self.assertEqual(db.execute("SELECT operation FROM errors").fetchall(),
                                         [("transfer",)] * attempts)

    def test_failure_after_ready_preserves_payload_for_local_recovery(self):
        for workers in (1, 2):
            for error in (OSError(errno.EIO, "promotion failed"), OSError(errno.ENOSPC, "disk full")):
                with self.subTest(workers=workers, error=error):
                    self.configure(workers)

                    def fail_ready(event, job, error=error):
                        if event == "ready":
                            raise error

                    code, summary = self.invoke(checkpoint=fail_ready)
                    disk_full = error.errno == errno.ENOSPC
                    self.assertEqual(code, 0 if disk_full else 2)
                    self.assertEqual(summary["stop_reason"], "disk_reserve" if disk_full else "completed")
                    self.recovery_checks.assert_not_called()
                    self.sleep.assert_not_called()
                    with closing(sqlite3.connect(self.cfg.database)) as db:
                        state, temp = db.execute("SELECT state,temp FROM transfers").fetchone()
                        self.assertEqual(state, "ready")
                        self.assertEqual(db.execute("SELECT operation FROM errors").fetchall(),
                                         [] if disk_full else [("recovery",)])
                    self.assertEqual((self.cfg.work / temp).read_bytes(), b"pdf")
                    self.assertEqual(self.invoke()[0], 0)
                    self.assertEqual(self.sessions.source.opens, ["a.pdf"])
                    self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"pdf")
