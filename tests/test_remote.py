"""Adapter contract tests use a Paramiko double; no service or credentials required."""
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from clm_exporter.config import Config
from clm_exporter.errors import FatalError, MetadataError
from clm_exporter.local import prepare_layout
from clm_exporter.remote import Remote, entry_metadata, file_metadata
from clm_exporter.state import State


class MetadataTests(unittest.TestCase):
    def test_valid_size_boundaries_and_timestamp_precision(self):
        from .fakes import attrs
        for size in (0, 2**63 - 1):
            for mtime in (0, 1.25):
                with self.subTest(size=size, mtime=mtime):
                    self.assertEqual(file_metadata(attrs(size=size, mtime=mtime)), (size, str(mtime)))

    def test_optional_attributes_are_only_optional_for_nonfiles(self):
        from .fakes import attrs
        info = attrs(size=None, mtime=None)
        self.assertEqual(entry_metadata(info), (None, None))
        with self.assertRaises(MetadataError):
            file_metadata(info)


class SSHException(Exception):
    pass


class PasswordRequiredException(SSHException):
    pass


class AuthenticationException(SSHException):
    pass


class BadHostKeyException(SSHException):
    pass


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.cfg = Config("trusted.example", "operator", root / "key", root / "known_hosts", "/root", root / "working")
        prepare_layout(self.cfg)
        self.state = State(self.cfg, initialize=True)
        self.addCleanup(self.state.close)
        self.client = Mock()
        self.client.get_transport().get_remote_server_key().asbytes.return_value = b"trusted-public-key"
        self.module = SimpleNamespace(SSHClient=Mock(return_value=self.client), RSAKey=Mock(), RejectPolicy=type("RejectPolicy", (), {}),
                                      SSHException=SSHException, PasswordRequiredException=PasswordRequiredException,
                                      AuthenticationException=AuthenticationException, BadHostKeyException=BadHostKeyException)
        self.context = patch.dict("sys.modules", {"paramiko": self.module})
        self.context.start()
        self.addCleanup(self.context.stop)

    def remote(self):
        remote = Remote(self.cfg, self.state, sleep=lambda _: None)
        self.addCleanup(remote.close)
        return remote

    def test_explicit_rsa_no_fallback_and_trusted_fingerprint(self):
        remote = self.remote()
        remote.connect()
        self.client.load_host_keys.assert_called_once_with(str(self.cfg.known_hosts_path))
        args = self.client.connect.call_args.kwargs
        self.assertFalse(args["allow_agent"])
        self.assertFalse(args["look_for_keys"])
        self.assertNotIn("password", args)
        self.assertNotIn("key_filename", args)
        self.assertEqual(args["username"], "operator")
        self.assertEqual(args["pkey"], self.module.RSAKey.from_private_key_file.return_value)
        self.assertTrue(self.state.db.execute("SELECT host_key_fingerprint FROM source").fetchone()[0].startswith("SHA256:"))
        self.client.get_transport().set_keepalive.assert_called_once_with(30)
        self.client.open_sftp().get_channel().settimeout.assert_called_once_with(60)

    def test_protected_key_prompts_only_in_memory(self):
        key = object()
        self.module.RSAKey.from_private_key_file.side_effect = [PasswordRequiredException(), key]
        with patch("clm_exporter.remote.getpass.getpass", return_value="secret") as prompt:
            remote = self.remote()
        self.assertEqual(remote.key, key)
        prompt.assert_called_once()
        self.module.RSAKey.from_private_key_file.assert_called_with(str(self.cfg.private_key_path), password="secret")

    def test_invalid_rsa_key_is_fatal_without_connection(self):
        self.module.RSAKey.from_private_key_file.side_effect = SSHException("bad key")
        with self.assertRaises(FatalError):
            self.remote()
        self.client.connect.assert_not_called()

    def test_unknown_host_is_fatal(self):
        def handshake(**kwargs):
            policy = self.client.set_missing_host_key_policy.call_args.args[0]
            policy.missing_host_key(self.client, "unknown.example", Mock())
        self.client.connect.side_effect = handshake
        with self.assertRaisesRegex(FatalError, "Untrusted SSH host key"):
            self.remote().connect()
        self.assertEqual(self.client.connect.call_count, 1)

    def test_mismatched_host_and_invalid_credentials_are_fatal(self):
        for failure in (BadHostKeyException("mismatch"), AuthenticationException("rejected")):
            self.client.connect.reset_mock()
            self.client.connect.side_effect = failure
            with self.assertRaises(FatalError):
                self.remote().connect()
            self.assertEqual(self.client.connect.call_count, 1)

    def test_connection_and_reconnection_budgets_are_bounded(self):
        self.client.connect.side_effect = ConnectionError("down")
        with self.assertRaisesRegex(FatalError, "after 3 attempts"):
            self.remote().connect()
        self.assertEqual(self.client.connect.call_count, 3)
        self.client.connect.side_effect = None
        remote = self.remote()
        for _ in range(3):
            remote.reconnect(ConnectionError("dropped"))
        with self.assertRaisesRegex(FatalError, "budget exhausted"):
            remote.reconnect(ConnectionError("dropped"))

    def test_missing_known_hosts_is_fatal(self):
        self.client.load_host_keys.side_effect = FileNotFoundError()
        with self.assertRaisesRegex(FatalError, "known_hosts"):
            self.remote().connect()
        self.client.connect.assert_not_called()

    def test_bounded_directory_readahead_and_read_only_file_mode(self):
        remote = self.remote()
        remote.connect()
        sftp = self.client.open_sftp.return_value
        sftp.listdir_iter.return_value = iter([])
        list(remote.iterdir("dir"))
        sftp.listdir_iter.assert_called_once_with("/root/dir", read_aheads=1)
        sftp.open.return_value = unittest.mock.MagicMock()
        with remote.open("dir/a.pdf"):
            pass
        sftp.open.assert_called_once_with("/root/dir/a.pdf", "rb", bufsize=0)


if __name__ == "__main__":
    unittest.main()
