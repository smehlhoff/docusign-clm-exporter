"""Optional real SSH/SFTP protocol tests over a local socket pair.

Run after installing requirements.txt. These do not contact CLM or use user keys.
"""
import importlib.util
import io
import os
import socket
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from clm_exporter.cli import execute
from clm_exporter.config import Config


@unittest.skipUnless(importlib.util.find_spec("paramiko"), "Paramiko is not installed; real protocol tests unavailable")
class ProtocolTests(unittest.TestCase):
    def exercise(self, trust="trusted", password=None, accepted=True, stall=None, invalid_name=False,
                 reported_mode=None, directory_mode=None):
        import paramiko as p
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            client_key, host_key = p.RSAKey.generate(2048), p.RSAKey.generate(2048)
            key_path, known = root / "id_rsa", root / "known_hosts"
            client_key.write_private_key_file(str(key_path), password=password)
            trusted_key = host_key if trust == "trusted" else p.RSAKey.generate(2048)
            known.write_text("" if trust == "unknown" else f"sftp.test {trusted_key.get_name()} {trusted_key.get_base64()}\n")
            source = root / "source"
            source.mkdir()
            (source / "Example.PDF").write_bytes(b"fixture content selected by extension")
            pdfs = ["Example.PDF"]
            if directory_mode is not None:
                nested = "System Admin/Templates/1/2/3/test.pdf"
                (source / nested).parent.mkdir(parents=True)
                (source / nested).write_bytes(b"nested PDF")
                (source / "System Admin/Templates/Empty").mkdir()
                pdfs.append(nested)

            release = threading.Event()

            class Server(p.ServerInterface):
                def get_allowed_auths(self, username):
                    return "publickey"

                def check_auth_publickey(self, username, key):
                    return p.AUTH_SUCCESSFUL if accepted and username == "operator" and key == client_key else p.AUTH_FAILED

                def check_channel_request(self, kind, channel_id):
                    return p.OPEN_SUCCEEDED if kind == "session" else p.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

                def check_channel_subsystem_request(self, channel, name):
                    if stall == "request":
                        release.wait(5)
                        return False
                    return super().check_channel_subsystem_request(channel, name)

            class StalledSFTP(p.SFTPServer):
                def start_subsystem(self, name, transport, channel):
                    release.wait(5)

            class Files(p.SFTPServerInterface):
                def attributes(self, file):
                    info = p.SFTPAttributes.from_stat(file.lstat())
                    if reported_mode is not None and file.is_file():
                        info.st_mode = reported_mode
                    if directory_mode is not None and file.is_dir():
                        info.st_mode = directory_mode
                    return info

                def lstat(self, path):
                    return self.attributes(source / path.lstrip("/"))

                stat = lstat

                def list_folder(self, path):
                    if (source / path.lstrip("/")).is_file():
                        # SFTP v3 servers may reject OPENDIR on a file using
                        # either generic FAILURE or NO_SUCH_FILE.
                        return p.SFTP_FAILURE if directory_mode == 0o140750 else p.SFTP_NO_SUCH_FILE
                    result = []
                    for file in (source / path.lstrip("/")).iterdir():
                        info = self.attributes(file)
                        info.filename = file.name
                        result.append(info)
                    if invalid_name:
                        bad = p.SFTPAttributes.from_stat((source / "Example.PDF").stat())
                        bad.filename = b"bad\xff.pdf"
                        result.append(bad)
                    return result

                def open(self, path, flags, attr):
                    if flags != os.O_RDONLY:
                        return p.SFTP_PERMISSION_DENIED
                    handle = p.SFTPHandle(flags)
                    handle.readfile = (source / path.lstrip("/")).open("rb")
                    return handle

            cfg = Config("sftp.test", "operator", key_path, known, "/", root / "working", min_free_bytes=0,
                         max_attempts=1 if stall else 3, read_timeout_seconds=1 if stall else 60)
            with redirect_stderr(io.StringIO()):
                execute(cfg, "init")
            client_socket, server_socket = socket.socketpair()
            server = p.Transport(server_socket)
            server.add_server_key(host_key)
            server.set_subsystem_handler("sftp", StalledSFTP if stall == "version" else p.SFTPServer, Files)
            errors = []

            def serve():
                try:
                    server.start_server(server=Server())
                except (EOFError, p.SSHException) as exc:
                    errors.append(str(exc))
            thread = threading.Thread(target=serve, daemon=True)
            thread.start()
            original_connect = p.SSHClient.connect
            connections = 0

            def connect(client, **kwargs):
                nonlocal connections
                connections += 1
                return original_connect(client, sock=client_socket, **kwargs)
            # Bound the regression itself when exercising the unfixed implementation.
            watchdog = threading.Timer(4 if stall else 30, client_socket.close)
            watchdog.daemon = True
            watchdog.start()
            started = time.monotonic()
            try:
                with (
                    patch.object(p.SSHClient, "connect", connect),
                    patch("getpass.getpass", return_value=password) as prompt,
                    redirect_stderr(io.StringIO()) as stderr,
                ):
                    code, summary = execute(cfg, "download", sleep=lambda _: None)
                if password:
                    prompt.assert_called_once()
                else:
                    prompt.assert_not_called()
                if stall:
                    self.assertLess(time.monotonic() - started, 3)
                    self.assertEqual(code, 1)
                    self.assertEqual(summary["stop_reason"], "fatal_error")
                    self.assertEqual(summary["historical_successful_transfers"], 0)
                elif invalid_name:
                    self.assertEqual(code, 2)
                    self.assertEqual(summary["scan_status"], "incomplete")
                    self.assertEqual(summary["unresolved_directory_errors"], 1)
                    self.assertEqual(summary["new_successes"], 1)
                    self.assertTrue(Path(summary["reports"]["backup"]).exists())
                elif trust == "trusted" and accepted:
                    self.assertEqual(code, 0)
                    self.assertEqual(summary["scan_status"], "complete")
                    self.assertEqual(summary["new_successes"], len(pdfs))
                    for relative in pdfs:
                        self.assertEqual((cfg.export / relative).read_bytes(), (source / relative).read_bytes())
                    if directory_mode is not None:
                        self.assertEqual(summary["directories_discovered"], 6)
                else:
                    self.assertEqual(code, 1)
                    self.assertEqual(summary["stop_reason"], "fatal_error")
                    self.assertEqual(summary["historical_successful_transfers"], 0)
                    self.assertEqual(connections, 1)
                    message = "Untrusted SSH host key" if trust == "unknown" else "SSH authentication or host-key verification failed"
                    self.assertIn(message, stderr.getvalue())
            finally:
                watchdog.cancel()
                watchdog.join()
                release.set()
                server.close()
                client_socket.close()
                server_socket.close()
                thread.join(timeout=5)

    def test_rsa_transfer_with_plain_and_encrypted_keys(self):
        for password in (None, "test-only-passphrase"):
            with self.subTest(encrypted=password is not None):
                self.exercise(password=password)

    def test_nested_transfer_with_clm_special_types(self):
        for directory_mode in (0o140750, 0o750):
            with self.subTest(directory_mode=oct(directory_mode)):
                self.exercise(reported_mode=0o140640, directory_mode=directory_mode)

    def test_security_rejections_are_fatal_without_retry(self):
        for trust, accepted in (("unknown", True), ("mismatch", True), ("trusted", False)):
            with self.subTest(trust=trust, accepted=accepted):
                self.exercise(trust=trust, accepted=accepted)

    def test_stalled_sftp_setup_obeys_timeout(self):
        for stage in ("version", "request"):
            with self.subTest(stage=stage):
                self.exercise(stall=stage)

    def test_malformed_known_hosts_is_reported_and_backed_up(self):
        import paramiko as p

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            key, known = root / "key", root / "known_hosts"
            p.RSAKey.generate(2048).write_private_key_file(str(key))
            cfg = Config("sftp.test", "operator", key, known, "/", root / "working", min_free_bytes=0)
            with redirect_stderr(io.StringIO()):
                execute(cfg, "init", progress=False)
            for content in (b"sftp.test ssh-rsa a\n", b"\xff\n"):
                with self.subTest(content=content):
                    known.write_bytes(content)
                    with patch.object(p.SSHClient, "connect") as connect, redirect_stderr(io.StringIO()) as stderr:
                        code, summary = execute(cfg, "download", progress=False)
                    connect.assert_not_called()
                    self.assertEqual(code, 1)
                    self.assertEqual(summary["stop_reason"], "fatal_error")
                    self.assertIn("Cannot load the configured known_hosts file", stderr.getvalue())
                    self.assertTrue(Path(summary["reports"]["summary"]).exists())
                    self.assertTrue(Path(summary["reports"]["backup"]).exists())

    def test_invalid_utf8_filename_retains_partial_scan_and_reports_error(self):
        self.exercise(invalid_name=True)
