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
    def exercise(self, trust="trusted", password=None, accepted=True, stall=None, invalid_name=False):
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
                def lstat(self, path):
                    return p.SFTPAttributes.from_stat((source / path.lstrip("/")).lstat())

                stat = lstat

                def list_folder(self, path):
                    result = []
                    for file in (source / path.lstrip("/")).iterdir():
                        info = p.SFTPAttributes.from_stat(file.lstat())
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
                         max_attempts=1, read_timeout_seconds=1 if stall else 60)
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

            def connect(client, **kwargs):
                return original_connect(client, sock=client_socket, **kwargs)
            # Bound the regression itself when exercising the unfixed implementation.
            watchdog = threading.Timer(4 if stall else 30, client_socket.close)
            watchdog.daemon = True
            watchdog.start()
            started = time.monotonic()
            try:
                with patch.object(p.SSHClient, "connect", connect), patch("getpass.getpass", return_value=password), redirect_stderr(io.StringIO()):
                    code, summary = execute(cfg, "download")
                if stall:
                    self.assertLess(time.monotonic() - started, 3)
                    self.assertEqual(code, 1)
                    self.assertEqual(summary["stop_reason"], "fatal_error")
                    self.assertEqual(summary["historical_successful_transfers"], 0)
                elif invalid_name:
                    self.assertEqual(code, 2)
                    self.assertFalse(summary["scan_complete"])
                    self.assertEqual(summary["unresolved_directory_errors"], 1)
                    self.assertEqual(summary["new_successes"], 1)
                    self.assertTrue(Path(summary["reports"]["backup"]).exists())
                elif trust == "trusted" and accepted:
                    self.assertEqual(code, 0)
                    self.assertEqual(summary["new_successes"], 1)
                    self.assertEqual((cfg.export / "Example.PDF").read_bytes(), (source / "Example.PDF").read_bytes())
                else:
                    self.assertEqual(code, 1)
                    self.assertEqual(summary["historical_successful_transfers"], 0)
            finally:
                watchdog.cancel()
                watchdog.join()
                release.set()
                server.close()
                client_socket.close()
                server_socket.close()
                thread.join(timeout=5)

    def test_trusted_rsa_transfer(self):
        self.exercise()

    def test_encrypted_rsa_transfer(self):
        self.exercise(password="test-only-passphrase")

    def test_unknown_host_rejected(self):
        self.exercise(trust="unknown")

    def test_mismatched_host_rejected(self):
        self.exercise(trust="mismatch")

    def test_invalid_credentials_rejected(self):
        self.exercise(accepted=False)

    def test_stalled_sftp_version_negotiation_obeys_timeout(self):
        self.exercise(stall="version")

    def test_stalled_sftp_subsystem_request_obeys_timeout(self):
        self.exercise(stall="request")

    def test_invalid_utf8_filename_retains_partial_scan_and_reports_error(self):
        self.exercise(invalid_name=True)
