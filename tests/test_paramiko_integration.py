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
from types import SimpleNamespace
from unittest.mock import Mock, patch

from clm_exporter.cli import execute
from clm_exporter.config import Config
from clm_exporter.errors import MetadataError
from clm_exporter.remote import Remote


@unittest.skipUnless(importlib.util.find_spec("paramiko"), "Paramiko is not installed; real protocol tests unavailable")
class ProtocolTests(unittest.TestCase):
    def test_invalid_status_text_is_a_reportable_metadata_error(self):
        import paramiko as p
        from paramiko.sftp import (
            CMD_CLOSE,
            CMD_HANDLE,
            CMD_LSTAT,
            CMD_OPEN,
            CMD_OPENDIR,
            CMD_READ,
        )

        for operation, command in (("stat", CMD_LSTAT), ("probe", CMD_OPENDIR), ("probe_close", CMD_CLOSE),
                                   ("open", CMD_OPEN), ("read", CMD_READ), ("close", CMD_CLOSE),
                                   ("interrupt_close", CMD_CLOSE)):
            with self.subTest(operation=operation):
                sftp = object.__new__(p.SFTPClient)
                sftp._adjust_cwd = lambda path: path
                sftp._log = Mock()
                status = p.Message()
                status.add_int(p.SFTP_PERMISSION_DENIED)
                status.add_string(b"invalid\xffstatus")
                status.add_string(b"")

                def request(kind, *args, command=command, status=status, sftp=sftp):
                    if kind == command:
                        sftp._convert_status(p.Message(status.asbytes()))
                    handle = p.Message()
                    handle.add_string(b"handle")
                    return CMD_HANDLE, p.Message(handle.asbytes())

                sftp._request = Mock(side_effect=request)
                remote = object.__new__(Remote)
                remote.p, remote.sftp = p, sftp
                remote.cfg = Config("test", "user", Path("key"), Path("hosts"), "/", Path("/tmp/unused"))
                expected = (KeyboardInterrupt if operation == "interrupt_close" else
                            ConnectionError if operation in ("probe_close", "close") else MetadataError)
                with self.assertRaises(expected):
                    if operation == "stat":
                        remote.lstat("a.pdf")
                    elif operation in ("probe", "probe_close"):
                        remote.check_directory("dir")
                    else:
                        with remote.open("a.pdf") as stream:
                            if operation == "read":
                                stream.read(1)
                            elif operation == "interrupt_close":
                                raise KeyboardInterrupt

    def test_directory_handles_close_on_errors_and_early_exit(self):
        import paramiko as p
        from paramiko.sftp import (
            CMD_CLOSE,
            CMD_HANDLE,
            CMD_NAME,
            CMD_OPENDIR,
            CMD_READDIR,
            CMD_STATUS,
        )

        for failure in ("eof", "decode", "read", "interrupt", "interrupt_decode", "early_exit", "close", "close_decode"):
            with self.subTest(failure=failure):
                # Use the installed Paramiko parser over recorded SFTP
                # messages; no socket or server cleanup can hide a leaked handle.
                body = p.Message()
                body.add_int(1)
                body.add_string(b"bad\xff.pdf" if failure == "decode" else b"a.pdf")
                body.add_string(b"")
                p.SFTPAttributes()._pack(body)
                read_error = {"read": PermissionError("READDIR failed"),
                              "interrupt": KeyboardInterrupt(), "interrupt_decode": KeyboardInterrupt()}.get(failure)
                reads = 0

                def request(command, *args, read_error=read_error, body=body, failure=failure):
                    nonlocal reads
                    if command == CMD_OPENDIR:
                        handle = p.Message()
                        handle.add_string(b"handle")
                        return CMD_HANDLE, p.Message(handle.asbytes())
                    if command == CMD_READDIR:
                        reads += 1
                        if read_error is not None:
                            raise read_error
                        if reads > 1:
                            raise EOFError
                        return CMD_NAME, p.Message(body.asbytes())
                    if command == CMD_CLOSE:
                        if failure in ("interrupt_decode", "close_decode"):
                            raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid status text")
                        if failure in ("interrupt", "close"):
                            raise OSError("CLOSE failed")
                        return CMD_STATUS, p.Message()
                    self.fail(f"Unexpected SFTP command: {command}")

                sftp = SimpleNamespace(_request=Mock(side_effect=request))
                remote = object.__new__(Remote)
                remote.p, remote.sftp = p, sftp
                remote.cfg = Config("test", "user", Path("key"), Path("hosts"), "/", Path("/tmp/unused"))
                iterator = remote.iterdir("dir")
                if failure == "eof":
                    self.assertEqual([entry.filename for entry in iterator], ["a.pdf"])
                elif failure == "early_exit":
                    self.assertEqual(next(iterator).filename, "a.pdf")
                    iterator.close()
                else:
                    expected = {"decode": MetadataError, "read": PermissionError,
                                "interrupt": KeyboardInterrupt, "interrupt_decode": KeyboardInterrupt,
                                "close": ConnectionError, "close_decode": ConnectionError}[failure]
                    with self.assertRaises(expected):
                        list(iterator)
                closes = [call for call in sftp._request.call_args_list if call.args[0] == CMD_CLOSE]
                self.assertEqual(len(closes), 1)
                self.assertEqual(closes[0].args, (CMD_CLOSE, b"handle"))

    def exercise(self, trust="trusted", password=None, accepted=True, stall=None, invalid_name=False,
                 reported_mode=None, directory_mode=None, workers=1, invalid_status=False):
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
            if invalid_status:
                (source / "healthy.pdf").write_bytes(b"healthy sibling")
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

            class StatusSFTP(p.SFTPServer):
                def _send_status(self, request_number, code, desc=None):
                    if invalid_status and code == p.SFTP_PERMISSION_DENIED:
                        desc = b"invalid\xffstatus"
                    return super()._send_status(request_number, code, desc)

            class Files(p.SFTPServerInterface):
                def attributes(self, file):
                    info = p.SFTPAttributes.from_stat(file.lstat())
                    if reported_mode is not None and file.is_file():
                        info.st_mode = reported_mode
                    if directory_mode is not None and file.is_dir():
                        info.st_mode = directory_mode
                    return info

                def lstat(self, path):
                    if invalid_status and path.endswith("Example.PDF"):
                        return p.SFTP_PERMISSION_DENIED
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
                         max_attempts=1 if stall else 3, read_timeout_seconds=1 if stall else 60, workers=workers)
            with redirect_stderr(io.StringIO()):
                execute(cfg, "init")
            servers, sockets, threads = [], [], []
            errors = []
            original_connect = p.SSHClient.connect
            connections = 0

            def connect(client, **kwargs):
                nonlocal connections
                connections += 1
                client_socket, server_socket = socket.socketpair()
                sockets.extend((client_socket, server_socket))
                server = p.Transport(server_socket)
                servers.append(server)
                server.add_server_key(host_key)
                server.set_subsystem_handler("sftp", StalledSFTP if stall == "version" else StatusSFTP, Files)
                def serve():
                    try:
                        server.start_server(server=Server())
                    except (EOFError, p.SSHException) as exc:
                        errors.append(str(exc))
                thread = threading.Thread(target=serve, daemon=True)
                threads.append(thread)
                thread.start()
                return original_connect(client, sock=client_socket, **kwargs)
            # Bound the regression itself when exercising a stalled subsystem.
            watchdog = threading.Timer(4 if stall else 30, lambda: [sock.close() for sock in sockets])
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
                elif invalid_status:
                    self.assertEqual(code, 2)
                    self.assertEqual(summary["scan_status"], "complete")
                    self.assertEqual(summary["unresolved_file_errors"], 1)
                    self.assertEqual(summary["new_successes"], 1)
                    self.assertEqual((cfg.export / "healthy.pdf").read_bytes(), b"healthy sibling")
                    self.assertIn("not valid UTF-8", stderr.getvalue())
                    self.assertTrue(Path(summary["reports"]["summary"]).exists())
                    self.assertTrue(Path(summary["reports"]["backup"]).exists())
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
                    self.assertEqual(connections, workers)
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
                for server in servers:
                    server.close()
                for sock in sockets:
                    sock.close()
                for thread in threads:
                    thread.join(timeout=5)

    def test_rsa_transfer_with_plain_and_encrypted_keys(self):
        for password in (None, "test-only-passphrase"):
            with self.subTest(encrypted=password is not None):
                self.exercise(password=password)

    def test_multiple_sessions_share_one_encrypted_key_initialization(self):
        self.exercise(workers=2, directory_mode=0o140750, password="test-only-passphrase")

    def test_nested_transfer_with_clm_special_types(self):
        for directory_mode in (0o140750, 0o750):
            with self.subTest(directory_mode=oct(directory_mode)):
                self.exercise(reported_mode=0o140640, directory_mode=directory_mode)

    def test_security_rejections_are_fatal_without_retry(self):
        for workers in (1, 2):
            for trust, accepted in (("unknown", True), ("mismatch", True), ("trusted", False)):
                with self.subTest(workers=workers, trust=trust, accepted=accepted):
                    self.exercise(workers=workers, trust=trust, accepted=accepted)

    def test_stalled_sftp_setup_obeys_timeout(self):
        for workers, stage in ((1, "version"), (1, "request"), (2, "version")):
            with self.subTest(workers=workers, stage=stage):
                self.exercise(workers=workers, stall=stage)

    def test_invalid_known_hosts_is_fatal_without_connecting_and_backed_up(self):
        import paramiko as p

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            key, known = root / "key", root / "known_hosts"
            p.RSAKey.generate(2048).write_private_key_file(str(key))
            cfg = Config("sftp.test", "operator", key, known, "/", root / "working", min_free_bytes=0)
            with redirect_stderr(io.StringIO()):
                execute(cfg, "init", progress=False)
            for content in (None, b"sftp.test ssh-rsa a\n", b"\xff\n"):
                with self.subTest(content=content):
                    if content is None:
                        known.unlink(missing_ok=True)
                    else:
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

    def test_invalid_utf8_status_preserves_reports_and_healthy_downloads(self):
        for workers in (1, 2):
            with self.subTest(workers=workers):
                self.exercise(workers=workers, invalid_status=True)
