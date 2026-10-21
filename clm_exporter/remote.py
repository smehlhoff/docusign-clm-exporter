"""Read-only SFTP adapter. Paramiko is imported only for online commands."""
import base64
import errno
import getpass
import hashlib
import math
import posixpath
import random
import stat
import sys
import threading
import time
from contextlib import contextmanager

from .errors import FatalError, MetadataError
from .local import relative_parts
from .state import metadata_time


def entry_kind(attrs):
    mode = getattr(attrs, "st_mode", None)
    if type(mode) is not int:
        return "unknown"
    for predicate, kind in ((stat.S_ISREG, "file"), (stat.S_ISDIR, "directory"),
                            (stat.S_ISLNK, "symlink"), (stat.S_ISFIFO, "special"),
                            (stat.S_ISSOCK, "special"), (stat.S_ISCHR, "special"),
                            (stat.S_ISBLK, "special")):
        if predicate(mode):
            return kind
    # Permission bits alone do not establish an entry's type.
    return "unknown"


def entry_metadata(attrs):
    """Validate optional numeric attributes before serializing or storing them."""
    size, mtime = (getattr(attrs, n, None) for n in ("st_size", "st_mtime"))
    # SFTP sizes are unsigned 64-bit; SQLite INTEGER is signed 64-bit.
    if size is not None and (type(size) is not int or not 0 <= size <= 2**63 - 1):
        raise MetadataError("Entry size is invalid or exceeds the state database limit")
    if mtime is not None:
        try:
            valid_time = type(mtime) in (int, float) and math.isfinite(mtime)
        except OverflowError:
            valid_time = False
        if not valid_time:
            raise MetadataError("Entry lacks a valid modification time")
    return size, metadata_time(mtime) if mtime is not None else None


def file_metadata(attrs):
    """Validate download metadata independently of the reported SFTP file type."""
    size, mtime = entry_metadata(attrs)
    if size is None or mtime is None:
        raise MetadataError("Entry lacks valid size or modification time")
    return size, mtime


def backoff(attempt):
    time.sleep(min(2 ** (attempt - 1), 4) + random.uniform(0, 0.25))


class Remote:
    def __init__(self, cfg, state=None, sleep=backoff, *, key=None, retry=None):
        try:
            import paramiko
        except ImportError as exc:
            raise FatalError("Paramiko is missing; install requirements.txt in your virtual environment") from exc
        self.p = paramiko
        self.cfg, self.sleep = cfg, sleep
        self.client = self.sftp = None
        self.reconnections = 0
        self.retry = retry
        if key is not None:
            self.key = key
            return
        try:
            try:
                self.key = paramiko.RSAKey.from_private_key_file(str(cfg.private_key_path))
            except paramiko.PasswordRequiredException:
                try:
                    secret = getpass.getpass("RSA private-key passphrase: ")
                except EOFError as exc:
                    raise FatalError("Cannot read RSA private-key passphrase; run from an interactive terminal") from exc
                try:
                    self.key = paramiko.RSAKey.from_private_key_file(str(cfg.private_key_path), password=secret)
                finally:
                    del secret
        except (OSError, paramiko.SSHException, ValueError) as exc:
            raise FatalError("Cannot load configured RSA private key") from exc

    def absolute(self, relative):
        if relative:
            relative_parts(relative)
        return posixpath.join(self.cfg.remote_root, relative) if relative else self.cfg.remote_root

    def connect(self):
        p, cfg = self.p, self.cfg

        class RejectUnknown(p.RejectPolicy):
            def missing_host_key(self, client, hostname, key):
                raise FatalError(f"Untrusted SSH host key for {hostname}; verify it and update the configured known_hosts file")

        for attempt in range(1, cfg.max_attempts + 1):
            self.close()
            client = p.SSHClient()
            self.client = client
            try:
                try:
                    client.load_host_keys(str(cfg.known_hosts_path))
                except (OSError, p.SSHException, p.hostkeys.InvalidHostKey, ValueError) as exc:
                    raise FatalError("Cannot load the configured known_hosts file") from exc
                client.set_missing_host_key_policy(RejectUnknown())
                client.connect(hostname=cfg.host, port=cfg.port, username=cfg.username,
                               pkey=self.key, allow_agent=False, look_for_keys=False,
                               timeout=cfg.connect_timeout_seconds, banner_timeout=cfg.connect_timeout_seconds,
                               auth_timeout=cfg.connect_timeout_seconds, channel_timeout=cfg.connect_timeout_seconds)
                transport = client.get_transport()
                transport.set_keepalive(cfg.keepalive_seconds)
                self._open_sftp(client)
                self.sftp.get_channel().settimeout(cfg.read_timeout_seconds)
                key = transport.get_remote_server_key()
                fingerprint = "SHA256:" + base64.b64encode(hashlib.sha256(key.asbytes()).digest()).decode().rstrip("=")
                return fingerprint
            except (p.AuthenticationException, p.BadHostKeyException) as exc:
                self.close()
                raise FatalError(f"SSH authentication or host-key verification failed: {exc}") from exc
            except (OSError, EOFError, p.SSHException) as exc:
                self.close()
                if attempt == cfg.max_attempts:
                    raise FatalError(f"SFTP connection failed after {attempt} attempts: {exc}") from exc
                if self.retry is not None:
                    self.retry(exc)
                self.sleep(attempt)
            except BaseException:
                self.close()
                raise

    def _open_sftp(self, client):
        # Paramiko does not bound subsystem acknowledgement or SFTP negotiation.
        # Channel read timeouts can only be set after open_sftp() returns.
        expired = threading.Event()

        def expire():
            expired.set()
            client.close()

        timer = threading.Timer(self.cfg.read_timeout_seconds, expire)
        timer.daemon = True
        timer.start()
        try:
            try:
                self.sftp = client.open_sftp()
            finally:
                timer.cancel()
                # A callback already in flight must finish before we reuse state.
                timer.join()
        except (OSError, EOFError, self.p.SSHException) as exc:
            if expired.is_set():
                raise TimeoutError("SFTP session setup timed out") from exc
            raise
        if expired.is_set():
            raise TimeoutError("SFTP session setup timed out")

    def is_connection_error(self, exc):
        return (isinstance(exc, (EOFError, ConnectionError, TimeoutError, self.p.SSHException))
                or isinstance(exc, OSError) and exc.errno in {errno.EPIPE, errno.ECONNRESET, errno.ENOTCONN, errno.ETIMEDOUT}
                or self.client is not None and (self.client.get_transport() is None or not self.client.get_transport().is_active()))

    def reconnect(self, exc):
        if self.is_connection_error(exc):
            self.reconnections += 1
            if self.reconnections > self.cfg.max_attempts:
                raise FatalError("Run aborted: SFTP reconnection budget exhausted") from exc
            self.connect()

    def lstat(self, relative):
        try:
            return self.sftp.lstat(self.absolute(relative))
        except UnicodeError as exc:
            raise MetadataError("SFTP metadata response contains text that is not valid UTF-8") from exc
        except self.p.SSHException as exc:
            raise ConnectionError(str(exc)) from exc

    def iterdir(self, relative):
        from paramiko.sftp import (
            CMD_CLOSE,
            CMD_HANDLE,
            CMD_NAME,
            CMD_OPENDIR,
            CMD_READDIR,
        )

        # Paramiko's listdir_iter closes its handle only at EOF. Own the handle
        # here so decode failures, READDIR failures and early exits also close it.
        # One synchronous READDIR at a time keeps listing memory bounded.
        sftp = self.sftp
        try:
            response, message = sftp._request(CMD_OPENDIR, self.absolute(relative))
            if response != CMD_HANDLE:
                raise MetadataError("Directory listing did not return an SFTP handle")
            handle = message.get_string()
            try:
                while True:
                    try:
                        response, message = sftp._request(CMD_READDIR, handle)
                    except EOFError:
                        return
                    if response != CMD_NAME:
                        raise MetadataError("Directory listing did not return SFTP names")
                    for _ in range(message.get_int()):
                        name, longname = message.get_text(), message.get_text()
                        attrs = self.p.SFTPAttributes._from_msg(message, name, longname)
                        if name not in (".", ".."):
                            yield attrs
            finally:
                error = sys.exception()
                try:
                    sftp._request(CMD_CLOSE, handle)
                except (OSError, EOFError, UnicodeError, self.p.SSHException) as exc:
                    # A failed CLOSE requires reconnection, but must never
                    # replace cancellation or a process interrupt.
                    if error is None or isinstance(error, Exception):
                        raise ConnectionError("Could not close directory listing handle") from exc
        except UnicodeError as exc:
            raise MetadataError("Directory listing contains text that is not valid UTF-8") from exc
        except self.p.SSHException as exc:
            raise ConnectionError(str(exc)) from exc

    def check_directory(self, relative):
        """Confirm an ambiguous entry with OPENDIR, without reading its contents."""
        from paramiko.sftp import CMD_CLOSE, CMD_HANDLE, CMD_OPENDIR

        # Paramiko has no public OPENDIR-only API. Close the handle immediately;
        # stopping listdir_iter early would leave its directory handle open.
        try:
            response, message = self.sftp._request(CMD_OPENDIR, self.absolute(relative))
            if response != CMD_HANDLE:
                raise MetadataError("Directory check did not return an SFTP handle")
            try:
                self.sftp._request(CMD_CLOSE, message.get_string())
            except (OSError, UnicodeError) as exc:
                # OPENDIR already confirmed a directory. Reconnect to release
                # its handle; this is not a negative directory-probe response.
                raise ConnectionError("Could not close confirmed directory handle") from exc
        except UnicodeError as exc:
            raise MetadataError("SFTP directory response contains text that is not valid UTF-8") from exc
        except self.p.SSHException as exc:
            raise ConnectionError(str(exc)) from exc

    @contextmanager
    def open(self, relative):
        try:
            stream = self.sftp.open(self.absolute(relative), "rb", bufsize=0)
            try:
                yield stream
            finally:
                error = sys.exception()
                try:
                    stream.close()
                except (OSError, EOFError, UnicodeError, self.p.SSHException) as exc:
                    # An unacknowledged close requires a fresh session, but
                    # cleanup must preserve an interrupt from the caller.
                    if error is None or isinstance(error, Exception):
                        raise ConnectionError("Could not close remote file handle") from exc
        except UnicodeError as exc:
            raise MetadataError("SFTP file response contains text that is not valid UTF-8") from exc
        except self.p.SSHException as exc:
            raise ConnectionError(str(exc)) from exc

    def close(self):
        try:
            if self.sftp is not None:
                self.sftp.close()
        finally:
            if self.client is not None:
                self.client.close()
            self.client = self.sftp = None
