"""Local containment, locking and bounded storage accounting (POSIX/macOS)."""
import errno
import fcntl
import hashlib
import os
import shutil
import stat
import unicodedata
import uuid
from contextlib import contextmanager
from pathlib import Path

from .errors import CapacityStop, FatalError, PathError


def collision_key(name):
    return unicodedata.normalize("NFD", unicodedata.normalize("NFD", name).casefold())


def component(name):
    if not isinstance(name, str) or name in ("", ".", "..") or any(c in name for c in ("/", "\\", "\x00")):
        raise PathError(f"Unsafe path component: {name!r}")
    try:
        name.encode("utf-8")
    except UnicodeError as exc:
        raise PathError(f"Unrepresentable name: {name!r}") from exc
    return name


def relative_parts(relative):
    return [component(p) for p in relative.split("/")]


def ensure_directory(path, create=False):
    path = Path(path).absolute()
    for part in [*reversed(path.parents), path]:
        try:
            mode = part.lstat().st_mode
        except FileNotFoundError:
            if not create:
                raise
            part.mkdir(mode=0o700)
            sync_directory(part.parent)
            mode = part.lstat().st_mode
        if not stat.S_ISDIR(mode):
            raise PathError(f"Directory is a symlink or not a directory: {part}")
    return path


def safe_target(root, relative, create_parents=False):
    parts = relative_parts(relative)
    ensure_directory(root, create=create_parents)
    parent = root
    for part in parts[:-1]:
        parent /= part
        ensure_directory(parent, create=create_parents)
    target = parent / parts[-1]
    try:
        mode = target.lstat().st_mode
    except FileNotFoundError:
        return target
    if not stat.S_ISREG(mode):
        raise PathError(f"Target is not a regular file: {target}")
    return target


def regular_open(path, flags=os.O_RDONLY):
    fd = os.open(path, flags | os.O_NOFOLLOW, 0o600)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise PathError(f"Not a regular file: {path}")
    return os.fdopen(fd, "rb" if flags == os.O_RDONLY else "wb")


def digest_file(path):
    with regular_open(path) as f:
        before = os.fstat(f.fileno())
        h = hashlib.file_digest(f, "sha256").hexdigest()
        after = os.fstat(f.fileno())
    now = path.lstat()

    def signature(s):
        return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns

    if signature(before) != signature(after) or signature(after) != signature(now):
        raise PathError(f"Local file changed while hashing: {path}")
    return after.st_size, h


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        try:
            os.fsync(fd)
        except OSError as exc:
            if exc.errno not in (errno.EINVAL, errno.ENOTSUP):
                raise
    finally:
        os.close(fd)


def prepare_layout(cfg):
    for p in (cfg.working_directory, cfg.export, cfg.work, cfg.state_dir, cfg.backups, cfg.reports):
        ensure_directory(p, create=True)
    if cfg.work.stat().st_dev != cfg.export.stat().st_dev:
        raise FatalError("work and the export folder must share a filesystem")
    export = cfg.export.resolve()
    for p in (cfg.work, cfg.state_dir, cfg.backups, cfg.reports, cfg.database):
        if p.resolve().is_relative_to(export):
            raise FatalError(f"Local state may not be inside the upload folder: {p}")
    for p in (cfg.database, Path(str(cfg.database) + "-wal"), Path(str(cfg.database) + "-shm")):
        if p.is_symlink():
            raise FatalError(f"State may not be a symlink: {p}")


@contextmanager
def working_lock(cfg):
    ensure_directory(cfg.working_directory, create=True)
    ensure_directory(cfg.state_dir, create=True)
    path = cfg.state_dir / "exporter.lock"
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise FatalError("Lock is not a regular file")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise FatalError("Another exporter holds the working-directory lock") from exc
        yield
    finally:
        os.close(fd)


def footprint(cfg):
    total = 0
    for root in (cfg.export, cfg.work):
        ensure_directory(root)
        queue = [root]
        while queue:
            with os.scandir(queue.pop()) as entries:
                for entry in entries:
                    info = entry.stat(follow_symlinks=False)
                    if stat.S_ISDIR(info.st_mode):
                        queue.append(Path(entry.path))
                    elif stat.S_ISREG(info.st_mode):
                        total += info.st_size
                    else:
                        raise PathError(f"Symlink or special file in staging: {entry.path}")
    return total


def probe_filesystem(cfg):
    """Probe the actual volume without changing any payload names."""
    root = cfg.work / ("probe-" + uuid.uuid4().hex)
    root.mkdir(mode=0o700)
    try:
        (root / "Case").touch()
        (root / "\u00e9").touch()
        return {"case_insensitive": (root / "case").exists(),
                "normalization_insensitive": (root / "e\u0301").exists(),
                "name_max_bytes": os.pathconf(root, "PC_NAME_MAX")}
    finally:
        for p in root.iterdir():
            p.unlink()
        root.rmdir()


class Capacity:
    def __init__(self, cfg, free=None):
        self.cfg = cfg
        self.staged = footprint(cfg)
        self.free = free or (lambda: shutil.disk_usage(cfg.export).free)

    def check(self, additional):
        if self.staged + additional > self.cfg.max_staged_bytes:
            raise CapacityStop("batch_capacity")
        if self.free() - additional < self.cfg.min_free_bytes:
            raise CapacityStop("disk_reserve")

    def written(self, amount):
        self.staged += amount

    def removed(self, amount):
        self.staged -= amount
