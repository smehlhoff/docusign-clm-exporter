import os
import posixpath
import tomllib
from dataclasses import dataclass
from pathlib import Path

from .errors import FatalError


@dataclass(frozen=True)
class Config:
    host: str
    username: str
    private_key_path: Path
    known_hosts_path: Path
    remote_root: str
    working_directory: Path
    port: int = 22
    export_folder_name: str = "CLM-export"
    max_staged_bytes: int = 350_000_000_000
    min_free_bytes: int = 50_000_000_000
    max_attempts: int = 3
    connect_timeout_seconds: int = 30
    read_timeout_seconds: int = 60
    keepalive_seconds: int = 30
    chunk_bytes: int = 1_048_576

    def __post_init__(self):
        # Canonicalize ancestors (e.g. macOS /var -> /private/var) while leaving
        # the working directory itself and all managed children subject to lstat.
        directory = self.working_directory.expanduser().absolute()
        object.__setattr__(self, "working_directory", directory.parent.resolve() / directory.name)

    @property
    def export(self):
        return self.working_directory / self.export_folder_name

    @property
    def work(self):
        return self.working_directory / "work"

    @property
    def state_dir(self):
        return self.working_directory / "state"

    @property
    def database(self):
        return self.state_dir / "export.sqlite3"

    @property
    def reports(self):
        return self.working_directory / "reports"

    @property
    def backups(self):
        return self.state_dir / "backups"

    @property
    def identity(self):
        return [self.host.lower(), self.port, self.username, self.remote_root]


def load(path):
    try:
        path = Path(path).expanduser().absolute()
        with path.open("rb") as f:
            raw = tomllib.load(f)
        allowed = {
            "sftp": {"host", "port", "username", "private_key_path", "known_hosts_path", "remote_root"},
            "local": {"working_directory", "export_folder_name"},
            "limits": {"max_staged_bytes", "min_free_bytes"},
            "transfer": {"max_attempts", "connect_timeout_seconds", "read_timeout_seconds", "keepalive_seconds", "chunk_bytes"},
        }
        values = {}
        for section, data in raw.items():
            if section not in allowed or not isinstance(data, dict) or data.keys() - allowed[section]:
                raise ValueError(f"Unknown configuration option in [{section}]")
            values.update(data)
        for key in ("private_key_path", "known_hosts_path", "working_directory"):
            p = Path(values[key]).expanduser()
            values[key] = Path(os.path.abspath(p if p.is_absolute() else path.parent / p))
        root = values["remote_root"]
        if not isinstance(root, str) or not root.startswith("/") or "\x00" in root:
            raise ValueError("remote_root must be an absolute POSIX path")
        values["remote_root"] = "/" + posixpath.normpath(root).lstrip("/")
        cfg = Config(**values)
        for key in ("host", "username"):
            if not isinstance(getattr(cfg, key), str) or not getattr(cfg, key).strip():
                raise ValueError(f"{key} must be a nonempty string")
        name = cfg.export_folder_name
        if (not isinstance(name, str) or name in ("", ".", "..") or "/" in name
                or "\\" in name or "\x00" in name
                or name.casefold() in {"work", "state", "reports", "config.toml"}):
            raise ValueError("export_folder_name must be a dedicated single directory name")
        for key in ("port", "max_staged_bytes", "min_free_bytes", "max_attempts",
                    "connect_timeout_seconds", "read_timeout_seconds", "keepalive_seconds", "chunk_bytes"):
            val = getattr(cfg, key)
            if type(val) is not int or val < (0 if key == "min_free_bytes" else 1):
                raise ValueError(f"{key} must be a {'nonnegative' if key == 'min_free_bytes' else 'positive'} integer")
        if cfg.port > 65535 or cfg.chunk_bytes > 16 * 1024 * 1024:
            raise ValueError("port must be <=65535 and chunk_bytes <=16777216")
        return cfg
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise FatalError(f"Invalid configuration: {exc}") from exc
