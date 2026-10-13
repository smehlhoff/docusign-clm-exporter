import io
import stat
from types import SimpleNamespace


def attrs(name="", kind="file", size=0, mtime=1):
    mode = {"file": stat.S_IFREG | 0o600, "directory": stat.S_IFDIR | 0o700,
            "symlink": stat.S_IFLNK | 0o777, "special": stat.S_IFIFO | 0o600}[kind]
    return SimpleNamespace(filename=name, st_mode=mode, st_size=size, st_mtime=mtime)


class FakeRemote:
    def __init__(self):
        self.entries = {"": {"kind": "directory", "data": b"", "mtime": 1}}
        self.opens = []
        self.reads = []
        self.denied = set()
        self.missing_attrs = set()
        self.list_missing_attrs = set()
        self.list_hook = None
        self.open_hook = None
        self.read_hook = None
        self.stat_hook = None
        self.reverse = False
        self.reconnects = 0
        self.connections = 0
        self.closed = False

    def add(self, path, data=b"pdf", mtime=1, kind="file"):
        parent = path.rpartition("/")[0]
        if parent not in self.entries:
            self.add(parent, kind="directory")
        self.entries[path] = {"kind": kind, "data": data, "mtime": mtime}

    def factory(self, cfg, state):
        return self

    def connect(self):
        self.connections += 1

    def close(self):
        self.closed = True

    def reconnect(self, exc):
        if isinstance(exc, (ConnectionError, EOFError, TimeoutError)):
            self.reconnects += 1

    def lstat(self, relative):
        if self.stat_hook:
            self.stat_hook(relative)
        if relative in self.denied:
            raise PermissionError("denied")
        if relative not in self.entries:
            raise FileNotFoundError(relative)
        e = self.entries[relative]
        result = attrs(relative.rpartition("/")[2], e["kind"], len(e["data"]), e["mtime"])
        if relative in self.missing_attrs:
            result.st_size = None
        return result

    def iterdir(self, relative):
        if relative in self.denied:
            raise PermissionError(relative)
        if self.list_hook:
            self.list_hook(relative)
        children = [p for p in self.entries if p and p.rpartition("/")[0] == relative]
        for p in sorted(children, reverse=self.reverse):
            e = self.entries[p]
            result = attrs(p.rpartition("/")[2], e["kind"], len(e["data"]), e["mtime"])
            if p in self.missing_attrs or p in self.list_missing_attrs:
                result.st_size = None
            yield result

    def open(self, relative):
        self.opens.append(relative)
        if relative in self.denied:
            raise PermissionError(relative)
        if self.open_hook:
            self.open_hook(relative)
        if relative not in self.entries:
            raise FileNotFoundError(relative)
        remote = self

        class Stream(io.BytesIO):
            def read(self, size=-1):
                if remote.read_hook:
                    remote.read_hook(relative, self.tell())
                result = super().read(size)
                remote.reads.append((relative, self.tell(), size))
                return result
        return Stream(self.entries[relative]["data"])
