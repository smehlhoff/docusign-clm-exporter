import errno


class FatalError(Exception):
    """The invocation cannot safely continue."""


class PathError(Exception):
    """An entry cannot be represented safely on the destination."""


class MetadataError(Exception):
    pass


class SourceChanged(Exception):
    pass


class CapacityStop(Exception):
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def category(exc):
    if isinstance(exc, OSError) and exc.errno == errno.ENAMETOOLONG:
        return "unrepresentable_path"
    if isinstance(exc, FileNotFoundError):
        return "source_unavailable"
    if isinstance(exc, PermissionError):
        return "permission_denied"
    if isinstance(exc, PathError):
        return "local_path_conflict"
    if isinstance(exc, MetadataError):
        return "missing_metadata"
    if isinstance(exc, SourceChanged):
        return "source_changed"
    return "io_error"
