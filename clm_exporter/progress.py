"""Terminal-only tqdm display; progress never becomes part of the JSON output."""
import logging
import re
import sys
from contextlib import contextmanager

from .errors import FatalError


def load_tqdm():
    try:
        from tqdm import tqdm
    except ImportError as exc:
        raise FatalError("tqdm is missing; install requirements.txt or use --no-progress") from exc
    class SpacedTqdm(tqdm):
        @property
        def format_dict(self):
            details = super().format_dict
            # Format only the rate, preserving tqdm's scaling and slow-rate
            # inversion without changing units or text in document names.
            rate = tqdm.format_meter(**dict(details, bar_format="{rate_fmt}"))
            details["spaced_rate"] = re.sub(r"^(\s*[\d.]+|\?)(\S)", r"\1 \2", rate)
            return details

    return SpacedTqdm


class QuietBar:
    def update(self, amount=1):
        pass


class Progress:
    def __init__(self, enabled=True, stream=None):
        self.stream = stream if stream is not None else sys.stderr
        self.enabled = enabled and self.stream.isatty()
        self.tqdm = load_tqdm() if self.enabled else None

    @contextmanager
    def bar(self, **options):
        if not self.enabled:
            yield QuietBar()
            return
        # Scan totals are unknown and file totals can be deferred by capacity.
        # Only the byte bar uses tqdm's ETA, once it has measured a transfer rate.
        if options.get("unit") != "B":
            options["bar_format"] = (
                "{desc}: {n_fmt} {unit} [{elapsed}, {spaced_rate}]"
                if options.get("total") is None
                else "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}, {spaced_rate}]"
            )
        else:
            options["bar_format"] = (
                "{l_bar}{bar}| {n_fmt}/{total_fmt} "
                "[{elapsed}<{remaining}, {spaced_rate}{postfix}]"
            )
        with self.tqdm(file=self.stream, dynamic_ncols=True, mininterval=0.2,
                       miniters=1, disable=False, **options) as bar:
            yield bar


class ProgressHandler(logging.StreamHandler):
    """Write console log lines above active bars; file handlers stay unchanged."""
    def __init__(self, progress):
        super().__init__(progress.stream)
        self.progress = progress

    def emit(self, record):
        if not self.progress.enabled:
            return super().emit(record)
        try:
            self.progress.tqdm.write(self.format(record), file=self.stream)
            self.flush()
        except RecursionError:
            raise
        except Exception:  # noqa: BLE001 - Preserve logging's handleError contract.
            self.handleError(record)
