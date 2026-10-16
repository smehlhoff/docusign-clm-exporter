"""Compare one and two workers with artificial latency; never contacts CLM."""
import io
import json
import tempfile
import time
import tracemalloc
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from clm_exporter import cli
from clm_exporter.config import Config
from clm_exporter.local import Capacity, Reservations
from clm_exporter.parallel_transfer import ParallelTransfers
from clm_exporter.transfer import Transfers

from .fakes import FakeRemote
from .test_concurrency import Sessions


def measure(root, workers):
    cfg = Config("simulated", "fixture", root / "unused", root / "unused", "/", root / str(workers),
                 workers=workers, min_free_bytes=0, chunk_bytes=65_536)
    sessions = Sessions()
    for i in range(32):
        sessions.source.add(f"folder-{i // 4}/file-{i}.pdf", bytes([i]) * 262_144)

    def listing(client, relative):
        time.sleep(0.02)
        yield from FakeRemote.iterdir(client, relative)

    sessions.listing = listing
    sessions.reading = lambda path, offset: time.sleep(0.01)
    phases = {}
    peak = {"staged_bytes": 0, "reserved_bytes": 0, "staged_plus_reserved_bytes": 0}
    scan, sequential, parallel = cli.scan, Transfers.download, ParallelTransfers.download
    check, written, write = Capacity.check, Capacity.written, Reservations.write

    def scan_phase(*args, **kwargs):
        start = time.perf_counter()
        result = scan(*args, **kwargs)
        phases["discovery_seconds"] = time.perf_counter() - start
        return result

    def download_phase(transfers, *args):
        start = time.perf_counter()
        result = (parallel if isinstance(transfers, ParallelTransfers) else sequential)(transfers, *args)
        phases["download_seconds"] = time.perf_counter() - start
        return result

    def admission(capacity, additional):
        check(capacity, additional)
        peak["staged_plus_reserved_bytes"] = max(peak["staged_plus_reserved_bytes"], capacity.staged + additional)
        if isinstance(capacity, Reservations):
            peak["reserved_bytes"] = max(peak["reserved_bytes"], additional)

    def recorded_write(capacity, *args):
        if isinstance(capacity, Reservations):
            with capacity.lock:
                write(capacity, *args)
                peak["staged_bytes"] = max(peak["staged_bytes"], capacity.staged)
        else:
            written(capacity, *args)
            peak["staged_bytes"] = max(peak["staged_bytes"], capacity.staged)

    with redirect_stderr(io.StringIO()):
        cli.execute(cfg, "init", progress=False)
        tracemalloc.start()
        start = time.perf_counter()
        with (patch.object(cli, "scan", scan_phase),
              patch.object(Transfers, "download", download_phase),
              patch.object(ParallelTransfers, "download", download_phase),
              patch.object(Capacity, "check", admission),
              patch.object(Capacity, "written", recorded_write),
              patch.object(Reservations, "write", recorded_write)):
            code, summary = cli.execute(cfg, "download", progress=False,
                                        remote_factory=lambda cfg, state: sessions.factory(cfg, None))
        elapsed = time.perf_counter() - start
        _, memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    assert code == 0 and summary["new_successes"] == 32
    assert sessions.peak <= workers and sessions.active == 0
    return {
        "workers": workers, **phases, "total_seconds_including_reports_backup": elapsed,
        "discovery_entries_per_second": 40 / phases["discovery_seconds"],
        "download_bytes_per_second": summary["downloaded_bytes"] / phases["download_seconds"],
        "peak_connections": sessions.peak, "peak_python_allocated_bytes": memory,
        **peak, "unresolved_errors": summary["unresolved_file_errors"] + summary["unresolved_directory_errors"],
    }


def main():
    with tempfile.TemporaryDirectory() as temp:
        result = {"transport": "in-memory fixture: 20 ms/listing, 10 ms/read; not a live CLM measurement",
                  "files": 32, "bytes": 8_388_608,
                  "runs": [measure(Path(temp), workers) for workers in (1, 2)]}
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
