"""Repeatable CSV scale simulation, with retained history and manual offloading."""
import argparse
import csv
import json
import platform
import resource
import sqlite3
import tempfile
import time
from collections import Counter
from contextlib import ExitStack
from functools import wraps
from pathlib import Path
from unittest.mock import patch

from clm_exporter.cli import execute
from clm_exporter.config import Config
from clm_exporter.state import utcnow
from clm_exporter.workers import Workers

from .test_concurrency import Sessions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=87000)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--latency", type=float, default=0.0001, help="Simulated delay per metadata request in seconds")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="clm-csv-pilot-") as directory:
        root = Path(directory)
        cfg = Config("simulated.invalid", "fixture", root / "key", root / "hosts", "/", root / "export",
                     workers=args.workers, max_staged_bytes=1000, min_free_bytes=0)
        sessions = Sessions()
        for index in range(args.rows):
            sessions.source.add(f"Folder-{index // 1000:03}/File-{index:05}.pdf", b"p")
        source = root / "input.csv"
        with source.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["DOCUMENT ID", "DOCUMENT NAME", "LOCATION PATH"])
            for index in range(args.rows):
                writer.writerow([f"ID-{index}", f"File-{index:05}.pdf", f"/Folder-{index // 1000:03}"])
        execute(cfg, "init", progress=False)
        phases = Counter()
        requests = Counter()
        stage = "other"
        max_events = 0

        def timed(name, function):
            @wraps(function)
            def measure(*values, **options):
                nonlocal stage
                previous = stage
                stage = name
                start = time.perf_counter()
                try:
                    return function(*values, **options)
                finally:
                    phases[name] += time.perf_counter() - start
                    stage = previous
            return measure

        def lookup(path):
            requests[stage] += 1
            if args.latency:
                time.sleep(args.latency)
        sessions.source.stat_hook = lookup
        sessions.listing = lambda *args: (_ for _ in ()).throw(AssertionError("CSV must not list directories"))
        put = Workers._put

        def counted_put(pool, event):
            nonlocal max_events
            put(pool, event)
            max_events = max(max_events, pool.events.qsize())

        from clm_exporter import cli, csv_discovery, csv_input, mapping
        from clm_exporter.parallel_transfer import ParallelTransfers
        from clm_exporter.state import State
        from clm_exporter.transfer import Transfers

        runs = []
        with ExitStack() as stack:
            for module, attribute, name in (
                (csv_input, "import_csv", "import"), (mapping, "reserve_csv_mappings", "mapping"),
                (csv_discovery, "scan_csv", "metadata"), (cli, "finalize_results", "result_finalization"),
                (cli, "write_reports", "reports"), (State, "backup", "backup"),
                (ParallelTransfers if args.workers > 1 else Transfers, "download", "download"),
            ):
                stack.enter_context(patch.object(module, attribute, timed(name, getattr(module, attribute))))
            stack.enter_context(patch.object(Workers, "_put", counted_put))
            for number in range(3):
                phases.clear()
                requests.clear()
                max_events = 0
                sessions.peak = 0
                connections = sessions.connections
                if number == 1:
                    sessions.source.denied = {f"Folder-{index // 1000:03}/File-{index:05}.pdf" for index in range(0, args.rows, 1000)}
                if number == 2:
                    sessions.source.denied.clear()
                    for index in range(min(100, args.rows)):
                        sessions.source.entries[f"Folder-{index // 1000:03}/File-{index:05}.pdf"]["mtime"] = 2
                start = time.perf_counter()
                code, summary = execute(cfg, "download", csv_path=source,
                                        remote_factory=sessions.factory if args.workers > 1 else sessions.source.factory,
                                        sleep=lambda _: None, progress=False)
                elapsed = time.perf_counter() - start
                assert sum(summary["document_outcomes"].values()) == args.rows
                assert summary["expected_document_count"] == args.rows
                assert sessions.peak <= args.workers
                with sqlite3.connect(cfg.database) as db:
                    assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
                    assert not db.execute("PRAGMA foreign_key_check").fetchall()
                backups = [path.stat().st_size for path in cfg.backups.glob("*.sqlite3")]
                runs.append({
                    "run": number + 1, "exit_code": code, "seconds": elapsed, "phase_seconds": dict(phases),
                    "metadata_requests_by_phase": dict(requests), "peak_connections": sessions.peak,
                    "connections_opened": sessions.connections - connections, "max_event_queue_observed": max_events,
                    "event_queue_bound": max(2, args.workers * 2),
                    "peak_process_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
                    "database_bytes": cfg.database.stat().st_size, "retained_backup_bytes": sum(backups),
                    "backup_sizes_bytes": backups, "report_bytes": sum(p.stat().st_size for p in cfg.reports.iterdir()),
                    "downloaded_bytes": summary["downloaded_bytes"], "outcomes": summary["document_outcomes"],
                    "historical_successful_transfers": summary["historical_successful_transfers"],
                    "simulated_payload_bytes_per_second": summary["downloaded_bytes"] / max(phases["download"], 0.000001),
                })
                # Operator-style offload: remove payloads, retain all state,
                # reports, snapshots and backups for the following invocation.
                for path in cfg.export.rglob("*.pdf"):
                    path.unlink()
        result = {"generated_at": utcnow(), "python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
                  "transport": "in-memory sessions; no SSH or live CLM", "rows": args.rows, "workers": args.workers,
                  "simulated_metadata_latency_seconds": args.latency, "runs": runs,
                  "live_clm_pilot": "Not run; path alignment and service concurrency require a representative live pilot."}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
