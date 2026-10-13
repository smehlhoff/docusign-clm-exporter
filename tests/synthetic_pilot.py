"""Reproducible scale and byte-transfer simulation; does not measure CLM."""
import argparse
import io
import json
import platform
import sqlite3
import tempfile
import time
from contextlib import redirect_stderr
from dataclasses import replace
from pathlib import Path

from clm_exporter.cli import execute
from clm_exporter.config import Config
from clm_exporter.state import utcnow

from .fakes import FakeRemote


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, help="Also save the JSON results to this path")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as temporary, redirect_stderr(io.StringIO()):
        root = Path(temporary)
        cfg = Config("simulated.invalid", "fixture", root / "unused", root / "unused", "/", root / "inventory",
                     max_staged_bytes=100_000_000, min_free_bytes=0)
        fake = FakeRemote()
        for i in range(50_000):
            fake.add(f"Folder-{i // 1000:02}/File-{i:05}.pdf", b"")
        execute(cfg, "init")
        start = time.perf_counter()
        code, summary = execute(cfg, "inventory", remote_factory=fake.factory)
        scan_seconds = time.perf_counter() - start
        assert code == 0 and summary["pdfs_discovered"] == 50_000 and not fake.opens
        cfg = replace(cfg, working_directory=root / "transfer")
        fake = FakeRemote()
        for i in range(8):
            fake.add(f"Customer-{i}/Agreement.PDF", bytes([i]) * 4_000_000, mtime=i)
        execute(cfg, "init")
        start = time.perf_counter()
        code, downloaded = execute(cfg, "download", remote_factory=fake.factory)
        transfer_seconds = time.perf_counter() - start
        assert code == 0 and downloaded["downloaded_bytes"] == 32_000_000
        for path in cfg.export.rglob("*.PDF"):
            path.unlink()
        code, skipped = execute(cfg, "download", remote_factory=fake.factory)
        assert code == 0 and skipped["unchanged_skips"] == 8 and skipped["downloaded_bytes"] == 0
        result = {
            "generated_at": utcnow(), "python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
            "platform": platform.platform(), "transport": "in-memory FakeRemote; no SSH or real service",
            "inventory_entries": 50_000, "inventory_seconds_including_reports_backup": scan_seconds,
            "inventory_entries_per_second": 50_000 / scan_seconds,
            "transfer_files": 8, "transfer_bytes": 32_000_000,
            "transfer_seconds_including_scan_hash_verification_reports_backup": transfer_seconds,
            "simulated_bytes_per_second": 32_000_000 / transfer_seconds,
            "after_cleanup_unchanged_skips": skipped["unchanged_skips"],
            "after_cleanup_downloaded_bytes": skipped["downloaded_bytes"],
            "real_clm_pilot": "not run; operator requested simulated tests for now",
        }
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
