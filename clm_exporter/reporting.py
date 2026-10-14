import csv
import json
import logging
import os
import shutil
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from sqlite3 import Row
from typing import Any, TextIO

from .errors import PathError, category
from .local import digest_file, footprint, safe_target, sync_directory
from .state import State, utcnow


def atomic_write(path: Path, writer: Callable[[TextIO], None]) -> None:
    temp = path.with_name(f".{path.name}-{uuid.uuid4().hex}.tmp")
    try:
        with temp.open("x", encoding="utf-8", newline="") as stream:
            writer(stream)
            stream.flush()
            os.fsync(stream.fileno())
        if path.is_symlink():
            raise PathError(f"Report destination is a symlink: {path}")
        os.replace(temp, path)
        sync_directory(path.parent)
    finally:
        temp.unlink(missing_ok=True)


def csv_file(
    path: Path, fields: Sequence[str], rows: Iterable[Mapping[str, Any]]
) -> None:
    def write(stream: TextIO) -> None:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    atomic_write(path, write)


def write_reports(
    state: State, run: str, scan_run: str | None = None, backup_run: str | None = None
) -> dict[str, Any]:
    """Verify current staging and persist reports for a recorded run and scan."""
    db, cfg = state.db, state.cfg
    record = db.execute("SELECT * FROM runs WHERE id=?", (run,)).fetchone()
    if scan_run is None:
        previous = db.execute("""SELECT id FROM runs WHERE scan_started_at IS NOT NULL
            AND started_at<=? ORDER BY started_at DESC LIMIT 1""", (record["started_at"],)).fetchone()
        scan_run = previous[0] if previous else None
    scan = db.execute("SELECT * FROM runs WHERE id=?", (scan_run,)).fetchone() if scan_run else None
    base = cfg.reports / f"run-{run}"
    locations = {name: f"{base}-{name}.{ext}" for name, ext in (
        ("failures", "csv"), ("unresolved", "csv"), ("downloads", "csv"), ("staging", "csv"),
        ("history", "csv"), ("gaps", "csv"), ("summary", "json"))}
    locations["path_map"] = str(cfg.reports / "path-map.csv")
    locations["log"] = f"{base}.log"
    locations["backup"] = str(cfg.backups / f"export-{backup_run or run}.sqlite3")
    if record["command"] == "init":
        locations = {"backup": locations["backup"]}

    def write_csv(name, fields, rows):
        if name in locations:
            csv_file(Path(locations[name]), fields, rows)

    latest = {r["path_id"]: r for r in db.execute("""SELECT t.* FROM transfers t
        WHERE t.state='downloaded' AND t.id=(SELECT MAX(t2.id) FROM transfers t2 WHERE t2.path_id=t.path_id AND t2.state='downloaded')""")}
    paths = {r["id"]: r for r in db.execute("SELECT * FROM paths")}
    try:
        staged_bytes = footprint(cfg)
        with state.transaction():
            state.resolve(run, "", ["local_layout"])
    except (OSError, PathError) as exc:
        staged_bytes = None
        state.error(run, "", "local_layout", category(exc), exc)
    staged, present = _verify_staging(state, run, latest, paths)
    transfer_fields = ["remote_path", "mapped_path", "size", "mtime", "sha256", "completed_at", "completion_run", "transfer_id", "forced"]
    write_csv("staging", transfer_fields, staged)

    def history_rows(only_run=False):
        for transfer in db.execute("SELECT * FROM transfers WHERE state='downloaded' ORDER BY id"):
            if only_run and transfer["completion_run"] != run:
                continue
            path = paths[transfer["path_id"]]
            is_latest = latest[path["id"]]["id"] == transfer["id"]
            yield dict(transfer, remote_path=path["remote"], mapped_path=path["local"], transfer_id=transfer["id"],
                       payload_status="superseded" if not is_latest else "staged" if path["id"] in present else "absent_or_unverified")
    write_csv("downloads", [field for field in transfer_fields if field != "completion_run"] + ["payload_status"], history_rows(True))
    write_csv("history", transfer_fields + ["payload_status"], history_rows())
    write_csv("path_map", ["remote_path", "mapped_path", "kind", "last_seen_run"],
             (dict(p, remote_path=p["remote"], mapped_path=p["local"]) for p in paths.values()))
    errors = [dict(e, remote_path=e["remote"], mapped_path=e["local"], attempt_count=e["attempts"])
              for e in db.execute("SELECT e.*,p.local FROM errors e LEFT JOIN paths p ON p.remote=e.remote ORDER BY e.id")]
    error_fields = ["run_id", "timestamp", "remote_path", "mapped_path", "operation", "category", "attempt_count", "message"]
    write_csv("failures", error_fields + ["resolved_at", "resolved_run"], (e for e in errors if e["run_id"] == run))
    unresolved = [e for e in errors if e["resolved_at"] is None]
    write_csv("unresolved", error_fields, unresolved)
    observations = db.execute("SELECT * FROM observations WHERE run_id=? AND selected=1", (scan_run,)).fetchall() if scan_run else []
    # Count the scan's observations, not paths retained from earlier runs.
    # The configured root is not a discovered entry and is excluded.
    directories_discovered = db.execute(
        "SELECT COUNT(*) FROM observations WHERE run_id=? AND kind='directory'", (scan_run,)
    ).fetchone()[0]
    active = {t["path_id"]: t for t in db.execute("SELECT * FROM transfers WHERE state IN ('pending','failed','ready','downloading')")}
    gaps, covered, unchanged = _download_gaps(observations, latest, paths, active, run)
    write_csv("gaps", ["remote_path", "mapped_path", "size", "mtime", "state", "reason"], gaps)
    completed = list(db.execute("SELECT * FROM transfers WHERE completion_run=? AND state='downloaded'", (run,)))
    changed = sum(db.execute("SELECT EXISTS(SELECT 1 FROM transfers WHERE path_id=? AND state='downloaded' AND id<?)",
                             (t["path_id"], t["id"])).fetchone()[0] for t in completed)
    source = db.execute("SELECT * FROM source WHERE id=1").fetchone()
    source_identity = dict(zip(("host", "port", "username", "remote_root"), json.loads(source["identity"]), strict=True))
    scan_complete = bool(scan and scan["scan_status"] == "complete")
    summary = {
        "run_id": run, "command": record["command"], "started_at": record["started_at"], "ended_at": record["ended_at"],
        "generated_at": utcnow(), "stop_reason": record["stop_reason"], "source": source_identity,
        "destination": str(cfg.export), "host_key_fingerprint": source["host_key_fingerprint"],
        "scan_run": scan_run, "scan_status": scan["scan_status"] if scan else "not_started",
        "scan_started_at": scan["scan_started_at"] if scan else None, "scan_ended_at": scan["scan_ended_at"] if scan else None,
        "directories_discovered": directories_discovered,
        "pdfs_discovered": len(observations), "bytes_discovered": sum(p["size"] or 0 for p in observations),
        "pdfs_with_unknown_size": sum(p["size"] is None for p in observations), "unchanged_skips": unchanged,
        "new_successes": len(completed) - changed, "changed_or_repeated_successes": changed,
        "forced_successes": sum(t["forced"] for t in completed), "downloaded_bytes": sum(t["size"] for t in completed),
        "zero_byte_pdfs": sum(p["size"] == 0 for p in observations), "latest_version_coverage": covered,
        "pending_deferred_count": len(gaps), "pending_deferred_bytes": sum(p["size"] or 0 for p in gaps),
        "unresolved_file_errors": sum(e["operation"] != "directory" for e in unresolved),
        "unresolved_directory_errors": sum(e["operation"] == "directory" for e in unresolved),
        "historical_successful_transfers": db.execute("SELECT COUNT(*) FROM transfers WHERE state='downloaded'").fetchone()[0],
        "historically_downloaded_paths": len(latest), "currently_staged_completed_pdfs": len(staged),
        "historically_downloaded_absent_or_unverified": len(latest) - len(staged),
        "current_staged_bytes": staged_bytes, "current_free_bytes": shutil.disk_usage(cfg.export).free,
        "local_download_complete": scan_complete and not gaps and not unresolved and record["stop_reason"] not in ("fatal_error", "interrupted"),
        "reports": locations,
    }
    # Persist summary before file output so a report-write interruption is reconstructable.
    with state.transaction():
        db.execute("UPDATE runs SET summary=? WHERE id=?", (json.dumps(summary), run))
    if "summary" in locations:
        atomic_write(Path(locations["summary"]), lambda f: json.dump(summary, f, indent=2, ensure_ascii=True))
    return summary


def _verify_staging(
    state: State, run: str, latest: dict[int, Row], paths: dict[int, Row]
) -> tuple[list[dict[str, Any]], set[int]]:
    """Verify latest payloads, recording conflicts and resolving old local errors."""
    cfg = state.cfg
    staged, present = [], set()
    progress_at = time.monotonic()
    for path_id, success in latest.items():
        path = paths[path_id]
        try:
            target = safe_target(cfg.export, path["local"])
            actual = digest_file(target)
            if actual != (success["size"], success["sha256"]):
                raise PathError("Staged payload does not match its latest successful size and SHA-256")
        except FileNotFoundError:
            with state.transaction():
                state.resolve(run, path["remote"], ["local"])
            continue
        except (OSError, PathError) as exc:
            state.error(run, path["remote"], "local", category(exc), exc)
            continue
        with state.transaction():
            state.resolve(run, path["remote"], ["local"])
        present.add(path_id)
        staged.append({"remote_path": path["remote"], "mapped_path": path["local"], "size": success["size"],
                       "mtime": success["mtime"], "sha256": success["sha256"], "completed_at": success["completed_at"],
                       "completion_run": success["completion_run"], "transfer_id": success["id"],
                       "forced": success["forced"]})
        if time.monotonic() - progress_at >= 5:
            logging.getLogger("clm_exporter").info("Staging manifest: verified %d present PDFs", len(staged))
            progress_at = time.monotonic()
    return staged, present


def _download_gaps(
    observations: Sequence[Row],
    latest: dict[int, Row],
    paths: dict[int, Row],
    active: dict[int, Row],
    run: str,
) -> tuple[list[dict[str, Any]], int, int]:
    """Return gaps, covered versions, and unchanged skips in input order.

    A matching successful version counts as covered even with a pending repeat.
    Active work absent from the scan follows the observed gaps.
    """
    gaps, covered, unchanged = [], 0, 0
    for observation in observations:
        path_id = observation["path_id"]
        success = latest.get(path_id)
        matches = success and (success["size"], success["mtime"]) == (
            observation["size"], observation["mtime"]
        )
        if matches:
            covered += 1
            unchanged += success["completion_run"] != run
        if not matches or path_id in active:
            path = paths[path_id]
            gaps.append({
                "remote_path": path["remote"], "mapped_path": path["local"],
                "size": observation["size"], "mtime": observation["mtime"],
                "state": active[path_id]["state"] if path_id in active else "unresolved",
                "reason": "explicit_repeat_pending" if matches else "latest_success_does_not_match",
            })
    observed_ids = {observation["path_id"] for observation in observations}
    for path_id, transfer in active.items():
        if path_id not in observed_ids:
            path = paths[path_id]
            gaps.append({"remote_path": path["remote"], "mapped_path": path["local"],
                         "size": transfer["size"], "mtime": transfer["mtime"], "state": transfer["state"],
                         "reason": "pending_path_not_observed"})
    return gaps, covered, unchanged
