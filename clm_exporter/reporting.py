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
        writer.writerows(dict(row) for row in rows)
    atomic_write(path, write)


def _history_rows(
    state: State,
    latest: dict[int, Row],
    paths: dict[int, Row],
    present: set[int],
    *,
    completion_run: str | None = None,
) -> Iterable[dict[str, Any]]:
    """Use the same version and payload labels for saved and published history."""
    for transfer in state.db.execute("SELECT * FROM transfers WHERE state='downloaded' ORDER BY id"):
        if completion_run is not None and transfer["completion_run"] != completion_run:
            continue
        path = paths[transfer["path_id"]]
        if latest[path["id"]]["id"] != transfer["id"]:
            payload_status = "superseded"
        elif path["id"] in present:
            payload_status = "staged"
        else:
            payload_status = "absent_or_unverified"
        yield dict(transfer, remote_path=path["remote"], mapped_path=path["local"],
                   transfer_id=transfer["id"], payload_status=payload_status)


def finalize_results(
    state: State, run: str, scan_run: str | None = None, backup_run: str | None = None
) -> dict[str, Any]:
    """Save immutable outcomes before any report or backup publication."""
    db, cfg = state.db, state.cfg
    record = db.execute("SELECT * FROM runs WHERE id=?", (run,)).fetchone()
    existing = db.execute("SELECT summary FROM run_results WHERE run_id=?", (run,)).fetchone()
    if existing:
        return json.loads(existing[0])
    online = latest_online(state) if record["command"] == "status" else None
    if online:
        scan_run = online["id"]
    elif record["command"] in ("inventory", "download", "reconcile"):
        scan_run = run
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
    csv_scope = (online["id"] if online else run) if record["discovery_mode"] == "csv" else None
    if csv_scope:
        for name in ("documents", "absent_from_report", "import_errors"):
            locations[name] = f"{base}-{name.replace('_', '-')}.csv"
    if record["command"] == "init":
        locations = {"backup": locations["backup"]}

    def write_csv(name, fields, rows):
        if name not in locations or name in ("history", "path_map"):
            return
        save_rows(state, run, name, fields, rows)

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

    write_csv("downloads", [field for field in transfer_fields if field != "completion_run"] + ["payload_status"],
              _history_rows(state, latest, paths, present, completion_run=run))
    write_csv("history", transfer_fields + ["payload_status"], _history_rows(state, latest, paths, present))
    write_csv("path_map", ["remote_path", "mapped_path", "kind", "last_seen_run"],
             (dict(p, remote_path=p["remote"], mapped_path=p["local"]) for p in paths.values()))
    errors = [dict(e, remote_path=e["remote"], mapped_path=e["local"], attempt_count=e["attempts"])
              for e in db.execute("SELECT e.*,p.local FROM errors e LEFT JOIN paths p ON p.remote=e.remote ORDER BY e.id")]
    error_fields = ["run_id", "timestamp", "remote_path", "mapped_path", "operation", "category", "attempt_count", "message"]
    if not csv_scope:
        # A rejected force request did not create durable work. It belongs to
        # that invocation; status still assesses the latest online invocation.
        # Valid forced jobs remain in the active transfer set until completed.
        for error in errors:
            error["in_current_scope"] = error["operation"] != "force" or error["run_id"] == scan_run
        error_fields.append("in_current_scope")
    write_csv("failures", error_fields + ["resolved_at", "resolved_run"], (e for e in errors if e["run_id"] == run))
    unresolved = [e for e in errors if e["resolved_at"] is None]
    blocking_errors = [e for e in unresolved if e.get("in_current_scope", True)]
    write_csv("unresolved", error_fields, unresolved)
    observations = db.execute("SELECT * FROM observations WHERE run_id=? AND selected=1", (scan_run,)).fetchall() if scan_run and not csv_scope else []
    # Count the scan's observations, not paths retained from earlier runs.
    # The configured root is not a discovered entry and is excluded.
    directories_discovered = db.execute(
        "SELECT COUNT(*) FROM observations WHERE run_id=? AND kind='directory'", (scan_run,)
    ).fetchone()[0]
    active = {t["path_id"]: t for t in db.execute("SELECT * FROM transfers WHERE state IN ('pending','failed','ready','downloading')")}
    gaps, covered, unchanged = _download_gaps(observations, latest, paths, active, run) if not csv_scope else ([], 0, 0)
    if not csv_scope:
        write_csv("gaps", ["remote_path", "mapped_path", "size", "mtime", "state", "reason"], gaps)
    completed = list(db.execute("SELECT * FROM transfers WHERE completion_run=? AND state='downloaded'", (run,)))
    changed = sum(db.execute("SELECT EXISTS(SELECT 1 FROM transfers WHERE path_id=? AND state='downloaded' AND id<?)",
                             (t["path_id"], t["id"])).fetchone()[0] for t in completed)
    source = db.execute("SELECT * FROM source WHERE id=1").fetchone()
    source_identity = dict(zip(("host", "port", "username", "remote_root"), json.loads(source["identity"]), strict=True))
    scan_complete = bool(scan and scan["scan_status"] == "complete")
    summary = {
        "run_id": run, "command": record["command"], "discovery_mode": record["discovery_mode"],
        "started_at": record["started_at"], "ended_at": record["ended_at"],
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
        "unresolved_file_errors": sum(e["operation"] != "directory" for e in blocking_errors),
        "unresolved_directory_errors": sum(e["operation"] == "directory" for e in blocking_errors),
        "historical_unresolved_errors": len(unresolved) - len(blocking_errors),
        "historical_successful_transfers": db.execute("SELECT COUNT(*) FROM transfers WHERE state='downloaded'").fetchone()[0],
        "historically_downloaded_paths": len(latest), "currently_staged_completed_pdfs": len(staged),
        "historically_downloaded_absent_or_unverified": len(latest) - len(staged),
        "current_staged_bytes": staged_bytes, "current_free_bytes": shutil.disk_usage(cfg.export).free,
        "local_download_complete": scan_complete and not gaps and not blocking_errors and record["stop_reason"] not in ("fatal_error", "interrupted"),
        "reports": locations,
    }
    if csv_scope:
        from .csv_results import collect_csv_results
        collect_csv_results(state, run, csv_scope, summary, errors, latest, paths, active, write_csv)
        # Status reassesses current integrity. The nested original result stays
        # immutable, and rejected or interrupted input must still fail.
        if online and summary["import_status"] != "accepted":
            summary["stop_reason"] = "interrupted" if summary["import_status"] in ("importing", "interrupted") else "fatal_error"
    if online:
        # Offline status cannot turn a failed or unfinished online attempt into
        # successful reconciliation, even when a scan covered all known files.
        reason = online["stop_reason"] if online["ended_at"] else "interrupted"
        if reason in ("fatal_error", "interrupted"):
            summary["stop_reason"] = reason
            summary["local_download_complete"] = False
    summary["original_results_available"] = True
    summary["export_exit_code"] = export_outcome(record["command"], summary)
    with state.transaction():
        db.execute("UPDATE runs SET summary=? WHERE id=?", (json.dumps(summary), run))
        db.execute("INSERT INTO run_results VALUES(?,?,?)", (run, utcnow(), json.dumps(summary)))
    return summary


def save_rows(state, run, name, fields, rows):
    """Persist independently bounded batches; only run_results finalizes them."""
    with state.transaction():
        state.db.execute("DELETE FROM result_rows WHERE run_id=? AND report=?", (run, name))
        state.db.execute("INSERT OR REPLACE INTO result_reports VALUES(?,?,?)", (run, name, json.dumps(fields)))
    batch = []
    for ordinal, row in enumerate(rows):
        values = dict(row)
        batch.append((run, name, ordinal, json.dumps({field: values.get(field) for field in fields}, ensure_ascii=True)))
        if len(batch) == 500:
            with state.transaction():
                state.db.executemany("INSERT INTO result_rows VALUES(?,?,?,?)", batch)
            batch.clear()
    if batch:
        with state.transaction():
            state.db.executemany("INSERT INTO result_rows VALUES(?,?,?,?)", batch)


def saved_rows(state, run, name):
    for row in state.db.execute("SELECT data FROM result_rows WHERE run_id=? AND report=? ORDER BY ordinal", (run, name)):
        yield json.loads(row[0])


def latest_online(state):
    return state.db.execute("""SELECT * FROM runs WHERE command IN ('inventory','download','reconcile')
        ORDER BY started_at DESC LIMIT 1""").fetchone()


def status_context(state):
    def details(record):
        if record is None:
            return None
        result = {key: record[key] for key in ("id", "started_at", "command", "discovery_mode", "stop_reason")}
        imported = state.db.execute("SELECT * FROM csv_imports WHERE run_id=?", (record["id"],)).fetchone()
        if imported:
            result.update(import_status=imported["state"], input_digest=imported["digest"],
                          expected_document_count=imported["expected_count"] if imported["state"] == "accepted" else 0,
                          validation_errors=[dict(row) for row in state.db.execute(
                              "SELECT row_number,category,message FROM csv_import_errors WHERE run_id=? ORDER BY id LIMIT 20", (record["id"],))])
            result["mapping_complete"] = bool(imported["mapping_complete"])
            result["metadata_counts"] = dict(state.db.execute(
                "SELECT lookup_state,COUNT(*) FROM csv_entries WHERE run_id=? GROUP BY lookup_state", (record["id"],)))
            if imported["state"] == "importing":
                result["import_diagnostic"] = "CSV import unfinished; partial rows are not accepted input"
        saved = state.db.execute("SELECT summary FROM run_results WHERE run_id=?", (record["id"],)).fetchone()
        result["results_status"] = "finalized" if saved else "unfinished_or_legacy"
        if saved:
            original = json.loads(saved[0])
            result.update({key: original.get(key) for key in ("local_download_complete", "document_outcomes", "export_exit_code")})
        else:
            result["local_download_complete"] = None
        return result
    accepted = state.db.execute("""SELECT r.* FROM runs r JOIN csv_imports c ON c.run_id=r.id
        WHERE c.state='accepted' ORDER BY c.accepted_at DESC LIMIT 1""").fetchone()
    return {"latest_online_attempt": details(latest_online(state)), "last_accepted_csv_run": details(accepted)}


def export_outcome(command, summary):
    if summary.get("stop_reason") == "fatal_error":
        return 1
    if summary.get("stop_reason") == "interrupted":
        return 130
    if summary.get("unresolved_file_errors") or summary.get("unresolved_directory_errors"):
        return 2
    if summary.get("scan_status") in ("incomplete", "running"):
        return 2
    if summary.get("discovery_mode") == "csv" and (
        summary.get("metadata_unchecked") or summary.get("metadata_failed")
        or summary.get("ancestor_checks", {}).get("pending")
        or summary.get("ancestor_checks", {}).get("failed")
    ):
        return 2
    if command == "reconcile" and not summary.get("local_download_complete"):
        return 2
    return 0


def result_summary(state, run):
    """Never invent a historical outcome for a legacy or unfinished run."""
    saved = state.db.execute("SELECT summary FROM run_results WHERE run_id=?", (run,)).fetchone()
    if saved:
        return json.loads(saved[0])
    record = state.db.execute("SELECT * FROM runs WHERE id=?", (run,)).fetchone()
    retained = json.loads(record["summary"]) if record["summary"] else {}
    summary = {key: retained[key] for key in ("source", "destination", "host_key_fingerprint") if key in retained}
    if retained:
        summary["retained_summary"] = retained.get("retained_summary", retained)
    summary.update(run_id=run, command=record["command"], discovery_mode=record["discovery_mode"],
                   started_at=record["started_at"], ended_at=record["ended_at"], stop_reason=record["stop_reason"],
                   original_results_available=False, original_result_note="Original completion result unavailable")
    summary.update(local_download_complete=None, export_exit_code=None, reports={})
    imported = state.db.execute("SELECT * FROM csv_imports WHERE run_id=?", (run,)).fetchone()
    if imported:
        summary.update(import_status=imported["state"], input_digest=imported["digest"],
                       expected_document_count=imported["expected_count"] if imported["state"] == "accepted" else 0)
    return summary


def publication_summary(state, run, attempt):
    original = state.db.execute("SELECT * FROM publication_attempts WHERE run_id=? AND is_retry=0 ORDER BY id LIMIT 1", (run,)).fetchone()
    current = state.db.execute("SELECT * FROM publication_attempts WHERE id=?", (attempt,)).fetchone()
    return {"original_exit_code": original["exit_code"] if original else None,
            "original_publication": dict(original) if original else None,
            "publication": dict(current), "invocation_exit_code": current["exit_code"]}


# Keep the full durable result for reconstruction; publish a focused view by default.
COMPACT_FIELDS = (
    "run_id", "command", "discovery_mode", "started_at", "ended_at", "stop_reason",
    "scan_status", "import_status", "input_source", "local_download_complete", "export_exit_code",
    "invocation_exit_code", "original_results_available", "original_result_note",
    "expected_document_count", "unique_path_count", "duplicate_row_count", "document_outcomes",
    "directories_discovered", "pdfs_discovered", "latest_version_coverage", "new_successes", "changed_or_repeated_successes",
    "downloaded_bytes", "pending_deferred_count", "pending_deferred_bytes",
    "pdfs_with_unknown_size", "zero_byte_pdfs",
    "unresolved_file_errors", "unresolved_directory_errors", "historical_unresolved_errors",
    "reports",
)


def summary_view(summary, full_reports=True):
    if full_reports:
        return summary
    result = {key: summary[key] for key in COMPACT_FIELDS if key in summary}
    if "source" in summary:
        result["source"] = {"remote_root": summary["source"]["remote_root"]}
    if "current_staging" in summary:
        result["current_staging"] = summary["current_staging"]
    if "publication" in summary:
        result["publication"] = {key: summary["publication"][key] for key in
                                 ("reports_status", "backup_status", "diagnostic")}
    return result


def write_reports(state, run, scan_run=None, backup_run=None, *, verification_run=None, full_reports=True):
    """Publish saved coverage alongside a separate current staging assessment."""
    db, cfg = state.db, state.cfg
    summary = result_summary(state, run)
    summary.update(status_context(state))
    base = cfg.reports / f"run-{run}"
    critical = {"documents", "staging", "gaps", "unresolved", "import_errors", "current_integrity"}
    locations = {key: value for key, value in summary["reports"].items()
                 if full_reports or key in critical | {"summary", "log", "backup"}}
    locations["backup"] = str(cfg.backups / f"export-{backup_run or run}.sqlite3")
    summary["reports"] = locations
    if summary["command"] == "init":
        return summary
    summary["generated_at"] = utcnow()
    locations["summary"] = f"{base}-summary.json"
    latest = {r["path_id"]: r for r in db.execute("""SELECT t.* FROM transfers t WHERE t.state='downloaded'
        AND t.id=(SELECT MAX(t2.id) FROM transfers t2 WHERE t2.path_id=t.path_id AND t2.state='downloaded')""")}
    paths = {r["id"]: r for r in db.execute("SELECT * FROM paths")}
    if verification_run:
        staged, present = _verify_staging(state, verification_run, latest, paths)
        try:
            staged_bytes = footprint(cfg)
            with state.transaction():
                state.resolve(verification_run, "", ["local_layout"])
        except (OSError, PathError) as exc:
            staged_bytes = None
            state.error(verification_run, "", "local_layout", category(exc), exc)
    else:
        staged = list(saved_rows(state, run, "staging"))
        present_paths = {row["remote_path"] for row in staged}
        present = {key for key, row in paths.items() if row["remote"] in present_paths}
        staged_bytes = summary.get("current_staged_bytes")
    current_errors = [dict(row) for row in db.execute("""SELECT * FROM errors WHERE operation IN ('local','local_layout')
        AND resolved_at IS NULL ORDER BY id""")]
    summary["current_staging"] = {"verified_at": utcnow(), "completed_pdfs": len(staged),
                                  "staged_bytes": staged_bytes, "free_bytes": shutil.disk_usage(cfg.export).free,
                                  "integrity_errors": current_errors}

    def payload_status(row):
        if not row.get("sha256"):
            return "not_applicable"
        path = next_path.get(row["remote_path"])
        success = latest.get(path["id"]) if path else None
        if path and path["id"] in present:
            return "staged" if success["id"] == row["transfer_id"] else "replaced"
        if path:
            try:
                safe_target(cfg.export, path["local"]).lstat()
            except FileNotFoundError:
                return "absent"
            except (OSError, PathError):
                pass
        return "unverified"
    next_path = {p["remote"]: p for p in paths.values()}
    if summary["original_results_available"]:
        for report in db.execute("SELECT report,fields FROM result_reports WHERE run_id=? ORDER BY report", (run,)):
            name = report["report"]
            if name == "staging" or not full_reports and name not in critical:
                continue
            if not full_reports and name == "import_errors" and not db.execute(
                "SELECT 1 FROM result_rows WHERE run_id=? AND report=? LIMIT 1", (run, name)
            ).fetchone():
                locations.pop(name, None)
                continue
            def rows(name=name):
                for row in saved_rows(state, run, name):
                    if name == "documents":
                        row["local_payload_status"] = payload_status(row)
                    yield row
            locations.setdefault(name, f"{base}-{name.replace('_', '-')}.csv")
            csv_file(Path(locations[name]), json.loads(report["fields"]), rows())
    elif summary["discovery_mode"] == "csv":
        # Partial import diagnostics are reliable even though no final document
        # outcomes exist. Status never accepts or resumes this input.
        if full_reports or db.execute("SELECT 1 FROM csv_import_errors WHERE run_id=? LIMIT 1", (run,)).fetchone():
            locations["import_errors"] = f"{base}-import-errors.csv"
            csv_file(Path(locations["import_errors"]), ["row_number", "category", "message"],
                     db.execute("SELECT row_number,category,message FROM csv_import_errors WHERE run_id=? ORDER BY id", (run,)))
        if summary.get("import_status") == "accepted":
            from .csv_results import DOCUMENT_FIELDS
            locations["documents"] = f"{base}-documents.csv"
            csv_file(Path(locations["documents"]), DOCUMENT_FIELDS + ["result_status"], db.execute("""SELECT e.*,
                p.local AS mapped_path,o.size,o.mtime,'Original completion result unavailable' AS result_status
                FROM csv_entries e LEFT JOIN paths p ON p.id=e.path_id LEFT JOIN observations o
                ON o.run_id=e.run_id AND o.path_id=e.path_id WHERE e.run_id=? ORDER BY e.row_number""", (run,)))

    fields = ["remote_path", "mapped_path", "size", "mtime", "sha256", "completed_at", "completion_run", "transfer_id", "forced"]
    locations["staging"] = f"{base}-staging.csv"
    csv_file(Path(locations["staging"]), fields, staged)

    if full_reports:
        locations["history"] = f"{base}-history.csv"
        csv_file(Path(locations["history"]), fields + ["payload_status"], _history_rows(state, latest, paths, present))
        if not summary["original_results_available"]:
            locations["downloads"] = f"{base}-downloads.csv"
            csv_file(Path(locations["downloads"]), fields + ["payload_status"],
                     (row for row in _history_rows(state, latest, paths, present) if row["completion_run"] == run))
        locations["path_map"] = str(cfg.reports / "path-map.csv")
        csv_file(Path(locations["path_map"]), ["remote_path", "mapped_path", "kind", "last_seen_run"],
                 (dict(p, remote_path=p["remote"], mapped_path=p["local"]) for p in paths.values()))
    if full_reports or current_errors:
        locations["current_integrity"] = f"{base}-current-integrity.csv"
        csv_file(Path(locations["current_integrity"]), ["run_id", "timestamp", "remote", "operation", "category", "message"], current_errors)
    atomic_write(Path(locations["summary"]), lambda f: json.dump(summary_view(summary, full_reports), f, indent=2))
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
        if success is not None and matches:
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
