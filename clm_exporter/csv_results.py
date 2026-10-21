"""Scoped CSV reconciliation, computed once before report publication."""
import json
from collections import Counter, defaultdict

OUTCOMES = ("failed", "unchecked", "pending", "downloaded_this_run", "previously_downloaded")
DOCUMENT_FIELDS = ["row_number", "document_id", "document_name", "location_path", "remote_path", "mapped_path",
                   "lookup_state", "size", "mtime", "outcome", "transfer_id", "sha256", "completion_run",
                   "error_category", "error_message", "local_payload_status"]
ABSENCE_FIELDS = ["row_number", "document_id", "document_name", "location_path", "remote_path", "previous_run"]


def ancestors(path):
    parent = path.rpartition("/")[0]
    while True:
        yield parent
        if not parent:
            return
        parent = parent.rpartition("/")[0]


def scoped_errors(state, scope, errors):
    db = state.db
    files = {r[0] for r in db.execute("SELECT remote_path FROM csv_entries WHERE run_id=?", (scope,))}
    folders = {r[0] for r in db.execute("SELECT remote FROM csv_ancestor_checks WHERE run_id=?", (scope,))}
    by_path = defaultdict(list)
    command = []
    for error in errors:
        path, operation = error["remote"], error["operation"]
        global_error = operation == "local_layout" or operation == "force" and error["run_id"] == scope
        applies = global_error or (
            operation not in ("directory", "force", "csv_ancestor", "csv_mapping") and
            (path in files or path in folders and operation in ("mapping", "local", "recovery")))
        if operation in ("csv_ancestor", "csv_mapping"):
            applies = error["run_id"] == scope and path in folders
        error["in_current_scope"] = bool(applies)
        if error["resolved_at"] is None and applies:
            if global_error:
                command.append(error)
            if path in files or path in folders and operation != "local_layout":
                by_path[path].append(error)
    # Ancestor state and its diagnostic are durable before the separate error
    # log write. An interruption in between must not hide the known failure.
    for ancestor in db.execute("""SELECT remote,category,message,'csv_ancestor' AS operation
        FROM csv_ancestor_checks WHERE run_id=? AND lookup_state='failed'""", (scope,)):
        blocking = by_path[ancestor["remote"]]
        if not any(error["operation"] == "csv_ancestor" for error in blocking):
            blocking.append(dict(ancestor))
    return by_path, command


def collect_csv_results(state, run, scope, summary, errors, latest, paths, active, write_csv):
    db = state.db
    imported = db.execute("SELECT * FROM csv_imports WHERE run_id=?", (scope,)).fetchone()
    accepted = imported is not None and imported["state"] == "accepted"
    summary.update(discovery_mode="csv", input_digest=imported["digest"] if imported else None,
                   import_status=imported["state"] if imported else "unavailable",
                   expected_document_count=imported["expected_count"] if accepted else 0,
                   input_source=imported["source_path"] if imported else None)
    write_csv("import_errors", ["row_number", "category", "message"],
              db.execute("SELECT row_number,category,message FROM csv_import_errors WHERE run_id=? ORDER BY id", (scope,)))
    by_path, command_errors = scoped_errors(state, scope, errors) if accepted else ({}, [])
    counts = Counter({outcome: 0 for outcome in OUTCOMES})
    lookups = Counter({key: 0 for key in ("checked", "failed", "pending")})
    ancestor_counts = Counter(dict(db.execute("SELECT lookup_state,COUNT(*) FROM csv_ancestor_checks WHERE run_id=? GROUP BY lookup_state", (scope,))))
    gap_count = gap_bytes = observed_bytes = unknown_size = zero_bytes = unchanged = 0

    def documents():
        nonlocal gap_count, gap_bytes, observed_bytes, unknown_size, zero_bytes, unchanged
        if not accepted:
            return
        for entry in db.execute("""SELECT e.*,o.size,o.mtime FROM csv_entries e LEFT JOIN observations o
            ON o.run_id=e.run_id AND o.path_id=e.path_id WHERE e.run_id=? ORDER BY e.row_number""", (scope,)):
            path_id = entry["path_id"]
            path = paths.get(path_id)
            success = latest.get(path_id)
            matches = (entry["lookup_state"] == "checked" and success is not None
                       and (success["size"], success["mtime"]) == (entry["size"], entry["mtime"]))
            blocking = list(by_path.get(entry["remote_path"], ()))
            for parent in ancestors(entry["remote_path"]):
                blocking.extend(by_path.get(parent, ()))
            if blocking or entry["lookup_state"] == "failed":
                outcome = "failed"
            elif entry["lookup_state"] != "checked":
                outcome = "unchecked"
            elif not matches or path_id in active:
                outcome = "pending"
            elif success["completion_run"] == run:
                outcome = "downloaded_this_run"
            else:
                outcome = "previously_downloaded"
            covered = outcome in ("downloaded_this_run", "previously_downloaded")
            counts[outcome] += 1
            lookups[entry["lookup_state"]] += 1
            observed_bytes += entry["size"] or 0
            unknown_size += entry["size"] is None
            zero_bytes += entry["size"] == 0
            unchanged += outcome == "previously_downloaded"
            if not covered:
                gap_count += 1
                gap_bytes += entry["size"] or 0
            yield {key: entry[key] for key in ("row_number", "document_id", "document_name", "location_path", "remote_path", "lookup_state", "size", "mtime")} | {
                "mapped_path": path["local"] if path else None, "outcome": outcome,
                "transfer_id": success["id"] if covered else active[path_id]["id"] if path_id in active else None,
                "sha256": success["sha256"] if covered else None,
                "completion_run": success["completion_run"] if covered else None,
                "error_category": blocking[0]["category"] if blocking else entry["category"],
                "error_message": "; ".join(e["message"] for e in blocking) if blocking else entry["message"],
            }
    write_csv("documents", DOCUMENT_FIELDS, documents())

    def gaps():
        # Documents are saved in bounded batches by the caller. Reuse that
        # authoritative result instead of evaluating outcome precedence twice.
        for row in db.execute("SELECT data FROM result_rows WHERE run_id=? AND report='documents' ORDER BY ordinal", (run,)):
            doc = json.loads(row[0])
            if doc["outcome"] not in ("downloaded_this_run", "previously_downloaded"):
                yield dict(doc, state=doc["outcome"], reason=doc["error_message"] or doc["outcome"])
    write_csv("gaps", ["remote_path", "mapped_path", "size", "mtime", "state", "reason"], gaps())
    previous = imported["previous_run"] if accepted else None
    absence_count = 0

    def absent():
        nonlocal absence_count
        if previous:
            for row in db.execute("""SELECT e.*,? AS previous_run FROM csv_entries e WHERE e.run_id=?
                AND NOT EXISTS(SELECT 1 FROM csv_entries n WHERE n.run_id=? AND n.remote_path=e.remote_path)
                ORDER BY e.row_number""", (previous, previous, scope)):
                absence_count += 1
                yield row
    write_csv("absent_from_report", ABSENCE_FIELDS, absent())
    unresolved = [e for e in errors if e["resolved_at"] is None]
    scoped = [e for e in unresolved if e.get("in_current_scope")]
    summary.update(
        document_outcomes=dict(counts), metadata_checked=lookups["checked"], metadata_failed=lookups["failed"],
        metadata_unchecked=lookups["pending"], ancestor_checks={key: ancestor_counts[key] for key in ("checked", "failed", "pending")},
        absence_count=absence_count, previous_accepted_csv_run=previous,
        pdfs_discovered=lookups["checked"], bytes_discovered=observed_bytes, pdfs_with_unknown_size=unknown_size,
        zero_byte_pdfs=zero_bytes, unchanged_skips=unchanged,
        latest_version_coverage=counts["downloaded_this_run"] + counts["previously_downloaded"],
        pending_deferred_count=gap_count, pending_deferred_bytes=gap_bytes,
        unresolved_file_errors=sum(e["operation"] != "csv_ancestor" for e in scoped),
        unresolved_directory_errors=sum(e["operation"] == "csv_ancestor" for e in scoped),
        applicable_command_errors=len(command_errors), scoped_unresolved_errors=len(scoped),
        historical_unresolved_errors=len(unresolved) - len(scoped),
        local_download_complete=bool(accepted and summary["scan_status"] == "complete"
                                     and not gap_count and not scoped
                                     and summary["stop_reason"] not in ("fatal_error", "interrupted")),
    )
    completed = list(db.execute("""SELECT t.* FROM transfers t JOIN csv_entries e ON e.path_id=t.path_id
        WHERE e.run_id=? AND t.completion_run=? AND t.state='downloaded'""", (scope, run))) if accepted else []
    changed = sum(db.execute("SELECT EXISTS(SELECT 1 FROM transfers WHERE path_id=? AND state='downloaded' AND id<?)",
                             (t["path_id"], t["id"])).fetchone()[0] for t in completed)
    all_completed = db.execute("SELECT COUNT(*) FROM transfers WHERE completion_run=? AND state='downloaded'", (run,)).fetchone()[0]
    summary.update(new_successes=len(completed) - changed, changed_or_repeated_successes=changed,
                   forced_successes=sum(t["forced"] for t in completed), downloaded_bytes=sum(t["size"] for t in completed),
                   recovered_outside_scope=all_completed - len(completed))
    error_fields = ["run_id", "timestamp", "remote_path", "mapped_path", "operation", "category", "attempt_count", "message", "in_current_scope"]
    write_csv("unresolved", error_fields, unresolved)
