import argparse
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

from .config import load
from .discovery import scan
from .errors import CapacityStop, FatalError, PathError
from .local import prepare_layout, probe_filesystem, relative_parts, working_lock
from .progress import Progress, ProgressHandler
from .remote import Remote
from .reporting import (
    atomic_write,
    finalize_results,
    latest_online,
    publication_summary,
    result_summary,
    status_context,
    write_reports,
)
from .state import State, utcnow
from .transfer import Transfers

LOG = logging.getLogger("clm_exporter")


def parser():
    result = argparse.ArgumentParser(description="Export CLM SFTP PDFs with persistent download history.")
    commands = result.add_subparsers(dest="command", required=True)
    for command in ("init", "inventory", "download", "status", "reconcile"):
        sub = commands.add_parser(command)
        sub.add_argument("--config", required=True, type=Path)
        sub.add_argument("--no-progress", action="store_true", help="Disable terminal progress bars (logs remain enabled)")
        if command in ("inventory", "download", "reconcile"):
            sub.add_argument("--csv", type=Path, help="Complete CSV document list; paths resolve from the invocation directory")
        if command == "download":
            sub.add_argument("--force-path", help="Exact case-sensitive relative remote path to explicitly download again")
        if command == "status":
            sub.add_argument("--run-id", help="Reconstruct reports for this existing run using current local payloads")
    return result


def execute(cfg, command, force_path=None, report_run=None, remote_factory=Remote, free=None, sleep=None, checkpoint=None, progress=True, csv_path=None):
    """CLI engine; transport and disk readings are injectable for acceptance tests."""
    if force_path is not None:
        relative_parts(force_path)
    if csv_path is not None and command not in ("inventory", "download", "reconcile"):
        raise FatalError("CSV input is supported only by inventory, download, and reconcile")
    display = Progress(enabled=progress and command in ("inventory", "download", "reconcile"))
    with working_lock(cfg):
        if command != "init" and not cfg.database.exists():
            raise FatalError("State database is missing; init is required only for a new export")
        prepare_layout(cfg)
        state = State(cfg, initialize=command == "init", filesystem=probe_filesystem(cfg) if command == "init" else None)
        remote = None
        handlers = []
        try:
            if report_run:
                previous = state.db.execute("SELECT * FROM runs WHERE id=?", (report_run,)).fetchone()
                if previous is None:
                    raise FatalError(f"Unknown run id: {report_run}")
                backup_run = report_run + "-reconstructed-" + str(time.time_ns())
                verification_run = state.run("status", previous["discovery_mode"])
                code, summary = publish(state, report_run, backup_run=backup_run, verification_run=verification_run)
                with state.transaction():
                    state.db.execute("UPDATE runs SET ended_at=?,stop_reason=? WHERE id=?",
                                     (utcnow(), "completed" if code == 0 else "publication_or_export_error", verification_run))
                return code, summary
            online = latest_online(state) if command == "status" else None
            mode = "csv" if csv_path is not None else online["discovery_mode"] if online else "recursive"
            run = state.run(command, mode)
            if command != "init":
                handlers.append(logging.FileHandler(cfg.reports / f"run-{run}.log", encoding="utf-8"))
            handlers.append(ProgressHandler(display))
            formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
            formatter.converter = time.gmtime
            for handler in handlers:
                handler.setFormatter(formatter)
                if command == "inventory":
                    handler.addFilter(lambda record: record.levelno >= logging.WARNING
                                      or record.getMessage().startswith("Discovery"))
                LOG.addHandler(handler)
            LOG.setLevel(logging.INFO)
            LOG.propagate = False
            reason, code = "completed", None
            try:
                if command in ("inventory", "download", "reconcile"):
                    if csv_path is not None:
                        from .csv_input import import_csv
                        import_csv(state, run, csv_path, display, checkpoint)
                    from .mapping import reserve_csv_mappings
                    reserve_csv_mappings(state, checkpoint)
                    options = {"free": free, "checkpoint": checkpoint, "progress": display}
                    if sleep:
                        options["sleep"] = sleep
                    if cfg.workers > 1:
                        from .parallel_transfer import ParallelTransfers
                        from .workers import Workers
                        transfers = ParallelTransfers(state, run, **options)
                        transfers.recover()
                        remote = Workers(cfg, state, remote_factory)
                    else:
                        transfers = Transfers(state, run, **options)
                        transfers.recover()
                        remote = remote_factory(cfg, state)
                        fingerprint = remote.connect()
                        if fingerprint:
                            with state.transaction():
                                state.db.execute("UPDATE source SET host_key_fingerprint=? WHERE id=1", (fingerprint,))
                    LOG.info("Discovery configured workers=%d", cfg.workers)
                    if csv_path is not None:
                        from .csv_discovery import scan_csv
                        scan_csv(state, remote, run, progress=display, **({"sleep": sleep} if sleep else {}))
                    else:
                        scan(state, remote, run, progress=display, **({"sleep": sleep} if sleep else {}))
                    if command == "download":
                        transfers.remote = remote
                        transfers.download(force_path)
                if command != "init":
                    LOG.info("Command finished: %s", command)
            except CapacityStop as exc:
                reason = exc.reason
                LOG.info("Planned stop: %s", reason)
            except KeyboardInterrupt:
                reason, code = "interrupted", 130
                LOG.warning("Interrupted; completed files retained")
            except (FatalError, sqlite3.Error, OSError, PathError) as exc:
                reason, code = "fatal_error", 1
                LOG.error("Fatal error: %s", exc)
            finally:
                if remote is not None:
                    try:
                        remote.close()
                    except Exception as exc:
                        LOG.warning("Connection close failed: %s", exc, exc_info=True)
                with state.transaction():
                    state.db.execute("UPDATE runs SET ended_at=?,stop_reason=?,scan_status=CASE WHEN scan_status='running' THEN 'incomplete' ELSE scan_status END WHERE id=?",
                                     (utcnow(), reason, run))
            try:
                finalize_results(state, run)
            except (OSError, sqlite3.Error, PathError, KeyboardInterrupt) as exc:
                code = 130 if isinstance(exc, KeyboardInterrupt) else 1
                LOG.error("Could not finalize export results: %s", exc)
            return publish(state, run, exit_override=code)
        finally:
            for handler in handlers:
                LOG.removeHandler(handler)
                handler.close()
            state.close()


def publish(state, run, *, backup_run=None, verification_run=None, exit_override=None):
    """Report and backup failures never rewrite a finalized export result."""
    db = state.db
    with state.transaction():
        attempt = db.execute("INSERT INTO publication_attempts(run_id,started_at,is_retry) VALUES(?,?,?)",
                             (run, utcnow(), int(verification_run is not None))).lastrowid
    summary = result_summary(state, run)
    summary.update(status_context(state))
    code = exit_override if exit_override is not None else summary.get("export_exit_code")
    if code is None:
        code = 130 if summary.get("import_status") in ("importing", "interrupted") else 1 if summary.get("import_status") == "rejected" else 0
    diagnostics = []

    def failure(stage, exc):
        nonlocal code
        interrupted = isinstance(exc, KeyboardInterrupt)
        code = 130 if interrupted or code == 130 else 1
        message = f"{stage}: {str(exc) or 'interrupted'}"
        diagnostics.append(message)
        LOG.error("Export results retained, but publication failed: %s", message)
        with state.transaction():
            db.execute(f"UPDATE publication_attempts SET {stage}_status=?,diagnostic=? WHERE id=?",
                       ("interrupted" if interrupted else "failed", "; ".join(diagnostics), attempt))

    try:
        summary = write_reports(state, run, backup_run=backup_run, verification_run=verification_run)
        with state.transaction():
            db.execute("UPDATE publication_attempts SET reports_status='complete' WHERE id=?", (attempt,))
    except (OSError, sqlite3.Error, PathError, KeyboardInterrupt) as exc:
        failure("reports", exc)
    summary.setdefault("reports", {})["backup"] = str(state.cfg.backups / f"export-{backup_run or run}.sqlite3")
    # The backup contains a truthful pending publication record. Success is only
    # committed to the live database after the backup has reached durable storage.
    summary.update(publication_summary(state, run, attempt))
    with state.transaction():
        db.execute("UPDATE runs SET summary=? WHERE id=?", (json.dumps(summary), run))
    try:
        state.backup(backup_run or run)
        with state.transaction():
            db.execute("UPDATE publication_attempts SET backup_status='complete' WHERE id=?", (attempt,))
    except (OSError, sqlite3.Error, PathError, KeyboardInterrupt) as exc:
        failure("backup", exc)
    summary.update(publication_summary(state, run, attempt))
    ended_at = utcnow()

    def finish_view():
        summary["publication"].update(ended_at=ended_at, exit_code=code)
        summary["invocation_exit_code"] = code
        if summary["original_publication"] and summary["original_publication"]["id"] == attempt:
            summary["original_publication"].update(ended_at=ended_at, exit_code=code)
            summary["original_exit_code"] = code
    finish_view()
    if summary["publication"]["reports_status"] == "complete" and "summary" in summary["reports"]:
        try:
            atomic_write(Path(summary["reports"]["summary"]), lambda stream: json.dump(summary, stream, indent=2))
        except (OSError, PathError, KeyboardInterrupt) as exc:
            failure("reports", exc)
            summary.update(publication_summary(state, run, attempt))
            finish_view()
    with state.transaction():
        db.execute("UPDATE publication_attempts SET ended_at=?,exit_code=? WHERE id=?", (ended_at, code, attempt))
        db.execute("UPDATE runs SET summary=? WHERE id=?", (json.dumps(summary), run))
    return code, summary


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        cfg = load(args.config)
        code, summary = execute(cfg, args.command, getattr(args, "force_path", None), getattr(args, "run_id", None),
                                progress=not args.no_progress, csv_path=getattr(args, "csv", None))
        if args.command == "init":
            if code == 0:
                print("Initialized.")
            else:
                print("Initialization interrupted." if code == 130 else "Initialization failed.", file=sys.stderr)
        else:
            print(json.dumps(summary, indent=2))
        return code
    except KeyboardInterrupt:
        return 130
    except (FatalError, PathError, OSError, sqlite3.Error) as exc:
        print(f"clm-exporter: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
