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
from .reporting import write_reports
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
        if command == "download":
            sub.add_argument("--force-path", help="Exact case-sensitive relative remote path to explicitly download again")
        if command == "status":
            sub.add_argument("--run-id", help="Reconstruct reports for this existing run using current local payloads")
    return result


def execute(cfg, command, force_path=None, report_run=None, remote_factory=Remote, free=None, sleep=None, checkpoint=None, progress=True):
    """CLI engine; transport and disk readings are injectable for acceptance tests."""
    if force_path is not None:
        relative_parts(force_path)
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
                try:
                    summary = write_reports(state, report_run, scan_run=report_run if previous["scan_started_at"] else None,
                                            backup_run=backup_run)
                finally:
                    state.backup(backup_run)
                return outcome(previous["command"], summary), summary
            run = state.run(command)
            file_handler = logging.FileHandler(cfg.reports / f"run-{run}.log", encoding="utf-8")
            stream_handler = ProgressHandler(display)
            formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
            formatter.converter = time.gmtime
            for handler in (file_handler, stream_handler):
                handler.setFormatter(formatter)
                if command == "inventory":
                    handler.addFilter(lambda record: record.levelno >= logging.WARNING
                                      or record.getMessage().startswith("Discovery"))
                LOG.addHandler(handler)
                handlers.append(handler)
            LOG.setLevel(logging.INFO)
            LOG.propagate = False
            reason, code = "completed", None
            try:
                if command in ("inventory", "download", "reconcile"):
                    options = {"free": free, "checkpoint": checkpoint, "progress": display}
                    if sleep:
                        options["sleep"] = sleep
                    transfers = Transfers(state, run, **options)
                    transfers.recover()
                    remote = remote_factory(cfg, state)
                    remote.connect()
                    scan(state, remote, run, progress=display, **({"sleep": sleep} if sleep else {}))
                    if command == "download":
                        transfers.remote = remote
                        transfers.download(force_path)
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
                summary = write_reports(state, run)
            finally:
                # Consistent database copy even when report generation fails.
                state.backup(run)
            return code if code is not None else outcome(command, summary), summary
        finally:
            for handler in handlers:
                LOG.removeHandler(handler)
                handler.close()
            state.close()


def outcome(command, summary):
    if summary["stop_reason"] == "fatal_error":
        return 1
    if summary["stop_reason"] == "interrupted":
        return 130
    if summary["unresolved_file_errors"] or summary["unresolved_directory_errors"]:
        return 2
    if summary["scan_status"] in ("incomplete", "running"):
        return 2
    if command == "reconcile" and not summary["local_download_complete"]:
        return 2
    return 0


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        cfg = load(args.config)
        code, summary = execute(cfg, args.command, getattr(args, "force_path", None), getattr(args, "run_id", None),
                                progress=not args.no_progress)
        print(json.dumps(summary, indent=2))
        return code
    except KeyboardInterrupt:
        return 130
    except (FatalError, PathError, OSError, sqlite3.Error) as exc:
        print(f"clm-exporter: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
