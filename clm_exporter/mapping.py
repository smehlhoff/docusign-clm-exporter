"""Allocate permanent component mappings, reserving natural sibling names first."""
import json
import os
from pathlib import PurePosixPath

from .errors import PathError
from .local import collision_key, component


def allocate_siblings(state, run, parent, *, csv_scope=False):
    db, cfg = state.db, state.cfg
    parent_row = db.execute("SELECT local FROM paths WHERE remote=?", (parent,)).fetchone() if parent else None
    prefix = parent_row["local"] if parent_row else ""
    if prefix is None:
        return
    if csv_scope:
        rows = db.execute("""SELECT p.* FROM paths p WHERE p.parent=? AND
            (EXISTS(SELECT 1 FROM csv_entries e WHERE e.run_id=? AND e.remote_path=p.remote)
             OR EXISTS(SELECT 1 FROM csv_ancestor_checks a WHERE a.run_id=? AND a.remote=p.remote))
            ORDER BY p.name COLLATE BINARY""", (parent, run, run)).fetchall()
        candidates = rows
    else:
        rows = db.execute("SELECT * FROM paths WHERE parent=? AND last_seen_run=? ORDER BY name COLLATE BINARY",
                          (parent, run)).fetchall()
        candidates = [r for r in rows if r["selected"] or r["kind"] == "directory"]
    reserved = {collision_key(r["name"]) for r in candidates}
    used = {collision_key(PurePosixPath(r[0]).name) for r in db.execute(
        "SELECT local FROM paths WHERE parent=? AND local IS NOT NULL", (parent,))}
    name_max = os.pathconf(cfg.export, "PC_NAME_MAX")
    failures = []
    with state.transaction():
        for row in candidates:
            if row["local"] is not None:
                continue
            try:
                name = component(row["name"])
                alias = name
                number = 0
                while collision_key(alias) in used or number and collision_key(alias) in reserved:
                    number += 1
                    if row["kind"] == "file" and name.casefold().endswith(".pdf"):
                        alias = f"{name[:-4]}-{number}{name[-4:]}"
                    else:
                        alias = f"{name}-{number}"
                if len(os.fsencode(alias)) > name_max:
                    raise PathError(f"Name exceeds filesystem component limit: {name!r}")
                mapped = f"{prefix}/{alias}" if prefix else alias
                # Do not truncate full paths; fail early when the host reports a limit.
                if len(os.fsencode(str(cfg.export / mapped))) >= os.pathconf(cfg.export, "PC_PATH_MAX"):
                    raise PathError(f"Path exceeds filesystem path limit: {mapped!r}")
                db.execute("UPDATE paths SET local=?,collision_key=? WHERE id=?",
                           (mapped, collision_key(mapped), row["id"]))
                used.add(collision_key(alias))
                state.resolve(run, row["remote"], ["mapping"])
            except (OSError, PathError) as exc:
                failures.append((row["remote"], exc))
    for remote, failure in failures:
        state.error(run, remote, "mapping", "unrepresentable_path", failure)


def reserve_csv_mappings(state, checkpoint=None):
    """Finish older accepted reservations before admitting a newer namespace."""
    db = state.db
    for imported in db.execute("""SELECT * FROM csv_imports WHERE state='accepted' AND mapping_complete=0
        ORDER BY accepted_at,run_id""").fetchall():
        run = imported["run_id"]
        # Bounded batches; only accepted input may create mapping placeholders.
        for query, kind in (
            ("SELECT remote AS remote_path FROM csv_ancestor_checks WHERE run_id=? AND remote<>'' ORDER BY depth,remote", "directory"),
            ("SELECT remote_path FROM csv_entries WHERE run_id=? ORDER BY row_number", "file"),
        ):
            cursor = db.execute(query, (run,))
            while batch := cursor.fetchmany(500):
                with state.transaction():
                    for row in batch:
                        remote = row["remote_path"]
                        parent, _, name = remote.rpartition("/")
                        db.execute("""INSERT OR IGNORE INTO paths(remote,parent,name,kind,attributes)
                            VALUES(?,?,?,?,?)""", (remote, parent, name, kind, json.dumps({"inferred_from_csv": True})))
                if checkpoint:
                    checkpoint("csv_mapping_batch", None)
        with state.transaction():
            db.execute("""UPDATE csv_entries SET path_id=(SELECT id FROM paths WHERE remote=remote_path)
                WHERE run_id=?""", (run,))
        for row in db.execute("SELECT remote FROM csv_ancestor_checks WHERE run_id=? ORDER BY depth,remote COLLATE BINARY", (run,)):
            allocate_siblings(state, run, row[0], csv_scope=True)
            if checkpoint:
                checkpoint("csv_mapping_siblings", None)
        with state.transaction():
            db.execute("UPDATE csv_imports SET mapping_complete=1 WHERE run_id=?", (run,))
