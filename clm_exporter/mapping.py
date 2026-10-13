"""Allocate permanent component mappings, reserving natural sibling names first."""
import os
from pathlib import PurePosixPath

from .errors import PathError
from .local import collision_key, component


def allocate_siblings(state, run, parent):
    db, cfg = state.db, state.cfg
    parent_row = db.execute("SELECT local FROM paths WHERE remote=?", (parent,)).fetchone() if parent else None
    prefix = parent_row["local"] if parent_row else ""
    if prefix is None:
        return
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
    for remote, exc in failures:
        state.error(run, remote, "mapping", "unrepresentable_path", exc)
