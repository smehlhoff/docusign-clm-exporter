# Docusign CLM PDF exporter

Download CLM PDFs over SFTP, preserving folders and download history across
batches. Requires macOS or Linux, Python 3.12+, and
[uv](https://docs.astral.sh/uv/getting-started/installation/).

`inventory`, `download`, and `reconcile` share the same recursive scan. PDFs are
selected by a case-insensitive `.pdf` suffix and require valid size and modification
time. PDFs reuse listing metadata, requesting `lstat` only when size or modification
time is missing; ambiguous types do not require directory probing. For other entries
reported as `special` or missing a type, the scan checks `lstat`, then briefly opens
a directory handle if the type remains unclear. Unverified entries and scan failures
are reported as incomplete. Known directory symlinks are not traversed.

From this directory:

```bash
uv python install 3.12
uv sync --python 3.12
cp config.example.toml config.toml
```

Skip the copy if `config.toml` already exists. uv manages the environment and dependencies.

Set your SFTP host, username, RSA key, trusted `known_hosts` file, and remote root
in the config. Verify and add the server host key before connecting. Encrypted RSA
keys prompt for a passphrase.

Relative paths resolve from the config file's folder. The default
`working_directory = "./export"` creates downloads, state, and reports under
`export/`. Both `config.toml` and `export/` are ignored by Git.

Keep the working directory outside synced folders. Configurable defaults allow
**350 GB** of staged files, including temporary downloads, and reserve **50 GB** of
free disk space (decimal GB).

Initialize once, then download:

```bash
uv run --python 3.12 python -m clm_exporter init --config ./config.toml
uv run --python 3.12 python -m clm_exporter download --config ./config.toml
```

`init` creates the database and backup under `state/`, leaving `reports/` empty.
Repeat `download` for later batches: it skips unchanged completed files even after
local cleanup, downloads changed files, and retries unfinished work.
**Keep `state/`; never reinitialize between batches.**

Replace `download` in the command above with:

| Command | Purpose |
| --- | --- |
| `inventory` | Scan the source without downloading content |
| `status` | Verify staged files, refresh reports, and back up state offline |
| `reconcile` | Scan for remaining download gaps |

Use `--no-progress` to hide progress bars. Add
`--force-path 'Customers/Acme/Agreement.pdf'` to `download` to repeat a completed
file; other pending files are still processed.

For each batch:

1. Run `download`, review its summary, then run `status` before offloading.
2. Copy `export/CLM-export` to a new archive folder for that batch.
3. Include the latest `run-<id>-staging.csv` and `path-map.csv` from `export/reports/`.
4. Verify the archived copy and keep a separate state backup. While the exporter is
   stopped, delete the offloaded local PDFs, retaining `state/` and `reports/`.

After CLM changes stop, finish downloading and offloading, then run `reconcile`.
Archive copying and verification are manual. A capacity stop leaves work pending.

Logs, CSV manifests, errors, and JSON summaries live in `export/reports/`.
`directories_discovered` counts distinct folders seen in the scan, including empty
folders and excluding the remote root.
Exit codes: `0` success or planned capacity stop; `2` unresolved errors or
reconciliation gaps; `1` fatal error; `130` interrupted.

Run tests:

```bash
uv run --python 3.12 python -m unittest discover -v
```

Optional synthetic pilot:

```bash
uv run --python 3.12 python -m tests.synthetic_pilot --output /tmp/clm-pilot.json
```
