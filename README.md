# Docusign CLM PDF exporter

Download PDFs from CLM over SFTP, preserving folders and download history across
download batches. Requires macOS or Linux and [uv](https://docs.astral.sh/uv/getting-started/installation/).
The commands below use uv to install and run Python 3.12.

From this directory:

```bash
uv python install 3.12
uv sync --python 3.12
cp config.example.toml config.toml
```

uv manages `.venv` and installs the project dependencies. No environment
activation is needed. If you already have `config.toml`, keep it and skip the copy.

Edit the config with your SFTP host, username, RSA key, trusted `known_hosts` file,
and remote root. Encrypted keys prompt for a passphrase. Add the verified server
host key to `known_hosts` before connecting.

Keep `config.toml` in the project root. Its `working_directory = "./export"`
setting puts downloads, state, and reports in the project's `export/` folder,
which is created automatically. Relative paths resolve from the config file's
folder. Both `config.toml` and `export/` are ignored by Git.

Keep the working directory outside synced folders. Defaults allow **350 GB** of
staged files, including temporary downloads, while preserving **50 GB** of free
disk space. Both limits are configurable and use decimal GB.

Initialize once, then download:

```bash
uv run --python 3.12 python -m clm_exporter init --config ./config.toml
uv run --python 3.12 python -m clm_exporter download --config ./config.toml
```

Repeat `download` for later batches. It skips unchanged completed files even after
local cleanup, downloads files whose size or modification time changed, and retries
unfinished work. Keep `state/`; never reinitialize between batches.

Use these commands with the same `--config` option:

```bash
uv run --python 3.12 python -m clm_exporter inventory --config ./config.toml
uv run --python 3.12 python -m clm_exporter status --config ./config.toml
uv run --python 3.12 python -m clm_exporter reconcile --config ./config.toml
```

| Command | Purpose |
| --- | --- |
| `inventory` | Scan the source without downloading content |
| `status` | Verify staged files, refresh reports, and back up state offline |
| `reconcile` | Scan for remaining download gaps |

Add `--no-progress` to hide progress bars. To request another copy of a completed
file, add `--force-path 'Customers/Acme/Agreement.pdf'` to `download`; other pending
files are still processed.

For each batch:

1. Run `download`, review its summary, then run `status` before offloading.
2. Copy `export/CLM-export` to your archive in a new `Batch 1`, `Batch 2`,
   etc. folder.
3. Include the latest `run-<id>-staging.csv` and `path-map.csv` from `export/reports/`.
4. Verify the archived copy and keep a separate state backup. While the exporter is
   stopped, delete the offloaded local PDFs, retaining `state/` and `reports/`.

After CLM changes stop, repeat downloading and offloading, then run `reconcile`.
Archive copying and verification are manual. A capacity stop leaves work pending;
it does not mean the export is complete.

Logs, CSV manifests, unresolved errors, and JSON summaries live in `export/reports/`.
Exit codes: `0` success or planned capacity stop; `2` unresolved errors or
reconciliation gaps; `1` fatal error; `130` interrupted.

Run tests:

```bash
uv run --python 3.12 python -m unittest discover -v
```

For the optional synthetic pilot:

```bash
uv run --python 3.12 python -m tests.synthetic_pilot --output /tmp/clm-pilot.json
```
