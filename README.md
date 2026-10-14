# Docusign CLM PDF exporter

Download CLM PDFs over SFTP, preserving PDF folder paths and download history across
batches. Requires macOS or Linux, Python 3.12+, and
[uv](https://docs.astral.sh/uv/getting-started/installation/).

`inventory`, `download`, and `reconcile` share the same recursive scan. PDFs are
selected by a case-insensitive `.pdf` suffix and require valid size and modification
time. During discovery, PDFs reuse listing metadata, requesting `lstat` only when
size or modification time is missing; ambiguous types do not require directory
probing. For other entries
reported as `special` or missing a type, the scan checks `lstat`, then briefly opens
a directory handle if the type remains unclear. Unverified entries and scan failures
are reported as incomplete. Known directory symlinks are not traversed.

Empty folders and folders containing only non-PDF files are recorded during scans.
Local folders are created as needed for PDF transfers. Filename conflicts caused
by capitalization or Unicode differences receive stable numbered suffixes;
`path-map.csv` records the assigned names. Distinct documents exposed with exactly
the same remote path cannot be distinguished by this exporter.

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
Scanning and downloading are sequential; a `workers` setting is not implemented.

Initialize once, then download:

```bash
uv run --python 3.12 python -m clm_exporter init --config ./config.toml
uv run --python 3.12 python -m clm_exporter download --config ./config.toml
```

`init` creates the database and backup under `state/`, leaving `reports/` empty.
Repeat `download` for later batches: it skips unchanged completed files even after
local cleanup, downloads changed files, and retries unfinished work.
Changes are detected using remote size and modification time. Partial downloads
restart from the beginning; verified completed temporary files can be recovered.
**Keep `state/`; never reinitialize between batches.**

Replace `download` in the command above with:

| Command | Purpose |
| --- | --- |
| `inventory` | Scan the source without downloading content |
| `status` | Verify staged files, refresh reports, and back up state offline |
| `reconcile` | Scan for remaining download gaps |

Online commands first recover previous interrupted transfers locally, so even
`inventory` and `reconcile` can finalize an already downloaded temporary file.
`status` uses the latest recorded scan without contacting SFTP. Use
`status --run-id <id>` to regenerate a previous run's reports using current local files.

Use `--no-progress` to hide progress bars. Add
`--force-path 'Customers/Acme/Agreement.pdf'` to `download` to repeat a completed
file; the path is case-sensitive and relative to `remote_root`. Other pending files
are still processed.

For each batch:

1. Run `download`, review its summary, then run `status` before offloading.
2. Copy `export/CLM-export` to a new archive folder for that batch, such as in
   Google Drive, preserving its directory structure.
3. Include the latest `run-<id>-staging.csv` and `path-map.csv` from `export/reports/`.
4. Verify the upload completed successfully and keep a separate backup of
   `export/state/`. The exporter does not verify Google Drive uploads.
5. With the exporter stopped, delete the uploaded local PDFs or the entire
   `export/CLM-export/` folder to free space. **Keep the parent `export/` folder,
   including `state/`, `reports/`, and the other exporter-managed files.**
6. Run the same `download` command again. It recreates the export folder as needed,
   skips unchanged PDFs already recorded as downloaded, and continues with pending
   or changed files. Do not run `init` again. Repeat for each batch.

After CLM changes stop, finish downloading and offloading, then run `reconcile`.
Archive copying and verification are manual. A capacity stop leaves work pending.

Logs, CSV manifests, errors, and JSON summaries live in `export/reports/` with the
default configuration. Commands other than `init` produce `run-<id>.log`,
`run-<id>-summary.json`, and the following CSVs. All commands also print a JSON
summary to stdout; `status --run-id` rewrites reports for the specified run.

| CSV | Contents |
| --- | --- |
| `run-<id>-staging.csv` | Completed PDFs currently on disk, verified by size and SHA-256; use for offloading |
| `run-<id>-downloads.csv` | Successful downloads completed during this run, including recovered transfers |
| `run-<id>-history.csv` | All successful download versions across runs |
| `run-<id>-gaps.csv` | Outstanding download work, with state and reason |
| `run-<id>-unresolved.csv` | Errors still unresolved, including those from earlier runs |
| `run-<id>-failures.csv` | Errors recorded during this run, including ones since resolved |
| `path-map.csv` | Refreshed mapping of discovered remote paths to assigned local paths, including non-PDF entries |

Run tests and lint checks:

```bash
uv run --python 3.12 python -m unittest discover -v
uvx ruff check clm_exporter tests
```

Optional synthetic pilot:

```bash
uv run --python 3.12 python -m tests.synthetic_pilot --output /tmp/clm-pilot.json
```
