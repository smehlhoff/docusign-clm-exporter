# Docusign CLM PDF exporter

Download CLM PDFs over SFTP, preserving folder paths and download history across
batches. Requires macOS or Linux, Python 3.12+, and
[uv](https://docs.astral.sh/uv/getting-started/installation/).

## Setup

From this directory:

```bash
uv python install 3.12
uv sync --python 3.12
cp config.example.toml config.toml
```

Skip the copy if `config.toml` already exists. Using the
[example config](config.example.toml), set your SFTP host, username, RSA key,
trusted `known_hosts` file, and remote root. Verify and add the server host key
before connecting; encrypted keys prompt for a passphrase.

The SFTP server must support SHA-2 RSA signatures and a modern key exchange.
Paramiko 5.0.1 removes legacy SHA-1 support; existing RSA keys and verified
`known_hosts` entries can still be used.

Relative config paths resolve from the config file's folder. Defaults:

- Working directory: `./export`. Keep it outside synced folders.
- Disk limits: **350 GB** staged (including temporary downloads), **50 GB** free
  space reserved, both configurable in decimal GB.
- Concurrency: one worker; increase `[concurrency] workers` for parallel scanning
  and downloads, then restart the command.

`config.toml` and `export/` are ignored by Git.

## Download

Initialize once, then download:

```bash
uv run --python 3.12 python -m clm_exporter init --config ./config.toml
uv run --python 3.12 python -m clm_exporter download --config ./config.toml
```

Repeat `download` for later batches. It skips unchanged completed files even after
local cleanup, downloads files whose remote size or modification time changed,
and retries unfinished work. Partial downloads restart from the beginning.
**Keep `export/state/`; never reinitialize between batches.**

Replace `download` in the command above with:

| Command | Purpose |
| --- | --- |
| `inventory` | Scan the source without downloading content |
| `status` | Verify staged files, refresh reports, and back up state offline |
| `reconcile` | Scan for remaining download gaps |

Scanning is recursive by default. To use a complete CLM document report, add
`--csv` to `inventory`, `download`, or `reconcile`:

```bash
uv run --python 3.12 python -m clm_exporter download --config ./config.toml --csv ./clm-report.csv
```

Required columns (in any order):

```csv
DOCUMENT ID,DOCUMENT NAME,LOCATION PATH
D02DC5F5-466A-43C0-95A8-A6B9F05C5F1F,test.pdf,/Main/Contracts/
```

This selects `/Main/Contracts/test.pdf`; locations must be absolute SFTP paths
beneath `remote_root`. CSV mode checks listed files directly, without directory
listings. Repeated paths download once, with every CSV row and document ID retained
in the results. Completion applies to the supplied list. See
[CSV rules and final reconciliation](REFERENCE.md#csv-exports).

## Offload and continue

Using the default `export/` working directory, repeat for each batch:

1. Run `download`, review its summary, then run `status` before offloading.
2. Copy `export/CLM-export/` to a new archive folder, preserving its structure.
   Include the latest `run-<id>-staging.csv` from `export/reports/` (it includes both remote and local paths).
3. Verify the archive upload and keep a separate backup of `export/state/`.
   Archive copying and verification are manual.
4. With the exporter stopped, delete the archived local PDFs or `export/CLM-export/`.
   **Keep the parent `export/` folder and its state, reports, and managed files.**
5. Repeat the same `download` command, including `--csv` if used. The export folder
   is recreated as needed; unchanged completed PDFs remain skipped.

A capacity stop leaves work pending. After CLM changes stop, finish downloading
and offloading, then run `reconcile`. For CSV exports, download and reconcile
against the same fresh, complete CLM report.

## Reference

Logs, critical CSV reports, and compact JSON summaries are in `export/reports/`.
Defaults are `staging`, `gaps`, `unresolved`, and (in CSV mode) `documents`.
Validation and current-integrity CSVs appear only when they contain errors.
Add `--full-reports` to any command for all diagnostic reports and the detailed
summary, or reconstruct them offline with `status --run-id <id> --full-reports`.
Existing report files are retained. See the
[export reference](REFERENCE.md) for report definitions, CSV validation,
recovery, state upgrades, command options, and development checks.
