# Export reference

See the [README](README.md) for setup, everyday commands, and batch offloading.

## Discovery and filenames

By default, `inventory`, `download`, and `reconcile` share the same recursive scan. PDFs are
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

## Concurrency

For concurrent scanning and downloads, set the connection limit in your config:

```toml
[concurrency]
workers = 3
```

The default is one worker. With multiple workers, entry metadata and directory
checks can use idle connections, and confirmed child folders can start scanning
before their parent listing finishes. Filename mappings are assigned after
discovery in a stable order. Downloads then use the same worker limit to transfer
separate PDFs concurrently. Restart the command after changing this setting.

## CSV exports

To export a complete CLM document report instead of recursively scanning, add
`--csv` to `inventory`, `download`, or `reconcile`:

```bash
uv run --python 3.12 python -m clm_exporter download --config ./config.toml --csv ./clm-report.csv
```

The CSV requires these three named columns, in any order. Header matching ignores
case and surrounding whitespace; extra named columns, UTF-8 BOMs, and quoted
values are supported.

```csv
DOCUMENT ID,DOCUMENT NAME,LOCATION PATH
D02DC5F5-466A-43C0-95A8-A6B9F05C5F1F,test.pdf,/Main/Contracts/
```

This row selects `/Main/Contracts/test.pdf`. Locations are absolute SFTP paths
beneath your configured `remote_root`; names must include `.pdf` and be a single
safe path component. Names and IDs retain their original spelling. Duplicate
resolved paths, invalid rows, and empty reports reject the entire input before
recovery or network work. Document IDs are report metadata: repeated IDs on
different paths are allowed, and changing an ID alone does not repeat a download.
CSV file paths resolve from your current shell directory.

CSV mode checks each distinct ancestor folder and listed file directly over SFTP,
using the configured worker limit and no directory listings. Explicit ancestor
symlinks or files block their descendants; independent files continue. The whole
accepted list reserves stable local aliases before network work, including files
whose metadata checks fail. Existing path history, capacity limits, recovery, and
offloading work in both modes. Recovery remains global, including completed
temporary files outside the current list. `--force-path` must identify a checked
file in the current CSV; an unlisted request is an error.

Repeat the same `download --csv` command after each offload. For the final pass,
generate a fresh complete CLM report, download its remaining files, and run
`reconcile --csv` with that same report. A quiet period reduces concurrent source
changes. Completion applies to the supplied list, not a point-in-time snapshot of
all CLM documents. Paths omitted from a later report remain in history and on disk;
their absence from the list does not prove deletion from CLM. Omitting `--csv`
always selects recursive discovery again.

## Recovery and saved results

Online commands first recover previous interrupted transfers locally, so even
`inventory` and `reconcile` can finalize an already downloaded temporary file.
`status` stays offline and identifies the latest online attempt. If its CSV was
rejected or interrupted, it shows that failure separately from the last accepted
CSV run; repeating `status` does not hide the failed attempt. Acceptance means
valid input, not completed downloads.

Offline `status` preserves the latest online attempt's fatal or interrupted exit
code (1 or 130) in either discovery mode. It reassesses local integrity after
files are restored or offloaded; the saved original result remains unchanged.

Use `status --run-id <id>` to regenerate a run's saved outcomes and retry reports
and backup without needing the original CSV. Later downloads, failures, or
offloading do not change finalized outcomes. Current staging verification appears
separately, including whether a document's recorded version is staged, absent,
replaced, or unverified. `export_exit_code` describes the saved export result;
`original_exit_code` and `publication` distinguish the original invocation from an
offline publication retry. A report or backup failure returns nonzero while
retaining successful transfers and saved coverage.

## State upgrades

Existing state databases upgrade automatically to schema version 2 under the
working-directory lock, after source/destination validation and a consistent
`pre-migration-v2` backup. Keep that backup. Older binaries cannot open the upgraded
database; rolling back requires the matching backup and local state. Legacy runs
without saved original results are labeled "Original completion result unavailable";
their successful transfer history still prevents unchanged downloads.

## Command options

Use `--no-progress` to hide progress bars. Add
`--force-path 'Customers/Acme/Agreement.pdf'` to `download` to repeat a completed
file; the path is case-sensitive and relative to `remote_root`. Other pending files
are still processed.

## Reports and backups

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
| `run-<id>-documents.csv` | CSV input entries, saved outcomes, transfer references, diagnostics, and current payload presence |
| `run-<id>-absent-from-report.csv` | Paths in the preceding accepted CSV that are absent from this list; informational |
| `run-<id>-import-errors.csv` | CSV validation diagnostics, with row numbers when available |
| `run-<id>-current-integrity.csv` | Current local integrity errors, separate from saved historical outcomes |

The three CSV input reports are produced for CSV mode. Its document outcomes are
`failed`, `unchecked`, `pending`, `downloaded_this_run`, and `previously_downloaded`.
Their counts sum to the accepted input count. Unrelated historical errors remain
visible with `in_current_scope=false` without blocking current CSV completion.
Staging and history reports remain comprehensive for offloading. Zero-byte PDFs
retain the existing successful-transfer behavior and review notice.

Full snapshots, results, transfer history, and backups are retained across runs.
Backup storage grows with usage; remove older redundant backup copies manually
when appropriate, retaining verified recent backups and the pre-migration backup.
Do not remove the active database or state directory during routine offloading.

## Development

Run tests and lint checks:

```bash
uv run --python 3.12 python -m unittest discover -v
uvx ruff check clm_exporter tests
```

Optional synthetic pilot:

```bash
uv run --python 3.12 python -m tests.synthetic_pilot --output /tmp/clm-pilot.json
uv run --python 3.12 python -m tests.concurrency_pilot
uv run --python 3.12 python -m tests.csv_pilot --output /tmp/clm-csv-pilot.json
```

The CSV pilot repeats 87,000-row runs with metadata failures, offloading, and
accumulated history, measuring connections, queues, memory, phase times, and
database/backup growth. Its in-memory transport and simulated delay are not live
CLM measurements. Confirm path alignment and usable concurrency with a
representative live SFTP pilot before a full production export.
