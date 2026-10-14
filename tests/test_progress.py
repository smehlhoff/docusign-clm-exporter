import importlib.util
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

from clm_exporter.cli import execute, main
from clm_exporter.config import Config
from clm_exporter.errors import FatalError
from clm_exporter.progress import Progress

from .fakes import FakeRemote


class Terminal(io.StringIO):
    def isatty(self):
        return True


class RecordingBar:
    def __init__(self, options):
        self.options, self.n, self.closed = options, 0, False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def update(self, amount=1):
        self.n += amount


class RecordingTqdm:
    def __init__(self):
        self.bars = []

    def __call__(self, **options):
        bar = RecordingBar(options)
        self.bars.append(bar)
        options["file"].write("\r" + options["desc"])
        return bar

    def write(self, message, file):
        file.write(message + "\n")


class ProgressTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.cfg = Config("simulated.test", "operator", root / "key", root / "known_hosts", "/", root / "working",
                          max_staged_bytes=100, min_free_bytes=10, chunk_bytes=2)
        self.remote = FakeRemote()
        self.display = RecordingTqdm()
        self.stderr = Terminal()
        with redirect_stderr(self.stderr):
            execute(self.cfg, "init")

    def invoke(self, command="download", **kwargs):
        with redirect_stderr(self.stderr), patch("clm_exporter.progress.load_tqdm", return_value=self.display):
            return execute(self.cfg, command, remote_factory=self.remote.factory,
                           free=kwargs.pop("free", lambda: 10000), sleep=lambda _: None, **kwargs)

    def bars(self, unit):
        return [bar for bar in self.display.bars if bar.options["unit"] == unit]

    def invoke_cli(self, *options):
        stdout = io.StringIO()

        def run(cfg, command, *args, **kwargs):
            return execute(cfg, command, *args, remote_factory=self.remote.factory, **kwargs)

        with redirect_stdout(stdout), patch("clm_exporter.cli.load", return_value=self.cfg), \
             patch("clm_exporter.cli.execute", side_effect=run):
            code = main(["download", "--config", "unused.toml", *options])
        return code, json.loads(stdout.getvalue())

    def test_scan_has_unknown_total_and_transfer_counts_written_bytes(self):
        self.remote.add("dir/a.pdf", b"12345")
        self.remote.add("dir/not.txt")
        code, _ = self.invoke()
        self.assertEqual(code, 0)
        scan = self.bars("entry")[0]
        self.assertIsNone(scan.options["total"])
        self.assertEqual(scan.n, 3)
        self.assertNotIn("remaining", scan.options["bar_format"])
        files = self.bars("file")[0]
        self.assertEqual((files.n, files.options["total"]), (1, 1))
        byte_bar = self.bars("B")[0]
        self.assertEqual((byte_bar.n, byte_bar.options["total"]), (5, 5))
        self.assertEqual(byte_bar.options["unit_divisor"], 1000)
        self.assertTrue(all(bar.closed for bar in self.display.bars))

    def test_empty_directories_report_rate_limited_progress_with_or_without_bars(self):
        for number in range(6):
            self.remote.add(f"empty-{number}", kind="directory")
        # A known symlink is skipped; ambiguous special entries must be checked.
        self.remote.add("ignored.txt", kind="symlink")

        for enabled in (True, False):
            with self.subTest(progress=enabled):
                clock = Mock(return_value=0)
                self.stderr = Terminal()

                def scan_empty_directory(directory, clock=clock):
                    if directory:
                        clock.return_value += 2

                self.remote.list_hook = scan_empty_directory
                with patch("clm_exporter.discovery.time.monotonic", clock):
                    code, summary = self.invoke("inventory", progress=enabled)

                self.assertEqual(code, 0)
                self.assertTrue(summary["scan_complete"])
                self.assertFalse(self.remote.opens)
                self.assertEqual(summary["directories_discovered"], 6)
                saved = json.loads(Path(summary["reports"]["summary"]).read_text())
                self.assertEqual(saved["directories_discovered"], 6)
                log = Path(summary["reports"]["log"]).read_text()
                messages = [line.split(" INFO ", 1)[1] for line in log.splitlines()
                            if " INFO Discovery:" in line]
                # Six two-second scans should produce two updates, even though
                # no entries arrive after the root listing. Counts include root.
                self.assertEqual(messages, [
                    "Discovery: 7 entries examined, 4 directories processed, 3 directories queued",
                    "Discovery: 7 entries examined, 7 directories processed, 0 directories queued",
                ])
                for message in messages:
                    self.assertIn(message, self.stderr.getvalue())
                self.assertIn("Discovery complete: 7 entries examined, 7 directories processed, 0 directories queued", log)
                for output in (log, self.stderr.getvalue()):
                    self.assertNotIn("Skipping", output)
                    self.assertNotIn("Command finished", output)

    def test_inventory_keeps_warnings_and_fatal_errors_visible(self):
        for failure, code, message in (
            (PermissionError("listing denied"), 2, "Directory scan failed"),
            (FatalError("connection unavailable"), 1, "Fatal error"),
        ):
            with self.subTest(failure=type(failure).__name__):
                self.stderr = Terminal()
                with patch.object(self.remote, "iterdir", side_effect=failure):
                    actual, summary = self.invoke("inventory", progress=False)
                self.assertEqual(actual, code)
                self.assertIn(message, self.stderr.getvalue())
                self.assertIn(message, Path(summary["reports"]["log"]).read_text())

    def test_retry_starts_new_bar_with_updated_size(self):
        self.remote.add("a.pdf", b"old")
        def change(path, offset):
            if offset == 2:
                self.remote.add(path, b"newer", 2)
                self.remote.read_hook = None
        self.remote.read_hook = change
        self.assertEqual(self.invoke()[0], 0)
        first, second = self.bars("B")
        self.assertEqual((first.n, first.options["total"]), (3, 3))
        self.assertEqual((second.n, second.options["total"]), (5, 5))
        self.assertIn("attempt 2", second.options["desc"])
        self.assertEqual(self.bars("file")[0].n, 1)
        self.assertTrue(all(bar.closed for bar in self.display.bars))

    def test_capacity_stop_keeps_deferred_file_out_of_processed_count(self):
        self.cfg = replace(self.cfg, max_staged_bytes=4)
        self.remote.add("a.pdf", b"1234")
        self.remote.add("b.pdf", b"5678")
        code, summary = self.invoke()
        self.assertEqual((code, summary["stop_reason"]), (0, "batch_capacity"))
        files = self.bars("file")[0]
        self.assertEqual((files.n, files.options["total"]), (1, 2))
        self.assertEqual(len(self.bars("B")), 1)
        self.assertTrue(all(bar.closed for bar in self.display.bars))

    def test_interrupt_closes_partial_bars(self):
        self.remote.add("a.pdf", b"1234")
        def interrupt(path, offset):
            if offset == 2:
                raise KeyboardInterrupt
        self.remote.read_hook = interrupt
        self.assertEqual(self.invoke()[0], 130)
        self.assertEqual(self.bars("B")[0].n, 2)
        self.assertEqual(self.bars("file")[0].n, 0)
        self.assertTrue(all(bar.closed for bar in self.display.bars))
        self.assertFalse(list(self.cfg.work.iterdir()))

    def test_zero_byte_pdf_and_failure_count_processed_not_successes(self):
        self.remote.add("a.pdf", b"")
        self.remote.add("b.pdf")
        self.remote.open_hook = lambda path: (_ for _ in ()).throw(PermissionError("denied")) if path == "b.pdf" else None
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual(summary["new_successes"], 1)
        self.assertEqual(self.bars("file")[0].n, 2)
        self.assertEqual(self.bars("B")[0].options["total"], 0)
        self.assertEqual(self.bars("B")[0].n, 0)
        self.assertTrue(all(bar.closed for bar in self.display.bars))

    def test_cli_nonterminal_and_opt_out_do_not_require_tqdm(self):
        for name, stream, options in (
            ("redirected", io.StringIO(), ()),
            ("disabled", Terminal(), ("--no-progress",)),
        ):
            with self.subTest(mode=name), redirect_stderr(stream), \
                 patch("clm_exporter.progress.load_tqdm", side_effect=AssertionError("must not import")):
                self.remote.add(f"{name}.pdf")
                code, summary = self.invoke_cli(*options)
                self.assertEqual(code, 0)
                self.assertEqual(summary["new_successes"], 1)
                self.assertEqual((self.cfg.export / f"{name}.pdf").read_bytes(), b"pdf")
                self.assertNotIn("\r", stream.getvalue())

    @unittest.skipUnless(importlib.util.find_spec("tqdm"), "tqdm is not installed; real rendering test unavailable")
    def test_interactive_cli_keeps_stdout_json_and_file_logs_clean(self):
        self.remote.add("line\nbreak.pdf")
        with redirect_stderr(self.stderr):
            code, summary = self.invoke_cli()
        self.assertEqual(code, 0)
        self.assertEqual(summary["new_successes"], 1)
        self.assertIn("Scanning", self.stderr.getvalue())
        self.assertIn("Files processed", self.stderr.getvalue())
        self.assertIn("Downloaded", self.stderr.getvalue())
        self.assertNotIn("\r", Path(summary["reports"]["log"]).read_text())
        self.assertIn("Receiving 'line\\nbreak.pdf'", self.stderr.getvalue())
        self.assertNotIn("line\nbreak.pdf", self.stderr.getvalue())

    def test_missing_tqdm_has_actionable_error(self):
        with patch.dict("sys.modules", {"tqdm": None}), self.assertRaisesRegex(FatalError, "requirements.txt or use --no-progress"):
            Progress(stream=Terminal())
