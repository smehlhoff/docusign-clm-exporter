"""Focused publications retain recoverable full results and failure diagnostics."""
import csv
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from clm_exporter.cli import execute, main
from clm_exporter.config import Config

from .fakes import FakeRemote


class CompactReportTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.cfg = Config('unused.invalid', 'user', self.root / 'key', self.root / 'hosts',
                          '/root', self.root / 'export', min_free_bytes=0)
        self.remote = FakeRemote()
        self.invoke('init')

    def invoke(self, command='download', **kwargs):
        with redirect_stderr(io.StringIO()):
            return execute(self.cfg, command, remote_factory=self.remote.factory, progress=False,
                           full_reports=kwargs.pop('full_reports', False), sleep=lambda _: None, **kwargs)

    def csv_input(self, content):
        path = self.root / 'input.csv'
        path.write_text('DOCUMENT ID,DOCUMENT NAME,LOCATION PATH\n' + content)
        return path

    def test_csv_default_and_full_reconstruction(self):
        self.remote.add('a.pdf', b'abc')
        code, result = self.invoke(csv_path=self.csv_input('1,a.pdf,/root\n2,a.pdf,/root\n'))
        self.assertEqual(code, 0)
        csv_names = {key for key, value in result['reports'].items() if value.endswith('.csv')}
        self.assertEqual(csv_names, {'documents', 'staging', 'gaps', 'unresolved'})
        self.assertEqual(len(list(self.cfg.reports.glob('*.csv'))), 4)
        saved = json.loads(Path(result['reports']['summary']).read_text())
        self.assertEqual(saved['pdfs_with_unknown_size'], 0)
        self.assertNotIn('latest_online_attempt', saved)
        self.assertEqual(saved['unique_path_count'], 1)
        self.assertEqual(saved['expected_document_count'], 2)
        self.assertEqual(saved['publication']['backup_status'], 'complete')
        code, full = self.invoke('status', report_run=result['run_id'], full_reports=True)
        self.assertEqual(code, 0)
        self.assertTrue(Path(full['reports']['history']).is_file())
        self.assertTrue(Path(full['reports']['path_map']).is_file())
        self.assertIn('pdfs_with_unknown_size', json.loads(Path(full['reports']['summary']).read_text()))
        with Path(full['reports']['documents']).open() as stream:
            self.assertEqual(len(list(csv.DictReader(stream))), 2)

    def test_input_identity_and_size_counts_survive_offline_status(self):
        self.remote.add('empty.pdf', b'')
        source = self.csv_input('1,empty.pdf,/root\n2,empty.pdf,/root\n3,missing.pdf,/root\n4,missing.pdf,/root\n')
        code, result = self.invoke(csv_path=source)
        self.assertEqual(code, 2)
        _, status = self.invoke('status')
        _, reconstructed = self.invoke('status', report_run=result['run_id'])
        for published in (result, status, reconstructed):
            saved = json.loads(Path(published['reports']['summary']).read_text())
            self.assertEqual(saved['input_source'], str(source))
            self.assertEqual(saved['source'], {'remote_root': '/root'})
            self.assertEqual(saved['pdfs_with_unknown_size'], 1)
            self.assertEqual(saved['zero_byte_pdfs'], 1)
            self.assertEqual(saved['pending_deferred_count'], 1)
            self.assertEqual(saved['pending_deferred_bytes'], 0)

    def test_directory_count_is_scoped_and_excludes_root(self):
        self.remote.add('Contracts/Active/a.pdf')
        self.remote.add('Unrelated/b.pdf')
        source = self.csv_input('1,a.pdf,/root/Contracts/Active\n2,a.pdf,/root/Contracts/Active\n')
        code, result = self.invoke(csv_path=source)
        self.assertEqual(code, 0)
        saved = json.loads(Path(result['reports']['summary']).read_text())
        self.assertEqual(saved['directories_discovered'], 2)
        _, status = self.invoke('status')
        self.assertEqual(json.loads(Path(status['reports']['summary']).read_text())['directories_discovered'], 2)
        _, recursive = self.invoke('inventory')
        self.assertEqual(json.loads(Path(recursive['reports']['summary']).read_text())['directories_discovered'], 3)

    def test_validation_and_current_integrity_are_not_hidden(self):
        code, rejected = self.invoke(csv_path=self.csv_input('1,../bad.pdf,/root\n'))
        self.assertEqual(code, 1)
        self.assertTrue(Path(rejected['reports']['import_errors']).is_file())
        self.remote.add('a.pdf', b'abc')
        _, result = self.invoke()
        with Path(result['reports']['staging']).open() as stream:
            row = next(csv.DictReader(stream))
        (self.cfg.export / row['mapped_path']).write_bytes(b'corrupt')
        _, status = self.invoke('status', report_run=result['run_id'])
        self.assertTrue(Path(status['reports']['current_integrity']).is_file())
        saved = json.loads(Path(status['reports']['summary']).read_text())
        self.assertTrue(saved['current_staging']['integrity_errors'])

    def test_cli_defaults_to_compact_and_full_is_opt_in(self):
        self.remote.add('a.pdf')
        for flags in ([], ['--full-reports']):
            def run(*args, **kwargs):
                return execute(*args, **kwargs, remote_factory=self.remote.factory, sleep=lambda _: None)
            output = io.StringIO()
            with patch('clm_exporter.cli.load', return_value=self.cfg), patch('clm_exporter.cli.execute', side_effect=run), \
                    redirect_stdout(output), redirect_stderr(io.StringIO()):
                code = main(['download', '--config', 'unused.toml', '--no-progress', *flags])
            self.assertEqual(code, 0)
            summary = json.loads(output.getvalue())
            self.assertEqual('latest_online_attempt' in summary, bool(flags))
            self.assertEqual(summary['source']['remote_root'], '/root')
            self.assertEqual(summary['pdfs_with_unknown_size'], 0)
            self.assertEqual(summary['zero_byte_pdfs'], 0)
            self.assertEqual('history' in summary['reports'], bool(flags))
            self.assertEqual(summary, json.loads(Path(summary['reports']['summary']).read_text()))

    def test_backup_failure_remains_visible(self):
        with patch('clm_exporter.state.State.backup', side_effect=OSError('backup unavailable')):
            code, summary = self.invoke()
        self.assertEqual(code, 1)
        saved = json.loads(Path(summary['reports']['summary']).read_text())
        self.assertEqual(saved['invocation_exit_code'], 1)
        self.assertEqual(saved['publication']['backup_status'], 'failed')
        self.assertIn('backup unavailable', saved['publication']['diagnostic'])
