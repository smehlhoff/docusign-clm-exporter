import tempfile
import unittest
from pathlib import Path

from clm_exporter.config import load
from clm_exporter.errors import FatalError


class ConfigLoadingTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.path = self.root / "config.toml"
        self.valid = '''
[sftp]
host = "SFTP.example"
username = "operator"
private_key_path = "keys/../key"
known_hosts_path = "known_hosts"
remote_root = "//root/child/../"
[local]
working_directory = "./export"
'''

    def test_example_uses_documented_capacity_defaults(self):
        cfg = load(Path(__file__).parents[1] / "config.example.toml")
        self.assertEqual(cfg.max_staged_bytes, 350_000_000_000)
        self.assertEqual(cfg.min_free_bytes, 50_000_000_000)

    def test_relative_paths_and_remote_root_normalization(self):
        self.path.write_text(self.valid)
        cfg = load(str(self.path))
        self.assertEqual(cfg.private_key_path, self.root / "key")
        self.assertEqual(cfg.known_hosts_path, self.root / "known_hosts")
        self.assertEqual(cfg.working_directory, self.root.resolve() / "export")
        self.assertEqual(cfg.identity, ["sftp.example", 22, "operator", "/root"])

    def test_managed_directory_symlink_is_not_resolved(self):
        target = self.root / "elsewhere"
        target.mkdir()
        (self.root / "export").symlink_to(target, target_is_directory=True)
        self.path.write_text(self.valid)
        self.assertEqual(load(self.path).working_directory, self.root.resolve() / "export")

    def test_validation_preserves_first_error_and_cause(self):
        cases = [
            (self.valid + '\n[extra]\n', "Unknown configuration option in [extra]"),
            (self.valid + '\n[transfer]\nworkers = 2\n', "Unknown configuration option in [transfer]"),
            (self.valid + '\n[concurrency]\npassphrase = "secret"\n',
             "Unknown configuration option in [concurrency]"),
            ('sftp = "invalid"\n', "Unknown configuration option in [sftp]"),
            (self.valid.replace('"//root/child/../"', '"relative"').replace(
                '"SFTP.example"', '""'), "remote_root must be an absolute POSIX path"),
            (self.valid.replace('"SFTP.example"', '""') + '\nexport_folder_name = "state"\n',
             "host must be a nonempty string"),
            (self.valid + '\nexport_folder_name = "state"\n[transfer]\nchunk_bytes = 0\n',
             "export_folder_name must be a dedicated single directory name"),
        ]
        for content, message in cases:
            with self.subTest(message=message, content=content):
                self.path.write_text(content)
                with self.assertRaises(FatalError) as raised:
                    load(self.path)
                self.assertEqual(str(raised.exception), f"Invalid configuration: {message}")
                self.assertIsInstance(raised.exception.__cause__, ValueError)

    def test_integer_limits_reject_booleans_and_accept_boundary_values(self):
        for key, value in (("max_attempts", "true"), ("max_attempts", "1.5"),
                           ("max_attempts", "0"), ("max_attempts", "-1"), ("chunk_bytes", "0")):
            with self.subTest(key=key, value=value):
                self.path.write_text(self.valid + f'\n[transfer]\n{key} = {value}\n')
                with self.assertRaisesRegex(FatalError, f"{key} must be a positive integer"):
                    load(self.path)
        self.path.write_text(self.valid.replace('username =', 'port = 65535\nusername =')
                             + '\n[limits]\nmin_free_bytes = 0\n[transfer]\nchunk_bytes = 16777216\n')
        cfg = load(self.path)
        self.assertEqual((cfg.port, cfg.min_free_bytes, cfg.chunk_bytes), (65535, 0, 16777216))

    def test_missing_required_value_keeps_key_error_cause(self):
        self.path.write_text(self.valid.replace('private_key_path = "keys/../key"\n', ''))
        with self.assertRaises(FatalError) as raised:
            load(self.path)
        self.assertEqual(str(raised.exception), "Invalid configuration: 'private_key_path'")
        self.assertIsInstance(raised.exception.__cause__, KeyError)

    def test_missing_file_keeps_io_error_cause(self):
        with self.assertRaises(FatalError) as raised:
            load(self.path)
        self.assertIsInstance(raised.exception.__cause__, FileNotFoundError)

    def test_worker_count_defaults_and_validation(self):
        self.path.write_text(self.valid)
        self.assertEqual(load(self.path).workers, 1)
        for value in (1, 2, 5, 1000):
            self.path.write_text(self.valid + f"\n[concurrency]\nworkers = {value}\n")
            self.assertEqual(load(self.path).workers, value)
        for value in ('true', 'false', '0', '-1', '1.5', '"2"'):
            with self.subTest(value=value):
                self.path.write_text(self.valid + f"\n[concurrency]\nworkers = {value}\n")
                with self.assertRaisesRegex(FatalError, "workers must be a positive integer"):
                    load(self.path)
