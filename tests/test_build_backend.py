"""Build dependency regressions; run with the pinned isolated build backend."""
import importlib.util
import unicodedata
import unittest


@unittest.skipUnless(importlib.util.find_spec("setuptools"), "Requires the optional build backend")
class BuildBackendTests(unittest.TestCase):
    def test_manifest_exclusion_handles_unicode_normalization(self):
        from setuptools.command.egg_info import FileList

        filename = "private_café.txt"
        files = FileList()
        files.files = [unicodedata.normalize("NFD", filename), "public.txt"]
        files.process_template_line(f"global-exclude {filename}")
        self.assertEqual(files.files, ["public.txt"])
