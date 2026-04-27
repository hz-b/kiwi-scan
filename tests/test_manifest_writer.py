import os
import tempfile
import unittest

from kiwi_scan.manifestwriter import ManifestWriter


class TestManifestWriter(unittest.TestCase):
    def test_newmanifest_and_append(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "manifest.yaml")

            # create new manifest
            path = ManifestWriter.newmanifest(manifest_path)
            self.assertTrue(os.path.exists(path))

            # get active writer
            writer = ManifestWriter.from_active()
            self.assertIsNotNone(writer)

            # append a dummy config
            dummy_config = {"test": 123}
            scan_id = writer.append_scan_config(
                dummy_config,
                scan_type="test",
                path=tmpdir,
                data_file="scan.txt",
            )

            self.assertTrue(scan_id.startswith("scan_"))

            # verify content
            with open(manifest_path, "r", encoding="utf-8") as f:
                content = f.read()

            self.assertIn("manifest:", content)
            self.assertIn("scans:", content)
            self.assertIn("scan_type: test", content)
            self.assertIn("scan.txt", content)


if __name__ == "__main__":
    unittest.main()
