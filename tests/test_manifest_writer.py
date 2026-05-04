import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import yaml

from kiwi_scan.manifestwriter import ManifestWriter, ManifestResolver

class TestManifestWriter(unittest.TestCase):
    def test_newmanifest_and_append(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "manifest.yaml")
            state_path = os.path.join(tmpdir, "active_manifest")

            with patch.dict(
                os.environ,
                {
                    ManifestWriter.ENV_MANIFEST_FILE: "",
                    ManifestWriter.ENV_MANIFEST_STATE_FILE: state_path,
                },
            ):
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

    def test_manifest_resolver_selects_recent_manifest_and_scan_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)

            for name in (
                "old_scan.txt",
                "old_meta.txt",
                "newer_scan.txt",
                "newer_meta.txt",
                "newest_scan.txt",
                "newest_meta.txt",
            ):
                (data_dir / name).write_text("dummy\n", encoding="utf-8")

            old_manifest = data_dir / "manifest_old.yaml"
            new_manifest = data_dir / "manifest_new.yaml"

            old_manifest.write_text(
                yaml.safe_dump(
                    {
                        "manifest": {"created_at": "2026-05-01T10:00:00+02:00"},
                        "scans": [
                            {
                                "id": "scan_old",
                                "created_at": "2026-05-01T10:01:00+02:00",
                                "data_file": "old_scan.txt",
                                "metadata_file": "old_meta.txt",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            new_manifest.write_text(
                yaml.safe_dump(
                    {
                        "manifest": {"created_at": "2026-05-02T10:00:00+02:00"},
                        "scans": [
                            {
                                "id": "scan_newer",
                                "created_at": "2026-05-02T10:01:00+02:00",
                                "data_file": "newer_scan.txt",
                                "metadata_file": "newer_meta.txt",
                            },
                            {
                                "id": "scan_newest",
                                "created_at": "2026-05-02T10:02:00+02:00",
                                "data_file": "newest_scan.txt",
                                "metadata_file": "newest_meta.txt",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            resolver = ManifestResolver(str(data_dir))

            self.assertEqual(resolver.select_manifest(0), new_manifest)
            self.assertEqual(resolver.select_manifest(1), old_manifest)
            self.assertEqual(
                resolver.select_file(source_type="scan", manifest_index=0, scan_index=0),
                data_dir / "newest_scan.txt",
            )
            self.assertEqual(
                resolver.select_file(source_type="meta", manifest_index=0, scan_index=0),
                data_dir / "newest_meta.txt",
            )
            self.assertEqual(
                resolver.select_file(source_type="scan", manifest_index=0, scan_index=1),
                data_dir / "newer_scan.txt",
            )

if __name__ == "__main__":
    unittest.main()
