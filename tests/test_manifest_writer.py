import os
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import yaml

from kiwi_scan.manifestwriter import ManifestArchiveDeleter, ManifestWriter, ManifestResolver

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
    
    def test_manifest_resolver_lists_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            old_state = os.environ.get(ManifestWriter.ENV_MANIFEST_STATE_FILE)
            os.environ[ManifestWriter.ENV_MANIFEST_STATE_FILE] = os.path.join(tmpdir, "active_manifest")
            try:
                manifest_path = os.path.join(tmpdir, "manifest.yaml")
                data_path = os.path.join(tmpdir, "scan.txt")
                meta_path = os.path.join(tmpdir, "scan_meta.txt")
                open(data_path, "w", encoding="utf-8").close()
                open(meta_path, "w", encoding="utf-8").close()

                ManifestWriter.newmanifest(manifest_path)
                writer = ManifestWriter.from_active()
                writer.append_scan_config(
                    {"test": 123},
                    scan_type="test",
                    path=tmpdir,
                    data_file="scan.txt",
                    metadata_file="scan_meta.txt",
                )

                resolver = ManifestResolver(tmpdir)
                files = resolver.list_files(manifest_path, include_meta=True, include_manifest=True)

                self.assertEqual(
                    files,
                    [
                        Path(manifest_path).resolve(),
                        Path(data_path).resolve(),
                        Path(meta_path).resolve(),
                    ],
                )
            finally:
                if old_state is None:
                    os.environ.pop(ManifestWriter.ENV_MANIFEST_STATE_FILE, None)
                else:
                    os.environ[ManifestWriter.ENV_MANIFEST_STATE_FILE] = old_state

    def test_append_stores_absolute_file_references(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "manifest.yaml")
            ManifestWriter.newmanifest(manifest_path)

            writer = ManifestWriter.from_active()
            self.assertIsNotNone(writer)

            writer.append_scan_config(
                {"test": 123},
                scan_type="test",
                path=".",
                data_file="scan.txt",
                metadata_file="scan_meta.txt",
            )

            with open(manifest_path, "r", encoding="utf-8") as f:
                data = f.read()

            self.assertIn(f"path: {os.getcwd()}", data)
            self.assertIn(f"data_file: {os.path.join(os.getcwd(), 'scan.txt')}", data)
            self.assertIn(f"metadata_file: {os.path.join(os.getcwd(), 'scan_meta.txt')}", data)

    def test_create_small_manifest_references_data_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_path = Path(tmpdir) / "scan.txt"
            manifest_path = Path(tmpdir) / "manifest_for_spec.yaml"
            state_path = Path(tmpdir) / "active_manifest"
            data_path.write_text("dummy\n", encoding="utf-8")

            old_state = os.environ.get(ManifestWriter.ENV_MANIFEST_STATE_FILE)
            os.environ[ManifestWriter.ENV_MANIFEST_STATE_FILE] = str(state_path)
            try:
                created = ManifestWriter.create_small_manifest(
                    [str(data_path)],
                    filename=str(manifest_path),
                )
            finally:
                if old_state is None:
                    os.environ.pop(ManifestWriter.ENV_MANIFEST_STATE_FILE, None)
                else:
                    os.environ[ManifestWriter.ENV_MANIFEST_STATE_FILE] = old_state

            self.assertEqual(created, str(manifest_path))
            with manifest_path.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f)

            self.assertEqual(len(data["scans"]), 1)
            scan = data["scans"][0]
            self.assertEqual(scan["manifest_mode"], "small")
            self.assertEqual(scan["data_file"], str(data_path))
            self.assertIsNone(scan["metadata_file"])
            self.assertNotIn("config", scan)

    def test_resolver_falls_back_to_active_manifest(self):
        with tempfile.TemporaryDirectory() as state_dir, tempfile.TemporaryDirectory() as manifest_dir:
            state_file = os.path.join(state_dir, "active_manifest")
            manifest_path = os.path.join(manifest_dir, "manifest_from_tmp.yaml")

            old_state = os.environ.get(ManifestWriter.ENV_MANIFEST_STATE_FILE)
            old_data_dir = os.environ.get(ManifestResolver.ENV_DATA_DIR)
            os.environ[ManifestWriter.ENV_MANIFEST_STATE_FILE] = state_file
            os.environ.pop(ManifestResolver.ENV_DATA_DIR, None)
            try:
                ManifestWriter.newmanifest(manifest_path)
                resolver = ManifestResolver.from_env()

                self.assertEqual(resolver.select_manifest(), Path(manifest_path))
            finally:
                if old_state is None:
                    os.environ.pop(ManifestWriter.ENV_MANIFEST_STATE_FILE, None)
                else:
                    os.environ[ManifestWriter.ENV_MANIFEST_STATE_FILE] = old_state

                if old_data_dir is None:
                    os.environ.pop(ManifestResolver.ENV_DATA_DIR, None)
                else:
                    os.environ[ManifestResolver.ENV_DATA_DIR] = old_data_dir


    def test_plan_delete_bundle_includes_manifest_data_and_meta(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            manifest_path = data_dir / "manifest.yaml"
            data_path = data_dir / "scan.txt"
            meta_path = data_dir / "scan_meta.txt"
            bundle_dir = data_dir / "bundles"
            data_path.write_text("scan\n", encoding="utf-8")
            meta_path.write_text("meta\n", encoding="utf-8")
            manifest_path.write_text(
                yaml.safe_dump(
                    {
                        "manifest": {"created_at": "2026-05-01T10:00:00+02:00"},
                        "scans": [
                            {
                                "id": "scan_1",
                                "created_at": "2026-05-01T10:01:00+02:00",
                                "data_file": "scan.txt",
                                "metadata_file": "scan_meta.txt",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            resolver = ManifestResolver(str(data_dir))
            plan = resolver.plan_delete_bundle(
                [manifest_path],
                include_meta=True,
                bundle_dir=bundle_dir,
            )

            self.assertEqual(plan.manifest_files, [manifest_path.resolve()])
            self.assertEqual(
                plan.files_to_delete,
                [manifest_path.resolve(), data_path.resolve(), meta_path.resolve()],
            )
            self.assertEqual(plan.missing_files, [])
            self.assertEqual(plan.skipped_files, [])
            self.assertEqual(plan.bundle_file.parent, bundle_dir)
            self.assertTrue(plan.bundle_file.name.startswith("kiwi-scan-delete_"))
            self.assertEqual(plan.bundle_file.suffixes[-2:], [".tar", ".gz"])

    def test_plan_delete_bundle_reports_missing_references(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            manifest_path = data_dir / "manifest.yaml"
            data_path = data_dir / "scan.txt"
            missing_meta = data_dir / "missing_meta.txt"
            data_path.write_text("scan\n", encoding="utf-8")
            manifest_path.write_text(
                yaml.safe_dump(
                    {
                        "manifest": {"created_at": "2026-05-01T10:00:00+02:00"},
                        "scans": [
                            {
                                "id": "scan_1",
                                "created_at": "2026-05-01T10:01:00+02:00",
                                "data_file": "scan.txt",
                                "metadata_file": "missing_meta.txt",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            resolver = ManifestResolver(str(data_dir))
            plan = resolver.plan_delete_bundle([manifest_path], include_meta=True, bundle_dir=data_dir)

            self.assertIn(missing_meta.resolve(), plan.missing_files)
            self.assertNotIn(missing_meta.resolve(), plan.files_to_delete)
            self.assertIn(data_path.resolve(), plan.files_to_delete)

    def test_archive_deleter_creates_bundle_and_deletes_only_archived_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            manifest_path = data_dir / "manifest.yaml"
            data_path = data_dir / "scan.txt"
            meta_path = data_dir / "scan_meta.txt"
            bundle_dir = data_dir / "bundles"
            data_path.write_text("scan\n", encoding="utf-8")
            meta_path.write_text("meta\n", encoding="utf-8")
            manifest_path.write_text(
                yaml.safe_dump(
                    {
                        "manifest": {"created_at": "2026-05-01T10:00:00+02:00"},
                        "scans": [
                            {
                                "id": "scan_1",
                                "created_at": "2026-05-01T10:01:00+02:00",
                                "data_file": "scan.txt",
                                "metadata_file": "scan_meta.txt",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            resolver = ManifestResolver(str(data_dir))
            plan = resolver.plan_delete_bundle([manifest_path], include_meta=True, bundle_dir=bundle_dir)
            result = ManifestArchiveDeleter.archive_and_delete(plan)

            self.assertTrue(result.bundle_file.is_file())
            self.assertEqual(set(result.archived_files), set(plan.files_to_delete))
            self.assertEqual(set(result.deleted_files), set(plan.files_to_delete))
            self.assertFalse(manifest_path.exists())
            self.assertFalse(data_path.exists())
            self.assertFalse(meta_path.exists())
            self.assertEqual(result.failed_files, [])

            with tarfile.open(result.bundle_file, "r:gz") as tar:
                names = tar.getnames()
            self.assertIn("restore_map.yaml", names)
            self.assertTrue(any(name.endswith("manifest.yaml") for name in names))
            self.assertTrue(any(name.endswith("scan.txt") for name in names))
            self.assertTrue(any(name.endswith("scan_meta.txt") for name in names))

    def test_archive_deleter_dry_run_does_not_delete_or_archive(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            manifest_path = data_dir / "manifest.yaml"
            data_path = data_dir / "scan.txt"
            data_path.write_text("scan\n", encoding="utf-8")
            manifest_path.write_text(
                yaml.safe_dump(
                    {
                        "manifest": {"created_at": "2026-05-01T10:00:00+02:00"},
                        "scans": [
                            {
                                "id": "scan_1",
                                "created_at": "2026-05-01T10:01:00+02:00",
                                "data_file": "scan.txt",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            resolver = ManifestResolver(str(data_dir))
            plan = resolver.plan_delete_bundle([manifest_path], bundle_dir=data_dir)
            result = ManifestArchiveDeleter.archive_and_delete(plan, dry_run=True)

            self.assertTrue(result.dry_run)
            self.assertTrue(manifest_path.exists())
            self.assertTrue(data_path.exists())
            self.assertFalse(plan.bundle_file.exists())
            self.assertEqual(result.deleted_files, [])
            self.assertEqual(result.archived_files, [])

if __name__ == "__main__":
    unittest.main(verbosity=2)
