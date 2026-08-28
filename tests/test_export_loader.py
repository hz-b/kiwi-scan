# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, call, patch

import pandas as pd

from kiwi_scan.data.manifestwriter import parse_manifest_datetime
from kiwi_scan.export.loader import (
    EmptyScanDataError,
    _contains_non_comment_content,
    _load_metadata,
    _load_scan_dataframe,
    _resolve_reference_path,
    load_export_bundle_from_latest_manifest,
    load_export_bundle_from_manifest,
    load_export_bundle_from_scan_file,
)


class TemporaryDirectoryTestCase(unittest.TestCase):
    def setUp(self):
        self._temporary_directory = tempfile.TemporaryDirectory(
            prefix="kiwi-export-loader-"
        )
        self.addCleanup(self._temporary_directory.cleanup)
        self.directory = Path(self._temporary_directory.name)

    def write_file(self, relative_path, content="data\n"):
        path = self.directory / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path


class TestContainsNonCommentContent(TemporaryDirectoryTestCase):
    def test_rejects_zero_byte_whitespace_and_comment_only_files(self):
        files = [
            self.write_file("zero.dat", ""),
            self.write_file("whitespace.dat", " \n\t\n"),
            self.write_file("comments.dat", "# header\n  # another header\n"),
        ]

        for path in files:
            with self.subTest(path=path.name):
                self.assertFalse(_contains_non_comment_content(path))

    def test_accepts_first_non_comment_line(self):
        path = self.write_file(
            "scan.dat",
            "# Position Signal\n\n0.0 1.0\n# trailing comment\n",
        )

        self.assertTrue(_contains_non_comment_content(path))


class TestParseDatetime(unittest.TestCase):
    def test_parses_supported_values_and_rejects_invalid_values(self):
        aware = datetime(
            2026,
            8,
            26,
            10,
            30,
            tzinfo=timezone(timedelta(hours=2)),
        )
        naive = aware.replace(tzinfo=None)

        self.assertIs(parse_manifest_datetime(aware), aware)
        self.assertEqual(
            parse_manifest_datetime(naive),
            naive.replace(tzinfo=timezone.utc),
        )
        self.assertEqual(
            parse_manifest_datetime("2026-08-26T08:30:00Z"),
            datetime(2026, 8, 26, 8, 30, tzinfo=timezone.utc),
        )
        self.assertIsNone(parse_manifest_datetime(None))
        self.assertIsNone(parse_manifest_datetime("not-a-timestamp"))


class TestResolveReferencePath(TemporaryDirectoryTestCase):
    def setUp(self):
        super().setUp()
        self.manifest = self.write_file("manifest.yaml", "manifest: {}\n")

    def test_empty_references_return_none(self):
        self.assertIsNone(_resolve_reference_path(self.manifest, None))
        self.assertIsNone(_resolve_reference_path(self.manifest, ""))

    def test_resolves_relative_reference_from_extra_base(self):
        entry_directory = self.directory / "relocated"
        data_file = self.write_file("relocated/scan.dat")

        result = _resolve_reference_path(
            self.manifest,
            "scan.dat",
            extra_bases=(None, entry_directory),
        )

        self.assertEqual(result, data_file)

    def test_resolves_relative_reference_from_manifest_directory(self):
        data_file = self.write_file("scan.dat")

        result = _resolve_reference_path(self.manifest, "scan.dat")

        self.assertEqual(result, data_file)

    def test_returns_manifest_relative_fallback_for_missing_relative_path(self):
        result = _resolve_reference_path(self.manifest, "missing.dat")

        self.assertEqual(result, self.directory / "missing.dat")

    def test_preserves_existing_absolute_reference(self):
        data_file = self.write_file("absolute.dat")

        result = _resolve_reference_path(self.manifest, data_file)

        self.assertEqual(result, data_file)

    def test_relocates_missing_absolute_reference_to_entry_directory(self):
        entry_directory = self.directory / "run"
        data_file = self.write_file("run/scan.dat")
        obsolete = Path("/old-machine/archive/scan.dat")

        result = _resolve_reference_path(
            self.manifest,
            obsolete,
            extra_bases=(entry_directory,),
        )

        self.assertEqual(result, data_file)

    def test_relocates_absolute_reference_using_parent_tail(self):
        data_file = self.write_file("old-run/scan.dat")
        obsolete = Path("/old-machine/old-run/scan.dat")

        result = _resolve_reference_path(self.manifest, obsolete)

        self.assertEqual(result, data_file)

    def test_relocates_absolute_reference_using_manifest_directory(self):
        data_file = self.write_file("scan.dat")
        obsolete = Path("/old-machine/archive/scan.dat")

        result = _resolve_reference_path(self.manifest, obsolete)

        self.assertEqual(result, data_file)

    def test_preserves_missing_absolute_reference_as_fallback(self):
        obsolete = Path("/old-machine/archive/missing.dat")

        result = _resolve_reference_path(self.manifest, obsolete)

        self.assertEqual(result, obsolete)


class TestLoadScanDataframe(TemporaryDirectoryTestCase):
    def setUp(self):
        super().setUp()
        self.data_file = self.write_file("scan.dat", "Position Signal\n0 1\n")

    @patch("kiwi_scan.export.loader.DataLoader")
    def test_loads_dataframe_with_requested_data_directory(self, loader_class):
        dataframe = pd.DataFrame({"Position": [0.0], "Signal": [1.0]})
        loader_class.return_value.load_data.return_value = dataframe

        result = _load_scan_dataframe(self.data_file, self.directory)

        self.assertIs(result, dataframe)
        loader_class.assert_called_once_with(
            str(self.data_file),
            data_dir=str(self.directory),
        )

    @patch("kiwi_scan.export.loader.DataLoader")
    def test_passes_none_when_no_data_directory_is_given(self, loader_class):
        dataframe = pd.DataFrame({"Position": [0.0]})
        loader_class.return_value.load_data.return_value = dataframe

        result = _load_scan_dataframe(self.data_file)

        self.assertIs(result, dataframe)
        loader_class.assert_called_once_with(str(self.data_file), data_dir=None)

    @patch("kiwi_scan.export.loader.DataLoader")
    def test_rejects_loader_failure_and_empty_dataframe(self, loader_class):
        loader_class.return_value.load_data.side_effect = [
            None,
            pd.DataFrame(columns=["Position"]),
        ]

        with self.assertRaisesRegex(FileNotFoundError, "Could not load"):
            _load_scan_dataframe(self.data_file)
        with self.assertRaisesRegex(EmptyScanDataError, "no data rows"):
            _load_scan_dataframe(self.data_file)

    @patch("kiwi_scan.export.loader.DataLoader")
    def test_rejects_comment_only_file_before_data_loader(self, loader_class):
        empty_file = self.write_file("empty.dat", "# header only\n")

        with self.assertRaisesRegex(EmptyScanDataError, "is empty"):
            _load_scan_dataframe(empty_file)

        loader_class.assert_not_called()


class TestLoadMetadata(TemporaryDirectoryTestCase):
    @patch("kiwi_scan.export.loader.parse_metadata_file")
    def test_skips_disabled_missing_and_unspecified_metadata(self, parse_metadata):
        missing = self.directory / "missing-metadata.dat"

        self.assertIsNone(_load_metadata(missing, include_metadata=False))
        self.assertIsNone(_load_metadata(None, include_metadata=True))
        with self.assertLogs("kiwi_scan.export.loader", level="WARNING"):
            self.assertIsNone(_load_metadata(missing, include_metadata=True))
        parse_metadata.assert_not_called()

    @patch("kiwi_scan.export.loader.parse_metadata_file")
    def test_parses_existing_metadata_file(self, parse_metadata):
        metadata_file = self.write_file("metadata.dat")
        metadata = Mock(name="metadata")
        parse_metadata.return_value = metadata

        result = _load_metadata(metadata_file, include_metadata=True)

        self.assertIs(result, metadata)
        parse_metadata.assert_called_once_with(str(metadata_file))


class TestLoadExportBundleFromScanFile(TemporaryDirectoryTestCase):
    def test_rejects_missing_scan_file(self):
        missing = self.directory / "missing.dat"

        with self.assertRaisesRegex(FileNotFoundError, "not found"):
            load_export_bundle_from_scan_file(missing)

    @patch("kiwi_scan.export.loader._load_metadata")
    @patch("kiwi_scan.export.loader._load_scan_dataframe")
    def test_builds_single_scan_bundle_with_explicit_fields(
        self,
        load_dataframe,
        load_metadata,
    ):
        data_file = self.write_file("scan.dat")
        metadata_file = self.write_file("metadata.dat")
        dataframe = pd.DataFrame({"Position": [1.0]})
        metadata = Mock(name="metadata")
        load_dataframe.return_value = dataframe
        load_metadata.return_value = metadata

        bundle = load_export_bundle_from_scan_file(
            data_file,
            metadata_file=metadata_file,
            scan_id="scan-17",
            scan_type="linear",
            include_metadata=False,
        )

        self.assertEqual(len(bundle.scans), 1)
        scan = bundle.scans[0]
        self.assertEqual(scan.scan_id, "scan-17")
        self.assertEqual(scan.scan_type, "linear")
        self.assertEqual(scan.data_file, data_file)
        self.assertEqual(scan.metadata_file, metadata_file)
        self.assertIs(scan.data, dataframe)
        self.assertIs(scan.metadata, metadata)
        self.assertIsNone(scan.created_at)
        self.assertEqual(scan.manifest_entry, {})
        load_dataframe.assert_called_once_with(data_file, data_file.parent)
        load_metadata.assert_called_once_with(metadata_file, False)

    @patch("kiwi_scan.export.loader._load_metadata", return_value=None)
    @patch("kiwi_scan.export.loader._load_scan_dataframe")
    def test_uses_filename_stem_as_default_scan_id(
        self,
        load_dataframe,
        _load_metadata,
    ):
        data_file = self.write_file("default-id.dat")
        load_dataframe.return_value = pd.DataFrame({"Position": [1.0]})

        bundle = load_export_bundle_from_scan_file(data_file)

        self.assertEqual(bundle.scans[0].scan_id, "default-id")
        self.assertIsNone(bundle.scans[0].metadata_file)


class TestLoadExportBundleFromManifest(TemporaryDirectoryTestCase):
    def setUp(self):
        super().setUp()
        self.manifest_file = self.write_file("manifest.yaml", "manifest: {}\n")

    @patch("kiwi_scan.export.loader.ManifestResolver.load_manifest")
    def test_rejects_invalid_scans_section(self, load_manifest):
        load_manifest.return_value = {"scans": {"invalid": "mapping"}}

        with self.assertRaisesRegex(TypeError, "expected a list"):
            load_export_bundle_from_manifest(self.manifest_file)

    @patch("kiwi_scan.export.loader._load_metadata")
    @patch("kiwi_scan.export.loader._load_scan_dataframe")
    @patch("kiwi_scan.export.loader.ManifestResolver.load_manifest")
    def test_loads_manifest_entries_in_order_and_skips_invalid_entry(
        self,
        load_manifest,
        load_dataframe,
        load_metadata,
    ):
        first_data = self.write_file("run/first.dat")
        first_metadata = self.write_file("run/first-metadata.dat")
        second_data = self.write_file("second.dat")
        first_frame = pd.DataFrame({"Position": [1.0]})
        second_frame = pd.DataFrame({"Position": [2.0]})
        load_dataframe.side_effect = [first_frame, second_frame]
        first_metadata_result = Mock(name="first_metadata")
        load_metadata.side_effect = [first_metadata_result, None]
        first_entry = {
            "id": "scan-a",
            "scan_type": "linear",
            "path": "run",
            "data_file": "first.dat",
            "metadata_file": "first-metadata.dat",
            "created_at": "2026-08-26T08:30:00Z",
        }
        second_entry = {
            "data_file": "second.dat",
            "created_at": "invalid",
        }
        load_manifest.return_value = {
            "manifest": {"title": "commissioning"},
            "scans": [first_entry, "invalid-entry", second_entry],
        }

        with self.assertLogs("kiwi_scan.export.loader", level="WARNING"):
            bundle = load_export_bundle_from_manifest(self.manifest_file)

        self.assertEqual(
            [scan.scan_id for scan in bundle.scans],
            ["scan-a", "scan_3"],
        )
        self.assertEqual(bundle.manifest_file, self.manifest_file)
        self.assertEqual(bundle.manifest_header, {"title": "commissioning"})
        first_scan, second_scan = bundle.scans
        self.assertEqual(first_scan.data_file, first_data)
        self.assertEqual(first_scan.metadata_file, first_metadata)
        self.assertIs(first_scan.data, first_frame)
        self.assertIs(first_scan.metadata, first_metadata_result)
        self.assertEqual(
            first_scan.created_at,
            datetime(2026, 8, 26, 8, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(first_scan.manifest_entry, first_entry)
        self.assertEqual(second_scan.data_file, second_data)
        self.assertIs(second_scan.data, second_frame)
        self.assertIsNone(second_scan.created_at)
        self.assertEqual(
            load_dataframe.call_args_list,
            [
                call(first_data, first_data.parent),
                call(second_data, second_data.parent),
            ],
        )

    @patch("kiwi_scan.export.loader.ManifestResolver.load_manifest")
    def test_missing_data_file_raises_by_default(self, load_manifest):
        load_manifest.return_value = {
            "scans": [{"id": "missing", "data_file": "missing.dat"}],
        }

        with self.assertRaisesRegex(FileNotFoundError, "missing data file"):
            load_export_bundle_from_manifest(self.manifest_file)

    @patch("kiwi_scan.export.loader.ManifestResolver.load_manifest")
    def test_missing_data_file_can_be_skipped(self, load_manifest):
        load_manifest.return_value = {
            "scans": [{"id": "missing", "data_file": "missing.dat"}],
        }

        with self.assertLogs("kiwi_scan.export.loader", level="WARNING"):
            bundle = load_export_bundle_from_manifest(
                self.manifest_file,
                skip_missing_data=True,
            )

        self.assertEqual(bundle.scans, [])

    @patch("kiwi_scan.export.loader._load_scan_dataframe")
    @patch("kiwi_scan.export.loader.ManifestResolver.load_manifest")
    def test_empty_data_file_is_skipped(self, load_manifest, load_dataframe):
        empty_data = self.write_file("empty.dat", "# no rows\n")
        load_manifest.return_value = {
            "scans": [{"id": "empty", "data_file": empty_data}],
        }
        load_dataframe.side_effect = EmptyScanDataError("empty")

        with self.assertLogs("kiwi_scan.export.loader", level="WARNING"):
            bundle = load_export_bundle_from_manifest(self.manifest_file)

        self.assertEqual(bundle.scans, [])
        load_dataframe.assert_called_once_with(empty_data, empty_data.parent)

    @patch("kiwi_scan.export.loader._load_scan_dataframe")
    @patch("kiwi_scan.export.loader.ManifestResolver.load_manifest")
    def test_non_directory_entry_path_does_not_hide_absolute_data_file(
        self,
        load_manifest,
        load_dataframe,
    ):
        data_file = self.write_file("absolute.dat")
        dataframe = pd.DataFrame({"Position": [1.0]})
        load_dataframe.return_value = dataframe
        load_manifest.return_value = {
            "scans": [
                {
                    "path": "missing-directory",
                    "data_file": data_file,
                }
            ],
        }

        bundle = load_export_bundle_from_manifest(
            self.manifest_file,
            include_metadata=False,
        )

        self.assertEqual(bundle.scans[0].data_file, data_file)


class TestLoadExportBundleFromLatestManifest(unittest.TestCase):
    @patch("kiwi_scan.export.loader.load_export_bundle_from_manifest")
    @patch("kiwi_scan.export.loader.ManifestResolver")
    def test_selects_manifest_and_forwards_options(
        self,
        resolver_class,
        load_manifest,
    ):
        manifest_path = Path("/scan-data/manifest-3.yaml")
        expected_bundle = Mock(name="bundle")
        resolver_class.return_value.select_manifest.return_value = manifest_path
        load_manifest.return_value = expected_bundle

        result = load_export_bundle_from_latest_manifest(
            data_dir=Path("/scan-data"),
            manifest_index=3,
            include_metadata=False,
            skip_missing_data=True,
        )

        self.assertIs(result, expected_bundle)
        resolver_class.assert_called_once_with("/scan-data")
        resolver_class.return_value.select_manifest.assert_called_once_with(3)
        load_manifest.assert_called_once_with(
            manifest_path,
            include_metadata=False,
            skip_missing_data=True,
        )


if __name__ == "__main__":
    unittest.main()
