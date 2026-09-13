"""Regression coverage for value-only plugin output."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kiwi_scan.scan._point_frame import _DetectorLayout
from kiwi_scan.scan.point_pipeline import PointPipeline


class Plugin:
    def get_headers(self, timestamps):
        if timestamps:
            raise AssertionError('Plugin timestamp headers must never be requested')
        return ['Calc', 'Plain', 'Missing']


class Provider:
    def get_headers(self, include_timestamps=False):
        return ['Provider']

    def get_values(self):
        return [9.0]

    def update_last_point(self, last, include_timestamps=False):
        last['Provider'] = 9.0


class TestPluginTimestampOutput(unittest.TestCase):
    def test_headers_rows_caches_and_writers(self):
        for enabled in (False, True):
            for fmt in ('unix', 'iso8601'):
                for route in ('active', 'legacy', 'internal_fallback'):
                    for detector_count in (0, 2):
                        with self.subTest(enabled=enabled, fmt=fmt, route=route,
                                          detector_count=detector_count):
                            self.check_output(enabled, fmt, route, detector_count)

    def check_output(self, enabled, fmt, route, detector_count):
        pipeline = PointPipeline(
            include_timestamps=enabled, timestamp_output_format=fmt,
            detector_layout=_DetectorLayout.from_headers(['D1', 'D2'][:detector_count]),
            get_plugins=lambda: [Plugin()],
        )
        pipeline.add_column_provider(Provider())
        detectors = [{'value': 2.0, 'timestamp': 1000.0}, None][:detector_count]
        metadata = {'value': 4.0, 'timestamp': 1001.0}
        plugins = [metadata, 5.0, None]
        values = detectors + plugins
        prefix = 'TS-UNIX' if fmt == 'unix' else 'TS-ISO8601'
        expected_headers = ['Position', 'Provider', prefix]
        expected_row = [1.0, 9.0, 1002.0]
        expected_indices = {2}
        for name, value, ts in [('D1', 2.0, 1000.0), ('D2', None, None)][:detector_count]:
            expected_headers.append(name)
            expected_row.append(value)
            if enabled:
                expected_headers.append(prefix + '-' + name)
                expected_indices.add(len(expected_row))
                expected_row.append(ts)
        expected_headers.extend(['Calc', 'Plain', 'Missing'])
        expected_row.extend([4.0, 5.0, None])
        self.assertEqual(pipeline.build_output_headers(), expected_headers)
        self.assertEqual(pipeline.build_plugin_headers(True), ['Calc', 'Plain', 'Missing'])
        self.assertEqual(pipeline.build_output_row_values(
            1.0, values, line_timestamp=1002.0), expected_row)
        with patch('kiwi_scan.scan.point_pipeline.time.time', return_value=1002.0):
            if route == 'active':
                pipeline.begin_point_frame(idx=0, pos=1.0, values=detectors)
                pipeline.append_plugin_point_values(['Calc', 'Plain'], plugins[:2])
                pipeline.append_plugin_point_values(['Missing'], plugins[2:])
                self.assertEqual(pipeline.get_current_row_value('Calc'), 4.0)
                self.assertEqual(pipeline.get_current_row_value('TS-Calc'), 1001.0)
                point = pipeline.prepare_internal_point(1.0, values, enabled)
            elif route == 'legacy':
                point = pipeline.prepare_legacy_point(1.0, values, enabled)
            else:
                point = pipeline.prepare_internal_point(1.0, values, enabled)
        self.assertEqual(list(point.row_values), expected_row)
        self.assertEqual(point.timestamp_indices, expected_indices)
        self.assertEqual(pipeline.get_value('TS'), 1002.0)
        self.assertIs(pipeline.get_value('Calc', with_metadata=True), metadata)
        with tempfile.TemporaryDirectory() as directory:
            sync = str(Path(directory) / 'sync.tsv')
            asynchronous = str(Path(directory) / 'async.tsv')
            pipeline.persist_prepared_point_sync(sync, point)
            try:
                pipeline.persist_prepared_point_async(asynchronous, point)
            finally:
                pipeline.stop_parallel_writer()
            sync_text = Path(sync).read_text()
            self.assertEqual(sync_text, Path(asynchronous).read_text())
            columns = sync_text.rstrip('\n').split('\t')
            self.assertEqual(len(columns), len(expected_headers))
            self.assertEqual(columns[-3:], ['4.000000000000e+00', '5.000000000000e+00', ''])
            for index in expected_indices:
                if expected_row[index] is None:
                    self.assertEqual(columns[index], '')
                elif fmt == 'unix':
                    self.assertEqual(float(columns[index]), expected_row[index])
                else:
                    from datetime import datetime
                    self.assertEqual(datetime.fromisoformat(columns[index]).timestamp(), expected_row[index])


if __name__ == '__main__':
    unittest.main()
