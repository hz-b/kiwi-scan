# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import asyncio
import math
import os
import sys
import unittest
from unittest.mock import patch
from typing import Any, Dict, List, Optional

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from kiwi_scan.datamodels import ActuatorConfig, ScanConfig, ScanDimension
from kiwi_scan.ioc.generic_scan_ioc import GenericScanIOC
from kiwi_scan.ioc import (
    DataPVSpec,
    ScanIOCController,
    ScanIOCStatus,
    parse_data_pv_spec,
    parse_data_pv_specs,
)


class FakeScan:
    def __init__(self, data_writing_enabled: bool = True):
        self.busyflag = False
        self.position = 1.25
        self.executed = False
        self.stopped = False
        self.data_writing_enabled = bool(data_writing_enabled)
        self.output_file = "/tmp/fake_scan.txt"
        self.values: Dict[str, Any] = {
            "Position": self.position,
            "DET:I0": 42.0,
            "StatusText": "ok",
        }

    @property
    def busy(self) -> bool:
        return self.busyflag

    def execute(self) -> None:
        self.executed = True
        self.busyflag = True
        self.position = 2.5
        self.values["Position"] = self.position
        self.values["DET:I0"] = 84.0
        self.busyflag = False

    def stop(self) -> None:
        self.stopped = True
        self.busyflag = False

    def get_value(self, name: str, *, default: Any = None, with_metadata: bool = False) -> Any:
        return self.values.get(name, default)

    def get_output_file(self) -> str:
        return self.output_file

    def set_data_writing_enabled(self, enabled: bool) -> None:
        self.data_writing_enabled = bool(enabled)

    def get_data_writing_enabled(self) -> bool:
        return self.data_writing_enabled


class FakeRecord:
    def __init__(self, name: str, initial_value: Any = None, on_update: Any = None, **kwargs: Any):
        self.name = name
        self.value = initial_value
        self.on_update = on_update
        self.kwargs = kwargs
        self.set_values: List[Any] = []

    def get(self) -> Any:
        return self.value

    def set(self, value: Any) -> None:
        self.value = value
        self.set_values.append(value)


class FakeBuilder:
    def __init__(self):
        self.prefixes: List[str] = []
        self.records: Dict[str, FakeRecord] = {}

    def PushPrefix(self, prefix: str) -> None:
        self.prefixes.append(prefix)

    def _record(self, kind: str, name: str, initial_value: Any = None, on_update: Any = None, **kwargs: Any):
        rec = FakeRecord(name=name, initial_value=initial_value, on_update=on_update, kind=kind, **kwargs)
        self.records[name] = rec
        return rec

    def aIn(self, name: str, initial_value: Any = None, **kwargs: Any):
        return self._record("aIn", name, initial_value, **kwargs)

    def aOut(self, name: str, initial_value: Any = None, on_update: Any = None, **kwargs: Any):
        return self._record("aOut", name, initial_value, on_update, **kwargs)

    def longIn(self, name: str, initial_value: Any = None, **kwargs: Any):
        return self._record("longIn", name, initial_value, **kwargs)

    def longOut(self, name: str, initial_value: Any = None, on_update: Any = None, **kwargs: Any):
        return self._record("longOut", name, initial_value, on_update, **kwargs)

    def stringIn(self, name: str, initial_value: Any = None, **kwargs: Any):
        return self._record("stringIn", name, initial_value, **kwargs)

    def stringOut(self, name: str, initial_value: Any = None, on_update: Any = None, **kwargs: Any):
        return self._record("stringOut", name, initial_value, on_update, **kwargs)

    def boolIn(self, name: str, initial_value: Any = None, **kwargs: Any):
        return self._record("boolIn", name, initial_value, **kwargs)

    def boolOut(self, name: str, initial_value: Any = None, on_update: Any = None, **kwargs: Any):
        return self._record("boolOut", name, initial_value, on_update, **kwargs)

    def mbbIn(self, name: str, initial_value: Any = None, **kwargs: Any):
        return self._record("mbbIn", name, initial_value, **kwargs)

    def mbbOut(self, name: str, initial_value: Any = None, on_update: Any = None, **kwargs: Any):
        return self._record("mbbOut", name, initial_value, on_update, **kwargs)


class FakeDispatcher:
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.loop = loop or asyncio.new_event_loop()


class TestDataPVSpec(unittest.TestCase):
    def test_parse_data_pv_with_colon_key_and_type(self):
        spec = parse_data_pv_spec("I0=ue521sgm1:liIDcics:float")
        self.assertEqual(spec.local_name, "I0")
        self.assertEqual(spec.key, "ue521sgm1:liIDcics")
        self.assertEqual(spec.value_type, "float")

    def test_parse_data_pv_without_type_keeps_full_colon_key(self):
        spec = parse_data_pv_spec("Raw=IOC:SUBSYS:PV")
        self.assertEqual(spec.local_name, "Raw")
        self.assertEqual(spec.key, "IOC:SUBSYS:PV")
        self.assertEqual(spec.value_type, "float")

    def test_parse_data_pv_string_alias(self):
        spec = parse_data_pv_spec("Time=TS-ISO8601:string")
        self.assertEqual(spec.value_type, "str")
        self.assertEqual(spec.cast(123), "123")

    def test_cast_defaults(self):
        self.assertTrue(math.isnan(DataPVSpec("F", "k", "float").cast("not-a-number")))
        self.assertEqual(DataPVSpec("I", "k", "int").cast("bad"), 0)
        self.assertTrue(DataPVSpec("B", "k", "bool").cast("yes"))
        self.assertFalse(DataPVSpec("B", "k", "bool").cast("off"))

    def test_parse_list(self):
        specs = parse_data_pv_specs(["A=PV:A", "B=PV:B:int"])
        self.assertEqual([s.local_name for s in specs], ["A", "B"])
        self.assertEqual(specs[1].value_type, "int")


class TestScanIOCController(unittest.TestCase):
    def make_base_config(
        self,
        *,
        actuator: str = "motor",
        start: float = 0.0,
        stop: float = 1.0,
        steps: int = 2,
        velocity: float = 0.0,
        data_writing_enabled: bool = True,
    ) -> ScanConfig:
        return ScanConfig(
            actuators={
                actuator: ActuatorConfig(
                    pv="SIM:%s" % actuator.upper(),
                    rb_pv="SIM:%s:RBV" % actuator.upper(),
                    type="simulation",
                    dwell_time=0.0,
                )
            },
            detector_pvs=[],
            scan_dimensions=[
                ScanDimension(actuator, start, stop, steps, velocity)
            ],
            data_dir=".",
            output_file="scan_results.txt",
            data_writing_enabled=data_writing_enabled,
        )

    def make_controller(
        self,
        created: Optional[List[Any]] = None,
        base_config: Optional[ScanConfig] = None,
        configs: Optional[Dict[str, ScanConfig]] = None,
    ):
        base_config = base_config or self.make_base_config()
        configs = dict(configs or {"unit": base_config})
        created = created if created is not None else []

        def config_loader(config_dir, replacements):
            return configs

        def scan_factory(scan_type, cfg, data_dir):
            scan = FakeScan(data_writing_enabled=cfg.data_writing_enabled)
            created.append((scan_type, cfg, data_dir, scan))
            return scan

        controller = ScanIOCController(
            scan_type="linear",
            config_name="unit",
            config_dir="/tmp/configs",
            data_dir="/tmp/data",
            replacements={},
            scan_factory=scan_factory,
            config_loader=config_loader,
        )
        return controller

    def test_dimension_defaults_from_base_config(self):
        controller = self.make_controller()
        self.assertEqual(controller.dimension_defaults(), ("motor", 0.0, 1.0, 2, 0.0))

    def test_selection_getters_return_current_named_config_and_scan_type(self):
        controller = self.make_controller()
        self.assertEqual(controller.get_config_name(), "unit")
        self.assertEqual(controller.get_scan_type(), "linear")

    def test_set_config_name_loads_named_config_and_preserves_runtime_write_flag(self):
        unit = self.make_base_config()
        alternate = self.make_base_config(
            actuator="theta",
            start=10.0,
            stop=20.0,
            steps=6,
            velocity=0.5,
            data_writing_enabled=True,
        )
        controller = self.make_controller(
            base_config=unit,
            configs={"unit": unit, "alternate": alternate},
        )
        controller.set_data_writing_enabled(False)

        controller.set_config_name("alternate")

        self.assertEqual(controller.get_config_name(), "alternate")
        self.assertEqual(
            controller.dimension_defaults(),
            ("theta", 10.0, 20.0, 6, 0.5),
        )
        self.assertFalse(controller.get_data_writing_enabled())

    def test_invalid_config_name_does_not_replace_current_selection(self):
        controller = self.make_controller()
        original_config = controller.base_config

        with self.assertRaises(KeyError):
            controller.set_config_name("missing")

        self.assertEqual(controller.get_config_name(), "unit")
        self.assertIs(controller.base_config, original_config)

    def test_config_and_scan_type_changes_are_rejected_while_active(self):
        controller = self.make_controller()
        controller.start([ScanDimension("motor", 0.0, 1.0, 2, 0.0)])

        with self.assertRaises(RuntimeError):
            controller.set_config_name("unit")
        with self.assertRaises(RuntimeError):
            controller.set_scan_type("linear")

    def test_set_scan_type_is_used_for_the_next_scan(self):
        created: List[Any] = []
        controller = self.make_controller(created=created)

        with patch(
            "kiwi_scan.ioc.controller.get_available_scan_types",
            return_value=["linear", "cm"],
        ):
            controller.set_scan_type("cm")

        controller.start([ScanDimension("motor", 0.0, 1.0, 2, 0.0)])
        self.assertEqual(controller.get_scan_type(), "cm")
        self.assertEqual(created[0][0], "cm")

    def test_start_deep_copies_base_config_and_execute_clears_scan(self):
        created: List[Any] = []
        base = self.make_base_config()
        controller = self.make_controller(created=created, base_config=base)
        dim = ScanDimension("motor", 5.0, 7.0, 3, 0.5)

        scan = controller.start([dim])
        self.assertIs(scan, created[0][3])
        self.assertEqual(controller.status, ScanIOCStatus.INITIALIZING)

        created_scan_type, created_cfg, created_data_dir, created_scan = created[0]
        self.assertEqual(created_scan_type, "linear")
        self.assertEqual(created_data_dir, "/tmp/data")
        self.assertIsNot(created_cfg, base)
        self.assertEqual(created_cfg.scan_dimensions, [dim])
        self.assertEqual(base.scan_dimensions[0].start, 0.0)
        self.assertEqual(base.scan_dimensions[0].stop, 1.0)

        controller.execute_current_scan()
        self.assertTrue(created_scan.executed)
        self.assertIsNone(controller.scan)
        self.assertEqual(controller.status, ScanIOCStatus.IDLE)
        self.assertEqual(controller.message, "idle")

    def test_start_rejects_active_scan(self):
        controller = self.make_controller()
        dim = ScanDimension("motor", 0.0, 1.0, 2, 0.0)
        controller.start([dim])
        with self.assertRaises(RuntimeError):
            controller.start([dim])

    def test_stop_forwards_to_scan(self):
        controller = self.make_controller()
        dim = ScanDimension("motor", 0.0, 1.0, 2, 0.0)
        scan = controller.start([dim])
        controller.stop()
        self.assertTrue(scan.stopped)
        self.assertEqual(controller.message, "stop requested")

    def test_data_writing_default_and_runtime_forwarding(self):
        created: List[Any] = []
        controller = self.make_controller(created=created)
        controller.set_data_writing_enabled(False)
        self.assertFalse(controller.get_data_writing_enabled())

        scan = controller.start([ScanDimension("motor", 0.0, 1.0, 2, 0.0)])
        self.assertFalse(scan.get_data_writing_enabled())
        self.assertFalse(created[0][1].data_writing_enabled)

        controller.set_data_writing_enabled(True)
        self.assertTrue(scan.get_data_writing_enabled())
        self.assertTrue(controller.get_data_writing_enabled())

    def test_getters_return_defaults_without_scan(self):
        controller = self.make_controller()
        self.assertTrue(math.isnan(controller.get_position()))
        self.assertEqual(controller.get_value("missing", default="x"), "x")
        self.assertEqual(controller.get_output_file(), "")
        self.assertFalse(controller.get_busy())


class DummyController:
    def __init__(self):
        self.status = ScanIOCStatus.RUNNING
        self.message = "running"
        self.data_writing_enabled = True
        self.config_name = "unit"
        self.scan_type = "linear"
        self._dimension_defaults = ("motor", 1.0, 2.0, 5, 0.25)
        self.started_dims: List[ScanDimension] = []
        self.stopped = False
        self.values = {"DET:I0": 123.4, "StatusText": "good", "Flag": 1}

    def dimension_defaults(self):
        return self._dimension_defaults

    def get_config_name(self):
        return self.config_name

    def set_config_name(self, config_name):
        config_name = str(config_name).strip()
        if config_name == "missing":
            raise KeyError("unknown config: missing")
        self.config_name = config_name
        if config_name == "alternate":
            self._dimension_defaults = ("theta", 3.0, 9.0, 7, 0.5)
        self.message = "config selected: %s" % config_name

    def get_scan_type(self):
        return self.scan_type

    def set_scan_type(self, scan_type):
        scan_type = str(scan_type).strip()
        if scan_type == "missing":
            raise ValueError("unknown scan type: missing")
        self.scan_type = scan_type
        self.message = "scan type selected: %s" % scan_type

    def get_busy(self):
        return True

    def get_output_file(self):
        return "/tmp/out.txt"

    def get_data_writing_enabled(self):
        return self.data_writing_enabled

    def set_data_writing_enabled(self, enabled):
        self.data_writing_enabled = bool(enabled)

    def get_position(self, default=float("nan")):
        return 7.5

    def get_value(self, key, default=None):
        return self.values.get(key, default)

    def make_dimension(self, **kwargs):
        return ScanIOCController.make_dimension(**kwargs)

    def start(self, dims):
        self.started_dims = list(dims)

    def stop(self):
        self.stopped = True

    def execute_current_scan(self):
        self.status = ScanIOCStatus.IDLE
        self.message = "idle"


class TestGenericScanIOC(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.addCleanup(self.loop.close)

    def make_ioc(self, controller=None, data_pvs=None):
        builder = FakeBuilder()
        ioc = GenericScanIOC(
            prefix="KIWI:TEST",
            controller=controller or DummyController(),
            data_pvs=data_pvs or [],
            builder_module=builder,
            dispatcher=FakeDispatcher(self.loop),
            start_publish_loop=False,
        )
        return ioc, builder

    def test_registers_fixed_records_with_normalized_prefix(self):
        _ioc, builder = self.make_ioc()
        self.assertEqual(builder.prefixes, ["KIWI:TEST"])
        for name in (
            "Status",
            "Start",
            "Stop",
            "Busy",
            "Message",
            "OutputFile",
            "DataWritingEnabled",
            "LogLevel",
            "kill",
            "Position",
            "Config",
            "ScanType",
            "Actuator",
            "StartPos",
            "StopPos",
            "Steps",
            "Velocity",
        ):
            self.assertIn(name, builder.records)

    def test_publish_once_updates_status_position_output_and_data_pvs(self):
        data_pvs = [
            DataPVSpec("I0", "DET:I0", "float"),
            DataPVSpec("State", "StatusText", "str"),
            DataPVSpec("Flag", "Flag", "bool"),
        ]
        ioc, builder = self.make_ioc(data_pvs=data_pvs)
        ioc.publish_once()

        self.assertEqual(builder.records["Status"].value, ScanIOCStatus.RUNNING.value)
        self.assertTrue(builder.records["Busy"].value)
        self.assertEqual(builder.records["Message"].value, "running")
        self.assertEqual(builder.records["OutputFile"].value, "out.txt")
        self.assertEqual(builder.records["Position"].value, 7.5)
        self.assertEqual(builder.records["DATA:I0"].value, 123.4)
        self.assertEqual(builder.records["DATA:State"].value, "good")
        self.assertTrue(builder.records["DATA:Flag"].value)

    def test_read_dimension_from_pvs(self):
        ioc, builder = self.make_ioc()
        builder.records["Actuator"].set("motor")
        builder.records["StartPos"].set(10.0)
        builder.records["StopPos"].set(11.0)
        builder.records["Steps"].set(4)
        builder.records["Velocity"].set(0.75)

        dim = ioc._read_dimension_from_pvs()
        self.assertEqual(dim, ScanDimension("motor", 10.0, 11.0, 4, 0.75))

    def test_config_callback_updates_selection_and_dimension_defaults(self):
        controller = DummyController()
        ioc, builder = self.make_ioc(controller=controller)

        self.loop.run_until_complete(ioc._on_config_update("alternate"))

        self.assertEqual(controller.config_name, "alternate")
        self.assertEqual(builder.records["Config"].value, "alternate")
        self.assertEqual(builder.records["Actuator"].value, "theta")
        self.assertEqual(builder.records["StartPos"].value, 3.0)
        self.assertEqual(builder.records["StopPos"].value, 9.0)
        self.assertEqual(builder.records["Steps"].value, 7)
        self.assertEqual(builder.records["Velocity"].value, 0.5)

    def test_invalid_config_callback_restores_accepted_record_value(self):
        controller = DummyController()
        ioc, builder = self.make_ioc(controller=controller)
        builder.records["Config"].set("missing")

        self.loop.run_until_complete(ioc._on_config_update("missing"))

        self.assertEqual(controller.config_name, "unit")
        self.assertEqual(builder.records["Config"].value, "unit")
        self.assertIn("unknown config", controller.message)

    def test_scan_type_callback_updates_selection(self):
        controller = DummyController()
        ioc, builder = self.make_ioc(controller=controller)

        self.loop.run_until_complete(ioc._on_scan_type_update("cm"))

        self.assertEqual(controller.scan_type, "cm")
        self.assertEqual(builder.records["ScanType"].value, "cm")

    def test_invalid_scan_type_callback_restores_accepted_record_value(self):
        controller = DummyController()
        ioc, builder = self.make_ioc(controller=controller)
        builder.records["ScanType"].set("missing")

        self.loop.run_until_complete(ioc._on_scan_type_update("missing"))

        self.assertEqual(controller.scan_type, "linear")
        self.assertEqual(builder.records["ScanType"].value, "linear")
        self.assertIn("unknown scan type", controller.message)

    def test_stop_callback_forwards_to_controller_and_resets_record(self):
        controller = DummyController()
        ioc, builder = self.make_ioc(controller=controller)
        self.loop.run_until_complete(ioc._on_stop_update(True))
        self.assertTrue(controller.stopped)
        self.assertFalse(builder.records["Stop"].value)

    def test_data_writing_callback_forwards_to_controller(self):
        controller = DummyController()
        ioc, builder = self.make_ioc(controller=controller)
        self.loop.run_until_complete(ioc._on_data_writing_update(False))
        self.assertFalse(controller.data_writing_enabled)
        self.assertFalse(builder.records["DataWritingEnabled"].value)


if __name__ == "__main__":
    unittest.main(verbosity=2)
