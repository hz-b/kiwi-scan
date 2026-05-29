# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""
pythonSoftIOC wrapper for the generic kiwi-scan IOC API. This is the only file that references softioc.
Install kiwi-scan[ioc]  for this module!
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Callable, Dict, Optional, Sequence

from kiwi_scan.datamodels import ScanDimension

from .controller import ScanIOCController
from .datamodels import DataPVSpec, ScanIOCStatus

logger = logging.getLogger(__name__)

_EPICS_STRING_VALUE_MAX_BYTES = 39


def _fit_epics_string(value: Any, *, max_bytes: int = _EPICS_STRING_VALUE_MAX_BYTES) -> str:
    """Return a string that fits into string records."""
    text = "" if value is None else str(value)
    raw = text.encode("utf-8")
    if len(raw) <= max_bytes:
        return text
    return raw[:max_bytes].decode("utf-8", "ignore")

def _output_file_record_value(path: str) -> str:
    """Return the short value of outputfile."""
    if not path:
        return ""
    return _fit_epics_string(os.path.basename(str(path)))

def _load_builder_module() -> Any:
    try:
        from softioc import builder
    except Exception as exc:
        raise RuntimeError(
            "pythonSoftIOC is required to instantiate GenericScanIOC. "
            "Install kiwi-scan[ioc]."
        ) from exc
    return builder

def _load_dispatcher() -> Any:
    try:
        from softioc import asyncio_dispatcher
    except Exception as exc:
        raise RuntimeError(
            "pythonSoftIOC asyncio_dispatcher is required to instantiate GenericScanIOC."
            "Install kiwi-scan[ioc]."
        ) from exc
    return asyncio_dispatcher.AsyncioDispatcher()


def _load_softioc_module() -> Any:
    try:
        from softioc import softioc
    except Exception as exc:
        raise RuntimeError(
            "pythonSoftIOC is required to run GenericScanIOC. "
            "Install kiwi-scan[ioc]."
        ) from exc
    return softioc


class GenericScanIOC():
    def __init__(
        self,
        *,
        prefix: str = "KIWI:SCAN:",
        controller: ScanIOCController,
        data_pvs: Optional[Sequence[DataPVSpec]] = None,
        publish_period: float = 1.0,
        builder_module: Any = None,
        dispatcher: Any = None,
        start_publish_loop: bool = True,
    ) -> None:
        
        self.controller = controller
        self.prefix = self._normalize_prefix(prefix)
        self.publish_period = max(0.05, float(publish_period))
        self.data_pvs = list(data_pvs or [])
        self._builder = builder_module or _load_builder_module()
        if dispatcher is not None:
            self._disp = dispatcher
        elif not hasattr(self, "_disp"):
            self._disp = _load_dispatcher()

        self._pvs: Dict[str, Any] = {}
        self._data_records: Dict[str, Any] = {}
        self._scan_task: Any = None
        self._publish_task: Any = None
        self._database_loaded = False
        self._publish_counter = 0

        logger.info(
            "Initializing GenericScanIOC prefix=%s publish_period=%.3f data_pvs=%d",
            self.prefix,
            self.publish_period,
            len(self.data_pvs),
        )
        self._builder.PushPrefix(self.prefix)
        self._register_records()
        self.publish_once()

        if start_publish_loop:
            self._start_publish_loop()

    @staticmethod
    def _normalize_prefix(prefix: str) -> str:
        prefix = str(prefix or "")
        if prefix and not prefix.endswith(":"):
            return prefix + ":"
        return prefix

    def _loop(self) -> asyncio.AbstractEventLoop:
        loop = getattr(getattr(self, "_disp", None), "loop", None)
        if loop is not None:
            return loop
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.get_event_loop()

    # -------------------------
    # Record creation

    def _register_records(self) -> None:
        actuator, start, stop, steps, velocity = self.controller.dimension_defaults()
        logger.info(
            "Registering IOC records with default dimension actuator=%s start=%s stop=%s steps=%s velocity=%s",
            actuator,
            start,
            stop,
            steps,
            velocity,
        )

        self._pvs["Status"] = self._mbb_in(
            "Status",
            ScanIOCStatus.IDLE.value,
            ZRST="idle",
            ONST="running",
            TWST="initializing",
            THST="error",
        )
        self._pvs["Start"] = self._bool_out("Start", False, self._on_start_update)
        self._pvs["Stop"] = self._bool_out("Stop", False, self._on_stop_update)
        self._pvs["Busy"] = self._bool_in("Busy", False)
        self._pvs["Message"] = self._string_in("Message", "idle")
        self._pvs["OutputFile"] = self._string_in("OutputFile", "")
        self._pvs["DataWritingEnabled"] = self._bool_out(
            "DataWritingEnabled",
            self.controller.get_data_writing_enabled(),
            self._on_data_writing_update,
        )
        self._pvs["Position"] = self._float_in("Position", float("nan"), PREC=6, MDEL=-1)
        self._pvs["Actuator"] = self._string_out("Actuator", actuator)
        self._pvs["StartPos"] = self._float_out("StartPos", start)
        self._pvs["StopPos"] = self._float_out("StopPos", stop)
        self._pvs["Steps"] = self._int_out("Steps", steps)
        self._pvs["Velocity"] = self._float_out("Velocity", velocity)

        for spec in self.data_pvs:
            record_name = self._data_record_name(spec.local_name)
            logger.info(
                "Registering IOC data record %s from scan key %s as type=%s",
                record_name,
                spec.key,
                spec.value_type,
            )
            if spec.value_type == "float":
                rec = self._float_in(record_name, spec.default_value(), PREC=6, MDEL=-1)
            elif spec.value_type == "int":
                rec = self._int_in(record_name, spec.default_value())
            elif spec.value_type == "bool":
                rec = self._bool_in(record_name, spec.default_value())
            else:
                rec = self._string_in(record_name, spec.default_value())
            self._data_records[spec.local_name] = rec

        logger.info("Registered %d control records and %d data records", len(self._pvs), len(self._data_records))

    @staticmethod
    def _data_record_name(local_name: str) -> str:
        return local_name if ":" in local_name else "DATA:" + local_name

    def _mk_record(self, fn_name: str, name: str, initial_value: Any, **kwargs: Any) -> Any:
        """ 
        Create one IOC input/status record via the pythonSoftIOC builder.
        ----------  Parameters:
        fn_name:
            Name of the pythonSoftIOC builder factory function to call (aIn, boolIn, mbbIn).
        name:
            IOC record name suffix without IOC prefix.
        initial_value:
            Initial record value passed to the builder.
        **kwargs:
            Additional keyword arguments forwarded directly to the builder (e.g. PREC)
        TODO: on_update
        """
        logger.debug("Creating IOC input record %s via %s initial=%r kwargs=%r", name, fn_name, initial_value, kwargs)
        fn = getattr(self._builder, fn_name)
        return fn(name, initial_value=initial_value, DESC=name, **kwargs)

    def _mk_out_record(
        self,
        fn_name: str,
        name: str,
        initial_value: Any,
        on_update: Optional[Callable[[Any], Any]] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Create one IOC output/control record via the pythonSoftIOC builder
        ----------  Parameters:
        fn_name:
            Name of the pythonSoftIOC builder factory function to call (aOut, boolOut, longOut).
        name:
            IOC record name suffix without IOC prefix.
        initial_value:
            Initial record value passed to the builder.
        on_update:
            Optional callback executed whenever the record value changes.
            Typically used to trigger scan start/stop actions or runtime configuration changes.
        **kwargs:
            Additional keyword arguments forwarded directly to the builder (e.g. PREC)
        """
        logger.debug("Creating IOC output record %s via %s initial=%r kwargs=%r", name, fn_name, initial_value, kwargs)
        fn = getattr(self._builder, fn_name)
        if on_update is None:
            return fn(name, initial_value=initial_value, DESC=name, **kwargs)
        return fn(name, initial_value=initial_value, on_update=on_update, DESC=name, **kwargs)

    def _float_in(self, name: str, initial: Any, **kwargs: Any) -> Any:
        opts = {"PREC": 6, "MDEL": -1}
        opts.update(kwargs)
        return self._mk_record("aIn", name, initial, **opts)

    def _float_out(self, name: str, initial: Any, **kwargs: Any) -> Any:
        opts = {"PREC": 6}
        opts.update(kwargs)
        return self._mk_out_record("aOut", name, initial, **opts)

    def _int_in(self, name: str, initial: Any, **kwargs: Any) -> Any:
        return self._mk_record("longIn", name, int(initial), **kwargs)

    def _int_out(self, name: str, initial: Any, **kwargs: Any) -> Any:
        return self._mk_out_record("longOut", name, int(initial), **kwargs)

    def _string_in(self, name: str, initial: Any, **kwargs: Any) -> Any:
        return self._mk_record("stringIn", name, str(initial), **kwargs)

    def _string_out(self, name: str, initial: Any, **kwargs: Any) -> Any:
        return self._mk_out_record("stringOut", name, str(initial), **kwargs)

    def _bool_in(self, name: str, initial: Any, **kwargs: Any) -> Any:
        opts = {"ZNAM": "False", "ONAM": "True"}
        opts.update(kwargs)
        return self._mk_record("boolIn", name, bool(initial), **opts)

    def _bool_out(
        self,
        name: str,
        initial: Any,
        on_update: Optional[Callable[[Any], Any]] = None,
        **kwargs: Any,
    ) -> Any:
        opts = {"ZNAM": "False", "ONAM": "True"}
        opts.update(kwargs)
        return self._mk_out_record("boolOut", name, bool(initial), on_update=on_update, **opts)

    def _mbb_in(self, name: str, initial: Any, **kwargs: Any) -> Any:
        return self._mk_record("mbbIn", name, int(initial), **kwargs)

    def runIOC(self) -> None:
        """Load the softIOC database, initialize Channel Access, and run asyncio."""
        softioc = _load_softioc_module()

        if not self._database_loaded:
            logger.info("Loading softIOC database for prefix=%s", self.prefix)
            self._builder.LoadDatabase()
            self._database_loaded = True

        logger.info("Calling softioc.iocInit for GenericScanIOC")
        softioc.iocInit(self._disp)

        loop = self._loop()
        try:
            if loop.is_running():
                logger.info("GenericScanIOC dispatcher loop is already running; entering interactive IOC shell")
                softioc.interactive_ioc(globals())
                # FIXME: CTRL+D clean exit (segfault)
            else:
                logger.info("Starting GenericScanIOC dispatcher event loop")
                loop.run_forever()
        except KeyboardInterrupt:
            logger.info("GenericScanIOC interrupted")

    # -----------------------------------------------
    # Record update handler

    def _read_dimension_from_pvs(self) -> ScanDimension:
        dim = self.controller.make_dimension(
            actuator=self._pvs["Actuator"].get(),
            start=self._pvs["StartPos"].get(),
            stop=self._pvs["StopPos"].get(),
            steps=self._pvs["Steps"].get(),
            velocity=self._pvs["Velocity"].get(),
        )
        logger.info(
            "Read IOC scan dimension actuator=%s start=%s stop=%s steps=%s velocity=%s",
            dim.actuator,
            dim.start,
            dim.stop,
            dim.steps,
            getattr(dim, "velocity", 0.0),
        )
        return dim

    async def _on_start_update(self, value: Any) -> None:
        logger.debug("Start record update value=%r", value)
        if not bool(value):
            return
        try:
            dim = self._read_dimension_from_pvs()
            self.controller.start([dim])
            self._pvs["Start"].set(False)
            loop = self._loop()
            self._scan_task = loop.create_task(self._run_scan_task())
            logger.info("Submitted IOC scan task")
        except Exception as exc:
            logger.exception("Failed to start scan")
            self.controller.status = ScanIOCStatus.ERROR
            self.controller.message = str(exc)
            self._pvs["Start"].set(False)
            self.publish_once()

    async def _on_stop_update(self, value: Any) -> None:
        logger.debug("Stop record update value=%r", value)
        if not bool(value):
            return
        try:
            self.controller.stop()
        except Exception:
            logger.exception("Stop request failed")
        finally:
            self._pvs["Stop"].set(False)
            self.publish_once()

    async def _on_data_writing_update(self, value: Any) -> None:
        logger.info("DataWritingEnabled record update value=%r", value)
        self.controller.set_data_writing_enabled(bool(value))
        self.publish_once()

    async def _run_scan_task(self) -> None:
        loop = self._loop()
        logger.info("Running IOC scan in executor")
        try:
            await loop.run_in_executor(None, self.controller.execute_current_scan)
        except Exception as exc:
            logger.exception("Scan execution failed: %s", exc)
        finally:
            logger.info("IOC scan task finished; publishing final state")
            self.publish_once()

    def _start_publish_loop(self) -> None:
        loop = self._loop()
        logger.info("Starting IOC publish loop period=%.3fs", self.publish_period)
        try:
            self._publish_task = asyncio.run_coroutine_threadsafe(self._publish_loop(), loop)
        except RuntimeError:
            self._publish_task = loop.create_task(self._publish_loop())

    async def _publish_loop(self) -> None:
        while True:
            self.publish_once()
            await asyncio.sleep(self.publish_period)

    @staticmethod
    def _safe_set(record: Any, value: Any) -> None:
        try:
            record.set(value)
        except Exception:
            logger.debug("Failed to set IOC record %r", record, exc_info=True)

    def publish_once(self) -> None:
        """Publish the current controller state into IOC records once."""
        self._publish_counter += 1
        status = int(self.controller.status)
        busy = self.controller.get_busy()
        message = _fit_epics_string(self.controller.message or "")
        output_file = _output_file_record_value(self.controller.get_output_file())
        data_writing = bool(self.controller.get_data_writing_enabled())
        pos = self.controller.get_position(default=float("nan"))

        self._safe_set(self._pvs["Status"], status)
        self._safe_set(self._pvs["Busy"], busy)
        self._safe_set(self._pvs["Message"], message)
        self._safe_set(self._pvs["OutputFile"], output_file)
        self._safe_set(self._pvs["DataWritingEnabled"], data_writing)
        self._safe_set(self._pvs["Position"], pos)

        for spec in self.data_pvs:
            record = self._data_records.get(spec.local_name)
            if record is None:
                logger.debug("No IOC data record found for local name %s", spec.local_name)
                continue
            raw = self.controller.get_value(spec.key, default=spec.default_value())
            self._safe_set(record, spec.cast(raw))

        if self._publish_counter == 1 or self._publish_counter % 30 == 0 or busy:
            logger.info(
                "Published IOC state #%d status=%s busy=%s pos=%s data_writing=%s output_file=%s message=%s",
                self._publish_counter,
                ScanIOCStatus(status).name if status in [s.value for s in ScanIOCStatus] else status,
                busy,
                pos,
                data_writing,
                output_file,
                message,
            )
