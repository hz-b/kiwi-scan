# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Helpers for loading actuators and running actuator monitors."""

from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from kiwi_scan.actuator.factory import create_actuators
from kiwi_scan.actuator.single import AbstractActuator, PvEvent
from kiwi_scan.datamodels import MonitorSpec
from kiwi_scan.yaml_loader import yaml_loader

logger = logging.getLogger(__name__)

MonitorSpecLike = Union[str, MonitorSpec]
MonitorCompletionCheck = Callable[[], bool]


def load_actuators(
    config_file: str,
    replacements: Optional[Mapping[str, str]] = None,
) -> Dict[str, AbstractActuator]:
    """Load actuator definitions from YAML and create actuator instances."""
    config = yaml_loader(config_file, dict(replacements or {}))
    return create_actuators(config.get("actuators") or {})


class _MonitorCounters:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seen = 0
        self._dropped = 0

    def increment_seen(self) -> None:
        with self._lock:
            self._seen += 1

    def increment_dropped(self) -> None:
        with self._lock:
            self._dropped += 1

    def get(self) -> Tuple[int, int]:
        with self._lock:
            return self._seen, self._dropped


class _EventWriter(threading.Thread):
    """ Event writer example from command line:
            actuator_runner --config-file /path/to/mono.yaml  --monitor energy:rbv --monitor-duration 0.3
            [mon#1 energy:rbv]     0.044s pv=TESTU171PGM1:monoGetEnergy value=300.00499331369656
            [mon#1 energy:rbv]     0.145s pv=TESTU171PGM1:monoGetEnergy value=300.00697949045144
            [mon#1 energy:rbv]     0.244s pv=TESTU171PGM1:monoGetEnergy value=300.0208477500313
            Done. events_seen=3 dropped=0
    """
    def __init__(self, event_queue: queue.Queue, out_path: Optional[str]) -> None:
        super().__init__(daemon=True)
        self._queue = event_queue
        self._out_path = out_path

    @staticmethod
    def _format_event(item: Mapping[str, Any]) -> str:
        return (
            "[mon#{monitor_id} {actuator}:{source}] "
            "{t_rel_s:9.3f}s pv={pvname} value={value!r}"
        ).format(**item)

    def _run_writer(self, file_handle=None) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                return

            line = self._format_event(item)
            print(line)
            if file_handle is not None:
                file_handle.write(line + "\n")

    def run(self) -> None:
        if self._out_path is None:
            self._run_writer()
            return

        with open(
            self._out_path,
            "a",
            encoding="utf-8",
            buffering=1,
        ) as file_handle:
            self._run_writer(file_handle)

    def close(self) -> None:
        self._queue.put(None)
        self.join(timeout=2.0)
        if self.is_alive():
            logger.warning("Monitor writer thread did not stop cleanly")


def _pick_monitor_provider(
    actuators: Mapping[str, AbstractActuator],
) -> AbstractActuator:
    """Return the first actuator backend that supports monitor subscriptions."""
    for actuator in actuators.values():
        if actuator.supports_monitors():
            return actuator

    raise RuntimeError("No actuator backend supports monitors in this config.")


def _as_monitor_spec(spec: MonitorSpecLike) -> MonitorSpec:
    if isinstance(spec, MonitorSpec):
        return spec
    return MonitorSpec.from_arg(spec)


def _make_monitor_callback(
    *,
    monitor_id: int,
    name: str,
    source: str,
    pvname: str,
    event_queue: queue.Queue,
    counters: _MonitorCounters,
    start_time: float,
) -> Callable[[PvEvent], None]:
    def _callback(event: PvEvent) -> None:
        now = time.time()
        payload = {
            "monitor_id": monitor_id,
            "actuator": name,
            "source": source,
            "pvname": getattr(event, "pvname", pvname),
            "value": getattr(event, "value", None),
            "t_abs_s": now,
            "t_rel_s": now - start_time,
            "timestamp": getattr(event, "timestamp", None),
            "posixseconds": getattr(event, "posixseconds", None),
            "nanoseconds": getattr(event, "nanoseconds", None),
            "severity": getattr(event, "severity", None),
            "status": getattr(event, "status", None),
            "raw": getattr(event, "raw", None),
        }

        try:
            event_queue.put_nowait(payload)
            counters.increment_seen()
        except queue.Full:
            counters.increment_dropped()

    return _callback


def _stop_monitors(
    provider: AbstractActuator,
    monitor_handles: Sequence[Tuple[str, Any]],
) -> None:
    for pvname, _handle in monitor_handles:
        try:
            provider.remove_monitor(pvname)
        except Exception:
            logger.exception("Failed to remove monitor %s during shutdown", pvname)


def _start_monitors(
    *,
    provider: AbstractActuator,
    actuators: Mapping[str, AbstractActuator],
    monitor_specs: Sequence[MonitorSpecLike],
    event_queue: queue.Queue,
    counters: _MonitorCounters,
    start_time: float,
) -> List[Tuple[str, Any]]:
    monitor_handles: List[Tuple[str, Any]] = []
    try:
        for monitor_id, raw_spec in enumerate(monitor_specs, start=1):
            monitor_spec = _as_monitor_spec(raw_spec)
            name = monitor_spec.name

            try:
                actuator = actuators[name]
            except KeyError as exc:
                raise ValueError(f"monitor refers to unknown actuator {name!r}") from exc

            pvname = monitor_spec.resolve_pv(actuator.config)
            callback = _make_monitor_callback(
                monitor_id=monitor_id,
                name=name,
                source=monitor_spec.source,
                pvname=pvname,
                event_queue=event_queue,
                counters=counters,
                start_time=start_time,
            )
            handle = provider.add_monitor(pvname, user_callback=callback)
            monitor_handles.append((pvname, handle))
    except Exception:
        _stop_monitors(provider, monitor_handles)
        raise

    logger.info("Started %d monitors via %s", len(monitor_handles), type(provider).__name__)
    return monitor_handles


def _monitor_completion_reached(
    *,
    end_time: Optional[float],
    target_count: Optional[int],
    completion_check: Optional[MonitorCompletionCheck],
    counters: _MonitorCounters,
) -> bool:
    conditions: List[bool] = []

    if completion_check is not None:
        conditions.append(bool(completion_check()))
    if end_time is not None:
        conditions.append(time.time() >= end_time)
    if target_count is not None:
        seen, _dropped = counters.get()
        conditions.append(seen >= target_count)

    return bool(conditions) and all(conditions)


def _wait_for_monitors(
    *,
    duration: Optional[float],
    count: Optional[int],
    stop_event: threading.Event,
    completion_check: Optional[MonitorCompletionCheck],
    counters: _MonitorCounters,
) -> None:
    end_time = (
        time.time() + float(duration)
        if duration is not None
        else None
    )
    target_count = int(count) if count is not None else None

    while not stop_event.is_set():
        if _monitor_completion_reached(
            end_time=end_time,
            target_count=target_count,
            completion_check=completion_check,
            counters=counters,
        ):
            return
        time.sleep(0.05)


def run_monitors(
    actuators: Mapping[str, AbstractActuator],
    monitor_specs: Sequence[MonitorSpecLike], # supports args
    *,
    duration: Optional[float] = None,
    count: Optional[int] = None,
    out_path: Optional[str] = None,
    stop_event: Optional[threading.Event] = None,
    completion_check: Optional[MonitorCompletionCheck] = None,
) -> Tuple[int, int]:
    """
    Run actuator monitor subscriptions until ``duration`` or ``count`` exit conditions. 
    ``completion_check`` lets define other conditions such as actuator motion. 
    All must be satisfied.  With no conditions, monitoring continues until ``stop_event`` is set.

    Returns ``(events_seen, events_dropped)``.
    """
    if not monitor_specs:
        return 0, 0

    active_stop_event = stop_event or threading.Event()
    provider = _pick_monitor_provider(actuators)
    event_queue: queue.Queue = queue.Queue(maxsize=10000)
    counters = _MonitorCounters()
    writer = _EventWriter(event_queue, out_path)
    monitor_handles: List[Tuple[str, Any]] = []
    start_time = time.time()

    writer.start()
    try:
        monitor_handles = _start_monitors(
            provider=provider,
            actuators=actuators,
            monitor_specs=monitor_specs,
            event_queue=event_queue,
            counters=counters,
            start_time=start_time,
        )
        _wait_for_monitors(
            duration=duration,
            count=count,
            stop_event=active_stop_event,
            completion_check=completion_check,
            counters=counters,
        )
    finally:
        _stop_monitors(provider, monitor_handles)
        writer.close()

    seen, dropped = counters.get()
    if dropped:
        logger.warning("Dropped %d monitor events (queue full).", dropped)
    return seen, dropped
