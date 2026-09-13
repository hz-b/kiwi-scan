# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Pluggable detector acquisition strategies for scan engines."""

from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from typing import Any, Callable, List, Optional, Protocol, Sequence, Tuple

logger = logging.getLogger(__name__)


class DetectorPV(Protocol):
    """Detector-PV operations required by the built-in strategies."""

    pvname: str

    def get_with_metadata(self, *, use_monitor: bool) -> Optional[Any]:
        """Return the current value and metadata."""
        ...

    def add_callback(
        self,
        callback: Callable[..., None],
        *,
        run_now: bool = False,
    ) -> int:
        """Register a monitor callback and return its identifier."""
        ...

    def remove_callback(self, callback_id: int) -> None:
        """Remove one callback previously returned by ``add_callback``."""
        ...

class DetectorReadStrategy(ABC):
    """Strategy interface used by :class:`DetectorReader`."""

    @abstractmethod
    def start(self) -> None:
        """Prepare acquisition resources."""

    @abstractmethod
    def read(self) -> List[Any]:
        """Return one ordered reading per configured detector."""

    @abstractmethod
    def stop(self) -> None:
        """Release acquisition resources."""


class DirectDetectorReadStrategy(DetectorReadStrategy):
    """Read every detector through the established synchronous wrapper API."""

    def __init__(
        self,
        detector_pvs: Sequence[DetectorPV],
        *,
        use_monitor: bool,
    ) -> None:
        self._detector_pvs = tuple(detector_pvs)
        self._use_monitor = use_monitor

    def start(self) -> None:
        """Direct acquisition does not own additional resources."""

    def read(self) -> List[Any]:
        readings: List[Any] = []
        for pv in self._detector_pvs:
            try:
                reading = pv.get_with_metadata(
                    use_monitor=self._use_monitor
                )
                if reading is None:
                    logger.warning("Received None for PV %s", pv.pvname)
                readings.append(reading)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Failed to read metadata for PV %s, %s",
                    pv.pvname,
                    exc,
                )
                readings.append(None)
        return readings

    def stop(self) -> None:
        """Direct acquisition does not own additional resources."""


class MonitorSnapshotDetectorReadStrategy(DetectorReadStrategy):
    """Return atomic snapshots of values maintained by monitor callbacks."""

    def __init__(self, detector_pvs: Sequence[DetectorPV]) -> None:
        self._detector_pvs = tuple(detector_pvs)
        self._lock = threading.RLock()
        self._lifecycle_lock = threading.RLock()
        self._latest: List[Any] = [None] * len(self._detector_pvs)
        self._callback_handles: List[Tuple[DetectorPV, int]] = []
        self._started = False
        self._generation = 0
        self._active_generation: Optional[int] = None

    def _build_callback(
        self,
        index: int,
        generation: int,
    ) -> Callable[..., None]:
        def _callback(
            pvname: Optional[str] = None,
            value: Any = None,
            **metadata: Any,
        ) -> None:
            reading = dict(metadata)
            reading["value"] = value
            reading["pvname"] = (
                pvname or self._detector_pvs[index].pvname
            )
            with self._lock:
                if (
                    self._started
                    and self._active_generation == generation
                ):
                    self._latest[index] = reading

        return _callback

    def start(self) -> None:
        with self._lifecycle_lock:
            with self._lock:
                if self._started:
                    return
                cleanup_required = bool(self._callback_handles)

            if cleanup_required:
                self.stop()

            with self._lock:
                self._latest = [None] * len(self._detector_pvs)
                self._generation += 1
                generation = self._generation
                self._active_generation = generation
                self._started = True

            try:
                for index, pv in enumerate(self._detector_pvs):
                    callback_id = pv.add_callback(
                        self._build_callback(index, generation),
                        run_now=True,
                    )
                    with self._lock:
                        self._callback_handles.append((pv, callback_id))
            except Exception:
                try:
                    self.stop()
                except Exception:
                    logger.exception(
                        "Failed to clean up detector callbacks after "
                        "startup error"
                    )
                raise

    def read(self) -> List[Any]:
        with self._lock:
            if not self._started:
                raise RuntimeError("Detector snapshot strategy is not started")
            return list(self._latest)

    def stop(self) -> None:
        with self._lifecycle_lock:
            with self._lock:
                if not self._started and not self._callback_handles:
                    return
                self._started = False
                self._active_generation = None
                callback_handles = list(self._callback_handles)
                self._callback_handles = []

            failed_handles: List[Tuple[DetectorPV, int]] = []
            first_error: Optional[Exception] = None
            for pv, callback_id in callback_handles:
                try:
                    pv.remove_callback(callback_id)
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Failed to remove detector callback for PV %s: %s",
                        pv.pvname,
                        exc,
                    )
                    failed_handles.append((pv, callback_id))
                    if first_error is None:
                        first_error = exc

            with self._lock:
                self._callback_handles = failed_handles

            if first_error is not None:
                raise RuntimeError(
                    "Failed to stop one or more detector callbacks"
                ) from first_error


class DetectorReader:
    """Facade that keeps scan engines independent of acquisition strategy."""

    def __init__(self, strategy: DetectorReadStrategy) -> None:
        self._strategy = strategy

    @property
    def strategy(self) -> DetectorReadStrategy:
        """Return the configured strategy for diagnostics and testing."""
        return self._strategy

    def start(self) -> None:
        self._strategy.start()

    def read(self) -> List[Any]:
        return self._strategy.read()

    def stop(self) -> None:
        self._strategy.stop()


def create_detector_reader(
    strategy_name: str,
    detector_pvs: Sequence[DetectorPV],
    *,
    use_monitor: bool,
) -> DetectorReader:
    """Create a detector reader for one of the built-in strategy names."""
    normalized = str(strategy_name).strip().lower().replace("-", "_")
    if normalized == "monitor_snapshot":
        normalized = "snapshot"

    if normalized == "direct":
        strategy: DetectorReadStrategy = DirectDetectorReadStrategy(
            detector_pvs,
            use_monitor=use_monitor,
        )
    elif normalized == "snapshot":
        strategy = MonitorSnapshotDetectorReadStrategy(detector_pvs)
    else:
        raise ValueError(
            f"Unknown detector reader strategy {strategy_name!r}"
        )
    return DetectorReader(strategy)


__all__ = [
    "DetectorReadStrategy",
    "DetectorReader",
    "DirectDetectorReadStrategy",
    "MonitorSnapshotDetectorReadStrategy",
    "create_detector_reader",
]
