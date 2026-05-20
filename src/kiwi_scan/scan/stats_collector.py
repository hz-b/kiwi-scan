# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

from dataclasses import dataclass
import logging
import math
import re
import threading
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from kiwi_scan import stats
from kiwi_scan.actuator.single import PvEvent
from kiwi_scan.datamodels import SubscriptionConfig

# StatsCollector and helpers
logger = logging.getLogger(__name__)

_PREFIX_CLEAN_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class StatsSnapshot:
    """Immutable representation of one subscription statistics snapshot."""

    mean: float = 0.0
    std: float = 0.0
    minimum: float = 0.0
    maximum: float = 0.0
    nsamples: int = 0

    def values(self) -> List[Any]:
        return [self.mean, self.std, self.minimum, self.maximum, self.nsamples]


class _RunningStatsWindow:
    """One resettable running-statistics window."""

    def __init__(self) -> None:
        self._last_value: Optional[float] = None
        self.reset_window(clear_last=True)

    def reset_window(self, clear_last: bool = False) -> None:
        self._mean = stats.Mean()
        self._var = stats.Var()
        self._minimum: Optional[float] = None
        self._maximum: Optional[float] = None
        self._n = 0
        if clear_last:
            self._last_value = None

    def set_idle_value(self, value: float) -> None:
        """Remember the latest value outside the DAQ window."""
        self.reset_window(clear_last=False)
        self._last_value = float(value)

    def update(self, value: float) -> None:
        value = float(value)
        self._last_value = value
        self._mean.update(value)
        self._var.update(value)
        self._n += 1
        if self._minimum is None or value < self._minimum:
            self._minimum = value
        if self._maximum is None or value > self._maximum:
            self._maximum = value

    def snapshot(self) -> StatsSnapshot:
        if self._n <= 0:
            if self._last_value is None:
                return StatsSnapshot()
            return StatsSnapshot(
                mean=float(self._last_value),
                std=0.0,
                minimum=float(self._last_value),
                maximum=float(self._last_value),
                nsamples=0,
            )

        var = float(self._var.get() or 0.0)
        std = math.sqrt(var) if var > 0.0 else 0.0
        minimum = self._minimum if self._minimum is not None else 0.0
        maximum = self._maximum if self._maximum is not None else 0.0
        return StatsSnapshot(
            mean=float(self._mean.get()),
            std=float(std),
            minimum=float(minimum),
            maximum=float(maximum),
            nsamples=int(self._n),
        )

class StatsCollector:
    """
    Collect per-subscription running statistics as scan output columns for events from role callbacks.
    The first version of legacy stats was simply collecting all events into one stat.
    The collector provides one group of scan-file columns per configured subscription.

    Parameters
    ---------------------------------------------------------------------------
    subscriptions:
        Scan subscriptions.  Only subscriptions whose ``role`` matches ``role``
        get statistics columns.
    role:
        Subscription role to collect.  The linear scan uses ``stat``.
    prefix_fn:
        Optional callable that maps a :class:`SubscriptionConfig` to a column
        prefix.  The default uses the actuator name when it is unique, otherwise
        the subscription name.
    fields:
        Output fields.  The supported field names are ``mean``, ``std``,
        ``min``, ``max``, and ``nsamples``.
    """

    DEFAULT_FIELDS: Tuple[str, ...] = ("mean", "std", "min", "max", "nsamples")
    FIELD_SUFFIXES: Dict[str, str] = {
        "mean": "Mean",
        "std": "Std",
        "min": "Min",
        "max": "Max",
        "nsamples": "NSamples",
    }

    def __init__(
        self,
        subscriptions: Optional[Sequence[SubscriptionConfig]] = None,
        *,
        role: str = "stat",
        prefix_fn: Optional[Callable[[SubscriptionConfig], str]] = None,
        fields: Optional[Iterable[str]] = None,
    ) -> None:
        self.role = role
        self._lock = threading.RLock()
        self._fields: Tuple[str, ...] = self._validate_fields(fields or self.DEFAULT_FIELDS)
        self._prefix_fn = prefix_fn

        all_subscriptions = list(subscriptions or [])
        self._subscriptions: List[SubscriptionConfig] = [
            sub for sub in all_subscriptions
            if getattr(sub, "role", None) == self.role
        ]
        self._actuator_counts = self._count_actuators(self._subscriptions)

        self._prefix_by_subscription_name: Dict[str, str] = {}
        self._state_by_prefix: Dict[str, _RunningStatsWindow] = {}
        self._ordered_prefixes: List[str] = []
        logger.info("Initializing StatsCollector role=%r fields=%s subscriptions=%d",
            self.role, self._fields, len(all_subscriptions))

        for subscription in self._subscriptions:
            raw_prefix = self._make_prefix(subscription)
            prefix = self._unique_prefix(raw_prefix)
            if prefix != raw_prefix:
                logger.warning("StatsCollector column prefix %r already exists; using %r for subscription %r",
                    raw_prefix, prefix, getattr(subscription, "name", None))
            self._prefix_by_subscription_name[subscription.name] = prefix
            self._state_by_prefix[prefix] = _RunningStatsWindow()
            self._ordered_prefixes.append(prefix)

            logger.debug("StatsCollector registered subscription %r role=%r actuator=%r "
                "source=%r pv=%r prefix=%r",
                getattr(subscription, "name", None),
                getattr(subscription, "role", None),
                getattr(subscription, "actuator", None),
                getattr(subscription, "source", None),
                getattr(subscription, "pv", None),
                prefix)
        if not self._subscriptions:
            logger.debug("StatsCollector role=%r has no matching subscriptions. No stats columns will be produced", 
                         self.role)
        else:
            logger.debug("StatsCollector role=%r ready with prefixes=%s",
                self.role, self._ordered_prefixes)
    @staticmethod
    def _validate_fields(fields: Iterable[str]) -> Tuple[str, ...]:
        result = tuple(str(field).lower() for field in fields)
        unknown = [field for field in result if field not in StatsCollector.FIELD_SUFFIXES]
        if unknown:
            logger.error("Unsupported StatsCollector field(s): %s; supported fields are: %s",
                unknown, sorted(StatsCollector.FIELD_SUFFIXES))
            raise ValueError(
                "Unsupported stats field(s): %s. Supported fields are: %s"
                % (", ".join(unknown), ", ".join(StatsCollector.FIELD_SUFFIXES))
            )
        return result

    @staticmethod
    def _count_actuators(subscriptions: Iterable[SubscriptionConfig]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for sub in subscriptions:
            actuator = getattr(sub, "actuator", None)
            if actuator:
                counts[str(actuator)] = counts.get(str(actuator), 0) + 1
        return counts

    @staticmethod
    def _clean_prefix(prefix: Any) -> str:
        text = str(prefix or "Stats").strip()
        text = _PREFIX_CLEAN_RE.sub("_", text)
        return text or "Stats"

    def _make_prefix(self, subscription: SubscriptionConfig) -> str:
        if self._prefix_fn is not None:
            return self._clean_prefix(self._prefix_fn(subscription))

        actuator = getattr(subscription, "actuator", None)
        if actuator and self._actuator_counts.get(str(actuator), 0) == 1:
            return self._clean_prefix(actuator)

        return self._clean_prefix(getattr(subscription, "name", None))

    def _unique_prefix(self, prefix: str) -> str:
        if prefix not in self._state_by_prefix:
            return prefix

        idx = 2
        while "%s%d" % (prefix, idx) in self._state_by_prefix:
            idx += 1
        return "%s%d" % (prefix, idx)

    def has_columns(self) -> bool:
        return bool(self._ordered_prefixes)

    def reset_window(self) -> None:
        """Start a new DAQ statistics window for all configured subscriptions."""
        with self._lock:
            for state in self._state_by_prefix.values():
                state.reset_window(clear_last=False)

    def update(
        self,
        event: PvEvent,
        subscription: Optional[SubscriptionConfig] = None,
        *,
        collect: bool = True,
    ) -> None:
        """Update statistics from one subscription event.

        ``collect=True`` adds the value to the current DAQ window.  With
        ``collect=False`` the value is recorded as an idle/zero-sample snapshot
        and the current window for that subscription is cleared.
        """
        if event is None:
            logger.warning("Ignoring stats update for subscription %r: event is None",
                getattr(subscription, "name", None))
            return
        prefix = self._prefix_for_event(subscription)
        if prefix is None:
            return

        try:
            raw_value = getattr(event, "value")
            value = float(raw_value)
        except (AttributeError, TypeError, ValueError):
            logger.debug("Ignoring non-numeric stats event for subscription %r pv=%r: %r",
                getattr(subscription, "name", None), getattr(event, "pvname", None), getattr(event, "value", None))
            return

        with self._lock:
            state = self._state_by_prefix.get(prefix)
            if state is None:
                logger.warning("Ignoring stats event for subscription %r: prefix %r is not registered",
                    getattr(subscription, "name", None), prefix)
                return
            try:
                if collect:
                    state.update(value)
                    action = "collected"
                else:
                    state.set_idle_value(value)
                    action = "idle"
            except Exception:
                logger.exception("Failed to update stats for subscription %r prefix=%r value=%r collect=%s",
                    getattr(subscription, "name", None), prefix, raw_value, collect)
                raise
            if logger.isEnabledFor(logging.DEBUG):
                snapshot = state.snapshot()
                logger.debug("StatsCollector %s sample subscription=%r prefix=%r pv=%r value=%r n=%d mean=%g std=%g min=%g max=%g",
                    action,
                    getattr(subscription, "name", None),
                    prefix,
                    getattr(event, "pvname", None),
                    raw_value,
                    snapshot.nsamples, snapshot.mean, snapshot.std, snapshot.minimum, snapshot.maximum)

    def _prefix_for_event(self, subscription: Optional[SubscriptionConfig]) -> Optional[str]:
        if subscription is None:
            if len(self._ordered_prefixes) == 1:
                logger.info("StatsCollector received event without subscription; using only prefix %r",
                    self._ordered_prefixes[0])
                return self._ordered_prefixes[0]
            logger.debug("Ignoring stats event without subscription for multi-source collector")
            return None

        if getattr(subscription, "role", None) != self.role:
            logger.debug("Ignoring stats event from subscription %r with role=%r; collector role=%r",
                getattr(subscription, "name", None), getattr(subscription, "role", None), self.role)
            return None

        prefix = self._prefix_by_subscription_name.get(getattr(subscription, "name", None))
        if prefix is not None:
            return prefix

        logger.debug("Ignoring stats event from unregistered subscription %r",
            getattr(subscription, "name", None),
        )
        return None

    def get_headers(self, include_timestamps: bool = False) -> List[str]:
        del include_timestamps  # stats columns do not add per-value timestamps
        with self._lock:
            headers: List[str] = []
            for prefix in self._ordered_prefixes:
                for field in self._fields:
                    headers.append(prefix + self.FIELD_SUFFIXES[field])
            logger.debug("StatsCollector headers: %s", headers)
            return headers

    def get_values(self) -> List[Any]:
        with self._lock:
            values: List[Any] = []
            for prefix in self._ordered_prefixes:
                snapshot = self._state_by_prefix[prefix].snapshot()
                values.extend(self._select_snapshot_values(snapshot))
            logger.debug("StatsCollector values: %s", values)
            return values

    def update_last_point(
        self,
        last_point: Dict[str, Any],
        include_timestamps: bool = False,
    ) -> None:
        del include_timestamps
        headers = self.get_headers(False)
        values = self.get_values()
        if len(headers) != len(values):
            logger.error("StatsCollector header/value length mismatch: headers=%d values=%d",
                len(headers), len(values))
        for header, value in zip(headers, values):
            last_point[header] = value
        logger.debug("StatsCollector updated last-point cache keys: %s", headers)

    def snapshot_by_prefix(self) -> Dict[str, StatsSnapshot]:
        """Return a structured snapshot, useful for tests and diagnostics."""
        with self._lock:
            snapshots = {
                prefix: self._state_by_prefix[prefix].snapshot()
                for prefix in self._ordered_prefixes
            }
            logger.debug("StatsCollector snapshot_by_prefix: %s", snapshots)
            return snapshots

    def _select_snapshot_values(self, snapshot: StatsSnapshot) -> List[Any]:
        mapping = {
            "mean": snapshot.mean,
            "std": snapshot.std,
            "min": snapshot.minimum,
            "max": snapshot.maximum,
            "nsamples": snapshot.nsamples,
        }
        return [mapping[field] for field in self._fields]
