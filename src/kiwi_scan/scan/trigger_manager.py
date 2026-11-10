# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence

from kiwi_scan.datamodels import ScanTriggers, TriggerAction
from kiwi_scan.epics_wrapper import EpicsPV

logger = logging.getLogger(__name__)

@dataclass
class PreparedTriggerAction:
    pv: EpicsPV
    value: Any
    delay: float = 0.0
    put_timeout: float = 0.0


class TriggerManager:
    """
    * Trigger parsing, 
    * Create PVs
    * value format
    * Interface to fire triggers."""

    DEFAULT_PHASES = ("before", "on_point", "after_point", "after", "monitor")

    def __init__(
        self,
        actions_by_phase: Optional[Dict[str, List[PreparedTriggerAction]]] = None,
        phases: Optional[Sequence[str]] = None,
    ):
        self._phases = tuple(phases or self.DEFAULT_PHASES)
        self._actions_by_phase: Dict[str, List[PreparedTriggerAction]] = {
            phase: [] for phase in self._phases
        }
        if actions_by_phase:
            for phase in self._phases:
                self._actions_by_phase[phase] = list(actions_by_phase.get(phase, []))

    @classmethod
    def from_config(cls, triggers: Optional[ScanTriggers]) -> "TriggerManager":
        phases = cls._detect_phases(triggers)
        manager = cls(phases=phases)
        if triggers is None:
            logger.info("No triggers configured.")
            return manager

        for phase in phases:
            actions = getattr(triggers, phase, []) or []
            manager._actions_by_phase[phase] = manager._prepare_actions(phase, actions)

        return manager

    @classmethod
    def _detect_phases(cls, triggers: Optional[ScanTriggers]) -> Sequence[str]:
        phases: List[str] = list(cls.DEFAULT_PHASES)
        if triggers is None:
            return phases

        for phase in vars(triggers):
            if phase not in phases:
                phases.append(phase)
        return tuple(phases)

    @staticmethod
    def _prepare_actions(phase: str, actions: Iterable[TriggerAction]) -> List[PreparedTriggerAction]: 
        """ prepare actions from trigger config. """
        prepared: List[PreparedTriggerAction] = []
        for action in actions:
            pvname = action.pv
            if not pvname:
                logger.warning("Trigger action missing 'pv' in phase %s: %r", phase, action)
                continue

            value = TriggerManager._normalize_value(action.value)
            delay = float(action.delay or 0.0)
            if delay < 0:
                logger.warning(f"Invalid trigger delay {action.delay!r} for {pvname} (phase {phase})")
                delay = 0.0
            try:
                pv = EpicsPV(pvname, timeout=1.0, queueing_delay=0.0)
            except Exception:  # noqa: BLE001
                logger.error(f"Failed to init trigger PV {pvname}, phase {phase}")
                continue

            prepared.append(PreparedTriggerAction(pv=pv, value=value, delay=delay, put_timeout=action.put_timeout))
            logger.debug("Initialized trigger PV %s (phase=%s, value=%r, delay=%.3f, put_timeout=%.3f)", pvname, phase, value, delay, action.put_timeout)

        return prepared

    @staticmethod
    def _normalize_value(value: Any) -> Any: # pylint: disable=too-many-return-statements
        if isinstance(value, (list, tuple)):
            return value

        tolist = getattr(value, "tolist", None)
        if callable(tolist):
            try:
                return tolist()
            except Exception: # noqa BLE001   - fails arbitrary 
                return value

        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                inner = stripped[1:-1].strip()
                if not inner:
                    return []
                parts = inner.replace(",", " ").split()
                try:
                    return [float(part) for part in parts]
                except ValueError:
                    return value
        return value

    def fire(self, phase: str, stop_event: Optional[threading.Event] = None) -> None:
        """
        Write the phase's actions in order.

        An action with ``put_timeout > 0`` is written with a CA put callback
        and the next step (its ``delay``, then the next action) waits until the
        IOC reports that processing finished, including asynchronous device
        support. A completion timeout logs a warning and the phase continues.
        A set ``stop_event`` ends the wait at once; the remaining actions are
        still written so that a phase is never left half done.
        """
        if phase not in self._actions_by_phase:
            logger.warning("Unknown trigger phase: %s", phase)
            return

        logger.debug("************ FIRE TRIGGERS (%s) ************", phase)
        for action in self._actions_by_phase.get(phase, []):
            if action.put_timeout > 0:
                self._put_and_wait(phase, action, stop_event)
            else:
                ok = action.pv.put(action.value)
                if not ok:
                    logger.error("Failed to write trigger PV %s value %r (phase=%s)",
                                 action.pv.pvname, action.value, phase)
                else:
                    logger.debug("Wrote trigger PV %s value %r (phase=%s)",
                                 action.pv.pvname, action.value, phase)
            if action.delay > 0:
                logger.debug("Trigger PV %s post-write delay %.3fs (phase=%s)",
                             action.pv.pvname, action.delay, phase)
                time.sleep(action.delay)

    @staticmethod
    def _put_and_wait(phase: str, action: PreparedTriggerAction,
                      stop_event: Optional[threading.Event]) -> bool:
        """Write one action with a put callback and wait for completion."""
        completion = action.pv.put_with_completion(action.value, timeout=action.put_timeout)
        if completion.wait(stop_event):
            logger.debug("Wrote trigger PV %s value %r, put completed (phase=%s)",
                         action.pv.pvname, action.value, phase)
            return True
        if completion.failed:
            # put_with_completion() already logged the error.
            return False
        if completion.unarmed:
            logger.warning("Trigger PV %s did not accept a put callback (phase=%s); not waiting",
                           action.pv.pvname, phase)
        elif stop_event is not None and stop_event.is_set():
            completion.cancel()
            logger.debug("Trigger PV %s put completion wait stopped (phase=%s)",
                         action.pv.pvname, phase)
        else:
            # PutCompletion.wait() already logged the timeout warning.
            logger.debug("Continuing phase %s after put completion timeout on %s",
                         phase, action.pv.pvname)
        return False

    def close(self) -> None:
        """Close all trigger PVs and drop the prepared actions. Idempotent."""
        for actions in self._actions_by_phase.values():
            for action in actions:
                try:
                    action.pv.close()
                except Exception:
                    logger.debug("Failed to close trigger PV %s", action.pv.pvname, exc_info=True)
            actions.clear()

    def has_actions(self, phase: Optional[str] = None) -> bool:
        if phase is None:
            return any(self._actions_by_phase[p] for p in self._phases)
        return bool(self._actions_by_phase.get(phase, []))

    @property
    def phases(self) -> Sequence[str]:
        return self._phases
