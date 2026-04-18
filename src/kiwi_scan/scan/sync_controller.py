import logging
import threading
import time
from typing import Dict, List, Optional, Tuple

from kiwi_scan.datamodels import SubscriptionConfig


class SyncController:
    """
    Coordinate waiting for one or more sync-role subscriptions.
    TODO: Advanced sync logic to be implemented here (groups, fault tolerance, AND/OR)

    - Every configured role="sync" subscription is one required source
    - arm() starts a new cycle
    - note_event(name) marks one source as updated for the current cycle
    - wait() blocks until all required sources updated after the last arm()
    """

    def __init__(self, subscriptions: Optional[List[SubscriptionConfig]] = None):
        
        # Specific SyncController logger
        self._logger = logging.getLogger(__name__)

        sync_subs = [
            sub for sub in (subscriptions or [])
            if getattr(sub, "role", None) == "sync"
        ]
        self._required_names: Tuple[str, ...] = tuple(sub.name for sub in sync_subs)
        self._counts: Dict[str, int] = {name: 0 for name in self._required_names}
        self._baseline: Dict[str, int] = dict(self._counts)
        self._cond = threading.Condition()
        self._logger.info(
            "SyncController initialized with %d sync sources: %s",
            len(self._required_names),
            self._required_names)

    @property
    def required_names(self) -> Tuple[str, ...]:
        return self._required_names

    def is_enabled(self) -> bool:
        return bool(self._required_names)

    def arm(self) -> None:
        """Start a new wait cycle from the current counters."""
        if not self.is_enabled():
            return
        with self._cond:
            self._baseline = dict(self._counts)
        self._logger.debug(
                "SyncController armed: baseline=%s counts=%s",
                self._baseline,
                self._counts)

    def note_event(self, subscription_name: Optional[str]) -> None:
        """Record one event from a sync subscription."""
        if not subscription_name:
            self._logger.debug("No subscription_name")
            return
        # self._logger.debug( "Event %s", subscription_name)
        with self._cond:
            if subscription_name not in self._counts:
                self._logger.debug(
                    "Ignoring event from unknown subscription '%s'",
                    subscription_name)
                return
            self._counts[subscription_name] += 1
            #self._logger.debug(
            #    "Event received: %s -> count=%d",
            #    subscription_name,
            #    self._counts[subscription_name],
            #)
            self._cond.notify_all()

    def is_ready(self) -> bool:
        if not self.is_enabled():
            return True
        ready = all(
             self._counts[name] > self._baseline.get(name, 0)
             for name in self._required_names
         )
        #self._logger.debug(
        #    "SyncController is_ready=%s baseline=%s counts=%s",
        #    ready,
        #    self._baseline,
        #    self._counts,
        #)
        return ready


    def wait(
        self,
        timeout: Optional[float] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> bool:
        """
        Wait until all required sync subscriptions updated after arm().
        Returns False on timeout or stop request.
        """
        if not self.is_enabled():
            return True
        self._logger.debug("SyncController wait start (timeout=%s)", timeout)

        deadline = None if timeout is None else time.monotonic() + max(0.0, float(timeout))

        with self._cond:
            while not self.is_ready():
                # self._logger.debug("Waiting for sync events...")
                if stop_event is not None and stop_event.is_set():
                    self._logger.info("SyncController wait aborted by stop_event")
                    return False

                wait_time = None
                if deadline is not None:
                    wait_time = deadline - time.monotonic()
                    if wait_time <= 0.0:
                        self._logger.warning("SyncController wait timeout")
                        return False

                self._cond.wait(timeout=wait_time)
                # self._logger.debug("SyncController wait complete (all sources updated)")
            return True
