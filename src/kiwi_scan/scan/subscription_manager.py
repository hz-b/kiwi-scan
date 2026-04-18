# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union

from kiwi_scan.actuator.single import AbstractActuator, PvEvent
from kiwi_scan.datamodels import ActuatorConfig, SubscriptionConfig
from kiwi_scan.epics_wrapper import EpicsPV

logger = logging.getLogger(__name__)

RoleHandler = Callable[..., None]
ActuatorConfigLike = Union[ActuatorConfig, Dict[str, Any]]


@dataclass(frozen=True)
class RoleBinding:
    """Describe how a subscription role should be dispatched.

    Attributes
    ----------
    handler:
        Callback registered for the role.
    accepts_subscription:
        ``True`` when the handler accepts both ``(event, subscription)``.
        ``False`` when it only accepts ``(event,)``.
    """

    handler: RoleHandler
    accepts_subscription: bool


class SubscriptionManager:
    """Manage subscription setup, dispatch, and teardown for a scan.

    A subscription can be configured in two ways:

    * directly through a PV name via ``SubscriptionConfig.pv``
    * indirectly via ``SubscriptionConfig.actuator`` plus ``source`` 
      so that it can be called simply "rbv" for example

    When subscriptions are started, the manager prefers a monitor-capable
    actuator backend. This keeps all monitor handling inside the actuator layer
    when possible. If no actuator backend offers monitors, the manager falls
    back to creating direct ``EpicsPV`` monitors.
    """

    def __init__(
        self,
        subscriptions: Optional[List[SubscriptionConfig]] = None,
        *,
        actuator_configs: Optional[Dict[str, ActuatorConfigLike]] = None,
        actuators: Optional[Dict[str, AbstractActuator]] = None,
    ) -> None:
        """Store subscription definitions and runtime dependencies.

        Parameters
        ----------
        subscriptions:
            Subscription definitions to manage.
        actuator_configs:
            Mapping used to resolve ``actuator + source`` subscriptions to
            concrete PV names.
        actuators:
            Live actuator instances. They are inspected to find a backend that
            supports monitors.
        """
        self._subscriptions: List[SubscriptionConfig] = list(subscriptions or [])
        self._actuator_configs: Dict[str, ActuatorConfigLike] = dict(actuator_configs or {})
        self._actuators: Dict[str, AbstractActuator] = dict(actuators or {})

        self._role_bindings: Dict[str, RoleBinding] = {}

        self._provider: Optional[AbstractActuator] = None
        self._handles_by_name: Dict[str, Any] = {}
        self._pvname_by_name: Dict[str, str] = {}
        self._started = False

    # ------------------------------------------------------------------
    # Role registration and dispatch
    # ------------------------------------------------------------------

    def register_role(self, role: str, handler: RoleHandler) -> None:
        """Register the callback used for one subscription role.

        Parameters
        ----------
        role:
            Logical role name such as ``heartbeat`` or ``stop``.
        handler:
            Callback that receives a :class:`PvEvent`. The handler may accept
            either ``handler(event)`` or ``handler(event, subscription)``.

        Raises
        ------
        ValueError
            If ``role`` is empty.
        TypeError
            If ``handler`` is not callable.
        """
        if not role:
            raise ValueError("role must not be empty")
        if not callable(handler):
            raise TypeError(f"handler for role '{role}' must be callable")

        binding = RoleBinding(
            handler=handler,
            accepts_subscription=self._handler_accepts_subscription(handler),
        )

        previous = self._role_bindings.get(role)
        if previous is not None and not self._same_handler(previous.handler, handler):
            logger.warning("Replacing subscription role handler for role '%s'", role)

        self._role_bindings[role] = binding
        logger.debug(
            "Registered subscription role '%s' (accepts_subscription=%s)",
            role,
            binding.accepts_subscription,
        )

    @staticmethod
    def _same_handler(lhs: RoleHandler, rhs: RoleHandler) -> bool:
        """Return ``True`` when two handler references point to the same target."""
        if lhs is rhs:
            return True

        lhs_self = getattr(lhs, "__self__", None)
        rhs_self = getattr(rhs, "__self__", None)
        lhs_func = getattr(lhs, "__func__", None)
        rhs_func = getattr(rhs, "__func__", None)

        if lhs_func is not None or rhs_func is not None:
            return lhs_self is rhs_self and lhs_func is rhs_func

        return False

    @staticmethod
    def _handler_accepts_subscription(handler: RoleHandler) -> bool:
        """Detect whether a handler supports ``(event, subscription)``.

        The scan code historically used both signatures. This helper allows the
        manager to keep both forms working without forcing callers to adapt.
        """
        try:
            params = list(inspect.signature(handler).parameters.values())
        except (TypeError, ValueError):
            return True

        positional_count = 0
        for param in params:
            if param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                return True
            if param.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                positional_count += 1

        return positional_count >= 2

    def _dispatch_role_event(
        self,
        role: str,
        event: PvEvent,
        subscription: SubscriptionConfig,
    ) -> None:
        """Dispatch one subscription event to the registered role handler."""
        binding = self._role_bindings.get(role)
        if binding is None:
            logger.debug(
                "No handler registered for subscription role '%s' (subscription '%s')",
                role,
                subscription.name,
            )
            return

        if binding.accepts_subscription:
            binding.handler(event, subscription)
        else:
            binding.handler(event)

    # ------------------------------------------------------------------
    # PV and provider resolution
    # ------------------------------------------------------------------

    def _get_subscription_provider(self) -> Optional[AbstractActuator]:
        """Return the first actuator backend that supports monitors.

        The provider is used only as a monitor backend. It does not need to be
        the actuator mentioned by the subscription itself.
        """
        for name, actuator in self._actuators.items():
            try:
                if actuator is not None and actuator.supports_monitors():
                    logger.debug(
                        "Using actuator '%s' as subscription monitor provider (%s)",
                        name,
                        type(actuator).__name__,
                    )
                    return actuator
            except Exception:
                logger.debug(
                    "Actuator '%s' could not be probed for monitor support",
                    name,
                    exc_info=True,
                )

        return None

    def _get_actuator_config(self, actuator_name: str) -> ActuatorConfig:
        """Return one actuator configuration in normalized dataclass form.

        The method accepts both already-built :class:`ActuatorConfig` objects
        and raw dictionaries. Raw dictionaries are converted once and cached.
        """
        raw_config = self._actuator_configs.get(actuator_name)
        if raw_config is None:
            raise ValueError(f"Unknown actuator '{actuator_name}' in subscription config")

        if isinstance(raw_config, ActuatorConfig):
            return raw_config

        if isinstance(raw_config, dict):
            normalized = ActuatorConfig.from_dict(raw_config)
            self._actuator_configs[actuator_name] = normalized
            return normalized

        raise TypeError(
            "Actuator config for '%s' must be dict or ActuatorConfig, got %s"
            % (actuator_name, type(raw_config))
        )

    def resolve_pv(self, subscription: SubscriptionConfig) -> str:
        """Resolve a subscription definition to the concrete PV name.

        Parameters
        ----------
        subscription:
            Subscription to resolve.

        Returns
        -------
        str
            The resolved PV name.

        Raises
        ------
        ValueError
            If the subscription configuration is incomplete or inconsistent.
        """
        if subscription.pv:
            return subscription.pv

        if not subscription.actuator:
            raise ValueError(
                f"Subscription '{subscription.name}' must define either 'pv' or 'actuator'"
            )

        config = self._get_actuator_config(subscription.actuator)
        source = (subscription.source or "rbv").lower()

        if source == "rbv":
            return config.rb_pv or config.pv

        if source in ("cmd", "set", "command"):
            return config.cmd_pv or config.pv

        if source == "status":
            if not config.status_pv:
                raise ValueError(
                    f"Subscription '{subscription.name}': actuator '{subscription.actuator}' has no status_pv"
                )
            return config.status_pv

        if source == "stop":
            if not config.stop_pv:
                raise ValueError(
                    f"Subscription '{subscription.name}': actuator '{subscription.actuator}' has no stop_pv"
                )
            return config.stop_pv

        if source == "velocity":
            return config.get_velocity_pv or config.velocity_pv or config.cmdvel_pv or config.pv

        raise ValueError(
            f"Subscription '{subscription.name}': unsupported source '{subscription.source}'"
        )

    # ------------------------------------------------------------------
    # Start helpers
    # ------------------------------------------------------------------

    def _build_provider_callback(self, subscription: SubscriptionConfig) -> Callable[[PvEvent], None]:
        """Create the callback passed to a monitor-capable actuator backend."""

        def _callback(event: PvEvent, _subscription: SubscriptionConfig = subscription) -> None:
            self._dispatch_role_event(_subscription.role, event, _subscription)

        return _callback

    def _build_epics_callback(
        self,
        subscription: SubscriptionConfig,
        fallback_pvname: str,
    ) -> Callable[..., None]:
        """Create the callback used by the direct ``EpicsPV`` fallback path."""

        def _callback(
            pvname: Optional[str] = None,
            value: Any = None,
            _subscription: SubscriptionConfig = subscription,
            _fallback_pvname: str = fallback_pvname,
            **kwargs: Any,
        ) -> None:
            event = PvEvent(
                pvname=pvname or _fallback_pvname,
                value=value,
                source=kwargs.get("source", "epics_monitor"),
                timestamp=kwargs.get("timestamp"),
                posixseconds=kwargs.get("posixseconds"),
                nanoseconds=kwargs.get("nanoseconds"),
                severity=kwargs.get("severity"),
                status=kwargs.get("status"),
                raw=dict(kwargs) if kwargs else {},
            )
            self._dispatch_role_event(_subscription.role, event, _subscription)

        return _callback

    def _start_subscription_with_provider(
        self,
        subscription: SubscriptionConfig,
        pvname: str,
    ) -> None:
        """Start one subscription using the selected actuator monitor provider."""
        if self._provider is None:
            raise RuntimeError("No subscription provider is available")

        callback = self._build_provider_callback(subscription)
        handle = self._provider.add_monitor(pvname, user_callback=callback)
        self._handles_by_name[subscription.name] = handle

        logger.debug(
            "Started subscription '%s' -> %s (role=%s) via provider %s",
            subscription.name,
            pvname,
            subscription.role,
            type(self._provider).__name__,
        )

    def _start_subscription_with_epics(
        self,
        subscription: SubscriptionConfig,
        pvname: str,
    ) -> None:
        """Start one subscription using a dedicated direct ``EpicsPV`` monitor."""
        handle = EpicsPV(pvname, timeout=1.0, queueing_delay=0.01, auto_monitor=True)
        handle.add_callback(self._build_epics_callback(subscription, pvname))
        self._handles_by_name[subscription.name] = handle

        logger.debug(
            "Started subscription '%s' -> %s (role=%s) via direct EpicsPV",
            subscription.name,
            pvname,
            subscription.role,
        )

    def _reset_runtime_state(self) -> None:
        """Clear runtime-only bookkeeping without touching external resources."""
        self._provider = None
        self._handles_by_name.clear()
        self._pvname_by_name.clear()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start all configured subscriptions.

        The method is idempotent. Repeated calls after a successful start do
        nothing.

        Raises
        ------
        Exception
            Propagates startup failures after best-effort cleanup of partially
            created subscriptions.
        """
        if self._started:
            logger.debug("SubscriptionManager.start() called while already started")
            return

        if not self._subscriptions:
            logger.debug("SubscriptionManager.start(): no subscriptions configured")
            self._started = True
            return

        self._reset_runtime_state()
        self._provider = self._get_subscription_provider()

        if self._provider is not None:
            logger.info(
                "Starting %d subscriptions using monitor provider %s",
                len(self._subscriptions),
                type(self._provider).__name__,
            )
        else:
            logger.info(
                "Starting %d subscriptions using direct EpicsPV monitors",
                len(self._subscriptions),
            )

        try:
            for subscription in self._subscriptions:
                pvname = self.resolve_pv(subscription)
                self._pvname_by_name[subscription.name] = pvname

                if self._provider is not None:
                    self._start_subscription_with_provider(subscription, pvname)
                else:
                    self._start_subscription_with_epics(subscription, pvname)

            self._started = True
            logger.info("Started %d subscriptions", len(self._subscriptions))

        except Exception:
            logger.exception("Failed to start subscriptions")
            self._stop_active_handles()
            self._reset_runtime_state()
            self._started = False
            raise

    def _stop_active_handles(self) -> None:
        """Release active monitors regardless of the current started flag."""
        if self._provider is not None:
            for pvname in set(self._pvname_by_name.values()):
                try:
                    self._provider.remove_monitor(pvname)
                except Exception:
                    logger.exception(
                        "Failed to remove provider monitor for subscription PV '%s'",
                        pvname,
                    )
            return

        for handle in self._handles_by_name.values():
            try:
                if hasattr(handle, "clear_callbacks"):
                    handle.clear_callbacks()

                raw_pv = getattr(handle, "_pv", None)
                if raw_pv is not None and hasattr(raw_pv, "disconnect"):
                    raw_pv.disconnect()
            except Exception:
                logger.exception("Failed to clear EPICS callbacks for subscription handle")

    def stop(self) -> None:
        """Stop all active subscriptions and clear runtime bookkeeping.
        """
        if not self._started:
            logger.debug("SubscriptionManager.stop() called while not started")
            return

        if self._subscriptions:
            logger.info("Stopping %d subscriptions", len(self._subscriptions))

        self._stop_active_handles()
        self._reset_runtime_state()
        self._started = False

        if self._subscriptions:
            logger.info("Subscriptions stopped")

    @property
    def started(self) -> bool:
        """Return ``True`` when subscriptions have been started."""
        return self._started
