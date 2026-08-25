# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from typing import Any, ClassVar, Dict, Mapping, Optional, Type, Union

from kiwi_scan.actuator.single import AbstractActuator
from kiwi_scan.actuator_concrete.single_epics import EpicsActuator
from kiwi_scan.actuator_concrete.single_simulation import SimulatedActuator
from kiwi_scan.datamodels import ActuatorConfig

ActuatorConfigLike = Union[ActuatorConfig, Mapping[str, Any]]


class ActuatorFactory:
    """
    Factory for creating actuator instances based on configuration.

    Usage:
        # simple flag-based simulation
        actuator = ActuatorFactory.create(config, use_simulation=True)

        # config-based (EPICS)
        actuator = ActuatorFactory.create(config)
    """
    # Registry mapping type keys to actuator classes
    _registry: ClassVar[Dict[str, Type[AbstractActuator]]] = {
        'epics': EpicsActuator,
        'sim': SimulatedActuator,
    }

    @classmethod
    def create(
        cls,
        config: ActuatorConfig,
        use_simulation: Optional[bool] = None,
        actuator_type: Optional[str] = None,
    ) -> AbstractActuator:
        """
        Create an actuator based on:
          - explicit `use_simulation` flag (overrides everything)
          - `actuator_type` argument ('epics' or 'sim')
          - `config.actuator_type` attribute, if defined in your datamodel

        Falls back to EpicsActuator.
        """
        # Determine type key
        if use_simulation is True:
            key = 'sim'
        elif use_simulation is False:
            key = 'epics'
        else:
            # try explicit override
            key = actuator_type or getattr(config, 'actuator_type', None)
            # default if missing
            key = key or 'epics'

        try:
            actuator_cls = cls._registry[key]
        except KeyError:
            raise ValueError(f"Unknown actuator type '{key}'. "
                             f"Supported types: {list(cls._registry)}")

        return actuator_cls(config)


def create_actuator(config: ActuatorConfig) -> AbstractActuator:
    key = config.type  # “epics” or “sim”
    cls = ActuatorFactory._registry.get(key)
    if cls is None:
        raise ValueError(f"Unknown type {key}")
    return cls(config)


def create_actuators(
    configs: Mapping[str, ActuatorConfigLike],
) -> Dict[str, AbstractActuator]:
    """ 
    Create a named collection of actuators from config objects or mappings.
    """
    if not isinstance(configs, Mapping) or not configs:
        raise ValueError("Config has no 'actuators:' mapping (or it's empty).")

    actuators: Dict[str, AbstractActuator] = {}
    for name, raw_config in configs.items():
        if isinstance(raw_config, ActuatorConfig):
            config = raw_config
        elif isinstance(raw_config, Mapping):
            config = ActuatorConfig.from_dict(dict(raw_config))
        else:
            raise TypeError(f"Actuator '{name}' must be an ActuatorConfig or mapping, got {type(raw_config).__name__}")

        try:
            actuators[name] = create_actuator(config)
        except ConnectionError as exc:
            raise ConnectionError( f"Failed to create actuator '{name}': {exc}") from exc

    return actuators
