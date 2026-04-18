# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from typing import Type, Dict, Optional

from kiwi_scan.datamodels import ActuatorConfig
from kiwi_scan.actuator_concrete.single_epics import EpicsActuator
from kiwi_scan.actuator_concrete.single_simulation import SimulatedActuator
from kiwi_scan.actuator.single import AbstractActuator


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
    _registry: Dict[str, Type[AbstractActuator]] = {
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
