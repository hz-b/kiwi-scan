# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
from dataclasses import dataclass, field, fields, replace, is_dataclass, asdict
from typing import List, Dict, Optional, Union, Any

def filter_known_fields(cls, data: Dict[str, Any]) -> Dict[str, Any]:
    """Filter out unknown fields from a dict for a given dataclass."""
    known_fields = {f.name for f in fields(cls)}
    return {k: v for k, v in data.items() if k in known_fields}

@dataclass
class JogConfig:
    # PV to write jog velocity directly
    velocity_pv: Optional[str] = None
    abs_velocity: bool = False
    # PV to write a jog start command
    command_pv: Optional[str] = None
    # Command value for positive-direction jog (written to command_pv)
    command_pos: Optional[float] = None
    # Command value for negative-direction jog (written to command_pv)
    command_neg: Optional[float] = None

@dataclass
class ActuatorConfig:
    pv: str = None
    type: str = "epics"
    rel_pv: Optional[str] = None
    rb_pv: Optional[str] = None
    cmd_pv: Optional[str] = None
    cmdvel_pv: Optional[str] = None
    stop_pv: Optional[str] = None
    stop_command: float = 0.0
    status_pv: Optional[str] = None
    queueing_delay: float = 0.01
    startup_timeout: float = 1.0
    in_position_band: float = -1.0
    dwell_time: float = 1.0
    ready_value: int = 0
    ready_bitmask: int = 0
    backlash: float = 0.0
    start_pv: Optional[str] = None
    start_command: float = 0.0
    velocity_pv: Optional[str] = None
    get_velocity_pv: Optional[str] = None
    velocity: float = 0.0
    jog: Optional[JogConfig] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ActuatorConfig":
        # 1) Extract & convert the jog block if it exists
        jog_raw = data.get("jog")
        if isinstance(jog_raw, dict):
            # filter_known_fields should take a class and a dict,
            # returning only keys that match its __annotations__
            jog_kwargs = filter_known_fields(JogConfig, jog_raw)
            data["jog"] = JogConfig(**jog_kwargs)

        # 2) Now build the top‐level config
        cfg_kwargs = filter_known_fields(cls, data)
        return cls(**cfg_kwargs)

@dataclass
class ScanDimension:
    """
    Represents one scan axis for an actuator in a multi-dimensional scan.
    """
    actuator: str
    start: float
    stop: float
    steps: int
    velocity: float = 0.0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ScanDimension":
        """
        Construct a ScanDimension from a dict, filtering any unknown fields.
        """
        clean_data = filter_known_fields(cls, data)
        return cls(**clean_data)

    @classmethod
    def list_from_dicts(
        cls,
        data: Union[
            Dict[str, Any],
            List[Dict[str, Any]],
            Dict[str, Dict[str, Any]]
        ]
    ) -> List["ScanDimension"]:
        """
        Construct ScanDimension instances from:
        - a single dict with keys 'actuator', 'start', 'stop', 'steps'
        - a list of such dicts
        - a mapping of actuator name -> dict of start/stop/steps (e.g., from YAML under 'actuators')
        """
        dims: List[ScanDimension] = []  # type: ignore

        # Mapping of actuator -> params dict
        if isinstance(data, dict) and all(isinstance(v, dict) for v in data.values()):
            for actuator, params in data.items():
                merged = {"actuator": actuator, **params}
                clean = filter_known_fields(cls, merged)
                dims.append(cls(**clean))
            return dims

        # Single dict of parameters
        if isinstance(data, dict):
            return [cls.from_dict(data)]

        # List of dicts
        if isinstance(data, list):
            return [cls.from_dict(d) for d in data]

        raise TypeError(
            f"Unsupported data type for ScanDimension.list_from_dicts: {type(data)}"
        )

    @classmethod
    def from_dim_args(
        cls,
        dim_args: List[str]
    ) -> List["ScanDimension"]:
        """
        Parse a list of CLI "--dim" strings into a flat list of ScanDimension.
        Each string must be of form: actuator=NAME,start=VAL,stop=VAL,steps=N
        """
        def parse_single(dim_str: str) -> Dict[str, Any]:
            parts = dim_str.split(',')
            kv = dict(part.split('=') for part in parts)
            return {
                'actuator': kv['actuator'],
                'start': float(kv['start']),
                'stop': float(kv['stop']),
                'steps': int(kv['steps']),
                'velocity': float(kv.get('velocity', 0.0)),
            }

        return [cls(**parse_single(s)) for s in dim_args]

    def compute_positions_linear(self) -> List[float]:
        """
        Linearly generate a list of positions evenly spaced from start to stop.
        If steps < 2, returns [start].
        """
        if self.steps < 2:
            return [self.start]
        step = (self.stop - self.start) / (self.steps - 1)
        return [self.start + i * step for i in range(self.steps)]

    @staticmethod
    def get_actuators(
        dims: List["ScanDimension"]
    ) -> List[str]:
        """
        Extract actuator names from a list of ScanDimension.
        """
        return [d.actuator for d in dims]

@dataclass
class TriggerAction:
    pv: str
    value: Any
    delay: float = 0.0 

@dataclass
class ScanTriggers:
    before: List[TriggerAction] = field(default_factory=list)
    on_point: List[TriggerAction] = field(default_factory=list)
    after: List[TriggerAction] = field(default_factory=list)
    monitor: List[TriggerAction] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ScanTriggers":
        def parse_action_list(key: str) -> List[TriggerAction]:
            raw = data.get(key, [])
            return [TriggerAction(**a) for a in raw if isinstance(a, dict)]

        triggers = cls(
            before=parse_action_list("before"),
            on_point=parse_action_list("on_point"),
            after=parse_action_list("after"),
            monitor=parse_action_list("monitor"),
        )

        for key, raw in data.items():
            if key not in {"before", "on_point", "after", "monitor"}:
                setattr(triggers, key, [TriggerAction(**a) for a in raw if isinstance(a, dict)])

        return triggers

@dataclass
class SubscriptionConfig:
    name: str
    role: str

    # exactly one of these must be set
    pv: Optional[str] = None
    actuator: Optional[str] = None

    # used only when actuator is set
    source: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SubscriptionConfig":
        return cls(
            name=data["name"],
            role=data["role"],
            pv=data.get("pv"),
            actuator=data.get("actuator"),
            source=data.get("source"),
        )

@dataclass
class ScanConfig:
    actuators: Dict[str, ActuatorConfig]
    detector_pvs: List[str]
    scan_dimensions: Optional[List[ScanDimension]] = None
    parallel_scans: Optional[List[ScanDimension]] = None
    nested_scans: Optional[List[ScanDimension]] = None
    plugin_configs: List[Dict[str, Any]] = field(default_factory=list)
    monitor_type: str = None
    stop_pv: Optional[str] = None
    data_dir: str = "."
    output_file: str = "scan_results.txt"
    include_timestamps: bool = False
    integration_time: float = 0.0
    debug: bool = False
    performance_report: bool = False
    data_writing_enabled: bool = True
    triggers: Optional[ScanTriggers] = None
    metadata_pvs: List[str] = field(default_factory=list)          # EPICS PVs to monitor in parallel
    metadata_constants: Dict[str, Any] = field(default_factory=dict)  # key/value string constants
    metadata_file: str = "scan_metadata.txt"                        # sidecar filename
    subscriptions: List[SubscriptionConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "ScanConfig":
        known_keys = {f.name for f in fields(cls)}
        logging.debug(f"Creating ScanConfig from dict, known_keys = {known_keys}")
        
        actuators = {
            name: ActuatorConfig.from_dict(cfg)
            for name, cfg in config_dict.get("actuators", {}).items()
            if isinstance(cfg, dict)
        }

        def parse_dimensions(key: str) -> Optional[List[ScanDimension]]:
            dims = config_dict.get(key)
            if dims is None:
                return None
            return [ScanDimension.from_dict(d) for d in dims if isinstance(d, dict)]

        triggers_raw = config_dict.get("triggers", None)
        # logging.info(f"TRIGGERS_RAW:{triggers_raw}<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        triggers = ScanTriggers.from_dict(triggers_raw) if triggers_raw else None

        # Parse subscriptions
        subs = []
        for sub_data in config_dict.get("subscriptions", []):
            subs.append(SubscriptionConfig.from_dict(sub_data))

        # Log unknown keys
        for key in config_dict:
            if key not in known_keys:
                logging.debug(f"Unknown key in ScanConfig YAML: {key} → {config_dict[key]}")

        return cls(
            actuators=actuators,
            detector_pvs=config_dict.get("detector_pvs", []),
            scan_dimensions=parse_dimensions("scan_dimensions"),
            parallel_scans=parse_dimensions("parallel_scans"),
            nested_scans=parse_dimensions("nested_scans"),
            plugin_configs=config_dict.get("plugin_configs", []),
            monitor_type=config_dict.get("monitor_type", None),
            stop_pv=config_dict.get("stop_pv") or None,
            data_dir=config_dict.get("data_dir", "."),
            output_file=config_dict.get("output_file", "scan_results.txt"),
            include_timestamps=config_dict.get("include_timestamps", False),
            integration_time=config_dict.get("integration_time", 0.0),
            debug=config_dict.get("debug", False),
            performance_report=config_dict.get("performance_report", False),
            data_writing_enabled=config_dict.get("data_writing_enabled", True),
            triggers=triggers,
            metadata_pvs=config_dict.get("metadata_pvs", []),
            metadata_constants=config_dict.get("metadata_constants", {}),
            metadata_file=config_dict.get("metadata_file", "scan_metadata.txt"),
            subscriptions=subs,
        )


def build_scan_dim(actuator: str, start: float, stop: float, steps: int) -> ScanDimension:
    """
    Construct a ScanDimension. TODO: replace
    Args:
        actuator (str): Name of the actuator (e.g. "energy", "pitch").
        start (float): Start position of the scan.
        stop (float): Stop position of the scan.
        steps (int): Number of scan points.
    Returns:
        ScanDimension: Configured scan dimension object.
    """
    return ScanDimension(actuator=actuator, start=start, stop=stop, steps=steps)

