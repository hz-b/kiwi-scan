# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

from kiwi_scan.yaml_loader import (
    parse_replacements,
    get_env_replacements,
    get_replacements_help_and_required,
    yaml_loader,
)
from kiwi_scan.scan.tools import get_scan_config_dir, load_scan_configs, set_valid_logging_level
from kiwi_scan.datamodels import ActuatorConfig
from kiwi_scan.actuator.factory import create_actuator
from kiwi_scan.actuator.single import AbstractActuator, PvEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s"
)

def _parse_name_value(spec: str) -> Tuple[str, float]:
    # NAME=VAL
    if "=" not in spec:
        raise ValueError(f"Expected NAME=VALUE, got {spec!r}")
    name, s_val = spec.split("=", 1)
    name = name.strip()
    if not name:
        raise ValueError(f"Empty name in {spec!r}")
    return name, float(s_val.strip())

def _parse_name_value_any(spec: str) -> Tuple[str, Any]:
    """Parse NAME=VALUE where VALUE can be a float list (e.g. [1, 2])."""
    if "=" not in spec:
        raise ValueError(f"Expected NAME=VALUE, got {spec!r}")
    name, s_val = spec.split("=", 1)
    name = name.strip()
    if not name:
        raise ValueError(f"Empty name in {spec!r}")
    s_val = s_val.strip()
    if not s_val:
        raise ValueError(f"Empty value in {spec!r}")

    # Allow multi-axis relative moves for MultiActuator.
    if s_val.startswith("["):
        try:
            return name, json.loads(s_val)
        except Exception:
            # fall back to float parsing below
            pass

    return name, float(s_val)

def _parse_monitor_spec(spec: str) -> Dict[str, Optional[str]]:
    """
    Monitor spec formats:
      - NAME:source         (source default 'rbv' if omitted)
      - NAME@PVNAME         (direct PV override)
      - NAME               (equivalent to NAME:rbv)
    """
    s = spec.strip()
    if not s:
        raise ValueError("Empty --monitor spec")

    if "@" in s:
        name, pv = s.split("@", 1)
        name = name.strip()
        pv = pv.strip()
        if not name or not pv:
            raise ValueError(f"Invalid monitor spec {spec!r}, expected NAME@PV")
        return {"name": name, "source": None, "pv": pv}

    if ":" in s:
        name, source = s.split(":", 1)
        name = name.strip()
        source = source.strip() or "rbv"
        if not name:
            raise ValueError(f"Invalid monitor spec {spec!r}, empty NAME")
        return {"name": name, "source": source, "pv": None}

    return {"name": s, "source": "rbv", "pv": None}


def _resolve_pv_for_source(act_cfg: ActuatorConfig, source: str) -> str:
    src = (source or "rbv").lower()
    if src == "rbv":
        return act_cfg.rb_pv or act_cfg.pv
    if src in ("cmd", "set", "command"):
        return act_cfg.cmd_pv or act_cfg.pv
    if src == "status":
        if not act_cfg.status_pv:
            raise ValueError("source=status requested but actuator has no status_pv")
        return act_cfg.status_pv
    if src == "stop":
        if not act_cfg.stop_pv:
            raise ValueError("source=stop requested but actuator has no stop_pv")
        return act_cfg.stop_pv
    if src == "velocity":
        return act_cfg.get_velocity_pv or act_cfg.velocity_pv or act_cfg.cmdvel_pv or act_cfg.pv
    raise ValueError(f"Unsupported source {source!r}. Use rbv|cmd|status|stop|velocity.")


# ----------------------------- config + actuators -----------------------------

def _load_raw_config(args) -> Tuple[Dict[str, Any], str]:
    # Replacements (CLI + env)
    repl = parse_replacements(args.replace or [])
    repl.update(get_env_replacements("KIWI_SCAN"))

    if args.config_file:
        return yaml_loader(args.config_file, repl), args.config_file

    config_dir = os.environ.get("KIWI_SCAN_CONFIG_DIR", get_scan_config_dir())
    # For argparse choices/help we preloaded keys with replacements=None,
    # but for actual load we load raw yaml with replacements applied:
    cfg_path = os.path.join(config_dir, f"{args.config}.yaml")
    cfg = yaml_loader(cfg_path, repl)
    return cfg, cfg_path


def _build_actuators(raw_cfg: Dict[str, Any]) -> Dict[str, AbstractActuator]:
    acts_raw = raw_cfg.get("actuators") or {}
    if not isinstance(acts_raw, dict) or not acts_raw:
        raise ValueError("Config has no 'actuators:' mapping (or it's empty).")

    actuators: Dict[str, AbstractActuator] = {}
    for name, v in acts_raw.items():
        if not isinstance(v, dict):
            raise TypeError(f"Actuator '{name}' must be a mapping, got {type(v)}")
        cfg = ActuatorConfig.from_dict(v)
        actuators[name] = create_actuator(cfg)
    return actuators


def _pick_monitor_provider(actuators: Dict[str, AbstractActuator]) -> AbstractActuator:
    for act in actuators.values():
        try:
            if act.supports_monitors():
                return act
        except Exception:
            continue
    raise RuntimeError("No actuator backend supports monitors in this config.")

# ----------------------------- monitor + output -----------------------------

class _EventWriter(threading.Thread):
    def __init__(
        self,
        q: "queue.Queue[dict]",
        *,
        out_path: Optional[str],
        stop_event: threading.Event,
    ):
        super().__init__(daemon=True)
        self._q = q
        self._stop_event = stop_event
        self._fh = open(out_path, "a", encoding="utf-8", buffering=1) if out_path else None

    def close(self) -> None:
        try:
            if self._fh:
                self._fh.close()
        finally:
            self._fh = None

    def _emit(self, line: str) -> None:
        print(line)
        if self._fh:
            self._fh.write(line + "\n")

    def run(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    item = self._q.get(timeout=0.1)
                except queue.Empty:
                    continue

                if item is None:  # sentinel
                    break

                # line format
                mon = item.get("monitor_id")
                name = item.get("actuator")
                src = item.get("source")
                pv = item.get("pvname")
                rel = item.get("t_rel_s")
                val = item.get("value")
                self._emit(f"[mon#{mon} {name}:{src}] {rel:9.3f}s pv={pv} value={val!r}")
        finally:
            self.close()


# ----------------------------- monitor startup -----------------------------
# TODO: Align actuator_runner monitor handling with SubscriptionManager.
def _start_monitors(
    *,
    have_monitors: bool,
    args,
    raw_cfg: Dict[str, Any],
    actuators: Dict[str, AbstractActuator],
    ev_q: "queue.Queue[dict]",
    t0: float,
    _inc_seen,
    _inc_dropped,
) -> Tuple[Optional[AbstractActuator], List[Tuple[str, Any]], List[dict]]:
    provider: Optional[AbstractActuator] = None
    monitor_handles: List[Tuple[str, Any]] = []
    monitor_specs: List[dict] = []

    if not have_monitors:
        return provider, monitor_handles, monitor_specs

    provider = _pick_monitor_provider(actuators)

    # We'll resolve PVs using the named actuator config, but subscribe via provider
    acts_raw = raw_cfg.get("actuators") or {}

    for idx, spec_s in enumerate(args.monitor, start=1):
        ms = _parse_monitor_spec(spec_s)
        name = ms["name"]
        src = ms["source"] or "pv"
        pv_override = ms["pv"]

        raw_act = acts_raw.get(name)
        if not isinstance(raw_act, dict):
            raise ValueError(f"--monitor refers to unknown actuator {name!r}")

        act_cfg = ActuatorConfig.from_dict(raw_act)
        pvname = pv_override if pv_override else _resolve_pv_for_source(act_cfg, ms["source"] or "rbv")

        monitor_id = idx

        def _mk_cb(_monitor_id: int, _name: str, _src: str, _pv: str):
            def _cb(ev: PvEvent) -> None:
                payload = {
                    "monitor_id": _monitor_id,
                    "actuator": _name,
                    "source": _src,
                    "pvname": getattr(ev, "pvname", _pv),
                    "value": getattr(ev, "value", None),
                    "t_abs_s": time.time(),
                    "t_rel_s": time.time() - t0,
                    "timestamp": getattr(ev, "timestamp", None),
                    "posixseconds": getattr(ev, "posixseconds", None),
                    "nanoseconds": getattr(ev, "nanoseconds", None),
                    "severity": getattr(ev, "severity", None),
                    "status": getattr(ev, "status", None),
                    # raw may be large/non-serializable; include if present but safe via default=str
                    "raw": getattr(ev, "raw", None),
                }
                try:
                    ev_q.put_nowait(payload)
                    _inc_seen()
                except queue.Full:
                    _inc_dropped()
            return _cb

        cb = _mk_cb(monitor_id, name, src, pvname)
        handle = provider.add_monitor(pvname, user_callback=cb)
        monitor_handles.append((pvname, handle))
        monitor_specs.append({"monitor_id": monitor_id, "name": name, "source": src, "pvname": pvname})

    logging.info("Started %d monitors via %s", len(monitor_handles), type(provider).__name__)
    return provider, monitor_handles, monitor_specs

# ----------- immediate synchonous non blocking actions ---------------

def _run_actions(args, actuators: Dict[str, AbstractActuator]) -> None:
    """
    Execute immediate actuator actions (non-threaded):
      - --stop
      - --set-velocity
    """
    # Stop actions
    for name in args.stop:
        if name not in actuators:
            raise SystemExit(f"--stop unknown actuator {name!r}")
        actuators[name].stop()

    # Set velocity actions
    for spec in args.set_velocity:
        name, vel = _parse_name_value(spec)
        if name not in actuators:
            raise SystemExit(f"--set-velocity unknown actuator {name!r}")
        actuators[name].set_velocity(float(vel))

# ----------------------------- main logic -----------------------------

def main() -> None:
    # For --config choices: same style as scan_runner (keys only)
    config_dir = os.environ.get("KIWI_SCAN_CONFIG_DIR", get_scan_config_dir())
    scan_configs = load_scan_configs(config_dir, None)  # keys only

    p = argparse.ArgumentParser(
        prog="actuator_runner",
        formatter_class=argparse.RawTextHelpFormatter,
        description=(
            "Actuator CLI (single-shot): start multiple monitors and motion commands concurrently.\n\n"
            "Examples:\n"
            "  actuator_runner --config mono \\\n"
            "    --monitor energy:rbv --monitor energy:status --set-velocity energy=5 --move energy=250\n\n"
            "  actuator_runner --config mono \\\n"
            "    --monitor energy:rbv --monitor slit:rbv --move energy=250 --move slit=0.1 --monitor-duration 10\n"
        ),
    )

    cfg = p.add_mutually_exclusive_group(required=True)
    cfg.add_argument("--config", choices=scan_configs.keys(), help="Preset config name (from KIWI_SCAN_CONFIG_DIR)")
    cfg.add_argument("--config-file", help="Path to YAML config file")

    p.add_argument("--replace", nargs="*", default=[], help="Replacements KEY=VALUE for ${KEY} tokens")
    p.add_argument(
        "--log-level",
        type=int,
        choices=range(0, 6),
        metavar="0-5",
        help="MBBO record level (0..5) mapped to python logging via scanlib helper",
    )

    # repeatable action options
    p.add_argument("--monitor", action="append", default=[], help="Repeatable. SPEC = NAME:source | NAME@PV | NAME")
    p.add_argument("--monitor-duration", type=float, default=None, help="Stop monitors after N seconds")
    p.add_argument("--monitor-count", type=int, default=None, help="Stop after N total monitor events")
    p.add_argument("--out", default=None, help="Optional output file (append).")
    p.add_argument("--move", action="append", default=[], help="Repeatable. SPEC = NAME=POS")
    p.add_argument(
        "--rel-move",
        action="append",
        default=[],
        help="Repeatable. SPEC = NAME=DELTA (also supports lists for MultiActuator, e.g. name=[0.1, -0.2])",
    )
    p.add_argument("--jog", action="append", default=[], help="Repeatable. SPEC = NAME=VEL")
    p.add_argument("--stop", action="append", default=[], help="Repeatable. SPEC = NAME")
    p.add_argument("--set-velocity", action="append", default=[], help="Repeatable. SPEC = NAME=VEL")

    p.add_argument("--keep-alive", action="store_true", help="Keep running until Ctrl+C (ignores moves done).")

    args = p.parse_args()

    if args.log_level is not None:
        set_valid_logging_level(args.log_level)

    try:
        raw_cfg, origin = _load_raw_config(args)
    except FileNotFoundError as exc:
        raise SystemExit(f"Config file not found: {exc}")

    # Show required replacements help for presets (like scan_runner)
    if args.config and not args.config_file:
        help_text, required = get_replacements_help_and_required(config_dir, [args.config + ".yaml"])
        if required:
            print(help_text)

    actuators = _build_actuators(raw_cfg)

    # Validate "monitors only" mode
    have_moves = bool(args.move or args.rel_move or args.jog or args.stop or args.set_velocity)
    have_monitors = bool(args.monitor)
    if have_monitors and not have_moves and not (args.monitor_duration or args.monitor_count or args.keep_alive):
        raise SystemExit(
            "You started monitors but provided no exit condition.\n"
            "Add --monitor-duration, --monitor-count, or --keep-alive."
        )

    # Setup shutdown handling
    stop_all = threading.Event()

    def _sigint(_signum, _frame):
        stop_all.set()

    signal.signal(signal.SIGINT, _sigint)

    t0 = time.time()

    # Writer thread + queue
    ev_q: "queue.Queue[dict]" = queue.Queue(maxsize=10000)
    writer = _EventWriter(ev_q, out_path=args.out, stop_event=stop_all)
    writer.start()

    # Monitor counters
    counter_lock = threading.Lock()
    events_seen = 0
    dropped = 0

    def _inc_seen() -> None:
        nonlocal events_seen
        with counter_lock:
            events_seen += 1

    def _inc_dropped() -> None:
        nonlocal dropped
        with counter_lock:
            dropped += 1

    def _get_counts() -> Tuple[int, int]:
        with counter_lock:
            return events_seen, dropped

    # Start monitors (if any)
    provider, monitor_handles, monitor_specs = _start_monitors(
        have_monitors=have_monitors,
        args=args,
        raw_cfg=raw_cfg,
        actuators=actuators,
        ev_q=ev_q,
        t0=t0,
        _inc_seen=_inc_seen,
        _inc_dropped=_inc_dropped,
    )
    
    # set stop, velocity, ...
    _run_actions(args, actuators)

    # If there are no async actions, exit now.
    if not (args.monitor or args.move or args.rel_move or args.jog):
        return
    # Submit moves/jogs concurrently
    used_motion_actuators: List[AbstractActuator] = []
    futures: List[Future] = []

    # serialize commands per actuator to avoid overlapping for same device
    per_act_lock: Dict[str, threading.Lock] = {k: threading.Lock() for k in actuators.keys()}

    def _with_lock(name: str, fn, *a, **kw):
        lock = per_act_lock[name]
        with lock:
            return fn(*a, **kw)

    max_workers = max(1, min(8, len(actuators)))  # keep it simple
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for spec in args.move:
            name, pos = _parse_name_value(spec)
            if name not in actuators:
                raise ValueError(f"--move unknown actuator {name!r}")
            act = actuators[name]
            used_motion_actuators.append(act)
            futures.append(ex.submit(_with_lock, name, act.run_move, float(pos), True))
 
        for spec in args.rel_move:
            name, delta = _parse_name_value_any(spec)
            if name not in actuators:
                raise ValueError(f"--rel-move unknown actuator {name!r}")
            act = actuators[name]
            used_motion_actuators.append(act)
            futures.append(ex.submit(_with_lock, name, act.run_rel_move, delta, True))

        for spec in args.jog:
            name, vel = _parse_name_value(spec)
            if name not in actuators:
                raise ValueError(f"--jog unknown actuator {name!r}")
            act = actuators[name]
            used_motion_actuators.append(act)
            futures.append(ex.submit(_with_lock, name, act.jog, float(vel), True))

        # Main wait loop: satisfy all active conditions unless Ctrl+C
        end_t = (time.time() + float(args.monitor_duration)) if args.monitor_duration is not None else None
        target_count = int(args.monitor_count) if args.monitor_count is not None else None

        moves_submitted = bool(futures)

        def _moves_done() -> bool:
            return all(f.done() for f in futures)

        try:
            while True:
                if stop_all.is_set():
                    break

                # Conditions
                conds: List[bool] = []

                # moves condition (ignored if keep-alive)
                if moves_submitted and not args.keep_alive:
                    conds.append(_moves_done())

                # duration condition
                if end_t is not None:
                    conds.append(time.time() >= end_t)

                # count condition
                if target_count is not None:
                    seen, _dr = _get_counts()
                    conds.append(seen >= target_count)

                # If there are no conditions (e.g. keep-alive only), run until Ctrl+C
                if conds and all(conds):
                    break

                time.sleep(0.05)

        finally:
            # If interrupted, best-effort stop motion actuators still running
            if stop_all.is_set():
                for act in used_motion_actuators:
                    try:
                        act.stop()
                    except Exception:
                        pass

            # Remove monitors
            if provider is not None:
                for pvname, handle in monitor_handles:
                    try:
                        provider.remove_monitor(pvname)
                    except Exception:
                        pass
                    try:
                        if hasattr(handle, "close"):
                            handle.close()
                    except Exception:
                        pass

            # stop writer thread
            stop_all.set()
            try:
                ev_q.put_nowait(None)  # sentinel
            except Exception:
                pass
            writer.join(timeout=2.0)

    seen, dr = _get_counts()
    if dr:
        logging.warning("Dropped %d monitor events (queue full).", dr)

    logging.debug("Config origin: %s", origin)
    print(f"Done. events_seen={seen} dropped={dr}")
