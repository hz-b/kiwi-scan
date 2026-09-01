# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import signal
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

from kiwi_scan.actuator.factory import create_actuators
from kiwi_scan.actuator.single import AbstractActuator, PvEvent
from kiwi_scan.datamodels import ActuatorConfig, MonitorSpec
from kiwi_scan.scan.tools import (
    get_scan_config_dir,
    load_scan_configs,
    set_valid_logging_level,
)
from kiwi_scan.yaml_loader import (
    get_env_replacements,
    get_replacements_help_and_required,
    parse_replacements,
    yaml_loader,
)

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s"
)

def _parse_name_value(spec: str) -> Tuple[str, float]:
    # NAME=VAL
    if "=" not in spec:
        raise ValueError(f"Expected NAME=VALUE, got {spec!r}")
    name, s_val = spec.split("=", 1)
    name = name.strip()
    s_val = s_val.strip()
    if not name:
        raise ValueError(f"Empty name in {spec!r}")
    if not s_val:
        raise ValueError(f"Empty value in {spec!r}")
    try:
        return name, float(s_val)
    except ValueError as exc:
        raise ValueError(f"Expected numeric value in {spec!r}") from exc

def _parse_name_value_any(spec: str) -> Tuple[str, Any]:
    """ Parse NAME=VALUE where VALUE is a number or a JSON list. """
    if "=" not in spec:
        raise ValueError(f"Expected NAME=VALUE, got {spec!r}")

    name, value_text = spec.split("=", 1)
    name = name.strip()
    value_text = value_text.strip()

    if not name:
        raise ValueError(f"Empty name in {spec!r}")

    if not value_text:
        raise ValueError(f"Empty value in {spec!r}")

    # A value starting with "[" may be a list, for example: motor=[1.0, 2.0]
    # Try JSON first. If it is not valid JSON, continue below and
    # let the normal numeric-value error handling produce the final error.
    if value_text.startswith("["):
        try:
            return name, json.loads(value_text)
        except json.JSONDecodeError:
            pass

    # Otherwise expect a single numeric value, for example: motor=1.5
    try:
        return name, float(value_text)
    except ValueError as exc:
        raise ValueError(f"Expected numeric value or JSON list in {spec!r}") from exc


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

def _pick_monitor_provider( actuators: Dict[str, AbstractActuator]) -> AbstractActuator:
    """ Return the first actuator that supports monitor subscriptions. """
    for actuator in actuators.values():
        if actuator.supports_monitors():
            return actuator

    raise RuntimeError(
        "No actuator backend supports monitors in this config."
    )

# ----------------------------- monitor + output -----------------------------

class _EventWriter(threading.Thread):
    def __init__(
        self,
        q: queue.Queue[dict],
        *,
        out_path: Optional[str],
        stop_event: threading.Event,
    ):
        super().__init__(daemon=True)

        # monitor events
        self._q = q

        # When this event is set, the writer thread should stop.
        self._stop_event = stop_event

        self._out_path = out_path

    def _emit(self, line: str, file_handle=None) -> None:
        """ Print one monitor line and optionally write it to the output file. """
        print(line)

        if file_handle is not None:
            file_handle.write(line + "\n")

    def _run_writer(self, file_handle=None) -> None:
        """Process queued monitor events until the writer is stopped."""
        while not self._stop_event.is_set():
            try:
                # Wait briefly for the next monitor event.
                item = self._q.get(timeout=0.1)
            except queue.Empty:
                # Nothing arrived yet. Go around and check stop_event again.
                continue

            # None is used as a special "stop now" message.
            if item is None:
                break

            # Extract the values we want to display.
            mon = item.get("monitor_id")
            name = item.get("actuator")
            src = item.get("source")
            pv = item.get("pvname")
            rel = item.get("t_rel_s")
            val = item.get("value")

            line = (
                f"[mon#{mon} {name}:{src}] "
                f"{rel:9.3f}s pv={pv} value={val!r}"
            )

            self._emit(line, file_handle)

    def run(self) -> None:
        """ Thread entry point. """

        # print events to the terminal.
        if self._out_path is None:
            self._run_writer()
            return

        # --out was given - context manager closes it automatically on exit.
        with open(
            self._out_path,
            "a",
            encoding="utf-8",
            buffering=1,
        ) as file_handle:
            self._run_writer(file_handle)


# TODO: Align actuator_runner monitor handling with SubscriptionManager.
def _start_monitors(
    *,
    have_monitors: bool,
    args,
    raw_cfg: Dict[str, Any],
    actuators: Dict[str, AbstractActuator],
    ev_q: queue.Queue[dict],
    t0: float,
    _inc_seen,
    _inc_dropped,
) -> Tuple[Optional[AbstractActuator], List[Tuple[str, Any]]]:
    """Start all monitor subscriptions requested on the command line."""

    provider: Optional[AbstractActuator] = None
    monitor_handles: List[Tuple[str, Any]] = []

    # no --monitor arguments were given.
    if not have_monitors:
        return provider, monitor_handles

    # provides the actual monitor subscriptions.
    provider = _pick_monitor_provider(actuators)

    # resolve PV names.
    actuators_raw = raw_cfg.get("actuators") or {}

    for monitor_id, spec_text in enumerate(args.monitor, start=1):
        monitor_spec = MonitorSpec.from_arg(spec_text)

        name = monitor_spec.name
        source = monitor_spec.source

        # First check whether the requested actuator actually exists.
        if name not in actuators_raw:
            raise ValueError(f"--monitor refers to unknown actuator {name!r}")

        raw_actuator = actuators_raw[name]

        # An existing actuator entry must contain a configuration dictionary.
        if not isinstance(raw_actuator, dict):
            raise TypeError(f"Configuration for actuator {name!r} must be a dictionary")

        actuator_config = ActuatorConfig.from_dict(raw_actuator)
        pvname = monitor_spec.resolve_pv(actuator_config)

        def _mk_cb(_monitor_id: int, _name: str, _source: str, _pvname: str):
            def _cb(ev: PvEvent) -> None:
                now = time.time()

                payload = {
                    "monitor_id": _monitor_id,
                    "actuator": _name,
                    "source": _source,
                    "pvname": getattr(ev, "pvname", _pvname),
                    "value": getattr(ev, "value", None),
                    "t_abs_s": now,
                    "t_rel_s": now - t0,
                    "timestamp": getattr(ev, "timestamp", None),
                    "posixseconds": getattr(ev, "posixseconds", None),
                    "nanoseconds": getattr(ev, "nanoseconds", None),
                    "severity": getattr(ev, "severity", None),
                    "status": getattr(ev, "status", None),
                    # raw may be large or not JSON serializable.
                    # The final JSON writer handles that with default=str.
                    "raw": getattr(ev, "raw", None),
                }

                try:
                    ev_q.put_nowait(payload)
                    _inc_seen()
                except queue.Full:
                    _inc_dropped()

            return _cb

        callback = _mk_cb(monitor_id, name, source, pvname)

        handle = provider.add_monitor(pvname, user_callback=callback)

        monitor_handles.append((pvname, handle))

    logger.info("Started %d monitors via %s", len(monitor_handles), type(provider).__name__)

    return provider, monitor_handles


def _validate_cli_specs(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    raw_cfg: Dict[str, Any],
    actuators: Dict[str, AbstractActuator],
) -> None:
    """Validate repeatable CLI specs before starting monitors or motion."""
    acts_raw = raw_cfg.get("actuators") or {}

    def _check_known(option: str, name: str) -> None:
        if name not in actuators:
            known = ", ".join(sorted(actuators)) or "<none>"
            parser.error(f"{option} unknown actuator {name!r}. Known actuators: {known}")

    for option, specs, parser_fn in (
        ("--move", args.move, _parse_name_value),
        ("--rel-move", args.rel_move, _parse_name_value_any),
        ("--jog", args.jog, _parse_name_value),
        ("--set-velocity", args.set_velocity, _parse_name_value),
    ):
        for spec in specs:
            try:
                name, _value = parser_fn(spec)
            except ValueError as exc:
                parser.error(f"{option}: {exc}")
            _check_known(option, name)

    for name in args.stop:
        _check_known("--stop", name)

    for spec in args.monitor:
        try:
            monitor_spec = MonitorSpec.from_arg(spec)
        except ValueError as exc:
            parser.error(f"--monitor: {exc}")

        name = monitor_spec.name
        raw_act = acts_raw.get(name)
        if not isinstance(raw_act, dict):
            known = ", ".join(sorted(acts_raw)) or "<none>"
            parser.error(
                f"--monitor unknown actuator {name!r}. "
                "Use NAME:source, NAME@PV, or NAME. "
                f"Known actuators: {known}"
            )

        try:
            monitor_spec.resolve_pv(ActuatorConfig.from_dict(raw_act))
        except ValueError as exc:
            parser.error(f"--monitor {spec!r}: {exc}")


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
        choices=range(6),
        metavar="0-5",
        help="MBBO record level (0..5) mapped to python logging via scanlib helper",
    )

    # repeatable action options
    p.add_argument(
        "--monitor",
        action="append",
        default=[],
        metavar="NAME[:SOURCE]|NAME@PV",
        help="Repeatable. Examples: --monitor energy:rbv, --monitor energy:status, --monitor energy@IOC:PV",
    )
    p.add_argument("--monitor-duration", type=float, default=None, help="Stop monitors after N seconds")
    p.add_argument("--monitor-count", type=int, default=None, help="Stop after N total monitor events")
    p.add_argument("--out", default=None, help="Optional output file (append).")
    p.add_argument("--move", action="append", default=[], metavar="NAME=POS", help="Repeatable. Example: --move energy=250")
    p.add_argument(
        "--rel-move",
        action="append",
        default=[],
        metavar="NAME=DELTA",
        help="Repeatable. Example: --rel-move energy=1.0. Also supports lists for MultiActuator, e.g. undulator=[0.1, -0.2]",
    )
    p.add_argument("--jog", action="append", default=[], metavar="NAME=VEL", help="Repeatable. Example: --jog energy=0.2")
    p.add_argument("--stop", action="append", default=[], metavar="NAME", help="Repeatable. Example: --stop energy")
    p.add_argument("--set-velocity", action="append", default=[], metavar="NAME=VEL", help="Repeatable. Example: --set-velocity energy=5")

    p.add_argument("--keep-alive", action="store_true", help="Keep running until Ctrl+C (ignores moves done).")

    args = p.parse_args()

    if args.log_level is not None:
        set_valid_logging_level(args.log_level)

    try:
        raw_cfg, origin = _load_raw_config(args)
    except (FileNotFoundError, ValueError, TypeError) as exc:
        p.error(f"failed to load config: {exc}")

    # Show required replacements help for presets (like scan_runner)
    if args.config and not args.config_file:
        help_text, required = get_replacements_help_and_required(config_dir, [args.config + ".yaml"])
        if required:
            print(help_text)

    try:
        actuators = create_actuators(raw_cfg.get("actuators") or {})
    except (ValueError, TypeError, ConnectionError) as exc:
        p.error(f"failed to build actuators: {exc}")

    _validate_cli_specs(p, args, raw_cfg, actuators)

    # Validate "monitors only" mode
    have_moves = bool(args.move or args.rel_move or args.jog or args.stop or args.set_velocity)
    have_monitors = bool(args.monitor)
    if have_monitors and not have_moves and not (args.monitor_duration or args.monitor_count or args.keep_alive):
        p.error(
            "You started monitors but provided no exit condition. "
            "Add --monitor-duration, --monitor-count, or --keep-alive."
        )

    # Setup shutdown handling.
    # First Ctrl-C requests a graceful stop and sends stop commands.
    # Second Ctrl-C forces process exit    
    stop_all = threading.Event()
    sigint_count = 0

    def _sigint(_signum, _frame):
        nonlocal sigint_count
        sigint_count += 1

        if sigint_count >= 2:
            print(
                "Second Ctrl-C received: forcing actuator_runner exit.",
                file=sys.stderr,
                flush=True,
            )
            os._exit(130)

        print(
            "Ctrl-C received: stopping actuators. "
            "Press Ctrl-C again to force exit.",
            file=sys.stderr,
            flush=True,
        )
        stop_all.set()

        for act in actuators.values():
            try:
                act.stop()
            except Exception:
                logger.exception("Failed to stop actuator during Ctrl-C handling")

    signal.signal(signal.SIGINT, _sigint)

    t0 = time.time()

    # Writer thread + queue
    ev_q: queue.Queue[dict] = queue.Queue(maxsize=10000)
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
    provider, monitor_handles = _start_monitors(
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
    per_act_lock: Dict[str, threading.Lock] = {
        name: threading.Lock()
        for name in actuators
    }

    def _with_lock(name: str, fn, *args, **kwargs):
        with per_act_lock[name]:
            return fn(*args, **kwargs)

    max_workers = max(1, min(8, len(actuators)))  # keep it simple
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for spec in args.move:
            name, pos = _parse_name_value(spec)
            if name not in actuators:
                p.error(f"--move unknown actuator {name!r}")
            act = actuators[name]
            used_motion_actuators.append(act)
            futures.append(ex.submit(_with_lock, name, act.run_move, float(pos), True))
 
        for spec in args.rel_move:
            name, delta = _parse_name_value_any(spec)
            if name not in actuators:
                p.error(f"--rel-move unknown actuator {name!r}")
            act = actuators[name]
            used_motion_actuators.append(act)
            futures.append(ex.submit(_with_lock, name, act.run_rel_move, delta, True))

        for spec in args.jog:
            name, vel = _parse_name_value(spec)
            if name not in actuators:
                p.error(f"--jog unknown actuator {name!r}")
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
                        logger.exception("Failed to stop actuator during shutdown")
            # Remove monitors
            if provider is not None:
                for pvname, _handle in monitor_handles:
                    try:
                        provider.remove_monitor(pvname)
                    except Exception:
                        logger.exception(f"Failed to remove monitor {pvname} during shutdown")

            # Stop writer thread.
            stop_all.set()
            writer.join(timeout=2.0)

            if writer.is_alive():
                logger.warning("Writer thread did not stop cleanly")            

    seen, dropped = _get_counts()
    if dropped:
        logger.warning("Dropped %d monitor events (queue full).", dropped)

    logger.debug("Config origin: %s", origin)
    print(f"Done. events_seen={seen} dropped={dropped}")

if __name__ == "__main__":
    main()
