# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional, Tuple

from kiwi_scan.actuator.single import AbstractActuator
from kiwi_scan.actuator.tools import load_actuators, run_monitors
from kiwi_scan.datamodels import MonitorSpec
from kiwi_scan.scan.tools import (
    get_scan_config_dir,
    load_scan_configs,
    set_valid_logging_level,
)
from kiwi_scan.yaml_loader import (
    get_env_replacements,
    get_replacements_help_and_required,
    parse_replacements,
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

def _resolve_config_input(args, config_dir: str) -> Tuple[str, Dict[str, str]]:
    """Return the selected YAML path and merged CLI/environment replacements."""
    replacements = parse_replacements(args.replace or [])
    replacements.update(get_env_replacements("KIWI_SCAN"))

    if args.config_file:
        return args.config_file, replacements

    config_file = os.path.join(config_dir, f"{args.config}.yaml")
    return config_file, replacements

def _validate_cli_specs(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    actuators: Dict[str, AbstractActuator],
) -> None:
    """Validate repeatable CLI specs before starting monitors or motion."""
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
        if name not in actuators:
            known = ", ".join(sorted(actuators)) or "<none>"
            parser.error(
                f"--monitor unknown actuator {name!r}. "
                "Use NAME:source, NAME@PV, or NAME. "
                f"Known actuators: {known}"
            )

        try:
            monitor_spec.resolve_pv(actuators[name].config)
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

    config_file, replacements = _resolve_config_input(args, config_dir)

    # Show required replacements help for presets (like scan_runner)
    if args.config and not args.config_file:
        help_text, required = get_replacements_help_and_required(config_dir, [args.config + ".yaml"])
        if required:
            print(help_text)

    try:
        actuators = load_actuators(config_file, replacements)
    except (FileNotFoundError, ValueError, TypeError, ConnectionError) as exc:
        p.error(f"failed to load actuators: {exc}")

    completed = False
    try:
        result = _run_loaded_actuators(p, args, actuators)
        completed = True
    finally:
        closed = _close_actuators(actuators)
        if completed and not closed:
            raise SystemExit(1)

    if result is not None:
        seen, dropped = result
        if dropped:
            logger.warning("Dropped %d monitor events (queue full).", dropped)
        logger.debug("Config origin: %s", config_file)
        print(f"Done. events_seen={seen} dropped={dropped}")


def _close_actuators(actuators: Dict[str, AbstractActuator]) -> bool:
    """Release all owned instances, retaining any earlier execution error."""
    closed = True
    for actuator in {id(act): act for act in actuators.values()}.values():
        try:
            actuator.close()
        except Exception:
            closed = False
            logger.exception("Failed to close actuator")
    return closed


def _run_loaded_actuators(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    actuators: Dict[str, AbstractActuator],
) -> Optional[Tuple[int, int]]:
    _validate_cli_specs(parser, args, actuators)
    have_actions = bool(args.move or args.rel_move or args.jog or args.stop or args.set_velocity)
    if args.monitor and not have_actions and not (args.monitor_duration or args.monitor_count or args.keep_alive):
        parser.error(
            "You started monitors but provided no exit condition. "
            "Add --monitor-duration, --monitor-count, or --keep-alive."
        )

    stop_all = threading.Event()
    futures: List[Future] = []
    sigint_count = 0
    stopped = False

    def _shutdown():
        nonlocal stopped
        stop_all.set()
        # Python 3.8 has no shutdown(cancel_futures=...). Running workers
        # waiting for an actuator lock also check stop_all after acquiring it.
        for future in futures:
            future.cancel()
        if stopped:
            return
        stopped = True
        for act in {id(act): act for act in actuators.values()}.values():
            try:
                act.stop()
            except Exception:
                logger.exception("Failed to stop actuator during shutdown")

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
        _shutdown()

    previous_sigint_handler = signal.signal(signal.SIGINT, _sigint)
    try:
        _run_actions(args, actuators)
        if not (args.monitor or args.move or args.rel_move or args.jog):
            return None
        return _run_motion_commands(args, actuators, stop_all, futures, _shutdown)
    except BaseException:
        _shutdown()
        raise
    finally:
        signal.signal(signal.SIGINT, previous_sigint_handler)


def _motion_commands(args, actuators):
    """Keep the existing move, relative move, then jog submission order."""
    for spec in args.move:
        name, value = _parse_name_value(spec)
        yield name, actuators[name].run_move, value
    for spec in args.rel_move:
        name, value = _parse_name_value_any(spec)
        yield name, actuators[name].run_rel_move, value
    for spec in args.jog:
        name, value = _parse_name_value(spec)
        yield name, actuators[name].jog, value


def _run_motion_commands(
    args: argparse.Namespace,
    actuators: Dict[str, AbstractActuator],
    stop_all: threading.Event,
    futures: List[Future],
    shutdown: Callable[[], None],
) -> Tuple[int, int]:
    per_act_lock = {name: threading.Lock() for name in actuators}

    def _with_lock(name, command, value):
        with per_act_lock[name]:
            if stop_all.is_set():
                return
            try:
                result = command(value, True)
                if result is False:
                    raise RuntimeError(f"Actuator {name!r} rejected command")
            except BaseException as exc:
                # Set the event before releasing the lock: the next command
                # for this actuator must not start after a worker failure.
                stop_all.set()
                logger.error("Actuator %s command failed: %s", name, exc)
                raise

    seen, dropped = 0, 0
    with ThreadPoolExecutor(max_workers=max(1, min(8, len(actuators)))) as executor:
        try:
            for name, command, value in _motion_commands(args, actuators):
                futures.append(executor.submit(_with_lock, name, command, value))

            if args.monitor:
                completion_check = None
                if futures and not args.keep_alive:
                    completion_check = lambda: all(future.done() for future in futures)
                seen, dropped = run_monitors(
                    actuators,
                    args.monitor,
                    duration=args.monitor_duration,
                    count=args.monitor_count,
                    out_path=args.out,
                    stop_event=stop_all,
                    completion_check=completion_check,
                )
            # Monitoring may end while commands still run (duration/count
            # with --keep-alive). Keep observing errors before executor exit
            # starts its blocking wait, so a failed worker can stop its peers.
            while not stop_all.is_set():
                if (args.monitor or not args.keep_alive) and all(future.done() for future in futures):
                    break
                stop_all.wait(0.05)
        except BaseException:
            # Stop hardware before the executor waits for synchronous workers.
            shutdown()
            raise
        finally:
            if stop_all.is_set():
                shutdown()

    # A completed future may have failed. Inspect results after every worker
    # has finished, and never print the normal completion message on failure.
    if any(not future.cancelled() and future.exception() is not None for future in futures):
        raise SystemExit(1)
    return seen, dropped

if __name__ == "__main__":
    main()
