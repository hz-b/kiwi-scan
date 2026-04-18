# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""
This module provides the startup helpers for the import of scans and plugins.

Loading order:
1. Built-in
2. Optional

__all__:
- Plugin classes register themselves via decorators at import time.
- If a module is not imported, its class is not registered.
- Debug logging here helps diagnose missing registrations 
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import logging
import os
import pkgutil
import sys
from pathlib import Path
from typing import Set
from kiwi_scan.scan.registry import load_all_scan_types
from importlib.metadata import version, PackageNotFoundError

__all__ = ["__version__", "load_all_plugins", "load_all_scan_types"]
try:
    __version__ = version("kiwi-scan")
except PackageNotFoundError:
    # fallback for editable installs / no metadata
    __version__ = "0+unknown"

logger = logging.getLogger(__name__)

PLUGIN_ENVVAR = "KIWI_SCAN_PLUGIN_PATH"


def _module_suffix_for_path(pyfile: Path) -> str:
    """
    Avoids module-name collisions when two different plugin directories
    contain files with the same basename.
    """
    resolved = str(pyfile.resolve())
    digest = hashlib.sha1(resolved.encode("utf-8")).hexdigest()[:12]
    return f"{pyfile.stem}_{digest}"


def _import_plugin_file(pyfile: Path, raise_on_error: bool = False) -> None:
    """
    Import plugin module.
    """
    pyfile = pyfile.resolve()
    module_name = f"kiwi_scan_ext_{_module_suffix_for_path(pyfile)}"

    try:
        logger.debug("Importing plugin file: %s as %s", pyfile, module_name)

        spec = importlib.util.spec_from_file_location(module_name, pyfile)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not create import spec for {pyfile}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        logger.debug("Imported plugin file successfully: %s", pyfile)

    except Exception:
        logger.exception("Failed to import plugin file %s", pyfile)
        if raise_on_error:
            raise


def _import_package_modules(package_name: str, raise_on_error: bool = False) -> None:
    """
    Import all modules provided by kiwi scan
    """
    try:
        package = importlib.import_module(package_name)
    except Exception:
        logger.exception("Failed to import package %s", package_name)
        if raise_on_error:
            raise
        return

    package_path = getattr(package, "__path__", None)
    if not package_path:
        logger.debug("Package %s has no __path__; nothing to scan", package_name)
        return

    modules = sorted(pkgutil.iter_modules(package_path), key=lambda m: m.name)
    logger.debug("Scanning built-in plugin package %s (%d modules)", package_name, len(modules))

    for module_info in modules:
        if module_info.name.startswith("_"):
            logger.debug("Skipping private module %s.%s", package_name, module_info.name)
            continue

        fullname = f"{package_name}.{module_info.name}"
        try:
            logger.debug("Importing built-in plugin module %s", fullname)
            importlib.import_module(fullname)
            logger.debug("Imported built-in plugin module %s", fullname)
        except Exception:
            logger.exception("Failed to import built-in plugin module %s", fullname)
            if raise_on_error:
                raise


def load_all_plugins(raise_on_error: bool = False) -> None:
    """
    Import all plugin modules.
    """
    package_root = __package__ or __name__
    builtin_package = f"{package_root}.plugin_concrete"

    raw = os.environ.get(PLUGIN_ENVVAR, "")

    logger.debug("Starting plugin loading")
    logger.debug("Builtin plugin package: %s", builtin_package)
    logger.debug("%s=%r", PLUGIN_ENVVAR, raw)

    # Always load the plugins that ship with kiwi_scan.
    _import_package_modules(builtin_package, raise_on_error=raise_on_error)

    # Then load any external plugins configured by the user.
    if not raw.strip():
        logger.debug("%s not set; built-in plugins loaded only", PLUGIN_ENVVAR)
    else:
        seen: Set[Path] = set()

        for entry in raw.split(os.pathsep):
            entry = entry.strip()
            if not entry:
                continue

            p = Path(entry).expanduser().resolve()
            logger.debug("Considering plugin path entry: %s", p)

            if not p.exists():
                logger.warning("Plugin path does not exist: %s", p)
                continue

            if p.is_file():
                if p.suffix != ".py":
                    logger.debug("Skipping non-Python file: %s", p)
                    continue

                if p in seen:
                    logger.debug("Skipping duplicate plugin file: %s", p)
                    continue

                _import_plugin_file(p, raise_on_error=raise_on_error)
                seen.add(p)
                continue

            if p.is_dir():
                pyfiles = sorted(
                    candidate
                    for candidate in p.glob("*.py")
                    if not candidate.name.startswith("_")
                )
                logger.debug("Scanning plugin directory %s (%d files)", p, pyfiles.__len__())

                for pyfile in pyfiles:
                    pyfile = pyfile.resolve()
                    if pyfile in seen:
                        logger.debug("Skipping already imported plugin file: %s", pyfile)
                        continue

                    _import_plugin_file(pyfile, raise_on_error=raise_on_error)
                    seen.add(pyfile)

                continue

            logger.warning("Plugin path is neither a file nor directory: %s", p)

    try:
        from kiwi_scan.plugin.registry import PLUGIN_REGISTRY

        logger.debug(
            "Plugin registry now contains %d entries: %s",
            len(PLUGIN_REGISTRY),
            sorted(PLUGIN_REGISTRY.keys()),
        )
    except Exception:
        logger.exception("Could not inspect plugin registry after loading")
