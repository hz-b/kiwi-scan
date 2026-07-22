# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Dict, List

from .model import ExportBundle

logger = logging.getLogger(__name__)


class ExportWriter(ABC):
    """Base class for concrete scan data format writers."""

    name = ""
    extension = ""

    @abstractmethod
    def write(self, bundle: ExportBundle, output_file: Path) -> Path:
        """ Write data *bundle* to *output_file* and return the written path. """


_WRITER_FACTORIES: Dict[str, Callable[..., ExportWriter]] = {}


def register_writer(name: str, factory: Callable[..., ExportWriter]) -> None:
    """Register a writer factory under a format name."""
    normalized = str(name).strip().lower()
    if not normalized:
        raise ValueError("writer name must not be empty")
    if normalized in _WRITER_FACTORIES:
        raise ValueError("export writer %r is already registered" % normalized)
    _WRITER_FACTORIES[normalized] = factory
    logger.debug("Registered export writer name=%s factory=%r", normalized, factory)


def available_writers() -> List[str]:
    """ Return registered writer names sorted alphabetically. """
    writers = sorted(_WRITER_FACTORIES.keys())
    logger.debug("Available export writers: %s", writers)
    return writers


def get_writer(name: str, **kwargs) -> ExportWriter:
    """ Instantiate a registered writer by name. """
    normalized = str(name).strip().lower()
    try:
        factory = _WRITER_FACTORIES[normalized]
    except KeyError as exc:
        known = ", ".join(available_writers()) or "<none>"
        raise ValueError("Unknown export format %r. Available formats: %s" % (name, known)) from exc
    logger.debug("Creating export writer name=%s kwargs=%s", normalized, sorted(kwargs.keys()))
    writer = factory(**kwargs)
    logger.debug("Created export writer instance type=%s", type(writer).__name__)
    return writer
