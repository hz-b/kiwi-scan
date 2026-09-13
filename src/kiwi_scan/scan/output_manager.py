# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

"""Main scan-output file lifecycle and runtime writing state."""

from __future__ import annotations

import os
import random
import string
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Callable, Iterable, Iterator, Optional


class OutputManager:
    """Own main scan-file state without owning scan orchestration.

    The manager owns the output filename, lazy file creation, header state,
    runtime data-writing flag, and the lock that serializes changes to those
    values. BaseScan remains responsible for deciding *when* metadata
    monitoring, point persistence, and other scan lifecycle actions occur.

    Point persistence uses :meth:`point_write`, which holds the output lock once
    across the enabled/file/header checks and the caller's enqueue/write step.
    Internal helpers used inside that context never reacquire the lock.
    """

    def __init__(
        self,
        *,
        data_dir: str,
        requested_output_file: str,
        data_writing_enabled: bool = True,
        output_timestamp: Optional[str] = None,
        header_factory: Optional[Callable[[], Iterable[str]]] = None,
    ) -> None:
        self.data_dir = os.path.abspath(data_dir)
        self.requested_output_file = requested_output_file
        self.output_timestamp = output_timestamp or (
            datetime.now().astimezone().strftime("%Y%m%d%H%M%S")
        )
        self._lock = threading.RLock()
        self._data_writing_enabled = bool(data_writing_enabled)
        self._header_written = False
        self._output_file: Optional[str] = None
        self._header_factory = header_factory

    @contextmanager
    def locked(self) -> Iterator[None]:
        """Serialize a BaseScan orchestration section using the output lock."""
        with self._lock:
            yield

    @contextmanager
    def point_write(self) -> Iterator[Optional[str]]:
        """Prepare one point write under a single lock acquisition.

        The returned path is ``None`` when runtime data writing is disabled.
        Otherwise the output file exists and its header has been written before
        control is yielded to the caller. The lock remains held until the caller
        leaves the context, keeping runtime enable/disable changes synchronized
        with point enqueue/write operations.
        """
        with self._lock:
            if not self._data_writing_enabled:
                yield None
                return

            output_file = self._ensure_output_file_exists_unlocked()
            if output_file is None:
                yield None
                return

            if not self._header_written:
                if self._header_factory is None:
                    raise RuntimeError("Output header factory is not configured")
                self._write_header_unlocked(self._header_factory())

            yield output_file

    def set_header_factory(
        self,
        header_factory: Callable[[], Iterable[str]],
    ) -> None:
        """Set the lazy header builder used when writing starts without a header."""
        with self._lock:
            self._header_factory = header_factory

    @property
    def output_file(self) -> Optional[str]:
        with self._lock:
            return self._output_file

    @output_file.setter
    def output_file(self, path: Optional[str]) -> None:
        with self._lock:
            normalized = None if path is None else str(path)
            if normalized != self._output_file:
                self._header_written = False
            self._output_file = normalized

    @property
    def header_written(self) -> bool:
        with self._lock:
            return self._header_written

    def get_data_writing_enabled(self) -> bool:
        with self._lock:
            return self._data_writing_enabled

    def set_data_writing_enabled(self, enabled: bool) -> bool:
        """Set runtime writing state and return whether the value changed."""
        enabled = bool(enabled)
        with self._lock:
            if self._data_writing_enabled == enabled:
                return False
            self._data_writing_enabled = enabled
            return True

    def _generate_and_create_file_unlocked(
        self,
        base_filename: Optional[str] = None,
    ) -> str:
        filename = (
            self.requested_output_file
            if base_filename is None
            else base_filename
        )

        while True:
            name, ext = os.path.splitext(filename)
            new_filename = os.path.join(
                self.data_dir,
                f"{name}-{self.output_timestamp}{ext}",
            )
            if not os.path.exists(new_filename):
                with open(new_filename, "w", encoding="utf-8"):
                    pass
                return new_filename

            random_suffix = "".join(
                random.choices(  # nosec B311
                    string.ascii_letters + string.digits,
                    k=6,
                )
            )
            new_filename = os.path.join(
                self.data_dir,
                f"{name}-{self.output_timestamp}_{random_suffix}{ext}",
            )
            if not os.path.exists(new_filename):
                with open(new_filename, "w", encoding="utf-8"):
                    pass
                return new_filename

    def generate_and_create_file(
        self,
        base_filename: Optional[str] = None,
    ) -> str:
        """Create a timestamped output file and return its absolute path."""
        with self._lock:
            return self._generate_and_create_file_unlocked(base_filename)

    def _ensure_output_file_exists_unlocked(self) -> Optional[str]:
        if not self._data_writing_enabled:
            return None
        if self._output_file is None:
            self._output_file = self._generate_and_create_file_unlocked()
            self._header_written = False
        return self._output_file

    def ensure_output_file_exists(self) -> Optional[str]:
        """Create the configured output file lazily when writing is enabled."""
        with self._lock:
            return self._ensure_output_file_exists_unlocked()

    def _write_header_unlocked(self, headers: Iterable[str]) -> Optional[str]:
        if not self._data_writing_enabled:
            return None
        if self._header_written:
            return self._output_file

        output_file = self._ensure_output_file_exists_unlocked()
        if output_file is None:
            return None

        with open(output_file, "w", encoding="utf-8") as file:
            file.write("\t".join(str(header) for header in headers) + "\n")
        self._header_written = True
        return output_file

    def write_header(self, headers: Iterable[str]) -> Optional[str]:
        """Write the main data-file header once and return the output path."""
        with self._lock:
            return self._write_header_unlocked(headers)
