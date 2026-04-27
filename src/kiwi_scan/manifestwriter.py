# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from importlib.metadata import version, PackageNotFoundError
import os
import socket
import getpass
import yaml
import logging

def get_package_version(package_name: str = "kiwi-scan") -> str:
    try:
        return version(package_name)
    except PackageNotFoundError:
        return "unknown"

class ManifestWriter:
    """
    Maintain scan history by adding scan info to manifest files.

    The active manifest path is stored outside the scan object so that
    independent CLI/API scans can append to the same manifest until
    newmanifest() selects a new one.
    """

    ENV_MANIFEST_FILE = "KIWI_SCAN_MANIFEST_FILE"
    ENV_MANIFEST_STATE_FILE = "KIWI_SCAN_MANIFEST_STATE_FILE"

    DEFAULT_STATE_FILE = Path.home() / ".config" / "kiwi-scan" / "active_manifest"
    DEFAULT_MANIFEST_DIR = Path.cwd()
    # logger - available in classmethods 
    logger = logging.getLogger(__name__)

    def __init__(self, filename: str):
        self.path = Path(filename).expanduser()
        self.logger.debug("Initialized ManifestWriter with path: %s", self.path)

    @classmethod
    def _create_manifest_header(self) -> Dict[str, Any]:
        """
        Create the initial manifest header structure.
        Can be called from other class methods and has access to 
        class level attributes for later extension.
        Returns:
            A dictionary containing the manifest header and empty scan list.
        """
        now = datetime.now().astimezone()
        header = {
            "manifest": {
                "kiwi_scan_version": get_package_version(),
                "created_at": now.isoformat(timespec="seconds"),
                "host": socket.gethostname(),
                "user": getpass.getuser(),
            },
            "scans": [],
        }
        return header
    
    @classmethod
    def from_active(cls) -> Optional["ManifestWriter"]:
        """
        Return a writer for the currently active manifest.

        Resolution order:
          1. KIWI_SCAN_MANIFEST_FILE
          2. Path stored in the active-manifest state file
          3. None
        """
        filename = cls.get_active_manifest()
        cls.logger.debug("No active manifest found")
        if filename is None:
            return None
        cls.logger.debug("Using active manifest: %s", filename)
        return cls(filename)

    @classmethod
    def get_active_manifest(cls) -> Optional[str]:
        env_file = os.environ.get(cls.ENV_MANIFEST_FILE)
        if env_file:
            return env_file

        state_file = cls._state_file()
        if not state_file.exists():
            return None

        content = state_file.read_text(encoding="utf-8").strip()
        return content or None

    @classmethod
    def newmanifest(
        cls,
        filename: Optional[str] = None,
        directory: Optional[str] = None,
        prefix: str = "manifest",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Create/select a new active manifest file.

        If filename is omitted, a timestamped manifest filename is created.
        The selected manifest is written to the persistent state file.

        Returns:
            The new active manifest filename.
        """
        now = datetime.now().astimezone()

        if filename is None:
            base_dir = Path(directory).expanduser() if directory else cls.DEFAULT_MANIFEST_DIR
            filename = str(base_dir / ("%s_%s.yaml" % (prefix, now.strftime("%Y%m%d_%H%M%S"))))

        path = Path(filename).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)

        if not path.exists():
            header = cls._create_manifest_header()
            with path.open("w", encoding="utf-8") as f:
                yaml.safe_dump(header, f, sort_keys=False, default_flow_style=False)

        cls._set_active_manifest(str(path))
        cls.logger.info("Set active manifest: %s", path)
        return str(path)

    @classmethod
    def _state_file(cls) -> Path:
        override = os.environ.get(cls.ENV_MANIFEST_STATE_FILE)
        if override:
            return Path(override).expanduser()
        return cls.DEFAULT_STATE_FILE

    @classmethod
    def _set_active_manifest(cls, filename: str) -> None:
        state_file = cls._state_file()
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text(str(Path(filename).expanduser()) + "\n", encoding="utf-8")
        cls.logger.debug("Updated state file: %s -> %s", state_file, filename)

    # ------------------------------------------------------------------
    # Append scan entries
    # ------------------------------------------------------------------

    def append_scan_config(
        self,
        config: Any,
        scan_type: Optional[str] = None,
        path: Optional[str] = None,
        data_file: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Append one scan configuration entry to the manifest.

        Returns:
            The generated scan id.
        """
        now = datetime.now().astimezone()
        scan_id = "scan_" + now.strftime("%Y%m%dT%H%M%S%z")
        self.logger.info(
            "Appending scan to manifest: id=%s type=%s file=%s",
            scan_id,
            scan_type,
            data_file,
        )

        entry = {
            "id": scan_id,
            "created_at": now.isoformat(timespec="seconds"),
            "scan_type": scan_type,
            "path": path,
            "data_file": data_file,
            "config": self._to_plain_data(config),
        }
        # add optional meta data
        if metadata:
            entry["metadata"] = metadata

        self._append_entry(entry)
        self.logger.debug("Appended entry: %s", scan_id)
        return scan_id

    def _append_entry(self, entry: Dict[str, Any]) -> None:
        """
        Read, append and rewriting the manifest. YAML structure layout:

            manifest:
              ...
            scans:
              - ...
              - ...
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)

        if self.path.exists() and self.path.stat().st_size > 0:
            self.logger.debug("Reading existing manifest: %s", self.path)
            with self.path.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        else:
            data = cls._create_manifest_header() 
        
        if "scans" not in data or data["scans"] is None:
            data["scans"] = []

        data["scans"].append(entry)

        with self.path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False, default_flow_style=False)
        self.logger.debug("Manifest written successfully: %s", self.path)

    @staticmethod
    def _to_plain_data(value: Any) -> Any:
        if is_dataclass(value):
            return asdict(value)
        return value
