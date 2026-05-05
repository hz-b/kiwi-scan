# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations
from dataclasses import asdict, is_dataclass, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from importlib.metadata import version, PackageNotFoundError
import os
import socket
import getpass
import yaml
import logging
    
logger = logging.getLogger(__name__)

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

    MODE_FULL = "full"
    MODE_SMALL = "small"
    MODE_OFF = "off"
    VALID_MODES = {MODE_FULL, MODE_SMALL, MODE_OFF}

    DEFAULT_STATE_FILE = Path.home() / ".config" / "kiwi-scan" / "active_manifest"
    DEFAULT_MANIFEST_DIR = Path.cwd()
    # logger - available in classmethods 
    logger = logger

    def __init__(self, filename: str):
        self.path = Path(filename).expanduser()
        self.logger.debug("Initialized ManifestWriter with path: %s", self.path)
    
    @classmethod
    def _create_manifest_header(cls) -> Dict[str, Any]:
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
        Create/select a new active manifest file. Timestamped manifest filename or filename.
        The selected manifest is written to the state file.

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

    @classmethod
    def normalize_mode(cls, mode: Optional[str]) -> str:
        """Return a validated manifest mode.

        Accepted values are case-insensitive:
          - full: write the complete scan entry including config
          - small: write scan references only, without the full config
          - off: do not write a manifest entry
        """
        if mode is None:
            return cls.MODE_FULL

        normalized = str(mode).strip().lower()
        if normalized not in cls.VALID_MODES:
            allowed = ", ".join(sorted(cls.VALID_MODES))
            raise ValueError(f"Unsupported manifest_mode {mode!r}. Expected one of: {allowed}")
        return normalized

    def append_scan_config(
        self,
        config: Any,
        scan_type: Optional[str] = None,
        path: Optional[str] = None,
        data_file: Optional[str] = None,
        metadata_file: Optional[str] = None,
        mode: str = MODE_FULL,
    ) -> str:
        """
        Append one scan configuration entry to the manifest.

        Returns:
            The generated scan id, or None when mode is ``off``.
        """
        mode = self.normalize_mode(mode)
        if mode == self.MODE_OFF:
            self.logger.info("Manifest writing disabled by manifest_mode=off")
            return None

        now = datetime.now().astimezone()
        scan_id = "scan_" + now.strftime("%Y%m%dT%H%M%S%z")
        self.logger.info(
            "Appending scan to manifest: id=%s type=%s file=%s mode=%s",
            scan_id,
            scan_type,
            data_file,
            mode,
        )

        entry = {
            "id": scan_id,
            "created_at": now.isoformat(timespec="seconds"),
            "scan_type": scan_type,
            "path": path,
            "data_file": data_file,
            "metadata_file": metadata_file,
            "manifest_mode": mode,
        }
        if mode == self.MODE_FULL:
            entry["config"] = self._to_plain_data(config)

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
            data = self._create_manifest_header() 
        
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


@dataclass(frozen=True)
class ManifestScanRef:
    """Resolved reference to one scan entry in a manifest file."""

    manifest_file: Path
    scan_id: Optional[str]
    created_at: Optional[datetime]
    data_file: Optional[Path]
    metadata_file: Optional[Path]
    scan_type: Optional[str]
    raw: Dict[str, Any]

class ManifestResolver:
    """Read manifest files and select scan-data or metadata references."""

    ENV_DATA_DIR = "KIWI_SCAN_DATA_DIR"

    def __init__(self, data_dir: Optional[str] = None):
        self.logger = logger
        self.data_dir = self._resolve_data_dir(data_dir)
        self.logger.debug(f"Using manifest data directory: {self.data_dir}")

    @classmethod
    def from_env(cls) -> "ManifestResolver":
        return cls(os.environ.get(cls.ENV_DATA_DIR))

    @classmethod
    def from_manifest_file(cls, manifest_file: str) -> "ManifestResolver":
        """Create a resolver using an explicit manifest's parent as fallback dir."""
        return cls(str(Path(manifest_file).expanduser().parent))

    @classmethod
    def _resolve_data_dir(cls, data_dir: Optional[str]) -> Path:
        value = data_dir or os.environ.get(cls.ENV_DATA_DIR)
        if not value:
            raise ValueError(
                f"{cls.ENV_DATA_DIR} is not set. Set it, use --manifest-file, "
                "or provide explicit file=... in every --series."
            )

        path = Path(value).expanduser()
        if not path.is_dir():
            raise FileNotFoundError(f"Manifest directory does not exist: {path}")
        return path

    def list_manifests(self) -> List[Path]:
        """Return manifest*.yaml/yml files newest-first by manifest created_at."""
        paths = set()
        for pattern in ("manifest*.yaml", "manifest*.yml"):
            paths.update(self.data_dir.glob(pattern))
        
        manifests = sorted(paths, key=self._manifest_sort_key, reverse=True)
        self.logger.debug(f"Found {len(manifests)} manifest file(s) in {self.data_dir}")
        return manifests

    def select_manifest(self, index: int = 0) -> Path:
        if index < 0:
            raise IndexError("--manifest-index must be >= 0")

        manifests = self.list_manifests()
        if not manifests:
            raise FileNotFoundError(
                f"No manifest*.yaml or manifest*.yml files found in {self.data_dir}"
            )
        if index >= len(manifests):
            raise IndexError(
                f"--manifest-index {index} is out of range; found {len(manifests)} manifest file(s)"
            )
        selected = manifests[index]
        self.logger.debug(f"Selected manifest index {index}: {selected}")
        return selected

    def list_scan_refs(self, manifest_file: str) -> List[ManifestScanRef]:
        path = Path(manifest_file).expanduser()
        data = self.load_manifest(path)
        scans = data.get("scans") or []
        if not isinstance(scans, list):
            raise ValueError(f"Manifest {path} has invalid 'scans' section; expected a list")

        refs: List[ManifestScanRef] = []
        for entry in scans:
            if not isinstance(entry, dict):
                continue
            refs.append(
                ManifestScanRef(
                    manifest_file=path,
                    scan_id=entry.get("id"),
                    created_at=self._parse_datetime(entry.get("created_at")),
                    data_file=self._resolve_ref_path(path, entry.get("data_file")),
                    metadata_file=self._resolve_ref_path(path, entry.get("metadata_file")),
                    scan_type=entry.get("scan_type"),
                    raw=entry,
                )
            )

        sorted_refs = sorted(refs, key=self._scan_sort_key, reverse=True)
        self.logger.debug(f"Resolved {len(sorted_refs)} scan reference(s) from manifest {path}")
        return sorted_refs
 
    def select_scan_ref(self, manifest_file: str, scan_index: int = 0) -> ManifestScanRef:
        if scan_index < 0:
            raise IndexError("--scan-index must be >= 0")

        refs = self.list_scan_refs(manifest_file)
        if not refs:
            raise ValueError(f"Manifest {manifest_file} contains no scan entries")
        if scan_index >= len(refs):
            raise IndexError(
                f"--scan-index {scan_index} is out of range; found {len(refs)} scan entry/entries"
            )
        return refs[scan_index]

    def select_file(
        self,
        *,
        source_type: str,
        manifest_file: Optional[str] = None,
        manifest_index: int = 0,
        scan_index: int = 0,
    ) -> Path:
        """Select one scan or metadata file from a manifest entry."""
        source_type = source_type.lower()
        if source_type not in {"scan", "meta"}:
            raise ValueError("source type must be 'scan' or 'meta'")

        if manifest_file is None:
            manifest_path = self.select_manifest(manifest_index)
        else:
            manifest_path = Path(manifest_file).expanduser()
            if not manifest_path.is_file():
                raise FileNotFoundError(f"Manifest file not found: {manifest_path}")

        ref = self.select_scan_ref(str(manifest_path), scan_index)
        selected = ref.data_file if source_type == "scan" else ref.metadata_file
        field_name = "data_file" if source_type == "scan" else "metadata_file"

        if selected is None:
            scan_label = ref.scan_id or f"index {scan_index}"
            raise ValueError(f"Manifest scan {scan_label!r} has no {field_name} reference")
        if not selected.exists():
            raise FileNotFoundError(
                f"Manifest scan {ref.scan_id or scan_index!r} refers to missing {field_name}: {selected}"
            )
        return selected

    @staticmethod
    def load_manifest(path: Path) -> Dict[str, Any]:
        path = Path(path).expanduser()
        if not path.is_file():
            raise FileNotFoundError(f"Manifest file not found: {path}")
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            raise ValueError(f"Manifest {path} is invalid; expected a YAML mapping")
        return data

    def _manifest_sort_key(self, path: Path) -> float:
        try:
            data = self.load_manifest(path)
            created_at = (data.get("manifest") or {}).get("created_at")
            dt = self._parse_datetime(created_at)
            if dt is not None:
                return dt.timestamp()
        except Exception:
            pass
        return path.stat().st_mtime

    @staticmethod
    def _scan_sort_key(ref: ManifestScanRef) -> float:
        if ref.created_at is not None:
            return ref.created_at.timestamp()
        return 0.0

    def _resolve_ref_path(self, manifest_file: Path, value: Optional[str]) -> Optional[Path]:
        if not value:
            return None

        path = Path(str(value)).expanduser()
        if path.is_absolute():
            return path

        candidates = [manifest_file.parent / path, self.data_dir / path]
        for candidate in candidates:
            if candidate.exists():
                self.logger.debug(f"Resolved manifest path reference {value!r} to {candidate}")
                return candidate

        self.logger.debug(
            f"Manifest path reference {value!r} did not exist relative to "
            f"{manifest_file.parent} or {self.data_dir}; using {candidates[0]}"
        )
        return candidates[0]

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value
        else:
            try:
                dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            except ValueError:
                return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
