# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations
from dataclasses import asdict, is_dataclass, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
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


    @staticmethod
    def _absolute_path_value(value: Optional[str], base_dir: Optional[str] = None) -> Optional[str]:
        """Return an absolute, user-expanded path string for manifest references."""
        if not value:
            return None

        path = Path(str(value)).expanduser()
        if path.is_absolute():
            return str(path)

        if base_dir:
            return str(Path(base_dir).expanduser().joinpath(path).resolve(strict=False))

        return str(path.resolve(strict=False))

    @classmethod
    def _create_manifest_header(cls) -> Dict[str, Any]:
        """
        Create the initial manifest header structure.
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
          3. Recreate a new manifest in KIWI_SCAN_DATA_DIR
        """
        filename = cls.get_active_manifest()
        data_dir = os.environ.get("KIWI_SCAN_DATA_DIR")
        # No active manifest configured at all
        if filename is None:
            cls.logger.warning("No active manifest found")
            try:
                filename = cls.newmanifest(directory=data_dir)
                cls.logger.warning("Created new manifest because no active manifest existed: %s", filename)
                return cls(filename)
            except Exception:
                cls.logger.exception("Failed to create fallback manifest")
                return None

            return None

        path = Path(filename).expanduser()
        if not path.is_file():
            cls.logger.warning("Active manifest does not exist anymore: %s", path)
            try:
                filename = cls.newmanifest(directory=data_dir)
                cls.logger.warning("Created replacement manifest because active manifest was missing: %s", filename)
                return cls(filename)
            except Exception:
                cls.logger.exception("Failed to create replacement manifest")
                return None
            return None

        cls.logger.info("Using active manifest: %s", filename)
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
    def create_small_manifest(
        cls,
        data_files: Sequence[str],
        filename: Optional[str] = None,
        directory: Optional[str] = None,
        scan_type: Optional[str] = "external",
    ) -> str:
        """Create a new small manifest that references existing data files.

        This helper is intended for imported or externally produced scan data
        where kiwi-scan only needs a compact manifest with file references,
        for example before converting the data to another format.

        Returns:
            The created manifest filename.
        """
        if not data_files:
            raise ValueError("At least one data file is required")

        manifest_file = cls.newmanifest(filename=filename, directory=directory)
        writer = cls(manifest_file)

        for data_file in data_files:
            path = Path(str(data_file)).expanduser()
            writer.append_scan_config(
                config={},
                scan_type=scan_type,
                path=str(path.parent) if path.parent != Path("") else None,
                data_file=str(path),
                metadata_file=None,
                mode=cls.MODE_SMALL,
            )

        return manifest_file

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
        absolute_path = self._absolute_path_value(path)
        if mode == self.MODE_OFF:
            self.logger.info("Manifest writing disabled by manifest_mode=off")
            return None

        now = datetime.now().astimezone()
        scan_id = "scan_" + now.strftime("%Y%m%dT%H%M%S%z")
        self.logger.info("Appending scan to manifest: id=%s type=%s file=%s mode=%s",
            scan_id, scan_type, data_file, mode)

        entry = {
            "id": scan_id,
            "created_at": now.isoformat(timespec="seconds"),
            "scan_type": scan_type,
            "path": absolute_path,
            "data_file": self._absolute_path_value(data_file, absolute_path),
            "metadata_file": self._absolute_path_value(metadata_file, absolute_path),
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
    logger = logger
    def __init__(self, data_dir: Optional[str] = None):
        self.data_dir = self._resolve_data_dir(data_dir)
        self._warned_missing_active_manifest = False
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
        # Fall back to the currently active manifest
        if not value:
            cls.logger.info(f"{cls.ENV_DATA_DIR} is not set.")
            active_manifest = ManifestWriter.get_active_manifest()
            if active_manifest:
                active_path = Path(active_manifest).expanduser()
                if active_path.is_file():
                    return active_path.parent
        if not value:
            cls.logger.info("No active manifest.")
            return None
        path = Path(value).expanduser()
        if not path.is_dir():
            raise FileNotFoundError(f"Manifest directory does not exist: {path}")
        return path

    def _active_manifest_fallback(self) -> Optional[Path]:
        active = ManifestWriter.get_active_manifest()
        if not active:
            return None

        path = Path(active).expanduser()
        if path.is_file():
            self.logger.debug(f"Found active manifest fallback: {path}")
            return path

        self.logger.debug(f"Ignoring active manifest fallback because it does not exist: {path}")
        return None

    def list_manifests(self) -> List[Path]:
        """Return manifest*.yaml/yml files newest-first by manifest created_at."""
        paths = set()
        if self.data_dir is not None:
            for pattern in ("manifest*.yaml", "manifest*.yml"):
                paths.update(self.data_dir.glob(pattern))

        # Also include the active manifest even if it lives outside data_dir.
        active_manifest = ManifestWriter.get_active_manifest()
        if active_manifest:
            active_path = Path(active_manifest).expanduser()
            if active_path.is_file():
                paths.add(active_path)
            else:
                if not self._warned_missing_active_manifest:
                    self.logger.warning("Configured active manifest does not exist: %s", active_path)
                    self._warned_missing_active_manifest = True
                else:
                    self.logger.debug("Configured active manifest still missing: %s", active_path)

        manifests = sorted(paths, key=self._manifest_sort_key, reverse=True)
        if manifests:
            self.logger.debug(f"Found {len(manifests)} manifest file(s) in {self.data_dir}")
            return manifests

        active_manifest = self._active_manifest_fallback()
        if active_manifest is not None:
            return [active_manifest]

        self.logger.debug(f"Found no manifest file(s) in {self.data_dir}")
        return []

    def select_manifest(self, index: int = 0) -> Path:
        if index < 0:
            raise IndexError("--manifest-index must be >= 0")

        manifests = self.list_manifests()
        if not manifests:
            location = self.data_dir if self.data_dir is not None else f"{self.ENV_DATA_DIR} is not set"
            raise FileNotFoundError(
                f"No manifest*.yaml or manifest*.yml files found in {location}, "
                "and no active manifest fallback is available"
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



    def list_files(
        self,
        manifest_file: str,
        *,
        include_meta: bool = False,
        include_manifest: bool = False,
        missing: bool = False,
    ) -> List[Path]:
        """Return files referenced by one manifest as absolute paths.

        By default only existing scan data files are returned. Metadata sidecars
        and the manifest file itself can be included explicitly. Duplicate paths
        are removed while preserving manifest order.
        """
        manifest_path = Path(manifest_file).expanduser()
        refs = self.list_scan_refs(str(manifest_path))
        result: List[Path] = []
        seen = set()

        def add(path: Optional[Path]) -> None:
            if path is None:
                return
            full = path.expanduser().resolve(strict=False)
            if full in seen:
                return
            if missing or full.exists():
                seen.add(full)
                result.append(full)

        if include_manifest:
            add(manifest_path)

        for ref in refs:
            add(ref.data_file)
            if include_meta:
                add(ref.metadata_file)

        return result

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
        if self.data_dir is not None:
            candidates.append(self.data_dir / path)

        for candidate in candidates:
            if candidate.exists():
                self.logger.debug(f"Resolved manifest path reference {value!r} to {candidate}")
                return candidate

        self.logger.debug(
            f"Manifest path reference {value!r} did not exist relative to "
            f"{', '.join(str(candidate.parent) for candidate in candidates)}; using {candidates[0]}"
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
