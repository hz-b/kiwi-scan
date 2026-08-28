# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import getpass
import io
import logging
import os
import socket
import tarfile
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import yaml

logger = logging.getLogger(__name__)


def parse_manifest_datetime(value: Any) -> Optional[datetime]:
    """Parse a manifest datetime value and normalize naive values to UTC."""
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


def get_package_version(package_name: str = "kiwi-scan") -> str:
    """ TODO: move into tool subpackage """
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
    VALID_MODES = frozenset({MODE_FULL, MODE_SMALL, MODE_OFF})

    DEFAULT_STATE_FILE = Path.home() / ".config" / "kiwi-scan" / "active_manifest"
    DEFAULT_MANIFEST_DIR = Path.cwd()
    # logger - available in classmethods 
    logger = logger

    def __init__(self, filename: str):
        self.path = Path(filename).expanduser()
        self.logger.debug("Initialized ManifestWriter with path: %s", self.path)


    @staticmethod
    def _absolute_path_value(value: Optional[str], base_dir: Optional[str] = None) -> Optional[str]:
        """ 
        Return an absolute, expanded path like (~/data/..) string 
        optionally from base dir to resolve relative path. 
        """
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
    def from_active(cls) -> Optional[ManifestWriter]:
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
            filename = str(base_dir / f"{prefix}_{now:%Y%m%d_%H%M%S}.yaml")
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
        """ 
        Create a new small manifest that references existing data files.

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
        """
        Validate and return cls manifest mode.

        Accepted values are case-insensitive and can be overwritten:
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


@dataclass(frozen=True)
class ManifestDeletePlan:
    """Plan for archiving and deleting manifest-related files.

    The plan is intentionally side-effect free. It can be printed by a CLI,
    inspected by tests, or passed to :class:`ManifestArchiveDeleter` for the
    destructive archive-and-delete step.
    """

    manifest_files: List[Path]
    files_to_archive: List[Path]
    missing_files: List[Path]
    skipped_files: List[Path]
    bundle_file: Path

    @property
    def files_to_delete(self) -> List[Path]:
        """Existing regular files that should be removed after archiving."""
        return list(self.files_to_archive)


@dataclass(frozen=True)
class ManifestDeleteResult:
    """Result of an archive-and-delete operation."""

    bundle_file: Path
    archived_files: List[Path]
    deleted_files: List[Path]
    missing_files: List[Path]
    skipped_files: List[Path]
    failed_files: List[Path]
    dry_run: bool = False


class ManifestArchiveDeleter:
    """ Archive files from a :class:`ManifestDeletePlan` and delete them safely. """

    logger = logger

    @classmethod
    def _create_delete_bundle(
        cls,
        bundle: Path,
        files_to_archive: Iterable[Path],
    ) -> Tuple[List[Path], List[Path]]:
        """
        Create and verify a recovery bundle for the requested files.
        """
        archived_files: List[Path] = []
        failed_files: List[Path] = []

        cls.logger.info("Creating delete bundle: %s", bundle)
        try:
            tar_context = tarfile.open(bundle, "x:gz") # noqa: SIM115 - Keep archive creation outside for exclusive check
        except FileExistsError as exc:
            raise FileExistsError(f"Delete bundle already exists; refusing to overwrite recovery archive: {bundle}") from exc

        with tar_context as tar:
            restore_map = {
                "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "bundle_file": str(bundle),
                "files": [],
            }

            for index, path in enumerate(files_to_archive):
                full = path.expanduser().resolve(strict=False)
                if not full.is_file():
                    cls.logger.warning("Skipping non-file during archive: %s", full)
                    failed_files.append(full)
                    continue

                arcname = f"files/{index:04d}_{full.name}"
                cls.logger.debug("Adding %s as %s", full, arcname)
                tar.add(str(full), arcname=arcname, recursive=False)
                archived_files.append(full)
                restore_map["files"].append(
                    {
                        "archive_name": arcname,
                        "original_path": str(full),
                    }
                )

            manifest_bytes = yaml.safe_dump(restore_map, sort_keys=False).encode("utf-8")
            info = tarfile.TarInfo("restore_map.yaml")
            info.size = len(manifest_bytes)
            info.mtime = int(datetime.now(timezone.utc).timestamp())
            tar.addfile(info, io.BytesIO(manifest_bytes))

        if not bundle.is_file() or bundle.stat().st_size <= 0:
            raise OSError(f"Delete bundle was not created correctly: {bundle}")

        return archived_files, failed_files

    @classmethod
    def _delete_archived_files(
        cls,
        archived_files: Iterable[Path],
    ) -> Tuple[List[Path], List[Path]]:
        """Delete files that were successfully stored in the recovery bundle."""
        deleted_files: List[Path] = []
        failed_files: List[Path] = []

        for path in archived_files:
            try:
                cls.logger.debug("Deleting archived file: %s", path)
                path.unlink()
                deleted_files.append(path)
            except OSError:
                cls.logger.exception("Failed to delete archived file: %s", path)
                failed_files.append(path)

        return deleted_files, failed_files

    @classmethod
    def archive_and_delete(
        cls,
        plan: ManifestDeletePlan,
        *,
        dry_run: bool = False,
    ) -> ManifestDeleteResult:
        """
        Archive all planned files and then delete the archived files.
        No file is deleted unless it was present in the plan and the recovery bundle was successfully created and verified. 
        """
        cls.logger.debug("Archive/delete requested: files=%d missing=%d skipped=%d bundle=%s dry_run=%s",
            len(plan.files_to_archive), len(plan.missing_files), len(plan.skipped_files), plan.bundle_file, dry_run)

        if dry_run:
            cls.logger.info("Dry-run archive/delete plan for %d file(s)", len(plan.files_to_archive))
            return ManifestDeleteResult(
                bundle_file=plan.bundle_file,
                archived_files=[],
                deleted_files=[],
                missing_files=list(plan.missing_files),
                skipped_files=list(plan.skipped_files),
                failed_files=[],
                dry_run=True,
            )

        if not plan.files_to_archive:
            raise ValueError("No existing regular files to archive and delete")

        bundle = plan.bundle_file.expanduser()
        bundle.parent.mkdir(parents=True, exist_ok=True)

        archived_files, archive_failures = cls._create_delete_bundle(
            bundle,
            plan.files_to_archive,
        )
        deleted_files, delete_failures = cls._delete_archived_files(archived_files)
        failed_files = archive_failures + delete_failures

        cls.logger.info("Archive/delete complete: archived=%d deleted=%d failed=%d bundle=%s",
            len(archived_files), len(deleted_files), len(failed_files), bundle)
        return ManifestDeleteResult(
            bundle_file=bundle,
            archived_files=archived_files,
            deleted_files=deleted_files,
            missing_files=list(plan.missing_files),
            skipped_files=list(plan.skipped_files),
            failed_files=failed_files,
            dry_run=False,
        )

class ManifestResolver:
    """Read manifest files and select scan-data or metadata references."""

    ENV_DATA_DIR = "KIWI_SCAN_DATA_DIR"
    logger = logger
    def __init__(self, data_dir: Optional[str] = None):
        self.data_dir = self._resolve_data_dir(data_dir)
        self._warned_missing_active_manifest = False
        self.logger.debug(f"Using manifest data directory: {self.data_dir}")

    @classmethod
    def from_env(cls) -> ManifestResolver:
        return cls(os.environ.get(cls.ENV_DATA_DIR))

    @classmethod
    def from_manifest_file(cls, manifest_file: str) -> ManifestResolver:
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
        """ Return manifest yaml files newest-first by manifest created_at. """
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

    def resolve_manifests(
        self,
        manifest_files: Optional[Iterable[str | Path]] = None,
    ) -> List[Path]:
        """
        Return manifest files or discover manifests via the resolver.
        Files are expanded and validated.
        """
        explicit_files = list(manifest_files or [])
        if not explicit_files:
            self.logger.debug( "No explicit manifest files supplied; discovering manifests in %s", self.data_dir)
            manifests = self.list_manifests()
            self.logger.debug("Manifest discovery selected %d file(s): %s", len(manifests), manifests)
            return manifests

        manifests = [Path(filename).expanduser() for filename in explicit_files]
        self.logger.debug("Validating %d explicit manifest file(s): %s", len(manifests), manifests)

        missing = [path for path in manifests if not path.is_file()]
        if missing:
            self.logger.debug("Explicit manifest files not found: %s", missing)
            raise FileNotFoundError("Manifest file not found: " + ", ".join(str(path) for path in missing))

        self.logger.debug("Using explicit manifest files: %s", manifests)
        return manifests

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
            raise TypeError(f"Manifest {path} has invalid 'scans' section; expected a list")

        refs: List[ManifestScanRef] = []
        for entry in scans:
            if not isinstance(entry, dict):
                continue
            refs.append(
                ManifestScanRef(
                    manifest_file=path,
                    scan_id=entry.get("id"),
                    created_at=parse_manifest_datetime(entry.get("created_at")),
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
            raise IndexError("scan-index must be >= 0")

        refs = self.list_scan_refs(manifest_file)
        if not refs:
            raise ValueError(f"Manifest {manifest_file} contains no scan entries")
        if scan_index >= len(refs):
            raise IndexError(f"scan-index {scan_index} is out of range; found {len(refs)} scan entry/entries")
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

    def iterate_files(
        self,
        manifest_files: Iterable[str | Path],
        *,
        include_meta: bool = False,
        include_manifest: bool = False,
        missing: bool = False,
    ) -> Iterator[Path]:
        """
        Generate unique files referenced by multiple manifests.
        """

        manifests = list(manifest_files)
        self.logger.debug("Iterating files from %d manifest(s): include_meta=%s include_manifest=%s missing=%s",
            len(manifests), include_meta, include_manifest, missing)

        seen = set()
        for manifest_file in manifests:
            manifest_path = Path(manifest_file).expanduser()
            files = self.list_files(
                str(manifest_path),
                include_meta=include_meta,
                include_manifest=include_manifest,
                missing=missing,
            )
            self.logger.debug("Manifest %s resolved to %d matching file(s)", manifest_path, len(files))

            for path in files:
                if path in seen:
                    self.logger.debug("Skipping duplicate manifest file reference: %s", path)
                    continue

                seen.add(path)
                self.logger.debug("yield: file: %s", path)
                yield path

        self.logger.debug("Completed manifest file iteration with %d unique file(s)", len(seen))

    def _resolve_delete_manifests( self, manifest_files: Optional[Iterable[str | Path]]) -> List[Path]:
        """ Resolve and validate manifests selected for archive/delete. """
        if manifest_files is None:
            selected_manifests = self.list_manifests()
        else:
            selected_manifests = [Path(path).expanduser() for path in manifest_files]

        if not selected_manifests:
            location = (
                self.data_dir
                if self.data_dir is not None
                else f"{self.ENV_DATA_DIR} is not set"
            )
            raise FileNotFoundError(f"No manifest files selected from {location}")

        manifest_paths: List[Path] = []
        for manifest in selected_manifests:
            full = manifest.expanduser().resolve(strict=False)
            if not full.is_file():
                raise FileNotFoundError(f"Manifest file not found: {full}")
            manifest_paths.append(full)

        return manifest_paths

    def _collect_delete_candidates(
        self,
        manifest_paths: Iterable[Path],
        *,
        include_meta: bool,
        include_manifest: bool,
    ) -> Tuple[List[Path], List[Path], List[Path]]:
        """ Classify referenced paths for a planned archive/delete operation. """
        seen = set()
        files_to_archive: List[Path] = []
        missing_files: List[Path] = []
        skipped_files: List[Path] = []

        def add_candidate(path: Optional[Path]) -> None:
            if path is None:
                return

            full = path.expanduser().resolve(strict=False)
            if full in seen:
                self.logger.debug("Skipping duplicate delete candidate: %s", full)
                return

            seen.add(full)

            if not full.exists():
                self.logger.debug("Delete candidate is missing: %s", full)
                missing_files.append(full)
                return

            if not full.is_file():
                self.logger.warning("Delete candidate is not a regular file: %s", full)
                skipped_files.append(full)
                return

            self.logger.debug("Planned delete candidate: %s", full)
            files_to_archive.append(full)

        for manifest in manifest_paths:
            if include_manifest:
                add_candidate(manifest)

            for ref in self.list_scan_refs(str(manifest)):
                add_candidate(ref.data_file)
                if include_meta:
                    add_candidate(ref.metadata_file)

        return files_to_archive, missing_files, skipped_files

    def plan_delete_bundle(
        self,
        manifest_files: Optional[Iterable[str | Path]] = None,
        *,
        include_meta: bool = False,
        include_manifest: bool = True,
        bundle_dir: str | Path = "/tmp",
        bundle_prefix: str = "kiwi-scan-delete",
    ) -> ManifestDeletePlan:
        """ Build a plan to archive and delete manifest files.

        Args:
            manifest_files: Explicit manifest files. If omitted, all manifests discovered by this resolver are used.
            include_meta: Include metadata sidecar files referenced by scans.
            include_manifest: Include the manifest YAML files themselves.
            bundle_dir: Directory where the archive bundle should be created.
            bundle_prefix: Prefix for the generated archive filename.

        Returns:
            A :class:`ManifestDeletePlan` containing existing regular files to archive/delete, 
            missing references, skipped pathes, and the target bundle path.
        """
        manifest_paths = self._resolve_delete_manifests(manifest_files)

        self.logger.debug("Planning archive/delete for %d manifest(s), include_meta=%s include_manifest=%s",
            len(manifest_paths), include_meta, include_manifest)

        files_to_archive, missing_files, skipped_files = self._collect_delete_candidates(
            manifest_paths,
            include_meta=include_meta,
            include_manifest=include_manifest,
        )

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        bundle_path = (Path(bundle_dir).expanduser() / f"{bundle_prefix}_{timestamp}.tar.gz")

        self.logger.info("Delete plan: manifests=%d files=%d missing=%d skipped=%d bundle=%s",
            len(manifest_paths), len(files_to_archive), len(missing_files), len(skipped_files), bundle_path)

        return ManifestDeletePlan(
            manifest_files=manifest_paths,
            files_to_archive=files_to_archive,
            missing_files=missing_files,
            skipped_files=skipped_files,
            bundle_file=bundle_path,
        )

    def select_file(
        self,
        *,
        source_type: str,
        manifest_file: Optional[str] = None,
        manifest_index: int = 0,
        scan_index: int = 0,
    ) -> Path:
        """ Select one scan or metadata file from a manifest entry."""
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
            raise FileNotFoundError(f"Manifest scan {ref.scan_id or scan_index!r} refers to missing {field_name}: {selected}")
        return selected

    @staticmethod
    def load_manifest(path: Path) -> Dict[str, Any]:
        path = Path(path).expanduser()
        if not path.is_file():
            raise FileNotFoundError(f"Manifest file not found: {path}")
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            raise TypeError(f"Manifest {path} is invalid; expected a YAML mapping")
        return data

    def _manifest_sort_key(self, path: Path) -> float:
        try:
            data = self.load_manifest(path)
        except (OSError, UnicodeError, yaml.YAMLError, ValueError) as exc:
            self.logger.debug( "Could not read creation time from manifest %s; using file modification time: %s", path, exc)
        else:
            manifest = data.get("manifest") or {}

            if not isinstance(manifest, dict):
                self.logger.debug( "Manifest section in %s is not a mapping; using file modification time", path)
            else:
                created_at = manifest.get("created_at")
                dt = parse_manifest_datetime(created_at)
                if dt is not None:
                    try:
                        return dt.timestamp()
                    except (OverflowError, OSError, ValueError) as exc:
                        self.logger.debug( "Could not convert creation time from manifest %s; using file modification time: %s", path, exc)

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

        candidates = [manifest_file.parent / path]
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
