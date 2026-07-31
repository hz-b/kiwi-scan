# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Union

from kiwi_scan.data.loader import DataLoader
from kiwi_scan.data.manifestwriter import ManifestResolver
from kiwi_scan.data.metadata_loader import parse_metadata_file
from .model import ExportBundle, ExportScan

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


class EmptyScanDataError(ValueError):
    """Raised when a scan file exists but contains no scan data."""

def _contains_non_comment_content(path: Path) -> bool:
    """Return True when *path* contains at least one non-empty, non-comment line."""
    if path.stat().st_size == 0:
        return False

    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                return True
    return False

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


def _resolve_reference_path(
    manifest_file: Path,
    value: Optional[Any],
    extra_bases: Sequence[Optional[Path]] = (),
) -> Optional[Path]:
    """
    Resolve one manifest path, including relocated absolute references.
    Manifests could contain absolute paths from the machine that created the scan. 
    If path does not exists, try the same file or directory below.
    Existing absolute paths preferred.
    """
    if value is None or value == "":
        logger.debug("Manifest referencea is empty for manifest=%s", manifest_file)
        return None

    path = Path(str(value)).expanduser()
    manifest_dir = Path(manifest_file).expanduser().resolve(strict=False).parent
    bases = [
        Path(base).expanduser()
        for base in extra_bases
        if base is not None
    ]

    candidates = []

    def add_candidate(candidate: Path) -> None:
        candidate = Path(candidate).expanduser()
        if candidate not in candidates:
            candidates.append(candidate)

    if path.is_absolute():
        # First preserve the original reference when it is still valid.
        add_candidate(path)

        # A relocated entry path is the strongest hint for scan/metadata files.
        for base in bases:
            add_candidate(base / path.name)

        # Then try useful path tails below the manifest directory.
        if path.parent.name:
            add_candidate(manifest_dir / path.parent.name / path.name)
        add_candidate(manifest_dir / path.name)
    else:
        for base in bases:
            add_candidate(base / path)
        add_candidate(manifest_dir / path)

    for candidate in candidates:
        if candidate.exists():
            if candidate == path:
                logger.debug("Using existing absolute manifest reference %s", candidate)
            elif path.is_absolute():
                logger.debug(
                    "Relocated missing absolute manifest reference %s -> %s",
                    path,
                    candidate,
                )
            else:
                logger.debug("Resolved manifest reference %r to %s", value, candidate)
            return candidate

    # Keep the original absolute path in error messages. For relative paths,
    # return the normal manifest-relative location.
    fallback = path if path.is_absolute() else candidates[-1]
    logger.debug(
        "Manifest reference %r was not found; tried: %s; using fallback %s",
        value,
        ", ".join(str(candidate) for candidate in candidates),
        fallback,
    )
    return fallback


def _load_scan_dataframe(data_file: Path, data_dir: Optional[Path] = None):
    logger.debug("Loading scan dataframe data_file=%s data_dir=%s", data_file, data_dir)

    # Detect zero-byte, whitespace-only, and comment-only files before pandas is
    # called. DataLoader otherwise logs pandas' EmptyDataError and returns None,
    # which is indistinguishable from other load failures at this level.
    if not _contains_non_comment_content(data_file):
        raise EmptyScanDataError("Scan data file is empty: %s" % data_file)

    loader = DataLoader(
        str(data_file),
        data_dir=str(data_dir) if data_dir is not None else None,
    )
    df = loader.load_data()
    if df is None:
        raise FileNotFoundError("Could not load scan data file: %s" % data_file)
    if df.empty:
        raise EmptyScanDataError("Scan data file contains no data rows: %s" % data_file)
    logger.debug("Loaded scan dataframe rows=%d columns=%d from %s", len(df), len(df.columns), data_file)
    return df


def _load_metadata(metadata_file: Optional[Path], include_metadata: bool):
    if not include_metadata or metadata_file is None:
        logger.debug("Metadata loading skipped include_metadata=%s metadata_file=%s", include_metadata, metadata_file)
        return None
    if not metadata_file.exists():
        logger.warning("Metadata file does not exist: %s", metadata_file)
        return None
    metadata = parse_metadata_file(str(metadata_file))
    logger.debug("Metadata load result file=%s loaded=%s", metadata_file, metadata is not None)
    return metadata


def load_export_bundle_from_scan_file(
    data_file: PathLike,
    *,
    metadata_file: Optional[PathLike] = None,
    scan_id: Optional[str] = None,
    scan_type: Optional[str] = None,
    include_metadata: bool = True,
) -> ExportBundle:
    """ Load one kiwi-scan data file into an export bundle. """

    data_path = Path(data_file).expanduser()
    logger.debug(
        "Loading bundle from scan file data=%s metadata=%s scan_id=%r scan_type=%r include_metadata=%s",
        data_path, metadata_file, scan_id, scan_type, include_metadata,
    )
    if not data_path.is_file():
        raise FileNotFoundError("Scan data file not found: %s" % data_path)

    meta_path = Path(metadata_file).expanduser() if metadata_file else None
    df = _load_scan_dataframe(data_path, data_path.parent)
    metadata = _load_metadata(meta_path, include_metadata)

    scan = ExportScan(
        scan_id=scan_id or data_path.stem,
        scan_type=scan_type,
        data_file=data_path,
        metadata_file=meta_path,
        data=df,
        metadata=metadata,
        created_at=None,
        manifest_entry={},
    )
    bundle = ExportBundle(scans=[scan])
    logger.debug("Created single-scan bundle, label=%s", scan.label)
    return bundle


def load_export_bundle_from_manifest(
    manifest_file: PathLike,
    *,
    include_metadata: bool = True,
    skip_missing_data: bool = False,
) -> ExportBundle:
    """ 
    Load all scan files from one manifest into an export bundle.
    Manifest scan order is preserved. ``#S 1``, ``#S 2`` then match the scan series.
    """
    manifest_path = Path(manifest_file).expanduser()
    logger.debug(
        "Loading bundle from manifest=%s include_metadata=%s skip_missing_data=%s",
        manifest_path, include_metadata, skip_missing_data,
    )
    manifest_data = ManifestResolver.load_manifest(manifest_path)
    scans_raw = manifest_data.get("scans") or []
    if not isinstance(scans_raw, list):
        raise ValueError("Manifest %s has invalid 'scans' section; expected a list" % manifest_path)

    logger.debug("Manifest contains %d raw scan entries", len(scans_raw))
    export_scans = []
    skipped_empty = 0  # ignore empty data files
    for index, entry in enumerate(scans_raw, start=1):
        if not isinstance(entry, dict):
            logger.warning("Skipping invalid manifest scan entry at index %d: %r", index, entry)
            continue

        logger.debug("Processing manifest scan entry index=%d id=%r", index, entry.get("id"))
        entry_dir = _resolve_reference_path(manifest_path, entry.get("path"))
        if entry_dir is not None and not entry_dir.is_dir():
            logger.debug("Manifest scan path is not an existing directory: %s", entry_dir)

        data_path = _resolve_reference_path(
            manifest_path,
            entry.get("data_file"),
            extra_bases=(entry_dir,),
        )
        metadata_path = _resolve_reference_path(
            manifest_path,
            entry.get("metadata_file"),
            extra_bases=(entry_dir,),
        )

        if data_path is None or not data_path.exists():
            message = "Manifest scan %r refers to missing data file: %s" % (
                entry.get("id", index),
                data_path,
            )
            if skip_missing_data:
                logger.warning("%s; skipping", message)
                continue
            raise FileNotFoundError(message)

        try:
            df = _load_scan_dataframe(data_path, data_path.parent)
        except EmptyScanDataError as exc:
            skipped_empty += 1
            logger.warning(
                "Skipping manifest scan %r because its data file is empty: %s",
                entry.get("id", index),
                data_path,
            )
            logger.debug("Empty scan details: %s", exc)
            continue

        metadata = _load_metadata(metadata_path, include_metadata)
        logger.debug(
            "Loaded manifest scan index=%d id=%r data=%s metadata=%s rows=%d columns=%d",
            index, entry.get("id"), data_path, metadata_path, len(df), len(df.columns),
        )
        export_scans.append(
            ExportScan(
                scan_id=entry.get("id") or "scan_%d" % index,
                scan_type=entry.get("scan_type"),
                data_file=data_path,
                metadata_file=metadata_path,
                data=df,
                metadata=metadata,
                created_at=_parse_datetime(entry.get("created_at")),
                manifest_entry=dict(entry),
            )
        )

    logger.debug(
        "Created manifest export bundle scans=%d skipped_empty=%d manifest=%s",
        len(export_scans),
        skipped_empty,
        manifest_path,
    )
    return ExportBundle(
        scans=export_scans,
        manifest_file=manifest_path,
        manifest_header=dict(manifest_data.get("manifest") or {}),
    )


def load_export_bundle_from_latest_manifest(
    *,
    data_dir: Optional[PathLike] = None,
    manifest_index: int = 0,
    include_metadata: bool = True,
    skip_missing_data: bool = False,
) -> ExportBundle:
    """ Select a manifest using ``ManifestResolver`` and load it. """
    
    logger.debug("Selecting latest manifest data_dir=%s manifest_index=%d", data_dir, manifest_index)
    resolver = ManifestResolver(str(data_dir) if data_dir is not None else None)
    manifest_path = resolver.select_manifest(manifest_index)
    logger.debug("Selected manifest: %s", manifest_path)
    return load_export_bundle_from_manifest(
        manifest_path,
        include_metadata=include_metadata,
        skip_missing_data=skip_missing_data,
    )
