# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

# src/scan/metadata_loader.py
import logging
from dataclasses import dataclass
from typing import Dict, Tuple, Optional, List, Any

import numpy as np
import pandas as pd


@dataclass
class MetadataFile:
    path: str
    constants: Dict[str, str]
    df_raw: pd.DataFrame           # flat (TS-ISO8601, PV, VALUE, ...)
    df_pivot: pd.DataFrame         # index=TS-ISO8601, columns=PV


def _parse_constants(lines: List[str]) -> Dict[str, str]:
    """
    Parse lines like:
        # beamline\tue521sgm1
        # user\tbalzer
    into a dict.
    """
    constants: Dict[str, str] = {}
    for line in lines:
        line = line.strip()
        if not line.startswith("#"):
            continue
        # strip leading '#'
        payload = line[1:].strip()
        if not payload or payload == "metadata_constants":
            continue
        # split once on whitespace or tab
        parts = payload.split("\t", 1)
        if len(parts) != 2:
            continue
        key = parts[0].strip()
        value = parts[1].strip()
        constants[key] = value
    return constants


def _split_header_and_body(path: str) -> Tuple[List[str], List[str]]:
    header_lines: List[str] = []
    body_lines: List[str] = []
    in_header = True

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if in_header:
                header_lines.append(line)
                # Detect the separator line
                if line.strip().startswith("# --- metadata above; monitor data below ---"):
                    in_header = False
            else:
                body_lines.append(line)

    return header_lines, body_lines


def _parse_value(v: Any) -> Any:
    """
    Parse VALUE column:
      * scalar numeric: float
      * waveform text like "[1.0 2.0 ...]": np.ndarray(float)
      * else: original string
    """
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)

    s = str(v).strip()
    if s == "":
        return None

    # waveform?
    if s.startswith("[") and s.endswith("]"):
        inner = s[1:-1].strip()
        if not inner:
            return np.array([], dtype=float)
        parts = inner.split()
        try:
            arr = np.array([float(p) for p in parts], dtype=float)
            return arr
        except ValueError:
            # fall back to raw string if not purely numeric
            return s

    # scalar numeric as string?
    try:
        return float(s)
    except ValueError:
        return s


def parse_metadata_file(path: str) -> Optional[MetadataFile]:
    """
    Fully parse a metadata_monitor file:
    - read constants
    - load table into df_raw
    - pivot df_raw into df_pivot (index=TS-ISO8601, columns=PV)
    """
    try:
        header_lines, body_lines = _split_header_and_body(path)
        constants = _parse_constants(header_lines)

        if not body_lines:
            logging.error("No monitor data in metadata file %s", path)
            return None

        # Let pandas read the TSV from the body lines
        from io import StringIO
        body_text = "".join(body_lines)
        df_raw = pd.read_csv(
            StringIO(body_text),
            sep=r"\s*\t\s*",        # tab-separated
            engine="python",
            dtype={"PV": str, "VALUE": str},
        )

        # Ensure timestamps are UTC-aware datetime
        for col in ("TS-ISO8601", "PV-TS-ISO8601"):
            if col in df_raw.columns:
                df_raw[col] = pd.to_datetime(df_raw[col], utc=True, errors="coerce")

        # Parse VALUE column
        if "VALUE" in df_raw.columns:
            df_raw["VALUE"] = df_raw["VALUE"].apply(_parse_value)

        # Pivot: index=TS-ISO8601, columns=PV, values=VALUE
        if {"TS-ISO8601", "PV", "VALUE"} <= set(df_raw.columns):
            df_pivot = df_raw.pivot(index="TS-ISO8601", columns="PV", values="VALUE")
        else:
            logging.warning("Metadata file %s does not have expected TS-ISO8601 / PV / VALUE columns", path)
            df_pivot = df_raw.copy()

        # Sort by time
        df_pivot = df_pivot.sort_index()

        return MetadataFile(path=path, constants=constants, df_raw=df_raw, df_pivot=df_pivot)

    except Exception as exc:
        logging.exception("Failed to parse metadata file %s: %s", path, exc)
        return None

