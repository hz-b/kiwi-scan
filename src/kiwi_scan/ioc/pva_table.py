# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin fuer Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from __future__ import annotations

import logging
import time
from typing import Dict, Optional

import pandas as pd
from p4p import Value
from p4p.nt import NTTable
from p4p.rpc import quickRPCServer, rpc

from kiwi_scan.data.loader import DataLoader
from kiwi_scan.data.manifestwriter import ManifestResolver

logger = logging.getLogger(__name__)



def pva_timestamp(timestamp: Optional[float] = None) -> Dict[str, int]:
    """ 
    Get PVA time_t from POSIX timestamp
    Used as alternative to NTTable.wrap() for constructing value
    """
    if timestamp is None:
        total_nanoseconds = time.time_ns()
    else:
        total_nanoseconds = round(timestamp * 1_000_000_000)

    seconds, nanoseconds = divmod(total_nanoseconds, 1_000_000_000)

    return {
        "secondsPastEpoch": seconds,
        "nanoseconds": nanoseconds,
        "userTag": 0,
    }


class ScanReader:
    """ 
    Read the Nth latest scan from the last manifest.
    """

    def __init__(self, data_dir: Optional[str] = None) -> None:
        logger.debug("Initializing ScanReader data_dir=%r", data_dir)
        self.resolver = ManifestResolver(data_dir)

    def read_table(self, scan_index: int = 0) -> pd.DataFrame:
        scan_index = int(scan_index)
        logger.debug("Selecting scan table manifest_index=0 scan_index=%d", scan_index)
        path = self.resolver.select_file(
            source_type="scan",
            manifest_index=0,
            scan_index=scan_index,
        )
        logger.info("Reading scan table scan_index=%d file=%s", scan_index, path)

        table = DataLoader(str(path), data_dir=str(path.parent)).load_data()
        if table is None:
            logger.error("Could not load scan data file: %s", path)
            raise FileNotFoundError("Could not load scan data file: %s" % path)

        table.attrs["source_file"] = path.name
        logger.debug(
            "Loaded scan table rows=%d columns=%d labels=%s",
            len(table.index),
            len(table.columns),
            [str(name) for name in table.columns],
        )
        return table


class DataFrameToNTTableConverter:
    """Convert numeric columns to double arrays and all other columns to strings."""

    @staticmethod
    def convert(table: pd.DataFrame) -> Value:
        labels = [str(name) for name in table.columns]
        descriptor = table.attrs.get("source_file", "")
        columns = []
        values = {}

        logger.debug(
            "Converting DataFrame to NTTable rows=%d columns=%d descriptor=%r",
            len(table.index),
            len(table.columns),
            descriptor,
        )

        for index, name in enumerate(table.columns):
            field = "c%d" % index
            series = table[name]
            numeric = pd.api.types.is_numeric_dtype(series)
            pva_type = "d" if numeric else "s"

            logger.debug( "Mapping column index=%d label=%r field=%s dtype=%s pva_type=%s nulls=%d",
                index, str(name), field, series.dtype, pva_type, int(series.isna().sum()))

            columns.append((field, pva_type))
            if numeric:
                values[field] = (
                    pd.to_numeric(series, errors="coerce")
                    .astype(float)
                    .to_numpy()
                )
            else:
                values[field] = [
                    "" if pd.isna(value) else str(value)
                    for value in series
                ]
        nt = NTTable(columns)
        result = Value(
            nt.type,
            {
                "labels": labels,
                "value": values,
                "descriptor": descriptor,
                "timeStamp": pva_timestamp(),
            },
        )
        logger.debug( "Created NTTable fields=%s labels=%s descriptor=%r",
            [field for field, _type in columns], labels, descriptor)
        return result


class PvaKiwiDataAdapter:
    def __init__(self, reader: ScanReader) -> None:
        self.reader = reader
        logger.debug("Initialized PvaKiwiDataAdapter reader=%s", type(reader).__name__)

    @rpc()
    def TABLE(self, scan_index=0):
        scan_index = int(scan_index)
        logger.info("PVA TABLE request scan_index=%d", scan_index)
        try:
            table = self.reader.read_table(scan_index)
            result = DataFrameToNTTableConverter.convert(table)
        except IndexError as exc:
            logger.warning( "PVA TABLE request rejected scan_index=%d: %s", scan_index, exc)
            table = pd.DataFrame({"error": [str(exc)]})
            table.attrs["source_file"] = "request error"
            return DataFrameToNTTableConverter.convert(table)
        except Exception:
            logger.exception("PVA TABLE request failed scan_index=%d", scan_index)
            raise

        logger.info(
            "PVA TABLE response scan_index=%d rows=%d columns=%d source=%r",
            scan_index,
            len(table.index),
            len(table.columns),
            table.attrs.get("source_file", ""),
        )
        return result


def serve(data_dir: Optional[str] = None, prefix: str = "KIWI:DATA") -> None:
    """Serve ``<prefix>:SCAN:TABLE`` as an NTURI-style PVA RPC endpoint."""
    rpc_prefix = prefix.rstrip(":") + ":SCAN:"
    endpoint = rpc_prefix + "TABLE"
    provider = "KiwiPVAServer"

    logger.info(
        "Starting PVA table server provider=%s endpoint=%s data_dir=%r",
        provider,
        endpoint,
        data_dir,
    )
    try:
        quickRPCServer(
            provider=provider,
            prefix=rpc_prefix,
            target=PvaKiwiDataAdapter(ScanReader(data_dir)),
        )
    except Exception:
        logger.exception("PVA table server failed endpoint=%s", endpoint)
        raise
    finally:
        logger.info("PVA table server stopped endpoint=%s", endpoint)
