# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

import logging
import os
import pandas as pd
from typing import Optional

def get_kiwi_data_dir_from_environ():
    """
    Returns the absolute path from KIWI_SCAN_DATA_DIR if defined and is valid, otherwise None.
    """
    dir_path = os.environ.get("KIWI_SCAN_DATA_DIR")
    if dir_path and os.path.isdir(dir_path):
        return os.path.abspath(dir_path)
    return None

def get_scan_data_dir(user_data_dir):
    """
    Returns the absolute path to the 'scan_data' directory.
    If KIWI_SCAN_DATA_DIR is defined (and valid), returns its 'scan_data' subdirectory;
    otherwise returns the original default path.
    """
    kiwi_data_dir = get_kiwi_data_dir_from_environ()
    if kiwi_data_dir is not None:
        # If the environment variable is set, override the default
        return os.path.normpath(os.path.join(kiwi_data_dir, user_data_dir))
    
    # Fallback to the original default if the environment variable is not set or invalid
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(current_file_dir, '..', '..', user_data_dir))

def resolve_data_dir(base_dir: Optional[str], relative_dir: str) -> str:
    """
    Resolves a full data directory path, respecting base_dir if valid,
    or falling back to get_scan_data_dir.
    """
    candidate=base_dir
    if base_dir:
        candidate = os.path.join(base_dir, relative_dir)
        if os.path.isdir(candidate):
            return candidate
    logging.info(f"Path {candidate} does not exist")
    fallback = get_scan_data_dir(relative_dir)
    if not os.path.isdir(fallback):
        logging.error(f"ERROR: Directory '{fallback}' does not exist.")
        return "."

    return fallback

class DataLoader:
    """Loads tabular data from a file into a pandas DataFrame."""
    def __init__(self, file_path, data_dir=None, delimiter=None):
        self.file_path = file_path
        self.data_dir = data_dir
        # Use whitespace as default delimiter
        self.delimiter = delimiter if delimiter is not None else r'\s+'
    
    def load_data(self):
        file_to_load = self.file_path
        if not os.path.exists(file_to_load) and self.data_dir:
            alt_path = os.path.join(self.data_dir, os.path.basename(self.file_path))
            if os.path.exists(alt_path):
                logging.debug(f"File not found at {self.file_path}. Using manifest directory: {alt_path}")
                file_to_load = alt_path
            else:
                logging.error(f"File not found at {self.file_path} or in manifest directory: {alt_path}")
                return None
        elif not os.path.exists(file_to_load):
            logging.error(f"File not found at {self.file_path} and no manifest directory provided.")
            return None

        try:
            # Identify timestamp columns before fully reading data
            timestamp_cols = [col for col in pd.read_csv(file_to_load, sep=self.delimiter, nrows=0).columns if "TS-ISO8601" in col]

            # Load data parsing timestamp columns directly, without using deprecated date_parser
            df = pd.read_csv(
                file_to_load,
                sep=self.delimiter,
                comment='#',  # Ignore lines starting with '#'
                parse_dates=timestamp_cols,  # Let pandas automatically parse dates
                date_format='ISO8601'  # kiwi-scan format
            )

            # Ensure UTC timezone explicitly after loading
            for col in timestamp_cols:
                df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')

            return df

        except Exception as e:
            logging.error(f"Error loading {file_to_load}: {e}")
            return None

