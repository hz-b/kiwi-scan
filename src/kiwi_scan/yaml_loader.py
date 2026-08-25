# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

# TODO: Replace with scheduler version to enable template support in this module
import logging
import os
import re
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)
_token_re = re.compile(r'\$\{([^}]+)\}')

def _expand_tokens(raw: str, repl: Dict[str, str]) -> str:
    return _token_re.sub(lambda m: repl.get(m.group(1), m.group(0)), raw)

def yaml_loader(path: str, replacements: Optional[Dict[str, str]] = None) -> Dict[str,Any]:
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    with open(path) as f:
        text = f.read()
    try:
        if replacements: 
            expanded = _expand_tokens(text, replacements)
            data = yaml.safe_load(expanded)
        else:
            data = yaml.safe_load(text)
        return data
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in {path}: {exc}") from exc

def parse_replacements(replacements_list: List[str]) -> Dict[str, str]:
    """
    Parse the replacement list provided as a command line argument.
    Args:
        replacements_list (list): A list of replacement strings in the format KEY=VALUE.
    Returns:
        dict: A dictionary of replacements.
    """
    replacements = {}
    if not replacements_list:
        return replacements
    
    for item in replacements_list:
        if '=' in item:
            key, value = item.split('=', 1)
            replacements[key] = value
    return replacements

def list_required_replacements(config_dir, filenames):
    """
    Lists all unique ${...} replacements used in the given YAML files.

    :param config_dir: Directory where YAML files are located.
    :param filenames: List of YAML file names.
    :return: Sorted list of required replacements (as strings).
    """
    placeholder_pattern = re.compile(r'\$\{([^}]+)\}')
    replacements = set()

    for filename in filenames:
        filepath = os.path.join(config_dir, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                content = file.read()
                matches = placeholder_pattern.findall(content)
                replacements.update(matches)
        except FileNotFoundError:
            logger.error("Warning: File not found: %s", filepath)
        except (OSError, UnicodeError, yaml.YAMLError) as exc:
            logger.error("Error reading %s: %s", filepath, exc)

    return sorted(replacements)

def get_replacements_help_and_required(config_dir, filenames):
    """
    Returns (help_text, required_flag) for argparse, based on replacements in given yaml files.

    :param config_dir: Path to config directory.
    :param filenames: List of yaml filenames.
    :return: (help_text, required_flag)
    """
    replacements = list_required_replacements(config_dir, filenames)

    if replacements:
        help_text = "\nRequired replacements:\n" + "\n".join(
            f"  ${{{r}}}" for r in replacements
        )
    else:
        help_text = "\n(No replacements required)"

    required_flag = bool(replacements)

    return help_text, required_flag

def get_env_replacements(prefix: str) -> dict:
    """
    Extract environment variable replacements of the form:
    <prefix>_REPLACE_<KEY>=<VALUE>

    Example:
        If prefix="HASMI_EMILDCM", and env contains:
            HASMI_EMILDCM_REPLACE_IOC_MONO=U171DCM1
        Then:
            returns {"IOC_MONO": "U171DCM1"}

    Args:
        prefix (str): The environment variable prefix (e.g. "HASMI_EMILDCM")

    Returns:
        dict: Mapping of KEY to VALUE
    """
    replacements = {}
    for key, val in os.environ.items():
        match_prefix = f"{prefix}_REPLACE_"
        if key.startswith(match_prefix):
            repl_key = key[len(match_prefix):]
            replacements[repl_key] = val
    return replacements

