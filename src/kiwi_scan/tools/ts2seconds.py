# SPDX-FileCopyrightText: 2026 Helmholtz-Zentrum Berlin für Materialien und Energie GmbH
# SPDX-License-Identifier: MIT

from datetime import datetime, timezone
import math
import numbers
from typing import Any, Optional

def timestamp_to_seconds(value: Any) -> Optional[float]:
    """ 
    Convert a numeric, datetime, or ISO-8601 timestamp to POSIX seconds. 
    """
    if value is None:
        return None

    if isinstance(value, numbers.Real):
        seconds = float(value)
        return seconds if math.isfinite(seconds) else None

    if isinstance(value, datetime):
        timestamp = value
    else:
        text = str(value).strip()
        if not text:
            return None

        # Python 3.8-3.10 do not accept trailing "Z".
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        try:
            timestamp = datetime.fromisoformat(text)
        except ValueError:
            return None

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    try:
        seconds = timestamp.timestamp()
    except (OSError, OverflowError, ValueError):
        return None

    return seconds if math.isfinite(seconds) else None
