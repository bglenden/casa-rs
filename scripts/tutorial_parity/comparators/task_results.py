"""Normalization helpers for CASA and casa-rs task result captures."""

from __future__ import annotations

import json
import re
from typing import Any


def native_payload(value: Any) -> dict[str, Any]:
    current = value
    for _ in range(4):
        if isinstance(current, dict) and "stdout" in current:
            text = current["stdout"]
            if isinstance(text, str):
                start, end = text.find("{"), text.rfind("}")
                if start >= 0 and end >= start:
                    return json.loads(text[start : end + 1])
        if isinstance(current, dict) and "result" in current:
            current = current["result"]
            continue
        break
    if isinstance(current, dict):
        return current
    raise ValueError("native task capture does not contain JSON payload")


def numeric_vector(value: Any) -> list[float]:
    if isinstance(value, list):
        return [float(item) for item in value]
    return [
        float(item)
        for item in re.findall(r"[-+]?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][-+]?\d+)?", str(value))
    ]


def string_vector(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    text = str(value)
    quoted = re.findall(r"'([^']*)'", text)
    return quoted or [item for item in re.split(r"\s+", text.strip("[] ")) if item]
