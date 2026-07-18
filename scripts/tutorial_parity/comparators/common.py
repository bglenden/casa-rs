"""Common typed comparison helpers."""

from __future__ import annotations

import math
from typing import Any


def nested(value: Any, dotted: str) -> Any:
    current = value
    for part in dotted.split("."):
        if isinstance(current, dict):
            current = current[part]
        elif isinstance(current, list) and part.isdigit():
            current = current[int(part)]
        else:
            raise KeyError(dotted)
    return current


def compare_values(left: Any, right: Any, *, atol: float, rtol: float) -> dict[str, Any]:
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        finite = math.isfinite(float(left)) and math.isfinite(float(right))
        difference = abs(float(left) - float(right)) if finite else None
        passed = bool(finite and difference <= atol + rtol * abs(float(right)))
        return {"passed": passed, "native": left, "oracle": right, "absolute_difference": difference}
    return {"passed": left == right, "native": left, "oracle": right}
