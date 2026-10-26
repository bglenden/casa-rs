# SPDX-License-Identifier: LGPL-3.0-or-later
"""Comparisons for `skymodel_oracle.py --check` (#841 Phase 5b): an image as
CASA reads it against the one CASA made, without CASA, so that the tests in
`test_skymodel_compare.py` can run anywhere.

Pixels come as lists of floats in Fortran order, `None` for a non-finite
value (as the oracle records NaN); masks as lists of booleans, or `None`
for an image with no mask.
"""

from __future__ import annotations

import math
from typing import Callable, Optional, Sequence


def finite(value: Optional[float]) -> bool:
    return value is not None and math.isfinite(value)


def pixel_problems(
    where: str,
    actual: Sequence[Optional[float]],
    expected: Sequence[Optional[float]],
    actual_mask: Optional[Sequence[bool]] = None,
    expected_mask: Optional[Sequence[bool]] = None,
    tolerance: float = 1e-6,
) -> tuple[list[str], float]:
    """Where `actual` differs from `expected`, and the largest difference of
    the unmasked finite pixels as a fraction of the expected peak.

    The masks must agree. Of the pixels both leave unmasked, a finite value
    must meet a finite value within `tolerance` of the peak, and a
    non-finite value a non-finite one.
    """
    problems: list[str] = []
    if len(actual) != len(expected):
        return [f"{where}: {len(actual)} pixels, CASA {len(expected)}"], math.inf
    unmasked = [True] * len(expected)
    actual_mask = unmasked if actual_mask is None else actual_mask
    expected_mask = unmasked if expected_mask is None else expected_mask
    if len(actual_mask) != len(expected) or len(expected_mask) != len(expected):
        return [f"{where}: a mask of the wrong length"], math.inf
    differing_masks = sum(1 for a, e in zip(actual_mask, expected_mask) if a != e)
    if differing_masks:
        problems.append(f"{where}: the mask differs at {differing_masks} pixels")
    valid = [a and e for a, e in zip(actual_mask, expected_mask)]
    mismatched = sum(
        1 for a, e, v in zip(actual, expected, valid) if v and finite(a) != finite(e)
    )
    if mismatched:
        problems.append(f"{where}: {mismatched} pixels finite on one side only")
    finite_pairs = [(a, e) for a, e, v in zip(actual, expected, valid) if v and finite(a) and finite(e)]
    peak = max((abs(e) for _, e in finite_pairs), default=0.0) or 1.0
    worst = max((abs(a - e) for a, e in finite_pairs), default=0.0) / peak
    if worst > tolerance:
        problems.append(f"{where}: pixels differ by {worst:.3g} of the peak")
    return problems, worst


def record_differences(
    actual: object,
    expected: object,
    where: str,
    report: Callable[[str], None],
    skipped: tuple[str, ...] = (),
) -> None:
    """Report where two records differ: strings, booleans and lengths
    exactly, numbers to 1e-9 of their size; top-level keys starting with any
    of `skipped` are left out."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key in sorted(set(expected) | set(actual)):
            if skipped and key.startswith(skipped):
                continue
            if key not in actual or key not in expected:
                report(f"{where}.{key}: only in {'CASA' if key in expected else 'casa-rs'}")
            else:
                record_differences(actual[key], expected[key], f"{where}.{key}", report)
    elif isinstance(expected, list) and isinstance(actual, list):
        if len(actual) != len(expected):
            report(f"{where}: {len(actual)} values, CASA {len(expected)}")
        for index, (a, e) in enumerate(zip(actual, expected)):
            record_differences(a, e, f"{where}[{index}]", report)
    elif (
        isinstance(expected, (int, float))
        and isinstance(actual, (int, float))
        and not isinstance(expected, bool)
        and not isinstance(actual, bool)
    ):
        same = actual == expected or abs(actual - expected) <= 1e-9 * abs(expected)
        if not same:
            report(f"{where}: {actual!r}, CASA {expected!r}")
    elif actual != expected:
        report(f"{where}: {actual!r}, CASA {expected!r}")
