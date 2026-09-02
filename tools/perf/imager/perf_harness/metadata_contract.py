# SPDX-License-Identifier: LGPL-3.0-or-later
"""Manifest-bound metadata checks for CASA image comparisons."""

from __future__ import annotations

import math
from typing import Any


def normalize_metadata_contract(
    value: Any, *, products: list[str]
) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("metadata_contract must be an object")
    expected = {
        "require_shape_parity",
        "require_mask_parity",
        "coordinates",
        "products",
    }
    if set(value) != expected:
        raise ValueError(
            "metadata_contract fields must be exactly " + ", ".join(sorted(expected))
        )
    for name in ("require_shape_parity", "require_mask_parity"):
        if value.get(name) is not True:
            raise ValueError(f"metadata_contract.{name} must be true")

    coordinates = value.get("coordinates")
    coordinate_fields = {
        "excluded_fields",
        "relative_tolerance",
        "absolute_tolerance",
    }
    if not isinstance(coordinates, dict) or set(coordinates) != coordinate_fields:
        raise ValueError(
            "metadata_contract.coordinates fields must be exactly "
            + ", ".join(sorted(coordinate_fields))
        )
    excluded = coordinates.get("excluded_fields")
    if (
        not isinstance(excluded, list)
        or len(excluded) != len(set(excluded))
        or not all(isinstance(field, str) and field for field in excluded)
    ):
        raise ValueError(
            "metadata_contract.coordinates.excluded_fields must contain unique "
            "nonempty field names"
        )
    for name in ("relative_tolerance", "absolute_tolerance"):
        number = coordinates.get(name)
        if (
            isinstance(number, bool)
            or not isinstance(number, (int, float))
            or not math.isfinite(float(number))
            or float(number) < 0.0
        ):
            raise ValueError(
                f"metadata_contract.coordinates.{name} must be finite and >= 0"
            )

    declared_products = value.get("products")
    if not isinstance(declared_products, dict):
        raise ValueError("metadata_contract.products must be an object")
    unknown = sorted(set(declared_products) - set(products))
    if unknown:
        raise ValueError(
            "metadata_contract.products names unrequested product(s): "
            + ", ".join(unknown)
        )
    for suffix, policy in declared_products.items():
        _validate_product_policy(
            policy, source=f"metadata_contract.products[{suffix!r}]"
        )
    return value


def metadata_field_parity(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    suffix: str,
    contract: dict[str, Any],
) -> dict[str, bool]:
    coordinates = contract["coordinates"]
    excluded = set(coordinates["excluded_fields"])
    product_policy = contract["products"].get(suffix, {})
    return {
        "shape": left.get("shape") == right.get("shape"),
        "unit": _unit_matches(left.get("unit"), right.get("unit"), product_policy),
        "coordinates": coordinate_records_equivalent(
            _without_fields(left.get("coordinates"), excluded),
            _without_fields(right.get("coordinates"), excluded),
            relative_tolerance=float(coordinates["relative_tolerance"]),
            absolute_tolerance=float(coordinates["absolute_tolerance"]),
        ),
        "restoring_beam": _restoring_beam_matches(
            left.get("restoring_beam"),
            right.get("restoring_beam"),
            product_policy,
        ),
        "masks": left.get("masks") == right.get("masks"),
    }


def scientific_beam_products(contract: dict[str, Any] | None) -> list[str]:
    if not isinstance(contract, dict):
        return []
    return sorted(
        suffix
        for suffix, policy in contract.get("products", {}).items()
        if policy.get("restoring_beam", {}).get("comparison") == "scientific"
    )


def coordinate_records_equivalent(
    left: Any,
    right: Any,
    *,
    relative_tolerance: float,
    absolute_tolerance: float,
) -> bool:
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            coordinate_records_equivalent(
                left[key],
                right[key],
                relative_tolerance=relative_tolerance,
                absolute_tolerance=absolute_tolerance,
            )
            for key in left
        )
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return len(left) == len(right) and all(
            coordinate_records_equivalent(
                left_value,
                right_value,
                relative_tolerance=relative_tolerance,
                absolute_tolerance=absolute_tolerance,
            )
            for left_value, right_value in zip(left, right)
        )
    if isinstance(left, bool) or isinstance(right, bool):
        return left == right
    if isinstance(left, int) and isinstance(right, int):
        return left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return math.isclose(
            float(left),
            float(right),
            rel_tol=relative_tolerance,
            abs_tol=absolute_tolerance,
        )
    return left == right


def _validate_product_policy(value: Any, *, source: str) -> None:
    if not isinstance(value, dict) or not value:
        raise ValueError(f"{source} must be a nonempty object")
    unknown = sorted(set(value) - {"unit", "restoring_beam"})
    if unknown:
        raise ValueError(f"{source} has unknown field(s): {', '.join(unknown)}")
    if "unit" in value:
        unit = value["unit"]
        if not isinstance(unit, dict) or unit.get("comparison") not in {
            "parity",
            "expected",
        }:
            raise ValueError(f"{source}.unit comparison must be parity or expected")
        expected_fields = (
            {"comparison"}
            if unit["comparison"] == "parity"
            else {"comparison", "left", "right"}
        )
        if set(unit) != expected_fields:
            raise ValueError(f"{source}.unit fields do not match its comparison mode")
        for side in ("left", "right"):
            if side in unit and not isinstance(unit[side], str):
                raise ValueError(f"{source}.unit.{side} must be a string")
    if "restoring_beam" in value:
        beam = value["restoring_beam"]
        expected_fields = {"comparison", "left", "right"}
        if not isinstance(beam, dict) or set(beam) != expected_fields:
            raise ValueError(
                f"{source}.restoring_beam fields must be exactly comparison, left, right"
            )
        if beam.get("comparison") not in {"presence", "scientific"}:
            raise ValueError(
                f"{source}.restoring_beam comparison must be presence or scientific"
            )
        for side in ("left", "right"):
            if beam.get(side) not in {"present", "absent", "any"}:
                raise ValueError(
                    f"{source}.restoring_beam.{side} must be present, absent, or any"
                )
        if beam["comparison"] == "scientific" and (
            beam["left"] != "present" or beam["right"] != "present"
        ):
            raise ValueError(
                f"{source}.restoring_beam scientific comparison requires both beams"
            )


def _without_fields(value: Any, excluded: set[str]) -> Any:
    if not isinstance(value, dict):
        return value
    return {key: item for key, item in value.items() if key not in excluded}


def _unit_matches(left: Any, right: Any, product_policy: dict[str, Any]) -> bool:
    policy = product_policy.get("unit")
    if policy is None:
        return True
    if policy["comparison"] == "parity":
        return left == right
    return left == policy["left"] and right == policy["right"]


def _restoring_beam_matches(
    left: Any, right: Any, product_policy: dict[str, Any]
) -> bool:
    policy = product_policy.get("restoring_beam")
    if policy is None:
        return True
    left_present = isinstance(left, dict) and bool(left)
    right_present = isinstance(right, dict) and bool(right)
    return _presence_matches(left_present, policy["left"]) and _presence_matches(
        right_present, policy["right"]
    )


def _presence_matches(actual: bool, expected: str) -> bool:
    return expected == "any" or actual is (expected == "present")
