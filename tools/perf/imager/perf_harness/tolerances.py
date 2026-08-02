# SPDX-License-Identifier: LGPL-3.0-or-later
"""Frozen numerical acceptance contracts for imaging product comparisons."""

from __future__ import annotations

import math
from typing import Any


CONTRACT_VERSION = 1
NUMERICAL_CEILINGS = {
    "beam_major_relative",
    "beam_minor_relative",
    "beam_pa_degrees",
    "centroid_pixels",
    "diff_abs_max_over_right_peak",
    "diff_rms_over_right_rms",
    "integrated_flux_relative",
    "peak_relative",
}
BOOLEAN_REQUIREMENTS = {"require_topology_parity"}
TOLERANCE_FIELDS = (
    NUMERICAL_CEILINGS | BOOLEAN_REQUIREMENTS | {"allowed_structure_labels"}
)


class ToleranceContractError(ValueError):
    """A numerical tolerance contract is malformed or cannot be evaluated."""


def validate_tolerance_contract(value: Any, *, source: str = "tolerances") -> None:
    if not isinstance(value, dict):
        raise ToleranceContractError(f"{source} must be an object")
    expected = {"contract_version", "require_full_array", "default", "products"}
    if set(value) != expected:
        raise ToleranceContractError(
            f"{source} fields must be exactly {', '.join(sorted(expected))}"
        )
    if value.get("contract_version") != CONTRACT_VERSION:
        raise ToleranceContractError(
            f"{source}.contract_version must be {CONTRACT_VERSION}"
        )
    if not isinstance(value.get("require_full_array"), bool):
        raise ToleranceContractError(f"{source}.require_full_array must be a boolean")
    _validate_thresholds(value.get("default"), source=f"{source}.default")
    products = value.get("products")
    if not isinstance(products, dict):
        raise ToleranceContractError(f"{source}.products must be an object")
    for suffix, thresholds in products.items():
        if not isinstance(suffix, str) or not suffix.startswith("."):
            raise ToleranceContractError(
                f"{source}.products keys must be product suffixes"
            )
        _validate_thresholds(thresholds, source=f"{source}.products[{suffix!r}]")


def evaluate_comparison_tolerances(
    comparison: dict[str, Any], contract: dict[str, Any]
) -> dict[str, Any]:
    """Evaluate hard ceilings without silently accepting missing measurements."""

    validate_tolerance_contract(contract)
    checks: list[dict[str, Any]] = []
    if contract["require_full_array"] and comparison.get("comparison_mode") != "full":
        checks.append(
            _check(
                "comparison_mode",
                actual=comparison.get("comparison_mode"),
                ceiling="full",
                status="incomplete",
                reason="frozen tolerance contract requires full-array comparison",
            )
        )
    products = comparison.get("products")
    if not isinstance(products, dict) or not products:
        checks.append(
            _check(
                "products",
                actual=None,
                ceiling="nonempty",
                status="incomplete",
                reason="comparison produced no product measurements",
            )
        )
        return _result(checks)

    declared_source_regions = comparison.get("source_regions")
    if not isinstance(declared_source_regions, list):
        declared_source_regions = []
    declared_products = set(contract["products"])
    unexpected_contracts = sorted(declared_products - set(products))
    for suffix in unexpected_contracts:
        checks.append(
            _check(
                f"{suffix}.presence",
                actual=False,
                ceiling=True,
                status="incomplete",
                reason="tolerance contract names a missing comparison product",
            )
        )
    for suffix, product in sorted(products.items()):
        if not isinstance(product, dict) or product.get("status") != "compared":
            checks.append(
                _check(
                    f"{suffix}.status",
                    actual=product.get("status") if isinstance(product, dict) else None,
                    ceiling="compared",
                    status="incomplete",
                    reason="product comparison is incomplete",
                )
            )
            continue
        thresholds = {**contract["default"], **contract["products"].get(suffix, {})}
        if not thresholds:
            checks.append(
                _check(
                    f"{suffix}.tolerance",
                    actual=None,
                    ceiling="declared",
                    status="incomplete",
                    reason="product has no frozen numerical tolerance",
                )
            )
            continue
        expected_regions = [
            region
            for region in declared_source_regions
            if isinstance(region, dict) and suffix in region.get("products", [])
        ]
        checks.extend(
            _evaluate_product(
                suffix,
                product,
                thresholds,
                expected_regions=expected_regions,
            )
        )
    return _result(checks)


def _validate_thresholds(value: Any, *, source: str) -> None:
    if not isinstance(value, dict):
        raise ToleranceContractError(f"{source} must be an object")
    unknown = sorted(set(value) - TOLERANCE_FIELDS)
    if unknown:
        raise ToleranceContractError(
            f"{source} has unknown field(s): {', '.join(unknown)}"
        )
    for name in NUMERICAL_CEILINGS & set(value):
        number = value[name]
        if (
            isinstance(number, bool)
            or not isinstance(number, (int, float))
            or not math.isfinite(float(number))
            or float(number) < 0.0
        ):
            raise ToleranceContractError(f"{source}.{name} must be finite and >= 0")
    for name in BOOLEAN_REQUIREMENTS & set(value):
        if not isinstance(value[name], bool):
            raise ToleranceContractError(f"{source}.{name} must be a boolean")
        if value[name] is not True:
            raise ToleranceContractError(
                f"{source}.{name} must be true when it is declared"
            )
    if "allowed_structure_labels" in value:
        labels = value["allowed_structure_labels"]
        allowed = {"good", "not_applicable_exact_zero"}
        if (
            not isinstance(labels, list)
            or not labels
            or not all(isinstance(label, str) and label in allowed for label in labels)
        ):
            raise ToleranceContractError(
                f"{source}.allowed_structure_labels must be a nonempty known-label list"
            )


def _evaluate_product(
    suffix: str,
    product: dict[str, Any],
    thresholds: dict[str, Any],
    *,
    expected_regions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    direct_metrics = {
        "diff_rms_over_right_rms": product.get("diff_rms_over_right_rms"),
        "diff_abs_max_over_right_peak": product.get("diff_abs_max_over_right_peak"),
    }
    for name, actual in direct_metrics.items():
        if name in thresholds:
            checks.append(_ceiling_check(f"{suffix}.{name}", actual, thresholds[name]))
    if thresholds.get("require_topology_parity"):
        topology = product.get("topology_parity")
        checks.append(
            _check(
                f"{suffix}.topology_parity",
                actual=topology,
                ceiling=True,
                status="passed"
                if topology is True
                else "failed"
                if topology is False
                else "incomplete",
                reason=None
                if topology is True
                else "finite/mask topology is not proven identical",
            )
        )
    checks.extend(_beam_checks(suffix, product, thresholds))
    checks.extend(
        _source_region_checks(
            suffix,
            product,
            thresholds,
            expected_regions=expected_regions,
        )
    )
    if "allowed_structure_labels" in thresholds:
        structure = product.get("structured_difference")
        label = _nested(structure, "review", "label")
        classification_label = _nested(structure, "classification", "overall")
        structure_status = (
            structure.get("status") if isinstance(structure, dict) else None
        )
        allowed = thresholds["allowed_structure_labels"]
        expected_status = (
            "not_applicable_exact_zero"
            if label == "not_applicable_exact_zero"
            else "computed"
        )
        complete = all(
            isinstance(value, str)
            for value in (label, classification_label, structure_status)
        )
        accepted = bool(
            complete
            and label in allowed
            and classification_label == label
            and structure_status == expected_status
        )
        checks.append(
            _check(
                f"{suffix}.structured_difference",
                actual={
                    "status": structure_status,
                    "classification": classification_label,
                    "review": label,
                },
                ceiling=allowed,
                status=(
                    "passed" if accepted else "failed" if complete else "incomplete"
                ),
                reason=(
                    None
                    if accepted
                    else "structured-difference status, classification, and review "
                    "must agree on an accepted result"
                ),
            )
        )
    return checks


def _beam_checks(
    suffix: str, product: dict[str, Any], thresholds: dict[str, Any]
) -> list[dict[str, Any]]:
    checks = []
    metadata = product.get("metadata")
    for axis, threshold_name in (
        ("major", "beam_major_relative"),
        ("minor", "beam_minor_relative"),
    ):
        if threshold_name not in thresholds:
            continue
        checks.append(
            _ceiling_check(
                f"{suffix}.{threshold_name}",
                _relative_scalar(
                    _beam_quantity(metadata, "left", axis, output_unit="arcsec"),
                    _beam_quantity(metadata, "right", axis, output_unit="arcsec"),
                ),
                thresholds[threshold_name],
            )
        )
    if "beam_pa_degrees" in thresholds:
        left = _beam_quantity(metadata, "left", "positionangle", output_unit="deg")
        right = _beam_quantity(metadata, "right", "positionangle", output_unit="deg")
        difference = None
        if _finite(left) and _finite(right):
            raw = abs(float(left) - float(right)) % 180.0
            difference = min(raw, 180.0 - raw)
        checks.append(
            _ceiling_check(
                f"{suffix}.beam_pa_degrees",
                difference,
                thresholds["beam_pa_degrees"],
            )
        )
    return checks


def _source_region_checks(
    suffix: str,
    product: dict[str, Any],
    thresholds: dict[str, Any],
    *,
    expected_regions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    names = {
        "centroid_pixels",
        "integrated_flux_relative",
        "peak_relative",
    } & set(thresholds)
    if not names:
        return []
    expected_ids = [region.get("id") for region in expected_regions]
    if not expected_ids or not all(isinstance(value, str) for value in expected_ids):
        return [
            _check(
                f"{suffix}.source_contract",
                actual=expected_ids,
                ceiling="one or more frozen source-region ids",
                status="incomplete",
                reason="source-local tolerances lack a frozen source-region contract",
            )
        ]
    regions = product.get("source_regions")
    if not isinstance(regions, list) or not regions:
        return [
            _check(
                f"{suffix}.{name}",
                actual=None,
                ceiling=thresholds[name],
                status="incomplete",
                reason="source-region measurement is missing",
            )
            for name in sorted(names)
        ]
    observed_ids = [
        region.get("id") if isinstance(region, dict) else None for region in regions
    ]
    region_contract_checks = []
    if observed_ids != expected_ids:
        missing = [value for value in expected_ids if value not in observed_ids]
        extra = [value for value in observed_ids if value not in expected_ids]
        region_contract_checks.append(
            _check(
                f"{suffix}.source_region_inventory",
                actual=observed_ids,
                ceiling=expected_ids,
                status="failed" if extra else "incomplete",
                reason=(
                    f"source-region inventory differs; missing={missing}, extra={extra}"
                ),
            )
        )
    checks = []
    checks.extend(region_contract_checks)
    by_id = {
        region.get("id"): region
        for region in regions
        if isinstance(region, dict) and isinstance(region.get("id"), str)
    }
    expected_by_id = {region["id"]: region for region in expected_regions}
    for region_id in expected_ids:
        region = by_id.get(region_id)
        if region is None:
            continue
        expected_region = expected_by_id[region_id]
        observed_contract = {
            key: region.get(key) for key in ("id", "products", "blc", "trc")
        }
        expected_contract = {
            key: expected_region.get(key) for key in ("id", "products", "blc", "trc")
        }
        checks.append(
            _check(
                f"{suffix}.source[{region_id}].contract",
                actual=observed_contract,
                ceiling=expected_contract,
                status="passed" if observed_contract == expected_contract else "failed",
                reason=(
                    None
                    if observed_contract == expected_contract
                    else "source-region measurement is not bound to the frozen box"
                ),
            )
        )
        for side in ("left", "right"):
            measurement_status = _nested(region, side, "status")
            checks.append(
                _check(
                    f"{suffix}.source[{region_id}].{side}.status",
                    actual=measurement_status,
                    ceiling="measured",
                    status=(
                        "passed"
                        if measurement_status == "measured"
                        else "incomplete"
                        if measurement_status is None
                        else "failed"
                    ),
                    reason=(
                        None
                        if measurement_status == "measured"
                        else "source-region measurement did not complete"
                    ),
                )
            )
        region_id = (
            region.get("id", "unnamed") if isinstance(region, dict) else "invalid"
        )
        if "centroid_pixels" in names:
            left = _nested(region, "left", "centroid_pixels")
            right = _nested(region, "right", "centroid_pixels")
            distance = None
            if (
                isinstance(left, list)
                and isinstance(right, list)
                and len(left) == len(right)
                and left
                and all(_finite(value) for value in [*left, *right])
            ):
                distance = math.sqrt(
                    sum((float(a) - float(b)) ** 2 for a, b in zip(left, right))
                )
            checks.append(
                _ceiling_check(
                    f"{suffix}.source[{region_id}].centroid_pixels",
                    distance,
                    thresholds["centroid_pixels"],
                )
            )
        if "integrated_flux_relative" in names:
            checks.append(
                _ceiling_check(
                    f"{suffix}.source[{region_id}].integrated_flux_relative",
                    _relative_scalar(
                        _nested(region, "left", "integrated_flux"),
                        _nested(region, "right", "integrated_flux"),
                    ),
                    thresholds["integrated_flux_relative"],
                )
            )
        if "peak_relative" in names:
            checks.append(
                _ceiling_check(
                    f"{suffix}.source[{region_id}].peak_relative",
                    _relative_scalar(
                        _nested(region, "left", "peak_abs", "abs_value"),
                        _nested(region, "right", "peak_abs", "abs_value"),
                    ),
                    thresholds["peak_relative"],
                )
            )
    return checks


def _relative_scalar(left: Any, right: Any) -> float | None:
    if not _finite(left) or not _finite(right):
        return None
    if float(right) == 0.0:
        return 0.0 if float(left) == 0.0 else None
    return abs(float(left) - float(right)) / abs(float(right))


def _beam_quantity(
    metadata: Any, side: str, name: str, *, output_unit: str
) -> float | None:
    quantity = _nested(metadata, side, "restoring_beam", name)
    if not isinstance(quantity, dict) or not _finite(quantity.get("value")):
        return None
    unit = quantity.get("unit")
    to_degrees = {
        "rad": 180.0 / math.pi,
        "deg": 1.0,
        "arcmin": 1.0 / 60.0,
        "arcsec": 1.0 / 3600.0,
    }
    if unit not in to_degrees:
        return None
    degrees = float(quantity["value"]) * to_degrees[unit]
    return degrees if output_unit == "deg" else degrees * 3600.0


def _ceiling_check(name: str, actual: Any, ceiling: Any) -> dict[str, Any]:
    if not _finite(actual):
        return _check(
            name,
            actual=None,
            ceiling=ceiling,
            status="incomplete",
            reason="required numerical measurement is missing",
        )
    passed = float(actual) <= float(ceiling)
    return _check(
        name,
        actual=float(actual),
        ceiling=float(ceiling),
        status="passed" if passed else "failed",
        reason=None if passed else "hard numerical ceiling exceeded",
    )


def _check(
    name: str,
    *,
    actual: Any,
    ceiling: Any,
    status: str,
    reason: str | None,
) -> dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "actual": actual,
        "ceiling": ceiling,
        "reason": reason,
    }


def _result(checks: list[dict[str, Any]]) -> dict[str, Any]:
    if not checks:
        checks = [
            _check(
                "tolerance_contract",
                actual=0,
                ceiling="one or more enforceable checks",
                status="incomplete",
                reason="tolerance contract produced no enforceable checks",
            )
        ]
    failed = [check["name"] for check in checks if check["status"] == "failed"]
    incomplete = [check["name"] for check in checks if check["status"] == "incomplete"]
    status = "failed" if failed else "incomplete" if incomplete else "passed"
    return {
        "contract_version": CONTRACT_VERSION,
        "status": status,
        "checks": checks,
        "failed_checks": failed,
        "incomplete_checks": incomplete,
    }


def _nested(value: Any, *keys: str) -> Any:
    current = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _finite(value: Any) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    )
