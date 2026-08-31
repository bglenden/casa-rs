#!/usr/bin/env python3
"""Compare T44 MT-MFS products with the frozen CASA oracle.

The Rust input may be the focused sealed-member JSON or the CASA-image prefix
published by the production application. JSON units are semantic
``ProductUnit`` values; persisted products use CASA brightness-unit strings.
CASA leaves several physically meaningful product units blank. The frozen CASA
prefix is read only. This comparator never invokes ``tclean`` or regenerates
the oracle.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np


RUST_SCHEMA = "casa-rs-t44-mtmfs-products-v1"
SUMMARY_SCHEMA = "casa-rs-t44-mtmfs-products-comparison-v1"
NRMS_CEILING = 1.0e-3
ZERO_ABSOLUTE_CEILING = 1.0e-7

PRODUCT_UNITS = {
    ".alpha": "dimensionless",
    ".alpha.error": "dimensionless",
    ".image.tt0": "jy_per_beam",
    ".image.tt1": "jy_per_beam",
    ".image.tt0.pbcor": "jy_per_beam",
    ".image.tt1.pbcor": "jy_per_beam",
    ".mask": "dimensionless",
    ".model.tt0": "jy_per_pixel",
    ".model.tt1": "jy_per_pixel",
    ".pb.tt0": "dimensionless",
    ".pb.tt1": "dimensionless",
    ".psf.tt0": "jy_per_beam",
    ".psf.tt1": "jy_per_beam",
    ".psf.tt2": "jy_per_beam",
    ".residual.tt0": "jy_per_beam",
    ".residual.tt1": "jy_per_beam",
    ".sumwt.tt0": "visibility_weight",
    ".sumwt.tt1": "visibility_weight",
    ".sumwt.tt2": "visibility_weight",
}
PERSISTED_RUST_UNITS = {
    name: (
        "Jy/pixel"
        if name.startswith(".model.")
        else "Jy/beam"
        if name.startswith((".psf.", ".residual.", ".image."))
        else ""
    )
    for name in PRODUCT_UNITS
}
EXPECTED_PRODUCTS = frozenset(PRODUCT_UNITS)
IMAGE_SHAPE = (128, 128, 1, 1)
STATE_SHAPE = (1, 1, 1, 1)
STATE_PRODUCTS = frozenset(name for name in EXPECTED_PRODUCTS if name.startswith(".sumwt."))
ALPHA_PRODUCTS = frozenset({".alpha", ".alpha.error"})
COMMON_BEAM_PRODUCTS = (
    ".image.tt0",
    ".image.tt1",
    ".image.tt0.pbcor",
    ".image.tt1.pbcor",
    ".alpha",
    ".alpha.error",
)


class ComparisonError(RuntimeError):
    """Evidence is missing or malformed and cannot be compared safely."""


@dataclass(frozen=True)
class Product:
    values: np.ndarray
    validity: np.ndarray
    shape: tuple[int, ...]
    unit: str
    beam: Mapping[str, float] | None


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ComparisonError(f"{label} must be an object")
    return value


def _shape(value: Any, label: str) -> tuple[int, ...]:
    if not isinstance(value, list) or len(value) != 4:
        raise ComparisonError(f"{label} must contain four dimensions")
    shape = tuple(int(extent) for extent in value)
    if any(extent <= 0 for extent in shape):
        raise ComparisonError(f"{label} extents must be positive")
    return shape


def _beam(value: Any, label: str) -> Mapping[str, float] | None:
    if value is None:
        return None
    record = _object(value, label)
    keys = ("major_rad", "minor_rad", "position_angle_rad")
    result = {key: float(record.get(key, math.nan)) for key in keys}
    if not all(math.isfinite(entry) for entry in result.values()):
        raise ComparisonError(f"{label} must contain finite radian values")
    if result["major_rad"] <= 0.0 or result["minor_rad"] <= 0.0:
        raise ComparisonError(f"{label} beam axes must be positive")
    return result


def _rust_products(document: Mapping[str, Any]) -> dict[str, Product]:
    if document.get("schema") != RUST_SCHEMA:
        raise ComparisonError(f"casa-rs artifact must use schema {RUST_SCHEMA!r}")
    members = document.get("members")
    if not isinstance(members, list):
        raise ComparisonError("casa-rs members must be an array")
    products: dict[str, Product] = {}
    for index, raw in enumerate(members):
        member = _object(raw, f"casa-rs member {index}")
        name = member.get("name")
        if not isinstance(name, str) or not name.startswith("."):
            raise ComparisonError(f"casa-rs member {index} has an invalid name")
        if name in products:
            raise ComparisonError(f"casa-rs member {name} is duplicated")
        shape = _shape(member.get("shape"), f"casa-rs member {name} shape")
        expected_size = math.prod(shape)
        values = np.asarray(member.get("payload"), dtype=np.float64)
        validity = np.asarray(member.get("validity"), dtype=bool)
        if values.size != expected_size or validity.size != expected_size:
            raise ComparisonError(
                f"casa-rs member {name} payload/validity does not match shape {shape}"
            )
        products[name] = Product(
            values=np.ascontiguousarray(values.reshape(shape)),
            validity=np.ascontiguousarray(validity.reshape(shape)),
            shape=shape,
            unit=str(member.get("unit")),
            beam=_beam(member.get("beam"), f"casa-rs member {name} beam"),
        )
    return products


def _casa_beam(tool: Any) -> Mapping[str, float] | None:
    try:
        beam = tool.restoringbeam()
    except Exception:
        return None
    if not isinstance(beam, dict) or not beam:
        return None
    major = beam["major"]
    minor = beam["minor"]
    angle = beam["positionangle"]
    if major["unit"] != "arcsec" or minor["unit"] != "arcsec" or angle["unit"] != "deg":
        raise ComparisonError("frozen CASA beam uses unexpected angular units")
    arcsec_to_rad = math.pi / (180.0 * 3600.0)
    return {
        "major_rad": float(major["value"]) * arcsec_to_rad,
        "minor_rad": float(minor["value"]) * arcsec_to_rad,
        "position_angle_rad": math.radians(float(angle["value"])),
    }


def load_image_products(prefix: Path, owner: str) -> dict[str, Product]:
    try:
        from casatools import image as image_tool
    except ImportError as error:
        raise ComparisonError(f"reading {owner} images requires casatools") from error

    products: dict[str, Product] = {}
    for name in sorted(EXPECTED_PRODUCTS):
        path = Path(f"{prefix}{name}")
        if not path.is_dir():
            raise ComparisonError(f"{owner} product is missing: {path}")
        tool = image_tool()
        try:
            if not tool.open(str(path)):
                raise ComparisonError(f"{owner} product could not be opened: {path}")
            shape = tuple(int(extent) for extent in tool.shape())
            values = np.asarray(tool.getchunk(), dtype=np.float64).reshape(shape)
            validity = np.asarray(tool.getchunk(getmask=True), dtype=bool).reshape(shape)
            unit = str(tool.brightnessunit())
            beam = _casa_beam(tool)
        finally:
            tool.close()
        products[name] = Product(
            values=np.ascontiguousarray(values),
            validity=np.ascontiguousarray(validity),
            shape=shape,
            unit=unit,
            beam=beam,
        )
    return products


def load_casa_products(prefix: Path) -> dict[str, Product]:
    return load_image_products(prefix, "frozen CASA")


def load_rust_products(prefix: Path) -> dict[str, Product]:
    return load_image_products(prefix, "persisted casa-rs")


def _metric(
    candidate: np.ndarray, reference: np.ndarray, support: np.ndarray
) -> dict[str, Any]:
    if candidate.shape != reference.shape or support.shape != reference.shape:
        raise ComparisonError("comparison arrays have incompatible shapes")
    if not np.any(support):
        raise ComparisonError("comparison support is empty")
    actual = candidate[support]
    expected = reference[support]
    if not np.all(np.isfinite(actual)) or not np.all(np.isfinite(expected)):
        return {"pass": False, "finite": False, "support_count": int(support.sum())}
    difference_rms = float(np.sqrt(np.mean(np.square(actual - expected))))
    reference_rms = float(np.sqrt(np.mean(np.square(expected))))
    reference_scale = float(np.max(np.abs(expected)))
    if reference_scale == 0.0:
        maximum_absolute = float(np.max(np.abs(actual)))
        return {
            "pass": maximum_absolute <= ZERO_ABSOLUTE_CEILING,
            "finite": True,
            "support_count": int(support.sum()),
            "maximum_absolute": maximum_absolute,
            "absolute_ceiling": ZERO_ABSOLUTE_CEILING,
        }
    normalized_rms = difference_rms / reference_rms
    return {
        "pass": normalized_rms <= NRMS_CEILING,
        "finite": True,
        "support_count": int(support.sum()),
        "difference_rms": difference_rms,
        "reference_rms": reference_rms,
        "normalized_rms": normalized_rms,
        "ceiling": NRMS_CEILING,
    }


def _beam_metric(candidate: Mapping[str, float], reference: Mapping[str, float]) -> dict[str, Any]:
    values = {
        key: abs(candidate[key] - reference[key]) / max(abs(reference[key]), 1.0e-15)
        for key in ("major_rad", "minor_rad", "position_angle_rad")
    }
    return {"pass": max(values.values()) <= NRMS_CEILING, "relative_errors": values}


def compare_documents(
    casa: Mapping[str, Product], rust_document: Mapping[str, Any]
) -> dict[str, Any]:
    return compare_products(casa, _rust_products(rust_document), PRODUCT_UNITS)


def compare_products(
    casa: Mapping[str, Product],
    rust: Mapping[str, Product],
    expected_rust_units: Mapping[str, str],
) -> dict[str, Any]:
    failures: list[str] = []

    def record(label: str, evidence: dict[str, Any]) -> dict[str, Any]:
        evidence["pass"] = bool(evidence["pass"])
        if not evidence["pass"]:
            failures.append(label)
        return evidence

    inventory = record(
        "inventory",
        {
            "pass": set(rust) == EXPECTED_PRODUCTS and set(casa) == EXPECTED_PRODUCTS,
            "expected": sorted(EXPECTED_PRODUCTS),
            "casa_rs": sorted(rust),
            "casa": sorted(casa),
            "forbidden": [".alpha.pbcor", ".weight.tt0", ".weight.tt1", ".weight.tt2"],
        },
    )
    product_metrics: dict[str, Any] = {}
    for name in sorted(EXPECTED_PRODUCTS & set(rust) & set(casa)):
        candidate = rust[name]
        reference = casa[name]
        expected_shape = STATE_SHAPE if name in STATE_PRODUCTS else IMAGE_SHAPE
        shape = record(
            f"{name}_shape",
            {"pass": candidate.shape == reference.shape == expected_shape,
             "expected": list(expected_shape), "casa_rs": list(candidate.shape),
             "casa": list(reference.shape)},
        )
        unit = record(
            f"{name}_unit",
            {"pass": candidate.unit == expected_rust_units[name],
             "expected_casa_rs_unit": expected_rust_units[name], "casa_rs": candidate.unit,
             "casa_brightness_unit": reference.unit},
        )
        masks_equal = candidate.validity.shape == reference.validity.shape and np.array_equal(
            candidate.validity, reference.validity
        )
        validity = record(
            f"{name}_validity",
            {"pass": masks_equal, "casa_true": int(reference.validity.sum()),
             "casa_rs_true": int(candidate.validity.sum())},
        )
        support = reference.validity & np.isfinite(reference.values)
        values = record(f"{name}_values", _metric(candidate.values, reference.values, support))
        product_metrics[name] = {
            "shape": shape,
            "unit": unit,
            "validity": validity,
            "values": values,
        }

    blanking: dict[str, Any] = {}
    for name in sorted(ALPHA_PRODUCTS & set(rust) & set(casa)):
        candidate = rust[name]
        reference = casa[name]
        outside = ~reference.validity
        blanking[name] = record(
            f"{name}_zero_blanking",
            {
                "pass": np.any(outside)
                and np.count_nonzero(candidate.values[outside]) == 0
                and not np.any(candidate.validity[outside]),
                "outside_count": int(outside.sum()),
                "outside_nonzero": int(np.count_nonzero(candidate.values[outside])),
            },
        )

    beams: dict[str, Any] = {}
    for name in (".psf.tt0", *COMMON_BEAM_PRODUCTS):
        if name not in rust or name not in casa:
            continue
        candidate = rust[name].beam
        reference = casa[name].beam
        beams[name] = record(
            f"{name}_beam",
            {"pass": candidate is not None and reference is not None}
            if candidate is None or reference is None
            else _beam_metric(candidate, reference),
        )
    common_candidates = [rust[name].beam for name in COMMON_BEAM_PRODUCTS if name in rust]
    common_beam = record(
        "common_beam",
        {
            "pass": len(common_candidates) == len(COMMON_BEAM_PRODUCTS)
            and all(beam is not None for beam in common_candidates)
            and all(beam == common_candidates[0] for beam in common_candidates[1:])
        },
    )

    algebra: dict[str, Any] = {}
    if all(name in rust for name in (".image.tt0", ".image.tt1", ".pb.tt0",
                                     ".image.tt0.pbcor", ".image.tt1.pbcor")):
        pb = rust[".pb.tt0"].values
        pb_support = rust[".pb.tt0"].validity & (pb > 0.0)
        algebra["pbcor_tt0"] = record(
            "pbcor_tt0_algebra",
            _metric(rust[".image.tt0.pbcor"].values,
                    rust[".image.tt0"].values / pb, pb_support),
        )
        algebra["pbcor_tt1"] = record(
            "pbcor_tt1_algebra",
            _metric(rust[".image.tt1.pbcor"].values,
                    rust[".image.tt1"].values / pb, pb_support),
        )
    if all(name in rust for name in (".alpha", ".image.tt0", ".image.tt1")):
        support = rust[".alpha"].validity & (rust[".image.tt0"].values != 0.0)
        algebra["alpha"] = record(
            "alpha_algebra",
            _metric(rust[".alpha"].values,
                    rust[".image.tt1"].values / rust[".image.tt0"].values, support),
        )

    return {
        "schema": SUMMARY_SCHEMA,
        "pass": not failures,
        "failures": failures,
        "contract": {
            "normalized_rms_ceiling": NRMS_CEILING,
            "zero_absolute_ceiling": ZERO_ABSOLUTE_CEILING,
            "member_count": len(EXPECTED_PRODUCTS),
            "component_order_normative": False,
        },
        "inventory": inventory,
        "products": product_metrics,
        "blanking": blanking,
        "beams": beams,
        "common_beam": common_beam,
        "algebra": algebra,
    }


def compare(
    casa_prefix: Path,
    rust_json: Path | None = None,
    rust_prefix: Path | None = None,
) -> dict[str, Any]:
    casa = load_casa_products(casa_prefix)
    if rust_prefix is not None:
        return compare_products(casa, load_rust_products(rust_prefix), PERSISTED_RUST_UNITS)
    if rust_json is None or not rust_json.is_file():
        raise ComparisonError(f"casa-rs product artifact is missing: {rust_json}")
    rust = _object(json.loads(rust_json.read_text(encoding="utf-8")), "casa-rs artifact")
    return compare_documents(casa, rust)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--casa-prefix", required=True, type=Path)
    rust = parser.add_mutually_exclusive_group(required=True)
    rust.add_argument("--rust-json", type=Path)
    rust.add_argument("--rust-prefix", type=Path)
    parser.add_argument("--summary-output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        summary = compare(args.casa_prefix, args.rust_json, args.rust_prefix)
    except (ComparisonError, OSError, ValueError, json.JSONDecodeError) as error:
        print(f"t44_mtmfs_products_compare: {error}", file=sys.stderr)
        return 2
    encoded = json.dumps(summary, indent=2, sort_keys=True, allow_nan=False)
    print(encoded)
    if args.summary_output is not None:
        args.summary_output.parent.mkdir(parents=True, exist_ok=True)
        args.summary_output.write_text(encoded + "\n", encoding="utf-8")
    return 0 if summary["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
