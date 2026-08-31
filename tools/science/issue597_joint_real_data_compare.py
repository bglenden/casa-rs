#!/usr/bin/env python3
"""Compare issue #597's Rust sequential anchor with its frozen CASA products.

CASA has no counterpart for ADR-0011's same-support joint solver.  This tool
therefore compares only the sequential continuum and uvcontsub-plus-line
observables.  Joint-only correctness is checked against simulation truth by
the Rust application test.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Mapping

import numpy as np


ORACLE_SCHEMA = "casa-rs-issue597-sequential-oracle-v1"
SUMMARY_SCHEMA = "casa-rs-issue597-sequential-comparison-v1"
SOURCE_SHA256 = "ae80d9199e2d313e951b650ed670881bebc8d686eff4b38c017d3df917fb2710"
NRMS_CEILING = 1.0e-3
EXPECTED_SELECTION = {
    "source_channels": "0:52~67",
    "continuum_anchors": "0:52~59",
    "line_support": "0:60~67",
}
EXPECTED_IMAGING = {
    "image_size": 32,
    "cell": "0.01arcsec",
    "weighting": "natural",
    "deconvolver": "hogbom",
    "threshold": "0.002Jy",
    "niter": 512,
    "cycleniter": 16,
    "gain": 0.1,
}
PRODUCTS = ("model", "residual", "image", "psf", "mask")
DEFAULT_RECEIPT = Path(__file__).with_name("issue597_joint_sequential_oracle.json")


class ComparisonError(RuntimeError):
    """The frozen evidence is missing or does not match the declared fixture."""


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ComparisonError(f"{label} must be an object")
    return value


def validate_oracle(receipt: Mapping[str, Any]) -> None:
    if receipt.get("schema") != ORACLE_SCHEMA:
        raise ComparisonError(f"oracle must use schema {ORACLE_SCHEMA!r}")
    if receipt.get("role") != "sequential_casa_reference_not_a_joint_solver":
        raise ComparisonError("oracle role must explicitly exclude a CASA joint solver")
    source = _object(receipt.get("source"), "oracle source")
    if source.get("tree_sha256_excluding_table_lock") != SOURCE_SHA256:
        raise ComparisonError("oracle source provenance changed")
    if dict(_object(receipt.get("selection"), "oracle selection")) != EXPECTED_SELECTION:
        raise ComparisonError("oracle channel selection changed")
    if dict(_object(receipt.get("imaging"), "oracle imaging")) != EXPECTED_IMAGING:
        raise ComparisonError("oracle imaging parameters changed")
    products = _object(receipt.get("products"), "oracle products")
    if products.get("numeric_archive") != "tools/science/issue597_joint_sequential_products.npz":
        raise ComparisonError("oracle product location changed")
    recipe = _object(receipt.get("recipe"), "oracle recipe")
    if recipe.get("path") != "tools/science/issue597_joint_sequential_oracle.py":
        raise ComparisonError("oracle recipe location changed")


def normalized_rms(candidate: np.ndarray, reference: np.ndarray) -> dict[str, Any]:
    candidate = np.asarray(candidate, dtype=np.float64)
    reference = np.asarray(reference, dtype=np.float64)
    if candidate.shape != reference.shape:
        raise ComparisonError(
            f"product shapes differ: {candidate.shape} != {reference.shape}"
        )
    finite = bool(np.all(np.isfinite(candidate)) and np.all(np.isfinite(reference)))
    if not finite:
        return {"pass": False, "finite": False, "ceiling": NRMS_CEILING}
    difference_rms = float(np.sqrt(np.mean(np.square(candidate - reference))))
    reference_rms = float(np.sqrt(np.mean(np.square(reference))))
    value = 0.0 if reference_rms == 0.0 and difference_rms == 0.0 else (
        None if reference_rms == 0.0 else difference_rms / reference_rms
    )
    return {
        "pass": value is not None and value <= NRMS_CEILING,
        "finite": True,
        "shape": list(reference.shape),
        "difference_rms": difference_rms,
        "reference_rms": reference_rms,
        "normalized_rms": value,
        "ceiling": NRMS_CEILING,
    }


def compare_arrays(
    casa: Mapping[str, Mapping[str, np.ndarray]],
    rust: Mapping[str, Mapping[str, np.ndarray]],
) -> dict[str, Any]:
    failures: list[str] = []
    metrics: dict[str, dict[str, Any]] = {}
    for workflow in ("continuum", "line"):
        workflow_metrics: dict[str, Any] = {}
        for product in PRODUCTS:
            label = f"{workflow}_{product}"
            try:
                metric = normalized_rms(rust[workflow][product], casa[workflow][product])
            except KeyError as error:
                raise ComparisonError(f"missing {label} product") from error
            if not metric["pass"]:
                failures.append(label)
            workflow_metrics[product] = metric
        metrics[workflow] = workflow_metrics
    return {
        "schema": SUMMARY_SCHEMA,
        "pass": not failures,
        "failures": failures,
        "contract": {
            "casa_role": "sequential_reference_only",
            "joint_truth_owner": "Rust source-backed application test",
            "normalized_rms_ceiling": NRMS_CEILING,
        },
        "products": metrics,
    }


def _load_image(path: Path) -> np.ndarray:
    try:
        from casatools import image as image_tool
    except ImportError as error:
        raise ComparisonError("reading CASA images requires Python with casatools") from error
    if not path.is_dir():
        raise ComparisonError(f"image product is missing: {path}")
    tool = image_tool()
    try:
        if not tool.open(str(path)):
            raise ComparisonError(f"image product could not be opened: {path}")
        values = np.asarray(tool.getchunk(), dtype=np.float64)
    finally:
        tool.close()
    return np.ascontiguousarray(np.squeeze(values))


def load_products(prefixes: Mapping[str, Path]) -> dict[str, dict[str, np.ndarray]]:
    return {
        workflow: {
            product: _load_image(Path(f"{prefix}.{product}"))
            for product in PRODUCTS
        }
        for workflow, prefix in prefixes.items()
    }


def load_frozen_products(path: Path) -> dict[str, dict[str, np.ndarray]]:
    if not path.is_file():
        raise ComparisonError(f"frozen numeric oracle is missing: {path}")
    with np.load(path, allow_pickle=False) as archive:
        expected = {f"{workflow}_{product}" for workflow in ("continuum", "line") for product in PRODUCTS}
        if set(archive.files) != expected:
            raise ComparisonError("frozen numeric oracle has the wrong product inventory")
        return {
            workflow: {
                product: np.ascontiguousarray(np.squeeze(archive[f"{workflow}_{product}"]))
                for product in PRODUCTS
            }
            for workflow in ("continuum", "line")
        }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(64 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compare(oracle_products: Path, receipt_path: Path, rust_prefix: Path) -> dict[str, Any]:
    if not receipt_path.is_file():
        raise ComparisonError(f"oracle receipt is missing: {receipt_path}")
    receipt = _object(json.loads(receipt_path.read_text()), "oracle receipt")
    validate_oracle(receipt)
    expected_archive_sha = _object(receipt["products"], "oracle products").get(
        "numeric_archive_sha256"
    )
    if not oracle_products.is_file() or _sha256(oracle_products) != expected_archive_sha:
        raise ComparisonError("frozen numeric oracle digest changed")
    recipe_sha = _object(receipt["recipe"], "oracle recipe").get("sha256")
    recipe_path = Path(__file__).with_name("issue597_joint_sequential_oracle.py")
    if _sha256(recipe_path) != recipe_sha:
        raise ComparisonError("oracle recipe digest changed")
    casa = load_frozen_products(oracle_products)
    rust = load_products(
        {
            "continuum": Path(f"{rust_prefix}-sequential-continuum"),
            "line": Path(f"{rust_prefix}-sequential-line"),
        }
    )
    return compare_arrays(casa, rust)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle-products", required=True, type=Path)
    parser.add_argument("--oracle-receipt", default=DEFAULT_RECEIPT, type=Path)
    parser.add_argument("--rust-prefix", required=True, type=Path)
    parser.add_argument("--summary-output", type=Path)
    args = parser.parse_args()
    try:
        summary = compare(args.oracle_products, args.oracle_receipt, args.rust_prefix)
    except (ComparisonError, OSError, ValueError, json.JSONDecodeError) as error:
        print(f"issue597_joint_real_data_compare: {error}", file=sys.stderr)
        return 2
    encoded = json.dumps(summary, indent=2, sort_keys=True, allow_nan=False)
    print(encoded)
    if args.summary_output is not None:
        args.summary_output.parent.mkdir(parents=True, exist_ok=True)
        args.summary_output.write_text(encoded + "\n")
    return 0 if summary["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
