#!/usr/bin/env python3
"""Validate and enumerate the ImPerformance Wave 3 single-plane matrix."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any

from perf_harness import load_json_object
from perf_harness.artifacts import ArtifactError


MATRIX_PATH = pathlib.Path(__file__).resolve().parent / "wave3_single_plane_matrix.json"
REQUIRED_TIERS = {"smoke", "medium", "stress"}
REQUIRED_REVIEW_ROLES = {
    "before_baseline",
    "after_multi_worker_cpu",
    "after_gpu_metal",
    "casa_cpp",
}


class MatrixError(Exception):
    """Validation error shown without a traceback."""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--matrix",
        type=pathlib.Path,
        default=MATRIX_PATH,
        help="Wave 3 matrix JSON path",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="output format",
    )
    args = parser.parse_args()

    try:
        matrix = load_matrix(args.matrix)
        rows = enumerate_rows(matrix)
        if args.format == "json":
            json.dump({"status": "ok", "rows": rows}, sys.stdout, indent=2, sort_keys=True)
            sys.stdout.write("\n")
        else:
            for row in rows:
                products = ",".join(row["products"])
                print(
                    f"issue=#{row['issue']} mode={row['mode_id']} tier={row['tier']} "
                    f"dataset={row['dataset_key']} specmode={row['specmode']} "
                    f"gridder={row['gridder']} deconvolver={row['deconvolver']} "
                    f"products={products}"
                )
    except MatrixError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None


def load_matrix(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = load_json_object(path, description="Wave 3 matrix")
    except ArtifactError as error:
        raise MatrixError(str(error)) from error
    validate_matrix(value)
    return value


def validate_matrix(matrix: dict[str, Any]) -> None:
    if matrix.get("schema_version") != 1:
        raise MatrixError("schema_version must be 1")
    review_contract = object_field(matrix, "review_contract")
    roles = set(list_field(review_contract, "required_evidence_roles"))
    if not REQUIRED_REVIEW_ROLES.issubset(roles):
        missing = ", ".join(sorted(REQUIRED_REVIEW_ROLES - roles))
        raise MatrixError(f"review_contract.required_evidence_roles missing {missing}")

    modes = list_field(matrix, "modes")
    if not modes:
        raise MatrixError("modes must not be empty")
    seen_issues: set[int] = set()
    for mode in modes:
        issue = int_field(mode, "issue")
        if issue in seen_issues:
            raise MatrixError(f"duplicate mode issue #{issue}")
        seen_issues.add(issue)
        imaging = object_field(mode, "imaging")
        for key in ("specmode", "gridder", "deconvolver"):
            string_field(imaging, key)
        variants = optional_string_list_field(imaging, "deconvolver_variants")
        if variants and string_field(imaging, "deconvolver") not in variants:
            raise MatrixError(
                f"issue #{issue} deconvolver_variants must include primary deconvolver"
            )
        tiers = {string_field(row, "tier") for row in list_field(mode, "rows")}
        if not REQUIRED_TIERS.issubset(tiers):
            missing = ", ".join(sorted(REQUIRED_TIERS - tiers))
            raise MatrixError(f"issue #{issue} missing tiers: {missing}")
        for row in list_field(mode, "rows"):
            string_field(row, "dataset_key")
            string_field(row, "dataset_source")
            string_field(row, "relative_path")
            products = list_field(row, "products")
            if not products or not all(isinstance(product, str) and product.startswith(".") for product in products):
                raise MatrixError(f"issue #{issue} row {row.get('tier')!r} has invalid products")


def enumerate_rows(matrix: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for mode in list_field(matrix, "modes"):
        imaging = object_field(mode, "imaging")
        for row in list_field(mode, "rows"):
            rows.append(
                {
                    "issue": int_field(mode, "issue"),
                    "mode_id": string_field(mode, "mode_id"),
                    "title": string_field(mode, "title"),
                    "tier": string_field(row, "tier"),
                    "dataset_key": string_field(row, "dataset_key"),
                    "dataset_source": string_field(row, "dataset_source"),
                    "relative_path": string_field(row, "relative_path"),
                    "specmode": string_field(imaging, "specmode"),
                    "gridder": string_field(imaging, "gridder"),
                    "deconvolver": string_field(imaging, "deconvolver"),
                    "deconvolver_variants": optional_string_list_field(
                        imaging, "deconvolver_variants"
                    )
                    or [string_field(imaging, "deconvolver")],
                    "channel_count": int(imaging.get("channel_count", 1)),
                    "products": list(row["products"]),
                }
            )
    return rows


def object_field(obj: dict[str, Any], key: str) -> dict[str, Any]:
    value = obj.get(key)
    if not isinstance(value, dict):
        raise MatrixError(f"{key!r} must be an object")
    return value


def list_field(obj: dict[str, Any], key: str) -> list[Any]:
    value = obj.get(key)
    if not isinstance(value, list):
        raise MatrixError(f"{key!r} must be a list")
    return value


def optional_string_list_field(obj: dict[str, Any], key: str) -> list[str] | None:
    value = obj.get(key)
    if value is None:
        return None
    if not isinstance(value, list) or not value:
        raise MatrixError(f"{key!r} must be a non-empty list when present")
    if not all(isinstance(item, str) and item for item in value):
        raise MatrixError(f"{key!r} must contain non-empty strings")
    return list(value)


def string_field(obj: dict[str, Any], key: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value:
        raise MatrixError(f"{key!r} must be a non-empty string")
    return value


def int_field(obj: dict[str, Any], key: str) -> int:
    value = obj.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise MatrixError(f"{key!r} must be an integer")
    return value


if __name__ == "__main__":
    main()
