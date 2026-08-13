#!/usr/bin/env python3
"""Reassess immutable full-array comparison evidence under a newer contract."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import pathlib
import sys
from typing import Any

IMAGER_TOOLS = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(IMAGER_TOOLS))

from perf_harness.artifacts import atomic_write_json  # noqa: E402
from perf_harness.image_compare import (  # noqa: E402
    _canonical_sha256,
    comparison_request_binding,
    validate_comparison_output,
)
from perf_harness.tolerances import (  # noqa: E402
    evaluate_comparison_tolerances,
    validate_tolerance_contract,
)


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_object(path: pathlib.Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def bounded_mask_threshold(
    contract: dict[str, Any],
    suffix: str,
) -> float | None:
    default = contract.get("default")
    products = contract.get("products")
    if not isinstance(default, dict) or not isinstance(products, dict):
        return None
    overrides = products.get(suffix, {})
    if not isinstance(overrides, dict):
        return None
    value = {**default, **overrides}.get("mask_mismatch_fraction")
    return float(value) if isinstance(value, (int, float)) else None


def promote_bounded_mask_products(
    comparison: dict[str, Any],
    contract: dict[str, Any],
) -> list[dict[str, Any]]:
    promotions = []
    products = comparison.get("products")
    if not isinstance(products, dict):
        return promotions
    for suffix, product in sorted(products.items()):
        if not isinstance(product, dict) or product.get("status") != "topology_mismatch":
            continue
        threshold = bounded_mask_threshold(contract, suffix)
        full = product.get("full_array")
        topology = full.get("topology") if isinstance(full, dict) else None
        total = full.get("total_elements") if isinstance(full, dict) else None
        mismatch = (
            topology.get("mask_mismatch_count")
            if isinstance(topology, dict)
            else None
        )
        if (
            threshold is None
            or not isinstance(total, int)
            or isinstance(total, bool)
            or total < 1
            or not isinstance(mismatch, int)
            or isinstance(mismatch, bool)
            or mismatch < 0
            or not isinstance(topology, dict)
            or topology.get("finite_equal") is not True
            or topology.get("nonfinite_kind_equal") is not True
        ):
            continue
        mismatch_fraction = mismatch / total
        if mismatch_fraction > threshold:
            continue
        product["status"] = "compared"
        promotions.append(
            {
                "suffix": suffix,
                "from_status": "topology_mismatch",
                "to_status": "compared",
                "basis": "bounded_mask_only_topology",
                "mask_mismatch_count": mismatch,
                "total_elements": total,
                "mask_mismatch_fraction": mismatch_fraction,
                "ceiling": threshold,
                "finite_equal": True,
                "nonfinite_kind_equal": True,
            }
        )
    return promotions


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-request", required=True, type=pathlib.Path)
    parser.add_argument("--source-output", required=True, type=pathlib.Path)
    parser.add_argument("--contract", required=True, type=pathlib.Path)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    args = parser.parse_args()

    source_request = load_object(args.source_request)
    source_output = load_object(args.source_output)
    contract = load_object(args.contract)
    validate_tolerance_contract(contract, source=str(args.contract))
    validate_comparison_output(source_output, source_request)

    reassessed = copy.deepcopy(source_output)
    promotions = promote_bounded_mask_products(reassessed, contract)
    product_failures = [
        suffix
        for suffix, product in reassessed["products"].items()
        if product.get("status") != "compared"
    ]
    inventory_failed = reassessed["product_inventory"]["status"] == "mismatch"
    failures = []
    if inventory_failed:
        failures.append("exact product inventory differs")
    if product_failures:
        failures.append("product comparison failed for " + ", ".join(product_failures))
    reassessed["status"] = "comparison_failed" if failures else "completed"
    reassessed["reason"] = "; ".join(failures) if failures else None

    rebound_request = copy.deepcopy(source_request)
    rebound_request["tolerances"] = contract
    rebound_request["request_binding"] = comparison_request_binding(rebound_request)
    rebound_request["request_sha256"] = _canonical_sha256(
        rebound_request["request_binding"]
    )
    reassessed["tolerances"] = contract
    reassessed["request_binding"] = rebound_request["request_binding"]
    reassessed["request_sha256"] = rebound_request["request_sha256"]
    validate_comparison_output(reassessed, rebound_request)

    evaluation = evaluate_comparison_tolerances(reassessed, contract)
    status = (
        "completed"
        if reassessed["status"] == "completed" and evaluation["status"] == "passed"
        else "failed"
    )
    receipt = {
        "schema_version": 1,
        "evidence_role": "contract_reassessment_of_immutable_full_array_comparison",
        "status": status,
        "source": {
            "request": str(args.source_request.resolve()),
            "request_sha256": sha256(args.source_request),
            "output": str(args.source_output.resolve()),
            "output_sha256": sha256(args.source_output),
            "validated_status": source_output["status"],
        },
        "contract": {
            "path": str(args.contract.resolve()),
            "sha256": sha256(args.contract),
            "contract_version": contract["contract_version"],
        },
        "rebound_request_sha256": rebound_request["request_sha256"],
        "comparison_status": reassessed["status"],
        "comparison_reason": reassessed["reason"],
        "product_count": len(reassessed["products"]),
        "promotions": promotions,
        "tolerance_evaluation": evaluation,
    }
    atomic_write_json(args.output, receipt)
    print(json.dumps(receipt, indent=2, sort_keys=True))
    if status != "completed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
