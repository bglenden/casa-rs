#!/usr/bin/env python3
"""Compare a casa-rs VLASS prefix with an already-frozen CASA prefix."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

IMAGER_TOOLS = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(IMAGER_TOOLS))

from perf_harness.image_compare import compare_products  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workload", type=pathlib.Path)
    parser.add_argument("rust_prefix", type=pathlib.Path)
    parser.add_argument("casa_prefix", type=pathlib.Path)
    parser.add_argument("artifact_prefix", type=pathlib.Path)
    parser.add_argument("--casa-python", required=True)
    parser.add_argument(
        "--products",
        help=(
            "comma-separated diagnostic subset of the workload products; "
            "the default retains the exact full-product contract"
        ),
    )
    args = parser.parse_args()

    workload = json.loads(args.workload.read_text(encoding="utf-8"))
    comparison = workload["comparison"]
    products = comparison["products"]
    diagnostic_subset = False
    if args.products:
        requested_products = [
            suffix.strip() for suffix in args.products.split(",") if suffix.strip()
        ]
        unknown_products = sorted(set(requested_products) - set(products))
        if unknown_products:
            parser.error(
                "diagnostic product subset is not in the workload contract: "
                + ", ".join(unknown_products)
            )
        products = requested_products
        diagnostic_subset = True
    source_regions = []
    for region in comparison.get("source_regions", []):
        region_products = [
            suffix for suffix in region["products"] if suffix in products
        ]
        if region_products:
            source_regions.append({**region, "products": region_products})
    artifact_prefix = args.artifact_prefix.resolve()
    request = {
        "left_prefix": str(args.rust_prefix.resolve()),
        "right_prefix": str(args.casa_prefix.resolve()),
        "left_label": "casa-rs",
        "right_label": "CASA",
        "products": products,
        "max_elements_per_product": comparison["max_elements_per_product"],
        "mode": comparison["mode"],
        "full_chunk_elements": comparison["full_chunk_elements"],
        "require_exact_product_inventory": (
            False
            if diagnostic_subset
            else comparison["require_exact_product_inventory"]
        ),
        "require_metadata_parity": comparison["require_metadata_parity"],
        "source_regions": source_regions,
        "tolerances": comparison.get("tolerances"),
        "panel_dir": str(artifact_prefix.with_suffix(".panels")),
        "structure_workspace_dir": str(
            artifact_prefix.with_suffix(".structure-workspace")
        ),
    }
    result = compare_products(
        casa_python=args.casa_python,
        request=request,
        artifact_prefix=artifact_prefix,
        cwd=IMAGER_TOOLS.parents[2],
    )
    summary = {
        "status": result.get("status"),
        "reason": result.get("reason"),
        "tolerance_evaluation": result.get("tolerance_evaluation"),
        "products": {
            suffix: {
                "status": product.get("status"),
                "diff_rms_over_right_rms": product.get(
                    "full_array", {}
                ).get("diff_rms_over_right_rms"),
                "diff_abs_max_over_right_peak": product.get(
                    "full_array", {}
                ).get("diff_abs_max_over_right_peak"),
                "correlation": product.get("full_array", {}).get("correlation"),
            }
            for suffix, product in result.get("products", {}).items()
        },
        "input": result.get("input"),
        "output": result.get("output"),
        "log": result.get("log"),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    if result.get("status") != "completed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
