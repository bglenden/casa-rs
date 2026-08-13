#!/usr/bin/env python3
"""Validate an existing common-model probe against its preservation contract."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from vlass_common_model_major_cycle import protected_product_preservation


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--probe-receipt", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite validation receipt: {args.output}")
    probe = json.loads(args.probe_receipt.read_text(encoding="utf-8"))
    if probe.get("kind") != "vlass_common_model_major_cycle":
        raise RuntimeError("input is not a common-model major-cycle receipt")
    zero_prefix = Path(probe["zero_prefix"])
    model_prefix = Path(probe["model_prefix"])
    output_prefix = Path(probe["output_prefix"])
    preservation = protected_product_preservation(
        zero_prefix=zero_prefix,
        model_prefix=model_prefix,
        output_prefix=output_prefix,
    )
    result = {
        "schema": "casa-rs-vlass-common-model-preservation-validation-v1",
        "role": "offline_validation_of_existing_probe_not_performance_evidence",
        "probe_receipt": str(args.probe_receipt.resolve()),
        "probe_receipt_sha256": sha256(args.probe_receipt),
        "zero_prefix": str(zero_prefix),
        "model_prefix": str(model_prefix),
        "output_prefix": str(output_prefix),
        "preservation": preservation,
        "classification": (
            "numerically-preserved" if preservation["passed"] else "not-preserved"
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, sort_keys=True), flush=True)
    if not preservation["passed"]:
        raise RuntimeError("existing common-model probe failed preservation validation")


if __name__ == "__main__":
    main()
