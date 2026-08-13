#!/usr/bin/env python3
"""Compare bounded CASA and casa-rs residual-value streams before AW gridding."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


CASA_SCHEMA = "casa-aw-datagrid-input-hash-v1"
CASARS_SCHEMA = "casa-rs-vlass-frozen-model-prediction-sidecar-host-v1"
VALUE_CONTRACT = (
    "fnv1a64-source-order-RR-then-LL-production-tile-ingress-residual-complex32-"
    "then-term-weight-times-residual-complex32"
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"{path} does not contain a JSON object")
    return value


def checkpoint_map(checkpoints: object, label: str) -> dict[int, int]:
    if not isinstance(checkpoints, list) or not checkpoints:
        raise RuntimeError(f"{label} checkpoints are absent")
    output: dict[int, int] = {}
    for checkpoint in checkpoints:
        if not isinstance(checkpoint, dict):
            raise RuntimeError(f"{label} checkpoint is not an object")
        sources = int(checkpoint["sources"])
        value = int(checkpoint["value"])
        if sources <= 0 or sources in output:
            raise RuntimeError(f"{label} checkpoint source {sources} is invalid")
        output[sources] = value
    return output


def compare(casa: dict[str, Any], casars: dict[str, Any]) -> dict[str, Any]:
    if casa.get("schema") != CASA_SCHEMA:
        raise RuntimeError("CASA receipt schema differs")
    if casa.get("status") != "completed-before-grid":
        raise RuntimeError("CASA receipt did not complete before gridding")
    if casars.get("schema") != CASARS_SCHEMA:
        raise RuntimeError("casa-rs sidecar schema differs")
    boundary = casars.get("casa_datatogrid_tt0_value_boundary")
    if not isinstance(boundary, dict):
        raise RuntimeError("casa-rs sidecar lacks the DataToGrid value boundary")
    if boundary.get("contract") != VALUE_CONTRACT:
        raise RuntimeError("casa-rs DataToGrid value contract differs")

    casa_sources = int(casa["source_count"])
    casars_sources = int(boundary["source_count"])
    casa_checkpoints = checkpoint_map(casa["checkpoints"], "CASA")
    casars_checkpoints = checkpoint_map(boundary["checkpoints"], "casa-rs")
    shared_sources = sorted(casa_checkpoints.keys() & casars_checkpoints.keys())
    first_mismatch = next(
        (
            source
            for source in shared_sources
            if casa_checkpoints[source] != casars_checkpoints[source]
        ),
        None,
    )
    exact_topology = (
        casa_sources == casars_sources
        and int(casa["role_count"]) == int(boundary["role_count"])
        and set(casa_checkpoints) == set(casars_checkpoints)
    )
    exact_value_hash = int(casa["hashes"]["value"]) == int(boundary["value_hash"])
    passed = exact_topology and exact_value_hash and first_mismatch is None
    return {
        "schema": "casa-vlass-aw-datatogrid-value-hash-comparison-v1",
        "role": "bounded-correctness-diagnostic-not-performance-evidence",
        "classification": (
            "residual-value-stream-exact"
            if passed
            else (
                "residual-value-first-mismatch"
                if first_mismatch is not None
                else "residual-value-topology-or-final-hash-mismatch"
            )
        ),
        "passed": passed,
        "topology": {
            "exact": exact_topology,
            "casa_sources": casa_sources,
            "casars_sources": casars_sources,
            "casa_roles": int(casa["role_count"]),
            "casars_roles": int(boundary["role_count"]),
            "shared_checkpoint_count": len(shared_sources),
            "casa_checkpoint_count": len(casa_checkpoints),
            "casars_checkpoint_count": len(casars_checkpoints),
        },
        "value": {
            "exact_final_hash": exact_value_hash,
            "casa_hash": int(casa["hashes"]["value"]),
            "casars_hash": int(boundary["value_hash"]),
            "first_mismatch_source": first_mismatch,
            "previous_matching_source": (
                None if first_mismatch in (None, 1) else first_mismatch - 1
            ),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--casa-receipt", required=True, type=Path)
    parser.add_argument("--casars-sidecar", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite output: {args.output}")
    receipt = compare(
        load_json(args.casa_receipt),
        load_json(args.casars_sidecar),
    )
    receipt["inputs"] = {
        "casa_receipt": str(args.casa_receipt.resolve()),
        "casa_receipt_sha256": sha256_file(args.casa_receipt),
        "casars_sidecar": str(args.casars_sidecar.resolve()),
        "casars_sidecar_sha256": sha256_file(args.casars_sidecar),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(receipt, sort_keys=True))


if __name__ == "__main__":
    main()
