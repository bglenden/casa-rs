#!/usr/bin/env python3
"""Compare CASA/casa-rs CF selection and placement before AW degridding."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import struct
from pathlib import Path
from typing import Any


CONTRACT = (
    "source-role-cell-frequency-w-mueller-pa-placement-conjugation-"
    "support-normalization-complex32-little-endian"
)
RECORD = struct.Struct("<IIddidqqqqIIIff")
DISCRETE_FIELDS = {
    "source_ordinal": 0,
    "role_ordinal": 1,
    "mueller": 4,
    "loc_x": 6,
    "loc_y": 7,
    "off_x": 8,
    "off_y": 9,
    "conjugate_for_grid": 10,
    "x_support": 11,
    "y_support": 12,
}
FLOAT64_FIELDS = {
    "cell_frequency_hz": 2,
    "cell_w_lambda": 3,
    "cell_pa_deg": 5,
}


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"{path} does not contain a JSON object")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def stream(receipt: dict[str, Any], casa: bool) -> dict[str, Any]:
    value = (
        receipt.get("prediction_cf_metadata_stream")
        if casa
        else receipt.get("casa_datatogrid_tt0_value_boundary", {}).get(
            "prediction_cf_metadata_stream"
        )
    )
    if not isinstance(value, dict):
        raise RuntimeError("receipt lacks a CF metadata stream")
    if value.get("contract") != CONTRACT:
        raise RuntimeError("CF metadata stream contract differs")
    if int(value.get("record_size", 0)) != RECORD.size:
        raise RuntimeError("CF metadata stream record size differs")
    return value


def decode(payload: bytes) -> list[tuple[int | float, ...]]:
    if len(payload) % RECORD.size:
        raise RuntimeError("CF metadata stream has a partial record")
    return list(RECORD.iter_unpack(payload))


def divergent_sources(value_comparison: dict[str, Any] | None) -> set[int]:
    if value_comparison is None:
        return set()
    ranges = (
        value_comparison.get("raw_residual", {})
        .get("per_source_relative_l2", {})
        .get("over_contract_limit_ordinal_ranges", [])
    )
    result: set[int] = set()
    for start, end in ranges:
        result.update(range(int(start), int(end) + 1))
    return result


def field_comparison(
    reference: list[tuple[int | float, ...]],
    candidate: list[tuple[int | float, ...]],
    index: int,
    failing_sources: set[int],
    *,
    float_bits: bool,
) -> dict[str, Any]:
    mismatches: list[int] = []
    for record_index, (left, right) in enumerate(zip(reference, candidate)):
        equal = (
            struct.pack("<d", float(left[index]))
            == struct.pack("<d", float(right[index]))
            if float_bits
            else int(left[index]) == int(right[index])
        )
        if not equal:
            mismatches.append(record_index)
    return {
        "mismatch_count": len(mismatches),
        "mismatch_at_value_divergent_source_count": sum(
            (record_index // 2) in failing_sources for record_index in mismatches
        ),
        "first_mismatch": (
            {
                "record": mismatches[0],
                "source_ordinal": mismatches[0] // 2,
                "role_ordinal": mismatches[0] % 2,
                "casa": reference[mismatches[0]][index],
                "casars": candidate[mismatches[0]][index],
            }
            if mismatches
            else None
        ),
    }


def compare(
    reference: list[tuple[int | float, ...]],
    candidate: list[tuple[int | float, ...]],
    failing_sources: set[int],
) -> dict[str, Any]:
    topology_exact = len(reference) == len(candidate) and all(
        int(left[0]) == int(right[0]) and int(left[1]) == int(right[1])
        for left, right in zip(reference, candidate)
    )
    fields = {
        name: field_comparison(
            reference, candidate, index, failing_sources, float_bits=False
        )
        for name, index in DISCRETE_FIELDS.items()
    }
    fields.update(
        {
            name: field_comparison(
                reference, candidate, index, failing_sources, float_bits=True
            )
            for name, index in FLOAT64_FIELDS.items()
        }
    )
    difference_sum = 0.0
    reference_sum = 0.0
    failing_difference_sum = 0.0
    failing_reference_sum = 0.0
    nonfinite = 0
    for record_index, (left, right) in enumerate(zip(reference, candidate)):
        for index in (13, 14):
            reference_value = float(left[index])
            candidate_value = float(right[index])
            nonfinite += int(
                not math.isfinite(reference_value)
                or not math.isfinite(candidate_value)
            )
            difference = candidate_value - reference_value
            difference_sum += difference * difference
            reference_sum += reference_value * reference_value
            if record_index // 2 in failing_sources:
                failing_difference_sum += difference * difference
                failing_reference_sum += reference_value * reference_value
    selector_mismatches = sum(
        value["mismatch_count"]
        for name, value in fields.items()
        if name not in {"source_ordinal", "role_ordinal"}
    )
    return {
        "schema": "casa-vlass-aw-cf-metadata-comparison-v1",
        "role": "bounded-correctness-diagnostic-not-performance-evidence",
        "topology_exact": topology_exact,
        "record_count": min(len(reference), len(candidate)),
        "value_divergent_source_count": len(failing_sources),
        "fields": fields,
        "normalization": {
            "normalized_rms": (
                math.sqrt(difference_sum / reference_sum)
                if reference_sum
                else math.inf
            ),
            "value_divergent_sources_normalized_rms": (
                math.sqrt(failing_difference_sum / failing_reference_sum)
                if failing_reference_sum
                else None
            ),
            "nonfinite_component_count": nonfinite,
        },
        "classification": (
            "cf-selector-or-placement-divergence"
            if selector_mismatches
            else "cf-selector-and-placement-exact"
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--casa-receipt", required=True, type=Path)
    parser.add_argument("--casars-sidecar", required=True, type=Path)
    parser.add_argument("--value-comparison", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite output: {args.output}")
    casa_receipt = load_json(args.casa_receipt)
    casars_receipt = load_json(args.casars_sidecar)
    casa_stream = stream(casa_receipt, True)
    casars_stream = stream(casars_receipt, False)
    casa_path = Path(casa_stream["path"])
    casars_path = Path(casars_stream["path"])
    value_comparison = (
        load_json(args.value_comparison) if args.value_comparison else None
    )
    receipt = compare(
        decode(casa_path.read_bytes()),
        decode(casars_path.read_bytes()),
        divergent_sources(value_comparison),
    )
    receipt["inputs"] = {
        "casa_receipt": str(args.casa_receipt.resolve()),
        "casa_receipt_sha256": sha256_file(args.casa_receipt),
        "casa_stream": str(casa_path.resolve()),
        "casa_stream_sha256": sha256_file(casa_path),
        "casars_sidecar": str(args.casars_sidecar.resolve()),
        "casars_sidecar_sha256": sha256_file(args.casars_sidecar),
        "casars_stream": str(casars_path.resolve()),
        "casars_stream_sha256": sha256_file(casars_path),
        "value_comparison": (
            str(args.value_comparison.resolve()) if args.value_comparison else None
        ),
        "value_comparison_sha256": (
            sha256_file(args.value_comparison) if args.value_comparison else None
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(receipt, sort_keys=True))


if __name__ == "__main__":
    main()
