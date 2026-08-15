#!/usr/bin/env python3
"""Compare complete CASA and casa-rs residual/value streams before AW gridding."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import struct
from pathlib import Path
from typing import Any, Sequence


CASA_SCHEMA = "casa-aw-datagrid-input-hash-v1"
CASARS_SCHEMA = "casa-rs-vlass-frozen-model-prediction-sidecar-host-v1"
STREAM_CONTRACT = (
    "source-order-identity4u32-source-phase-complex32-RR-then-LL-"
    "raw-residual-complex32-grid-residual-complex32-"
    "term-weight-times-grid-residual-complex32-little-endian"
)
RECORD = struct.Struct("<4I14f")
RECORD_SIZE = RECORD.size
DEFAULT_NORMALIZED_RMS_LIMIT = 1.0e-3


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


def stream_metadata(
    casa: dict[str, Any], casars: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    if casa.get("schema") != CASA_SCHEMA:
        raise RuntimeError("CASA receipt schema differs")
    if casa.get("status") != "completed-before-grid":
        raise RuntimeError("CASA receipt did not complete before gridding")
    if casars.get("schema") != CASARS_SCHEMA:
        raise RuntimeError("casa-rs sidecar schema differs")
    casa_stream = casa.get("value_stream")
    boundary = casars.get("casa_datatogrid_tt0_value_boundary")
    if not isinstance(casa_stream, dict):
        raise RuntimeError("CASA receipt lacks a value stream")
    if not isinstance(boundary, dict):
        raise RuntimeError("casa-rs sidecar lacks the DataToGrid value boundary")
    casars_stream = boundary.get("value_stream")
    if not isinstance(casars_stream, dict):
        raise RuntimeError("casa-rs sidecar lacks a value stream")
    for label, stream in [("CASA", casa_stream), ("casa-rs", casars_stream)]:
        if stream.get("contract") != STREAM_CONTRACT:
            raise RuntimeError(f"{label} value-stream contract differs")
        if int(stream.get("record_size", 0)) != RECORD_SIZE:
            raise RuntimeError(f"{label} value-stream record size differs")
    return casa_stream, casars_stream


def decode_records(payload: bytes, label: str) -> list[tuple[int | float, ...]]:
    if len(payload) % RECORD_SIZE != 0:
        raise RuntimeError(
            f"{label} value stream has {len(payload)} bytes, not a multiple of "
            f"{RECORD_SIZE}"
        )
    return list(RECORD.iter_unpack(payload))


def percentile(values: Sequence[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = quantile * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def metric(
    reference: Sequence[tuple[int | float, ...]],
    candidate: Sequence[tuple[int | float, ...]],
    component_indices: Sequence[int],
    global_floor: float,
    normalized_rms_limit: float,
) -> dict[str, Any]:
    difference_sum_squares = 0.0
    reference_sum_squares = 0.0
    max_absolute = 0.0
    per_source_relative: list[float] = []
    first_over_limit: int | None = None
    over_limit_ordinals: list[int] = []
    over_limit_by_spw: dict[int, int] = {}
    source_diagnostics: list[tuple[float, int]] = []
    exact_component_count = 0
    component_count = 0
    for source_index, (left, right) in enumerate(zip(reference, candidate), start=1):
        source_difference = 0.0
        source_reference = 0.0
        for component_index in component_indices:
            reference_value = float(left[component_index])
            candidate_value = float(right[component_index])
            difference = candidate_value - reference_value
            difference_sum_squares += difference * difference
            reference_sum_squares += reference_value * reference_value
            source_difference += difference * difference
            source_reference += reference_value * reference_value
            max_absolute = max(max_absolute, abs(difference))
            exact_component_count += int(
                struct.pack("<f", reference_value)
                == struct.pack("<f", candidate_value)
            )
            component_count += 1
        source_relative = math.sqrt(source_difference) / max(
            math.sqrt(source_reference), global_floor
        )
        per_source_relative.append(source_relative)
        source_diagnostics.append((source_relative, source_index - 1))
        if first_over_limit is None and source_relative > normalized_rms_limit:
            first_over_limit = source_index
        if source_relative > normalized_rms_limit:
            ordinal = source_index - 1
            over_limit_ordinals.append(ordinal)
            spw = int(left[2])
            over_limit_by_spw[spw] = over_limit_by_spw.get(spw, 0) + 1
    normalized_rms = (
        math.sqrt(difference_sum_squares / reference_sum_squares)
        if reference_sum_squares > 0.0
        else (0.0 if difference_sum_squares == 0.0 else math.inf)
    )
    def identity(ordinal: int) -> dict[str, Any]:
        return {
            "ordinal": ordinal,
            "casa": [int(value) for value in reference[ordinal][:4]],
            "casars": [int(value) for value in candidate[ordinal][:4]],
        }

    over_limit_ranges: list[list[int]] = []
    for ordinal in over_limit_ordinals:
        if over_limit_ranges and ordinal == over_limit_ranges[-1][1] + 1:
            over_limit_ranges[-1][1] = ordinal
        else:
            over_limit_ranges.append([ordinal, ordinal])
    largest = sorted(source_diagnostics, reverse=True)[:12]
    maximum_ordinal = largest[0][1] if largest else None
    return {
        "normalized_rms": normalized_rms,
        "difference_rms": math.sqrt(difference_sum_squares / component_count),
        "reference_rms": math.sqrt(reference_sum_squares / component_count),
        "max_absolute": max_absolute,
        "exact_component_fraction": exact_component_count / component_count,
        "per_source_relative_l2": {
            "normalization_floor": global_floor,
            "p50": percentile(per_source_relative, 0.50),
            "p95": percentile(per_source_relative, 0.95),
            "p99": percentile(per_source_relative, 0.99),
            "maximum": max(per_source_relative, default=None),
            "first_over_contract_limit": first_over_limit,
            "first_over_contract_limit_identity": (
                identity(first_over_limit - 1)
                if first_over_limit is not None
                else None
            ),
            "maximum_identity": (
                identity(maximum_ordinal)
                if maximum_ordinal is not None
                else None
            ),
            "over_contract_limit_count": len(over_limit_ordinals),
            "over_contract_limit_by_spw": {
                str(spw): count for spw, count in sorted(over_limit_by_spw.items())
            },
            "over_contract_limit_ordinal_ranges": over_limit_ranges,
            "largest_sources": [
                {
                    **identity(ordinal),
                    "relative_l2": relative,
                }
                for relative, ordinal in largest
            ],
        },
    }


def compare(
    casa: dict[str, Any],
    casars: dict[str, Any],
    casa_payload: bytes,
    casars_payload: bytes,
    normalized_rms_limit: float = DEFAULT_NORMALIZED_RMS_LIMIT,
) -> dict[str, Any]:
    casa_stream, casars_stream = stream_metadata(casa, casars)
    reference = decode_records(casa_payload, "CASA")
    candidate = decode_records(casars_payload, "casa-rs")
    boundary = casars["casa_datatogrid_tt0_value_boundary"]
    casa_sources = int(casa["source_count"])
    casars_sources = int(boundary["source_count"])
    expected_casa_bytes = casa_sources * RECORD_SIZE
    expected_casars_bytes = casars_sources * RECORD_SIZE
    topology_exact = (
        casa_sources == casars_sources
        and int(casa["role_count"]) == int(boundary["role_count"])
        and len(reference) == casa_sources
        and len(candidate) == casars_sources
        and int(casa_stream["allocated_bytes"]) == expected_casa_bytes
        and int(casars_stream["allocated_bytes"]) == expected_casars_bytes
        and len(casa_payload) == expected_casa_bytes
        and len(casars_payload) == expected_casars_bytes
    )
    all_values = [
        float(value) for record in reference + candidate for value in record[4:]
    ]
    nonfinite_count = sum(not math.isfinite(value) for value in all_values)
    comparable_sources = min(len(reference), len(candidate))
    reference = reference[:comparable_sources]
    candidate = candidate[:comparable_sources]
    reference_component_count = max(comparable_sources * 4, 1)
    phase_reference_rms = math.sqrt(
        sum(
            float(record[index]) * float(record[index])
            for record in reference
            for index in (4, 5)
        )
        / max(comparable_sources * 2, 1)
    )
    raw_reference_rms = math.sqrt(
        sum(
            float(record[index]) * float(record[index])
            for record in reference
            for index in (6, 7, 12, 13)
        )
        / reference_component_count
    )
    grid_reference_rms = math.sqrt(
        sum(
            float(record[index]) * float(record[index])
            for record in reference
            for index in (8, 9, 14, 15)
        )
        / reference_component_count
    )
    value_reference_rms = math.sqrt(
        sum(
            float(record[index]) * float(record[index])
            for record in reference
            for index in (10, 11, 16, 17)
        )
        / reference_component_count
    )
    source_phase = metric(
        reference,
        candidate,
        (4, 5),
        max(phase_reference_rms, 1.0e-30),
        normalized_rms_limit,
    )
    raw_residual = metric(
        reference,
        candidate,
        (6, 7, 12, 13),
        max(raw_reference_rms, 1.0e-30),
        normalized_rms_limit,
    )
    grid_residual = metric(
        reference,
        candidate,
        (8, 9, 14, 15),
        max(grid_reference_rms, 1.0e-30),
        normalized_rms_limit,
    )
    value = metric(
        reference,
        candidate,
        (10, 11, 16, 17),
        max(value_reference_rms, 1.0e-30),
        normalized_rms_limit,
    )
    numerical_pass = (
        source_phase["normalized_rms"] <= normalized_rms_limit
        and raw_residual["normalized_rms"] <= normalized_rms_limit
        and grid_residual["normalized_rms"] <= normalized_rms_limit
        and value["normalized_rms"] <= normalized_rms_limit
    )
    passed = topology_exact and nonfinite_count == 0 and numerical_pass
    return {
        "schema": "casa-vlass-aw-datatogrid-value-comparison-v1",
        "role": "bounded-correctness-diagnostic-not-performance-evidence",
        "classification": (
            "residual-value-stream-within-contract"
            if passed
            else (
                "residual-value-stream-topology-failure"
                if not topology_exact
                else (
                    "residual-value-stream-nonfinite"
                    if nonfinite_count
                    else "residual-value-stream-numerical-divergence"
                )
            )
        ),
        "passed": passed,
        "contract": {
            "normalized_rms_limit": normalized_rms_limit,
            "topology_exact_required": True,
            "finite_values_required": True,
            "exact_bits_are_diagnostic_only": True,
        },
        "topology": {
            "exact": topology_exact,
            "casa_sources": casa_sources,
            "casars_sources": casars_sources,
            "casa_roles": int(casa["role_count"]),
            "casars_roles": int(boundary["role_count"]),
            "casa_bytes": len(casa_payload),
            "casars_bytes": len(casars_payload),
            "record_size": RECORD_SIZE,
            "casa_first_identity": (
                list(reference[0][:4]) if reference else None
            ),
            "casars_first_identity": (
                list(candidate[0][:4]) if candidate else None
            ),
        },
        "nonfinite_component_count": nonfinite_count,
        "source_phase": source_phase,
        "raw_residual": raw_residual,
        "grid_residual": grid_residual,
        "weighted_tt0_value": value,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--casa-receipt", required=True, type=Path)
    parser.add_argument("--casars-sidecar", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--normalized-rms-limit",
        type=float,
        default=DEFAULT_NORMALIZED_RMS_LIMIT,
    )
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite output: {args.output}")
    casa = load_json(args.casa_receipt)
    casars = load_json(args.casars_sidecar)
    casa_stream, casars_stream = stream_metadata(casa, casars)
    casa_path = Path(casa_stream["path"])
    casars_path = Path(casars_stream["path"])
    receipt = compare(
        casa,
        casars,
        casa_path.read_bytes(),
        casars_path.read_bytes(),
        normalized_rms_limit=args.normalized_rms_limit,
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
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(receipt, sort_keys=True))


if __name__ == "__main__":
    main()
