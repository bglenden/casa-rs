#!/usr/bin/env python3
"""Localize the remaining VLASS source-1446 TT0 mismatch.

The CASA input is one bounded, instrumented GridToData footprint.  The
casa-rs input is the matching packed Metal replay program evaluated on the
CPU before Metal dispatch.  The frozen source-phase receipt supplies the
official CASA and casa-rs-wide TT0 boundary bits.  No MeasurementSet, CASA,
Metal command, imaging grid, image product, or CLEAN execution occurs here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from vlass_degrid_prefix_compare import parse_casa_trace


CASARS_SCHEMA = "casa-rs-vlass-aw-prediction-prefix-trace-v1"
PHASE_SCHEMA = "casa-rs-vlass-aw-source-phase-placement-replay-v1"
OUTPUT_SCHEMA = "casa-rs-vlass-source1446-aw-prefix-comparison-v1"
EXPECTED_SOURCE_ORDINAL = 1_446
EXPECTED_SAMPLE_COUNT = 98_239
EXPECTED_ROW_IN_VB = 35
EXPECTED_MAIN_ROW = 353_635
EXPECTED_CHANNEL = 19
EXPECTED_SPW = 2


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def casa_bits(record: dict[str, str], label: str) -> list[int]:
    try:
        return [int(record[f"{label}_re"]), int(record[f"{label}_im"])]
    except KeyError as error:
        raise RuntimeError(f"CASA trace is missing {error.args[0]}") from error


def json_bits(value: Any, label: str) -> list[int]:
    if not isinstance(value, list) or len(value) != 2:
        raise RuntimeError(f"{label} must contain two component words")
    result = [int(word) for word in value]
    if any(word < 0 or word > 0xFFFF_FFFF for word in result):
        raise RuntimeError(f"{label} contains a word outside uint32")
    return result


def analyze(
    *,
    casa_trace: dict[str, Any],
    casars_trace: dict[str, Any],
    phase_receipt: dict[str, Any],
) -> dict[str, Any]:
    if casars_trace.get("schema") != CASARS_SCHEMA:
        raise RuntimeError("unexpected casa-rs prefix schema")
    if phase_receipt.get("schema") != PHASE_SCHEMA:
        raise RuntimeError("unexpected source-phase receipt schema")
    if int(casars_trace.get("source_ordinal", -1)) != EXPECTED_SOURCE_ORDINAL:
        raise RuntimeError("casa-rs prefix does not target source 1446")
    if casars_trace.get("logical_role") != "rr" or casars_trace.get("model_term") != 0:
        raise RuntimeError("casa-rs prefix does not target RR TT0")
    if (
        int(casars_trace.get("program", {}).get("sample_count", -1))
        != EXPECTED_SAMPLE_COUNT
    ):
        raise RuntimeError("casa-rs packed-program source census changed")

    meta = casa_trace["meta"]
    if (
        int(meta.get("row", -1)) != EXPECTED_ROW_IN_VB
        or int(meta.get("channel", -1)) != EXPECTED_CHANNEL
        or int(meta.get("pol", -1)) != 0
        or int(meta.get("mcol", -1)) != 0
    ):
        raise RuntimeError("CASA trace does not target row 35, channel 19, RR TT0")
    plan = casars_trace.get("plan", {})
    casa_loc = [int(meta["loc_x"]), int(meta["loc_y"])]
    casa_support = [int(meta["support_x"]), int(meta["support_y"])]
    if [int(value) for value in plan.get("loc", [])] != casa_loc:
        raise RuntimeError("CASA and casa-rs tap origins differ")
    if [int(value) for value in plan.get("support", [])] != casa_support:
        raise RuntimeError("CASA and casa-rs tap supports differ")
    expected_taps = (2 * casa_support[0] + 1) * (2 * casa_support[1] + 1)
    casa_taps = casa_trace["taps"]
    casars_taps = casars_trace.get("taps")
    if (
        not isinstance(casars_taps, list)
        or len(casa_taps) != expected_taps
        or len(casars_taps) != expected_taps
        or int(casars_trace.get("result", {}).get("tap_count", -1)) != expected_taps
    ):
        raise RuntimeError(
            "trace tap count does not match the support-derived footprint"
        )

    first_phase_mismatch = phase_receipt.get("first_mismatch")
    if not isinstance(first_phase_mismatch, dict):
        raise RuntimeError("source-phase receipt has no remaining mismatch")
    if first_phase_mismatch.get("boundary") != "raw_tt0":
        raise RuntimeError("source-phase receipt first mismatch is not raw TT0")
    source = first_phase_mismatch.get("source", {}).get("current", {})
    if (
        int(source.get("source_ordinal", -1)) != EXPECTED_SOURCE_ORDINAL
        or int(source.get("row_id", -1)) != EXPECTED_MAIN_ROW
        or int(source.get("spw_id", -1)) != EXPECTED_SPW
        or int(source.get("channel", -1)) != EXPECTED_CHANNEL
        or source.get("role") != "rr"
    ):
        raise RuntimeError("source-phase receipt source identity changed")

    mismatch_counts = {
        "geometry": 0,
        "degrid_coefficient": 0,
        "model_tt0": 0,
        "tap_product": 0,
        "accumulator_prefix": 0,
    }
    first_tap_mismatch: dict[str, Any] | None = None
    for ordinal, (casa_tap, casars_tap) in enumerate(
        zip(casa_taps, casars_taps, strict=True)
    ):
        geometry_pairs = (
            ("tap_ordinal", int(casa_tap["index"]), int(casars_tap["tap_ordinal"])),
            ("iy", int(casa_tap["iy"]), int(casars_tap["iy"])),
            ("ix", int(casa_tap["ix"]), int(casars_tap["ix"])),
            ("grid_x", int(casa_tap["grid_x"]), int(casars_tap["grid_x"])),
            ("grid_y", int(casa_tap["grid_y"]), int(casars_tap["grid_y"])),
        )
        for field, expected, actual in geometry_pairs:
            if expected == actual:
                continue
            mismatch_counts["geometry"] += 1
            if first_tap_mismatch is None:
                first_tap_mismatch = {
                    "stage": "geometry",
                    "field": field,
                    "tap_ordinal": ordinal,
                    "casa": expected,
                    "casa_rs": actual,
                }
        comparisons = (
            (
                "degrid_coefficient",
                casa_bits(casa_tap, "post_phase_cf"),
                json_bits(
                    casars_tap.get("degrid_coefficient_bits"),
                    "degrid_coefficient_bits",
                ),
            ),
            (
                "model_tt0",
                casa_bits(casa_tap, "grid"),
                json_bits(casars_tap.get("model_tt0_bits"), "model_tt0_bits"),
            ),
            (
                "tap_product",
                casa_bits(casa_tap, "product"),
                json_bits(casars_tap.get("product_bits"), "product_bits"),
            ),
            (
                "accumulator_prefix",
                casa_bits(casa_tap, "accumulator"),
                json_bits(
                    casars_tap.get("accumulator_bits"),
                    "accumulator_bits",
                ),
            ),
        )
        for stage, expected, actual in comparisons:
            if expected == actual:
                continue
            mismatch_counts[stage] += 1
            if first_tap_mismatch is None:
                first_tap_mismatch = {
                    "stage": stage,
                    "tap_ordinal": ordinal,
                    "casa_bits": expected,
                    "casa_rs_bits": actual,
                }

    casa_normalizer = casa_bits(casa_trace["result"], "normalization")
    casars_normalizer = json_bits(
        casars_trace["result"].get("normalizer_bits"),
        "result.normalizer_bits",
    )
    casars_normalizer_conjugate = [
        casars_normalizer[0],
        casars_normalizer[1] ^ 0x8000_0000,
    ]
    normalizer_mapping_exact = casa_normalizer == casars_normalizer_conjugate
    casa_numerator = casa_bits(casa_trace["result"], "pre_phasor")
    casars_numerator = json_bits(
        casars_trace["result"].get("numerator_bits"),
        "result.numerator_bits",
    )
    numerator_exact = casa_numerator == casars_numerator
    official_casa_tt0 = json_bits(
        first_phase_mismatch.get("expected_bits"),
        "first_mismatch.expected_bits",
    )
    casars_wide_tt0 = json_bits(
        first_phase_mismatch.get("actual_bits"),
        "first_mismatch.actual_bits",
    )
    prefix_exact = (
        not any(mismatch_counts.values())
        and normalizer_mapping_exact
        and numerator_exact
    )
    if not prefix_exact:
        classification = "tap-prefix-or-normalizer-divergence"
    elif official_casa_tt0 == casars_wide_tt0:
        classification = "source1446-raw-tt0-exact"
    else:
        classification = "exact-tap-prefix-final-normalization-boundary"

    return {
        "schema": OUTPUT_SCHEMA,
        "instrumentation_valid": True,
        "classification": classification,
        "source": {
            "source_ordinal": EXPECTED_SOURCE_ORDINAL,
            "main_row": EXPECTED_MAIN_ROW,
            "row_in_casa_vb": EXPECTED_ROW_IN_VB,
            "spw": EXPECTED_SPW,
            "channel": EXPECTED_CHANNEL,
            "role": "rr",
            "term": 0,
            "frequency_hz": float(meta["frequency_hz"]),
        },
        "footprint": {
            "loc": casa_loc,
            "support": casa_support,
            "tap_count": expected_taps,
            "tap_order": "y-outer-x-inner",
        },
        "tap_prefix": {
            "exact": not any(mismatch_counts.values()),
            "mismatch_counts": mismatch_counts,
            "first_mismatch": first_tap_mismatch,
        },
        "normalizer": {
            "mapping": "CASA-normalizer-equals-conjugate-of-packed-casa-rs-normalizer",
            "exact": normalizer_mapping_exact,
            "casa_bits": casa_normalizer,
            "casa_rs_packed_bits": casars_normalizer,
            "casa_rs_conjugated_bits": casars_normalizer_conjugate,
        },
        "numerator": {
            "exact": numerator_exact,
            "casa_bits": casa_numerator,
            "casa_rs_bits": casars_numerator,
        },
        "raw_tt0_boundary": {
            "official_casa_bits": official_casa_tt0,
            "casa_rs_wide_candidate_bits": casars_wide_tt0,
            "recompiled_instrumented_casa_post_source_phasor_bits": casa_bits(
                casa_trace["result"],
                "prediction",
            ),
            "source_phasor_bits": casa_bits(casa_trace["result"], "phasor"),
        },
        "first_proven_divergence": (
            None
            if classification == "source1446-raw-tt0-exact"
            else {
                "stage": (
                    "final_normalization"
                    if prefix_exact
                    else first_tap_mismatch["stage"]
                    if first_tap_mismatch is not None
                    else "normalizer_or_numerator"
                ),
                "tap_ordinal": (
                    None
                    if prefix_exact or first_tap_mismatch is None
                    else first_tap_mismatch["tap_ordinal"]
                ),
            }
        ),
        "limitations": [
            "the instrumented CASA rebuild proves its tap prefix but its final quotient is not the frozen installed-CASA quotient oracle",
            "this receipt does not select a replacement final-division operation graph",
            "the casa-rs input run reconstructed the frozen final state through initial dirty imaging, the bounded minor cycle, model FFT, and compact-program build before its residual-refresh capture stop",
        ],
        "offline_analyzer_did_not_execute": [
            "measurement_set_read",
            "casa",
            "metal",
            "imaging_grid",
            "fft",
            "image_product",
            "clean",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--casa-trace", type=Path, required=True)
    parser.add_argument("--casars-trace", type=Path, required=True)
    parser.add_argument("--phase-receipt", type=Path, required=True)
    parser.add_argument("--casa-run-log", type=Path)
    parser.add_argument("--casars-run-log", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit(f"refusing to overwrite {args.output}")

    receipt = analyze(
        casa_trace=parse_casa_trace(args.casa_trace),
        casars_trace=json.loads(args.casars_trace.read_text(encoding="utf-8")),
        phase_receipt=json.loads(args.phase_receipt.read_text(encoding="utf-8")),
    )
    inputs = {
        "casa_trace": args.casa_trace,
        "casa_rs_trace": args.casars_trace,
        "source_phase_receipt": args.phase_receipt,
    }
    if args.casa_run_log is not None:
        inputs["casa_run_log"] = args.casa_run_log
    if args.casars_run_log is not None:
        inputs["casa_rs_run_log"] = args.casars_run_log
    receipt["inputs"] = {
        label: {"path": str(path), "sha256": sha256_file(path)}
        for label, path in inputs.items()
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(args.output)


if __name__ == "__main__":
    main()
