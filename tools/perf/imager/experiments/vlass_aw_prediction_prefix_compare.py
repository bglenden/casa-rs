#!/usr/bin/env python3
"""Compare one matched CASA/casa-rs AW prediction footprint offline."""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
from pathlib import Path
from typing import Any

from vlass_degrid_prefix_compare import parse_casa_trace


CASARS_SCHEMAS = {
    "casa-rs-vlass-aw-prediction-prefix-trace-v1",
    "casa-rs-vlass-aw-prediction-prefix-trace-v2",
}
OUTPUT_SCHEMA = "casa-rs-vlass-aw-prediction-prefix-comparison-v1"


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


def ordered_float_word(word: int) -> int:
    """Map an IEEE-754 float word to monotonically ordered integer space."""
    return (~word & 0xFFFF_FFFF) if word & 0x8000_0000 else word | 0x8000_0000


def ulp_distance(first: int, second: int) -> int:
    return abs(ordered_float_word(first) - ordered_float_word(second))


def float_from_bits(word: int) -> float:
    return struct.unpack("<f", struct.pack("<I", word))[0]


def complex_comparison(casa: list[int], casars: list[int]) -> dict[str, Any]:
    casa_value = complex(float_from_bits(casa[0]), float_from_bits(casa[1]))
    casars_value = complex(float_from_bits(casars[0]), float_from_bits(casars[1]))
    difference = abs(casa_value - casars_value)
    return {
        "exact": casa == casars,
        "casa_bits": casa,
        "casa_rs_bits": casars,
        "component_ulp": [
            ulp_distance(casa[0], casars[0]),
            ulp_distance(casa[1], casars[1]),
        ],
        "absolute_difference": difference,
        "relative_difference": difference / max(abs(casa_value), 1.0e-30),
    }


def analyze(
    *,
    casa_trace: dict[str, Any],
    casars_trace: dict[str, Any],
    expected_row: int,
    expected_channel: int,
    expected_source_ordinal: int,
) -> dict[str, Any]:
    if casars_trace.get("schema") not in CASARS_SCHEMAS:
        raise RuntimeError("unexpected casa-rs prefix schema")
    if int(casars_trace.get("source_ordinal", -1)) != expected_source_ordinal:
        raise RuntimeError("casa-rs source ordinal does not match the request")
    if casars_trace.get("logical_role") != "rr" or casars_trace.get("model_term") != 0:
        raise RuntimeError("casa-rs prefix does not target RR TT0")

    meta = casa_trace["meta"]
    identity = {
        "row": int(meta["row"]),
        "channel": int(meta["channel"]),
        "polarization": int(meta["pol"]),
        "model_column": int(meta["mcol"]),
    }
    if identity != {
        "row": expected_row,
        "channel": expected_channel,
        "polarization": 0,
        "model_column": 0,
    }:
        raise RuntimeError("CASA trace identity does not match the requested footprint")

    plan = casars_trace.get("plan", {})
    casa_loc = [int(meta["loc_x"]), int(meta["loc_y"])]
    casa_support = [int(meta["support_x"]), int(meta["support_y"])]
    casars_loc = [int(value) for value in plan.get("loc", [])]
    casars_support = [int(value) for value in plan.get("support", [])]
    expected_casa_taps = (2 * casa_support[0] + 1) * (2 * casa_support[1] + 1)
    expected_casars_taps = (2 * casars_support[0] + 1) * (
        2 * casars_support[1] + 1
    )
    casa_taps = casa_trace["taps"]
    casars_taps = casars_trace.get("taps")
    if not isinstance(casars_taps, list):
        raise RuntimeError("casa-rs trace has no tap list")
    if (
        len(casa_taps) != expected_casa_taps
        or len(casars_taps) != expected_casars_taps
        or int(casars_trace.get("result", {}).get("tap_count", -1))
        != expected_casars_taps
    ):
        raise RuntimeError("a trace tap count does not match its support-derived footprint")

    stages = (
        ("degrid_coefficient", "post_phase_cf", "degrid_coefficient_bits"),
        ("model_tt0", "grid", "model_tt0_bits"),
        ("tap_product", "product", "product_bits"),
        ("accumulator_prefix", "accumulator", "accumulator_bits"),
    )
    mismatch_counts = {"geometry": 0, **{stage: 0 for stage, _, _ in stages}}
    maximum_component_ulp = {stage: 0 for stage, _, _ in stages}
    maximum_relative_difference = {stage: 0.0 for stage, _, _ in stages}
    first_mismatch: dict[str, Any] | None = None
    for field, casa_value, casars_value in (
        ("loc", casa_loc, casars_loc),
        ("support", casa_support, casars_support),
        ("tap_count", expected_casa_taps, expected_casars_taps),
    ):
        if casa_value == casars_value:
            continue
        mismatch_counts["geometry"] += 1
        if first_mismatch is None:
            first_mismatch = {
                "stage": "geometry",
                "field": field,
                "casa": casa_value,
                "casa_rs": casars_value,
            }
    geometry_exact = (
        casa_loc == casars_loc
        and casa_support == casars_support
        and expected_casa_taps == expected_casars_taps
    )
    for ordinal, (casa_tap, casars_tap) in enumerate(
        zip(casa_taps, casars_taps) if geometry_exact else ()
    ):
        geometry = {
            "tap_ordinal": (int(casa_tap["index"]), int(casars_tap["tap_ordinal"])),
            "iy": (int(casa_tap["iy"]), int(casars_tap["iy"])),
            "ix": (int(casa_tap["ix"]), int(casars_tap["ix"])),
            "grid_x": (int(casa_tap["grid_x"]), int(casars_tap["grid_x"])),
            "grid_y": (int(casa_tap["grid_y"]), int(casars_tap["grid_y"])),
        }
        for field, (casa_value, casars_value) in geometry.items():
            if casa_value == casars_value:
                continue
            mismatch_counts["geometry"] += 1
            if first_mismatch is None:
                first_mismatch = {
                    "stage": "geometry",
                    "field": field,
                    "tap_ordinal": ordinal,
                    "casa": casa_value,
                    "casa_rs": casars_value,
                }
        for stage, casa_label, casars_label in stages:
            comparison = complex_comparison(
                casa_bits(casa_tap, casa_label),
                json_bits(casars_tap.get(casars_label), casars_label),
            )
            maximum_component_ulp[stage] = max(
                maximum_component_ulp[stage],
                *comparison["component_ulp"],
            )
            maximum_relative_difference[stage] = max(
                maximum_relative_difference[stage],
                comparison["relative_difference"],
            )
            if comparison["exact"]:
                continue
            mismatch_counts[stage] += 1
            if first_mismatch is None:
                first_mismatch = {
                    "stage": stage,
                    "tap_ordinal": ordinal,
                    **comparison,
                }

    normalizer = None
    numerator = None
    if geometry_exact:
        normalizer_bits = json_bits(
            casars_trace["result"].get("normalizer_bits"),
            "result.normalizer_bits",
        )
        normalizer = complex_comparison(
            casa_bits(casa_trace["result"], "normalization"),
            [normalizer_bits[0], normalizer_bits[1] ^ 0x8000_0000],
        )
        numerator = complex_comparison(
            casa_bits(casa_trace["result"], "pre_phasor"),
            json_bits(
                casars_trace["result"].get("numerator_bits"),
                "result.numerator_bits",
            ),
        )
    coefficient_exact = mismatch_counts["degrid_coefficient"] == 0
    if not geometry_exact or mismatch_counts["geometry"]:
        classification = "geometry-divergence"
    elif not coefficient_exact:
        classification = "aw-coefficient-divergence"
    elif mismatch_counts["model_tt0"]:
        classification = "model-grid-roundoff-boundary"
    elif mismatch_counts["tap_product"] or mismatch_counts["accumulator_prefix"]:
        classification = "tap-arithmetic-roundoff-boundary"
    elif not normalizer["exact"] or not numerator["exact"]:
        classification = "final-prefix-roundoff-boundary"
    else:
        classification = "exact-prefix"

    return {
        "schema": OUTPUT_SCHEMA,
        "instrumentation_valid": True,
        "classification": classification,
        "source": {
            "casa_row_in_vb": expected_row,
            "channel": expected_channel,
            "source_ordinal": expected_source_ordinal,
            "source_sample_index": int(
                casars_trace.get("source_sample_index", -1)
            ),
            "frequency_hz": float(meta["frequency_hz"]),
            "data_w_m": float(meta["data_w_m"]),
            "source_sample": casars_trace.get("source_sample"),
        },
        "footprint": {
            "casa_loc": casa_loc,
            "casa_rs_loc": casars_loc,
            "casa_support": casa_support,
            "casa_rs_support": casars_support,
            "casa_tap_count": expected_casa_taps,
            "casa_rs_tap_count": expected_casars_taps,
            "geometry_exact": geometry_exact and mismatch_counts["geometry"] == 0,
        },
        "tap_prefix": {
            "arithmetic_comparable": geometry_exact,
            "mismatch_counts": mismatch_counts,
            "maximum_component_ulp": maximum_component_ulp,
            "maximum_relative_difference": maximum_relative_difference,
            "first_mismatch": first_mismatch,
        },
        "normalizer": {
            "arithmetic_comparable": geometry_exact,
            "mapping": "CASA-equals-conjugate-of-packed-casa-rs",
            "comparison": normalizer,
        },
        "numerator": {
            "arithmetic_comparable": geometry_exact,
            "comparison": numerator,
        },
        "casa_result": {
            "source_phasor_bits": casa_bits(casa_trace["result"], "phasor"),
            "post_phasor_bits": casa_bits(casa_trace["result"], "post_phasor"),
            "prediction_bits": casa_bits(casa_trace["result"], "prediction"),
        },
        "offline_analyzer_did_not_execute": [
            "measurement_set_read",
            "casa",
            "metal",
            "imaging_grid",
            "fft",
            "clean",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--casa-trace", required=True, type=Path)
    parser.add_argument("--casars-trace", required=True, type=Path)
    parser.add_argument("--casa-run-log", type=Path)
    parser.add_argument("--casars-run-log", type=Path)
    parser.add_argument("--expected-row", required=True, type=int)
    parser.add_argument("--expected-channel", required=True, type=int)
    parser.add_argument("--expected-source-ordinal", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite comparison: {args.output}")

    receipt = analyze(
        casa_trace=parse_casa_trace(args.casa_trace),
        casars_trace=json.loads(args.casars_trace.read_text(encoding="utf-8")),
        expected_row=args.expected_row,
        expected_channel=args.expected_channel,
        expected_source_ordinal=args.expected_source_ordinal,
    )
    inputs = {
        "casa_trace": args.casa_trace,
        "casa_rs_trace": args.casars_trace,
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
    print(json.dumps(receipt, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
