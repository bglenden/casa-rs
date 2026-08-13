#!/usr/bin/env python3
"""Compare one matched CASA/casa-rs MT-MFS residual source sample offline."""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
from pathlib import Path
from typing import Any


CASARS_SCHEMA = "casa-rs-vlass-aw-prediction-prefix-trace-v2"
CASA_RESIDUAL_SCHEMA = "casa-vlass-aw-datatogrid-sample-v1"
CASA_TERM_SCHEMA = "casa-vlass-frozen-model-term-degrid-oracle-v1"
OUTPUT_SCHEMA = "casa-rs-vlass-aw-residual-source-comparison-v2"
TERM_RECORD = struct.Struct("<IIQIIdfI16f")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def float_from_bits(word: int) -> float:
    return struct.unpack("<f", struct.pack("<I", word))[0]


def bits_from_float(value: float) -> int:
    return struct.unpack("<I", struct.pack("<f", value))[0]


def ordered_float_word(word: int) -> int:
    return (~word & 0xFFFF_FFFF) if word & 0x8000_0000 else word | 0x8000_0000


def ulp_distance(first: int, second: int) -> int:
    return abs(ordered_float_word(first) - ordered_float_word(second))


def checked_bits(value: Any, label: str) -> list[int]:
    if not isinstance(value, list) or len(value) != 2:
        raise RuntimeError(f"{label} must contain two uint32 words")
    words = [int(word) for word in value]
    if any(word < 0 or word > 0xFFFF_FFFF for word in words):
        raise RuntimeError(f"{label} contains a word outside uint32")
    return words


def complex_comparison(reference: list[int], candidate: list[int]) -> dict[str, Any]:
    reference_value = complex(
        float_from_bits(reference[0]), float_from_bits(reference[1])
    )
    candidate_value = complex(
        float_from_bits(candidate[0]), float_from_bits(candidate[1])
    )
    difference = abs(reference_value - candidate_value)
    return {
        "reference_bits": reference,
        "candidate_bits": candidate,
        "exact": reference == candidate,
        "component_ulp": [
            ulp_distance(reference[0], candidate[0]),
            ulp_distance(reference[1], candidate[1]),
        ],
        "absolute_difference": difference,
        "relative_difference": difference / max(abs(reference_value), 1.0e-30),
    }


def f32_subtract(first_bits: list[int], second_bits: list[int]) -> list[int]:
    return [
        bits_from_float(
            float_from_bits(first_bits[component])
            - float_from_bits(second_bits[component])
        )
        for component in range(2)
    ]


def boundary_complex_bits(
    term_record: dict[str, Any], boundary: str, polarization: str
) -> list[int]:
    boundaries = term_record["boundaries"]
    return [
        int(boundaries[f"{boundary}_{polarization}_re"]["bits"]),
        int(boundaries[f"{boundary}_{polarization}_im"]["bits"]),
    ]


def select_term_record(
    binary: Path,
    *,
    call: int,
    row_in_vb: int,
    channel: int,
    spw: int,
) -> dict[str, Any]:
    if binary.stat().st_size % TERM_RECORD.size != 0:
        raise RuntimeError("CASA term-degrid binary has a partial record")
    selected: tuple[Any, ...] | None = None
    with binary.open("rb") as stream:
        while record_bytes := stream.read(TERM_RECORD.size):
            record = TERM_RECORD.unpack(record_bytes)
            if (
                record[0] == call
                and record[1] == row_in_vb
                and record[3] == spw
                and record[4] == channel
            ):
                if selected is not None:
                    raise RuntimeError("duplicate CASA term-degrid source identity")
                selected = record
    if selected is None:
        raise RuntimeError("CASA term-degrid binary lacks the requested source")
    float_names = (
        "tt0_rr_re",
        "tt0_rr_im",
        "tt0_ll_re",
        "tt0_ll_im",
        "tt1_raw_rr_re",
        "tt1_raw_rr_im",
        "tt1_raw_ll_re",
        "tt1_raw_ll_im",
        "tt1_scaled_rr_re",
        "tt1_scaled_rr_im",
        "tt1_scaled_ll_re",
        "tt1_scaled_ll_im",
        "combined_rr_re",
        "combined_rr_im",
        "combined_ll_re",
        "combined_ll_im",
    )
    return {
        "call": selected[0],
        "row_in_vb": selected[1],
        "row_id": selected[2],
        "spw": selected[3],
        "channel": selected[4],
        "frequency_hz": selected[5],
        "taylor_power1_bits": bits_from_float(selected[6]),
        "boundaries": {
            name: {
                "value": selected[8 + index],
                "bits": bits_from_float(selected[8 + index]),
            }
            for index, name in enumerate(float_names)
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--casa-residual-sample", required=True, type=Path)
    parser.add_argument("--casa-term-binary", required=True, type=Path)
    parser.add_argument("--casa-term-receipt", required=True, type=Path)
    parser.add_argument("--casa-rs-trace", required=True, type=Path)
    parser.add_argument("--casa-rs-run-log", required=True, type=Path)
    parser.add_argument("--raw-data-real-bits", required=True, type=int)
    parser.add_argument("--raw-data-imag-bits", required=True, type=int)
    parser.add_argument("--expected-call", default=0, type=int)
    parser.add_argument("--expected-row", default=0, type=int)
    parser.add_argument("--expected-channel", default=11, type=int)
    parser.add_argument("--expected-spw", default=2, type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite comparison: {args.output}")

    casa_residual = json.loads(
        args.casa_residual_sample.read_text(encoding="utf-8")
    )
    casa_term = json.loads(args.casa_term_receipt.read_text(encoding="utf-8"))
    casa_rs = json.loads(args.casa_rs_trace.read_text(encoding="utf-8"))
    if casa_residual.get("schema") != CASA_RESIDUAL_SCHEMA:
        raise RuntimeError("unexpected CASA residual sample schema")
    if casa_term.get("schema") != CASA_TERM_SCHEMA:
        raise RuntimeError("unexpected CASA term-degrid receipt schema")
    if casa_rs.get("schema") != CASARS_SCHEMA:
        raise RuntimeError("unexpected casa-rs prediction-prefix schema")
    if casa_term.get("status") != "completed-before-residual-gridding":
        raise RuntimeError("CASA term-degrid oracle did not stop at the bounded get")
    if casa_term.get("terminal_boundary") != "bounded-get":
        raise RuntimeError("CASA term-degrid oracle terminal boundary differs")
    if int(casa_rs.get("source_ordinal", -1)) != 0:
        raise RuntimeError("casa-rs trace is not the first retained source")

    expected_identity = {
        "row": args.expected_row,
        "channel": args.expected_channel,
        "polarization": 0,
        "spw": args.expected_spw,
    }
    actual_identity = {
        "row": int(casa_residual.get("row", -1)),
        "channel": int(casa_residual.get("channel", -1)),
        "polarization": int(casa_residual.get("polarization", -1)),
        "spw": int(casa_residual.get("spw", -1)),
    }
    if actual_identity != expected_identity:
        raise RuntimeError("CASA residual sample identity differs from the request")
    term_record = select_term_record(
        args.casa_term_binary,
        call=args.expected_call,
        row_in_vb=args.expected_row,
        channel=args.expected_channel,
        spw=args.expected_spw,
    )

    raw_data_bits = [
        int(args.raw_data_real_bits),
        int(args.raw_data_imag_bits),
    ]
    casa_residual_bits = checked_bits(
        casa_residual.get("vis_cube_bits"), "CASA vis_cube_bits"
    )
    sample_implied_prediction = f32_subtract(raw_data_bits, casa_residual_bits)
    casa_term_prediction_bits = boundary_complex_bits(term_record, "combined", "rr")
    authoritative_casa_residual_bits = f32_subtract(
        raw_data_bits, casa_term_prediction_bits
    )
    casa_rs_result = casa_rs.get("result", {})
    casa_rs_residual_bits = checked_bits(
        casa_rs_result.get("casa_frame_residual_bits"),
        "casa-rs CASA-frame residual",
    )
    casa_rs_prediction_bits = checked_bits(
        casa_rs_result.get("casa_frame_prediction_bits"),
        "casa-rs CASA-frame prediction",
    )
    sample_residual_comparison = complex_comparison(
        casa_residual_bits, casa_rs_residual_bits
    )
    sample_prediction_comparison = complex_comparison(
        sample_implied_prediction, casa_rs_prediction_bits
    )
    casa_oracle_consistency = complex_comparison(
        casa_term_prediction_bits, sample_implied_prediction
    )
    residual_comparison = complex_comparison(
        authoritative_casa_residual_bits,
        casa_rs_residual_bits,
    )
    prediction_comparison = complex_comparison(
        casa_term_prediction_bits,
        casa_rs_prediction_bits,
    )
    residual_identity_check = complex_comparison(
        authoritative_casa_residual_bits,
        f32_subtract(raw_data_bits, casa_term_prediction_bits),
    )
    if not residual_identity_check["exact"]:
        raise RuntimeError(
            "internal error constructing the authoritative CASA residual"
        )
    casa_taylor_bits = int(term_record["taylor_power1_bits"])
    casa_rs_taylor_bits = int(casa_rs["source_sample"]["taylor_x_bits"])
    taylor_comparison = {
        "casa_bits": casa_taylor_bits,
        "casa_rs_bits": casa_rs_taylor_bits,
        "casa": float_from_bits(casa_taylor_bits),
        "casa_rs": float_from_bits(casa_rs_taylor_bits),
        "ulp": ulp_distance(casa_taylor_bits, casa_rs_taylor_bits),
    }
    taylor_comparison["relative_difference"] = abs(
        taylor_comparison["casa"] - taylor_comparison["casa_rs"]
    ) / max(abs(taylor_comparison["casa"]), 1.0e-30)

    residual_parity_limit = 5.0e-7
    taylor_parity_limit = 2.0e-6
    oracle_consistency_passed = (
        casa_oracle_consistency["relative_difference"] <= residual_parity_limit
    )
    if not oracle_consistency_passed:
        classification = "casa-residual-sample-inconsistent-with-term-oracle"
        passed = False
    elif (
        residual_comparison["relative_difference"] <= residual_parity_limit
        and prediction_comparison["relative_difference"] <= residual_parity_limit
        and taylor_comparison["relative_difference"] <= taylor_parity_limit
    ):
        classification = "source-residual-parity-within-f32-roundoff"
        passed = True
    else:
        classification = "source-residual-divergence"
        passed = False

    inputs = {
        "casa_residual_sample": args.casa_residual_sample,
        "casa_term_binary": args.casa_term_binary,
        "casa_term_receipt": args.casa_term_receipt,
        "casa_rs_trace": args.casa_rs_trace,
        "casa_rs_run_log": args.casa_rs_run_log,
    }
    receipt = {
        "schema": OUTPUT_SCHEMA,
        "classification": classification,
        "pass": passed,
        "limits": {
            "prediction_and_residual_relative_difference": residual_parity_limit,
            "taylor_power_relative_difference": taylor_parity_limit,
        },
        "identity": {
            **expected_identity,
            "source_ordinal": 0,
            "source_sample_index": int(casa_rs.get("source_sample_index", -1)),
            "term_record": term_record,
        },
        "raw_data_bits": raw_data_bits,
        "casa_oracle_consistency": {
            "pass": oracle_consistency_passed,
            "term_degrid_combined_vs_raw_minus_sample_residual": casa_oracle_consistency,
        },
        "authoritative_comparison": {
            "reference": "casa-term-degrid-combined",
            "prediction": prediction_comparison,
            "residual": residual_comparison,
        },
        "intermediate_sample_diagnostic": {
            "residual": sample_residual_comparison,
            "prediction_implied_by_raw_minus_sample_residual": (
                sample_prediction_comparison
            ),
        },
        "taylor_power1": taylor_comparison,
        "inputs": {
            label: {"path": str(path), "sha256": sha256_file(path)}
            for label, path in inputs.items()
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
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(receipt, sort_keys=True), flush=True)
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
