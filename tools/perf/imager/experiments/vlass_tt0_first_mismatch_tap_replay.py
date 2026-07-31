#!/usr/bin/env python3
"""Replay the first four-SPW VLASS TT0 mismatch at float32 boundaries.

The current four-SPW casa-rs model-grid prefix is combined with the already
frozen CASA 361-tap coefficient trace.  The accompanying Rust receipt proves
that the four-SPW and full-16-SPW caches select the same bit-identical imaging
CF for this source.  This is an offline arithmetic diagnostic: it does not run
CASA, read an MS, grid residuals, form images, or execute CLEAN.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
from pathlib import Path
from typing import Any

import numpy as np

from vlass_degrid_phase_arithmetic import (
    complex_add,
    complex_divide,
    complex_multiply,
    encoded,
    f32,
)
from vlass_degrid_prefix_compare import (
    complex_from_bits,
    f32_bits,
    f32_from_bits,
    parse_casa_trace,
)


EXPECTED_INPUT_SCHEMA = "casa-rs-vlass-tt0-first-mismatch-replay-inputs-v1"
EXPECTED_TERM_SCHEMA = "casa-rs-vlass-term-separated-prediction-comparison-v1"
EXPECTED_MODEL_GRID_SHA256 = (
    "2cc338fcd624042ece5727245d51182f990f78fef85200b8fd7ca4011c745289"
)


def fma_f32(left: float, right: float, addend: float) -> np.float32:
    """Return an exactly rounded float32 fused multiply-add for float32 inputs."""

    # A product of two binary32 values and its sum with another binary32 value
    # fit exactly in binary64, so the final narrowing is a true binary32 FMA.
    return f32(float(f32(left)) * float(f32(right)) + float(f32(addend)))


def complex_divide_fused_numerator(left: complex, right: complex) -> complex:
    left_re = f32(left.real)
    left_im = f32(left.imag)
    right_re = f32(right.real)
    right_im = f32(right.imag)
    denominator = f32(f32(right_re * right_re) + f32(right_im * right_im))
    return complex(
        f32(
            fma_f32(
                left_re,
                right_re,
                f32(left_im * right_im),
            )
            / denominator
        ),
        f32(
            fma_f32(
                left_im,
                right_re,
                -f32(left_re * right_im),
            )
            / denominator
        ),
    )


def complex_divide_wide_intermediate(left: complex, right: complex) -> complex:
    """Evaluate the complex quotient in binary64 and narrow each result once."""

    left_re = float(f32(left.real))
    left_im = float(f32(left.imag))
    right_re = float(f32(right.real))
    right_im = float(f32(right.imag))
    denominator = right_re * right_re + right_im * right_im
    return complex(
        f32((left_re * right_re + left_im * right_im) / denominator),
        f32((left_im * right_re - left_re * right_im) / denominator),
    )


def bits_to_complex(bits: list[int]) -> complex:
    if len(bits) != 2:
        raise ValueError(f"expected two complex component bits, got {bits!r}")
    return complex(f32_from_bits(int(bits[0])), f32_from_bits(int(bits[1])))


def complex_bits(value: complex) -> list[int]:
    return [f32_bits(value.real), f32_bits(value.imag)]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def validate_inputs(
    *,
    trace: dict[str, Any],
    replay_inputs: dict[str, Any],
    term_comparison: dict[str, Any],
) -> dict[str, Any]:
    if replay_inputs.get("schema") != EXPECTED_INPUT_SCHEMA:
        raise RuntimeError("unexpected replay-input schema")
    if term_comparison.get("schema") != EXPECTED_TERM_SCHEMA:
        raise RuntimeError("unexpected term-comparison schema")
    if not term_comparison.get("instrumentation_valid"):
        raise RuntimeError("term-comparison instrumentation is not valid")
    model = replay_inputs.get("model", {})
    if model.get("grid_sha256") != EXPECTED_MODEL_GRID_SHA256:
        raise RuntimeError("four-SPW TT0 model-grid hash changed")
    if model.get("support_positions") != 2166:
        raise RuntimeError("four-SPW frozen-model support changed")
    cache = replay_inputs.get("coefficient_cache_binding", {})
    if cache.get("pixel_mismatch_count") != 0:
        raise RuntimeError("four-SPW and full-16-SPW selected CF pixels differ")

    meta = trace["meta"]
    taps = trace["taps"]
    result = trace["result"]
    geometry = replay_inputs.get("tap_geometry", {})
    if len(taps) != 361 or geometry.get("tap_count") != 361:
        raise RuntimeError("expected exactly 361 taps")
    if [int(meta["loc_x"]), int(meta["loc_y"])] != geometry.get("loc"):
        raise RuntimeError("CASA trace and four-SPW tap origins differ")
    if [int(meta["support_x"]), int(meta["support_y"])] != geometry.get("support"):
        raise RuntimeError("CASA trace and four-SPW tap supports differ")

    source = term_comparison["first_mismatch"]["source"]["current"]
    if int(source["source_ordinal"]) != 0 or source["role"] != "rr":
        raise RuntimeError("term comparison no longer targets source-zero RR")
    if int(meta["row"]) != 0 or int(meta["channel"]) != int(source["channel"]):
        raise RuntimeError("CASA trace and term comparison source identity differ")
    if np.float64(meta["frequency_hz"]).view(np.uint64) != np.float64(
        source["frequency_hz"]
    ).view(np.uint64):
        raise RuntimeError("CASA trace and term comparison frequencies differ")
    trace_phasor = complex_from_bits(result, "phasor")
    source_phase = bits_to_complex(
        [int(source["phase_re_bits"]), int(source["phase_im_bits"])]
    )
    if complex_bits(trace_phasor) != complex_bits(source_phase):
        raise RuntimeError("CASA trace and four-SPW source phasors differ")

    coefficient_chain_mismatches = 0
    norm = 0j
    for index, tap in enumerate(taps):
        grid = replay_inputs["taps"][index]
        if (
            int(tap["index"]) != index
            or int(tap["grid_x"]) != int(grid["grid_x"])
            or int(tap["grid_y"]) != int(grid["grid_y"])
        ):
            raise RuntimeError(f"tap geometry differs at index {index}")
        raw = complex_from_bits(tap, "raw_cf")
        post_w = complex_from_bits(tap, "post_w_cf")
        phase = complex_from_bits(tap, "phase")
        post_phase = complex_from_bits(tap, "post_phase_cf")
        expected_post_w = raw if float(meta["data_w_m"]) > 0.0 else raw.conjugate()
        expected_post_phase = complex_multiply(post_w, phase.conjugate())
        coefficient_chain_mismatches += int(
            complex_bits(post_w) != complex_bits(expected_post_w)
        )
        coefficient_chain_mismatches += int(
            complex_bits(post_phase) != complex_bits(expected_post_phase)
        )
        norm = complex_add(norm, post_w)
    recorded_norm = complex_from_bits(result, "normalization")
    if complex_bits(norm) != complex_bits(recorded_norm):
        raise RuntimeError("CASA trace normalization is not self-consistent")
    if coefficient_chain_mismatches:
        raise RuntimeError("CASA trace coefficient chain is not self-consistent")
    return {
        "source_ordinal": 0,
        "role": "rr",
        "term": 0,
        "tap_count": len(taps),
        "model_grid_sha256": model["grid_sha256"],
        "selected_cf_pixel_count": cache["pixel_count"],
        "selected_cf_pixel_mismatch_count": 0,
        "coefficient_chain_mismatch_count": 0,
        "source_phasor_bits": complex_bits(trace_phasor),
        "normalization_bits": complex_bits(recorded_norm),
    }


def replay(
    *,
    trace: dict[str, Any],
    replay_inputs: dict[str, Any],
    term_comparison: dict[str, Any],
) -> dict[str, Any]:
    validation = validate_inputs(
        trace=trace,
        replay_inputs=replay_inputs,
        term_comparison=term_comparison,
    )
    stream_hash = hashlib.sha256()
    prefix_hash = hashlib.sha256()
    accumulator = 0j
    for index, (tap, grid_record) in enumerate(
        zip(trace["taps"], replay_inputs["taps"], strict=True)
    ):
        coefficient = complex_from_bits(tap, "post_phase_cf")
        grid = bits_to_complex([int(value) for value in grid_record["grid_bits"]])
        product = complex_multiply(coefficient, grid)
        accumulator = complex_add(accumulator, product)
        coefficient_bits = complex_bits(coefficient)
        grid_bits = complex_bits(grid)
        product_bits = complex_bits(product)
        accumulator_bits = complex_bits(accumulator)
        stream_hash.update(struct.pack("<I4I", index, *coefficient_bits, *grid_bits))
        prefix_hash.update(struct.pack("<I4I", index, *product_bits, *accumulator_bits))

    phasor = complex_from_bits(trace["result"], "phasor")
    normalization = complex_from_bits(trace["result"], "normalization")
    post_phasor = complex_multiply(accumulator, phasor.conjugate())
    quotients = {
        "uncontracted_float32": complex_divide(post_phasor, normalization),
        "fused_float32_numerator": complex_divide_fused_numerator(
            post_phasor, normalization
        ),
        "wide_intermediate": complex_divide_wide_intermediate(
            post_phasor, normalization
        ),
    }
    actual = term_comparison["first_mismatch"]
    casa_bits = [int(value) for value in actual["casa_tt0_raw_bits"]]
    casars_bits = [int(value) for value in actual["casars_tt0_bits"]]
    matches = {
        name: {
            "casa": complex_bits(value) == casa_bits,
            "casa_rs": complex_bits(value) == casars_bits,
        }
        for name, value in quotients.items()
    }
    classification = (
        "final-complex-division-arithmetic-boundary"
        if (
            matches["wide_intermediate"]["casa"]
            and matches["fused_float32_numerator"]["casa_rs"]
        )
        else "not-localized"
    )
    return {
        "schema": "casa-rs-vlass-tt0-first-mismatch-tap-replay-v1",
        "role": "offline_single_source_diagnostic_not_promotion_evidence",
        "classification": classification,
        "first_output_owning_divergence": {
            "stage": "post-loop-complex-normalization",
            "left_operand_bits": complex_bits(post_phasor),
            "right_operand_bits": complex_bits(normalization),
            "casa_wide_intermediate_result_bits": complex_bits(
                quotients["wide_intermediate"]
            ),
            "casa_rs_contractible_float_result_bits": complex_bits(
                quotients["fused_float32_numerator"]
            ),
        },
        "validation": validation,
        "hashes": {
            "ordered_coefficient_grid_stream_sha256": stream_hash.hexdigest(),
            "product_accumulator_prefix_sha256": prefix_hash.hexdigest(),
        },
        "tap_arithmetic": {
            "complex_product_contract": (
                "four float32 products rounded by explicit fma-with-zero, "
                "then one float32 add or subtract"
            ),
            "accumulator_contract": ("one ordered float32 complex addition per tap"),
            "accumulator": encoded(accumulator),
            "post_phasor": encoded(post_phasor),
        },
        "division_variants": {
            name: {
                "prediction": encoded(value),
                "matches": matches[name],
            }
            for name, value in quotients.items()
        },
        "observed_targets": {
            "casa_tt0_raw_bits": casa_bits,
            "casa_rs_tt0_bits": casars_bits,
        },
        "prohibited_work": {
            "casa": "not-run",
            "measurement_set": "not-read",
            "prediction": "not-run",
            "residual_grid": "not-run",
            "fft": "not-run-by-analyzer",
            "image_products": "not-formed",
            "clean": "not-run",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--casa-prefix-trace", required=True, type=Path)
    parser.add_argument("--replay-inputs", required=True, type=Path)
    parser.add_argument("--term-comparison", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    trace = parse_casa_trace(args.casa_prefix_trace)
    replay_inputs = json.loads(args.replay_inputs.read_text(encoding="utf-8"))
    term_comparison = json.loads(args.term_comparison.read_text(encoding="utf-8"))
    result = replay(
        trace=trace,
        replay_inputs=replay_inputs,
        term_comparison=term_comparison,
    )
    result["inputs"] = {
        "casa_prefix_trace": str(args.casa_prefix_trace),
        "casa_prefix_trace_sha256": sha256_file(args.casa_prefix_trace),
        "replay_inputs": str(args.replay_inputs),
        "replay_inputs_sha256": sha256_file(args.replay_inputs),
        "term_comparison": str(args.term_comparison),
        "term_comparison_sha256": sha256_file(args.term_comparison),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
