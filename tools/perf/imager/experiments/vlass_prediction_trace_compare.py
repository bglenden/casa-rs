#!/usr/bin/env python3
"""Join bounded casa-rs AW predictions to CASA MODEL_DATA by UVW/frequency."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np


LIGHT_SPEED_M_S = 299_792_458.0


def parse_rust_trace(path: Path) -> list[dict[str, str]]:
    records = []
    seen_lines = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("awproject_prediction_trace "):
            continue
        if line in seen_lines:
            continue
        seen_lines.add(line)
        record = {}
        for field in line.split()[1:]:
            key, value = field.split("=", 1)
            record[key] = value
        records.append(record)
    if not records:
        raise RuntimeError(f"no awproject_prediction_trace records in {path}")
    return records


def complex_metrics(candidate: np.ndarray, reference: np.ndarray) -> dict:
    difference = candidate - reference
    reference_l2 = float(np.linalg.norm(reference))
    candidate_l2 = float(np.linalg.norm(candidate))
    difference_l2 = float(np.linalg.norm(difference))
    denominator = np.vdot(reference, reference)
    best_scale = (
        np.vdot(reference, candidate) / denominator
        if denominator.real > 0.0
        else complex(math.nan, math.nan)
    )
    scaled_difference = candidate - best_scale * reference
    coherence = (
        float(abs(np.vdot(reference, candidate)) / (reference_l2 * candidate_l2))
        if reference_l2 > 0.0 and candidate_l2 > 0.0
        else math.nan
    )
    return {
        "candidate_l2": candidate_l2,
        "reference_l2": reference_l2,
        "difference_l2": difference_l2,
        "relative_l2": difference_l2 / reference_l2 if reference_l2 else math.nan,
        "coherence": coherence,
        "best_complex_scale": [float(best_scale.real), float(best_scale.imag)],
        "relative_l2_after_complex_scale": (
            float(np.linalg.norm(scaled_difference)) / reference_l2
            if reference_l2
            else math.nan
        ),
        "max_absolute_error": float(np.max(np.abs(difference))),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust-log", required=True, type=Path)
    parser.add_argument("--casa-npz", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    rust_records = parse_rust_trace(args.rust_log)
    with np.load(args.casa_npz) as casa:
        uvw_m = np.asarray(casa["uvw_m"], dtype=np.float64)
        frequencies_hz = np.asarray(casa["channel_frequency_hz"], dtype=np.float64)
        model_data = np.asarray(casa["model_data"], dtype=np.complex64)
        flags = np.asarray(casa["flag"], dtype=np.bool_)
        row_ids = np.asarray(casa["row_id"], dtype=np.int64)
        spw_ids = np.asarray(casa["spectral_window_id"], dtype=np.int32)

    rust_rr = []
    rust_ll = []
    casa_rr = []
    casa_ll = []
    matches = []
    for record in rust_records:
        frequency_hz = float(record["frequency_hz"])
        rust_uvw_m = np.asarray(
            [
                float(record["u_lambda"]) * LIGHT_SPEED_M_S / frequency_hz,
                float(record["v_lambda"]) * LIGHT_SPEED_M_S / frequency_hz,
                float(record["w_lambda"]) * LIGHT_SPEED_M_S / frequency_hz,
            ],
            dtype=np.float64,
        )
        uv_distances = np.linalg.norm(uvw_m - rust_uvw_m, axis=1)
        min_uv_distance = float(np.min(uv_distances))
        uv_candidates = np.flatnonzero(uv_distances <= min_uv_distance + 1.0e-6)
        best_row = -1
        best_channel = -1
        best_frequency_delta = math.inf
        for row in uv_candidates:
            channel = int(np.argmin(np.abs(frequencies_hz[row] - frequency_hz)))
            frequency_delta = abs(float(frequencies_hz[row, channel]) - frequency_hz)
            if frequency_delta < best_frequency_delta:
                best_row = int(row)
                best_channel = channel
                best_frequency_delta = frequency_delta
        if best_row < 0:
            raise RuntimeError(f"failed to match Rust trace index {record['index']}")
        rr = complex(
            float(record["rr_prediction_re"]),
            float(record["rr_prediction_im"]),
        )
        ll = complex(
            float(record["ll_prediction_re"]),
            float(record["ll_prediction_im"]),
        )
        casa_rr_value = complex(model_data[best_row, 0, best_channel])
        casa_ll_value = complex(model_data[best_row, 3, best_channel])
        rust_rr.append(rr)
        rust_ll.append(ll)
        casa_rr.append(casa_rr_value)
        casa_ll.append(casa_ll_value)
        matches.append(
            {
                "trace_index": int(record["index"]),
                "source_sample_index": int(record["sample_index"]),
                "ms_row_id": int(row_ids[best_row]),
                "spectral_window_id": int(spw_ids[best_row]),
                "channel": best_channel,
                "uvw_distance_m": min_uv_distance,
                "frequency_delta_hz": best_frequency_delta,
                "rr_flagged": bool(flags[best_row, 0, best_channel]),
                "ll_flagged": bool(flags[best_row, 3, best_channel]),
                "rust_rr": [rr.real, rr.imag],
                "casa_rr": [casa_rr_value.real, casa_rr_value.imag],
                "rust_ll": [ll.real, ll.imag],
                "casa_ll": [casa_ll_value.real, casa_ll_value.imag],
                "rr_absolute_error": abs(rr - casa_rr_value),
                "ll_absolute_error": abs(ll - casa_ll_value),
            }
        )

    rust_rr_array = np.asarray(rust_rr, dtype=np.complex128)
    rust_ll_array = np.asarray(rust_ll, dtype=np.complex128)
    casa_rr_array = np.asarray(casa_rr, dtype=np.complex128)
    casa_ll_array = np.asarray(casa_ll, dtype=np.complex128)
    result = {
        "kind": "vlass_awproject_prediction_trace_comparison",
        "role": "bounded_correctness_trace_not_performance_evidence",
        "rust_log": str(args.rust_log),
        "casa_npz": str(args.casa_npz),
        "matched_samples": len(matches),
        "max_uvw_distance_m": max(match["uvw_distance_m"] for match in matches),
        "max_frequency_delta_hz": max(
            match["frequency_delta_hz"] for match in matches
        ),
        "flagged_matches": sum(
            match["rr_flagged"] or match["ll_flagged"] for match in matches
        ),
        "rr": complex_metrics(rust_rr_array, casa_rr_array),
        "ll": complex_metrics(rust_ll_array, casa_ll_array),
        "rr_against_conjugated_casa": complex_metrics(
            rust_rr_array,
            np.conj(casa_rr_array),
        ),
        "ll_against_conjugated_casa": complex_metrics(
            rust_ll_array,
            np.conj(casa_ll_array),
        ),
        "rr_against_casa_ll": complex_metrics(rust_rr_array, casa_ll_array),
        "ll_against_casa_rr": complex_metrics(rust_ll_array, casa_rr_array),
        "matches": matches,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                key: value
                for key, value in result.items()
                if key != "matches"
            },
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
