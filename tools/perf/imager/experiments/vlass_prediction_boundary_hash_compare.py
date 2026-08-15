#!/usr/bin/env python3
"""Compare a frozen CASA MODEL_DATA stream with casa-rs final-state hashes.

The comparison first requires an exact observed-visibility hash match.  That
binds the selected sample set and source order before MODEL_DATA is allowed to
say anything about the prediction boundary.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
from pathlib import Path

import numpy as np


PARALLEL_HANDS = (0, 3)
CHECKPOINT_PREFIX = "awproject_frozen_final_state_visibilities "
CHECKPOINT_CONTRACT = (
    "sha256-le-sample-ordinal-rr-re-im-ll-re-im-f32-bits"
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_checkpoint(path: Path) -> dict[str, str]:
    records: list[dict[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        marker = line.find(CHECKPOINT_PREFIX)
        if marker < 0:
            continue
        fields: dict[str, str] = {}
        for field in line[marker + len(CHECKPOINT_PREFIX) :].split():
            if "=" not in field:
                continue
            key, value = field.split("=", 1)
            fields[key] = value
        if fields.get("contract") == CHECKPOINT_CONTRACT:
            records.append(fields)
    unique = {
        (
            record.get("samples"),
            record.get("observed_sha256"),
            record.get("predicted_sha256"),
            record.get("residual_sha256"),
        )
        for record in records
    }
    if len(unique) != 1:
        raise RuntimeError(
            f"expected one unique frozen-final-state checkpoint in {path}, "
            f"found {len(unique)}"
        )
    samples, observed, predicted, residual = unique.pop()
    if not all((samples, observed, predicted, residual)):
        raise RuntimeError(f"incomplete frozen-final-state checkpoint in {path}")
    return {
        "samples": samples,
        "observed_sha256": observed,
        "predicted_sha256": predicted,
        "residual_sha256": residual,
    }


def finite_complex(values: np.ndarray) -> np.ndarray:
    return np.isfinite(values.real) & np.isfinite(values.imag)


def selected_parallel_hands(
    trace: dict[str, np.ndarray],
) -> tuple[np.ndarray, np.ndarray, dict[str, object]]:
    observed = np.asarray(trace["observed_data"], dtype=np.complex64)
    model = np.asarray(trace["model_data"], dtype=np.complex64)
    flags = np.asarray(trace["flag"], dtype=np.bool_)
    weights = np.asarray(trace["weight"], dtype=np.float32)
    uv_selected = np.asarray(trace["uv_range_selected"], dtype=np.bool_)
    antenna1 = np.asarray(trace["antenna1"], dtype=np.int32)
    antenna2 = np.asarray(trace["antenna2"], dtype=np.int32)
    row_ids = np.asarray(trace["row_id"], dtype=np.int64)
    ddids = np.asarray(trace["data_description_id"], dtype=np.int32)
    if observed.shape != model.shape or observed.shape != flags.shape:
        raise RuntimeError(
            "observed_data, model_data, and flag arrays must have identical shapes"
        )
    if observed.ndim != 3 or observed.shape[1] <= max(PARALLEL_HANDS):
        raise RuntimeError(f"unsupported visibility shape: {observed.shape}")
    if weights.shape != observed.shape[:2]:
        raise RuntimeError(
            f"weight shape {weights.shape} does not match {observed.shape[:2]}"
        )
    row_count, _, channel_count = observed.shape
    if (
        uv_selected.shape != (row_count,)
        or antenna1.shape != (row_count,)
        or antenna2.shape != (row_count,)
        or row_ids.shape != (row_count,)
        or ddids.shape != (row_count,)
    ):
        raise RuntimeError("row-level trace arrays do not match visibility row count")

    # The production streaming frontend executes BTreeSet-ordered DDID plans.
    # Rows within each plan retain selected MAIN row order, and channels remain
    # the innermost source-order dimension.
    row_order = np.lexsort((row_ids, ddids))
    observed = observed[row_order]
    model = model[row_order]
    flags = flags[row_order]
    weights = weights[row_order]
    uv_selected = uv_selected[row_order]
    antenna1 = antenna1[row_order]
    antenna2 = antenna2[row_order]
    ddids = ddids[row_order]

    first = observed[:, PARALLEL_HANDS[0], :]
    second = observed[:, PARALLEL_HANDS[1], :]
    first_model = model[:, PARALLEL_HANDS[0], :]
    second_model = model[:, PARALLEL_HANDS[1], :]
    first_weight = weights[:, PARALLEL_HANDS[0]]
    second_weight = weights[:, PARALLEL_HANDS[1]]
    row_admitted = (
        uv_selected
        & (antenna1 != antenna2)
        & np.isfinite(first_weight)
        & (first_weight > 0.0)
        & np.isfinite(second_weight)
        & (second_weight > 0.0)
    )
    sample_admitted = (
        row_admitted[:, np.newaxis]
        & ~flags[:, PARALLEL_HANDS[0], :]
        & ~flags[:, PARALLEL_HANDS[1], :]
        & finite_complex(first)
        & finite_complex(second)
    )
    observed_pairs = np.stack((first[sample_admitted], second[sample_admitted]), axis=1)
    model_pairs = np.stack(
        (first_model[sample_admitted], second_model[sample_admitted]),
        axis=1,
    )
    census = {
        "rows": int(row_count),
        "channels_per_row": int(channel_count),
        "row_admitted": int(np.count_nonzero(row_admitted)),
        "samples_admitted": int(np.count_nonzero(sample_admitted)),
        "samples_rejected": int(sample_admitted.size - np.count_nonzero(sample_admitted)),
        "ddid_execution_order": [
            int(value) for value in np.unique(ddids, return_index=False)
        ],
    }
    return observed_pairs, model_pairs, census


def f32_from_bits(bits: int) -> np.float32:
    return np.asarray([bits], dtype=np.uint32).view(np.float32)[0]


def f32_bits(value: np.float32) -> int:
    return int(np.asarray([value], dtype=np.float32).view(np.uint32)[0])


def multiply_f32_complex(
    value: np.complex64,
    phase_re: np.float32,
    phase_im: np.float32,
) -> np.complex64:
    value_re = np.float32(value.real)
    value_im = np.float32(value.imag)
    real = np.float32(
        np.float32(value_re * phase_re) - np.float32(value_im * phase_im)
    )
    imag = np.float32(
        np.float32(value_re * phase_im) + np.float32(value_im * phase_re)
    )
    return np.complex64(complex(float(real), float(imag)))


def source_trace_parallel_hands(
    trace: dict[str, np.ndarray],
    source_trace: dict,
) -> tuple[np.ndarray, np.ndarray, dict[str, object]]:
    observed_data = np.asarray(trace["observed_data"], dtype=np.complex64)
    model_data = np.asarray(trace["model_data"], dtype=np.complex64)
    flags = np.asarray(trace["flag"], dtype=np.bool_)
    weights = np.asarray(trace["weight"], dtype=np.float32)
    uv_selected = np.asarray(trace["uv_range_selected"], dtype=np.bool_)
    antenna1 = np.asarray(trace["antenna1"], dtype=np.int32)
    antenna2 = np.asarray(trace["antenna2"], dtype=np.int32)
    row_ids = np.asarray(trace["row_id"], dtype=np.int64)
    ddids = np.asarray(trace["data_description_id"], dtype=np.int32)
    spws = np.asarray(trace["spectral_window_id"], dtype=np.int32)
    row_by_id = {int(row_id): index for index, row_id in enumerate(row_ids)}
    if len(row_by_id) != row_ids.size:
        raise RuntimeError("CASA trace contains duplicate MAIN row ids")

    source_samples = source_trace.get("samples")
    if not isinstance(source_samples, list) or not source_samples:
        raise RuntimeError("casa-rs source trace contains no samples")
    observed_pairs = np.empty((len(source_samples), 2), dtype=np.complex64)
    model_pairs = np.empty_like(observed_pairs)
    collapsed_mismatches = 0
    first_collapsed_mismatch = None
    for ordinal, sample in enumerate(source_samples):
        if sample.get("source_ordinal") != ordinal:
            raise RuntimeError(
                f"casa-rs source ordinal {sample.get('source_ordinal')} "
                f"does not match position {ordinal}"
            )
        row_id = int(sample["row_id"])
        channel = int(sample["channel"])
        try:
            row = row_by_id[row_id]
        except KeyError as error:
            raise RuntimeError(
                f"casa-rs source row {row_id} is absent from CASA trace"
            ) from error
        if (
            int(ddids[row]) != int(sample["ddid"])
            or int(spws[row]) != int(sample["spw_id"])
        ):
            raise RuntimeError(f"row/DDID/SPW identity differs at source {ordinal}")
        if not (
            bool(uv_selected[row])
            and antenna1[row] != antenna2[row]
            and not flags[row, PARALLEL_HANDS[0], channel]
            and not flags[row, PARALLEL_HANDS[1], channel]
            and np.isfinite(weights[row, PARALLEL_HANDS[0]])
            and weights[row, PARALLEL_HANDS[0]] > 0.0
            and np.isfinite(weights[row, PARALLEL_HANDS[1]])
            and weights[row, PARALLEL_HANDS[1]] > 0.0
        ):
            raise RuntimeError(f"CASA trace rejects casa-rs source {ordinal}")
        phase_re = f32_from_bits(int(sample["phase_re_bits"]))
        phase_im = f32_from_bits(int(sample["phase_im_bits"]))
        for hand_slot, correlation in enumerate(PARALLEL_HANDS):
            observed_pairs[ordinal, hand_slot] = multiply_f32_complex(
                observed_data[row, correlation, channel],
                phase_re,
                phase_im,
            )
            model_pairs[ordinal, hand_slot] = multiply_f32_complex(
                model_data[row, correlation, channel],
                phase_re,
                phase_im,
            )
        collapsed = np.complex64(
            np.complex64(observed_pairs[ordinal, 0] + observed_pairs[ordinal, 1])
            * np.float32(0.5)
        )
        collapsed_bits = (f32_bits(collapsed.real), f32_bits(collapsed.imag))
        expected_bits = (
            int(sample["collapsed_visibility_re_bits"]),
            int(sample["collapsed_visibility_im_bits"]),
        )
        if collapsed_bits != expected_bits:
            collapsed_mismatches += 1
            if first_collapsed_mismatch is None:
                first_collapsed_mismatch = {
                    "source_ordinal": ordinal,
                    "actual_bits": list(collapsed_bits),
                    "expected_bits": list(expected_bits),
                }
    census = {
        "rows": int(row_ids.size),
        "samples_admitted": len(source_samples),
        "collapsed_visibility_bit_mismatches": collapsed_mismatches,
        "first_collapsed_visibility_bit_mismatch": first_collapsed_mismatch,
    }
    return observed_pairs, model_pairs, census


def hash_parallel_hands(values: np.ndarray) -> str:
    values = np.asarray(values, dtype=np.complex64)
    if values.ndim != 2 or values.shape[1] != 2:
        raise RuntimeError(f"parallel-hand values must have shape (samples, 2): {values.shape}")
    digest = hashlib.sha256()
    for ordinal, pair in enumerate(values):
        digest.update(struct.pack("<Q", ordinal))
        for value in pair:
            digest.update(struct.pack("<f", float(value.real)))
            digest.update(struct.pack("<f", float(value.imag)))
    return digest.hexdigest()


def casa_f32_residual_and_recovered_prediction(
    observed: np.ndarray,
    prediction: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    observed = np.asarray(observed, dtype=np.complex64)
    prediction = np.asarray(prediction, dtype=np.complex64)
    residual = np.asarray(observed - prediction, dtype=np.complex64)
    recovered = np.asarray(observed - residual, dtype=np.complex64)
    return residual, recovered


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--casa-npz", required=True, type=Path)
    parser.add_argument("--casars-source-trace", required=True, type=Path)
    parser.add_argument("--phase-b-log", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite comparison receipt: {args.output}")
    checkpoint = parse_checkpoint(args.phase_b_log)
    with np.load(args.casa_npz) as loaded:
        trace = {name: np.asarray(loaded[name]) for name in loaded.files}
    source_trace = json.loads(args.casars_source_trace.read_text(encoding="utf-8"))
    observed, casa_prediction, census = source_trace_parallel_hands(
        trace,
        source_trace,
    )
    casa_residual, casa_recovered_prediction = (
        casa_f32_residual_and_recovered_prediction(observed, casa_prediction)
    )
    hashes = {
        "observed_sha256": hash_parallel_hands(observed),
        "casa_model_data_sha256": hash_parallel_hands(casa_prediction),
        "casa_derived_residual_sha256": hash_parallel_hands(casa_residual),
        "casa_recovered_prediction_sha256": hash_parallel_hands(
            casa_recovered_prediction
        ),
    }
    sample_count_matches = observed.shape[0] == int(checkpoint["samples"])
    observed_matches = hashes["observed_sha256"] == checkpoint["observed_sha256"]
    collapsed_matches = census["collapsed_visibility_bit_mismatches"] == 0
    comparison_valid = sample_count_matches and observed_matches and collapsed_matches
    result = {
        "kind": "vlass_frozen_prediction_boundary_hash_comparison",
        "role": "bounded_correctness_trace_not_performance_evidence",
        "casa_npz": str(args.casa_npz),
        "casa_npz_sha256": sha256_file(args.casa_npz),
        "casars_source_trace": str(args.casars_source_trace),
        "casars_source_trace_sha256": sha256_file(args.casars_source_trace),
        "phase_b_log": str(args.phase_b_log),
        "phase_b_log_sha256": sha256_file(args.phase_b_log),
        "contract": CHECKPOINT_CONTRACT,
        "census": census,
        "phase_b_checkpoint": checkpoint,
        "hashes": hashes,
        "sample_count_matches": sample_count_matches,
        "collapsed_visibility_matches": collapsed_matches,
        "observed_matches": observed_matches,
        "comparison_valid": comparison_valid,
        "recovered_prediction_matches": (
            comparison_valid
            and hashes["casa_recovered_prediction_sha256"]
            == checkpoint["predicted_sha256"]
        ),
        "derived_residual_matches": (
            comparison_valid
            and hashes["casa_derived_residual_sha256"]
            == checkpoint["residual_sha256"]
        ),
        "classification": (
            "prediction-boundary-match"
            if comparison_valid
            and hashes["casa_recovered_prediction_sha256"]
            == checkpoint["predicted_sha256"]
            else "prediction-boundary-difference"
            if comparison_valid
            else "invalid-source-order-or-admission"
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, sort_keys=True), flush=True)
    if not comparison_valid:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
