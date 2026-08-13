#!/usr/bin/env python3
"""Classify the frozen VLASS prediction boundary from one Metal sidecar.

The sidecar observes locals from the production global-AW prediction kernel.
This comparator binds those records to the already-frozen CASA MODEL_DATA and
source-order artifacts.  A CASA mismatch is a valid scientific result; broken
instrumentation or changed Phase-B hashes make the receipt invalid.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

import vlass_prediction_boundary_hash_compare as boundary


AUDIT_DTYPE = np.dtype(
    [
        ("sample_ordinal", "<u4"),
        ("written_generation", "<u4"),
        ("taylor_power0", "<f4"),
        ("taylor_power1", "<f4"),
        ("first_imaging_mueller", "<u4"),
        ("second_imaging_mueller", "<u4"),
        ("first_model_term0_re", "<f4"),
        ("first_model_term0_im", "<f4"),
        ("first_model_term1_re", "<f4"),
        ("first_model_term1_im", "<f4"),
        ("first_combined_re", "<f4"),
        ("first_combined_im", "<f4"),
        ("first_observed_re", "<f4"),
        ("first_observed_im", "<f4"),
        ("first_local_residual_re", "<f4"),
        ("first_local_residual_im", "<f4"),
        ("second_model_term0_re", "<f4"),
        ("second_model_term0_im", "<f4"),
        ("second_model_term1_re", "<f4"),
        ("second_model_term1_im", "<f4"),
        ("second_combined_re", "<f4"),
        ("second_combined_im", "<f4"),
        ("second_observed_re", "<f4"),
        ("second_observed_im", "<f4"),
        ("second_local_residual_re", "<f4"),
        ("second_local_residual_im", "<f4"),
    ],
    align=False,
)

RESULT_DTYPE = np.dtype(
    [
        ("first_residual_re", "<f4"),
        ("first_residual_im", "<f4"),
        ("second_residual_re", "<f4"),
        ("second_residual_im", "<f4"),
    ],
    align=False,
)


def complex_pair(
    records: np.ndarray,
    first_prefix: str,
    second_prefix: str,
) -> np.ndarray:
    first = np.asarray(
        records[f"{first_prefix}_re"] + 1j * records[f"{first_prefix}_im"],
        dtype=np.complex64,
    )
    second = np.asarray(
        records[f"{second_prefix}_re"] + 1j * records[f"{second_prefix}_im"],
        dtype=np.complex64,
    )
    return np.stack((first, second), axis=1)


def literal_taylor_pair(records: np.ndarray) -> np.ndarray:
    output = np.empty((records.size, 2), dtype=np.complex64)
    power0 = np.asarray(records["taylor_power0"], dtype=np.float32)
    power1 = np.asarray(records["taylor_power1"], dtype=np.float32)
    for role, prefix in enumerate(("first", "second")):
        term0_re = np.asarray(records[f"{prefix}_model_term0_re"], dtype=np.float32)
        term0_im = np.asarray(records[f"{prefix}_model_term0_im"], dtype=np.float32)
        term1_re = np.asarray(records[f"{prefix}_model_term1_re"], dtype=np.float32)
        term1_im = np.asarray(records[f"{prefix}_model_term1_im"], dtype=np.float32)
        real = np.asarray(
            np.asarray(term0_re * power0, dtype=np.float32)
            + np.asarray(term1_re * power1, dtype=np.float32),
            dtype=np.float32,
        )
        imag = np.asarray(
            np.asarray(term0_im * power0, dtype=np.float32)
            + np.asarray(term1_im * power1, dtype=np.float32),
            dtype=np.float32,
        )
        output[:, role] = np.asarray(real + 1j * imag, dtype=np.complex64)
    return output


def canonical_returned_residuals(
    audit: np.ndarray,
    results: np.ndarray,
) -> np.ndarray:
    if audit.size != results.size:
        raise RuntimeError("audit and result record counts differ")
    returned = complex_pair(results, "first_residual", "second_residual")
    canonical = np.empty_like(returned)
    for ordinal in range(audit.size):
        first_mueller = int(audit["first_imaging_mueller"][ordinal])
        second_mueller = int(audit["second_imaging_mueller"][ordinal])
        if {first_mueller, second_mueller} != {0, 15}:
            raise RuntimeError(
                f"source {ordinal} has Mueller pair "
                f"{first_mueller},{second_mueller}, expected 0,15"
            )
        canonical[ordinal, 0 if first_mueller == 0 else 1] = returned[ordinal, 0]
        canonical[ordinal, 0 if second_mueller == 0 else 1] = returned[ordinal, 1]
    return canonical


def first_pair_mismatch(actual: np.ndarray, expected: np.ndarray) -> tuple[int, int] | None:
    if actual.shape != expected.shape:
        raise RuntimeError(f"pair shapes differ: {actual.shape} != {expected.shape}")
    actual_bits = (
        actual.view(np.float32).reshape(actual.shape + (2,)).view(np.uint32)
    )
    expected_bits = (
        expected.view(np.float32).reshape(expected.shape + (2,)).view(np.uint32)
    )
    mismatch = np.any(actual_bits != expected_bits, axis=2)
    indices = np.argwhere(mismatch)
    if indices.size == 0:
        return None
    ordinal, role = indices[0]
    return int(ordinal), int(role)


def raw_casa_model_pairs(
    trace: dict[str, np.ndarray],
    source_trace: dict,
) -> np.ndarray:
    model = np.asarray(trace["model_data"], dtype=np.complex64)
    row_ids = np.asarray(trace["row_id"], dtype=np.int64)
    row_by_id = {int(row_id): index for index, row_id in enumerate(row_ids)}
    output = np.empty((len(source_trace["samples"]), 2), dtype=np.complex64)
    for ordinal, sample in enumerate(source_trace["samples"]):
        row = row_by_id[int(sample["row_id"])]
        channel = int(sample["channel"])
        output[ordinal, 0] = model[row, boundary.PARALLEL_HANDS[0], channel]
        output[ordinal, 1] = model[row, boundary.PARALLEL_HANDS[1], channel]
    return output


def bit_values(pair: np.ndarray, ordinal: int, role: int) -> list[int]:
    value = np.asarray([pair[ordinal, role]], dtype=np.complex64).view(np.uint32)
    return [int(value[0]), int(value[1])]


def source_context(source_trace: dict, ordinal: int, role: int) -> dict[str, object]:
    samples = source_trace["samples"]
    sample = dict(samples[ordinal])
    sample["role"] = "rr" if role == 0 else "ll"
    return {
        "previous": dict(samples[ordinal - 1]) if ordinal > 0 else None,
        "current": sample,
        "next": dict(samples[ordinal + 1]) if ordinal + 1 < len(samples) else None,
    }


def analyze(
    *,
    host: dict,
    audit: np.ndarray,
    results: np.ndarray,
    trace: dict[str, np.ndarray],
    source_trace: dict,
    phase_b: dict[str, str],
    boundary_receipt: dict,
) -> dict[str, object]:
    observed, casa_prediction, census = boundary.source_trace_parallel_hands(
        trace,
        source_trace,
    )
    raw_casa_prediction = raw_casa_model_pairs(trace, source_trace)
    production_combined = complex_pair(
        audit,
        "first_combined",
        "second_combined",
    )
    literal_combined = literal_taylor_pair(audit)
    captured_observed = complex_pair(
        audit,
        "first_observed",
        "second_observed",
    )
    captured_local_residual = complex_pair(
        audit,
        "first_local_residual",
        "second_local_residual",
    )
    returned_stored = complex_pair(results, "first_residual", "second_residual")
    returned_canonical = canonical_returned_residuals(audit, results)
    literal_residual = np.asarray(
        captured_observed - production_combined,
        dtype=np.complex64,
    )
    recovered_stored = np.asarray(
        captured_observed - returned_stored,
        dtype=np.complex64,
    )
    casa_residual, casa_recovered = boundary.casa_f32_residual_and_recovered_prediction(
        observed,
        casa_prediction,
    )

    hashes = {
        "captured_observed_sha256": boundary.hash_parallel_hands(captured_observed),
        "production_combined_sha256": boundary.hash_parallel_hands(production_combined),
        "literal_combined_sha256": boundary.hash_parallel_hands(literal_combined),
        "captured_local_residual_sha256": boundary.hash_parallel_hands(
            captured_local_residual
        ),
        "literal_residual_sha256": boundary.hash_parallel_hands(literal_residual),
        "returned_stored_residual_sha256": boundary.hash_parallel_hands(returned_stored),
        "returned_canonical_residual_sha256": boundary.hash_parallel_hands(
            returned_canonical
        ),
        "recovered_stored_prediction_sha256": boundary.hash_parallel_hands(
            recovered_stored
        ),
        "casa_raw_model_data_sha256": boundary.hash_parallel_hands(
            raw_casa_prediction
        ),
        "casa_phase_rotated_model_data_sha256": boundary.hash_parallel_hands(
            casa_prediction
        ),
        "casa_derived_residual_sha256": boundary.hash_parallel_hands(casa_residual),
        "casa_recovered_prediction_sha256": boundary.hash_parallel_hands(
            casa_recovered
        ),
    }
    boundary_hashes = boundary_receipt["hashes"]
    instrumentation_valid = all(
        (
            audit.size == int(host["sample_count"]) == int(phase_b["samples"]),
            results.size == audit.size,
            np.array_equal(
                audit["sample_ordinal"],
                np.arange(audit.size, dtype=np.uint32),
            ),
            np.all(audit["written_generation"] == int(host["generation"])),
            np.all(audit["taylor_power0"].view(np.uint32) == np.float32(1.0).view(np.uint32)),
            hashes["captured_observed_sha256"] == phase_b["observed_sha256"],
            hashes["returned_stored_residual_sha256"] == phase_b["residual_sha256"],
            hashes["recovered_stored_prediction_sha256"] == phase_b["predicted_sha256"],
            hashes["casa_phase_rotated_model_data_sha256"]
            == boundary_hashes["casa_model_data_sha256"],
            hashes["casa_derived_residual_sha256"]
            == boundary_hashes["casa_derived_residual_sha256"],
            hashes["casa_recovered_prediction_sha256"]
            == boundary_hashes["casa_recovered_prediction_sha256"],
            census["collapsed_visibility_bit_mismatches"] == 0,
            int(host["audit"]["record_size"]) == AUDIT_DTYPE.itemsize,
            int(host["result"]["record_size"]) == RESULT_DTYPE.itemsize,
            int(host["audit"]["unexpected_generation_count"]) == 0,
            int(host["audit"]["unexpected_ordinal_count"]) == 0,
            int(host["audit"]["nonfinite_count"]) == 0,
            int(host["integrity"]["local_result_mismatch_count"]) == 0,
        )
    )

    combination_mismatch = first_pair_mismatch(production_combined, literal_combined)
    casa_mismatch = first_pair_mismatch(production_combined, casa_prediction)
    raw_casa_mismatch = first_pair_mismatch(production_combined, raw_casa_prediction)
    subtraction_mismatch = first_pair_mismatch(
        captured_local_residual,
        literal_residual,
    )
    result_mismatch = first_pair_mismatch(
        returned_canonical,
        captured_local_residual,
    )
    if not instrumentation_valid:
        classification = "invalid-instrumentation"
        decisive = None
    elif combination_mismatch is not None:
        classification = (
            "taylor-combination-difference"
            if first_pair_mismatch(literal_combined, casa_prediction) is None
            else "taylor-combination-and-upstream-difference"
        )
        decisive = combination_mismatch
    elif casa_mismatch is not None and raw_casa_mismatch is None:
        classification = "phase-application-difference"
        decisive = casa_mismatch
    elif casa_mismatch is not None:
        classification = "term-degrid-or-folded-phase-difference"
        decisive = casa_mismatch
    elif subtraction_mismatch is not None:
        classification = "residual-subtraction-difference"
        decisive = subtraction_mismatch
    elif result_mismatch is not None:
        classification = "result-abi-or-readback-difference"
        decisive = result_mismatch
    elif (
        hashes["returned_canonical_residual_sha256"]
        != hashes["casa_derived_residual_sha256"]
    ):
        classification = "full-internal-boundary-exact-receipt-conflict"
        decisive = None
    else:
        classification = "prediction-boundary-exact"
        decisive = None

    first_mismatch = None
    if decisive is not None:
        ordinal, role = decisive
        first_mismatch = {
            "source": source_context(source_trace, ordinal, role),
            "production_combined_bits": bit_values(
                production_combined,
                ordinal,
                role,
            ),
            "literal_combined_bits": bit_values(literal_combined, ordinal, role),
            "casa_raw_model_data_bits": bit_values(
                raw_casa_prediction,
                ordinal,
                role,
            ),
            "casa_phase_rotated_model_data_bits": bit_values(
                casa_prediction,
                ordinal,
                role,
            ),
            "captured_observed_bits": bit_values(captured_observed, ordinal, role),
            "captured_local_residual_bits": bit_values(
                captured_local_residual,
                ordinal,
                role,
            ),
            "literal_residual_bits": bit_values(literal_residual, ordinal, role),
            "returned_canonical_residual_bits": bit_values(
                returned_canonical,
                ordinal,
                role,
            ),
            "model_term0_bits": bit_values(
                complex_pair(
                    audit,
                    "first_model_term0",
                    "second_model_term0",
                ),
                ordinal,
                role,
            ),
            "model_term1_bits": bit_values(
                complex_pair(
                    audit,
                    "first_model_term1",
                    "second_model_term1",
                ),
                ordinal,
                role,
            ),
            "taylor_power0_bits": int(
                audit["taylor_power0"][ordinal].view(np.uint32)
            ),
            "taylor_power1_bits": int(
                audit["taylor_power1"][ordinal].view(np.uint32)
            ),
            "mueller_pair": [
                int(audit["first_imaging_mueller"][ordinal]),
                int(audit["second_imaging_mueller"][ordinal]),
            ],
        }
    return {
        "schema": "casa-rs-vlass-frozen-model-prediction-sidecar-comparison-v1",
        "role": "bounded_prediction_only_correctness_diagnostic_not_performance_evidence",
        "sample_count": int(audit.size),
        "instrumentation_valid": instrumentation_valid,
        "classification": classification,
        "hashes": hashes,
        "first_boundaries": {
            "taylor_combination": combination_mismatch,
            "casa_phase_rotated_prediction": casa_mismatch,
            "casa_raw_prediction": raw_casa_mismatch,
            "residual_subtraction": subtraction_mismatch,
            "result_abi_or_readback": result_mismatch,
        },
        "first_mismatch": first_mismatch,
        "mueller_order": {
            "natural_0_15": int(
                np.count_nonzero(
                    (audit["first_imaging_mueller"] == 0)
                    & (audit["second_imaging_mueller"] == 15)
                )
            ),
            "swapped_15_0": int(
                np.count_nonzero(
                    (audit["first_imaging_mueller"] == 15)
                    & (audit["second_imaging_mueller"] == 0)
                )
            ),
        },
        "phase_application_location": host["phase_application_location"],
        "prohibited_post_prediction_stages": host[
            "prohibited_post_prediction_stages"
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host-receipt", required=True, type=Path)
    parser.add_argument("--casa-npz", required=True, type=Path)
    parser.add_argument("--casars-source-trace", required=True, type=Path)
    parser.add_argument("--phase-b-log", required=True, type=Path)
    parser.add_argument("--boundary-receipt", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite comparison receipt: {args.output}")

    host = json.loads(args.host_receipt.read_text(encoding="utf-8"))
    audit_path = Path(host["audit"]["path"])
    result_path = Path(host["result"]["path"])
    for path, expected in [
        (audit_path, host["audit"]["sha256"]),
        (result_path, host["result"]["sha256"]),
    ]:
        actual = boundary.sha256_file(path)
        if actual != expected:
            raise RuntimeError(
                f"sidecar artifact hash differs for {path}: {actual} != {expected}"
            )
    audit = np.fromfile(audit_path, dtype=AUDIT_DTYPE)
    results = np.fromfile(result_path, dtype=RESULT_DTYPE)
    with np.load(args.casa_npz) as loaded:
        trace = {name: np.asarray(loaded[name]) for name in loaded.files}
    source_trace = json.loads(
        args.casars_source_trace.read_text(encoding="utf-8")
    )
    phase_b = boundary.parse_checkpoint(args.phase_b_log)
    boundary_receipt = json.loads(
        args.boundary_receipt.read_text(encoding="utf-8")
    )
    result = analyze(
        host=host,
        audit=audit,
        results=results,
        trace=trace,
        source_trace=source_trace,
        phase_b=phase_b,
        boundary_receipt=boundary_receipt,
    )
    result["inputs"] = {
        "host_receipt": str(args.host_receipt),
        "host_receipt_sha256": boundary.sha256_file(args.host_receipt),
        "casa_npz": str(args.casa_npz),
        "casa_npz_sha256": boundary.sha256_file(args.casa_npz),
        "casars_source_trace": str(args.casars_source_trace),
        "casars_source_trace_sha256": boundary.sha256_file(
            args.casars_source_trace
        ),
        "phase_b_log": str(args.phase_b_log),
        "phase_b_log_sha256": boundary.sha256_file(args.phase_b_log),
        "boundary_receipt": str(args.boundary_receipt),
        "boundary_receipt_sha256": boundary.sha256_file(args.boundary_receipt),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, sort_keys=True), flush=True)
    if not result["instrumentation_valid"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
