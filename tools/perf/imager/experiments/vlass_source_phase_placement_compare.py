#!/usr/bin/env python3
"""Test the missing post-degrid VLASS source phase from frozen artifacts.

This analyzer consumes the frozen CASA term oracle, the frozen casa-rs
CPU-wide prediction sidecar, and the frozen source-order phasor trace.  It
does not read the MeasurementSet, invoke CASA or Metal, grid residuals, form
images, or run CLEAN.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import resource
import struct
import time
from pathlib import Path
from typing import Any

import numpy as np

import vlass_casa_mtmfs_term_degrid_compare as term_compare
import vlass_prediction_boundary_hash_compare as boundary
import vlass_prediction_sidecar_compare as sidecar


EXPECTED_TERM_SCHEMA = "casa-rs-vlass-term-separated-prediction-comparison-v1"
EXPECTED_WIDE_HOST_SCHEMA = "casa-rs-vlass-frozen-model-prediction-sidecar-host-v1"
EXPECTED_SAMPLE_COUNT = 98_239
EXPECTED_ROLE_COUNT = EXPECTED_SAMPLE_COUNT * 2
EXPECTED_TERM_COUNT = EXPECTED_ROLE_COUNT * 2


def phase_pairs(
    values: np.ndarray,
    source_trace: dict[str, Any],
) -> np.ndarray:
    """Apply the recorded source phasor with CASA Complex<Float> ordering."""

    values = np.asarray(values, dtype=np.complex64)
    samples = source_trace.get("samples")
    if values.shape != (len(samples), 2):
        raise RuntimeError("phase input does not match the source trace")
    output = np.empty_like(values)
    for ordinal, sample in enumerate(samples):
        phase_re = boundary.f32_from_bits(int(sample["phase_re_bits"]))
        phase_im = boundary.f32_from_bits(int(sample["phase_im_bits"]))
        for role in range(2):
            output[ordinal, role] = boundary.multiply_f32_complex(
                values[ordinal, role],
                phase_re,
                phase_im,
            )
    return output


def add_pairs(left: np.ndarray, right: np.ndarray) -> np.ndarray:
    """Add complex pairs with separate binary32 component operations."""

    left = np.asarray(left, dtype=np.complex64)
    right = np.asarray(right, dtype=np.complex64)
    if left.shape != right.shape:
        raise RuntimeError("complex-add inputs differ in shape")
    real = np.asarray(
        np.asarray(left.real, dtype=np.float32)
        + np.asarray(right.real, dtype=np.float32),
        dtype=np.float32,
    )
    imag = np.asarray(
        np.asarray(left.imag, dtype=np.float32)
        + np.asarray(right.imag, dtype=np.float32),
        dtype=np.float32,
    )
    return np.asarray(real + 1j * imag, dtype=np.complex64)


def subtract_pairs(left: np.ndarray, right: np.ndarray) -> np.ndarray:
    """Subtract complex pairs with separate binary32 component operations."""

    left = np.asarray(left, dtype=np.complex64)
    right = np.asarray(right, dtype=np.complex64)
    if left.shape != right.shape:
        raise RuntimeError("complex-subtract inputs differ in shape")
    real = np.asarray(
        np.asarray(left.real, dtype=np.float32)
        - np.asarray(right.real, dtype=np.float32),
        dtype=np.float32,
    )
    imag = np.asarray(
        np.asarray(left.imag, dtype=np.float32)
        - np.asarray(right.imag, dtype=np.float32),
        dtype=np.float32,
    )
    return np.asarray(real + 1j * imag, dtype=np.complex64)


def source_key_hash(
    source_trace: dict[str, Any],
    selected_casa: np.ndarray,
    audit: np.ndarray,
) -> str:
    """Bind source order, CASA selection, roles, Muellers, and frequencies."""

    samples = source_trace.get("samples")
    if len(samples) != selected_casa.size or selected_casa.size != audit.size:
        raise RuntimeError("source-key inputs differ in extent")
    digest = hashlib.sha256()
    for ordinal, sample in enumerate(samples):
        if int(sample["source_ordinal"]) != ordinal:
            raise RuntimeError(f"source ordinal differs at position {ordinal}")
        if int(selected_casa["spw_id"][ordinal]) != int(sample["spw_id"]):
            raise RuntimeError(f"source SPW differs at position {ordinal}")
        if int(selected_casa["channel"][ordinal]) != int(sample["channel"]):
            raise RuntimeError(f"source channel differs at position {ordinal}")
        frequency_bits = np.float64(sample["frequency_hz"]).view(np.uint64)
        if selected_casa["frequency_hz"][ordinal].view(np.uint64) != frequency_bits:
            raise RuntimeError(f"source frequency differs at position {ordinal}")
        muellers = (
            int(audit["first_imaging_mueller"][ordinal]),
            int(audit["second_imaging_mueller"][ordinal]),
        )
        if set(muellers) != {0, 15}:
            raise RuntimeError(
                f"source {ordinal} has Mueller pair {muellers}, expected 0 and 15"
            )
        # The audit's first/second term and observed slots remain RR/LL.
        # Mueller order only controls how the two returned residual slots are
        # routed to the replay consumer.
        for role, mueller in enumerate(muellers):
            digest.update(
                struct.pack(
                    "<IQIIIQII",
                    ordinal,
                    int(sample["row_id"]),
                    int(sample["ddid"]),
                    int(sample["spw_id"]),
                    int(sample["channel"]),
                    int(frequency_bits),
                    role,
                    mueller,
                )
            )
    return digest.hexdigest()


def classify(
    *,
    instrumentation_valid: bool,
    raw_term_mismatches: int,
    aligned_term_mismatches: int,
    casa_power_combined_exact: bool,
    casa_power_residual_exact: bool,
    rust_power_combined_exact: bool,
    rust_power_residual_exact: bool,
) -> str:
    if not instrumentation_valid:
        return "invalid-instrumentation"
    if raw_term_mismatches:
        return "unphased-raw-terms-still-differ"
    if aligned_term_mismatches:
        return "raw-terms-exact-postphase-differs"
    casa_closes = casa_power_combined_exact and casa_power_residual_exact
    rust_closes = rust_power_combined_exact and rust_power_residual_exact
    if casa_closes and not rust_closes:
        return "terms-exact-casa-power-closes-combined"
    if casa_closes and rust_closes:
        return "full-phase-and-current-power-closure"
    return "terms-exact-casa-power-still-differs"


def first_difference(
    *,
    source_trace: dict[str, Any],
    candidates: list[tuple[str, np.ndarray, np.ndarray]],
) -> dict[str, Any] | None:
    for boundary_name, actual, expected in candidates:
        mismatch = sidecar.first_pair_mismatch(actual, expected)
        if mismatch is None:
            continue
        ordinal, role = mismatch
        return {
            "boundary": boundary_name,
            "source": sidecar.source_context(source_trace, ordinal, role),
            "actual_bits": sidecar.bit_values(actual, ordinal, role),
            "expected_bits": sidecar.bit_values(expected, ordinal, role),
        }
    return None


def analyze(
    *,
    term_receipt: dict[str, Any],
    wide_host: dict[str, Any],
    audit: np.ndarray,
    casa_records: np.ndarray,
    casa_trace: dict[str, np.ndarray],
    source_trace: dict[str, Any],
    frozen_prediction_receipt: dict[str, Any],
) -> dict[str, Any]:
    started = time.perf_counter()
    selected_casa, row_identity = term_compare.select_source_records(
        casa_records,
        source_trace,
        casa_trace,
    )
    key_hash = source_key_hash(source_trace, selected_casa, audit)

    casa_tt0_raw = term_compare.complex_boundary(selected_casa, "tt0")
    casa_tt1_raw = term_compare.complex_boundary(selected_casa, "tt1_raw")
    casa_tt1_scaled_raw = term_compare.complex_boundary(
        selected_casa,
        "tt1_scaled",
    )
    casa_combined_raw = term_compare.complex_boundary(selected_casa, "combined")
    casars_tt0_raw = sidecar.complex_pair(
        audit,
        "first_model_term0",
        "second_model_term0",
    )
    casars_tt1_raw = sidecar.complex_pair(
        audit,
        "first_model_term1",
        "second_model_term1",
    )
    observed = sidecar.complex_pair(
        audit,
        "first_observed",
        "second_observed",
    )
    casa_powers = np.asarray(selected_casa["taylor_power1"], dtype=np.float32)
    rust_powers = np.asarray(audit["taylor_power1"], dtype=np.float32)

    phase_started = time.perf_counter()
    casa_tt0_aligned = phase_pairs(casa_tt0_raw, source_trace)
    casa_tt1_aligned = phase_pairs(casa_tt1_raw, source_trace)
    casa_tt1_scaled_aligned = phase_pairs(casa_tt1_scaled_raw, source_trace)
    casa_combined_aligned = phase_pairs(casa_combined_raw, source_trace)
    candidate_tt0_aligned = phase_pairs(casars_tt0_raw, source_trace)
    candidate_tt1_aligned = phase_pairs(casars_tt1_raw, source_trace)
    phase_elapsed = time.perf_counter() - phase_started

    counterfactual_started = time.perf_counter()
    casa_power_scaled = term_compare.separately_scale_pairs(
        candidate_tt1_aligned,
        casa_powers,
    )
    rust_power_scaled = term_compare.separately_scale_pairs(
        candidate_tt1_aligned,
        rust_powers,
    )
    casa_power_combined = add_pairs(candidate_tt0_aligned, casa_power_scaled)
    rust_power_combined = add_pairs(candidate_tt0_aligned, rust_power_scaled)
    casa_power_residual = subtract_pairs(observed, casa_power_combined)
    rust_power_residual = subtract_pairs(observed, rust_power_combined)
    frozen_residual = subtract_pairs(observed, casa_combined_aligned)
    counterfactual_elapsed = time.perf_counter() - counterfactual_started

    hashes = {
        "source_key_sha256": key_hash,
        "casa_tt0_raw_sha256": boundary.hash_parallel_hands(casa_tt0_raw),
        "casa_tt1_raw_sha256": boundary.hash_parallel_hands(casa_tt1_raw),
        "casa_tt0_aligned_sha256": boundary.hash_parallel_hands(casa_tt0_aligned),
        "casa_tt1_aligned_sha256": boundary.hash_parallel_hands(casa_tt1_aligned),
        "casa_tt1_scaled_aligned_sha256": boundary.hash_parallel_hands(
            casa_tt1_scaled_aligned
        ),
        "casa_combined_aligned_sha256": boundary.hash_parallel_hands(
            casa_combined_aligned
        ),
        "casars_tt0_raw_sha256": boundary.hash_parallel_hands(casars_tt0_raw),
        "casars_tt1_raw_sha256": boundary.hash_parallel_hands(casars_tt1_raw),
        "candidate_tt0_aligned_sha256": boundary.hash_parallel_hands(
            candidate_tt0_aligned
        ),
        "candidate_tt1_aligned_sha256": boundary.hash_parallel_hands(
            candidate_tt1_aligned
        ),
        "casa_power_scaled_tt1_sha256": boundary.hash_parallel_hands(casa_power_scaled),
        "rust_power_scaled_tt1_sha256": boundary.hash_parallel_hands(rust_power_scaled),
        "casa_power_combined_sha256": boundary.hash_parallel_hands(casa_power_combined),
        "rust_power_combined_sha256": boundary.hash_parallel_hands(rust_power_combined),
        "casa_power_residual_sha256": boundary.hash_parallel_hands(casa_power_residual),
        "rust_power_residual_sha256": boundary.hash_parallel_hands(rust_power_residual),
        "frozen_casa_residual_sha256": boundary.hash_parallel_hands(frozen_residual),
    }
    expected_term_hashes = term_receipt["hashes"]
    expected_prediction_hashes = frozen_prediction_receipt["hashes"]
    instrumentation_valid = all(
        (
            term_receipt.get("schema") == EXPECTED_TERM_SCHEMA,
            bool(term_receipt.get("instrumentation_valid")),
            wide_host.get("schema") == EXPECTED_WIDE_HOST_SCHEMA,
            audit.size == selected_casa.size == EXPECTED_SAMPLE_COUNT,
            np.array_equal(
                audit["sample_ordinal"],
                np.arange(audit.size, dtype=np.uint32),
            ),
            np.all(audit["written_generation"] == int(wide_host["generation"])),
            int(wide_host["audit"]["record_size"]) == sidecar.AUDIT_DTYPE.itemsize,
            int(wide_host["audit"]["unexpected_generation_count"]) == 0,
            int(wide_host["audit"]["unexpected_ordinal_count"]) == 0,
            int(wide_host["audit"]["nonfinite_count"]) == 0,
            hashes["casa_tt0_raw_sha256"]
            == expected_term_hashes["casa_tt0_raw_sha256"],
            hashes["casa_tt1_raw_sha256"]
            == expected_term_hashes["casa_tt1_raw_sha256"],
            hashes["casa_tt0_aligned_sha256"]
            == expected_term_hashes["casa_tt0_phase_rotated_sha256"],
            hashes["casa_tt1_aligned_sha256"]
            == expected_term_hashes["casa_tt1_phase_rotated_sha256"],
            hashes["casa_tt1_scaled_aligned_sha256"]
            == expected_term_hashes["casa_tt1_scaled_phase_rotated_sha256"],
            hashes["casa_combined_aligned_sha256"]
            == expected_term_hashes["casa_combined_phase_rotated_sha256"],
            hashes["casa_combined_aligned_sha256"]
            == expected_prediction_hashes["casa_phase_rotated_model_data_sha256"],
            hashes["frozen_casa_residual_sha256"]
            == expected_prediction_hashes["casa_derived_residual_sha256"],
        )
    )

    mismatch_counts = {
        "raw_tt0": term_compare.mismatch_count(casars_tt0_raw, casa_tt0_raw),
        "raw_tt1": term_compare.mismatch_count(casars_tt1_raw, casa_tt1_raw),
        "aligned_tt0": term_compare.mismatch_count(
            candidate_tt0_aligned,
            casa_tt0_aligned,
        ),
        "aligned_tt1": term_compare.mismatch_count(
            candidate_tt1_aligned,
            casa_tt1_aligned,
        ),
        "casa_power_scaled_tt1": term_compare.mismatch_count(
            casa_power_scaled,
            casa_tt1_scaled_aligned,
        ),
        "casa_power_combined": term_compare.mismatch_count(
            casa_power_combined,
            casa_combined_aligned,
        ),
        "casa_power_residual": term_compare.mismatch_count(
            casa_power_residual,
            frozen_residual,
        ),
        "rust_power_scaled_tt1": term_compare.mismatch_count(
            rust_power_scaled,
            casa_tt1_scaled_aligned,
        ),
        "rust_power_combined": term_compare.mismatch_count(
            rust_power_combined,
            casa_combined_aligned,
        ),
        "rust_power_residual": term_compare.mismatch_count(
            rust_power_residual,
            frozen_residual,
        ),
    }
    raw_term_mismatches = mismatch_counts["raw_tt0"] + mismatch_counts["raw_tt1"]
    aligned_term_mismatches = (
        mismatch_counts["aligned_tt0"] + mismatch_counts["aligned_tt1"]
    )
    classification = classify(
        instrumentation_valid=instrumentation_valid,
        raw_term_mismatches=raw_term_mismatches,
        aligned_term_mismatches=aligned_term_mismatches,
        casa_power_combined_exact=mismatch_counts["casa_power_combined"] == 0,
        casa_power_residual_exact=mismatch_counts["casa_power_residual"] == 0,
        rust_power_combined_exact=mismatch_counts["rust_power_combined"] == 0,
        rust_power_residual_exact=mismatch_counts["rust_power_residual"] == 0,
    )
    first = first_difference(
        source_trace=source_trace,
        candidates=[
            ("raw_tt0", casars_tt0_raw, casa_tt0_raw),
            ("raw_tt1", casars_tt1_raw, casa_tt1_raw),
            ("aligned_tt0", candidate_tt0_aligned, casa_tt0_aligned),
            ("aligned_tt1", candidate_tt1_aligned, casa_tt1_aligned),
            ("casa_power_scaled_tt1", casa_power_scaled, casa_tt1_scaled_aligned),
            ("casa_power_combined", casa_power_combined, casa_combined_aligned),
            ("rust_power_combined", rust_power_combined, casa_combined_aligned),
        ],
    )

    return {
        "schema": "casa-rs-vlass-aw-source-phase-placement-replay-v1",
        "role": "offline-correctness-certificate-not-performance-evidence",
        "instrumentation_valid": instrumentation_valid,
        "classification": classification,
        "source_count": int(audit.size),
        "role_count": int(audit.size * 2),
        "term_count": int(audit.size * 4),
        "source_key_contract": (
            "sha256-le-source-ordinal-row-ddid-spw-channel-frequency-bits-role-mueller"
        ),
        "row_identity": row_identity,
        "hashes": hashes,
        "mismatch_counts": mismatch_counts,
        "power_mismatch_count": int(
            np.count_nonzero(casa_powers.view(np.uint32) != rust_powers.view(np.uint32))
        ),
        "first_mismatch": first,
        "timings_ms": {
            "post_degrid_phase_multiplication": phase_elapsed * 1_000.0,
            "taylor_scale_add_and_residual": counterfactual_elapsed * 1_000.0,
            "total_analyzer": (time.perf_counter() - started) * 1_000.0,
        },
        "peak_resident_bytes": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        "operation_counts": {
            "post_degrid_complex32_phase_multiplications": EXPECTED_TERM_COUNT,
            "wide_divisions_reused_from_frozen_sidecar": EXPECTED_TERM_COUNT,
            "tap_visits": 0,
        },
        "prohibited_work": {
            "casa": "not-run",
            "measurement_set": "not-read",
            "metal": "not-run",
            "prediction": "not-run",
            "residual_grid": "not-run",
            "fft": "not-run",
            "image_products": "not-formed",
            "clean": "not-run",
        },
    }


def checked_input(path: Path, expected_sha256: str) -> None:
    actual = boundary.sha256_file(path)
    if actual != expected_sha256:
        raise RuntimeError(f"frozen input hash differs for {path}: {actual}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--term-comparison", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite receipt: {args.output}")

    term_receipt = json.loads(args.term_comparison.read_text(encoding="utf-8"))
    if term_receipt.get("schema") != EXPECTED_TERM_SCHEMA:
        raise RuntimeError("unexpected term-comparison schema")
    inputs = term_receipt["inputs"]
    for name in (
        "host_receipt",
        "binary",
        "casa_npz",
        "casars_source_trace",
        "casars_sidecar_host",
        "casars_sidecar_comparison",
    ):
        checked_input(Path(inputs[name]), str(inputs[f"{name}_sha256"]))

    casa_host = json.loads(Path(inputs["host_receipt"]).read_text(encoding="utf-8"))
    binary_path = Path(inputs["binary"])
    if term_compare.fnv1a64_file(binary_path) != int(casa_host["binary_fnv1a64"]):
        raise RuntimeError("CASA term binary FNV-1a differs from its host receipt")
    casa_records = np.fromfile(binary_path, dtype=term_compare.CASA_DTYPE)
    if casa_records.nbytes != int(casa_host["binary_bytes"]):
        raise RuntimeError("CASA term binary byte length differs from its host receipt")
    with np.load(inputs["casa_npz"]) as loaded:
        casa_trace = {name: np.asarray(loaded[name]) for name in loaded.files}
    source_trace = json.loads(
        Path(inputs["casars_source_trace"]).read_text(encoding="utf-8")
    )
    wide_host = json.loads(
        Path(inputs["casars_sidecar_host"]).read_text(encoding="utf-8")
    )
    audit_path = Path(wide_host["audit"]["path"])
    checked_input(audit_path, str(wide_host["audit"]["sha256"]))
    audit = np.fromfile(audit_path, dtype=sidecar.AUDIT_DTYPE)
    frozen_prediction_receipt = json.loads(
        Path(inputs["casars_sidecar_comparison"]).read_text(encoding="utf-8")
    )

    result = analyze(
        term_receipt=term_receipt,
        wide_host=wide_host,
        audit=audit,
        casa_records=casa_records,
        casa_trace=casa_trace,
        source_trace=source_trace,
        frozen_prediction_receipt=frozen_prediction_receipt,
    )
    result["inputs"] = {
        "term_comparison": str(args.term_comparison),
        "term_comparison_sha256": boundary.sha256_file(args.term_comparison),
        "analyzer": str(Path(__file__).resolve()),
        "analyzer_sha256": boundary.sha256_file(Path(__file__).resolve()),
        **{
            name: str(inputs[name])
            for name in (
                "host_receipt",
                "binary",
                "casa_npz",
                "casars_source_trace",
                "casars_sidecar_host",
                "casars_sidecar_comparison",
            )
        },
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
