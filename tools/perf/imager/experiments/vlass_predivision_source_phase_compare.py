#!/usr/bin/env python3
"""Certify CASA's VLASS source phase before AW complex normalization.

This analyzer consumes only frozen four-SPW artifacts.  It does not read a
MeasurementSet, invoke CASA or Metal, grid, FFT, form products, or run CLEAN.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import resource
import struct
import time
from pathlib import Path
from typing import Any

import numpy as np

import vlass_casa_mtmfs_term_degrid_compare as term_compare
import vlass_prediction_boundary_hash_compare as boundary
import vlass_prediction_sidecar_compare as sidecar
import vlass_source_phase_placement_compare as phase_compare


EXPECTED_SOURCE_COUNT = 98_239
EXPECTED_ROLE_COUNT = EXPECTED_SOURCE_COUNT * 2
EXPECTED_TERM_COUNT = EXPECTED_ROLE_COUNT * 2
EXPECTED_CURRENT_MISMATCHES = {"tt0": 151, "tt1": 190}
SOURCE_1446_LIVE_NUMERATOR = [969_396_469, 3_196_932_265]
SOURCE_1446_LIVE_RESULT = [3_145_554_492, 3_197_138_881]


def _raw_dtype() -> np.dtype:
    fields: list[tuple[str, str]] = [
        ("sample_ordinal", "<u4"),
        ("written_generation", "<u4"),
        ("first_imaging_mueller", "<u4"),
        ("second_imaging_mueller", "<u4"),
    ]
    for role in ("first", "second"):
        for term in ("tt0", "tt1"):
            for value in ("numerator", "normalizer", "current"):
                fields.extend(
                    [
                        (f"{role}_{term}_{value}_re", "<f4"),
                        (f"{role}_{term}_{value}_im", "<f4"),
                    ]
                )
    return np.dtype(fields, align=False)


RAW_DTYPE = _raw_dtype()


def raw_pairs(records: np.ndarray, term: str, value: str) -> np.ndarray:
    """Return canonical RR/LL pairs from the frozen wide sidecar."""

    if term not in {"tt0", "tt1"} or value not in {
        "numerator",
        "normalizer",
        "current",
    }:
        raise RuntimeError(f"unsupported raw field {term}/{value}")
    output = np.empty((records.size, 2), dtype=np.complex64)
    for role, prefix in enumerate(("first", "second")):
        real = np.asarray(records[f"{prefix}_{term}_{value}_re"], dtype=np.float32)
        imag = np.asarray(records[f"{prefix}_{term}_{value}_im"], dtype=np.float32)
        output[:, role] = np.asarray(real + 1j * imag, dtype=np.complex64)
    return output


def predivision_phase_pairs(
    numerators: np.ndarray,
    source_trace: dict[str, Any],
) -> np.ndarray:
    """Apply CASA GridToData's ``nvalue *= conj(phasor)`` in Complex32."""

    numerators = np.asarray(numerators, dtype=np.complex64)
    samples = source_trace.get("samples")
    if numerators.shape != (len(samples), 2):
        raise RuntimeError("pre-division phase input does not match source trace")
    output = np.empty_like(numerators)
    for ordinal, sample in enumerate(samples):
        phase_re = boundary.f32_from_bits(int(sample["phase_re_bits"]))
        phase_im = boundary.f32_from_bits(int(sample["phase_im_bits"]))
        conjugate_im = np.float32(-phase_im)
        for role in range(2):
            output[ordinal, role] = boundary.multiply_f32_complex(
                numerators[ordinal, role],
                phase_re,
                conjugate_im,
            )
    return output


def wide_divide_one(
    numerator: np.complex64,
    stored_normalizer: np.complex64,
) -> np.complex64:
    """Replay the audited installed-CASA finite ``___divsc3`` graph."""

    a = float(np.float32(numerator.real))
    b = float(np.float32(numerator.imag))
    c = float(np.float32(stored_normalizer.real))
    # The compact sidecar stores the CF normalizer. GridToData divides by its
    # conjugate, so the helper's fourth argument has the opposite sign.
    d = -float(np.float32(stored_normalizer.imag))
    if not all(math.isfinite(value) for value in (a, b, c, d)):
        raise RuntimeError("wide division received a nonfinite operand")
    if c == 0.0 and d == 0.0:
        raise RuntimeError("wide division received a zero normalizer")
    denominator = math.fma(c, c, d * d)
    real_numerator = math.fma(a, c, b * d)
    imaginary_numerator = math.fma(b, c, -(a * d))
    return np.complex64(
        complex(
            float(np.float32(real_numerator / denominator)),
            float(np.float32(imaginary_numerator / denominator)),
        )
    )


def wide_divide_pairs(
    numerators: np.ndarray,
    normalizers: np.ndarray,
) -> np.ndarray:
    numerators = np.asarray(numerators, dtype=np.complex64)
    normalizers = np.asarray(normalizers, dtype=np.complex64)
    if numerators.shape != normalizers.shape:
        raise RuntimeError("wide-division inputs differ in shape")
    output = np.empty_like(numerators)
    output_flat = output.reshape(-1)
    for index, (numerator, normalizer) in enumerate(
        zip(numerators.reshape(-1), normalizers.reshape(-1), strict=True)
    ):
        output_flat[index] = wide_divide_one(numerator, normalizer)
    return output


def pair_bits(values: np.ndarray, ordinal: int, role: int) -> list[int]:
    return sidecar.bit_values(np.asarray(values, dtype=np.complex64), ordinal, role)


def casa_normalizer_bits(
    normalizers: np.ndarray,
    ordinal: int,
    role: int,
) -> list[int]:
    stored = pair_bits(normalizers, ordinal, role)
    return [stored[0], stored[1] ^ 0x8000_0000]


def _ordered_f32(bits: np.ndarray) -> np.ndarray:
    bits = np.asarray(bits, dtype=np.uint32)
    return np.where(
        (bits & np.uint32(0x8000_0000)) != 0,
        np.bitwise_not(bits),
        bits ^ np.uint32(0x8000_0000),
    ).astype(np.int64)


def comparison_stats(
    *,
    actual: np.ndarray,
    expected: np.ndarray,
    source_trace: dict[str, Any],
) -> dict[str, Any]:
    actual = np.asarray(actual, dtype=np.complex64)
    expected = np.asarray(expected, dtype=np.complex64)
    if actual.shape != expected.shape:
        raise RuntimeError("comparison pair shapes differ")
    actual_bits = actual.view(np.float32).reshape(actual.shape + (2,)).view(np.uint32)
    expected_bits = expected.view(np.float32).reshape(expected.shape + (2,)).view(np.uint32)
    mismatch = np.any(actual_bits != expected_bits, axis=2)
    mismatch_count = int(np.count_nonzero(mismatch))
    difference = np.abs(_ordered_f32(actual_bits) - _ordered_f32(expected_bits))
    first = sidecar.first_pair_mismatch(actual, expected)
    return {
        "bit_exact_count": int(actual.size - mismatch_count),
        "mismatch_count": mismatch_count,
        "max_component_ulp_distance": int(difference.max(initial=0)),
        "first_mismatch": (
            None
            if first is None
            else {
                "source": sidecar.source_context(source_trace, first[0], first[1]),
                "candidate_bits": pair_bits(actual, first[0], first[1]),
                "casa_bits": pair_bits(expected, first[0], first[1]),
            }
        ),
    }


def stream_hash(values: np.ndarray) -> str:
    return boundary.hash_parallel_hands(np.asarray(values, dtype=np.complex64))


def raw_identity_hash(records: np.ndarray) -> str:
    digest = hashlib.sha256()
    for record in records:
        digest.update(
            struct.pack(
                "<IIII",
                int(record["sample_ordinal"]),
                int(record["written_generation"]),
                int(record["first_imaging_mueller"]),
                int(record["second_imaging_mueller"]),
            )
        )
    return digest.hexdigest()


def classify(
    *,
    instrumentation_valid: bool,
    source_1446_exact: bool,
    tt0_mismatches: int,
    tt1_mismatches: int,
    current_counts_valid: bool,
) -> str:
    if not current_counts_valid:
        return "current-order-control-invalid"
    if not instrumentation_valid or not source_1446_exact:
        return "predivision-phase-does-not-close-source1446"
    if tt0_mismatches == 0 and tt1_mismatches == 0:
        return "predivision-phase-closes-all-terms"
    if tt0_mismatches == 0:
        return "predivision-phase-closes-tt0-only"
    if tt1_mismatches == 0:
        return "predivision-phase-closes-tt1-only"
    return "predivision-phase-improves-but-not-exact"


def analyze(
    *,
    term_receipt: dict[str, Any],
    phase_receipt: dict[str, Any],
    wide_sidecar_host: dict[str, Any],
    callsite_receipt: dict[str, Any],
    audit: np.ndarray,
    raw: np.ndarray,
    casa_records: np.ndarray,
    casa_trace: dict[str, np.ndarray],
    source_trace: dict[str, Any],
) -> dict[str, Any]:
    started = time.perf_counter()
    selected_casa, row_identity = term_compare.select_source_records(
        casa_records,
        source_trace,
        casa_trace,
    )
    source_key_sha256 = phase_compare.source_key_hash(
        source_trace,
        selected_casa,
        audit,
    )
    casa = {
        "tt0": term_compare.complex_boundary(selected_casa, "tt0"),
        "tt1": term_compare.complex_boundary(selected_casa, "tt1_raw"),
    }
    wide_audit = {
        "tt0": sidecar.complex_pair(
            audit,
            "first_model_term0",
            "second_model_term0",
        ),
        "tt1": sidecar.complex_pair(
            audit,
            "first_model_term1",
            "second_model_term1",
        ),
    }

    numerators = {
        term: raw_pairs(raw, term, "numerator") for term in ("tt0", "tt1")
    }
    normalizers = {
        term: raw_pairs(raw, term, "normalizer") for term in ("tt0", "tt1")
    }
    current = {
        term: wide_divide_pairs(numerators[term], normalizers[term])
        for term in ("tt0", "tt1")
    }
    phase_started = time.perf_counter()
    phased_numerators = {
        term: predivision_phase_pairs(numerators[term], source_trace)
        for term in ("tt0", "tt1")
    }
    casa_order = {
        term: wide_divide_pairs(phased_numerators[term], normalizers[term])
        for term in ("tt0", "tt1")
    }
    current_aligned = {
        term: phase_compare.phase_pairs(current[term], source_trace)
        for term in ("tt0", "tt1")
    }
    casa_aligned = {
        term: phase_compare.phase_pairs(casa[term], source_trace)
        for term in ("tt0", "tt1")
    }
    phase_elapsed = time.perf_counter() - phase_started

    hashes = {
        "source_key_sha256": source_key_sha256,
        "raw_identity_sha256": raw_identity_hash(raw),
    }
    for term in ("tt0", "tt1"):
        hashes.update(
            {
                f"casa_{term}_raw_sha256": stream_hash(casa[term]),
                f"current_{term}_raw_sha256": stream_hash(current[term]),
                f"current_{term}_aligned_sha256": stream_hash(
                    current_aligned[term]
                ),
                f"predivision_phased_numerator_{term}_sha256": stream_hash(
                    phased_numerators[term]
                ),
                f"casa_order_{term}_sha256": stream_hash(casa_order[term]),
            }
        )

    current_stats = {
        term: comparison_stats(
            actual=current[term],
            expected=casa[term],
            source_trace=source_trace,
        )
        for term in ("tt0", "tt1")
    }
    current_aligned_stats = {
        term: comparison_stats(
            actual=current_aligned[term],
            expected=casa_aligned[term],
            source_trace=source_trace,
        )
        for term in ("tt0", "tt1")
    }
    casa_order_stats = {
        term: comparison_stats(
            actual=casa_order[term],
            expected=casa[term],
            source_trace=source_trace,
        )
        for term in ("tt0", "tt1")
    }

    source_zero = {
        "prephase_numerator_bits": pair_bits(numerators["tt0"], 0, 0),
        "phased_numerator_bits": pair_bits(phased_numerators["tt0"], 0, 0),
        "casa_normalizer_bits": casa_normalizer_bits(normalizers["tt0"], 0, 0),
        "quotient_bits": pair_bits(casa_order["tt0"], 0, 0),
    }
    source_1446 = {
        "prephase_numerator_bits": pair_bits(numerators["tt0"], 1446, 0),
        "phased_numerator_bits": pair_bits(phased_numerators["tt0"], 1446, 0),
        "casa_normalizer_bits": casa_normalizer_bits(
            normalizers["tt0"],
            1446,
            0,
        ),
        "quotient_bits": pair_bits(casa_order["tt0"], 1446, 0),
        "casa_bits": pair_bits(casa["tt0"], 1446, 0),
    }
    callsite_zero = callsite_receipt["source_zero_control"]
    callsite_1446 = callsite_receipt["source_1446"]
    source_zero_exact = (
        source_zero["phased_numerator_bits"] == callsite_zero["pre_bits"][:2]
        and source_zero["casa_normalizer_bits"] == callsite_zero["pre_bits"][2:]
        and source_zero["quotient_bits"] == callsite_zero["post_bits"]
    )
    source_1446_exact = (
        source_1446["phased_numerator_bits"] == SOURCE_1446_LIVE_NUMERATOR
        and source_1446["phased_numerator_bits"] == callsite_1446["pre_bits"][:2]
        and source_1446["casa_normalizer_bits"] == callsite_1446["pre_bits"][2:]
        and source_1446["quotient_bits"] == SOURCE_1446_LIVE_RESULT
        and source_1446["quotient_bits"] == callsite_1446["post_bits"]
        and source_1446["quotient_bits"] == source_1446["casa_bits"]
    )

    current_counts_valid = all(
        current_stats[term]["mismatch_count"] == EXPECTED_CURRENT_MISMATCHES[term]
        and current_aligned_stats[term]["mismatch_count"]
        == EXPECTED_CURRENT_MISMATCHES[term]
        for term in ("tt0", "tt1")
    )
    topology_valid = all(
        (
            RAW_DTYPE.itemsize == 112,
            raw.size == audit.size == selected_casa.size == EXPECTED_SOURCE_COUNT,
            np.array_equal(
                raw["sample_ordinal"],
                np.arange(raw.size, dtype=np.uint32),
            ),
            np.all(raw["written_generation"] == int(wide_sidecar_host["generation"])),
            np.array_equal(
                raw["first_imaging_mueller"],
                audit["first_imaging_mueller"],
            ),
            np.array_equal(
                raw["second_imaging_mueller"],
                audit["second_imaging_mueller"],
            ),
        )
    )
    hash_contract_valid = all(
        (
            source_key_sha256 == phase_receipt["hashes"]["source_key_sha256"],
            hashes["casa_tt0_raw_sha256"]
            == phase_receipt["hashes"]["casa_tt0_raw_sha256"],
            hashes["casa_tt1_raw_sha256"]
            == phase_receipt["hashes"]["casa_tt1_raw_sha256"],
            hashes["current_tt0_raw_sha256"]
            == phase_receipt["hashes"]["casars_tt0_raw_sha256"],
            hashes["current_tt1_raw_sha256"]
            == phase_receipt["hashes"]["casars_tt1_raw_sha256"],
            hashes["current_tt0_aligned_sha256"]
            == phase_receipt["hashes"]["candidate_tt0_aligned_sha256"],
            hashes["current_tt1_aligned_sha256"]
            == phase_receipt["hashes"]["candidate_tt1_aligned_sha256"],
        )
    )
    wide_identity_valid = all(
        np.array_equal(
            current[term].view(np.uint32),
            wide_audit[term].view(np.uint32),
        )
        for term in ("tt0", "tt1")
    )
    instrumentation_valid = all(
        (
            term_receipt.get("instrumentation_valid"),
            phase_receipt.get("instrumentation_valid"),
            callsite_receipt.get("valid"),
            callsite_receipt.get("classification") == "operands-differ-at-callsite",
            int(wide_sidecar_host["sample_count"]) == EXPECTED_SOURCE_COUNT,
            int(wide_sidecar_host["complex_division_count"]) == EXPECTED_TERM_COUNT,
            wide_sidecar_host["integrity"]["nonfinite_or_zero_normalizer_count"] == 0,
            topology_valid,
            hash_contract_valid,
            wide_identity_valid,
            source_zero_exact,
        )
    )
    classification = classify(
        instrumentation_valid=instrumentation_valid,
        source_1446_exact=source_1446_exact,
        tt0_mismatches=casa_order_stats["tt0"]["mismatch_count"],
        tt1_mismatches=casa_order_stats["tt1"]["mismatch_count"],
        current_counts_valid=current_counts_valid,
    )
    return {
        "schema": "casa-rs-vlass-aw-predivision-source-phase-v1",
        "role": "offline-correctness-certificate-not-performance-evidence",
        "instrumentation_valid": instrumentation_valid,
        "classification": classification,
        "source_count": int(raw.size),
        "role_count": int(raw.size * 2),
        "term_count": int(raw.size * 4),
        "source_key_contract": phase_receipt["source_key_contract"],
        "row_identity": row_identity,
        "controls": {
            "topology_valid": topology_valid,
            "hash_contract_valid": hash_contract_valid,
            "wide_replay_matches_frozen_wide_audit": wide_identity_valid,
            "current_counts_valid": current_counts_valid,
            "source_zero_exact": source_zero_exact,
            "source_1446_exact": source_1446_exact,
        },
        "source_zero_tt0_rr": source_zero,
        "source_1446_tt0_rr": source_1446,
        "hashes": hashes,
        "comparisons": {
            "current_unphased_vs_casa_raw": current_stats,
            "current_postdivision_phase_vs_casa_aligned": current_aligned_stats,
            "casa_predivision_phase_vs_casa_raw": casa_order_stats,
        },
        "arithmetic_contract": {
            "source_phase": (
                "Complex32 numerator times conjugate of recorded source phasor; "
                "four separately rounded f32 products; separately rounded add/sub"
            ),
            "normalizer": "unchanged compact CF normalizer; conjugated at division",
            "division": wide_sidecar_host["precision_contract"],
        },
        "timings_ms": {
            "predivision_phase_and_wide_division": phase_elapsed * 1_000.0,
            "total_analyzer": (time.perf_counter() - started) * 1_000.0,
        },
        "peak_resident_bytes": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        "operation_counts": {
            "predivision_complex32_phase_multiplications": EXPECTED_TERM_COUNT,
            "wide_divisions": EXPECTED_TERM_COUNT * 2,
            "tap_visits": 0,
        },
        "prohibited_work": {
            "casa": "not-run",
            "measurement_set": "not-read",
            "metal": "not-run",
            "prediction_dispatch": "not-run",
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
    parser.add_argument("--phase-receipt", required=True, type=Path)
    parser.add_argument("--wide-sidecar-host", required=True, type=Path)
    parser.add_argument("--callsite-comparison", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite receipt: {args.output}")

    term_receipt = json.loads(args.term_comparison.read_text(encoding="utf-8"))
    phase_receipt = json.loads(args.phase_receipt.read_text(encoding="utf-8"))
    wide_sidecar_host = json.loads(
        args.wide_sidecar_host.read_text(encoding="utf-8")
    )
    callsite_receipt = json.loads(
        args.callsite_comparison.read_text(encoding="utf-8")
    )
    if phase_receipt.get("schema") != "casa-rs-vlass-aw-source-phase-placement-replay-v1":
        raise RuntimeError("unexpected source-phase receipt schema")
    if wide_sidecar_host.get("schema") != "casa-rs-vlass-aw-wide-division-sidecar-host-v1":
        raise RuntimeError("unexpected wide-sidecar host schema")
    if callsite_receipt.get("schema") != "casa-rs-vlass-casa-aw-divsc3-callsite-comparison-v1":
        raise RuntimeError("unexpected call-site comparison schema")

    term_inputs = term_receipt["inputs"]
    if (
        Path(phase_receipt["inputs"]["term_comparison"]) != args.term_comparison
        or phase_receipt["inputs"]["term_comparison_sha256"]
        != boundary.sha256_file(args.term_comparison)
    ):
        raise RuntimeError("source-phase receipt is not bound to term comparison")
    for name in (
        "host_receipt",
        "binary",
        "casa_npz",
        "casars_source_trace",
        "casars_sidecar_host",
    ):
        checked_input(
            Path(term_inputs[name]),
            str(term_inputs[f"{name}_sha256"]),
        )
    nested_wide_path = Path(wide_sidecar_host["wide_candidate"]["receipt"])
    checked_input(
        nested_wide_path,
        str(wide_sidecar_host["wide_candidate"]["receipt_sha256"]),
    )
    if nested_wide_path != Path(term_inputs["casars_sidecar_host"]):
        raise RuntimeError("wide sidecar candidate differs from term comparison")

    casa_host = json.loads(
        Path(term_inputs["host_receipt"]).read_text(encoding="utf-8")
    )
    binary_path = Path(term_inputs["binary"])
    if term_compare.fnv1a64_file(binary_path) != int(casa_host["binary_fnv1a64"]):
        raise RuntimeError("CASA term binary FNV-1a differs from host receipt")
    casa_records = np.fromfile(binary_path, dtype=term_compare.CASA_DTYPE)
    if casa_records.nbytes != int(casa_host["binary_bytes"]):
        raise RuntimeError("CASA term binary byte length differs")
    with np.load(term_inputs["casa_npz"]) as loaded:
        casa_trace = {name: np.asarray(loaded[name]) for name in loaded.files}
    source_trace = json.loads(
        Path(term_inputs["casars_source_trace"]).read_text(encoding="utf-8")
    )
    nested_wide_host = json.loads(nested_wide_path.read_text(encoding="utf-8"))
    audit_path = Path(nested_wide_host["audit"]["path"])
    checked_input(audit_path, str(nested_wide_host["audit"]["sha256"]))
    audit = np.fromfile(audit_path, dtype=sidecar.AUDIT_DTYPE)
    raw_path = Path(wide_sidecar_host["raw"]["path"])
    checked_input(raw_path, str(wide_sidecar_host["raw"]["sha256"]))
    raw = np.fromfile(raw_path, dtype=RAW_DTYPE)
    if raw.nbytes != int(wide_sidecar_host["raw"]["allocated_bytes"]):
        raise RuntimeError("wide raw sidecar byte length differs")

    result = analyze(
        term_receipt=term_receipt,
        phase_receipt=phase_receipt,
        wide_sidecar_host=wide_sidecar_host,
        callsite_receipt=callsite_receipt,
        audit=audit,
        raw=raw,
        casa_records=casa_records,
        casa_trace=casa_trace,
        source_trace=source_trace,
    )
    result["inputs"] = {
        "term_comparison": str(args.term_comparison),
        "term_comparison_sha256": boundary.sha256_file(args.term_comparison),
        "phase_receipt": str(args.phase_receipt),
        "phase_receipt_sha256": boundary.sha256_file(args.phase_receipt),
        "wide_sidecar_host": str(args.wide_sidecar_host),
        "wide_sidecar_host_sha256": boundary.sha256_file(args.wide_sidecar_host),
        "wide_raw": str(raw_path),
        "wide_raw_sha256": boundary.sha256_file(raw_path),
        "callsite_comparison": str(args.callsite_comparison),
        "callsite_comparison_sha256": boundary.sha256_file(
            args.callsite_comparison
        ),
        "analyzer": str(Path(__file__).resolve()),
        "analyzer_sha256": boundary.sha256_file(Path(__file__).resolve()),
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
