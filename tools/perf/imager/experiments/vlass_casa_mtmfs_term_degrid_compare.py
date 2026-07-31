#!/usr/bin/env python3
"""Compare CASA and casa-rs term-separated VLASS prediction boundaries."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

import vlass_prediction_boundary_hash_compare as boundary
import vlass_prediction_sidecar_compare as sidecar


CASA_DTYPE = np.dtype(
    [
        ("call", "<u4"),
        ("row_in_vb", "<u4"),
        ("row_id", "<u8"),
        ("spw_id", "<u4"),
        ("channel", "<u4"),
        ("frequency_hz", "<f8"),
        ("taylor_power1", "<f4"),
        ("_pad0", "<u4"),
        ("tt0_rr_re", "<f4"),
        ("tt0_rr_im", "<f4"),
        ("tt0_ll_re", "<f4"),
        ("tt0_ll_im", "<f4"),
        ("tt1_raw_rr_re", "<f4"),
        ("tt1_raw_rr_im", "<f4"),
        ("tt1_raw_ll_re", "<f4"),
        ("tt1_raw_ll_im", "<f4"),
        ("tt1_scaled_rr_re", "<f4"),
        ("tt1_scaled_rr_im", "<f4"),
        ("tt1_scaled_ll_re", "<f4"),
        ("tt1_scaled_ll_im", "<f4"),
        ("combined_rr_re", "<f4"),
        ("combined_rr_im", "<f4"),
        ("combined_ll_re", "<f4"),
        ("combined_ll_im", "<f4"),
    ],
    align=False,
)
FNV1A64_OFFSET = 0xCBF29CE484222325
FNV1A64_PRIME = 0x100000001B3


def fnv1a64_file(path: Path) -> int:
    value = FNV1A64_OFFSET
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            for byte in chunk:
                value ^= byte
                value = (value * FNV1A64_PRIME) & 0xFFFFFFFFFFFFFFFF
    return value


def complex_boundary(records: np.ndarray, name: str) -> np.ndarray:
    rr = np.asarray(
        records[f"{name}_rr_re"] + 1j * records[f"{name}_rr_im"],
        dtype=np.complex64,
    )
    ll = np.asarray(
        records[f"{name}_ll_re"] + 1j * records[f"{name}_ll_im"],
        dtype=np.complex64,
    )
    return np.stack((rr, ll), axis=1)


def phase_rotate_pairs(values: np.ndarray, source_trace: dict) -> np.ndarray:
    if values.shape != (len(source_trace["samples"]), 2):
        raise RuntimeError("phase-rotation input does not match source trace")
    output = np.empty_like(values)
    for ordinal, sample in enumerate(source_trace["samples"]):
        phase_re = boundary.f32_from_bits(int(sample["phase_re_bits"]))
        phase_im = boundary.f32_from_bits(int(sample["phase_im_bits"]))
        for role in range(2):
            output[ordinal, role] = boundary.multiply_f32_complex(
                values[ordinal, role],
                phase_re,
                phase_im,
            )
    return output


def select_source_records(
    records: np.ndarray,
    source_trace: dict,
    casa_trace: dict[str, np.ndarray],
) -> tuple[np.ndarray, dict[str, object]]:
    captured_spws = np.unique(records["spw_id"])
    source_rows = np.asarray(casa_trace["row_id"], dtype=np.int64)
    source_spws = np.asarray(casa_trace["spectral_window_id"], dtype=np.int32)
    if source_rows.shape != source_spws.shape:
        raise RuntimeError("frozen CASA row and SPW identities differ in extent")
    rows_per_spw: int | None = None
    original_base: int | None = None
    row_map: dict[tuple[int, int], int] = {}
    spw_receipts: list[dict[str, int]] = []
    for spw_value in captured_spws:
        spw = int(spw_value)
        captured_rows = np.unique(records["row_id"][records["spw_id"] == spw])
        if captured_rows.size == 0 or not np.array_equal(
            captured_rows,
            np.arange(captured_rows[0], captured_rows[-1] + 1),
        ):
            raise RuntimeError(f"CASA selected rows are not contiguous for SPW {spw}")
        if rows_per_spw is None:
            rows_per_spw = int(captured_rows.size)
        elif captured_rows.size != rows_per_spw:
            raise RuntimeError("CASA selected row count differs among SPWs")

        original_rows = np.unique(source_rows[source_spws == spw])
        if original_rows.size == 0:
            raise RuntimeError(f"frozen CASA trace lacks SPW {spw}")
        original_start = int(original_rows[-1]) - rows_per_spw + 1
        candidate_base = original_start - spw * rows_per_spw
        if original_base is None:
            original_base = candidate_base
        elif candidate_base != original_base:
            raise RuntimeError("frozen CASA row blocks do not share one SPW stride")
        if int(original_rows[0]) < original_start:
            raise RuntimeError(f"frozen CASA rows exceed selected block for SPW {spw}")

        captured_start = int(captured_rows[0])
        for original_row in original_rows:
            local_row = captured_start + int(original_row) - original_start
            if local_row > int(captured_rows[-1]):
                raise RuntimeError(
                    f"mapped CASA row exceeds selected block for SPW {spw}"
                )
            row_map[(spw, int(original_row))] = local_row
        spw_receipts.append(
            {
                "spw_id": spw,
                "captured_row_start": captured_start,
                "captured_row_end": int(captured_rows[-1]),
                "original_row_start": original_start,
                "original_row_end": original_start + rows_per_spw - 1,
                "frozen_source_rows": int(original_rows.size),
            }
        )
    if rows_per_spw is None or original_base is None:
        raise RuntimeError("CASA term-degrid capture contains no SPWs")

    keys: dict[tuple[int, int], int] = {}
    for index, record in enumerate(records):
        key = (int(record["row_id"]), int(record["channel"]))
        if key in keys:
            raise RuntimeError(f"duplicate CASA term-degrid record {key}")
        keys[key] = index
    selected = np.empty(len(source_trace["samples"]), dtype=CASA_DTYPE)
    for ordinal, sample in enumerate(source_trace["samples"]):
        if int(sample["source_ordinal"]) != ordinal:
            raise RuntimeError(f"source ordinal differs at position {ordinal}")
        spw = int(sample["spw_id"])
        original_row = int(sample["row_id"])
        try:
            local_row = row_map[(spw, original_row)]
        except KeyError as error:
            raise RuntimeError(
                f"CASA selected-row map lacks source {(spw, original_row)}"
            ) from error
        key = (local_row, int(sample["channel"]))
        try:
            record = records[keys[key]]
        except KeyError as error:
            raise RuntimeError(f"CASA term-degrid oracle lacks source {key}") from error
        if int(record["spw_id"]) != int(sample["spw_id"]):
            raise RuntimeError(f"CASA SPW differs at source {ordinal}")
        expected_frequency = np.float64(sample["frequency_hz"]).view(np.uint64)
        actual_frequency = record["frequency_hz"].view(np.uint64)
        if actual_frequency != expected_frequency:
            raise RuntimeError(f"CASA frequency differs at source {ordinal}")
        selected[ordinal] = record
    return selected, {
        "schema": "casa-vlass-selected-row-identity-map-v1",
        "derivation": (
            "VI2 selected-table row blocks aligned to frozen original-MS "
            "rows by common per-SPW stride"
        ),
        "rows_per_spw": rows_per_spw,
        "original_row_base": original_base,
        "spws": spw_receipts,
    }


def separately_scale_pairs(values: np.ndarray, powers: np.ndarray) -> np.ndarray:
    output = np.empty_like(values)
    for role in range(2):
        real = np.asarray(
            np.asarray(values[:, role].real, dtype=np.float32) * powers,
            dtype=np.float32,
        )
        imag = np.asarray(
            np.asarray(values[:, role].imag, dtype=np.float32) * powers,
            dtype=np.float32,
        )
        output[:, role] = np.asarray(real + 1j * imag, dtype=np.complex64)
    return output


def mismatch_count(actual: np.ndarray, expected: np.ndarray) -> int:
    actual_bits = actual.view(np.float32).reshape(actual.shape + (2,)).view(np.uint32)
    expected_bits = (
        expected.view(np.float32).reshape(expected.shape + (2,)).view(np.uint32)
    )
    return int(np.count_nonzero(np.any(actual_bits != expected_bits, axis=2)))


def classify(
    *,
    casars_tt0: np.ndarray,
    casars_tt1: np.ndarray,
    casars_scaled: np.ndarray,
    casars_literal_combined: np.ndarray,
    casa_tt0_raw: np.ndarray,
    casa_tt1_raw: np.ndarray,
    casa_tt1_scaled_raw: np.ndarray,
    casa_combined_raw: np.ndarray,
    casa_tt0_rotated: np.ndarray,
    casa_tt1_rotated: np.ndarray,
    casa_tt1_scaled_rotated: np.ndarray,
    casa_combined_rotated: np.ndarray,
    power_bits_match: bool,
) -> tuple[str, tuple[int, int] | None]:
    tt0 = sidecar.first_pair_mismatch(casars_tt0, casa_tt0_rotated)
    if tt0 is not None:
        if sidecar.first_pair_mismatch(casars_tt0, casa_tt0_raw) is None:
            return "tt0-phase-application-difference", tt0
        return "tt0-degrid-or-folded-phase-difference", tt0
    tt1 = sidecar.first_pair_mismatch(casars_tt1, casa_tt1_rotated)
    if tt1 is not None:
        if sidecar.first_pair_mismatch(casars_tt1, casa_tt1_raw) is None:
            return "tt1-phase-application-difference", tt1
        return "tt1-degrid-or-folded-phase-difference", tt1
    if not power_bits_match:
        mismatch = sidecar.first_pair_mismatch(
            casars_scaled,
            casa_tt1_scaled_rotated,
        )
        return "taylor-power-difference", mismatch
    scaled = sidecar.first_pair_mismatch(
        casars_scaled,
        casa_tt1_scaled_rotated,
    )
    if scaled is not None:
        return "taylor-scaling-or-phase-order-difference", scaled
    combined = sidecar.first_pair_mismatch(
        casars_literal_combined,
        casa_combined_rotated,
    )
    if combined is not None:
        return "taylor-addition-or-phase-order-difference", combined
    if (
        sidecar.first_pair_mismatch(casa_tt0_raw, casa_tt0_raw) is not None
        or sidecar.first_pair_mismatch(casa_tt1_raw, casa_tt1_raw) is not None
        or sidecar.first_pair_mismatch(
            casa_tt1_scaled_raw,
            casa_tt1_scaled_raw,
        )
        is not None
        or sidecar.first_pair_mismatch(casa_combined_raw, casa_combined_raw) is not None
    ):
        raise AssertionError("unreachable self-comparison mismatch")
    return "term-separated-prediction-exact", None


def analyze(
    *,
    host: dict,
    records: np.ndarray,
    audit: np.ndarray,
    casa_trace: dict[str, np.ndarray],
    source_trace: dict,
    sidecar_receipt: dict,
) -> dict[str, object]:
    selected, row_identity = select_source_records(
        records,
        source_trace,
        casa_trace,
    )
    if audit.size != selected.size:
        raise RuntimeError("CASA and casa-rs source counts differ")

    casa_tt0_raw = complex_boundary(selected, "tt0")
    casa_tt1_raw = complex_boundary(selected, "tt1_raw")
    casa_tt1_scaled_raw = complex_boundary(selected, "tt1_scaled")
    casa_combined_raw = complex_boundary(selected, "combined")
    casa_tt0_rotated = phase_rotate_pairs(casa_tt0_raw, source_trace)
    casa_tt1_rotated = phase_rotate_pairs(casa_tt1_raw, source_trace)
    casa_tt1_scaled_rotated = phase_rotate_pairs(
        casa_tt1_scaled_raw,
        source_trace,
    )
    casa_combined_rotated = phase_rotate_pairs(casa_combined_raw, source_trace)

    _, frozen_casa_combined, census = boundary.source_trace_parallel_hands(
        casa_trace,
        source_trace,
    )
    raw_frozen_casa = sidecar.raw_casa_model_pairs(casa_trace, source_trace)
    casars_tt0 = sidecar.complex_pair(
        audit,
        "first_model_term0",
        "second_model_term0",
    )
    casars_tt1 = sidecar.complex_pair(
        audit,
        "first_model_term1",
        "second_model_term1",
    )
    casars_combined = sidecar.complex_pair(
        audit,
        "first_combined",
        "second_combined",
    )
    casars_powers = np.asarray(audit["taylor_power1"], dtype=np.float32)
    casa_powers = np.asarray(selected["taylor_power1"], dtype=np.float32)
    casars_scaled = separately_scale_pairs(casars_tt1, casars_powers)
    casars_literal_combined = np.asarray(
        casars_tt0 + casars_scaled,
        dtype=np.complex64,
    )
    power_bits_match = bool(
        np.array_equal(casars_powers.view(np.uint32), casa_powers.view(np.uint32))
    )
    power_mismatches = np.flatnonzero(
        casars_powers.view(np.uint32) != casa_powers.view(np.uint32)
    )

    hashes = {
        "casa_tt0_raw_sha256": boundary.hash_parallel_hands(casa_tt0_raw),
        "casa_tt1_raw_sha256": boundary.hash_parallel_hands(casa_tt1_raw),
        "casa_tt1_scaled_raw_sha256": boundary.hash_parallel_hands(casa_tt1_scaled_raw),
        "casa_combined_raw_sha256": boundary.hash_parallel_hands(casa_combined_raw),
        "casa_tt0_phase_rotated_sha256": boundary.hash_parallel_hands(casa_tt0_rotated),
        "casa_tt1_phase_rotated_sha256": boundary.hash_parallel_hands(casa_tt1_rotated),
        "casa_tt1_scaled_phase_rotated_sha256": boundary.hash_parallel_hands(
            casa_tt1_scaled_rotated
        ),
        "casa_combined_phase_rotated_sha256": boundary.hash_parallel_hands(
            casa_combined_rotated
        ),
        "casars_tt0_sha256": boundary.hash_parallel_hands(casars_tt0),
        "casars_tt1_sha256": boundary.hash_parallel_hands(casars_tt1),
        "casars_scaled_tt1_sha256": boundary.hash_parallel_hands(casars_scaled),
        "casars_literal_combined_sha256": boundary.hash_parallel_hands(
            casars_literal_combined
        ),
        "casars_production_combined_sha256": boundary.hash_parallel_hands(
            casars_combined
        ),
    }
    instrumentation_valid = all(
        (
            int(host["binary_record_size"]) == CASA_DTYPE.itemsize,
            int(host["binary_record_count"]) == records.size,
            records.size > selected.size,
            selected.size == 98239,
            census["collapsed_visibility_bit_mismatches"] == 0,
            hashes["casa_combined_raw_sha256"]
            == boundary.hash_parallel_hands(raw_frozen_casa),
            hashes["casa_combined_phase_rotated_sha256"]
            == boundary.hash_parallel_hands(frozen_casa_combined),
            hashes["casa_combined_phase_rotated_sha256"]
            == sidecar_receipt["hashes"]["casa_phase_rotated_model_data_sha256"],
            np.all(np.isfinite(selected["taylor_power1"])),
        )
    )

    if instrumentation_valid:
        classification, first = classify(
            casars_tt0=casars_tt0,
            casars_tt1=casars_tt1,
            casars_scaled=casars_scaled,
            casars_literal_combined=casars_literal_combined,
            casa_tt0_raw=casa_tt0_raw,
            casa_tt1_raw=casa_tt1_raw,
            casa_tt1_scaled_raw=casa_tt1_scaled_raw,
            casa_combined_raw=casa_combined_raw,
            casa_tt0_rotated=casa_tt0_rotated,
            casa_tt1_rotated=casa_tt1_rotated,
            casa_tt1_scaled_rotated=casa_tt1_scaled_rotated,
            casa_combined_rotated=casa_combined_rotated,
            power_bits_match=power_bits_match,
        )
    else:
        classification, first = "invalid-instrumentation", None

    first_mismatch = None
    if first is not None:
        ordinal, role = first
        first_mismatch = {
            "source": sidecar.source_context(source_trace, ordinal, role),
            "role": "rr" if role == 0 else "ll",
            "casa_tt0_raw_bits": sidecar.bit_values(
                casa_tt0_raw,
                ordinal,
                role,
            ),
            "casa_tt0_phase_rotated_bits": sidecar.bit_values(
                casa_tt0_rotated,
                ordinal,
                role,
            ),
            "casars_tt0_bits": sidecar.bit_values(casars_tt0, ordinal, role),
            "casa_tt1_raw_bits": sidecar.bit_values(
                casa_tt1_raw,
                ordinal,
                role,
            ),
            "casa_tt1_phase_rotated_bits": sidecar.bit_values(
                casa_tt1_rotated,
                ordinal,
                role,
            ),
            "casars_tt1_bits": sidecar.bit_values(casars_tt1, ordinal, role),
            "casa_tt1_scaled_phase_rotated_bits": sidecar.bit_values(
                casa_tt1_scaled_rotated,
                ordinal,
                role,
            ),
            "casars_scaled_tt1_bits": sidecar.bit_values(
                casars_scaled,
                ordinal,
                role,
            ),
            "casa_combined_phase_rotated_bits": sidecar.bit_values(
                casa_combined_rotated,
                ordinal,
                role,
            ),
            "casars_literal_combined_bits": sidecar.bit_values(
                casars_literal_combined,
                ordinal,
                role,
            ),
            "casars_production_combined_bits": sidecar.bit_values(
                casars_combined,
                ordinal,
                role,
            ),
            "casa_taylor_power_bits": int(casa_powers[ordinal].view(np.uint32)),
            "casars_taylor_power_bits": int(casars_powers[ordinal].view(np.uint32)),
        }

    return {
        "schema": "casa-rs-vlass-term-separated-prediction-comparison-v1",
        "role": "bounded_correctness_oracle_not_performance_evidence",
        "instrumentation_valid": instrumentation_valid,
        "classification": classification,
        "source_count": int(selected.size),
        "casa_full_record_count": int(records.size),
        "row_identity": row_identity,
        "power_bits_match": power_bits_match,
        "power_mismatch_count": int(power_mismatches.size),
        "first_power_mismatch": (
            int(power_mismatches[0]) if power_mismatches.size else None
        ),
        "hashes": hashes,
        "mismatch_counts": {
            "tt0": mismatch_count(casars_tt0, casa_tt0_rotated),
            "tt1_raw": mismatch_count(casars_tt1, casa_tt1_rotated),
            "tt1_scaled": mismatch_count(
                casars_scaled,
                casa_tt1_scaled_rotated,
            ),
            "literal_combined": mismatch_count(
                casars_literal_combined,
                casa_combined_rotated,
            ),
            "production_combined": mismatch_count(
                casars_combined,
                casa_combined_rotated,
            ),
        },
        "first_mismatch": first_mismatch,
        "prohibited_stages": {
            "formed_residual": host["formed_residual"],
            "residual_grid_dispatch": host["residual_grid_dispatch"],
            "finalize_to_vis": host["finalize_to_vis"],
            "fft": host["fft"],
            "image_formation": host["image_formation"],
            "products": host["products"],
            "clean_iterations": host["clean_iterations"],
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host-receipt", required=True, type=Path)
    parser.add_argument("--casa-npz", required=True, type=Path)
    parser.add_argument("--casars-source-trace", required=True, type=Path)
    parser.add_argument("--casars-sidecar-host", required=True, type=Path)
    parser.add_argument("--casars-sidecar-comparison", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite comparison receipt: {args.output}")

    host = json.loads(args.host_receipt.read_text(encoding="utf-8"))
    if host["schema"] != "casa-vlass-frozen-model-term-degrid-oracle-v1":
        raise RuntimeError("unexpected CASA term-degrid host schema")
    if host["status"] != "completed-before-finalize-to-vis":
        raise RuntimeError("CASA term-degrid host receipt is not complete")
    binary = Path(host["binary"])
    if fnv1a64_file(binary) != int(host["binary_fnv1a64"]):
        raise RuntimeError("CASA term-degrid binary FNV-1a differs from host receipt")
    records = np.fromfile(binary, dtype=CASA_DTYPE)
    if records.nbytes != int(host["binary_bytes"]):
        raise RuntimeError("CASA binary byte length differs from host receipt")
    with np.load(args.casa_npz) as loaded:
        casa_trace = {name: np.asarray(loaded[name]) for name in loaded.files}
    source_trace = json.loads(args.casars_source_trace.read_text(encoding="utf-8"))
    casars_host = json.loads(args.casars_sidecar_host.read_text(encoding="utf-8"))
    audit_path = Path(casars_host["audit"]["path"])
    audit = np.fromfile(audit_path, dtype=sidecar.AUDIT_DTYPE)
    sidecar_receipt = json.loads(
        args.casars_sidecar_comparison.read_text(encoding="utf-8")
    )
    result = analyze(
        host=host,
        records=records,
        audit=audit,
        casa_trace=casa_trace,
        source_trace=source_trace,
        sidecar_receipt=sidecar_receipt,
    )
    result["inputs"] = {
        "host_receipt": str(args.host_receipt),
        "host_receipt_sha256": boundary.sha256_file(args.host_receipt),
        "binary": str(binary),
        "binary_sha256": boundary.sha256_file(binary),
        "casa_npz": str(args.casa_npz),
        "casa_npz_sha256": boundary.sha256_file(args.casa_npz),
        "casars_source_trace": str(args.casars_source_trace),
        "casars_source_trace_sha256": boundary.sha256_file(args.casars_source_trace),
        "casars_sidecar_host": str(args.casars_sidecar_host),
        "casars_sidecar_host_sha256": boundary.sha256_file(args.casars_sidecar_host),
        "casars_sidecar_comparison": str(args.casars_sidecar_comparison),
        "casars_sidecar_comparison_sha256": boundary.sha256_file(
            args.casars_sidecar_comparison
        ),
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
