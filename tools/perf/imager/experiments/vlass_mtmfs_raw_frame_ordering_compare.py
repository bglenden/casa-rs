#!/usr/bin/env python3
"""Certify CASA MT-MFS raw-frame scale/add ordering from frozen artifacts.

This bounded offline diagnostic compares two Float operation graphs and stops
at their combined-prediction streams. A tiny Rust helper performs all numeric
operations; this driver only binds immutable inputs, compares bit patterns,
and writes the receipt.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import numpy as np

import vlass_casa_mtmfs_term_degrid_compare as term_compare
import vlass_prediction_boundary_hash_compare as boundary
import vlass_prediction_sidecar_compare as sidecar
import vlass_taylor_power_graph_compare as taylor


SCHEMA = "casa-rs-vlass-mtmfs-raw-frame-ordering-v1"
HELPER_SCHEMA = "casa-rs-vlass-mtmfs-raw-frame-ordering-helper-v1"
EXPECTED_TERM_SCHEMA = "casa-rs-vlass-term-separated-prediction-comparison-v1"
EXPECTED_TAYLOR_SCHEMA = "casa-rs-vlass-taylor-power-graph-census-v1"
EXPECTED_TAYLOR_RECEIPT_SHA256 = (
    "24cbbc511c56add7d69aa4fc8ae525ce47c8cd06a405864df4094dde59329615"
)
EXPECTED_REFERENCE_BITS = int("41e642f2b1117f64", 16)
EXPECTED_SOURCE_COUNT = 98_239
EXPECTED_ROLE_COUNT = 196_478
EXPECTED_ALIGNED_TT0_HASH = (
    "0dee472a2f19ea3f03af86442f086d383980853390ecd5188066f5e8f4b1cedb"
)
EXPECTED_ALIGNED_TT1_HASH = (
    "950dc856071fb616be4d130b925d61b5d107b7580edf92b55771312ce3381545"
)
EXPECTED_CURRENT_MISMATCHES = {
    "scaled_tt1": 230,
    "combined": 434,
}

INPUT_DTYPE = np.dtype(
    [
        ("frequency_bits", "<u8"),
        ("raw_tt0_bits", "<u4", (4,)),
        ("raw_tt1_bits", "<u4", (4,)),
        ("phase_bits", "<u4", (2,)),
    ],
    align=False,
)
OUTPUT_DTYPE = np.dtype(
    [
        ("frequency_bits", "<u8"),
        ("freq_f32_bits", "<u4"),
        ("mulfactor_f64_bits", "<u8"),
        ("power_f32_bits", "<u4"),
        ("aligned_tt0_bits", "<u4", (4,)),
        ("aligned_tt1_bits", "<u4", (4,)),
        ("scaled_current_bits", "<u4", (4,)),
        ("combined_current_bits", "<u4", (4,)),
        ("scaled_raw_bits", "<u4", (4,)),
        ("combined_raw_bits", "<u4", (4,)),
        ("aligned_scaled_raw_bits", "<u4", (4,)),
        ("aligned_combined_raw_bits", "<u4", (4,)),
    ],
    align=False,
)
SOURCE_KEY_DTYPE = np.dtype(
    [
        ("source_ordinal", "<u8"),
        ("row_id", "<i8"),
        ("ddid", "<i4"),
        ("spw_id", "<i4"),
        ("channel", "<i4"),
        ("frequency_bits", "<u8"),
    ],
    align=False,
)


def hash_array(values: np.ndarray, dtype: str | np.dtype[Any]) -> str:
    array = np.ascontiguousarray(values, dtype=dtype)
    return hashlib.sha256(array.tobytes(order="C")).hexdigest()


def pair_bits(values: np.ndarray) -> np.ndarray:
    pairs = np.ascontiguousarray(values, dtype=np.complex64)
    if pairs.ndim != 2 or pairs.shape[1] != 2:
        raise RuntimeError(f"expected [source,2] complex pairs, got {pairs.shape}")
    return pairs.view(np.float32).reshape(pairs.shape[0], 4).view(np.uint32)


def pairs_from_bits(bits: np.ndarray) -> np.ndarray:
    words = np.ascontiguousarray(bits, dtype=np.uint32)
    if words.ndim != 2 or words.shape[1] != 4:
        raise RuntimeError(f"expected [source,4] component bits, got {words.shape}")
    components = words.view(np.float32).reshape(words.shape[0], 2, 2)
    result = np.empty((words.shape[0], 2), dtype=np.complex64)
    result.view(np.float32).reshape(words.shape[0], 2, 2)[:] = components
    return result


def build_input_records(
    *,
    frequencies: np.ndarray,
    raw_tt0: np.ndarray,
    raw_tt1: np.ndarray,
    source_trace: dict[str, Any],
) -> np.ndarray:
    count = frequencies.size
    if raw_tt0.shape != (count, 2) or raw_tt1.shape != (count, 2):
        raise RuntimeError("raw term shapes differ from frequency count")
    if len(source_trace["samples"]) != count:
        raise RuntimeError("source trace differs from frequency count")
    records = np.empty(count, dtype=INPUT_DTYPE)
    records["frequency_bits"] = np.ascontiguousarray(
        frequencies,
        dtype=np.float64,
    ).view(np.uint64)
    records["raw_tt0_bits"] = pair_bits(raw_tt0)
    records["raw_tt1_bits"] = pair_bits(raw_tt1)
    records["phase_bits"] = np.asarray(
        [
            [int(sample["phase_re_bits"]), int(sample["phase_im_bits"])]
            for sample in source_trace["samples"]
        ],
        dtype=np.uint32,
    )
    return records


def compile_helper(
    *,
    rustc: str,
    source: Path,
    temporary: Path,
) -> tuple[Path, dict[str, Any]]:
    helper = temporary / "vlass_mtmfs_raw_frame_ordering"
    command = [
        rustc,
        "--edition=2024",
        "-C",
        "opt-level=3",
        "-C",
        "overflow-checks=yes",
        "-D",
        "warnings",
        str(source),
        "-o",
        str(helper),
    ]
    subprocess.run(command, check=True, capture_output=True, text=True)
    description = json.loads(
        subprocess.run(
            [str(helper), "--describe"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    compiler = subprocess.run(
        [rustc, "-vV"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    linkage = subprocess.run(
        ["otool", "-L", str(helper)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return helper, {
        **description,
        "compile_command": command,
        "compiler": compiler,
        "linkage": linkage,
        "source": str(source),
        "source_sha256": boundary.sha256_file(source),
        "executable_sha256": boundary.sha256_file(helper),
    }


def run_helper(
    *,
    helper: Path,
    records: np.ndarray,
    reference_bits: int,
    temporary: Path,
) -> tuple[np.ndarray, dict[str, str]]:
    input_path = temporary / "ordering-input.bin"
    output_path = temporary / "ordering-output.bin"
    np.ascontiguousarray(records, dtype=INPUT_DTYPE).tofile(input_path)
    subprocess.run(
        [
            str(helper),
            str(input_path),
            str(output_path),
            f"{reference_bits:016x}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    output = np.fromfile(output_path, dtype=OUTPUT_DTYPE)
    if output.size != records.size:
        raise RuntimeError("Rust helper output count differs from frozen sources")
    return output, {
        "input_sha256": boundary.sha256_file(input_path),
        "output_sha256": boundary.sha256_file(output_path),
    }


def pair_mismatch_count(actual: np.ndarray, expected: np.ndarray) -> int:
    return taylor.pair_mismatch_count(actual, expected)


def comparison_metrics(
    *,
    candidate: np.ndarray,
    reference: np.ndarray,
    source_trace: dict[str, Any],
) -> dict[str, Any]:
    if candidate.shape != reference.shape:
        raise RuntimeError("comparison pair shapes differ")
    mismatch_count = pair_mismatch_count(candidate, reference)
    first = sidecar.first_pair_mismatch(candidate, reference)
    candidate_components = candidate.view(np.float32)
    reference_components = reference.view(np.float32)
    maximum_absolute = float(
        np.max(
            np.abs(
                candidate_components.astype(np.float64)
                - reference_components.astype(np.float64)
            ),
            initial=0.0,
        )
    )
    first_detail = None
    if first is not None:
        ordinal, role = first
        first_detail = {
            "source": sidecar.source_context(source_trace, ordinal, role),
            "role": "rr" if role == 0 else "ll",
            "candidate_bits": sidecar.bit_values(candidate, ordinal, role),
            "reference_bits": sidecar.bit_values(reference, ordinal, role),
        }
    return {
        "candidate_sha256": boundary.hash_parallel_hands(candidate),
        "reference_sha256": boundary.hash_parallel_hands(reference),
        "bit_exact_count": int(candidate.shape[0] * candidate.shape[1] - mismatch_count),
        "mismatch_count": mismatch_count,
        "maximum_component_ulp_distance": taylor.maximum_ulp_distance(
            candidate.view(np.float32),
            reference.view(np.float32),
        ),
        "maximum_component_absolute_difference": maximum_absolute,
        "first_mismatch": first_detail,
    }


def source_key_hash(source_trace: dict[str, Any]) -> str:
    samples = source_trace["samples"]
    keys = np.empty(len(samples), dtype=SOURCE_KEY_DTYPE)
    for ordinal, sample in enumerate(samples):
        if int(sample["source_ordinal"]) != ordinal:
            raise RuntimeError(f"source ordinal differs at position {ordinal}")
        keys[ordinal] = (
            ordinal,
            int(sample["row_id"]),
            int(sample["ddid"]),
            int(sample["spw_id"]),
            int(sample["channel"]),
            int(np.float64(sample["frequency_hz"]).view(np.uint64)),
        )
    return hash_array(keys, SOURCE_KEY_DTYPE)


def classify_ordering(
    *,
    controls_valid: bool,
    raw_order_scaled_mismatches: int,
    raw_order_combined_mismatches: int,
) -> str:
    if not controls_valid:
        return "evidence-or-control-invalid"
    if raw_order_scaled_mismatches > 0:
        return "raw-frame-scale-different"
    if raw_order_combined_mismatches > 0:
        return "raw-frame-scale-exact-combined-different"
    return "raw-frame-scale-add-closes-all"


def source_775_detail(
    *,
    source_trace: dict[str, Any],
    selected: np.ndarray,
    output: np.ndarray,
    reference_bits: int,
    graph_values: dict[str, np.ndarray],
) -> dict[str, Any]:
    ordinal = 775
    role = 0
    return {
        "source": sidecar.source_context(source_trace, ordinal, role),
        "frequency_f64_bits": int(output["frequency_bits"][ordinal]),
        "freq_f32_bits": int(output["freq_f32_bits"][ordinal]),
        "reference_frequency_f64_bits": reference_bits,
        "mulfactor_f64_bits": int(output["mulfactor_f64_bits"][ordinal]),
        "power_f32_bits": int(output["power_f32_bits"][ordinal]),
        "rejected_auxiliary_power_f32_bits": int(
            selected["taylor_power1"][ordinal].view(np.uint32)
        ),
        "raw_tt0_bits": sidecar.bit_values(
            graph_values["raw_tt0"],
            ordinal,
            role,
        ),
        "raw_tt1_bits": sidecar.bit_values(
            graph_values["raw_tt1"],
            ordinal,
            role,
        ),
        "phase_bits": [
            int(source_trace["samples"][ordinal]["phase_re_bits"]),
            int(source_trace["samples"][ordinal]["phase_im_bits"]),
        ],
        **{
            f"{name}_bits": sidecar.bit_values(values, ordinal, role)
            for name, values in graph_values.items()
            if name not in ("raw_tt0", "raw_tt1")
        },
    }


def analyze(
    *,
    term_receipt: dict[str, Any],
    taylor_receipt: dict[str, Any],
    selected: np.ndarray,
    source_trace: dict[str, Any],
    helper_output: np.ndarray,
    helper_metadata: dict[str, Any],
    helper_stream_hashes: dict[str, str],
    reference_bits: int,
    row_identity: dict[str, Any],
) -> dict[str, Any]:
    raw_tt0 = term_compare.complex_boundary(selected, "tt0")
    raw_tt1 = term_compare.complex_boundary(selected, "tt1_raw")
    casa_scaled_raw = term_compare.complex_boundary(selected, "tt1_scaled")
    casa_combined_raw = term_compare.complex_boundary(selected, "combined")
    casa_scaled_aligned = term_compare.phase_rotate_pairs(
        casa_scaled_raw,
        source_trace,
    )
    casa_combined_aligned = term_compare.phase_rotate_pairs(
        casa_combined_raw,
        source_trace,
    )

    graphs = {
        name: pairs_from_bits(helper_output[field])
        for name, field in (
            ("aligned_tt0", "aligned_tt0_bits"),
            ("aligned_tt1", "aligned_tt1_bits"),
            ("scaled_current", "scaled_current_bits"),
            ("combined_current", "combined_current_bits"),
            ("scaled_raw", "scaled_raw_bits"),
            ("combined_raw", "combined_raw_bits"),
            ("aligned_scaled_raw", "aligned_scaled_raw_bits"),
            ("aligned_combined_raw", "aligned_combined_raw_bits"),
        )
    }
    graph_values = {
        "raw_tt0": raw_tt0,
        "raw_tt1": raw_tt1,
        **graphs,
        "frozen_casa_scaled_raw": casa_scaled_raw,
        "frozen_casa_combined_raw": casa_combined_raw,
        "frozen_casa_scaled_aligned": casa_scaled_aligned,
        "frozen_casa_combined_aligned": casa_combined_aligned,
    }

    comparisons = {
        "current_scaled_aligned": comparison_metrics(
            candidate=graphs["scaled_current"],
            reference=casa_scaled_aligned,
            source_trace=source_trace,
        ),
        "current_combined_aligned": comparison_metrics(
            candidate=graphs["combined_current"],
            reference=casa_combined_aligned,
            source_trace=source_trace,
        ),
        "casa_order_scaled_raw": comparison_metrics(
            candidate=graphs["scaled_raw"],
            reference=casa_scaled_raw,
            source_trace=source_trace,
        ),
        "casa_order_combined_raw": comparison_metrics(
            candidate=graphs["combined_raw"],
            reference=casa_combined_raw,
            source_trace=source_trace,
        ),
        "casa_order_scaled_aligned": comparison_metrics(
            candidate=graphs["aligned_scaled_raw"],
            reference=casa_scaled_aligned,
            source_trace=source_trace,
        ),
        "casa_order_combined_aligned": comparison_metrics(
            candidate=graphs["aligned_combined_raw"],
            reference=casa_combined_aligned,
            source_trace=source_trace,
        ),
    }

    identity_graph = taylor_receipt["graphs"]["identity"]
    power_hash = hash_array(
        helper_output["power_f32_bits"],
        "<u4",
    )
    freq_f64_hash = hash_array(helper_output["frequency_bits"], "<u8")
    freq_f32_hash = hash_array(helper_output["freq_f32_bits"], "<u4")
    mulfactor_f64_hash = hash_array(helper_output["mulfactor_f64_bits"], "<u8")
    phasors = np.asarray(
        [
            [int(sample["phase_re_bits"]), int(sample["phase_im_bits"])]
            for sample in source_trace["samples"]
        ],
        dtype=np.uint32,
    )
    expected_helper_input = build_input_records(
        frequencies=np.asarray(selected["frequency_hz"], dtype=np.float64),
        raw_tt0=raw_tt0,
        raw_tt1=raw_tt1,
        source_trace=source_trace,
    )
    current_controls = {
        "aligned_tt0_hash_exact": (
            boundary.hash_parallel_hands(graphs["aligned_tt0"])
            == EXPECTED_ALIGNED_TT0_HASH
        ),
        "aligned_tt1_hash_exact": (
            boundary.hash_parallel_hands(graphs["aligned_tt1"])
            == EXPECTED_ALIGNED_TT1_HASH
        ),
        "power_hash_exact": power_hash == identity_graph["power_sha256"],
        "source_775_power_exact": int(helper_output["power_f32_bits"][775])
        == 3_198_777_242,
        "source_775_rejected_auxiliary_distinct": int(
            selected["taylor_power1"][775].view(np.uint32)
        )
        == 3_198_777_243,
        "scaled_mismatch_count_exact": (
            comparisons["current_scaled_aligned"]["mismatch_count"]
            == EXPECTED_CURRENT_MISMATCHES["scaled_tt1"]
        ),
        "combined_mismatch_count_exact": (
            comparisons["current_combined_aligned"]["mismatch_count"]
            == EXPECTED_CURRENT_MISMATCHES["combined"]
        ),
        "scaled_hash_exact": (
            comparisons["current_scaled_aligned"]["candidate_sha256"]
            == identity_graph["scaled_tt1_sha256"]
        ),
        "combined_hash_exact": (
            comparisons["current_combined_aligned"]["candidate_sha256"]
            == identity_graph["combined_sha256"]
        ),
    }
    artifact_controls = {
        "term_instrumentation_valid": bool(term_receipt["instrumentation_valid"]),
        "taylor_instrumentation_valid": bool(taylor_receipt["instrumentation_valid"]),
        "helper_schema_exact": helper_metadata["schema"] == HELPER_SCHEMA,
        "helper_input_layout_exact": (
            int(helper_metadata["input_record_bytes"]) == INPUT_DTYPE.itemsize
        ),
        "helper_output_layout_exact": (
            int(helper_metadata["output_record_bytes"]) == OUTPUT_DTYPE.itemsize
        ),
        "source_count_exact": selected.size == EXPECTED_SOURCE_COUNT,
        "role_count_exact": selected.size * 2 == EXPECTED_ROLE_COUNT,
        "reference_bits_exact": reference_bits == EXPECTED_REFERENCE_BITS,
        "helper_frequency_order_exact": bool(
            np.array_equal(
                helper_output["frequency_bits"],
                expected_helper_input["frequency_bits"],
            )
        ),
        "all_values_finite": bool(
            all(
                np.all(np.isfinite(values))
                for values in graph_values.values()
            )
            and np.all(
                np.isfinite(
                    np.ascontiguousarray(
                        helper_output["mulfactor_f64_bits"],
                        dtype=np.uint64,
                    ).view(np.float64)
                )
            )
            and np.all(
                np.isfinite(
                    np.ascontiguousarray(
                        helper_output["power_f32_bits"],
                        dtype=np.uint32,
                    ).view(np.float32)
                )
            )
        ),
    }
    controls_valid = all(current_controls.values()) and all(
        artifact_controls.values()
    )
    classification = classify_ordering(
        controls_valid=controls_valid,
        raw_order_scaled_mismatches=comparisons["casa_order_scaled_aligned"][
            "mismatch_count"
        ],
        raw_order_combined_mismatches=comparisons["casa_order_combined_aligned"][
            "mismatch_count"
        ],
    )
    promoted_candidate = classification == "raw-frame-scale-add-closes-all"

    return {
        "schema": SCHEMA,
        "role": "bounded_offline_correctness_discriminator_not_performance_evidence",
        "instrumentation_valid": controls_valid,
        "classification": classification,
        "experimental_prediction_candidate_authorized": promoted_candidate,
        "production_default_authorized": False,
        "clean_authorized": False,
        "tolerance_change_authorized": False,
        "ui_exposure_authorized": False,
        "source_count": int(selected.size),
        "role_count": int(selected.size * 2),
        "row_identity": row_identity,
        "reference_frequency": {
            "bits_hex": f"{reference_bits:016x}",
            "bits": reference_bits,
            "value_hz": taylor.f64_from_bits(reference_bits),
        },
        "stop_boundary": "graph-combined-prediction-streams-before-residual",
        "promotion_threshold": {
            "casa_order_scaled_mismatches": 0,
            "casa_order_combined_mismatches": 0,
            "exact_frozen_casa_hashes": True,
            "all_controls_valid": True,
        },
        "helper": helper_metadata,
        "helper_stream_hashes": helper_stream_hashes,
        "ordered_stream_hashes": {
            "frequency_hz_f64": freq_f64_hash,
            "freq_f32": freq_f32_hash,
            "mulfactor_f64": mulfactor_f64_hash,
            "power_f32": power_hash,
            "source_keys": source_key_hash(source_trace),
            "source_phasors": hash_array(phasors, "<u4"),
        },
        "controls": {
            "artifact": artifact_controls,
            "current_order": current_controls,
            "all_valid": controls_valid,
        },
        "graph_hashes": {
            name: boundary.hash_parallel_hands(values)
            for name, values in graph_values.items()
        },
        "comparisons": comparisons,
        "source_775_rr": source_775_detail(
            source_trace=source_trace,
            selected=selected,
            output=helper_output,
            reference_bits=reference_bits,
            graph_values=graph_values,
        ),
        "prohibited_execution": {
            "casa_entered": False,
            "ms_opened": False,
            "metal_entered": False,
            "prediction_runtime_executed": False,
            "residual_formed": False,
            "grid_formed": False,
            "fft_entered": False,
            "product_formed": False,
            "controller_entered": False,
            "clean_entered": False,
        },
    }


def checked_json(path: Path, expected_sha256: str | None = None) -> dict[str, Any]:
    if expected_sha256 is not None:
        taylor.checked_input(path, expected_sha256)
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--term-comparison", required=True, type=Path)
    parser.add_argument("--taylor-census", required=True, type=Path)
    parser.add_argument(
        "--taylor-census-sha256",
        default=EXPECTED_TAYLOR_RECEIPT_SHA256,
    )
    parser.add_argument(
        "--rust-source",
        type=Path,
        default=Path(__file__).with_name("vlass_mtmfs_raw_frame_ordering.rs"),
    )
    parser.add_argument("--rustc", default="rustc")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite receipt: {args.output}")

    term_receipt = checked_json(args.term_comparison)
    taylor_receipt = checked_json(
        args.taylor_census,
        args.taylor_census_sha256,
    )
    if term_receipt.get("schema") != EXPECTED_TERM_SCHEMA:
        raise RuntimeError("unexpected term-comparison schema")
    if taylor_receipt.get("schema") != EXPECTED_TAYLOR_SCHEMA:
        raise RuntimeError("unexpected Taylor-census schema")
    if (
        taylor_receipt["inputs"]["term_comparison_sha256"]
        != boundary.sha256_file(args.term_comparison)
    ):
        raise RuntimeError("Taylor census binds a different term comparison")

    inputs = term_receipt["inputs"]
    for name in (
        "host_receipt",
        "binary",
        "casa_npz",
        "casars_source_trace",
        "casars_sidecar_host",
        "casars_sidecar_comparison",
    ):
        taylor.checked_input(Path(inputs[name]), str(inputs[f"{name}_sha256"]))

    records = np.fromfile(inputs["binary"], dtype=term_compare.CASA_DTYPE)
    with np.load(inputs["casa_npz"]) as loaded:
        casa_trace = {name: np.asarray(loaded[name]) for name in loaded.files}
    source_trace = checked_json(Path(inputs["casars_source_trace"]))
    selected, row_identity = term_compare.select_source_records(
        records,
        source_trace,
        casa_trace,
    )
    reference_bits = int(taylor_receipt["reference_frequency"]["bits"])
    raw_tt0 = term_compare.complex_boundary(selected, "tt0")
    raw_tt1 = term_compare.complex_boundary(selected, "tt1_raw")
    helper_input = build_input_records(
        frequencies=np.asarray(selected["frequency_hz"], dtype=np.float64),
        raw_tt0=raw_tt0,
        raw_tt1=raw_tt1,
        source_trace=source_trace,
    )

    with tempfile.TemporaryDirectory(prefix="vlass-mtmfs-ordering-") as directory:
        temporary = Path(directory)
        helper, helper_metadata = compile_helper(
            rustc=args.rustc,
            source=args.rust_source,
            temporary=temporary,
        )
        helper_output, helper_stream_hashes = run_helper(
            helper=helper,
            records=helper_input,
            reference_bits=reference_bits,
            temporary=temporary,
        )
        result = analyze(
            term_receipt=term_receipt,
            taylor_receipt=taylor_receipt,
            selected=selected,
            source_trace=source_trace,
            helper_output=helper_output,
            helper_metadata=helper_metadata,
            helper_stream_hashes=helper_stream_hashes,
            reference_bits=reference_bits,
            row_identity=row_identity,
        )

    result["inputs"] = {
        "term_comparison": str(args.term_comparison),
        "term_comparison_sha256": boundary.sha256_file(args.term_comparison),
        "taylor_census": str(args.taylor_census),
        "taylor_census_sha256": boundary.sha256_file(args.taylor_census),
        "rust_source": str(args.rust_source),
        "rust_source_sha256": boundary.sha256_file(args.rust_source),
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
