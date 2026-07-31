#!/usr/bin/env python3
"""Classify CASA MT-MFS Taylor-power operation graphs from frozen evidence.

This is a bounded offline correctness diagnostic.  It compiles a tiny C++
helper for the scalar ``pow(Float, Int)`` overload census, but does not open a
MeasurementSet or enter CASA, Metal, gridding, FFT, product, or CLEAN paths.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import numpy as np

import vlass_casa_mtmfs_term_degrid_compare as term_compare
import vlass_prediction_boundary_hash_compare as boundary
import vlass_prediction_sidecar_compare as sidecar


EXPECTED_TERM_SCHEMA = "casa-rs-vlass-term-separated-prediction-comparison-v1"
EXPECTED_SOURCE_COUNT = 98_239
EXPECTED_ROLE_COUNT = 196_478
EXPECTED_TT0_HASH = (
    "0dee472a2f19ea3f03af86442f086d383980853390ecd5188066f5e8f4b1cedb"
)
EXPECTED_TT1_HASH = (
    "950dc856071fb616be4d130b925d61b5d107b7580edf92b55771312ce3381545"
)
EXPECTED_CURRENT_MISMATCHES = {
    "power": 71_588,
    "scaled_tt1": 230,
    "combined": 434,
}
HELPER_DTYPE = np.dtype(
    [
        ("frequency_bits", "<u8"),
        ("freq_f32_bits", "<u4"),
        ("delta_bits", "<u8"),
        ("ratio_bits", "<u8"),
        ("x_bits", "<u4"),
        ("source_bits", "<u4"),
        ("casacore_bits", "<u4"),
        ("standard_bits", "<u4"),
    ],
    align=False,
)
REFERENCE_BITS_PATTERN = re.compile(r"\breffreq_bits=([0-9a-fA-F]{16})\b")
FROZEN_MODEL_PATTERN = re.compile(
    r"^awproject_frozen_model_refresh prefix=(.+?) terms=2 "
    r"image_shape=4096x4096$",
    re.MULTILINE,
)


def checked_input(path: Path, expected_sha256: str) -> None:
    if not path.exists():
        raise RuntimeError(f"frozen input does not exist: {path}")
    actual = boundary.sha256_file(path)
    if actual != expected_sha256:
        raise RuntimeError(
            f"frozen input hash differs for {path}: {actual} != {expected_sha256}"
        )


def hash_f32(values: np.ndarray) -> str:
    array = np.ascontiguousarray(values, dtype="<f4")
    return hashlib.sha256(array.tobytes(order="C")).hexdigest()


def bits_f32(values: np.ndarray) -> np.ndarray:
    return np.ascontiguousarray(values, dtype=np.float32).view(np.uint32)


def values_f32(bits: np.ndarray) -> np.ndarray:
    return np.ascontiguousarray(bits, dtype=np.uint32).view(np.float32)


def f64_from_bits(bits: int) -> float:
    return float(np.asarray([bits], dtype=np.uint64).view(np.float64)[0])


def bits_f64(value: float) -> int:
    return int(np.asarray([value], dtype=np.float64).view(np.uint64)[0])


def parse_reference_receipt(
    path: Path,
    expected_sha256: str,
) -> tuple[int, list[str]]:
    checked_input(path, expected_sha256)
    text = path.read_text(encoding="utf-8")
    reference_bits = {
        int(match, 16) for match in REFERENCE_BITS_PATTERN.findall(text)
    }
    if len(reference_bits) != 1:
        raise RuntimeError(
            "reference receipt does not bind exactly one reference-frequency value"
        )
    prefixes = sorted(set(FROZEN_MODEL_PATTERN.findall(text)))
    if len(prefixes) != 1:
        raise RuntimeError(
            "reference receipt does not bind exactly one frozen 4096 model prefix"
        )
    prefix = Path(prefixes[0])
    for term in (0, 1):
        product = Path(f"{prefix}.model.tt{term}")
        if not product.is_dir():
            raise RuntimeError(f"frozen model product is unavailable: {product}")
    return next(iter(reference_bits)), prefixes


def compile_helper(
    *,
    cxx: str,
    helper_source: Path,
    casacore_include_root: Path,
    expected_math_header_sha256: str,
    temporary: Path,
) -> tuple[Path, dict[str, Any]]:
    math_header = (
        casacore_include_root / "casacore" / "casa" / "BasicMath" / "Math.h"
    )
    checked_input(math_header, expected_math_header_sha256)
    helper = temporary / "vlass_taylor_power_graphs"
    command = [
        cxx,
        "-std=c++17",
        "-O3",
        "-fno-fast-math",
        f"-I{casacore_include_root}",
        str(helper_source),
        "-o",
        str(helper),
    ]
    subprocess.run(command, check=True, capture_output=True, text=True)
    describe = subprocess.run(
        [str(helper), "--describe"],
        check=True,
        capture_output=True,
        text=True,
    )
    compiler = subprocess.run(
        [cxx, "--version"],
        check=True,
        capture_output=True,
        text=True,
    )
    linkage = subprocess.run(
        ["otool", "-L", str(helper)],
        check=True,
        capture_output=True,
        text=True,
    )
    metadata = json.loads(describe.stdout)
    metadata.update(
        {
            "compiler": compiler.stdout.strip(),
            "command": command,
            "helper_source": str(helper_source),
            "helper_source_sha256": boundary.sha256_file(helper_source),
            "helper_executable_sha256": boundary.sha256_file(helper),
            "casacore_math_header": str(math_header),
            "casacore_math_header_sha256": boundary.sha256_file(math_header),
            "linkage": linkage.stdout.strip(),
        }
    )
    return helper, metadata


def run_helper(
    *,
    helper: Path,
    frequencies: np.ndarray,
    reference_bits: int,
    temporary: Path,
) -> tuple[np.ndarray, dict[str, str]]:
    input_path = temporary / "frequencies-f64.bin"
    output_path = temporary / "graphs.bin"
    np.ascontiguousarray(frequencies, dtype="<f8").tofile(input_path)
    subprocess.run(
        [
            str(helper),
            str(input_path),
            str(output_path),
            f"{reference_bits:016x}",
            "1",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    records = np.fromfile(output_path, dtype=HELPER_DTYPE)
    if records.size != frequencies.size:
        raise RuntimeError("C++ helper output count differs from frozen sources")
    return records, {
        "frequency_input_sha256": boundary.sha256_file(input_path),
        "graph_output_sha256": boundary.sha256_file(output_path),
    }


def separately_scale_pairs(values: np.ndarray, powers: np.ndarray) -> np.ndarray:
    output = np.empty_like(values)
    for role in range(2):
        real = np.asarray(values[:, role].real, dtype=np.float32)
        imag = np.asarray(values[:, role].imag, dtype=np.float32)
        scaled_real = np.asarray(real * powers, dtype=np.float32)
        scaled_imag = np.asarray(imag * powers, dtype=np.float32)
        output[:, role].real = scaled_real
        output[:, role].imag = scaled_imag
    return output


def separately_add_pairs(left: np.ndarray, right: np.ndarray) -> np.ndarray:
    output = np.empty_like(left)
    for role in range(2):
        output[:, role].real = np.asarray(
            np.asarray(left[:, role].real, dtype=np.float32)
            + np.asarray(right[:, role].real, dtype=np.float32),
            dtype=np.float32,
        )
        output[:, role].imag = np.asarray(
            np.asarray(left[:, role].imag, dtype=np.float32)
            + np.asarray(right[:, role].imag, dtype=np.float32),
            dtype=np.float32,
        )
    return output


def pair_mismatch_count(actual: np.ndarray, expected: np.ndarray) -> int:
    actual_bits = actual.view(np.float32).reshape(actual.shape + (2,)).view(np.uint32)
    expected_bits = (
        expected.view(np.float32).reshape(expected.shape + (2,)).view(np.uint32)
    )
    return int(np.count_nonzero(np.any(actual_bits != expected_bits, axis=2)))


def ordered_f32_bits(bits: np.ndarray) -> np.ndarray:
    unsigned = np.asarray(bits, dtype=np.uint32)
    return np.where(
        unsigned & np.uint32(0x8000_0000),
        np.bitwise_not(unsigned),
        unsigned | np.uint32(0x8000_0000),
    ).astype(np.uint64)


def maximum_ulp_distance(actual: np.ndarray, expected: np.ndarray) -> int:
    actual_ordered = ordered_f32_bits(bits_f32(actual))
    expected_ordered = ordered_f32_bits(bits_f32(expected))
    distance = np.maximum(actual_ordered, expected_ordered) - np.minimum(
        actual_ordered,
        expected_ordered,
    )
    return int(np.max(distance, initial=0))


def source_neighbors(
    source_trace: dict[str, Any],
    ordinal: int | None,
    role: int = 0,
) -> dict[str, Any] | None:
    if ordinal is None:
        return None
    result: dict[str, Any] = {
        "current": sidecar.source_context(source_trace, ordinal, role),
    }
    if ordinal > 0:
        result["previous"] = sidecar.source_context(source_trace, ordinal - 1, role)
    if ordinal + 1 < len(source_trace["samples"]):
        result["next"] = sidecar.source_context(source_trace, ordinal + 1, role)
    return result


def graph_result(
    *,
    powers: np.ndarray,
    casa_powers: np.ndarray,
    tt0: np.ndarray,
    tt1: np.ndarray,
    casa_scaled: np.ndarray,
    casa_combined: np.ndarray,
    source_trace: dict[str, Any],
) -> tuple[dict[str, Any], np.ndarray, np.ndarray]:
    scaled = separately_scale_pairs(tt1, powers)
    combined = separately_add_pairs(tt0, scaled)
    power_mismatches = np.flatnonzero(bits_f32(powers) != bits_f32(casa_powers))
    first_scaled = sidecar.first_pair_mismatch(scaled, casa_scaled)
    first_combined = sidecar.first_pair_mismatch(combined, casa_combined)
    return (
        {
            "power_sha256": hash_f32(powers),
            "power_exact_count": int(powers.size - power_mismatches.size),
            "power_mismatch_count": int(power_mismatches.size),
            "power_maximum_ulp_distance": maximum_ulp_distance(
                powers,
                casa_powers,
            ),
            "first_power_mismatch": source_neighbors(
                source_trace,
                int(power_mismatches[0]) if power_mismatches.size else None,
            ),
            "scaled_tt1_sha256": boundary.hash_parallel_hands(scaled),
            "scaled_tt1_mismatch_count": pair_mismatch_count(
                scaled,
                casa_scaled,
            ),
            "first_scaled_tt1_mismatch": (
                {
                    "role": "rr" if first_scaled[1] == 0 else "ll",
                    "sources": source_neighbors(
                        source_trace,
                        first_scaled[0],
                        first_scaled[1],
                    ),
                }
                if first_scaled is not None
                else None
            ),
            "combined_sha256": boundary.hash_parallel_hands(combined),
            "combined_mismatch_count": pair_mismatch_count(
                combined,
                casa_combined,
            ),
            "first_combined_mismatch": (
                {
                    "role": "rr" if first_combined[1] == 0 else "ll",
                    "sources": source_neighbors(
                        source_trace,
                        first_combined[0],
                        first_combined[1],
                    ),
                }
                if first_combined is not None
                else None
            ),
        },
        scaled,
        combined,
    )


def closes_all(result: dict[str, Any]) -> bool:
    return (
        result["power_mismatch_count"] == 0
        and result["scaled_tt1_mismatch_count"] == 0
        and result["combined_mismatch_count"] == 0
    )


def classify(
    graph_results: dict[str, dict[str, Any]],
    equality: dict[str, bool],
    helper: dict[str, Any],
) -> str:
    closing = [name for name, result in graph_results.items() if closes_all(result)]
    distinct_closing_hashes = {
        graph_results[name]["power_sha256"] for name in closing
    }
    if len(distinct_closing_hashes) > 1:
        return "multiple-graphs-close-all"
    if closes_all(graph_results["identity"]):
        return "identity-closes-all"
    if closes_all(graph_results["late_frequency_cast"]):
        return "late-cast-closes-all"
    if (
        closes_all(graph_results["source"])
        and equality["source==casacore"]
        and helper["source_expression_is_float"]
    ):
        return "source-casacore-pow-closes-all"
    if (
        closes_all(graph_results["source"])
        and equality["source==standard"]
        and not equality["source==casacore"]
    ):
        return "source-standard-pow-closes-all"
    if any(
        result["power_mismatch_count"] == 0
        for result in graph_results.values()
    ):
        return "power-exact-downstream-different"
    return "no-candidate-closes-all"


def analyze(
    *,
    term_receipt: dict[str, Any],
    casa_records: np.ndarray,
    casa_trace: dict[str, np.ndarray],
    source_trace: dict[str, Any],
    audit: np.ndarray,
    helper_records: np.ndarray,
    helper_metadata: dict[str, Any],
    reference_bits: int,
    reference_prefixes: list[str],
    helper_hashes: dict[str, str],
) -> dict[str, Any]:
    selected, row_identity = term_compare.select_source_records(
        casa_records,
        source_trace,
        casa_trace,
    )
    frequencies = np.asarray(selected["frequency_hz"], dtype=np.float64)
    if not np.array_equal(
        frequencies.view(np.uint64),
        helper_records["frequency_bits"],
    ):
        raise RuntimeError("C++ helper frequency order differs from CASA oracle")

    casa_tt0 = term_compare.phase_rotate_pairs(
        term_compare.complex_boundary(selected, "tt0"),
        source_trace,
    )
    casa_tt1 = term_compare.phase_rotate_pairs(
        term_compare.complex_boundary(selected, "tt1_raw"),
        source_trace,
    )
    casa_scaled = term_compare.phase_rotate_pairs(
        term_compare.complex_boundary(selected, "tt1_scaled"),
        source_trace,
    )
    casa_combined = term_compare.phase_rotate_pairs(
        term_compare.complex_boundary(selected, "combined"),
        source_trace,
    )
    casa_powers = np.asarray(selected["taylor_power1"], dtype=np.float32)
    audit_powers = np.asarray(audit["taylor_power1"], dtype=np.float32)
    x = values_f32(helper_records["x_bits"])
    late_x = np.asarray(
        (frequencies - f64_from_bits(reference_bits))
        / f64_from_bits(reference_bits),
        dtype=np.float32,
    )
    graphs = {
        "source": values_f32(helper_records["source_bits"]),
        "casacore": values_f32(helper_records["casacore_bits"]),
        "standard": values_f32(helper_records["standard_bits"]),
        "identity": x,
        "late_frequency_cast": late_x,
    }
    results: dict[str, dict[str, Any]] = {}
    for name, powers in graphs.items():
        result, _, _ = graph_result(
            powers=powers,
            casa_powers=casa_powers,
            tt0=casa_tt0,
            tt1=casa_tt1,
            casa_scaled=casa_scaled,
            casa_combined=casa_combined,
            source_trace=source_trace,
        )
        results[name] = result

    equality: dict[str, bool] = {}
    names = list(graphs)
    for left_index, left in enumerate(names):
        for right in names[left_index + 1 :]:
            equality[f"{left}=={right}"] = bool(
                np.array_equal(bits_f32(graphs[left]), bits_f32(graphs[right]))
            )

    tt0_hash = boundary.hash_parallel_hands(casa_tt0)
    tt1_hash = boundary.hash_parallel_hands(casa_tt1)
    current_control_valid = all(
        (
            np.array_equal(bits_f32(graphs["identity"]), bits_f32(audit_powers)),
            results["identity"]["power_mismatch_count"]
            == EXPECTED_CURRENT_MISMATCHES["power"],
            results["identity"]["scaled_tt1_mismatch_count"]
            == EXPECTED_CURRENT_MISMATCHES["scaled_tt1"],
            results["identity"]["combined_mismatch_count"]
            == EXPECTED_CURRENT_MISMATCHES["combined"],
        )
    )
    instrumentation_valid = all(
        (
            selected.size == EXPECTED_SOURCE_COUNT,
            selected.size * 2 == EXPECTED_ROLE_COUNT,
            audit.size == EXPECTED_SOURCE_COUNT,
            helper_records.size == EXPECTED_SOURCE_COUNT,
            term_receipt["instrumentation_valid"],
            term_receipt["classification"] == "taylor-power-difference",
            tt0_hash == EXPECTED_TT0_HASH,
            tt1_hash == EXPECTED_TT1_HASH,
            np.all(np.isfinite(frequencies)),
            np.all(np.isfinite(casa_powers)),
            np.all(np.isfinite(casa_tt0)),
            np.all(np.isfinite(casa_tt1)),
            np.all(np.isfinite(casa_scaled)),
            np.all(np.isfinite(casa_combined)),
            (
                helper_metadata["source_expression_is_float"]
                or helper_metadata["source_expression_is_double"]
            ),
            helper_metadata["source_expression_size"] in (4, 8),
            current_control_valid,
        )
    )
    classification = (
        classify(results, equality, helper_metadata)
        if instrumentation_valid
        else "invalid-artifact-or-operation-contract"
    )

    source_775 = {
        "source": sidecar.source_context(source_trace, 775, 0),
        "previous": sidecar.source_context(source_trace, 774, 0),
        "next": sidecar.source_context(source_trace, 776, 0),
        "frequency_bits": int(helper_records["frequency_bits"][775]),
        "freq_f32_bits": int(helper_records["freq_f32_bits"][775]),
        "reference_frequency_bits": reference_bits,
        "delta_bits": int(helper_records["delta_bits"][775]),
        "ratio_bits": int(helper_records["ratio_bits"][775]),
        "x_bits": int(helper_records["x_bits"][775]),
        "casa_power_bits": int(bits_f32(casa_powers)[775]),
        "audit_power_bits": int(bits_f32(audit_powers)[775]),
        "graph_power_bits": {
            name: int(bits_f32(values)[775]) for name, values in graphs.items()
        },
        "tt0_bits": sidecar.bit_values(casa_tt0, 775, 0),
        "tt1_bits": sidecar.bit_values(casa_tt1, 775, 0),
        "casa_scaled_tt1_bits": sidecar.bit_values(casa_scaled, 775, 0),
        "casa_combined_bits": sidecar.bit_values(casa_combined, 775, 0),
    }

    return {
        "schema": "casa-rs-vlass-taylor-power-graph-census-v1",
        "role": "bounded_offline_correctness_discriminator_not_performance_evidence",
        "instrumentation_valid": instrumentation_valid,
        "classification": classification,
        "promotion_threshold": {
            "unique_source_plausible_graph": True,
            "power_mismatches": 0,
            "scaled_tt1_mismatches": 0,
            "combined_mismatches": 0,
        },
        "stop_boundary": "combined-prediction-stream-before-residual",
        "source_count": int(selected.size),
        "role_count": int(selected.size * 2),
        "row_identity": row_identity,
        "reference_frequency": {
            "bits": reference_bits,
            "value_hz": f64_from_bits(reference_bits),
            "frozen_model_prefixes": reference_prefixes,
        },
        "helper": helper_metadata,
        "helper_stream_hashes": helper_hashes,
        "controls": {
            "current_control_valid": current_control_valid,
            "current_audit_power_sha256": hash_f32(audit_powers),
            "aligned_tt0_sha256": tt0_hash,
            "aligned_tt1_sha256": tt1_hash,
            "casa_power_sha256": hash_f32(casa_powers),
            "casa_scaled_tt1_sha256": boundary.hash_parallel_hands(casa_scaled),
            "casa_combined_sha256": boundary.hash_parallel_hands(casa_combined),
            "all_values_finite": True,
            "prohibited_stages_entered": False,
        },
        "graphs": results,
        "graph_stream_equality": equality,
        "source_775_rr": source_775,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--term-comparison", required=True, type=Path)
    parser.add_argument("--reference-receipt", required=True, type=Path)
    parser.add_argument("--reference-receipt-sha256", required=True)
    parser.add_argument("--casacore-include-root", required=True, type=Path)
    parser.add_argument("--casacore-math-header-sha256", required=True)
    parser.add_argument(
        "--helper-source",
        type=Path,
        default=Path(__file__).with_name("vlass_taylor_power_graphs.cc"),
    )
    parser.add_argument("--cxx", default="clang++")
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

    reference_bits, reference_prefixes = parse_reference_receipt(
        args.reference_receipt,
        args.reference_receipt_sha256,
    )
    casa_host = json.loads(Path(inputs["host_receipt"]).read_text(encoding="utf-8"))
    casa_records = np.fromfile(inputs["binary"], dtype=term_compare.CASA_DTYPE)
    if casa_records.nbytes != int(casa_host["binary_bytes"]):
        raise RuntimeError("CASA term binary byte length differs from host receipt")
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
    selected, _ = term_compare.select_source_records(
        casa_records,
        source_trace,
        casa_trace,
    )

    with tempfile.TemporaryDirectory(prefix="vlass-taylor-power-") as directory:
        temporary = Path(directory)
        helper, helper_metadata = compile_helper(
            cxx=args.cxx,
            helper_source=args.helper_source,
            casacore_include_root=args.casacore_include_root,
            expected_math_header_sha256=args.casacore_math_header_sha256,
            temporary=temporary,
        )
        helper_records, helper_hashes = run_helper(
            helper=helper,
            frequencies=np.asarray(selected["frequency_hz"], dtype=np.float64),
            reference_bits=reference_bits,
            temporary=temporary,
        )
        result = analyze(
            term_receipt=term_receipt,
            casa_records=casa_records,
            casa_trace=casa_trace,
            source_trace=source_trace,
            audit=audit,
            helper_records=helper_records,
            helper_metadata=helper_metadata,
            reference_bits=reference_bits,
            reference_prefixes=reference_prefixes,
            helper_hashes=helper_hashes,
        )

    result["inputs"] = {
        "term_comparison": str(args.term_comparison),
        "term_comparison_sha256": boundary.sha256_file(args.term_comparison),
        "reference_receipt": str(args.reference_receipt),
        "reference_receipt_sha256": boundary.sha256_file(args.reference_receipt),
        "helper_source": str(args.helper_source),
        "helper_source_sha256": boundary.sha256_file(args.helper_source),
        "casacore_include_root": str(args.casacore_include_root),
        "casacore_math_header_sha256": args.casacore_math_header_sha256,
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
