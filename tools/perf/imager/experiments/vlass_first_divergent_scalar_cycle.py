#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Find the first honest CASA/casa-rs VLASS scalar-state divergence.

Only two complete CASA cycles and the start of a third survive at full
precision.  The 171-cycle receipt is useful for discrete-trajectory context,
but its CASA numerical values were parsed from rounded log text and cannot be
used to locate a bit-level numerical divergence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import struct
from pathlib import Path
from typing import Any


SCHEMA = "casa-rs-vlass-first-divergent-scalar-cycle-v1"
CASA_TRACE_KIND = "vlass_reduced_casa_clean_component_trace"
CASA_FULL_KIND = "vlass_reduced_casa_clean_correctness_oracle"
MAJOR_TRACE_KIND = "vlass_clean_major_cycle_trace_comparison"
CONTROL_TRACE_KIND = "vlass_clean_casars_trace_comparison"
EXPECTED_CYCLES = 171
COMPLETE_EXACT_CYCLES = 2
EXACT_START_CYCLES = 3
PARAMETER_DIFFERENCES = frozenset(
    {
        "cycleniter",
        "fullsummary",
        "imagename",
        "niter",
    }
)

RUST_MINOR_RE = re.compile(
    r"^mosaic_mtmfs_minor_cycle "
    r"cycle=(?P<cycle>\d+) "
    r"start_iteration=(?P<start_iteration>\d+) "
    r"reported_updates=(?P<reported_updates>\d+) "
    r"actual_updates=(?P<actual_updates>\d+) "
    r"start_peak=(?P<start_peak>[-+0-9.eE]+) "
    r"approximate_end_peak=(?P<unmasked_end_peak>[-+0-9.eE]+) "
    r"cycle_threshold=(?P<cycle_threshold>[-+0-9.eE]+) "
    r"nsigma_threshold=(?P<nsigma_threshold>[-+0-9.eE]+) "
    r"model_flux=(?P<model_flux>[-+0-9.eE]+) "
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"{path} does not contain a JSON object")
    return value


def read_last_json_object(path: Path) -> dict[str, Any]:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    for line in reversed(lines):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise RuntimeError(f"{path} contains no JSON object line")


def write_json_new(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")


def require_sha256(path: Path, expected: str, label: str) -> str:
    actual = sha256_file(path)
    if actual != expected:
        raise RuntimeError(
            f"{label} SHA-256 differs: expected {expected}, observed {actual}"
        )
    return actual


def f32_bits(value: float) -> int:
    return struct.unpack("<I", struct.pack("<f", value))[0]


def f32_value(value: float) -> float:
    return struct.unpack("<f", struct.pack("<f", value))[0]


def compare_f32_metric(
    *,
    cycle: int,
    name: str,
    casa_value: float,
    rust_text: str,
) -> dict[str, Any]:
    rust_value = float(rust_text)
    if not math.isfinite(casa_value) or not math.isfinite(rust_value):
        raise RuntimeError(f"non-finite {name} at cycle {cycle}")
    casa_bits = f32_bits(casa_value)
    rust_bits = f32_bits(rust_value)
    casa_f32 = f32_value(casa_value)
    rust_f32 = f32_value(rust_value)
    absolute_difference = abs(casa_f32 - rust_f32)
    scale = max(abs(casa_f32), abs(rust_f32), 1.0e-30)
    return {
        "cycle": cycle,
        "metric": name,
        "casa_source_f64": casa_value,
        "casa_f32": casa_f32,
        "casa_f32_bits": casa_bits,
        "rust_source_text": rust_text,
        "rust_f32": rust_f32,
        "rust_f32_bits": rust_bits,
        "f32_equal": casa_bits == rust_bits,
        "ulp_distance": abs(casa_bits - rust_bits),
        "absolute_difference": absolute_difference,
        "relative_difference": absolute_difference / scale,
    }


def comparable_parameters(
    parameters: dict[str, Any],
) -> dict[str, Any]:
    return {
        key: value
        for key, value in parameters.items()
        if key not in PARAMETER_DIFFERENCES
    }


def parameters_match(
    trace_parameters: dict[str, Any],
    full_parameters: dict[str, Any],
) -> bool:
    return comparable_parameters(trace_parameters) == comparable_parameters(
        full_parameters
    )


def parse_rust_minor_cycles(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = RUST_MINOR_RE.match(line)
        if match is None:
            continue
        row: dict[str, Any] = {key: value for key, value in match.groupdict().items()}
        for key in (
            "cycle",
            "start_iteration",
            "reported_updates",
            "actual_updates",
        ):
            row[key] = int(row[key])
        rows.append(row)
    return rows


def exact_casa_rows(receipt: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        summary = receipt["summary"]
        minor = summary["summaryminor"]["0"]["0"]["0"]
    except (KeyError, TypeError) as error:
        raise RuntimeError("CASA trace receipt lacks scalar fullsummary") from error
    required = (
        "cycleStartIters",
        "cycleThresh",
        "iterDone",
        "modelFlux",
        "peakResNM",
        "startIterDone",
        "startPeakRes",
        "stopCode",
    )
    if any(
        not isinstance(minor.get(key), list) or len(minor[key]) != EXACT_START_CYCLES
        for key in required
    ):
        raise RuntimeError("CASA exact scalar window is not three cycles")
    return [
        {
            "cycle": index,
            "start_iteration": int(minor["startIterDone"][index]),
            "cycle_start_iteration": int(minor["cycleStartIters"][index]),
            "reported_updates": int(minor["iterDone"][index]),
            "start_peak": float(minor["startPeakRes"][index]),
            "cycle_threshold": float(minor["cycleThresh"][index]),
            "unmasked_end_peak": float(minor["peakResNM"][index]),
            "model_flux": float(minor["modelFlux"][index]),
            "stop_code": int(minor["stopCode"][index]),
        }
        for index in range(EXACT_START_CYCLES)
    ]


def validate_context(
    *,
    casa_trace: dict[str, Any],
    casa_full: dict[str, Any],
    major_trace: dict[str, Any],
    control_trace: dict[str, Any],
    rust_rows: list[dict[str, Any]],
    rust_log: Path,
) -> dict[str, bool]:
    checks = {
        "casa_trace_kind": casa_trace.get("kind") == CASA_TRACE_KIND,
        "casa_full_kind": casa_full.get("kind") == CASA_FULL_KIND,
        "parameter_identity": parameters_match(
            casa_trace.get("parameters", {}),
            casa_full.get("parameters", {}),
        ),
        "trace_niter": casa_trace.get("parameters", {}).get("niter") == 270,
        "full_niter": casa_full.get("parameters", {}).get("niter") == 2000,
        "trace_fullsummary": (
            casa_trace.get("parameters", {}).get("fullsummary") is True
        ),
        "major_trace_kind": major_trace.get("kind") == MAJOR_TRACE_KIND,
        "major_trace_cycles": (
            major_trace.get("casa_cycles") == EXPECTED_CYCLES
            and major_trace.get("rust_cycles") == EXPECTED_CYCLES
            and major_trace.get("aligned_cycles") == EXPECTED_CYCLES
        ),
        "major_trace_discrete_parity": (
            major_trace.get("discrete_parity", {}).get("status") == "passed"
            and major_trace.get("discrete_parity", {}).get("mismatch_count") == 0
        ),
        "control_trace_kind": control_trace.get("kind") == CONTROL_TRACE_KIND,
        "control_minor_discrete_parity": (
            control_trace.get("minor", {}).get("discrete_parity") is True
            and control_trace.get("minor", {}).get("candidate_count") == EXPECTED_CYCLES
            and control_trace.get("minor", {}).get("reference_count") == EXPECTED_CYCLES
        ),
        "control_refresh_discrete_parity": (
            control_trace.get("refresh", {}).get("discrete_parity") is True
        ),
        "control_final_iteration_parity": (
            control_trace.get("final_refresh_iteration_parity") is True
        ),
        "control_candidate_log": (
            Path(control_trace.get("candidate_log", "")).name == rust_log.name
        ),
        "rust_cycle_count": len(rust_rows) == EXPECTED_CYCLES,
        "rust_cycle_indices": all(
            row["cycle"] == index for index, row in enumerate(rust_rows)
        ),
    }
    if not all(checks.values()):
        failed = sorted(key for key, passed in checks.items() if not passed)
        raise RuntimeError(f"frozen scalar context is invalid: {failed}")
    return checks


def build_exact_window(
    casa_rows: list[dict[str, Any]],
    rust_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    discrete: list[dict[str, Any]] = []
    numerical: list[dict[str, Any]] = []
    for cycle in range(EXACT_START_CYCLES):
        casa = casa_rows[cycle]
        rust = rust_rows[cycle]
        fields = ("start_iteration", "cycle_start_iteration")
        for field in fields:
            rust_field = (
                "start_iteration" if field == "cycle_start_iteration" else field
            )
            discrete.append(
                {
                    "cycle": cycle,
                    "field": field,
                    "casa": casa[field],
                    "rust": rust[rust_field],
                    "equal": casa[field] == rust[rust_field],
                }
            )
        if cycle < COMPLETE_EXACT_CYCLES:
            for field in ("reported_updates",):
                discrete.append(
                    {
                        "cycle": cycle,
                        "field": field,
                        "casa": casa[field],
                        "rust": rust[field],
                        "equal": casa[field] == rust[field],
                    }
                )
            discrete.append(
                {
                    "cycle": cycle,
                    "field": "actual_updates_equal_reported",
                    "casa": casa["reported_updates"],
                    "rust": rust["actual_updates"],
                    "equal": casa["reported_updates"] == rust["actual_updates"],
                }
            )
        metrics = ["start_peak", "cycle_threshold"]
        if cycle < COMPLETE_EXACT_CYCLES:
            metrics.extend(("unmasked_end_peak", "model_flux"))
        for metric in metrics:
            numerical.append(
                compare_f32_metric(
                    cycle=cycle,
                    name=metric,
                    casa_value=casa[metric],
                    rust_text=rust[metric],
                )
            )
    return discrete, numerical


def classify(
    discrete: list[dict[str, Any]],
    numerical: list[dict[str, Any]],
) -> str:
    if any(item.get("equal") is not True for item in discrete):
        return "invalid-discrete-trajectory"
    mismatches = [item for item in numerical if item.get("f32_equal") is False]
    if not mismatches:
        return "no-divergence-in-exact-scalar-window"
    first_cycle = int(mismatches[0]["cycle"])
    first_metric = str(mismatches[0]["metric"])
    if first_cycle == 0 and first_metric in ("start_peak", "cycle_threshold"):
        return "diverges-at-cycle-0-input-scalar"
    if first_cycle == 0:
        return "diverges-within-cycle-0-scalars"
    return "diverges-after-cycle-0-scalars"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--casa-trace-log", required=True, type=Path)
    parser.add_argument("--casa-trace-sha256", required=True)
    parser.add_argument("--casa-full-log", required=True, type=Path)
    parser.add_argument("--casa-full-sha256", required=True)
    parser.add_argument("--rust-log", required=True, type=Path)
    parser.add_argument("--rust-log-sha256", required=True)
    parser.add_argument("--major-trace", required=True, type=Path)
    parser.add_argument("--major-trace-sha256", required=True)
    parser.add_argument("--control-trace", required=True, type=Path)
    parser.add_argument("--control-trace-sha256", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    paths = {
        "casa_trace_log": args.casa_trace_log.resolve(),
        "casa_full_log": args.casa_full_log.resolve(),
        "rust_log": args.rust_log.resolve(),
        "major_trace": args.major_trace.resolve(),
        "control_trace": args.control_trace.resolve(),
    }
    expected_hashes = {
        "casa_trace_log": args.casa_trace_sha256,
        "casa_full_log": args.casa_full_sha256,
        "rust_log": args.rust_log_sha256,
        "major_trace": args.major_trace_sha256,
        "control_trace": args.control_trace_sha256,
    }
    identities = {
        label: {
            "path": str(path),
            "sha256": require_sha256(path, expected_hashes[label], label),
        }
        for label, path in paths.items()
    }

    casa_trace = read_last_json_object(paths["casa_trace_log"])
    casa_full = read_last_json_object(paths["casa_full_log"])
    major_trace = read_json(paths["major_trace"])
    control_trace = read_json(paths["control_trace"])
    rust_rows = parse_rust_minor_cycles(paths["rust_log"])
    context_checks = validate_context(
        casa_trace=casa_trace,
        casa_full=casa_full,
        major_trace=major_trace,
        control_trace=control_trace,
        rust_rows=rust_rows,
        rust_log=paths["rust_log"],
    )
    casa_rows = exact_casa_rows(casa_trace)
    discrete, numerical = build_exact_window(casa_rows, rust_rows)
    classification = classify(discrete, numerical)
    mismatches = [item for item in numerical if item["f32_equal"] is False]
    exact_matches = [item for item in numerical if item["f32_equal"] is True]

    receipt = {
        "schema": SCHEMA,
        "role": "offline-frozen-correctness-localization-only",
        "classification": classification,
        "inputs": identities,
        "context_checks": context_checks,
        "coverage": {
            "casa_exact_complete_cycles": COMPLETE_EXACT_CYCLES,
            "casa_exact_start_boundaries": EXACT_START_CYCLES,
            "rounded_context_cycles": EXPECTED_CYCLES,
            "rust_cycles": len(rust_rows),
            "classification_uses_rounded_values": False,
        },
        "semantic_mapping": {
            "start_peak": "CASA startPeakRes versus casa-rs masked start_peak",
            "cycle_threshold": ("CASA cycleThresh versus casa-rs cycle_threshold"),
            "unmasked_end_peak": ("CASA peakResNM versus casa-rs approximate_end_peak"),
            "model_flux": "CASA modelFlux versus casa-rs model_flux",
            "excluded": [
                (
                    "CASA peakRes is masked and is not compared with the "
                    "casa-rs full-plane approximate_end_peak"
                ),
                (
                    "cycle 2 post-minor fields are excluded because the "
                    "niter=270 CASA trace truncated that cycle after one update"
                ),
                (
                    "rounded 171-cycle CASA numerics are context only and "
                    "cannot establish a bit-level first divergence"
                ),
            ],
        },
        "discrete": {
            "all_equal": all(item["equal"] for item in discrete),
            "comparisons": discrete,
        },
        "numerical": {
            "comparison_domain": (
                "CASA fullsummary values rounded once to f32; casa-rs "
                "shortest-roundtrip decimal parsed and rounded once to f32"
            ),
            "comparison_count": len(numerical),
            "exact_match_count": len(exact_matches),
            "mismatch_count": len(mismatches),
            "first_mismatch": mismatches[0] if mismatches else None,
            "maximum_ulp_distance": max(
                (item["ulp_distance"] for item in numerical),
                default=0,
            ),
            "comparisons": numerical,
        },
        "rounded_171_cycle_context": {
            "discrete_parity": (major_trace["discrete_parity"]["status"] == "passed"),
            "numerical_summary": major_trace["numerical_summary"],
            "current_casars_control_minor_maxima": (
                control_trace["minor"]["numerical_maxima"]
            ),
        },
        "proof_limits": {
            "array_level_localization_possible": False,
            "reason": (
                "no 4096-square four-SPW CASA initial or per-cycle residual "
                "and model arrays survive"
            ),
            "classification_claim": (
                "locates only the first surviving cross-producer scalar "
                "difference; it does not assign an array or algorithm owner"
            ),
        },
        "authorization": {
            "new_clean": False,
            "production_change": False,
            "tolerance_change": False,
            "runtime_change": False,
            "next_required_evidence": (
                "a new, distinct CASA initial-residual array oracle is required "
                "for causal array-level localization"
            ),
        },
        "execution_counters": {
            "casa_calls": 0,
            "measurement_set_opens": 0,
            "prediction_calls": 0,
            "grid_calls": 0,
            "fft_calls": 0,
            "minor_cycle_calls": 0,
            "clean_calls": 0,
            "product_trees_written": 0,
        },
    }
    write_json_new(args.output, receipt)
    print(
        json.dumps(
            {
                "classification": classification,
                "exact_match_count": len(exact_matches),
                "first_mismatch": mismatches[0] if mismatches else None,
                "maximum_ulp_distance": receipt["numerical"]["maximum_ulp_distance"],
                "mismatch_count": len(mismatches),
                "output": str(args.output),
            },
            indent=2,
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
