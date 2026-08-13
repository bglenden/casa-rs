#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Validate that a VLASS landmark measures real end-to-end CLEAN."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONTRACT = ROOT / "tools/perf/imager/vlass_recovery_contract.json"
COMPLETION_RE = re.compile(
    r"Wrote CASA-compatible products at prefix (?P<prefix>.+) "
    r"\((?P<samples>[0-9]+) gridded samples, "
    r"(?P<major_cycles>[0-9]+) major cycles, "
    r"(?P<minor_iterations>[0-9]+) minor iterations, "
    r"stop=(?P<stop>.+)\)"
)
KEY_VALUE_RE = re.compile(r"(?P<key>[A-Za-z0-9_]+)=(?P<value>\S+)")


class LandmarkError(ValueError):
    """A landmark receipt cannot be promoted."""


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise LandmarkError(f"{path} must contain a JSON object")
    return value


def parse_log(text: str) -> dict[str, Any]:
    lines = text.splitlines()
    completion_matches = [
        match for line in lines if (match := COMPLETION_RE.fullmatch(line))
    ]
    if len(completion_matches) != 1:
        raise LandmarkError(
            "landmark log must contain exactly one CASA-compatible completion marker"
        )
    completion = completion_matches[0].groupdict()
    fftw_f64_provenance = []
    image_response_bytes = []
    for line in lines:
        if line.startswith("fftw_runtime_provenance precision=f64 "):
            fftw_f64_provenance.append(
                {
                    match.group("key"): match.group("value")
                    for match in KEY_VALUE_RE.finditer(line)
                }
            )
        if line.startswith("awproject_image_response_calibrated "):
            values = {
                match.group("key"): match.group("value")
                for match in KEY_VALUE_RE.finditer(line)
            }
            if "response_bytes" in values:
                image_response_bytes.append(int(values["response_bytes"]))
    return {
        "frozen_model_loads": sum(
            line.startswith("awproject_frozen_model_refresh ") for line in lines
        ),
        "minor_cycle_records": sum(
            line.startswith("mosaic_mtmfs_minor_cycle ") for line in lines
        ),
        "sparse_rhs_plans": sum(
            line.startswith("mtmfs_multiscale_rhs_experiment ")
            and "storage=sparse-positions" in line
            for line in lines
        ),
        "radix_statistics": sum(
            line.startswith("robust_rms_order_statistic ")
            and "algorithm=exact-radix-histogram" in line
            for line in lines
        ),
        "image_response_calibrations": sum(
            line.startswith("awproject_image_response_calibrated ") for line in lines
        ),
        "image_response_syntheses": sum(
            line.startswith("awproject_image_response_synthesize ") for line in lines
        ),
        "exact_final_refreshes": sum(
            line.startswith("awproject_image_response_final_refresh ")
            and "algorithm=exact-production" in line
            for line in lines
        ),
        "fftw_f64_provenance": fftw_f64_provenance,
        "image_response_dyadic_encodes": sum(
            line.startswith("awproject_image_response_dyadic_encode ")
            for line in lines
        ),
        "image_response_bytes": image_response_bytes,
        "global_metal_program_reports": sum(
            line.startswith("awproject_compact_replay_cache ")
            and "global_metal_program=true" in line
            for line in lines
        ),
        "completion": {
            "output_prefix": completion["prefix"],
            "gridded_samples": int(completion["samples"]),
            "major_cycles": int(completion["major_cycles"]),
            "minor_iterations": int(completion["minor_iterations"]),
            "stop": completion["stop"],
        },
    }


def find_landmark(contract: dict[str, Any], landmark_id: str) -> dict[str, Any]:
    preservation = contract.get("performance_preservation")
    if not isinstance(preservation, dict):
        raise LandmarkError("performance-preservation contract is missing")
    rows = preservation.get("landmark_rows")
    if not isinstance(rows, list):
        raise LandmarkError("performance-preservation landmark rows are missing")
    matches = [row for row in rows if row.get("id") == landmark_id]
    if len(matches) != 1:
        raise LandmarkError(f"unknown or duplicate landmark id: {landmark_id}")
    return matches[0]


def evaluate(
    landmark: dict[str, Any],
    runtime: dict[str, Any],
    *,
    binary: Path,
    wall_seconds: float,
) -> list[str]:
    errors: list[str] = []
    activity = landmark["required_activity"]
    required_runtime = landmark.get("required_runtime", {})
    completion = runtime["completion"]

    if not binary.is_file():
        errors.append("receipt-bound executable is missing")
    if binary.parent.name != "release":
        errors.append("timed executable is not from a release directory")
    if wall_seconds <= 0.0:
        errors.append("end-to-end wall time must be positive")
    if (
        not activity["frozen_model_allowed"]
        and runtime["frozen_model_loads"] != 0
    ):
        errors.append("frozen-model execution cannot satisfy a CLEAN landmark")
    if completion["major_cycles"] < activity["minimum_major_cycles"]:
        errors.append("major-cycle activity is below the landmark contract")
    if runtime["minor_cycle_records"] < activity["minimum_minor_cycle_records"]:
        errors.append("real minor-cycle records are missing")
    if "actual_minor_iterations" in activity:
        if completion["minor_iterations"] != activity["actual_minor_iterations"]:
            errors.append("minor-iteration count differs from the landmark contract")
    elif completion["minor_iterations"] < activity["minimum_actual_minor_iterations"]:
        errors.append("minor-cycle execution did no model work")
    if runtime["image_response_calibrations"] < activity[
        "minimum_image_response_calibrations"
    ]:
        errors.append("image-response calibration was not exercised")
    if runtime["image_response_syntheses"] < activity[
        "minimum_image_response_syntheses"
    ]:
        errors.append("image-response synthesis was not exercised")
    if runtime["exact_final_refreshes"] != activity["exact_final_refreshes"]:
        errors.append("exact final-refresh count differs from the landmark contract")
    if activity["sparse_rhs_required"] and runtime["sparse_rhs_plans"] < 1:
        errors.append("sparse MT-MFS RHS was not exercised")
    if activity["radix_madfm_required"] and runtime["radix_statistics"] < 1:
        errors.append("exact radix statistics were not exercised")
    expected_fftw_threads = required_runtime.get("fftw_threads")
    expected_wisdom_sha256 = required_runtime.get("fftw_f64_wisdom_sha256")
    if expected_fftw_threads is not None or expected_wisdom_sha256 is not None:
        provenance = runtime["fftw_f64_provenance"]
        matching_provenance = [
            entry
            for entry in provenance
            if entry.get("fft_threads") == str(expected_fftw_threads)
            and entry.get("planner_flags") == "wisdom-only"
            and entry.get("wisdom_sha256") == expected_wisdom_sha256
        ]
        if not matching_provenance:
            errors.append(
                "f64 FFTW threads or immutable wisdom differ from the landmark contract"
            )
    if required_runtime.get("image_response_storage") == "raw":
        if runtime["image_response_dyadic_encodes"] != 0:
            errors.append("dyadic response storage cannot reproduce the raw landmark")
        expected_response_bytes = required_runtime.get("image_response_bytes")
        if expected_response_bytes is not None and runtime[
            "image_response_bytes"
        ] != [expected_response_bytes]:
            errors.append("image-response byte count differs from the raw landmark")
    if (
        required_runtime.get("global_metal_program_required")
        and runtime["global_metal_program_reports"] < 1
    ):
        errors.append("global Metal replay program was not exercised")

    historical = float(landmark["historical_casa_rs_wall_seconds"])
    allowed = historical * (
        1.0 + float(landmark["maximum_regression_fraction_without_user_approval"])
    )
    if wall_seconds > allowed:
        errors.append(
            f"wall time {wall_seconds:.6f}s exceeds the no-signoff ceiling "
            f"{allowed:.6f}s"
        )
    return errors


def make_receipt(
    *,
    contract_path: Path,
    landmark: dict[str, Any],
    log_path: Path,
    runtime: dict[str, Any],
    binary: Path,
    wall_seconds: float,
    errors: list[str],
) -> dict[str, Any]:
    casa_wall = landmark.get("historical_casa_wall_seconds")
    return {
        "schema_version": 1,
        "evidence_role": "end_to_end_clean_landmark",
        "landmark_id": landmark["id"],
        "status": "passed" if not errors else "failed",
        "errors": errors,
        "contract": {
            "path": str(contract_path),
            "sha256": sha256(contract_path),
        },
        "release_binary": {
            "path": str(binary),
            "sha256": sha256(binary) if binary.is_file() else None,
        },
        "run_log": {
            "path": str(log_path),
            "sha256": sha256(log_path),
        },
        "wall_seconds": wall_seconds,
        "matched_casa_wall_seconds": casa_wall,
        "casa_divided_by_casa_rs": (
            float(casa_wall) / wall_seconds if casa_wall is not None else None
        ),
        "runtime_activity": runtime,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--landmark-id", required=True)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--wall-seconds", type=float, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    contract = load_object(args.contract)
    landmark = find_landmark(contract, args.landmark_id)
    runtime = parse_log(args.log.read_text(encoding="utf-8"))
    errors = evaluate(
        landmark,
        runtime,
        binary=args.binary,
        wall_seconds=args.wall_seconds,
    )
    receipt = make_receipt(
        contract_path=args.contract,
        landmark=landmark,
        log_path=args.log,
        runtime=runtime,
        binary=args.binary,
        wall_seconds=args.wall_seconds,
        errors=errors,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
