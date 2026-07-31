#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Classify the frozen VLASS final-model terms without running imaging.

The Rust batch executable replays only Phase-A product arithmetic.  This
driver adds the existing full-plane structured-difference gate, proves that
Control A exactly reproduces the frozen Phase-A receipt, and launches the two
mixed-term cases only when the complete casa-rs model does not pass.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import resource
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

import numpy as np


IMAGER_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(IMAGER_ROOT))

from perf_harness.casa_image_compare import (  # noqa: E402
    structured_difference_metrics,
)


BATCH_SCHEMA = "casa-rs-vlass-final-model-term-causality-case-batch-v1"
FINAL_SCHEMA = "casa-rs-vlass-final-model-term-causality-certificate-v1"
PRIMARY_LABELS = ("control-a", "complete-rust-model")
HYBRID_LABELS = ("tt0-rust-only", "tt1-rust-only")
IMAGE_SUFFIXES = (".image.tt0", ".image.tt1")


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


def write_json_new(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")


def case_map(*batches: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for batch in batches:
        for case in batch.get("cases", []):
            label = case.get("label")
            if not isinstance(label, str) or label in result:
                raise RuntimeError(f"invalid or duplicate case label: {label!r}")
            result[label] = case
    return result


def validate_batch(batch: dict[str, Any], name: str) -> None:
    expected_labels = PRIMARY_LABELS if name == "primary" else HYBRID_LABELS
    if (
        batch.get("schema") != BATCH_SCHEMA
        or batch.get("batch") != name
        or tuple(batch.get("case_labels", ())) != expected_labels
        or tuple(case.get("label") for case in batch.get("cases", ()))
        != expected_labels
    ):
        raise RuntimeError(f"{name} case batch violates its identity contract")
    boundary = batch.get("execution_boundary", {})
    prohibited = (
        "measurement_set_opened",
        "prediction_entered",
        "residual_refresh_entered",
        "grid_allocated",
        "fft_entered",
        "controller_entered",
        "minor_cycle_entered",
        "clean_entered",
        "beam_fit_entered",
        "response_cache_entered",
        "product_tree_written",
    )
    if any(boundary.get(key) is not False for key in prohibited):
        raise RuntimeError(f"{name} case batch crossed a prohibited boundary")


def raw_plane(record: dict[str, Any]) -> np.memmap:
    path = Path(record["path"])
    expected_shape = tuple(int(value) for value in record["shape"])
    expected_bytes = int(np.prod(expected_shape)) * np.dtype("<f4").itemsize
    if (
        record.get("dtype") != "little-endian-f32"
        or int(record.get("bytes", -1)) != expected_bytes
        or path.stat().st_size != expected_bytes
        or sha256_file(path) != record.get("sha256")
    ):
        raise RuntimeError(f"raw plane contract differs for {path}")
    return np.memmap(path, dtype="<f4", mode="r", shape=expected_shape, order="C")


def structure_is_good(metrics: dict[str, Any]) -> bool:
    return (
        metrics.get("status") == "computed"
        and metrics.get("classification", {}).get("overall") == "good"
    )


def enrich_batch_structure(
    batch: dict[str, Any],
    *,
    beam_info: dict[str, Any],
    scratch_root: Path,
) -> None:
    if (
        beam_info.get("status") != "estimated_from_psf"
        or beam_info.get("coordinate_domain") != "native_direction_pixels"
        or not beam_info.get("native_plane_coverage", {}).get("coverage_complete")
    ):
        raise RuntimeError("frozen native beam information is incomplete")
    references = batch["reference_raw"]
    for case in batch["cases"]:
        image_structure_pass = True
        for suffix in IMAGE_SUFFIXES:
            candidate = raw_plane(case["products"][suffix]["raw"])
            reference = raw_plane(references[suffix])
            try:
                metrics = structured_difference_metrics(
                    suffix=suffix,
                    rust_data=candidate,
                    casa_data=reference,
                    diff_data=np.subtract(candidate, reference, dtype=np.float32),
                    beam_info=beam_info,
                    scratch_root=scratch_root,
                )
            finally:
                del candidate
                del reference
            case["products"][suffix]["structured_difference"] = metrics
            image_structure_pass &= structure_is_good(metrics)
        preliminary = case["gates_before_image_structure"]
        metadata_pass = True
        case["gates"] = {
            "finite": preliminary.get("finite") is True,
            "numerical": preliminary.get("numerical") is True,
            "topology": preliminary.get("topology") is True,
            "non_spatial_structure": (preliminary.get("non_spatial_structure") is True),
            "image_structure": image_structure_pass,
            "metadata": metadata_pass,
            "metadata_basis": (
                "no product tree is written; frozen Phase-A reference metadata "
                "and the completed exact-inventory comparison remain unchanged"
            ),
        }
        case["gates"]["pass"] = all(
            case["gates"][key]
            for key in (
                "finite",
                "numerical",
                "topology",
                "non_spatial_structure",
                "image_structure",
                "metadata",
            )
        )


def phase_a_control_exact(batch: dict[str, Any]) -> tuple[bool, dict[str, bool]]:
    control = case_map(batch)["control-a"]
    expected = batch["frozen_identity"]["phase_a_contract"]["products"]
    checks: dict[str, bool] = {}
    for suffix in IMAGE_SUFFIXES:
        checks[f"{suffix}.numeric"] = (
            control["products"][suffix]["numeric"] == expected[suffix]
        )
    for suffix in (".alpha", ".alpha.error"):
        checks[f"{suffix}.numeric"] = (
            control["products"][suffix]["numeric"] == expected[suffix]["numeric"]
        )
        actual_topology = control["products"][suffix]["topology"]
        checks[f"{suffix}.topology"] = all(
            actual_topology.get(key) == value
            for key, value in expected[suffix]["topology"].items()
        )
    checks["phase_a_comparison_completed"] = (
        batch["frozen_identity"].get("phase_a_comparison_status") == "completed"
    )
    checks["derived_gates_pass"] = control.get("gates", {}).get("pass") is True
    return all(checks.values()), checks


def derived_case_pass(case: dict[str, Any]) -> bool:
    return case.get("gates", {}).get("pass") is True


def required_batches(
    primary: dict[str, Any],
    *,
    control_exact: bool,
) -> tuple[str, ...]:
    cases = case_map(primary)
    if not control_exact or not derived_case_pass(cases["control-a"]):
        return ("primary",)
    if derived_case_pass(cases["complete-rust-model"]):
        return ("primary",)
    return ("primary", "term-hybrids")


def classify_cases(
    primary: dict[str, Any],
    hybrid: dict[str, Any] | None,
    *,
    control_exact: bool,
) -> str:
    cases = case_map(primary, *(() if hybrid is None else (hybrid,)))
    if not control_exact or not derived_case_pass(cases["control-a"]):
        return "invalid-phase-a-control"
    if derived_case_pass(cases["complete-rust-model"]):
        if hybrid is not None:
            raise RuntimeError("term hybrids were run despite a passing complete model")
        return "final-model-not-sufficient"
    if hybrid is None:
        raise RuntimeError("failed complete model requires both term hybrids")
    tt0_rust_pass = derived_case_pass(cases["tt0-rust-only"])
    tt1_rust_pass = derived_case_pass(cases["tt1-rust-only"])
    if not tt0_rust_pass and tt1_rust_pass:
        return "tt0-model-state-sufficient"
    if tt0_rust_pass and not tt1_rust_pass:
        return "tt1-model-state-sufficient"
    if not tt0_rust_pass and not tt1_rust_pass:
        return "both-model-terms-independently-sufficient"
    return "joint-model-term-interaction-required"


def term_ledger_authorization(
    classification: str,
    cases: dict[str, dict[str, Any]],
    *,
    control_exact: bool,
) -> dict[str, Any]:
    implicated = {
        "tt0-model-state-sufficient": ("tt0-rust-only", "tt1-rust-only", "tt0"),
        "tt1-model-state-sufficient": ("tt1-rust-only", "tt0-rust-only", "tt1"),
    }.get(classification)
    if implicated is None:
        return {
            "authorized": False,
            "reason": "classification does not isolate exactly one final model term",
        }
    failing_label, complementary_label, term = implicated
    failing = cases[failing_label]
    complementary = cases[complementary_label]
    failing_signature = failing["current_failure_signature"]
    complementary_products = complementary["products"]
    signature_exact = (
        failing_signature.get("alpha_exact") is True
        and failing_signature.get("alpha_error_exact") is True
    )
    complement_zero = all(
        complementary_products[suffix]["topology"].get("mismatch_count") == 0
        for suffix in (".alpha", ".alpha.error")
    )
    authorized = control_exact and signature_exact and complement_zero
    return {
        "authorized": authorized,
        "term": term if authorized else None,
        "diagnostic": (
            "one bounded minor-cycle coefficient/update ledger" if authorized else None
        ),
        "control_exact": control_exact,
        "implicated_case": failing_label,
        "implicated_signature_exact": signature_exact,
        "complementary_case": complementary_label,
        "complementary_topology_zero": complement_zero,
        "production_change_authorized": False,
        "new_clean_authorized": False,
    }


def run_rust_batch(
    *,
    binary: Path,
    name: str,
    common_paths: tuple[Path, ...],
    scratch: Path,
    receipt: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    command = [
        str(binary),
        name,
        *(str(path) for path in common_paths),
        str(scratch),
        str(receipt),
    ]
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    elapsed = time.perf_counter() - started
    process = {
        "batch": name,
        "command": command,
        "returncode": completed.returncode,
        "elapsed_seconds": elapsed,
        "stdout_sha256": hashlib.sha256(completed.stdout.encode()).hexdigest(),
        "stderr_sha256": hashlib.sha256(completed.stderr.encode()).hexdigest(),
        "stderr": completed.stderr[-8_192:],
    }
    if completed.returncode != 0:
        raise RuntimeError(
            f"{name} Rust batch failed with {completed.returncode}: "
            f"{completed.stderr[-2000:]}"
        )
    batch = read_json(receipt)
    validate_batch(batch, name)
    process["receipt_sha256"] = sha256_file(receipt)
    return batch, process


def run_certificate(
    args: argparse.Namespace,
    *,
    batch_runner: Callable[..., tuple[dict[str, Any], dict[str, Any]]] = (
        run_rust_batch
    ),
) -> dict[str, Any]:
    if args.scratch_root.exists():
        raise RuntimeError(f"scratch root already exists: {args.scratch_root}")
    for output in (
        args.receipt,
        args.primary_batch_receipt,
        args.hybrid_batch_receipt,
    ):
        if output.exists():
            raise RuntimeError(f"refusing to overwrite output: {output}")
    args.scratch_root.mkdir(parents=True)
    common_paths = (
        args.casa_prefix,
        args.rust_prefix,
        args.phase_a_receipt,
        args.phase_a_comparison,
        args.clean_log,
        args.control_trace,
        args.clean_comparison,
    )
    clean_comparison = read_json(args.clean_comparison)
    beam_info = clean_comparison["beam_info"]
    started = time.perf_counter()
    primary, primary_process = batch_runner(
        binary=args.binary,
        name="primary",
        common_paths=common_paths,
        scratch=args.scratch_root / "primary",
        receipt=args.primary_batch_receipt,
    )
    enrich_batch_structure(
        primary,
        beam_info=beam_info,
        scratch_root=args.scratch_root,
    )
    control_exact, control_checks = phase_a_control_exact(primary)
    hybrid = None
    processes = [primary_process]
    planned_batches = required_batches(primary, control_exact=control_exact)
    if planned_batches == ("primary", "term-hybrids"):
        hybrid, hybrid_process = batch_runner(
            binary=args.binary,
            name="term-hybrids",
            common_paths=common_paths,
            scratch=args.scratch_root / "term-hybrids",
            receipt=args.hybrid_batch_receipt,
        )
        enrich_batch_structure(
            hybrid,
            beam_info=beam_info,
            scratch_root=args.scratch_root,
        )
        processes.append(hybrid_process)
    classification = classify_cases(
        primary,
        hybrid,
        control_exact=control_exact,
    )
    cases = case_map(primary, *(() if hybrid is None else (hybrid,)))
    authorization = term_ledger_authorization(
        classification,
        cases,
        control_exact=control_exact,
    )
    receipt = {
        "schema": FINAL_SCHEMA,
        "role": "offline-correctness-certificate-not-performance-evidence",
        "classification": classification,
        "control_a": {
            "exact_phase_a_receipt_reproduction": control_exact,
            "checks": control_checks,
        },
        "conditional_execution": {
            "required_batches": list(planned_batches),
            "observed_batches": [process["batch"] for process in processes],
            "term_hybrids_skipped": hybrid is None,
        },
        "inputs": {
            "binary": str(args.binary),
            "binary_sha256": sha256_file(args.binary),
            "clean_comparison": str(args.clean_comparison),
            "clean_comparison_sha256": sha256_file(args.clean_comparison),
            "candidate_commit": primary["candidate_commit"],
            "frozen_identity": primary["frozen_identity"],
        },
        "beam_info": beam_info,
        "model_ledger": primary["model_ledger"],
        "cases": cases,
        "batch_receipts": {
            "primary": {
                "path": str(args.primary_batch_receipt),
                "sha256": sha256_file(args.primary_batch_receipt),
            },
            "term_hybrids": (
                None
                if hybrid is None
                else {
                    "path": str(args.hybrid_batch_receipt),
                    "sha256": sha256_file(args.hybrid_batch_receipt),
                }
            ),
        },
        "processes": processes,
        "next_diagnostic_authorization": authorization,
        "execution_boundary": {
            "casa_run": False,
            "measurement_set_opened": False,
            "prediction": False,
            "gridding": False,
            "fft": False,
            "controller": False,
            "minor_cycle": False,
            "clean": False,
            "new_product_tree": False,
            "transient_raw_planes_removed_after_metrics_recorded": True,
        },
        "timing": {
            "total_seconds": time.perf_counter() - started,
            "peak_resident_bytes": int(
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            ),
            "peak_resident_units": (
                "bytes" if platform.system() == "Darwin" else "kibibytes"
            ),
        },
        "environment": {
            "platform": platform.platform(),
            "python": sys.version,
            "pid": os.getpid(),
        },
        "next_boundary": (
            "stop-no-new-clean"
            if not authorization.get("authorized")
            else "one-term-specific-minor-cycle-ledger-only"
        ),
    }
    shutil.rmtree(args.scratch_root)
    write_json_new(args.receipt, receipt)
    return receipt


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--casa-prefix", required=True, type=Path)
    parser.add_argument("--rust-prefix", required=True, type=Path)
    parser.add_argument("--phase-a-receipt", required=True, type=Path)
    parser.add_argument("--phase-a-comparison", required=True, type=Path)
    parser.add_argument("--clean-log", required=True, type=Path)
    parser.add_argument("--control-trace", required=True, type=Path)
    parser.add_argument("--clean-comparison", required=True, type=Path)
    parser.add_argument("--scratch-root", required=True, type=Path)
    parser.add_argument("--primary-batch-receipt", required=True, type=Path)
    parser.add_argument("--hybrid-batch-receipt", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_certificate(args)
    print(json.dumps(result, sort_keys=True))
    return int(result["classification"] == "invalid-phase-a-control")


if __name__ == "__main__":
    raise SystemExit(main())
