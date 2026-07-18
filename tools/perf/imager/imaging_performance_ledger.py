#!/usr/bin/env python3
"""Validate and summarize the broad imaging performance ledger."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import pathlib
import re
import statistics
import sys
from typing import Any

from perf_harness import (
    ContractError,
    finite_number as contract_finite_number,
    load_json_object,
    load_run_result,
)
from perf_harness.artifacts import ArtifactError


LEDGER_PATH = (
    pathlib.Path(__file__).resolve().parent / "imaging_performance_ledger.json"
)
REQUIRED_ISSUES = {56, 343, 262, 352}
REQUIRED_GLOBAL_EVIDENCE_ROLES = {"casa_oracle"}
SUPPORTED_EVIDENCE_ROLES = {
    "casa_oracle",
    "cpu_f32_baseline",
    "metal_f32_candidate",
    "metal_candidate",
    "mosaic_mtmfs_candidate",
    "multi_cpu_baseline",
    "parallel_auto",
    "parallel_chanchunks",
    "read_ahead_disabled",
    "read_ahead_enabled",
    "read_ahead_negative_control",
    "rejected_metal_negative_control",
    "serial_cpu",
}
SUPPORTED_ARTIFACT_ROLES = {
    "baseline",
    "candidate",
    "casa_oracle",
    "counterbalanced_comparison",
    "full_comparison",
    "primary_comparison",
    "product_comparison",
}
ARTIFACT_RESULT_PATH_KEYS = {
    "baseline": "baseline",
    "candidate": "candidate",
    "casa_oracle": "oracle",
    "counterbalanced_comparison": "counterbalanced_comparison",
    "full_comparison": "all_product_comparison",
    "primary_comparison": "primary_comparison",
    "product_comparison": "product_comparison",
}
CANONICAL_FORMULAS = {
    "speedup": "baseline_total_wall_s / candidate_total_wall_s",
    "wall_reduction_fraction": (
        "(baseline_total_wall_s - candidate_total_wall_s) / baseline_total_wall_s"
    ),
    "contribution_to_total_saved_wall": (
        "(baseline_stage_s - candidate_stage_s) / "
        "(baseline_total_wall_s - candidate_total_wall_s)"
    ),
}
# Ledger-derived fractions are commonly recorded to six decimal places.
ARITHMETIC_REL_TOLERANCE = 1.0e-5
ARITHMETIC_ABS_TOLERANCE = 5.0e-6
EVIDENCE_REL_TOLERANCE = 1.0e-6
EVIDENCE_ABS_TOLERANCE = 1.0e-9
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
COMPARISON_LABEL_SEVERITY = {
    "unknown": 0,
    "good": 1,
    "investigate": 2,
    "bad": 3,
}
REQUIRED_SUMMARY_COLUMNS = {
    "workload_id",
    "issue_slice",
    "evidence_roles",
    "baseline_total_wall_s",
    "candidate_total_wall_s",
    "speedup",
    "wall_reduction_fraction",
    "correctness_status",
}
REQUIRED_STAGE_COLUMNS = {
    "stage",
    "baseline_s",
    "candidate_s",
    "saved_fraction_of_baseline_wall",
    "contribution_to_total_saved_wall",
}
REQUIRED_OVERLAP_COLUMNS = {
    "producer_active_s",
    "consumer_active_s",
    "producer_consumer_overlap_s",
    "queue_high_water_bytes",
    "effective_read_bandwidth_mb_s",
}
REQUIRED_GPU_COLUMNS = {
    "device_fft_s",
    "device_product_assembly_s",
    "device_to_host_final_export_s",
    "fallback_reason",
}
REQUIRED_METAL_EVIDENCE_FIELDS = {
    "dirty_product_fft_fallback_used",
    "dirty_product_fft_selected_backend",
    "dirty_product_fft_total_ms",
    "dirty_product_gpu_resident_postprocess_ms",
    "f32_cast_pack_s",
    "fallback_reason",
    "gpu_sync_wait_s",
    "host_to_device_staging_s",
    "metal_device_available",
    "device_fft_s",
    "device_product_assembly_s",
    "device_to_host_final_export_s",
}
REQUIRED_METAL_ARTIFACT_TIMINGS = {
    "dirty_product_fft_total_ms",
    "dirty_product_gpu_resident_device_exec_ms",
    "dirty_product_gpu_resident_exec_ms",
    "dirty_product_gpu_resident_pack_ms",
    "dirty_product_gpu_resident_plan_ms",
    "dirty_product_gpu_resident_postprocess_ms",
    "dirty_product_gpu_resident_sync_ms",
    "dirty_product_gpu_resident_total_ms",
    "dirty_product_gpu_resident_transfer_from_device_ms",
    "dirty_product_gpu_resident_transfer_to_device_ms",
}
GPU_LEDGER_TO_ARTIFACT = {
    "dirty_product_fft_fallback_used": ("dirty_product_fft_fallback_used", 1.0),
    "dirty_product_fft_selected_backend": (
        "dirty_product_fft_selected_backend",
        1.0,
    ),
    "dirty_product_fft_total_ms": ("dirty_product_fft_total_ms", 1.0),
    "dirty_product_gpu_resident_postprocess_ms": (
        "dirty_product_gpu_resident_postprocess_ms",
        1.0,
    ),
    "f32_cast_pack_s": ("dirty_product_gpu_resident_pack_ms", 0.001),
    "gpu_sync_wait_s": ("dirty_product_gpu_resident_sync_ms", 0.001),
    "host_to_device_staging_s": (
        "dirty_product_gpu_resident_transfer_to_device_ms",
        0.001,
    ),
    "metal_device_available": ("metal_device_available", 1.0),
    "device_fft_s": ("dirty_product_gpu_resident_device_exec_ms", 0.001),
    "device_product_assembly_s": (
        "dirty_product_gpu_resident_postprocess_ms",
        0.001,
    ),
    "device_to_host_final_export_s": (
        "dirty_product_gpu_resident_transfer_from_device_ms",
        0.001,
    ),
}
STAGE_BACKEND_ALIASES = {
    "source_read": "source_read_ahead_source_read_ms",
    "source_prepare_and_consume": "source_read_ahead_consumer_ms",
}
REQUIRED_CASA_COLUMNS = {
    "oracle_result_path",
    "comparison_result_path",
    "comparison_status",
    "max_abs",
    "rms",
}
ROLE_EVIDENCE_REQUIREMENTS = {
    "read_ahead_enabled": ("overlap", REQUIRED_OVERLAP_COLUMNS),
    "metal_f32_candidate": ("gpu", REQUIRED_METAL_EVIDENCE_FIELDS),
    "casa_oracle": ("casa", REQUIRED_CASA_COLUMNS),
}
INTEGER_EVIDENCE_FIELDS = {("overlap", "queue_high_water_bytes")}
BOOLEAN_EVIDENCE_FIELDS = {
    ("gpu", "dirty_product_fft_fallback_used"),
    ("gpu", "metal_device_available"),
}
STRING_EVIDENCE_FIELDS = {
    ("gpu", "dirty_product_fft_selected_backend"),
    ("gpu", "fallback_reason"),
    ("casa", "oracle_result_path"),
    ("casa", "comparison_result_path"),
    ("casa", "comparison_status"),
}


class LedgerError(Exception):
    """Validation error shown without a traceback."""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=pathlib.Path, default=LEDGER_PATH)
    parser.add_argument(
        "--format", choices=("text", "markdown", "json"), default="text"
    )
    args = parser.parse_args()

    try:
        ledger = load_ledger(args.ledger)
        summary = summarize(ledger)
        if args.format == "json":
            json.dump(summary, sys.stdout, indent=2, sort_keys=True)
            sys.stdout.write("\n")
        elif args.format == "markdown":
            sys.stdout.write(render_markdown(summary))
        else:
            print(
                "ledger ok: "
                f"{summary['workload_group_count']} workload groups, "
                f"{summary['run_count']} recorded runs"
            )
    except LedgerError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None


def load_ledger(path: pathlib.Path) -> dict[str, Any]:
    try:
        ledger = load_json_object(path, description="imaging performance ledger")
    except ArtifactError as error:
        raise LedgerError(str(error)) from error
    validate_ledger(ledger, path)
    return ledger


def validate_ledger(ledger: dict[str, Any], path: pathlib.Path) -> None:
    if ledger.get("schema_version") != 1:
        raise LedgerError("schema_version must be 1")
    issues = {int(value) for value in list_field(ledger, "wave_issues")}
    if issues != REQUIRED_ISSUES:
        raise LedgerError(
            "wave_issues must be exactly "
            + ", ".join(str(issue) for issue in sorted(REQUIRED_ISSUES))
        )
    require_columns(ledger, "summary_columns", REQUIRED_SUMMARY_COLUMNS)
    require_columns(ledger, "stage_columns", REQUIRED_STAGE_COLUMNS)
    require_columns(ledger, "overlap_columns", REQUIRED_OVERLAP_COLUMNS)
    require_columns(ledger, "gpu_columns", REQUIRED_GPU_COLUMNS)
    require_columns(ledger, "casa_columns", REQUIRED_CASA_COLUMNS)
    formulas = object_field(ledger, "formulas")
    for key, expected in CANONICAL_FORMULAS.items():
        if formulas.get(key) != expected:
            raise LedgerError(f"formulas.{key} must be {expected!r}")
    manifest_artifacts = load_evidence_manifest(ledger, path)
    workload_groups = list_field(ledger, "workload_groups")
    if not workload_groups:
        raise LedgerError("workload_groups must not be empty")
    workload_dir = path.parent / "workloads"
    covered_issues: set[int] = set()
    required_roles_by_group: dict[str, set[str]] = {}
    workload_ids_by_group: dict[str, set[str]] = {}
    for index, group in enumerate(workload_groups):
        if not isinstance(group, dict):
            raise LedgerError(f"workload_groups[{index}] must be an object")
        group_id = string_field(group, "group_id")
        if group_id in required_roles_by_group:
            raise LedgerError(f"duplicate workload group_id: {group_id}")
        owner_issues = {int(value) for value in list_field(group, "owner_issues")}
        if not owner_issues:
            raise LedgerError(f"{group_id}: owner_issues must not be empty")
        if not owner_issues <= REQUIRED_ISSUES:
            raise LedgerError(f"{group_id}: owner_issues contains unexpected issue")
        covered_issues |= owner_issues
        workloads = [
            string
            for string in list_field(group, "workloads")
            if isinstance(string, str)
        ]
        if not workloads:
            raise LedgerError(f"{group_id}: workloads must not be empty")
        missing = [
            workload for workload in workloads if not (workload_dir / workload).exists()
        ]
        if missing:
            raise LedgerError(
                f"{group_id}: missing workload file(s): {', '.join(missing)}"
            )
        roles = string_list_field(group, "required_roles")
        if not roles:
            raise LedgerError(f"{group_id}: required_roles must not be empty")
        if len(roles) != len(set(roles)):
            raise LedgerError(f"{group_id}: required_roles must not contain duplicates")
        unsupported_roles = set(roles) - SUPPORTED_EVIDENCE_ROLES
        if unsupported_roles:
            raise LedgerError(
                f"{group_id}: required_roles contains unsupported evidence role(s): "
                + ", ".join(sorted(unsupported_roles))
            )
        required_roles_by_group[group_id] = set(roles)
        workload_ids_by_group[group_id] = {
            pathlib.Path(workload).stem for workload in workloads
        }
    missing_issues = REQUIRED_ISSUES - covered_issues
    if missing_issues:
        raise LedgerError(
            "workload_groups do not cover issue(s): "
            + ", ".join(str(issue) for issue in sorted(missing_issues))
        )
    claimed_roles_by_group = {group_id: set() for group_id in required_roles_by_group}
    claimed_roles: set[str] = set()
    referenced_artifact_ids: set[str] = set()
    for index, run in enumerate(list_field(ledger, "runs")):
        if not isinstance(run, dict):
            raise LedgerError(f"runs[{index}] must be an object")
        for key in REQUIRED_SUMMARY_COLUMNS:
            if key not in run:
                raise LedgerError(f"runs[{index}].{key} is missing")
        claims = run.get("evidence_roles")
        if not isinstance(claims, list) or not claims:
            raise LedgerError(f"runs[{index}].evidence_roles must be a non-empty list")
        validate_run_arithmetic(run, index)
        anchored_artifacts = validate_run_artifact_anchors(
            run,
            index,
            manifest_artifacts,
            referenced_artifact_ids,
        )
        validate_anchored_run_evidence(run, index, anchored_artifacts)
        seen_claims: set[tuple[str, str]] = set()
        for claim_index, claim in enumerate(claims):
            claim_path = f"runs[{index}].evidence_roles[{claim_index}]"
            if not isinstance(claim, dict):
                raise LedgerError(f"{claim_path} must be an object")
            group_id = non_empty_string(claim.get("group_id"), f"{claim_path}.group_id")
            role = non_empty_string(claim.get("role"), f"{claim_path}.role")
            if group_id not in required_roles_by_group:
                raise LedgerError(
                    f"{claim_path} references unknown group_id {group_id}"
                )
            if role not in required_roles_by_group[group_id]:
                raise LedgerError(
                    f"{claim_path} claims undeclared role {group_id}:{role}"
                )
            if role not in SUPPORTED_EVIDENCE_ROLES:
                raise LedgerError(
                    f"{claim_path} claims unsupported evidence role {role}"
                )
            workload_id = string_field(run, "workload_id")
            if workload_id not in workload_ids_by_group[group_id]:
                raise LedgerError(
                    f"{claim_path} claims {group_id}:{role} with workload_id {workload_id}, "
                    "which is not declared by that group"
                )
            baseline_workload_id = run.get("baseline_workload_id")
            if baseline_workload_id is not None:
                baseline_workload_id = non_empty_string(
                    baseline_workload_id, f"runs[{index}].baseline_workload_id"
                )
                if baseline_workload_id not in workload_ids_by_group[group_id]:
                    raise LedgerError(
                        f"{claim_path} claims {group_id}:{role} with baseline_workload_id "
                        f"{baseline_workload_id}, which is not declared by that group"
                    )
            role_claim = (group_id, role)
            if role_claim in seen_claims:
                raise LedgerError(
                    f"{claim_path} duplicates role claim {group_id}:{role}"
                )
            seen_claims.add(role_claim)
            claimed_roles_by_group[group_id].add(role)
            claimed_roles.add(role)
            validate_role_evidence(run, index, role, anchored_artifacts)
    for group_id, required_roles in required_roles_by_group.items():
        missing_roles = required_roles - claimed_roles_by_group[group_id]
        if missing_roles:
            raise LedgerError(
                f"{group_id}: required_roles missing recorded evidence: "
                + ", ".join(sorted(missing_roles))
            )
    missing_global_roles = REQUIRED_GLOBAL_EVIDENCE_ROLES - claimed_roles
    if missing_global_roles:
        raise LedgerError(
            "ledger missing required evidence role(s): "
            + ", ".join(sorted(missing_global_roles))
        )
    unreferenced_artifact_ids = set(manifest_artifacts) - referenced_artifact_ids
    if unreferenced_artifact_ids:
        raise LedgerError(
            "evidence manifest contains unreferenced artifact(s): "
            + ", ".join(sorted(unreferenced_artifact_ids))
        )


def summarize(ledger: dict[str, Any]) -> dict[str, Any]:
    return {
        "wave_issues": ledger["wave_issues"],
        "workload_group_count": len(ledger["workload_groups"]),
        "run_count": len(ledger.get("runs", [])),
        "workload_groups": [
            {
                "group_id": group["group_id"],
                "owner_issues": group["owner_issues"],
                "workload_count": len(group["workloads"]),
                "required_roles": group["required_roles"],
            }
            for group in ledger["workload_groups"]
        ],
    }


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "| Group | Issues | Workloads | Required roles |",
        "| --- | --- | ---: | --- |",
    ]
    for group in summary["workload_groups"]:
        lines.append(
            "| {group_id} | {issues} | {count} | {roles} |".format(
                group_id=group["group_id"],
                issues=", ".join(str(issue) for issue in group["owner_issues"]),
                count=group["workload_count"],
                roles=", ".join(group["required_roles"]),
            )
        )
    return "\n".join(lines) + "\n"


def require_columns(ledger: dict[str, Any], key: str, required: set[str]) -> None:
    columns = set(string_list_field(ledger, key))
    missing = required - columns
    if missing:
        raise LedgerError(f"{key} missing " + ", ".join(sorted(missing)))


def validate_role_evidence(
    run: dict[str, Any],
    run_index: int,
    role: str,
    anchored_artifacts: dict[str, dict[str, Any]],
) -> None:
    requirement = ROLE_EVIDENCE_REQUIREMENTS.get(role)
    if requirement is None:
        return
    object_name, required_fields = requirement
    evidence = run.get(object_name)
    if not isinstance(evidence, dict):
        raise LedgerError(
            f"runs[{run_index}] evidence role {role} requires {object_name} to be an object"
        )
    for field in sorted(required_fields):
        field_path = f"{object_name}.{field}"
        if field not in evidence:
            raise LedgerError(
                f"runs[{run_index}] evidence role {role} requires {field_path}; field is missing"
            )
        value = evidence[field]
        if value is None:
            raise LedgerError(
                f"runs[{run_index}] evidence role {role} requires {field_path}; field is null"
            )
        if (object_name, field) in STRING_EVIDENCE_FIELDS:
            non_empty_string(value, f"runs[{run_index}].{field_path}")
        elif (object_name, field) in INTEGER_EVIDENCE_FIELDS:
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise LedgerError(
                    f"runs[{run_index}].{field_path} must be a non-negative integer"
                )
        elif (object_name, field) in BOOLEAN_EVIDENCE_FIELDS:
            if not isinstance(value, bool):
                raise LedgerError(f"runs[{run_index}].{field_path} must be a boolean")
        elif (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
        ):
            raise LedgerError(
                f"runs[{run_index}].{field_path} must be a non-negative number"
            )
    if role == "metal_f32_candidate":
        validate_metal_candidate(run, run_index, evidence, anchored_artifacts)
    if role == "casa_oracle":
        oracle_artifact = anchored_artifacts.get("casa_oracle")
        if (
            oracle_artifact is None
            or oracle_artifact["source_path"] != evidence["oracle_result_path"]
        ):
            raise LedgerError(
                f"runs[{run_index}] evidence role casa_oracle requires an anchored oracle artifact"
            )
        comparison_artifact = anchored_artifacts.get("full_comparison")
        if (
            comparison_artifact is None
            or comparison_artifact["source_path"] != evidence["comparison_result_path"]
        ):
            raise LedgerError(
                f"runs[{run_index}] evidence role casa_oracle requires an anchored full comparison artifact"
            )
        validate_casa_oracle_evidence(
            run,
            run_index,
            evidence,
            oracle_artifact,
            comparison_artifact,
        )


def validate_anchored_run_evidence(
    run: dict[str, Any],
    run_index: int,
    anchored_artifacts: dict[str, dict[str, Any]],
) -> None:
    baseline_artifact = anchored_artifacts.get("baseline")
    candidate_artifact = anchored_artifacts.get("candidate")
    counterbalanced_artifact = anchored_artifacts.get("counterbalanced_comparison")
    if (baseline_artifact is None) != (candidate_artifact is None):
        raise LedgerError(
            f"runs[{run_index}] must anchor baseline and candidate artifacts together"
        )
    if baseline_artifact is not None and candidate_artifact is not None:
        baseline_wall = artifact_wall_seconds(
            baseline_artifact, "rust", f"runs[{run_index}] baseline artifact"
        )
        candidate_wall = artifact_wall_seconds(
            candidate_artifact, "rust", f"runs[{run_index}] candidate artifact"
        )
        if counterbalanced_artifact is None:
            validate_evidence_number(
                run.get("baseline_total_wall_s"),
                baseline_wall,
                f"runs[{run_index}].baseline_total_wall_s",
                "baseline artifact results.rust.timings_seconds.median",
            )
            validate_evidence_number(
                run.get("candidate_total_wall_s"),
                candidate_wall,
                f"runs[{run_index}].candidate_total_wall_s",
                "candidate artifact results.rust.timings_seconds.median",
            )
            validate_stages_against_artifacts(
                run,
                run_index,
                baseline_artifact,
                candidate_artifact,
                "rust",
                "rust",
            )
        validate_read_ahead_evidence(
            run, run_index, baseline_artifact, candidate_artifact
        )
        validate_backend_evidence(run, run_index, candidate_artifact)

    if counterbalanced_artifact is not None:
        counterbalanced = derive_counterbalanced_evidence(
            counterbalanced_artifact,
            f"runs[{run_index}] counterbalanced comparison artifact",
        )
        expected_baseline = run.get("baseline_workload_id")
        if expected_baseline is None:
            expected_baseline = string_field(run, "workload_id")
        expected_candidate = string_field(run, "workload_id")
        if counterbalanced["baseline_workload_id"] != expected_baseline:
            raise LedgerError(
                f"runs[{run_index}] counterbalanced baseline workload does not match "
                f"{expected_baseline}"
            )
        if counterbalanced["candidate_workload_id"] != expected_candidate:
            raise LedgerError(
                f"runs[{run_index}] counterbalanced candidate workload does not match "
                f"{expected_candidate}"
            )
        validate_evidence_number(
            run.get("baseline_total_wall_s"),
            counterbalanced["baseline_seconds"],
            f"runs[{run_index}].baseline_total_wall_s",
            "counterbalanced median-delta block baseline",
        )
        validate_evidence_number(
            run.get("candidate_total_wall_s"),
            counterbalanced["candidate_seconds"],
            f"runs[{run_index}].candidate_total_wall_s",
            "counterbalanced median-delta block candidate",
        )

    comparison_artifact = preferred_comparison_artifact(anchored_artifacts)
    if comparison_artifact is not None:
        comparison = derive_comparison_evidence(
            comparison_artifact, f"runs[{run_index}] comparison artifact"
        )
        validate_comparison_claims(run, run_index, comparison)


def derive_counterbalanced_evidence(
    artifact: dict[str, Any], path: str
) -> dict[str, Any]:
    payload = artifact["payload"]
    if payload.get("status") not in {"completed", "out_of_tolerance"}:
        raise LedgerError(f"{path}.status must be completed or out_of_tolerance")
    results = payload.get("results")
    if not isinstance(results, dict) or not isinstance(
        results.get("alternating_comparison"), dict
    ):
        raise LedgerError(f"{path}.results.alternating_comparison must be an object")
    payload = results["alternating_comparison"]
    configuration = payload.get("configuration")
    if not isinstance(configuration, dict):
        raise LedgerError(f"{path}.configuration must be an object")
    configured_workloads = {
        "baseline": non_empty_string(
            configuration.get("baseline_workload"),
            f"{path}.configuration.baseline_workload",
        ),
        "candidate": non_empty_string(
            configuration.get("candidate_workload"),
            f"{path}.configuration.candidate_workload",
        ),
    }
    schedule = payload.get("schedule")
    runs = payload.get("runs")
    if not isinstance(schedule, list) or not schedule:
        raise LedgerError(f"{path}.schedule must be a non-empty list")
    if not isinstance(runs, list) or len(runs) != len(schedule):
        raise LedgerError(f"{path}.runs must contain one entry per scheduled run")
    measured_by_block: dict[int, dict[str, list[float]]] = {}
    schedule_fields = (
        "sequence_index",
        "phase",
        "block_index",
        "position_in_block",
        "role",
        "workload",
    )
    for index, (scheduled, run) in enumerate(zip(schedule, runs, strict=True)):
        if not isinstance(scheduled, dict) or not isinstance(run, dict):
            raise LedgerError(f"{path}.schedule/runs[{index}] must be objects")
        for field in schedule_fields:
            if run.get(field) != scheduled.get(field):
                raise LedgerError(f"{path}.runs[{index}].{field} does not match schedule")
        if run.get("result_status") != "completed":
            raise LedgerError(f"{path}.runs[{index}].result_status must be completed")
        if run.get("phase") != "measured":
            continue
        block_index = run.get("block_index")
        if isinstance(block_index, bool) or not isinstance(block_index, int) or block_index < 1:
            raise LedgerError(f"{path}.runs[{index}].block_index must be positive")
        role = run.get("role")
        if role not in {"baseline", "candidate"}:
            raise LedgerError(f"{path}.runs[{index}].role is invalid")
        if run.get("workload") != configured_workloads[role]:
            raise LedgerError(
                f"{path}.runs[{index}].workload does not match configured {role} workload"
            )
        wall = positive_number(
            run.get("total_wall_seconds"), f"{path}.runs[{index}].total_wall_seconds"
        )
        measured_by_block.setdefault(
            block_index, {"baseline": [], "candidate": []}
        )[role].append(wall)
    expected_block_count = positive_number(
        configuration.get("measured_pair_count"),
        f"{path}.configuration.measured_pair_count",
    )
    if expected_block_count != len(measured_by_block):
        raise LedgerError(f"{path} measured block count does not match configuration")
    recomputed_deltas = []
    for block_index in sorted(measured_by_block):
        block = measured_by_block[block_index]
        if len(block["baseline"]) != 2 or len(block["candidate"]) != 2:
            raise LedgerError(
                f"{path} measured block {block_index} must contain two runs per role"
            )
        baseline_seconds = statistics.median(block["baseline"])
        candidate_seconds = statistics.median(block["candidate"])
        recomputed_deltas.append(
            {
                "block_index": block_index,
                "baseline_seconds": baseline_seconds,
                "candidate_seconds": candidate_seconds,
                "delta_seconds": candidate_seconds - baseline_seconds,
                "relative_delta": (candidate_seconds - baseline_seconds)
                / baseline_seconds,
            }
        )
    paired_deltas = payload.get("paired_deltas")
    if not isinstance(paired_deltas, list) or len(paired_deltas) != len(recomputed_deltas):
        raise LedgerError(f"{path}.paired_deltas does not match measured blocks")
    for index, (reported, recomputed) in enumerate(
        zip(paired_deltas, recomputed_deltas, strict=True)
    ):
        if not isinstance(reported, dict):
            raise LedgerError(f"{path}.paired_deltas[{index}] must be an object")
        if reported.get("block_index") != recomputed["block_index"]:
            raise LedgerError(f"{path}.paired_deltas[{index}].block_index is inconsistent")
        for field in (
            "baseline_seconds",
            "candidate_seconds",
            "delta_seconds",
            "relative_delta",
        ):
            validate_evidence_number(
                reported.get(field),
                recomputed[field],
                f"{path}.paired_deltas[{index}].{field}",
                "raw scheduled runs",
            )
    median_delta = statistics.median(
        delta["relative_delta"] for delta in recomputed_deltas
    )
    tolerance = non_negative_number(
        configuration.get("slowdown_tolerance_fraction"),
        f"{path}.configuration.slowdown_tolerance_fraction",
    )
    verdict = payload.get("verdict")
    if not isinstance(verdict, dict):
        raise LedgerError(f"{path}.verdict must be an object")
    expected_pass = median_delta <= tolerance
    if verdict.get("status") != ("pass" if expected_pass else "fail"):
        raise LedgerError(f"{path}.verdict.status does not match raw scheduled runs")
    if verdict.get("no_slowdown") is not expected_pass:
        raise LedgerError(f"{path}.verdict.no_slowdown does not match raw scheduled runs")
    validate_evidence_number(
        verdict.get("observed_median_relative_delta"),
        median_delta,
        f"{path}.verdict.observed_median_relative_delta",
        "raw scheduled runs",
    )
    if not expected_pass:
        raise LedgerError(f"{path} records a counterbalanced slowdown")
    matching_raw_block = min(
        recomputed_deltas,
        key=lambda delta: abs(delta["relative_delta"] - median_delta),
    )
    return {
        "baseline_workload_id": pathlib.Path(configured_workloads["baseline"]).stem,
        "candidate_workload_id": pathlib.Path(configured_workloads["candidate"]).stem,
        "baseline_seconds": matching_raw_block["baseline_seconds"],
        "candidate_seconds": matching_raw_block["candidate_seconds"],
    }


def validate_stages_against_artifacts(
    run: dict[str, Any],
    run_index: int,
    baseline_artifact: dict[str, Any],
    candidate_artifact: dict[str, Any],
    baseline_implementation: str,
    candidate_implementation: str,
) -> None:
    for stage_index, stage in enumerate(list_field(run, "stages")):
        stage_path = f"runs[{run_index}].stages[{stage_index}]"
        if not isinstance(stage, dict):
            raise LedgerError(f"{stage_path} must be an object")
        name = non_empty_string(stage.get("stage"), f"{stage_path}.stage")
        baseline = artifact_stage_seconds(
            baseline_artifact,
            baseline_implementation,
            name,
            f"{stage_path}.baseline_s",
        )
        candidate = artifact_stage_seconds(
            candidate_artifact,
            candidate_implementation,
            name,
            f"{stage_path}.candidate_s",
        )
        validate_evidence_number(
            stage.get("baseline_s"),
            baseline,
            f"{stage_path}.baseline_s",
            "artifact stage",
        )
        validate_evidence_number(
            stage.get("candidate_s"),
            candidate,
            f"{stage_path}.candidate_s",
            "artifact stage",
        )


def artifact_stage_seconds(
    artifact: dict[str, Any], implementation: str, stage: str, path: str
) -> float:
    results = artifact_results(artifact, path)
    stage_medians = results.get("stage_medians_ms")
    if not isinstance(stage_medians, dict):
        raise LedgerError(f"{path} artifact results.stage_medians_ms must be an object")
    implementation_stages = stage_medians.get(implementation)
    if not isinstance(implementation_stages, dict):
        raise LedgerError(
            f"{path} artifact results.stage_medians_ms.{implementation} must be an object"
        )
    value = implementation_stages.get(stage)
    if value is None and stage == "frontend_total":
        return artifact_wall_seconds(artifact, implementation, path)
    if value is None and implementation == "rust":
        summary = artifact_backend_summary(artifact, path)
        alias = STAGE_BACKEND_ALIASES.get(stage)
        if alias is not None:
            value = summary.get(alias)
    if value is None:
        raise LedgerError(f"{path} has no derivable artifact timing for stage {stage}")
    return non_negative_number(value, f"{path} artifact stage {stage}") / 1000.0


def validate_read_ahead_evidence(
    run: dict[str, Any],
    run_index: int,
    baseline_artifact: dict[str, Any],
    candidate_artifact: dict[str, Any],
) -> None:
    overlap = run.get("overlap")
    if overlap is None:
        return
    if not isinstance(overlap, dict):
        raise LedgerError(f"runs[{run_index}].overlap must be an object")
    summary = artifact_backend_summary(
        candidate_artifact, f"runs[{run_index}] candidate artifact"
    )
    source_read_ms = required_summary_number(
        summary, "source_read_ahead_source_read_ms", run_index
    )
    source_route_ms = required_summary_number(
        summary, "source_read_ahead_source_route_ms", run_index
    )
    expected = {
        "producer_active_s": (source_read_ms + source_route_ms) / 1000.0,
        "consumer_active_s": required_summary_number(
            summary, "source_read_ahead_consumer_ms", run_index
        )
        / 1000.0,
        "producer_consumer_overlap_s": required_summary_number(
            summary, "source_read_ahead_producer_consumer_overlap_ms", run_index
        )
        / 1000.0,
        "queue_high_water_bytes": required_summary_integer(
            summary, "source_stream_buffer_bytes", run_index
        ),
        "effective_read_bandwidth_mb_s": required_summary_number(
            summary, "source_read_ahead_effective_read_bandwidth_mib_s", run_index
        ),
    }
    optional = {
        "consumer_blocked_on_input_s": (
            "source_read_ahead_consumer_recv_blocked_ms",
            0.001,
        ),
        "producer_blocked_on_capacity_s": (
            "source_read_ahead_producer_send_blocked_ms",
            0.001,
        ),
        "queue_high_water_blocks": (
            "source_read_ahead_live_row_block_high_water",
            1.0,
        ),
        "queue_capacity_blocks": ("source_read_ahead_queue_capacity", 1.0),
        "max_live_row_blocks": ("source_read_ahead_max_live_row_blocks", 1.0),
    }
    for field, value in expected.items():
        validate_evidence_value(
            overlap.get(field),
            value,
            f"runs[{run_index}].overlap.{field}",
            "candidate artifact",
        )
    for field, (artifact_field, scale) in optional.items():
        if field not in overlap:
            continue
        value = summary.get(artifact_field)
        if value is None:
            raise LedgerError(
                f"runs[{run_index}].overlap.{field} has no artifact telemetry source"
            )
        validate_evidence_value(
            overlap[field],
            scale_artifact_value(value, scale, artifact_field),
            f"runs[{run_index}].overlap.{field}",
            f"candidate artifact {artifact_field}",
        )

    validate_read_ahead_plan_value(
        run,
        run_index,
        baseline_artifact,
        candidate_artifact,
    )


def validate_read_ahead_plan_value(
    run: dict[str, Any],
    run_index: int,
    baseline_artifact: dict[str, Any],
    candidate_artifact: dict[str, Any],
) -> None:
    recorded = run.get("read_ahead_blocks")
    if recorded is None:
        return
    if isinstance(recorded, dict):
        pairs = (
            ("baseline", baseline_artifact),
            ("candidate", candidate_artifact),
        )
        for side, artifact in pairs:
            summary = artifact_backend_summary(
                artifact, f"runs[{run_index}] {side} artifact"
            )
            expected = required_summary_integer(
                summary, "source_read_ahead_max_live_row_blocks", run_index
            )
            validate_evidence_value(
                recorded.get(side),
                expected,
                f"runs[{run_index}].read_ahead_blocks.{side}",
                f"{side} artifact",
            )
        return
    summary = artifact_backend_summary(
        candidate_artifact, f"runs[{run_index}] candidate artifact"
    )
    expected = required_summary_integer(
        summary, "source_read_ahead_max_live_row_blocks", run_index
    )
    validate_evidence_value(
        recorded,
        expected,
        f"runs[{run_index}].read_ahead_blocks",
        "candidate artifact",
    )


def validate_backend_evidence(
    run: dict[str, Any], run_index: int, candidate_artifact: dict[str, Any]
) -> None:
    gpu = run.get("gpu")
    if not isinstance(gpu, dict):
        return
    summary = artifact_backend_summary(
        candidate_artifact, f"runs[{run_index}] candidate artifact"
    )
    mappings = {
        "metal_device_available": "metal_device_available",
        "selected_backend": "resolved_backend",
    }
    for ledger_field, artifact_field in mappings.items():
        if ledger_field not in gpu:
            continue
        value = summary.get(artifact_field)
        if value is None:
            raise LedgerError(
                f"runs[{run_index}].gpu.{ledger_field} has no artifact telemetry source"
            )
        validate_evidence_value(
            gpu[ledger_field],
            value,
            f"runs[{run_index}].gpu.{ledger_field}",
            f"candidate artifact {artifact_field}",
        )


def validate_metal_candidate(
    run: dict[str, Any],
    run_index: int,
    evidence: dict[str, Any],
    anchored_artifacts: dict[str, dict[str, Any]],
) -> None:
    baseline_artifact = anchored_artifacts.get("baseline")
    candidate_artifact = anchored_artifacts.get("candidate")
    if baseline_artifact is None or candidate_artifact is None:
        raise LedgerError(
            f"runs[{run_index}] evidence role metal_f32_candidate requires anchored baseline and candidate artifacts"
        )
    baseline_wall = artifact_wall_seconds(
        baseline_artifact, "rust", f"runs[{run_index}] baseline artifact"
    )
    candidate_wall = artifact_wall_seconds(
        candidate_artifact, "rust", f"runs[{run_index}] candidate artifact"
    )
    wall_uncertainty_seconds = artifact_wall_median_absolute_deviation(
        baseline_artifact, "rust", f"runs[{run_index}] baseline artifact"
    ) + artifact_wall_median_absolute_deviation(
        candidate_artifact, "rust", f"runs[{run_index}] candidate artifact"
    )
    if candidate_wall > baseline_wall + wall_uncertainty_seconds:
        wall_uncertainty_fraction = wall_uncertainty_seconds / baseline_wall
        raise LedgerError(
            f"runs[{run_index}] accepted Metal candidate exceeds the "
            f"{wall_uncertainty_fraction:.1%} measured no-slowdown uncertainty: "
            f"baseline {baseline_wall}s, candidate {candidate_wall}s"
        )
    baseline_fft = optional_artifact_stage_seconds(
        baseline_artifact, "rust", "psf_fft", f"runs[{run_index}] baseline artifact"
    )
    candidate_fft = optional_artifact_stage_seconds(
        candidate_artifact, "rust", "psf_fft", f"runs[{run_index}] candidate artifact"
    )
    if (baseline_fft is None) != (candidate_fft is None):
        raise LedgerError(
            f"runs[{run_index}] accepted Metal psf_fft stage must be present in both "
            "artifacts or omitted from both"
        )
    if baseline_fft is not None and candidate_fft is not None and candidate_fft >= baseline_fft:
        raise LedgerError(
            f"runs[{run_index}] accepted Metal psf_fft stage is not faster: "
            f"baseline {baseline_fft}s, candidate {candidate_fft}s"
        )
    summary = artifact_backend_summary(
        candidate_artifact, f"runs[{run_index}] candidate artifact"
    )
    required_backend_values = {
        "metal_device_available": True,
        "dirty_product_fft_requested_backend": "metal-mpsgraph",
        "dirty_product_fft_selected_backend": "metal-mpsgraph",
        "dirty_product_fft_fallback_used": False,
        "dirty_product_gpu_resident_requested_backend": "metal-mpsgraph",
        "dirty_product_gpu_resident_selected_backend": "metal-mpsgraph",
        "dirty_product_gpu_resident_fallback_used": False,
    }
    for field, expected in required_backend_values.items():
        validate_evidence_value(
            summary.get(field),
            expected,
            f"runs[{run_index}] candidate artifact {field}",
            "accepted Metal semantics",
        )
    for field in sorted(REQUIRED_METAL_ARTIFACT_TIMINGS):
        non_negative_number(
            summary.get(field), f"runs[{run_index}] candidate artifact {field}"
        )
    device_exec = required_positive_number(
        summary.get("dirty_product_gpu_resident_device_exec_ms"),
        f"runs[{run_index}] candidate artifact dirty_product_gpu_resident_device_exec_ms",
    )
    exec_ms = required_positive_number(
        summary.get("dirty_product_gpu_resident_exec_ms"),
        f"runs[{run_index}] candidate artifact dirty_product_gpu_resident_exec_ms",
    )
    if device_exec > exec_ms + EVIDENCE_ABS_TOLERANCE:
        raise LedgerError(
            f"runs[{run_index}] candidate artifact device_exec_ms exceeds exec_ms"
        )
    for ledger_field, (artifact_field, scale) in GPU_LEDGER_TO_ARTIFACT.items():
        expected = scale_artifact_value(
            summary.get(artifact_field), scale, artifact_field
        )
        validate_evidence_value(
            evidence.get(ledger_field),
            expected,
            f"runs[{run_index}].gpu.{ledger_field}",
            f"candidate artifact {artifact_field}",
        )
    if evidence["fallback_reason"] != "not_applicable_metal_selected":
        raise LedgerError(
            f"runs[{run_index}].gpu.fallback_reason must be not_applicable_metal_selected"
        )


def validate_casa_oracle_evidence(
    run: dict[str, Any],
    run_index: int,
    evidence: dict[str, Any],
    oracle_artifact: dict[str, Any],
    comparison_artifact: dict[str, Any],
) -> None:
    casa_wall = artifact_wall_seconds(
        oracle_artifact, "casa", f"runs[{run_index}] CASA oracle artifact"
    )
    rust_wall = artifact_wall_seconds(
        oracle_artifact, "rust", f"runs[{run_index}] CASA oracle artifact"
    )
    validate_evidence_number(
        run.get("baseline_total_wall_s"),
        casa_wall,
        f"runs[{run_index}].baseline_total_wall_s",
        "CASA artifact median",
    )
    validate_evidence_number(
        run.get("candidate_total_wall_s"),
        rust_wall,
        f"runs[{run_index}].candidate_total_wall_s",
        "Rust artifact median",
    )
    validate_stages_against_artifacts(
        run,
        run_index,
        oracle_artifact,
        oracle_artifact,
        "casa",
        "rust",
    )
    comparison = derive_comparison_evidence(
        comparison_artifact, f"runs[{run_index}] full comparison artifact"
    )
    if evidence["comparison_status"] != comparison["overall"]:
        raise LedgerError(
            f"runs[{run_index}].casa.comparison_status must equal full comparison overall "
            f"{comparison['overall']!r}"
        )
    validate_evidence_number(
        evidence.get("max_abs"),
        comparison["max_abs"],
        f"runs[{run_index}].casa.max_abs",
        "full comparison maximum absolute difference",
    )
    validate_evidence_number(
        evidence.get("rms"),
        comparison["max_normalized_rms"],
        f"runs[{run_index}].casa.rms",
        "full comparison maximum normalized RMS",
    )
    validate_casa_correctness_adjudication(evidence, run_index, comparison)


def validate_casa_correctness_adjudication(
    evidence: dict[str, Any], run_index: int, comparison: dict[str, Any]
) -> None:
    overall = comparison["overall"]
    if overall == "bad":
        raise LedgerError(
            f"runs[{run_index}] CASA comparison status 'bad' is not admissible"
        )
    if overall != "investigate":
        return

    adjudication_path = f"runs[{run_index}].casa.adjudication"
    adjudication = evidence.get("adjudication")
    if not isinstance(adjudication, dict):
        raise LedgerError(
            f"runs[{run_index}] CASA comparison status 'investigate' requires "
            "casa.adjudication to be an object"
        )
    rationale = non_empty_string(
        adjudication.get("rationale"), f"{adjudication_path}.rationale"
    )
    if not rationale.strip():
        raise LedgerError(f"{adjudication_path}.rationale must not be blank")

    products = string_list_field(adjudication, "products")
    if not products:
        raise LedgerError(f"{adjudication_path}.products must not be empty")
    if len(products) != len(set(products)):
        raise LedgerError(f"{adjudication_path}.products must not contain duplicates")
    expected_products = {
        product
        for product, label in comparison["product_labels"].items()
        if label == "investigate"
    }
    recorded_products = set(products)
    if recorded_products != expected_products:
        missing = sorted(expected_products - recorded_products)
        unexpected = sorted(recorded_products - expected_products)
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if unexpected:
            details.append("unexpected " + ", ".join(unexpected))
        raise LedgerError(
            f"{adjudication_path}.products must exactly match investigate products: "
            + "; ".join(details)
        )

    bounds = adjudication.get("bounds")
    if not isinstance(bounds, dict):
        raise LedgerError(f"{adjudication_path}.bounds must be an object")
    bound_fields = {
        "max_abs": "max_abs",
        "max_normalized_rms": "max_normalized_rms",
    }
    for bound_name, metric_name in bound_fields.items():
        bound = non_negative_number(
            bounds.get(bound_name), f"{adjudication_path}.bounds.{bound_name}"
        )
        observed_values = []
        for product in products:
            metrics = comparison["product_metrics"].get(product)
            if metrics is None:
                raise LedgerError(
                    f"{adjudication_path}.products references {product!r}, which is "
                    "missing from the comparison artifact products"
                )
            observed = metrics[metric_name]
            if observed is not None:
                observed_values.append(observed)
        if not observed_values:
            raise LedgerError(
                f"{adjudication_path}.bounds.{bound_name} cannot be verified from "
                "the comparison artifact"
            )
        observed_max = max(observed_values)
        if observed_max > bound and not math.isclose(
            observed_max,
            bound,
            rel_tol=EVIDENCE_REL_TOLERANCE,
            abs_tol=EVIDENCE_ABS_TOLERANCE,
        ):
            raise LedgerError(
                f"{adjudication_path}.bounds.{bound_name} is {bound}, but the "
                f"comparison artifact records {observed_max}"
            )


def preferred_comparison_artifact(
    anchored_artifacts: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    for role in ("full_comparison", "product_comparison", "primary_comparison"):
        artifact = anchored_artifacts.get(role)
        if artifact is not None:
            return artifact
    return None


def derive_comparison_evidence(artifact: dict[str, Any], path: str) -> dict[str, Any]:
    payload = artifact["payload"]
    results = payload.get("results")
    if isinstance(results, dict) and isinstance(results.get("product_comparison"), dict):
        payload = results["product_comparison"]
    if payload.get("status") != "completed":
        raise LedgerError(f"{path}.status must be completed")
    review = payload.get("structured_difference_review")
    if not isinstance(review, dict):
        raise LedgerError(f"{path}.structured_difference_review must be an object")
    overall = non_empty_string(
        review.get("label"), f"{path}.structured_difference_review.label"
    )
    if overall not in COMPARISON_LABEL_SEVERITY:
        raise LedgerError(f"{path} has unsupported comparison label {overall}")
    review_products = review.get("products")
    if not isinstance(review_products, dict) or not review_products:
        raise LedgerError(
            f"{path}.structured_difference_review.products must be non-empty"
        )
    product_labels = []
    for product, label in review_products.items():
        if label not in COMPARISON_LABEL_SEVERITY:
            raise LedgerError(f"{path} product {product} has unsupported label {label}")
        product_labels.append(label)
    derived_overall = max(
        product_labels, key=lambda label: COMPARISON_LABEL_SEVERITY[label]
    )
    if overall != derived_overall:
        raise LedgerError(
            f"{path} overall {overall!r} does not match worst product label {derived_overall!r}"
        )
    products = payload.get("products")
    if not isinstance(products, dict) or not products:
        raise LedgerError(f"{path}.products must be a non-empty object")
    max_abs_values: list[float] = []
    normalized_rms_values: list[float] = []
    product_metrics: dict[str, dict[str, float | None]] = {}
    for product, metrics in products.items():
        if not isinstance(metrics, dict):
            raise LedgerError(f"{path}.products.{product} must be an object")
        max_abs = metrics.get("diff_abs_max")
        if max_abs is not None:
            max_abs = non_negative_number(
                max_abs, f"{path}.products.{product}.diff_abs_max"
            )
            max_abs_values.append(max_abs)
        structured = metrics.get("structured_difference")
        normalized_rms = None
        if isinstance(structured, dict):
            normalized_rms = structured.get("normalized_diff_rms")
        if normalized_rms is None:
            normalized_rms = metrics.get("diff_rms_over_casa_rms")
        if normalized_rms is not None:
            normalized_rms = non_negative_number(
                normalized_rms,
                f"{path}.products.{product}.normalized_diff_rms",
            )
            normalized_rms_values.append(normalized_rms)
        product_metrics[product] = {
            "max_abs": max_abs,
            "max_normalized_rms": normalized_rms,
        }
    if not max_abs_values:
        raise LedgerError(f"{path} has no product maximum absolute differences")
    if not normalized_rms_values:
        raise LedgerError(f"{path} has no product normalized RMS values")
    return {
        "overall": overall,
        "max_abs": max(max_abs_values),
        "max_normalized_rms": max(normalized_rms_values),
        "product_labels": dict(review_products),
        "product_metrics": product_metrics,
    }


def validate_comparison_claims(
    run: dict[str, Any], run_index: int, comparison: dict[str, Any]
) -> None:
    correctness_status = non_empty_string(
        run.get("correctness_status"), f"runs[{run_index}].correctness_status"
    )
    expected_phrase = f"overall {comparison['overall']}"
    normalized_status = " ".join(correctness_status.lower().split())
    if expected_phrase not in normalized_status:
        raise LedgerError(
            f"runs[{run_index}].correctness_status must report comparison {expected_phrase!r}"
        )
    validate_evidence_number(
        run.get("max_abs"),
        comparison["max_abs"],
        f"runs[{run_index}].max_abs",
        "comparison maximum absolute difference",
    )
    validate_evidence_number(
        run.get("rms"),
        comparison["max_normalized_rms"],
        f"runs[{run_index}].rms",
        "comparison maximum normalized RMS",
    )


def artifact_results(artifact: dict[str, Any], path: str) -> dict[str, Any]:
    payload = artifact["payload"]
    if payload.get("status") != "completed":
        raise LedgerError(f"{path}.status must be completed")
    results = payload.get("results")
    if not isinstance(results, dict):
        raise LedgerError(f"{path}.results must be an object")
    return results


def artifact_wall_seconds(
    artifact: dict[str, Any], implementation: str, path: str
) -> float:
    results = artifact_results(artifact, path)
    implementation_result = results.get(implementation)
    if not isinstance(implementation_result, dict):
        raise LedgerError(f"{path}.results.{implementation} must be an object")
    timings = implementation_result.get("timings_seconds")
    if not isinstance(timings, dict):
        raise LedgerError(
            f"{path}.results.{implementation}.timings_seconds must be an object"
        )
    return positive_number(
        timings.get("median"),
        f"{path}.results.{implementation}.timings_seconds.median",
    )


def artifact_wall_median_absolute_deviation(
    artifact: dict[str, Any], implementation: str, path: str
) -> float:
    results = artifact_results(artifact, path)
    implementation_result = results.get(implementation)
    if not isinstance(implementation_result, dict):
        raise LedgerError(f"{path}.results.{implementation} must be an object")
    timings = implementation_result.get("timings_seconds")
    if not isinstance(timings, dict):
        raise LedgerError(
            f"{path}.results.{implementation}.timings_seconds must be an object"
        )
    runs = timings.get("runs")
    if not isinstance(runs, list) or not runs:
        raise LedgerError(
            f"{path}.results.{implementation}.timings_seconds.runs must be a non-empty array"
        )
    measured = [
        positive_number(
            value,
            f"{path}.results.{implementation}.timings_seconds.runs[{index}]",
        )
        for index, value in enumerate(runs)
    ]
    median = artifact_wall_seconds(artifact, implementation, path)
    validate_evidence_number(
        median,
        statistics.median(measured),
        f"{path}.results.{implementation}.timings_seconds.median",
        "measured runs",
    )
    return statistics.median(abs(value - median) for value in measured)


def artifact_backend_summary(artifact: dict[str, Any], path: str) -> dict[str, Any]:
    results = artifact_results(artifact, path)
    logs = results.get("backend_plan_logs")
    if not isinstance(logs, dict):
        raise LedgerError(f"{path}.results.backend_plan_logs must be an object")
    summary = logs.get("summary")
    if not isinstance(summary, dict):
        raise LedgerError(f"{path}.results.backend_plan_logs.summary must be an object")
    summary = dict(summary)
    resident_aliases = {
        "dirty_product_fft_requested_backend": (
            "dirty_product_gpu_resident_requested_backend"
        ),
        "dirty_product_fft_selected_backend": (
            "dirty_product_gpu_resident_selected_backend"
        ),
        "dirty_product_fft_fallback_used": "dirty_product_gpu_resident_fallback_used",
        "dirty_product_fft_total_ms": "dirty_product_gpu_resident_total_ms",
    }
    for generic_field, resident_field in resident_aliases.items():
        if summary.get(generic_field) is None and summary.get(resident_field) is not None:
            summary[generic_field] = summary[resident_field]
    return summary


def optional_artifact_stage_seconds(
    artifact: dict[str, Any], implementation: str, stage: str, path: str
) -> float | None:
    results = artifact_results(artifact, path)
    stage_medians = results.get("stage_medians_ms")
    if not isinstance(stage_medians, dict):
        raise LedgerError(f"{path} artifact results.stage_medians_ms must be an object")
    implementation_stages = stage_medians.get(implementation)
    if not isinstance(implementation_stages, dict):
        raise LedgerError(
            f"{path} artifact results.stage_medians_ms.{implementation} must be an object"
        )
    value = implementation_stages.get(stage)
    if value is None:
        return None
    return non_negative_number(value, f"{path} artifact stage {stage}") / 1000.0


def required_summary_number(
    summary: dict[str, Any], field: str, run_index: int
) -> float:
    return non_negative_number(
        summary.get(field), f"runs[{run_index}] candidate artifact {field}"
    )


def required_summary_integer(
    summary: dict[str, Any], field: str, run_index: int
) -> int:
    value = summary.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise LedgerError(
            f"runs[{run_index}] candidate artifact {field} must be a non-negative integer"
        )
    return value


def scale_artifact_value(value: Any, scale: float, field: str) -> Any:
    if isinstance(value, (bool, str)):
        if scale != 1.0:
            raise LedgerError(f"artifact field {field} cannot be scaled")
        return value
    return non_negative_number(value, f"candidate artifact {field}") * scale


def validate_evidence_value(value: Any, expected: Any, path: str, source: str) -> None:
    if (
        isinstance(expected, bool)
        or isinstance(expected, str)
        or isinstance(expected, int)
    ):
        if type(value) is not type(expected) or value != expected:
            raise LedgerError(
                f"{path} does not match {source}: recorded {value!r}, expected {expected!r}"
            )
        return
    validate_evidence_number(value, expected, path, source)


def validate_evidence_number(
    value: Any, expected: float, path: str, source: str
) -> None:
    actual = finite_number(value, path)
    if not math.isclose(
        actual,
        expected,
        rel_tol=EVIDENCE_REL_TOLERANCE,
        abs_tol=EVIDENCE_ABS_TOLERANCE,
    ):
        raise LedgerError(
            f"{path} does not match {source}: recorded {actual}, expected {expected}"
        )


def validate_run_arithmetic(run: dict[str, Any], run_index: int) -> None:
    baseline = positive_number(
        run.get("baseline_total_wall_s"), f"runs[{run_index}].baseline_total_wall_s"
    )
    candidate = positive_number(
        run.get("candidate_total_wall_s"), f"runs[{run_index}].candidate_total_wall_s"
    )
    validate_derived_number(
        run.get("speedup"),
        baseline / candidate,
        f"runs[{run_index}].speedup",
        "baseline_total_wall_s / candidate_total_wall_s",
    )
    validate_derived_number(
        run.get("wall_reduction_fraction"),
        (baseline - candidate) / baseline,
        f"runs[{run_index}].wall_reduction_fraction",
        "(baseline_total_wall_s - candidate_total_wall_s) / baseline_total_wall_s",
    )
    validate_stage_arithmetic(run, run_index, baseline, candidate)


def validate_stage_arithmetic(
    run: dict[str, Any], run_index: int, baseline_wall: float, candidate_wall: float
) -> None:
    stages = run.get("stages")
    if not isinstance(stages, list) or not stages:
        raise LedgerError(f"runs[{run_index}].stages must be a non-empty list")
    total_saved = baseline_wall - candidate_wall
    if math.isclose(total_saved, 0.0, abs_tol=ARITHMETIC_ABS_TOLERANCE):
        raise LedgerError(
            f"runs[{run_index}] cannot derive stage contribution with zero total saved wall"
        )
    seen_stages: set[str] = set()
    for stage_index, stage in enumerate(stages):
        stage_path = f"runs[{run_index}].stages[{stage_index}]"
        if not isinstance(stage, dict):
            raise LedgerError(f"{stage_path} must be an object")
        name = non_empty_string(stage.get("stage"), f"{stage_path}.stage")
        if name in seen_stages:
            raise LedgerError(f"{stage_path}.stage duplicates {name}")
        seen_stages.add(name)
        baseline = non_negative_number(
            stage.get("baseline_s"), f"{stage_path}.baseline_s"
        )
        candidate = non_negative_number(
            stage.get("candidate_s"), f"{stage_path}.candidate_s"
        )
        validate_derived_number(
            stage.get("delta_s"),
            candidate - baseline,
            f"{stage_path}.delta_s",
            "candidate_s - baseline_s",
        )
        validate_derived_number(
            stage.get("baseline_wall_fraction"),
            baseline / baseline_wall,
            f"{stage_path}.baseline_wall_fraction",
            "baseline_s / baseline_total_wall_s",
        )
        validate_derived_number(
            stage.get("candidate_wall_fraction"),
            candidate / candidate_wall,
            f"{stage_path}.candidate_wall_fraction",
            "candidate_s / candidate_total_wall_s",
        )
        validate_derived_number(
            stage.get("saved_fraction_of_baseline_wall"),
            (baseline - candidate) / baseline_wall,
            f"{stage_path}.saved_fraction_of_baseline_wall",
            "(baseline_s - candidate_s) / baseline_total_wall_s",
        )
        validate_derived_number(
            stage.get("contribution_to_total_saved_wall"),
            (baseline - candidate) / total_saved,
            f"{stage_path}.contribution_to_total_saved_wall",
            "(baseline_s - candidate_s) / total saved wall",
        )


def validate_derived_number(
    value: Any, expected: float, path: str, expression: str
) -> None:
    actual = finite_number(value, path)
    if not math.isclose(
        actual,
        expected,
        rel_tol=ARITHMETIC_REL_TOLERANCE,
        abs_tol=ARITHMETIC_ABS_TOLERANCE,
    ):
        raise LedgerError(
            f"{path} is inconsistent with {expression}: recorded {actual}, expected {expected}"
        )


def load_evidence_manifest(
    ledger: dict[str, Any], ledger_path: pathlib.Path
) -> dict[str, dict[str, Any]]:
    manifest_reference = non_empty_string(
        ledger.get("evidence_manifest"), "evidence_manifest"
    )
    relative_path = pathlib.Path(manifest_reference)
    if relative_path.is_absolute():
        raise LedgerError("evidence_manifest must be a relative path under evidence/")
    evidence_root = (ledger_path.parent / "evidence").resolve()
    manifest_path = (ledger_path.parent / relative_path).resolve()
    try:
        manifest_path.relative_to(evidence_root)
    except ValueError:
        raise LedgerError("evidence_manifest must resolve under evidence/") from None
    try:
        manifest = load_json_object(manifest_path, description="evidence manifest")
    except ArtifactError as error:
        raise LedgerError(str(error)) from error
    if manifest.get("schema_version") != 1:
        raise LedgerError("evidence manifest schema_version must be 1")
    artifacts = list_field(manifest, "artifacts")
    if not artifacts:
        raise LedgerError("evidence manifest artifacts must not be empty")
    artifacts_by_id: dict[str, dict[str, Any]] = {}
    seen_checked_in_paths: set[pathlib.Path] = set()
    seen_paths: set[str] = set()
    for index, artifact in enumerate(artifacts):
        artifact_path = f"evidence manifest artifacts[{index}]"
        if not isinstance(artifact, dict):
            raise LedgerError(f"{artifact_path} must be an object")
        artifact_id = non_empty_string(
            artifact.get("artifact_id"), f"{artifact_path}.artifact_id"
        )
        if artifact_id in artifacts_by_id:
            raise LedgerError(f"duplicate evidence manifest artifact_id: {artifact_id}")
        workload_id = non_empty_string(
            artifact.get("workload_id"), f"{artifact_path}.workload_id"
        )
        artifact_role = non_empty_string(
            artifact.get("artifact_role"), f"{artifact_path}.artifact_role"
        )
        if artifact_role not in SUPPORTED_ARTIFACT_ROLES:
            raise LedgerError(
                f"{artifact_path}.artifact_role is unsupported: {artifact_role}"
            )
        checked_in_reference = non_empty_string(
            artifact.get("checked_in_path"), f"{artifact_path}.checked_in_path"
        )
        checked_in_relative_path = pathlib.Path(checked_in_reference)
        if checked_in_relative_path.is_absolute():
            raise LedgerError(f"{artifact_path}.checked_in_path must be relative")
        checked_in_path = (ledger_path.parent / checked_in_relative_path).resolve()
        try:
            checked_in_path.relative_to(evidence_root)
        except ValueError:
            raise LedgerError(
                f"{artifact_path}.checked_in_path must resolve under evidence/"
            ) from None
        if checked_in_path in seen_checked_in_paths:
            raise LedgerError(
                f"duplicate evidence manifest checked_in_path: {checked_in_reference}"
            )
        source_path = non_empty_string(
            artifact.get("source_path"), f"{artifact_path}.source_path"
        )
        if not pathlib.Path(source_path).is_absolute():
            raise LedgerError(f"{artifact_path}.source_path must be absolute")
        if source_path in seen_paths:
            raise LedgerError(f"duplicate evidence manifest source_path: {source_path}")
        sha256 = non_empty_string(artifact.get("sha256"), f"{artifact_path}.sha256")
        if SHA256_PATTERN.fullmatch(sha256) is None:
            raise LedgerError(
                f"{artifact_path}.sha256 must be 64 lowercase hexadecimal characters"
            )
        if not checked_in_path.is_file():
            raise LedgerError(
                f"{artifact_path}.checked_in_path does not exist: {checked_in_reference}"
            )
        actual_sha256 = sha256_file(checked_in_path)
        if actual_sha256 != sha256:
            raise LedgerError(
                f"{artifact_path}.checked_in_path sha256 mismatch: "
                f"recorded {sha256}, computed {actual_sha256}"
            )
        try:
            payload = load_run_result(checked_in_path)
        except ContractError as error:
            raise LedgerError(str(error)) from error
        payload_workload = payload.get("workload")
        if not isinstance(payload_workload, dict):
            raise LedgerError(
                f"{artifact_path}.checked_in_path workload must be an object"
            )
        if payload_workload.get("id") != workload_id:
            raise LedgerError(
                f"{artifact_path}.checked_in_path workload.id does not match "
                f"manifest workload_id {workload_id}"
            )
        artifacts_by_id[artifact_id] = {
            "artifact_id": artifact_id,
            "workload_id": workload_id,
            "artifact_role": artifact_role,
            "checked_in_path": checked_in_reference,
            "source_path": source_path,
            "sha256": sha256,
            "payload": payload,
        }
        seen_checked_in_paths.add(checked_in_path)
        seen_paths.add(source_path)
    return artifacts_by_id


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as artifact_file:
        for chunk in iter(lambda: artifact_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_run_artifact_anchors(
    run: dict[str, Any],
    run_index: int,
    manifest_artifacts: dict[str, dict[str, Any]],
    referenced_artifact_ids: set[str],
) -> dict[str, dict[str, Any]]:
    anchors = run.get("evidence_artifacts")
    if anchors is None:
        return {}
    if not isinstance(anchors, dict) or not anchors:
        raise LedgerError(
            f"runs[{run_index}].evidence_artifacts must be a non-empty object"
        )
    result_paths = run.get("result_paths")
    if not isinstance(result_paths, dict):
        raise LedgerError(
            f"runs[{run_index}].result_paths must be an object when evidence_artifacts is present"
        )
    workload_id = string_field(run, "workload_id")
    anchored_artifacts: dict[str, dict[str, Any]] = {}
    for artifact_role, artifact_id_value in anchors.items():
        anchor_path = f"runs[{run_index}].evidence_artifacts.{artifact_role}"
        if artifact_role not in SUPPORTED_ARTIFACT_ROLES:
            raise LedgerError(f"{anchor_path} uses unsupported artifact role")
        artifact_id = non_empty_string(artifact_id_value, anchor_path)
        artifact = manifest_artifacts.get(artifact_id)
        if artifact is None:
            raise LedgerError(
                f"{anchor_path} references unknown manifest artifact {artifact_id}"
            )
        if artifact["artifact_role"] != artifact_role:
            raise LedgerError(
                f"{anchor_path} role does not match manifest artifact role "
                f"{artifact['artifact_role']}"
            )
        artifact_workload_id = workload_id
        if artifact_role == "baseline" and run.get("baseline_workload_id") is not None:
            artifact_workload_id = non_empty_string(
                run["baseline_workload_id"], f"runs[{run_index}].baseline_workload_id"
            )
        if artifact["workload_id"] != artifact_workload_id:
            raise LedgerError(
                f"{anchor_path} workload_id does not match manifest artifact workload_id "
                f"{artifact['workload_id']}"
            )
        result_path_key = ARTIFACT_RESULT_PATH_KEYS[artifact_role]
        if result_paths.get(result_path_key) != artifact["source_path"]:
            raise LedgerError(
                f"{anchor_path} is not anchored to result_paths.{result_path_key}"
            )
        if artifact_id in referenced_artifact_ids:
            raise LedgerError(
                f"manifest artifact {artifact_id} is referenced more than once"
            )
        referenced_artifact_ids.add(artifact_id)
        anchored_artifacts[artifact_role] = artifact
    return anchored_artifacts


def non_empty_string(value: Any, path: str) -> str:
    if not isinstance(value, str) or not value:
        raise LedgerError(f"{path} must be a non-empty string")
    return value


def finite_number(value: Any, path: str) -> float:
    try:
        number = contract_finite_number(value, field=path, optional=False)
    except ContractError as error:
        raise LedgerError(str(error)) from error
    assert number is not None
    return number


def positive_number(value: Any, path: str) -> float:
    number = finite_number(value, path)
    if number <= 0:
        raise LedgerError(f"{path} must be greater than zero")
    return number


def required_positive_number(value: Any, path: str) -> float:
    if value is None:
        raise LedgerError(f"{path} must be present and greater than zero")
    return positive_number(value, path)


def non_negative_number(value: Any, path: str) -> float:
    number = finite_number(value, path)
    if number < 0:
        raise LedgerError(f"{path} must be non-negative")
    return number


def object_field(value: dict[str, Any], key: str) -> dict[str, Any]:
    field = value.get(key)
    if not isinstance(field, dict):
        raise LedgerError(f"{key} must be an object")
    return field


def list_field(value: dict[str, Any], key: str) -> list[Any]:
    field = value.get(key)
    if not isinstance(field, list):
        raise LedgerError(f"{key} must be a list")
    return field


def string_field(value: dict[str, Any], key: str) -> str:
    field = value.get(key)
    if not isinstance(field, str) or not field:
        raise LedgerError(f"{key} must be a non-empty string")
    return field


def string_list_field(value: dict[str, Any], key: str) -> list[str]:
    field = list_field(value, key)
    if not all(isinstance(item, str) and item for item in field):
        raise LedgerError(f"{key} must contain only non-empty strings")
    return field


if __name__ == "__main__":
    main()
