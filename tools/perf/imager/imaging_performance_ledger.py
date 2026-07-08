#!/usr/bin/env python3
"""Validate and summarize the broad imaging performance ledger."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any


LEDGER_PATH = pathlib.Path(__file__).resolve().parent / "imaging_performance_ledger.json"
REQUIRED_ISSUES = {56, 343, 262, 352}
REQUIRED_SUMMARY_COLUMNS = {
    "workload_id",
    "issue_slice",
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
}
REQUIRED_GPU_COLUMNS = {
    "device_fft_s",
    "device_product_assembly_s",
    "device_to_host_final_export_s",
    "fallback_reason",
}


class LedgerError(Exception):
    """Validation error shown without a traceback."""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=pathlib.Path, default=LEDGER_PATH)
    parser.add_argument("--format", choices=("text", "markdown", "json"), default="text")
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
        ledger = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise LedgerError(f"parse {path}: {error}") from error
    if not isinstance(ledger, dict):
        raise LedgerError(f"{path} must contain a JSON object")
    validate_ledger(ledger, path)
    return ledger


def validate_ledger(ledger: dict[str, Any], path: pathlib.Path) -> None:
    if ledger.get("schema_version") != 1:
        raise LedgerError("schema_version must be 1")
    issues = {int(value) for value in list_field(ledger, "wave_issues")}
    if issues != REQUIRED_ISSUES:
        raise LedgerError(
            "wave_issues must be exactly " + ", ".join(str(issue) for issue in sorted(REQUIRED_ISSUES))
        )
    require_columns(ledger, "summary_columns", REQUIRED_SUMMARY_COLUMNS)
    require_columns(ledger, "stage_columns", REQUIRED_STAGE_COLUMNS)
    require_columns(ledger, "overlap_columns", REQUIRED_OVERLAP_COLUMNS)
    require_columns(ledger, "gpu_columns", REQUIRED_GPU_COLUMNS)
    formulas = object_field(ledger, "formulas")
    for key in ("speedup", "wall_reduction_fraction", "contribution_to_total_saved_wall"):
        if not isinstance(formulas.get(key), str) or not formulas[key]:
            raise LedgerError(f"formulas.{key} must be a non-empty string")
    workload_groups = list_field(ledger, "workload_groups")
    if not workload_groups:
        raise LedgerError("workload_groups must not be empty")
    workload_dir = path.parent / "workloads"
    covered_issues: set[int] = set()
    for index, group in enumerate(workload_groups):
        if not isinstance(group, dict):
            raise LedgerError(f"workload_groups[{index}] must be an object")
        group_id = string_field(group, "group_id")
        owner_issues = {int(value) for value in list_field(group, "owner_issues")}
        if not owner_issues:
            raise LedgerError(f"{group_id}: owner_issues must not be empty")
        if not owner_issues <= REQUIRED_ISSUES:
            raise LedgerError(f"{group_id}: owner_issues contains unexpected issue")
        covered_issues |= owner_issues
        workloads = [string for string in list_field(group, "workloads") if isinstance(string, str)]
        if not workloads:
            raise LedgerError(f"{group_id}: workloads must not be empty")
        missing = [workload for workload in workloads if not (workload_dir / workload).exists()]
        if missing:
            raise LedgerError(f"{group_id}: missing workload file(s): {', '.join(missing)}")
        roles = [role for role in list_field(group, "required_roles") if isinstance(role, str)]
        if not roles:
            raise LedgerError(f"{group_id}: required_roles must not be empty")
    missing_issues = REQUIRED_ISSUES - covered_issues
    if missing_issues:
        raise LedgerError(
            "workload_groups do not cover issue(s): "
            + ", ".join(str(issue) for issue in sorted(missing_issues))
        )
    for index, run in enumerate(list_field(ledger, "runs")):
        if not isinstance(run, dict):
            raise LedgerError(f"runs[{index}] must be an object")
        for key in REQUIRED_SUMMARY_COLUMNS:
            if key not in run:
                raise LedgerError(f"runs[{index}] missing {key}")


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
