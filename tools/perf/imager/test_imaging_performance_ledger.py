#!/usr/bin/env python3
"""Tests for the broad imaging performance ledger validator."""

from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest

import imaging_performance_ledger as ledger_tool


def test_default_ledger_validates_and_covers_wave_issues() -> None:
    ledger = ledger_tool.load_ledger(ledger_tool.LEDGER_PATH)
    summary = ledger_tool.summarize(ledger)

    assert set(summary["wave_issues"]) == {56, 262, 343, 352}
    assert summary["workload_group_count"] == 6
    assert summary["run_count"] == 7


def valid_ledger(tmp_path: pathlib.Path) -> tuple[dict[str, Any], pathlib.Path]:
    ledger = json.loads(ledger_tool.LEDGER_PATH.read_text(encoding="utf-8"))
    ledger["summary_columns"] = [*ledger["summary_columns"], "evidence_roles"]
    ledger["casa_columns"] = sorted(ledger_tool.REQUIRED_CASA_COLUMNS)
    ledger["workload_groups"] = [
        {
            "group_id": "complete_evidence",
            "owner_issues": [56, 262, 343, 352],
            "workloads": ["complete-evidence.json"],
            "required_roles": [
                "serial_cpu",
                "read_ahead_enabled",
                "metal_f32_candidate",
                "casa_oracle",
            ],
        }
    ]
    ledger["runs"] = [
        {
            "workload_id": "complete-evidence",
            "issue_slice": "validator-contract",
            "baseline_total_wall_s": 2.0,
            "candidate_total_wall_s": 1.0,
            "speedup": 2.0,
            "wall_reduction_fraction": 0.5,
            "correctness_status": "good",
            "evidence_roles": [
                {"group_id": "complete_evidence", "role": "serial_cpu"},
                {"group_id": "complete_evidence", "role": "read_ahead_enabled"},
                {"group_id": "complete_evidence", "role": "metal_f32_candidate"},
                {"group_id": "complete_evidence", "role": "casa_oracle"},
            ],
            "overlap": {
                "producer_active_s": 0.8,
                "consumer_active_s": 0.9,
                "producer_consumer_overlap_s": 0.7,
                "queue_high_water_bytes": 4096,
                "effective_read_bandwidth_mb_s": 512.0,
            },
            "gpu": {
                "device_fft_s": 0.1,
                "device_product_assembly_s": 0.2,
                "device_to_host_final_export_s": 0.3,
                "fallback_reason": "not_applicable_gpu_selected",
            },
            "casa": {
                "oracle_result_path": "/tmp/oracle.json",
                "comparison_result_path": "/tmp/comparison.json",
                "comparison_status": "good",
                "max_abs": 1.0e-7,
                "rms": 1.0e-8,
            },
        }
    ]

    workload_dir = tmp_path / "workloads"
    workload_dir.mkdir()
    (workload_dir / "complete-evidence.json").write_text("{}\n", encoding="utf-8")
    path = tmp_path / "ledger.json"
    write_ledger(path, ledger)
    return ledger, path


def write_ledger(path: pathlib.Path, ledger: dict[str, Any]) -> None:
    path.write_text(json.dumps(ledger), encoding="utf-8")


def test_complete_role_evidence_validates_and_covers_wave_issues(
    tmp_path: pathlib.Path,
) -> None:
    _, path = valid_ledger(tmp_path)
    ledger = ledger_tool.load_ledger(path)
    summary = ledger_tool.summarize(ledger)

    assert set(summary["wave_issues"]) == {56, 343, 262, 352}
    assert summary["workload_group_count"] == 1
    assert summary["run_count"] == 1


def test_ledger_rejects_missing_required_issue(tmp_path: pathlib.Path) -> None:
    ledger, path = valid_ledger(tmp_path)
    ledger["wave_issues"] = [56, 343, 352]
    write_ledger(path, ledger)

    with pytest.raises(ledger_tool.LedgerError, match="wave_issues"):
        ledger_tool.load_ledger(path)


def test_ledger_rejects_run_without_explicit_evidence_roles(tmp_path: pathlib.Path) -> None:
    ledger, path = valid_ledger(tmp_path)
    del ledger["runs"][0]["evidence_roles"]
    write_ledger(path, ledger)

    with pytest.raises(ledger_tool.LedgerError, match=r"runs\[0\]\.evidence_roles"):
        ledger_tool.load_ledger(path)


def test_ledger_rejects_unsatisfied_required_role(tmp_path: pathlib.Path) -> None:
    ledger, path = valid_ledger(tmp_path)
    ledger["runs"][0]["evidence_roles"] = [
        claim
        for claim in ledger["runs"][0]["evidence_roles"]
        if claim["role"] != "serial_cpu"
    ]
    write_ledger(path, ledger)

    with pytest.raises(
        ledger_tool.LedgerError,
        match=r"complete_evidence: required_roles missing recorded evidence: serial_cpu",
    ):
        ledger_tool.load_ledger(path)


def test_ledger_rejects_role_claim_from_undeclared_workload(tmp_path: pathlib.Path) -> None:
    ledger, path = valid_ledger(tmp_path)
    ledger["runs"][0]["workload_id"] = "different-workload"
    write_ledger(path, ledger)

    with pytest.raises(ledger_tool.LedgerError, match="not declared by that group"):
        ledger_tool.load_ledger(path)


def test_ledger_rejects_failing_casa_oracle_claim(tmp_path: pathlib.Path) -> None:
    ledger, path = valid_ledger(tmp_path)
    ledger["runs"][0]["casa"]["comparison_status"] = "bad"
    write_ledger(path, ledger)

    with pytest.raises(ledger_tool.LedgerError, match="non-failing comparison_status"):
        ledger_tool.load_ledger(path)


@pytest.mark.parametrize(
    ("role", "object_name", "field"),
    [
        ("read_ahead_enabled", "overlap", "producer_consumer_overlap_s"),
        ("read_ahead_enabled", "overlap", "effective_read_bandwidth_mb_s"),
        ("metal_f32_candidate", "gpu", "device_product_assembly_s"),
        ("metal_f32_candidate", "gpu", "fallback_reason"),
        ("casa_oracle", "casa", "comparison_result_path"),
        ("casa_oracle", "casa", "rms"),
    ],
)
@pytest.mark.parametrize("failure_mode", ["missing", "null"])
def test_claimed_role_rejects_incomplete_evidence(
    tmp_path: pathlib.Path,
    role: str,
    object_name: str,
    field: str,
    failure_mode: str,
) -> None:
    ledger, path = valid_ledger(tmp_path)
    if failure_mode == "missing":
        del ledger["runs"][0][object_name][field]
    else:
        ledger["runs"][0][object_name][field] = None
    write_ledger(path, ledger)

    with pytest.raises(
        ledger_tool.LedgerError,
        match=rf"{role}.*{object_name}\.{field}.*(?:missing|null)",
    ):
        ledger_tool.load_ledger(path)
