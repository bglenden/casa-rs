#!/usr/bin/env python3
"""Adversarial tests for the imaging performance evidence ledger."""

from __future__ import annotations

import hashlib
import json
import pathlib
from typing import Any, Callable

import pytest

import imaging_performance_ledger as ledger_tool


def canonical_result(
    *,
    kind: str,
    run_id: str,
    workload_id: str,
    results: dict[str, Any],
    status: str = "completed",
) -> dict[str, Any]:
    if status not in {"completed", "dry_run"}:
        results = {
            **results,
            "failure": {"kind": "comparison_tolerance", "reason": "test failure"},
        }
    return {
        "schema_version": 2,
        "kind": kind,
        "status": status,
        "run_id": run_id,
        "created_at": "2026-07-18T00:00:00Z",
        "workload": {"id": workload_id},
        "environment": {},
        "artifacts": {},
        "results": results,
    }


def write_json(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def stage_row(
    name: str,
    baseline: float,
    candidate: float,
    baseline_wall: float,
    candidate_wall: float,
) -> dict[str, Any]:
    total_saved = baseline_wall - candidate_wall
    return {
        "stage": name,
        "baseline_s": baseline,
        "candidate_s": candidate,
        "delta_s": candidate - baseline,
        "baseline_wall_fraction": baseline / baseline_wall,
        "candidate_wall_fraction": candidate / candidate_wall,
        "saved_fraction_of_baseline_wall": (baseline - candidate) / baseline_wall,
        "contribution_to_total_saved_wall": (baseline - candidate) / total_saved,
    }


def recompute_run_arithmetic(run: dict[str, Any]) -> None:
    baseline = run["baseline_total_wall_s"]
    candidate = run["candidate_total_wall_s"]
    run["speedup"] = baseline / candidate
    run["wall_reduction_fraction"] = (baseline - candidate) / baseline
    for stage in run["stages"]:
        stage.update(
            stage_row(
                stage["stage"],
                stage["baseline_s"],
                stage["candidate_s"],
                baseline,
                candidate,
            )
        )


def run_artifact(
    workload_id: str,
    wall_s: float,
    stages_ms: dict[str, float],
    backend_summary: dict[str, Any],
) -> dict[str, Any]:
    return canonical_result(
        kind="workload_run",
        run_id=f"{workload_id}-run",
        workload_id=workload_id,
        results={
            "rust": {"timings_seconds": {"median": wall_s, "runs": [wall_s]}},
            "casa": {"timings_seconds": {"median": None, "runs": []}},
            "stage_medians_ms": {"rust": stages_ms, "casa": {}},
            "backend_plan_logs": {"summary": backend_summary},
        },
    )


def comparison_artifact(
    overall: str, max_abs: float, max_normalized_rms: float
) -> dict[str, Any]:
    return {
        "status": "completed",
        "structured_difference_review": {
            "label": overall,
            "products": {".image": overall},
        },
        "products": {
            ".image": {
                "status": "compared",
                "diff_abs_max": max_abs,
                "diff_rms": max_abs / 2.0,
                "diff_rms_over_casa_rms": max_normalized_rms,
                "structured_difference": {
                    "normalized_diff_rms": max_normalized_rms,
                    "classification": {"overall": overall},
                },
            }
        },
    }


def test_derive_comparison_accepts_nested_workload_result() -> None:
    nested = comparison_artifact("good", 0.02, 0.001)
    artifact = {
        "payload": {
            "status": "completed",
            "results": {"product_comparison": nested},
        }
    }

    derived = ledger_tool.derive_comparison_evidence(artifact, "nested")

    assert derived["overall"] == "good"
    assert derived["max_abs"] == 0.02
    assert derived["max_normalized_rms"] == 0.001


def counterbalanced_artifact(*, no_slowdown: bool = True) -> dict[str, Any]:
    baseline_seconds = 20.0
    candidate_seconds = 15.0 if no_slowdown else 25.0
    relative_delta = (candidate_seconds - baseline_seconds) / baseline_seconds
    roles = ("baseline", "candidate", "candidate", "baseline")
    schedule = []
    runs = []
    for position, role in enumerate(roles, start=1):
        workload = f"workloads/wave352-{'cpu' if role == 'baseline' else 'metal'}.json"
        scheduled = {
            "sequence_index": position,
            "phase": "measured",
            "block_index": 1,
            "position_in_block": position,
            "role": role,
            "workload": workload,
        }
        schedule.append(scheduled)
        runs.append(
            {
                **scheduled,
                "result_status": "completed",
                "total_wall_seconds": (
                    baseline_seconds if role == "baseline" else candidate_seconds
                ),
            }
        )
    details = {
            "configuration": {
                "baseline_workload": "workloads/wave352-cpu.json",
                "candidate_workload": "workloads/wave352-metal.json",
                "measured_pair_count": 1,
                "slowdown_tolerance_fraction": 0.0,
            },
            "verdict": {
                "status": "pass" if no_slowdown else "fail",
                "no_slowdown": no_slowdown,
                "observed_median_relative_delta": relative_delta,
            },
            "schedule": schedule,
            "runs": runs,
            "paired_deltas": [
                {
                    "block_index": 1,
                    "baseline_seconds": 20.0,
                    "candidate_seconds": candidate_seconds,
                    "delta_seconds": candidate_seconds - 20.0,
                    "relative_delta": relative_delta,
                },
            ],
    }
    status = "completed" if no_slowdown else "out_of_tolerance"
    return {
        "payload": canonical_result(
            kind="alternating_comparison",
            run_id="counterbalanced-test",
            workload_id="workloads/wave352-metal.json",
            status=status,
            results={"alternating_comparison": details},
        )
    }


def counterbalanced_details(artifact: dict[str, Any]) -> dict[str, Any]:
    return artifact["payload"]["results"]["alternating_comparison"]


def valid_fixture(tmp_path: pathlib.Path) -> dict[str, Any]:
    baseline_workload = "wave352-cpu"
    candidate_workload = "wave352-metal"
    oracle_workload = "wave262-oracle"
    for workload in (baseline_workload, candidate_workload, oracle_workload):
        write_json(tmp_path / "workloads" / f"{workload}.json", {})

    baseline_summary = {
        "source_read_ahead_enabled": False,
        "source_read_ahead_max_live_row_blocks": 1,
    }
    candidate_summary = {
        "dirty_product_fft_fallback_used": False,
        "dirty_product_fft_requested_backend": "metal-mpsgraph",
        "dirty_product_fft_selected_backend": "metal-mpsgraph",
        "dirty_product_fft_total_ms": 12.0,
        "dirty_product_gpu_resident_device_exec_ms": 3.5,
        "dirty_product_gpu_resident_exec_ms": 4.0,
        "dirty_product_gpu_resident_fallback_used": False,
        "dirty_product_gpu_resident_pack_ms": 2.0,
        "dirty_product_gpu_resident_plan_ms": 0.1,
        "dirty_product_gpu_resident_postprocess_ms": 0.6,
        "dirty_product_gpu_resident_requested_backend": "metal-mpsgraph",
        "dirty_product_gpu_resident_selected_backend": "metal-mpsgraph",
        "dirty_product_gpu_resident_sync_ms": 0.2,
        "dirty_product_gpu_resident_total_ms": 12.0,
        "dirty_product_gpu_resident_transfer_from_device_ms": 2.5,
        "dirty_product_gpu_resident_transfer_to_device_ms": 3.0,
        "metal_device_available": True,
        "resolved_backend": "cpu",
        "source_read_ahead_consumer_ms": 5000.0,
        "source_read_ahead_consumer_recv_blocked_ms": 500.0,
        "source_read_ahead_effective_read_bandwidth_mib_s": 250.0,
        "source_read_ahead_enabled": True,
        "source_read_ahead_live_row_block_high_water": 2,
        "source_read_ahead_max_live_row_blocks": 2,
        "source_read_ahead_producer_consumer_overlap_ms": 2000.0,
        "source_read_ahead_producer_send_blocked_ms": 250.0,
        "source_read_ahead_queue_capacity": 0,
        "source_read_ahead_source_read_ms": 3000.0,
        "source_read_ahead_source_route_ms": 1000.0,
        "source_stream_buffer_bytes": 4096,
    }
    artifacts = {
        "cpu": run_artifact(
            baseline_workload,
            12.0,
            {"frontend_total": 11000.0, "psf_fft": 80.0},
            baseline_summary,
        ),
        "metal": run_artifact(
            candidate_workload,
            10.0,
            {"frontend_total": 9000.0, "psf_fft": 40.0},
            candidate_summary,
        ),
        "metal-comparison": canonical_result(
            kind="image_comparison",
            run_id="metal-comparison",
            workload_id=candidate_workload,
            results={"product_comparison": comparison_artifact("good", 0.02, 0.001)},
        ),
        "oracle": canonical_result(
            kind="workload_run",
            run_id="oracle-run",
            workload_id=oracle_workload,
            results={
                "casa": {"timings_seconds": {"median": 20.0, "runs": [20.0]}},
                "rust": {"timings_seconds": {"median": 8.0, "runs": [8.0]}},
                "stage_medians_ms": {"casa": {}, "rust": {}},
                "backend_plan_logs": {"summary": {}},
            },
        ),
        "oracle-comparison": canonical_result(
            kind="image_comparison",
            run_id="oracle-comparison",
            workload_id=oracle_workload,
            results={
                "product_comparison": comparison_artifact("investigate", 0.2, 0.02)
            },
        ),
    }
    artifact_specs = [
        ("cpu", baseline_workload, "baseline"),
        ("metal", candidate_workload, "candidate"),
        ("metal-comparison", candidate_workload, "product_comparison"),
        ("oracle", oracle_workload, "casa_oracle"),
        ("oracle-comparison", oracle_workload, "full_comparison"),
    ]
    manifest_artifacts = []
    artifact_paths: dict[str, pathlib.Path] = {}
    for artifact_id, workload_id, artifact_role in artifact_specs:
        checked_in_path = pathlib.Path("evidence") / "artifacts" / f"{artifact_id}.json"
        artifact_path = tmp_path / checked_in_path
        write_json(artifact_path, artifacts[artifact_id])
        artifact_paths[artifact_id] = artifact_path
        manifest_artifacts.append(
            {
                "artifact_id": artifact_id,
                "workload_id": workload_id,
                "artifact_role": artifact_role,
                "checked_in_path": str(checked_in_path),
                "source_path": f"/private/tmp/evidence/{artifact_id}.json",
                "sha256": hashlib.sha256(artifact_path.read_bytes()).hexdigest(),
            }
        )
    manifest = {"schema_version": 1, "artifacts": manifest_artifacts}
    manifest_path = tmp_path / "evidence" / "manifest.json"
    write_json(manifest_path, manifest)

    metal_run = {
        "workload_id": candidate_workload,
        "baseline_workload_id": baseline_workload,
        "issue_slice": "352-metal",
        "baseline_total_wall_s": 12.0,
        "candidate_total_wall_s": 10.0,
        "speedup": 1.2,
        "wall_reduction_fraction": 1.0 / 6.0,
        "correctness_status": "CPU versus Metal comparison is overall good.",
        "max_abs": 0.02,
        "rms": 0.001,
        "read_ahead_blocks": {"baseline": 1, "candidate": 2},
        "evidence_roles": [
            {"group_id": "complete", "role": "cpu_f32_baseline"},
            {"group_id": "complete", "role": "metal_f32_candidate"},
            {"group_id": "complete", "role": "read_ahead_enabled"},
        ],
        "result_paths": {
            "baseline": "/private/tmp/evidence/cpu.json",
            "candidate": "/private/tmp/evidence/metal.json",
            "product_comparison": "/private/tmp/evidence/metal-comparison.json",
        },
        "evidence_artifacts": {
            "baseline": "cpu",
            "candidate": "metal",
            "product_comparison": "metal-comparison",
        },
        "stages": [
            stage_row("frontend_total", 11.0, 9.0, 12.0, 10.0),
            stage_row("psf_fft", 0.08, 0.04, 12.0, 10.0),
        ],
        "overlap": {
            "producer_active_s": 4.0,
            "consumer_active_s": 5.0,
            "producer_consumer_overlap_s": 2.0,
            "consumer_blocked_on_input_s": 0.5,
            "producer_blocked_on_capacity_s": 0.25,
            "queue_high_water_blocks": 2,
            "queue_high_water_bytes": 4096,
            "effective_read_bandwidth_mb_s": 250.0,
        },
        "gpu": {
            "dirty_product_fft_fallback_used": False,
            "dirty_product_fft_selected_backend": "metal-mpsgraph",
            "dirty_product_fft_total_ms": 12.0,
            "dirty_product_gpu_resident_postprocess_ms": 0.6,
            "f32_cast_pack_s": 0.002,
            "fallback_reason": "not_applicable_metal_selected",
            "gpu_sync_wait_s": 0.0002,
            "host_to_device_staging_s": 0.003,
            "metal_device_available": True,
            "device_fft_s": 0.0035,
            "device_product_assembly_s": 0.0006,
            "device_to_host_final_export_s": 0.0025,
        },
    }
    oracle_run = {
        "workload_id": oracle_workload,
        "issue_slice": "262-casa",
        "baseline_total_wall_s": 20.0,
        "candidate_total_wall_s": 8.0,
        "speedup": 2.5,
        "wall_reduction_fraction": 0.6,
        "correctness_status": "Full comparison is overall investigate.",
        "max_abs": 0.2,
        "rms": 0.02,
        "evidence_roles": [{"group_id": "complete", "role": "casa_oracle"}],
        "result_paths": {
            "oracle": "/private/tmp/evidence/oracle.json",
            "all_product_comparison": "/private/tmp/evidence/oracle-comparison.json",
        },
        "evidence_artifacts": {
            "casa_oracle": "oracle",
            "full_comparison": "oracle-comparison",
        },
        "stages": [stage_row("frontend_total", 20.0, 8.0, 20.0, 8.0)],
        "casa": {
            "oracle_result_path": "/private/tmp/evidence/oracle.json",
            "comparison_result_path": "/private/tmp/evidence/oracle-comparison.json",
            "comparison_status": "investigate",
            "max_abs": 0.2,
            "rms": 0.02,
            "adjudication": {
                "rationale": (
                    "The investigated product remains within explicit numerical "
                    "acceptance bounds."
                ),
                "products": [".image"],
                "bounds": {
                    "max_abs": 0.2,
                    "max_normalized_rms": 0.02,
                },
            },
        },
    }
    ledger = {
        "schema_version": 1,
        "wave_issues": [56, 262, 343, 352],
        "evidence_manifest": "evidence/manifest.json",
        "summary_columns": sorted(ledger_tool.REQUIRED_SUMMARY_COLUMNS),
        "stage_columns": sorted(ledger_tool.REQUIRED_STAGE_COLUMNS),
        "overlap_columns": sorted(ledger_tool.REQUIRED_OVERLAP_COLUMNS),
        "gpu_columns": sorted(ledger_tool.REQUIRED_GPU_COLUMNS),
        "casa_columns": sorted(ledger_tool.REQUIRED_CASA_COLUMNS),
        "formulas": dict(ledger_tool.CANONICAL_FORMULAS),
        "workload_groups": [
            {
                "group_id": "complete",
                "owner_issues": [56, 262, 343, 352],
                "workloads": [
                    f"{baseline_workload}.json",
                    f"{candidate_workload}.json",
                    f"{oracle_workload}.json",
                ],
                "required_roles": [
                    "cpu_f32_baseline",
                    "metal_f32_candidate",
                    "read_ahead_enabled",
                    "casa_oracle",
                ],
            }
        ],
        "runs": [metal_run, oracle_run],
    }
    ledger_path = tmp_path / "ledger.json"
    write_json(ledger_path, ledger)
    return {
        "ledger": ledger,
        "ledger_path": ledger_path,
        "manifest": manifest,
        "manifest_path": manifest_path,
        "artifact_paths": artifact_paths,
        "artifacts": artifacts,
    }


def validate(fixture: dict[str, Any]) -> None:
    ledger_tool.validate_ledger(fixture["ledger"], fixture["ledger_path"])


def rewrite_artifact(
    fixture: dict[str, Any], artifact_id: str, mutate: Callable[[dict[str, Any]], None]
) -> None:
    artifact = fixture["artifacts"][artifact_id]
    target = artifact
    if artifact.get("kind") == "image_comparison":
        target = artifact["results"]["product_comparison"]
    mutate(target)
    artifact_path = fixture["artifact_paths"][artifact_id]
    write_json(artifact_path, artifact)
    manifest_entry = next(
        entry
        for entry in fixture["manifest"]["artifacts"]
        if entry["artifact_id"] == artifact_id
    )
    manifest_entry["sha256"] = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    write_json(fixture["manifest_path"], fixture["manifest"])


def test_semantically_bound_fixture_validates(tmp_path: pathlib.Path) -> None:
    validate(valid_fixture(tmp_path))


def test_rejects_wall_median_not_derived_from_artifact(tmp_path: pathlib.Path) -> None:
    fixture = valid_fixture(tmp_path)
    fixture["ledger"]["runs"][0]["baseline_total_wall_s"] = 99.0
    recompute_run_arithmetic(fixture["ledger"]["runs"][0])

    with pytest.raises(ledger_tool.LedgerError, match="baseline_total_wall_s.*median"):
        validate(fixture)


@pytest.mark.parametrize(
    "field",
    [
        "delta_s",
        "baseline_wall_fraction",
        "candidate_wall_fraction",
        "saved_fraction_of_baseline_wall",
        "contribution_to_total_saved_wall",
    ],
)
def test_rejects_impossible_stage_arithmetic(
    tmp_path: pathlib.Path, field: str
) -> None:
    fixture = valid_fixture(tmp_path)
    fixture["ledger"]["runs"][0]["stages"][0][field] += 0.25

    with pytest.raises(ledger_tool.LedgerError, match=field):
        validate(fixture)


def test_rejects_stage_value_not_derived_from_artifact(tmp_path: pathlib.Path) -> None:
    fixture = valid_fixture(tmp_path)
    rewrite_artifact(
        fixture,
        "metal",
        lambda artifact: artifact["results"]["stage_medians_ms"]["rust"].update(
            {"psf_fft": 45.0}
        ),
    )

    with pytest.raises(ledger_tool.LedgerError, match=r"candidate_s.*artifact stage"):
        validate(fixture)


def test_rejects_fabricated_overlap_telemetry(tmp_path: pathlib.Path) -> None:
    fixture = valid_fixture(tmp_path)
    fixture["ledger"]["runs"][0]["overlap"]["producer_active_s"] = 1.0

    with pytest.raises(ledger_tool.LedgerError, match="producer_active_s"):
        validate(fixture)


def test_rejects_fabricated_backend_telemetry(tmp_path: pathlib.Path) -> None:
    fixture = valid_fixture(tmp_path)
    fixture["ledger"]["runs"][0]["gpu"]["metal_device_available"] = False

    with pytest.raises(ledger_tool.LedgerError, match="metal_device_available"):
        validate(fixture)


def test_rejects_fabricated_comparison_overall(tmp_path: pathlib.Path) -> None:
    fixture = valid_fixture(tmp_path)

    def investigate(artifact: dict[str, Any]) -> None:
        artifact["structured_difference_review"]["label"] = "investigate"
        artifact["structured_difference_review"]["products"][".image"] = "investigate"

    rewrite_artifact(fixture, "metal-comparison", investigate)

    with pytest.raises(
        ledger_tool.LedgerError, match="correctness_status.*overall investigate"
    ):
        validate(fixture)


def test_rejects_fabricated_max_normalized_rms(tmp_path: pathlib.Path) -> None:
    fixture = valid_fixture(tmp_path)
    fixture["ledger"]["runs"][0]["rms"] = 0.0001

    with pytest.raises(ledger_tool.LedgerError, match="maximum normalized RMS"):
        validate(fixture)


def test_rejects_accepted_metal_wall_slowdown(tmp_path: pathlib.Path) -> None:
    fixture = valid_fixture(tmp_path)
    rewrite_artifact(
        fixture,
        "cpu",
        lambda artifact: artifact["results"]["rust"]["timings_seconds"].update(
            {"median": 9.0, "runs": [9.0]}
        ),
    )
    metal_run = fixture["ledger"]["runs"][0]
    metal_run["baseline_total_wall_s"] = 9.0
    recompute_run_arithmetic(metal_run)

    with pytest.raises(
        ledger_tool.LedgerError,
        match="accepted Metal candidate exceeds.*measured no-slowdown uncertainty",
    ):
        validate(fixture)


def test_single_run_metal_slowdown_has_no_fabricated_tolerance(tmp_path: pathlib.Path) -> None:
    fixture = valid_fixture(tmp_path)
    rewrite_artifact(
        fixture,
        "cpu",
        lambda artifact: artifact["results"]["rust"]["timings_seconds"].update(
            {"median": 10.0, "runs": [10.0]}
        ),
    )
    rewrite_artifact(
        fixture,
        "metal",
        lambda artifact: artifact["results"]["rust"]["timings_seconds"].update(
            {"median": 10.04, "runs": [10.04]}
        ),
    )
    metal_run = fixture["ledger"]["runs"][0]
    metal_run["baseline_total_wall_s"] = 10.0
    metal_run["candidate_total_wall_s"] = 10.04
    recompute_run_arithmetic(metal_run)

    with pytest.raises(
        ledger_tool.LedgerError,
        match="accepted Metal candidate exceeds.*measured no-slowdown uncertainty",
    ):
        validate(fixture)


def test_repeated_metal_wall_within_measured_spread_is_accepted(
    tmp_path: pathlib.Path,
) -> None:
    fixture = valid_fixture(tmp_path)
    rewrite_artifact(
        fixture,
        "cpu",
        lambda artifact: artifact["results"]["rust"]["timings_seconds"].update(
            {"median": 10.0, "runs": [9.9, 10.0, 10.1]}
        ),
    )
    rewrite_artifact(
        fixture,
        "metal",
        lambda artifact: artifact["results"]["rust"]["timings_seconds"].update(
            {"median": 10.04, "runs": [9.95, 10.04, 10.13]}
        ),
    )
    metal_run = fixture["ledger"]["runs"][0]
    metal_run["baseline_total_wall_s"] = 10.0
    metal_run["candidate_total_wall_s"] = 10.04
    recompute_run_arithmetic(metal_run)

    validate(fixture)


def test_rejects_accepted_metal_stage_slowdown(tmp_path: pathlib.Path) -> None:
    fixture = valid_fixture(tmp_path)
    rewrite_artifact(
        fixture,
        "metal",
        lambda artifact: artifact["results"]["stage_medians_ms"]["rust"].update(
            {"psf_fft": 90.0}
        ),
    )
    stage = fixture["ledger"]["runs"][0]["stages"][1]
    stage["candidate_s"] = 0.09
    recompute_run_arithmetic(fixture["ledger"]["runs"][0])

    with pytest.raises(
        ledger_tool.LedgerError, match="accepted Metal psf_fft stage is not faster"
    ):
        validate(fixture)


def test_rejects_absent_required_352_timing(tmp_path: pathlib.Path) -> None:
    fixture = valid_fixture(tmp_path)
    rewrite_artifact(
        fixture,
        "metal",
        lambda artifact: artifact["results"]["backend_plan_logs"]["summary"].update(
            {"dirty_product_gpu_resident_pack_ms": None}
        ),
    )

    with pytest.raises(
        ledger_tool.LedgerError,
        match="dirty_product_gpu_resident_pack_ms.*finite number",
    ):
        validate(fixture)


def test_accepts_zero_direct_resident_pack_and_upload_timings(
    tmp_path: pathlib.Path,
) -> None:
    fixture = valid_fixture(tmp_path)
    rewrite_artifact(
        fixture,
        "metal",
        lambda artifact: artifact["results"]["backend_plan_logs"]["summary"].update(
            {
                "dirty_product_gpu_resident_pack_ms": 0.0,
                "dirty_product_gpu_resident_transfer_to_device_ms": 0.0,
            }
        ),
    )
    gpu = fixture["ledger"]["runs"][0]["gpu"]
    gpu["f32_cast_pack_s"] = 0.0
    gpu["host_to_device_staging_s"] = 0.0

    validate(fixture)


def test_rejects_fabricated_detailed_metal_timing(tmp_path: pathlib.Path) -> None:
    fixture = valid_fixture(tmp_path)
    fixture["ledger"]["runs"][0]["gpu"]["device_to_host_final_export_s"] = 1.0

    with pytest.raises(ledger_tool.LedgerError, match="device_to_host_final_export_s"):
        validate(fixture)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dirty_product_fft_selected_backend", "rustfft"),
        ("dirty_product_fft_fallback_used", True),
        ("dirty_product_gpu_resident_fallback_used", True),
    ],
)
def test_rejects_fabricated_metal_selection_or_fallback(
    tmp_path: pathlib.Path, field: str, value: Any
) -> None:
    fixture = valid_fixture(tmp_path)
    rewrite_artifact(
        fixture,
        "metal",
        lambda artifact: artifact["results"]["backend_plan_logs"]["summary"].update(
            {field: value}
        ),
    )

    with pytest.raises(ledger_tool.LedgerError, match=field):
        validate(fixture)


def test_rejects_fabricated_casa_or_rust_median(tmp_path: pathlib.Path) -> None:
    fixture = valid_fixture(tmp_path)
    rewrite_artifact(
        fixture,
        "oracle",
        lambda artifact: artifact["results"]["casa"]["timings_seconds"].update(
            {"median": 21.0, "runs": [21.0]}
        ),
    )

    with pytest.raises(ledger_tool.LedgerError, match="baseline_total_wall_s.*CASA"):
        validate(fixture)


def test_rejects_casa_status_not_equal_to_full_comparison(
    tmp_path: pathlib.Path,
) -> None:
    fixture = valid_fixture(tmp_path)
    fixture["ledger"]["runs"][1]["casa"]["comparison_status"] = "good"

    with pytest.raises(ledger_tool.LedgerError, match="comparison_status.*investigate"):
        validate(fixture)


def test_rejects_bad_casa_comparison_even_with_adjudication(
    tmp_path: pathlib.Path,
) -> None:
    fixture = valid_fixture(tmp_path)

    def mark_bad(artifact: dict[str, Any]) -> None:
        artifact["structured_difference_review"]["label"] = "bad"
        artifact["structured_difference_review"]["products"][".image"] = "bad"

    rewrite_artifact(fixture, "oracle-comparison", mark_bad)
    oracle_run = fixture["ledger"]["runs"][1]
    oracle_run["correctness_status"] = "Full comparison is overall bad."
    oracle_run["casa"]["comparison_status"] = "bad"

    with pytest.raises(
        ledger_tool.LedgerError, match="CASA comparison status 'bad' is not admissible"
    ):
        validate(fixture)


def test_rejects_investigate_casa_comparison_without_adjudication(
    tmp_path: pathlib.Path,
) -> None:
    fixture = valid_fixture(tmp_path)
    del fixture["ledger"]["runs"][1]["casa"]["adjudication"]

    with pytest.raises(
        ledger_tool.LedgerError, match="investigate.*requires casa.adjudication"
    ):
        validate(fixture)


def test_rejects_investigate_casa_adjudication_without_rationale(
    tmp_path: pathlib.Path,
) -> None:
    fixture = valid_fixture(tmp_path)
    del fixture["ledger"]["runs"][1]["casa"]["adjudication"]["rationale"]

    with pytest.raises(ledger_tool.LedgerError, match=r"adjudication\.rationale"):
        validate(fixture)


def test_rejects_investigate_casa_adjudication_without_bounds(
    tmp_path: pathlib.Path,
) -> None:
    fixture = valid_fixture(tmp_path)
    del fixture["ledger"]["runs"][1]["casa"]["adjudication"]["bounds"]

    with pytest.raises(ledger_tool.LedgerError, match=r"adjudication\.bounds"):
        validate(fixture)


def test_rejects_tampered_casa_adjudication_product_list(
    tmp_path: pathlib.Path,
) -> None:
    fixture = valid_fixture(tmp_path)
    fixture["ledger"]["runs"][1]["casa"]["adjudication"]["products"] = [
        ".residual"
    ]

    with pytest.raises(
        ledger_tool.LedgerError, match="products must exactly match investigate products"
    ):
        validate(fixture)


def test_rejects_tampered_casa_adjudication_bound(
    tmp_path: pathlib.Path,
) -> None:
    fixture = valid_fixture(tmp_path)
    fixture["ledger"]["runs"][1]["casa"]["adjudication"]["bounds"][
        "max_normalized_rms"
    ] = 0.019

    with pytest.raises(
        ledger_tool.LedgerError, match=r"bounds\.max_normalized_rms.*records 0.02"
    ):
        validate(fixture)


def test_recomputes_casa_adjudication_against_tampered_artifact(
    tmp_path: pathlib.Path,
) -> None:
    fixture = valid_fixture(tmp_path)

    def increase_rms(artifact: dict[str, Any]) -> None:
        product = artifact["products"][".image"]
        product["diff_rms_over_casa_rms"] = 0.03
        product["structured_difference"]["normalized_diff_rms"] = 0.03

    rewrite_artifact(fixture, "oracle-comparison", increase_rms)
    oracle_run = fixture["ledger"]["runs"][1]
    oracle_run["rms"] = 0.03
    oracle_run["casa"]["rms"] = 0.03

    with pytest.raises(
        ledger_tool.LedgerError, match=r"bounds\.max_normalized_rms.*records 0.03"
    ):
        validate(fixture)


@pytest.mark.parametrize(("field", "value"), [("max_abs", 0.1), ("rms", 0.01)])
def test_rejects_casa_metrics_not_derived_from_full_comparison(
    tmp_path: pathlib.Path, field: str, value: float
) -> None:
    fixture = valid_fixture(tmp_path)
    fixture["ledger"]["runs"][1]["casa"][field] = value

    with pytest.raises(
        ledger_tool.LedgerError, match=rf"casa\.{field}.*full comparison"
    ):
        validate(fixture)


def test_rejects_tampered_checked_in_artifact(tmp_path: pathlib.Path) -> None:
    fixture = valid_fixture(tmp_path)
    artifact_path = fixture["artifact_paths"]["metal"]
    artifact_path.write_bytes(artifact_path.read_bytes() + b"\n")

    with pytest.raises(ledger_tool.LedgerError, match="sha256 mismatch"):
        validate(fixture)


def test_rejects_fabricated_role_even_when_declared(tmp_path: pathlib.Path) -> None:
    fixture = valid_fixture(tmp_path)
    group = fixture["ledger"]["workload_groups"][0]
    group["required_roles"].append("fabricated_speedup_evidence")
    fixture["ledger"]["runs"][0]["evidence_roles"].append(
        {"group_id": "complete", "role": "fabricated_speedup_evidence"}
    )

    with pytest.raises(ledger_tool.LedgerError, match="unsupported evidence role"):
        validate(fixture)


def test_counterbalanced_evidence_uses_the_median_delta_block() -> None:
    evidence = ledger_tool.derive_counterbalanced_evidence(
        counterbalanced_artifact(), "counterbalanced"
    )

    assert evidence == {
        "baseline_workload_id": "wave352-cpu",
        "candidate_workload_id": "wave352-metal",
        "baseline_seconds": 20.0,
        "candidate_seconds": 15.0,
    }


def test_counterbalanced_evidence_rejects_a_slowdown_verdict() -> None:
    with pytest.raises(ledger_tool.LedgerError, match="records a counterbalanced slowdown"):
        ledger_tool.derive_counterbalanced_evidence(
            counterbalanced_artifact(no_slowdown=False), "counterbalanced"
        )


def test_counterbalanced_evidence_rejects_tampered_raw_run() -> None:
    artifact = counterbalanced_artifact()
    counterbalanced_details(artifact)["runs"][1]["total_wall_seconds"] = 19.0

    with pytest.raises(ledger_tool.LedgerError, match="raw scheduled runs"):
        ledger_tool.derive_counterbalanced_evidence(artifact, "counterbalanced")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("baseline_seconds", 10.0),
        ("candidate_seconds", 10.0),
        ("delta_seconds", -10.0),
        ("relative_delta", -0.5),
    ],
)
def test_counterbalanced_evidence_rejects_tampered_paired_delta(
    field: str, value: float
) -> None:
    artifact = counterbalanced_artifact()
    counterbalanced_details(artifact)["paired_deltas"][0][field] = value

    with pytest.raises(ledger_tool.LedgerError, match="raw scheduled runs"):
        ledger_tool.derive_counterbalanced_evidence(artifact, "counterbalanced")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "fail"),
        ("no_slowdown", False),
        ("observed_median_relative_delta", -0.5),
    ],
)
def test_counterbalanced_evidence_rejects_tampered_verdict(
    field: str, value: Any
) -> None:
    artifact = counterbalanced_artifact()
    counterbalanced_details(artifact)["verdict"][field] = value

    with pytest.raises(ledger_tool.LedgerError, match="raw scheduled runs"):
        ledger_tool.derive_counterbalanced_evidence(artifact, "counterbalanced")


def test_counterbalanced_evidence_rejects_consistent_tampered_summaries() -> None:
    artifact = counterbalanced_artifact(no_slowdown=False)
    details = counterbalanced_details(artifact)
    details["paired_deltas"][0].update(
        {
            "candidate_seconds": 15.0,
            "delta_seconds": -5.0,
            "relative_delta": -0.25,
        }
    )
    details["verdict"].update(
        {
            "status": "pass",
            "no_slowdown": True,
            "observed_median_relative_delta": -0.25,
        }
    )

    with pytest.raises(ledger_tool.LedgerError, match="raw scheduled runs"):
        ledger_tool.derive_counterbalanced_evidence(artifact, "counterbalanced")


def test_counterbalanced_evidence_rejects_role_workload_mismatch() -> None:
    artifact = counterbalanced_artifact()
    details = counterbalanced_details(artifact)
    details["schedule"][0]["workload"] = (
        "workloads/wave352-metal.json"
    )
    details["runs"][0]["workload"] = "workloads/wave352-metal.json"

    with pytest.raises(
        ledger_tool.LedgerError, match="does not match configured baseline workload"
    ):
        ledger_tool.derive_counterbalanced_evidence(artifact, "counterbalanced")
