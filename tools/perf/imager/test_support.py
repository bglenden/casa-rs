# SPDX-License-Identifier: LGPL-3.0-or-later
"""Canonical synthetic records shared by imaging-harness tests."""

from __future__ import annotations

from typing import Any

from perf_harness import RUN_RESULT_SCHEMA_VERSION


def canonical_test_environment() -> dict[str, Any]:
    """Return deterministic provenance that satisfies the durable v3 contract."""

    return {
        "schema_version": 1,
        "repository": {
            "root": "/repo",
            "revision": "1" * 40,
            "branch": "codex/test",
            "dirty": False,
        },
        "runtime": {
            "python": "3.13.5",
            "platform": "test",
            "machine": "arm64",
            "logical_cores": 10,
            "physical_cores": 10,
            "physical_memory_bytes": 34_359_738_368,
        },
        "executables": {},
        "datasets": {},
        "storage_label": "test",
    }


def canonical_benchmark_features() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "visibility": {},
        "image": {},
        "mode_cost": {},
        "resources": {},
        "backend": {},
    }


def canonical_workload_result(
    *,
    status: str = "completed",
    extra_results: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a minimal but fully typed workload receipt for unit tests."""

    reason = "test result"
    results: dict[str, Any] = {
        "rust": {
            "status": "not_run",
            "reason": reason,
            "timings_seconds": {"runs": [], "median": None},
        },
        "casa": {
            "status": "not_run",
            "reason": reason,
            "timings_seconds": {"runs": [], "median": None},
        },
        "stage_medians_ms": {"rust": {}, "casa": {}},
        "stage_breakdown": {
            "schema_version": 1,
            "units": "milliseconds",
            "instrumentation_scope": "test",
            "rust": {"status": "skipped", "categories": {}},
            "casa": {"status": "skipped", "categories": {}},
        },
        "product_paths": {},
        "product_comparison": {
            "status": "skipped",
            "reason": reason,
            "products": {},
        },
    }
    if extra_results:
        results.update(extra_results)
    result: dict[str, Any] = {
        "schema_version": RUN_RESULT_SCHEMA_VERSION,
        "kind": "workload_run",
        "status": status,
        "run_id": "test-run",
        "created_at": "2026-07-18T00:00:00Z",
        "manifest_path": "/repo/workload.json",
        "workload": {"id": "test", "mode_id": "test", "description": ""},
        "dataset": {"key": "fixture", "path": "/tmp/fixture.ms"},
        "mode": {},
        "run": {},
        "comparison": {},
        "review": {},
        "run_support": {
            "status": "runnable",
            "reason": None,
            "bench_script": "/repo/bench.sh",
        },
        "environment": canonical_test_environment(),
        "command": {
            "kind": "legacy_benchmark_script",
            "argv": ["/repo/bench.sh"],
            "env": {},
        },
        "artifacts": {"checked_in_path": "evidence/test.json"},
        "products": {"root": "/tmp/products"},
        "logs": {"benchmark_log": "/tmp/benchmark.log"},
        "exit_code": 0,
        "results": results,
        "benchmark_features": canonical_benchmark_features(),
        "human_review": {},
    }
    if status != "dry_run":
        result["started_at"] = "2026-07-18T00:00:01Z"
        result["completed_at"] = "2026-07-18T00:00:02Z"
    return result
