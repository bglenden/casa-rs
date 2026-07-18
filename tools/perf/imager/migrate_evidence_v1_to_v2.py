#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""One-time migration of the checked-in imaging evidence manifest to v2."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import pathlib
import re
from typing import Any

from perf_harness import (
    RUN_RESULT_SCHEMA_VERSION,
    atomic_write_json,
    load_json_object,
    validate_run_result,
)


DEFAULT_MANIFEST = (
    pathlib.Path(__file__).resolve().parent
    / "evidence"
    / "imaging_performance_evidence_manifest.json"
)
RUN_ROLES = {"baseline", "candidate", "casa_oracle"}
COMPARISON_ROLES = {
    "full_comparison",
    "primary_comparison",
    "product_comparison",
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=pathlib.Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args()
    migrate_manifest(args.manifest.resolve())


def migrate_manifest(manifest_path: pathlib.Path) -> None:
    manifest = load_json_object(manifest_path, description="evidence manifest")
    if manifest.get("schema_version") != 1:
        raise ValueError("evidence manifest schema_version must be 1")
    entries = manifest.get("artifacts")
    if not isinstance(entries, list) or not entries:
        raise ValueError("evidence manifest artifacts must be a non-empty list")
    imager_root = manifest_path.parent.parent
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("evidence manifest artifact must be an object")
        path = imager_root / required_string(entry, "checked_in_path")
        old = load_json_object(path, description="v1 evidence artifact")
        if old.get("schema_version") == RUN_RESULT_SCHEMA_VERSION:
            if old.get("kind") == "image_comparison":
                product = old.get("results", {}).get("product_comparison")
                if looks_like_workload_run(product):
                    old = repair_wrapped_workload_result(old, product)
                    atomic_write_json(path, old)
                    product = old.get("results", {}).get("product_comparison")
                if isinstance(product, dict) and "status" not in product:
                    product["status"] = "completed"
                    atomic_write_json(path, old)
            validate_run_result(old, source=str(path))
            entry["sha256"] = sha256(path)
            continue
        role = required_string(entry, "artifact_role")
        version = old.get("schema_version")
        if version != 1 and not (role in COMPARISON_ROLES and version is None):
            raise ValueError(
                f"{path}: expected v1 evidence or an unversioned comparison artifact"
            )
        migrated = migrate_artifact(entry, old, path)
        validate_run_result(migrated, source=str(path))
        atomic_write_json(path, migrated)
        entry["sha256"] = sha256(path)
    atomic_write_json(manifest_path, manifest)


def migrate_artifact(
    entry: dict[str, Any], old: dict[str, Any], path: pathlib.Path
) -> dict[str, Any]:
    role = required_string(entry, "artifact_role")
    workload_id = required_string(entry, "workload_id")
    artifact_id = required_string(entry, "artifact_id")
    if role in RUN_ROLES or looks_like_workload_run(old):
        result = dict(old)
        result["schema_version"] = RUN_RESULT_SCHEMA_VERSION
        result["kind"] = "workload_run"
        result["status"] = canonical_status(old.get("status"))
        result.setdefault("run_id", artifact_id)
        result.setdefault("created_at", timestamp_from_path(path))
        result.setdefault("workload", {"id": workload_id})
        result["environment"] = migrate_provenance(
            old.get("environment"), result.get("dataset"), result.get("run")
        )
        result.setdefault("artifacts", {})
        result.setdefault("results", {})
        attach_failure(result)
        return result
    if role in COMPARISON_ROLES:
        return evidence_envelope(
            kind="image_comparison",
            status=comparison_status(old),
            run_id=artifact_id,
            workload_id=workload_id,
            created_at=timestamp_from_path(path),
            artifacts={"checked_in_path": str(path)},
            results={"product_comparison": image_comparison_details(old)},
        )
    if role == "counterbalanced_comparison":
        verdict = old.get("verdict")
        verdict_status = verdict.get("status") if isinstance(verdict, dict) else None
        status = (
            "completed"
            if verdict_status == "pass"
            else "out_of_tolerance"
            if verdict_status == "fail"
            else "failed_comparison"
        )
        return evidence_envelope(
            kind="alternating_comparison",
            status=status,
            run_id=str(old.get("comparison_id") or artifact_id),
            workload_id=workload_id,
            created_at=str(old.get("created_at") or timestamp_from_path(path)),
            artifacts={
                "checked_in_path": str(path),
                "report_path": old.get("report_path"),
            },
            results={"alternating_comparison": without_envelope_fields(old)},
        )
    raise ValueError(f"{path}: unsupported artifact role {role}")


def evidence_envelope(
    *,
    kind: str,
    status: str,
    run_id: str,
    workload_id: str,
    created_at: str,
    artifacts: dict[str, Any],
    results: dict[str, Any],
) -> dict[str, Any]:
    value = {
        "schema_version": RUN_RESULT_SCHEMA_VERSION,
        "kind": kind,
        "status": status,
        "run_id": run_id,
        "created_at": created_at,
        "workload": {"id": workload_id},
        "environment": migrate_provenance(None, None, None),
        "artifacts": artifacts,
        "results": results,
    }
    attach_failure(value)
    return value


def without_envelope_fields(old: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in old.items()
        if key
        not in {
            "schema_version",
            "status",
            "run_id",
            "comparison_id",
            "created_at",
            "environment",
            "artifacts",
            "workload",
        }
    }


def image_comparison_details(old: dict[str, Any]) -> dict[str, Any]:
    details = without_envelope_fields(old)
    details["status"] = old.get("status", "completed")
    return details


def looks_like_workload_run(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and isinstance(value.get("results"), dict)
        and bool({"rust", "casa"} & set(value["results"]))
    )


def repair_wrapped_workload_result(
    envelope: dict[str, Any], nested: dict[str, Any]
) -> dict[str, Any]:
    repaired = dict(nested)
    repaired["schema_version"] = RUN_RESULT_SCHEMA_VERSION
    repaired["kind"] = "workload_run"
    repaired["status"] = canonical_status(nested.get("status"))
    repaired["run_id"] = envelope["run_id"]
    repaired["created_at"] = envelope["created_at"]
    repaired["workload"] = envelope["workload"]
    repaired["environment"] = envelope["environment"]
    repaired["artifacts"] = envelope["artifacts"]
    attach_failure(repaired)
    return repaired


def migrate_provenance(
    old: Any, dataset: Any, run: Any
) -> dict[str, Any]:
    legacy = old if isinstance(old, dict) else {}
    dataset_record = dataset if isinstance(dataset, dict) else {}
    run_record = run if isinstance(run, dict) else {}
    return {
        "schema_version": 1,
        "repository": {
            "root": legacy.get("repo_root"),
            "revision": legacy.get("git_commit"),
            "branch": legacy.get("git_branch"),
            "dirty": None,
        },
        "runtime": {
            "python": legacy.get("python"),
            "platform": None,
            "machine": None,
            "logical_cores": legacy.get("logical_cores"),
            "physical_cores": legacy.get("physical_cores"),
            "physical_memory_bytes": legacy.get("physical_memory_bytes"),
        },
        "executables": {
            "bench_script": historical_path(
                legacy.get("bench_script"), legacy.get("bench_script_sha256")
            ),
            "casa_python": historical_path(legacy.get("casa_python"), None),
        },
        "datasets": {
            "measurement_set": historical_path(dataset_record.get("path"), None)
        },
        "storage_label": run_record.get("storage_label"),
        "migration": {
            "source_schema_version": 1,
            "method": "checked-in one-time v1-to-v2 migration",
        },
    }


def historical_path(path: Any, digest: Any) -> dict[str, Any] | None:
    if not isinstance(path, str) or not path:
        return None
    value = {"path": path, "exists_at_migration": pathlib.Path(path).exists()}
    if isinstance(digest, str) and digest:
        value["sha256"] = digest
    return value


def comparison_status(value: dict[str, Any]) -> str:
    status = value.get("status")
    if status in {"completed", "passed"}:
        return "completed"
    if status == "unavailable":
        return "unavailable"
    if status in {"failed_execution", "failed"}:
        return "failed_comparison"
    return "failed_comparison"


def canonical_status(value: Any) -> str:
    if value == "failed":
        return "failed_execution"
    if value in {
        "completed",
        "dry_run",
        "failed_execution",
        "failed_comparison",
        "out_of_tolerance",
        "unavailable",
    }:
        return str(value)
    return "failed_execution"


def attach_failure(value: dict[str, Any]) -> None:
    if value["status"] in {"completed", "dry_run"}:
        return
    results = value["results"]
    results.setdefault(
        "failure",
        {
            "kind": "historical_evidence",
            "reason": f"migrated historical evidence status is {value['status']}",
        },
    )


def timestamp_from_path(path: pathlib.Path) -> str:
    match = re.search(r"(20\d{6})T(\d{6})Z", path.name)
    if match:
        parsed = dt.datetime.strptime("".join(match.groups()), "%Y%m%d%H%M%S")
        return parsed.replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")
    return "1970-01-01T00:00:00Z"


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def required_string(value: dict[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} must be a non-empty string")
    return item


if __name__ == "__main__":
    main()
