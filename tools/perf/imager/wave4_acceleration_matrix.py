#!/usr/bin/env python3
"""Validate Wave 4 acceleration coverage and build closeout speedup tables."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any


MATRIX_PATH = pathlib.Path(__file__).resolve().parent / "wave4_acceleration_matrix.json"
REQUIRED_ROW_IDS = {
    "standard_cube_dirty",
    "standard_cube_clean_hogbom",
    "standard_cube_clean_clark",
    "standard_cube_clean_multiscale",
    "cubedata_dirty",
    "cubedata_clean_hogbom",
    "cubedata_clean_clark",
    "cubedata_clean_multiscale",
    "mosaic_cube_dirty",
    "mosaic_cube_clean_hogbom",
    "mosaic_cube_clean_clark",
    "mosaic_cube_clean_multiscale",
}
REQUIRED_EVIDENCE_ROLES = {
    "serial_cpu",
    "multi_worker_cpu",
    "metal_default",
    "casa_cpp",
    "single_plane_stream_baseline",
    "large_baseline",
}
NON_SPATIAL_PRODUCTS = {".sumwt"}
ALLOWED_TARGET_STATUSES = {
    "met",
    "missed-accepted-by-Brian",
    "not-applicable-accepted-by-Brian",
    "blocked",
}


class MatrixError(Exception):
    """Validation error shown without a traceback."""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=pathlib.Path, default=MATRIX_PATH)
    parser.add_argument(
        "--result",
        action="append",
        type=pathlib.Path,
        default=[],
        help="run_workload.py result JSON to include in the closeout table",
    )
    parser.add_argument(
        "--evidence-list",
        action="append",
        type=pathlib.Path,
        default=[],
        help="JSON file listing run_workload.py result paths and optional row/role overrides",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json", "markdown"),
        default="text",
        help="output format",
    )
    args = parser.parse_args()

    try:
        matrix = load_matrix(args.matrix)
        rows = enumerate_rows(matrix)
        results = [load_result(path) for path in args.result]
        for evidence_list in args.evidence_list:
            results.extend(load_evidence_list(evidence_list))
        if results:
            table = build_closeout_table(matrix, results)
            output: Any = {"status": "ok", "rows": table}
        else:
            output = {"status": "ok", "rows": rows}
        if args.format == "json":
            json.dump(output, sys.stdout, indent=2, sort_keys=True)
            sys.stdout.write("\n")
        elif args.format == "markdown" and results:
            sys.stdout.write(render_markdown_table(output["rows"]))
        else:
            for row in output["rows"]:
                print(render_text_row(row))
    except MatrixError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None


def load_matrix(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise MatrixError(f"parse {path}: {error}") from error
    if not isinstance(value, dict):
        raise MatrixError(f"{path} must contain a JSON object")
    validate_matrix(value)
    return value


def load_result(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise MatrixError(f"parse result {path}: {error}") from error
    if not isinstance(value, dict):
        raise MatrixError(f"{path} must contain a JSON object")
    value.setdefault("_source_path", str(path))
    return value


def load_evidence_list(path: pathlib.Path) -> list[dict[str, Any]]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise MatrixError(f"parse evidence list {path}: {error}") from error
    if not isinstance(value, dict):
        raise MatrixError(f"{path} must contain a JSON object")
    if int_field(value, "schema_version") != 1:
        raise MatrixError(f"{path}: schema_version must be 1")
    entries = list_field(value, "results")
    loaded: list[dict[str, Any]] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise MatrixError(f"{path}: results[{index}] must be an object")
        result_path = pathlib.Path(string_field(entry, "path"))
        result = load_result(result_path)
        row_id = entry.get("row_id")
        if row_id is not None:
            if not isinstance(row_id, str) or not row_id:
                raise MatrixError(f"{path}: results[{index}].row_id must be a string")
            result.setdefault("wave4_acceleration", {})["matrix_row_id"] = row_id
        role = entry.get("role")
        if role is not None:
            if not isinstance(role, str) or not role:
                raise MatrixError(f"{path}: results[{index}].role must be a string")
            result.setdefault("review", {})["evidence_role"] = role
        loaded.append(result)
    return loaded


def validate_matrix(matrix: dict[str, Any]) -> None:
    if matrix.get("schema_version") != 1:
        raise MatrixError("schema_version must be 1")
    if int_field(matrix, "wave_issue") != 295:
        raise MatrixError("wave_issue must be 295")
    review = object_field(matrix, "review_contract")
    roles = set(string_list_field(review, "required_evidence_roles"))
    missing_roles = REQUIRED_EVIDENCE_ROLES - roles
    if missing_roles:
        raise MatrixError(
            "review_contract.required_evidence_roles missing "
            + ", ".join(sorted(missing_roles))
        )
    statuses = set(string_list_field(review, "allowed_target_statuses"))
    missing_statuses = ALLOWED_TARGET_STATUSES - statuses
    if missing_statuses:
        raise MatrixError(
            "review_contract.allowed_target_statuses missing "
            + ", ".join(sorted(missing_statuses))
        )
    targets = object_field(matrix, "performance_targets")
    for key in (
        "multi_worker_speedup_vs_serial",
        "metal_speedup_vs_multi_worker_cpu",
        "default_speedup_vs_casa_for_medium_deconvolution",
        "dirty_backend_reduction_fraction",
        "mosaic_total_speedup_vs_single_plane_stream",
    ):
        positive_number_field(targets, key)
    columns = string_list_field(matrix, "closeout_table_columns")
    for required in (
        "mode_family",
        "phase",
        "deconvolver",
        "speedup_auto_vs_serial",
        "speedup_metal_vs_multi_worker_cpu",
        "speedup_default_vs_casa",
        "target_status",
        "evidence_link",
    ):
        if required not in columns:
            raise MatrixError(f"closeout_table_columns missing {required}")
    rows = list_field(matrix, "rows")
    row_ids = {string_field(row, "row_id") for row in rows if isinstance(row, dict)}
    if row_ids != REQUIRED_ROW_IDS:
        missing = REQUIRED_ROW_IDS - row_ids
        extra = row_ids - REQUIRED_ROW_IDS
        parts = []
        if missing:
            parts.append("missing " + ", ".join(sorted(missing)))
        if extra:
            parts.append("extra " + ", ".join(sorted(extra)))
        raise MatrixError("rows mismatch: " + "; ".join(parts))
    for row in rows:
        if not isinstance(row, dict):
            raise MatrixError("rows entries must be objects")
        validate_row(row)


def validate_row(row: dict[str, Any]) -> None:
    row_id = string_field(row, "row_id")
    int_field(row, "owner_issue")
    for key in (
        "mode_family",
        "phase",
        "deconvolver",
        "specmode",
        "gridder",
        "multi_worker_requirement",
        "metal_requirement",
    ):
        string_field(row, key)
    if row["phase"] not in {"dirty", "clean"}:
        raise MatrixError(f"{row_id}: phase must be dirty or clean")
    products = string_list_field(row, "products")
    if not products or any(not product.startswith(".") for product in products):
        raise MatrixError(f"{row_id}: products must be CASA suffixes")
    speedups = string_list_field(row, "required_speedups")
    if not speedups:
        raise MatrixError(f"{row_id}: required_speedups must not be empty")


def enumerate_rows(matrix: dict[str, Any]) -> list[dict[str, Any]]:
    return [public_matrix_row(row) for row in list_field(matrix, "rows")]


def public_matrix_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "row_id": string_field(row, "row_id"),
        "owner_issue": int_field(row, "owner_issue"),
        "mode_family": string_field(row, "mode_family"),
        "phase": string_field(row, "phase"),
        "deconvolver": string_field(row, "deconvolver"),
        "specmode": string_field(row, "specmode"),
        "gridder": string_field(row, "gridder"),
        "products": list(row["products"]),
        "required_speedups": list(row["required_speedups"]),
    }


def build_closeout_table(
    matrix: dict[str, Any], results: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    evidence = index_evidence(results)
    return [
        build_closeout_row(row, evidence.get(string_field(row, "row_id"), {}), matrix)
        for row in list_field(matrix, "rows")
    ]


def index_evidence(results: list[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    indexed: dict[str, dict[str, dict[str, Any]]] = {}
    for result in results:
        row_id = explicit_row_id(result) or infer_row_id(result)
        if row_id is None:
            continue
        role = evidence_role(result)
        add_evidence_result(indexed.setdefault(row_id, {}), role, result)
    return indexed


def add_evidence_result(
    evidence: dict[str, dict[str, Any]], role: str, result: dict[str, Any]
) -> None:
    existing = evidence.get(role)
    if existing is None:
        evidence[role] = result
        return
    if performance_preference_score(result) >= performance_preference_score(existing):
        evidence[extra_evidence_role(role, evidence)] = existing
        evidence[role] = result
    else:
        evidence[extra_evidence_role(role, evidence)] = result


def extra_evidence_role(role: str, evidence: dict[str, dict[str, Any]]) -> str:
    index = 1
    while f"{role}:extra:{index}" in evidence:
        index += 1
    return f"{role}:extra:{index}"


def performance_preference_score(result: dict[str, Any]) -> tuple[int, str]:
    tier_score = {"small": 1, "medium": 2, "large": 3}.get(dataset_tier(result), 0)
    return (tier_score, evidence_recency_key(result))


def evidence_recency_key(result: dict[str, Any]) -> str:
    for key in ("completed_at", "started_at", "created_at", "run_id"):
        value = result.get(key)
        if isinstance(value, str) and value:
            return value
    source_path = result.get("_source_path")
    if isinstance(source_path, str) and source_path:
        return pathlib.Path(source_path).name
    return ""


def build_closeout_row(
    matrix_row: dict[str, Any],
    evidence: dict[str, dict[str, Any]],
    matrix: dict[str, Any],
) -> dict[str, Any]:
    serial = evidence.get("serial_cpu") or evidence.get("single_plane_stream_baseline")
    multi = evidence.get("multi_worker_cpu")
    metal = evidence.get("metal_default")
    default = metal or evidence.get("auto_default") or multi
    casa = first_result_with_casa(evidence)
    baseline = evidence.get("large_baseline") or evidence.get("single_plane_stream_baseline")
    default_seconds = rust_seconds(default)
    performance_result = default or multi or serial or baseline
    serial_seconds = comparable_rust_seconds(serial, performance_result)
    multi_seconds = comparable_rust_seconds(multi, performance_result)
    metal_seconds = comparable_rust_seconds(metal, performance_result)
    casa = first_comparable_result_with_casa(evidence, performance_result)
    casa_seconds = casa_seconds_from_result(casa)
    baseline_seconds = rust_seconds(baseline)
    correctness_result = best_correctness_result(evidence, performance_result)
    correctness = correctness_status(correctness_result)
    speedup_auto_vs_serial = speedup_between_for_reference(serial, multi, performance_result)
    speedup_metal_vs_multi = speedup_between_for_reference(multi, metal, performance_result)
    speedup_default_vs_casa = speedup(casa_seconds, default_seconds)
    speedup_default_vs_baseline = speedup_between(baseline, default)
    row = {
        "mode_family": matrix_row["mode_family"],
        "phase": matrix_row["phase"],
        "deconvolver": matrix_row["deconvolver"],
        "dataset_tier": dataset_tier(performance_result),
        "shape": image_shape(performance_result),
        "niter": clean_iteration_count(performance_result),
        "serial_single_worker_s": serial_seconds,
        "multi_worker_cpu_auto_s": multi_seconds,
        "metal_default_s": metal_seconds,
        "casa_s": casa_seconds,
        "speedup_auto_vs_serial": speedup_auto_vs_serial,
        "speedup_metal_vs_multi_worker_cpu": speedup_metal_vs_multi,
        "speedup_default_vs_casa": speedup_default_vs_casa,
        "speedup_default_vs_large_or_single_plane_baseline": speedup_default_vs_baseline,
        "worker_count": worker_count(performance_result),
        "selected_backend": selected_backend(performance_result),
        "metal_status": metal_status(default or metal or multi or serial),
        "correctness_status": correctness,
        "target_status": target_status(
            matrix_row,
            matrix,
            speedup_auto_vs_serial,
            speedup_metal_vs_multi,
            speedup_default_vs_casa,
            speedup_default_vs_baseline,
            correctness,
            evidence,
        ),
        "evidence_link": evidence_link(performance_result),
        "performance_evidence_link": evidence_link(performance_result),
        "correctness_evidence_link": evidence_link(correctness_result),
        "row_id": matrix_row["row_id"],
        "owner_issue": matrix_row["owner_issue"],
    }
    return row


def target_status(
    matrix_row: dict[str, Any],
    matrix: dict[str, Any],
    auto_vs_serial: float | None,
    metal_vs_multi: float | None,
    default_vs_casa: float | None,
    default_vs_baseline: float | None,
    correctness: str | None,
    evidence: dict[str, dict[str, Any]],
) -> str:
    if not evidence:
        return "blocked"
    if correctness not in {"good", "completed"} and not brian_accepts_current_status(
        matrix_row, correctness
    ):
        return "blocked"
    targets = matrix["performance_targets"]
    required = set(matrix_row["required_speedups"])
    checks: list[bool] = []
    if "auto_vs_serial" in required:
        checks.append(
            auto_vs_serial is not None
            and auto_vs_serial >= targets["multi_worker_speedup_vs_serial"]
        )
    if "auto_vs_single_plane_stream" in required:
        checks.append(
            default_vs_baseline is not None
            and default_vs_baseline >= targets["mosaic_total_speedup_vs_single_plane_stream"]
        )
    if "metal_vs_multi_worker_cpu" in required:
        checks.append(
            metal_vs_multi is not None
            and metal_vs_multi >= targets["metal_speedup_vs_multi_worker_cpu"]
        )
    if "default_vs_casa" in required:
        checks.append(
            default_vs_casa is not None
            and default_vs_casa
            >= targets["default_speedup_vs_casa_for_medium_deconvolution"]
        )
    if "default_vs_casa_or_large_baseline" in required:
        checks.append(
            (default_vs_casa is not None and default_vs_casa >= 1.0)
            or (default_vs_baseline is not None and default_vs_baseline >= 1.0)
        )
    if checks and all(checks):
        return "met"
    if brian_accepts_current_status(matrix_row, correctness):
        return "missed-accepted-by-Brian"
    return "blocked"


def brian_accepts_current_status(matrix_row: dict[str, Any], correctness: str | None) -> bool:
    acceptance = matrix_row.get("brian_acceptance")
    if not isinstance(acceptance, dict):
        return False
    statuses = acceptance.get("correctness_statuses")
    if isinstance(statuses, list) and correctness not in statuses:
        return False
    return acceptance.get("target_status") == "missed-accepted-by-Brian"


def explicit_row_id(result: dict[str, Any]) -> str | None:
    value = result.get("wave4_acceleration")
    if isinstance(value, dict) and isinstance(value.get("matrix_row_id"), str):
        return value["matrix_row_id"]
    mode = result.get("mode")
    if isinstance(mode, dict) and isinstance(mode.get("wave4_matrix_row_id"), str):
        return mode["wave4_matrix_row_id"]
    return None


def infer_row_id(result: dict[str, Any]) -> str | None:
    mode = result.get("mode", {})
    if not isinstance(mode, dict):
        return None
    specmode = str(mode.get("specmode") or "").lower()
    gridder = str(mode.get("gridder") or "").lower()
    phase = "dirty" if int_or_none(mode.get("niter")) in (None, 0) else "clean"
    deconvolver = str(mode.get("deconvolver") or "hogbom").lower()
    if phase == "dirty":
        deconvolver = "none"
    if specmode == "cubedata":
        family = "cubedata"
    elif specmode == "cube" and gridder == "mosaic":
        family = "mosaic_cube"
    elif specmode == "cube" and gridder == "standard":
        family = "standard_cube"
    else:
        return None
    if phase == "dirty":
        return f"{family}_dirty"
    return f"{family}_clean_{deconvolver}"


def evidence_role(result: dict[str, Any]) -> str:
    review = result.get("review")
    if isinstance(review, dict) and isinstance(review.get("evidence_role"), str):
        role = normalize_role(review["evidence_role"])
        if role != "unspecified":
            return role
    run = result.get("run")
    if isinstance(run, dict) and isinstance(run.get("evidence_role"), str):
        role = normalize_role(run["evidence_role"])
        if role != "unspecified":
            return role
    backend = backend_features(result)
    mosaic_workers = int_or_none(backend.get("mosaic_cube_slab_worker_count"))
    if mosaic_workers is not None:
        if mosaic_metal_backend_selected(backend):
            return "metal_default"
        return "multi_worker_cpu" if mosaic_workers > 1 else "single_plane_stream_baseline"
    cube_workers = int_or_none(backend.get("cube_per_plane_workers"))
    if cube_workers is not None:
        selected = str(backend.get("cube_per_plane_backend") or "").lower()
        if "metal" in selected:
            return "metal_default"
        return "multi_worker_cpu" if cube_workers > 1 else "serial_cpu"
    mode = result.get("mode")
    if isinstance(mode, dict):
        acceleration = str(mode.get("standard_mfs_acceleration") or "").lower()
        if acceleration == "cpu":
            return "serial_cpu"
        if acceleration == "multi-cpu":
            return "multi_worker_cpu"
        if acceleration in {"metal", "auto"}:
            return "metal_default" if acceleration == "metal" else "auto_default"
    return "unspecified"


def normalize_role(role: str) -> str:
    aliases = {
        "before_baseline": "serial_cpu",
        "after_multi_worker_cpu": "multi_worker_cpu",
        "after_gpu_metal": "metal_default",
        "after_auto_default": "metal_default",
        "auto_default": "metal_default",
        "casa_cpp": "casa_cpp",
        "optimization_turnaround_serial": "single_plane_stream_baseline",
        "optimization_turnaround_auto": "metal_default",
        "optimization_turnaround_multicpu": "multi_worker_cpu",
    }
    return aliases.get(role, role)


def first_result_with_casa(evidence: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    for role in ("casa_cpp", "metal_default", "multi_worker_cpu", "serial_cpu"):
        result = evidence.get(role)
        if casa_seconds_from_result(result) is not None:
            return result
    return None


def first_comparable_result_with_casa(
    evidence: dict[str, dict[str, Any]], reference: dict[str, Any] | None
) -> dict[str, Any] | None:
    if reference is None:
        return first_result_with_casa(evidence)
    for role in ("casa_cpp", "metal_default", "multi_worker_cpu", "serial_cpu"):
        result = evidence.get(role)
        if casa_seconds_from_result(result) is not None and comparable_shape(result, reference):
            return result
    return None


def comparable_shape(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return (
        dataset_tier(left) == dataset_tier(right)
        and image_shape(left) == image_shape(right)
        and clean_iteration_count(left) == clean_iteration_count(right)
    )


def rust_seconds(result: dict[str, Any] | None) -> float | None:
    if not result:
        return None
    return nested_float(result, ["results", "rust", "timings_seconds", "median"])


def comparable_rust_seconds(
    result: dict[str, Any] | None, reference: dict[str, Any] | None
) -> float | None:
    if result is None:
        return None
    if reference is not None and not comparable_shape(result, reference):
        return None
    return rust_seconds(result)


def casa_seconds_from_result(result: dict[str, Any] | None) -> float | None:
    if not result:
        return None
    return nested_float(result, ["results", "casa", "timings_seconds", "median"])


def speedup(before: float | None, after: float | None) -> float | None:
    if before is None or after is None or after <= 0:
        return None
    return before / after


def speedup_between(
    before: dict[str, Any] | None, after: dict[str, Any] | None
) -> float | None:
    if before is None or after is None or not comparable_shape(before, after):
        return None
    return speedup(rust_seconds(before), rust_seconds(after))


def speedup_between_for_reference(
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    reference: dict[str, Any] | None,
) -> float | None:
    if (
        before is None
        or after is None
        or reference is None
        or not comparable_shape(before, reference)
        or not comparable_shape(after, reference)
    ):
        return None
    return speedup_between(before, after)


def dataset_tier(result: dict[str, Any] | None) -> str | None:
    if not result:
        return None
    dataset = result.get("dataset")
    if isinstance(dataset, dict):
        key = str(dataset.get("key") or "")
        for tier in ("small", "medium", "large"):
            if tier in key:
                return tier
    run = result.get("run")
    if isinstance(run, dict):
        label = str(run.get("storage_label") or "")
        for tier in ("small", "medium", "large"):
            if tier in label:
                return tier
    return None


def image_shape(result: dict[str, Any] | None) -> str | None:
    if not result:
        return None
    mode = result.get("mode")
    if not isinstance(mode, dict):
        return None
    image = mode.get("image_shape")
    if isinstance(image, list) and image:
        imsize = image[0]
    else:
        imsize = mode.get("imsize")
    channels = mode.get("channel_count")
    if imsize and channels:
        return f"{channels} ch, {imsize}"
    if imsize:
        return str(imsize)
    return None


def clean_iteration_count(result: dict[str, Any] | None) -> int | None:
    if not result:
        return None
    mode = result.get("mode")
    if not isinstance(mode, dict):
        return None
    return int_or_none(mode.get("niter"))


def worker_count(result: dict[str, Any] | None) -> int | None:
    backend = backend_features(result)
    for key in ("cube_per_plane_workers", "mosaic_cube_slab_worker_count"):
        value = int_or_none(backend.get(key))
        if value is not None:
            return value
    return int_or_none(backend.get("resolved_grid_threads"))


def selected_backend(result: dict[str, Any] | None) -> str | None:
    backend = backend_features(result)
    cube_backend = backend.get("cube_per_plane_backend")
    if (
        cube_backend == "serial_cpu"
        and backend.get("cube_per_plane_phase") == "dirty_control"
        and (worker_count(result) or 0) > 1
    ):
        return "cpu_multi_plane_workers"
    for key in (
        "mosaic_cube_slab_executor_capabilities",
        "cube_per_plane_backend",
        "resolved_backend",
    ):
        value = backend.get(key)
        if isinstance(value, str):
            return value
    return None


def metal_status(result: dict[str, Any] | None) -> str | None:
    backend = backend_features(result)
    selected = selected_backend(result)
    if selected and "metal" in selected:
        return "selected"
    if mosaic_metal_backend_selected(backend):
        return "selected"
    reasons = backend.get("cube_per_plane_fallback_reasons")
    if isinstance(reasons, str) and reasons not in {"none", ""}:
        return f"rejected:{reasons}"
    eligible = backend.get("cube_per_plane_metal_eligible")
    if eligible is True:
        return "eligible-not-selected"
    if backend.get("metal_device") is False:
        return "rejected:metal_device_unavailable"
    return None


def mosaic_metal_backend_selected(backend: dict[str, Any]) -> bool:
    for key in ("resolved_initial_dirty_backend", "resolved_residual_backend"):
        value = backend.get(key)
        if isinstance(value, str) and "metal" in value:
            return True
    return False


def correctness_status(result: dict[str, Any] | None) -> str | None:
    if not result:
        return None
    review = nested_value(
        result, ["results", "product_comparison", "structured_difference_review", "label"]
    )
    if isinstance(review, str):
        if review == "investigate" and stale_non_spatial_only_review(result):
            return "good"
        return review
    status = nested_value(result, ["results", "product_comparison", "status"])
    return status if isinstance(status, str) else None


def stale_non_spatial_only_review(result: dict[str, Any]) -> bool:
    comparison = nested_value(result, ["results", "product_comparison"])
    if not isinstance(comparison, dict):
        return False
    review = comparison.get("structured_difference_review")
    if not isinstance(review, dict):
        return False
    reviewed_products = review.get("products")
    if not isinstance(reviewed_products, dict):
        return False
    flagged = {
        suffix
        for suffix, label in reviewed_products.items()
        if isinstance(suffix, str) and label not in {"good", "unknown", None}
    }
    if not flagged or not flagged <= NON_SPATIAL_PRODUCTS:
        return False
    product_details = comparison.get("products")
    if not isinstance(product_details, dict):
        return False
    return all(non_spatial_amplitude_is_good(product_details.get(suffix)) for suffix in flagged)


def non_spatial_amplitude_is_good(product: Any) -> bool:
    if not isinstance(product, dict):
        return False
    structured = product.get("structured_difference")
    if not isinstance(structured, dict):
        return False
    classification = structured.get("classification")
    if isinstance(classification, dict) and classification.get("amplitude") == "good":
        return True
    review = structured.get("review")
    checks = review.get("checks") if isinstance(review, dict) else None
    if isinstance(checks, list):
        for check in checks:
            if (
                isinstance(check, dict)
                and check.get("name") == "normalized_diff_rms"
                and check.get("label") == "good"
            ):
                return True
    normalized = structured.get("normalized_diff_rms")
    return isinstance(normalized, int | float) and normalized < 1e-4


def best_correctness_result(
    evidence: dict[str, dict[str, Any]], reference: dict[str, Any] | None = None
) -> dict[str, Any] | None:
    ranked: list[tuple[int, dict[str, Any]]] = []
    for result in evidence.values():
        if reference is not None and not comparable_correctness_evidence(result, reference):
            continue
        status = correctness_status(result)
        rank = correctness_rank(status)
        if rank is not None:
            ranked.append((rank, result))
    if not ranked:
        return None
    best_rank = min(rank for rank, _ in ranked)
    tied = [result for rank, result in ranked if rank == best_rank]
    tied.sort(key=performance_preference_score, reverse=True)
    return tied[0]


def comparable_correctness_evidence(result: dict[str, Any], reference: dict[str, Any]) -> bool:
    result_niter = clean_iteration_count(result)
    reference_niter = clean_iteration_count(reference)
    if result_niter != reference_niter:
        return False
    if reference_niter and reference_niter > 0:
        return image_shape(result) == image_shape(reference)
    return True


def correctness_rank(status: str | None) -> int | None:
    if status is None:
        return None
    return {
        "good": 0,
        "completed": 1,
        "investigate": 2,
        "unknown": 3,
        "bad": 4,
        "shape_mismatch": 5,
        "missing": 6,
        "not_run": 7,
    }.get(status, 3)


def evidence_link(result: dict[str, Any] | None) -> str | None:
    if not result:
        return None
    source = result.get("_source_path")
    if isinstance(source, str):
        return source
    logs = result.get("logs")
    if isinstance(logs, dict) and isinstance(logs.get("benchmark_log"), str):
        return logs["benchmark_log"]
    return None


def backend_features(result: dict[str, Any] | None) -> dict[str, Any]:
    if not result:
        return {}
    backend = nested_value(result, ["benchmark_features", "backend"])
    return backend if isinstance(backend, dict) else {}


def nested_float(result: dict[str, Any], path: list[str]) -> float | None:
    value = nested_value(result, path)
    if isinstance(value, int | float):
        return float(value)
    return None


def nested_value(result: dict[str, Any], path: list[str]) -> Any:
    value: Any = result
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def render_text_row(row: dict[str, Any]) -> str:
    return (
        f"row={row['row_id']} mode={row['mode_family']} phase={row['phase']} "
        f"deconvolver={row['deconvolver']} target_status={row.get('target_status', 'n/a')}"
    )


def render_markdown_table(rows: list[dict[str, Any]]) -> str:
    columns = [
        "mode_family",
        "phase",
        "deconvolver",
        "dataset_tier",
        "shape",
        "niter",
        "serial_single_worker_s",
        "multi_worker_cpu_auto_s",
        "metal_default_s",
        "casa_s",
        "speedup_auto_vs_serial",
        "speedup_metal_vs_multi_worker_cpu",
        "speedup_default_vs_casa",
        "worker_count",
        "selected_backend",
        "metal_status",
        "correctness_status",
        "target_status",
        "evidence_link",
        "performance_evidence_link",
        "correctness_evidence_link",
    ]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(format_cell(row.get(column)) for column in columns) + " |")
    return "\n".join(lines) + "\n"


def format_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value).replace("|", "\\|")


def object_field(obj: dict[str, Any], key: str) -> dict[str, Any]:
    value = obj.get(key)
    if not isinstance(value, dict):
        raise MatrixError(f"{key!r} must be an object")
    return value


def list_field(obj: dict[str, Any], key: str) -> list[Any]:
    value = obj.get(key)
    if not isinstance(value, list):
        raise MatrixError(f"{key!r} must be a list")
    return value


def string_list_field(obj: dict[str, Any], key: str) -> list[str]:
    value = list_field(obj, key)
    if not all(isinstance(item, str) and item for item in value):
        raise MatrixError(f"{key!r} must contain non-empty strings")
    return list(value)


def string_field(obj: dict[str, Any], key: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value:
        raise MatrixError(f"{key!r} must be a non-empty string")
    return value


def int_field(obj: dict[str, Any], key: str) -> int:
    value = obj.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise MatrixError(f"{key!r} must be an integer")
    return value


def positive_number_field(obj: dict[str, Any], key: str) -> float:
    value = obj.get(key)
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        raise MatrixError(f"{key!r} must be a positive number")
    return float(value)


if __name__ == "__main__":
    main()
