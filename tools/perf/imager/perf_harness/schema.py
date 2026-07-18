# SPDX-License-Identifier: LGPL-3.0-or-later
"""Strict versioned workload and run-result contracts."""

from __future__ import annotations

import math
import pathlib
from typing import Any

from .artifacts import ArtifactError, load_json_object


WORKLOAD_SCHEMA_VERSION = 1
RUN_RESULT_SCHEMA_VERSION = 2

WORKLOAD_FIELDS = {
    "schema_version",
    "id",
    "mode_id",
    "description",
    "dataset",
    "imaging",
    "run",
    "comparison",
    "review",
}
DATASET_FIELDS = {"key", "path", "relative_path", "root_env"}
IMAGING_FIELDS = {
    "casa_gridder",
    "cell_arcsec",
    "chanchunks",
    "channel_count",
    "channel_start",
    "cyclefactor",
    "deconvolver",
    "field",
    "gain",
    "gridder",
    "hogbom_iteration_mode",
    "imaging_fft_backend",
    "imaging_fft_precision",
    "imaging_memory_target_mb",
    "imaging_prepare_buffer_mb",
    "imaging_prepare_workers",
    "imaging_read_ahead_blocks",
    "imaging_row_block_rows",
    "imsize",
    "interpolation",
    "max_psf_fraction",
    "min_psf_fraction",
    "minor_cycle_length",
    "mode",
    "niter",
    "nsigma",
    "nterms",
    "parallel",
    "pbcor",
    "pblimit",
    "perchanweightdensity",
    "phasecenter_field",
    "psfcutoff",
    "robust",
    "scales",
    "specmode",
    "spw",
    "standard_mfs_acceleration",
    "standard_mfs_grid_threads",
    "standard_mfs_metal_minor_cycle_chunk",
    "start",
    "threshold_jy",
    "weighting",
    "width",
    "wprojplanes",
    "write_pb",
    "wterm",
}
RUN_FIELDS = {
    "env",
    "evidence_role",
    "ms_staging",
    "phase_probe",
    "profile_repeats",
    "repeats",
    "reuse_casa_prefix",
    "reuse_rust_prefix",
    "run_label",
    "skip_casa",
    "skip_profile",
    "skip_rust",
    "storage_label",
    "stream_log",
}
COMPARISON_FIELDS = {"max_elements_per_product", "products"}
REVIEW_FIELDS = {
    "required_evidence_roles",
    "required_reviewer",
    "requires_human_acceptance_before_done",
}
RUN_RESULT_FIELDS = {
    "schema_version",
    "kind",
    "status",
    "run_id",
    "created_at",
    "started_at",
    "completed_at",
    "manifest_path",
    "workload",
    "dataset",
    "mode",
    "run",
    "comparison",
    "review",
    "run_support",
    "environment",
    "command",
    "artifacts",
    "products",
    "logs",
    "exit_code",
    "results",
    "benchmark_features",
    "human_review",
    "wave4_acceleration",
}
RUN_STATUSES = {
    "completed",
    "dry_run",
    "failed_execution",
    "failed_comparison",
    "out_of_tolerance",
    "unavailable",
}
FAILURE_STATUSES = RUN_STATUSES - {"completed", "dry_run"}


class ContractError(ValueError):
    """A workload or result violates the canonical evidence contract."""


def load_workload_manifest(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = load_json_object(path, description="workload manifest")
    except ArtifactError as error:
        raise ContractError(str(error)) from error
    validate_workload_manifest(value, source=str(path))
    return value


def validate_workload_manifest(value: dict[str, Any], *, source: str = "workload") -> None:
    _schema_version(value, WORKLOAD_SCHEMA_VERSION, source)
    unknown = sorted(set(value) - WORKLOAD_FIELDS)
    if unknown:
        raise ContractError(f"{source}: unknown workload field(s): {', '.join(unknown)}")
    _nonempty_string(value, "id", source)
    _nonempty_string(value, "mode_id", source)
    if "description" in value and not isinstance(value["description"], str):
        raise ContractError(f"{source}: description must be a string")
    dataset = _object(value, "dataset", source)
    _allowed_fields(dataset, DATASET_FIELDS, f"{source}: dataset")
    _nonempty_string(dataset, "key", f"{source}: dataset")
    if "path" not in dataset and "relative_path" not in dataset:
        raise ContractError(f"{source}: dataset requires path or relative_path")
    for key in ("path", "relative_path", "root_env"):
        if key in dataset:
            _nonempty_string(dataset, key, f"{source}: dataset")

    imaging = _object(value, "imaging", source)
    _allowed_fields(imaging, IMAGING_FIELDS, f"{source}: imaging")
    for key in ("specmode", "gridder", "mode"):
        _nonempty_string(imaging, key, f"{source}: imaging")
    _validate_imaging_types(imaging, source)

    run = value.get("run", {})
    if not isinstance(run, dict):
        raise ContractError(f"{source}: run must be an object")
    _allowed_fields(run, RUN_FIELDS, f"{source}: run")
    _validate_run_types(run, source)

    comparison = value.get("comparison", {})
    if not isinstance(comparison, dict):
        raise ContractError(f"{source}: comparison must be an object")
    _allowed_fields(comparison, COMPARISON_FIELDS, f"{source}: comparison")
    if "max_elements_per_product" in comparison:
        _integer(comparison, "max_elements_per_product", f"{source}: comparison")
    if "products" in comparison:
        _string_list(comparison, "products", f"{source}: comparison")

    review = value.get("review", {})
    if not isinstance(review, dict):
        raise ContractError(f"{source}: review must be an object")
    _allowed_fields(review, REVIEW_FIELDS, f"{source}: review")
    if "required_evidence_roles" in review:
        _string_list(review, "required_evidence_roles", f"{source}: review")
    if "required_reviewer" in review:
        _nonempty_string(review, "required_reviewer", f"{source}: review")
    if "requires_human_acceptance_before_done" in review and not isinstance(
        review["requires_human_acceptance_before_done"], bool
    ):
        raise ContractError(
            f"{source}: review.requires_human_acceptance_before_done must be a boolean"
        )


def load_run_result(
    path: pathlib.Path, *, source_key: str | None = None
) -> dict[str, Any]:
    try:
        value = load_json_object(path, description="run result")
    except ArtifactError as error:
        raise ContractError(str(error)) from error
    validate_run_result(value, source=str(path))
    if source_key is not None:
        value[source_key] = str(path)
    return value


def validate_run_result(value: dict[str, Any], *, source: str = "result") -> None:
    _schema_version(value, RUN_RESULT_SCHEMA_VERSION, source)
    unknown = sorted(set(value) - RUN_RESULT_FIELDS)
    if unknown:
        raise ContractError(f"{source}: unknown result field(s): {', '.join(unknown)}")
    status = value.get("status")
    if status not in RUN_STATUSES:
        raise ContractError(
            f"{source}: status must be one of {', '.join(sorted(RUN_STATUSES))}"
        )
    _nonempty_string(value, "kind", source)
    _nonempty_string(value, "run_id", source)
    _nonempty_string(value, "created_at", source)
    _object(value, "environment", source)
    _object(value, "artifacts", source)
    results = _object(value, "results", source)
    if status in FAILURE_STATUSES:
        failure = results.get("failure")
        if not isinstance(failure, dict):
            raise ContractError(f"{source}: {status} requires results.failure")
        _nonempty_string(failure, "kind", f"{source}: results.failure")
        _nonempty_string(failure, "reason", f"{source}: results.failure")
    for key in ("workload", "mode", "run"):
        if key in value:
            _object(value, key, source)


def finite_number(value: Any, *, field: str = "value", optional: bool = True) -> float | None:
    if value is None and optional:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractError(f"{field} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise ContractError(f"{field} must be finite")
    return number


def nested_value(value: dict[str, Any], *keys: str) -> Any:
    current: Any = value
    traversed: list[str] = []
    for key in keys:
        traversed.append(key)
        if not isinstance(current, dict):
            raise ContractError(f"/{'/'.join(traversed[:-1])} must be an object")
        if key not in current:
            return None
        current = current[key]
    return current


def nested_object(value: dict[str, Any], *keys: str) -> dict[str, Any]:
    current = nested_value(value, *keys)
    if current is None:
        return {}
    if not isinstance(current, dict):
        raise ContractError(f"/{'/'.join(keys)} must be an object")
    return current


def _schema_version(value: dict[str, Any], expected: int, source: str) -> None:
    actual = value.get("schema_version")
    if isinstance(actual, bool) or actual != expected:
        raise ContractError(f"{source}: schema_version must be {expected}")


def _nonempty_string(value: dict[str, Any], key: str, source: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ContractError(f"{source}: {key} must be a non-empty string")
    return item


def _object(value: dict[str, Any], key: str, source: str) -> dict[str, Any]:
    item = value.get(key)
    if not isinstance(item, dict):
        raise ContractError(f"{source}: {key} must be an object")
    return item


def _allowed_fields(value: dict[str, Any], allowed: set[str], source: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ContractError(f"{source}: unknown field(s): {', '.join(unknown)}")


def _validate_imaging_types(imaging: dict[str, Any], source: str) -> None:
    integers = {
        "chanchunks",
        "channel_count",
        "channel_start",
        "imaging_memory_target_mb",
        "imaging_prepare_buffer_mb",
        "imaging_prepare_workers",
        "imaging_read_ahead_blocks",
        "imaging_row_block_rows",
        "imsize",
        "minor_cycle_length",
        "niter",
        "nterms",
    }
    numbers = {
        "cell_arcsec",
        "cyclefactor",
        "gain",
        "max_psf_fraction",
        "min_psf_fraction",
        "nsigma",
        "pblimit",
        "psfcutoff",
        "robust",
        "threshold_jy",
    }
    booleans = {"parallel", "pbcor", "perchanweightdensity", "write_pb"}
    special = {
        "phasecenter_field",
        "scales",
        "standard_mfs_grid_threads",
        "standard_mfs_metal_minor_cycle_chunk",
        "wprojplanes",
    }
    for key in integers & set(imaging):
        _integer(imaging, key, f"{source}: imaging")
    for key in numbers & set(imaging):
        finite_number(imaging[key], field=f"{source}: imaging.{key}", optional=False)
    for key in booleans & set(imaging):
        if not isinstance(imaging[key], bool):
            raise ContractError(f"{source}: imaging.{key} must be a boolean")
    for key in set(imaging) - integers - numbers - booleans - special:
        _nonempty_string(imaging, key, f"{source}: imaging")
    if "phasecenter_field" in imaging and imaging["phasecenter_field"] is not None:
        _integer(imaging, "phasecenter_field", f"{source}: imaging")
    if "scales" in imaging:
        scales = imaging["scales"]
        if not isinstance(scales, str) and not (
            isinstance(scales, list)
            and all(isinstance(item, int) and not isinstance(item, bool) for item in scales)
        ):
            raise ContractError(f"{source}: imaging.scales must be a string or integer list")
    for key in (
        "standard_mfs_grid_threads",
        "standard_mfs_metal_minor_cycle_chunk",
        "wprojplanes",
    ):
        if key in imaging and not (
            isinstance(imaging[key], str)
            or (isinstance(imaging[key], int) and not isinstance(imaging[key], bool))
        ):
            raise ContractError(f"{source}: imaging.{key} must be a string or integer")


def _validate_run_types(run: dict[str, Any], source: str) -> None:
    for key in ("profile_repeats", "repeats"):
        if key in run:
            _integer(run, key, f"{source}: run")
    if "stream_log" in run and not isinstance(run["stream_log"], bool):
        raise ContractError(f"{source}: run.stream_log must be a boolean")
    if "env" in run:
        env = run["env"]
        if not isinstance(env, dict) or not all(
            isinstance(key, str) and isinstance(item, str)
            for key, item in env.items()
        ):
            raise ContractError(f"{source}: run.env must contain string keys and values")
    for key in set(run) - {"profile_repeats", "repeats", "stream_log", "env"}:
        _nonempty_string(run, key, f"{source}: run")


def _integer(value: dict[str, Any], key: str, source: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int):
        raise ContractError(f"{source}: {key} must be an integer")
    return item


def _string_list(value: dict[str, Any], key: str, source: str) -> list[str]:
    item = value.get(key)
    if not isinstance(item, list) or not item or not all(
        isinstance(entry, str) and entry for entry in item
    ):
        raise ContractError(f"{source}: {key} must be a non-empty string list")
    return item
