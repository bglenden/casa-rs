# SPDX-License-Identifier: LGPL-3.0-or-later
"""Strict versioned workload and run-result contracts."""

from __future__ import annotations

import math
import pathlib
import re
from typing import Any

from .artifacts import ArtifactError, load_json_object
from .casa_tclean import (
    RESULT_FIELDS as CASA_TCLEAN_RESULT_FIELDS,
    RESULT_KIND as CASA_TCLEAN_RESULT_KIND,
    RESULT_STATUSES as CASA_TCLEAN_RESULT_STATUSES,
    ProtocolError,
    canonical_sha256,
    validate_result_envelope,
)
from .image_compare import (
    COMPARISON_SCHEMA_VERSION,
    comparison_request_binding,
    validate_comparison_output,
)
from .tolerances import ToleranceContractError, validate_tolerance_contract


WORKLOAD_SCHEMA_VERSION = 1
LEGACY_RUN_RESULT_SCHEMA_VERSION = 2
RUN_RESULT_SCHEMA_VERSION = 3
LEGACY_COMPARISON_SCHEMA_VERSIONS = {3}
LIVE_UNVERSIONED_COMPARISON_VARIANT = "live_unversioned"
LEGACY_UNVERSIONED_COMPARISON_VARIANT = "legacy_unversioned"
LIVE_UNVERSIONED_COMPARISON_STATUSES = {
    "failed_execution",
    "skipped",
    "unavailable",
}
LEGACY_CASA_TCLEAN_RESULT_SCHEMA_VERSION = 2
LEGACY_CASA_RESOURCE_FIELDS = {
    "user_cpu_seconds",
    "system_cpu_seconds",
    "peak_rss_bytes",
    "minor_page_faults",
    "major_page_faults",
    "block_inputs",
    "block_outputs",
    "voluntary_context_switches",
    "involuntary_context_switches",
}

WORKLOAD_FIELDS = {
    "schema_version",
    "id",
    "mode_id",
    "description",
    "casa",
    "dataset",
    "imaging",
    "run",
    "comparison",
    "review",
}
CASA_FIELDS = {
    "dataset_geometry_path",
    "dataset_geometry_sha256",
    "dataset_selection",
    "recipe_path",
    "recipe_sha256",
    "runtime_identity_path",
    "runtime_identity_sha256",
}
DATASET_FIELDS = {"key", "path", "relative_path", "root_env"}
IMAGING_FIELDS = {
    "aterm",
    "calcpsf",
    "calcres",
    "casa_gridder",
    "cell_arcsec",
    "chanchunks",
    "channel_count",
    "channel_start",
    "computepastep",
    "conjbeams",
    "cyclefactor",
    "datacolumn",
    "deconvolver",
    "facets",
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
    "mask_image",
    "mask_sha256",
    "min_psf_fraction",
    "minor_cycle_length",
    "mode",
    "mosweight",
    "niter",
    "normtype",
    "nsigma",
    "nterms",
    "parallel",
    "pbcor",
    "pblimit",
    "perchanweightdensity",
    "phasecenter_field",
    "pointingoffsetsigdev",
    "projection",
    "psfcutoff",
    "psfphasecenter",
    "psterm",
    "restart",
    "restoration",
    "restoringbeam",
    "rotatepastep",
    "robust",
    "savemodel",
    "scales",
    "smallscalebias",
    "specmode",
    "spw",
    "standard_mfs_acceleration",
    "standard_mfs_grid_threads",
    "standard_mfs_metal_minor_cycle_chunk",
    "start",
    "threshold_jy",
    "intent",
    "interactive",
    "stokes",
    "usemask",
    "usepointing",
    "uvrange",
    "vptable",
    "wbawp",
    "weighting",
    "width",
    "wprojplanes",
    "write_pb",
    "wterm",
}
RUN_FIELDS = {
    "cf_cache_role",
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
    "warmups",
}
COMPARISON_FIELDS = {
    "full_chunk_elements",
    "max_elements_per_product",
    "mode",
    "products",
    "require_exact_product_inventory",
    "require_metadata_parity",
    "source_regions",
    "tolerances",
}
REVIEW_FIELDS = {
    "required_evidence_roles",
    "required_reviewer",
    "requires_human_acceptance_before_done",
}
LEGACY_RUN_RESULT_FIELDS = frozenset(
    {
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
)
RUN_RESULT_FIELDS = set(LEGACY_RUN_RESULT_FIELDS)
RUN_STATUSES = {
    "completed",
    "dry_run",
    "recovered_publication",
    "failed_execution",
    "failed_comparison",
    "out_of_tolerance",
    "unavailable",
}
LEGACY_RUN_STATUSES = frozenset(
    {
        "completed",
        "dry_run",
        "failed_execution",
        "failed_comparison",
        "out_of_tolerance",
        "unavailable",
    }
)

RUN_RESULT_KIND_STATUSES = {
    "workload_run": frozenset(RUN_STATUSES),
    "image_comparison": frozenset({"completed", "failed_comparison", "unavailable"}),
    "alternating_comparison": frozenset(
        {"completed", "failed_execution", "failed_comparison", "out_of_tolerance"}
    ),
    "simobserve_benchmark": frozenset({"completed", "out_of_tolerance"}),
    "aca_simalma_benchmark": frozenset(
        {"completed", "failed_comparison", "out_of_tolerance", "unavailable"}
    ),
}
NON_WORKLOAD_RESULT_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "status",
        "run_id",
        "created_at",
        "workload",
        "environment",
        "artifacts",
        "results",
    }
)
ENVIRONMENT_FIELDS = {
    "schema_version",
    "repository",
    "runtime",
    "executables",
    "datasets",
    "storage_label",
    "migration",
}
REPOSITORY_FIELDS = {"root", "revision", "branch", "dirty"}
RUNTIME_FIELDS = {
    "python",
    "platform",
    "machine",
    "logical_cores",
    "physical_cores",
    "physical_memory_bytes",
}
PATH_PROVENANCE_FIELDS = {
    "path",
    "exists",
    "exists_at_migration",
    "kind",
    "resolved_path",
    "sha256",
}
RESULT_MODE_FIELDS = {
    "bench_mode",
    "chanchunks",
    "channel_count",
    "deconvolver",
    "gridder",
    "hogbom_iteration_mode",
    "image_shape",
    "imaging_fft_backend",
    "imaging_fft_precision",
    "imaging_read_ahead_blocks",
    "niter",
    "nterms",
    "parallel",
    "perchanweightdensity",
    "specmode",
    "standard_mfs_acceleration",
    "standard_mfs_metal_minor_cycle_chunk",
    "start",
    "weighting",
    "width",
    "wprojplanes",
    "wave4_matrix_row_id",
}
RESULT_RUN_FIELDS = {
    "repeats",
    "run_label",
    "storage_label",
    "ms_staging",
    "phase_probe",
    "skip_casa",
    "skip_rust",
    "reuse_rust_prefix",
    "reuse_casa_prefix",
    "env",
    "stream_log",
    "profile_repeats",
    "warmups",
    "cf_cache_role",
    "evidence_role",
}
RESULT_REVIEW_FIELDS = REVIEW_FIELDS | {"evidence_role"}
HUMAN_REVIEW_FIELDS = {
    "status",
    "required_reviewer",
    "requires_human_acceptance_before_done",
    "evidence_role",
    "required_evidence_roles",
    "panel_status",
    "panel_reason",
    "structured_difference_label",
    "structured_difference_summary",
    "structured_difference_legend",
    "reason",
}
WORKLOAD_ARTIFACT_FIELDS = {
    "root",
    "result_dir",
    "products_root",
    "comparison_root",
    "protocol_root",
    "tmp_root",
    "cf_cache_root",
    "execution_products_root",
    "retained_products_root",
    "execution_comparison_root",
    "retained_comparison_root",
    "execution_protocol_root",
    "retained_protocol_root",
    "checked_in_path",
    "bundle",
}
BUNDLE_FIELDS = {
    "state",
    "partial_root",
    "final_root",
    "retained_root",
    "receipt_path",
    "execution_to_retained",
}
PRODUCT_PATH_FIELDS = {
    "root",
    "rust_prefix",
    "casa_prefix",
    "execution_root",
    "execution_casa_prefix",
}
LOG_FIELDS = {
    "benchmark_log",
    "benchmark_log_sha256",
    "execution_benchmark_log",
}
WORKLOAD_RESULT_FIELDS = {
    "rust",
    "casa",
    "stage_medians_ms",
    "casa_clean_control_diagnostics",
    "product_paths",
    "backend_plan_logs",
    "benchmark_features",
    "stage_breakdown",
    "product_comparison",
    "casa_repeatability_comparison",
    "casa_tclean_calls",
    "publication_recovery",
    "bundle_integrity",
    "failure",
}
FAILURE_FIELDS = {"kind", "reason", "return_code"}
TIMING_RESULT_FIELDS = {
    "status",
    "reason",
    "timings_seconds",
    "warmup_count",
    "cache_role",
    "evidence_summary",
}
TIMINGS_FIELDS = {"runs", "median"}
STAGE_BREAKDOWN_FIELDS = {
    "schema_version",
    "units",
    "instrumentation_scope",
    "contract_review",
    "rust",
    "casa",
}
STAGE_IMPLEMENTATION_FIELDS = {"status", "reason", "categories"}
STAGE_CATEGORY_FIELDS = {
    "status",
    "reason",
    "total_ms",
    "components_ms",
    "source_fields",
    "missing_fields",
    "description",
}
BENCHMARK_FEATURE_FIELDS = {
    "schema_version",
    "visibility",
    "image",
    "mode_cost",
    "resources",
    "backend",
}
BENCHMARK_VISIBILITY_FIELDS = {
    "selected_rows",
    "selected_channels",
    "correlations",
    "correlation_source",
    "flagged_fraction",
    "visibility_work",
    "gridded_samples",
    "source_stream_throughput_samples_per_s",
}
BENCHMARK_IMAGE_FIELDS = {
    "imsize_x",
    "imsize_y",
    "output_planes",
    "product_count",
    "image_work",
}
BENCHMARK_MODE_COST_FIELDS = {
    "specmode",
    "gridder",
    "deconvolver",
    "weighting",
    "niter",
    "cycleniter",
    "actual_major_cycles",
    "actual_minor_iterations",
    "multiscale_scale_count",
    "mtmfs_nterms",
    "wprojplanes",
    "mosaic_field_count",
}
COMPARISON_OUTPUT_FIELDS = {
    "schema_version",
    "request_binding",
    "request_sha256",
    "status",
    "reason",
    "comparison_mode",
    "max_elements_per_product",
    "full_chunk_elements",
    "left_prefix",
    "right_prefix",
    "left_label",
    "right_label",
    "requested_products",
    "require_exact_product_inventory",
    "require_metadata_parity",
    "legacy_operand_aliases",
    "source_regions",
    "tolerances",
    "panel_dir",
    "structure_workspace_dir",
    "product_inventory",
    "beam_info",
    "products",
    "structured_difference_review",
    "input",
    "input_sha256",
    "output",
    "output_sha256",
    "log",
    "log_sha256",
    "return_code",
    "failure",
    "tolerance_evaluation",
    "left_call",
    "right_call",
    "comparison_kind",
    "retained_artifacts",
    "retained_input",
    "retained_output",
    "retained_log",
    "retained_panel_dir",
    "retained_left_prefix",
    "retained_right_prefix",
}
LEGACY_COMPARISON_SUMMARY_FIELDS = {
    "status",
    "products",
    "beam_info",
    "structured_difference_review",
    "input",
    "log",
    "panel_dir",
}
LEGACY_COMPARISON_FAILURE_SUMMARY_FIELDS = {"status", "reason", "products"}
LEGACY_UNVERSIONED_COMPARISON_PROTOCOL_FIELDS = {
    "comparison_kind",
    "comparison_mode",
    "input",
    "input_sha256",
    "left_call",
    "left_label",
    "log",
    "log_sha256",
    "output",
    "output_sha256",
    "panel_dir",
    "products",
    "reason",
    "retained_artifacts",
    "retained_input",
    "retained_log",
    "retained_output",
    "retained_panel_dir",
    "return_code",
    "right_call",
    "right_label",
    "status",
    "tolerance_evaluation",
    "tolerances",
}
COMPARISON_REQUEST_BINDING_FIELDS = {
    "schema_version",
    "mode",
    "left_prefix",
    "right_prefix",
    "left_label",
    "right_label",
    "products",
    "max_elements_per_product",
    "full_chunk_elements",
    "require_exact_product_inventory",
    "require_metadata_parity",
    "legacy_operand_aliases",
    "source_regions",
    "tolerances",
    "panel_dir",
    "structure_workspace_dir",
}
COMPARISON_PRODUCT_FIELDS = {
    "status",
    "rust_exists",
    "casa_exists",
    "rust_path",
    "casa_path",
    "left_path",
    "right_path",
    "retained_rust_path",
    "retained_casa_path",
    "retained_left_path",
    "retained_right_path",
    "left_label",
    "right_label",
    "comparison_mode",
    "metadata_parity_required",
    "metadata",
    "topology_parity",
    "shape",
    "sample_stride",
    "sampled_elements",
    "finite_overlap",
    "rust_min",
    "rust_max",
    "rust_rms",
    "casa_min",
    "casa_max",
    "casa_rms",
    "left_min",
    "left_max",
    "left_rms",
    "right_min",
    "right_max",
    "right_rms",
    "diff_rms",
    "diff_abs_max",
    "diff_rms_over_casa_rms",
    "diff_abs_max_over_casa_peak",
    "diff_rms_over_right_rms",
    "diff_abs_max_over_right_peak",
    "correlation",
    "rust_peak_abs",
    "casa_peak_abs",
    "left_peak_abs",
    "right_peak_abs",
    "diff_peak_abs",
    "review_panel",
    "structured_difference",
    "sampled_structured_difference",
    "full_array",
}
COMPARISON_REVIEW_PANEL_FIELDS = {
    "status",
    "path",
    "sha256",
    "retained_path",
    "left_label",
    "right_label",
    "casa_rs_and_casa_color_limits",
    "left_and_right_color_limits",
    "difference_color_limits",
    "display_bounds",
    "display_description",
    "display_reason",
    "display_sample_stride",
    "display_shape",
    "display_status",
    "display_transform",
    "structured_difference_label",
    "structured_difference_summary",
    "zoom_panel",
}
COMPARISON_ZOOM_PANEL_FIELDS = {
    "status",
    "path",
    "retained_path",
    "reason",
    "bounds",
    "casa_rs_and_casa_color_limits",
    "difference_color_limits",
}
COMPARISON_BEAM_INFO_FIELDS = {
    "status",
    "psf_path",
    "retained_psf_path",
    "sample_stride",
    "peak_abs",
    "peak_location",
    "fwhm_pixels",
    "beam_area_pixels",
    "beam_block_side_pixels",
    "coordinate_domain",
    "estimation_method",
    "native_plane_coverage",
}
COMPARISON_INVENTORY_FIELDS = {
    "status",
    "required",
    "observed_match",
    "expected",
    "left",
    "right",
    "left_missing",
    "left_extra",
    "right_missing",
    "right_extra",
    "left_right_equal",
}
COMPARISON_STRUCTURED_REVIEW_FIELDS = {
    "label",
    "summary",
    "products",
    "product_summaries",
    "checks_by_product",
    "thresholds",
    "legend",
}
COMPARISON_TOLERANCE_EVALUATION_FIELDS = {
    "contract_version",
    "status",
    "checks",
    "failed_checks",
    "incomplete_checks",
}
COMPARISON_TOLERANCE_CHECK_FIELDS = {
    "name",
    "status",
    "actual",
    "ceiling",
    "reason",
}
COMPARISON_RETAINED_ARTIFACT_FIELDS = {"input", "output", "log", "panel_dir"}
COMPARISON_STRUCTURED_DIFFERENCE_FIELDS = {
    "status",
    "evidence_scope",
    "analysis_pixels",
    "masked_pixels",
    "diff_rms",
    "normalized_diff_rms",
    "normalization",
    "mask",
    "beam_info",
    "beam_block_side_pixels",
    "beam_block_rms_by_scale",
    "block_rms_decay_slope_vs_independent_beams",
    "large_scale_power_fraction",
    "low_order_r2_quadratic",
    "scale_offset_gradient_fit",
    "native_spatial_evidence",
    "classification",
    "review",
}
LEGACY_COMPARISON_PRODUCT_FIELDS = COMPARISON_PRODUCT_FIELDS - {
    "full_array",
    "sampled_structured_difference",
    "topology_parity",
}
LEGACY_COMPARISON_BEAM_INFO_FIELDS = COMPARISON_BEAM_INFO_FIELDS - {
    "coordinate_domain",
    "estimation_method",
    "native_plane_coverage",
}
LEGACY_COMPARISON_STRUCTURED_DIFFERENCE_FIELDS = (
    COMPARISON_STRUCTURED_DIFFERENCE_FIELDS
    - {
        "evidence_scope",
        "beam_info",
        "native_spatial_evidence",
    }
)
COMPARISON_CLASSIFICATION_FIELDS = {
    "amplitude",
    "structure",
    "overall",
    "structure_components",
    "structure_suppressed_by_numerical_floor",
    "thresholds",
}
COMPARISON_STRUCTURE_FIT_FIELDS = {
    "status",
    "reason",
    "model",
    "coefficients",
    "r2",
    "diff_rms",
    "residual_rms",
    "fit_pixels",
    "masked_pixels",
    "excluded_nonfinite_basis_pixels",
}
COMPARISON_STRUCTURE_BLOCK_FIELDS = {
    "beam_width_multiplier",
    "block_side_pixels",
    "n_blocks",
    "approx_independent_beams_per_block",
    "mean_pixel_rms_in_blocks",
    "block_mean_rms",
    "block_mean_rms_over_mean_pixel_rms",
    "normalized_block_mean_rms",
    "median_abs_block_mean",
    "max_block_robust_z",
}
COMPARISON_METADATA_FIELDS = {"status", "parity", "left", "right", "field_parity"}
COMPARISON_METADATA_SIDE_FIELDS = {
    "status",
    "shape",
    "unit",
    "masks",
    "coordinates",
    "restoring_beam",
    "errors",
}
CASA_CALL_FIELDS = {
    "name",
    "role",
    "measured",
    "prefix",
    "request_path",
    "request_sha256",
    "result_path",
    "result_sha256",
    "stdout_stderr_path",
    "stdout_stderr_sha256",
    "exit_code",
    "casa_log_paths",
    "casa_log_identities",
    "cache_receipt_sha256",
    "result",
    "retained_prefix",
    "retained_request_path",
    "retained_result_path",
    "retained_stdout_stderr_path",
    "retained_casa_log_paths",
    "retained_casa_log_identities",
}
CASA_CALL_IDENTITY_FIELDS = {"path", "sha256"}


class ContractError(ValueError):
    """A workload or result violates the canonical evidence contract."""


def load_workload_manifest(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = load_json_object(path, description="workload manifest")
    except ArtifactError as error:
        raise ContractError(str(error)) from error
    validate_workload_manifest(value, source=str(path))
    return value


def validate_workload_manifest(
    value: dict[str, Any], *, source: str = "workload"
) -> None:
    _schema_version(value, WORKLOAD_SCHEMA_VERSION, source)
    unknown = sorted(set(value) - WORKLOAD_FIELDS)
    if unknown:
        raise ContractError(
            f"{source}: unknown workload field(s): {', '.join(unknown)}"
        )
    _nonempty_string(value, "id", source)
    _nonempty_string(value, "mode_id", source)
    if "description" in value and not isinstance(value["description"], str):
        raise ContractError(f"{source}: description must be a string")
    casa = value.get("casa")
    if casa is not None:
        if not isinstance(casa, dict):
            raise ContractError(f"{source}: casa must be an object")
        _allowed_fields(casa, CASA_FIELDS, f"{source}: casa")
        _nonempty_string(casa, "recipe_path", f"{source}: casa")
        recipe_sha256 = _nonempty_string(casa, "recipe_sha256", f"{source}: casa")
        if re.fullmatch(r"[0-9a-f]{64}", recipe_sha256) is None:
            raise ContractError(
                f"{source}: casa.recipe_sha256 must be a lowercase SHA-256 digest"
            )
        identity_pairs = (
            ("dataset_geometry_path", "dataset_geometry_sha256"),
            ("runtime_identity_path", "runtime_identity_sha256"),
        )
        for path_key, digest_key in identity_pairs:
            if (path_key in casa) != (digest_key in casa):
                raise ContractError(
                    f"{source}: casa.{path_key} and casa.{digest_key} must be set together"
                )
            if path_key in casa:
                _nonempty_string(casa, path_key, f"{source}: casa")
                digest = _nonempty_string(casa, digest_key, f"{source}: casa")
                if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                    raise ContractError(
                        f"{source}: casa.{digest_key} must be a lowercase SHA-256 digest"
                    )
        if "dataset_geometry_path" in casa and "dataset_selection" not in casa:
            raise ContractError(
                f"{source}: casa.dataset_selection is required with dataset geometry"
            )
        if "dataset_selection" in casa:
            _nonempty_string(casa, "dataset_selection", f"{source}: casa")
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
        if (
            _integer(comparison, "max_elements_per_product", f"{source}: comparison")
            < 1
        ):
            raise ContractError(
                f"{source}: comparison.max_elements_per_product must be >= 1"
            )
    if "full_chunk_elements" in comparison:
        if _integer(comparison, "full_chunk_elements", f"{source}: comparison") < 1:
            raise ContractError(
                f"{source}: comparison.full_chunk_elements must be >= 1"
            )
    if "products" in comparison:
        _string_list(comparison, "products", f"{source}: comparison")
    comparison_mode = comparison.get("mode", "sampled")
    if comparison_mode not in {"full", "sampled"}:
        raise ContractError(f"{source}: comparison.mode must be full or sampled")
    for key in ("require_exact_product_inventory", "require_metadata_parity"):
        if key in comparison and not isinstance(comparison[key], bool):
            raise ContractError(f"{source}: comparison.{key} must be a boolean")
    if "tolerances" in comparison:
        try:
            validate_tolerance_contract(
                comparison["tolerances"], source=f"{source}: comparison.tolerances"
            )
        except ToleranceContractError as error:
            raise ContractError(str(error)) from error
    if "source_regions" in comparison:
        _validate_source_regions(comparison, source=source)

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

    _validate_cross_fields(
        imaging=imaging,
        run=run,
        comparison=comparison,
        source=source,
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
    _validate_run_result_version(
        value,
        source=source,
        expected_version=RUN_RESULT_SCHEMA_VERSION,
        fields=RUN_RESULT_FIELDS,
        statuses=RUN_STATUSES,
        allow_publication_recovery=True,
    )


def validate_legacy_run_result_v2(
    value: dict[str, Any], *, source: str = "legacy result"
) -> None:
    """Validate schema-v2 evidence only for the explicit v2-to-v3 migrator."""

    _validate_run_result_version(
        value,
        source=source,
        expected_version=LEGACY_RUN_RESULT_SCHEMA_VERSION,
        fields=LEGACY_RUN_RESULT_FIELDS,
        statuses=LEGACY_RUN_STATUSES,
        allow_publication_recovery=False,
    )


def _validate_run_result_version(
    value: dict[str, Any],
    *,
    source: str,
    expected_version: int,
    fields: set[str] | frozenset[str],
    statuses: set[str] | frozenset[str],
    allow_publication_recovery: bool,
) -> None:
    _schema_version(value, expected_version, source)
    unknown = sorted(set(value) - fields)
    if unknown:
        raise ContractError(f"{source}: unknown result field(s): {', '.join(unknown)}")
    status = value.get("status")
    if status not in statuses:
        raise ContractError(
            f"{source}: status must be one of {', '.join(sorted(statuses))}"
        )
    if expected_version == RUN_RESULT_SCHEMA_VERSION:
        _validate_strict_run_result_v3(value, source=source)
        return

    # Schema v2 is an input format for the one-time migrator, not the durable
    # evidence contract. Keep its historical shallow validation unchanged so
    # malformed old records can be repaired by the explicit migration code.
    _nonempty_string(value, "kind", source)
    _nonempty_string(value, "run_id", source)
    _nonempty_string(value, "created_at", source)
    _object(value, "environment", source)
    _object(value, "artifacts", source)
    results = _object(value, "results", source)
    failure_statuses = statuses - {"completed", "dry_run", "recovered_publication"}
    if status in failure_statuses:
        failure = results.get("failure")
        if not isinstance(failure, dict):
            raise ContractError(f"{source}: {status} requires results.failure")
        _nonempty_string(failure, "kind", f"{source}: results.failure")
        _nonempty_string(failure, "reason", f"{source}: results.failure")
    if allow_publication_recovery and status == "recovered_publication":
        recovery = _object(results, "publication_recovery", f"{source}: results")
        expected = {
            "kind": "cold_cf_cache_publication",
            "status": "completed",
            "protocol_status": "recovered_publication",
            "benchmark_eligible": False,
            "timing_accepted": False,
            "tclean_reinvoked": False,
        }
        for key, expected_value in expected.items():
            if recovery.get(key) != expected_value:
                raise ContractError(
                    f"{source}: results.publication_recovery.{key} "
                    f"must be {expected_value!r}"
                )
        casa = _object(results, "casa", f"{source}: results")
        timings = _object(casa, "timings_seconds", f"{source}: results.casa")
        if timings.get("runs") != [] or timings.get("median") is not None:
            raise ContractError(
                f"{source}: publication recovery cannot contain accepted CASA timings"
            )
        bundle = value.get("artifacts", {}).get("bundle")
        if isinstance(bundle, dict) and bundle.get("state") == "complete":
            raise ContractError(
                f"{source}: publication recovery cannot be a complete evidence bundle"
            )
    for key in ("workload", "mode", "run"):
        if key in value:
            _object(value, key, source)


def _validate_strict_run_result_v3(value: dict[str, Any], *, source: str) -> None:
    kind = _nonempty_string(value, "kind", source)
    if kind not in RUN_RESULT_KIND_STATUSES:
        raise ContractError(
            f"{source}: kind must be one of "
            + ", ".join(sorted(RUN_RESULT_KIND_STATUSES))
        )
    status = value["status"]
    allowed_statuses = RUN_RESULT_KIND_STATUSES[kind]
    if status not in allowed_statuses:
        raise ContractError(f"{source}: status {status!r} is invalid for kind {kind!r}")
    if kind != "workload_run":
        unknown = sorted(set(value) - NON_WORKLOAD_RESULT_FIELDS)
        if unknown:
            raise ContractError(
                f"{source}: field(s) are invalid for {kind}: {', '.join(unknown)}"
            )

    _nonempty_string(value, "run_id", source)
    _utc_timestamp(value, "created_at", source)
    environment = _object(value, "environment", source)
    _validate_environment(environment, source=f"{source}: environment")
    migrated_environment = "migration" in environment
    artifacts = _object(value, "artifacts", source)
    _validate_result_artifacts(artifacts, kind=kind, source=f"{source}: artifacts")
    results = _object(value, "results", source)
    _validate_results(
        results,
        kind=kind,
        migrated_environment=migrated_environment,
        source=f"{source}: results",
    )

    failure_statuses = allowed_statuses - {
        "completed",
        "dry_run",
        "recovered_publication",
    }
    failure = results.get("failure")
    if status in failure_statuses:
        if not isinstance(failure, dict):
            raise ContractError(f"{source}: {status} requires results.failure")
        _validate_failure(failure, source=f"{source}: results.failure")
    elif failure is not None:
        raise ContractError(f"{source}: status {status} forbids results.failure")

    if kind == "workload_run":
        _validate_workload_result_envelope(value, source=source)
    elif kind == "image_comparison":
        if "workload" not in value:
            raise ContractError(f"{source}: image_comparison requires workload")
        _validate_result_workload(value["workload"], source=f"{source}: workload")
    elif "workload" in value:
        _validate_result_workload(value["workload"], source=f"{source}: workload")


def _validate_workload_result_envelope(value: dict[str, Any], *, source: str) -> None:
    status = value["status"]
    environment = value["environment"]
    migrated = isinstance(environment, dict) and "migration" in environment
    if status == "dry_run":
        for key in ("started_at", "completed_at"):
            if key in value:
                raise ContractError(f"{source}: dry_run forbids {key}")
    else:
        for key in ("started_at", "completed_at"):
            if key in value:
                _utc_timestamp(value, key, source)
            elif not migrated:
                raise ContractError(f"{source}: {status} requires {key}")

    if "exit_code" in value:
        exit_code = _integer(value, "exit_code", source)
        if (
            status in {"completed", "dry_run", "recovered_publication"}
            and exit_code != 0
        ):
            raise ContractError(f"{source}: {status} requires exit_code=0")
    elif not migrated:
        raise ContractError(f"{source}: workload_run requires exit_code")

    if "manifest_path" in value:
        _nonempty_string(value, "manifest_path", source)
    if "workload" in value:
        _validate_result_workload(value["workload"], source=f"{source}: workload")
    if "dataset" in value:
        _validate_result_dataset(value["dataset"], source=f"{source}: dataset")
    if "mode" in value:
        _validate_result_mode(value["mode"], source=f"{source}: mode")
    if "run" in value:
        _validate_result_run(value["run"], source=f"{source}: run")
    if "comparison" in value:
        _validate_result_comparison(value["comparison"], source=f"{source}: comparison")
    if "review" in value:
        _validate_result_review(value["review"], source=f"{source}: review")
    if "run_support" in value:
        _validate_run_support(value["run_support"], source=f"{source}: run_support")
    if "command" in value:
        _validate_result_command(value["command"], source=f"{source}: command")
    if "products" in value:
        _validate_products(value["products"], source=f"{source}: products")
    if "logs" in value:
        _validate_logs(value["logs"], source=f"{source}: logs")
    if "benchmark_features" in value:
        _validate_benchmark_features(
            value["benchmark_features"], source=f"{source}: benchmark_features"
        )
    if "human_review" in value:
        _validate_human_review(value["human_review"], source=f"{source}: human_review")
    if "wave4_acceleration" in value:
        wave4 = _require_dict(
            value["wave4_acceleration"], f"{source}: wave4_acceleration"
        )
        _allowed_fields(wave4, {"matrix_row_id"}, f"{source}: wave4_acceleration")
        _nonempty_string(wave4, "matrix_row_id", f"{source}: wave4_acceleration")

    if status == "recovered_publication":
        _validate_publication_recovery(value, source=source)


def _validate_publication_recovery(value: dict[str, Any], *, source: str) -> None:
    results = value["results"]
    recovery = _require_dict(
        results.get("publication_recovery"), f"{source}: results.publication_recovery"
    )
    expected = {
        "kind": "cold_cf_cache_publication",
        "status": "completed",
        "protocol_status": "recovered_publication",
        "benchmark_eligible": False,
        "timing_accepted": False,
        "tclean_reinvoked": False,
    }
    for key, expected_value in expected.items():
        if recovery.get(key) != expected_value:
            raise ContractError(
                f"{source}: results.publication_recovery.{key} must be {expected_value!r}"
            )
    casa = _require_dict(results.get("casa"), f"{source}: results.casa")
    timings = _require_dict(
        casa.get("timings_seconds"), f"{source}: results.casa.timings_seconds"
    )
    if timings.get("runs") != [] or timings.get("median") is not None:
        raise ContractError(
            f"{source}: publication recovery cannot contain accepted CASA timings"
        )
    bundle = value.get("artifacts", {}).get("bundle")
    if isinstance(bundle, dict) and bundle.get("state") == "complete":
        raise ContractError(
            f"{source}: publication recovery cannot be a complete evidence bundle"
        )


def _validate_environment(value: dict[str, Any], *, source: str) -> None:
    _allowed_fields(value, ENVIRONMENT_FIELDS, source)
    _schema_version(value, 1, source)
    required = {
        "repository",
        "runtime",
        "executables",
        "datasets",
        "storage_label",
    }
    missing = sorted(required - set(value))
    if missing:
        raise ContractError(f"{source}: missing field(s): {', '.join(missing)}")
    repository = _object(value, "repository", source)
    _allowed_fields(repository, REPOSITORY_FIELDS, f"{source}: repository")
    if set(repository) != REPOSITORY_FIELDS:
        raise ContractError(f"{source}: repository fields do not match protocol")
    for key in ("root", "revision", "branch"):
        _optional_string(repository.get(key), f"{source}: repository.{key}")
    if repository["dirty"] is not None and not isinstance(repository["dirty"], bool):
        raise ContractError(f"{source}: repository.dirty must be boolean or null")

    runtime = _object(value, "runtime", source)
    _allowed_fields(runtime, RUNTIME_FIELDS, f"{source}: runtime")
    if set(runtime) != RUNTIME_FIELDS:
        raise ContractError(f"{source}: runtime fields do not match protocol")
    for key in ("python", "platform", "machine"):
        _optional_string(runtime.get(key), f"{source}: runtime.{key}")
    for key in ("logical_cores", "physical_cores", "physical_memory_bytes"):
        _optional_integer(runtime.get(key), f"{source}: runtime.{key}")

    migrated_environment = "migration" in value
    for key in ("executables", "datasets"):
        records = _object(value, key, source)
        for name, record in records.items():
            if not isinstance(name, str) or not name:
                raise ContractError(f"{source}: {key} names must be non-empty strings")
            _validate_path_provenance(
                record,
                migrated_environment=migrated_environment,
                source=f"{source}: {key}.{name}",
            )
    _optional_string(value["storage_label"], f"{source}: storage_label")
    if "migration" in value:
        migration = _require_dict(value["migration"], f"{source}: migration")
        _allowed_fields(
            migration, {"source_schema_version", "method"}, f"{source}: migration"
        )
        if set(migration) != {"source_schema_version", "method"}:
            raise ContractError(f"{source}: migration fields do not match protocol")
        _integer(migration, "source_schema_version", f"{source}: migration")
        _nonempty_string(migration, "method", f"{source}: migration")


def _validate_path_provenance(
    value: Any, *, migrated_environment: bool, source: str
) -> None:
    if value is None:
        return
    record = _require_dict(value, source)
    _allowed_fields(record, PATH_PROVENANCE_FIELDS, source)
    _nonempty_string(record, "path", source)
    live = "exists" in record
    migrated = "exists_at_migration" in record
    if live == migrated:
        raise ContractError(
            f"{source}: requires exactly one of exists or exists_at_migration"
        )
    if migrated_environment != migrated:
        expected = "exists_at_migration" if migrated_environment else "exists"
        raise ContractError(
            f"{source}: environment variant requires {expected} provenance"
        )
    existence_key = "exists" if live else "exists_at_migration"
    if not isinstance(record[existence_key], bool):
        raise ContractError(f"{source}: {existence_key} must be a boolean")
    if live:
        kind = _nonempty_string(record, "kind", source)
        if kind not in {"directory", "file", "missing"}:
            raise ContractError(f"{source}: kind is invalid")
    elif "kind" in record or "resolved_path" in record:
        raise ContractError(f"{source}: migrated path contains live-only fields")
    if "resolved_path" in record:
        _nonempty_string(record, "resolved_path", source)
    if "sha256" in record:
        _sha256_or_historical(record["sha256"], f"{source}: sha256")


def _validate_result_artifacts(
    value: dict[str, Any], *, kind: str, source: str
) -> None:
    allowed_by_kind = {
        "workload_run": WORKLOAD_ARTIFACT_FIELDS,
        "image_comparison": {"checked_in_path"},
        "alternating_comparison": {
            "checked_in_path",
            "report_path",
            "comparison_artifact_root",
        },
        "simobserve_benchmark": {"result_json", "report_html", "run_root"},
        "aca_simalma_benchmark": {"result_json", "run_root"},
    }
    _allowed_fields(value, set(allowed_by_kind[kind]), source)
    if not value:
        raise ContractError(f"{source}: {kind} artifacts must not be empty")
    required_by_kind = {
        "image_comparison": {"checked_in_path"},
        "simobserve_benchmark": {"result_json", "report_html", "run_root"},
        "aca_simalma_benchmark": {"result_json", "run_root"},
    }
    required = required_by_kind.get(kind, set())
    missing = sorted(required - set(value))
    if missing:
        raise ContractError(f"{source}: missing field(s): {', '.join(missing)}")
    if kind == "alternating_comparison" and not (
        {"checked_in_path", "report_path"} <= set(value)
        or {"report_path", "comparison_artifact_root"} <= set(value)
    ):
        raise ContractError(
            f"{source}: alternating comparison artifacts require a report and "
            "checked-in path or comparison artifact root"
        )
    for key, item in value.items():
        if key == "bundle":
            _validate_bundle(item, source=f"{source}: bundle")
        else:
            _optional_string(item, f"{source}: {key}")


def _validate_bundle(value: Any, *, source: str) -> None:
    bundle = _require_dict(value, source)
    _allowed_fields(bundle, BUNDLE_FIELDS, source)
    required = {
        "state",
        "partial_root",
        "final_root",
        "retained_root",
        "execution_to_retained",
    }
    missing = sorted(required - set(bundle))
    if missing:
        raise ContractError(f"{source}: missing field(s): {', '.join(missing)}")
    state = _nonempty_string(bundle, "state", source)
    if state not in {
        "planned",
        "partial",
        "complete",
        "promotion_failed",
        "integrity_failed",
    }:
        raise ContractError(f"{source}: state is invalid")
    for key in ("partial_root", "final_root"):
        _nonempty_string(bundle, key, source)
    _optional_string(bundle["retained_root"], f"{source}: retained_root")
    if "receipt_path" in bundle:
        _nonempty_string(bundle, "receipt_path", source)
    mapping = _require_dict(
        bundle["execution_to_retained"], f"{source}: execution_to_retained"
    )
    _allowed_fields(mapping, {"from", "to"}, f"{source}: execution_to_retained")
    if set(mapping) != {"from", "to"}:
        raise ContractError(
            f"{source}: execution_to_retained fields do not match protocol"
        )
    _nonempty_string(mapping, "from", f"{source}: execution_to_retained")
    _optional_string(mapping["to"], f"{source}: execution_to_retained.to")


def _validate_result_workload(value: Any, *, source: str) -> None:
    workload = _require_dict(value, source)
    _allowed_fields(workload, {"id", "mode_id", "description"}, source)
    _nonempty_string(workload, "id", source)
    if "mode_id" in workload:
        _nonempty_string(workload, "mode_id", source)
    if "description" in workload and not isinstance(workload["description"], str):
        raise ContractError(f"{source}: description must be a string")


def _validate_result_dataset(value: Any, *, source: str) -> None:
    dataset = _require_dict(value, source)
    _allowed_fields(dataset, DATASET_FIELDS, source)
    _nonempty_string(dataset, "key", source)
    for key in ("path", "relative_path", "root_env"):
        if key in dataset:
            _optional_string(dataset[key], f"{source}: {key}")


def _validate_result_mode(value: Any, *, source: str) -> None:
    mode = _require_dict(value, source)
    _allowed_fields(mode, RESULT_MODE_FIELDS, source)
    integer_fields = {"channel_count", "niter", "nterms"}
    nullable_integer_fields = {"chanchunks", "imaging_read_ahead_blocks"}
    nullable_string_fields = {
        "start",
        "width",
        "wprojplanes",
        "standard_mfs_metal_minor_cycle_chunk",
    }
    special = (
        integer_fields
        | nullable_integer_fields
        | nullable_string_fields
        | {
            "image_shape",
            "parallel",
        }
    )
    for key in set(mode) - special:
        _nonempty_string(mode, key, source)
    for key in integer_fields & set(mode):
        _integer(mode, key, source)
    for key in nullable_integer_fields & set(mode):
        _optional_integer(mode[key], f"{source}: {key}")
    for key in nullable_string_fields & set(mode):
        _optional_string(mode[key], f"{source}: {key}")
    if (
        "parallel" in mode
        and mode["parallel"] is not None
        and not isinstance(mode["parallel"], bool)
    ):
        raise ContractError(f"{source}: parallel must be boolean or null")
    if "image_shape" in mode:
        shape = mode["image_shape"]
        if not isinstance(shape, list) or len(shape) != 2:
            raise ContractError(f"{source}: image_shape must contain two integers")
        for index, dimension in enumerate(shape):
            _positive_integer(dimension, f"{source}: image_shape[{index}]")


def _validate_result_run(value: Any, *, source: str) -> None:
    run = _require_dict(value, source)
    _allowed_fields(run, RESULT_RUN_FIELDS, source)
    for key in ("repeats", "profile_repeats", "warmups"):
        if key in run:
            _optional_integer(run[key], f"{source}: {key}", optional=False)
    if "stream_log" in run and not isinstance(run["stream_log"], bool):
        raise ContractError(f"{source}: stream_log must be a boolean")
    if "env" in run:
        _validate_string_map(run["env"], f"{source}: env")
    nullable = {"reuse_rust_prefix", "reuse_casa_prefix"}
    special = {"repeats", "profile_repeats", "warmups", "stream_log", "env"} | nullable
    for key in set(run) - special:
        _nonempty_string(run, key, source)
    for key in nullable & set(run):
        _optional_string(run[key], f"{source}: {key}")


def _validate_result_comparison(value: Any, *, source: str) -> None:
    comparison = _require_dict(value, source)
    _allowed_fields(comparison, COMPARISON_FIELDS, source)
    for key in ("max_elements_per_product", "full_chunk_elements"):
        if key in comparison:
            _positive_integer(comparison[key], f"{source}: {key}")
    if "mode" in comparison:
        mode = _nonempty_string(comparison, "mode", source)
        if mode not in {"full", "sampled"}:
            raise ContractError(f"{source}: mode must be full or sampled")
    if "products" in comparison:
        _nonempty_string_list(comparison["products"], f"{source}: products")
    for key in ("require_exact_product_inventory", "require_metadata_parity"):
        if key in comparison and not isinstance(comparison[key], bool):
            raise ContractError(f"{source}: {key} must be a boolean")
    if "source_regions" in comparison:
        if not isinstance(comparison["source_regions"], list):
            raise ContractError(f"{source}: source_regions must be a list")
        if comparison["source_regions"]:
            _validate_source_regions(comparison, source=source)
    if "tolerances" in comparison and comparison["tolerances"] is not None:
        try:
            validate_tolerance_contract(
                comparison["tolerances"], source=f"{source}: tolerances"
            )
        except ToleranceContractError as error:
            raise ContractError(str(error)) from error


def _validate_result_review(value: Any, *, source: str) -> None:
    review = _require_dict(value, source)
    _allowed_fields(review, RESULT_REVIEW_FIELDS, source)
    for key in {"required_reviewer", "evidence_role"} & set(review):
        _nonempty_string(review, key, source)
    if "required_evidence_roles" in review:
        _nonempty_string_list(
            review["required_evidence_roles"], f"{source}: required_evidence_roles"
        )
    if "requires_human_acceptance_before_done" in review and not isinstance(
        review["requires_human_acceptance_before_done"], bool
    ):
        raise ContractError(
            f"{source}: requires_human_acceptance_before_done must be boolean"
        )


def _validate_run_support(value: Any, *, source: str) -> None:
    support = _require_dict(value, source)
    _allowed_fields(support, {"status", "reason", "bench_script", "targets"}, source)
    _nonempty_string(support, "status", source)
    _optional_string(support.get("reason"), f"{source}: reason")
    if "bench_script" not in support:
        raise ContractError(f"{source}: bench_script is required")
    _optional_string(support["bench_script"], f"{source}: bench_script")
    if "targets" not in support:
        return
    targets = _require_dict(support["targets"], f"{source}: targets")
    if set(targets) != {"casa", "rust"}:
        raise ContractError(f"{source}: targets must contain exactly casa and rust")
    casa = _require_dict(targets["casa"], f"{source}: targets.casa")
    _allowed_fields(casa, {"status", "reason", "runner"}, f"{source}: targets.casa")
    if set(casa) != {"status", "reason", "runner"}:
        raise ContractError(f"{source}: targets.casa fields do not match protocol")
    _nonempty_string(casa, "status", f"{source}: targets.casa")
    _optional_string(casa["reason"], f"{source}: targets.casa.reason")
    _nonempty_string(casa, "runner", f"{source}: targets.casa")
    rust = _require_dict(targets["rust"], f"{source}: targets.rust")
    _allowed_fields(
        rust,
        {"status", "reason", "missing_capabilities"},
        f"{source}: targets.rust",
    )
    if set(rust) != {"status", "reason", "missing_capabilities"}:
        raise ContractError(f"{source}: targets.rust fields do not match protocol")
    _nonempty_string(rust, "status", f"{source}: targets.rust")
    _optional_string(rust["reason"], f"{source}: targets.rust.reason")
    _string_list_allow_empty(
        rust["missing_capabilities"], f"{source}: targets.rust.missing_capabilities"
    )


def _validate_result_command(value: Any, *, source: str) -> None:
    command = _require_dict(value, source)
    kind = command.get("kind", "legacy_benchmark_script")
    if kind == "legacy_benchmark_script":
        _allowed_fields(command, {"kind", "argv", "env"}, source)
        _string_list_allow_empty(command.get("argv"), f"{source}: argv")
        _validate_string_map(command.get("env"), f"{source}: env")
        return
    if kind != "casa_tclean_protocol":
        raise ContractError(f"{source}: unsupported command kind {kind!r}")
    _allowed_fields(
        command,
        {"kind", "env", "casa", "rust", "evidence_storage", "argv"},
        source,
    )
    _validate_string_map(command.get("env"), f"{source}: env")
    _string_list_allow_empty(command.get("argv"), f"{source}: argv")
    casa = _require_dict(command.get("casa"), f"{source}: casa")
    casa_fields = {
        "python",
        "runner",
        "expected_version",
        "recipe",
        "base_overrides",
        "cache_plan",
        "runtime_identity",
        "dataset_geometry",
        "mask_identity",
        "request_template",
        "effective_plan",
        "cache_path",
        "cache_receipt_path",
        "cache_plan_sha256",
    }
    _allowed_fields(casa, casa_fields, f"{source}: casa")
    for key in ("python", "runner", "expected_version"):
        _nonempty_string(casa, key, f"{source}: casa")
    for key in ("cache_path", "cache_receipt_path", "cache_plan_sha256"):
        if key in casa:
            _nonempty_string(casa, key, f"{source}: casa")
    recipe = _require_dict(casa.get("recipe"), f"{source}: casa.recipe")
    _allowed_fields(
        recipe, {"path", "sha256", "task", "parameter_names"}, f"{source}: casa.recipe"
    )
    if set(recipe) != {"path", "sha256", "task", "parameter_names"}:
        raise ContractError(f"{source}: casa.recipe fields do not match protocol")
    for key in ("path", "task"):
        _nonempty_string(recipe, key, f"{source}: casa.recipe")
    _sha256_or_historical(recipe["sha256"], f"{source}: casa.recipe.sha256")
    _string_list_allow_empty(
        recipe["parameter_names"], f"{source}: casa.recipe.parameter_names"
    )
    for key in ("base_overrides", "cache_plan", "request_template", "effective_plan"):
        if key in casa:
            _require_dict(casa[key], f"{source}: casa.{key}")
    for key in ("runtime_identity", "dataset_geometry", "mask_identity"):
        if key in casa and casa[key] is not None:
            _require_dict(casa[key], f"{source}: casa.{key}")
    rust = _require_dict(command.get("rust"), f"{source}: rust")
    _allowed_fields(
        rust,
        {"status", "intended_parameters", "missing_capabilities"},
        f"{source}: rust",
    )
    if set(rust) != {"status", "intended_parameters", "missing_capabilities"}:
        raise ContractError(f"{source}: rust fields do not match protocol")
    _nonempty_string(rust, "status", f"{source}: rust")
    intended = _require_dict(
        rust["intended_parameters"], f"{source}: rust.intended_parameters"
    )
    _allowed_fields(intended, IMAGING_FIELDS, f"{source}: rust.intended_parameters")
    _string_list_allow_empty(
        rust["missing_capabilities"], f"{source}: rust.missing_capabilities"
    )
    requirement = command.get("evidence_storage")
    if requirement is not None:
        _validate_evidence_storage(requirement, source=f"{source}: evidence_storage")


def _validate_evidence_storage(value: Any, *, source: str) -> None:
    requirement = _require_dict(value, source)
    fields = {
        "schema_version",
        "kind",
        "policy_id",
        "required_root",
        "minimum_free_bytes",
        "forbidden_path_parts",
    }
    if set(requirement) != fields:
        raise ContractError(f"{source}: fields do not match protocol")
    _schema_version(requirement, 1, source)
    if requirement["kind"] != "imaging_evidence_storage_requirement":
        raise ContractError(f"{source}: kind is invalid")
    for key in ("policy_id", "required_root"):
        _nonempty_string(requirement, key, source)
    _positive_integer(
        requirement["minimum_free_bytes"], f"{source}: minimum_free_bytes"
    )
    _string_list_allow_empty(
        requirement["forbidden_path_parts"], f"{source}: forbidden_path_parts"
    )


def _validate_products(value: Any, *, source: str) -> None:
    products = _require_dict(value, source)
    _allowed_fields(products, PRODUCT_PATH_FIELDS, source)
    if not products:
        raise ContractError(f"{source}: products must not be empty")
    for key, item in products.items():
        _optional_string(item, f"{source}: {key}")


def _validate_logs(value: Any, *, source: str) -> None:
    logs = _require_dict(value, source)
    _allowed_fields(logs, LOG_FIELDS, source)
    if not logs:
        raise ContractError(f"{source}: logs must not be empty")
    for key, item in logs.items():
        _optional_string(item, f"{source}: {key}")
    digest = logs.get("benchmark_log_sha256")
    if digest is not None:
        _sha256_or_historical(digest, f"{source}: benchmark_log_sha256")


def _validate_human_review(value: Any, *, source: str) -> None:
    review = _require_dict(value, source)
    _allowed_fields(review, HUMAN_REVIEW_FIELDS, source)
    for key in (
        "status",
        "required_reviewer",
        "evidence_role",
        "panel_status",
        "structured_difference_label",
        "structured_difference_summary",
        "reason",
    ):
        if key in review:
            _nonempty_string(review, key, source)
    if "panel_reason" in review:
        _optional_string(review["panel_reason"], f"{source}: panel_reason")
    if "required_evidence_roles" in review:
        _string_list_allow_empty(
            review["required_evidence_roles"], f"{source}: required_evidence_roles"
        )
    if "requires_human_acceptance_before_done" in review and not isinstance(
        review["requires_human_acceptance_before_done"], bool
    ):
        raise ContractError(
            f"{source}: requires_human_acceptance_before_done must be boolean"
        )
    if "structured_difference_legend" in review:
        _require_dict(
            review["structured_difference_legend"],
            f"{source}: structured_difference_legend",
        )


def _validate_results(
    value: dict[str, Any],
    *,
    kind: str,
    migrated_environment: bool,
    source: str,
) -> None:
    allowed_by_kind = {
        "workload_run": WORKLOAD_RESULT_FIELDS,
        "image_comparison": {"product_comparison", "failure"},
        "alternating_comparison": {"alternating_comparison", "failure"},
        "simobserve_benchmark": {"simobserve", "failure"},
        "aca_simalma_benchmark": {"aca_simalma", "failure"},
    }
    _allowed_fields(value, set(allowed_by_kind[kind]), source)
    if not value or set(value) == {"failure"}:
        raise ContractError(f"{source}: {kind} evidence payload is empty")
    if "failure" in value:
        _validate_failure(value["failure"], source=f"{source}: failure")
    if kind == "image_comparison":
        comparison = value.get("product_comparison")
        if comparison is None:
            raise ContractError(
                f"{source}: image_comparison requires product_comparison"
            )
        _validate_comparison_output_parent(
            comparison,
            migrated_environment=migrated_environment,
            source=f"{source}: product_comparison",
        )
        return
    if kind == "alternating_comparison":
        details = value.get("alternating_comparison")
        if details is None:
            raise ContractError(f"{source}: alternating_comparison payload is required")
        _validate_alternating_comparison(
            details, source=f"{source}: alternating_comparison"
        )
        return
    if kind == "simobserve_benchmark":
        details = _require_dict(value.get("simobserve"), f"{source}: simobserve")
        _allowed_fields(
            details,
            {
                "dataset",
                "shape",
                "native_parallel",
                "native_serial",
                "native_fixed",
                "casa",
                "casa_oracle",
                "correctness",
                "oracle_comparison",
                "speedup_vs_casa",
                "performance_relative_to_casa",
                "native_performance",
                "analytic_tier_performance",
                "native_worker_comparison",
                "target",
            },
            f"{source}: simobserve",
        )
        return
    if kind == "aca_simalma_benchmark":
        details = _require_dict(value.get("aca_simalma"), f"{source}: aca_simalma")
        _allowed_fields(
            details,
            {
                "selected_scenarios",
                "targets",
                "inputs",
                "casa",
                "casa_field_overrides",
                "native",
                "comparisons",
                "closeout_gate",
            },
            f"{source}: aca_simalma",
        )
        return

    for key in ("rust", "casa"):
        if key in value:
            _validate_timing_result(value[key], source=f"{source}: {key}")
    if "stage_medians_ms" in value:
        stages = _require_dict(value["stage_medians_ms"], f"{source}: stage_medians_ms")
        _allowed_fields(stages, {"rust", "casa"}, f"{source}: stage_medians_ms")
        for implementation, medians in stages.items():
            _validate_number_map(
                medians, f"{source}: stage_medians_ms.{implementation}"
            )
    if "stage_breakdown" in value:
        _validate_stage_breakdown(
            value["stage_breakdown"], source=f"{source}: stage_breakdown"
        )
    if "product_paths" in value:
        paths = _require_dict(value["product_paths"], f"{source}: product_paths")
        _allowed_fields(
            paths,
            {"product_root", "rust_prefix", "casa_prefix", "execution_casa_prefix"},
            f"{source}: product_paths",
        )
        for key, item in paths.items():
            _optional_string(item, f"{source}: product_paths.{key}")
    if "benchmark_features" in value:
        _validate_benchmark_features(
            value["benchmark_features"], source=f"{source}: benchmark_features"
        )
    if "product_comparison" in value:
        _validate_comparison_output_parent(
            value["product_comparison"],
            migrated_environment=migrated_environment,
            source=f"{source}: product_comparison",
        )
    if "backend_plan_logs" in value:
        _validate_backend_plan_logs(
            value["backend_plan_logs"], source=f"{source}: backend_plan_logs"
        )
    if "casa_clean_control_diagnostics" in value:
        diagnostics = value["casa_clean_control_diagnostics"]
        if not isinstance(diagnostics, list) or not all(
            isinstance(item, dict) for item in diagnostics
        ):
            raise ContractError(
                f"{source}: casa_clean_control_diagnostics must be a list of objects"
            )
    if "casa_repeatability_comparison" in value:
        _validate_repeatability(
            value["casa_repeatability_comparison"],
            migrated_environment=migrated_environment,
            source=f"{source}: casa_repeatability_comparison",
        )
    if "casa_tclean_calls" in value:
        _validate_casa_call_groups(
            value["casa_tclean_calls"], source=f"{source}: casa_tclean_calls"
        )
    if "publication_recovery" in value:
        _validate_publication_recovery_record(
            value["publication_recovery"], source=f"{source}: publication_recovery"
        )
    if "bundle_integrity" in value:
        _validate_bundle_integrity(
            value["bundle_integrity"], source=f"{source}: bundle_integrity"
        )


def _validate_failure(value: Any, *, source: str) -> None:
    failure = _require_dict(value, source)
    _allowed_fields(failure, FAILURE_FIELDS, source)
    _nonempty_string(failure, "kind", source)
    _nonempty_string(failure, "reason", source)
    if "return_code" in failure:
        _integer(failure, "return_code", source)


def _validate_timing_result(value: Any, *, source: str) -> None:
    result = _require_dict(value, source)
    _allowed_fields(result, TIMING_RESULT_FIELDS, source)
    if "status" in result:
        _nonempty_string(result, "status", source)
    if "reason" in result:
        _optional_string(result["reason"], f"{source}: reason")
    timings = result.get("timings_seconds")
    if timings is None:
        raise ContractError(f"{source}: timings_seconds is required")
    timing = _require_dict(timings, f"{source}: timings_seconds")
    _allowed_fields(timing, TIMINGS_FIELDS, f"{source}: timings_seconds")
    if "runs" in timing:
        runs = timing["runs"]
        if not isinstance(runs, list):
            raise ContractError(f"{source}: timings_seconds.runs must be a list")
        for index, item in enumerate(runs):
            finite_number(
                item, field=f"{source}: timings_seconds.runs[{index}]", optional=False
            )
    if "median" in timing:
        finite_number(timing["median"], field=f"{source}: timings_seconds.median")
    if "warmup_count" in result:
        _optional_integer(
            result["warmup_count"], f"{source}: warmup_count", optional=False
        )
    if "cache_role" in result:
        _nonempty_string(result, "cache_role", source)
    if "evidence_summary" in result:
        _require_dict(result["evidence_summary"], f"{source}: evidence_summary")


def _validate_stage_breakdown(value: Any, *, source: str) -> None:
    breakdown = _require_dict(value, source)
    _allowed_fields(breakdown, STAGE_BREAKDOWN_FIELDS, source)
    _schema_version(breakdown, 1, source)
    for key in ("units", "instrumentation_scope"):
        _nonempty_string(breakdown, key, source)
    if "contract_review" in breakdown:
        _nonempty_string(breakdown, "contract_review", source)
    for implementation in ("rust", "casa"):
        entry = _require_dict(
            breakdown.get(implementation), f"{source}: {implementation}"
        )
        _allowed_fields(
            entry, STAGE_IMPLEMENTATION_FIELDS, f"{source}: {implementation}"
        )
        _nonempty_string(entry, "status", f"{source}: {implementation}")
        if "reason" in entry:
            _optional_string(entry["reason"], f"{source}: {implementation}.reason")
        categories = _require_dict(
            entry.get("categories"), f"{source}: {implementation}.categories"
        )
        for name, category_value in categories.items():
            if not isinstance(name, str) or not name:
                raise ContractError(
                    f"{source}: category names must be non-empty strings"
                )
            category = _require_dict(
                category_value, f"{source}: {implementation}.categories.{name}"
            )
            _allowed_fields(
                category,
                STAGE_CATEGORY_FIELDS,
                f"{source}: {implementation}.categories.{name}",
            )
            _nonempty_string(
                category, "status", f"{source}: {implementation}.categories.{name}"
            )
            _optional_string(
                category.get("reason"),
                f"{source}: {implementation}.categories.{name}.reason",
            )
            finite_number(
                category.get("total_ms"),
                field=f"{source}: {implementation}.categories.{name}.total_ms",
            )
            _validate_number_map(
                category.get("components_ms"),
                f"{source}: {implementation}.categories.{name}.components_ms",
            )
            _string_list_allow_empty(
                category.get("source_fields"),
                f"{source}: {implementation}.categories.{name}.source_fields",
            )
            if "missing_fields" in category:
                _string_list_allow_empty(
                    category["missing_fields"],
                    f"{source}: {implementation}.categories.{name}.missing_fields",
                )
            _nonempty_string(
                category, "description", f"{source}: {implementation}.categories.{name}"
            )


def _validate_comparison_output_parent(
    value: Any, *, migrated_environment: bool, source: str
) -> int | str:
    comparison = _require_dict(value, source)
    protocol_version = comparison.get("schema_version")
    if protocol_version is None:
        protocol_variant = _validate_unversioned_comparison_shape(
            comparison,
            migrated_environment=migrated_environment,
            source=source,
        )
    else:
        if isinstance(protocol_version, bool) or protocol_version not in {
            COMPARISON_SCHEMA_VERSION,
            *LEGACY_COMPARISON_SCHEMA_VERSIONS,
        }:
            raise ContractError(
                f"{source}: unsupported comparison schema_version {protocol_version!r}"
            )
        if (
            protocol_version in LEGACY_COMPARISON_SCHEMA_VERSIONS
            and not migrated_environment
        ):
            raise ContractError(
                f"{source}: legacy comparison schema_version {protocol_version} "
                "requires migrated environment provenance"
            )
        protocol_variant = protocol_version
        allowed_fields = set(COMPARISON_OUTPUT_FIELDS)
        if protocol_version in LEGACY_COMPARISON_SCHEMA_VERSIONS:
            allowed_fields.remove("structure_workspace_dir")
        _allowed_fields(comparison, allowed_fields, source)
        if "request_binding" not in comparison:
            raise ContractError(
                f"{source}: versioned comparison requires request_binding"
            )
    _nonempty_string(comparison, "status", source)
    if "reason" in comparison:
        _optional_string(comparison["reason"], f"{source}: reason")
    for key in ("max_elements_per_product", "full_chunk_elements"):
        if key in comparison:
            _positive_integer(comparison[key], f"{source}: {key}")
    for key in ("return_code",):
        if key in comparison:
            _optional_integer(comparison[key], f"{source}: {key}")
    for key in (
        "require_exact_product_inventory",
        "require_metadata_parity",
        "legacy_operand_aliases",
    ):
        if key in comparison and not isinstance(comparison[key], bool):
            raise ContractError(f"{source}: {key} must be boolean")
    if "requested_products" in comparison:
        _validate_product_suffix_list(
            comparison["requested_products"], f"{source}: requested_products"
        )
    if "source_regions" in comparison:
        _validate_comparison_source_regions(
            comparison["source_regions"],
            products=comparison.get("requested_products", []),
            source=f"{source}: source_regions",
        )
    if comparison.get("tolerances") is not None:
        _validate_frozen_tolerances(
            comparison["tolerances"], source=f"{source}: tolerances"
        )
    products = comparison.get("products")
    if not isinstance(products, dict):
        raise ContractError(f"{source}: products must be an object")
    for suffix, product in products.items():
        if not isinstance(suffix, str) or not suffix.startswith("."):
            raise ContractError(f"{source}: product keys must be suffix strings")
        _validate_comparison_product(
            product,
            protocol_variant=protocol_variant,
            source=f"{source}: products.{suffix}",
        )
    for key in (
        "input",
        "output",
        "log",
        "panel_dir",
        "left_prefix",
        "right_prefix",
        "retained_input",
        "retained_output",
        "retained_log",
        "retained_panel_dir",
        "retained_left_prefix",
        "retained_right_prefix",
    ):
        if key in comparison:
            _optional_string(comparison[key], f"{source}: {key}")
    for key in ("request_sha256", "input_sha256", "output_sha256", "log_sha256"):
        if comparison.get(key) is not None:
            _sha256_or_historical(comparison[key], f"{source}: {key}")
    if "beam_info" in comparison:
        _validate_comparison_beam_info(
            comparison["beam_info"],
            protocol_variant=protocol_variant,
            source=f"{source}: beam_info",
        )
    if "product_inventory" in comparison:
        _validate_comparison_inventory(
            comparison["product_inventory"], source=f"{source}: product_inventory"
        )
    if "structured_difference_review" in comparison:
        _validate_comparison_structured_review(
            comparison["structured_difference_review"],
            source=f"{source}: structured_difference_review",
        )
    if "tolerance_evaluation" in comparison:
        _validate_tolerance_evaluation(
            comparison["tolerance_evaluation"],
            source=f"{source}: tolerance_evaluation",
        )
    if "failure" in comparison:
        _validate_comparison_failure(comparison["failure"], source=f"{source}: failure")
    if "retained_artifacts" in comparison:
        retained = _require_dict(
            comparison["retained_artifacts"], f"{source}: retained_artifacts"
        )
        _allowed_fields(
            retained,
            COMPARISON_RETAINED_ARTIFACT_FIELDS,
            f"{source}: retained_artifacts",
        )
        for key in retained:
            _nonempty_string(retained, key, f"{source}: retained_artifacts")

    binding = comparison.get("request_binding")
    if binding is not None:
        _validate_bound_comparison_output(
            comparison,
            binding=binding,
            protocol_version=protocol_version,
            source=source,
        )
    return protocol_variant


def _validate_unversioned_comparison_shape(
    comparison: dict[str, Any], *, migrated_environment: bool, source: str
) -> str:
    if "request_binding" in comparison:
        raise ContractError(
            f"{source}: request_binding requires a comparison schema_version"
        )
    if "comparison_kind" in comparison:
        if not migrated_environment:
            raise ContractError(
                f"{source}: unversioned comparison protocol requires migrated "
                "environment provenance"
            )
        if set(comparison) != LEGACY_UNVERSIONED_COMPARISON_PROTOCOL_FIELDS:
            raise ContractError(
                f"{source}: unversioned protocol fields do not match the "
                "historical closed variant"
            )
        return LEGACY_UNVERSIONED_COMPARISON_VARIANT
    if "structured_difference_review" in comparison:
        if not migrated_environment:
            raise ContractError(
                f"{source}: unversioned completed comparison requires migrated "
                "environment provenance"
            )
        _allowed_fields(comparison, LEGACY_COMPARISON_SUMMARY_FIELDS, source)
        core_fields = {
            "status",
            "products",
            "beam_info",
            "structured_difference_review",
        }
        missing = sorted(core_fields - set(comparison))
        if missing:
            raise ContractError(
                f"{source}: legacy comparison summary missing field(s): "
                + ", ".join(missing)
            )
        artifact_fields = {"input", "log", "panel_dir"}
        present_artifact_fields = artifact_fields & set(comparison)
        if present_artifact_fields and present_artifact_fields != artifact_fields:
            raise ContractError(
                f"{source}: legacy comparison summary artifact fields must be "
                "present together"
            )
        if frozenset(comparison) not in {
            frozenset(core_fields),
            frozenset(core_fields | artifact_fields),
        }:
            raise ContractError(
                f"{source}: legacy comparison summary fields do not match a "
                "closed historical variant"
            )
        if comparison.get("status") != "completed":
            raise ContractError(
                f"{source}: legacy comparison summary status must be completed"
            )
        return LEGACY_UNVERSIONED_COMPARISON_VARIANT
    if set(comparison) != LEGACY_COMPARISON_FAILURE_SUMMARY_FIELDS:
        raise ContractError(
            f"{source}: unversioned comparison fields do not match a closed "
            "historical variant"
        )
    status = comparison.get("status")
    if status not in LIVE_UNVERSIONED_COMPARISON_STATUSES:
        raise ContractError(
            f"{source}: unversioned terminal comparison status must be one of "
            + ", ".join(sorted(LIVE_UNVERSIONED_COMPARISON_STATUSES))
        )
    _nonempty_string(comparison, "reason", source)
    if comparison.get("products") != {}:
        raise ContractError(
            f"{source}: unversioned terminal comparison products must be empty"
        )
    return (
        LEGACY_UNVERSIONED_COMPARISON_VARIANT
        if migrated_environment
        else LIVE_UNVERSIONED_COMPARISON_VARIANT
    )


def _validate_bound_comparison_output(
    comparison: dict[str, Any],
    *,
    binding: Any,
    protocol_version: Any,
    source: str,
) -> None:
    request = _require_dict(binding, f"{source}: request_binding")
    binding_fields = (
        COMPARISON_REQUEST_BINDING_FIELDS
        if protocol_version == COMPARISON_SCHEMA_VERSION
        else COMPARISON_REQUEST_BINDING_FIELDS - {"structure_workspace_dir"}
    )
    _allowed_fields(request, binding_fields, f"{source}: request_binding")
    if set(request) != binding_fields:
        missing = sorted(binding_fields - set(request))
        raise ContractError(
            f"{source}: request_binding missing field(s): {', '.join(missing)}"
        )
    if protocol_version is None:
        raise ContractError(f"{source}: bound comparison requires schema_version")
    _schema_version(request, protocol_version, f"{source}: request_binding")
    if request.get("mode") not in {"sampled", "full"}:
        raise ContractError(f"{source}: request_binding.mode must be sampled or full")
    for key in (
        "left_prefix",
        "right_prefix",
        "left_label",
        "right_label",
        "panel_dir",
    ):
        _nonempty_string(request, key, f"{source}: request_binding")
    if "structure_workspace_dir" in binding_fields:
        _nonempty_string(
            request, "structure_workspace_dir", f"{source}: request_binding"
        )
    _validate_product_suffix_list(
        request["products"], f"{source}: request_binding.products", nonempty=True
    )
    for key in ("max_elements_per_product", "full_chunk_elements"):
        _positive_integer(request[key], f"{source}: request_binding.{key}")
    for key in (
        "require_exact_product_inventory",
        "require_metadata_parity",
        "legacy_operand_aliases",
    ):
        if not isinstance(request[key], bool):
            raise ContractError(f"{source}: request_binding.{key} must be boolean")
    _validate_comparison_source_regions(
        request["source_regions"],
        products=request["products"],
        source=f"{source}: request_binding.source_regions",
    )
    if request["tolerances"] is not None:
        _validate_frozen_tolerances(
            request["tolerances"], source=f"{source}: request_binding.tolerances"
        )
    if comparison.get("request_sha256") is None:
        raise ContractError(f"{source}: bound comparison requires request_sha256")
    # Schema-v3 comparison artifacts remain durable historical evidence.  Their
    # closed field/type contract is validated above, but only the current
    # schema-v4 protocol has the derivational science validator used below.
    if protocol_version in LEGACY_COMPARISON_SCHEMA_VERSIONS:
        return
    if comparison_request_binding(request) != request:
        raise ContractError(f"{source}: request_binding has noncanonical fields")

    # Tolerance evaluation is layered over the raw comparison protocol and can
    # change only the outer status/reason.  Restore those two derived protocol
    # fields before asking the comparator's own fail-closed validator to bind
    # every other field to the embedded request.
    raw = dict(comparison)
    if isinstance(comparison.get("beam_info"), dict):
        raw_beam = dict(comparison["beam_info"])
        raw_beam.pop("retained_psf_path", None)
        raw["beam_info"] = raw_beam
    inventory = comparison.get("product_inventory", {})
    failures: list[str] = []
    if isinstance(inventory, dict) and inventory.get("status") == "mismatch":
        failures.append("exact product inventory differs")
    product_failures = [
        suffix
        for suffix in request["products"]
        if not isinstance(comparison.get("products", {}).get(suffix), dict)
        or comparison["products"][suffix].get("status") != "compared"
    ]
    if product_failures:
        failures.append("product comparison failed for " + ", ".join(product_failures))
    raw["status"] = "comparison_failed" if failures else "completed"
    raw["reason"] = "; ".join(failures) if failures else None
    try:
        validate_comparison_output(raw, request)
    except ValueError as error:
        raise ContractError(f"{source}: invalid bound comparison: {error}") from error


def _validate_comparison_product(
    value: Any, *, protocol_variant: int | str, source: str
) -> None:
    product = _require_dict(value, source)
    allowed_fields = (
        COMPARISON_PRODUCT_FIELDS
        if protocol_variant == COMPARISON_SCHEMA_VERSION
        else LEGACY_COMPARISON_PRODUCT_FIELDS
    )
    _allowed_fields(product, allowed_fields, source)
    _nonempty_string(product, "status", source)
    for key in (
        "rust_path",
        "casa_path",
        "left_path",
        "right_path",
        "retained_rust_path",
        "retained_casa_path",
        "retained_left_path",
        "retained_right_path",
        "left_label",
        "right_label",
        "comparison_mode",
    ):
        if key in product:
            _nonempty_string(product, key, source)
    for key in (
        "rust_exists",
        "casa_exists",
        "metadata_parity_required",
        "topology_parity",
    ):
        if key in product and not isinstance(product[key], bool):
            raise ContractError(f"{source}: {key} must be boolean")
    for key in ("sampled_elements", "finite_overlap"):
        if key in product:
            _nonnegative_integer(product[key], f"{source}: {key}")
    for key in ("shape", "sample_stride"):
        if key in product:
            _validate_nonnegative_integer_list(product[key], f"{source}: {key}")
    for key in (
        "rust_min",
        "rust_max",
        "rust_rms",
        "casa_min",
        "casa_max",
        "casa_rms",
        "left_min",
        "left_max",
        "left_rms",
        "right_min",
        "right_max",
        "right_rms",
        "diff_rms",
        "diff_abs_max",
        "diff_rms_over_casa_rms",
        "diff_abs_max_over_casa_peak",
        "diff_rms_over_right_rms",
        "diff_abs_max_over_right_peak",
        "correlation",
    ):
        if key in product:
            finite_number(product[key], field=f"{source}: {key}")
    for key in (
        "rust_peak_abs",
        "casa_peak_abs",
        "left_peak_abs",
        "right_peak_abs",
        "diff_peak_abs",
    ):
        if key in product:
            _validate_peak(product[key], source=f"{source}: {key}")
    if "metadata" in product:
        _validate_comparison_metadata(product["metadata"], source=f"{source}: metadata")
    if "review_panel" in product:
        _validate_comparison_review_panel(
            product["review_panel"], source=f"{source}: review_panel"
        )
    for key in ("structured_difference", "sampled_structured_difference"):
        if key in product:
            _validate_structured_difference(
                product[key],
                protocol_variant=protocol_variant,
                source=f"{source}: {key}",
            )
    if "full_array" in product:
        _require_dict(product["full_array"], f"{source}: full_array")


def _validate_comparison_review_panel(value: Any, *, source: str) -> None:
    panel = _require_dict(value, source)
    _allowed_fields(panel, COMPARISON_REVIEW_PANEL_FIELDS, source)
    _nonempty_string(panel, "status", source)
    for key in (
        "path",
        "retained_path",
        "left_label",
        "right_label",
        "display_description",
        "display_status",
        "display_transform",
        "structured_difference_label",
        "structured_difference_summary",
    ):
        if key in panel:
            _nonempty_string(panel, key, source)
    if panel.get("sha256") is not None:
        _sha256_or_historical(panel["sha256"], f"{source}: sha256")
    if "display_reason" in panel:
        _optional_string(panel["display_reason"], f"{source}: display_reason")
    for key in (
        "casa_rs_and_casa_color_limits",
        "left_and_right_color_limits",
        "difference_color_limits",
    ):
        if key in panel:
            _validate_number_list(panel[key], f"{source}: {key}")
    for key in ("display_sample_stride", "display_shape"):
        if key in panel:
            _validate_nonnegative_integer_list(panel[key], f"{source}: {key}")
    if panel.get("display_bounds") is not None:
        _validate_array_bounds(panel["display_bounds"], f"{source}: display_bounds")
    if "zoom_panel" in panel:
        zoom = _require_dict(panel["zoom_panel"], f"{source}: zoom_panel")
        _allowed_fields(zoom, COMPARISON_ZOOM_PANEL_FIELDS, f"{source}: zoom_panel")
        _nonempty_string(zoom, "status", f"{source}: zoom_panel")
        for key in ("path", "retained_path", "reason"):
            if key in zoom:
                _nonempty_string(zoom, key, f"{source}: zoom_panel")
        if "bounds" in zoom:
            _validate_zoom_bounds(zoom["bounds"], f"{source}: zoom_panel.bounds")
        for key in (
            "casa_rs_and_casa_color_limits",
            "difference_color_limits",
        ):
            if key in zoom:
                _validate_number_list(zoom[key], f"{source}: zoom_panel.{key}")


def _validate_comparison_beam_info(
    value: Any, *, protocol_variant: int | str, source: str
) -> None:
    beam = _require_dict(value, source)
    allowed_fields = (
        COMPARISON_BEAM_INFO_FIELDS
        if protocol_variant == COMPARISON_SCHEMA_VERSION
        else LEGACY_COMPARISON_BEAM_INFO_FIELDS
    )
    _allowed_fields(beam, allowed_fields, source)
    _nonempty_string(beam, "status", source)
    for key in (
        "psf_path",
        "retained_psf_path",
        "coordinate_domain",
        "estimation_method",
    ):
        if key in beam:
            _nonempty_string(beam, key, source)
    for key in ("peak_abs", "beam_area_pixels"):
        if key in beam:
            finite_number(beam[key], field=f"{source}: {key}", optional=False)
    if "beam_block_side_pixels" in beam:
        _positive_integer(
            beam["beam_block_side_pixels"], f"{source}: beam_block_side_pixels"
        )
    for key in ("sample_stride", "peak_location", "fwhm_pixels"):
        if key in beam:
            _validate_nonnegative_integer_list(beam[key], f"{source}: {key}")
    if "native_plane_coverage" in beam:
        coverage = _require_dict(
            beam["native_plane_coverage"], f"{source}: native_plane_coverage"
        )
        expected = {"coverage_complete", "expected_pixels", "pixels_visited"}
        if set(coverage) != expected:
            raise ContractError(
                f"{source}: native_plane_coverage fields do not match protocol"
            )
        if not isinstance(coverage["coverage_complete"], bool):
            raise ContractError(
                f"{source}: native_plane_coverage.coverage_complete must be boolean"
            )
        for key in ("expected_pixels", "pixels_visited"):
            _nonnegative_integer(
                coverage[key], f"{source}: native_plane_coverage.{key}"
            )


def _validate_comparison_inventory(value: Any, *, source: str) -> None:
    inventory = _require_dict(value, source)
    _allowed_fields(inventory, COMPARISON_INVENTORY_FIELDS, source)
    if set(inventory) != COMPARISON_INVENTORY_FIELDS:
        missing = sorted(COMPARISON_INVENTORY_FIELDS - set(inventory))
        raise ContractError(f"{source}: missing field(s): {', '.join(missing)}")
    _nonempty_string(inventory, "status", source)
    for key in ("required", "observed_match", "left_right_equal"):
        if not isinstance(inventory[key], bool):
            raise ContractError(f"{source}: {key} must be boolean")
    for key in (
        "expected",
        "left",
        "right",
        "left_missing",
        "left_extra",
        "right_missing",
        "right_extra",
    ):
        _validate_product_suffix_list(inventory[key], f"{source}: {key}")


def _validate_comparison_structured_review(value: Any, *, source: str) -> None:
    review = _require_dict(value, source)
    _allowed_fields(review, COMPARISON_STRUCTURED_REVIEW_FIELDS, source)
    for key in ("label", "summary"):
        _nonempty_string(review, key, source)
    for key in ("products", "product_summaries"):
        if key in review:
            _validate_string_map(review[key], f"{source}: {key}")
    for key in ("checks_by_product", "thresholds"):
        if key in review:
            _validate_nested_string_maps(review[key], f"{source}: {key}")
    if "legend" in review:
        _validate_string_map(review["legend"], f"{source}: legend")


def _validate_tolerance_evaluation(value: Any, *, source: str) -> None:
    evaluation = _require_dict(value, source)
    _allowed_fields(evaluation, COMPARISON_TOLERANCE_EVALUATION_FIELDS, source)
    if evaluation.get("contract_version") != 1 or isinstance(
        evaluation.get("contract_version"), bool
    ):
        raise ContractError(f"{source}: contract_version must be 1")
    _nonempty_string(evaluation, "status", source)
    for key in ("failed_checks", "incomplete_checks"):
        _string_list_allow_empty(evaluation.get(key), f"{source}: {key}")
    checks = evaluation.get("checks")
    if not isinstance(checks, list):
        raise ContractError(f"{source}: checks must be a list")
    for index, raw in enumerate(checks):
        label = f"{source}: checks[{index}]"
        check = _require_dict(raw, label)
        _allowed_fields(check, COMPARISON_TOLERANCE_CHECK_FIELDS, label)
        for key in ("name", "status"):
            _nonempty_string(check, key, label)
        if "reason" in check:
            _optional_string(check["reason"], f"{label}: reason")
        for key in ("actual", "ceiling"):
            if key not in check:
                raise ContractError(f"{label}: {key} is required")


def _validate_comparison_failure(value: Any, *, source: str) -> None:
    failure = _require_dict(value, source)
    _allowed_fields(failure, {"kind", "reason"}, source)
    _nonempty_string(failure, "kind", source)
    _nonempty_string(failure, "reason", source)


def _validate_comparison_metadata(value: Any, *, source: str) -> None:
    metadata = _require_dict(value, source)
    _allowed_fields(metadata, COMPARISON_METADATA_FIELDS, source)
    _nonempty_string(metadata, "status", source)
    if "parity" in metadata and not isinstance(metadata["parity"], bool):
        raise ContractError(f"{source}: parity must be boolean")
    if "field_parity" in metadata:
        parity = _require_dict(metadata["field_parity"], f"{source}: field_parity")
        allowed = {"shape", "unit", "masks", "coordinates", "restoring_beam"}
        _allowed_fields(parity, allowed, f"{source}: field_parity")
        for key, item in parity.items():
            if not isinstance(item, bool):
                raise ContractError(f"{source}: field_parity.{key} must be boolean")
    for side in ("left", "right"):
        if side not in metadata:
            continue
        details = _require_dict(metadata[side], f"{source}: {side}")
        _allowed_fields(details, COMPARISON_METADATA_SIDE_FIELDS, f"{source}: {side}")
        _nonempty_string(details, "status", f"{source}: {side}")
        if "shape" in details:
            _validate_nonnegative_integer_list(
                details["shape"], f"{source}: {side}.shape"
            )
        if "unit" in details:
            _optional_string(details["unit"], f"{source}: {side}.unit")
        for key in ("masks", "errors"):
            if key in details and not isinstance(details[key], list):
                raise ContractError(f"{source}: {side}.{key} must be a list")
        for key in ("coordinates", "restoring_beam"):
            if key in details and details[key] is not None:
                _require_dict(details[key], f"{source}: {side}.{key}")


def _validate_structured_difference(
    value: Any, *, protocol_variant: int | str, source: str
) -> None:
    structured = _require_dict(value, source)
    allowed_fields = (
        COMPARISON_STRUCTURED_DIFFERENCE_FIELDS
        if protocol_variant == COMPARISON_SCHEMA_VERSION
        else LEGACY_COMPARISON_STRUCTURED_DIFFERENCE_FIELDS
    )
    _allowed_fields(structured, allowed_fields, source)
    _nonempty_string(structured, "status", source)
    if "evidence_scope" in structured:
        _nonempty_string(structured, "evidence_scope", source)
    for key in ("analysis_pixels", "masked_pixels", "beam_block_side_pixels"):
        if key in structured:
            _nonnegative_integer(structured[key], f"{source}: {key}")
    for key in (
        "diff_rms",
        "normalized_diff_rms",
        "block_rms_decay_slope_vs_independent_beams",
        "low_order_r2_quadratic",
    ):
        if key in structured:
            finite_number(structured[key], field=f"{source}: {key}")
    if "normalization" in structured:
        normalization = _require_dict(
            structured["normalization"], f"{source}: normalization"
        )
        _allowed_fields(normalization, {"type", "value"}, f"{source}: normalization")
        _nonempty_string(normalization, "type", f"{source}: normalization")
        finite_number(
            normalization.get("value"),
            field=f"{source}: normalization.value",
            optional=False,
        )
    if "mask" in structured:
        mask = _require_dict(structured["mask"], f"{source}: mask")
        _allowed_fields(
            mask,
            {"type", "product_family", "threshold", "threshold_fraction_of_peak"},
            f"{source}: mask",
        )
        _nonempty_string(mask, "type", f"{source}: mask")
        if "product_family" in mask:
            _nonempty_string(mask, "product_family", f"{source}: mask")
        for key in ("threshold", "threshold_fraction_of_peak"):
            if key in mask:
                finite_number(mask[key], field=f"{source}: mask.{key}", optional=False)
    if "beam_info" in structured:
        _validate_comparison_beam_info(
            structured["beam_info"],
            protocol_variant=protocol_variant,
            source=f"{source}: beam_info",
        )
    if "beam_block_rms_by_scale" in structured:
        blocks = structured["beam_block_rms_by_scale"]
        if not isinstance(blocks, list):
            raise ContractError(f"{source}: beam_block_rms_by_scale must be a list")
        for index, raw in enumerate(blocks):
            label = f"{source}: beam_block_rms_by_scale[{index}]"
            block = _require_dict(raw, label)
            _allowed_fields(block, COMPARISON_STRUCTURE_BLOCK_FIELDS, label)
            for key in ("block_side_pixels", "n_blocks"):
                if key in block:
                    _nonnegative_integer(block[key], f"{label}: {key}")
            for key in set(block) - {"block_side_pixels", "n_blocks"}:
                finite_number(block[key], field=f"{label}: {key}")
    if structured.get("large_scale_power_fraction") is not None:
        large_scale = _require_dict(
            structured["large_scale_power_fraction"],
            f"{source}: large_scale_power_fraction",
        )
        allowed = {
            "fraction",
            "frequency_cutoff_cycles_per_pixel",
            "min_wavelength_beams",
        }
        _allowed_fields(large_scale, allowed, f"{source}: large_scale_power_fraction")
        for key, item in large_scale.items():
            finite_number(
                item,
                field=f"{source}: large_scale_power_fraction.{key}",
                optional=False,
            )
    if "scale_offset_gradient_fit" in structured:
        _validate_structure_fit(
            structured["scale_offset_gradient_fit"],
            source=f"{source}: scale_offset_gradient_fit",
        )
    if "native_spatial_evidence" in structured:
        _validate_native_spatial_evidence(
            structured["native_spatial_evidence"],
            source=f"{source}: native_spatial_evidence",
        )
    if "classification" in structured:
        _validate_structure_classification(
            structured["classification"], source=f"{source}: classification"
        )
    if "review" in structured:
        _validate_structure_product_review(
            structured["review"], source=f"{source}: review"
        )


def _validate_structure_fit(value: Any, *, source: str) -> None:
    fit = _require_dict(value, source)
    _allowed_fields(fit, COMPARISON_STRUCTURE_FIT_FIELDS, source)
    _nonempty_string(fit, "status", source)
    for key in ("reason",):
        if key in fit:
            _optional_string(fit[key], f"{source}: {key}")
    if "model" in fit:
        _nonempty_string(fit, "model", source)
    if "coefficients" in fit:
        coefficients = _require_dict(fit["coefficients"], f"{source}: coefficients")
        _allowed_fields(
            coefficients,
            {"scale", "offset", "dx_pixels", "dy_pixels"},
            f"{source}: coefficients",
        )
        for key, item in coefficients.items():
            finite_number(item, field=f"{source}: coefficients.{key}", optional=False)
    for key in ("r2", "diff_rms", "residual_rms"):
        if key in fit:
            finite_number(fit[key], field=f"{source}: {key}")
    for key in ("fit_pixels", "masked_pixels", "excluded_nonfinite_basis_pixels"):
        if key in fit:
            _nonnegative_integer(fit[key], f"{source}: {key}")


def _validate_native_spatial_evidence(value: Any, *, source: str) -> None:
    evidence = _require_dict(value, source)
    allowed = {
        "method",
        "source_shape",
        "storage",
        "array_count",
        "temporary_bytes",
        "spatial_pixels_visited",
        "covered_pixels",
        "expected_pixels",
        "overlap_write_pixels",
        "coverage_complete",
        "write_chunks",
        "structure_value_domain",
        "left_raw_finite_pixels",
        "right_raw_finite_pixels",
        "paired_raw_finite_pixels",
        "paired_image_mask_finite_pixels",
        "central_mask_mismatch_pixels",
        "workspace_lifecycle",
    }
    _allowed_fields(evidence, allowed, source)
    for key in (
        "method",
        "storage",
        "structure_value_domain",
        "workspace_lifecycle",
    ):
        if key in evidence:
            _nonempty_string(evidence, key, source)
    if "source_shape" in evidence:
        _validate_nonnegative_integer_list(
            evidence["source_shape"], f"{source}: source_shape"
        )
    for key in allowed - {
        "method",
        "source_shape",
        "storage",
        "coverage_complete",
        "structure_value_domain",
        "workspace_lifecycle",
    }:
        if key in evidence:
            _nonnegative_integer(evidence[key], f"{source}: {key}")
    if "coverage_complete" in evidence and not isinstance(
        evidence["coverage_complete"], bool
    ):
        raise ContractError(f"{source}: coverage_complete must be boolean")


def _validate_structure_classification(value: Any, *, source: str) -> None:
    classification = _require_dict(value, source)
    _allowed_fields(classification, COMPARISON_CLASSIFICATION_FIELDS, source)
    for key in ("amplitude", "structure", "overall"):
        _nonempty_string(classification, key, source)
    if "structure_suppressed_by_numerical_floor" in classification and not isinstance(
        classification["structure_suppressed_by_numerical_floor"], bool
    ):
        raise ContractError(
            f"{source}: structure_suppressed_by_numerical_floor must be boolean"
        )
    if "structure_components" in classification:
        _validate_string_map(
            classification["structure_components"],
            f"{source}: structure_components",
        )
    if "thresholds" in classification:
        _validate_nested_string_maps(
            classification["thresholds"], f"{source}: thresholds"
        )


def _validate_structure_product_review(value: Any, *, source: str) -> None:
    review = _require_dict(value, source)
    _allowed_fields(review, {"checks", "label", "summary", "legend"}, source)
    for key in ("label", "summary"):
        _nonempty_string(review, key, source)
    _validate_string_map(review.get("legend"), f"{source}: legend")
    checks = review.get("checks")
    if not isinstance(checks, list):
        raise ContractError(f"{source}: checks must be a list")
    for index, raw in enumerate(checks):
        label = f"{source}: checks[{index}]"
        check = _require_dict(raw, label)
        _allowed_fields(check, {"name", "label", "meaning", "value"}, label)
        for key in ("name", "label", "meaning"):
            _nonempty_string(check, key, label)
        if not isinstance(check.get("value"), bool):
            finite_number(check.get("value"), field=f"{label}: value")


def _validate_benchmark_features(value: Any, *, source: str) -> None:
    features = _require_dict(value, source)
    _allowed_fields(features, BENCHMARK_FEATURE_FIELDS, source)
    if "schema_version" in features:
        _schema_version(features, 1, source)

    visibility = _require_dict(features.get("visibility", {}), f"{source}: visibility")
    _allowed_fields(visibility, BENCHMARK_VISIBILITY_FIELDS, f"{source}: visibility")
    integer_fields = {
        "selected_rows",
        "selected_channels",
        "correlations",
        "visibility_work",
        "gridded_samples",
    }
    for key in integer_fields & set(visibility):
        _optional_integer(visibility[key], f"{source}: visibility.{key}")
    if "correlation_source" in visibility:
        _nonempty_string(visibility, "correlation_source", f"{source}: visibility")
    for key in ("flagged_fraction", "source_stream_throughput_samples_per_s"):
        if key in visibility:
            finite_number(visibility[key], field=f"{source}: visibility.{key}")

    image = _require_dict(features.get("image", {}), f"{source}: image")
    _allowed_fields(image, BENCHMARK_IMAGE_FIELDS, f"{source}: image")
    for key in BENCHMARK_IMAGE_FIELDS & set(image):
        _optional_integer(image[key], f"{source}: image.{key}")

    mode_cost = _require_dict(features.get("mode_cost", {}), f"{source}: mode_cost")
    _allowed_fields(mode_cost, BENCHMARK_MODE_COST_FIELDS, f"{source}: mode_cost")
    for key in {"specmode", "gridder", "deconvolver", "weighting"} & set(mode_cost):
        _nonempty_string(mode_cost, key, f"{source}: mode_cost")
    for key in {"niter"} & set(mode_cost):
        _optional_integer(mode_cost[key], f"{source}: mode_cost.{key}", optional=False)
    for key in {
        "actual_major_cycles",
        "actual_minor_iterations",
        "multiscale_scale_count",
        "mtmfs_nterms",
        "mosaic_field_count",
    } & set(mode_cost):
        _optional_integer(mode_cost[key], f"{source}: mode_cost.{key}")
    for key in {"cycleniter", "wprojplanes"} & set(mode_cost):
        _optional_string(mode_cost[key], f"{source}: mode_cost.{key}")

    for key in ("resources", "backend"):
        if key in features:
            _require_dict(features[key], f"{source}: {key}")


def _validate_alternating_comparison(value: Any, *, source: str) -> None:
    details = _require_dict(value, source)
    allowed = {
        "comparison_id",
        "configuration",
        "order",
        "schedule",
        "runs",
        "measured_summaries",
        "paired_deltas",
        "paired_delta_summary",
        "adjacent_pair_deltas",
        "adjacent_pair_delta_summary",
        "verdict",
        "report_path",
        "error",
    }
    _allowed_fields(details, allowed, source)
    required = {"configuration", "order", "schedule", "runs", "verdict"}
    missing = sorted(required - set(details))
    if missing:
        raise ContractError(f"{source}: missing field(s): {', '.join(missing)}")
    _validate_alternating_configuration(
        details["configuration"], source=f"{source}: configuration"
    )
    _validate_alternating_order(details["order"], source=f"{source}: order")
    _validate_alternating_schedule(details["schedule"], source=f"{source}: schedule")
    _validate_alternating_runs(details["runs"], source=f"{source}: runs")
    if "measured_summaries" in details:
        _require_dict(details["measured_summaries"], f"{source}: measured_summaries")
    for key in ("paired_deltas", "adjacent_pair_deltas"):
        if key in details:
            _validate_alternating_deltas(
                details[key], source=f"{source}: {key}", paired=key == "paired_deltas"
            )
    for key in ("paired_delta_summary", "adjacent_pair_delta_summary"):
        if key in details:
            _validate_alternating_delta_summary(details[key], source=f"{source}: {key}")
    _validate_alternating_verdict(details["verdict"], source=f"{source}: verdict")
    if "report_path" in details:
        _nonempty_string(details, "report_path", source)
    if "comparison_id" in details:
        _nonempty_string(details, "comparison_id", source)
    if "error" in details:
        _optional_string(details["error"], f"{source}: error")


def _validate_alternating_configuration(value: Any, *, source: str) -> None:
    configuration = _require_dict(value, source)
    fields = {
        "baseline_workload",
        "candidate_workload",
        "warmup_pair_count",
        "measured_pair_count",
        "slowdown_tolerance_fraction",
        "output_root",
        "artifact_root",
        "comparison_artifact_root",
        "run_workload_options",
        "baseline_imaging_overrides",
        "candidate_imaging_overrides",
        "run_workload_repeats",
    }
    _allowed_fields(configuration, fields, source)
    required = fields - {
        "baseline_imaging_overrides",
        "candidate_imaging_overrides",
    }
    missing = sorted(required - set(configuration))
    if missing:
        raise ContractError(f"{source}: missing field(s): {', '.join(missing)}")
    for key in (
        "baseline_workload",
        "candidate_workload",
        "output_root",
        "artifact_root",
        "comparison_artifact_root",
    ):
        _nonempty_string(configuration, key, source)
    for key in ("warmup_pair_count", "measured_pair_count", "run_workload_repeats"):
        _optional_integer(configuration[key], f"{source}: {key}", optional=False)
    finite_number(
        configuration["slowdown_tolerance_fraction"],
        field=f"{source}: slowdown_tolerance_fraction",
        optional=False,
    )
    for key in (
        "run_workload_options",
        "baseline_imaging_overrides",
        "candidate_imaging_overrides",
    ):
        if key in configuration:
            _string_list_allow_empty(configuration[key], f"{source}: {key}")


def _validate_alternating_order(value: Any, *, source: str) -> None:
    order = _require_dict(value, source)
    if set(order) != {"warmup", "measured"}:
        raise ContractError(f"{source}: fields must be exactly warmup and measured")
    for key in ("warmup", "measured"):
        roles = _string_list_allow_empty(order[key], f"{source}: {key}")
        if any(role not in {"baseline", "candidate"} for role in roles):
            raise ContractError(f"{source}: {key} contains an invalid role")


def _validate_alternating_schedule(value: Any, *, source: str) -> None:
    if not isinstance(value, list):
        raise ContractError(f"{source} must be a list")
    for index, item in enumerate(value):
        _validate_alternating_schedule_item(item, source=f"{source}[{index}]")


def _validate_alternating_schedule_item(value: Any, *, source: str) -> None:
    item = _require_dict(value, source)
    fields = {
        "sequence_index",
        "phase",
        "block_index",
        "position_in_block",
        "role",
        "workload",
    }
    if set(item) != fields:
        raise ContractError(f"{source}: fields do not match protocol")
    for key in ("sequence_index", "block_index", "position_in_block"):
        _positive_integer(item[key], f"{source}: {key}")
    if item["phase"] not in {"warmup", "measured"}:
        raise ContractError(f"{source}: phase is invalid")
    if item["role"] not in {"baseline", "candidate"}:
        raise ContractError(f"{source}: role is invalid")
    _nonempty_string(item, "workload", source)


def _validate_alternating_runs(value: Any, *, source: str) -> None:
    if not isinstance(value, list):
        raise ContractError(f"{source} must be a list")
    schedule_fields = {
        "sequence_index",
        "phase",
        "block_index",
        "position_in_block",
        "role",
        "workload",
    }
    required = schedule_fields | {
        "command",
        "result_status",
        "result_path",
        "recorded_paths",
        "total_wall_seconds",
        "stage_timings_ms",
        "backend_identity",
    }
    allowed = required | {"error"}
    for index, run_value in enumerate(value):
        label = f"{source}[{index}]"
        run = _require_dict(run_value, label)
        _allowed_fields(run, allowed, label)
        missing = sorted(required - set(run))
        if missing:
            raise ContractError(f"{label}: missing field(s): {', '.join(missing)}")
        _validate_alternating_schedule_item(
            {key: run[key] for key in schedule_fields}, source=label
        )
        _nonempty_string_list(run["command"], f"{label}: command")
        _nonempty_string(run, "result_status", label)
        _optional_string(run["result_path"], f"{label}: result_path")
        recorded_paths = run["recorded_paths"]
        if not isinstance(recorded_paths, list):
            raise ContractError(f"{label}: recorded_paths must be a list")
        for path_index, path_value in enumerate(recorded_paths):
            path_label = f"{label}: recorded_paths[{path_index}]"
            path = _require_dict(path_value, path_label)
            if set(path) != {"field", "path"}:
                raise ContractError(f"{path_label}: fields do not match protocol")
            _nonempty_string(path, "field", path_label)
            _nonempty_string(path, "path", path_label)
        finite_number(run["total_wall_seconds"], field=f"{label}: total_wall_seconds")
        stage_timings = _require_dict(
            run["stage_timings_ms"], f"{label}: stage_timings_ms"
        )
        for implementation, stages in stage_timings.items():
            if not isinstance(implementation, str) or not implementation:
                raise ContractError(
                    f"{label}: stage timing implementation names must be strings"
                )
            _validate_number_map(stages, f"{label}: stage_timings_ms.{implementation}")
        backend_identity = run["backend_identity"]
        if backend_identity is not None:
            _require_dict(backend_identity, f"{label}: backend_identity")
        if "error" in run:
            _nonempty_string(run, "error", label)


def _validate_alternating_deltas(value: Any, *, source: str, paired: bool) -> None:
    if not isinstance(value, list):
        raise ContractError(f"{source} must be a list")
    fields = {
        "block_index",
        "pair_index",
        "order",
        "baseline_seconds",
        "candidate_seconds",
        "delta_seconds",
        "relative_delta",
    }
    for index, item_value in enumerate(value):
        label = f"{source}[{index}]"
        item = _require_dict(item_value, label)
        if set(item) != fields:
            raise ContractError(f"{label}: fields do not match protocol")
        _positive_integer(item["block_index"], f"{label}: block_index")
        pair_index = item["pair_index"]
        if paired:
            if pair_index is not None:
                raise ContractError(f"{label}: pair_index must be null")
        else:
            _positive_integer(pair_index, f"{label}: pair_index")
        _nonempty_string(item, "order", label)
        for key in (
            "baseline_seconds",
            "candidate_seconds",
            "delta_seconds",
        ):
            finite_number(item[key], field=f"{label}: {key}", optional=False)
        finite_number(item["relative_delta"], field=f"{label}: relative_delta")


def _validate_alternating_delta_summary(value: Any, *, source: str) -> None:
    summary = _require_dict(value, source)
    if set(summary) != {"delta_seconds", "relative_delta"}:
        raise ContractError(f"{source}: fields do not match protocol")
    for key in ("delta_seconds", "relative_delta"):
        _validate_robust_summary(summary[key], source=f"{source}: {key}")


def _validate_robust_summary(value: Any, *, source: str) -> None:
    summary = _require_dict(value, source)
    fields = {"count", "median", "mad", "q1", "q3", "iqr", "minimum", "maximum"}
    if set(summary) != fields:
        raise ContractError(f"{source}: fields do not match protocol")
    _optional_integer(summary["count"], f"{source}: count", optional=False)
    for key in fields - {"count"}:
        finite_number(summary[key], field=f"{source}: {key}")


def _validate_alternating_verdict(value: Any, *, source: str) -> None:
    verdict = _require_dict(value, source)
    fields = {
        "status",
        "no_slowdown",
        "tolerance_fraction",
        "observed_median_relative_delta",
        "basis",
        "reason",
    }
    if set(verdict) != fields:
        raise ContractError(f"{source}: fields do not match protocol")
    status = _nonempty_string(verdict, "status", source)
    if status not in {"pass", "fail", "inconclusive"}:
        raise ContractError(f"{source}: status is invalid")
    if verdict["no_slowdown"] is not None and not isinstance(
        verdict["no_slowdown"], bool
    ):
        raise ContractError(f"{source}: no_slowdown must be boolean or null")
    finite_number(
        verdict["tolerance_fraction"],
        field=f"{source}: tolerance_fraction",
        optional=False,
    )
    finite_number(
        verdict["observed_median_relative_delta"],
        field=f"{source}: observed_median_relative_delta",
    )
    for key in ("basis", "reason"):
        _nonempty_string(verdict, key, source)


def _validate_backend_plan_logs(value: Any, *, source: str) -> None:
    logs = _require_dict(value, source)
    bucket_names = {
        "single_plane_execution_plan",
        "standard_mfs_runtime_plan",
        "source_stream_memory_plan",
        "imaging_source_read_ahead",
        "standard_mfs_source_read_ahead",
        "dirty_product_fft",
        "dirty_product_gpu_resident",
        "dirty_product_gpu_resident_fallback",
        "source_stream_consumer",
        "frontend_progress",
        "profile_runs",
        "spectral_slab_events",
        "spectral_slab_memory",
        "spectral_slab_plans",
        "mosaic_cube_slab_plans",
        "mosaic_cube_slab_planes",
        "mosaic_cube_slab_executor_summaries",
        "cube_per_plane_backend",
        "cube_resident_clean_control",
        "cube_resident_clean_executor",
        "cube_resident_clean_finish_planes",
        "cube_resident_clean_stage",
        "cube_source_row_blocks",
        "cube_product_summaries",
        "image_product_writes",
        "cube_plane_state_store",
        "visibility_geometry_cache",
        "executor_limitations",
        "worker_diagnostics",
        "minor_cycle_diagnostics",
        "hogbom_minor_cycle_diagnostics",
        "clark_minor_cycle_diagnostics",
        "multiscale_minor_cycle_diagnostics",
        "clean_residual_refresh_diagnostics",
        "metal_diagnostics",
    }
    _allowed_fields(
        logs, bucket_names | {"schema_version", "summary", "collection_stats"}, source
    )
    _schema_version(logs, 1, source)
    _require_dict(logs.get("summary"), f"{source}: summary")
    for name in bucket_names & set(logs):
        entries = logs[name]
        if not isinstance(entries, list):
            raise ContractError(f"{source}: {name} must be a list")
        for index, entry_value in enumerate(entries):
            entry = _require_dict(entry_value, f"{source}: {name}[{index}]")
            _allowed_fields(
                entry, {"name", "raw", "fields"}, f"{source}: {name}[{index}]"
            )
            if set(entry) != {"name", "raw", "fields"}:
                raise ContractError(
                    f"{source}: {name}[{index}] fields do not match protocol"
                )
            _nonempty_string(entry, "name", f"{source}: {name}[{index}]")
            _nonempty_string(entry, "raw", f"{source}: {name}[{index}]")
            _require_dict(entry["fields"], f"{source}: {name}[{index}].fields")
    if "collection_stats" in logs:
        stats = _require_dict(logs["collection_stats"], f"{source}: collection_stats")
        _allowed_fields(stats, bucket_names, f"{source}: collection_stats")
        for name, record_value in stats.items():
            record = _require_dict(record_value, f"{source}: collection_stats.{name}")
            expected = {"observed_count", "retained_count", "truncated"}
            if set(record) != expected:
                raise ContractError(
                    f"{source}: collection_stats.{name} fields do not match protocol"
                )
            for key in ("observed_count", "retained_count"):
                _optional_integer(
                    record[key],
                    f"{source}: collection_stats.{name}.{key}",
                    optional=False,
                )
            if not isinstance(record["truncated"], bool):
                raise ContractError(
                    f"{source}: collection_stats.{name}.truncated must be boolean"
                )


def _validate_repeatability(
    value: Any, *, migrated_environment: bool, source: str
) -> None:
    comparison = _require_dict(value, source)
    allowed = {
        "baseline_call",
        "compared_calls",
        "comparison_kind",
        "comparison_mode",
        "comparisons",
        "product_inventory",
        "products",
        "reason",
        "source_regions",
        "status",
        "structured_difference_review",
        "tolerances",
    }
    _allowed_fields(comparison, allowed, source)
    _nonempty_string(comparison, "status", source)
    for key in ("baseline_call", "comparison_kind", "comparison_mode"):
        if key in comparison:
            _nonempty_string(comparison, key, source)
    if "reason" in comparison:
        _optional_string(comparison["reason"], f"{source}: reason")
    if "compared_calls" in comparison:
        _string_list_allow_empty(
            comparison["compared_calls"], f"{source}: compared_calls"
        )
    if "source_regions" in comparison:
        products = comparison.get("products", {})
        _validate_comparison_source_regions(
            comparison["source_regions"],
            products=list(products) if isinstance(products, dict) else [],
            source=f"{source}: source_regions",
        )
    protocol_variants: list[int | str] = []
    if "comparisons" in comparison:
        comparisons = comparison["comparisons"]
        if not isinstance(comparisons, list):
            raise ContractError(f"{source}: comparisons must be a list")
        for index, item in enumerate(comparisons):
            protocol_variants.append(
                _validate_comparison_output_parent(
                    item,
                    migrated_environment=migrated_environment,
                    source=f"{source}: comparisons[{index}]",
                )
            )
    if len(set(protocol_variants)) > 1:
        raise ContractError(
            f"{source}: repeatability comparisons must use one comparison protocol "
            "variant"
        )
    protocol_variant: int | str = (
        protocol_variants[0]
        if protocol_variants
        else (
            LEGACY_UNVERSIONED_COMPARISON_VARIANT
            if migrated_environment
            else LIVE_UNVERSIONED_COMPARISON_VARIANT
        )
    )
    if "products" in comparison:
        products = _require_dict(comparison["products"], f"{source}: products")
        if products and not protocol_variants:
            raise ContractError(
                f"{source}: aggregate products require bound comparison evidence"
            )
        for suffix, product in products.items():
            if not isinstance(suffix, str) or not suffix.startswith("."):
                raise ContractError(f"{source}: product keys must be suffix strings")
            _validate_comparison_product(
                product,
                protocol_variant=protocol_variant,
                source=f"{source}: products.{suffix}",
            )
    if comparison.get("product_inventory") is not None:
        _validate_comparison_inventory(
            comparison["product_inventory"], source=f"{source}: product_inventory"
        )
    if comparison.get("structured_difference_review") is not None:
        review = _require_dict(
            comparison["structured_difference_review"],
            f"{source}: structured_difference_review",
        )
        _allowed_fields(
            review,
            {"label", "summary"},
            f"{source}: structured_difference_review",
        )
        for key in ("label", "summary"):
            _nonempty_string(review, key, f"{source}: structured_difference_review")
    if comparison.get("tolerances") is not None:
        _validate_frozen_tolerances(
            comparison["tolerances"], source=f"{source}: tolerances"
        )


def _validate_casa_call_groups(value: Any, *, source: str) -> None:
    groups = _require_dict(value, source)
    _allowed_fields(groups, {"warmups", "measured", "partial"}, source)
    if not groups:
        raise ContractError(f"{source}: CASA call groups must not be empty")
    for group, records in groups.items():
        if not isinstance(records, list):
            raise ContractError(f"{source}: {group} must be a list of objects")
        for index, record in enumerate(records):
            _validate_casa_call_record(
                record, group=group, source=f"{source}: {group}[{index}]"
            )


def _validate_casa_call_record(value: Any, *, group: str, source: str) -> None:
    record = _require_dict(value, source)
    _allowed_fields(record, CASA_CALL_FIELDS, source)
    for key in (
        "name",
        "role",
        "prefix",
        "request_path",
        "result_path",
        "stdout_stderr_path",
    ):
        if key in record:
            _nonempty_string(record, key, source)
    for key in (
        "retained_prefix",
        "retained_request_path",
        "retained_result_path",
        "retained_stdout_stderr_path",
    ):
        if key in record:
            _optional_string(record[key], f"{source}: {key}")
    if record.get("role") not in {None, "none", "cold", "warm"}:
        raise ContractError(f"{source}: role must be none, cold, or warm")
    if "measured" in record:
        if not isinstance(record["measured"], bool):
            raise ContractError(f"{source}: measured must be boolean")
        if group == "warmups" and record["measured"]:
            raise ContractError(f"{source}: warmup calls cannot be measured")
        if group == "measured" and not record["measured"]:
            raise ContractError(f"{source}: measured calls must set measured=true")
    if "exit_code" in record:
        _optional_integer(record["exit_code"], f"{source}: exit_code", optional=False)
    for key in (
        "request_sha256",
        "result_sha256",
        "stdout_stderr_sha256",
        "cache_receipt_sha256",
    ):
        if record.get(key) is not None:
            _sha256_or_historical(record[key], f"{source}: {key}")
    for key in ("casa_log_paths", "retained_casa_log_paths"):
        if key in record:
            _string_list_allow_empty(record[key], f"{source}: {key}")
    for key in ("casa_log_identities", "retained_casa_log_identities"):
        if key in record:
            identities = record[key]
            if not isinstance(identities, list):
                raise ContractError(f"{source}: {key} must be a list")
            for index, raw in enumerate(identities):
                label = f"{source}: {key}[{index}]"
                identity = _require_dict(raw, label)
                _allowed_fields(identity, CASA_CALL_IDENTITY_FIELDS, label)
                if set(identity) != CASA_CALL_IDENTITY_FIELDS:
                    raise ContractError(f"{label}: path and sha256 are required")
                _nonempty_string(identity, "path", label)
                _sha256_or_historical(identity["sha256"], f"{label}: sha256")
    if "result" in record:
        _validate_embedded_casa_result(record["result"], source=f"{source}: result")


def _validate_embedded_casa_result(value: Any, *, source: str) -> None:
    result = _require_dict(value, source)
    if result.get("schema_version") == LEGACY_CASA_TCLEAN_RESULT_SCHEMA_VERSION:
        _validate_legacy_embedded_casa_result(result, source=source)
        return
    if "schema_version" in result or "kind" in result:
        try:
            validate_result_envelope(result)
        except ProtocolError as error:
            raise ContractError(
                f"{source}: invalid CASA protocol result: {error}"
            ) from error
        return

    # Tests and interrupted publication recovery can retain a deliberately
    # reduced summary when the subprocess envelope was never completed.  Keep
    # that variant closed rather than accepting arbitrary nested dictionaries.
    _allowed_fields(result, {"status", "wall_seconds", "casa", "cache"}, source)
    _nonempty_string(result, "status", source)
    if "wall_seconds" in result:
        finite_number(
            result["wall_seconds"], field=f"{source}: wall_seconds", optional=False
        )
    if "casa" in result:
        casa = _require_dict(result["casa"], f"{source}: casa")
        _allowed_fields(casa, {"publication_recovery"}, f"{source}: casa")
        if "publication_recovery" in casa:
            recovery = _require_dict(
                casa["publication_recovery"], f"{source}: casa.publication_recovery"
            )
            _allowed_fields(
                recovery,
                {"status", "tclean_reinvoked", "exact_request_replay_required"},
                f"{source}: casa.publication_recovery",
            )
            _nonempty_string(recovery, "status", f"{source}: casa.publication_recovery")
            for key in ("tclean_reinvoked", "exact_request_replay_required"):
                if key in recovery and not isinstance(recovery[key], bool):
                    raise ContractError(
                        f"{source}: casa.publication_recovery.{key} must be boolean"
                    )
    if "cache" in result:
        cache = _require_dict(result["cache"], f"{source}: cache")
        _allowed_fields(cache, {"path", "receipt_path", "after"}, f"{source}: cache")
        for key in ("path", "receipt_path"):
            if key in cache:
                _nonempty_string(cache, key, f"{source}: cache")
        if "after" in cache:
            after = _require_dict(cache["after"], f"{source}: cache.after")
            _allowed_fields(after, {"role", "inventory"}, f"{source}: cache.after")
            if "role" in after:
                _nonempty_string(after, "role", f"{source}: cache.after")
            if "inventory" in after:
                inventory = _require_dict(
                    after["inventory"], f"{source}: cache.after.inventory"
                )
                _allowed_fields(
                    inventory,
                    {"stable_tree_sha256"},
                    f"{source}: cache.after.inventory",
                )
                if inventory.get("stable_tree_sha256") is not None:
                    _sha256_or_historical(
                        inventory["stable_tree_sha256"],
                        f"{source}: cache.after.inventory.stable_tree_sha256",
                    )


def _validate_legacy_embedded_casa_result(
    result: dict[str, Any], *, source: str
) -> None:
    """Validate the schema-v2 CASA envelope retained by interrupted VLASS runs.

    Schema v3 added stage-level timings and a richer resource receipt.  Failed
    publication attempts can still contain the complete, checksummed v2
    subprocess result, so keep that historical variant readable as a closed
    protocol rather than weakening validation of current v3 results.
    """

    common_fields = {"schema_version", "kind", "status", "request_id"}
    plan_fields = common_fields | {
        "action",
        "casa",
        "recipe",
        "compatibility_normalizations",
        "version_defaults",
        "reproducibility_overrides",
        "effective_kwargs",
        "effective_kwargs_sha256",
        "cache",
        "mask_identity",
    }
    execution_fields = plan_fields | {
        "wall_seconds",
        "resources",
        "products",
        "tclean_return",
    }
    allowed_fields = CASA_TCLEAN_RESULT_FIELDS - {"stage_timings_seconds"}
    _allowed_fields(result, allowed_fields, source)
    _schema_version(result, LEGACY_CASA_TCLEAN_RESULT_SCHEMA_VERSION, source)
    if result.get("kind") != CASA_TCLEAN_RESULT_KIND:
        raise ContractError(f"{source}: kind must be {CASA_TCLEAN_RESULT_KIND!r}")
    status = _nonempty_string(result, "status", source)
    if status not in CASA_TCLEAN_RESULT_STATUSES:
        raise ContractError(f"{source}: status is invalid")
    _nonempty_string(result, "request_id", source)

    if status == "planned":
        expected_fields = plan_fields
    elif status in {"completed", "recovered_publication"}:
        expected_fields = execution_fields
    elif status.startswith("failed"):
        minimal_failure_fields = common_fields | {"failure"}
        expected_fields = (
            minimal_failure_fields
            if set(result) == minimal_failure_fields
            else execution_fields | {"failure"}
        )
    else:  # Defensive even if CASA_TCLEAN_RESULT_STATUSES grows later.
        raise ContractError(f"{source}: status has no legacy envelope shape")
    if set(result) != expected_fields:
        missing = sorted(expected_fields - set(result))
        extra = sorted(set(result) - expected_fields)
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if extra:
            details.append("unexpected " + ", ".join(extra))
        raise ContractError(f"{source}: legacy envelope fields: {'; '.join(details)}")

    if "effective_kwargs" in result:
        effective_kwargs = _require_dict(
            result["effective_kwargs"], f"{source}: effective_kwargs"
        )
        digest = _sha256_or_historical(
            result["effective_kwargs_sha256"],
            f"{source}: effective_kwargs_sha256",
        )
        try:
            expected_digest = canonical_sha256(effective_kwargs)
        except ProtocolError as error:
            raise ContractError(
                f"{source}: effective_kwargs are not canonical JSON: {error}"
            ) from error
        if digest != expected_digest:
            raise ContractError(
                f"{source}: effective_kwargs_sha256 does not match effective_kwargs"
            )

    for key in (
        "casa",
        "recipe",
        "version_defaults",
        "reproducibility_overrides",
        "cache",
    ):
        if key in result:
            _require_dict(result[key], f"{source}: {key}")
    if "compatibility_normalizations" in result:
        normalizations = result["compatibility_normalizations"]
        if not isinstance(normalizations, list) or not all(
            isinstance(item, dict) for item in normalizations
        ):
            raise ContractError(
                f"{source}: compatibility_normalizations must be a list of objects"
            )
    if result.get("mask_identity") is not None and not isinstance(
        result["mask_identity"], dict
    ):
        raise ContractError(f"{source}: mask_identity must be an object or null")
    if "wall_seconds" in result:
        wall_seconds = finite_number(
            result["wall_seconds"], field=f"{source}: wall_seconds", optional=False
        )
        if wall_seconds is not None and wall_seconds < 0.0:
            raise ContractError(f"{source}: wall_seconds must be nonnegative")
        _validate_legacy_casa_resources(
            result["resources"], source=f"{source}: resources"
        )
        products = _require_dict(result["products"], f"{source}: products")
        if set(products) != {"before", "after"}:
            raise ContractError(f"{source}: products fields must be before and after")
        for phase in ("before", "after"):
            values = products[phase]
            if not isinstance(values, list) or not all(
                isinstance(item, dict) for item in values
            ):
                raise ContractError(
                    f"{source}: products.{phase} must be a list of objects"
                )
        tclean_return = _require_dict(
            result["tclean_return"], f"{source}: tclean_return"
        )
        if set(tclean_return) != {"present", "type"}:
            raise ContractError(
                f"{source}: tclean_return fields must be present and type"
            )
        if not isinstance(tclean_return["present"], bool):
            raise ContractError(f"{source}: tclean_return.present must be boolean")
        _nonempty_string(tclean_return, "type", f"{source}: tclean_return")
    if "failure" in result:
        failure = _require_dict(result["failure"], f"{source}: failure")
        if set(failure) != {"kind", "reason", "exception_type"}:
            raise ContractError(
                f"{source}: failure fields must be kind, reason, and exception_type"
            )
        for key in ("kind", "reason", "exception_type"):
            _nonempty_string(failure, key, f"{source}: failure")


def _validate_legacy_casa_resources(value: Any, *, source: str) -> None:
    resources = _require_dict(value, source)
    if set(resources) != {"before", "after", "delta"}:
        raise ContractError(f"{source}: fields must be before, after, and delta")
    snapshots: dict[str, dict[str, Any]] = {}
    for phase in ("before", "after", "delta"):
        snapshot = _require_dict(resources[phase], f"{source}: {phase}")
        if set(snapshot) != LEGACY_CASA_RESOURCE_FIELDS:
            raise ContractError(
                f"{source}: {phase} fields do not match the schema-v2 resource set"
            )
        snapshots[phase] = snapshot
        for key, item in snapshot.items():
            if key in {"user_cpu_seconds", "system_cpu_seconds"}:
                number = finite_number(
                    item, field=f"{source}: {phase}.{key}", optional=False
                )
                if number is not None and number < 0.0:
                    raise ContractError(f"{source}: {phase}.{key} must be nonnegative")
            else:
                _nonnegative_integer(item, f"{source}: {phase}.{key}")

    if snapshots["after"]["peak_rss_bytes"] <= 0:
        raise ContractError(f"{source}: after.peak_rss_bytes must be positive")
    for key in LEGACY_CASA_RESOURCE_FIELDS:
        before = snapshots["before"][key]
        after = snapshots["after"][key]
        delta = snapshots["delta"][key]
        if key == "peak_rss_bytes":
            if after < before or delta != after:
                raise ContractError(
                    f"{source}: delta.peak_rss_bytes must report the observed peak"
                )
            continue
        if after < before:
            raise ContractError(f"{source}: resource counter decreased for {key}")
        expected = after - before
        matches = (
            math.isclose(delta, expected, rel_tol=0.0, abs_tol=1e-12)
            if isinstance(expected, float)
            else delta == expected
        )
        if not matches:
            raise ContractError(f"{source}: delta.{key} does not match after-before")


def _validate_publication_recovery_record(value: Any, *, source: str) -> None:
    recovery = _require_dict(value, source)
    allowed = {
        "kind",
        "status",
        "protocol_status",
        "call_name",
        "call_phase",
        "benchmark_eligible",
        "timing_accepted",
        "tclean_reinvoked",
        "exact_request_replay_required",
        "cache_path",
        "cache_receipt_path",
        "cache_receipt_sha256",
        "stable_tree_sha256",
    }
    _allowed_fields(recovery, allowed, source)
    for key in ("kind", "status", "protocol_status"):
        _nonempty_string(recovery, key, source)
    for key in ("call_name", "call_phase", "cache_path", "cache_receipt_path"):
        if key in recovery:
            _optional_string(recovery[key], f"{source}: {key}")
    for key in ("cache_receipt_sha256", "stable_tree_sha256"):
        if recovery.get(key) is not None:
            _sha256_or_historical(recovery[key], f"{source}: {key}")
    for key in (
        "benchmark_eligible",
        "timing_accepted",
        "tclean_reinvoked",
        "exact_request_replay_required",
    ):
        if key in recovery and not isinstance(recovery[key], bool):
            raise ContractError(f"{source}: {key} must be boolean")


def _validate_bundle_integrity(value: Any, *, source: str) -> None:
    integrity = _require_dict(value, source)
    status = _nonempty_string(integrity, "status", source)
    if status == "failed":
        failure_fields = {"status", "validator_version", "reason"}
        if set(integrity) != failure_fields:
            raise ContractError(f"{source}: failed fields do not match protocol")
        _optional_integer(
            integrity["validator_version"],
            f"{source}: validator_version",
            optional=False,
        )
        _nonempty_string(integrity, "reason", source)
        return
    if status != "passed":
        raise ContractError(f"{source}: status must be passed or failed")
    success_fields = {
        "status",
        "validator_version",
        "volatile_tree_exclusions",
        "call_count",
        "product_tree_count",
        "comparison_count",
        "written_panel_count",
        "cache_tree_count",
    }
    if set(integrity) != success_fields:
        raise ContractError(f"{source}: fields do not match protocol")
    for key in success_fields - {"status", "volatile_tree_exclusions"}:
        _optional_integer(integrity[key], f"{source}: {key}", optional=False)
    _string_list_allow_empty(
        integrity["volatile_tree_exclusions"],
        f"{source}: volatile_tree_exclusions",
    )


def _utc_timestamp(value: dict[str, Any], key: str, source: str) -> str:
    timestamp = _nonempty_string(value, key, source)
    if (
        re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.][0-9]+)?Z",
            timestamp,
        )
        is None
    ):
        raise ContractError(f"{source}: {key} must be a UTC RFC3339 timestamp")
    return timestamp


def _require_dict(value: Any, source: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ContractError(f"{source} must be an object")
    return value


def _optional_string(value: Any, source: str) -> None:
    if value is not None and not isinstance(value, str):
        raise ContractError(f"{source} must be a string or null")


def _optional_integer(value: Any, source: str, *, optional: bool = True) -> None:
    if value is None and optional:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        suffix = " or null" if optional else ""
        raise ContractError(f"{source} must be an integer{suffix}")


def _positive_integer(value: Any, source: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ContractError(f"{source} must be a positive integer")
    return value


def _nonnegative_integer(value: Any, source: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ContractError(f"{source} must be a nonnegative integer")
    return value


def _validate_product_suffix_list(
    value: Any, source: str, *, nonempty: bool = False
) -> list[str]:
    products = _string_list_allow_empty(value, source)
    if nonempty and not products:
        raise ContractError(f"{source} must not be empty")
    if len(products) != len(set(products)) or any(
        not suffix.startswith(".") for suffix in products
    ):
        raise ContractError(f"{source} must contain unique product suffixes")
    return products


def _validate_comparison_source_regions(
    value: Any, *, products: list[str], source: str
) -> None:
    if not isinstance(value, list):
        raise ContractError(f"{source} must be a list")
    seen: set[str] = set()
    known_products = set(products)
    for index, raw in enumerate(value):
        label = f"{source}[{index}]"
        region = _require_dict(raw, label)
        expected = {"id", "products", "blc", "trc"}
        if set(region) != expected:
            raise ContractError(f"{label} fields must be id, products, blc, and trc")
        region_id = _nonempty_string(region, "id", label)
        if region_id in seen:
            raise ContractError(f"{label}: id must be unique")
        seen.add(region_id)
        suffixes = _validate_product_suffix_list(
            region["products"], f"{label}: products", nonempty=True
        )
        if known_products and not set(suffixes) <= known_products:
            raise ContractError(
                f"{label}: products are outside the comparison inventory"
            )
        for key in ("blc", "trc"):
            corner = region[key]
            if (
                not isinstance(corner, list)
                or len(corner) != 2
                or any(
                    isinstance(item, bool) or not isinstance(item, int) or item < 0
                    for item in corner
                )
            ):
                raise ContractError(f"{label}: {key} must be nonnegative [x, y]")
        if any(left > right for left, right in zip(region["blc"], region["trc"])):
            raise ContractError(f"{label}: blc must not exceed trc")


def _validate_frozen_tolerances(value: Any, *, source: str) -> None:
    try:
        validate_tolerance_contract(_require_dict(value, source))
    except ToleranceContractError as error:
        raise ContractError(f"{source}: {error}") from error


def _validate_number_list(value: Any, source: str) -> None:
    if not isinstance(value, list):
        raise ContractError(f"{source} must be a list")
    for index, item in enumerate(value):
        finite_number(item, field=f"{source}[{index}]", optional=False)


def _validate_peak(value: Any, *, source: str) -> None:
    peak = _require_dict(value, source)
    if set(peak) != {"location", "value", "abs_value"}:
        raise ContractError(f"{source} fields must be location, value, and abs_value")
    _validate_nonnegative_integer_list(peak["location"], f"{source}: location")
    for key in ("value", "abs_value"):
        finite_number(peak[key], field=f"{source}: {key}", optional=False)


def _validate_nonnegative_integer_list(value: Any, source: str) -> None:
    if not isinstance(value, list):
        raise ContractError(f"{source} must be a list")
    for index, item in enumerate(value):
        _nonnegative_integer(item, f"{source}[{index}]")


def _validate_array_bounds(value: Any, source: str) -> None:
    bounds = _require_dict(value, source)
    if set(bounds) != {"blc", "trc", "inc"}:
        raise ContractError(f"{source} fields must be blc, trc, and inc")
    lengths: set[int] = set()
    for key in ("blc", "trc", "inc"):
        _validate_nonnegative_integer_list(bounds[key], f"{source}: {key}")
        lengths.add(len(bounds[key]))
    if len(lengths) != 1:
        raise ContractError(f"{source} vectors must have equal length")


def _validate_zoom_bounds(value: Any, source: str) -> None:
    bounds = _require_dict(value, source)
    expected = {"x_start", "x_end", "y_start", "y_end"}
    if set(bounds) != expected:
        raise ContractError(
            f"{source} fields must be x_start, x_end, y_start, and y_end"
        )
    for key in expected:
        _nonnegative_integer(bounds[key], f"{source}: {key}")
    if bounds["x_start"] > bounds["x_end"] or bounds["y_start"] > bounds["y_end"]:
        raise ContractError(f"{source} starts must not exceed ends")


def _validate_nested_string_maps(value: Any, source: str) -> None:
    mapping = _require_dict(value, source)
    for key, nested in mapping.items():
        if not isinstance(key, str) or not key:
            raise ContractError(f"{source} keys must be non-empty strings")
        _validate_string_map(nested, f"{source}.{key}")


def _validate_string_map(value: Any, source: str) -> None:
    mapping = _require_dict(value, source)
    if not all(
        isinstance(key, str) and key and isinstance(item, str)
        for key, item in mapping.items()
    ):
        raise ContractError(f"{source} must contain string keys and values")


def _validate_number_map(value: Any, source: str) -> None:
    mapping = _require_dict(value, source)
    for key, item in mapping.items():
        if not isinstance(key, str) or not key:
            raise ContractError(f"{source} keys must be non-empty strings")
        finite_number(item, field=f"{source}.{key}", optional=False)


def _string_list_allow_empty(value: Any, source: str) -> list[str]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise ContractError(f"{source} must be a string list")
    return value


def _nonempty_string_list(value: Any, source: str) -> list[str]:
    items = _string_list_allow_empty(value, source)
    if not items:
        raise ContractError(f"{source} must be a non-empty string list")
    return items


def _sha256_or_historical(value: Any, source: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ContractError(f"{source} must be a lowercase SHA-256 digest")
    return value


def finite_number(
    value: Any, *, field: str = "value", optional: bool = True
) -> float | None:
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


def _string(value: dict[str, Any], key: str, source: str) -> str:
    item = value.get(key)
    if not isinstance(item, str):
        raise ContractError(f"{source}: {key} must be a string")
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
        "facets",
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
        "computepastep",
        "pointingoffsetsigdev",
        "rotatepastep",
        "smallscalebias",
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
    booleans = {
        "aterm",
        "calcpsf",
        "calcres",
        "conjbeams",
        "interactive",
        "mosweight",
        "parallel",
        "pbcor",
        "perchanweightdensity",
        "psterm",
        "restart",
        "restoration",
        "usepointing",
        "wbawp",
        "write_pb",
    }
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
    string_fields = set(imaging) - integers - numbers - booleans - special
    for key in string_fields:
        if key in {"psfphasecenter", "vptable"}:
            _string(imaging, key, f"{source}: imaging")
        else:
            _nonempty_string(imaging, key, f"{source}: imaging")
    if "phasecenter_field" in imaging and imaging["phasecenter_field"] is not None:
        _integer(imaging, "phasecenter_field", f"{source}: imaging")
    if "scales" in imaging:
        scales = imaging["scales"]
        if not isinstance(scales, str) and not (
            isinstance(scales, list)
            and all(
                isinstance(item, int) and not isinstance(item, bool) for item in scales
            )
        ):
            raise ContractError(
                f"{source}: imaging.scales must be a string or integer list"
            )
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
    for key in ("profile_repeats", "repeats", "warmups"):
        if key in run:
            _integer(run, key, f"{source}: run")
    if "stream_log" in run and not isinstance(run["stream_log"], bool):
        raise ContractError(f"{source}: run.stream_log must be a boolean")
    if "env" in run:
        env = run["env"]
        if not isinstance(env, dict) or not all(
            isinstance(key, str) and isinstance(item, str) for key, item in env.items()
        ):
            raise ContractError(
                f"{source}: run.env must contain string keys and values"
            )
    if "cf_cache_role" in run:
        role = _nonempty_string(run, "cf_cache_role", f"{source}: run")
        if role not in {"none", "cold", "warm"}:
            raise ContractError(
                f"{source}: run.cf_cache_role must be none, cold, or warm"
            )
    for key in set(run) - {
        "cf_cache_role",
        "profile_repeats",
        "repeats",
        "stream_log",
        "warmups",
        "env",
    }:
        _nonempty_string(run, key, f"{source}: run")


def _validate_cross_fields(
    *,
    imaging: dict[str, Any],
    run: dict[str, Any],
    comparison: dict[str, Any],
    source: str,
) -> None:
    warmups = run.get("warmups", 0)
    if warmups < 0:
        raise ContractError(f"{source}: run.warmups must be >= 0")
    cache_role = run.get("cf_cache_role", "none")
    if cache_role == "warm" and warmups < 1:
        raise ContractError(
            f"{source}: run.cf_cache_role=warm requires run.warmups >= 1"
        )
    if cache_role in {"none", "cold"} and warmups != 0:
        raise ContractError(
            f"{source}: run.cf_cache_role={cache_role} requires run.warmups=0"
        )

    aw_control_fields = {"aterm", "psterm", "wbawp", "conjbeams"}
    aw_surface_requested = bool(aw_control_fields & set(imaging))
    if aw_surface_requested:
        missing = sorted(aw_control_fields - set(imaging))
        if missing:
            raise ContractError(
                f"{source}: explicit AW imaging requires fields: {', '.join(missing)}"
            )
        aw_gridders = {"awp2", "awphpg", "awproject"}
        gridder = imaging.get("gridder")
        casa_gridder = imaging.get("casa_gridder", gridder)
        if gridder not in aw_gridders:
            raise ContractError(f"{source}: explicit AW controls require an AW gridder")
        if casa_gridder not in aw_gridders:
            raise ContractError(
                f"{source}: explicit AW controls cannot downgrade casa_gridder to W-projection"
            )
        if imaging.get("wterm") != "wproject":
            raise ContractError(
                f"{source}: explicit AW controls require imaging.wterm=wproject"
            )
        wprojplanes = imaging.get("wprojplanes")
        if (
            isinstance(wprojplanes, bool)
            or not isinstance(wprojplanes, int)
            or wprojplanes < 1
        ):
            raise ContractError(
                f"{source}: explicit AW controls require imaging.wprojplanes >= 1"
            )
        facets = imaging.get("facets")
        if isinstance(facets, bool) or not isinstance(facets, int) or facets < 1:
            raise ContractError(
                f"{source}: explicit AW controls require imaging.facets >= 1"
            )
        for key in ("computepastep", "rotatepastep", "pointingoffsetsigdev"):
            if key not in imaging:
                raise ContractError(
                    f"{source}: explicit AW controls require imaging.{key}"
                )
        if imaging["wbawp"] and not imaging["aterm"]:
            raise ContractError(
                f"{source}: imaging.wbawp=true requires imaging.aterm=true"
            )
        if imaging["conjbeams"] and not imaging["aterm"]:
            raise ContractError(
                f"{source}: imaging.conjbeams=true requires imaging.aterm=true"
            )
        if cache_role == "none":
            raise ContractError(
                f"{source}: explicit AW controls require run.cf_cache_role=cold or warm"
            )

    mask_image = imaging.get("mask_image")
    mask_sha256 = imaging.get("mask_sha256")
    if bool(mask_image) != bool(mask_sha256):
        raise ContractError(
            f"{source}: imaging.mask_image and imaging.mask_sha256 must be set together"
        )
    if mask_sha256 and re.fullmatch(r"[0-9a-f]{64}", mask_sha256) is None:
        raise ContractError(
            f"{source}: imaging.mask_sha256 must be a lowercase SHA-256 digest"
        )
    if mask_image and imaging.get("usemask") != "user":
        raise ContractError(
            f"{source}: imaging.mask_image requires imaging.usemask=user"
        )
    deterministic_clean = (
        imaging.get("mode") == "clean"
        and imaging.get("niter", 0) > 0
        and imaging.get("usemask") == "user"
    )
    if deterministic_clean and not mask_image:
        raise ContractError(
            f"{source}: deterministic user-mask clean requires imaging.mask_image and imaging.mask_sha256"
        )

    comparison_mode = comparison.get("mode", "sampled")
    if comparison_mode == "full":
        if comparison.get("full_chunk_elements", 0) < 1:
            raise ContractError(
                f"{source}: comparison.mode=full requires comparison.full_chunk_elements >= 1"
            )
        if comparison.get("require_exact_product_inventory") is not True:
            raise ContractError(
                f"{source}: comparison.mode=full requires require_exact_product_inventory=true"
            )
        if comparison.get("require_metadata_parity") is not True:
            raise ContractError(
                f"{source}: comparison.mode=full requires require_metadata_parity=true"
            )
        if "products" not in comparison:
            raise ContractError(
                f"{source}: comparison.mode=full requires an explicit product inventory"
            )
        if "tolerances" not in comparison:
            raise ContractError(
                f"{source}: comparison.mode=full requires frozen tolerances"
            )
        if comparison["tolerances"].get("require_full_array") is not True:
            raise ContractError(
                f"{source}: full comparison tolerances must require_full_array=true"
            )
    if comparison.get("source_regions") and comparison_mode != "full":
        raise ContractError(
            f"{source}: comparison.source_regions require comparison.mode=full"
        )
    imsize = imaging.get("imsize")
    if isinstance(imsize, int) and not isinstance(imsize, bool):
        for region in comparison.get("source_regions", []):
            if any(value >= imsize for value in region["trc"]):
                raise ContractError(
                    f"{source}: source region {region['id']!r} exceeds imsize={imsize}"
                )
    tolerances = comparison.get("tolerances")
    if isinstance(tolerances, dict):
        thresholds = [tolerances.get("default", {})]
        products = tolerances.get("products", {})
        if isinstance(products, dict):
            thresholds.extend(products.values())
        source_metrics = {
            "centroid_pixels",
            "integrated_flux_relative",
            "peak_relative",
        }
        if any(
            isinstance(value, dict) and source_metrics & set(value)
            for value in thresholds
        ) and not comparison.get("source_regions"):
            raise ContractError(
                f"{source}: source-local tolerances require frozen source_regions"
            )


def _validate_source_regions(comparison: dict[str, Any], *, source: str) -> None:
    regions = comparison.get("source_regions")
    if not isinstance(regions, list) or not regions:
        raise ContractError(
            f"{source}: comparison.source_regions must be a nonempty list"
        )
    known_products = set(comparison.get("products", []))
    seen: set[str] = set()
    for index, region in enumerate(regions):
        label = f"{source}: comparison.source_regions[{index}]"
        if not isinstance(region, dict) or set(region) != {
            "id",
            "products",
            "blc",
            "trc",
        }:
            raise ContractError(f"{label} fields must be id, products, blc, and trc")
        region_id = _nonempty_string(region, "id", label)
        if region_id in seen:
            raise ContractError(f"{label}.id must be unique")
        seen.add(region_id)
        suffixes = _string_list(region, "products", label)
        if any(not suffix.startswith(".") for suffix in suffixes):
            raise ContractError(f"{label}.products must contain product suffixes")
        unknown_products = sorted(set(suffixes) - known_products)
        if unknown_products:
            raise ContractError(
                f"{label}.products are not in comparison.products: "
                + ", ".join(unknown_products)
            )
        for corner in ("blc", "trc"):
            values = region.get(corner)
            if (
                not isinstance(values, list)
                or len(values) != 2
                or any(
                    isinstance(value, bool) or not isinstance(value, int) or value < 0
                    for value in values
                )
            ):
                raise ContractError(f"{label}.{corner} must be nonnegative [x, y]")
        if any(start > end for start, end in zip(region["blc"], region["trc"])):
            raise ContractError(f"{label}.blc must not exceed trc")


def _integer(value: dict[str, Any], key: str, source: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int):
        raise ContractError(f"{source}: {key} must be an integer")
    return item


def _string_list(value: dict[str, Any], key: str, source: str) -> list[str]:
    item = value.get(key)
    if (
        not isinstance(item, list)
        or not item
        or not all(isinstance(entry, str) and entry for entry in item)
    ):
        raise ContractError(f"{source}: {key} must be a non-empty string list")
    return item
