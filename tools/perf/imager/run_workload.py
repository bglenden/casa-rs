#!/usr/bin/env python3
"""Run manifest-driven CASA C++ versus casa-rs imaging benchmarks."""

from __future__ import annotations

import argparse
from collections import Counter
import datetime as dt
import json
import os
import pathlib
import re
import statistics
import subprocess
import sys
import uuid
import math
from typing import Any

import perf_paths


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
WORKLOAD_DIR = pathlib.Path(__file__).resolve().parent / "workloads"
BENCH_SCRIPT = REPO_ROOT / "scripts" / "bench-imager-vs-casa.sh"
SUPPORTED_GRIDDER_VALUES = {
    "awp2",
    "awphpg",
    "awproject",
    "mosaic",
    "standard",
    "widefield",
    "wproject",
}
RUNNABLE_GRIDDER_VALUES = {
    "awp2",
    "awphpg",
    "awproject",
    "mosaic",
    "standard",
    "widefield",
    "wproject",
}
SUPPORTED_SPEC_MODES = {"cubedata", "cube", "mfs"}
RUNNABLE_SPEC_MODES = {"cubedata", "cube", "mfs"}
SUPPORTED_BENCH_MODES = {"dirty", "clean"}
SUPPORTED_INTERPOLATION = {"nearest", "linear"}
SUPPORTED_HOGBOM_ITERATION_MODES = {"strict", "casa", "casa-inclusive", "casa_inclusive"}
SUPPORTED_MS_STAGING = {"copy", "direct"}
SUPPORTED_BOOLEAN_FLAGS = {"0", "1", "false", "true", "no", "yes", "off", "on"}
STRING_IMAGING_OVERRIDE_KEYS = {"start", "width"}
DEFAULT_COMPARISON_PRODUCTS = [".image", ".residual", ".psf"]
STRUCTURED_DIFFERENCE_REVIEW_LEGEND = {
    "good": "No review action expected from this check.",
    "investigate": "Plausible but needs review in context.",
    "bad": "Structured or large enough difference; do not close without explanation.",
    "unknown": "Check could not be evaluated for this product.",
}
RUST_STAGE_FIELDS = {
    "open_measurement_set",
    "prepare_plane_input",
    "get_ms_values_into_processing_buffer",
    "prepare_processing_buffer",
    "extract_phase_center",
    "run_imaging",
    "build_coordinate_system",
    "write_products",
    "io_time",
    "frontend_total",
    "controller_overhead",
    "weighting",
    "executor_build",
    "planned_sample_replay",
    "grid_update",
    "psf_grid",
    "psf_fft",
    "psf_normalize",
    "model_fft",
    "residual_degrid_grid",
    "residual_fft",
    "residual_normalize",
    "clean_cycle_setup",
    "deconvolver_setup",
    "major_cycle_refresh",
    "residual_refresh_overhead",
    "multiscale_scale_refresh",
    "minor_cycle",
    "minor_cycle_solve",
    "beam_fit",
    "restore",
    "total",
}
RUST_STAGE_FIELD_ALIASES = {
    "prepared_source_read": "get_ms_values_into_processing_buffer",
    "prepared_source_prepare": "prepare_processing_buffer",
}
CASA_STAGE_FIELDS = {
    "parameter_setup",
    "construct_imager",
    "initialize_imagers",
    "select_data",
    "define_image",
    "normalizer_info",
    "cf_cache_setup",
    "initialize_normalizers",
    "set_weighting",
    "set_weighting_core",
    "initialize_deconvolvers",
    "estimate_memory",
    "initialize_iteration_control",
    "make_psf",
    "make_pb",
    "calcres_major_cycle",
    "update_mask",
    "has_converged",
    "minor_cycle",
    "clean_major_cycle",
    "restore_images",
    "delete_tools",
    "total",
}


class HarnessError(Exception):
    """Error that should be shown without a Python traceback."""


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workload", help="workload manifest id or JSON path")
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=perf_paths.artifact_path("imager", "runs"),
        help="directory for result JSON and benchmark log",
    )
    parser.add_argument(
        "--artifact-root",
        type=pathlib.Path,
        default=None,
        help=(
            "root for large benchmark artifacts such as image products, panels, "
            f"and scratch data; defaults to ${perf_paths.ARTIFACT_ROOT_ENV} or "
            f"{perf_paths.DEFAULT_EXTERNAL_ARTIFACT_ROOT}"
        ),
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=None,
        help="override manifest run.repeats",
    )
    parser.add_argument(
        "--run-label",
        default=None,
        help="override manifest run.run_label, for example cold, warm, or fresh-open",
    )
    parser.add_argument(
        "--storage-label",
        default=None,
        help="override manifest run.storage_label",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate manifest support and write the planned command without running",
    )
    parser.add_argument(
        "--stream-log",
        action="store_true",
        default=None,
        help="stream benchmark stdout while still recording the full benchmark log",
    )
    parser.add_argument(
        "--set-imaging",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="override one imaging manifest field for benchmark sweeps",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        manifest_path = resolve_workload(args.workload)
        manifest = load_manifest(manifest_path)
        apply_imaging_overrides(manifest, args.set_imaging)
        plan = build_plan(
            manifest_path=manifest_path,
            manifest=manifest,
            repeats_override=args.repeats,
            run_label_override=args.run_label,
            storage_label_override=args.storage_label,
            stream_log_override=args.stream_log,
            dry_run=args.dry_run,
        )
        output_dir = args.output_dir.expanduser().resolve()
        artifact_root = (
            args.artifact_root.expanduser()
            if args.artifact_root is not None
            else perf_paths.default_artifact_root()
        ).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        if not args.dry_run:
            perf_paths.mark_safe_to_delete(artifact_root)
        attach_output_paths(plan, output_dir, artifact_root, dry_run=args.dry_run)
        result_path = output_dir / f"{plan['run_id']}.json"
        log_path = output_dir / f"{plan['run_id']}.log"

        if args.dry_run:
            result = {
                "schema_version": 1,
                "status": "dry_run",
                **plan,
                "logs": {"benchmark_log": None},
                "results": empty_results(casa_status="not_run", reason="dry run"),
                "human_review": human_review_gate(plan, None),
            }
            write_json(result_path, result)
            print(result_path)
            return

        result = run_plan(plan, log_path)
        result["logs"] = {"benchmark_log": str(log_path)}
        write_json(result_path, result)
        print(result_path)
    except HarnessError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None


def resolve_workload(value: str) -> pathlib.Path:
    candidate = pathlib.Path(value)
    if candidate.exists():
        return candidate.resolve()
    if candidate.suffix != ".json":
        candidate = WORKLOAD_DIR / f"{value}.json"
    if candidate.exists():
        return candidate.resolve()
    raise HarnessError(f"workload manifest not found: {value}")


def load_manifest(path: pathlib.Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except json.JSONDecodeError as error:
        raise HarnessError(f"parse {path}: {error}") from error
    if not isinstance(value, dict):
        raise HarnessError(f"{path} must contain a JSON object")
    return value


def apply_imaging_overrides(manifest: dict[str, Any], overrides: list[str]) -> None:
    if not overrides:
        return
    imaging = required_object(manifest, "imaging")
    for override in overrides:
        if "=" not in override:
            raise HarnessError("--set-imaging values must use KEY=VALUE")
        key, value = override.split("=", 1)
        if not key:
            raise HarnessError("--set-imaging key must not be empty")
        imaging[key] = parse_override_value(key, value)


def parse_override_value(key: str, value: str) -> Any:
    if key in STRING_IMAGING_OVERRIDE_KEYS:
        return value
    lowered = value.strip().lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none"}:
        return None
    if re.fullmatch(r"-?[0-9]+", value):
        return int(value)
    if re.fullmatch(r"-?(?:[0-9]+[.][0-9]*|[0-9]*[.][0-9]+)(?:[eE][+-]?[0-9]+)?", value):
        return float(value)
    if "," in value and all(re.fullmatch(r"-?[0-9]+", part) for part in value.split(",")):
        return [int(part) for part in value.split(",")]
    return value


def build_plan(
    *,
    manifest_path: pathlib.Path,
    manifest: dict[str, Any],
    repeats_override: int | None,
    run_label_override: str | None,
    storage_label_override: str | None,
    dry_run: bool,
    stream_log_override: bool | None = None,
) -> dict[str, Any]:
    workload_id = required_str(manifest, "id")
    mode_id = required_str(manifest, "mode_id")
    dataset = required_object(manifest, "dataset")
    imaging = required_object(manifest, "imaging")
    run = object_value(manifest, "run")
    comparison = object_value(manifest, "comparison")

    specmode = enum_value(imaging, "specmode", SUPPORTED_SPEC_MODES)
    gridder = enum_value(imaging, "gridder", SUPPORTED_GRIDDER_VALUES)
    bench_mode = enum_value(imaging, "mode", SUPPORTED_BENCH_MODES)
    interpolation = enum_value_default(imaging, "interpolation", "linear", SUPPORTED_INTERPOLATION)
    hogbom_iteration_mode = enum_value_default(
        imaging, "hogbom_iteration_mode", "strict", SUPPORTED_HOGBOM_ITERATION_MODES
    )
    if hogbom_iteration_mode == "casa_inclusive":
        hogbom_iteration_mode = "casa"
    wterm = str_value(imaging, "wterm", "none")
    run_support = benchmark_run_support(
        workload_id=workload_id,
        specmode=specmode,
        gridder=gridder,
        wterm=wterm,
    )
    if not dry_run and run_support["status"] != "runnable":
        raise HarnessError(f"{workload_id}: {run_support['reason']}")
    repeats = repeats_override if repeats_override is not None else int_value(run, "repeats", 5)
    if repeats < 1:
        raise HarnessError("repeats must be >= 1")
    ms_staging = os.environ.get("CASA_RS_BENCH_MS_STAGING") or str_value(
        run, "ms_staging", "direct"
    )
    if ms_staging not in SUPPORTED_MS_STAGING:
        raise HarnessError("run.ms_staging must be copy or direct")
    phase_probe = os.environ.get("CASA_RS_BENCH_PHASE_PROBE") or str_value(
        run, "phase_probe", "0"
    )
    if phase_probe.lower() not in SUPPORTED_BOOLEAN_FLAGS:
        raise HarnessError("run.phase_probe must be 0/1, true/false, yes/no, or on/off")
    skip_casa = os.environ.get("CASA_RS_BENCH_SKIP_CASA") or str_value(
        run, "skip_casa", "0"
    )
    if skip_casa.lower() not in SUPPORTED_BOOLEAN_FLAGS:
        raise HarnessError("run.skip_casa must be 0/1, true/false, yes/no, or on/off")
    skip_rust = os.environ.get("CASA_RS_BENCH_SKIP_RUST") or str_value(
        run, "skip_rust", "0"
    )
    if skip_rust.lower() not in SUPPORTED_BOOLEAN_FLAGS:
        raise HarnessError("run.skip_rust must be 0/1, true/false, yes/no, or on/off")
    skip_profile = os.environ.get("CASA_RS_BENCH_SKIP_PROFILE") or str_value(
        run, "skip_profile", "0"
    )
    if skip_profile.lower() not in SUPPORTED_BOOLEAN_FLAGS:
        raise HarnessError("run.skip_profile must be 0/1, true/false, yes/no, or on/off")
    reuse_rust_prefix = os.environ.get("CASA_RS_BENCH_REUSE_RUST_PREFIX") or str_value(
        run, "reuse_rust_prefix", ""
    )
    reuse_casa_prefix = os.environ.get("CASA_RS_BENCH_REUSE_CASA_PREFIX") or str_value(
        run, "reuse_casa_prefix", ""
    )
    profile_repeats = os.environ.get("CASA_RS_BENCH_PROFILE_REPEATS") or str(
        int_value(run, "profile_repeats", repeats)
    )
    if not profile_repeats.isdigit() or int(profile_repeats) < 1:
        raise HarnessError("run.profile_repeats must be an integer >= 1")
    extra_env = string_map_value(run, "env")

    dataset_path = resolve_dataset_path(dataset, dry_run=dry_run)
    casa_python = os.environ.get("CASA_RS_CASA_PYTHON")
    if not dry_run and not casa_python:
        raise HarnessError("CASA_RS_CASA_PYTHON is required for a benchmark run")
    if not dry_run and casa_python and not pathlib.Path(casa_python).is_file():
        raise HarnessError(f"CASA_RS_CASA_PYTHON does not exist: {casa_python}")

    env = {
        "BENCH_REPEATS": str(repeats),
        "IMAGER_BENCH_MODE": bench_mode,
        "IMAGER_BENCH_SPECMODE": specmode,
        "IMAGER_BENCH_GRIDDER": gridder,
        "IMAGER_BENCH_CASA_GRIDDER": str_value(imaging, "casa_gridder", gridder),
        "IMAGER_BENCH_INTERPOLATION": interpolation,
        "IMAGER_BENCH_FIELD": str_value(imaging, "field", "0"),
        "IMAGER_BENCH_PHASECENTER_FIELD": optional_int_string(imaging, "phasecenter_field"),
        "IMAGER_BENCH_SPW": str_value(imaging, "spw", "0"),
        "IMAGER_BENCH_CHANNEL_START": str(int_value(imaging, "channel_start", 0)),
        "IMAGER_BENCH_CHANNEL_COUNT": str(int_value(imaging, "channel_count", 1)),
        "IMAGER_BENCH_CUBE_START": str_value(imaging, "start", ""),
        "IMAGER_BENCH_CUBE_WIDTH": str_value(imaging, "width", ""),
        "IMAGER_BENCH_IMSIZE": str(int_value(imaging, "imsize", 128)),
        "IMAGER_BENCH_CELL_ARCSEC": str(float_value(imaging, "cell_arcsec", 30.0)),
        "IMAGER_BENCH_WEIGHTING": str_value(imaging, "weighting", "natural"),
        "IMAGER_BENCH_ROBUST": str(float_value(imaging, "robust", 0.5)),
        "IMAGER_BENCH_PERCHANWEIGHTDENSITY": boolean_env_value(
            imaging,
            "perchanweightdensity",
            specmode in {"cube", "cubedata"},
        ),
        "IMAGER_BENCH_DECONVOLVER": str_value(imaging, "deconvolver", "hogbom"),
        "IMAGER_BENCH_STANDARD_MFS_ACCELERATION": str_value(
            imaging, "standard_mfs_acceleration", "auto"
        ),
        "IMAGER_BENCH_IMAGING_FFT_PRECISION": str_value(
            imaging, "imaging_fft_precision", "auto"
        ),
        "IMAGER_BENCH_HOGBOM_ITERATION_MODE": hogbom_iteration_mode,
        "IMAGER_BENCH_NTERMS": str(int_value(imaging, "nterms", 1)),
        "IMAGER_BENCH_SCALES": scales_value(imaging),
        "IMAGER_BENCH_WTERM": wterm,
        "IMAGER_BENCH_WPROJPLANES": optional_int_string(imaging, "wprojplanes"),
        "IMAGER_BENCH_NITER": str(int_value(imaging, "niter", 4)),
        "IMAGER_BENCH_GAIN": str(float_value(imaging, "gain", 0.1)),
        "IMAGER_BENCH_THRESHOLD_JY": str(float_value(imaging, "threshold_jy", 0.0)),
        "IMAGER_BENCH_NSIGMA": str(float_value(imaging, "nsigma", 0.0)),
        "IMAGER_BENCH_PSFCUTOFF": str(float_value(imaging, "psfcutoff", 0.35)),
        "IMAGER_BENCH_PBLIMIT": str(float_value(imaging, "pblimit", 0.2)),
        "IMAGER_BENCH_WRITE_PB": boolean_env_value(imaging, "write_pb", False),
        "IMAGER_BENCH_PBCOR": boolean_env_value(imaging, "pbcor", False),
        "IMAGER_BENCH_MINOR_CYCLE_LENGTH": str(
            int_value(imaging, "minor_cycle_length", 2)
        ),
        "IMAGER_BENCH_CYCLEFACTOR": str(float_value(imaging, "cyclefactor", 1.0)),
        "IMAGER_BENCH_MIN_PSFFRACTION": str(
            float_value(imaging, "min_psf_fraction", 0.05)
        ),
        "IMAGER_BENCH_MAX_PSFFRACTION": str(
            float_value(imaging, "max_psf_fraction", 0.8)
        ),
        "IMAGER_BENCH_MS_STAGING": ms_staging,
        "IMAGER_BENCH_PHASE_PROBE": phase_probe,
        "IMAGER_BENCH_SKIP_CASA": skip_casa,
        "IMAGER_BENCH_SKIP_RUST": skip_rust,
        "IMAGER_BENCH_SKIP_PROFILE": skip_profile,
        "IMAGER_BENCH_PROFILE_REPEATS": profile_repeats,
    }
    if reuse_rust_prefix:
        env["IMAGER_BENCH_REUSE_RUST_PREFIX"] = reuse_rust_prefix
    if reuse_casa_prefix:
        env["IMAGER_BENCH_REUSE_CASA_PREFIX"] = reuse_casa_prefix
    optional_imaging_env = {
        "standard_mfs_grid_threads": "IMAGER_BENCH_STANDARD_MFS_GRID_THREADS",
        "standard_mfs_metal_minor_cycle_chunk": "IMAGER_BENCH_STANDARD_MFS_METAL_MINOR_CYCLE_CHUNK",
        "imaging_memory_target_mb": "IMAGER_BENCH_IMAGING_MEMORY_TARGET_MB",
        "imaging_prepare_buffer_mb": "IMAGER_BENCH_IMAGING_PREPARE_BUFFER_MB",
        "imaging_row_block_rows": "IMAGER_BENCH_IMAGING_ROW_BLOCK_ROWS",
        "imaging_prepare_workers": "IMAGER_BENCH_IMAGING_PREPARE_WORKERS",
    }
    for imaging_key, env_key in optional_imaging_env.items():
        if imaging.get(imaging_key) is not None:
            if imaging_key in {
                "standard_mfs_grid_threads",
                "standard_mfs_metal_minor_cycle_chunk",
            }:
                env[env_key] = str(imaging[imaging_key])
            else:
                env[env_key] = str(int_value(imaging, imaging_key, 0))
    env.setdefault("CASA_RS_STANDARD_MFS_PROFILE_DETAIL", "1")
    env.update(extra_env)

    command = [str(BENCH_SCRIPT), str(dataset_path)]
    plan = {
        "run_id": f"{utc_stamp()}-{workload_id}-{uuid.uuid4().hex[:8]}",
        "created_at": utc_now(),
        "manifest_path": str(manifest_path),
        "workload": {
            "id": workload_id,
            "mode_id": mode_id,
            "description": str_value(manifest, "description", ""),
        },
        "dataset": {
            "key": required_str(dataset, "key"),
            "path": str(dataset_path),
            "relative_path": dataset.get("relative_path"),
            "root_env": dataset.get("root_env"),
        },
        "mode": {
            "specmode": specmode,
            "gridder": gridder,
            "bench_mode": bench_mode,
            "image_shape": [int_value(imaging, "imsize", 128), int_value(imaging, "imsize", 128)],
            "channel_count": int_value(imaging, "channel_count", 1),
            "start": str_value(imaging, "start", "") or None,
            "width": str_value(imaging, "width", "") or None,
            "weighting": str_value(imaging, "weighting", "natural"),
            "perchanweightdensity": boolean_env_value(
                imaging,
                "perchanweightdensity",
                specmode in {"cube", "cubedata"},
            ),
            "deconvolver": str_value(imaging, "deconvolver", "hogbom"),
            "standard_mfs_acceleration": str_value(
                imaging, "standard_mfs_acceleration", "auto"
            ),
            "imaging_fft_precision": str_value(
                imaging, "imaging_fft_precision", "auto"
            ),
            "standard_mfs_metal_minor_cycle_chunk": (
                str(imaging["standard_mfs_metal_minor_cycle_chunk"])
                if imaging.get("standard_mfs_metal_minor_cycle_chunk") is not None
                else None
            ),
            "hogbom_iteration_mode": hogbom_iteration_mode,
            "nterms": int_value(imaging, "nterms", 1),
            "niter": int_value(imaging, "niter", 4),
            "wprojplanes": optional_int_string(imaging, "wprojplanes") or None,
        },
        "run": {
            "repeats": repeats,
            "run_label": run_label_override or str_value(run, "run_label", "warm"),
            "storage_label": storage_label_override
            or str_value(run, "storage_label", "script-staged-tempdir"),
            "ms_staging": ms_staging,
            "phase_probe": phase_probe,
            "skip_casa": skip_casa,
            "skip_rust": skip_rust,
            "reuse_rust_prefix": reuse_rust_prefix or None,
            "reuse_casa_prefix": reuse_casa_prefix or None,
            "env": extra_env,
            "stream_log": (
                bool(stream_log_override)
                if stream_log_override is not None
                else bool_value(run, "stream_log", False)
            ),
            "profile_repeats": int(profile_repeats),
        },
        "run_support": run_support,
        "review": review_contract_value(manifest, run),
        "comparison": {
            "products": product_suffixes_value(comparison),
            "max_elements_per_product": int_value(
                comparison, "max_elements_per_product", 1_000_000
            ),
        },
        "command": {
            "argv": command,
            "env": env,
        },
        "environment": collect_environment(casa_python),
    }
    plan["benchmark_features"] = build_benchmark_feature_summary(plan, None)
    return plan


def attach_output_paths(
    plan: dict[str, Any],
    output_dir: pathlib.Path,
    artifact_root: pathlib.Path,
    *,
    dry_run: bool,
) -> None:
    product_root = artifact_root / "products" / plan["run_id"]
    comparison_root = artifact_root / "comparisons" / plan["run_id"]
    tmp_root = artifact_root / "tmp"
    plan["products"] = {
        "root": None if dry_run else str(product_root),
        "rust_prefix": None if dry_run else str(product_root / "rust" / "rust"),
        "casa_prefix": None if dry_run else str(product_root / "casa" / "casa"),
    }
    plan["artifacts"] = {
        "root": str(artifact_root),
        "result_dir": str(output_dir),
        "products_root": None if dry_run else str(product_root),
        "comparison_root": None if dry_run else str(comparison_root),
        "tmp_root": None if dry_run else str(tmp_root),
    }
    if not dry_run:
        tmp_root.mkdir(parents=True, exist_ok=True)
        plan["command"]["env"]["IMAGER_BENCH_KEEP_OUTPUT_ROOT"] = str(product_root)
        plan["command"]["env"]["IMAGER_BENCH_TMP_ROOT"] = str(tmp_root)


def benchmark_run_support(
    *, workload_id: str, specmode: str, gridder: str, wterm: str
) -> dict[str, Any]:
    reasons = []
    if specmode not in RUNNABLE_SPEC_MODES:
        reasons.append(f"specmode={specmode!r} needs benchmark-script execution support")
    if gridder not in RUNNABLE_GRIDDER_VALUES:
        reasons.append(f"gridder={gridder!r} needs benchmark-script execution support")
    if wterm != "none" and not (
        gridder in {"wproject", "widefield", "awproject", "awp2", "awphpg"}
        and wterm == "wproject"
    ):
        reasons.append(f"wterm={wterm!r} needs benchmark-script execution support")
    if reasons:
        return {
            "status": "dry_run_only",
            "reason": f"{workload_id}: " + "; ".join(reasons),
            "bench_script": str(BENCH_SCRIPT),
        }
    return {
        "status": "runnable",
        "reason": None,
        "bench_script": str(BENCH_SCRIPT),
    }


def review_contract_value(manifest: dict[str, Any], run: dict[str, Any]) -> dict[str, Any]:
    review = object_value(manifest, "review")
    required_roles = review.get(
        "required_evidence_roles",
        ["before_baseline", "after_multi_worker_cpu", "after_gpu_metal", "casa_cpp"],
    )
    if not isinstance(required_roles, list) or not all(
        isinstance(role, str) and role for role in required_roles
    ):
        raise HarnessError("review.required_evidence_roles must be a non-empty string list")
    evidence_role = str_value(run, "evidence_role", "unspecified")
    return {
        "required_reviewer": str_value(review, "required_reviewer", "Brian"),
        "requires_human_acceptance_before_done": bool(
            review.get("requires_human_acceptance_before_done", True)
        ),
        "required_evidence_roles": required_roles,
        "evidence_role": evidence_role,
    }


def human_review_gate(
    plan: dict[str, Any], comparison: dict[str, Any] | None
) -> dict[str, Any]:
    review = plan.get("review", {})
    panel_status = "not_run"
    panel_reason = "benchmark has not produced comparison panels yet"
    structured_review = {}
    if comparison:
        panel_status, panel_reason = review_panel_status(comparison)
        value = comparison.get("structured_difference_review")
        if isinstance(value, dict):
            structured_review = value
    return {
        "status": "pending",
        "required_reviewer": review.get("required_reviewer", "Brian"),
        "requires_human_acceptance_before_done": review.get(
            "requires_human_acceptance_before_done", True
        ),
        "evidence_role": review.get("evidence_role", "unspecified"),
        "required_evidence_roles": review.get("required_evidence_roles", []),
        "panel_status": panel_status,
        "panel_reason": panel_reason,
        "structured_difference_label": structured_review.get("label", "not_run"),
        "structured_difference_summary": structured_review.get(
            "summary", "structured-difference review has not run yet"
        ),
        "structured_difference_legend": structured_review.get(
            "legend", STRUCTURED_DIFFERENCE_REVIEW_LEGEND
        ),
        "reason": "mode ticket cannot move to Done until the review bundle is accepted",
    }


def review_panel_status(comparison: dict[str, Any]) -> tuple[str, str | None]:
    if comparison.get("status") != "completed":
        return comparison.get("status", "missing"), comparison.get("reason")
    products = comparison.get("products", {})
    if not products:
        return "missing", "comparison did not include products"
    missing = []
    skipped = []
    for suffix, product in products.items():
        panel = product.get("review_panel") if isinstance(product, dict) else None
        if not panel:
            missing.append(suffix)
        elif panel.get("status") != "written":
            skipped.append(f"{suffix}: {panel.get('reason') or panel.get('status')}")
    if missing:
        return "missing", "missing review panels for " + ", ".join(missing)
    if skipped:
        return "incomplete", "; ".join(skipped)
    return "ready", None


def run_plan(plan: dict[str, Any], log_path: pathlib.Path) -> dict[str, Any]:
    env = os.environ.copy()
    env.update(plan["command"]["env"])
    if bool(plan.get("run", {}).get("stream_log", False)):
        env["IMAGER_BENCH_STREAM_LOG"] = "1"
        env["CASA_RS_IMAGING_PROGRESS"] = "1"
    started = utc_now()
    completed = run_benchmark_command(
        plan["command"]["argv"],
        env=env,
        stream_log=bool(plan.get("run", {}).get("stream_log", False)),
    )
    log_path.write_text(completed.stdout, encoding="utf-8")
    if completed.returncode != 0:
        reason = benchmark_failure_reason(completed.stdout, completed.returncode)
        return {
            "schema_version": 1,
            "status": "failed",
            **plan,
            "started_at": started,
            "completed_at": utc_now(),
            "exit_code": completed.returncode,
            "results": empty_results(
                casa_status="blocked",
                reason=reason,
            ),
            "human_review": human_review_gate(plan, None),
        }

    parsed = parse_benchmark_log(completed.stdout)
    parsed["backend_plan_logs"] = parse_backend_plan_logs(completed.stdout)
    parsed["benchmark_features"] = build_benchmark_feature_summary(plan, parsed)
    attach_stage_breakdown(plan, parsed)
    comparison = compare_products(plan, parsed, log_path)
    parsed["product_comparison"] = comparison
    completed_plan = dict(plan)
    completed_plan["benchmark_features"] = parsed["benchmark_features"]
    return {
        "schema_version": 1,
        "status": "completed",
        **completed_plan,
        "started_at": started,
        "completed_at": utc_now(),
        "exit_code": completed.returncode,
        "results": parsed,
        "human_review": human_review_gate(plan, comparison),
    }


def run_benchmark_command(
    argv: list[str], *, env: dict[str, str], stream_log: bool
) -> subprocess.CompletedProcess[str]:
    if not stream_log:
        return subprocess.run(
            argv,
            cwd=REPO_ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )

    process = subprocess.Popen(
        argv,
        cwd=REPO_ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
    )
    output_chunks = []
    assert process.stdout is not None
    for line in process.stdout:
        output_chunks.append(line)
        print(line, end="", flush=True)
    returncode = process.wait()
    return subprocess.CompletedProcess(argv, returncode, "".join(output_chunks), None)


def benchmark_failure_reason(text: str, returncode: int) -> str:
    for raw_line in reversed(text.splitlines()):
        line = raw_line.strip()
        if line.startswith("Error:"):
            return line
    for raw_line in reversed(text.splitlines()):
        line = raw_line.strip()
        if "bounded source stream rejected" in line:
            return line
    return f"benchmark command exited {returncode}"


def empty_results(*, casa_status: str, reason: str) -> dict[str, Any]:
    return {
        "rust": {
            "status": "not_run",
            "reason": reason,
            "timings_seconds": {"runs": [], "median": None},
        },
        "casa": {
            "status": casa_status,
            "reason": reason,
            "timings_seconds": {"runs": [], "median": None},
        },
        "stage_medians_ms": {"rust": {}, "casa": {}},
        "stage_breakdown": empty_stage_breakdown(reason),
        "product_paths": {},
        "product_comparison": {"status": "skipped", "reason": reason, "products": {}},
    }


def parse_benchmark_log(text: str) -> dict[str, Any]:
    rust_runs, rust_median = parse_timing_section(text, "Rust release CLI timings")
    casa_runs, casa_median = parse_timing_section(text, "CASA tclean timings")
    rust_stages = parse_stage_section(text, "Rust stage medians")
    casa_stages = parse_stage_section(text, "CASA PySynthesisImager stage medians")
    return {
        "rust": timing_result(
            rust_runs,
            rust_median,
            missing_reason="Rust release CLI timing section was not reported",
        ),
        "casa": timing_result(
            casa_runs,
            casa_median,
            missing_reason="CASA tclean timing section was not reported",
        ),
        "stage_medians_ms": {"rust": rust_stages, "casa": casa_stages},
        "casa_clean_control_diagnostics": parse_casa_clean_control_diagnostics(text),
        "product_paths": parse_product_paths(text),
    }


def parse_backend_plan_logs(text: str) -> dict[str, Any]:
    """Extract compact backend/source-stream diagnostics from benchmark logs."""
    buckets = {
        "single_plane_execution_plan": [],
        "standard_mfs_runtime_plan": [],
        "source_stream_memory_plan": [],
        "source_stream_consumer": [],
        "frontend_progress": [],
        "profile_runs": [],
        "spectral_slab_events": [],
        "spectral_slab_memory": [],
        "spectral_slab_plans": [],
        "mosaic_cube_slab_plans": [],
        "mosaic_cube_slab_planes": [],
        "mosaic_cube_slab_executor_summaries": [],
        "cube_per_plane_backend": [],
        "cube_resident_clean_control": [],
        "cube_resident_clean_executor": [],
        "cube_resident_clean_finish_planes": [],
        "cube_resident_clean_stage": [],
        "cube_source_row_blocks": [],
        "cube_product_summaries": [],
        "image_product_writes": [],
        "cube_plane_state_store": [],
        "visibility_geometry_cache": [],
        "executor_limitations": [],
        "worker_diagnostics": [],
        "minor_cycle_diagnostics": [],
        "hogbom_minor_cycle_diagnostics": [],
        "clark_minor_cycle_diagnostics": [],
        "multiscale_minor_cycle_diagnostics": [],
        "clean_residual_refresh_diagnostics": [],
        "metal_diagnostics": [],
    }
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parsed = parse_key_value_line(line)
        if not parsed:
            continue
        name = parsed["name"]
        if name == "single_plane_execution_plan":
            buckets["single_plane_execution_plan"].append(parsed)
        elif name == "standard_mfs_runtime_plan":
            buckets["standard_mfs_runtime_plan"].append(parsed)
        elif name == "standard_mfs_memory_plan_actual":
            buckets["source_stream_memory_plan"].append(parsed)
        elif name == "visibility_source_stream_consumer":
            buckets["source_stream_consumer"].append(parsed)
        elif name == "frontend":
            buckets["frontend_progress"].append(parsed)
        elif name == "standard_mfs_profile_run":
            buckets["profile_runs"].append(parsed)
        elif name == "spectral_slab_event":
            buckets["spectral_slab_events"].append(parsed)
        elif name == "spectral_slab_memory":
            buckets["spectral_slab_memory"].append(parsed)
        elif name == "spectral_slab_plan":
            buckets["spectral_slab_plans"].append(parsed)
        elif name == "mosaic_cube_slab_plan":
            buckets["mosaic_cube_slab_plans"].append(parsed)
        elif name == "mosaic_cube_slab_plane":
            buckets["mosaic_cube_slab_planes"].append(parsed)
        elif name == "mosaic_cube_slab_executor_summary":
            buckets["mosaic_cube_slab_executor_summaries"].append(parsed)
        elif name == "cube_per_plane_backend_summary":
            buckets["cube_per_plane_backend"].append(parsed)
        elif name == "cube_resident_clean_control":
            buckets["cube_resident_clean_control"].append(parsed)
        elif name == "cube_resident_clean_executor_summary":
            buckets["cube_resident_clean_executor"].append(parsed)
        elif name == "cube_resident_clean_finish_plane":
            buckets["cube_resident_clean_finish_planes"].append(parsed)
        elif name in {
            "cube_resident_clean_stage_summary",
            "cube_resident_clean_finish_plane_stage_detail",
        }:
            buckets["cube_resident_clean_stage"].append(parsed)
        elif name == "cube_source_row_blocks":
            buckets["cube_source_row_blocks"].append(parsed)
        elif name == "cube_plane_state_store_summary":
            buckets["cube_plane_state_store"].append(parsed)
        elif name == "visibility_geometry_cache_summary":
            buckets["visibility_geometry_cache"].append(parsed)
        elif name in {
            "cube_shared_direct_plane_executor_summary",
            "cube_shared_plane_executor_summary",
        }:
            buckets["cube_product_summaries"].append(parsed)
        elif name == "image_product_write":
            buckets["image_product_writes"].append(parsed)
        elif name.endswith("_executor_limitation"):
            buckets["executor_limitations"].append(parsed)
        elif name == "standard_mfs_hogbom_minor_cycle_summary":
            buckets["minor_cycle_diagnostics"].append(parsed)
            buckets["hogbom_minor_cycle_diagnostics"].append(parsed)
            if parsed.get("fields", {}).get("backend") != "cpu":
                buckets["metal_diagnostics"].append(parsed)
        elif name == "standard_mfs_clark_minor_cycle_summary":
            buckets["minor_cycle_diagnostics"].append(parsed)
            buckets["clark_minor_cycle_diagnostics"].append(parsed)
            if parsed.get("fields", {}).get("backend") != "cpu":
                buckets["metal_diagnostics"].append(parsed)
        elif name == "standard_mfs_multiscale_minor_cycle_summary":
            buckets["minor_cycle_diagnostics"].append(parsed)
            buckets["multiscale_minor_cycle_diagnostics"].append(parsed)
            if parsed.get("fields", {}).get("backend") != "cpu":
                buckets["metal_diagnostics"].append(parsed)
        elif name in {
            "standard_mfs_multiscale_metal_minor_cycle_summary",
            "standard_mfs_multiscale_metal_indirect_summary",
        }:
            buckets["metal_diagnostics"].append(parsed)
        elif name == "standard_mfs_clean_residual_refresh_summary":
            buckets["clean_residual_refresh_diagnostics"].append(parsed)
            residual_backend = parsed.get("fields", {}).get("residual_backend")
            if isinstance(residual_backend, str) and "metal" in residual_backend:
                buckets["metal_diagnostics"].append(parsed)
        elif "worker" in name or "prepare_parallel" in name:
            buckets["worker_diagnostics"].append(parsed)
        elif "metal" in name:
            buckets["metal_diagnostics"].append(parsed)
    return {
        "schema_version": 1,
        "summary": summarize_backend_plan_logs(buckets),
        **buckets,
    }


def parse_key_value_line(line: str) -> dict[str, Any] | None:
    if "=" not in line:
        return None
    parts = line.split(None, 1)
    name = parts[0]
    if "=" in name:
        name = "line"
        rest = line
    else:
        rest = parts[1] if len(parts) > 1 else ""
    fields = {
        key: parse_scalar_value(value)
        for key, value in re.findall(r"([A-Za-z0-9_]+)=([^ \t]+)", rest)
    }
    if not fields:
        return None
    return {"name": name, "raw": line, "fields": fields}


def parse_scalar_value(value: str) -> Any:
    if value in {"true", "false"}:
        return value == "true"
    if value in {"None", "none", "unset", "auto"}:
        return value
    if re.fullmatch(r"-?[0-9]+", value):
        try:
            return int(value)
        except ValueError:
            return value
    if re.fullmatch(r"-?(?:[0-9]+[.][0-9]*|[0-9]*[.][0-9]+)(?:[eE][+-]?[0-9]+)?", value):
        try:
            return float(value)
        except ValueError:
            return value
    return value


def summarize_backend_plan_logs(buckets: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    runtime = last_fields(buckets.get("standard_mfs_runtime_plan", []))
    memory = last_fields(buckets.get("source_stream_memory_plan", []))
    profile = last_fields(buckets.get("profile_runs", []))
    single_plane = last_fields(buckets.get("single_plane_execution_plan", []))
    spectral_plan = last_fields(buckets.get("spectral_slab_plans", []))
    mosaic_cube_slab_plan = last_fields(buckets.get("mosaic_cube_slab_plans", []))
    mosaic_cube_slab_planes = [
        entry.get("fields", {})
        for entry in unique_entries_by_raw(buckets.get("mosaic_cube_slab_planes", []))
        if isinstance(entry.get("fields", {}), dict)
    ]
    mosaic_cube_slab_executor_summaries = [
        entry.get("fields", {})
        for entry in unique_entries_by_raw(
            buckets.get("mosaic_cube_slab_executor_summaries", [])
        )
        if isinstance(entry.get("fields", {}), dict)
    ]
    cube_per_plane_backend = last_fields(buckets.get("cube_per_plane_backend", []))
    cube_resident_control = last_fields(buckets.get("cube_resident_clean_control", []))
    cube_resident_executor = last_fields(buckets.get("cube_resident_clean_executor", []))
    cube_resident_stage = last_fields(
        [
            entry
            for entry in buckets.get("cube_resident_clean_stage", [])
            if entry.get("name") == "cube_resident_clean_stage_summary"
        ]
    )
    cube_resident_finish_planes = [
        entry.get("fields", {})
        for entry in unique_entries_by_raw(
            buckets.get("cube_resident_clean_finish_planes", [])
        )
        if isinstance(entry.get("fields", {}), dict)
    ]
    cube_resident_finished_cleaned_planes = [
        entry
        for entry in cube_resident_finish_planes
        if entry.get("skipped_minor_cycle") is False
    ]
    cube_resident_finished_skipped_planes = [
        entry
        for entry in cube_resident_finish_planes
        if entry.get("skipped_minor_cycle") is True
    ]
    cube_source_rows = last_fields(buckets.get("cube_source_row_blocks", []))
    cube_product_summaries = [
        entry.get("fields", {})
        for entry in unique_entries_by_raw(buckets.get("cube_product_summaries", []))
        if isinstance(entry.get("fields", {}), dict)
    ]
    cube_plane_state_store = [
        entry.get("fields", {})
        for entry in unique_entries_by_raw(buckets.get("cube_plane_state_store", []))
        if isinstance(entry.get("fields", {}), dict)
    ]
    last_cube_plane_state_store = (
        cube_plane_state_store[-1] if cube_plane_state_store else {}
    )
    visibility_geometry_cache = [
        entry.get("fields", {})
        for entry in unique_entries_by_raw(buckets.get("visibility_geometry_cache", []))
        if isinstance(entry.get("fields", {}), dict)
    ]
    last_visibility_geometry_cache = (
        visibility_geometry_cache[-1] if visibility_geometry_cache else {}
    )
    executor_limitation = last_fields(buckets.get("executor_limitations", []))
    metal_entries = unique_entries_by_raw(buckets.get("metal_diagnostics", []))
    clean_residual_refresh = [
        entry.get("fields", {})
        for entry in unique_entries_by_raw(
            buckets.get("clean_residual_refresh_diagnostics", [])
        )
        if isinstance(entry.get("fields", {}), dict)
    ]
    metal_residual_refresh = fields_for_names(
        metal_entries,
        {
            "standard_mfs_metal_residual_refresh",
            "standard_mfs_metal_row_run_residual_refresh",
            "standard_mfs_metal_row_run_grouped_residual_refresh",
        },
    )
    metal_residual_refresh_detail = fields_for_names(
        metal_entries,
        {
            "standard_mfs_metal_residual_refresh_detail",
            "standard_mfs_metal_row_run_residual_refresh_detail",
            "standard_mfs_metal_row_run_grouped_residual_refresh_detail",
        },
    )
    metal_grouped_append_detail = fields_for_names(
        metal_entries, {"standard_mfs_metal_row_run_grouped_append_detail"}
    )
    image_product_writes = [
        entry.get("fields", {})
        for entry in unique_entries_by_raw(buckets.get("image_product_writes", []))
        if isinstance(entry.get("fields", {}), dict)
    ]
    spectral_memory = [
        entry.get("fields", {})
        for entry in buckets.get("spectral_slab_memory", [])
        if isinstance(entry.get("fields", {}), dict)
    ]
    max_current_rss = max_int_field(spectral_memory, "current_rss_bytes")
    max_peak_rss = max_int_field(spectral_memory, "peak_rss_bytes")
    max_baseline_delta = max_int_field(spectral_memory, "delta_from_baseline_bytes")
    max_previous_delta_entry = max_entry_by_int_field(
        spectral_memory, "delta_from_previous_bytes"
    )
    return {
        "single_plane_reason": {
            "cpu_multi_worker": single_plane.get("cpu_multi_worker_reason"),
            "gpu_metal": single_plane.get("gpu_metal_reason"),
        },
        "resolved_backend": runtime.get("backend"),
        "resolved_grid_threads": runtime.get("grid_threads"),
        "resolved_tile_anchor": runtime.get("tile_anchor"),
        "resolved_residual_backend": runtime.get("residual_backend"),
        "resolved_initial_dirty_backend": runtime.get("initial_dirty_backend"),
        "resolved_minor_cycle_backend": runtime.get("minor_cycle_backend"),
        "resolved_minor_cycle_backend_reason": runtime.get("minor_cycle_backend_reason"),
        "metal_device_available": runtime.get("metal_device_available"),
        "metal_grouped_input_cache": runtime.get("metal_grouped_input_cache"),
        "cube_per_plane_backend": cube_per_plane_backend.get("selected_backend"),
        "cube_per_plane_phase": cube_per_plane_backend.get("phase"),
        "cube_per_plane_workers": cube_per_plane_backend.get("plane_worker_count"),
        "cube_per_plane_grid_threads": cube_per_plane_backend.get("per_plane_grid_threads"),
        "cube_per_plane_fixed_tile_eligible": cube_per_plane_backend.get(
            "fixed_tile_cpu_eligible"
        ),
        "cube_per_plane_metal_eligible": cube_per_plane_backend.get("metal_eligible"),
        "cube_per_plane_fallback_reasons": cube_per_plane_backend.get("fallback_reasons"),
        "cube_resident_clean_planes": cube_resident_control.get("planes"),
        "cube_resident_clean_cycle_threshold": cube_resident_control.get(
            "cycle_threshold"
        ),
        "cube_resident_clean_planes_at_or_below_threshold": cube_resident_control.get(
            "planes_at_or_below_threshold"
        ),
        "cube_resident_clean_residency": cube_resident_control.get("residency"),
        "cube_resident_clean_completed": cube_resident_executor.get("completed"),
        "cube_resident_clean_skipped_minor_cycle_planes": cube_resident_executor.get(
            "skipped_minor_cycle_planes"
        ),
        "cube_resident_clean_cleaned_planes": cube_resident_executor.get(
            "cleaned_planes"
        ),
        "cube_resident_clean_elapsed_ms": cube_resident_executor.get("elapsed_ms"),
        "cube_resident_clean_control_source_read_ms": cube_resident_executor.get(
            "control_source_read_ms"
        ),
        "cube_resident_clean_publish_source_read_ms": cube_resident_executor.get(
            "publish_source_read_ms"
        ),
        "cube_resident_clean_control_prepare_ms": cube_resident_executor.get(
            "control_prepare_ms"
        ),
        "cube_resident_clean_publish_prepare_ms": cube_resident_executor.get(
            "publish_prepare_ms"
        ),
        "cube_resident_clean_product_write_ms": cube_resident_executor.get(
            "product_write_ms"
        ),
        "cube_resident_clean_result_wait_ms": cube_resident_stage.get("result_wait_ms"),
        "cube_resident_clean_consume_ms": cube_resident_stage.get("consume_ms"),
        "cube_resident_clean_core_total_ms": cube_resident_stage.get("total_ms"),
        "cube_resident_clean_minor_cycle_ms": cube_resident_stage.get("minor_cycle_ms"),
        "cube_resident_clean_minor_cycle_solve_ms": cube_resident_stage.get(
            "minor_cycle_solve_ms"
        ),
        "cube_resident_clean_major_cycle_refresh_ms": cube_resident_stage.get(
            "major_cycle_refresh_ms"
        ),
        "cube_resident_clean_residual_refresh_overhead_ms": cube_resident_stage.get(
            "residual_refresh_overhead_ms"
        ),
        "cube_resident_clean_restore_ms": cube_resident_stage.get("restore_ms"),
        "cube_resident_clean_controller_overhead_ms": cube_resident_stage.get(
            "controller_overhead_ms"
        ),
        "cube_resident_clean_finish_plane_count": len(cube_resident_finish_planes),
        "cube_resident_clean_finish_cleaned_plane_count": len(
            cube_resident_finished_cleaned_planes
        ),
        "cube_resident_clean_finish_skipped_plane_count": len(
            cube_resident_finished_skipped_planes
        ),
        "cube_resident_clean_actual_updates": sum_int_or_float_field(
            cube_resident_finish_planes, "actual_updates"
        ),
        "cube_resident_clean_reported_updates": sum_int_or_float_field(
            cube_resident_finish_planes, "reported_updates"
        ),
        "cube_resident_clean_trace_minor_cycles": sum_int_or_float_field(
            cube_resident_finish_planes, "minor_cycle_count"
        ),
        "cube_resident_clean_minor_iterations_from_planes": sum_int_or_float_field(
            cube_resident_finish_planes, "minor_iterations"
        ),
        "cube_resident_clean_max_actual_updates_per_plane": max_int_field(
            cube_resident_finish_planes, "actual_updates"
        ),
        "cube_resident_clean_model_nonzero_pixels": sum_int_or_float_field(
            cube_resident_finish_planes, "model_nonzero_pixels"
        ),
        "cube_resident_clean_model_nonzero_planes": count_positive_field(
            cube_resident_finish_planes, "model_nonzero_pixels"
        ),
        "cube_resident_clean_skipped_model_nonzero_planes": count_positive_field(
            cube_resident_finished_skipped_planes, "model_nonzero_pixels"
        ),
        "cube_resident_clean_model_sum_abs_jy": sum_int_or_float_field(
            cube_resident_finish_planes, "model_sum_abs_jy"
        ),
        "cube_resident_clean_model_peak_abs_jy": max_int_or_float_field(
            cube_resident_finish_planes, "model_peak_abs_jy"
        ),
        "cube_resident_clean_stop_reason_counts": compact_value_counts(
            cube_resident_finish_planes, "stop_reason"
        ),
        "mosaic_cube_slab_schedule": mosaic_cube_slab_plan.get("schedule"),
        "mosaic_cube_slab_executor_capabilities": mosaic_cube_slab_plan.get(
            "executor_capabilities"
        ),
        "mosaic_cube_slab_nplanes": mosaic_cube_slab_plan.get("nplanes"),
        "mosaic_cube_slab_active_planes": mosaic_cube_slab_plan.get("active_planes"),
        "mosaic_cube_slab_count": mosaic_cube_slab_plan.get("slab_count"),
        "mosaic_cube_slab_worker_count": mosaic_cube_slab_plan.get("worker_count"),
        "mosaic_cube_slab_source_reuse": mosaic_cube_slab_plan.get("source_reuse"),
        "mosaic_cube_slab_product_state": mosaic_cube_slab_plan.get("product_state"),
        "mosaic_cube_slab_plane_count": len(mosaic_cube_slab_planes),
        "mosaic_cube_slab_plane_publish_ms": sum_int_or_float_field(
            mosaic_cube_slab_planes, "publish_elapsed_ms"
        ),
        "mosaic_cube_slab_plane_worker_sum_ms": sum_int_or_float_field(
            mosaic_cube_slab_planes, "worker_elapsed_ms"
        ),
        "mosaic_cube_slab_executor_summary_count": len(
            mosaic_cube_slab_executor_summaries
        ),
        "mosaic_cube_slab_executor_elapsed_ms": sum_int_or_float_field(
            mosaic_cube_slab_executor_summaries, "elapsed_ms"
        ),
        "mosaic_cube_slab_executor_worker_sum_ms": sum_int_or_float_field(
            mosaic_cube_slab_executor_summaries, "worker_sum_ms"
        ),
        "mosaic_cube_slab_executor_worker_max_ms": max_int_or_float_field(
            mosaic_cube_slab_executor_summaries, "worker_max_ms"
        ),
        "mosaic_cube_slab_product_write_ms": sum_int_or_float_field(
            mosaic_cube_slab_executor_summaries, "product_write_ms"
        ),
        "row_block_rows": memory.get("row_block_rows"),
        "selected_channels": memory.get("selected_channels"),
        "active_rows": memory.get("rows_total"),
        "memory_target_bytes": memory.get("memory_target_bytes"),
        "planned_active_bytes": memory.get("planned_active_bytes"),
        "source_stream_buffer_bytes": memory.get("source_stream_buffer_bytes"),
        "visibility_row_channel_bytes": memory.get("visibility_row_channel_bytes"),
        "visibility_row_fixed_bytes": memory.get("visibility_row_fixed_bytes"),
        "visibility_row_fixed_resident_bytes": memory.get(
            "visibility_row_fixed_resident_bytes"
        ),
        "visibility_row_cache_overhead_bytes": memory.get(
            "visibility_row_cache_overhead_bytes"
        ),
        "modeled_source_read_bytes": memory.get("modeled_source_read_bytes"),
        "peak_rss_bytes": profile.get("peak_rss_bytes"),
        "frontend_io_time_ms": profile.get("io_time_ms"),
        "frontend_wall_to_io_ratio": profile.get("wall_to_io_ratio"),
        "gridded_samples": profile.get("gridded_samples"),
        "major_cycles": profile.get("major_cycles"),
        "minor_iterations": profile.get("minor_iterations"),
        "cube_source_row_blocks": cube_source_rows.get("blocks"),
        "cube_source_row_block_rows": cube_source_rows.get("row_block_rows"),
        "cube_source_row_blocks_wall_ms": cube_source_rows.get("wall_ms"),
        "cube_source_row_blocks_read_ms": cube_source_rows.get("read_wall_ms"),
        "cube_source_row_blocks_prepare_ms": cube_source_rows.get("prepare_ms"),
        "cube_source_row_blocks_visibility_capacity_bytes": cube_source_rows.get(
            "visibility_capacity_bytes"
        ),
        "cube_product_summary_count": len(cube_product_summaries),
        "cube_product_write_ms": sum_int_or_float_field(
            cube_product_summaries, "product_write_ms"
        ),
        "image_product_write_count": len(image_product_writes),
        "image_product_write_ms_by_suffix": sum_image_product_writes_by_suffix(
            image_product_writes, "elapsed_ms"
        ),
        "image_product_write_elements_by_suffix": sum_image_product_writes_by_suffix(
            image_product_writes, "elements"
        ),
        "image_product_write_shape_by_suffix": last_image_product_value_by_suffix(
            image_product_writes, "shape"
        ),
        "image_product_write_role_by_suffix": last_image_product_value_by_suffix(
            image_product_writes, "role"
        ),
        "cube_product_role_ms": sum_int_or_float_field(
            cube_product_summaries, "product_role_ms"
        ),
        "cube_product_psf_ms": sum_int_or_float_field(
            cube_product_summaries, "product_psf_ms"
        ),
        "cube_product_residual_ms": sum_int_or_float_field(
            cube_product_summaries, "product_residual_ms"
        ),
        "cube_product_model_ms": sum_int_or_float_field(
            cube_product_summaries, "product_model_ms"
        ),
        "cube_product_image_ms": sum_int_or_float_field(
            cube_product_summaries, "product_image_ms"
        ),
        "cube_product_sumwt_ms": sum_int_or_float_field(
            cube_product_summaries, "product_sumwt_ms"
        ),
        "cube_product_weight_ms": sum_int_or_float_field(
            cube_product_summaries, "product_weight_ms"
        ),
        "cube_product_pb_ms": sum_int_or_float_field(
            cube_product_summaries, "product_pb_ms"
        ),
        "cube_product_image_pbcor_ms": sum_int_or_float_field(
            cube_product_summaries, "product_image_pbcor_ms"
        ),
        "cube_product_bytes": sum_int_or_float_field(cube_product_summaries, "product_bytes"),
        "cube_product_groups": sum_int_or_float_field(cube_product_summaries, "product_groups"),
        "cube_product_group_planes": sum_int_or_float_field(
            cube_product_summaries, "product_group_planes"
        ),
        "cube_product_tiled_c_order_calls": sum_int_or_float_field(
            cube_product_summaries, "tiled_c_order_calls"
        ),
        "cube_product_tiled_fortran_calls": sum_int_or_float_field(
            cube_product_summaries, "tiled_fortran_calls"
        ),
        "cube_product_tiled_tile_visits": sum_int_or_float_field(
            cube_product_summaries, "tiled_tile_visits"
        ),
        "cube_product_tiled_copied_elements": sum_int_or_float_field(
            cube_product_summaries, "tiled_copied_elements"
        ),
        "cube_product_tiled_lru_zero_fill_tiles": sum_int_or_float_field(
            cube_product_summaries, "tiled_lru_zero_fill_tiles"
        ),
        "cube_product_tiled_lru_batch_flush_tiles": sum_int_or_float_field(
            cube_product_summaries, "tiled_lru_batch_flush_tiles"
        ),
        "cube_product_tiled_lru_batch_flush_bytes": sum_int_or_float_field(
            cube_product_summaries, "tiled_lru_batch_flush_bytes"
        ),
        "cube_product_tiled_direct_write_calls": sum_int_or_float_field(
            cube_product_summaries, "tiled_direct_write_calls"
        ),
        "cube_product_tiled_direct_write_tiles": sum_int_or_float_field(
            cube_product_summaries, "tiled_direct_write_tiles"
        ),
        "cube_product_tiled_direct_write_bytes": sum_int_or_float_field(
            cube_product_summaries, "tiled_direct_write_bytes"
        ),
        "cube_product_tiled_direct_pack_ns": sum_int_or_float_field(
            cube_product_summaries, "tiled_direct_pack_ns"
        ),
        "cube_product_tiled_direct_swap_ns": sum_int_or_float_field(
            cube_product_summaries, "tiled_direct_swap_ns"
        ),
        "cube_product_tiled_direct_write_ns": sum_int_or_float_field(
            cube_product_summaries, "tiled_direct_write_ns"
        ),
        "cube_plane_state_store_count": len(cube_plane_state_store),
        "cube_plane_state_store_bytes_read": sum_int_or_float_field(
            cube_plane_state_store, "bytes_read"
        ),
        "cube_plane_state_store_bytes_written": sum_int_or_float_field(
            cube_plane_state_store, "bytes_written"
        ),
        "cube_plane_state_store_elapsed_ms": sum_int_or_float_field(
            cube_plane_state_store, "elapsed_ms"
        ),
        "cube_plane_state_store_kind": last_cube_plane_state_store.get("kind"),
        "cube_plane_state_store_cleanup_policy": last_cube_plane_state_store.get(
            "cleanup_policy"
        ),
        "visibility_geometry_cache_enabled": last_visibility_geometry_cache.get(
            "enabled"
        ),
        "visibility_geometry_cache_budget_bytes": last_visibility_geometry_cache.get(
            "budget_bytes"
        ),
        "visibility_geometry_cache_resident_bytes": last_visibility_geometry_cache.get(
            "resident_bytes"
        ),
        "visibility_geometry_cache_entries": last_visibility_geometry_cache.get(
            "entries"
        ),
        "visibility_geometry_cache_fills": last_visibility_geometry_cache.get("fills"),
        "visibility_geometry_cache_hits": last_visibility_geometry_cache.get("hits"),
        "visibility_geometry_cache_misses": last_visibility_geometry_cache.get(
            "misses"
        ),
        "visibility_geometry_cache_shares": last_visibility_geometry_cache.get(
            "shares"
        ),
        "visibility_geometry_cache_bypasses": last_visibility_geometry_cache.get(
            "bypasses"
        ),
        "visibility_geometry_cache_rejected_model_dependent": last_visibility_geometry_cache.get(
            "rejected_model_dependent"
        ),
        "visibility_geometry_cache_elapsed_ms": last_visibility_geometry_cache.get(
            "elapsed_ms"
        ),
        "metal_diagnostic_count": len(metal_entries),
        "clean_residual_refresh_calls": len(clean_residual_refresh),
        "clean_residual_refresh_backend": last_field_value(
            clean_residual_refresh, "residual_backend"
        ),
        "clean_residual_refresh_ms": sum_int_or_float_field(
            clean_residual_refresh, "refresh_ms"
        ),
        "clean_residual_refresh_accounted_ms": sum_int_or_float_field(
            clean_residual_refresh, "accounted_ms"
        ),
        "clean_residual_refresh_overhead_ms": sum_int_or_float_field(
            clean_residual_refresh, "overhead_ms"
        ),
        "clean_residual_refresh_model_fft_ms": sum_int_or_float_field(
            clean_residual_refresh, "model_fft_ms"
        ),
        "clean_residual_refresh_residual_degrid_grid_ms": sum_int_or_float_field(
            clean_residual_refresh, "residual_degrid_grid_ms"
        ),
        "clean_residual_refresh_residual_fft_ms": sum_int_or_float_field(
            clean_residual_refresh, "residual_fft_ms"
        ),
        "clean_residual_refresh_residual_normalize_ms": sum_int_or_float_field(
            clean_residual_refresh, "residual_normalize_ms"
        ),
        "metal_residual_refresh_calls": len(metal_residual_refresh),
        "metal_residual_refresh_prepare_plus_dispatch_ms": sum_int_or_float_field(
            metal_residual_refresh, "prepare_plus_dispatch_ms"
        ),
        "metal_residual_refresh_dispatch_wait_ms": sum_int_or_float_field(
            metal_residual_refresh, "dispatch_wait_ms"
        ),
        "metal_residual_refresh_dispatch_gpu_ms": sum_int_or_float_field(
            metal_residual_refresh, "dispatch_gpu_ms"
        ),
        "metal_residual_refresh_dispatch_kernel_ms": sum_int_or_float_field(
            metal_residual_refresh, "dispatch_kernel_ms"
        ),
        "metal_residual_refresh_readback_ms": sum_int_or_float_field(
            metal_residual_refresh, "readback_ms"
        ),
        "metal_residual_refresh_chunks": sum_int_or_float_field(
            metal_residual_refresh, "chunks"
        ),
        "metal_residual_refresh_runs": sum_int_or_float_field(
            metal_residual_refresh, "runs"
        ),
        "metal_residual_refresh_logical_lanes": sum_int_or_float_field(
            metal_residual_refresh, "logical_lanes"
        ),
        "metal_residual_refresh_group_descs": sum_int_or_float_field(
            metal_residual_refresh, "group_descs"
        ),
        "metal_residual_refresh_lane_refs": sum_int_or_float_field(
            metal_residual_refresh, "lane_refs"
        ),
        "metal_residual_refresh_input_cache_hits": sum_int_or_float_field(
            metal_residual_refresh, "input_cache_hit"
        ),
        "metal_residual_refresh_input_cache_fills": sum_int_or_float_field(
            metal_residual_refresh, "input_cache_fill"
        ),
        "metal_residual_refresh_input_cache_chunks": sum_int_or_float_field(
            metal_residual_refresh, "input_cache_chunks"
        ),
        "metal_residual_refresh_input_cache_host_bytes": sum_int_or_float_field(
            metal_residual_refresh, "input_cache_host_bytes"
        ),
        "metal_residual_refresh_model_pack_ms": sum_int_or_float_field(
            metal_residual_refresh_detail, "model_pack_ms"
        ),
        "metal_residual_refresh_model_buffer_ms": sum_int_or_float_field(
            metal_residual_refresh_detail, "model_buffer_ms"
        ),
        "metal_residual_refresh_density_buffer_ms": sum_int_or_float_field(
            metal_residual_refresh_detail, "density_buffer_ms"
        ),
        "metal_residual_refresh_grid_buffer_ms": sum_int_or_float_field(
            metal_residual_refresh_detail, "grid_buffer_ms"
        ),
        "metal_residual_refresh_replay_ms": sum_int_or_float_field(
            metal_residual_refresh_detail, "replay_ms"
        ),
        "metal_residual_refresh_append_total_ms": sum_int_or_float_field(
            metal_residual_refresh_detail, "append_total_ms"
        ),
        "metal_residual_refresh_dispatch_input_buffers_ms": sum_int_or_float_field(
            metal_residual_refresh_detail, "dispatch_input_buffers_ms"
        ),
        "metal_residual_refresh_dispatch_params_buffer_ms": sum_int_or_float_field(
            metal_residual_refresh_detail, "dispatch_params_buffer_ms"
        ),
        "metal_residual_refresh_dispatch_encode_ms": sum_int_or_float_field(
            metal_residual_refresh_detail, "dispatch_encode_ms"
        ),
        "metal_residual_refresh_detail_dispatch_wait_ms": sum_int_or_float_field(
            metal_residual_refresh_detail, "dispatch_wait_ms"
        ),
        "metal_residual_refresh_detail_dispatch_gpu_ms": sum_int_or_float_field(
            metal_residual_refresh_detail, "dispatch_gpu_ms"
        ),
        "metal_residual_refresh_detail_dispatch_kernel_ms": sum_int_or_float_field(
            metal_residual_refresh_detail, "dispatch_kernel_ms"
        ),
        "metal_residual_refresh_detail_readback_ms": sum_int_or_float_field(
            metal_residual_refresh_detail, "readback_ms"
        ),
        "metal_residual_refresh_staged_bytes": sum_int_or_float_field(
            metal_residual_refresh_detail, "staged_bytes"
        ),
        "metal_residual_refresh_candidate_tap_visits": sum_int_or_float_field(
            metal_residual_refresh_detail, "candidate_tap_visits"
        ),
        "metal_residual_refresh_candidate_model_reads": sum_int_or_float_field(
            metal_residual_refresh_detail, "candidate_model_reads"
        ),
        "metal_residual_refresh_exact_candidate_grid_atomic_adds": sum_int_or_float_field(
            metal_residual_refresh_detail, "exact_candidate_grid_atomic_adds"
        ),
        "metal_residual_refresh_grouped_candidate_grid_atomic_adds": sum_int_or_float_field(
            metal_residual_refresh_detail, "grouped_candidate_grid_atomic_adds"
        ),
        "metal_residual_refresh_grouped_candidate_scan_tests": sum_int_or_float_field(
            metal_residual_refresh_detail, "grouped_candidate_scan_tests"
        ),
        "metal_residual_refresh_unsupported_runs": sum_int_or_float_field(
            metal_residual_refresh_detail, "unsupported_runs"
        ),
        "metal_grouped_append_setup_ms": sum_int_or_float_field(
            metal_grouped_append_detail, "setup_ms"
        ),
        "metal_grouped_append_lane_push_ms": sum_int_or_float_field(
            metal_grouped_append_detail, "lane_push_ms"
        ),
        "metal_grouped_append_data_flag_copy_ms": sum_int_or_float_field(
            metal_grouped_append_detail, "data_flag_copy_ms"
        ),
        "metal_grouped_append_run_desc_ms": sum_int_or_float_field(
            metal_grouped_append_detail, "run_desc_ms"
        ),
        "metal_grouped_append_group_assign_ms": sum_int_or_float_field(
            metal_grouped_append_detail, "group_assign_ms"
        ),
        "metal_grouped_append_group_finalize_ms": sum_int_or_float_field(
            metal_grouped_append_detail, "group_finalize_ms"
        ),
        "executor_limitation_materialization": executor_limitation.get(
            "materialization"
        ),
        "executor_limitation_reason": executor_limitation.get("reason"),
        "spectral_active_planes": spectral_plan.get("active_planes"),
        "spectral_slab_count": spectral_plan.get("slab_count"),
        "spectral_schedule": spectral_plan.get("schedule"),
        "spectral_best_modeled_schedule": spectral_plan.get("best_modeled_schedule"),
        "spectral_executor_capabilities": spectral_plan.get("executor_capabilities"),
        "spectral_cache_budget_bytes": spectral_plan.get("cache_budget_bytes"),
        "spectral_visibility_cache_policy": spectral_plan.get("visibility_cache_policy"),
        "spectral_prepared_residency": spectral_plan.get("prepared_residency"),
        "spectral_visibility_cache_bytes": spectral_plan.get("visibility_cache_bytes"),
        "spectral_product_batch_planes": spectral_plan.get("product_batch_planes"),
        "spectral_source_channel_visits": spectral_plan.get("source_channel_visits"),
        "spectral_max_slab_source_channels": spectral_plan.get("max_slab_source_channels"),
        "spectral_full_source_channel_count": spectral_plan.get("full_source_channel_count"),
        "spectral_source_cell_channel_count": spectral_plan.get("source_cell_channel_count"),
        "spectral_visibility_row_channel_bytes": spectral_plan.get(
            "visibility_row_channel_bytes"
        ),
        "spectral_visibility_row_fixed_bytes": spectral_plan.get(
            "visibility_row_fixed_bytes"
        ),
        "spectral_visibility_row_fixed_resident_bytes": spectral_plan.get(
            "visibility_row_fixed_resident_bytes"
        ),
        "spectral_visibility_row_cache_overhead_bytes": spectral_plan.get(
            "visibility_row_cache_overhead_bytes"
        ),
        "spectral_visibility_resident_cache_layout": spectral_plan.get(
            "visibility_resident_cache_layout"
        ),
        "spectral_data_channel_read_granularity": spectral_plan.get(
            "data_channel_read_granularity"
        ),
        "spectral_flag_channel_read_granularity": spectral_plan.get(
            "flag_channel_read_granularity"
        ),
        "spectral_weight_spectrum_channel_read_granularity": spectral_plan.get(
            "weight_spectrum_channel_read_granularity"
        ),
        "spectral_best_modeled_total_io_bytes": spectral_plan.get(
            "best_modeled_total_io_bytes"
        ),
        "spectral_best_modeled_source_read_bytes": spectral_plan.get(
            "best_modeled_source_read_bytes"
        ),
        "spectral_best_modeled_visibility_cache_io_bytes": spectral_plan.get(
            "best_modeled_visibility_cache_io_bytes"
        ),
        "spectral_best_modeled_output_spill_io_bytes": spectral_plan.get(
            "best_modeled_output_spill_io_bytes"
        ),
        "spectral_best_modeled_product_write_bytes": spectral_plan.get(
            "best_modeled_product_write_bytes"
        ),
        "spectral_best_modeled_active_planes": spectral_plan.get(
            "best_modeled_active_planes"
        ),
        "spectral_best_modeled_slab_count": spectral_plan.get("best_modeled_slab_count"),
        "spectral_best_modeled_source_channel_visits": spectral_plan.get(
            "best_modeled_source_channel_visits"
        ),
        "spectral_modeled_total_io_bytes": spectral_plan.get("modeled_total_io_bytes"),
        "spectral_modeled_source_read_bytes": spectral_plan.get("modeled_source_read_bytes"),
        "spectral_modeled_visibility_cache_fill_bytes": spectral_plan.get(
            "modeled_visibility_cache_fill_bytes"
        ),
        "spectral_modeled_visibility_cache_read_bytes": spectral_plan.get(
            "modeled_visibility_cache_read_bytes"
        ),
        "spectral_modeled_visibility_cache_io_bytes": spectral_plan.get(
            "modeled_visibility_cache_io_bytes"
        ),
        "spectral_modeled_output_spill_read_bytes": spectral_plan.get(
            "modeled_output_spill_read_bytes"
        ),
        "spectral_modeled_output_spill_write_bytes": spectral_plan.get(
            "modeled_output_spill_write_bytes"
        ),
        "spectral_modeled_output_spill_io_bytes": spectral_plan.get(
            "modeled_output_spill_io_bytes"
        ),
        "spectral_modeled_product_write_bytes": spectral_plan.get(
            "modeled_product_write_bytes"
        ),
        "spectral_modeled_no_cache_source_read_bytes": spectral_plan.get(
            "modeled_no_cache_source_read_bytes"
        ),
        "spectral_modeled_full_cache_source_read_bytes": spectral_plan.get(
            "modeled_full_cache_source_read_bytes"
        ),
        "spectral_visibility_cache_saved_read_bytes": spectral_plan.get(
            "visibility_cache_saved_read_bytes"
        ),
        "spectral_candidate_io_costs": spectral_plan.get("candidate_io_costs"),
        "spectral_backend": spectral_plan.get("backend"),
        "spectral_memory_max_current_rss_bytes": max_current_rss,
        "spectral_memory_max_peak_rss_bytes": max_peak_rss,
        "spectral_memory_max_delta_from_baseline_bytes": max_baseline_delta,
        "spectral_memory_max_delta_from_previous_bytes": max_previous_delta_entry.get(
            "delta_from_previous_bytes"
        ),
        "spectral_memory_max_delta_stage": max_previous_delta_entry.get("stage"),
        "spectral_memory_max_delta_slab_id": max_previous_delta_entry.get("slab_id"),
    }


def last_fields(entries: list[dict[str, Any]]) -> dict[str, Any]:
    if not entries:
        return {}
    value = entries[-1].get("fields", {})
    return value if isinstance(value, dict) else {}


def unique_entries_by_raw(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    raw_entries = [entry.get("raw") for entry in entries]
    half = len(raw_entries) // 2
    if half > 0 and len(raw_entries) % 2 == 0 and raw_entries[:half] == raw_entries[half:]:
        return entries[:half]
    return entries


def fields_for_names(
    entries: list[dict[str, Any]], names: set[str]
) -> list[dict[str, Any]]:
    return [
        entry.get("fields", {})
        for entry in entries
        if entry.get("name") in names and isinstance(entry.get("fields", {}), dict)
    ]


def max_int_field(entries: list[dict[str, Any]], field: str) -> int | None:
    values = [entry.get(field) for entry in entries if isinstance(entry.get(field), int)]
    return max(values) if values else None


def max_int_or_float_field(
    entries: list[dict[str, Any]], field: str
) -> int | float | None:
    values = [
        entry.get(field)
        for entry in entries
        if isinstance(entry.get(field), int | float)
        and not isinstance(entry.get(field), bool)
    ]
    return max(values) if values else None


def sum_int_or_float_field(entries: list[dict[str, Any]], field: str) -> int | float | None:
    values = [
        entry.get(field)
        for entry in entries
        if isinstance(entry.get(field), int | float)
    ]
    return sum(values) if values else None


def sum_image_product_writes_by_suffix(
    entries: list[dict[str, Any]], field: str
) -> dict[str, int | float]:
    totals: dict[str, int | float] = {}
    for entry in entries:
        suffix = entry.get("suffix")
        value = entry.get(field)
        if not isinstance(suffix, str) or not isinstance(value, int | float):
            continue
        totals[suffix] = totals.get(suffix, 0) + value
    return totals


def last_image_product_value_by_suffix(
    entries: list[dict[str, Any]], field: str
) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for entry in entries:
        suffix = entry.get("suffix")
        if not isinstance(suffix, str):
            continue
        value = entry.get(field)
        if value is not None:
            values[suffix] = value
    return values


def count_positive_field(entries: list[dict[str, Any]], field: str) -> int:
    return sum(
        1
        for entry in entries
        if isinstance(entry.get(field), int | float)
        and not isinstance(entry.get(field), bool)
        and entry[field] > 0
    )


def compact_value_counts(entries: list[dict[str, Any]], field: str) -> str | None:
    counts = Counter(
        str(entry[field])
        for entry in entries
        if field in entry and entry[field] is not None
    )
    if not counts:
        return None
    return ",".join(f"{key}:{counts[key]}" for key in sorted(counts))


def last_field_value(entries: list[dict[str, Any]], field: str) -> Any:
    for entry in reversed(entries):
        if field in entry:
            return entry[field]
    return None


def max_entry_by_int_field(entries: list[dict[str, Any]], field: str) -> dict[str, Any]:
    candidates = [entry for entry in entries if isinstance(entry.get(field), int)]
    if not candidates:
        return {}
    return max(candidates, key=lambda entry: entry[field])


def build_benchmark_feature_summary(
    plan: dict[str, Any], parsed: dict[str, Any] | None
) -> dict[str, Any]:
    mode = plan.get("mode", {})
    comparison = plan.get("comparison", {})
    environment = plan.get("environment", {})
    backend_logs = (parsed or {}).get("backend_plan_logs", {})
    backend_summary = backend_logs.get("summary", {}) if isinstance(backend_logs, dict) else {}
    stages = ((parsed or {}).get("stage_medians_ms") or {}).get("rust", {})
    image_shape = mode.get("image_shape") or [None, None]
    imsize_x = int(image_shape[0]) if image_shape and image_shape[0] is not None else None
    imsize_y = int(image_shape[1]) if len(image_shape) > 1 and image_shape[1] is not None else imsize_x
    selected_channels = first_int(
        backend_summary.get("selected_channels"),
        source_channel_width(mode),
        mode.get("channel_count"),
    )
    selected_rows = first_int(backend_summary.get("active_rows"))
    gridded_samples = first_int(backend_summary.get("gridded_samples"))
    correlations = planned_correlation_count(plan)
    flagged_fraction = None
    if gridded_samples is not None and selected_rows and selected_channels and correlations:
        denominator = selected_rows * selected_channels * correlations
        if denominator > 0:
            flagged_fraction = max(0.0, min(1.0, 1.0 - (gridded_samples / denominator)))
    visibility_work = None
    if selected_rows is not None and selected_channels is not None and correlations is not None:
        visibility_work = selected_rows * selected_channels * correlations
        if flagged_fraction is not None:
            visibility_work = int(round(visibility_work * (1.0 - flagged_fraction)))
    product_count = len(comparison.get("products") or [])
    output_planes = planned_output_planes(mode)
    image_work = None
    if imsize_x is not None and imsize_y is not None:
        image_work = imsize_x * imsize_y * output_planes * max(1, product_count)
    source_stream_throughput = None
    prepare_ms = sum(
        float(stages.get(name, 0.0) or 0.0)
        for name in ("get_ms_values_into_processing_buffer", "prepare_processing_buffer")
    )
    if prepare_ms > 0 and visibility_work:
        source_stream_throughput = visibility_work / (prepare_ms / 1000.0)
    return {
        "schema_version": 1,
        "visibility": {
            "selected_rows": selected_rows,
            "selected_channels": selected_channels,
            "correlations": correlations,
            "correlation_source": "scalar-plane-after-polarization-collapse",
            "flagged_fraction": finite_float(flagged_fraction),
            "visibility_work": visibility_work,
            "gridded_samples": gridded_samples,
            "source_stream_throughput_samples_per_s": finite_float(source_stream_throughput),
        },
        "image": {
            "imsize_x": imsize_x,
            "imsize_y": imsize_y,
            "output_planes": output_planes,
            "product_count": product_count,
            "image_work": image_work,
        },
        "mode_cost": {
            "specmode": mode.get("specmode"),
            "gridder": mode.get("gridder"),
            "deconvolver": mode.get("deconvolver"),
            "weighting": mode.get("weighting"),
            "niter": mode.get("niter"),
            "cycleniter": plan.get("command", {}).get("env", {}).get("IMAGER_BENCH_MINOR_CYCLE_LENGTH"),
            "actual_major_cycles": backend_summary.get("major_cycles"),
            "actual_minor_iterations": backend_summary.get("minor_iterations"),
            "multiscale_scale_count": multiscale_scale_count(plan),
            "mtmfs_nterms": mode.get("nterms") if mode.get("deconvolver") == "mtmfs" else None,
            "wprojplanes": mode.get("wprojplanes"),
            "mosaic_field_count": field_count(plan),
        },
        "resources": {
            "physical_cores": environment.get("physical_cores"),
            "logical_cores": environment.get("logical_cores"),
            "available_parallelism": environment.get("logical_cores"),
            "physical_memory_bytes": environment.get("physical_memory_bytes"),
            "memory_target_bytes": backend_summary.get("memory_target_bytes"),
            "planned_active_bytes": backend_summary.get("planned_active_bytes"),
            "memory_headroom_bytes": memory_headroom_bytes(backend_summary),
            "row_block_count": row_block_count(selected_rows, backend_summary.get("row_block_rows")),
            "row_block_rows": backend_summary.get("row_block_rows"),
            "peak_rss_bytes": backend_summary.get("peak_rss_bytes"),
            "metal_device": backend_summary.get("metal_device_available"),
            "metal_grouped_input_cache": backend_summary.get("metal_grouped_input_cache"),
        },
        "backend": {
            "requested_acceleration": mode.get("standard_mfs_acceleration"),
            "resolved_backend": backend_summary.get("resolved_backend"),
            "resolved_grid_threads": backend_summary.get("resolved_grid_threads"),
            "resolved_tile_anchor": backend_summary.get("resolved_tile_anchor"),
            "resolved_residual_backend": backend_summary.get("resolved_residual_backend"),
            "resolved_initial_dirty_backend": backend_summary.get("resolved_initial_dirty_backend"),
            "cube_per_plane_backend": backend_summary.get("cube_per_plane_backend"),
            "cube_per_plane_phase": backend_summary.get("cube_per_plane_phase"),
            "cube_per_plane_workers": backend_summary.get("cube_per_plane_workers"),
            "cube_per_plane_grid_threads": backend_summary.get(
                "cube_per_plane_grid_threads"
            ),
            "cube_per_plane_fixed_tile_eligible": backend_summary.get(
                "cube_per_plane_fixed_tile_eligible"
            ),
            "cube_per_plane_metal_eligible": backend_summary.get(
                "cube_per_plane_metal_eligible"
            ),
            "cube_per_plane_fallback_reasons": backend_summary.get(
                "cube_per_plane_fallback_reasons"
            ),
            "mosaic_cube_slab_schedule": backend_summary.get(
                "mosaic_cube_slab_schedule"
            ),
            "mosaic_cube_slab_executor_capabilities": backend_summary.get(
                "mosaic_cube_slab_executor_capabilities"
            ),
            "mosaic_cube_slab_nplanes": backend_summary.get("mosaic_cube_slab_nplanes"),
            "mosaic_cube_slab_active_planes": backend_summary.get(
                "mosaic_cube_slab_active_planes"
            ),
            "mosaic_cube_slab_count": backend_summary.get("mosaic_cube_slab_count"),
            "mosaic_cube_slab_worker_count": backend_summary.get(
                "mosaic_cube_slab_worker_count"
            ),
            "mosaic_cube_slab_source_reuse": backend_summary.get(
                "mosaic_cube_slab_source_reuse"
            ),
            "mosaic_cube_slab_product_state": backend_summary.get(
                "mosaic_cube_slab_product_state"
            ),
            "cpu_multi_worker_reason": backend_summary.get("single_plane_reason", {}).get("cpu_multi_worker")
            if isinstance(backend_summary.get("single_plane_reason"), dict)
            else None,
            "gpu_metal_reason": backend_summary.get("single_plane_reason", {}).get("gpu_metal")
            if isinstance(backend_summary.get("single_plane_reason"), dict)
            else None,
        },
    }


def source_channel_width(mode: dict[str, Any]) -> int | None:
    width = mode.get("width")
    if isinstance(width, str) and width.isdigit():
        return int(width)
    value = mode.get("channel_count")
    return value if isinstance(value, int) else None


def planned_correlation_count(plan: dict[str, Any]) -> int:
    return 1


def planned_output_planes(mode: dict[str, Any]) -> int:
    if mode.get("deconvolver") == "mtmfs":
        return max(1, int(mode.get("nterms") or 1))
    if mode.get("specmode") in {"cube", "cubedata"}:
        return max(1, int(mode.get("channel_count") or 1))
    return 1


def multiscale_scale_count(plan: dict[str, Any]) -> int | None:
    env_value = plan.get("command", {}).get("env", {}).get("IMAGER_BENCH_SCALES")
    if not env_value:
        return None
    return len([part for part in str(env_value).split(",") if part != ""])


def field_count(plan: dict[str, Any]) -> int | None:
    field = plan.get("command", {}).get("env", {}).get("IMAGER_BENCH_FIELD")
    if field is None or field == "":
        return None
    return len([part for part in str(field).split(",") if part != ""])


def memory_headroom_bytes(summary: dict[str, Any]) -> int | None:
    target = first_int(summary.get("memory_target_bytes"))
    active = first_int(summary.get("planned_active_bytes"))
    if target is None or active is None:
        return None
    return target - active


def row_block_count(selected_rows: int | None, row_block_rows: Any) -> int | None:
    block_rows = first_int(row_block_rows)
    if selected_rows is None or not block_rows:
        return None
    return int(math.ceil(selected_rows / block_rows))


def first_int(*values: Any) -> int | None:
    for value in values:
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def finite_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def timing_result(
    runs: list[float], median: float | None, *, missing_reason: str
) -> dict[str, Any]:
    status = "ran" if median is not None else "missing"
    return {
        "status": status,
        "reason": None if status == "ran" else missing_reason,
        "timings_seconds": {"runs": runs, "median": median},
    }


def attach_stage_breakdown(plan: dict[str, Any], parsed: dict[str, Any]) -> None:
    medians = parsed.get("stage_medians_ms", {})
    parsed["stage_breakdown"] = {
        "schema_version": 1,
        "units": "milliseconds",
        "instrumentation_scope": "benchmark-harness",
        "contract_review": (
            "local benchmark result JSON only; no provider protocol or managed-output "
            "schema change"
        ),
        "rust": build_rust_stage_breakdown(plan, medians.get("rust", {})),
        "casa": build_casa_stage_breakdown(medians.get("casa", {})),
    }


def empty_stage_breakdown(reason: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "units": "milliseconds",
        "instrumentation_scope": "benchmark-harness",
        "rust": {"status": "skipped", "reason": reason, "categories": {}},
        "casa": {"status": "skipped", "reason": reason, "categories": {}},
    }


def build_rust_stage_breakdown(plan: dict[str, Any], stages: dict[str, float]) -> dict[str, Any]:
    dirty_only = plan["mode"]["bench_mode"] == "dirty" or plan["mode"]["niter"] == 0
    categories = {
        "frontend_ms_preparation": stage_category(
            stages,
            ["open_measurement_set", "prepare_plane_input", "extract_phase_center"],
            "MS open, selection, row adaptation, and phase-center resolution.",
        ),
        "visibility_adaptation_and_chunking": stage_category(
            stages,
            ["prepare_plane_input"],
            "Visibility adaptation before entering the imaging core.",
        ),
        "standard_mfs_source_read": stage_category(
            stages,
            ["get_ms_values_into_processing_buffer"],
            "Prepared source reads from MS and table columns. Accepts the legacy raw timer key.",
            skipped="get_ms_values_into_processing_buffer" not in stages,
            skip_reason="not reported for this preparation path",
        ),
        "standard_mfs_source_prepare": stage_category(
            stages,
            ["prepare_processing_buffer"],
            "Prepared-source adaptation into imaging batches.",
            skipped="prepare_processing_buffer" not in stages,
            skip_reason="not reported for this preparation path",
        ),
        "weighting_density_setup": stage_category(
            stages,
            ["weighting"],
            "Imaging weights, density grids, and taper setup.",
        ),
        "projection_pb_cf_preparation": stage_category(
            stages,
            [],
            "No dedicated Wave 1 casa-rs timing field yet; projection/PB setup is included in the gridding/PB product paths where applicable.",
            skipped=True,
        ),
        "gridding_degridding": stage_category(
            stages,
            ["psf_grid", "residual_degrid_grid"],
            "PSF gridding plus residual degrid/grid work.",
        ),
        "fft": stage_category(
            stages,
            ["psf_fft", "model_fft", "residual_fft"],
            "PSF, model, and residual FFT work.",
        ),
        "normalization_pb_correction": stage_category(
            stages,
            ["psf_normalize", "residual_normalize"],
            "PSF and residual normalization; PB correction is included when the selected mode produces PB products.",
        ),
        "deconvolution_minor_cycle": stage_category(
            stages,
            ["minor_cycle_solve"],
            "Minor-cycle component selection and subtraction.",
            skipped=dirty_only,
            skip_reason="dirty-only or niter=0 workload",
        ),
        "model_prediction_and_residual_refresh": stage_category(
            stages,
            ["major_cycle_refresh"],
            "Major-cycle model prediction and residual refresh aggregate.",
            skipped=dirty_only,
            skip_reason="dirty-only or niter=0 workload",
        ),
        "restore_and_beam_fit": stage_category(
            stages,
            ["beam_fit", "restore"],
            "Restoring-beam fit and restored-image generation.",
            skipped=dirty_only,
            skip_reason="dirty-only or niter=0 workload",
        ),
        "coordinate_and_product_writeback": stage_category(
            stages,
            ["build_coordinate_system", "write_products"],
            "Output coordinate construction and image product writeback.",
        ),
        "preview_sidecar_generation": stage_category(
            stages,
            [],
            "The benchmark script passes --no-preview-pngs, so preview sidecars are disabled.",
            skipped=True,
            skip_reason="disabled by benchmark harness",
        ),
        "frontend_total": stage_category(
            stages,
            ["frontend_total"],
            "Total frontend wallclock from the Rust profiler.",
        ),
        "core_total": stage_category(
            stages,
            ["total"],
            "Total pure imaging-core wallclock from the Rust profiler.",
        ),
    }
    return {"status": "reported" if stages else "missing", "categories": categories}


def build_casa_stage_breakdown(stages: dict[str, float]) -> dict[str, Any]:
    categories = {
        "setup_and_tool_construction": stage_category(
            stages,
            [
                "parameter_setup",
                "construct_imager",
                "initialize_imagers",
                "initialize_normalizers",
                "initialize_deconvolvers",
                "initialize_iteration_control",
                "estimate_memory",
            ],
            "CASA PySynthesisImager setup, construction, initialization, and memory estimation.",
        ),
        "ms_selection_and_image_definition": stage_category(
            stages,
            [
                "select_data",
                "define_image",
                "normalizer_info",
                "cf_cache_setup",
            ],
            (
                "Narrow CASA helper timings for synthesisimager.selectdata, "
                "defineimage, normalizerinfo, and CF-cache setup during initializeImagers."
            ),
        ),
        "weighting_density_setup": stage_category(
            stages,
            ["set_weighting", "set_weighting_core"],
            (
                "CASA weighting setup; set_weighting_core is the direct "
                "synthesisimager.setweighting call when the phase probe is enabled."
            ),
        ),
        "cube_major_cycle_algorithm_envelope": stage_category(
            stages,
            ["make_psf", "calcres_major_cycle", "clean_major_cycle"],
            (
                "CASA cube major-cycle envelope. For cube imaging this brackets "
                "CubeMajorCycleAlgorithm work including C++ tuneSelectData, "
                "nSubCubeFitInMemory, gridding/degridding, normalization, and "
                "subimage writeback that are not exposed as separate Python timers."
            ),
        ),
        "psf_and_primary_beam": stage_category(
            stages,
            ["make_psf", "make_pb"],
            "CASA PSF and PB construction.",
        ),
        "major_cycle_residual": stage_category(
            stages,
            ["calcres_major_cycle", "clean_major_cycle"],
            "CASA residual major-cycle and clean major-cycle work.",
        ),
        "deconvolution_minor_cycle": stage_category(
            stages,
            ["minor_cycle", "update_mask", "has_converged"],
            (
                "CASA minor-cycle, mask update, and convergence checks. For cube "
                "imaging minor_cycle brackets CubeMinorCycleAlgorithm."
            ),
        ),
        "image_store_writeback_and_restore": stage_category(
            stages,
            ["clean_major_cycle", "calcres_major_cycle", "restore_images"],
            (
                "CASA image-store writeback envelope: cube subimage writes occur "
                "inside major-cycle C++ calls, while final restored-image writes "
                "are included in restore_images."
            ),
        ),
        "restore_and_cleanup": stage_category(
            stages,
            ["restore_images", "delete_tools"],
            "CASA restore and tool cleanup.",
        ),
        "total": stage_category(stages, ["total"], "CASA phase probe total."),
    }
    return {"status": "reported" if stages else "missing", "categories": categories}


def stage_category(
    stages: dict[str, float],
    fields: list[str],
    description: str,
    *,
    skipped: bool = False,
    skip_reason: str | None = None,
) -> dict[str, Any]:
    components = {field: stages[field] for field in fields if field in stages}
    if skipped and not components:
        return {
            "status": "skipped",
            "reason": skip_reason or description,
            "total_ms": None,
            "components_ms": {},
            "source_fields": fields,
            "description": description,
        }
    if not fields:
        return {
            "status": "not_reported",
            "reason": description,
            "total_ms": None,
            "components_ms": {},
            "source_fields": [],
            "description": description,
        }
    missing = [field for field in fields if field not in stages]
    if components:
        total = sum(components.values())
        status = "measured" if total > 0 else "measured_zero"
        if skipped and total == 0:
            status = "skipped"
        return {
            "status": status,
            "reason": skip_reason if status == "skipped" else None,
            "total_ms": total,
            "components_ms": components,
            "source_fields": fields,
            "missing_fields": missing,
            "description": description,
        }
    return {
        "status": "missing",
        "reason": f"no source timing fields found: {', '.join(fields)}",
        "total_ms": None,
        "components_ms": {},
        "source_fields": fields,
        "missing_fields": missing,
        "description": description,
    }


def parse_product_paths(text: str) -> dict[str, str]:
    paths = {}
    for key in ("product_root", "rust_prefix", "casa_prefix"):
        match = re.search(rf"^\s*{key}=(.+)$", text, flags=re.MULTILINE)
        if match:
            paths[key] = match.group(1).strip()
    return paths


def compare_products(
    plan: dict[str, Any], parsed: dict[str, Any], log_path: pathlib.Path
) -> dict[str, Any]:
    product_paths = parsed.get("product_paths", {})
    rust_prefix = product_paths.get("rust_prefix") or plan.get("products", {}).get("rust_prefix")
    casa_prefix = product_paths.get("casa_prefix") or plan.get("products", {}).get("casa_prefix")
    casa_python = plan.get("environment", {}).get("casa_python")
    if not rust_prefix or not casa_prefix:
        return {
            "status": "skipped",
            "reason": "benchmark did not preserve product prefixes",
            "products": {},
        }
    if not casa_python:
        return {
            "status": "skipped",
            "reason": "CASA Python is required for CASA image product comparison",
            "products": {},
        }

    comparison_root = plan.get("artifacts", {}).get("comparison_root")
    panel_dir = (
        pathlib.Path(comparison_root) / "panels"
        if comparison_root
        else log_path.with_suffix(".panels")
    )
    request = {
        "rust_prefix": rust_prefix,
        "casa_prefix": casa_prefix,
        "products": plan["comparison"]["products"],
        "max_elements_per_product": plan["comparison"]["max_elements_per_product"],
        "panel_dir": str(panel_dir),
    }
    request_path = log_path.with_suffix(".comparison-input.json")
    output_path = log_path.with_suffix(".comparison.json")
    script_path = log_path.with_suffix(".compare-products.py")
    request_path.write_text(json.dumps(request, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    script_path.write_text(PRODUCT_COMPARISON_SCRIPT, encoding="utf-8")
    completed = subprocess.run(
        [casa_python, str(script_path), str(request_path), str(output_path)],
        cwd=REPO_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    comparison_log_path = log_path.with_suffix(".comparison.log")
    comparison_log_path.write_text(completed.stdout, encoding="utf-8")
    if completed.returncode != 0:
        return {
            "status": "failed",
            "reason": f"product comparison exited {completed.returncode}",
            "log": str(comparison_log_path),
            "products": {},
        }
    try:
        comparison = json.loads(output_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return {
            "status": "failed",
            "reason": f"read product comparison output: {error}",
            "log": str(comparison_log_path),
            "products": {},
        }
    comparison["log"] = str(comparison_log_path)
    comparison["input"] = str(request_path)
    comparison["panel_dir"] = str(panel_dir)
    return comparison


def parse_timing_section(text: str, heading: str) -> tuple[list[float], float | None]:
    lines = timing_section_lines(text, f"{heading} ")
    runs = []
    median_value = None
    for line in lines:
        run_match = re.search(r"\brun=\d+\s+real=([0-9.]+)", line)
        if run_match:
            runs.append(float(run_match.group(1)))
        median_match = re.search(r"\bmedian=([0-9.]+)", line)
        if median_match:
            median_value = float(median_match.group(1))
    if median_value is None and runs:
        median_value = statistics.median(runs)
    return runs, median_value


def timing_section_lines(text: str, heading_prefix: str) -> list[str]:
    boundaries = (
        "Rust release CLI timings ",
        "Rust stage medians ",
        "CASA tclean timings ",
        "Kept benchmark products:",
        "CASA PySynthesisImager stage medians ",
    )
    lines = text.splitlines()
    result = []
    collecting = False
    for line in lines:
        if line.startswith(heading_prefix):
            collecting = True
            continue
        if collecting and any(
            line.startswith(boundary)
            for boundary in boundaries
            if boundary != heading_prefix
        ):
            break
        if collecting:
            result.append(line.strip())
    return result


def parse_stage_section(text: str, heading: str) -> dict[str, float]:
    lines = timing_section_lines(text, f"{heading} ")
    stages: dict[str, float] = {}
    for line in lines:
        for name, value in re.findall(r"([A-Za-z0-9_]+)=([0-9.]+)", line):
            if heading == "Rust stage medians":
                name = RUST_STAGE_FIELD_ALIASES.get(name, name)
            if heading == "Rust stage medians" and name not in RUST_STAGE_FIELDS:
                continue
            if (
                heading == "CASA PySynthesisImager stage medians"
                and name not in CASA_STAGE_FIELDS
            ):
                continue
            if name != "run":
                stages[name] = float(value)
    return stages


def parse_casa_clean_control_diagnostics(text: str) -> list[Any]:
    for raw_line in text.splitlines():
        line = raw_line.strip()
        prefix = "clean_control_diagnostics_json="
        if line.startswith(prefix):
            try:
                value = json.loads(line[len(prefix) :])
            except json.JSONDecodeError:
                return []
            return value if isinstance(value, list) else []
    return []


def section_lines(text: str, heading_prefix: str) -> list[str]:
    lines = text.splitlines()
    result = []
    collecting = False
    for line in lines:
        if line.startswith(heading_prefix):
            collecting = True
            continue
        if collecting and line.strip() == "":
            break
        if collecting:
            result.append(line.strip())
    return result


def resolve_dataset_path(dataset: dict[str, Any], *, dry_run: bool) -> pathlib.Path:
    if "path" in dataset:
        path = pathlib.Path(os.path.expanduser(required_str(dataset, "path")))
    else:
        root_env = str_value(dataset, "root_env", "CASA_RS_TESTDATA_ROOT")
        root = os.environ.get(root_env)
        if not root:
            if dry_run:
                root = f"${root_env}"
            else:
                raise HarnessError(f"{root_env} is required for dataset {dataset.get('key')!r}")
        path = pathlib.Path(root) / required_str(dataset, "relative_path")
    if not dry_run and not path.is_dir():
        raise HarnessError(f"dataset path does not exist: {path}")
    return path


def collect_environment(casa_python: str | None) -> dict[str, Any]:
    return {
        "repo_root": str(REPO_ROOT),
        "git_commit": git_value(["rev-parse", "HEAD"]),
        "git_branch": git_value(["branch", "--show-current"]),
        "python": sys.version.split()[0],
        "casa_python": casa_python,
        "bench_script": str(BENCH_SCRIPT),
        "bench_script_sha256": file_sha256(BENCH_SCRIPT),
        "physical_cores": physical_core_count(),
        "logical_cores": os.cpu_count(),
        "physical_memory_bytes": physical_memory_bytes(),
    }


def git_value(args: list[str]) -> str | None:
    completed = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def file_sha256(path: pathlib.Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def physical_core_count() -> int | None:
    value = sysctl_int("hw.physicalcpu")
    return value or os.cpu_count()


def physical_memory_bytes() -> int | None:
    return sysctl_int("hw.memsize") or sysconf_int("SC_PAGE_SIZE", "SC_PHYS_PAGES")


def sysctl_int(name: str) -> int | None:
    try:
        completed = subprocess.run(
            ["sysctl", "-n", name],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    text = completed.stdout.strip()
    return int(text) if text.isdigit() else None


def sysconf_int(page_size_name: str, page_count_name: str) -> int | None:
    try:
        page_size = os.sysconf(page_size_name)
        page_count = os.sysconf(page_count_name)
    except (AttributeError, OSError, ValueError):
        return None
    if not isinstance(page_size, int) or not isinstance(page_count, int):
        return None
    return page_size * page_count


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: pathlib.Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def required_object(obj: dict[str, Any], key: str) -> dict[str, Any]:
    value = obj.get(key)
    if not isinstance(value, dict):
        raise HarnessError(f"missing object field {key!r}")
    return value


def object_value(obj: dict[str, Any], key: str) -> dict[str, Any]:
    value = obj.get(key, {})
    if not isinstance(value, dict):
        raise HarnessError(f"{key!r} must be an object")
    return value


def string_map_value(obj: dict[str, Any], key: str) -> dict[str, str]:
    value = obj.get(key, {})
    if not isinstance(value, dict):
        raise HarnessError(f"{key!r} must be an object")
    result = {}
    for item_key, item_value in value.items():
        if not isinstance(item_key, str) or not isinstance(item_value, str):
            raise HarnessError(f"{key!r} must contain only string keys and values")
        result[item_key] = item_value
    return result


def required_str(obj: dict[str, Any], key: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value:
        raise HarnessError(f"missing string field {key!r}")
    return value


def str_value(obj: dict[str, Any], key: str, default: str) -> str:
    value = obj.get(key, default)
    if not isinstance(value, str):
        raise HarnessError(f"{key!r} must be a string")
    return value


def int_value(obj: dict[str, Any], key: str, default: int) -> int:
    value = obj.get(key, default)
    if not isinstance(value, int):
        raise HarnessError(f"{key!r} must be an integer")
    return value


def optional_int_string(obj: dict[str, Any], key: str) -> str:
    value = obj.get(key)
    if value is None:
        return ""
    if isinstance(value, bool) or not isinstance(value, int):
        raise HarnessError(f"{key!r} must be an integer or null")
    return str(value)


def float_value(obj: dict[str, Any], key: str, default: float) -> float:
    value = obj.get(key, default)
    if not isinstance(value, (int, float)):
        raise HarnessError(f"{key!r} must be numeric")
    return float(value)


def boolean_env_value(obj: dict[str, Any], key: str, default: bool) -> str:
    value = obj.get(key, default)
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in SUPPORTED_BOOLEAN_FLAGS:
            return "1" if normalized in {"1", "true", "yes", "on"} else "0"
    raise HarnessError(f"{key!r} must be a boolean")


def bool_value(obj: dict[str, Any], key: str, default: bool) -> bool:
    return boolean_env_value(obj, key, default) == "1"


def enum_value(obj: dict[str, Any], key: str, allowed: set[str]) -> str:
    return enum_value_default(obj, key, None, allowed)


def enum_value_default(
    obj: dict[str, Any], key: str, default: str | None, allowed: set[str]
) -> str:
    value = obj.get(key, default)
    if not isinstance(value, str) or value not in allowed:
        allowed_text = ", ".join(sorted(allowed))
        raise HarnessError(f"{key!r} must be one of: {allowed_text}")
    return value


def scales_value(imaging: dict[str, Any]) -> str:
    value = imaging.get("scales", "")
    if value == "":
        return ""
    if not isinstance(value, list) or not all(isinstance(item, int) for item in value):
        raise HarnessError("'scales' must be a list of integers")
    return ",".join(str(item) for item in value)


def product_suffixes_value(comparison: dict[str, Any]) -> list[str]:
    value = comparison.get("products", DEFAULT_COMPARISON_PRODUCTS)
    if not isinstance(value, list) or not value:
        raise HarnessError("'comparison.products' must be a non-empty list")
    result = []
    for item in value:
        if not isinstance(item, str) or not item.startswith("."):
            raise HarnessError("'comparison.products' values must be suffix strings")
        result.append(item)
    return result


PRODUCT_COMPARISON_SCRIPT = r'''#!/usr/bin/env python3
import json
import math
import os
import sys

import numpy as np
from casatools import image

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception as error:
    plt = None
    MATPLOTLIB_ERROR = str(error)
else:
    MATPLOTLIB_ERROR = None


def main():
    with open(sys.argv[1], "r", encoding="utf-8") as handle:
        request = json.load(handle)
    products = {}
    os.makedirs(request["panel_dir"], exist_ok=True)
    max_elements = int(request["max_elements_per_product"])
    beam_info = estimate_beam_info(request["casa_prefix"] + ".psf", max_elements)
    panel_displays = product_panel_displays(request, max_elements)
    for suffix in request["products"]:
        rust_path = request["rust_prefix"] + suffix
        casa_path = request["casa_prefix"] + suffix
        products[suffix] = compare_one(
            rust_path,
            casa_path,
            max_elements,
            request["panel_dir"],
            suffix,
            beam_info,
            panel_displays.get(suffix),
        )
    output = {
        "status": "completed",
        "beam_info": beam_info,
        "products": products,
        "structured_difference_review": summarize_product_reviews(products),
    }
    with open(sys.argv[2], "w", encoding="utf-8") as handle:
        json.dump(output, handle, indent=2, sort_keys=True)
        handle.write("\n")


def summarize_product_reviews(products):
    product_labels = {}
    product_summaries = {}
    check_labels = {}
    for suffix, product in sorted(products.items()):
        if not isinstance(product, dict):
            continue
        structure = product.get("structured_difference")
        if not isinstance(structure, dict):
            product_labels[suffix] = product.get("status", "unknown")
            continue
        review = structure.get("review")
        if not isinstance(review, dict):
            product_labels[suffix] = structure.get("status", "unknown")
            continue
        product_labels[suffix] = review.get("label", "unknown")
        product_summaries[suffix] = review.get("summary")
        for check in review.get("checks", []):
            if not isinstance(check, dict):
                continue
            name = check.get("name")
            if not isinstance(name, str):
                continue
            check_labels.setdefault(name, {})[suffix] = check.get("label", "unknown")
    overall = worst_review_label(product_labels.values())
    return {
        "label": overall,
        "summary": structured_difference_rollup_summary(overall, product_labels),
        "products": product_labels,
        "product_summaries": product_summaries,
        "checks_by_product": check_labels,
        "thresholds": structured_difference_thresholds(),
        "legend": structured_difference_review_legend(),
    }


def structured_difference_rollup_summary(overall, product_labels):
    if not product_labels:
        return "No structured-difference product reviews were available."
    grouped = {
        label: [suffix for suffix, product_label in product_labels.items() if product_label == label]
        for label in ("bad", "investigate", "good", "unknown")
    }
    parts = []
    for label in ("bad", "investigate", "good", "unknown"):
        suffixes = grouped[label]
        if suffixes:
            parts.append(f"{label}: {', '.join(suffixes)}")
    return f"overall {overall}; " + "; ".join(parts)


def product_panel_displays(request, max_elements):
    displays = {}
    if ".model" not in request["products"]:
        return displays
    restored = restored_model_panel_display(
        rust_prefix=request["rust_prefix"],
        casa_prefix=request["casa_prefix"],
        max_elements=max_elements,
    )
    if restored is not None:
        displays[".model"] = restored
    return displays


def restored_model_panel_display(rust_prefix, casa_prefix, max_elements):
    required = {
        "rust_image": rust_prefix + ".image",
        "rust_residual": rust_prefix + ".residual",
        "casa_image": casa_prefix + ".image",
        "casa_residual": casa_prefix + ".residual",
    }
    missing = [path for path in required.values() if not os.path.isdir(path)]
    if missing:
        return {
            "status": "unavailable",
            "reason": "restored model visualization requires .image and .residual",
            "missing_paths": missing,
        }
    try:
        rust_image = load_image_display_plane(required["rust_image"], max_elements)
        rust_residual = load_image_display_plane(required["rust_residual"], max_elements)
        casa_image = load_image_display_plane(required["casa_image"], max_elements)
        casa_residual = load_image_display_plane(required["casa_residual"], max_elements)
    except Exception as error:
        return {
            "status": "unavailable",
            "reason": f"failed to load restored model visualization inputs: {error}",
        }
    inputs = [rust_image, rust_residual, casa_image, casa_residual]
    shapes = [item["shape"] for item in inputs]
    strides = [item["sample_stride"] for item in inputs]
    if any(shape != shapes[0] for shape in shapes):
        return {
            "status": "unavailable",
            "reason": "restored model visualization inputs have mismatched shapes",
            "shapes": shapes,
        }
    if any(stride != strides[0] for stride in strides):
        return {
            "status": "unavailable",
            "reason": "restored model visualization inputs have mismatched sampling strides",
            "sample_strides": strides,
        }
    rust_display = rust_image["data"] - rust_residual["data"]
    casa_display = casa_image["data"] - casa_residual["data"]
    return {
        "status": "available",
        "rust_data": rust_display,
        "casa_data": casa_display,
        "diff_data": rust_display - casa_display,
        "transform": "restored_model_from_image_minus_residual",
        "description": ".model visualized as restoring-beam-convolved model via .image - .residual",
        "product_label": ".model restored",
        "value_label": "Jy/beam",
        "shape": shapes[0],
        "sample_stride": strides[0],
    }


def compare_one(rust_path, casa_path, max_elements, panel_dir, suffix, beam_info, panel_display=None):
    if not os.path.isdir(rust_path) or not os.path.isdir(casa_path):
        return {
            "status": "missing",
            "rust_path": rust_path,
            "casa_path": casa_path,
            "rust_exists": os.path.isdir(rust_path),
            "casa_exists": os.path.isdir(casa_path),
        }
    rust = load_image(rust_path, max_elements)
    casa = load_image(casa_path, max_elements)
    if rust["shape"] != casa["shape"]:
        return {
            "status": "shape_mismatch",
            "rust_path": rust_path,
            "casa_path": casa_path,
            "rust_shape": rust["shape"],
            "casa_shape": casa["shape"],
        }
    rust_data = rust["data"]
    casa_data = casa["data"]
    mask = np.isfinite(rust_data) & np.isfinite(casa_data)
    valid_count = int(np.count_nonzero(mask))
    if valid_count == 0:
        return {
            "status": "no_finite_overlap",
            "rust_path": rust_path,
            "casa_path": casa_path,
            "shape": rust["shape"],
            "sample_stride": rust["sample_stride"],
            "sampled_elements": int(rust_data.size),
        }
    rust_valid = rust_data[mask]
    casa_valid = casa_data[mask]
    diff = rust_valid - casa_valid
    casa_peak = max(abs(float(np.nanmin(casa_valid))), abs(float(np.nanmax(casa_valid))))
    casa_rms = rms(casa_valid)
    diff_rms = rms(diff)
    diff_abs_max = float(np.nanmax(np.abs(diff)))
    correlation = correlation_value(rust_valid, casa_valid)
    rust_peak = peak_summary(rust_data)
    casa_peak_summary = peak_summary(casa_data)
    diff_full = rust_data - casa_data
    diff_peak = peak_summary(diff_full)
    structure = structured_difference_metrics(
        suffix=suffix,
        rust_data=rust_data,
        casa_data=casa_data,
        diff_data=diff_full,
        beam_info=beam_info,
    )
    panel_display_data = panel_display
    if panel_display_data is None:
        rust_display = load_image_display_plane(rust_path, max_elements)
        casa_display = load_image_display_plane(casa_path, max_elements)
        panel_display_data = {
            "status": "available",
            "rust_data": rust_display["data"],
            "casa_data": casa_display["data"],
            "diff_data": rust_display["data"] - casa_display["data"],
            "transform": "center_plane_full_spatial_display",
            "description": (
                "center display plane loaded with spatial-only stride; "
                "non-spatial axes fixed at their center"
            ),
            "shape": rust_display["shape"],
            "display_bounds": rust_display["display_bounds"],
            "sample_stride": rust_display["sample_stride"],
        }
    panel = write_review_panel(
        panel_dir=panel_dir,
        suffix=suffix,
        rust_data=rust_data,
        casa_data=casa_data,
        diff_data=diff_full,
        review=structure.get("review") if isinstance(structure, dict) else None,
        display=panel_display_data,
    )
    return {
        "status": "compared",
        "rust_path": rust_path,
        "casa_path": casa_path,
        "shape": rust["shape"],
        "sample_stride": rust["sample_stride"],
        "sampled_elements": int(rust_data.size),
        "finite_overlap": valid_count,
        "rust_min": finite_float(np.nanmin(rust_valid)),
        "rust_max": finite_float(np.nanmax(rust_valid)),
        "rust_rms": finite_float(rms(rust_valid)),
        "casa_min": finite_float(np.nanmin(casa_valid)),
        "casa_max": finite_float(np.nanmax(casa_valid)),
        "casa_rms": finite_float(casa_rms),
        "diff_abs_max": finite_float(diff_abs_max),
        "diff_rms": finite_float(diff_rms),
        "diff_rms_over_casa_rms": finite_float(diff_rms / abs(casa_rms)) if casa_rms else None,
        "diff_abs_max_over_casa_peak": finite_float(diff_abs_max / casa_peak) if casa_peak else None,
        "correlation": finite_float(correlation) if correlation is not None else None,
        "rust_peak_abs": rust_peak,
        "casa_peak_abs": casa_peak_summary,
        "diff_peak_abs": diff_peak,
        "structured_difference": structure,
        "review_panel": panel,
    }


def estimate_beam_info(psf_path, max_elements):
    if not os.path.isdir(psf_path):
        return {"status": "missing_psf", "psf_path": psf_path}
    try:
        psf = load_image(psf_path, max_elements)
    except Exception as error:
        return {"status": "failed", "psf_path": psf_path, "reason": str(error)}
    plane = display_plane(psf["data"])
    finite = np.isfinite(plane)
    if not np.any(finite):
        return {"status": "no_finite_psf", "psf_path": psf_path}
    peak_index = np.unravel_index(int(np.nanargmax(np.abs(plane))), plane.shape)
    peak_abs = float(abs(plane[peak_index]))
    if not np.isfinite(peak_abs) or peak_abs <= 0.0:
        return {"status": "zero_psf_peak", "psf_path": psf_path}
    half = 0.5 * peak_abs
    x_width = contiguous_threshold_width(np.abs(plane[:, peak_index[1]]), peak_index[0], half)
    y_width = contiguous_threshold_width(np.abs(plane[peak_index[0], :]), peak_index[1], half)
    beam_area = math.pi * x_width * y_width / (4.0 * math.log(2.0))
    block_side = max(1, int(round(math.sqrt(max(1.0, beam_area)))))
    return {
        "status": "estimated_from_psf",
        "psf_path": psf_path,
        "sample_stride": psf["sample_stride"],
        "peak_location": [int(value) for value in peak_index],
        "peak_abs": finite_float(peak_abs),
        "fwhm_pixels": [int(x_width), int(y_width)],
        "beam_area_pixels": finite_float(beam_area),
        "beam_block_side_pixels": int(block_side),
    }


def contiguous_threshold_width(values, center, threshold):
    center = int(center)
    lower = center
    upper = center
    while lower > 0 and values[lower - 1] >= threshold:
        lower -= 1
    while upper + 1 < values.size and values[upper + 1] >= threshold:
        upper += 1
    return int(upper - lower + 1)


def structured_difference_metrics(suffix, rust_data, casa_data, diff_data, beam_info):
    rust_plane = display_plane(rust_data)
    casa_plane = display_plane(casa_data)
    diff_plane = display_plane(diff_data)
    base_mask = np.isfinite(rust_plane) & np.isfinite(casa_plane) & np.isfinite(diff_plane)
    mask, mask_description = structured_difference_mask(suffix, rust_plane, casa_plane, base_mask)
    finite_count = int(np.count_nonzero(mask))
    if finite_count == 0:
        return {
            "status": "no_masked_pixels",
            "mask": mask_description,
            "finite_overlap": int(np.count_nonzero(base_mask)),
            "beam_info_status": beam_info.get("status") if isinstance(beam_info, dict) else None,
        }

    beam_side = 1
    if isinstance(beam_info, dict) and beam_info.get("status") == "estimated_from_psf":
        beam_side = max(1, int(beam_info.get("beam_block_side_pixels") or 1))
    analysis_mask = erode_mask_for_product(mask, suffix, beam_side)
    if not np.any(analysis_mask):
        analysis_mask = mask

    diff_values = diff_plane[analysis_mask]
    casa_values = casa_plane[analysis_mask]
    flux_norm = robust_product_scale(casa_values)
    diff_rms = rms(diff_values)
    normalized_diff_rms = diff_rms / flux_norm if flux_norm else None
    if non_spatial_product(suffix):
        classification = non_spatial_difference_classification(normalized_diff_rms)
        review = structured_difference_review(
            suffix=suffix,
            classification=classification,
            normalized_diff_rms=normalized_diff_rms,
            low_order_r2=None,
            large_scale_power=None,
            block_decay_slope=None,
        )
        return {
            "status": "computed",
            "mask": mask_description,
            "masked_pixels": finite_count,
            "analysis_pixels": int(np.count_nonzero(analysis_mask)),
            "beam_block_side_pixels": int(beam_side),
            "normalization": {
                "type": "casa_support_rms_or_peak",
                "value": finite_float(flux_norm),
            },
            "diff_rms": finite_float(diff_rms),
            "normalized_diff_rms": finite_float(normalized_diff_rms),
            "low_order_r2_quadratic": None,
            "large_scale_power_fraction": None,
            "scale_offset_gradient_fit": {
                "status": "not_applicable",
                "reason": "non_spatial_product",
            },
            "beam_block_rms_by_scale": [],
            "block_rms_decay_slope_vs_independent_beams": None,
            "classification": classification,
            "review": review,
        }
    low_order_r2 = low_order_r2_score(diff_plane, analysis_mask)
    large_scale_power = large_scale_power_fraction(
        diff_plane,
        analysis_mask,
        beam_side,
        min_wavelength_beams=8.0,
    )
    basis_fit = difference_basis_fit(casa_plane, diff_plane, analysis_mask)
    block_metrics = beam_block_metrics(diff_plane, analysis_mask, beam_side, flux_norm)
    classification = structured_difference_classification(
        normalized_diff_rms=normalized_diff_rms,
        low_order_r2=low_order_r2,
        large_scale_power=large_scale_power,
        block_decay_slope=block_metrics["decay_slope"],
    )
    review = structured_difference_review(
        suffix=suffix,
        classification=classification,
        normalized_diff_rms=normalized_diff_rms,
        low_order_r2=low_order_r2,
        large_scale_power=large_scale_power,
        block_decay_slope=block_metrics["decay_slope"],
    )
    return {
        "status": "computed",
        "mask": mask_description,
        "masked_pixels": finite_count,
        "analysis_pixels": int(np.count_nonzero(analysis_mask)),
        "beam_block_side_pixels": int(beam_side),
        "normalization": {
            "type": "casa_support_rms_or_peak",
            "value": finite_float(flux_norm),
        },
        "diff_rms": finite_float(diff_rms),
        "normalized_diff_rms": finite_float(normalized_diff_rms),
        "low_order_r2_quadratic": finite_float(low_order_r2),
        "large_scale_power_fraction": large_scale_power,
        "scale_offset_gradient_fit": basis_fit,
        "beam_block_rms_by_scale": block_metrics["scales"],
        "block_rms_decay_slope_vs_independent_beams": block_metrics["decay_slope"],
        "classification": classification,
        "review": review,
    }


def structured_difference_mask(suffix, rust_plane, casa_plane, base_mask):
    if suffix == ".weight":
        scale = max(
            finite_absmax(rust_plane[base_mask]),
            finite_absmax(casa_plane[base_mask]),
        )
        if scale > 0.0:
            threshold = 1.0e-3 * scale
            return (
                base_mask & ((np.abs(rust_plane) > threshold) | (np.abs(casa_plane) > threshold)),
                {
                    "type": "weight_union_support",
                    "threshold_fraction_of_peak": 1.0e-3,
                    "threshold": finite_float(threshold),
                },
            )
    if suffix == ".pb":
        return (
            base_mask & (casa_plane > 0.01),
            {
                "type": "casa_pb_support",
                "threshold": 0.01,
            },
        )
    return base_mask, {"type": "finite_overlap"}


def non_spatial_product(suffix):
    return suffix in {".sumwt"}


def erode_mask_for_product(mask, suffix, beam_side):
    if suffix not in {".pb", ".weight"} or beam_side <= 1:
        return mask
    radius = int(max(1, round(beam_side)))
    if mask.shape[0] <= 2 * radius or mask.shape[1] <= 2 * radius:
        return mask
    eroded = np.zeros_like(mask, dtype=bool)
    eroded[radius:-radius, radius:-radius] = mask[radius:-radius, radius:-radius]
    return eroded


def robust_product_scale(values):
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return 0.0
    scale = rms(finite)
    if scale > 0.0 and np.isfinite(scale):
        return float(abs(scale))
    return finite_absmax(finite)


def finite_absmax(values):
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return 0.0
    return float(np.nanmax(np.abs(finite)))


def low_order_r2_score(data, mask):
    if int(np.count_nonzero(mask)) < 8:
        return None
    y_index, x_index = np.indices(data.shape)
    x_values = x_index[mask].astype(np.float64)
    y_values = y_index[mask].astype(np.float64)
    x_span = float(np.ptp(x_values))
    y_span = float(np.ptp(y_values))
    x = (x_values - float(np.mean(x_values))) / (0.5 * x_span if x_span else 1.0)
    y = (y_values - float(np.mean(y_values))) / (0.5 * y_span if y_span else 1.0)
    z = data[mask].astype(np.float64)
    z = z - float(np.mean(z))
    total = float(np.sum(z * z))
    if total <= 0.0 or not np.isfinite(total):
        return None
    basis = np.vstack([np.ones_like(x), x, y, x * x, x * y, y * y, x * x + y * y]).T
    coefficients, *_ = np.linalg.lstsq(basis, z, rcond=None)
    fitted = basis @ coefficients
    residual = z - fitted
    return 1.0 - float(np.sum(residual * residual)) / total


def difference_basis_fit(reference, diff, mask):
    if int(np.count_nonzero(mask)) < 8:
        return {"status": "insufficient_pixels"}
    if reference.ndim != 2 or min(reference.shape) < 2:
        return {
            "status": "insufficient_dimensions",
            "shape": [int(v) for v in reference.shape],
        }
    y_gradient, x_gradient = np.gradient(reference.astype(np.float64))
    diff_values = diff[mask].astype(np.float64)
    reference_values = reference[mask].astype(np.float64)
    basis = np.vstack(
        [
            reference_values,
            np.ones_like(reference_values),
            x_gradient[mask].astype(np.float64),
            y_gradient[mask].astype(np.float64),
        ]
    ).T
    coefficients, *_ = np.linalg.lstsq(basis, diff_values, rcond=None)
    fitted = basis @ coefficients
    residual = diff_values - fitted
    total = float(np.sum((diff_values - float(np.mean(diff_values))) ** 2))
    residual_sum = float(np.sum(residual * residual))
    r2 = 1.0 - residual_sum / total if total > 0.0 and np.isfinite(total) else None
    return {
        "status": "computed",
        "model": "diff ~= scale*reference + offset + dx*d_reference_dx + dy*d_reference_dy",
        "r2": finite_float(r2),
        "diff_rms": finite_float(rms(diff_values)),
        "residual_rms": finite_float(rms(residual)),
        "coefficients": {
            "scale": finite_float(coefficients[0]),
            "offset": finite_float(coefficients[1]),
            "dx_pixels": finite_float(coefficients[2]),
            "dy_pixels": finite_float(coefficients[3]),
        },
    }


def structured_difference_classification(
    normalized_diff_rms,
    low_order_r2,
    large_scale_power,
    block_decay_slope,
):
    amplitude = classify_amplitude(normalized_diff_rms)
    structure_components = {
        "block_rms_decay_slope_vs_independent_beams": classify_block_decay(block_decay_slope),
        "large_scale_power_fraction": classify_large_scale_power(
            large_scale_power.get("fraction") if isinstance(large_scale_power, dict) else None
        ),
        "low_order_r2_quadratic": classify_low_order_r2(low_order_r2),
    }
    numerical_floor_override = (
        normalized_diff_rms is not None and normalized_diff_rms < 1.0e-6
    )
    if numerical_floor_override and amplitude == "good":
        structure = "good"
        overall = "good"
    else:
        structure = worst_classification(structure_components.values())
        overall = overall_structured_difference_label(amplitude, structure)
    return {
        "overall": overall,
        "amplitude": amplitude,
        "structure": structure,
        "structure_components": structure_components,
        "structure_suppressed_by_numerical_floor": numerical_floor_override,
        "thresholds": structured_difference_thresholds(),
    }


def non_spatial_difference_classification(normalized_diff_rms):
    amplitude = classify_amplitude(normalized_diff_rms)
    return {
        "overall": amplitude,
        "amplitude": amplitude,
        "structure": "not_applicable",
        "structure_components": {},
        "thresholds": structured_difference_thresholds(),
    }


def structured_difference_review(
    suffix,
    classification,
    normalized_diff_rms,
    low_order_r2,
    large_scale_power,
    block_decay_slope,
):
    large_scale_fraction = (
        large_scale_power.get("fraction") if isinstance(large_scale_power, dict) else None
    )
    components = classification.get("structure_components", {})
    checks = [
        {
            "name": "normalized_diff_rms",
            "label": classification.get("amplitude", "unknown"),
            "value": finite_float(normalized_diff_rms),
            "meaning": "beam/product-scale RMS amplitude difference",
        },
        {
            "name": "block_rms_decay_slope_vs_independent_beams",
            "label": components.get(
                "block_rms_decay_slope_vs_independent_beams", "unknown"
            ),
            "value": finite_float(block_decay_slope),
            "meaning": "whether averaging over independent beams suppresses the difference",
        },
        {
            "name": "large_scale_power_fraction",
            "label": components.get("large_scale_power_fraction", "unknown"),
            "value": finite_float(large_scale_fraction),
            "meaning": "fraction of difference power on scales much larger than the beam",
        },
        {
            "name": "low_order_r2_quadratic",
            "label": components.get("low_order_r2_quadratic", "unknown"),
            "value": finite_float(low_order_r2),
            "meaning": "fraction of difference variance explained by a smooth quadratic surface",
        },
    ]
    return {
        "label": classification.get("overall", "unknown"),
        "summary": structured_difference_review_summary(suffix, classification),
        "checks": checks,
        "legend": structured_difference_review_legend(),
    }


def structured_difference_thresholds():
    return {
        "normalized_diff_rms": {
            "good": "< 1e-4",
            "numerical_floor": "< 1e-6 suppresses structure-only escalation",
            "investigate": "1e-4 .. 1e-3",
            "bad": "> 1e-3",
        },
        "block_rms_decay_slope_vs_independent_beams": {
            "good": "<= -0.35",
            "investigate": "-0.35 .. -0.15",
            "bad": "> -0.15",
        },
        "large_scale_power_fraction": {
            "good": "< 0.25",
            "investigate": "0.25 .. 0.5",
            "bad": "> 0.5",
        },
        "low_order_r2_quadratic": {
            "good": "< 0.05",
            "investigate": "0.05 .. 0.2",
            "bad": "> 0.2",
        },
    }


def structured_difference_review_legend():
    return {
        "good": "No review action expected from this check.",
        "investigate": "Plausible but needs review in context.",
        "bad": "Structured or large enough difference; do not close without explanation.",
        "unknown": "Check could not be evaluated for this product.",
    }


def structured_difference_review_summary(suffix, classification):
    overall = classification.get("overall", "unknown")
    amplitude = classification.get("amplitude", "unknown")
    structure = classification.get("structure", "unknown")
    if structure == "not_applicable":
        if overall == "good":
            return f"{suffix}: good; non-spatial product amplitude check passed."
        if overall == "bad":
            return (
                f"{suffix}: bad; non-spatial product amplitude is {amplitude}. "
                "Treat this as a correctness blocker until instrumented or explained."
            )
        if overall == "investigate":
            return f"{suffix}: investigate; non-spatial product amplitude is {amplitude}."
        return f"{suffix}: unknown; non-spatial product amplitude check did not run."
    if overall == "good":
        return f"{suffix}: good; amplitude and beam-scale structure checks passed."
    if overall == "bad":
        return (
            f"{suffix}: bad; amplitude is {amplitude} and structure is {structure}. "
            "Treat this as a correctness blocker until instrumented or explained."
        )
    if overall == "investigate":
        return (
            f"{suffix}: investigate; amplitude is {amplitude} and structure is "
            f"{structure}."
        )
    return f"{suffix}: unknown; one or more structured-difference checks did not run."


def classify_amplitude(value):
    if value is None:
        return "unknown"
    if value < 1.0e-4:
        return "good"
    if value <= 1.0e-3:
        return "investigate"
    return "bad"


def classify_block_decay(value):
    if value is None:
        return "unknown"
    if value <= -0.35:
        return "good"
    if value <= -0.15:
        return "investigate"
    return "bad"


def classify_large_scale_power(value):
    if value is None:
        return "unknown"
    if value < 0.25:
        return "good"
    if value <= 0.5:
        return "investigate"
    return "bad"


def classify_low_order_r2(value):
    if value is None:
        return "unknown"
    if value < 0.05:
        return "good"
    if value <= 0.2:
        return "investigate"
    return "bad"


def worst_classification(labels):
    rank = {"unknown": 0, "good": 1, "investigate": 2, "bad": 3}
    labels = list(labels)
    if not labels:
        return "unknown"
    return max(labels, key=lambda label: rank.get(label, 0))


def worst_review_label(labels):
    rank = {"unknown": 0, "good": 1, "investigate": 2, "bad": 3}
    labels = list(labels)
    if not labels:
        return "unknown"
    return max(labels, key=lambda label: rank.get(label, 0))


def overall_structured_difference_label(amplitude, structure):
    if amplitude == "bad":
        return "bad"
    if amplitude == "investigate" and structure == "bad":
        return "bad"
    if amplitude == "good" and structure == "good":
        return "good"
    if amplitude == "unknown" and structure == "unknown":
        return "unknown"
    return "investigate"


def large_scale_power_fraction(data, mask, beam_side, min_wavelength_beams):
    if int(np.count_nonzero(mask)) < 4:
        return None
    centered = np.asarray(data, dtype=np.float64).copy()
    centered[~mask] = np.nan
    mean = float(np.nanmean(centered))
    centered = np.where(np.isfinite(centered), centered - mean, 0.0)
    spectrum = np.fft.rfft2(centered)
    power = np.abs(spectrum) ** 2
    y_freq = np.fft.fftfreq(centered.shape[0])[:, np.newaxis]
    x_freq = np.fft.rfftfreq(centered.shape[1])[np.newaxis, :]
    radius = np.sqrt(x_freq * x_freq + y_freq * y_freq)
    dc = radius == 0.0
    total = float(np.sum(power[~dc]))
    if total <= 0.0 or not np.isfinite(total):
        return None
    cutoff = 1.0 / max(1.0, float(beam_side) * float(min_wavelength_beams))
    selected = (radius <= cutoff) & (~dc)
    return {
        "min_wavelength_beams": finite_float(min_wavelength_beams),
        "frequency_cutoff_cycles_per_pixel": finite_float(cutoff),
        "fraction": finite_float(float(np.sum(power[selected])) / total),
    }


def beam_block_metrics(data, mask, beam_side, flux_norm):
    scales = []
    block_sides = []
    normalized_rms_values = []
    independent_beam_counts = []
    for multiplier in [1, 2, 4, 8, 16, 32]:
        side = max(1, int(round(float(beam_side) * multiplier)))
        metric = block_metric_for_side(data, mask, side, beam_side, flux_norm, multiplier)
        if metric is None:
            continue
        scales.append(metric)
        normalized = metric.get("normalized_block_mean_rms")
        independent_beams = metric.get("approx_independent_beams_per_block")
        if normalized is not None and normalized > 0.0 and independent_beams and independent_beams > 0.0:
            block_sides.append(side)
            normalized_rms_values.append(float(normalized))
            independent_beam_counts.append(float(independent_beams))
    slope = None
    if len(normalized_rms_values) >= 2:
        x = np.log(np.asarray(independent_beam_counts, dtype=np.float64))
        y = np.log(np.asarray(normalized_rms_values, dtype=np.float64))
        if np.all(np.isfinite(x)) and np.all(np.isfinite(y)) and np.ptp(x) > 0.0:
            slope = float(np.polyfit(x, y, 1)[0])
    return {"scales": scales, "decay_slope": finite_float(slope) if slope is not None else None}


def block_metric_for_side(data, mask, side, beam_side, flux_norm, multiplier):
    height, width = data.shape
    means = []
    pixel_rms_values = []
    min_pixels = max(4, int(math.ceil(0.35 * side * side)))
    for y_start in range(0, height, side):
        for x_start in range(0, width, side):
            block_mask = mask[y_start : y_start + side, x_start : x_start + side]
            if int(np.count_nonzero(block_mask)) < min_pixels:
                continue
            block = data[y_start : y_start + side, x_start : x_start + side][block_mask]
            means.append(float(np.mean(block)))
            pixel_rms_values.append(rms(block))
    if len(means) < 3:
        return None
    mean_values = np.asarray(means, dtype=np.float64)
    pixel_rms_mean = float(np.mean(pixel_rms_values)) if pixel_rms_values else None
    block_mean_rms = rms(mean_values)
    robust_center = float(np.median(mean_values))
    robust_sigma = 1.4826 * float(np.median(np.abs(mean_values - robust_center)))
    max_robust_z = None
    if robust_sigma > 0.0 and np.isfinite(robust_sigma):
        max_robust_z = float(np.nanmax(np.abs(mean_values - robust_center)) / robust_sigma)
    independent_beams = (float(side) / max(1.0, float(beam_side))) ** 2
    return {
        "beam_width_multiplier": finite_float(multiplier),
        "block_side_pixels": int(side),
        "approx_independent_beams_per_block": finite_float(independent_beams),
        "n_blocks": int(len(means)),
        "block_mean_rms": finite_float(block_mean_rms),
        "normalized_block_mean_rms": finite_float(block_mean_rms / flux_norm) if flux_norm else None,
        "median_abs_block_mean": finite_float(float(np.median(np.abs(mean_values)))),
        "mean_pixel_rms_in_blocks": finite_float(pixel_rms_mean),
        "block_mean_rms_over_mean_pixel_rms": finite_float(block_mean_rms / pixel_rms_mean)
        if pixel_rms_mean
        else None,
        "max_block_robust_z": finite_float(max_robust_z) if max_robust_z is not None else None,
    }


def correlation_value(left, right):
    if left.size < 2 or right.size < 2:
        return None
    left_std = float(np.nanstd(left))
    right_std = float(np.nanstd(right))
    if left_std == 0.0 or right_std == 0.0:
        return None
    return float(np.corrcoef(left.ravel(), right.ravel())[0, 1])


def peak_summary(data):
    finite = np.isfinite(data)
    if not np.any(finite):
        return None
    filled = np.where(finite, np.abs(data), -np.inf)
    index = np.unravel_index(int(np.nanargmax(filled)), data.shape)
    return {
        "location": [int(value) for value in index],
        "value": finite_float(data[index]),
        "abs_value": finite_float(abs(data[index])),
    }


def write_review_panel(panel_dir, suffix, rust_data, casa_data, diff_data, review=None, display=None):
    if plt is None:
        return {
            "status": "skipped",
            "reason": f"matplotlib unavailable: {MATPLOTLIB_ERROR}",
        }
    display_status = "raw_product"
    display_transform = None
    display_description = None
    display_reason = None
    display_shape = None
    display_bounds = None
    display_sample_stride = None
    product_label = suffix if suffix else ".image"
    value_label = product_value_label(suffix)
    panel_rust_data = rust_data
    panel_casa_data = casa_data
    panel_diff_data = diff_data
    if isinstance(display, dict):
        if display.get("status") == "available":
            panel_rust_data = display["rust_data"]
            panel_casa_data = display["casa_data"]
            panel_diff_data = display["diff_data"]
            product_label = display.get("product_label") or product_label
            value_label = display.get("value_label") or value_label
            display_status = "derived"
            display_transform = display.get("transform")
            display_description = display.get("description")
            display_shape = display.get("shape")
            display_bounds = display.get("display_bounds")
            display_sample_stride = display.get("sample_stride")
        else:
            display_status = display.get("status", "unavailable")
            display_reason = display.get("reason")
    rust_plane = display_plane(panel_rust_data)
    casa_plane = display_plane(panel_casa_data)
    diff_plane = display_plane(panel_diff_data)
    shared = np.concatenate(
        [
            rust_plane[np.isfinite(rust_plane)].ravel(),
            casa_plane[np.isfinite(casa_plane)].ravel(),
        ]
    )
    if shared.size == 0:
        return {"status": "skipped", "reason": "no finite pixels for panel scaling"}
    image_vmin, image_vmax = panel_color_limits(shared)
    finite_diff = diff_plane[np.isfinite(diff_plane)]
    diff_abs = panel_symmetric_abs_limit(finite_diff)
    safe_name = suffix.strip(".").replace(".", "_") or "image"
    review_label = None
    review_summary = None
    if isinstance(review, dict):
        review_label = review.get("label")
        review_summary = review.get("summary")
    panel_path = os.path.join(panel_dir, f"{safe_name}.review.png")
    render_review_panel_figure(
        panel_path=panel_path,
        rust_plane=rust_plane,
        casa_plane=casa_plane,
        diff_plane=diff_plane,
        product_label=product_label,
        value_label=value_label,
        image_vmin=image_vmin,
        image_vmax=image_vmax,
        diff_abs=diff_abs,
        review_label=review_label,
    )
    zoom_panel = write_zoom_review_panel(
        panel_dir=panel_dir,
        safe_name=safe_name,
        rust_plane=rust_plane,
        casa_plane=casa_plane,
        diff_plane=diff_plane,
        product_label=product_label,
        value_label=value_label,
        review_label=review_label,
    )
    return {
        "status": "written",
        "path": panel_path,
        "casa_rs_and_casa_color_limits": [image_vmin, image_vmax],
        "difference_color_limits": [-diff_abs, diff_abs],
        "structured_difference_label": review_label,
        "structured_difference_summary": review_summary,
        "display_status": display_status,
        "display_transform": display_transform,
        "display_description": display_description,
        "display_reason": display_reason,
        "display_shape": display_shape,
        "display_bounds": display_bounds,
        "display_sample_stride": display_sample_stride,
        "zoom_panel": zoom_panel,
    }


def write_zoom_review_panel(
    panel_dir,
    safe_name,
    rust_plane,
    casa_plane,
    diff_plane,
    product_label,
    value_label,
    review_label,
):
    bounds = zoom_bounds_for_planes(rust_plane, casa_plane)
    if bounds is None:
        return {"status": "skipped", "reason": "no finite nonzero support for zoom panel"}
    x0, x1, y0, y1 = bounds
    if x0 == 0 and y0 == 0 and x1 == rust_plane.shape[0] and y1 == rust_plane.shape[1]:
        return {
            "status": "skipped",
            "reason": "zoom bounds cover the full review plane",
            "bounds": {"x_start": x0, "x_end": x1, "y_start": y0, "y_end": y1},
        }
    rust_zoom = rust_plane[x0:x1, y0:y1]
    casa_zoom = casa_plane[x0:x1, y0:y1]
    diff_zoom = diff_plane[x0:x1, y0:y1]
    shared = np.concatenate(
        [
            rust_zoom[np.isfinite(rust_zoom)].ravel(),
            casa_zoom[np.isfinite(casa_zoom)].ravel(),
        ]
    )
    if shared.size == 0:
        return {"status": "skipped", "reason": "no finite pixels for zoom panel scaling"}
    image_vmin, image_vmax = panel_color_limits(shared)
    finite_diff = diff_zoom[np.isfinite(diff_zoom)]
    diff_abs = panel_symmetric_abs_limit(finite_diff)
    zoom_path = os.path.join(panel_dir, f"{safe_name}.zoom.review.png")
    zoom_label = f"{product_label} zoom"
    render_review_panel_figure(
        panel_path=zoom_path,
        rust_plane=rust_zoom,
        casa_plane=casa_zoom,
        diff_plane=diff_zoom,
        product_label=zoom_label,
        value_label=value_label,
        image_vmin=image_vmin,
        image_vmax=image_vmax,
        diff_abs=diff_abs,
        review_label=review_label,
    )
    return {
        "status": "written",
        "path": zoom_path,
        "bounds": {"x_start": x0, "x_end": x1, "y_start": y0, "y_end": y1},
        "casa_rs_and_casa_color_limits": [image_vmin, image_vmax],
        "difference_color_limits": [-diff_abs, diff_abs],
    }


def render_review_panel_figure(
    panel_path,
    rust_plane,
    casa_plane,
    diff_plane,
    product_label,
    value_label,
    image_vmin,
    image_vmax,
    diff_abs,
    review_label,
):
    fig, axes = plt.subplots(1, 3, figsize=(13.5, 4.8), constrained_layout=True)
    if review_label:
        fig.suptitle(
            f"{product_label} structured difference: {review_label}",
            fontsize=11,
        )
    rust_artist = axes[0].imshow(
        rust_plane.T,
        origin="lower",
        vmin=image_vmin,
        vmax=image_vmax,
        aspect="equal",
    )
    axes[0].set_title(f"casa-rs {product_label}")
    casa_artist = axes[1].imshow(
        casa_plane.T,
        origin="lower",
        vmin=image_vmin,
        vmax=image_vmax,
        aspect="equal",
    )
    axes[1].set_title(f"CASA {product_label}")
    diff_artist = axes[2].imshow(
        diff_plane.T,
        origin="lower",
        vmin=-diff_abs,
        vmax=diff_abs,
        cmap="coolwarm",
        aspect="equal",
    )
    axes[2].set_title(f"difference {product_label}\n(casa-rs - CASA)")
    for axis in axes:
        axis.set_aspect("equal", adjustable="box")
        axis.set_box_aspect(1)
    fig.colorbar(rust_artist, ax=axes[0], fraction=0.046, pad=0.04, label=value_label)
    fig.colorbar(casa_artist, ax=axes[1], fraction=0.046, pad=0.04, label=value_label)
    fig.colorbar(
        diff_artist,
        ax=axes[2],
        fraction=0.046,
        pad=0.04,
        label=f"casa-rs - CASA ({value_label})",
    )
    for axis in axes:
        axis.set_xticks([])
        axis.set_yticks([])
    fig.savefig(panel_path, dpi=160)
    plt.close(fig)


def product_value_label(suffix):
    if suffix in {".image", ".residual", ".image.pbcor"}:
        return "Jy/beam"
    if suffix == ".model":
        return "Jy/pixel"
    if suffix == ".psf":
        return "PSF response"
    if suffix in {".pb", ".weight"}:
        return "relative weight"
    return "value"


def zoom_bounds_for_planes(rust_plane, casa_plane):
    if rust_plane.ndim != 2 or casa_plane.ndim != 2 or rust_plane.shape != casa_plane.shape:
        return None
    finite = np.isfinite(rust_plane) & np.isfinite(casa_plane)
    if not np.any(finite):
        return None
    amplitude = np.maximum(np.abs(rust_plane), np.abs(casa_plane))
    amplitude = np.where(finite, amplitude, 0.0)
    peak = finite_absmax(amplitude)
    if peak <= 0.0:
        return None
    support = amplitude >= peak * 1.0e-3
    if not np.any(support):
        peak_index = np.unravel_index(int(np.nanargmax(amplitude)), amplitude.shape)
        xs = np.asarray([peak_index[0]])
        ys = np.asarray([peak_index[1]])
    else:
        xs, ys = np.nonzero(support)
    height, width = rust_plane.shape
    x_min = int(np.min(xs))
    x_max = int(np.max(xs)) + 1
    y_min = int(np.min(ys))
    y_max = int(np.max(ys)) + 1
    support_side = max(x_max - x_min, y_max - y_min)
    min_side = min(min(height, width), max(32, min(height, width) // 16))
    side = min(min(height, width), max(min_side, support_side * 4))
    x_center = (x_min + x_max) // 2
    y_center = (y_min + y_max) // 2
    x0 = max(0, min(height - side, x_center - side // 2))
    y0 = max(0, min(width - side, y_center - side // 2))
    return int(x0), int(x0 + side), int(y0), int(y0 + side)


def panel_color_limits(values):
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return 0.0, 0.0
    vmin = finite_float(np.nanmin(finite))
    vmax = finite_float(np.nanmax(finite))
    if vmin is None or vmax is None:
        return 0.0, 0.0
    if vmax > vmin:
        return vmin, vmax
    abs_peak = finite_absmax(finite)
    delta = abs_peak * 1.0e-6 if abs_peak > 0.0 else 1.0
    return vmin - delta, vmax + delta


def panel_symmetric_abs_limit(values):
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        return 1.0
    abs_peak = finite_float(np.nanmax(np.abs(finite)))
    if abs_peak is None or abs_peak <= 0.0:
        return 1.0
    return abs_peak


def display_plane(data):
    plane = np.squeeze(data)
    while plane.ndim > 2:
        plane = plane[..., plane.shape[-1] // 2]
    if plane.ndim == 0:
        plane = np.asarray([[float(plane)]])
    elif plane.ndim == 1:
        plane = plane[:, np.newaxis]
    return np.asarray(plane, dtype=np.float64)


def load_image(path, max_elements):
    tool = image()
    try:
        tool.open(path)
        shape = [int(v) for v in tool.shape()]
        stride = stride_for(shape, max_elements)
        trc = [max(0, v - 1) for v in shape]
        data = tool.getchunk(
            blc=[0] * len(shape),
            trc=trc,
            inc=stride,
            dropdeg=False,
            getmask=False,
        )
    finally:
        tool.close()
    return {
        "shape": shape,
        "sample_stride": stride,
        "data": np.asarray(data, dtype=np.float64),
    }


def load_image_display_plane(path, max_elements):
    tool = image()
    try:
        tool.open(path)
        shape = [int(v) for v in tool.shape()]
        blc, trc = display_plane_bounds(shape)
        plane_shape = [shape[0] if shape else 1, shape[1] if len(shape) > 1 else 1]
        spatial_stride = stride_for(plane_shape, max_elements)
        inc = [1] * len(shape)
        if len(inc) >= 1:
            inc[0] = spatial_stride[0]
        if len(inc) >= 2:
            inc[1] = spatial_stride[1]
        data = tool.getchunk(
            blc=blc,
            trc=trc,
            inc=inc,
            dropdeg=False,
            getmask=False,
        )
    finally:
        tool.close()
    return {
        "shape": shape,
        "display_bounds": {
            "blc": blc,
            "trc": trc,
            "inc": inc,
        },
        "sample_stride": inc,
        "data": np.asarray(data, dtype=np.float64),
    }


def display_plane_bounds(shape):
    if not shape:
        return [], []
    blc = [0] * len(shape)
    trc = [max(0, int(size) - 1) for size in shape]
    for axis in range(2, len(shape)):
        center = max(0, int(shape[axis]) // 2)
        blc[axis] = center
        trc[axis] = center
    return blc, trc


def stride_for(shape, max_elements):
    if max_elements < 1:
        raise ValueError("max_elements_per_product must be >= 1")
    stride = [1] * len(shape)
    sampled = product(shape)
    if len(stride) >= 2:
        while sampled > max_elements and (shape[0] > stride[0] or shape[1] > stride[1]):
            stride[0] += 1
            stride[1] += 1
            sampled = product(math.ceil(size / step) for size, step in zip(shape, stride))
    index = 2 if len(stride) > 2 else 0
    while sampled > max_elements:
        stride[index % len(stride)] += 1
        sampled = product(math.ceil(size / step) for size, step in zip(shape, stride))
        index += 1
    return stride


def product(values):
    result = 1
    for value in values:
        result *= int(value)
    return result


def rms(values):
    return float(np.sqrt(np.nanmean(values * values)))


def finite_float(value):
    if value is None:
        return None
    value = float(value)
    return value if math.isfinite(value) else None


if __name__ == "__main__":
    main()
'''


if __name__ == "__main__":
    main()
