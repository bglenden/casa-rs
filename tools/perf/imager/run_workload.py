#!/usr/bin/env python3
"""Run manifest-driven CASA C++ versus casa-rs imaging benchmarks."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import re
import statistics
import subprocess
import sys
import uuid
from typing import Any


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
RUNNABLE_GRIDDER_VALUES = {"mosaic", "standard", "wproject"}
SUPPORTED_SPEC_MODES = {"cubedata", "cube", "mfs"}
RUNNABLE_SPEC_MODES = {"cube", "mfs"}
SUPPORTED_BENCH_MODES = {"dirty", "clean"}
SUPPORTED_INTERPOLATION = {"nearest", "linear"}
SUPPORTED_HOGBOM_ITERATION_MODES = {"strict", "casa", "casa-inclusive", "casa_inclusive"}
SUPPORTED_MS_STAGING = {"copy", "direct"}
SUPPORTED_BOOLEAN_FLAGS = {"0", "1", "false", "true", "no", "yes", "off", "on"}
DEFAULT_COMPARISON_PRODUCTS = [".image", ".residual", ".psf"]
RUST_STAGE_FIELDS = {
    "open_measurement_set",
    "prepare_plane_input",
    "get_ms_values_into_processing_buffer",
    "prepare_processing_buffer",
    "extract_phase_center",
    "run_imaging",
    "build_coordinate_system",
    "write_products",
    "frontend_total",
    "controller_overhead",
    "weighting",
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


class HarnessError(Exception):
    """Error that should be shown without a Python traceback."""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workload", help="workload manifest id or JSON path")
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=pathlib.Path("target/imperformance-wave1"),
        help="directory for result JSON and benchmark log",
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
    args = parser.parse_args()

    try:
        manifest_path = resolve_workload(args.workload)
        manifest = load_manifest(manifest_path)
        plan = build_plan(
            manifest_path=manifest_path,
            manifest=manifest,
            repeats_override=args.repeats,
            run_label_override=args.run_label,
            storage_label_override=args.storage_label,
            dry_run=args.dry_run,
        )
        output_dir = args.output_dir.resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        attach_output_paths(plan, output_dir, dry_run=args.dry_run)
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


def build_plan(
    *,
    manifest_path: pathlib.Path,
    manifest: dict[str, Any],
    repeats_override: int | None,
    run_label_override: str | None,
    storage_label_override: str | None,
    dry_run: bool,
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
        "IMAGER_BENCH_INTERPOLATION": interpolation,
        "IMAGER_BENCH_FIELD": str_value(imaging, "field", "0"),
        "IMAGER_BENCH_PHASECENTER_FIELD": optional_int_string(imaging, "phasecenter_field"),
        "IMAGER_BENCH_SPW": str_value(imaging, "spw", "0"),
        "IMAGER_BENCH_CHANNEL_START": str(int_value(imaging, "channel_start", 0)),
        "IMAGER_BENCH_CHANNEL_COUNT": str(int_value(imaging, "channel_count", 1)),
        "IMAGER_BENCH_IMSIZE": str(int_value(imaging, "imsize", 128)),
        "IMAGER_BENCH_CELL_ARCSEC": str(float_value(imaging, "cell_arcsec", 30.0)),
        "IMAGER_BENCH_WEIGHTING": str_value(imaging, "weighting", "natural"),
        "IMAGER_BENCH_ROBUST": str(float_value(imaging, "robust", 0.5)),
        "IMAGER_BENCH_DECONVOLVER": str_value(imaging, "deconvolver", "hogbom"),
        "IMAGER_BENCH_STANDARD_MFS_ACCELERATION": str_value(
            imaging, "standard_mfs_acceleration", "auto"
        ),
        "IMAGER_BENCH_HOGBOM_ITERATION_MODE": hogbom_iteration_mode,
        "IMAGER_BENCH_NTERMS": str(int_value(imaging, "nterms", 1)),
        "IMAGER_BENCH_SCALES": scales_value(imaging),
        "IMAGER_BENCH_WTERM": wterm,
        "IMAGER_BENCH_NITER": str(int_value(imaging, "niter", 4)),
        "IMAGER_BENCH_GAIN": str(float_value(imaging, "gain", 0.1)),
        "IMAGER_BENCH_THRESHOLD_JY": str(float_value(imaging, "threshold_jy", 0.0)),
        "IMAGER_BENCH_NSIGMA": str(float_value(imaging, "nsigma", 0.0)),
        "IMAGER_BENCH_PSFCUTOFF": str(float_value(imaging, "psfcutoff", 0.35)),
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
    }

    command = [str(BENCH_SCRIPT), str(dataset_path)]
    return {
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
            "weighting": str_value(imaging, "weighting", "natural"),
            "deconvolver": str_value(imaging, "deconvolver", "hogbom"),
            "standard_mfs_acceleration": str_value(
                imaging, "standard_mfs_acceleration", "auto"
            ),
            "hogbom_iteration_mode": hogbom_iteration_mode,
            "nterms": int_value(imaging, "nterms", 1),
            "niter": int_value(imaging, "niter", 4),
        },
        "run": {
            "repeats": repeats,
            "run_label": run_label_override or str_value(run, "run_label", "warm"),
            "storage_label": storage_label_override
            or str_value(run, "storage_label", "script-staged-tempdir"),
            "ms_staging": ms_staging,
            "phase_probe": phase_probe,
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


def attach_output_paths(plan: dict[str, Any], output_dir: pathlib.Path, *, dry_run: bool) -> None:
    product_root = output_dir / "products" / plan["run_id"]
    plan["products"] = {
        "root": None if dry_run else str(product_root),
        "rust_prefix": None if dry_run else str(product_root / "rust" / "rust"),
        "casa_prefix": None if dry_run else str(product_root / "casa" / "casa"),
    }
    if not dry_run:
        plan["command"]["env"]["IMAGER_BENCH_KEEP_OUTPUT_ROOT"] = str(product_root)


def benchmark_run_support(
    *, workload_id: str, specmode: str, gridder: str, wterm: str
) -> dict[str, Any]:
    reasons = []
    if specmode not in RUNNABLE_SPEC_MODES:
        reasons.append(f"specmode={specmode!r} needs benchmark-script execution support")
    if gridder not in RUNNABLE_GRIDDER_VALUES:
        reasons.append(f"gridder={gridder!r} needs benchmark-script execution support")
    if wterm != "none" and not (gridder == "wproject" and wterm == "wproject"):
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
    if comparison:
        panel_status, panel_reason = review_panel_status(comparison)
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
    started = utc_now()
    completed = subprocess.run(
        plan["command"]["argv"],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    log_path.write_text(completed.stdout, encoding="utf-8")
    if completed.returncode != 0:
        return {
            "schema_version": 1,
            "status": "failed",
            **plan,
            "started_at": started,
            "completed_at": utc_now(),
            "exit_code": completed.returncode,
            "results": empty_results(
                casa_status="blocked",
                reason=f"benchmark command exited {completed.returncode}",
            ),
            "human_review": human_review_gate(plan, None),
        }

    parsed = parse_benchmark_log(completed.stdout)
    attach_stage_breakdown(plan, parsed)
    comparison = compare_products(plan, parsed, log_path)
    parsed["product_comparison"] = comparison
    return {
        "schema_version": 1,
        "status": "completed",
        **plan,
        "started_at": started,
        "completed_at": utc_now(),
        "exit_code": completed.returncode,
        "results": parsed,
        "human_review": human_review_gate(plan, comparison),
    }


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
        "product_paths": parse_product_paths(text),
    }


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
        "standard_mfs_buffer_load": stage_category(
            stages,
            ["get_ms_values_into_processing_buffer"],
            "Owned standard-MFS processing-buffer loads from MS and table columns.",
            skipped="get_ms_values_into_processing_buffer" not in stages,
            skip_reason="not reported for this preparation path",
        ),
        "standard_mfs_buffer_prepare": stage_category(
            stages,
            ["prepare_processing_buffer"],
            "Standard-MFS processing-buffer adaptation into imaging batches.",
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
            "CASA PySynthesisImager setup, construction, and initialization.",
        ),
        "weighting_density_setup": stage_category(
            stages,
            ["set_weighting"],
            "CASA weighting setup.",
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
            "CASA minor-cycle, mask update, and convergence checks.",
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

    panel_dir = log_path.with_suffix(".panels")
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
            if heading == "Rust stage medians" and name not in RUST_STAGE_FIELDS:
                continue
            if name != "run":
                stages[name] = float(value)
    return stages


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
    for suffix in request["products"]:
        rust_path = request["rust_prefix"] + suffix
        casa_path = request["casa_prefix"] + suffix
        products[suffix] = compare_one(
            rust_path,
            casa_path,
            int(request["max_elements_per_product"]),
            request["panel_dir"],
            suffix,
        )
    output = {"status": "completed", "products": products}
    with open(sys.argv[2], "w", encoding="utf-8") as handle:
        json.dump(output, handle, indent=2, sort_keys=True)
        handle.write("\n")


def compare_one(rust_path, casa_path, max_elements, panel_dir, suffix):
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
    panel = write_review_panel(
        panel_dir=panel_dir,
        suffix=suffix,
        rust_data=rust_data,
        casa_data=casa_data,
        diff_data=diff_full,
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
        "review_panel": panel,
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


def write_review_panel(panel_dir, suffix, rust_data, casa_data, diff_data):
    if plt is None:
        return {
            "status": "skipped",
            "reason": f"matplotlib unavailable: {MATPLOTLIB_ERROR}",
        }
    rust_plane = display_plane(rust_data)
    casa_plane = display_plane(casa_data)
    diff_plane = display_plane(diff_data)
    shared = np.concatenate(
        [
            rust_plane[np.isfinite(rust_plane)].ravel(),
            casa_plane[np.isfinite(casa_plane)].ravel(),
        ]
    )
    if shared.size == 0:
        return {"status": "skipped", "reason": "no finite pixels for panel scaling"}
    image_vmin = finite_float(np.nanmin(shared))
    image_vmax = finite_float(np.nanmax(shared))
    finite_diff = diff_plane[np.isfinite(diff_plane)]
    diff_abs = finite_float(np.nanmax(np.abs(finite_diff))) if finite_diff.size else None
    if diff_abs is None:
        diff_abs = 0.0
    safe_name = suffix.strip(".").replace(".", "_") or "image"
    product_label = suffix if suffix else ".image"
    value_label = product_value_label(suffix)
    panel_path = os.path.join(panel_dir, f"{safe_name}.review.png")
    fig, axes = plt.subplots(1, 3, figsize=(13.5, 4.8), constrained_layout=True)
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
    return {
        "status": "written",
        "path": panel_path,
        "casa_rs_and_casa_color_limits": [image_vmin, image_vmax],
        "difference_color_limits": [-diff_abs, diff_abs],
    }


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


def display_plane(data):
    plane = np.squeeze(data)
    while plane.ndim > 2:
        plane = plane[..., 0]
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
    value = float(value)
    return value if math.isfinite(value) else None


if __name__ == "__main__":
    main()
'''


if __name__ == "__main__":
    main()
