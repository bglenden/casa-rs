#!/usr/bin/env python3
"""Run manifest-driven CASA C++ versus casa-rs imaging benchmarks."""

from __future__ import annotations

import argparse
from collections import Counter
import datetime as dt
import os
import pathlib
import re
import subprocess
import sys
import uuid
import math
from typing import Any

import perf_paths
from perf_harness import (
    ContractError,
    RUN_RESULT_SCHEMA_VERSION,
    atomic_write_json,
    finite_number,
    load_run_result,
    load_workload_manifest,
    validate_run_result,
    validate_workload_manifest,
)
from perf_harness import casa_tclean_workflow
from perf_harness.errors import HarnessError
from perf_harness.image_compare import compare_products as compare_image_products
from perf_harness.provenance import capture_provenance, executable_path
from perf_harness.stages import (
    parse_casa_clean_control_diagnostics,
    parse_stage_section,
    parse_timing_section,
)
from perf_harness.subprocesses import run_command


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
WORKLOAD_DIR = pathlib.Path(__file__).resolve().parent / "workloads"
BENCH_SCRIPT = REPO_ROOT / "scripts" / "bench-imager-vs-casa.sh"
CASA_TCLEAN_PROTOCOL = casa_tclean_workflow.CASA_TCLEAN_PROTOCOL
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
SUPPORTED_HOGBOM_ITERATION_MODES = {
    "strict",
    "casa",
    "casa-inclusive",
    "casa_inclusive",
}
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
MAX_BACKEND_LOG_ENTRIES_PER_BUCKET = 128


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "workload",
        nargs="?",
        help="workload manifest id or JSON path",
    )
    parser.add_argument(
        "--recover-receipt",
        type=pathlib.Path,
        help=(
            "recover a completed recipe bundle after outer receipt publication "
            "failed; never reinvokes CASA or the comparator"
        ),
    )
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
        "--cf-cache-root",
        type=pathlib.Path,
        default=None,
        help=(
            "durable root for CASA convolution-function caches; recipe-backed "
            "runs default to a cf-cache sibling of --artifact-root"
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
    plan: dict[str, Any] | None = None
    result_path: pathlib.Path | None = None
    log_path: pathlib.Path | None = None

    try:
        recovery_value = getattr(args, "recover_receipt", None)
        recovery_path = (
            recovery_value if isinstance(recovery_value, pathlib.Path) else None
        )
        if recovery_path is not None:
            if args.workload is not None:
                raise HarnessError(
                    "workload and --recover-receipt are mutually exclusive"
                )
            recover_recipe_receipt(recovery_path.expanduser().resolve())
            return
        if args.workload is None:
            raise HarnessError("workload is required unless --recover-receipt is used")
        manifest_path = resolve_workload(args.workload)
        manifest = load_manifest(manifest_path)
        apply_imaging_overrides(manifest, args.set_imaging)
        try:
            validate_workload_manifest(
                manifest, source=f"{manifest_path} after command-line overrides"
            )
        except ContractError as error:
            raise HarnessError(str(error)) from error
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
        cf_cache_root = (
            args.cf_cache_root.expanduser().resolve()
            if args.cf_cache_root is not None
            else casa_tclean_workflow.default_cf_cache_root(plan, artifact_root)
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        if not args.dry_run and plan["command"].get("kind") != "casa_tclean_protocol":
            perf_paths.mark_safe_to_delete(artifact_root)
        if not args.dry_run:
            casa_tclean_workflow.validate_storage_preconditions(
                plan,
                output_dir=output_dir,
                artifact_root=artifact_root,
                cf_cache_root=cf_cache_root,
            )
        result_path = output_dir / f"{plan['run_id']}.json"
        log_path = output_dir / f"{plan['run_id']}.log"
        attach_output_paths(
            plan,
            output_dir,
            artifact_root,
            cf_cache_root=cf_cache_root,
            dry_run=args.dry_run,
        )
        log_path = casa_tclean_workflow.bundle_benchmark_log_path(plan, log_path)

        if args.dry_run:
            result = {
                "schema_version": RUN_RESULT_SCHEMA_VERSION,
                "kind": "workload_run",
                "status": "dry_run",
                **plan,
                "exit_code": 0,
                "logs": casa_tclean_workflow.benchmark_log_evidence(None),
                "results": empty_results(casa_status="not_run", reason="dry run"),
                "human_review": human_review_gate(plan, None),
            }
            validate_run_result(result, source=str(result_path))
            atomic_write_json(result_path, result)
            print(result_path)
            return

        result = run_plan(plan, log_path)
        result["logs"] = casa_tclean_workflow.benchmark_log_evidence(log_path)
        if plan["command"].get("kind") == "casa_tclean_protocol":
            result = casa_tclean_workflow.finalize_bundle_result(result)
        exit_code = normalize_cli_exit_code(result)
        validate_run_result(result, source=str(result_path))
        atomic_write_json(result_path, result)
        print(result_path)
        if exit_code != 0:
            raise SystemExit(exit_code)
    except KeyboardInterrupt:
        if plan is not None and result_path is not None and log_path is not None:
            result = casa_tclean_workflow.interrupted_run_result(
                plan,
                log_path=log_path,
                reason="workload interrupted by operator before completion",
                services=recipe_execution_services(),
            )
            if plan["command"].get("kind") == "casa_tclean_protocol":
                result = casa_tclean_workflow.finalize_bundle_result(result)
            validate_run_result(result, source=str(result_path))
            atomic_write_json(result_path, result)
            print(f"interrupted receipt: {result_path}", file=sys.stderr)
        raise SystemExit(130) from None
    except HarnessError as error:
        if (
            plan is not None
            and result_path is not None
            and log_path is not None
            and plan.get("command", {}).get("kind") == "casa_tclean_protocol"
            and pathlib.Path(
                plan.get("artifacts", {}).get("bundle", {}).get("partial_root", "")
            ).is_dir()
        ):
            result = casa_tclean_workflow.failed_recipe_run_result(
                plan,
                log_path=log_path,
                reason=str(error),
                services=recipe_execution_services(),
            )
            result = casa_tclean_workflow.finalize_bundle_result(result)
            validate_run_result(result, source=str(result_path))
            atomic_write_json(result_path, result)
            print(f"failed receipt: {result_path}", file=sys.stderr)
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None
    except Exception as error:
        if (
            plan is not None
            and result_path is not None
            and log_path is not None
            and plan.get("command", {}).get("kind") == "casa_tclean_protocol"
            and pathlib.Path(
                plan.get("artifacts", {}).get("bundle", {}).get("partial_root", "")
            ).is_dir()
        ):
            try:
                result = casa_tclean_workflow.failed_recipe_run_result(
                    plan,
                    log_path=log_path,
                    reason=f"{type(error).__name__}: {error}",
                    services=recipe_execution_services(),
                    failure_kind="harness_internal",
                    exit_code=1,
                )
                result = casa_tclean_workflow.finalize_bundle_result(result)
                validate_run_result(result, source=str(result_path))
                atomic_write_json(result_path, result)
                print(f"internal-failure receipt: {result_path}", file=sys.stderr)
            except Exception as receipt_error:
                print(
                    "warning: could not retain typed internal-failure receipt: "
                    f"{type(receipt_error).__name__}: {receipt_error}",
                    file=sys.stderr,
                )
        print(f"internal error: {type(error).__name__}: {error}", file=sys.stderr)
        raise


def recover_recipe_receipt(receipt_path: pathlib.Path) -> None:
    """Publish an already-completed recipe bundle from fail-closed artifacts."""

    failed_result = load_run_result(receipt_path)
    bundle = failed_result.get("artifacts", {}).get("bundle")
    if not isinstance(bundle, dict) or not isinstance(bundle.get("partial_root"), str):
        raise HarnessError("recovery receipt does not name a partial artifact bundle")
    log_path = pathlib.Path(bundle["partial_root"]) / "benchmark-summary.log"
    try:
        recovered = casa_tclean_workflow.recover_completed_recipe_run(
            failed_result,
            log_path,
            services=recipe_execution_services(),
        )
    except casa_tclean_workflow.ProtocolError as error:
        raise HarnessError(str(error)) from error
    recovered["logs"] = casa_tclean_workflow.benchmark_log_evidence(log_path)
    recovered = casa_tclean_workflow.finalize_bundle_result(recovered)
    normalize_cli_exit_code(recovered)
    validate_run_result(recovered, source=str(receipt_path))
    atomic_write_json(receipt_path, recovered)
    if recovered.get("status") != "completed":
        failure = recovered.get("results", {}).get("failure")
        reason = failure.get("reason") if isinstance(failure, dict) else None
        raise HarnessError(
            "recovered bundle failed final publication validation"
            + (f": {reason}" if reason else "")
        )
    print(receipt_path)


CLI_SUCCESS_STATUSES = {"completed", "dry_run", "recovered_publication"}


def normalize_cli_exit_code(result: dict[str, Any]) -> int:
    """Bind process success to the durable receipt's top-level status."""

    status = result.get("status")
    if status in CLI_SUCCESS_STATUSES:
        exit_code = 0
    else:
        recorded = result.get("exit_code")
        exit_code = (
            recorded
            if isinstance(recorded, int)
            and not isinstance(recorded, bool)
            and recorded != 0
            else 1
        )
    result["exit_code"] = exit_code
    return exit_code


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
        return load_workload_manifest(path)
    except ContractError as error:
        raise HarnessError(str(error)) from error


def apply_imaging_overrides(manifest: dict[str, Any], overrides: list[str]) -> None:
    if not overrides:
        return
    if object_value(manifest, "casa"):
        raise HarnessError(
            "--set-imaging is forbidden for frozen recipe-backed evidence; "
            "add a separately reviewed non-fiducial workload instead"
        )
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
    if re.fullmatch(
        r"-?(?:[0-9]+[.][0-9]*|[0-9]*[.][0-9]+)(?:[eE][+-]?[0-9]+)?", value
    ):
        return float(value)
    if "," in value and all(
        re.fullmatch(r"-?[0-9]+", part) for part in value.split(",")
    ):
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
    casa = object_value(manifest, "casa")
    dataset = required_object(manifest, "dataset")
    imaging = required_object(manifest, "imaging")
    run = object_value(manifest, "run")
    comparison = object_value(manifest, "comparison")

    specmode = enum_value(imaging, "specmode", SUPPORTED_SPEC_MODES)
    gridder = enum_value(imaging, "gridder", SUPPORTED_GRIDDER_VALUES)
    bench_mode = enum_value(imaging, "mode", SUPPORTED_BENCH_MODES)
    interpolation = enum_value_default(
        imaging, "interpolation", "linear", SUPPORTED_INTERPOLATION
    )
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
    repeats = (
        repeats_override
        if repeats_override is not None
        else int_value(run, "repeats", 5)
    )
    if repeats < 1:
        raise HarnessError("repeats must be >= 1")
    warmups = int_value(run, "warmups", 0)
    if warmups < 0:
        raise HarnessError("run.warmups must be >= 0")
    cf_cache_role = str_value(run, "cf_cache_role", "none")
    if cf_cache_role not in {"none", "cold", "warm"}:
        raise HarnessError("run.cf_cache_role must be none, cold, or warm")
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
        raise HarnessError(
            "run.skip_profile must be 0/1, true/false, yes/no, or on/off"
        )
    reuse_rust_prefix = os.environ.get("CASA_RS_BENCH_REUSE_RUST_PREFIX") or str_value(
        run, "reuse_rust_prefix", ""
    )
    reuse_casa_prefix = os.environ.get("CASA_RS_BENCH_REUSE_CASA_PREFIX") or str_value(
        run, "reuse_casa_prefix", ""
    )
    if reuse_rust_prefix:
        skip_rust = "1"
    if reuse_casa_prefix:
        skip_casa = "1"
    recipe_path = casa_tclean_workflow.resolve_recipe_path(casa) if casa else None
    if casa:
        run_support = casa_tclean_workflow.recipe_run_support(
            workload_id=workload_id,
            imaging=imaging,
            skip_casa=boolean_flag(skip_casa),
            skip_rust=boolean_flag(skip_rust),
        )
    if not dry_run and run_support["status"] not in {"runnable", "casa_only"}:
        raise HarnessError(f"{workload_id}: {run_support['reason']}")
    profile_repeats = os.environ.get("CASA_RS_BENCH_PROFILE_REPEATS") or str(
        int_value(run, "profile_repeats", repeats)
    )
    if not profile_repeats.isdigit() or int(profile_repeats) < 1:
        raise HarnessError("run.profile_repeats must be an integer >= 1")
    extra_env = string_map_value(run, "env")
    if casa and extra_env:
        raise HarnessError(
            "recipe-backed run.env is unsupported because unbound environment "
            "values cannot be part of the frozen CASA invocation"
        )

    dataset_path = resolve_dataset_path(dataset, dry_run=dry_run)
    rust_requested = not boolean_flag(skip_rust)
    resolved_cfcache = ""
    if imaging.get("cfcache") is not None:
        resolved_cfcache = str(
            resolve_imaging_path(
                str_value(imaging, "cfcache", ""),
                dataset_path=dataset_path,
                field="imaging.cfcache",
                require_directory=not dry_run and rust_requested,
            )
        )
    if not casa and rust_requested and gridder in {
        "awproject",
        "awp2",
        "awphpg",
        "widefield",
    }:
        if gridder != "awproject":
            raise HarnessError(
                f"gridder={gridder!r} is not a Rust AWProject alias; use "
                "gridder='awproject' with an explicit imaging.cfcache"
            )
        if not resolved_cfcache:
            raise HarnessError(
                "Rust gridder='awproject' requires an explicit imaging.cfcache"
            )
    casa_python = os.environ.get("CASA_RS_CASA_PYTHON")
    if not dry_run and not casa_python:
        raise HarnessError("CASA_RS_CASA_PYTHON is required for a benchmark run")
    if not dry_run and casa_python and not pathlib.Path(casa_python).is_file():
        raise HarnessError(f"CASA_RS_CASA_PYTHON does not exist: {casa_python}")

    casa_gridder = str_value(imaging, "casa_gridder", gridder)
    wprojplanes = optional_int_string(imaging, "wprojplanes")
    casa_wprojplanes = wprojplanes
    if not casa_wprojplanes and casa_gridder in {"wproject", "widefield"}:
        casa_wprojplanes = "-1"

    env = {
        "BENCH_REPEATS": str(repeats),
        "IMAGER_BENCH_WARMUPS": str(warmups),
        "IMAGER_BENCH_MODE": bench_mode,
        "IMAGER_BENCH_SPECMODE": specmode,
        "IMAGER_BENCH_GRIDDER": gridder,
        "IMAGER_BENCH_CASA_GRIDDER": casa_gridder,
        "IMAGER_BENCH_INTERPOLATION": interpolation,
        "IMAGER_BENCH_FIELD": str_value(imaging, "field", "0"),
        "IMAGER_BENCH_PHASECENTER_FIELD": optional_int_string(
            imaging, "phasecenter_field"
        ),
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
        "IMAGER_BENCH_IMAGING_FFT_BACKEND": str_value(
            imaging, "imaging_fft_backend", "auto"
        ),
        "IMAGER_BENCH_HOGBOM_ITERATION_MODE": hogbom_iteration_mode,
        "IMAGER_BENCH_NTERMS": str(int_value(imaging, "nterms", 1)),
        "IMAGER_BENCH_SCALES": scales_value(imaging),
        "IMAGER_BENCH_WTERM": wterm,
        "IMAGER_BENCH_WPROJPLANES": wprojplanes,
        "IMAGER_BENCH_CASA_WPROJPLANES": casa_wprojplanes,
        "IMAGER_BENCH_DATACOLUMN": str_value(imaging, "datacolumn", "DATA"),
        "IMAGER_BENCH_STOKES": str_value(imaging, "stokes", "I"),
        "IMAGER_BENCH_PROJECTION": str_value(imaging, "projection", "SIN"),
        "IMAGER_BENCH_UVRANGE": str_value(imaging, "uvrange", ""),
        "IMAGER_BENCH_INTENT": str_value(imaging, "intent", ""),
        "IMAGER_BENCH_CFCACHE": resolved_cfcache,
        "IMAGER_BENCH_CF_RESIDENT_MB": str(
            int_value(imaging, "cf_resident_mb", 256)
        ),
        "IMAGER_BENCH_FACETS": str(int_value(imaging, "facets", 1)),
        "IMAGER_BENCH_PSFPHASECENTER": str_value(
            imaging, "psfphasecenter", ""
        ),
        "IMAGER_BENCH_VPTABLE": str_value(imaging, "vptable", ""),
        "IMAGER_BENCH_ATERM": boolean_env_value(imaging, "aterm", True),
        "IMAGER_BENCH_PSTERM": boolean_env_value(imaging, "psterm", False),
        "IMAGER_BENCH_WBAWP": boolean_env_value(imaging, "wbawp", True),
        "IMAGER_BENCH_CONJBEAMS": boolean_env_value(
            imaging, "conjbeams", True
        ),
        "IMAGER_BENCH_COMPUTEPASTEP": str(
            float_value(imaging, "computepastep", 360.0)
        ),
        "IMAGER_BENCH_ROTATEPASTEP": str(
            float_value(imaging, "rotatepastep", 360.0)
        ),
        "IMAGER_BENCH_POINTINGOFFSETSIGDEV": str(
            float_value(imaging, "pointingoffsetsigdev", 0.0)
        ),
        "IMAGER_BENCH_MOSWEIGHT": boolean_env_value(
            imaging, "mosweight", False
        ),
        "IMAGER_BENCH_NORMTYPE": str_value(imaging, "normtype", "flatnoise"),
        "IMAGER_BENCH_USEPOINTING": boolean_env_value(
            imaging, "usepointing", False
        ),
        "IMAGER_BENCH_NITER": str(int_value(imaging, "niter", 4)),
        "IMAGER_BENCH_GAIN": str(float_value(imaging, "gain", 0.1)),
        "IMAGER_BENCH_THRESHOLD_JY": str(float_value(imaging, "threshold_jy", 0.0)),
        "IMAGER_BENCH_NSIGMA": str(float_value(imaging, "nsigma", 0.0)),
        "IMAGER_BENCH_PSFCUTOFF": str(float_value(imaging, "psfcutoff", 0.35)),
        "IMAGER_BENCH_PBLIMIT": str(float_value(imaging, "pblimit", 0.2)),
        "IMAGER_BENCH_WRITE_PB": boolean_env_value(imaging, "write_pb", False),
        "IMAGER_BENCH_PBCOR": boolean_env_value(imaging, "pbcor", False),
        "IMAGER_BENCH_SMALLSCALEBIAS": str(
            float_value(imaging, "smallscalebias", 0.0)
        ),
        "IMAGER_BENCH_RESTORATION": boolean_env_value(
            imaging, "restoration", True
        ),
        "IMAGER_BENCH_RESTORINGBEAM": str_value(
            imaging, "restoringbeam", ""
        ),
        "IMAGER_BENCH_INTERACTIVE": boolean_env_value(
            imaging, "interactive", False
        ),
        "IMAGER_BENCH_USEMASK": str_value(imaging, "usemask", "user"),
        "IMAGER_BENCH_MASK_IMAGE": str_value(imaging, "mask_image", ""),
        "IMAGER_BENCH_RESTART": boolean_env_value(imaging, "restart", False),
        "IMAGER_BENCH_SAVEMODEL": str_value(imaging, "savemodel", "none"),
        "IMAGER_BENCH_CALCRES": boolean_env_value(imaging, "calcres", True),
        "IMAGER_BENCH_CALCPSF": boolean_env_value(imaging, "calcpsf", True),
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
        "IMAGER_BENCH_PROFILE_WARMUPS": str(warmups),
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
        "imaging_read_ahead_blocks": "IMAGER_BENCH_IMAGING_READ_AHEAD_BLOCKS",
        "chanchunks": "IMAGER_BENCH_CHANCHUNKS",
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
    if imaging.get("parallel") is not None:
        env["IMAGER_BENCH_PARALLEL"] = boolean_env_value(imaging, "parallel", True)
    env.setdefault("CASA_RS_STANDARD_MFS_PROFILE_DETAIL", "1")
    env.update(extra_env)

    command = [str(BENCH_SCRIPT), str(dataset_path)]
    command_plan: dict[str, Any]
    if casa:
        assert recipe_path is not None
        command_plan = casa_tclean_workflow.build_recipe_command_plan(
            casa=casa,
            recipe_path=recipe_path,
            dataset=dataset,
            dataset_path=dataset_path,
            imaging=imaging,
            run_support=run_support,
            casa_python=casa_python,
            dry_run=dry_run,
        )
        command_plan["evidence_storage"] = casa_tclean_workflow.storage_requirement(
            run, dataset
        )
    else:
        command_plan = {"kind": "legacy_benchmark_script", "argv": command, "env": env}
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
            "image_shape": [
                int_value(imaging, "imsize", 128),
                int_value(imaging, "imsize", 128),
            ],
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
            "parallel": (
                bool_value(imaging, "parallel", True)
                if imaging.get("parallel") is not None
                else None
            ),
            "chanchunks": (
                int_value(imaging, "chanchunks", 0)
                if imaging.get("chanchunks") is not None
                else None
            ),
            "imaging_fft_precision": str_value(
                imaging, "imaging_fft_precision", "auto"
            ),
            "imaging_fft_backend": str_value(imaging, "imaging_fft_backend", "auto"),
            "imaging_read_ahead_blocks": (
                int_value(imaging, "imaging_read_ahead_blocks", 0)
                if imaging.get("imaging_read_ahead_blocks") is not None
                else None
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
            "warmups": warmups,
            "cf_cache_role": cf_cache_role,
            "evidence_role": str_value(run, "evidence_role", "benchmark"),
        },
        "run_support": run_support,
        "review": review_contract_value(manifest, run),
        "comparison": {
            "mode": str_value(comparison, "mode", "sampled"),
            "products": product_suffixes_value(comparison),
            "max_elements_per_product": int_value(
                comparison, "max_elements_per_product", 1_000_000
            ),
            "full_chunk_elements": int_value(
                comparison, "full_chunk_elements", 1_000_000
            ),
            "require_exact_product_inventory": bool_value(
                comparison, "require_exact_product_inventory", False
            ),
            "require_metadata_parity": bool_value(
                comparison, "require_metadata_parity", False
            ),
            "source_regions": comparison.get("source_regions", []),
            "tolerances": comparison.get("tolerances"),
        },
        "command": command_plan,
        "environment": capture_provenance(
            repo_root=REPO_ROOT,
            executables={
                "bench_script": BENCH_SCRIPT,
                "casa_python": casa_python,
                "casa_tclean_protocol": CASA_TCLEAN_PROTOCOL if casa else None,
            },
            datasets={"measurement_set": dataset_path},
            storage_label=storage_label_override
            or str_value(run, "storage_label", "script-staged-tempdir"),
        ),
    }
    plan["benchmark_features"] = build_benchmark_feature_summary(plan, None)
    return plan


def attach_output_paths(
    plan: dict[str, Any],
    output_dir: pathlib.Path,
    artifact_root: pathlib.Path,
    *,
    cf_cache_root: pathlib.Path | None = None,
    dry_run: bool,
) -> None:
    if plan["command"].get("kind") == "casa_tclean_protocol":
        casa_tclean_workflow.attach_output_paths(
            plan,
            output_dir=output_dir,
            artifact_root=artifact_root,
            cf_cache_root=cf_cache_root
            or casa_tclean_workflow.default_cf_cache_root(plan, artifact_root),
            dry_run=dry_run,
        )
        return
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
        reasons.append(
            f"specmode={specmode!r} needs benchmark-script execution support"
        )
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


def boolean_flag(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "on"}


def review_contract_value(
    manifest: dict[str, Any], run: dict[str, Any]
) -> dict[str, Any]:
    review = object_value(manifest, "review")
    required_roles = review.get(
        "required_evidence_roles",
        ["before_baseline", "after_multi_worker_cpu", "after_gpu_metal", "casa_cpp"],
    )
    if not isinstance(required_roles, list) or not all(
        isinstance(role, str) and role for role in required_roles
    ):
        raise HarnessError(
            "review.required_evidence_roles must be a non-empty string list"
        )
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
    if plan.get("command", {}).get("kind") == "casa_tclean_protocol":
        return run_casa_recipe_plan(plan, log_path)
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
            "schema_version": RUN_RESULT_SCHEMA_VERSION,
            "kind": "workload_run",
            "status": "failed_execution",
            **plan,
            "started_at": started,
            "completed_at": utc_now(),
            "exit_code": completed.returncode,
            "results": {
                **empty_results(casa_status="blocked", reason=reason),
                "failure": {
                    "kind": "execution",
                    "reason": reason,
                    "return_code": completed.returncode,
                },
            },
            "human_review": human_review_gate(plan, None),
        }

    parsed = parse_benchmark_log(completed.stdout)
    parsed["backend_plan_logs"] = parse_backend_plan_logs(completed.stdout)
    parsed["benchmark_features"] = build_benchmark_feature_summary(plan, parsed)
    attach_stage_breakdown(plan, parsed)
    comparison = compare_products(plan, parsed, log_path)
    parsed["product_comparison"] = comparison
    evidence_status, failure = comparison_evidence_status(
        comparison, required=plan["comparison"].get("tolerances") is not None
    )
    if failure is not None:
        parsed["failure"] = failure
    completed_plan = dict(plan)
    completed_plan["benchmark_features"] = parsed["benchmark_features"]
    return {
        "schema_version": RUN_RESULT_SCHEMA_VERSION,
        "kind": "workload_run",
        "status": evidence_status,
        **completed_plan,
        "started_at": started,
        "completed_at": utc_now(),
        "exit_code": completed.returncode,
        "results": parsed,
        "human_review": human_review_gate(plan, comparison),
    }


def run_casa_recipe_plan(
    plan: dict[str, Any], log_path: pathlib.Path
) -> dict[str, Any]:
    """Dispatch a recipe-backed plan through the shared tclean workflow."""

    return casa_tclean_workflow.run_recipe_plan(
        plan, log_path, services=recipe_execution_services()
    )


def recipe_execution_services() -> casa_tclean_workflow.ExecutionServices:
    """Bind generic dispatcher services to the recipe workflow owner."""

    return casa_tclean_workflow.ExecutionServices(
        utc_now=utc_now,
        empty_results=empty_results,
        empty_stage_breakdown=empty_stage_breakdown,
        build_benchmark_feature_summary=build_benchmark_feature_summary,
        comparison_evidence_status=comparison_evidence_status,
        human_review_gate=human_review_gate,
        compare_image_products=compare_image_products,
    )


def comparison_evidence_status(
    comparison: dict[str, Any],
    *,
    required: bool = False,
) -> tuple[str, dict[str, Any] | None]:
    status = comparison.get("status")
    evaluation = comparison.get("tolerance_evaluation")
    if isinstance(evaluation, dict) and evaluation.get("status") != "passed":
        failed = evaluation.get("status") == "failed"
        names = (
            evaluation.get("failed_checks")
            if failed
            else evaluation.get("incomplete_checks")
        )
        return (
            "out_of_tolerance" if failed else "failed_comparison",
            {
                "kind": "comparison_tolerance" if failed else "comparison",
                "reason": (
                    f"frozen tolerance evaluation {evaluation.get('status')}: "
                    + ", ".join(names or [])
                ),
            },
        )
    if status == "out_of_tolerance":
        return (
            "out_of_tolerance",
            {
                "kind": "comparison_tolerance",
                "reason": str(comparison.get("reason") or status),
            },
        )
    if status in {"unavailable", "skipped"}:
        if required:
            return (
                "failed_comparison",
                {
                    "kind": "comparison",
                    "reason": str(
                        comparison.get("reason") or f"required comparison is {status}"
                    ),
                },
            )
        return "completed", None
    if status != "completed":
        return (
            "failed_comparison",
            {
                "kind": "comparison",
                "reason": str(
                    comparison.get("reason") or status or "comparison failed"
                ),
            },
        )
    products = comparison.get("products")
    if not isinstance(products, dict) or not products:
        return (
            "failed_comparison",
            {"kind": "comparison", "reason": "comparison produced no products"},
        )
    incomplete = [
        suffix
        for suffix, product in products.items()
        if not isinstance(product, dict) or product.get("status") != "compared"
    ]
    if incomplete:
        return (
            "failed_comparison",
            {
                "kind": "comparison",
                "reason": "product comparison incomplete for " + ", ".join(incomplete),
            },
        )
    review = comparison.get("structured_difference_review")
    if isinstance(review, dict) and review.get("label") in {"bad", "investigate"}:
        return (
            "out_of_tolerance",
            {
                "kind": "comparison_tolerance",
                "reason": str(
                    review.get("summary")
                    or "scientific product comparison needs tolerance review"
                ),
            },
        )
    return "completed", None


def run_benchmark_command(
    argv: list[str], *, env: dict[str, str], stream_log: bool
) -> subprocess.CompletedProcess[str]:
    return run_command(
        argv,
        cwd=REPO_ROOT,
        environment=env,
        stream_stdout=stream_log,
    )


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
        "imaging_source_read_ahead": [],
        "standard_mfs_source_read_ahead": [],
        "dirty_product_fft": [],
        "dirty_product_gpu_resident": [],
        "dirty_product_gpu_resident_fallback": [],
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
        elif name == "imaging_source_read_ahead_summary":
            buckets["imaging_source_read_ahead"].append(parsed)
        elif name == "standard_mfs_source_read_ahead_summary":
            parsed.setdefault("fields", {})["mode"] = "standard_mfs"
            buckets["imaging_source_read_ahead"].append(parsed)
            buckets["standard_mfs_source_read_ahead"].append(parsed)
        elif name == "dirty_product_fft_timing":
            buckets["dirty_product_fft"].append(parsed)
        elif name in {
            "dirty_product_gpu_resident",
            "mosaic_dirty_product_gpu_resident",
        }:
            buckets["dirty_product_gpu_resident"].append(parsed)
        elif name == "dirty_product_gpu_resident_fallback":
            buckets["dirty_product_gpu_resident_fallback"].append(parsed)
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
    summary = summarize_backend_plan_logs(buckets)
    retained_buckets: dict[str, list[dict[str, Any]]] = {}
    collection_stats: dict[str, dict[str, Any]] = {}
    for name, entries in buckets.items():
        retained = compact_backend_log_entries(entries)
        retained_buckets[name] = retained
        collection_stats[name] = {
            "observed_count": len(entries),
            "retained_count": len(retained),
            "truncated": len(retained) < len(entries),
        }
    return {
        "schema_version": 1,
        "summary": summary,
        "collection_stats": collection_stats,
        **retained_buckets,
    }


def compact_backend_log_entries(
    entries: list[dict[str, Any]],
    *,
    limit: int = MAX_BACKEND_LOG_ENTRIES_PER_BUCKET,
) -> list[dict[str, Any]]:
    """Keep bounded, representative raw diagnostics after full-stream aggregation."""
    if limit < 2:
        raise ValueError("backend log entry limit must be at least 2")
    if len(entries) <= limit:
        return entries
    head_count = limit // 2
    return entries[:head_count] + entries[-(limit - head_count) :]


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
    if re.fullmatch(
        r"-?(?:[0-9]+[.][0-9]*|[0-9]*[.][0-9]+)(?:[eE][+-]?[0-9]+)?", value
    ):
        try:
            return float(value)
        except ValueError:
            return value
    return value


def summarize_backend_plan_logs(
    buckets: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    runtime = last_fields(buckets.get("standard_mfs_runtime_plan", []))
    memory = last_fields(buckets.get("source_stream_memory_plan", []))
    source_read_ahead_entries = unique_entries_by_raw(
        buckets.get("imaging_source_read_ahead", [])
    )
    source_read_ahead = aggregate_source_read_ahead_fields(source_read_ahead_entries)
    source_read_ahead_modes = [
        entry.get("fields", {}).get("mode")
        for entry in source_read_ahead_entries
        if isinstance(entry.get("fields", {}), dict)
        and entry.get("fields", {}).get("mode") is not None
    ]
    dirty_product_fft = last_fields(buckets.get("dirty_product_fft", []))
    dirty_product_gpu_resident = last_fields(
        buckets.get("dirty_product_gpu_resident", [])
    )
    dirty_product_gpu_fallback = last_fields(
        buckets.get("dirty_product_gpu_resident_fallback", [])
    )
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
    cube_resident_executor = last_fields(
        buckets.get("cube_resident_clean_executor", [])
    )
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
        "resolved_minor_cycle_backend_reason": runtime.get(
            "minor_cycle_backend_reason"
        ),
        "requested_imaging_fft_backend": runtime.get("imaging_fft_backend"),
        "dirty_product_fft_selected_backend": dirty_product_fft.get("selected_backend"),
        "dirty_product_fft_requested_backend": dirty_product_fft.get(
            "requested_backend"
        ),
        "dirty_product_fft_fallback_used": dirty_product_fft.get("fallback_used"),
        "dirty_product_fft_total_ms": dirty_product_fft.get("total_ms"),
        "dirty_product_gpu_resident_products": dirty_product_gpu_resident.get(
            "products"
        ),
        "dirty_product_gpu_resident_requested_backend": dirty_product_gpu_resident.get(
            "requested_backend"
        ),
        "dirty_product_gpu_resident_selected_backend": dirty_product_gpu_resident.get(
            "selected_backend"
        ),
        "dirty_product_gpu_resident_fallback_used": dirty_product_gpu_resident.get(
            "fallback_used"
        ),
        "dirty_product_gpu_resident_reason": dirty_product_gpu_resident.get("reason"),
        "dirty_product_gpu_resident_plan_ms": dirty_product_gpu_resident.get("plan_ms"),
        "dirty_product_gpu_resident_pack_ms": dirty_product_gpu_resident.get("pack_ms"),
        "dirty_product_gpu_resident_transfer_to_device_ms": dirty_product_gpu_resident.get(
            "transfer_to_device_ms"
        ),
        "dirty_product_gpu_resident_exec_ms": dirty_product_gpu_resident.get("exec_ms"),
        "dirty_product_gpu_resident_device_exec_ms": dirty_product_gpu_resident.get(
            "device_exec_ms"
        ),
        "dirty_product_gpu_resident_transfer_from_device_ms": dirty_product_gpu_resident.get(
            "transfer_from_device_ms"
        ),
        "dirty_product_gpu_resident_sync_ms": dirty_product_gpu_resident.get("sync_ms"),
        "dirty_product_gpu_resident_postprocess_ms": dirty_product_gpu_resident.get(
            "postprocess_ms"
        ),
        "dirty_product_gpu_resident_total_ms": dirty_product_gpu_resident.get(
            "total_ms"
        ),
        "dirty_product_gpu_resident_fallback_reason": dirty_product_gpu_fallback.get(
            "reason"
        ),
        "metal_device_available": runtime.get("metal_device_available"),
        "metal_grouped_input_cache": runtime.get("metal_grouped_input_cache"),
        "cube_per_plane_backend": cube_per_plane_backend.get("selected_backend"),
        "cube_per_plane_phase": cube_per_plane_backend.get("phase"),
        "cube_per_plane_workers": cube_per_plane_backend.get("plane_worker_count"),
        "cube_per_plane_grid_threads": cube_per_plane_backend.get(
            "per_plane_grid_threads"
        ),
        "cube_per_plane_fixed_tile_eligible": cube_per_plane_backend.get(
            "fixed_tile_cpu_eligible"
        ),
        "cube_per_plane_metal_eligible": cube_per_plane_backend.get("metal_eligible"),
        "cube_per_plane_fallback_reasons": cube_per_plane_backend.get(
            "fallback_reasons"
        ),
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
        "source_read_ahead_enabled": source_read_ahead.get("enabled"),
        "source_read_ahead_mode": source_read_ahead.get("mode"),
        "source_read_ahead_modes": sorted(set(source_read_ahead_modes)),
        "source_read_ahead_summary_count": len(source_read_ahead_entries),
        "source_read_ahead_max_live_row_blocks": source_read_ahead.get(
            "max_live_row_blocks"
        ),
        "source_read_ahead_queue_capacity": source_read_ahead.get("queue_capacity"),
        "source_read_ahead_row_blocks": source_read_ahead.get("row_blocks"),
        "source_read_ahead_consumer_recv_blocked_ms": source_read_ahead.get(
            "consumer_recv_blocked_ms"
        ),
        "source_read_ahead_producer_send_blocked_ms": source_read_ahead.get(
            "producer_send_blocked_ms"
        ),
        "source_read_ahead_producer_consumer_overlap_ms": source_read_ahead.get(
            "producer_consumer_overlap_ms"
        ),
        "source_read_ahead_live_row_block_high_water": source_read_ahead.get(
            "live_row_block_high_water"
        ),
        "source_read_ahead_source_read_ms": source_read_ahead.get("source_read_ms"),
        "source_read_ahead_source_route_ms": source_read_ahead.get("source_route_ms"),
        "source_read_ahead_consumer_ms": source_read_ahead.get("consumer_ms"),
        "source_read_ahead_source_prepare_ms": source_read_ahead.get(
            "source_prepare_ms"
        ),
        "source_read_ahead_streamed_samples": source_read_ahead.get("streamed_samples"),
        "source_read_ahead_source_bytes": source_read_ahead.get("source_bytes"),
        "source_read_ahead_effective_read_bandwidth_mib_s": source_read_ahead.get(
            "effective_read_bandwidth_mib_s"
        ),
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
        "cube_product_bytes": sum_int_or_float_field(
            cube_product_summaries, "product_bytes"
        ),
        "cube_product_groups": sum_int_or_float_field(
            cube_product_summaries, "product_groups"
        ),
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
        "spectral_visibility_cache_policy": spectral_plan.get(
            "visibility_cache_policy"
        ),
        "spectral_prepared_residency": spectral_plan.get("prepared_residency"),
        "spectral_visibility_cache_bytes": spectral_plan.get("visibility_cache_bytes"),
        "spectral_product_batch_planes": spectral_plan.get("product_batch_planes"),
        "spectral_source_channel_visits": spectral_plan.get("source_channel_visits"),
        "spectral_max_slab_source_channels": spectral_plan.get(
            "max_slab_source_channels"
        ),
        "spectral_full_source_channel_count": spectral_plan.get(
            "full_source_channel_count"
        ),
        "spectral_source_cell_channel_count": spectral_plan.get(
            "source_cell_channel_count"
        ),
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
        "spectral_best_modeled_slab_count": spectral_plan.get(
            "best_modeled_slab_count"
        ),
        "spectral_best_modeled_source_channel_visits": spectral_plan.get(
            "best_modeled_source_channel_visits"
        ),
        "spectral_modeled_total_io_bytes": spectral_plan.get("modeled_total_io_bytes"),
        "spectral_modeled_source_read_bytes": spectral_plan.get(
            "modeled_source_read_bytes"
        ),
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


def aggregate_source_read_ahead_fields(entries: list[dict[str, Any]]) -> dict[str, Any]:
    fields = dict(last_fields(entries))
    if not fields:
        return fields
    entry_fields = [
        entry.get("fields", {})
        for entry in entries
        if isinstance(entry.get("fields", {}), dict)
    ]
    for field in (
        "row_blocks",
        "consumer_recv_blocked_ms",
        "producer_send_blocked_ms",
        "producer_consumer_overlap_ms",
        "source_read_ms",
        "source_route_ms",
        "consumer_ms",
        "source_prepare_ms",
        "streamed_samples",
        "source_bytes",
    ):
        total = sum_int_or_float_field(entry_fields, field)
        if total is not None:
            fields[field] = total
    for field in (
        "max_live_row_blocks",
        "queue_capacity",
        "live_row_block_high_water",
    ):
        maximum = max_int_or_float_field(entry_fields, field)
        if maximum is not None:
            fields[field] = maximum
    enabled = [entry.get("enabled") for entry in entry_fields if "enabled" in entry]
    if enabled:
        fields["enabled"] = any(value is True for value in enabled)
    source_bytes = fields.get("source_bytes")
    source_read_ms = fields.get("source_read_ms")
    if isinstance(source_bytes, (int, float)) and isinstance(
        source_read_ms, (int, float)
    ):
        fields["effective_read_bandwidth_mib_s"] = (
            source_bytes / (1024.0 * 1024.0) / (source_read_ms / 1000.0)
            if source_bytes > 0 and source_read_ms > 0
            else 0.0
        )
    return fields


def unique_entries_by_raw(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    raw_entries = [entry.get("raw") for entry in entries]
    half = len(raw_entries) // 2
    if (
        half > 0
        and len(raw_entries) % 2 == 0
        and raw_entries[:half] == raw_entries[half:]
    ):
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
    values = [
        entry.get(field) for entry in entries if isinstance(entry.get(field), int)
    ]
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


def sum_int_or_float_field(
    entries: list[dict[str, Any]], field: str
) -> int | float | None:
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
    runtime = environment.get("runtime", {})
    backend_logs = (parsed or {}).get("backend_plan_logs", {})
    backend_summary = (
        backend_logs.get("summary", {}) if isinstance(backend_logs, dict) else {}
    )
    stages = ((parsed or {}).get("stage_medians_ms") or {}).get("rust", {})
    casa_evidence_resources = (
        ((parsed or {}).get("casa") or {}).get("evidence_summary") or {}
    ).get("resources", {})
    image_shape = mode.get("image_shape") or [None, None]
    imsize_x = (
        int(image_shape[0]) if image_shape and image_shape[0] is not None else None
    )
    imsize_y = (
        int(image_shape[1])
        if len(image_shape) > 1 and image_shape[1] is not None
        else imsize_x
    )
    selected_channels = first_int(
        backend_summary.get("selected_channels"),
        source_channel_width(mode),
        mode.get("channel_count"),
    )
    selected_rows = first_int(backend_summary.get("active_rows"))
    gridded_samples = first_int(backend_summary.get("gridded_samples"))
    correlations = planned_correlation_count(plan)
    flagged_fraction = None
    if (
        gridded_samples is not None
        and selected_rows
        and selected_channels
        and correlations
    ):
        denominator = selected_rows * selected_channels * correlations
        if denominator > 0:
            flagged_fraction = max(0.0, min(1.0, 1.0 - (gridded_samples / denominator)))
    visibility_work = None
    if (
        selected_rows is not None
        and selected_channels is not None
        and correlations is not None
    ):
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
        for name in (
            "get_ms_values_into_processing_buffer",
            "prepare_processing_buffer",
        )
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
            "source_stream_throughput_samples_per_s": finite_float(
                source_stream_throughput
            ),
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
            "cycleniter": plan.get("command", {})
            .get("env", {})
            .get("IMAGER_BENCH_MINOR_CYCLE_LENGTH"),
            "actual_major_cycles": backend_summary.get("major_cycles"),
            "actual_minor_iterations": backend_summary.get("minor_iterations"),
            "multiscale_scale_count": multiscale_scale_count(plan),
            "mtmfs_nterms": mode.get("nterms")
            if mode.get("deconvolver") == "mtmfs"
            else None,
            "wprojplanes": mode.get("wprojplanes"),
            "mosaic_field_count": field_count(plan),
        },
        "resources": {
            "casa_peak_rss_bytes": casa_evidence_resources.get("peak_rss_bytes_max"),
            "casa_user_cpu_seconds_median": casa_evidence_resources.get(
                "user_cpu_seconds_median"
            ),
            "casa_system_cpu_seconds_median": casa_evidence_resources.get(
                "system_cpu_seconds_median"
            ),
            "casa_disk_read_bytes_median": casa_evidence_resources.get(
                "disk_read_bytes_median"
            ),
            "casa_disk_write_bytes_median": casa_evidence_resources.get(
                "disk_write_bytes_median"
            ),
            "casa_block_input_operations_median": casa_evidence_resources.get(
                "block_input_operations_median"
            ),
            "casa_block_output_operations_median": casa_evidence_resources.get(
                "block_output_operations_median"
            ),
            "casa_product_logical_bytes_median": casa_evidence_resources.get(
                "product_logical_bytes_median"
            ),
            "casa_cf_cache_logical_bytes_max": casa_evidence_resources.get(
                "cf_cache_logical_bytes_max"
            ),
            "casa_cf_cache_included_file_count_max": casa_evidence_resources.get(
                "cf_cache_included_file_count_max"
            ),
            "physical_cores": runtime.get("physical_cores"),
            "logical_cores": runtime.get("logical_cores"),
            "available_parallelism": runtime.get("logical_cores"),
            "physical_memory_bytes": runtime.get("physical_memory_bytes"),
            "memory_target_bytes": backend_summary.get("memory_target_bytes"),
            "planned_active_bytes": backend_summary.get("planned_active_bytes"),
            "memory_headroom_bytes": memory_headroom_bytes(backend_summary),
            "row_block_count": row_block_count(
                selected_rows, backend_summary.get("row_block_rows")
            ),
            "row_block_rows": backend_summary.get("row_block_rows"),
            "source_read_ahead_enabled": backend_summary.get(
                "source_read_ahead_enabled"
            ),
            "source_read_ahead_mode": backend_summary.get("source_read_ahead_mode"),
            "source_read_ahead_modes": backend_summary.get("source_read_ahead_modes"),
            "source_read_ahead_summary_count": backend_summary.get(
                "source_read_ahead_summary_count"
            ),
            "source_read_ahead_queue_capacity": backend_summary.get(
                "source_read_ahead_queue_capacity"
            ),
            "source_read_ahead_row_blocks": backend_summary.get(
                "source_read_ahead_row_blocks"
            ),
            "source_read_ahead_consumer_recv_blocked_ms": backend_summary.get(
                "source_read_ahead_consumer_recv_blocked_ms"
            ),
            "source_read_ahead_producer_send_blocked_ms": backend_summary.get(
                "source_read_ahead_producer_send_blocked_ms"
            ),
            "source_read_ahead_producer_consumer_overlap_ms": backend_summary.get(
                "source_read_ahead_producer_consumer_overlap_ms"
            ),
            "source_read_ahead_live_row_block_high_water": backend_summary.get(
                "source_read_ahead_live_row_block_high_water"
            ),
            "source_read_ahead_source_read_ms": backend_summary.get(
                "source_read_ahead_source_read_ms"
            ),
            "source_read_ahead_source_route_ms": backend_summary.get(
                "source_read_ahead_source_route_ms"
            ),
            "source_read_ahead_consumer_ms": backend_summary.get(
                "source_read_ahead_consumer_ms"
            ),
            "source_read_ahead_source_prepare_ms": backend_summary.get(
                "source_read_ahead_source_prepare_ms"
            ),
            "source_read_ahead_source_bytes": backend_summary.get(
                "source_read_ahead_source_bytes"
            ),
            "source_read_ahead_effective_read_bandwidth_mib_s": backend_summary.get(
                "source_read_ahead_effective_read_bandwidth_mib_s"
            ),
            "peak_rss_bytes": backend_summary.get("peak_rss_bytes"),
            "metal_device": backend_summary.get("metal_device_available"),
            "metal_grouped_input_cache": backend_summary.get(
                "metal_grouped_input_cache"
            ),
            "dirty_product_fft_selected_backend": backend_summary.get(
                "dirty_product_fft_selected_backend"
            ),
            "dirty_product_fft_requested_backend": backend_summary.get(
                "dirty_product_fft_requested_backend"
            ),
            "dirty_product_fft_total_ms": backend_summary.get(
                "dirty_product_fft_total_ms"
            ),
            "dirty_product_gpu_resident_plan_ms": backend_summary.get(
                "dirty_product_gpu_resident_plan_ms"
            ),
            "dirty_product_gpu_resident_pack_ms": backend_summary.get(
                "dirty_product_gpu_resident_pack_ms"
            ),
            "dirty_product_gpu_resident_transfer_to_device_ms": backend_summary.get(
                "dirty_product_gpu_resident_transfer_to_device_ms"
            ),
            "dirty_product_gpu_resident_exec_ms": backend_summary.get(
                "dirty_product_gpu_resident_exec_ms"
            ),
            "dirty_product_gpu_resident_device_exec_ms": backend_summary.get(
                "dirty_product_gpu_resident_device_exec_ms"
            ),
            "dirty_product_gpu_resident_transfer_from_device_ms": backend_summary.get(
                "dirty_product_gpu_resident_transfer_from_device_ms"
            ),
            "dirty_product_gpu_resident_sync_ms": backend_summary.get(
                "dirty_product_gpu_resident_sync_ms"
            ),
            "dirty_product_gpu_resident_postprocess_ms": backend_summary.get(
                "dirty_product_gpu_resident_postprocess_ms"
            ),
            "dirty_product_gpu_resident_total_ms": backend_summary.get(
                "dirty_product_gpu_resident_total_ms"
            ),
        },
        "backend": {
            "requested_acceleration": mode.get("standard_mfs_acceleration"),
            "requested_imaging_fft_backend": backend_summary.get(
                "requested_imaging_fft_backend"
            ),
            "resolved_backend": backend_summary.get("resolved_backend"),
            "resolved_grid_threads": backend_summary.get("resolved_grid_threads"),
            "resolved_tile_anchor": backend_summary.get("resolved_tile_anchor"),
            "resolved_residual_backend": backend_summary.get(
                "resolved_residual_backend"
            ),
            "resolved_initial_dirty_backend": backend_summary.get(
                "resolved_initial_dirty_backend"
            ),
            "dirty_product_fft_selected_backend": backend_summary.get(
                "dirty_product_fft_selected_backend"
            ),
            "dirty_product_fft_requested_backend": backend_summary.get(
                "dirty_product_fft_requested_backend"
            ),
            "dirty_product_fft_fallback_used": backend_summary.get(
                "dirty_product_fft_fallback_used"
            ),
            "dirty_product_gpu_resident_requested_backend": backend_summary.get(
                "dirty_product_gpu_resident_requested_backend"
            ),
            "dirty_product_gpu_resident_selected_backend": backend_summary.get(
                "dirty_product_gpu_resident_selected_backend"
            ),
            "dirty_product_gpu_resident_fallback_used": backend_summary.get(
                "dirty_product_gpu_resident_fallback_used"
            ),
            "dirty_product_gpu_resident_reason": backend_summary.get(
                "dirty_product_gpu_resident_reason"
            ),
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
            "cpu_multi_worker_reason": backend_summary.get(
                "single_plane_reason", {}
            ).get("cpu_multi_worker")
            if isinstance(backend_summary.get("single_plane_reason"), dict)
            else None,
            "gpu_metal_reason": backend_summary.get("single_plane_reason", {}).get(
                "gpu_metal"
            )
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
    try:
        return finite_number(value, field="run result numeric field")
    except ContractError:
        return None


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


def build_rust_stage_breakdown(
    plan: dict[str, Any], stages: dict[str, float]
) -> dict[str, Any]:
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
        "protocol_preflight": stage_category(
            stages,
            ["protocol_preflight"],
            "Checked-in tclean protocol preconditions and runtime/mask validation.",
        ),
        "opaque_tclean_task": stage_category(
            stages,
            ["tclean_task"],
            (
                "Opaque CASA tclean task envelope containing internal MS selection, "
                "CF/AW work, gridding, FFTs, deconvolution, restoration, and product "
                "writes; it does not claim internal attribution."
            ),
        ),
        "evidence_postconditions": stage_category(
            stages,
            ["product_inventory", "cache_postcondition"],
            "Stable product hashing plus CF-cache validation and publication evidence.",
        ),
        "protocol_total": stage_category(
            stages,
            ["protocol_total"],
            "End-to-end checked-in tclean protocol execution.",
        ),
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
    rust_prefix = product_paths.get("rust_prefix") or plan.get("products", {}).get(
        "rust_prefix"
    )
    casa_prefix = product_paths.get("casa_prefix") or plan.get("products", {}).get(
        "casa_prefix"
    )
    casa_python = executable_path(plan.get("environment", {}), "casa_python")
    if not rust_prefix or not casa_prefix:
        return {
            "status": "skipped",
            "reason": "benchmark did not preserve product prefixes",
            "source_regions": plan["comparison"].get("source_regions", []),
            "tolerances": plan["comparison"].get("tolerances"),
            "products": {},
        }
    if not casa_python:
        return {
            "status": "skipped",
            "reason": "CASA Python is required for CASA image product comparison",
            "source_regions": plan["comparison"].get("source_regions", []),
            "tolerances": plan["comparison"].get("tolerances"),
            "products": {},
        }

    comparison_root = plan.get("artifacts", {}).get("comparison_root")
    panel_dir = (
        pathlib.Path(comparison_root) / "panels"
        if comparison_root
        else log_path.with_suffix(".panels")
    )
    structure_workspace_dir = (
        pathlib.Path(comparison_root) / "structure-workspace"
        if comparison_root
        else log_path.with_suffix(".structure-workspace")
    ).resolve()
    request = {
        "rust_prefix": rust_prefix,
        "casa_prefix": casa_prefix,
        "products": plan["comparison"]["products"],
        "max_elements_per_product": plan["comparison"]["max_elements_per_product"],
        "mode": plan["comparison"]["mode"],
        "full_chunk_elements": plan["comparison"]["full_chunk_elements"],
        "require_exact_product_inventory": plan["comparison"][
            "require_exact_product_inventory"
        ],
        "require_metadata_parity": plan["comparison"]["require_metadata_parity"],
        "source_regions": plan["comparison"].get("source_regions", []),
        "tolerances": plan["comparison"].get("tolerances"),
        "panel_dir": str(panel_dir),
        "structure_workspace_dir": str(structure_workspace_dir),
    }
    comparison = compare_image_products(
        casa_python=casa_python,
        request=request,
        artifact_prefix=log_path,
        cwd=REPO_ROOT,
    )
    comparison["panel_dir"] = str(panel_dir)
    comparison["source_regions"] = plan["comparison"].get("source_regions", [])
    comparison["tolerances"] = plan["comparison"].get("tolerances")
    return comparison


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
                root = str(pathlib.Path("/") / "__casa_rs_dry_run__" / root_env)
            else:
                raise HarnessError(
                    f"{root_env} is required for dataset {dataset.get('key')!r}"
                )
        path = pathlib.Path(root) / required_str(dataset, "relative_path")
    if dry_run and not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    if not dry_run and not path.is_dir():
        raise HarnessError(f"dataset path does not exist: {path}")
    return path


def resolve_imaging_path(
    value: str,
    *,
    dataset_path: pathlib.Path,
    field: str,
    require_directory: bool,
) -> pathlib.Path:
    path = pathlib.Path(os.path.expanduser(value))
    if not path.is_absolute():
        path = dataset_path.parent / path
    path = path.resolve()
    if require_directory and not path.is_dir():
        raise HarnessError(f"{field} directory does not exist: {path}")
    return path


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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


if __name__ == "__main__":
    main()
