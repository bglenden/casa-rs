#!/usr/bin/env python3
"""Benchmark native simobserve against CASA simobserve for a Wave 1 dataset."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import statistics
import shutil
import sys
import time
from typing import Any

import perf_paths
from perf_harness import (
    RUN_RESULT_SCHEMA_VERSION,
    atomic_write_json,
    load_json_object,
    validate_run_result,
)
from perf_harness.artifacts import ArtifactError
from perf_harness.casa_protocol import run_json_file_protocol
from perf_harness.image_compare import compare_products as compare_image_products
from perf_harness.ms_compare import compare_measurement_sets
from perf_harness.provenance import capture_provenance
from perf_harness.subprocesses import run_command


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_BINARY = REPO_ROOT / "target" / "release" / "simobserve"
DEFAULT_OUTPUT_DIR = perf_paths.artifact_path("wave1", "simobserve-bench")
CASA_MS_TOOLS = pathlib.Path(__file__).resolve().parent / "perf_harness" / "casa_ms_tools.py"


class BenchError(Exception):
    """Error that should be shown without a Python traceback."""


def strict_data_cell_violates(
    abs_error: float,
    casa_amplitude: float,
    *,
    data_atol: float,
    data_rtol: float,
) -> bool:
    """Return true when one DATA cell fails both absolute and relative criteria."""

    relative = abs_error / max(casa_amplitude, data_atol)
    return abs_error > data_atol and relative > data_rtol


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("plan", type=pathlib.Path, help="wave1-dataset-plan.json")
    parser.add_argument("--dataset", required=True, help="dataset id from the plan")
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT_DIR,
    )
    parser.add_argument("--casars-binary", type=pathlib.Path, default=DEFAULT_BINARY)
    parser.add_argument(
        "--casa-python",
        default=os.environ.get(
            "CASA_RS_CASA_PYTHON",
            "/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python",
        ),
    )
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--channel-workers", type=int, default=None)
    parser.add_argument("--require-speedup", type=float, default=None)
    parser.add_argument(
        "--disable-noise",
        action="store_true",
        help="run both CASA and native simobserve without thermal/noise corruption",
    )
    parser.add_argument(
        "--disable-prediction",
        action="store_true",
        help=(
            "set native predict_model=false and remove corruption; intended for "
            "native-only write-path throughput checks"
        ),
    )
    parser.add_argument(
        "--require-native-throughput-mb-s",
        type=float,
        default=None,
        help="fail unless native output size divided by best runtime reaches this MB/s",
    )
    parser.add_argument(
        "--require-data-io-throughput-mb-s",
        type=float,
        default=None,
        help=(
            "fail unless streamed MAIN-column bytes divided by reported "
            "data_io_write_millis reaches this MB/s"
        ),
    )
    parser.add_argument(
        "--strict-values",
        action="store_true",
        help="fail unless CASA and native rows, UVW, and DATA agree numerically",
    )
    parser.add_argument("--strict-uvw-atol", type=float, default=1.0e-5)
    parser.add_argument(
        "--strict-data-atol",
        type=float,
        default=5.0e-2,
        help="absolute DATA tolerance for CASA-vs-native sampled comparisons",
    )
    parser.add_argument("--strict-data-rtol", type=float, default=1.0e-2)
    parser.add_argument("--skip-casa", action="store_true")
    parser.add_argument("--skip-serial-check", action="store_true")
    parser.add_argument(
        "--native-image-prefix",
        type=pathlib.Path,
        default=None,
        help="optional casa-rs image product prefix for CASA/native product comparison",
    )
    parser.add_argument(
        "--casa-image-prefix",
        type=pathlib.Path,
        default=None,
        help="optional CASA image product prefix for CASA/native product comparison",
    )
    parser.add_argument(
        "--image-product-suffixes",
        default=".image,.residual,.psf,.model,.sumwt,.pb",
        help="comma-separated image product suffixes to compare when image prefixes are supplied",
    )
    parser.add_argument(
        "--fixed-channel-workers",
        type=int,
        default=None,
        help=(
            "run an additional fixed-worker native CPU comparison while leaving "
            "the primary native run in auto mode unless --channel-workers is set"
        ),
    )
    args = parser.parse_args()

    try:
        result = run_benchmark(args)
        result_path = pathlib.Path(result["artifacts"]["result_json"])
        atomic_write_json(result_path, result)
        details = result["results"]["simobserve"]
        write_html_report(pathlib.Path(result["artifacts"]["report_html"]), details)
        print(result_path)
        if result["status"] == "out_of_tolerance":
            raise SystemExit(2)
    except (BenchError, ArtifactError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    if args.repeats < 1:
        raise BenchError("--repeats must be >= 1")
    if args.fixed_channel_workers is not None and args.fixed_channel_workers < 1:
        raise BenchError("--fixed-channel-workers must be >= 1")
    plan = load_json_object(args.plan, description="simobserve plan")
    dataset = select_dataset(plan, args.dataset)
    output_dir = args.output_dir.resolve()
    perf_paths.mark_safe_to_delete(perf_paths.default_artifact_root())
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{dataset['id']}"
    run_root = output_dir / run_id
    run_root.mkdir(parents=True, exist_ok=True)

    request = load_json_object(
        pathlib.Path(dataset["paths"]["casars_simobserve_request"]),
        description="simobserve request",
    )
    if args.disable_noise:
        request["request"]["corruption"] = None
        request["request"]["model_peak_jy_per_pixel"] = None
        dataset = without_noise(dataset)
    if args.disable_prediction:
        if not args.skip_casa:
            raise BenchError("--disable-prediction requires --skip-casa")
        request["request"]["predict_model"] = False
        request["request"]["corruption"] = None

    casa = None
    casa_oracle = casa_oracle_status(args.casa_python, skip_casa=args.skip_casa)
    should_run_casa = casa_oracle["status"] == "available"
    run_casa_first = args.strict_values and should_run_casa
    if run_casa_first:
        casa = run_casa_repeats(
            args.casa_python,
            dataset,
            run_root / "casa",
            repeats=args.repeats,
        )
        align_request_start_time_to_ms(
            args.casa_python,
            request,
            casa["last_ms"],
        )
        request["request"]["allow_below_elevation_limit"] = True
        request["request"]["elevation_limit_rad"] = 8.0 * 3.141592653589793 / 180.0

    native_parallel = run_native_repeats(
        args.casars_binary,
        request,
        run_root / "native-parallel",
        repeats=args.repeats,
        channel_workers=args.channel_workers,
    )
    native_serial = None
    if not args.skip_serial_check:
        native_serial = run_native_repeats(
            args.casars_binary,
            request,
            run_root / "native-serial",
            repeats=1,
            channel_workers=1,
        )
    native_fixed = None
    if args.fixed_channel_workers is not None:
        native_fixed = run_native_repeats(
            args.casars_binary,
            request,
            run_root / "native-fixed",
            repeats=1,
            channel_workers=args.fixed_channel_workers,
        )

    if should_run_casa and not run_casa_first:
        casa = run_casa_repeats(
            args.casa_python,
            dataset,
            run_root / "casa",
            repeats=args.repeats,
        )

    if pathlib.Path(args.casa_python).exists():
        correctness = collect_correctness(
            args.casa_python,
            native_parallel["last_ms"],
            None if native_serial is None else native_serial["last_ms"],
            None if casa is None else casa["last_ms"],
            strict_values=args.strict_values,
            strict_uvw_atol=args.strict_uvw_atol,
            strict_data_atol=args.strict_data_atol,
            strict_data_rtol=args.strict_data_rtol,
        )
    else:
        correctness = skipped_correctness(
            casa_oracle,
            strict_values=args.strict_values,
        )
    if casa is None:
        attach_casa_skip_to_correctness(
            correctness,
            casa_oracle,
            strict_values=args.strict_values,
        )
    speedup = None
    casa_relative = None
    if casa is not None:
        speedup = casa["best_seconds"] / native_parallel["best_seconds"]
        casa_relative = casa_relative_timing(native_parallel, casa)
        if args.require_speedup is not None and speedup < args.require_speedup:
            raise BenchError(
                f"native simobserve speedup {speedup:.2f}x is below target "
                f"{args.require_speedup:.2f}x"
            )
    native_performance = native_performance_summary(native_parallel)
    analytic_tier_performance = analytic_native_tier_performance(
        dataset,
        request,
        native_performance,
    )
    worker_comparison = native_worker_comparison(
        native_parallel,
        native_serial,
        native_fixed,
        primary_channel_workers=args.channel_workers,
        fixed_channel_workers=args.fixed_channel_workers,
    )
    oracle_comparison = oracle_comparison_summary(
        correctness,
        image_products=image_product_comparison(
            args.casa_python,
            args.native_image_prefix,
            args.casa_image_prefix,
            parse_image_product_suffixes(args.image_product_suffixes),
            casa_oracle,
        ),
    )
    enforce_native_performance_targets(args, native_performance)

    result_path = run_root / "simobserve-benchmark.json"
    report_path = run_root / "simobserve-benchmark.html"
    details = {
        "dataset": dataset["id"],
        "shape": dataset["shape"],
        "native_parallel": native_parallel,
        "native_serial": native_serial,
        "native_fixed": native_fixed,
        "casa": casa,
        "casa_oracle": casa_oracle if casa is None else {"status": "run"},
        "correctness": correctness,
        "oracle_comparison": oracle_comparison,
        "speedup_vs_casa": speedup,
        "performance_relative_to_casa": casa_relative,
        "native_performance": native_performance,
        "analytic_tier_performance": analytic_tier_performance,
        "native_worker_comparison": worker_comparison,
        "target": {
            "required_speedup": args.require_speedup,
            "required_native_throughput_mb_s": args.require_native_throughput_mb_s,
            "required_data_io_throughput_mb_s": args.require_data_io_throughput_mb_s,
        },
    }
    status = "completed"
    if correctness["status"] == "failed":
        status = "out_of_tolerance"
    results: dict[str, Any] = {"simobserve": details}
    if status != "completed":
        results["failure"] = {
            "kind": "comparison_tolerance",
            "reason": "; ".join(correctness.get("reasons", []))
            or "simobserve correctness comparison failed",
        }
    result = {
        "schema_version": RUN_RESULT_SCHEMA_VERSION,
        "kind": "simobserve_benchmark",
        "status": status,
        "run_id": run_id,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "environment": capture_provenance(
            repo_root=REPO_ROOT,
            executables={
                "native_simobserve": args.casars_binary,
                "casa_python": args.casa_python,
            },
            datasets={
                "plan": args.plan,
                "native_output": native_parallel["last_ms"],
                "casa_output": None if casa is None else casa["last_ms"],
            },
            storage_label="simobserve-benchmark",
        ),
        "artifacts": {
            "result_json": str(result_path),
            "report_html": str(report_path),
            "run_root": str(run_root),
        },
        "results": results,
    }
    validate_run_result(result, source=str(result_path))
    return result


def run_native_repeats(
    binary: pathlib.Path,
    request: dict[str, Any],
    output_dir: pathlib.Path,
    *,
    repeats: int,
    channel_workers: int | None,
) -> dict[str, Any]:
    if not binary.exists():
        raise BenchError(f"native simobserve binary does not exist: {binary}")
    output_dir.mkdir(parents=True, exist_ok=True)
    timings = []
    last_ms = None
    for index in range(repeats):
        run_dir = output_dir / f"run-{index + 1:02d}"
        if run_dir.exists():
            shutil.rmtree(run_dir)
        run_dir.mkdir(parents=True)
        run_request = json.loads(json.dumps(request))
        ms_path = run_dir / "native.ms"
        run_request["request"]["output_ms"] = str(ms_path)
        run_request["request"]["overwrite"] = True
        request_path = run_dir / "request.json"
        atomic_write_json(request_path, run_request)
        env = os.environ.copy()
        if channel_workers is not None:
            env["CASA_RS_SIMOBSERVE_CHANNEL_WORKERS"] = str(channel_workers)
        started = time.perf_counter()
        completed = run_command(
            [str(binary), "--json-run", str(request_path)],
            cwd=REPO_ROOT,
            environment=env,
            merge_stderr=False,
        )
        elapsed = time.perf_counter() - started
        (run_dir / "stdout.json").write_text(completed.stdout, encoding="utf-8")
        (run_dir / "stderr.log").write_text(completed.stderr, encoding="utf-8")
        if completed.returncode != 0:
            raise BenchError(
                f"native simobserve failed with exit {completed.returncode}: "
                f"{completed.stderr.strip()}"
            )
        native_result = parse_native_result(completed.stdout, run_dir / "stdout.json")
        timings.append(elapsed)
        last_ms = ms_path
    assert last_ms is not None
    return {
        "timings_seconds": timings,
        "best_seconds": min(timings),
        "median_seconds": statistics.median(timings),
        "last_ms": str(last_ms),
        "last_result": native_result,
        "channel_workers": channel_workers,
        "size_bytes": directory_size(last_ms),
    }


def run_casa_repeats(
    casa_python: str,
    dataset: dict[str, Any],
    output_dir: pathlib.Path,
    *,
    repeats: int,
) -> dict[str, Any]:
    python = pathlib.Path(casa_python)
    if not python.exists():
        raise BenchError(f"CASA Python does not exist: {python}")
    output_dir.mkdir(parents=True, exist_ok=True)
    timings = []
    last_ms = None
    for index in range(repeats):
        run_dir = output_dir / f"run-{index + 1:02d}"
        if run_dir.exists():
            shutil.rmtree(run_dir)
        run_dir.mkdir(parents=True)
        run_dataset = json.loads(json.dumps(dataset))
        run_dataset["paths"]["dataset_dir"] = str(run_dir / "dataset")
        run_dataset["paths"]["output_ms"] = str(run_dir / "casa.ms")
        run_dataset["paths"]["continuum_model_fits"] = str(
            run_dir / "dataset" / "models" / "structured-continuum.fits"
        )
        started = time.perf_counter()
        protocol = run_json_file_protocol(
            casa_python=str(python),
            script=CASA_MS_TOOLS,
            request={"operation": "generate_simobserve_dataset", "dataset": run_dataset},
            request_path=run_dir / "casa-request.json",
            output_path=run_dir / "casa-result.json",
            log_path=run_dir / "stdout.log",
            cwd=REPO_ROOT,
        )
        elapsed = time.perf_counter() - started
        if protocol.status != "completed":
            raise BenchError(
                f"CASA simobserve {protocol.status}: {protocol.reason}"
            )
        timings.append(elapsed)
        last_ms = run_dir / "casa.ms"
    assert last_ms is not None
    return {
        "timings_seconds": timings,
        "best_seconds": min(timings),
        "median_seconds": statistics.median(timings),
        "last_ms": str(last_ms),
        "size_bytes": directory_size(last_ms),
    }


def parse_native_result(stdout: str, source: pathlib.Path) -> dict[str, Any]:
    payload = parse_json_from_stdout(stdout, f"native simobserve {source}")
    if payload.get("kind") != "run":
        raise BenchError(f"native simobserve emitted unexpected result kind in {source}")
    return payload["result"]


def native_performance_summary(native: dict[str, Any]) -> dict[str, Any]:
    size_bytes = int(native["size_bytes"])
    best_seconds = float(native["best_seconds"])
    report = native.get("last_result", {}).get("report", {})
    main_timing = report.get("timing", {}).get("main_rows", {})
    data_io_bytes = int(main_timing.get("data_io_bytes") or 0)
    data_io_write_millis = float(main_timing.get("data_io_write_millis") or 0)
    return {
        "native_output_mb_per_second": mb_per_second(size_bytes, best_seconds),
        "data_io_mb_per_second": mb_per_second(
            data_io_bytes,
            data_io_write_millis / 1000.0 if data_io_write_millis > 0 else 0.0,
        ),
        "data_io_bytes": data_io_bytes,
        "data_io_write_millis": data_io_write_millis,
        "stage_timing": stage_timing_summary(report),
    }


def analytic_native_tier_performance(
    dataset: dict[str, Any],
    request: dict[str, Any],
    native_performance: dict[str, Any],
) -> dict[str, Any]:
    tier = str(dataset.get("tier") or dataset.get("shape", {}).get("tier") or "unknown")
    model_kind = request_model_kind(request)
    native_rate = native_performance.get("native_output_mb_per_second")
    data_io_rate = native_performance.get("data_io_mb_per_second")
    tiers = {"small": None, "medium": None}
    if tier in tiers and model_kind == "analytic_components":
        tiers[tier] = native_rate
    return {
        "status": "reported" if tiers.get(tier) is not None else "not_applicable",
        "dataset_tier": tier,
        "model_kind": model_kind,
        "small_native_output_mb_per_second": tiers["small"],
        "medium_native_output_mb_per_second": tiers["medium"],
        "native_output_mb_per_second": native_rate,
        "streamed_main_column_mb_per_second": data_io_rate,
    }


def request_model_kind(request: dict[str, Any]) -> str:
    payload = request.get("request", {})
    model = payload.get("model")
    if isinstance(model, dict) and model.get("kind"):
        return str(model["kind"])
    source_model = payload.get("source_model")
    if isinstance(source_model, dict) and source_model.get("kind"):
        return str(source_model["kind"])
    if payload.get("model_image") or payload.get("model_peak_jy_per_pixel") is not None:
        return "fits_image"
    return "unknown"


def native_worker_comparison(
    native_primary: dict[str, Any],
    native_serial: dict[str, Any] | None,
    native_fixed: dict[str, Any] | None,
    *,
    primary_channel_workers: int | None,
    fixed_channel_workers: int | None,
) -> dict[str, Any]:
    primary_mode = "auto" if primary_channel_workers is None else "fixed"
    auto = (
        worker_entry(native_primary, "auto", None)
        if primary_channel_workers is None
        else worker_not_run("primary run used fixed channel workers")
    )
    fixed = (
        worker_entry(native_primary, "fixed", primary_channel_workers)
        if primary_channel_workers is not None
        else worker_entry(native_fixed, "fixed", fixed_channel_workers)
    )
    serial = worker_entry(native_serial, "serial", 1)
    ratios = {
        "auto_speedup_vs_serial": speedup_against(serial, auto),
        "fixed_speedup_vs_serial": speedup_against(serial, fixed),
        "fixed_speedup_vs_auto": speedup_against(auto, fixed),
    }
    available = sum(
        1 for entry in (serial, auto, fixed) if entry.get("status") == "run"
    )
    return {
        "status": "complete" if available == 3 else "partial",
        "primary_mode": primary_mode,
        "serial": serial,
        "auto": auto,
        "fixed": fixed,
        "ratios": ratios,
    }


def worker_entry(
    run: dict[str, Any] | None,
    mode: str,
    channel_workers: int | None,
) -> dict[str, Any]:
    if run is None:
        return worker_not_run("not requested")
    return {
        "status": "run",
        "mode": mode,
        "channel_workers": channel_workers,
        "best_seconds": float(run["best_seconds"]),
        "median_seconds": float(run["median_seconds"]),
        "size_bytes": int(run["size_bytes"]),
    }


def worker_not_run(reason: str) -> dict[str, Any]:
    return {"status": "not_run", "reason": reason}


def speedup_against(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
) -> float | None:
    if baseline.get("status") != "run" or candidate.get("status") != "run":
        return None
    candidate_seconds = float(candidate["best_seconds"])
    if candidate_seconds <= 0.0:
        return None
    return float(baseline["best_seconds"]) / candidate_seconds


def casa_relative_timing(
    native: dict[str, Any], casa: dict[str, Any]
) -> dict[str, float]:
    native_best = float(native["best_seconds"])
    casa_best = float(casa["best_seconds"])
    native_size = int(native.get("size_bytes") or 0)
    casa_size = int(casa.get("size_bytes") or 0)
    return {
        "native_best_seconds": native_best,
        "casa_best_seconds": casa_best,
        "native_speedup_vs_casa": casa_best / native_best if native_best > 0.0 else 0.0,
        "native_time_fraction_of_casa": native_best / casa_best if casa_best > 0.0 else 0.0,
        "native_output_mb_per_second": mb_per_second(native_size, native_best),
        "casa_output_mb_per_second": mb_per_second(casa_size, casa_best),
        "native_size_fraction_of_casa": native_size / casa_size if casa_size > 0 else 0.0,
    }


def stage_timing_summary(report: dict[str, Any]) -> dict[str, Any]:
    timing = report.get("timing", {})
    main_rows = timing.get("main_rows", {})
    total_millis = float(timing.get("total_millis") or 0.0)
    stages = {
        "validate_millis": float(timing.get("validate_millis") or 0.0),
        "setup_millis": float(timing.get("setup_millis") or 0.0),
        "metadata_millis": float(timing.get("metadata_millis") or 0.0),
        "model_prepare_millis": float(timing.get("model_prepare_millis") or 0.0),
        "uvw_and_row_setup_millis": float(
            main_rows.get("uvw_and_row_setup_millis") or 0.0
        ),
        "prediction_millis": float(main_rows.get("prediction_millis") or 0.0),
        "prediction_worker_wall_millis": float(
            main_rows.get("prediction_worker_wall_millis") or 0.0
        ),
        "prediction_gather_millis": float(
            main_rows.get("prediction_gather_millis") or 0.0
        ),
        "corruption_millis": float(main_rows.get("corruption_millis") or 0.0),
        "data_io_enqueue_millis": float(
            main_rows.get("data_io_enqueue_millis") or 0.0
        ),
        "data_io_finalize_millis": float(
            main_rows.get("data_io_finalize_millis") or 0.0
        ),
        "data_io_assemble_millis": float(
            main_rows.get("data_io_assemble_millis") or 0.0
        ),
        "data_io_write_millis": float(main_rows.get("data_io_write_millis") or 0.0),
        "main_write_millis": float(main_rows.get("main_write_millis") or 0.0),
        "save_millis": float(timing.get("save_millis") or 0.0),
    }
    fractions = {
        name.replace("_millis", "_fraction"): (
            value / total_millis if total_millis > 0.0 else 0.0
        )
        for name, value in stages.items()
    }
    prediction_fraction = fractions.get("prediction_fraction", 0.0)
    io_fraction = (
        fractions.get("data_io_enqueue_fraction", 0.0)
        + fractions.get("data_io_finalize_fraction", 0.0)
        + fractions.get("data_io_assemble_fraction", 0.0)
        + fractions.get("data_io_write_fraction", 0.0)
    )
    return {
        "total_millis": total_millis,
        "stages_millis": stages,
        "stage_fractions": fractions,
        "prediction_fraction": prediction_fraction,
        "streamed_io_fraction": io_fraction,
        "gpu_candidate": prediction_fraction > max(0.25, io_fraction),
    }


def enforce_native_performance_targets(
    args: argparse.Namespace, performance: dict[str, Any]
) -> None:
    required_native = args.require_native_throughput_mb_s
    if required_native is not None:
        actual = performance["native_output_mb_per_second"]
        if actual < required_native:
            raise BenchError(
                f"native output throughput {actual:.1f} MB/s is below target "
                f"{required_native:.1f} MB/s"
            )
    required_data_io = args.require_data_io_throughput_mb_s
    if required_data_io is not None:
        actual = performance["data_io_mb_per_second"]
        if actual < required_data_io:
            raise BenchError(
                f"streamed DATA/FLAG/UVW/WEIGHT/SIGMA write throughput "
                f"{actual:.1f} MB/s is below target {required_data_io:.1f} MB/s"
            )


def mb_per_second(size_bytes: int, seconds: float) -> float:
    if seconds <= 0.0:
        return 0.0
    return size_bytes / seconds / 1_000_000.0


def casa_oracle_status(casa_python: str, *, skip_casa: bool) -> dict[str, Any]:
    if skip_casa:
        return {"status": "skipped", "reason": "--skip-casa was set"}
    python = pathlib.Path(casa_python)
    if not python.exists():
        return {
            "status": "skipped",
            "reason": f"CASA Python does not exist: {python}",
            "casa_python": str(python),
        }
    return {"status": "available", "casa_python": str(python)}


def attach_casa_skip_to_correctness(
    correctness: dict[str, Any],
    casa_oracle: dict[str, Any],
    *,
    strict_values: bool,
) -> None:
    correctness["casa_oracle"] = casa_oracle
    correctness["casa_status"] = casa_oracle["status"]
    if strict_values and correctness.get("strict_values") is None:
        correctness["strict_values"] = {
            "status": "skipped",
            "reason": casa_oracle.get("reason", "CASA oracle was not run"),
        }


def skipped_correctness(
    casa_oracle: dict[str, Any],
    *,
    strict_values: bool,
) -> dict[str, Any]:
    reason = casa_oracle.get(
        "reason",
        "CASA Python is unavailable, so casatools MS inspection could not run",
    )
    return {
        "status": "skipped",
        "reasons": [reason],
        "inspections": {},
        "strict_values": (
            {
                "status": "skipped",
                "reason": reason,
            }
            if strict_values
            else None
        ),
        "casa_oracle": casa_oracle,
        "casa_status": casa_oracle["status"],
    }


def parse_image_product_suffixes(value: str) -> list[str]:
    suffixes = [item.strip() for item in value.split(",") if item.strip()]
    if not suffixes:
        raise BenchError("--image-product-suffixes must include at least one suffix")
    invalid = [item for item in suffixes if not item.startswith(".")]
    if invalid:
        raise BenchError(
            "--image-product-suffixes entries must start with '.', got "
            + ", ".join(invalid)
        )
    return suffixes


def image_product_comparison(
    casa_python: str,
    native_prefix: pathlib.Path | None,
    casa_prefix: pathlib.Path | None,
    suffixes: list[str],
    casa_oracle: dict[str, Any],
) -> dict[str, Any]:
    if native_prefix is None and casa_prefix is None:
        return {
            "status": "not_run",
            "reason": "no image product prefixes were supplied",
        }
    if native_prefix is None or casa_prefix is None:
        return {
            "status": "skipped",
            "reason": "--native-image-prefix and --casa-image-prefix must be supplied together",
        }
    if casa_oracle.get("status") != "available":
        return {
            "status": "skipped",
            "reason": casa_oracle.get("reason", "CASA oracle is unavailable"),
            "native_prefix": str(native_prefix),
            "casa_prefix": str(casa_prefix),
        }
    return collect_image_product_comparison(
        casa_python,
        str(native_prefix),
        str(casa_prefix),
        suffixes,
    )


def collect_image_product_comparison(
    casa_python: str,
    native_prefix: str,
    casa_prefix: str,
    suffixes: list[str],
) -> dict[str, Any]:
    artifact_prefix = pathlib.Path(native_prefix).parent / "simobserve-image-products"
    panel_dir = artifact_prefix.with_suffix(".panels")
    comparison = compare_image_products(
        casa_python=casa_python,
        request={
            "rust_prefix": native_prefix,
            "casa_prefix": casa_prefix,
            "products": suffixes,
            "max_elements_per_product": 8_000_000,
            "panel_dir": str(panel_dir),
        },
        artifact_prefix=artifact_prefix,
        cwd=REPO_ROOT,
    )
    comparison["native_prefix"] = native_prefix
    comparison["casa_prefix"] = casa_prefix
    comparison["panel_dir"] = str(panel_dir)
    return comparison

def oracle_comparison_summary(
    correctness: dict[str, Any],
    *,
    image_products: dict[str, Any],
) -> dict[str, Any]:
    strict = correctness.get("strict_values")
    if correctness.get("status") == "skipped":
        ms_samples = {
            "status": "skipped",
            "reason": "; ".join(correctness.get("reasons", [])),
        }
    elif not strict:
        ms_samples = {
            "status": "not_run",
            "reason": "strict CASA/native value comparison was not requested",
        }
    elif strict.get("status") == "skipped":
        ms_samples = {
            "status": "skipped",
            "reason": strict.get("reason"),
        }
    else:
        ms_samples = {
            "status": strict.get("status", "unknown"),
            "row_count": strict.get("row_count"),
            "rows_sampled": strict.get("rows_sampled"),
            "uvw": strict.get("uvw"),
            "flag_counts": strict.get("flag_counts"),
            "raw_flag_mismatches": strict.get("raw_flag_mismatches"),
            "effective_flag_mismatches": strict.get("effective_flag_mismatches"),
            "weight": strict.get("weight"),
            "sigma": strict.get("sigma"),
            "data": strict.get("data"),
        }
    return {
        "measurement_set_samples": ms_samples,
        "imaging_products": image_products,
    }


def without_noise(dataset: dict[str, Any]) -> dict[str, Any]:
    dataset = json.loads(json.dumps(dataset))
    source_model = dataset.setdefault("source_model", {})
    source_model["noise_simplenoise_jy"] = 0.0
    return dataset


def align_request_start_time_to_ms(
    casa_python: str,
    request: dict[str, Any],
    ms_path: str,
) -> None:
    first_time = read_first_ms_time(casa_python, ms_path)
    integration_seconds = float(request["request"]["integration_seconds"])
    request["request"]["start_time_mjd_seconds"] = first_time - 0.5 * integration_seconds


def read_first_ms_time(casa_python: str, ms_path: str) -> float:
    artifact_root = pathlib.Path(ms_path).parent
    protocol = run_json_file_protocol(
        casa_python=casa_python,
        script=CASA_MS_TOOLS,
        request={"operation": "first_ms_time", "path": ms_path},
        request_path=artifact_root / "first-ms-time.request.json",
        output_path=artifact_root / "first-ms-time.result.json",
        log_path=artifact_root / "first-ms-time.log",
        cwd=REPO_ROOT,
    )
    if protocol.status != "completed" or protocol.output is None:
        raise BenchError(
            f"failed to inspect first CASA MS time: {protocol.status}: {protocol.reason}"
        )
    return float(protocol.output["first_time"])


def parse_json_from_stdout(stdout: str, context: str) -> dict[str, Any]:
    stripped_stdout = stdout.strip()
    if stripped_stdout:
        try:
            payload = json.loads(stripped_stdout)
            if isinstance(payload, dict):
                return payload
        except json.JSONDecodeError:
            pass
    for line in reversed(stdout.splitlines()):
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    raise BenchError(f"{context} stdout did not contain a JSON object")


def collect_correctness(
    casa_python: str,
    native_parallel_ms: str,
    native_serial_ms: str | None,
    casa_ms: str | None,
    *,
    strict_values: bool,
    strict_uvw_atol: float,
    strict_data_atol: float,
    strict_data_rtol: float,
) -> dict[str, Any]:
    paths = {"native_parallel": native_parallel_ms}
    if native_serial_ms is not None:
        paths["native_serial"] = native_serial_ms
    if casa_ms is not None:
        paths["casa"] = casa_ms
    artifact_root = pathlib.Path(native_parallel_ms).parent.parent / "ms-comparison"
    artifact_root.mkdir(parents=True, exist_ok=True)
    protocol = run_json_file_protocol(
        casa_python=casa_python,
        script=CASA_MS_TOOLS,
        request={"operation": "inspect_measurement_sets", "paths": paths},
        request_path=artifact_root / "inspection.request.json",
        output_path=artifact_root / "inspection.result.json",
        log_path=artifact_root / "inspection.log",
        cwd=REPO_ROOT,
    )
    if protocol.status != "completed" or protocol.output is None:
        raise BenchError(
            f"failed to inspect benchmark MS outputs: {protocol.status}: {protocol.reason}"
        )
    inspections = protocol.output
    status = "passed"
    reasons = []
    native = inspections["native_parallel"]
    if native_serial_ms is not None and inspections["native_serial"] != native:
        status = "failed"
        reasons.append("native parallel output differs from native serial output")
    if casa_ms is not None:
        casa = inspections["casa"]
        for key in ("rows", "data_shape"):
            if casa[key] != native[key]:
                status = "failed"
                reasons.append(f"CASA {key} differs from native {key}")
        for name in ("FIELD", "SPECTRAL_WINDOW", "DATA_DESCRIPTION", "OBSERVATION"):
            if casa["subtable_rows"][name] != native["subtable_rows"][name]:
                status = "failed"
                reasons.append(f"CASA {name} row count differs from native {name} row count")
        strict = None
        if strict_values:
            strict = collect_strict_value_comparison(
                casa_python,
                native_parallel_ms,
                casa_ms,
                uvw_atol=strict_uvw_atol,
                data_atol=strict_data_atol,
                data_rtol=strict_data_rtol,
            )
            if strict["status"] != "passed":
                status = "failed"
                reasons.extend(strict["reasons"])
    return {
        "status": status,
        "reasons": reasons,
        "inspections": inspections,
        "strict_values": strict if casa_ms is not None and strict_values else None,
    }


def collect_strict_value_comparison(
    casa_python: str,
    native_ms: str,
    casa_ms: str,
    *,
    uvw_atol: float,
    data_atol: float,
    data_rtol: float,
) -> dict[str, Any]:
    return collect_sampled_strict_value_comparison(
        casa_python,
        native_ms,
        casa_ms,
        uvw_atol=uvw_atol,
        data_atol=data_atol,
        data_rtol=data_rtol,
    )


def collect_sampled_strict_value_comparison(
    casa_python: str,
    native_ms: str,
    casa_ms: str,
    *,
    uvw_atol: float,
    data_atol: float,
    data_rtol: float,
) -> dict[str, Any]:
    artifact_prefix = pathlib.Path(native_ms).parent.parent / "ms-comparison" / "sampled"
    artifact_prefix.parent.mkdir(parents=True, exist_ok=True)
    result = compare_measurement_sets(
        casa_python=casa_python,
        native_path=native_ms,
        casa_path=casa_ms,
        mode="sampled",
        uvw_atol=uvw_atol,
        data_atol=data_atol,
        data_rtol=data_rtol,
        artifact_prefix=artifact_prefix,
        cwd=REPO_ROOT,
    )
    if result["status"] in {"failed_execution", "unavailable"}:
        raise BenchError(
            f"failed to run sampled strict MS value comparison: {result.get('reason')}"
        )
    return result


def write_html_report(path: pathlib.Path, result: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    speedup = result.get("speedup_vs_casa")
    speedup_text = "not run" if speedup is None else f"{speedup:.2f}x"
    native = result["native_parallel"]
    native_perf = result.get("native_performance", {})
    casa = result.get("casa")
    rows = result["shape"]["estimated_main_rows"]
    channels = result["shape"]["channels"]
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>simobserve benchmark: {escape(result["dataset"])}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #1f2933; }}
    h1, h2 {{ color: #102a43; }}
    table {{ border-collapse: collapse; margin: 12px 0 24px; width: 100%; max-width: 1100px; }}
    th, td {{ border: 1px solid #bcccdc; padding: 8px 10px; text-align: left; vertical-align: top; }}
    th {{ background: #f0f4f8; }}
    code, pre {{ background: #f0f4f8; border-radius: 4px; }}
    code {{ padding: 2px 4px; }}
    pre {{ padding: 12px; overflow: auto; }}
    .status-passed {{ color: #0b6b3a; font-weight: 700; }}
    .status-failed {{ color: #a61b1b; font-weight: 700; }}
  </style>
</head>
<body>
  <h1>simobserve benchmark: {escape(result["dataset"])}</h1>
  <table>
    <tr><th>Rows</th><td>{rows:,}</td></tr>
    <tr><th>Channels</th><td>{channels:,}</td></tr>
    <tr><th>Native best</th><td>{native["best_seconds"]:.3f} s</td></tr>
    <tr><th>Native size</th><td>{format_bytes(native["size_bytes"])}</td></tr>
    <tr><th>Native throughput</th><td>{format_rate(native_perf.get("native_output_mb_per_second"))}</td></tr>
    <tr><th>Streamed write throughput</th><td>{format_rate(native_perf.get("data_io_mb_per_second"))}</td></tr>
    <tr><th>CASA best</th><td>{format_seconds(casa)}</td></tr>
    <tr><th>CASA size</th><td>{format_bytes(casa["size_bytes"]) if casa else "not run"}</td></tr>
    <tr><th>Speedup vs CASA</th><td>{speedup_text}</td></tr>
    <tr><th>Correctness status</th><td class="status-{escape(result["correctness"]["status"])}">{escape(result["correctness"]["status"])}</td></tr>
  </table>
  <h2>Native Timing</h2>
  <pre>{escape(json.dumps(native.get("last_result", {}).get("report", {}).get("timing", {}), indent=2, sort_keys=True))}</pre>
  <h2>MS Inspections</h2>
  <pre>{escape(json.dumps(result["correctness"], indent=2, sort_keys=True))}</pre>
  <h2>Full Result JSON</h2>
  <pre>{escape(json.dumps(result, indent=2, sort_keys=True))}</pre>
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")


def escape(value: Any) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def format_seconds(run: dict[str, Any] | None) -> str:
    if run is None:
        return "not run"
    return f"{run['best_seconds']:.3f} s"


def format_bytes(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0 or unit == "TiB":
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{size} B"


def format_rate(rate: Any) -> str:
    if rate is None:
        return "not reported"
    return f"{float(rate):.1f} MB/s"


def select_dataset(plan: dict[str, Any], dataset_id: str) -> dict[str, Any]:
    for dataset in plan.get("datasets", []):
        if dataset.get("id") == dataset_id:
            return dataset
    raise BenchError(f"dataset not found in plan: {dataset_id}")


def directory_size(path: pathlib.Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += item.stat().st_size
    return total


if __name__ == "__main__":
    main()
