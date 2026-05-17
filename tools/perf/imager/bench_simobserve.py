#!/usr/bin/env python3
"""Benchmark native simobserve against CASA simobserve for a Wave 1 dataset."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import statistics
import shutil
import subprocess
import sys
import time
from typing import Any


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
DEFAULT_BINARY = REPO_ROOT / "target" / "release" / "simobserve"


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
        default=pathlib.Path("target/imperformance-wave1/simobserve-bench"),
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
    parser.add_argument("--strict-data-rtol", type=float, default=1.0e-3)
    parser.add_argument("--skip-casa", action="store_true")
    parser.add_argument("--skip-serial-check", action="store_true")
    args = parser.parse_args()

    try:
        result = run_benchmark(args)
        result_path = pathlib.Path(result["result_json"])
        write_json(result_path, result)
        write_html_report(pathlib.Path(result["report_html"]), result)
        print(result_path)
        if result["correctness"]["status"] != "passed":
            raise SystemExit(2)
    except BenchError as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    if args.repeats < 1:
        raise BenchError("--repeats must be >= 1")
    plan = read_json(args.plan)
    dataset = select_dataset(plan, args.dataset)
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{dataset['id']}"
    run_root = output_dir / run_id
    run_root.mkdir(parents=True, exist_ok=True)

    request = read_json(pathlib.Path(dataset["paths"]["casars_simobserve_request"]))
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
    run_casa_first = args.strict_values and not args.skip_casa
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

    if not args.skip_casa and not run_casa_first:
        casa = run_casa_repeats(
            args.casa_python,
            dataset,
            run_root / "casa",
            repeats=args.repeats,
        )

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
    speedup = None
    if casa is not None:
        speedup = casa["best_seconds"] / native_parallel["best_seconds"]
        if args.require_speedup is not None and speedup < args.require_speedup:
            raise BenchError(
                f"native simobserve speedup {speedup:.2f}x is below target "
                f"{args.require_speedup:.2f}x"
            )
    native_performance = native_performance_summary(native_parallel)
    enforce_native_performance_targets(args, native_performance)

    result_path = run_root / "simobserve-benchmark.json"
    report_path = run_root / "simobserve-benchmark.html"
    return {
        "schema_version": 1,
        "result_json": str(result_path),
        "report_html": str(report_path),
        "run_root": str(run_root),
        "dataset": dataset["id"],
        "shape": dataset["shape"],
        "native_parallel": native_parallel,
        "native_serial": native_serial,
        "casa": casa,
        "correctness": correctness,
        "speedup_vs_casa": speedup,
        "native_performance": native_performance,
        "target": {
            "required_speedup": args.require_speedup,
            "required_native_throughput_mb_s": args.require_native_throughput_mb_s,
            "required_data_io_throughput_mb_s": args.require_data_io_throughput_mb_s,
        },
    }


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
        write_json(request_path, run_request)
        env = os.environ.copy()
        if channel_workers is not None:
            env["CASA_RS_SIMOBSERVE_CHANNEL_WORKERS"] = str(channel_workers)
        started = time.perf_counter()
        completed = subprocess.run(
            [str(binary), "--json-run", str(request_path)],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
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
        script = run_dir / "run-casa-simobserve.py"
        script.write_text(
            CASA_RUNNER.format(dataset_json=json.dumps(run_dataset)),
            encoding="utf-8",
        )
        started = time.perf_counter()
        completed = subprocess.run(
            [str(python), str(script)],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        elapsed = time.perf_counter() - started
        (run_dir / "stdout.jsonl").write_text(completed.stdout, encoding="utf-8")
        (run_dir / "stderr.log").write_text(completed.stderr, encoding="utf-8")
        if completed.returncode != 0:
            raise BenchError(
                f"CASA simobserve failed with exit {completed.returncode}: "
                f"{completed.stderr.strip()}"
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
    script = r'''
import json
import sys
from casatools import table

path = json.loads(sys.argv[1])
tb = table()
tb.open(path)
try:
    value = float(tb.getcell("TIME", 0))
finally:
    tb.close()
print(json.dumps({"first_time": value}))
'''
    completed = subprocess.run(
        [casa_python, "-c", script, json.dumps(ms_path)],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise BenchError(
            "failed to inspect first CASA MS time: " + completed.stderr.strip()
        )
    return float(parse_json_from_stdout(completed.stdout, "first MS time")["first_time"])


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


CASA_RUNNER = r'''
import json
import pathlib
import sys

sys.path.insert(0, "{tool_dir}")
import generate_wave1_casa_datasets as gen

dataset = json.loads({dataset_json!r})
result = gen.generate_dataset(
    dataset,
    skip_existing=False,
    overwrite=True,
    preview=False,
    preview_max_pixels=128,
)
print(json.dumps(result, sort_keys=True))
'''.replace("{tool_dir}", str(pathlib.Path(__file__).resolve().parent))


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
    script = (
        "import json,sys\n"
        "import numpy as np\n"
        "from casatools import table\n"
        "paths=json.loads(sys.argv[1])\n"
    )
    script += r'''
def subtable_rows(path, name):
    tb = table()
    try:
        tb.open(path + "/" + name)
        return int(tb.nrows())
    finally:
        try:
            tb.close()
        except Exception:
            pass

def stats(values):
    arr = np.asarray(values)
    return {
        "min": float(arr.min()),
        "max": float(arr.max()),
        "mean": float(arr.mean()),
    }

def data_stats(cell):
    arr = np.asarray(cell)
    amp = np.abs(arr)
    return {
        "shape": list(arr.shape),
        "abs_sum": float(amp.sum()),
        "abs_max": float(amp.max()),
        "real_mean": float(arr.real.mean()),
        "imag_mean": float(arr.imag.mean()),
    }

def inspect(path):
    tb = table()
    tb.open(path)
    try:
        rows = tb.nrows()
        colnames = tb.colnames()
        data = tb.getcell("DATA", 0)
        uvw = tb.getcell("UVW", 0)
        selected = sorted(set([0, max(0, rows // 2), max(0, rows - 1)]))
        selected_data = {str(row): data_stats(tb.getcell("DATA", row)) for row in selected}
        uvw_col = tb.getcol("UVW")
    finally:
        tb.close()
    return {
        "rows": int(rows),
        "columns": colnames,
        "complex_visibility_columns": [
            column for column in ["DATA", "MODEL_DATA", "CORRECTED_DATA", "FLOAT_DATA"]
            if column in colnames
        ],
        "data_shape": list(data.shape),
        "first_data_abs_sum": float(abs(data).sum()),
        "first_uvw": [float(value) for value in uvw],
        "uvw_stats": {
            "u_m": stats(uvw_col[0, :]),
            "v_m": stats(uvw_col[1, :]),
            "w_m": stats(uvw_col[2, :]),
        },
        "selected_data_stats": selected_data,
        "subtable_rows": {
            "FIELD": subtable_rows(path, "FIELD"),
            "SPECTRAL_WINDOW": subtable_rows(path, "SPECTRAL_WINDOW"),
            "DATA_DESCRIPTION": subtable_rows(path, "DATA_DESCRIPTION"),
            "OBSERVATION": subtable_rows(path, "OBSERVATION"),
            "POINTING": subtable_rows(path, "POINTING"),
        },
    }
print(json.dumps({name: inspect(path) for name, path in paths.items()}, sort_keys=True))
'''
    completed = subprocess.run(
        [casa_python, "-c", script, json.dumps(paths)],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise BenchError(f"failed to inspect benchmark MS outputs: {completed.stderr.strip()}")
    inspections = parse_json_from_stdout(completed.stdout, "MS inspection")
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
    script = r'''
import json
import sys
import numpy as np
from casatools import table

native_path, casa_path, uvw_atol, data_atol, data_rtol = json.loads(sys.argv[1])

KEY_COLUMNS = ["TIME", "FIELD_ID", "DATA_DESC_ID", "ANTENNA1", "ANTENNA2"]
SAMPLE_ROWS = 513

def open_table(path):
    tb = table()
    tb.open(path)
    return tb

def key_tuple(keys, row):
    return tuple(key[row].item() if hasattr(key[row], "item") else key[row] for key in keys)

def phase_residual_diagnostics(samples):
    if not samples:
        return {"samples": 0, "fields": {}}
    values = np.asarray(samples, dtype=np.float64)
    fields = {}
    for field_id in sorted(set(values[:, 0].astype(int))):
        field_values = values[values[:, 0] == field_id]
        if field_values.shape[0] < 4:
            continue
        high_amplitude_cut = np.percentile(field_values[:, 7], 50)
        fit_values = field_values[field_values[:, 7] >= high_amplitude_cut]
        if fit_values.shape[0] < 4:
            fit_values = field_values
        design = np.column_stack([
            fit_values[:, 2],
            fit_values[:, 3],
            fit_values[:, 4],
            np.ones(fit_values.shape[0]),
        ])
        phase = fit_values[:, 5]
        beta, *_ = np.linalg.lstsq(design, phase, rcond=None)
        residual = phase - design @ beta
        amp_ratio = fit_values[:, 6]
        abs_delta = fit_values[:, 8]
        fields[str(field_id)] = {
            "samples": int(field_values.shape[0]),
            "fit_samples": int(fit_values.shape[0]),
            "phase_fit_rad_per_lambda": {
                "u": float(beta[0]),
                "v": float(beta[1]),
                "w": float(beta[2]),
                "constant": float(beta[3]),
            },
            "phase_residual_rms_rad": float(np.sqrt(np.mean(residual * residual))),
            "phase_min_rad": float(phase.min()),
            "phase_max_rad": float(phase.max()),
            "amplitude_ratio": {
                "mean": float(amp_ratio.mean()),
                "std": float(amp_ratio.std()),
                "min": float(amp_ratio.min()),
                "max": float(amp_ratio.max()),
            },
            "abs_delta": {
                "mean": float(abs_delta.mean()),
                "max": float(abs_delta.max()),
            },
        }
    return {"samples": int(values.shape[0]), "fields": fields}

def read_keys(path):
    tb = open_table(path)
    try:
        rows = int(tb.nrows())
        keys = [np.asarray(tb.getcol(column)) for column in KEY_COLUMNS]
    finally:
        tb.close()
    return rows, keys

native_rows, native_keys = read_keys(native_path)
casa_rows, casa_keys = read_keys(casa_path)
result = {
    "status": "passed",
    "reasons": [],
    "sampled": True,
    "row_count": {"native": native_rows, "casa": casa_rows},
    "thresholds": {
        "uvw_atol": uvw_atol,
        "data_atol": data_atol,
        "data_rtol": data_rtol,
    },
}
if native_rows != casa_rows:
    result["status"] = "failed"
    result["reasons"].append("strict row count mismatch")
    print(json.dumps(result, sort_keys=True))
    raise SystemExit(0)

casa_by_key = {key_tuple(casa_keys, row): row for row in range(casa_rows)}
if native_rows <= SAMPLE_ROWS:
    native_sample_rows = list(range(native_rows))
else:
    native_sample_rows = sorted(set(np.linspace(0, native_rows - 1, SAMPLE_ROWS, dtype=np.int64).tolist()))

native_tb = open_table(native_path)
casa_tb = open_table(casa_path)
spw_tb = open_table(casa_path + "/SPECTRAL_WINDOW")
try:
    channel_frequencies_hz = np.asarray(spw_tb.getcell("CHAN_FREQ", 0), dtype=np.float64)
    uvw_max_abs = 0.0
    uvw_mean_sum = 0.0
    uvw_count = 0
    data_max_abs = 0.0
    data_sum_abs = 0.0
    data_max_relative = 0.0
    data_violation_count = 0
    data_violation_max_abs = 0.0
    data_violation_max_relative = 0.0
    data_count = 0
    raw_flag_mismatches = 0
    effective_flag_mismatches = 0
    weight_max_abs = 0.0
    sigma_max_abs = 0.0
    missing_keys = []
    worst_cells = []
    phase_samples = []

    for native_row in native_sample_rows:
        key = key_tuple(native_keys, native_row)
        casa_row = casa_by_key.get(key)
        if casa_row is None:
            missing_keys.append({"native_row": int(native_row), "key": list(key)})
            continue

        native_uvw = np.asarray(native_tb.getcell("UVW", int(native_row)), dtype=np.float64)
        casa_uvw = np.asarray(casa_tb.getcell("UVW", int(casa_row)), dtype=np.float64)
        uvw_delta = np.abs(native_uvw - casa_uvw)
        uvw_max_abs = max(uvw_max_abs, float(uvw_delta.max()) if uvw_delta.size else 0.0)
        uvw_mean_sum += float(uvw_delta.sum())
        uvw_count += int(uvw_delta.size)

        native_data = np.asarray(native_tb.getcell("DATA", int(native_row)))
        casa_data = np.asarray(casa_tb.getcell("DATA", int(casa_row)))
        native_flag = np.asarray(native_tb.getcell("FLAG", int(native_row)), dtype=bool)
        casa_flag = np.asarray(casa_tb.getcell("FLAG", int(casa_row)), dtype=bool)
        native_flag_row = bool(native_tb.getcell("FLAG_ROW", int(native_row)))
        casa_flag_row = bool(casa_tb.getcell("FLAG_ROW", int(casa_row)))
        native_effective_flag = native_flag | native_flag_row
        casa_effective_flag = casa_flag | casa_flag_row

        raw_flag_mismatches += int(np.count_nonzero(native_flag != casa_flag))
        effective_flag_mismatches += int(np.count_nonzero(native_effective_flag != casa_effective_flag))
        mask = ~(native_effective_flag | casa_effective_flag)
        if np.any(mask):
            delta = np.abs(native_data - casa_data)
            amp = np.abs(casa_data)
            selected_delta = delta[mask]
            selected_amp = amp[mask]
            relative = selected_delta / np.maximum(selected_amp, data_atol)
            row_max_abs = float(selected_delta.max())
            row_max_relative = float(relative.max())
            data_max_abs = max(data_max_abs, row_max_abs)
            data_max_relative = max(data_max_relative, row_max_relative)
            data_sum_abs += float(selected_delta.sum())
            data_count += int(selected_delta.size)
            violation_mask = (selected_delta > data_atol) & (relative > data_rtol)
            if np.any(violation_mask):
                data_violation_count += int(np.count_nonzero(violation_mask))
                violation_delta = selected_delta[violation_mask]
                violation_relative = relative[violation_mask]
                data_violation_max_abs = max(
                    data_violation_max_abs,
                    float(violation_delta.max()),
                )
                data_violation_max_relative = max(
                    data_violation_max_relative,
                    float(violation_relative.max()),
                )
            if row_max_abs == data_max_abs or row_max_relative == data_max_relative:
                corr, chan = np.argwhere(mask)[int(np.argmax(selected_delta))]
                worst_cells.append({
                    "native_row": int(native_row),
                    "casa_row": int(casa_row),
                    "correlation": int(corr),
                    "channel": int(chan),
                    "abs": row_max_abs,
                    "relative": row_max_relative,
                    "native": {
                        "real": float(native_data[corr, chan].real),
                        "imag": float(native_data[corr, chan].imag),
                    },
                    "casa": {
                        "real": float(casa_data[corr, chan].real),
                        "imag": float(casa_data[corr, chan].imag),
                    },
                })
            unflagged_cells = np.argwhere(mask)
            for corr, chan in unflagged_cells:
                # Correlations are identical for these scalar simulation
                # products; keep one polarization so fits describe rows and
                # channels rather than double-counting the same residual.
                if int(corr) != 0:
                    continue
                casa_value = casa_data[corr, chan]
                native_value = native_data[corr, chan]
                casa_amplitude = abs(casa_value)
                if casa_amplitude <= data_atol:
                    continue
                ratio = native_value / casa_value
                frequency_hz = float(channel_frequencies_hz[int(chan)])
                phase_samples.append((
                    int(key[1]),
                    int(chan),
                    float(native_uvw[0] * frequency_hz / 299792458.0),
                    float(native_uvw[1] * frequency_hz / 299792458.0),
                    float(native_uvw[2] * frequency_hz / 299792458.0),
                    float(np.angle(ratio)),
                    float(abs(ratio)),
                    float(casa_amplitude),
                    float(delta[corr, chan]),
                ))

        native_weight = np.asarray(native_tb.getcell("WEIGHT", int(native_row)), dtype=np.float64)
        casa_weight = np.asarray(casa_tb.getcell("WEIGHT", int(casa_row)), dtype=np.float64)
        native_sigma = np.asarray(native_tb.getcell("SIGMA", int(native_row)), dtype=np.float64)
        casa_sigma = np.asarray(casa_tb.getcell("SIGMA", int(casa_row)), dtype=np.float64)
        weight_max_abs = max(weight_max_abs, float(np.abs(native_weight - casa_weight).max()))
        sigma_max_abs = max(sigma_max_abs, float(np.abs(native_sigma - casa_sigma).max()))
finally:
    spw_tb.close()
    native_tb.close()
    casa_tb.close()

result["rows_sampled"] = len(native_sample_rows)
result["missing_key_count"] = len(missing_keys)
if missing_keys:
    result["missing_keys"] = missing_keys[:10]
    result["status"] = "failed"
    result["reasons"].append("strict sampled row key missing in CASA")
result["uvw"] = {
    "max_abs": uvw_max_abs,
    "mean_abs": uvw_mean_sum / uvw_count if uvw_count else 0.0,
}
result["data"] = {
    "compared_unflagged_cells": data_count,
    "max_abs": data_max_abs,
    "mean_abs": data_sum_abs / data_count if data_count else 0.0,
    "max_relative": data_max_relative,
    "violating_cells": data_violation_count,
    "violation_max_abs": data_violation_max_abs,
    "violation_max_relative": data_violation_max_relative,
    "worst_cells": worst_cells[-10:],
}
result["phase_residual_diagnostics"] = phase_residual_diagnostics(phase_samples)
result["raw_flag_mismatches"] = raw_flag_mismatches
result["effective_flag_mismatches"] = effective_flag_mismatches
result["weight"] = {"max_abs": weight_max_abs}
result["sigma"] = {"max_abs": sigma_max_abs}

if uvw_max_abs > uvw_atol:
    result["status"] = "failed"
    result["reasons"].append(f"strict sampled UVW max abs {uvw_max_abs:.6g} exceeds {uvw_atol:.6g}")
if data_violation_count:
    result["status"] = "failed"
    result["reasons"].append(
        f"strict sampled DATA has {data_violation_count} cells exceeding both tolerances "
        f"(max abs {data_violation_max_abs:.6g}, max rel {data_violation_max_relative:.6g})"
    )
if effective_flag_mismatches:
    result["status"] = "failed"
    result["reasons"].append(f"strict sampled effective FLAG differs in {effective_flag_mismatches} cells")
if weight_max_abs > 1.0e-6:
    result["status"] = "failed"
    result["reasons"].append(f"strict sampled WEIGHT max abs {weight_max_abs:.6g} exceeds 1e-6")
if sigma_max_abs > 1.0e-6:
    result["status"] = "failed"
    result["reasons"].append(f"strict sampled SIGMA max abs {sigma_max_abs:.6g} exceeds 1e-6")

print(json.dumps(result, sort_keys=True))
'''
    completed = subprocess.run(
        [
            casa_python,
            "-c",
            script,
            json.dumps([native_ms, casa_ms, uvw_atol, data_atol, data_rtol]),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise BenchError(
            "failed to run sampled strict MS value comparison: " + completed.stderr.strip()
        )
    return parse_json_from_stdout(completed.stdout, "sampled strict MS value comparison")


def collect_legacy_full_strict_value_comparison(
    casa_python: str,
    native_ms: str,
    casa_ms: str,
    *,
    uvw_atol: float,
    data_atol: float,
    data_rtol: float,
) -> dict[str, Any]:
    script = r'''
import json
import sys
from collections import Counter
import numpy as np
from casatools import table

native_path, casa_path, uvw_atol, data_atol, data_rtol = json.loads(sys.argv[1])

KEY_COLUMNS = ["TIME", "FIELD_ID", "DATA_DESC_ID", "ANTENNA1", "ANTENNA2"]

def read_ms(path):
    tb = table()
    tb.open(path)
    try:
        rows = int(tb.nrows())
        payload = {
            "rows": rows,
            "keys": [np.asarray(tb.getcol(column)) for column in KEY_COLUMNS],
            "uvw": np.asarray(tb.getcol("UVW")),
            "data": np.asarray(tb.getcol("DATA")),
            "flag": np.asarray(tb.getcol("FLAG")),
            "flag_row": np.asarray(tb.getcol("FLAG_ROW"), dtype=bool),
            "weight": np.asarray(tb.getcol("WEIGHT")),
            "sigma": np.asarray(tb.getcol("SIGMA")),
        }
    finally:
        tb.close()
    spw = table()
    spw.open(path + "/SPECTRAL_WINDOW")
    try:
        payload["chan_freq_hz"] = np.asarray(spw.getcell("CHAN_FREQ", 0), dtype=np.float64)
    finally:
        spw.close()
    return payload

def order_for(keys):
    sortable = []
    for row in range(len(keys[0])):
        sortable.append(tuple(key[row].item() if hasattr(key[row], "item") else key[row] for key in keys))
    return np.asarray(sorted(range(len(sortable)), key=lambda row: sortable[row]), dtype=np.int64), sortable

def scalar_column_delta(native_keys, casa_keys, native_order, casa_order, index):
    native = native_keys[index][native_order]
    casa = casa_keys[index][casa_order]
    if np.issubdtype(native.dtype, np.floating):
        return float(np.max(np.abs(native - casa))) if native.size else 0.0
    return int(np.count_nonzero(native != casa))

native = read_ms(native_path)
casa = read_ms(casa_path)
result = {
    "status": "passed",
    "reasons": [],
    "row_count": {"native": native["rows"], "casa": casa["rows"]},
    "thresholds": {
        "uvw_atol": uvw_atol,
        "data_atol": data_atol,
        "data_rtol": data_rtol,
    },
}
if native["rows"] != casa["rows"]:
    result["status"] = "failed"
    result["reasons"].append("strict row count mismatch")
    print(json.dumps(result, sort_keys=True))
    raise SystemExit(0)

native_order, native_sortable = order_for(native["keys"])
casa_order, casa_sortable = order_for(casa["keys"])

key_deltas = {}
for index, column in enumerate(KEY_COLUMNS):
    key_deltas[column] = scalar_column_delta(
        native["keys"], casa["keys"], native_order, casa_order, index
    )
result["key_deltas"] = key_deltas
for column, delta in key_deltas.items():
    if delta != 0:
        result["status"] = "failed"
        result["reasons"].append(f"strict {column} differs after row normalization")

native_uvw = native["uvw"][:, native_order]
casa_uvw = casa["uvw"][:, casa_order]
uvw_abs = np.abs(native_uvw - casa_uvw)
result["uvw"] = {
    "max_abs": float(uvw_abs.max()) if uvw_abs.size else 0.0,
    "mean_abs": float(uvw_abs.mean()) if uvw_abs.size else 0.0,
}
if result["uvw"]["max_abs"] > uvw_atol:
    result["status"] = "failed"
    result["reasons"].append(
        f"strict UVW max abs {result['uvw']['max_abs']:.6g} exceeds {uvw_atol:.6g}"
    )

native_data = native["data"][:, :, native_order]
casa_data = casa["data"][:, :, casa_order]
native_flag = native["flag"][:, :, native_order]
casa_flag = casa["flag"][:, :, casa_order]
native_effective_flag = native_flag | native["flag_row"][native_order].reshape(1, 1, -1)
casa_effective_flag = casa_flag | casa["flag_row"][casa_order].reshape(1, 1, -1)
data_abs_all = np.abs(native_data - casa_data)
casa_amp_all = np.abs(casa_data)
comparison_mask = ~(native_effective_flag | casa_effective_flag)
data_abs = data_abs_all[comparison_mask]
casa_amp = casa_amp_all[comparison_mask]
relative = data_abs / np.maximum(casa_amp, data_atol)
data_violation_mask = (
    comparison_mask
    & (data_abs_all > data_atol)
    & ((data_abs_all / np.maximum(casa_amp_all, data_atol)) > data_rtol)
)
data_violation_abs = data_abs_all[data_violation_mask]
data_violation_relative = (
    data_abs_all[data_violation_mask] / np.maximum(casa_amp_all[data_violation_mask], data_atol)
)
result["data"] = {
    "compared_unflagged_cells": int(np.count_nonzero(comparison_mask)),
    "all_cells_max_abs": float(data_abs_all.max()) if data_abs_all.size else 0.0,
    "max_abs": float(data_abs.max()) if data_abs.size else 0.0,
    "mean_abs": float(data_abs.mean()) if data_abs.size else 0.0,
    "max_relative": float(relative.max()) if relative.size else 0.0,
    "violating_cells": int(np.count_nonzero(data_violation_mask)),
    "violation_max_abs": float(data_violation_abs.max()) if data_violation_abs.size else 0.0,
    "violation_max_relative": (
        float(data_violation_relative.max()) if data_violation_relative.size else 0.0
    ),
    "native_abs_max": float(np.abs(native_data).max()) if native_data.size else 0.0,
    "casa_abs_max": float(casa_amp_all.max()) if casa_amp_all.size else 0.0,
}
phase_mask = comparison_mask & (casa_amp_all > max(data_atol, 1.0))
if np.count_nonzero(phase_mask) >= 3:
    phase_indices = np.argwhere(phase_mask)
    rows = phase_indices[:, 2]
    channels = phase_indices[:, 1]
    wavelengths_m = 299_792_458.0 / casa["chan_freq_hz"][channels]
    u_lambda = native_uvw[0, rows] / wavelengths_m
    v_lambda = native_uvw[1, rows] / wavelengths_m
    phase_delta = np.angle(
        native_data[phase_mask] * np.conj(casa_data[phase_mask])
    )
    design = np.column_stack([
        2.0 * np.pi * u_lambda,
        2.0 * np.pi * v_lambda,
        np.ones_like(u_lambda),
    ])
    fit, *_ = np.linalg.lstsq(design, phase_delta, rcond=None)
    residual = phase_delta - design @ fit
    phase_correction = np.exp(-1j * (design @ fit))
    corrected_native = native_data[phase_mask] * phase_correction
    corrected_abs = np.abs(corrected_native - casa_data[phase_mask])
    native_amp = np.abs(native_data[phase_mask])
    casa_amp_phase = np.abs(casa_data[phase_mask])
    amp_ratio = native_amp / np.maximum(casa_amp_phase, data_atol)
    result["data"]["phase_fit"] = {
        "cells": int(phase_delta.size),
        "min_casa_amp": float(casa_amp_phase.min()),
        "image_offset_l_rad": float(fit[0]),
        "image_offset_m_rad": float(fit[1]),
        "constant_phase_rad": float(fit[2]),
        "phase_delta_abs_max_rad": float(np.max(np.abs(phase_delta))),
        "phase_delta_abs_median_rad": float(np.median(np.abs(phase_delta))),
        "fit_residual_abs_max_rad": float(np.max(np.abs(residual))),
        "fit_residual_abs_median_rad": float(np.median(np.abs(residual))),
        "max_abs_after_phase_fit": float(np.max(corrected_abs)),
        "median_abs_after_phase_fit": float(np.median(corrected_abs)),
        "amplitude_ratio_min": float(np.min(amp_ratio)),
        "amplitude_ratio_median": float(np.median(amp_ratio)),
        "amplitude_ratio_max": float(np.max(amp_ratio)),
    }
if data_abs.size:
    result["data"]["abs_percentiles"] = {
        str(percentile): float(np.percentile(data_abs, percentile))
        for percentile in [50, 90, 95, 99, 99.9, 100]
    }
    result["data"]["relative_percentiles"] = {
        str(percentile): float(np.percentile(relative, percentile))
        for percentile in [50, 90, 95, 99, 99.9, 100]
    }
    comparison_indices = np.argwhere(comparison_mask)
    worst_indices = np.argsort(data_abs)[-10:][::-1]
    worst_cells = []
    for ordinal in worst_indices:
        corr, chan, sorted_row = comparison_indices[ordinal]
        native_row = int(native_order[sorted_row])
        casa_row = int(casa_order[sorted_row])
        worst_cells.append({
            "native_row": native_row,
            "casa_row": casa_row,
            "correlation": int(corr),
            "channel": int(chan),
            "key": {
                column: (
                    native["keys"][index][native_row].item()
                    if hasattr(native["keys"][index][native_row], "item")
                    else native["keys"][index][native_row]
                )
                for index, column in enumerate(KEY_COLUMNS)
            },
            "uvw_m": [float(value) for value in native_uvw[:, sorted_row]],
            "native": {
                "real": float(native_data[corr, chan, sorted_row].real),
                "imag": float(native_data[corr, chan, sorted_row].imag),
            },
            "casa": {
                "real": float(casa_data[corr, chan, sorted_row].real),
                "imag": float(casa_data[corr, chan, sorted_row].imag),
            },
            "abs": float(data_abs_all[corr, chan, sorted_row]),
            "relative": float(
                data_abs_all[corr, chan, sorted_row]
                / max(casa_amp_all[corr, chan, sorted_row], data_atol)
            ),
        })
    result["data"]["worst_cells"] = worst_cells
    worst_relative_indices = np.argsort(relative)[-10:][::-1]
    worst_relative_cells = []
    for ordinal in worst_relative_indices:
        corr, chan, sorted_row = comparison_indices[ordinal]
        native_row = int(native_order[sorted_row])
        casa_row = int(casa_order[sorted_row])
        worst_relative_cells.append({
            "native_row": native_row,
            "casa_row": casa_row,
            "correlation": int(corr),
            "channel": int(chan),
            "key": {
                column: (
                    native["keys"][index][native_row].item()
                    if hasattr(native["keys"][index][native_row], "item")
                    else native["keys"][index][native_row]
                )
                for index, column in enumerate(KEY_COLUMNS)
            },
            "uvw_m": [float(value) for value in native_uvw[:, sorted_row]],
            "native": {
                "real": float(native_data[corr, chan, sorted_row].real),
                "imag": float(native_data[corr, chan, sorted_row].imag),
                "abs": float(np.abs(native_data[corr, chan, sorted_row])),
            },
            "casa": {
                "real": float(casa_data[corr, chan, sorted_row].real),
                "imag": float(casa_data[corr, chan, sorted_row].imag),
                "abs": float(casa_amp_all[corr, chan, sorted_row]),
            },
            "abs": float(data_abs_all[corr, chan, sorted_row]),
            "relative": float(
                data_abs_all[corr, chan, sorted_row]
                / max(casa_amp_all[corr, chan, sorted_row], data_atol)
            ),
        })
    result["data"]["worst_relative_cells"] = worst_relative_cells
if result["data"]["violating_cells"]:
    result["status"] = "failed"
    result["reasons"].append(
        f"strict DATA has {result['data']['violating_cells']} cells exceeding both tolerances "
        f"(max abs {result['data']['violation_max_abs']:.6g}, "
        f"max rel {result['data']['violation_max_relative']:.6g})"
    )

raw_flag_mismatches = int(np.count_nonzero(native_flag != casa_flag))
effective_flag_mismatches = int(np.count_nonzero(native_effective_flag != casa_effective_flag))
result["raw_flag_mismatches"] = raw_flag_mismatches
result["effective_flag_mismatches"] = effective_flag_mismatches
if raw_flag_mismatches:
    raw_mismatch_baselines = Counter()
    for _corr, _chan, sorted_row in np.argwhere(native_flag != casa_flag):
        native_row = int(native_order[sorted_row])
        raw_mismatch_baselines[
            (int(native["keys"][3][native_row]), int(native["keys"][4][native_row]))
        ] += 1
    result["raw_flag_mismatch_baselines"] = [
        {"antenna1": antenna1, "antenna2": antenna2, "cells": cells}
        for (antenna1, antenna2), cells in raw_mismatch_baselines.most_common(20)
    ]
if effective_flag_mismatches:
    mismatch_baselines = Counter()
    for _corr, _chan, sorted_row in np.argwhere(native_effective_flag != casa_effective_flag):
        native_row = int(native_order[sorted_row])
        mismatch_baselines[
            (int(native["keys"][3][native_row]), int(native["keys"][4][native_row]))
        ] += 1
    result["flag_mismatch_baselines"] = [
        {"antenna1": antenna1, "antenna2": antenna2, "cells": cells}
        for (antenna1, antenna2), cells in mismatch_baselines.most_common(20)
    ]
if effective_flag_mismatches:
    result["status"] = "failed"
    result["reasons"].append(
        f"strict effective FLAG differs in {effective_flag_mismatches} cells"
    )

for column in ["weight", "sigma"]:
    native_values = native[column][:, native_order]
    casa_values = casa[column][:, casa_order]
    delta = np.abs(native_values - casa_values)
    max_abs = float(delta.max()) if delta.size else 0.0
    result[column] = {"max_abs": max_abs}
    if max_abs > 1.0e-6:
        result["status"] = "failed"
        result["reasons"].append(f"strict {column.upper()} max abs {max_abs:.6g} exceeds 1e-6")

print(json.dumps(result, sort_keys=True))
'''
    completed = subprocess.run(
        [
            casa_python,
            "-c",
            script,
            json.dumps([native_ms, casa_ms, uvw_atol, data_atol, data_rtol]),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise BenchError(
            "failed to run strict MS value comparison: " + completed.stderr.strip()
        )
    return parse_json_from_stdout(completed.stdout, "strict MS value comparison")


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


def read_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise BenchError(f"read {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise BenchError(f"parse {path}: {error}") from error


def write_json(path: pathlib.Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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
