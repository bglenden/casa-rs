#!/usr/bin/env python3
"""Measure casars-imager progress telemetry overhead for one JSON request."""

from __future__ import annotations

import argparse
import copy
import json
import statistics
import sys
import tempfile
import time
from pathlib import Path

from perf_harness import atomic_write_json, load_json_object
from perf_harness.subprocesses import run_command


PROGRESS_PREFIX = "CASARS_IMAGER_PROGRESS "


def load_request(path: Path) -> dict:
    payload = load_json_object(path, description="imager request")
    if payload.get("kind") != "run" or not isinstance(payload.get("request"), dict):
        raise SystemExit(f"{path} is not a run ImagerTaskRequest envelope")
    return payload


def request_variant(base: dict, enabled: bool, max_uv_points: int, min_interval_ms: int) -> dict:
    payload = copy.deepcopy(base)
    request = payload["request"]
    if enabled:
        request["progress"] = {
            "enabled": True,
            "max_uv_points": max_uv_points,
            "min_interval_ms": min_interval_ms,
        }
    else:
        request.pop("progress", None)
    return payload


def run_once(binary: Path, request_payload: dict, temp_dir: Path, index: int) -> dict:
    request_payload = copy.deepcopy(request_payload)
    product_dir = temp_dir / f"product-{index}"
    product_dir.mkdir(parents=True, exist_ok=True)
    request_payload["request"]["image_name"] = str(product_dir / "image")
    request_path = temp_dir / f"request-{index}.json"
    atomic_write_json(request_path, request_payload)
    started = time.perf_counter()
    completed = run_command(
        [str(binary), "--json-run", str(request_path)],
        merge_stderr=False,
    )
    elapsed_s = time.perf_counter() - started
    progress_lines = [
        line
        for line in completed.stderr.splitlines()
        if line.startswith(PROGRESS_PREFIX)
    ]
    progress_bytes = sum(len(line.encode("utf-8")) + 1 for line in progress_lines)
    result = {
        "exit_code": completed.returncode,
        "elapsed_s": elapsed_s,
        "progress_event_count": len(progress_lines),
        "progress_payload_bytes": progress_bytes,
        "stderr_bytes": len(completed.stderr.encode("utf-8")),
        "stdout_bytes": len(completed.stdout.encode("utf-8")),
    }
    try:
        output = json.loads(completed.stdout)
        run = output["result"]["run"]
        result["stage_timings"] = run.get("stage_timings", {})
        result["frontend_timings"] = run.get("frontend_timings", {})
    except (json.JSONDecodeError, KeyError, TypeError):
        result["stderr_tail"] = completed.stderr.strip().splitlines()[-5:]
    return result


def median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/casars-imager")
    parser.add_argument("--request", required=True, type=Path)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--max-uv-points", type=int, default=64)
    parser.add_argument("--min-interval-ms", type=int, default=250)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)

    if args.repeats <= 0:
        raise SystemExit("--repeats must be positive")
    binary = Path(args.binary)
    if not binary.exists():
        raise SystemExit(f"{binary} does not exist; build casars-imager first")

    base = load_request(args.request)
    disabled = request_variant(base, False, args.max_uv_points, args.min_interval_ms)
    enabled = request_variant(base, True, args.max_uv_points, args.min_interval_ms)

    with tempfile.TemporaryDirectory(prefix="casars-progress-overhead-") as temp:
        temp_dir = Path(temp)
        disabled_runs = []
        enabled_runs = []
        for index in range(args.repeats):
            disabled_index = index * 2
            enabled_index = disabled_index + 1
            if index % 2 == 0:
                disabled_runs.append(run_once(binary, disabled, temp_dir, disabled_index))
                enabled_runs.append(run_once(binary, enabled, temp_dir, enabled_index))
            else:
                enabled_runs.append(run_once(binary, enabled, temp_dir, enabled_index))
                disabled_runs.append(run_once(binary, disabled, temp_dir, disabled_index))

    disabled_median = median([run["elapsed_s"] for run in disabled_runs])
    enabled_median = median([run["elapsed_s"] for run in enabled_runs])
    overhead_fraction = (
        (enabled_median - disabled_median) / disabled_median
        if disabled_median > 0
        else 0.0
    )
    report = {
        "binary": str(binary),
        "request": str(args.request),
        "repeats": args.repeats,
        "disabled_median_s": disabled_median,
        "enabled_median_s": enabled_median,
        "overhead_fraction": overhead_fraction,
        "overhead_percent": overhead_fraction * 100.0,
        "disabled_runs": disabled_runs,
        "enabled_runs": enabled_runs,
    }
    failed = [
        run
        for run in disabled_runs + enabled_runs
        if run["exit_code"] != 0
    ]
    if failed:
        report["warning"] = "one or more runs exited non-zero"

    text = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        atomic_write_json(args.output, report)
    print(text)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
