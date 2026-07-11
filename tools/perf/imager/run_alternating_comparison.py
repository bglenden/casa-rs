#!/usr/bin/env python3
"""Run and aggregate counterbalanced baseline/candidate imaging comparisons."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import datetime as dt
import json
import math
import os
import pathlib
import re
import statistics
import subprocess
import sys
from typing import Any, Callable, Sequence
import uuid

import perf_paths


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
RUN_WORKLOAD = pathlib.Path(__file__).resolve().parent / "run_workload.py"
DEFAULT_OUTPUT_ROOT = perf_paths.artifact_path("imager", "alternating-comparisons")
DEFAULT_ARTIFACT_ROOT = (
    perf_paths.default_artifact_root() / "imager" / "alternating-comparisons"
)
MANAGED_RUN_WORKLOAD_OPTIONS = {
    "--artifact-root",
    "--dry-run",
    "--output-dir",
    "--repeats",
}
BACKEND_IDENTITY_TERMS = (
    "acceleration",
    "backend",
    "fallback",
    "grid_threads",
    "metal_device",
    "tile_anchor",
    "worker",
)


class ComparisonError(Exception):
    """Error that should be shown without a traceback."""


@dataclass(frozen=True)
class ComparisonConfig:
    baseline_workload: str
    candidate_workload: str
    warmup_pair_count: int
    measured_pair_count: int
    output_root: pathlib.Path
    artifact_root: pathlib.Path
    slowdown_tolerance: float = 0.0
    run_workload_options: tuple[str, ...] = ()
    baseline_imaging_overrides: tuple[str, ...] = ()
    candidate_imaging_overrides: tuple[str, ...] = ()


@dataclass(frozen=True)
class ScheduleItem:
    sequence_index: int
    phase: str
    block_index: int
    position_in_block: int
    role: str
    workload: str


CommandRunner = Callable[[list[str], dict[str, str]], subprocess.CompletedProcess[str]]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Arguments after -- are passed to run_workload.py. The comparison "
            "owns --repeats, --output-dir, --artifact-root, and --dry-run."
        ),
    )
    parser.add_argument("baseline", help="baseline workload manifest id or JSON path")
    parser.add_argument("candidate", help="candidate workload manifest id or JSON path")
    parser.add_argument(
        "--warmup-pairs",
        type=int,
        default=2,
        help="number of alternating AB/BA warmup blocks (default: 2)",
    )
    parser.add_argument(
        "--measured-pairs",
        type=int,
        default=3,
        help="number of alternating ABBA/BAAB measured blocks; minimum 3",
    )
    parser.add_argument(
        "--output-root",
        type=pathlib.Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="root for the aggregate report and per-invocation result directories",
    )
    parser.add_argument(
        "--artifact-root",
        type=pathlib.Path,
        default=DEFAULT_ARTIFACT_ROOT,
        help="root for run_workload.py products, comparisons, and scratch data",
    )
    parser.add_argument(
        "--slowdown-tolerance",
        "--tolerance",
        type=float,
        default=0.0,
        help="allowed median paired slowdown as a fraction (default: 0.0)",
    )
    parser.add_argument(
        "--baseline-set-imaging",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="repeatable run_workload.py imaging override applied only to baseline runs",
    )
    parser.add_argument(
        "--candidate-set-imaging",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="repeatable run_workload.py imaging override applied only to candidate runs",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    wrapper_argv, run_workload_options = split_passthrough_args(
        list(sys.argv[1:] if argv is None else argv)
    )
    parser = build_arg_parser()
    args = parser.parse_args(wrapper_argv)
    try:
        config = ComparisonConfig(
            baseline_workload=args.baseline,
            candidate_workload=args.candidate,
            warmup_pair_count=args.warmup_pairs,
            measured_pair_count=args.measured_pairs,
            output_root=args.output_root.expanduser().resolve(),
            artifact_root=args.artifact_root.expanduser().resolve(),
            slowdown_tolerance=args.slowdown_tolerance,
            run_workload_options=tuple(run_workload_options),
            baseline_imaging_overrides=tuple(args.baseline_set_imaging),
            candidate_imaging_overrides=tuple(args.candidate_set_imaging),
        )
        validate_config(config)
        report_path, report = run_comparison(config)
    except ComparisonError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    print(report_path)
    if report["status"] != "completed":
        print(f"error: {report['error']}", file=sys.stderr)
        return 1
    verdict = report.get("verdict")
    verdict_status = verdict.get("status") if isinstance(verdict, dict) else None
    if verdict_status != "pass":
        print(
            f"error: alternating comparison verdict is {verdict_status or 'missing'}",
            file=sys.stderr,
        )
        return 1
    return 0


def split_passthrough_args(argv: list[str]) -> tuple[list[str], list[str]]:
    if "--" not in argv:
        return argv, []
    separator = argv.index("--")
    return argv[:separator], argv[separator + 1 :]


def validate_config(config: ComparisonConfig) -> None:
    if config.warmup_pair_count < 0:
        raise ComparisonError("--warmup-pairs must be non-negative")
    if config.measured_pair_count < 3:
        raise ComparisonError("--measured-pairs must be at least 3")
    if not math.isfinite(config.slowdown_tolerance) or config.slowdown_tolerance < 0.0:
        raise ComparisonError(
            "--slowdown-tolerance must be a finite non-negative fraction"
        )
    for token in config.run_workload_options:
        option = token.split("=", 1)[0]
        if option in MANAGED_RUN_WORKLOAD_OPTIONS:
            raise ComparisonError(
                f"{option} is managed by the alternating comparison and cannot be passed through"
            )
    for role, overrides in (
        ("baseline", config.baseline_imaging_overrides),
        ("candidate", config.candidate_imaging_overrides),
    ):
        for override in overrides:
            key, separator, value = override.partition("=")
            if not separator or not key.strip() or not value.strip():
                raise ComparisonError(
                    f"--{role}-set-imaging requires non-empty KEY=VALUE, got {override!r}"
                )


def build_schedule(
    baseline_workload: str,
    candidate_workload: str,
    *,
    warmup_pair_count: int,
    measured_pair_count: int,
) -> list[ScheduleItem]:
    if warmup_pair_count < 0:
        raise ComparisonError("warmup_pair_count must be non-negative")
    if measured_pair_count < 3:
        raise ComparisonError("measured_pair_count must be at least 3")

    role_workloads = {
        "baseline": baseline_workload,
        "candidate": candidate_workload,
    }
    schedule: list[ScheduleItem] = []

    def append_block(phase: str, block_index: int, roles: Sequence[str]) -> None:
        for position, role in enumerate(roles, start=1):
            schedule.append(
                ScheduleItem(
                    sequence_index=len(schedule) + 1,
                    phase=phase,
                    block_index=block_index,
                    position_in_block=position,
                    role=role,
                    workload=role_workloads[role],
                )
            )

    for block_index in range(1, warmup_pair_count + 1):
        roles = (
            ("baseline", "candidate")
            if block_index % 2 == 1
            else ("candidate", "baseline")
        )
        append_block("warmup", block_index, roles)

    for block_index in range(1, measured_pair_count + 1):
        roles = (
            ("baseline", "candidate", "candidate", "baseline")
            if block_index % 2 == 1
            else ("candidate", "baseline", "baseline", "candidate")
        )
        append_block("measured", block_index, roles)
    return schedule


def run_comparison(
    config: ComparisonConfig,
    *,
    command_runner: CommandRunner | None = None,
    comparison_id: str | None = None,
) -> tuple[pathlib.Path, dict[str, Any]]:
    validate_config(config)
    runner = command_runner or run_command
    comparison_id = comparison_id or new_comparison_id(config)
    output_root = config.output_root.expanduser().resolve()
    artifact_root = config.artifact_root.expanduser().resolve() / comparison_id
    report_path = output_root / f"{comparison_id}.json"
    run_output_root = output_root / comparison_id / "runs"
    output_root.mkdir(parents=True, exist_ok=True)
    run_output_root.mkdir(parents=True, exist_ok=True)

    schedule = build_schedule(
        config.baseline_workload,
        config.candidate_workload,
        warmup_pair_count=config.warmup_pair_count,
        measured_pair_count=config.measured_pair_count,
    )
    runs: list[dict[str, Any]] = []
    execution_error: str | None = None
    for item in schedule:
        output_dir = run_output_root / run_directory_name(item)
        command = build_run_command(
            item,
            output_dir=output_dir,
            artifact_root=artifact_root,
            run_workload_options=config.run_workload_options,
            imaging_overrides=(
                config.baseline_imaging_overrides
                if item.role == "baseline"
                else config.candidate_imaging_overrides
            ),
        )
        try:
            run = execute_run(item, command, runner)
            runs.append(run)
            if run["result_status"] != "completed":
                execution_error = (
                    f"sequence item {item.sequence_index} produced "
                    f"run_workload status {run['result_status']!r}"
                )
                break
        except ComparisonError as error:
            execution_error = str(error)
            runs.append(
                {
                    **asdict(item),
                    "command": command,
                    "result_status": "execution_failed",
                    "error": execution_error,
                    "result_path": None,
                    "recorded_paths": [],
                    "total_wall_seconds": None,
                    "stage_timings_ms": {},
                    "backend_identity": None,
                }
            )
            break

    report = build_report(
        config,
        comparison_id=comparison_id,
        report_path=report_path,
        comparison_artifact_root=artifact_root,
        schedule=schedule,
        runs=runs,
        execution_error=execution_error,
    )
    write_json(report_path, report)
    return report_path, report


def build_run_command(
    item: ScheduleItem,
    *,
    output_dir: pathlib.Path,
    artifact_root: pathlib.Path,
    run_workload_options: Sequence[str],
    imaging_overrides: Sequence[str] = (),
) -> list[str]:
    return [
        sys.executable,
        str(RUN_WORKLOAD),
        item.workload,
        "--repeats",
        "1",
        "--output-dir",
        str(output_dir),
        "--artifact-root",
        str(artifact_root),
        *run_workload_options,
        *(
            token
            for override in imaging_overrides
            for token in ("--set-imaging", override)
        ),
    ]


def run_command(
    argv: list[str], environment: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=REPO_ROOT,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )


def execute_run(
    item: ScheduleItem,
    command: list[str],
    command_runner: CommandRunner,
) -> dict[str, Any]:
    environment = os.environ.copy()
    environment["CASA_RS_BENCH_PROFILE_REPEATS"] = "1"
    completed = command_runner(command, environment)
    stdout = completed.stdout or ""
    if completed.returncode != 0:
        detail = last_nonempty_line(stdout) or "no command output"
        raise ComparisonError(
            f"sequence item {item.sequence_index} exited {completed.returncode}: {detail}"
        )

    result_path = result_path_from_stdout(stdout)
    result = load_result(result_path)
    return {
        **asdict(item),
        "command": command,
        "result_status": result.get("status", "missing"),
        "result_path": str(result_path),
        "recorded_paths": collect_recorded_paths(result, result_path),
        "total_wall_seconds": extract_total_wall_seconds(result),
        "stage_timings_ms": extract_stage_timings(result),
        "backend_identity": extract_backend_identity(result),
    }


def result_path_from_stdout(stdout: str) -> pathlib.Path:
    value = last_nonempty_line(stdout)
    if value is None:
        raise ComparisonError("run_workload.py did not print a result path")
    path = pathlib.Path(value).expanduser()
    if not path.is_absolute():
        path = REPO_ROOT / path
    path = path.resolve()
    if not path.is_file():
        raise ComparisonError(f"run_workload.py result path does not exist: {path}")
    return path


def last_nonempty_line(text: str) -> str | None:
    return next(
        (line.strip() for line in reversed(text.splitlines()) if line.strip()), None
    )


def load_result(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ComparisonError(f"read result {path}: {error}") from error
    if not isinstance(value, dict):
        raise ComparisonError(f"result {path} must contain a JSON object")
    return value


def extract_total_wall_seconds(result: dict[str, Any]) -> float | None:
    timings = nested_dict(result, "results", "rust", "timings_seconds")
    runs = timings.get("runs")
    if isinstance(runs, list) and len(runs) == 1:
        return finite_number(runs[0])
    return finite_number(timings.get("median"))


def extract_stage_timings(result: dict[str, Any]) -> dict[str, dict[str, float]]:
    stage_medians = nested_dict(result, "results", "stage_medians_ms")
    extracted: dict[str, dict[str, float]] = {}
    for implementation, values in stage_medians.items():
        if not isinstance(implementation, str) or not isinstance(values, dict):
            continue
        numeric = {
            str(name): number
            for name, value in values.items()
            if (number := finite_number(value)) is not None
        }
        if numeric:
            extracted[implementation] = numeric
    return extracted


def extract_backend_identity(result: dict[str, Any]) -> dict[str, Any] | None:
    identity: dict[str, Any] = {}
    feature_backend = nested_value(result, "benchmark_features", "backend")
    if feature_backend is None:
        feature_backend = nested_value(
            result, "results", "benchmark_features", "backend"
        )
    if feature_backend is not None:
        identity["benchmark_features"] = feature_backend

    summary = nested_dict(result, "results", "backend_plan_logs", "summary")
    plan_identity = {
        key: value
        for key, value in summary.items()
        if value is not None
        and any(term in key.lower() for term in BACKEND_IDENTITY_TERMS)
    }
    if plan_identity:
        identity["plan_summary"] = plan_identity
    return identity or None


def collect_recorded_paths(
    result: dict[str, Any], result_path: pathlib.Path
) -> list[dict[str, str]]:
    recorded = [{"field": "result_json", "path": str(result_path)}]

    def visit(value: Any, pointer: str, key: str | None = None) -> None:
        if isinstance(value, dict):
            for child_key, child in value.items():
                escaped = str(child_key).replace("~", "~0").replace("/", "~1")
                visit(child, f"{pointer}/{escaped}", str(child_key))
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, f"{pointer}/{index}", key)
        elif isinstance(value, str) and is_recorded_path(key, value):
            recorded.append({"field": pointer or "/", "path": value})

    visit(result, "")
    return recorded


def is_recorded_path(key: str | None, value: str) -> bool:
    key_is_path = key is not None and bool(
        re.search(r"(?:^|_)(?:dir|input|log|path|prefix|root)$", key.lower())
    )
    value_is_path = value.startswith(("/", "./", "../", "~/"))
    return key_is_path or value_is_path


def build_report(
    config: ComparisonConfig,
    *,
    comparison_id: str,
    report_path: pathlib.Path,
    comparison_artifact_root: pathlib.Path,
    schedule: Sequence[ScheduleItem],
    runs: list[dict[str, Any]],
    execution_error: str | None = None,
) -> dict[str, Any]:
    measured_runs = [run for run in runs if run["phase"] == "measured"]
    summaries = {
        role: summarize_role(measured_runs, role) for role in ("baseline", "candidate")
    }
    paired_deltas = build_block_paired_deltas(measured_runs)
    adjacent_pair_deltas = build_adjacent_pair_deltas(measured_runs)
    expected_measured_runs = config.measured_pair_count * 4
    evidence_complete = (
        execution_error is None
        and len(measured_runs) == expected_measured_runs
        and all(run.get("total_wall_seconds") is not None for run in measured_runs)
        and len(paired_deltas) == config.measured_pair_count
    )
    verdict = build_verdict(
        paired_deltas,
        tolerance=config.slowdown_tolerance,
        evidence_complete=evidence_complete,
    )
    return {
        "schema_version": 1,
        "status": "completed" if execution_error is None else "failed",
        "error": execution_error,
        "comparison_id": comparison_id,
        "created_at": utc_now(),
        "report_path": str(report_path),
        "configuration": {
            "baseline_workload": config.baseline_workload,
            "candidate_workload": config.candidate_workload,
            "warmup_pair_count": config.warmup_pair_count,
            "measured_pair_count": config.measured_pair_count,
            "slowdown_tolerance_fraction": config.slowdown_tolerance,
            "output_root": str(config.output_root),
            "artifact_root": str(config.artifact_root),
            "comparison_artifact_root": str(comparison_artifact_root),
            "run_workload_options": list(config.run_workload_options),
            "baseline_imaging_overrides": list(config.baseline_imaging_overrides),
            "candidate_imaging_overrides": list(config.candidate_imaging_overrides),
            "run_workload_repeats": 1,
        },
        "order": {
            "warmup": [item.role for item in schedule if item.phase == "warmup"],
            "measured": [item.role for item in schedule if item.phase == "measured"],
        },
        "schedule": [asdict(item) for item in schedule],
        "runs": runs,
        "measured_summaries": summaries,
        "paired_deltas": paired_deltas,
        "paired_delta_summary": summarize_delta_records(paired_deltas),
        "adjacent_pair_deltas": adjacent_pair_deltas,
        "adjacent_pair_delta_summary": summarize_delta_records(adjacent_pair_deltas),
        "verdict": verdict,
    }


def summarize_role(runs: list[dict[str, Any]], role: str) -> dict[str, Any]:
    role_runs = [run for run in runs if run["role"] == role]
    walls = [
        value
        for run in role_runs
        if (value := finite_number(run.get("total_wall_seconds"))) is not None
    ]
    identities: list[dict[str, Any]] = []
    for run in role_runs:
        identity = run.get("backend_identity")
        if isinstance(identity, dict) and identity not in identities:
            identities.append(identity)
    return {
        "run_count": len(role_runs),
        "total_wall_seconds": robust_summary(walls),
        "stage_timings_ms": summarize_stage_timings(role_runs),
        "backend_identities": identities,
    }


def summarize_stage_timings(runs: list[dict[str, Any]]) -> dict[str, Any]:
    values: dict[str, dict[str, list[float]]] = {}
    for run in runs:
        for implementation, stages in run.get("stage_timings_ms", {}).items():
            for stage, value in stages.items():
                values.setdefault(implementation, {}).setdefault(stage, []).append(
                    value
                )
    return {
        implementation: {
            stage: robust_summary(stage_values)
            for stage, stage_values in sorted(stages.items())
        }
        for implementation, stages in sorted(values.items())
    }


def build_block_paired_deltas(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records = []
    block_indexes = sorted({run["block_index"] for run in runs})
    for block_index in block_indexes:
        block = sorted(
            (run for run in runs if run["block_index"] == block_index),
            key=lambda run: run["position_in_block"],
        )
        baseline = wall_values(block, "baseline")
        candidate = wall_values(block, "candidate")
        if len(baseline) != 2 or len(candidate) != 2:
            continue
        baseline_center = statistics.median(baseline)
        candidate_center = statistics.median(candidate)
        records.append(
            delta_record(
                block_index=block_index,
                pair_index=None,
                order="".join(short_role(run["role"]) for run in block),
                baseline_seconds=baseline_center,
                candidate_seconds=candidate_center,
            )
        )
    return records


def build_adjacent_pair_deltas(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records = []
    block_indexes = sorted({run["block_index"] for run in runs})
    for block_index in block_indexes:
        block = sorted(
            (run for run in runs if run["block_index"] == block_index),
            key=lambda run: run["position_in_block"],
        )
        for offset in range(0, len(block), 2):
            pair = block[offset : offset + 2]
            baseline = wall_values(pair, "baseline")
            candidate = wall_values(pair, "candidate")
            if len(pair) != 2 or len(baseline) != 1 or len(candidate) != 1:
                continue
            records.append(
                delta_record(
                    block_index=block_index,
                    pair_index=(offset // 2) + 1,
                    order="".join(short_role(run["role"]) for run in pair),
                    baseline_seconds=baseline[0],
                    candidate_seconds=candidate[0],
                )
            )
    return records


def delta_record(
    *,
    block_index: int,
    pair_index: int | None,
    order: str,
    baseline_seconds: float,
    candidate_seconds: float,
) -> dict[str, Any]:
    delta = candidate_seconds - baseline_seconds
    relative_delta = delta / baseline_seconds if baseline_seconds != 0.0 else None
    return {
        "block_index": block_index,
        "pair_index": pair_index,
        "order": order,
        "baseline_seconds": baseline_seconds,
        "candidate_seconds": candidate_seconds,
        "delta_seconds": delta,
        "relative_delta": relative_delta,
    }


def summarize_delta_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "delta_seconds": robust_summary(
            [record["delta_seconds"] for record in records]
        ),
        "relative_delta": robust_summary(
            [
                record["relative_delta"]
                for record in records
                if record["relative_delta"] is not None
            ]
        ),
    }


def build_verdict(
    paired_deltas: list[dict[str, Any]],
    *,
    tolerance: float,
    evidence_complete: bool,
) -> dict[str, Any]:
    relative_summary = robust_summary(
        [
            record["relative_delta"]
            for record in paired_deltas
            if record["relative_delta"] is not None
        ]
    )
    observed = relative_summary["median"]
    if not evidence_complete or observed is None:
        return {
            "status": "inconclusive",
            "no_slowdown": None,
            "tolerance_fraction": tolerance,
            "observed_median_relative_delta": observed,
            "basis": "median of per-block candidate-versus-baseline relative deltas",
            "reason": "measured counterbalanced evidence is incomplete",
        }
    passed = observed <= tolerance
    return {
        "status": "pass" if passed else "fail",
        "no_slowdown": passed,
        "tolerance_fraction": tolerance,
        "observed_median_relative_delta": observed,
        "basis": "median of per-block candidate-versus-baseline relative deltas",
        "reason": (
            "candidate median paired slowdown is within tolerance"
            if passed
            else "candidate median paired slowdown exceeds tolerance"
        ),
    }


def robust_summary(values: Sequence[float]) -> dict[str, Any]:
    finite = sorted(
        number for value in values if (number := finite_number(value)) is not None
    )
    if not finite:
        return {
            "count": 0,
            "median": None,
            "mad": None,
            "q1": None,
            "q3": None,
            "iqr": None,
            "minimum": None,
            "maximum": None,
        }
    median = statistics.median(finite)
    q1 = percentile(finite, 0.25)
    q3 = percentile(finite, 0.75)
    return {
        "count": len(finite),
        "median": median,
        "mad": statistics.median(abs(value - median) for value in finite),
        "q1": q1,
        "q3": q3,
        "iqr": q3 - q1,
        "minimum": finite[0],
        "maximum": finite[-1],
    }


def percentile(sorted_values: Sequence[float], fraction: float) -> float:
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    weight = position - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def wall_values(runs: list[dict[str, Any]], role: str) -> list[float]:
    return [
        value
        for run in runs
        if run["role"] == role
        and (value := finite_number(run.get("total_wall_seconds"))) is not None
    ]


def finite_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def nested_dict(value: dict[str, Any], *keys: str) -> dict[str, Any]:
    nested = nested_value(value, *keys)
    return nested if isinstance(nested, dict) else {}


def nested_value(value: dict[str, Any], *keys: str) -> Any:
    current: Any = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def short_role(role: str) -> str:
    return "A" if role == "baseline" else "B"


def run_directory_name(item: ScheduleItem) -> str:
    return (
        f"{item.sequence_index:03d}-{item.phase}-"
        f"{item.block_index:02d}-{item.position_in_block:02d}-{item.role}"
    )


def new_comparison_id(config: ComparisonConfig) -> str:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    baseline = slug(pathlib.Path(config.baseline_workload).stem)
    candidate = slug(pathlib.Path(config.candidate_workload).stem)
    return f"{stamp}-{baseline}-vs-{candidate}-{uuid.uuid4().hex[:8]}"


def slug(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()
    return cleaned[:40] or "workload"


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def write_json(path: pathlib.Path, value: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    raise SystemExit(main())
