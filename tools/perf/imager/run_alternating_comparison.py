#!/usr/bin/env python3
"""Run and aggregate counterbalanced baseline/candidate imaging comparisons.

Each role runs its own casa-rs checkout (``--baseline-repo``,
``--candidate-repo``; both default to this one). The slowdown gate judges wall
time alone; the blocking gate (``docs/measurement-set/imaging-port.md``
decision 4) also judges peak RSS and the image products.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import datetime as dt
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
from perf_harness import (
    ContractError,
    RUN_RESULT_SCHEMA_VERSION,
    atomic_write_json,
    finite_number,
    load_run_result,
    nested_object as nested_dict,
    nested_value,
    validate_run_result,
)
from perf_harness.image_compare import compare_products as compare_image_products
from perf_harness.provenance import capture_provenance, executable_path
from perf_harness.subprocesses import run_command as run_shared_command
import two_build_comparison as two_build
from two_build_comparison import ComparisonError


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
REUSED_PREFIX_ENVIRONMENT = (
    "CASA_RS_BENCH_REUSE_CASA_PREFIX",
    "CASA_RS_BENCH_REUSE_RUST_PREFIX",
    "CASA_RS_BENCH_PREPARED_AW_OUTPUT_PREFIX",
)


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
    gate: str = "slowdown"
    wall_tolerance: float = two_build.DEFAULT_WALL_TOLERANCE
    noise_sigmas: float = two_build.DEFAULT_NOISE_SIGMAS
    rss_tolerance: float = two_build.DEFAULT_RSS_TOLERANCE
    product_nrms: float = two_build.DEFAULT_PRODUCT_NRMS
    baseline_repo: pathlib.Path = REPO_ROOT
    candidate_repo: pathlib.Path = REPO_ROOT

    def role_repo(self, role: str) -> pathlib.Path:
        return self.baseline_repo if role == "baseline" else self.candidate_repo

    def wall_thresholds(self) -> tuple[float, float]:
        """(wall tolerance, noise sigmas). The slowdown gate is the noise-band
        check with ``--slowdown-tolerance`` as its tolerance and no band."""

        if self.gate == "slowdown":
            return self.slowdown_tolerance, 0.0
        return self.wall_tolerance, self.noise_sigmas

    def thresholds(self) -> dict[str, float]:
        wall_tolerance, noise_sigmas = self.wall_thresholds()
        thresholds = {
            "wall_tolerance_fraction": wall_tolerance,
            "noise_sigmas": noise_sigmas,
        }
        if self.gate == "blocking":
            thresholds["rss_tolerance_fraction"] = self.rss_tolerance
            thresholds["product_nrms"] = self.product_nrms
        return thresholds


@dataclass(frozen=True)
class ScheduleItem:
    sequence_index: int
    phase: str
    block_index: int
    position_in_block: int
    role: str
    workload: str


CommandRunner = Callable[
    [list[str], dict[str, str], pathlib.Path], subprocess.CompletedProcess[str]
]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
        "--baseline-repo",
        type=pathlib.Path,
        default=REPO_ROOT,
        help="casa-rs checkout that runs the baseline (default: this one)",
    )
    parser.add_argument(
        "--candidate-repo",
        type=pathlib.Path,
        default=REPO_ROOT,
        help="casa-rs checkout that runs the candidate (default: this one)",
    )
    parser.add_argument(
        "--gate",
        choices=two_build.GATES,
        default="slowdown",
        help=(
            "slowdown (default): wall time within --slowdown-tolerance; "
            "blocking: wall time within the baseline's noise band, peak RSS, "
            "and product NRMS"
        ),
    )
    parser.add_argument(
        "--slowdown-tolerance",
        "--tolerance",
        type=float,
        default=None,
        help="slowdown gate: allowed median paired slowdown as a fraction (default: 0.0)",
    )
    parser.add_argument(
        "--wall-tolerance",
        type=float,
        default=None,
        help=(
            "blocking gate: smallest allowed median paired slowdown "
            f"(default: {two_build.DEFAULT_WALL_TOLERANCE})"
        ),
    )
    parser.add_argument(
        "--noise-sigmas",
        type=float,
        default=None,
        help=(
            "blocking gate: allowed slowdown in baseline relative standard "
            f"deviations (default: {two_build.DEFAULT_NOISE_SIGMAS:g})"
        ),
    )
    parser.add_argument(
        "--rss-tolerance",
        type=float,
        default=None,
        help=(
            "blocking gate: allowed growth of the median peak RSS as a fraction "
            f"(default: {two_build.DEFAULT_RSS_TOLERANCE})"
        ),
    )
    parser.add_argument(
        "--product-nrms",
        type=float,
        default=None,
        help=(
            "blocking gate: largest RMS(candidate - baseline) / RMS(baseline) "
            f"per product (default: {two_build.DEFAULT_PRODUCT_NRMS:g})"
        ),
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
            run_workload_options=tuple(run_workload_options),
            baseline_imaging_overrides=tuple(args.baseline_set_imaging),
            candidate_imaging_overrides=tuple(args.candidate_set_imaging),
            baseline_repo=args.baseline_repo.expanduser().resolve(),
            candidate_repo=args.candidate_repo.expanduser().resolve(),
            **gate_settings(args),
        )
        validate_config(config)
        report_path, report = run_comparison(config)
    except (ComparisonError, ContractError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    print(report_path)
    if report["status"] != "completed":
        failure = report["results"].get("failure", {})
        print(f"error: {failure.get('reason', report['status'])}", file=sys.stderr)
        return 1
    details = report["results"]["alternating_comparison"]
    verdict = details.get("verdict")
    verdict_status = verdict.get("status") if isinstance(verdict, dict) else None
    if verdict_status != "pass":
        print(
            f"error: alternating comparison verdict is {verdict_status or 'missing'}",
            file=sys.stderr,
        )
        return 1
    return 0


def gate_settings(args: argparse.Namespace) -> dict[str, Any]:
    """The gate and its thresholds. A threshold flag of the other gate is an
    error rather than silently ignored."""

    blocking_flags = {
        "--wall-tolerance": ("wall_tolerance", args.wall_tolerance),
        "--noise-sigmas": ("noise_sigmas", args.noise_sigmas),
        "--rss-tolerance": ("rss_tolerance", args.rss_tolerance),
        "--product-nrms": ("product_nrms", args.product_nrms),
    }
    if args.gate == "slowdown":
        given = [
            flag for flag, (_, value) in blocking_flags.items() if value is not None
        ]
        if given:
            raise ComparisonError(f"{', '.join(given)} applies only to --gate blocking")
        tolerance = args.slowdown_tolerance
        return {
            "gate": "slowdown",
            "slowdown_tolerance": 0.0 if tolerance is None else tolerance,
        }
    if args.slowdown_tolerance is not None:
        raise ComparisonError(
            "--slowdown-tolerance applies only to --gate slowdown; "
            "use --wall-tolerance and --noise-sigmas"
        )
    return {
        "gate": "blocking",
        **{
            field: value
            for field, value in blocking_flags.values()
            if value is not None
        },
    }


def split_passthrough_args(argv: list[str]) -> tuple[list[str], list[str]]:
    if "--" not in argv:
        return argv, []
    separator = argv.index("--")
    return argv[:separator], argv[separator + 1 :]


def validate_config(
    config: ComparisonConfig, environment: dict[str, str] | None = None
) -> None:
    if config.gate not in two_build.GATES:
        raise ComparisonError(f"gate must be one of {', '.join(two_build.GATES)}")
    if config.warmup_pair_count < 0:
        raise ComparisonError("--warmup-pairs must be non-negative")
    if config.measured_pair_count < 3:
        raise ComparisonError("--measured-pairs must be at least 3")
    if not math.isfinite(config.slowdown_tolerance) or config.slowdown_tolerance < 0.0:
        raise ComparisonError(
            "--slowdown-tolerance must be a finite non-negative fraction"
        )
    for flag, value in (
        ("--wall-tolerance", config.wall_tolerance),
        ("--noise-sigmas", config.noise_sigmas),
        ("--rss-tolerance", config.rss_tolerance),
        ("--product-nrms", config.product_nrms),
    ):
        if not math.isfinite(value) or value < 0.0:
            raise ComparisonError(f"{flag} must be a finite non-negative number")
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
    two_build.validate_role_repos(
        {role: config.role_repo(role) for role in two_build.ROLES},
        dict(os.environ) if environment is None else environment,
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


def product_run_indexes(schedule: Sequence[ScheduleItem]) -> dict[str, int]:
    """The sequence index of each role's first measured run: the run whose
    products the blocking gate keeps and compares."""

    return {
        role: min(
            item.sequence_index
            for item in schedule
            if item.phase == "measured" and item.role == role
        )
        for role in two_build.ROLES
    }


def run_comparison(
    config: ComparisonConfig,
    *,
    command_runner: CommandRunner | None = None,
    product_comparer: two_build.ProductComparer | None = None,
    comparison_id: str | None = None,
) -> tuple[pathlib.Path, dict[str, Any]]:
    validate_config(config)
    runner = command_runner or (
        lambda argv, environment, cwd: run_shared_command(
            argv, cwd=cwd, environment=environment
        )
    )
    comparison_id = comparison_id or new_comparison_id(config)
    output_root = config.output_root.expanduser().resolve()
    artifact_root = config.artifact_root.expanduser().resolve() / comparison_id
    report_path = output_root / f"{comparison_id}.json"
    run_output_root = output_root / comparison_id / "runs"
    output_root.mkdir(parents=True, exist_ok=True)
    run_output_root.mkdir(parents=True, exist_ok=True)
    environment = dict(os.environ)
    roles = {
        role: two_build.describe_role(config.role_repo(role))
        for role in two_build.ROLES
    }

    schedule = build_schedule(
        config.baseline_workload,
        config.candidate_workload,
        warmup_pair_count=config.warmup_pair_count,
        measured_pair_count=config.measured_pair_count,
    )
    product_indexes = product_run_indexes(schedule)
    product_runs: dict[str, tuple[dict[str, Any], dict[str, Any] | None]] = {}
    binaries: dict[tuple[str, int, int], dict[str, Any]] = {}
    runs: list[dict[str, Any]] = []
    execution_error: str | None = None
    for item in schedule:
        output_dir = run_output_root / run_directory_name(item)
        repo = config.role_repo(item.role)
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
            run_workload=repo / two_build.RUN_WORKLOAD_RELATIVE,
        )
        try:
            run, result = execute_run(
                item, command, runner, repo=repo, binaries=binaries
            )
        except ComparisonError as error:
            execution_error = str(error)
            runs.append(failed_run_record(item, command, repo, execution_error))
            break
        # The blocking gate keeps one measured run's products per role, and a
        # failed run's for diagnosis; the slowdown gate keeps every run's.
        keep = (
            config.gate == "slowdown"
            or item.sequence_index in product_indexes.values()
            or run["result_status"] != "completed"
        )
        run["products"] = {
            "rust_prefix": two_build.rust_prefix(result),
            "retained": keep,
            "pruned": (
                []
                if keep
                else two_build.prune_run_products(
                    result,
                    owned_root=artifact_root,
                    protected=protected_prefixes(result, environment),
                )
            ),
        }
        runs.append(run)
        if item.sequence_index == product_indexes[item.role]:
            product_runs[item.role] = (run, result)
        if run["result_status"] != "completed":
            execution_error = (
                f"sequence item {item.sequence_index} produced "
                f"run_workload status {run['result_status']!r}"
            )
            break

    product_comparison = None
    if config.gate == "blocking":
        product_comparison = (
            two_build.not_compared(f"products were not compared: {execution_error}")
            if execution_error is not None
            else two_build.compare_role_products(
                product_runs,
                product_nrms=config.product_nrms,
                artifact_root=artifact_root,
                report_root=output_root / comparison_id,
                comparer=product_comparer or compare_products_with_comparator,
                casa_python=comparison_casa_python(product_runs, environment),
            )
        )
    report = build_report(
        config,
        comparison_id=comparison_id,
        report_path=report_path,
        comparison_artifact_root=artifact_root,
        schedule=schedule,
        runs=runs,
        execution_error=execution_error,
        roles=roles,
        product_comparison=product_comparison,
        run_environment=two_build.recorded_environment(environment),
    )
    validate_run_result(report, source=str(report_path))
    atomic_write_json(report_path, report)
    return report_path, report


def compare_products_with_comparator(
    *, request: dict[str, Any], artifact_prefix: pathlib.Path, casa_python: str | None
) -> dict[str, Any]:
    """The one imaging-product comparator, run under CASA's Python."""

    return compare_image_products(
        casa_python=casa_python,
        request=request,
        artifact_prefix=artifact_prefix,
        cwd=REPO_ROOT,
    )


def comparison_casa_python(
    product_runs: dict[str, tuple[dict[str, Any], dict[str, Any] | None]],
    environment: dict[str, str],
) -> str | None:
    _, result = product_runs.get("baseline", ({}, None))
    recorded = executable_path((result or {}).get("environment") or {}, "casa_python")
    return recorded or environment.get("CASA_RS_CASA_PYTHON") or None


def protected_prefixes(
    result: dict[str, Any], environment: dict[str, str]
) -> list[str]:
    """Product prefixes a run reused from elsewhere; pruning never removes them."""

    reused = [environment.get(name, "") for name in REUSED_PREFIX_ENVIRONMENT]
    for key in ("reuse_casa_prefix", "reuse_rust_prefix"):
        value = two_build.lookup(result, "run", key)
        if isinstance(value, str):
            reused.append(value)
    return [value for value in reused if value]


def build_run_command(
    item: ScheduleItem,
    *,
    output_dir: pathlib.Path,
    artifact_root: pathlib.Path,
    run_workload_options: Sequence[str],
    imaging_overrides: Sequence[str] = (),
    run_workload: pathlib.Path = RUN_WORKLOAD,
) -> list[str]:
    return [
        sys.executable,
        str(run_workload),
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


def execute_run(
    item: ScheduleItem,
    command: list[str],
    command_runner: CommandRunner,
    *,
    repo: pathlib.Path = REPO_ROOT,
    binaries: dict[tuple[str, int, int], dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run one scheduled item in ``repo`` and read its evidence back."""

    environment = os.environ.copy()
    environment["CASA_RS_BENCH_PROFILE_REPEATS"] = "1"
    completed = command_runner(command, environment, repo)
    stdout = completed.stdout or ""
    if completed.returncode != 0:
        detail = last_nonempty_line(stdout) or "no command output"
        raise ComparisonError(
            f"sequence item {item.sequence_index} exited {completed.returncode}: {detail}"
        )

    result_path = result_path_from_stdout(stdout, repo=repo)
    result = load_run_result(result_path)
    prefix = two_build.rust_prefix(result)
    run = {
        **asdict(item),
        "command": command,
        "repo": str(repo),
        "result_status": result.get("status", "missing"),
        "result_path": str(result_path),
        "recorded_paths": collect_recorded_paths(result, result_path),
        "total_wall_seconds": extract_total_wall_seconds(result),
        "stage_timings_ms": extract_stage_timings(result),
        "backend_identity": extract_backend_identity(result),
        "peak_rss_bytes": two_build.extract_peak_rss_bytes(result),
        "repository_revision": two_build.lookup(
            result, "environment", "repository", "revision"
        ),
        "run_summary": two_build.read_run_summary(prefix),
        "casars_imager": two_build.describe_binary(
            repo / two_build.IMAGER_BINARY_RELATIVE,
            {} if binaries is None else binaries,
        ),
    }
    return run, result


def failed_run_record(
    item: ScheduleItem, command: list[str], repo: pathlib.Path, error: str
) -> dict[str, Any]:
    return {
        **asdict(item),
        "command": command,
        "repo": str(repo),
        "result_status": "execution_failed",
        "error": error,
        "result_path": None,
        "recorded_paths": [],
        "total_wall_seconds": None,
        "stage_timings_ms": {},
        "backend_identity": None,
        "peak_rss_bytes": None,
        "repository_revision": None,
        "run_summary": None,
        "casars_imager": None,
        "products": None,
    }


def result_path_from_stdout(
    stdout: str, *, repo: pathlib.Path = REPO_ROOT
) -> pathlib.Path:
    value = last_nonempty_line(stdout)
    if value is None:
        raise ComparisonError("run_workload.py did not print a result path")
    path = pathlib.Path(value).expanduser()
    if not path.is_absolute():
        path = repo / path
    path = path.resolve()
    if not path.is_file():
        raise ComparisonError(f"run_workload.py result path does not exist: {path}")
    return path


def last_nonempty_line(text: str) -> str | None:
    return next(
        (line.strip() for line in reversed(text.splitlines()) if line.strip()), None
    )


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
    roles: dict[str, dict[str, Any]] | None = None,
    product_comparison: dict[str, Any] | None = None,
    run_environment: dict[str, str] | None = None,
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
    verdicts = build_gate_verdicts(
        config,
        measured_runs=measured_runs,
        paired_deltas=paired_deltas,
        evidence_complete=evidence_complete,
        product_comparison=product_comparison,
    )
    if config.gate == "slowdown":
        verdict = build_verdict(
            paired_deltas,
            tolerance=config.slowdown_tolerance,
            evidence_complete=evidence_complete,
        )
    else:
        verdict = build_blocking_verdict(verdicts)
    wall_tolerance, _ = config.wall_thresholds()
    details = {
        "error": execution_error,
        "comparison_id": comparison_id,
        "configuration": {
            "baseline_workload": config.baseline_workload,
            "candidate_workload": config.candidate_workload,
            "warmup_pair_count": config.warmup_pair_count,
            "measured_pair_count": config.measured_pair_count,
            "slowdown_tolerance_fraction": wall_tolerance,
            "gate": config.gate,
            "thresholds": config.thresholds(),
            "baseline_repo": str(config.baseline_repo),
            "candidate_repo": str(config.candidate_repo),
            "output_root": str(config.output_root),
            "artifact_root": str(config.artifact_root),
            "comparison_artifact_root": str(comparison_artifact_root),
            "run_workload_options": list(config.run_workload_options),
            "baseline_imaging_overrides": list(config.baseline_imaging_overrides),
            "candidate_imaging_overrides": list(config.candidate_imaging_overrides),
            "run_workload_repeats": 1,
            "run_environment": dict(run_environment or {}),
        },
        "roles": roles
        or {role: {"repo": str(config.role_repo(role))} for role in two_build.ROLES},
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
        "product_comparison": product_comparison,
        "verdicts": verdicts,
        "verdict": verdict,
    }
    if execution_error is not None:
        status = "failed_execution"
        failure = {"kind": "execution", "reason": execution_error}
    elif verdict["status"] == "pass":
        status = "completed"
        failure = None
    elif verdict["status"] == "fail":
        status = "out_of_tolerance"
        failure = {
            "kind": "comparison_tolerance",
            "reason": verdict.get("reason") or "candidate exceeded slowdown tolerance",
        }
    else:
        status = "failed_comparison"
        failure = {
            "kind": "comparison",
            "reason": verdict.get("reason") or "comparison evidence was inconclusive",
        }
    results: dict[str, Any] = {"alternating_comparison": details}
    if failure is not None:
        results["failure"] = failure
    return {
        "schema_version": RUN_RESULT_SCHEMA_VERSION,
        "kind": "alternating_comparison",
        "status": status,
        "run_id": comparison_id,
        "created_at": utc_now(),
        "environment": capture_provenance(
            repo_root=REPO_ROOT,
            executables={"run_workload": RUN_WORKLOAD},
            datasets={},
            storage_label="alternating-comparison",
        ),
        "artifacts": {
            "report_path": str(report_path),
            "comparison_artifact_root": str(comparison_artifact_root),
        },
        "results": results,
    }


def build_gate_verdicts(
    config: ComparisonConfig,
    *,
    measured_runs: list[dict[str, Any]],
    paired_deltas: list[dict[str, Any]],
    evidence_complete: bool,
    product_comparison: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    """The wall-time, peak-RSS and product verdicts; those outside the gate
    are recorded as not evaluated."""

    wall_tolerance, noise_sigmas = config.wall_thresholds()
    verdicts = {
        "wall_time": two_build.wall_time_verdict(
            [record["relative_delta"] for record in paired_deltas],
            wall_values(measured_runs, "baseline"),
            wall_tolerance=wall_tolerance,
            noise_sigmas=noise_sigmas,
            evidence_complete=evidence_complete,
        ),
        "peak_rss": two_build.peak_rss_verdict(
            role_values(measured_runs, "baseline", "peak_rss_bytes"),
            role_values(measured_runs, "candidate", "peak_rss_bytes"),
            rss_tolerance=config.rss_tolerance,
            evidence_complete=evidence_complete,
        ),
        "products": two_build.product_verdict(
            product_comparison, product_nrms=config.product_nrms
        ),
    }
    required = two_build.GATE_VERDICTS[config.gate]
    return {
        name: verdict if name in required else two_build.not_evaluated(config.gate)
        for name, verdict in verdicts.items()
    }


def build_blocking_verdict(verdicts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """The overall blocking verdict, in the slowdown verdict's shape: it
    passes only when every verdict passes."""

    status, reason = two_build.overall_verdict(
        verdicts, two_build.GATE_VERDICTS["blocking"]
    )
    wall = verdicts["wall_time"]
    allowed = wall.get("allowed_relative_delta")
    return {
        "status": status,
        "no_slowdown": {"pass": True, "fail": False}.get(wall["status"]),
        "tolerance_fraction": (
            allowed if allowed is not None else wall["wall_tolerance_fraction"]
        ),
        "observed_median_relative_delta": wall.get("observed_median_relative_delta"),
        "basis": (
            "blocking gate: wall time within the baseline noise band, "
            "peak RSS, and product NRMS all pass"
        ),
        "reason": reason,
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
    summaries = [run.get("run_summary") or {} for run in role_runs]
    return {
        "run_count": len(role_runs),
        "total_wall_seconds": robust_summary(walls),
        "peak_rss_bytes": robust_summary(
            [value for value in role_values(runs, role, "peak_rss_bytes") if value]
        ),
        "peak_reserved_bytes": robust_summary(
            [
                value
                for summary in summaries
                if (value := summary.get("peak_reserved_bytes")) is not None
            ]
        ),
        "stage_timings_ms": summarize_stage_timings(role_runs),
        "backend_identities": identities,
        "workers": distinct(summary.get("workers") for summary in summaries),
        "backends": distinct(summary.get("backend") for summary in summaries),
        "precisions": distinct(summary.get("precision") for summary in summaries),
        "repository_revisions": distinct(
            run.get("repository_revision") for run in role_runs
        ),
        "casars_imager_sha256": distinct(
            (run.get("casars_imager") or {}).get("sha256") for run in role_runs
        ),
    }


def role_values(runs: list[dict[str, Any]], role: str, key: str) -> list[Any]:
    return [run.get(key) for run in runs if run["role"] == role]


def distinct(values: Any) -> list[Any]:
    seen: list[Any] = []
    for value in values:
        if value is not None and value not in seen:
            seen.append(value)
    return seen


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
    """The slowdown gate's verdict: the median paired relative delta is at
    most ``tolerance``."""

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


if __name__ == "__main__":
    raise SystemExit(main())
