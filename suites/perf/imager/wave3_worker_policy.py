#!/usr/bin/env python3
"""Summarize Wave 3 CPU multi-worker policy evidence from workload result JSONs."""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import random
import statistics
import sys
from typing import Any

from perf_harness import ContractError, load_run_result


RELIABLE_MIN_PAIRS = 5
CONFIRMATION_PAIRS = 7


class PolicyError(Exception):
    """Validation error shown without a traceback."""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", nargs="+", type=pathlib.Path)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument(
        "--accepted-correctness-note",
        help=(
            "explicit note allowing timing-only rows to reuse already accepted "
            "correctness evidence"
        ),
    )
    args = parser.parse_args()

    try:
        loaded = []
        skipped_inputs = []
        for path in args.results:
            result = load_run_result(path, source_key="_policy_path")
            if is_result_bundle(result):
                loaded.append(result)
            else:
                skipped_inputs.append(str(path))
        report = build_policy_report(
            loaded,
            accepted_correctness_note=args.accepted_correctness_note,
        )
        report["skipped_inputs"] = skipped_inputs
    except (PolicyError, ContractError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None

    if args.format == "json":
        json.dump(report, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    else:
        for row in report["scenario_reports"]:
            print(
                f"scenario={row['scenario_id']} pairs={row['pair_count']} "
                f"serial={format_seconds(row['serial_median_seconds'])} "
                f"multi={format_seconds(row['multi_cpu_median_seconds'])} "
                f"speedup={format_percent(row['median_speedup'])} "
                f"noise={format_percent(row['noise'])} "
                f"classification={row['classification']} "
                f"recommendation={row['planner_recommendation']}"
            )


def is_result_bundle(value: dict[str, Any]) -> bool:
    return isinstance(value.get("results"), dict) and isinstance(value.get("mode"), dict)


def build_policy_report(
    results: list[dict[str, Any]], *, accepted_correctness_note: str | None = None
) -> dict[str, Any]:
    grouped: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for result in results:
        scenario = scenario_id(result)
        backend = backend_class(result)
        grouped.setdefault(scenario, {}).setdefault(backend, []).append(result)
    scenario_reports = []
    for scenario, by_backend in sorted(grouped.items()):
        serial = combine_backend_runs(by_backend.get("serial_cpu", []))
        multi = combine_backend_runs(by_backend.get("multi_cpu", []))
        auto = combine_backend_runs(by_backend.get("auto", []))
        metal = combine_backend_runs(by_backend.get("metal", []))
        scenario_reports.append(
            classify_scenario(
                scenario_id=scenario,
                serial_runs=serial,
                multi_runs=multi,
                auto_runs=auto,
                metal_runs=metal,
                serial_results=by_backend.get("serial_cpu", []),
                multi_results=by_backend.get("multi_cpu", []),
                auto_results=by_backend.get("auto", []),
                metal_results=by_backend.get("metal", []),
                accepted_correctness_note=accepted_correctness_note,
            )
        )
    return {
        "schema_version": 1,
        "decision_rule": {
            "reliable_min_pairs": RELIABLE_MIN_PAIRS,
            "preferred_confirmation_pairs": CONFIRMATION_PAIRS,
            "median_speedup_threshold": "max(5%, 2*noise)",
            "confidence_interval": "bootstrap 90% paired median speedup",
            "inconclusive_policy": "cpu-only auto should prefer serial unless Brian approves an exception",
        },
        "scenario_reports": scenario_reports,
    }


def scenario_id(result: dict[str, Any]) -> str:
    mode = result.get("mode", {})
    dataset = result.get("dataset", {})
    features = benchmark_features(result)
    image = features.get("image", {}) if isinstance(features, dict) else {}
    mode_cost = features.get("mode_cost", {}) if isinstance(features, dict) else {}
    scales = mode_cost.get("multiscale_scale_count")
    parts = [
        str(result.get("workload", {}).get("mode_id") or result.get("mode_id") or "unknown"),
        str(dataset.get("key") or "dataset-unknown"),
        f"spec={mode.get('specmode')}",
        f"gridder={mode.get('gridder')}",
        f"deconvolver={mode.get('deconvolver')}",
        f"nterms={mode.get('nterms')}",
        f"scales={scales}",
        f"niter={mode.get('niter')}",
        f"shape={image.get('imsize_x')}x{image.get('imsize_y')}",
        f"channels={mode.get('channel_count')}",
        f"width={mode.get('width')}",
        f"weighting={mode.get('weighting')}",
    ]
    return "|".join(parts)


def backend_class(result: dict[str, Any]) -> str:
    requested = str(result.get("mode", {}).get("standard_mfs_acceleration") or "").lower()
    if requested in {"cpu", "serial", "single"}:
        return "serial_cpu"
    if requested in {"multi-cpu", "multicpu", "fixed-tile", "fixed_tile"}:
        return "multi_cpu"
    if requested in {"metal", "gpu"}:
        return "metal"
    if requested == "auto":
        return "auto"
    return "unknown"


def combine_backend_runs(results: list[dict[str, Any]]) -> list[float]:
    runs: list[float] = []
    for result in results:
        timing = (
            result.get("results", {})
            .get("rust", {})
            .get("timings_seconds", {})
            .get("runs", [])
        )
        if isinstance(timing, list):
            runs.extend(float(value) for value in timing if isinstance(value, (int, float)))
    return runs


def benchmark_features(result: dict[str, Any]) -> dict[str, Any]:
    features = result.get("results", {}).get("benchmark_features")
    if isinstance(features, dict):
        return features
    features = result.get("benchmark_features")
    return features if isinstance(features, dict) else {}


def representative_features(results: list[dict[str, Any]]) -> dict[str, Any]:
    for result in results:
        features = benchmark_features(result)
        if features:
            return features
    return {}


def combine_stage_medians(results: list[dict[str, Any]]) -> dict[str, float]:
    by_name: dict[str, list[float]] = {}
    for result in results:
        stages = (
            result.get("results", {})
            .get("stage_medians_ms", {})
            .get("rust", {})
        )
        if not isinstance(stages, dict):
            continue
        for name, value in stages.items():
            if isinstance(value, (int, float)) and math.isfinite(float(value)):
                by_name.setdefault(name, []).append(float(value))
    return {name: statistics.median(values) for name, values in sorted(by_name.items())}


def classify_scenario(
    *,
    scenario_id: str,
    serial_runs: list[float],
    multi_runs: list[float],
    auto_runs: list[float],
    metal_runs: list[float],
    serial_results: list[dict[str, Any]],
    multi_results: list[dict[str, Any]],
    auto_results: list[dict[str, Any]],
    metal_results: list[dict[str, Any]],
    accepted_correctness_note: str | None,
) -> dict[str, Any]:
    pair_count = min(len(serial_runs), len(multi_runs))
    paired_speedups = [
        1.0 - (multi_runs[index] / serial_runs[index])
        for index in range(pair_count)
        if serial_runs[index] > 0
    ]
    serial_median = median_or_none(serial_runs)
    multi_median = median_or_none(multi_runs)
    median_speedup = median_or_none(paired_speedups)
    noise = paired_noise(serial_runs[:pair_count], multi_runs[:pair_count], paired_speedups)
    ci = bootstrap_ci(paired_speedups) if pair_count >= RELIABLE_MIN_PAIRS else [None, None]
    threshold = max(0.05, 2.0 * noise) if noise is not None else 0.05
    wins = sum(1 for value in paired_speedups if value > 0.0)
    losses = sum(1 for value in paired_speedups if value < 0.0)
    correctness = correctness_status(
        serial_results + multi_results,
        accepted_correctness_note=accepted_correctness_note,
    )
    classification = "missing_baseline"
    recommendation = "collect serial and multi-cpu paired runs"
    if pair_count > 0 and correctness not in {"green", "accepted_external"}:
        classification = "correctness_blocked"
        recommendation = "do not use timing for planner policy until correctness is green"
    elif pair_count < RELIABLE_MIN_PAIRS:
        if median_speedup is not None and median_speedup > 0.0:
            classification = "screening_win_needs_confirmation"
            recommendation = "run confirmation pairs before allowing CPU multi-worker auto"
        elif median_speedup is not None and median_speedup < 0.0:
            classification = "screening_loss"
            recommendation = "prefer serial unless later confirmation reverses this"
        elif pair_count > 0:
            classification = "screening_inconclusive"
            recommendation = "prefer serial for CPU-only auto until more evidence exists"
    else:
        lower_ci = ci[0]
        upper_ci = ci[1]
        required_wins = 5 if pair_count >= CONFIRMATION_PAIRS else 4
        if (
            median_speedup is not None
            and median_speedup > threshold
            and lower_ci is not None
            and lower_ci > 0.0
            and wins >= required_wins
        ):
            classification = "reliable_win"
            recommendation = "allow CPU multi-worker auto for matching shape/resource class"
        elif (
            median_speedup is not None
            and median_speedup < -threshold
            and upper_ci is not None
            and upper_ci < 0.0
            and losses >= required_wins
        ):
            classification = "reliable_loss"
            recommendation = "prefer serial for CPU-only auto"
        else:
            classification = "inconclusive"
            recommendation = "prefer serial for CPU-only auto; explicit multi-cpu remains available"
    return {
        "scenario_id": scenario_id,
        "pair_count": pair_count,
        "serial_run_seconds": serial_runs,
        "multi_cpu_run_seconds": multi_runs,
        "auto_run_seconds": auto_runs,
        "metal_run_seconds": metal_runs,
        "serial_median_seconds": serial_median,
        "multi_cpu_median_seconds": multi_median,
        "auto_median_seconds": median_or_none(auto_runs),
        "metal_median_seconds": median_or_none(metal_runs),
        "paired_speedups": paired_speedups,
        "median_speedup": median_speedup,
        "noise": noise,
        "threshold": threshold,
        "bootstrap_90ci": ci,
        "wins": wins,
        "losses": losses,
        "correctness_status": correctness,
        "correctness_evidence": accepted_correctness_note,
        "backend_failures": backend_failures(
            {
                "serial_cpu": serial_results,
                "multi_cpu": multi_results,
                "auto": auto_results,
                "metal": metal_results,
            }
        ),
        "features": representative_features(
            serial_results + multi_results + auto_results + metal_results
        ),
        "stage_medians_ms": {
            "serial_cpu": combine_stage_medians(serial_results),
            "multi_cpu": combine_stage_medians(multi_results),
            "auto": combine_stage_medians(auto_results),
            "metal": combine_stage_medians(metal_results),
        },
        "classification": classification,
        "planner_recommendation": recommendation,
    }


def backend_failures(results_by_backend: dict[str, list[dict[str, Any]]]) -> dict[str, list[str]]:
    failures: dict[str, list[str]] = {}
    for backend, results in results_by_backend.items():
        reasons = []
        for result in results:
            if result.get("status") != "failed_execution":
                continue
            reason = (
                result.get("results", {})
                .get("rust", {})
                .get("reason")
                or result.get("results", {})
                .get("product_comparison", {})
                .get("reason")
                or "run failed"
            )
            path = result.get("_policy_path")
            reasons.append(f"{path}: {reason}" if path else str(reason))
        if reasons:
            failures[backend] = reasons
    return failures


def correctness_status(
    results: list[dict[str, Any]], *, accepted_correctness_note: str | None = None
) -> str:
    if not results:
        return "unknown"
    labels = []
    for result in results:
        gate = result.get("human_review", {})
        label = gate.get("structured_difference_label")
        panel_status = gate.get("panel_status")
        if label in {"bad", "investigate"}:
            labels.append(label)
        elif label == "good" and panel_status in {"ready", "not_run", "missing", None}:
            labels.append("green")
        elif label in {"not_run", None} or panel_status in {"not_run", "missing", None}:
            labels.append("not_evaluated")
        else:
            labels.append(str(panel_status))
    if any(label == "bad" for label in labels):
        return "blocked"
    if any(label == "investigate" for label in labels):
        return "review"
    if any(label == "not_evaluated" for label in labels):
        return "accepted_external" if accepted_correctness_note else "not_evaluated"
    return "green"


def paired_noise(
    serial: list[float], multi: list[float], speedups: list[float]
) -> float | None:
    if not speedups:
        return None
    pieces = [
        normalized_mad(serial),
        normalized_mad(multi),
        mad([math.log(multi[index] / serial[index]) for index in range(len(speedups))]),
    ]
    finite = [value for value in pieces if value is not None and math.isfinite(value)]
    return max(finite) if finite else 0.0


def normalized_mad(values: list[float]) -> float | None:
    center = median_or_none(values)
    spread = mad(values)
    if center is None or center == 0.0 or spread is None:
        return None
    return abs(spread / center)


def mad(values: list[float]) -> float | None:
    if not values:
        return None
    center = statistics.median(values)
    return statistics.median([abs(value - center) for value in values])


def bootstrap_ci(values: list[float], *, samples: int = 2000) -> list[float | None]:
    if not values:
        return [None, None]
    rng = random.Random(287)
    medians = []
    for _ in range(samples):
        draw = [values[rng.randrange(len(values))] for _ in values]
        medians.append(statistics.median(draw))
    medians.sort()
    lower = medians[int(0.05 * (len(medians) - 1))]
    upper = medians[int(0.95 * (len(medians) - 1))]
    return [lower, upper]


def median_or_none(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def format_seconds(value: float | None) -> str:
    return "missing" if value is None else f"{value:.3f}s"


def format_percent(value: float | None) -> str:
    return "missing" if value is None else f"{100.0 * value:.1f}%"


if __name__ == "__main__":
    main()
