#!/usr/bin/env python3
"""Compare two casa-rs VLASS MT-MFS clean traces fail-closed."""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import re
import sys


MINOR_RE = re.compile(
    r"^mosaic_mtmfs_minor_cycle "
    r"cycle=(?P<cycle>\d+) "
    r"start_iteration=(?P<start_iteration>\d+) "
    r"reported_updates=(?P<reported_updates>\d+) "
    r"actual_updates=(?P<actual_updates>\d+) "
    r"start_peak=(?P<start_peak>[-+0-9.eE]+) "
    r"approximate_end_peak=(?P<end_peak>[-+0-9.eE]+) "
    r"cycle_threshold=(?P<cycle_threshold>[-+0-9.eE]+) "
    r"nsigma_threshold=(?P<nsigma_threshold>[-+0-9.eE]+) "
    r"model_flux=(?P<model_flux>[-+0-9.eE]+) "
    r"initial_scale_pixels=(?P<initial_scale>[^ ]+) "
    r"initial_candidate_strength=(?P<candidate_strength>[^ ]+) "
    r"initial_candidate_position=(?P<candidate_position>Some\(\[[^]]+\]\)|None) "
    r"stop_reason=(?P<stop_reason>.*)$"
)
REFRESH_RE = re.compile(
    r"^mosaic_mtmfs_residual_refresh "
    r"major_cycle=(?P<major_cycle>\d+) "
    r"reported_iterations=(?P<reported_iterations>\d+) "
    r"refreshed_peak=(?P<refreshed_peak>[-+0-9.eE]+) "
    r"model_flux=(?P<model_flux>[-+0-9.eE]+)$"
)
FINAL_RE = re.compile(
    r"^mosaic_mtmfs_final_residual_refresh "
    r"reported_iterations=(?P<reported_iterations>\d+) "
    r"refreshed_peak=(?P<refreshed_peak>[-+0-9.eE]+) "
    r"model_flux=(?P<model_flux>[-+0-9.eE]+)$"
)
OPTION_FLOAT_RE = re.compile(r"^Some\((?P<value>[-+0-9.eE]+)\)$")

DISCRETE_MINOR_FIELDS = (
    "cycle",
    "start_iteration",
    "reported_updates",
    "actual_updates",
    "initial_scale",
    "candidate_position",
    "stop_reason",
)
NUMERICAL_MINOR_FIELDS = (
    "start_peak",
    "end_peak",
    "cycle_threshold",
    "nsigma_threshold",
    "model_flux",
    "candidate_strength",
)
DISCRETE_REFRESH_FIELDS = ("major_cycle", "reported_iterations")
NUMERICAL_REFRESH_FIELDS = ("refreshed_peak", "model_flux")


def parse_optional_float(value: str) -> float | None:
    match = OPTION_FLOAT_RE.match(value)
    return float(match.group("value")) if match is not None else None


def parse_log(path: pathlib.Path) -> dict[str, object]:
    minor: list[dict[str, object]] = []
    refresh: list[dict[str, object]] = []
    final = None
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = MINOR_RE.match(line)
        if match is not None:
            row: dict[str, object] = {
                key: int(match.group(key))
                for key in (
                    "cycle",
                    "start_iteration",
                    "reported_updates",
                    "actual_updates",
                )
            }
            row.update(
                {
                    key: float(match.group(key))
                    for key in (
                        "start_peak",
                        "end_peak",
                        "cycle_threshold",
                        "nsigma_threshold",
                        "model_flux",
                    )
                }
            )
            row.update(
                {
                    "initial_scale": match.group("initial_scale"),
                    "candidate_strength": parse_optional_float(
                        match.group("candidate_strength")
                    ),
                    "candidate_position": match.group("candidate_position"),
                    "stop_reason": match.group("stop_reason"),
                }
            )
            minor.append(row)
            continue
        match = REFRESH_RE.match(line)
        if match is not None:
            refresh.append(
                {
                    "major_cycle": int(match.group("major_cycle")),
                    "reported_iterations": int(match.group("reported_iterations")),
                    "refreshed_peak": float(match.group("refreshed_peak")),
                    "model_flux": float(match.group("model_flux")),
                }
            )
            continue
        match = FINAL_RE.match(line)
        if match is not None:
            final = {
                "reported_iterations": int(match.group("reported_iterations")),
                "refreshed_peak": float(match.group("refreshed_peak")),
                "model_flux": float(match.group("model_flux")),
            }
    return {"minor": minor, "refresh": refresh, "final": final}


def relative_difference(left: float, right: float) -> float:
    return abs(left - right) / max(abs(left), abs(right), 1.0e-30)


def compare_rows(
    reference: list[dict[str, object]],
    candidate: list[dict[str, object]],
    discrete_fields: tuple[str, ...],
    numerical_fields: tuple[str, ...],
) -> dict[str, object]:
    discrete_mismatches: list[dict[str, object]] = []
    maxima = {
        field: {"absolute": 0.0, "relative": 0.0, "index": None}
        for field in numerical_fields
    }
    for index, (left, right) in enumerate(zip(reference, candidate, strict=False)):
        differing = {
            field: {"reference": left[field], "candidate": right[field]}
            for field in discrete_fields
            if left[field] != right[field]
        }
        if differing:
            discrete_mismatches.append({"index": index, "fields": differing})
        for field in numerical_fields:
            left_value = left[field]
            right_value = right[field]
            if left_value is None or right_value is None:
                if left_value != right_value:
                    discrete_mismatches.append(
                        {
                            "index": index,
                            "fields": {
                                field: {
                                    "reference": left_value,
                                    "candidate": right_value,
                                }
                            },
                        }
                    )
                continue
            absolute = abs(float(left_value) - float(right_value))
            relative = relative_difference(float(left_value), float(right_value))
            if relative > float(maxima[field]["relative"]):
                maxima[field] = {
                    "absolute": absolute,
                    "relative": relative,
                    "index": index,
                }
    same_count = len(reference) == len(candidate)
    return {
        "reference_count": len(reference),
        "candidate_count": len(candidate),
        "same_count": same_count,
        "discrete_parity": not discrete_mismatches and same_count,
        "discrete_mismatches": discrete_mismatches,
        "numerical_maxima": maxima,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-log", required=True, type=pathlib.Path)
    parser.add_argument("--candidate-log", required=True, type=pathlib.Path)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    args = parser.parse_args()

    reference = parse_log(args.reference_log)
    candidate = parse_log(args.candidate_log)
    if not reference["minor"] or not candidate["minor"]:
        parser.error("both logs must contain casa-rs minor-cycle trace rows")

    minor = compare_rows(
        reference["minor"],
        candidate["minor"],
        DISCRETE_MINOR_FIELDS,
        NUMERICAL_MINOR_FIELDS,
    )
    refresh = compare_rows(
        reference["refresh"],
        candidate["refresh"],
        DISCRETE_REFRESH_FIELDS,
        NUMERICAL_REFRESH_FIELDS,
    )
    final_equal = (
        reference["final"] is not None
        and candidate["final"] is not None
        and reference["final"]["reported_iterations"]
        == candidate["final"]["reported_iterations"]
    )
    result = {
        "kind": "vlass_clean_casars_trace_comparison",
        "reference_log": str(args.reference_log.resolve()),
        "candidate_log": str(args.candidate_log.resolve()),
        "minor": minor,
        "refresh": refresh,
        "final_refresh_iteration_parity": final_equal,
        "reference_final_refresh": reference["final"],
        "candidate_final_refresh": candidate["final"],
    }
    numerical_values = [
        float(metric[measure])
        for section in (minor, refresh)
        for metric in section["numerical_maxima"].values()
        for measure in ("absolute", "relative")
    ]
    if not all(math.isfinite(value) for value in numerical_values):
        parser.error("comparison produced a non-finite numerical metric")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "output": str(args.output),
                "minor_discrete_parity": minor["discrete_parity"],
                "refresh_discrete_parity": refresh["discrete_parity"],
                "final_refresh_iteration_parity": final_equal,
                "minor_numerical_maxima": minor["numerical_maxima"],
                "refresh_numerical_maxima": refresh["numerical_maxima"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    if not (
        minor["discrete_parity"]
        and refresh["discrete_parity"]
        and final_equal
    ):
        sys.exit(1)


if __name__ == "__main__":
    main()
