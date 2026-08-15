#!/usr/bin/env python3
"""Compare CASA and casa-rs VLASS MT-MFS major/minor-cycle traces."""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import re


CASA_MINOR_RE = re.compile(
    r"iters=\d+->\d+ \[(?P<updates>\d+)\], "
    r"model=[^>]+->(?P<model>[-+0-9.eE]+), "
    r"peakres=(?P<start_peak>[-+0-9.eE]+)->(?P<end_peak>[-+0-9.eE]+)"
)
RUST_MINOR_RE = re.compile(
    r"^mosaic_mtmfs_minor_cycle "
    r"cycle=(?P<cycle>\d+) "
    r"start_iteration=(?P<start_iteration>\d+) "
    r"reported_updates=(?P<updates>\d+) "
    r"actual_updates=(?P<actual_updates>\d+) "
    r"start_peak=(?P<start_peak>[-+0-9.eE]+) "
    r"approximate_end_peak=(?P<approximate_end_peak>[-+0-9.eE]+) "
    r".* model_flux=(?P<model>[-+0-9.eE]+) "
)
RUST_FINAL_REFRESH_RE = re.compile(
    r"^mosaic_mtmfs_final_residual_refresh "
    r"reported_iterations=(?P<iterations>\d+) "
    r"refreshed_peak=(?P<peak>[-+0-9.eE]+) "
    r"model_flux=(?P<model>[-+0-9.eE]+)"
)


def relative_difference(left: float, right: float) -> float:
    scale = max(abs(left), abs(right), 1.0e-30)
    return abs(left - right) / scale


def parse_casa(path: pathlib.Path) -> list[dict[str, float | int]]:
    rows: list[dict[str, float | int]] = []
    cumulative = 0
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = CASA_MINOR_RE.search(line)
        if match is None:
            continue
        updates = int(match.group("updates"))
        rows.append(
            {
                "cycle": len(rows),
                "start_iteration": cumulative,
                "updates": updates,
                "end_iteration": cumulative + updates,
                "start_peak": float(match.group("start_peak")),
                "minor_end_peak": float(match.group("end_peak")),
                "model": float(match.group("model")),
            }
        )
        cumulative += updates
    return rows


def parse_rust(
    path: pathlib.Path,
) -> tuple[list[dict[str, float | int]], dict[str, float | int] | None]:
    rows: list[dict[str, float | int]] = []
    final_refresh = None
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = RUST_MINOR_RE.search(line)
        if match is not None:
            rows.append(
                {
                    "cycle": int(match.group("cycle")),
                    "start_iteration": int(match.group("start_iteration")),
                    "updates": int(match.group("updates")),
                    "actual_updates": int(match.group("actual_updates")),
                    "end_iteration": int(match.group("start_iteration"))
                    + int(match.group("updates")),
                    "start_peak": float(match.group("start_peak")),
                    "approximate_end_peak": float(
                        match.group("approximate_end_peak")
                    ),
                    "model": float(match.group("model")),
                }
            )
            continue
        match = RUST_FINAL_REFRESH_RE.search(line)
        if match is not None:
            final_refresh = {
                "iterations": int(match.group("iterations")),
                "peak": float(match.group("peak")),
                "model": float(match.group("model")),
            }
    return rows, final_refresh


def max_metric(rows: list[dict[str, object]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return max(values) if values else None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--casa-log", required=True, type=pathlib.Path)
    parser.add_argument("--rust-log", required=True, type=pathlib.Path)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument("--top", type=int, default=12)
    args = parser.parse_args()

    casa = parse_casa(args.casa_log)
    rust, rust_final_refresh = parse_rust(args.rust_log)
    if not casa:
        parser.error(f"no CASA minor-cycle summaries found in {args.casa_log}")
    if not rust:
        parser.error(f"no casa-rs minor-cycle summaries found in {args.rust_log}")

    aligned: list[dict[str, object]] = []
    for index, (casa_row, rust_row) in enumerate(zip(casa, rust, strict=False)):
        start_peak_absolute = abs(
            float(casa_row["start_peak"]) - float(rust_row["start_peak"])
        )
        model_absolute = abs(float(casa_row["model"]) - float(rust_row["model"]))
        aligned.append(
            {
                "cycle": index,
                "casa": casa_row,
                "rust": rust_row,
                "cycle_equal": index == rust_row["cycle"],
                "start_iteration_equal": (
                    casa_row["start_iteration"] == rust_row["start_iteration"]
                ),
                "updates_equal": casa_row["updates"] == rust_row["updates"],
                "actual_updates_equal": (
                    rust_row["updates"] == rust_row["actual_updates"]
                ),
                "end_iteration_equal": (
                    casa_row["end_iteration"] == rust_row["end_iteration"]
                ),
                "start_peak_absolute_difference": start_peak_absolute,
                "start_peak_relative_difference": relative_difference(
                    float(casa_row["start_peak"]), float(rust_row["start_peak"])
                ),
                "model_absolute_difference": model_absolute,
                "model_relative_difference": relative_difference(
                    float(casa_row["model"]), float(rust_row["model"])
                ),
            }
        )

    discrete_mismatches = [
        row
        for row in aligned
        if not all(
            bool(row[key])
            for key in (
                "cycle_equal",
                "start_iteration_equal",
                "updates_equal",
                "actual_updates_equal",
                "end_iteration_equal",
            )
        )
    ]
    ranked = sorted(
        aligned,
        key=lambda row: max(
            float(row["start_peak_relative_difference"]),
            float(row["model_relative_difference"]),
        ),
        reverse=True,
    )
    result = {
        "kind": "vlass_clean_major_cycle_trace_comparison",
        "role": "reduced_turnaround_correctness_diagnostic",
        "casa_log": str(args.casa_log.resolve()),
        "rust_log": str(args.rust_log.resolve()),
        "casa_cycles": len(casa),
        "rust_cycles": len(rust),
        "aligned_cycles": len(aligned),
        "coverage": {
            "casa_complete": len(aligned) == len(casa),
            "rust_complete": len(aligned) == len(rust),
            "same_cycle_count": len(casa) == len(rust),
        },
        "discrete_parity": {
            "status": "passed" if not discrete_mismatches else "failed",
            "mismatch_count": len(discrete_mismatches),
            "mismatches": discrete_mismatches[: args.top],
        },
        "numerical_summary": {
            "max_start_peak_absolute_difference": max_metric(
                aligned, "start_peak_absolute_difference"
            ),
            "max_start_peak_relative_difference": max_metric(
                aligned, "start_peak_relative_difference"
            ),
            "max_model_absolute_difference": max_metric(
                aligned, "model_absolute_difference"
            ),
            "max_model_relative_difference": max_metric(
                aligned, "model_relative_difference"
            ),
        },
        "rust_final_refresh": rust_final_refresh,
        "largest_numerical_differences": ranked[: args.top],
        "aligned": aligned,
    }
    if any(
        not math.isfinite(float(value))
        for value in result["numerical_summary"].values()
        if value is not None
    ):
        parser.error("non-finite numerical comparison result")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "output": str(args.output),
                "casa_cycles": len(casa),
                "rust_cycles": len(rust),
                "aligned_cycles": len(aligned),
                "discrete_mismatch_count": len(discrete_mismatches),
                **result["numerical_summary"],
                "rust_final_refresh": rust_final_refresh,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
