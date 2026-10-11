# SPDX-License-Identifier: LGPL-3.0-or-later
"""Canonical parser for benchmark timing, stage, and backend evidence."""

from __future__ import annotations

import json
import re
import statistics
from typing import Any


# The kinds of `casars-imager` phase, as `stage_timing` logs them: a run's
# major cycles, minor cycles and their preparations are summed over the run.
RUST_STAGE_FIELDS = {
    "preparation",
    "setup",
    "major_cycles",
    "minor_cycle_preparations",
    "minor_cycles",
    "model_prediction",
    "products",
    "total",
}
RUST_STAGE_TARGET = "casa_imaging::stage_timing"
CASA_STAGE_FIELDS = {
    "parameter_setup",
    "construct_imager",
    "initialize_imagers",
    "select_data",
    "define_image",
    "normalizer_info",
    "cf_cache_setup",
    "initialize_normalizers",
    "set_weighting",
    "set_weighting_core",
    "initialize_deconvolvers",
    "estimate_memory",
    "initialize_iteration_control",
    "make_psf",
    "make_pb",
    "calcres_major_cycle",
    "update_mask",
    "has_converged",
    "minor_cycle",
    "clean_major_cycle",
    "restore_images",
    "delete_tools",
    "total",
}

_TIMING_BOUNDARIES = (
    "Rust release CLI timings ",
    "Rust stage medians ",
    "CASA tclean timings ",
    "Kept benchmark products:",
    "CASA PySynthesisImager stage medians ",
)


def parse_timing_section(text: str, heading: str) -> tuple[list[float], float | None]:
    """Parse per-run and median wall times from a named benchmark section."""

    runs: list[float] = []
    median_value = None
    for line in _section_lines(text, f"{heading} "):
        run_match = re.search(r"\brun=\d+\s+real=([0-9.]+)", line)
        if run_match:
            runs.append(float(run_match.group(1)))
        median_match = re.search(r"\bmedian=([0-9.]+)", line)
        if median_match:
            median_value = float(median_match.group(1))
    if median_value is None and runs:
        median_value = statistics.median(runs)
    return runs, median_value


def parse_stage_section(text: str, heading: str) -> dict[str, float]:
    """Parse a known Rust or CASA stage-median section."""

    if heading == "Rust stage medians":
        allowed = RUST_STAGE_FIELDS
    elif heading == "CASA PySynthesisImager stage medians":
        allowed = CASA_STAGE_FIELDS
    else:
        raise ValueError(f"unsupported stage heading: {heading}")

    stages: dict[str, float] = {}
    for line in _section_lines(text, f"{heading} "):
        for name, raw_value in re.findall(r"([A-Za-z0-9_]+)=([0-9.]+)", line):
            if name != "run" and name in allowed:
                stages[name] = float(raw_value)
    return stages


def rust_phase_milliseconds(stderr: str) -> dict[str, float]:
    """Total milliseconds of each phase kind in one run's stderr, from its
    `casa_imaging::stage_timing` events, with their sum as `total`."""

    totals: dict[str, float] = {}
    for line in stderr.splitlines():
        if RUST_STAGE_TARGET not in line:
            continue
        phase = re.search(r' phase="([^"]*)"', line)
        seconds = re.search(r" seconds=([0-9.eE+-]+)", line)
        if phase is None or seconds is None:
            raise ValueError(f"malformed stage-timing event: {line}")
        name = phase.group(1)
        kind = re.sub(r" \d+$", "s", name).replace(" ", "_").replace("-", "_")
        if kind not in RUST_STAGE_FIELDS:
            raise ValueError(f"unknown casars-imager phase {name!r}: {line}")
        totals[kind] = totals.get(kind, 0.0) + 1000.0 * float(seconds.group(1))
    if totals:
        totals["total"] = sum(totals.values())
    return totals


def rust_stage_medians(stderr_texts: list[str]) -> dict[str, float]:
    """Median over runs of each phase kind's total milliseconds."""

    runs = [rust_phase_milliseconds(text) for text in stderr_texts]
    kinds = sorted({kind for run in runs for kind in run})
    return {
        kind: statistics.median(run.get(kind, 0.0) for run in runs) for kind in kinds
    }


def _print_rust_stage_medians(paths: list[str]) -> None:
    texts = []
    for path in paths:
        with open(path, encoding="utf-8", errors="replace") as handle:
            texts.append(handle.read())
    for kind, milliseconds in rust_stage_medians(texts).items():
        print(f"  {kind}={milliseconds:.3f}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3 or sys.argv[1] != "rust-stage-medians":
        sys.exit("usage: stages.py rust-stage-medians STDERR_FILE...")
    _print_rust_stage_medians(sys.argv[2:])


def parse_casa_clean_control_diagnostics(text: str) -> list[Any]:
    """Parse the optional CASA clean-control diagnostic record."""

    prefix = "clean_control_diagnostics_json="
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line.startswith(prefix):
            try:
                value = json.loads(line[len(prefix) :])
            except json.JSONDecodeError:
                return []
            return value if isinstance(value, list) else []
    return []


def _section_lines(text: str, heading_prefix: str) -> list[str]:
    result: list[str] = []
    collecting = False
    for line in text.splitlines():
        if line.startswith(heading_prefix):
            collecting = True
            continue
        if collecting and any(
            line.startswith(boundary)
            for boundary in _TIMING_BOUNDARIES
            if boundary != heading_prefix
        ):
            break
        if collecting:
            result.append(line.strip())
    return result
