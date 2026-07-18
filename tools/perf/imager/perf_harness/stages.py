# SPDX-License-Identifier: LGPL-3.0-or-later
"""Canonical parser for benchmark timing, stage, and backend evidence."""

from __future__ import annotations

import json
import re
import statistics
from typing import Any


RUST_STAGE_FIELDS = {
    "open_measurement_set",
    "prepare_plane_input",
    "get_ms_values_into_processing_buffer",
    "prepare_processing_buffer",
    "extract_phase_center",
    "run_imaging",
    "build_coordinate_system",
    "write_products",
    "io_time",
    "frontend_total",
    "controller_overhead",
    "weighting",
    "executor_build",
    "planned_sample_replay",
    "grid_update",
    "psf_grid",
    "psf_fft",
    "psf_normalize",
    "model_fft",
    "residual_degrid_grid",
    "residual_fft",
    "residual_normalize",
    "clean_cycle_setup",
    "deconvolver_setup",
    "major_cycle_refresh",
    "residual_refresh_overhead",
    "multiscale_scale_refresh",
    "minor_cycle",
    "minor_cycle_solve",
    "beam_fit",
    "restore",
    "total",
}
RUST_STAGE_FIELD_ALIASES = {
    "prepared_source_read": "get_ms_values_into_processing_buffer",
    "prepared_source_prepare": "prepare_processing_buffer",
}
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
            if heading == "Rust stage medians":
                name = RUST_STAGE_FIELD_ALIASES.get(name, name)
            if name != "run" and name in allowed:
                stages[name] = float(raw_value)
    return stages


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
