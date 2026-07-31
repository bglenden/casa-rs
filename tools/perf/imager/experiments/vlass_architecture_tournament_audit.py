#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Audit radical VLASS imaging architectures from a frozen promoted run.

This tool does not run CASA, casa-rs, or inspect a MeasurementSet. It validates
the canonical 4096-square/full-16-SPW scientific promotion receipt, parses its
content-addressed casa-rs run log, and turns the measured operator trajectory
into quantitative architecture-tournament cards.

The arithmetic is deliberately a work proxy, not a speedup claim. A candidate
still needs an executable discriminator, the frozen science floor, end-to-end
timings, and a credible full-size liveness projection before promotion.
"""

from __future__ import annotations

import argparse
import ast
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
import vlass_full_geometry_memory_campaign as memory_campaign  # noqa: E402


EXPECTED_SPWS = list(range(2, 18))
SUBGRID_SIDES = (32, 48, 64, 96)
KEY_VALUE_PATTERN = re.compile(r"([A-Za-z0-9_]+)=((?:\[[^\]]*\])|(?:[^\s]+))")


class TournamentError(RuntimeError):
    """Raised when frozen evidence cannot support a valid tournament audit."""


def utc_now() -> str:
    """Return a stable UTC timestamp."""

    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    """Hash one file without loading it into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path, *, label: str) -> dict[str, Any]:
    """Load one JSON object with an audit-specific error."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise TournamentError(f"cannot read {label} {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise TournamentError(f"{label} is not valid JSON: {path}: {error}") from error
    if not isinstance(value, dict):
        raise TournamentError(f"{label} must contain a JSON object: {path}")
    return value


def parse_scalar(value: str) -> Any:
    """Parse one log scalar while leaving domain-specific tokens intact."""

    if value == "true":
        return True
    if value == "false":
        return False
    if value.startswith("[") and value.endswith("]"):
        try:
            return ast.literal_eval(value)
        except (SyntaxError, ValueError):
            return value
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def parse_event(line: str) -> tuple[str, dict[str, Any]]:
    """Parse one structured casa-rs log line."""

    name, separator, remainder = line.strip().partition(" ")
    if not separator:
        return name, {}
    return name, {
        match.group(1): parse_scalar(match.group(2))
        for match in KEY_VALUE_PATTERN.finditer(remainder)
    }


def parse_events(text: str) -> dict[str, list[dict[str, Any]]]:
    """Group all structured log events by event name."""

    events: dict[str, list[dict[str, Any]]] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        name, values = parse_event(line)
        events.setdefault(name, []).append(values)
    return events


def require_events(
    events: dict[str, list[dict[str, Any]]],
    name: str,
) -> list[dict[str, Any]]:
    """Return a non-empty event list."""

    found = events.get(name, [])
    if not found:
        raise TournamentError(f"run log lacks required {name} evidence")
    return found


def int_csv(value: Any, *, label: str) -> list[int]:
    """Parse a comma-separated integer log value."""

    if not isinstance(value, str):
        raise TournamentError(f"{label} must be a comma-separated string")
    try:
        return [int(item) for item in value.split(",")]
    except ValueError as error:
        raise TournamentError(f"{label} contains a non-integer value") from error


def require_int(values: dict[str, Any], key: str, *, event: str) -> int:
    """Read one required integer field."""

    value = values.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TournamentError(f"{event}.{key} must be an integer")
    return value


def require_float(values: dict[str, Any], key: str, *, event: str) -> float:
    """Read one required numeric field as float."""

    value = values.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TournamentError(f"{event}.{key} must be numeric")
    return float(value)


def _mask_contract(workload: dict[str, Any]) -> dict[str, Any]:
    imaging = workload.get("imaging")
    comparison = workload.get("comparison")
    if not isinstance(imaging, dict) or not isinstance(comparison, dict):
        raise TournamentError("workload lacks imaging or comparison contract")
    expected_imaging = {
        "imsize": 4096,
        "spw": "2~17",
        "gridder": "awproject",
        "wprojplanes": 32,
        "weighting": "briggs",
        "deconvolver": "mtmfs",
        "nterms": 2,
        "scales": [0, 5, 12],
        "niter": 2000,
        "aterm": True,
        "psterm": False,
        "wbawp": True,
        "conjbeams": True,
        "usepointing": True,
    }
    mismatches = {
        key: {"expected": expected, "observed": imaging.get(key)}
        for key, expected in expected_imaging.items()
        if imaging.get(key) != expected
    }
    if mismatches:
        raise TournamentError(
            f"workload changes promoted imaging contract: {mismatches}"
        )
    regions = comparison.get("source_regions")
    if not isinstance(regions, list) or len(regions) != 1:
        raise TournamentError(
            "workload must bind exactly one deterministic source region"
        )
    region = regions[0]
    if not isinstance(region, dict):
        raise TournamentError("source region must be a JSON object")
    blc = region.get("blc")
    trc = region.get("trc")
    if (
        not isinstance(blc, list)
        or not isinstance(trc, list)
        or len(blc) != 2
        or len(trc) != 2
        or not all(isinstance(value, int) for value in [*blc, *trc])
    ):
        raise TournamentError("source region must have two-dimensional integer bounds")
    width = trc[0] - blc[0] + 1
    height = trc[1] - blc[1] + 1
    if width < 1 or height < 1:
        raise TournamentError("source region bounds are empty")
    return {
        "mask_image": imaging.get("mask_image"),
        "mask_sha256": imaging.get("mask_sha256"),
        "region_id": region.get("id"),
        "blc": blc,
        "trc": trc,
        "width": width,
        "height": height,
        "pixels": width * height,
        "image_pixels": imaging["imsize"] ** 2,
        "image_fraction": (width * height) / (imaging["imsize"] ** 2),
        "max_scale_pixels": max(imaging["scales"]),
    }


def _completion(text: str) -> dict[str, Any]:
    match = re.search(
        r"Wrote CASA-compatible products .+ "
        r"\((\d+) gridded samples, (\d+) major cycles, "
        r"(\d+) minor iterations, stop=Some\(([^)]+)\)\)",
        text,
    )
    if match is None:
        raise TournamentError("run log lacks a completed clean summary")
    wall_match = re.search(r"^real\s+([0-9.]+)$", text, flags=re.MULTILINE)
    if wall_match is None:
        raise TournamentError("run log lacks a wall-clock receipt")
    return {
        "samples": int(match.group(1)),
        "major_cycles": int(match.group(2)),
        "minor_iterations": int(match.group(3)),
        "stop_reason": match.group(4),
        "wall_seconds": float(wall_match.group(1)),
    }


def _operator_trajectory(
    events: dict[str, list[dict[str, Any]]],
    *,
    completion: dict[str, Any],
    mask_pixels: int,
) -> dict[str, Any]:
    ddid = require_events(events, "mfs_ddid_execution_plan")[-1]
    if int_csv(ddid.get("spws"), label="mfs_ddid_execution_plan.spws") != EXPECTED_SPWS:
        raise TournamentError("run log does not contain the full 16-SPW selection")
    initial_summaries = [
        event
        for event in require_events(events, "awproject_metal_grid_summary")
        if event.get("pass") == "initial_dirty"
    ]
    residual_summaries = [
        event
        for event in require_events(events, "awproject_metal_grid_summary")
        if event.get("pass") == "residual_refresh"
    ]
    if len(initial_summaries) != 1:
        raise TournamentError("run log must contain exactly one initial dirty summary")
    initial = initial_summaries[0]
    refreshes = len(residual_summaries)
    if refreshes < 1:
        raise TournamentError("run log has no residual refresh")
    residual_records = require_events(events, "mosaic_mtmfs_residual_refresh")
    if len(residual_records) != refreshes:
        raise TournamentError("residual refresh event counts disagree")
    minor_cycles = require_events(events, "mosaic_mtmfs_minor_cycle")
    profiles = require_events(events, "mtmfs_multiscale_minor_cycle_profile")
    if len(minor_cycles) != refreshes or len(profiles) != refreshes:
        raise TournamentError("minor-cycle and refresh counts disagree")
    updates = [
        require_int(event, "actual_updates", event="mosaic_mtmfs_minor_cycle")
        for event in minor_cycles
    ]
    if sum(updates) != completion["minor_iterations"]:
        raise TournamentError("minor-cycle updates do not match completion summary")
    candidate_positions = [event.get("candidate_positions") for event in profiles]
    if any(
        not isinstance(positions, list)
        or len(positions) != 3
        or not all(isinstance(value, int) for value in positions)
        for positions in candidate_positions
    ):
        raise TournamentError("minor-cycle profiles lack three scale candidate counts")
    if any(positions != candidate_positions[0] for positions in candidate_positions):
        raise TournamentError("minor-cycle candidate topology changed across cycles")
    if candidate_positions[0][0] != mask_pixels:
        raise TournamentError(
            "point-scale candidates do not match deterministic mask cardinality"
        )

    initial_calls = require_int(initial, "calls", event="initial grid summary")
    residual_calls = [
        require_int(event, "calls", event="residual grid summary")
        for event in residual_summaries
    ]
    if any(calls != initial_calls for calls in residual_calls):
        raise TournamentError("replay block count changed between operator passes")
    replay_lines = require_events(events, "awproject_metal_resident_tile_chain")
    expected_replay_lines = refreshes * initial_calls
    if len(replay_lines) != expected_replay_lines:
        raise TournamentError(
            "residual replay line count does not match refreshes times replay blocks"
        )
    samples = completion["samples"]
    if require_int(initial, "samples", event="initial grid summary") != samples:
        raise TournamentError("initial summary sample count disagrees with completion")
    if any(
        require_int(event, "samples", event="residual grid summary") != samples
        for event in residual_summaries
    ):
        raise TournamentError("residual summary sample count changed")

    initial_kernel_values = require_int(
        initial,
        "kernel_values",
        event="initial grid summary",
    )
    prediction_kernel_values = sum(
        require_int(event, "prediction_kernel_values", event="residual replay")
        for event in replay_lines
    )
    imaging_kernel_values = sum(
        require_int(event, "imaging_kernel_values", event="residual replay")
        for event in replay_lines
    )
    residual_kernel_values = sum(
        require_int(event, "kernel_values", event="residual grid summary")
        for event in residual_summaries
    )
    if prediction_kernel_values + imaging_kernel_values != residual_kernel_values:
        raise TournamentError("residual replay and grid-summary kernel counts disagree")
    total_kernel_values = initial_kernel_values + residual_kernel_values

    cache_events = require_events(events, "awproject_compact_replay_cache")
    final_cache = max(
        cache_events,
        key=lambda event: require_int(
            event,
            "resident_blocks",
            event="replay cache",
        ),
    )
    resident_blocks = require_int(final_cache, "resident_blocks", event="replay cache")
    if resident_blocks != 16:
        raise TournamentError("promoted run did not retain all 16 replay blocks")
    compact_events = require_events(events, "awproject_compact_source_order")
    if len(compact_events) != resident_blocks:
        raise TournamentError("source-order materialization count is not 16 blocks")
    materialize_ms = sum(
        require_float(event, "materialize_ms", event="compact source order")
        for event in compact_events
    )
    packed_sample_bytes = {
        require_int(event, "packed_sample_bytes", event="compact source order")
        for event in compact_events
    }
    if len(packed_sample_bytes) != 1:
        raise TournamentError("packed sample size changed across replay blocks")

    return {
        "rows": require_int(ddid, "rows", event="mfs DDID plan"),
        "selected_channel_visits": require_int(
            ddid,
            "selected_channel_visits",
            event="mfs DDID plan",
        ),
        "samples": samples,
        "minor_updates_by_cycle": updates,
        "minor_iterations": completion["minor_iterations"],
        "reported_major_cycles": completion["major_cycles"],
        "residual_refreshes": refreshes,
        "logical_expensive_operator_calls": 1 + (2 * refreshes),
        "operator_call_formula": "one initial adjoint + refreshes * (prediction + adjoint)",
        "replay_blocks_per_operator": initial_calls,
        "initial_adjoint_kernel_interactions": initial_kernel_values,
        "residual_prediction_kernel_interactions": prediction_kernel_values,
        "residual_adjoint_kernel_interactions": imaging_kernel_values,
        "residual_kernel_interactions": residual_kernel_values,
        "total_kernel_interactions": total_kernel_values,
        "per_refresh_prediction_kernel_interactions": (
            prediction_kernel_values // refreshes
        ),
        "per_refresh_adjoint_kernel_interactions": (imaging_kernel_values // refreshes),
        "replay_program_resident_bytes": require_int(
            final_cache,
            "resident_bytes",
            event="replay cache",
        ),
        "replay_materialization_ms": materialize_ms,
        "packed_sample_bytes": packed_sample_bytes.pop(),
        "candidate_positions_by_scale": candidate_positions[0],
    }


def _timing_and_memory(
    events: dict[str, list[dict[str, Any]]],
    *,
    completion: dict[str, Any],
) -> dict[str, Any]:
    summaries = [
        event
        for event in require_events(events, "core")
        if event.get("stage") == "run_summary"
    ]
    if len(summaries) != 1:
        raise TournamentError("run log must contain exactly one core run summary")
    summary = summaries[0]
    stage_keys = (
        "controller_overhead_ms",
        "weighting_ms",
        "psf_grid_ms",
        "psf_fft_ms",
        "psf_normalize_ms",
        "model_fft_ms",
        "residual_degrid_grid_ms",
        "residual_fft_ms",
        "minor_cycle_ms",
        "major_cycle_refresh_ms",
        "restore_ms",
        "total_ms",
    )
    stages = {
        key.removesuffix("_ms"): require_float(summary, key, event="core run summary")
        for key in stage_keys
    }
    memory_events = require_events(events, "standard_mfs_stage_memory")
    peak_rss_bytes = max(
        require_int(event, "lifetime_peak_rss_bytes", event="stage memory")
        for event in memory_events
    )
    peak_footprint_bytes = max(
        require_int(
            event,
            "stage_observed_peak_process_physical_footprint_bytes",
            event="stage memory",
        )
        for event in memory_events
    )
    peak_metal_bytes = max(
        require_int(
            event,
            "stage_observed_peak_metal_allocated_bytes",
            event="stage memory",
        )
        for event in memory_events
    )
    wall_ms = completion["wall_seconds"] * 1000.0
    incumbent_operator_stage_ms = stages["psf_grid"] + stages["major_cycle_refresh"]
    removable_fraction = incumbent_operator_stage_ms / wall_ms
    return {
        "wall_seconds": completion["wall_seconds"],
        "core_stage_ms": stages,
        "operator_dominated_stage_ms": incumbent_operator_stage_ms,
        "operator_dominated_wall_fraction": removable_fraction,
        "operator_only_amdahl_ceiling": (
            1.0 / (1.0 - removable_fraction) if removable_fraction < 1.0 else None
        ),
        "memory": {
            "peak_rss_bytes": peak_rss_bytes,
            "peak_process_physical_footprint_bytes": peak_footprint_bytes,
            "peak_metal_allocated_bytes": peak_metal_bytes,
        },
    }


def _candidate_cards(
    trajectory: dict[str, Any],
    *,
    mask: dict[str, Any],
    niter_cap: int,
) -> list[dict[str, Any]]:
    samples = trajectory["samples"]
    iterations = trajectory["minor_iterations"]
    refreshes = trajectory["residual_refreshes"]
    current_work = trajectory["total_kernel_interactions"]
    per_refresh_adjoint = trajectory["per_refresh_adjoint_kernel_interactions"]
    direct_actual = samples * iterations
    direct_cap = samples * niter_cap
    naive_masked_dft = samples * mask["pixels"] * refreshes
    calls_after = 3
    calls_before = trajectory["logical_expensive_operator_calls"]
    optimistic_work = (
        trajectory["initial_adjoint_kernel_interactions"]
        + direct_actual
        + per_refresh_adjoint
    )
    optimistic_ratio = optimistic_work / current_work
    second_adjoint_work = optimistic_work + per_refresh_adjoint
    state_limit = min(
        trajectory["replay_program_resident_bytes"] // 10,
        2 * samples * trajectory["packed_sample_bytes"],
    )
    idg = []
    for side in SUBGRID_SIDES:
        per_operator = samples * side**2
        standalone = per_operator * calls_before
        idg.append(
            {
                "side": side,
                "interactions_per_operator": per_operator,
                "standalone_clean_interactions": standalone,
                "standalone_work_ratio": standalone / current_work,
            }
        )
    combined_idg_work = direct_actual + (2 * idg[0]["interactions_per_operator"])
    return [
        {
            "id": "visibility-resident-mask-local",
            "family": "visibility-resident components plus restricted adjoint",
            "status": "eligible-for-executable-discriminator",
            "work_proxy": {
                "direct_component_pairs_actual_trajectory": direct_actual,
                "direct_component_pairs_niter_cap": direct_cap,
                "naive_masked_dft_pairs": naive_masked_dft,
                "optimistic_interactions": optimistic_work,
                "optimistic_work_ratio": optimistic_ratio,
                "one_extra_adjoint_interactions": second_adjoint_work,
                "one_extra_adjoint_work_ratio": second_adjoint_work / current_work,
                "expensive_operator_calls_before": calls_before,
                "expensive_operator_calls_after": calls_after,
                "operator_calls_removed_fraction": 1.0 - (calls_after / calls_before),
            },
            "state": {
                "target_persistent_bytes_max": state_limit,
                "ten_percent_incumbent_replay_bytes": (
                    trajectory["replay_program_resident_bytes"] // 10
                ),
                "twice_retained_visibility_bytes": (
                    2 * samples * trajectory["packed_sample_bytes"]
                ),
            },
            "gates": {
                "operator_calls_removed_at_least_half": (
                    calls_after <= calls_before / 2
                ),
                "projected_operator_work_at_most_0_35x": optimistic_ratio <= 0.35,
                "projected_operator_work_below_0_70x_abort": optimistic_ratio <= 0.70,
                "second_adjoint_still_at_most_0_35x": (
                    second_adjoint_work / current_work <= 0.35
                ),
                "science_floor": "not-measured",
                "end_to_end_speedup_at_least_1_5x": "not-measured",
                "full_size_liveness": "not-measured",
            },
            "decision": (
                "build an exact, tunable AW/WB/POINTING-aware direct component "
                "predictor while retaining visibility residuals; permit only one "
                "mandatory final adjoint in the first discriminator"
            ),
            "rejected_variant": (
                "direct DFT over every mask pixel for every residual refresh"
            ),
        },
        {
            "id": "idg-image-domain-subgrid",
            "family": "image-domain gridding with compact subgrids",
            "status": "retain-only-as-combined-route",
            "side_sweep": idg,
            "combined_with_direct_components": {
                "side": SUBGRID_SIDES[0],
                "interactions": combined_idg_work,
                "work_ratio": combined_idg_work / current_work,
                "formula": "actual component pairs + one initial and one final subgrid adjoint",
            },
            "gates": {
                "standalone_best_below_0_80x": (
                    idg[0]["standalone_work_ratio"] <= 0.80
                ),
                "science_floor": "not-measured",
                "operator_speedup_at_least_2x": "not-measured",
                "bounded_worker_scratch": "not-measured",
            },
            "decision": (
                "reject standalone IDG on interaction count; retain a coherent "
                "L=32 discriminator only if combined with operator-call elimination"
            ),
        },
        {
            "id": "low-rank-a-pointing-wgridder",
            "family": "factored A/WB basis plus W gridder",
            "status": "rank-audit-required",
            "missing_measurement": (
                "numerical A/WB rank after factoring exact W and POINTING phase"
            ),
            "abort_gate": "projected work above 0.70x or rank scales with field count",
        },
        {
            "id": "residual-w-stacking",
            "family": "W stacking with residual-W correction",
            "status": "cost-curve-required",
            "missing_measurement": "measured stack-count cost curve for 1 through 32",
            "abort_gate": "projected work above 0.70x incumbent",
        },
        {
            "id": "separable-tensor-cf",
            "family": "separable or tensor-factored convolution functions",
            "status": "rank-and-action-audit-required",
            "missing_measurement": (
                "rank, approximation error, and cost without reconstructing sampled patches"
            ),
            "abort_gate": "projected work above 0.70x or frozen science-floor failure",
        },
    ]


def analyze_frozen_run(
    workload: dict[str, Any],
    log_text: str,
) -> dict[str, Any]:
    """Analyze already-validated frozen workload and log content."""

    if workload.get("id") != memory_campaign.PROMOTED_4096_WORKLOAD_IDS["single-field"]:
        raise TournamentError("workload is not the promoted single-field 4096 row")
    mask = _mask_contract(workload)
    completion = _completion(log_text)
    events = parse_events(log_text)
    trajectory = _operator_trajectory(
        events,
        completion=completion,
        mask_pixels=mask["pixels"],
    )
    timing = _timing_and_memory(events, completion=completion)
    imaging = workload["imaging"]
    return {
        "workload": {
            "id": workload["id"],
            "shape": [imaging["imsize"], imaging["imsize"]],
            "spws": EXPECTED_SPWS,
            "wplanes": imaging["wprojplanes"],
            "gridder": imaging["gridder"],
            "weighting": imaging["weighting"],
            "deconvolver": imaging["deconvolver"],
            "nterms": imaging["nterms"],
            "scales": imaging["scales"],
            "niter": imaging["niter"],
            "aw_terms": {
                "aterm": imaging["aterm"],
                "psterm": imaging["psterm"],
                "wbawp": imaging["wbawp"],
                "conjbeams": imaging["conjbeams"],
                "usepointing": imaging["usepointing"],
            },
            "mask": mask,
        },
        "incumbent": {
            "completion": completion,
            "operator_trajectory": trajectory,
            "timing": timing,
        },
        "candidate_cards": _candidate_cards(
            trajectory,
            mask=mask,
            niter_cap=imaging["niter"],
        ),
        "selection": {
            "first_executable_discriminator": "visibility-resident-mask-local",
            "required_semantics": (
                "exact or tunably bounded AW/WB/conjugate-beam/POINTING response "
                "with MT-MFS component moments and resident visibility residuals"
            ),
            "first_discriminator_operator_budget": (
                "one initial adjoint, direct component prediction, one final adjoint"
            ),
            "reason": (
                "it is the only candidate whose measured work proxy already clears "
                "the 0.35x promotion threshold; a second final/intermediate adjoint "
                "does not"
            ),
            "claim_boundary": (
                "projection only; no candidate speedup or scientific promotion claimed"
            ),
        },
    }


def _resolve_receipt_input(
    receipt_path: Path,
    receipt: dict[str, Any],
    name: str,
) -> Path:
    inputs = receipt.get("input")
    if not isinstance(inputs, dict) or not isinstance(inputs.get(name), dict):
        raise TournamentError(f"promotion receipt lacks input.{name}")
    reference = inputs[name]
    raw_path = reference.get("path")
    expected_sha256 = reference.get("sha256")
    if not isinstance(raw_path, str) or not isinstance(expected_sha256, str):
        raise TournamentError(f"promotion receipt input.{name} is malformed")
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = receipt_path.parent / path
    path = path.resolve()
    if sha256_file(path) != expected_sha256:
        raise TournamentError(f"promotion receipt input.{name} hash changed")
    return path


def build_audit(
    promotion_receipt_path: Path,
    workload_path: Path,
) -> dict[str, Any]:
    """Validate content-addressed evidence and build one tournament audit."""

    promotion_receipt_path = promotion_receipt_path.expanduser().resolve()
    workload_path = workload_path.expanduser().resolve()
    try:
        promotion_ref = memory_campaign.validate_promoted_4096_receipt(
            promotion_receipt_path,
            workload_kind="single-field",
        )
    except memory_campaign.CampaignError as error:
        raise TournamentError(str(error)) from error
    receipt = load_json(promotion_receipt_path, label="promotion receipt")
    workload = load_json(workload_path, label="workload")
    run_log_path = _resolve_receipt_input(
        promotion_receipt_path,
        receipt,
        "run_log",
    )
    analysis = analyze_frozen_run(
        workload,
        run_log_path.read_text(encoding="utf-8"),
    )
    return {
        "schema_version": 1,
        "kind": "vlass_architecture_tournament_audit",
        "status": "measured-from-frozen-evidence",
        "created_at": utc_now(),
        "role": {
            "runs_casa": False,
            "runs_imaging": False,
            "development_evidence_only": True,
            "speedup_claim": False,
            "scientific_promotion_claim": False,
        },
        "inputs": {
            "audit_source": {
                "path": str(Path(__file__).resolve()),
                "sha256": sha256_file(Path(__file__).resolve()),
            },
            "promotion_receipt": {
                "path": promotion_ref.path,
                "sha256": promotion_ref.sha256,
            },
            "workload": {
                "path": str(workload_path),
                "sha256": sha256_file(workload_path),
            },
            "run_log": {
                "path": str(run_log_path),
                "sha256": sha256_file(run_log_path),
            },
        },
        **analysis,
    }


def write_new_json(path: Path, value: dict[str, Any]) -> None:
    """Create a receipt without replacing prior evidence."""

    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(value, indent=2, sort_keys=True) + "\n"
    try:
        with path.open("x", encoding="utf-8") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as error:
        raise TournamentError(
            f"refusing to replace existing receipt: {path}"
        ) from error


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--promotion-receipt", required=True, type=Path)
    parser.add_argument("--workload", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the frozen-evidence architecture audit."""

    args = parse_args(argv)
    try:
        audit = build_audit(args.promotion_receipt, args.workload)
        if args.output is None:
            print(json.dumps(audit, indent=2, sort_keys=True))
        else:
            write_new_json(args.output, audit)
            print(
                json.dumps(
                    {
                        "status": audit["status"],
                        "output": str(args.output.expanduser().resolve()),
                        "sha256": sha256_file(args.output.expanduser().resolve()),
                        "first_executable_discriminator": audit["selection"][
                            "first_executable_discriminator"
                        ],
                    },
                    sort_keys=True,
                )
            )
    except (OSError, TournamentError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
