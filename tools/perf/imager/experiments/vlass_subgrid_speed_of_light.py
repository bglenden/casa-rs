#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Reduce the exact-plan VLASS subgrid speed-of-light race.

This tool parses an already completed casa-rs diagnostic log. It runs neither
CASA nor imaging. The resulting receipt promotes or retires the direct-subgrid
architecture from measured target-hardware device time, with 40% of the AW
budget reserved for the transforms and construction omitted by the race.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
from typing import Any


AUDIT_EVENT = "awproject_subgrid_speed_of_light"
DEFAULT_EXPECTED_SAMPLES = 385_862
DEFAULT_EXPECTED_REFERENCES = 21_608_272
DEFAULT_EXPECTED_CELL_UPDATES = 26_866_182_144
DEFAULT_AW_BUDGET_SECONDS = 5.6479536
DEFAULT_FULL_GEOMETRY_MEMORY_LIMIT_BYTES = 12 * 1024**3


class SubgridSpeedOfLightError(RuntimeError):
    """Raised when a log cannot support the architecture decision."""


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


def _required(line: str, key: str, pattern: str) -> str:
    match = re.search(rf"(?:^|\s){re.escape(key)}=({pattern})(?:\s|$)", line)
    if match is None:
        raise SubgridSpeedOfLightError(f"{AUDIT_EVENT} lacks {key}")
    return match.group(1)


def _required_int(line: str, key: str) -> int:
    return int(_required(line, key, r"[0-9]+"))


def _required_float(line: str, key: str) -> float:
    return float(_required(line, key, r"[0-9]+(?:\.[0-9]+)?"))


def _parse_event(line: str) -> dict[str, Any]:
    event: dict[str, Any] = {
        key: _required_int(line, key)
        for key in (
            "block",
            "window",
            "samples",
            "plans",
            "stored_plan_references",
            "logical_plan_references",
            "stored_stripes",
            "logical_groups",
            "logical_stripes",
            "logical_dispatches",
            "logical_cell_updates",
            "screen_values",
            "output_cells",
            "screen_bytes",
            "row_phase_bytes",
            "trace_bytes",
            "output_bytes",
            "resident_bytes",
            "projected_full_geometry_peak_bytes",
        )
    }
    event.update(
        {
            key: _required_float(line, key)
            for key in (
                "build_ms",
                "executor_setup_ms",
                "buffer_alloc_ms",
                "warm_wall_ms",
                "warm_device_ms",
                "core_wall_ms",
                "core_device_ms",
                "readback_ms",
                "cell_gs",
            )
        }
    )
    event["output_sha256"] = _required(line, "output_sha256", r"[0-9a-f]{64}")
    if event["logical_dispatches"] != 11:
        raise SubgridSpeedOfLightError(
            f"event has {event['logical_dispatches']} calls, expected 11"
        )
    if event["logical_plan_references"] != event["samples"] * 56:
        raise SubgridSpeedOfLightError(
            "event does not preserve the exact 56-reference per-sample trajectory"
        )
    if event["stored_plan_references"] != event["samples"] * 24:
        raise SubgridSpeedOfLightError(
            "event does not preserve the three stored 24-reference call templates"
        )
    if min(
        event["plans"],
        event["stored_stripes"],
        event["logical_groups"],
        event["logical_stripes"],
        event["logical_cell_updates"],
        event["screen_values"],
        event["output_cells"],
        event["resident_bytes"],
    ) <= 0:
        raise SubgridSpeedOfLightError("event contains an empty physical work stream")
    if event["core_wall_ms"] <= 0:
        raise SubgridSpeedOfLightError("event core wall time must be positive")
    return event


def analyze_log(
    text: str,
    *,
    expected_samples: int = DEFAULT_EXPECTED_SAMPLES,
    expected_references: int = DEFAULT_EXPECTED_REFERENCES,
    expected_cell_updates: int = DEFAULT_EXPECTED_CELL_UPDATES,
    aw_budget_seconds: float = DEFAULT_AW_BUDGET_SECONDS,
    full_geometry_memory_limit_bytes: int = (
        DEFAULT_FULL_GEOMETRY_MEMORY_LIMIT_BYTES
    ),
) -> dict[str, Any]:
    """Aggregate the exact real-plan trace and apply its predeclared gates."""

    if aw_budget_seconds <= 0:
        raise SubgridSpeedOfLightError("AW budget must be positive")
    events = [
        _parse_event(line)
        for line in text.splitlines()
        if line.startswith(f"{AUDIT_EVENT} ")
    ]
    if not events:
        raise SubgridSpeedOfLightError(f"log contains no {AUDIT_EVENT} events")
    window_ids = [(event["block"], event["window"]) for event in events]
    if len(set(window_ids)) != len(window_ids):
        raise SubgridSpeedOfLightError("trace contains duplicate block/window IDs")

    samples = sum(event["samples"] for event in events)
    references = sum(event["logical_plan_references"] for event in events)
    cell_updates = sum(event["logical_cell_updates"] for event in events)
    if samples != expected_samples:
        raise SubgridSpeedOfLightError(
            f"trace has {samples} samples, expected {expected_samples}"
        )
    if references != expected_references:
        raise SubgridSpeedOfLightError(
            f"trace has {references} references, expected {expected_references}"
        )
    if cell_updates != expected_cell_updates:
        raise SubgridSpeedOfLightError(
            f"trace has {cell_updates} cell updates, expected {expected_cell_updates}"
        )

    device_receipt_complete = all(event["core_device_ms"] > 0 for event in events)
    core_seconds = (
        sum(event["core_device_ms"] for event in events) / 1000
        if device_receipt_complete
        else sum(event["core_wall_ms"] for event in events) / 1000
    )
    if core_seconds <= 0:
        raise SubgridSpeedOfLightError("aggregate core time must be positive")
    cell_rate_gs = cell_updates / core_seconds / 1.0e9
    projected_peak = max(
        event["projected_full_geometry_peak_bytes"] for event in events
    )
    memory_gate_passes = projected_peak <= full_geometry_memory_limit_bytes
    green_seconds = aw_budget_seconds * 0.50
    conditional_seconds = aw_budget_seconds * 0.60
    if core_seconds <= green_seconds and memory_gate_passes:
        decision = "promote-full-inverse-cf-race"
    elif core_seconds <= conditional_seconds and memory_gate_passes:
        decision = "conditional-measure-omitted-floors-before-promotion"
    else:
        decision = "retire-direct-subgrid-family"

    return {
        "role": "physical-lower-bound-race-not-production-performance-or-science-evidence",
        "evidence_class": "measured",
        "contract": {
            "imsize": 4096,
            "spws": "2~17",
            "gridder": "awproject",
            "wprojplanes": 32,
            "nterms": 2,
            "logical_operator_calls": 11,
            "major_cycles": 5,
            "operator": (
                "exact real plan stream; mixed support; content-derived dense screens; "
                "X phase recurrence; deterministic threadgroup-owner writes"
            ),
            "omitted": [
                "screen construction and inverse FFT",
                "subgrid FFT and scatter",
                "CLEAN controller and products",
                "CASA numerical equivalence",
            ],
        },
        "coverage": {
            "windows": len(events),
            "samples": samples,
            "plans": sum(event["plans"] for event in events),
            "plan_references": references,
            "cell_updates": cell_updates,
            "logical_groups": sum(event["logical_groups"] for event in events),
            "logical_stripes": sum(event["logical_stripes"] for event in events),
        },
        "timing": {
            "source": (
                "metal-device-time"
                if device_receipt_complete
                else "wall-fallback-device-timestamp-missing"
            ),
            "core_seconds": core_seconds,
            "cell_rate_gs": cell_rate_gs,
            "warm_device_seconds": (
                sum(event["warm_device_ms"] for event in events) / 1000
            ),
            "warm_wall_seconds": sum(event["warm_wall_ms"] for event in events)
            / 1000,
            "core_wall_seconds": sum(event["core_wall_ms"] for event in events)
            / 1000,
            "build_seconds": sum(event["build_ms"] for event in events) / 1000,
            "buffer_alloc_seconds": sum(
                event["buffer_alloc_ms"] for event in events
            )
            / 1000,
            "readback_seconds": sum(event["readback_ms"] for event in events)
            / 1000,
        },
        "memory": {
            "peak_window_resident_bytes": max(
                event["resident_bytes"] for event in events
            ),
            "projected_full_geometry_peak_bytes": projected_peak,
            "full_geometry_limit_bytes": full_geometry_memory_limit_bytes,
            "gate_passes": memory_gate_passes,
            "projection": (
                "nine times each current-window resident footprint; conservative "
                "12150-over-4096 area bound"
            ),
        },
        "gates": {
            "aw_budget_seconds": aw_budget_seconds,
            "green_seconds": green_seconds,
            "conditional_seconds": conditional_seconds,
            "required_green_cell_rate_gs": cell_updates
            / green_seconds
            / 1.0e9,
            "required_conditional_cell_rate_gs": cell_updates
            / conditional_seconds
            / 1.0e9,
            "omitted_work_headroom_fraction": 0.40,
            "decision": decision,
        },
        "integrity": {
            "output_sha256_by_window": [
                {
                    "block": event["block"],
                    "window": event["window"],
                    "sha256": event["output_sha256"],
                }
                for event in events
            ],
            "all_outputs_nonzero_hash": all(
                event["output_sha256"] != "0" * 64 for event in events
            ),
        },
        "claim_boundary": (
            "This times only the core dense-screen accumulation lower bound. "
            "Promotion authorizes a full inverse-CF experiment, not production use."
        ),
    }


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-samples", type=int, default=DEFAULT_EXPECTED_SAMPLES)
    parser.add_argument(
        "--expected-references",
        type=int,
        default=DEFAULT_EXPECTED_REFERENCES,
    )
    parser.add_argument(
        "--expected-cell-updates",
        type=int,
        default=DEFAULT_EXPECTED_CELL_UPDATES,
    )
    parser.add_argument(
        "--aw-budget-seconds",
        type=float,
        default=DEFAULT_AW_BUDGET_SECONDS,
    )
    parser.add_argument(
        "--full-geometry-memory-limit-bytes",
        type=int,
        default=DEFAULT_FULL_GEOMETRY_MEMORY_LIMIT_BYTES,
    )
    return parser.parse_args()


def main() -> int:
    """Write one immutable architecture-race receipt."""

    args = parse_args()
    result = analyze_log(
        args.log.read_text(encoding="utf-8"),
        expected_samples=args.expected_samples,
        expected_references=args.expected_references,
        expected_cell_updates=args.expected_cell_updates,
        aw_budget_seconds=args.aw_budget_seconds,
        full_geometry_memory_limit_bytes=args.full_geometry_memory_limit_bytes,
    )
    result["generated_at"] = utc_now()
    result["inputs"] = {
        "log": str(args.log.resolve()),
        "log_sha256": sha256_file(args.log),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
