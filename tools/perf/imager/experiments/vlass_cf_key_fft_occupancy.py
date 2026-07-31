#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Reduce the exact-CF-key grouped-FFT occupancy audit.

The audit counts real sparse impulse tiles after factoring POINTING phase into
their values. Its FFT-unit formula ranks tile sizes but cannot price an FFT
against sampled-CF updates; a surviving candidate still needs a matched Metal
race.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
from typing import Any


AUDIT_EVENT = "awproject_cf_key_fft_occupancy"
EXPECTED_TILE_SIDES = [32, 64, 128, 256]
DEFAULT_EXPECTED_SAMPLES = 385_862
DEFAULT_EXPECTED_REFERENCES = 21_608_272
DEFAULT_AW_BUDGET_SECONDS = 5.6479536
DEFAULT_MEMORY_LIMIT_BYTES = 12 * 1024**3


class CfKeyFftOccupancyError(RuntimeError):
    """Raised when an occupancy log is incomplete or inconsistent."""


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
        raise CfKeyFftOccupancyError(f"{AUDIT_EVENT} lacks {key}")
    return match.group(1)


def _required_int(line: str, key: str) -> int:
    return int(_required(line, key, r"[0-9]+"))


def _required_float(line: str, key: str) -> float:
    return float(_required(line, key, r"[0-9]+(?:\.[0-9]+)?"))


def _parse_event(line: str) -> dict[str, Any]:
    event: dict[str, Any] = {
        key: _required_int(line, key)
        for key in (
            "tile_side",
            "samples",
            "windows",
            "logical_calls",
            "logical_plan_references",
            "exact_kernel_keys",
            "logical_rhs_buckets",
            "logical_active_tiles",
            "padded_fft_cells",
            "transform_complex_units",
            "kernel_fft_units",
            "total_complex_units",
            "persistent_kernel_spectrum_bytes",
            "peak_tile_scratch_bytes",
        )
    }
    event["references_per_active_tile"] = _required_float(
        line,
        "references_per_active_tile",
    )
    if event["logical_calls"] != 11:
        raise CfKeyFftOccupancyError("occupancy event must preserve 11 calls")
    if min(
        event["exact_kernel_keys"],
        event["logical_rhs_buckets"],
        event["logical_active_tiles"],
        event["total_complex_units"],
    ) <= 0:
        raise CfKeyFftOccupancyError("occupancy event contains an empty route")
    return event


def analyze_log(
    text: str,
    *,
    expected_samples: int = DEFAULT_EXPECTED_SAMPLES,
    expected_references: int = DEFAULT_EXPECTED_REFERENCES,
    aw_budget_seconds: float = DEFAULT_AW_BUDGET_SECONDS,
    memory_limit_bytes: int = DEFAULT_MEMORY_LIMIT_BYTES,
) -> dict[str, Any]:
    """Rank the tile formulas without pretending they are performance data."""

    events = [
        _parse_event(line)
        for line in text.splitlines()
        if line.startswith(f"{AUDIT_EVENT} ")
    ]
    if not events:
        raise CfKeyFftOccupancyError(f"log contains no {AUDIT_EVENT} events")
    events.sort(key=lambda event: event["tile_side"])
    sides = [event["tile_side"] for event in events]
    if sides != EXPECTED_TILE_SIDES:
        raise CfKeyFftOccupancyError(
            f"tile sides must be {EXPECTED_TILE_SIDES}, got {sides}"
        )
    for event in events:
        if event["samples"] != expected_samples:
            raise CfKeyFftOccupancyError(
                f"tile {event['tile_side']} has {event['samples']} samples, "
                f"expected {expected_samples}"
            )
        if event["logical_plan_references"] != expected_references:
            raise CfKeyFftOccupancyError(
                f"tile {event['tile_side']} changed the reference contract"
            )

    best = min(events, key=lambda event: event["total_complex_units"])
    working_bytes = (
        best["persistent_kernel_spectrum_bytes"] + best["peak_tile_scratch_bytes"]
    )
    memory_gate_passes = working_bytes <= memory_limit_bytes
    if not memory_gate_passes:
        decision = "retire-persistent-exact-key-spectra"
    elif best["references_per_active_tile"] < 2.0:
        decision = "retire-exact-key-overlap-save-no-spatial-reuse"
    else:
        decision = "promote-matched-metal-fft-race"
    curve = []
    for event in events:
        curve.append(
            {
                **event,
                "required_complex_unit_rate_gs": (
                    event["total_complex_units"] / aw_budget_seconds / 1.0e9
                ),
                "formula_ratio_to_best": (
                    event["total_complex_units"] / best["total_complex_units"]
                ),
            }
        )
    return {
        "role": "occupancy-and-formula-audit-not-performance-or-science-evidence",
        "evidence_class": "measured-occupancy-projected-work",
        "contract": {
            "imsize": 4096,
            "spws": "2~17",
            "gridder": "awproject",
            "wprojplanes": 32,
            "nterms": 2,
            "logical_operator_calls": 11,
            "pointing": "exact group phase carried by sparse impulse values",
            "kernel_key": (
                "exact CF cell, imaging/weight kind, oversampling offsets, "
                "and conjugation"
            ),
            "work_formula": (
                "overlap-save forward plus inverse FFT, one pointwise multiply, "
                "and one persistent kernel FFT"
            ),
        },
        "coverage": {
            "samples": expected_samples,
            "windows": events[0]["windows"],
            "plan_references": expected_references,
            "exact_kernel_keys": events[0]["exact_kernel_keys"],
        },
        "tile_curve": curve,
        "selection": {
            "tile_side": best["tile_side"],
            "total_complex_units": best["total_complex_units"],
            "references_per_active_tile": best["references_per_active_tile"],
            "required_complex_unit_rate_gs": (
                best["total_complex_units"] / aw_budget_seconds / 1.0e9
            ),
            "working_bytes": working_bytes,
            "memory_limit_bytes": memory_limit_bytes,
            "memory_gate_passes": memory_gate_passes,
            "decision": decision,
        },
        "claim_boundary": (
            "The formula may select or reject occupancy, but cannot compare FFT "
            "units with irregular sampled-CF updates. A promoted route requires "
            "an exact-stream target-hardware race before implementation."
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
        "--aw-budget-seconds",
        type=float,
        default=DEFAULT_AW_BUDGET_SECONDS,
    )
    parser.add_argument(
        "--memory-limit-bytes",
        type=int,
        default=DEFAULT_MEMORY_LIMIT_BYTES,
    )
    return parser.parse_args()


def main() -> int:
    """Write one immutable occupancy receipt."""

    args = parse_args()
    result = analyze_log(
        args.log.read_text(encoding="utf-8"),
        expected_samples=args.expected_samples,
        expected_references=args.expected_references,
        aw_budget_seconds=args.aw_budget_seconds,
        memory_limit_bytes=args.memory_limit_bytes,
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
