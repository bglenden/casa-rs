#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Reduce the VLASS hybrid W/A diagnostic to a content-bound cost receipt.

This tool parses an already completed casa-rs diagnostic log. It does not run
CASA, casa-rs, or read a MeasurementSet. The support counts are an arithmetic
proxy, not correctness, timing, or promotion evidence.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
from typing import Any


AUDIT_EVENT = "awproject_hybrid_support_audit"
SUMMARY_EVENT = "awproject_metal_grid_summary"
EXPECTED_STACK_COUNTS = list(range(1, 33))


class HybridAuditError(RuntimeError):
    """Raised when a diagnostic log cannot support the cost audit."""


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


def _required_int(line: str, key: str) -> int:
    match = re.search(rf"(?:^|\s){re.escape(key)}=([0-9]+)(?:\s|$)", line)
    if match is None:
        raise HybridAuditError(f"{AUDIT_EVENT} lacks integer {key}")
    return int(match.group(1))


def _required_float(line: str, key: str) -> float:
    match = re.search(
        rf"(?:^|\s){re.escape(key)}=([-+0-9.eE]+)(?:\s|$)",
        line,
    )
    if match is None:
        raise HybridAuditError(f"{AUDIT_EVENT} lacks numeric {key}")
    return float(match.group(1))


def _required_int_csv(line: str, key: str) -> list[int]:
    match = re.search(rf"(?:^|\s){re.escape(key)}=([0-9,]+)(?:\s|$)", line)
    if match is None:
        raise HybridAuditError(f"{AUDIT_EVENT} lacks integer vector {key}")
    return [int(value) for value in match.group(1).split(",")]


def _parse_audit_event(line: str) -> dict[str, Any]:
    stack_count_match = re.search(r"(?:^|\s)stack_counts=1-([0-9]+)(?:\s|$)", line)
    if stack_count_match is None:
        raise HybridAuditError(f"{AUDIT_EVENT} lacks stack_counts")
    stack_count = int(stack_count_match.group(1))
    weighted_taps = _required_int_csv(line, "stack_weighted_taps")
    if stack_count != len(EXPECTED_STACK_COUNTS) or len(weighted_taps) != stack_count:
        raise HybridAuditError(
            f"{AUDIT_EVENT} must contain exactly {len(EXPECTED_STACK_COUNTS)} stacks"
        )
    return {
        "block": _required_int(line, "block"),
        "window": _required_int(line, "window"),
        "samples": _required_int(line, "samples"),
        "plan_references": _required_int(line, "plan_references"),
        "represented_w_extent": _required_float(line, "represented_w_extent"),
        "w_increment": _required_float(line, "w_increment"),
        "current_weighted_taps": _required_int(line, "current_weighted_taps"),
        "a_only_weighted_taps": _required_int(line, "a_only_weighted_taps"),
        "stack_weighted_taps": weighted_taps,
    }


def analyze_log(
    text: str,
    *,
    expected_samples: int = 385_862,
    maximum_support_ratio: float = 0.5,
) -> dict[str, Any]:
    """Aggregate one full-16-SPW support diagnostic."""

    events = [
        _parse_audit_event(line)
        for line in text.splitlines()
        if line.startswith(f"{AUDIT_EVENT} ")
    ]
    if not events:
        raise HybridAuditError(f"log contains no {AUDIT_EVENT} events")
    window_ids = [(event["block"], event["window"]) for event in events]
    if len(set(window_ids)) != len(window_ids):
        raise HybridAuditError("hybrid support audit contains duplicate block/window IDs")

    samples = sum(event["samples"] for event in events)
    if samples != expected_samples:
        raise HybridAuditError(
            f"hybrid support audit has {samples} samples, expected {expected_samples}"
        )
    plan_references = sum(event["plan_references"] for event in events)
    expected_plan_references = samples * 16
    if plan_references != expected_plan_references:
        raise HybridAuditError(
            "hybrid support audit does not contain the expected two-hand "
            "MT-MFS imaging/PSF/weight plan multiplicity"
        )

    represented_w_extents = {event["represented_w_extent"] for event in events}
    w_increments = {event["w_increment"] for event in events}
    if len(represented_w_extents) != 1 or len(w_increments) != 1:
        raise HybridAuditError("CF W geometry changes between replay windows")

    current = sum(event["current_weighted_taps"] for event in events)
    a_only = sum(event["a_only_weighted_taps"] for event in events)
    if current <= 0:
        raise HybridAuditError("current weighted support work must be positive")
    stack_totals = [
        sum(event["stack_weighted_taps"][index] for event in events)
        for index in range(len(EXPECTED_STACK_COUNTS))
    ]
    if stack_totals[0] != current:
        raise HybridAuditError("one-stack W=0 control does not reproduce current support work")

    stack_curve = [
        {
            "stacks": stack_count,
            "weighted_taps": weighted_taps,
            "support_ratio": weighted_taps / current,
        }
        for stack_count, weighted_taps in zip(
            EXPECTED_STACK_COUNTS,
            stack_totals,
            strict=True,
        )
    ]
    best_stack = min(stack_curve, key=lambda entry: entry["support_ratio"])
    a_only_ratio = a_only / current
    support_gate_passed = (
        a_only_ratio <= maximum_support_ratio
        and best_stack["support_ratio"] <= maximum_support_ratio
    )

    summary_lines = [
        line
        for line in text.splitlines()
        if line.startswith(f"{SUMMARY_EVENT} ") and "pass=initial_dirty" in line
    ]
    if len(summary_lines) != 1:
        raise HybridAuditError("log must contain one initial-dirty Metal summary")
    materialized_kernel_values = _required_int(summary_lines[0], "kernel_values")

    return {
        "role": "bounded-support-cost-audit-not-performance-or-science-evidence",
        "contract": {
            "imsize": 4096,
            "spws": "2~17",
            "gridder": "awproject",
            "wprojplanes": 32,
            "nterms": 2,
            "support_gate_maximum_ratio": maximum_support_ratio,
        },
        "coverage": {
            "windows": len(events),
            "samples": samples,
            "plan_references": plan_references,
            "represented_w_extent_lambda": represented_w_extents.pop(),
            "w_increment": w_increments.pop(),
        },
        "incumbent": {
            "weighted_taps": current,
            "materialized_kernel_values": materialized_kernel_values,
        },
        "a_only_floor": {
            "weighted_taps": a_only,
            "support_ratio": a_only_ratio,
        },
        "stack_curve": stack_curve,
        "selection": {
            "best_stack_count": best_stack["stacks"],
            "best_support_ratio": best_stack["support_ratio"],
            "support_gate_passed": support_gate_passed,
            "status": (
                "eligible-for-complete-operator-discriminator"
                if support_gate_passed
                else "rejected-by-support-gate-before-added-fft-and-screen-work"
            ),
        },
    }


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-samples", type=int, default=385_862)
    parser.add_argument("--maximum-support-ratio", type=float, default=0.5)
    return parser.parse_args()


def main() -> int:
    """Write one immutable diagnostic receipt."""

    args = parse_args()
    if not 0.0 < args.maximum_support_ratio <= 1.0:
        raise HybridAuditError("--maximum-support-ratio must be in (0, 1]")
    result = analyze_log(
        args.log.read_text(encoding="utf-8"),
        expected_samples=args.expected_samples,
        maximum_support_ratio=args.maximum_support_ratio,
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
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
