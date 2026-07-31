#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Reduce the VLASS subgrid support diagnostic to a work-bound receipt.

This tool parses an already completed casa-rs diagnostic log. It runs neither
CASA nor imaging. It corrects the constant-L work estimate with the actual
sampled-patch widths and exact MT-MFS right-hand-side multiplicities.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
from typing import Any


AUDIT_EVENT = "awproject_subgrid_support_audit"
EXPECTED_SIDES = [32, 48, 64, 96, 128]


class SubgridSupportAuditError(RuntimeError):
    """Raised when a diagnostic log cannot support the subgrid audit."""


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
        raise SubgridSupportAuditError(f"{AUDIT_EVENT} lacks integer {key}")
    return int(match.group(1))


def _required_int_csv(line: str, key: str) -> list[int]:
    match = re.search(rf"(?:^|\s){re.escape(key)}=([0-9,]+)(?:\s|$)", line)
    if match is None:
        raise SubgridSupportAuditError(f"{AUDIT_EVENT} lacks integer vector {key}")
    return [int(value) for value in match.group(1).split(",")]


def _parse_event(line: str) -> dict[str, Any]:
    sides = _required_int_csv(line, "sides")
    if sides != EXPECTED_SIDES:
        raise SubgridSupportAuditError(
            f"{AUDIT_EVENT} sides must be {EXPECTED_SIDES}, got {sides}"
        )
    result: dict[str, Any] = {
        "block": _required_int(line, "block"),
        "window": _required_int(line, "window"),
        "samples": _required_int(line, "samples"),
        "max_patch_width": _required_int(line, "max_patch_width"),
    }
    for role in ("initial", "prediction", "adjoint"):
        references = _required_int(line, f"{role}_plan_references")
        histogram = _required_int_csv(line, f"{role}_plan_references_by_side")
        if len(histogram) != len(EXPECTED_SIDES):
            raise SubgridSupportAuditError(
                f"{role} side histogram must have {len(EXPECTED_SIDES)} entries"
            )
        if sum(histogram) != references:
            raise SubgridSupportAuditError(
                f"{role} side histogram does not sum to its plan references"
            )
        result[role] = {
            "plan_references": references,
            "plan_references_by_side": histogram,
            "tap_interactions": _required_int(line, f"{role}_tap_interactions"),
            "subgrid_interactions": _required_int(
                line,
                f"{role}_subgrid_interactions",
            ),
        }
    return result


def _sum_role(events: list[dict[str, Any]], role: str) -> dict[str, Any]:
    return {
        "plan_references": sum(
            event[role]["plan_references"] for event in events
        ),
        "plan_references_by_side": [
            sum(event[role]["plan_references_by_side"][index] for event in events)
            for index in range(len(EXPECTED_SIDES))
        ],
        "tap_interactions": sum(
            event[role]["tap_interactions"] for event in events
        ),
        "subgrid_interactions": sum(
            event[role]["subgrid_interactions"] for event in events
        ),
    }


def analyze_log(
    text: str,
    *,
    expected_samples: int = 385_862,
    residual_refreshes: int = 5,
) -> dict[str, Any]:
    """Aggregate one full16 real-signature support diagnostic."""

    if residual_refreshes < 0:
        raise SubgridSupportAuditError("residual_refreshes must be non-negative")
    events = [
        _parse_event(line)
        for line in text.splitlines()
        if line.startswith(f"{AUDIT_EVENT} ")
    ]
    if not events:
        raise SubgridSupportAuditError(f"log contains no {AUDIT_EVENT} events")
    window_ids = [(event["block"], event["window"]) for event in events]
    if len(set(window_ids)) != len(window_ids):
        raise SubgridSupportAuditError(
            "subgrid support audit contains duplicate block/window IDs"
        )
    samples = sum(event["samples"] for event in events)
    if samples != expected_samples:
        raise SubgridSupportAuditError(
            f"subgrid support audit has {samples} samples, expected {expected_samples}"
        )

    roles = {
        role: _sum_role(events, role)
        for role in ("initial", "prediction", "adjoint")
    }
    expected_multiplicities = {
        "initial": 16,
        "prediction": 4,
        "adjoint": 4,
    }
    for role, multiplicity in expected_multiplicities.items():
        expected_references = samples * multiplicity
        if roles[role]["plan_references"] != expected_references:
            raise SubgridSupportAuditError(
                f"{role} has {roles[role]['plan_references']} plan references, "
                f"expected {expected_references}"
            )

    full_histogram = [
        roles["initial"]["plan_references_by_side"][index]
        + residual_refreshes
        * (
            roles["prediction"]["plan_references_by_side"][index]
            + roles["adjoint"]["plan_references_by_side"][index]
        )
        for index in range(len(EXPECTED_SIDES))
    ]
    full_tap_interactions = roles["initial"]["tap_interactions"] + residual_refreshes * (
        roles["prediction"]["tap_interactions"]
        + roles["adjoint"]["tap_interactions"]
    )
    full_subgrid_interactions = roles["initial"][
        "subgrid_interactions"
    ] + residual_refreshes * (
        roles["prediction"]["subgrid_interactions"]
        + roles["adjoint"]["subgrid_interactions"]
    )
    if full_tap_interactions <= 0:
        raise SubgridSupportAuditError("full tap interaction count must be positive")
    full_references = sum(full_histogram)
    side_curve = [
        {
            "side": side,
            "plan_references": references,
            "plan_reference_fraction": references / full_references,
            "subgrid_interactions": references * side**2,
        }
        for side, references in zip(EXPECTED_SIDES, full_histogram, strict=True)
    ]
    if sum(entry["subgrid_interactions"] for entry in side_curve) != (
        full_subgrid_interactions
    ):
        raise SubgridSupportAuditError(
            "side histogram does not reproduce the reported subgrid work"
        )

    return {
        "role": "bounded-real-signature-work-audit-not-performance-or-science-evidence",
        "contract": {
            "imsize": 4096,
            "spws": "2~17",
            "gridder": "awproject",
            "wprojplanes": 32,
            "nterms": 2,
            "residual_refreshes": residual_refreshes,
            "logical_operator_calls": 1 + 2 * residual_refreshes,
            "side_schedule": EXPECTED_SIDES,
        },
        "coverage": {
            "windows": len(events),
            "samples": samples,
            "max_patch_width": max(event["max_patch_width"] for event in events),
        },
        "per_call_kind": roles,
        "full_trajectory": {
            "plan_references": full_references,
            "plan_references_by_side": full_histogram,
            "tap_interactions": full_tap_interactions,
            "subgrid_interactions": full_subgrid_interactions,
            "subgrid_to_tap_interaction_ratio": (
                full_subgrid_interactions / full_tap_interactions
            ),
            "side_curve": side_curve,
        },
        "selection": {
            "constant_l32_is_valid": all(
                references == 0
                for references in full_histogram[1:]
            ),
            "status": "measured-mixed-side-work-bound",
            "claim_boundary": (
                "work count only; unlike tap and dense-subgrid interactions "
                "still require a matched M4 race"
            ),
        },
    }


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-samples", type=int, default=385_862)
    parser.add_argument("--residual-refreshes", type=int, default=5)
    return parser.parse_args()


def main() -> int:
    """Write one immutable diagnostic receipt."""

    args = parse_args()
    result = analyze_log(
        args.log.read_text(encoding="utf-8"),
        expected_samples=args.expected_samples,
        residual_refreshes=args.residual_refreshes,
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
