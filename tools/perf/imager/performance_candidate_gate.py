#!/usr/bin/env python3
"""Evaluate whether a measured phase can justify an implementation candidate."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, Sequence


SCHEMA_NAME = "casa-rs-performance-candidate-eligibility"
SCHEMA_VERSION = 1
CAMPAIGN_THRESHOLD_FRACTION = 0.10
RECORD_KINDS = {"implementation", "diagnostic_only"}


class CandidateError(ValueError):
    """A candidate record cannot be evaluated."""


def load_record(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise CandidateError(f"cannot read {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise CandidateError(f"{path} is not valid JSON: {error}") from error
    if not isinstance(value, dict):
        raise CandidateError(f"{path} must contain a JSON object")
    return value


def evaluate(
    record: dict[str, Any],
    *,
    campaign_threshold_fraction: float = CAMPAIGN_THRESHOLD_FRACTION,
) -> dict[str, Any]:
    threshold = fraction(
        campaign_threshold_fraction, "campaign_threshold_fraction"
    )
    if record.get("schema_version") != SCHEMA_VERSION:
        raise CandidateError(f"schema_version must be {SCHEMA_VERSION}")

    candidate_id = non_empty_string(record.get("candidate_id"), "candidate_id")
    record_kind = non_empty_string(record.get("record_kind"), "record_kind")
    if record_kind not in RECORD_KINDS:
        raise CandidateError(
            "record_kind must be one of " + ", ".join(sorted(RECORD_KINDS))
        )

    projected_total_seconds = positive_number(
        record.get("projected_total_seconds"), "projected_total_seconds"
    )
    phase = record.get("phase")
    if not isinstance(phase, dict):
        raise CandidateError("phase must be an object")
    phase_name = non_empty_string(phase.get("name"), "phase.name")
    seconds_per_occurrence = positive_number(
        phase.get("seconds_per_occurrence"), "phase.seconds_per_occurrence"
    )
    occurrence_count = positive_integer(
        phase.get("occurrence_count"), "phase.occurrence_count"
    )
    affected_fraction = fraction(
        record.get("affected_fraction"), "affected_fraction"
    )
    removable_fraction = fraction(
        record.get("removable_fraction"), "removable_fraction"
    )

    weighted_seconds = seconds_per_occurrence * occurrence_count
    if weighted_seconds > projected_total_seconds:
        raise CandidateError(
            "phase occurrence-weighted seconds must not exceed projected_total_seconds"
        )
    weighted_fraction = weighted_seconds / projected_total_seconds
    affected_removable_ceiling = affected_fraction * removable_fraction
    optimistic_improvement = weighted_fraction * affected_removable_ceiling

    implementation_eligible = (
        record_kind == "implementation" and optimistic_improvement >= threshold
    )
    if record_kind == "diagnostic_only":
        decision = "allowed_diagnostic_only"
        allowed = True
    elif implementation_eligible:
        decision = "eligible_for_implementation"
        allowed = True
    else:
        decision = "rejected_below_campaign_threshold"
        allowed = False

    return {
        "schema_name": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "status": "evaluated",
        "candidate_id": candidate_id,
        "record_kind": record_kind,
        "decision": decision,
        "allowed": allowed,
        "implementation_eligible": implementation_eligible,
        "campaign_threshold_fraction": threshold,
        "metrics": {
            "projected_total_seconds": projected_total_seconds,
            "phase_name": phase_name,
            "phase_seconds_per_occurrence": seconds_per_occurrence,
            "phase_occurrence_count": occurrence_count,
            "phase_occurrence_weighted_baseline_seconds": weighted_seconds,
            "phase_occurrence_weighted_baseline_contribution_fraction": (
                weighted_fraction
            ),
            "affected_fraction": affected_fraction,
            "removable_fraction": removable_fraction,
            "affected_removable_ceiling_fraction": affected_removable_ceiling,
            "optimistic_maximum_end_to_end_improvement_fraction": (
                optimistic_improvement
            ),
            "optimistic_maximum_end_to_end_improvement_percent": (
                optimistic_improvement * 100.0
            ),
        },
    }


def finite_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CandidateError(f"{name} must be a finite number")
    result = float(value)
    if not math.isfinite(result):
        raise CandidateError(f"{name} must be a finite number")
    return result


def positive_number(value: Any, name: str) -> float:
    result = finite_number(value, name)
    if result <= 0.0:
        raise CandidateError(f"{name} must be positive")
    return result


def fraction(value: Any, name: str) -> float:
    result = finite_number(value, name)
    if not 0.0 <= result <= 1.0:
        raise CandidateError(f"{name} must be between 0 and 1")
    return result


def positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CandidateError(f"{name} must be a positive integer")
    return value


def non_empty_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CandidateError(f"{name} must be a non-empty string")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("record", type=Path)
    parser.add_argument(
        "--campaign-threshold-fraction",
        type=float,
        default=CAMPAIGN_THRESHOLD_FRACTION,
    )
    args = parser.parse_args(argv)

    try:
        result = evaluate(
            load_record(args.record),
            campaign_threshold_fraction=args.campaign_threshold_fraction,
        )
    except CandidateError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    json.dump(result, sys.stdout, indent=2, sort_keys=True, allow_nan=False)
    sys.stdout.write("\n")
    return 0 if result["allowed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
