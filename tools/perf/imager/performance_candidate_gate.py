#!/usr/bin/env python3
"""Evaluate whether a measured phase can justify an implementation candidate."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Any, Sequence


SCHEMA_NAME = "casa-rs-performance-candidate-eligibility"
SCHEMA_VERSION = 1
CAMPAIGN_THRESHOLD_FRACTION = 0.10
RECORD_KINDS = {"implementation", "diagnostic_only"}
ARTIFACT_RETENTION_CLASSES = {"immutable", "campaign_local"}
GIT_REVISION = re.compile(r"^[0-9a-f]{40}$")


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
    threshold = fraction(campaign_threshold_fraction, "campaign_threshold_fraction")
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
    affected_fraction = fraction(record.get("affected_fraction"), "affected_fraction")
    removable_fraction = fraction(
        record.get("removable_fraction"), "removable_fraction"
    )
    campaign_control = (
        validate_campaign_control(record) if record_kind == "implementation" else None
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
        "campaign_control": campaign_control,
    }


def validate_campaign_control(record: dict[str, Any]) -> dict[str, Any]:
    hypothesis = non_empty_string(record.get("hypothesis"), "hypothesis")
    parent_revision = non_empty_string(record.get("parent_revision"), "parent_revision")
    if GIT_REVISION.fullmatch(parent_revision) is None:
        raise CandidateError("parent_revision must be a full lowercase Git revision")

    discriminator = required_object(record, "discriminator")
    command = non_empty_string(discriminator.get("command"), "discriminator.command")
    baseline_seconds = positive_number(
        discriminator.get("baseline_seconds"), "discriminator.baseline_seconds"
    )
    maximum_candidate_seconds = positive_number(
        discriminator.get("maximum_candidate_seconds"),
        "discriminator.maximum_candidate_seconds",
    )
    if maximum_candidate_seconds >= baseline_seconds:
        raise CandidateError(
            "discriminator.maximum_candidate_seconds must improve on baseline_seconds"
        )
    checksum = non_empty_string(
        discriminator.get("exact_checksum"), "discriminator.exact_checksum"
    )
    if re.fullmatch(r"[0-9a-f]{64}", checksum) is None:
        raise CandidateError("discriminator.exact_checksum must be lowercase SHA-256")
    required_invariants = non_empty_strings(
        discriminator.get("required_invariants"),
        "discriminator.required_invariants",
    )

    limits = required_object(record, "limits")
    maximum_stage_seconds = positive_number(
        limits.get("maximum_stage_seconds"), "limits.maximum_stage_seconds"
    )
    maximum_wall_seconds = positive_number(
        limits.get("maximum_wall_seconds"), "limits.maximum_wall_seconds"
    )
    if maximum_stage_seconds > maximum_wall_seconds:
        raise CandidateError(
            "limits.maximum_stage_seconds must not exceed maximum_wall_seconds"
        )
    maximum_resident_bytes = positive_integer(
        limits.get("maximum_resident_bytes"), "limits.maximum_resident_bytes"
    )
    maximum_swap_operations = non_negative_integer(
        limits.get("maximum_swap_operations"), "limits.maximum_swap_operations"
    )

    falsifier = non_empty_string(record.get("falsifier"), "falsifier")
    reversion = non_empty_string(record.get("reversion"), "reversion")
    artifact_retention_class = non_empty_string(
        record.get("artifact_retention_class"), "artifact_retention_class"
    )
    if artifact_retention_class not in ARTIFACT_RETENTION_CLASSES:
        raise CandidateError(
            "artifact_retention_class must be one of "
            + ", ".join(sorted(ARTIFACT_RETENTION_CLASSES))
        )
    return {
        "artifact_retention_class": artifact_retention_class,
        "discriminator": {
            "baseline_seconds": baseline_seconds,
            "command": command,
            "exact_checksum": checksum,
            "maximum_candidate_seconds": maximum_candidate_seconds,
            "required_invariants": required_invariants,
        },
        "falsifier": falsifier,
        "hypothesis": hypothesis,
        "limits": {
            "maximum_resident_bytes": maximum_resident_bytes,
            "maximum_stage_seconds": maximum_stage_seconds,
            "maximum_swap_operations": maximum_swap_operations,
            "maximum_wall_seconds": maximum_wall_seconds,
        },
        "parent_revision": parent_revision,
        "reversion": reversion,
    }


def required_object(record: dict[str, Any], name: str) -> dict[str, Any]:
    value = record.get(name)
    if not isinstance(value, dict):
        raise CandidateError(f"{name} must be an object")
    return value


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


def non_negative_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CandidateError(f"{name} must be a non-negative integer")
    return value


def non_empty_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CandidateError(f"{name} must be a non-empty string")
    return value


def non_empty_strings(value: Any, name: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise CandidateError(f"{name} must be a non-empty array")
    return [
        non_empty_string(item, f"{name}[{index}]") for index, item in enumerate(value)
    ]


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
