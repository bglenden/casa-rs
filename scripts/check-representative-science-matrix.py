#!/usr/bin/env python3
"""Validate the programme #486 representative-science acceptance matrix."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "resources/imaging-architecture/representative-science-matrix.json"


def main() -> int:
    document = json.loads(MATRIX.read_text())
    failures: list[str] = []
    if document.get("schema") != "casa-rs-representative-science-matrix-v1":
        failures.append("unexpected matrix schema")
    contract = document["contract"]
    scenarios = {scenario["id"]: scenario for scenario in document["scenarios"]}
    if len(scenarios) != len(document["scenarios"]):
        failures.append("scenario identifiers are not unique")
    for scenario in scenarios.values():
        identifier = scenario["id"]
        if scenario.get("evidence_tier") != "representative_scientific_acceptance":
            failures.append(f"{identifier}: wrong evidence tier")
        if scenario.get("production_path") is not True:
            failures.append(f"{identifier}: does not use the production path")
        shape = scenario.get("image_shape", [])
        if len(shape) < 2 or min(shape[:2]) < contract["minimum_image_extent"]:
            failures.append(f"{identifier}: image is below the representative extent")
        if (
            scenario.get("selected_samples", 0)
            < contract["minimum_selected_correlation_channel_samples"]
            and not scenario.get("shape_exception")
        ):
            failures.append(f"{identifier}: sample volume is below contract without an exception")
        for evidence in scenario.get("evidence", []):
            if not (ROOT / evidence).exists():
                failures.append(f"{identifier}: repository evidence is missing: {evidence}")
        if scenario.get("status") != "pass":
            failures.append(f"{identifier}: status is {scenario.get('status')}")

    tickets = document["tickets"]
    issues = [ticket["issue"] for ticket in tickets]
    if len(issues) != len(set(issues)):
        failures.append("ticket issues are not unique")
    required = set(range(504, 534)) | {540, 574, 580, 581, 586, 589, 590, 591, 597}
    missing = sorted(required - set(issues))
    extra = sorted(set(issues) - required)
    if missing or extra:
        failures.append(f"ticket issue set differs: missing={missing} extra={extra}")
    for ticket in tickets:
        scenario = scenarios.get(ticket.get("scenario"))
        if scenario is None:
            failures.append(f"issue #{ticket['issue']}: scenario is missing")
        if not ticket.get("existing_evidence_tiers"):
            failures.append(f"issue #{ticket['issue']}: existing evidence is unclassified")
        if not ticket.get("comparators"):
            failures.append(f"issue #{ticket['issue']}: comparators are missing")

    if failures:
        for failure in failures:
            print(f"representative-science-matrix: {failure}", file=sys.stderr)
        return 1
    print(f"representative-science-matrix: {len(tickets)} tickets, {len(scenarios)} scenarios, all pass")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
