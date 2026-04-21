#!/usr/bin/env python3
"""Summarize `calibrate` apply-performance JSONL traces."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter
from pathlib import Path


def load_events(path: Path) -> list[dict]:
    events: list[dict] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        events.append(json.loads(line))
    return events


def median_ms(values_ns: list[int]) -> float:
    if not values_ns:
        return 0.0
    return statistics.median(values_ns) / 1_000_000.0


def p95_ms(values_ns: list[int]) -> float:
    if not values_ns:
        return 0.0
    ordered = sorted(values_ns)
    index = round((len(ordered) - 1) * 0.95)
    return ordered[index] / 1_000_000.0


def field_values(events: list[dict], field: str) -> list[int]:
    values: list[int] = []
    for event in events:
        value = event.get(field)
        if isinstance(value, int):
            values.append(value)
    return values


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl", type=Path, help="Path to a JSONL perf trace")
    args = parser.parse_args()

    events = load_events(args.jsonl)
    if not events:
        print("No events found.")
        return 0

    kind_counts = Counter(event["kind"] for event in events if "kind" in event)
    completed = [event for event in events if event.get("kind") == "apply_completed"]

    print(f"Trace: {args.jsonl}")
    print(f"Events: {len(events)}")
    print("\nEvent counts:")
    for kind, count in sorted(kind_counts.items()):
        print(f"  {kind:24} {count}")

    if completed:
        print("\nApply-completed medians:")
        for field in (
            "planning_ns",
            "open_measurement_set_ns",
            "row_field_index_lookup_ns",
            "ensure_corrected_data_ns",
            "correlation_lookup_ns",
            "calibration_load_ns",
            "execute_apply_plan_ns",
            "execute_apply_plan_unattributed_ns",
            "row_read_total_ns",
            "row_fetch_ns",
            "row_compute_ns",
            "row_read_overhead_ns",
            "row_writeback_ns",
            "save_ns",
            "drop_ns",
            "total_ns",
        ):
            values = field_values(completed, field)
            print(
                f"  {field:36} median={median_ms(values):8.3f} ms"
                f"  p95={p95_ms(values):8.3f} ms"
                f"  n={len(values)}"
            )

        latest = completed[-1]
        print("\nLatest apply-completed:")
        print(
            "  "
            f"rows={latest.get('selected_row_count', 0)} "
            f"tables={latest.get('calibration_table_count', 0)} "
            f"apply_mode={latest.get('apply_mode', '<unknown>')} "
            f"row_field_index_ms={latest.get('row_field_index_lookup_ns', 0) / 1_000_000.0:.3f} "
            f"row_read_ms={latest.get('row_read_total_ns', 0) / 1_000_000.0:.3f} "
            f"row_read_overhead_ms={latest.get('row_read_overhead_ns', 0) / 1_000_000.0:.3f} "
            f"row_write_ms={latest.get('row_writeback_ns', 0) / 1_000_000.0:.3f} "
            f"save_ms={latest.get('save_ns', 0) / 1_000_000.0:.3f} "
            f"total_ms={latest.get('total_ns', 0) / 1_000_000.0:.3f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
