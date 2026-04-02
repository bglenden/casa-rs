#!/usr/bin/env python3
"""Summarize `imexplore` movie performance JSONL traces."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter, defaultdict
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl", type=Path, help="Path to a JSONL perf trace")
    args = parser.parse_args()

    events = load_events(args.jsonl)
    if not events:
        print("No events found.")
        return 0

    kind_counts = Counter(event["kind"] for event in events)
    outcome_counts = Counter(
        event["outcome"] for event in events if event.get("outcome") is not None
    )

    latencies_by_kind: dict[str, list[int]] = defaultdict(list)
    queue_depths_by_kind: dict[str, list[int]] = defaultdict(list)
    pipeline_snapshots: list[dict] = []
    for event in events:
        duration_ns = event.get("duration_ns")
        if isinstance(duration_ns, int):
            latencies_by_kind[event["kind"]].append(duration_ns)
        queue_depth = event.get("queue_depth")
        if isinstance(queue_depth, int):
            queue_depths_by_kind[event["kind"]].append(queue_depth)
        pipeline = event.get("pipeline")
        if isinstance(pipeline, dict):
            pipeline_snapshots.append(pipeline)

    last_summary = next(
        (event for event in reversed(events) if event["kind"] == "summary"),
        None,
    )
    direct_overlay_events = [
        event for event in events if event["kind"] == "direct_overlay_changed"
    ]
    generation_invalidations = [
        event for event in events if event["kind"] == "generation_invalidated"
    ]
    deadline_misses = [event for event in events if event["kind"] == "deadline_missed"]

    print(f"Trace: {args.jsonl}")
    print(f"Events: {len(events)}")
    print("\nEvent counts:")
    for kind, count in sorted(kind_counts.items()):
        print(f"  {kind:24} {count}")

    if outcome_counts:
        print("\nOutcome counts:")
        for outcome, count in sorted(outcome_counts.items()):
            print(f"  {outcome:24} {count}")

    if latencies_by_kind:
        print("\nDurations:")
        for kind in sorted(latencies_by_kind):
            values = latencies_by_kind[kind]
            print(
                f"  {kind:24} median={median_ms(values):8.3f} ms"
                f"  p95={p95_ms(values):8.3f} ms"
                f"  n={len(values)}"
            )

    if queue_depths_by_kind:
        print("\nQueue depths:")
        for kind in sorted(queue_depths_by_kind):
            values = queue_depths_by_kind[kind]
            print(
                f"  {kind:24} median={statistics.median(values):5.1f}"
                f"  max={max(values):5d}"
                f"  n={len(values)}"
            )

    if pipeline_snapshots:
        latest = pipeline_snapshots[-1]

        def pipeline_series(key: str) -> list[int]:
            return [
                int(snapshot[key])
                for snapshot in pipeline_snapshots
                if isinstance(snapshot.get(key), int)
            ]

        print("\nPipeline state:")
        for key in (
            "render_queue_depth",
            "render_active_jobs",
            "protocol_queue_depth",
            "protocol_active_jobs",
            "ready_bundle_count",
            "ready_presentation_count",
        ):
            values = pipeline_series(key)
            if not values:
                continue
            print(
                f"  {key:24} latest={latest.get(key, 0):5d}"
                f"  median={statistics.median(values):5.1f}"
                f"  max={max(values):5d}"
            )
        bitmap_cache_mb = latest.get("bitmap_cache_bytes", 0) / (1024 * 1024)
        print(f"  {'bitmap_cache_mb':24} latest={bitmap_cache_mb:5.1f}")

    frame_presented = [
        event
        for event in events
        if event["kind"] in {"plane_presented", "bundle_presented"}
        and isinstance(event.get("frame_seq"), int)
    ]
    if frame_presented:
        frame_ids = [event["frame_seq"] for event in frame_presented]
        print(
            f"\nPresented frames: {len(frame_ids)}"
            f"  first={min(frame_ids)}  last={max(frame_ids)}"
        )

    if direct_overlay_events:
        print("\nDirect overlay transitions:")
        for event in direct_overlay_events:
            note = event.get("note", "<none>")
            frame_seq = event.get("frame_seq")
            axis_index = event.get("axis_index")
            print(
                f"  t={event.get('monotonic_ns', 0) / 1_000_000.0:10.3f} ms"
                f"  frame={frame_seq!s:>4}"
                f"  axis_index={axis_index!s:>4}"
                f"  {note}"
            )

    if generation_invalidations:
        print("\nGeneration invalidations:")
        for event in generation_invalidations:
            print(
                f"  t={event.get('monotonic_ns', 0) / 1_000_000.0:10.3f} ms"
                f"  axis_index={event.get('axis_index')!s:>4}"
                f"  {event.get('note', '<none>')}"
            )

    if deadline_misses:
        print("\nDeadline misses:")
        for event in deadline_misses:
            print(
                f"  t={event.get('monotonic_ns', 0) / 1_000_000.0:10.3f} ms"
                f"  queue_depth={event.get('queue_depth')!s:>4}"
                f"  {event.get('note', '<none>')}"
            )

    if last_summary is not None:
        print("\nLast summary:")
        print(f"  {last_summary.get('note', '<none>')}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
