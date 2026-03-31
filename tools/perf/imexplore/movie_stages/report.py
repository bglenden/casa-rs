#!/usr/bin/env python3
"""Summarize staged imexplore movie benchmark JSONL traces."""

from __future__ import annotations

import argparse
import glob
import json
from collections import defaultdict
from typing import Any


def ns_to_ms(value: int) -> float:
    return value / 1_000_000.0


def format_ms(value: int) -> str:
    return f"{ns_to_ms(value):8.3f} ms"


def load_records(paths: list[str]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for pattern in paths:
        for path_str in sorted(glob.glob(pattern)):
            with open(path_str, "r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    records.append(json.loads(line))
    return records


def summarize(records: list[dict[str, Any]]) -> None:
    if not records:
        print("No records found.")
        return

    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[(record["stage"], record["mode"], record["phase"])].append(record)

    for key in sorted(grouped):
        stage, mode, phase = key
        rows = grouped[key]
        summaries = [row for row in rows if row["kind"] == "summary"]
        frames = [row for row in rows if row["kind"] == "frame"]
        print(f"{stage} / {mode} / {phase}")
        print("-" * (len(stage) + len(mode) + len(phase) + 6))
        if summaries:
            latest = summaries[-1]
            print(f"frames           : {latest['frame_count']}")
            print(f"achieved fps     : {latest['achieved_fps']:.2f}")
            print(f"target fps       : {latest['target_fps']:.2f}")
            print(f"preview p50/p95  : {format_ms(latest['preview_p50_ns'])} / {format_ms(latest['preview_p95_ns'])}")
            print(f"render  p50/p95  : {format_ms(latest['render_p50_ns'])} / {format_ms(latest['render_p95_ns'])}")
            print(f"present p50/p95  : {format_ms(latest['present_p50_ns'])} / {format_ms(latest['present_p95_ns'])}")
            print(f"ready max        : {latest['ready_buffer_max']}")
            print(f"preview q max    : {latest['preview_queue_max']}")
            print(f"render q max     : {latest['render_queue_max']}")
            print(f"preview workers  : {latest['preview_max_active']}")
            print(f"render workers   : {latest['render_max_active']}")
            print(f"stale/dropped    : {latest['stale_count']} / {latest['dropped_count']}")
            print(f"late frames      : {latest['late_count']}")
            print(f"gate pass        : {latest['gate_pass']}")
        if frames:
            max_fps = max(row.get("achieved_fps", 0.0) for row in frames)
            max_ready = max(row.get("ready_buffer_size", 0) for row in frames)
            max_preview_active = max(row.get("preview_active_workers", 0) for row in frames)
            max_render_active = max(row.get("render_active_workers", 0) for row in frames)
            cache_results = sorted({row.get("cache_result", "unknown") for row in frames})
            print(f"max frame fps    : {max_fps:.2f}")
            print(f"frame ready max  : {max_ready}")
            print(f"frame workers    : preview={max_preview_active} render={max_render_active}")
            print(f"cache results    : {', '.join(cache_results)}")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="+",
        help="JSONL paths or glob patterns, for example /tmp/imexplore-movie-stages/*.jsonl",
    )
    args = parser.parse_args()
    summarize(load_records(args.paths))


if __name__ == "__main__":
    main()
