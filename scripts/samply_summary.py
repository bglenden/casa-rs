#!/usr/bin/env python3
"""Summarize a samply Firefox-profile JSON file from the terminal.

This prints per-thread sample totals plus the hottest leaf functions and
representative stacks so we can compare `casars` and `imexplore --session`
without opening the profiler UI.
"""

from __future__ import annotations

import argparse
import bisect
import gzip
import json
from collections import Counter
from pathlib import Path
from typing import Any


def load_profile(path: Path) -> dict[str, Any]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        return json.load(handle)


def sidecar_path(profile_path: Path) -> Path | None:
    if profile_path.name.endswith(".json.gz"):
        return profile_path.with_name(profile_path.name[:-8] + ".json.syms.json")
    if profile_path.name.endswith(".json"):
        return profile_path.with_name(profile_path.name[:-5] + ".syms.json")
    return None


def load_sidecar(profile_path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    path = sidecar_path(profile_path)
    if path is None or not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        sidecar = json.load(handle)
    data = {}
    for entry in sidecar.get("data", []):
        key = (
            str(entry.get("debug_name", "")),
            str(entry.get("code_id", "")).upper(),
        )
        data[key] = entry
    data["_string_table"] = sidecar.get("string_table", [])
    return data


def string_at(strings: list[str], index: int | None) -> str:
    if index is None or index < 0 or index >= len(strings):
        return "<unknown>"
    return strings[index]


def resolve_symbol_name(
    profile: dict[str, Any],
    sidecar: dict[tuple[str, str], dict[str, Any]],
    thread: dict[str, Any],
    frame_index: int | None,
) -> str | None:
    if frame_index is None or frame_index < 0:
        return None
    frame_table = thread["frameTable"]
    address = frame_table["address"][frame_index]
    if address in (None, -1):
        return None
    func_table = thread["funcTable"]
    func_index = frame_table["func"][frame_index]
    resource_index = func_table["resource"][func_index]
    if resource_index in (None, -1):
        return None
    resource_table = thread["resourceTable"]
    lib_index = resource_table["lib"][resource_index]
    if lib_index in (None, -1):
        return None
    libs = profile.get("libs", [])
    if lib_index < 0 or lib_index >= len(libs):
        return None
    lib = libs[lib_index]
    key = (
        str(lib.get("debugName") or lib.get("name") or ""),
        str(lib.get("codeId") or "").upper(),
    )
    symbols = sidecar.get(key)
    if not symbols:
        return None
    known_addresses = symbols.get("known_addresses", [])
    if not known_addresses:
        return None
    addresses = [entry[0] for entry in known_addresses]
    index = bisect.bisect_right(addresses, address) - 1
    if index < 0:
        return None
    symbol_table_index = known_addresses[index][1]
    symbol_entry = symbols["symbol_table"][symbol_table_index]
    symbol_string_index = symbol_entry["symbol"]
    string_table = sidecar.get("_string_table", [])
    if symbol_string_index < 0 or symbol_string_index >= len(string_table):
        return None
    return str(string_table[symbol_string_index])


def frame_lib_name(profile: dict[str, Any], thread: dict[str, Any], frame_index: int | None) -> str | None:
    if frame_index is None or frame_index < 0:
        return None
    frame_table = thread["frameTable"]
    func_index = frame_table["func"][frame_index]
    resource_index = thread["funcTable"]["resource"][func_index]
    if resource_index in (None, -1):
        return None
    lib_index = thread["resourceTable"]["lib"][resource_index]
    if lib_index in (None, -1):
        return None
    libs = profile.get("libs", [])
    if lib_index < 0 or lib_index >= len(libs):
        return None
    lib = libs[lib_index]
    return str(lib.get("name") or lib.get("debugName") or "")


def function_name(
    profile: dict[str, Any],
    sidecar: dict[tuple[str, str], dict[str, Any]],
    thread: dict[str, Any],
    frame_index: int | None,
) -> str:
    if frame_index is None or frame_index < 0:
        return "<unknown>"
    resolved = resolve_symbol_name(profile, sidecar, thread, frame_index)
    if resolved:
        return resolved
    frame_table = thread["frameTable"]
    func_index = frame_table["func"][frame_index]
    func_table = thread["funcTable"]
    name_index = func_table["name"][func_index]
    return string_at(thread["stringArray"], name_index)


def stack_frames(thread: dict[str, Any], stack_index: int | None) -> list[int]:
    if stack_index is None:
        return []
    stack_table = thread["stackTable"]
    frames: list[int] = []
    current = stack_index
    while current is not None:
        frames.append(stack_table["frame"][current])
        current = stack_table["prefix"][current]
    frames.reverse()
    return frames


def stack_signature(
    profile: dict[str, Any],
    sidecar: dict[tuple[str, str], dict[str, Any]],
    thread: dict[str, Any],
    stack_index: int | None,
    depth: int,
) -> str:
    frames = stack_frames(thread, stack_index)
    names = [
        function_name(profile, sidecar, thread, frame_index)
        for frame_index in frames[-depth:]
    ]
    return " -> ".join(names) if names else "<no stack>"


def sample_weight(samples: dict[str, Any], sample_index: int, weight_mode: str) -> int:
    if weight_mode == "cpu":
        cpu_deltas = samples.get("threadCPUDelta")
        if isinstance(cpu_deltas, list) and sample_index < len(cpu_deltas):
            return int(cpu_deltas[sample_index] or 0)
    weights = samples.get("weight")
    if isinstance(weights, list) and sample_index < len(weights):
        return int(weights[sample_index] or 0)
    return 1


def summarize_thread(
    profile: dict[str, Any],
    sidecar: dict[tuple[str, str], dict[str, Any]],
    thread: dict[str, Any],
    leaf_limit: int,
    stack_limit: int,
    stack_depth: int,
    main_lib_only: bool,
    weight_mode: str,
) -> str:
    samples = thread.get("samples", {})
    stacks = samples.get("stack", [])
    total_weight = 0
    leaf_counter: Counter[str] = Counter()
    stack_counter: Counter[str] = Counter()

    for index, stack_index in enumerate(stacks):
        weight = sample_weight(samples, index, weight_mode)
        if weight <= 0:
            continue
        total_weight += weight
        frames = stack_frames(thread, stack_index)
        if main_lib_only:
            process_name = str(thread.get("processName") or "")
            frames = [
                frame_index
                for frame_index in frames
                if frame_lib_name(profile, thread, frame_index) == process_name
            ]
        leaf_name = function_name(profile, sidecar, thread, frames[-1] if frames else None)
        leaf_counter[leaf_name] += weight
        stack_counter[
            " -> ".join(
                [
                    function_name(profile, sidecar, thread, frame_index)
                    for frame_index in frames[-stack_depth:]
                ]
            )
            if frames
            else "<no stack>"
        ] += weight

    process_name = thread.get("processName") or "<unknown>"
    thread_name = thread.get("name") or "<unnamed>"
    pid = thread.get("pid")
    tid = thread.get("tid")

    lines = [
        f"process={process_name} pid={pid} thread={thread_name} tid={tid} total_weight={total_weight}"
    ]
    lines.append("  top leaf frames:")
    for name, weight in leaf_counter.most_common(leaf_limit):
        lines.append(f"    {weight:>8}  {name}")
    lines.append(f"  top stacks (last {stack_depth} frames):")
    for signature, weight in stack_counter.most_common(stack_limit):
        lines.append(f"    {weight:>8}  {signature}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("profile", type=Path)
    parser.add_argument("--leaf-limit", type=int, default=10)
    parser.add_argument("--stack-limit", type=int, default=8)
    parser.add_argument("--stack-depth", type=int, default=8)
    parser.add_argument(
        "--process-filter",
        action="append",
        default=[],
        help="only include threads whose processName contains this substring",
    )
    parser.add_argument(
        "--main-lib-only",
        action="store_true",
        help="only count frames from the thread's main executable image",
    )
    parser.add_argument(
        "--weight-mode",
        choices=["samples", "cpu"],
        default="samples",
        help="count either wall-sample weights or per-sample CPU deltas",
    )
    args = parser.parse_args()

    profile = load_profile(args.profile)
    sidecar = load_sidecar(args.profile)
    threads = profile.get("threads", [])
    filters = [value.lower() for value in args.process_filter]

    matched = []
    for thread in threads:
        process_name = str(thread.get("processName", "")).lower()
        if filters and not any(token in process_name for token in filters):
            continue
        matched.append(thread)

    for index, thread in enumerate(matched):
        if index:
            print()
        print(
            summarize_thread(
                profile,
                sidecar,
                thread,
                leaf_limit=args.leaf_limit,
                stack_limit=args.stack_limit,
                stack_depth=args.stack_depth,
                main_lib_only=args.main_lib_only,
                weight_mode=args.weight_mode,
            )
        )

    if not matched:
        print("no matching threads")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
