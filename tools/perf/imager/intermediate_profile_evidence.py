#!/usr/bin/env python3
"""Classify a major-cycle receipt and normalize macOS sample evidence."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Sequence


SCHEMA_NAME = "casa-rs-intermediate-major-profile"
SCHEMA_VERSION = 1
MINOR_CYCLE_NODE = "spectral-cycle-minor-cycle"
IDLE_SYMBOLS = frozenset({"__workq_kernreturn", "semaphore_wait_trap"})

FINAL_MAJOR_READ = re.compile(r"^transaction-read-final-major-(\d+)$")
SAMPLE_HEADER = re.compile(
    r"^Analysis of sampling (?P<process>.+) \(pid (?P<pid>\d+)\) "
    r"every (?P<interval>\d+) milliseconds?$",
    re.MULTILINE,
)
MAIN_THREAD = re.compile(
    r"^\s*(?P<count>\d+)\s+Thread_\S+.*"
    r"(?:com\.apple\.main-thread|Main Thread)",
    re.MULTILINE,
)
EXCLUSIVE_LEAF = re.compile(
    r"^\s*(?P<symbol>.+?)\s{2,}\(in (?P<image>.+?)\)\s+"
    r"(?:load address 0x[0-9a-fA-F]+ \+ 0x[0-9a-fA-F]+\s+"
    r"\[0x[0-9a-fA-F]+\]\s+)?"
    r"(?P<count>\d+)\s*$"
)


class ProfileEvidenceError(ValueError):
    """The input cannot produce trustworthy profile evidence."""


def load_json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ProfileEvidenceError(f"cannot read {label} {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise ProfileEvidenceError(
            f"{label} {path} is not valid JSON: {error}"
        ) from error
    if not isinstance(value, dict):
        raise ProfileEvidenceError(f"{label} {path} must contain a JSON object")
    return value


def classify_receipt(document: dict[str, Any]) -> dict[str, Any]:
    receipt = document.get("receipt")
    if not isinstance(receipt, dict):
        raise ProfileEvidenceError("receipt document must contain a receipt object")
    if receipt.get("status") != "completed":
        raise ProfileEvidenceError("receipt status must be completed")

    plan = receipt.get("plan")
    nodes = plan.get("nodes") if isinstance(plan, dict) else None
    if (
        not isinstance(nodes, list)
        or not nodes
        or not all(isinstance(node, dict) for node in nodes)
    ):
        raise ProfileEvidenceError("receipt plan must contain node objects")

    reads: list[tuple[dict[str, Any], int]] = []
    for node in nodes:
        node_id = node.get("node_id")
        if isinstance(node_id, str) and (
            matched := FINAL_MAJOR_READ.fullmatch(node_id)
        ):
            reads.append((node, int(matched.group(1))))
    if len(reads) != 1:
        raise ProfileEvidenceError(
            "receipt must contain exactly one final-major transaction-read node"
        )
    read, ordinal = reads[0]
    require_completed_node(read)

    minor_nodes = [node for node in nodes if node.get("node_id") == MINOR_CYCLE_NODE]
    if len(minor_nodes) > 1:
        raise ProfileEvidenceError("receipt contains duplicate minor-cycle nodes")
    minor = minor_nodes[0] if minor_nodes else None
    if minor is not None:
        require_completed_node(minor)

    started = non_negative_integer(
        receipt.get("started_unix_millis"), "started_unix_millis"
    )
    finished = non_negative_integer(
        receipt.get("finished_unix_millis"), "finished_unix_millis"
    )
    if finished < started:
        raise ProfileEvidenceError("receipt finish precedes its start")

    intermediate = minor is not None
    return {
        "attempt_identity": optional_string(receipt.get("attempt_identity")),
        "classification": "continuing_intermediate" if intermediate else "terminal",
        "minor_cycle_node_present": intermediate,
        "minor_cycle_seconds": node_seconds(minor) if minor is not None else None,
        "ordinal": ordinal,
        "plan_wall_seconds": (finished - started) / 1_000.0,
        "terminal_visibility_excluded": intermediate,
        "transaction_read_seconds": node_seconds(read),
    }


def require_completed_node(node: dict[str, Any]) -> None:
    if node.get("status") != "completed":
        raise ProfileEvidenceError(f"node {node.get('node_id')!r} must be completed")


def node_seconds(node: dict[str, Any]) -> float:
    nanos = non_negative_integer(
        node.get("actual_elapsed_nanos"), "actual_elapsed_nanos"
    )
    return nanos / 1_000_000_000.0


def parse_sample(text: str) -> dict[str, Any]:
    header = SAMPLE_HEADER.search(text)
    if header is None:
        raise ProfileEvidenceError("sample header is missing or unsupported")
    main_threads = list(MAIN_THREAD.finditer(text))
    if len(main_threads) != 1:
        raise ProfileEvidenceError("sample must contain exactly one main-thread header")
    main_thread_samples = positive_integer(
        int(main_threads[0].group("count")), "main-thread sample count"
    )

    marker = "Sort by top of stack, same collapsed"
    start = text.find(marker)
    end = text.find("Binary Images:", start + len(marker))
    if start < 0 or end < 0:
        raise ProfileEvidenceError("sample exclusive-leaf section is missing")

    counts: dict[tuple[str, str], int] = {}
    section = text[start:end].splitlines()[1:]
    for line in section:
        if not line.strip():
            continue
        matched = EXCLUSIVE_LEAF.fullmatch(line)
        if matched is None:
            raise ProfileEvidenceError(
                f"unsupported exclusive-leaf line: {line.strip()}"
            )
        key = (matched.group("symbol"), matched.group("image"))
        count = positive_integer(int(matched.group("count")), "exclusive leaf count")
        counts[key] = counts.get(key, 0) + count
    if not counts:
        raise ProfileEvidenceError("sample exclusive-leaf section is empty")

    leaves = [
        {
            "count": count,
            "count_per_main_thread_sample": count / main_thread_samples,
            "idle": symbol in IDLE_SYMBOLS,
            "image": image,
            "symbol": symbol,
        }
        for (symbol, image), count in counts.items()
    ]
    leaves.sort(key=lambda leaf: (-leaf["count"], leaf["symbol"], leaf["image"]))
    non_idle = [leaf for leaf in leaves if not leaf["idle"]]
    return {
        "exclusive_leaves": leaves,
        "exclusive_scope": "all_threads",
        "idle_excluded_symbols": sorted(IDLE_SYMBOLS),
        "main_thread_samples": main_thread_samples,
        "non_idle_exclusive_count": sum(leaf["count"] for leaf in non_idle),
        "non_idle_exclusive_leaves": non_idle,
        "process": header.group("process"),
        "process_id": positive_integer(int(header.group("pid")), "process id"),
        "sampling_interval_milliseconds": positive_integer(
            int(header.group("interval")), "sampling interval"
        ),
    }


def add_groups(sample: dict[str, Any], definitions: dict[str, Any]) -> dict[str, Any]:
    compiled: dict[str, list[re.Pattern[str]]] = {}
    for name, patterns in sorted(definitions.items()):
        if not isinstance(name, str) or not name.strip():
            raise ProfileEvidenceError("group names must be non-empty strings")
        if (
            not isinstance(patterns, list)
            or not patterns
            or not all(isinstance(pattern, str) and pattern for pattern in patterns)
        ):
            raise ProfileEvidenceError(f"group {name!r} must contain regex strings")
        try:
            compiled[name] = [re.compile(pattern) for pattern in patterns]
        except re.error as error:
            raise ProfileEvidenceError(
                f"group {name!r} has invalid regex: {error}"
            ) from error

    grouped = {name: {"count": 0, "symbols": []} for name in compiled}
    ungrouped = 0
    for leaf in sample["non_idle_exclusive_leaves"]:
        matches = [
            name
            for name, patterns in compiled.items()
            if any(pattern.search(leaf["symbol"]) for pattern in patterns)
        ]
        if len(matches) > 1:
            raise ProfileEvidenceError(
                f"exclusive leaf {leaf['symbol']!r} matches multiple groups: "
                + ", ".join(matches)
            )
        if not matches:
            ungrouped += leaf["count"]
            continue
        group = grouped[matches[0]]
        group["count"] += leaf["count"]
        group["symbols"].append(leaf["symbol"])

    denominator = sample["main_thread_samples"]
    sample["exclusive_groups"] = {
        name: {
            "count": group["count"],
            "count_per_main_thread_sample": group["count"] / denominator,
            "symbols": sorted(group["symbols"]),
        }
        for name, group in grouped.items()
    }
    sample["ungrouped_non_idle_exclusive_count"] = ungrouped
    return sample


def build_evidence(
    receipt: dict[str, Any], sample_text: str, groups: dict[str, Any] | None = None
) -> dict[str, Any]:
    parsed_sample = parse_sample(sample_text)
    if groups is not None:
        add_groups(parsed_sample, groups)
    return {
        "receipt": classify_receipt(receipt),
        "sample": parsed_sample,
        "schema_name": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
    }


def non_negative_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProfileEvidenceError(f"{name} must be a non-negative integer")
    return value


def positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ProfileEvidenceError(f"{name} must be a positive integer")
    return value


def optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipt", type=Path)
    parser.add_argument("sample", type=Path)
    parser.add_argument("--groups", type=Path)
    args = parser.parse_args(argv)

    try:
        receipt = load_json_object(args.receipt, "receipt")
        sample_text = args.sample.read_text(encoding="utf-8")
        groups = (
            load_json_object(args.groups, "groups") if args.groups is not None else None
        )
        evidence = build_evidence(receipt, sample_text, groups)
    except (OSError, ProfileEvidenceError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    json.dump(evidence, sys.stdout, indent=2, sort_keys=True, allow_nan=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
