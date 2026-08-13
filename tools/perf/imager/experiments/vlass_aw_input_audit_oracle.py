#!/usr/bin/env python3
"""Seal and summarize a no-grid, all-block VLASS AW input-audit receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def fields(line: str) -> tuple[str, dict[str, str]]:
    parts = line.rstrip("\n").split("\t")
    parsed = dict(part.split("=", 1) for part in parts[1:])
    return parts[0], parsed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("receipt", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    payload = args.receipt.read_bytes()
    parsed = [fields(line) for line in payload.decode("utf-8").splitlines()]
    if not parsed or parsed[0][0] != "meta":
        raise SystemExit("receipt has no metadata row")
    metadata = parsed[0][1]
    windows = [row for kind, row in parsed[1:] if kind == "window"]
    blocks = [row for kind, row in parsed[1:] if kind == "block"]
    mismatches = [row for kind, row in parsed[1:] if kind == "mismatch"]
    expected_blocks = int(metadata["expected_blocks"])
    if len(blocks) != expected_blocks:
        raise SystemExit(f"found {len(blocks)} blocks, expected {expected_blocks}")
    if mismatches:
        raise SystemExit(f"receipt contains {len(mismatches)} mismatches")
    if any(row["raw_hash"] != row["compact_hash"] for row in windows + blocks):
        raise SystemExit("at least one raw/compact hash differs")
    if metadata["grid_dispatch"] != "skipped":
        raise SystemExit("receipt does not prove skipped grid dispatch")

    block_rows = [
        {
            "block": int(row["block"]),
            "window_count": int(row["window_count"]),
            "source_count": int(row["source_count"]),
            "role_count": int(row["role_count"]),
            "phased_tap_count": int(row["tap_count"]),
            "raw_hash": int(row["raw_hash"]),
            "compact_hash": int(row["compact_hash"]),
            "exact_match": row["exact_match"] == "true",
        }
        for row in blocks
    ]
    summary = {
        "schema": "casa-rs-vlass-aw-input-audit-v1",
        "receipt": str(args.receipt),
        "receipt_sha256": hashlib.sha256(payload).hexdigest(),
        "scope": {
            "formed_image": metadata["formed_image"] == "true",
            "ran_casa_tclean": metadata["ran_casa_tclean"] == "true",
            "grid_dispatch": metadata["grid_dispatch"],
            "row_provenance": metadata["row_provenance"],
            "channel_provenance": metadata["channel_provenance"],
            "block_count": len(block_rows),
            "window_count": len(windows),
            "source_count": sum(row["source_count"] for row in block_rows),
            "role_count": sum(row["role_count"] for row in block_rows),
            "phased_tap_count": sum(row["phased_tap_count"] for row in block_rows),
        },
        "source_oracle": {
            "casa_git_commit": "61020062cee290f5466cffed5ec5032e0c7a3434",
            "data_to_grid_order": "row-channel-polarization-mueller-y-x",
            "contribution_arithmetic": "Complex<Float> product promoted into DComplex grid",
        },
        "hash_contracts": {
            "tap": metadata["tap_hash_contract"],
            "block": metadata["block_hash_contract"],
        },
        "blocks": block_rows,
        "all_blocks_exact": all(row["exact_match"] for row in block_rows),
        "first_mismatch": None,
    }
    rendered = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
