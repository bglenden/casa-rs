#!/usr/bin/env python3
"""Seal and summarize a bounded VLASS AW residual-prefix TSV receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import struct
from pathlib import Path
from typing import Any


def parse_fields(line: str) -> tuple[str, dict[str, str]]:
    fields = line.rstrip("\n").split("\t")
    parsed: dict[str, str] = {}
    for field in fields[1:]:
        key, separator, value = field.partition("=")
        if not separator:
            raise ValueError(f"malformed field {field!r}")
        parsed[key] = value
    return fields[0], parsed


def f64_from_bits(bits: str) -> float:
    return struct.unpack("<d", int(bits).to_bytes(8, "little"))[0]


def ordered_f64_bits(bits: str) -> int:
    value = int(bits)
    if value & (1 << 63):
        return (~value) & ((1 << 64) - 1)
    return value | (1 << 63)


def ulp_distance(left: str, right: str) -> int:
    return abs(ordered_f64_bits(left) - ordered_f64_bits(right))


def first_difference(
    rows: list[dict[str, str]],
    left_re: str,
    left_im: str,
    right_re: str,
    right_im: str,
) -> dict[str, Any] | None:
    for row in rows:
        if row[left_re] != row[right_re] or row[left_im] != row[right_im]:
            return {
                "prefix": int(row["prefix"]),
                "block": int(row["block"]),
                "source": int(row["source"]),
                "row": row["row"],
                "channel": row["channel"],
                "planned": int(row["planned"]),
                "group": int(row["group"]),
                "role": int(row["role"]),
                "tap": int(row["tap"]),
                "ix": int(row["ix"]),
                "iy": int(row["iy"]),
                "left_re_bits": int(row[left_re]),
                "left_im_bits": int(row[left_im]),
                "right_re_bits": int(row[right_re]),
                "right_im_bits": int(row[right_im]),
            }
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("receipt", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    payload = args.receipt.read_bytes()
    lines = payload.decode("utf-8").splitlines()
    if len(lines) < 3:
        raise SystemExit("receipt has no contribution prefix")
    input_kind, input_meta = parse_fields(lines[0])
    meta_kind, metadata = parse_fields(lines[1])
    contributions = [parse_fields(line) for line in lines[2:]]
    if input_kind != "input-meta" or meta_kind != "meta":
        raise SystemExit("receipt does not start with input-meta and meta rows")
    if any(kind != "contribution" for kind, _ in contributions):
        raise SystemExit("receipt contains an unexpected row kind")
    rows = [row for _, row in contributions]

    expected_count = int(metadata["contribution_count"])
    if len(rows) != expected_count:
        raise SystemExit(
            f"receipt has {len(rows)} contributions, expected {expected_count}"
        )
    if input_meta["exact_match"] != "true":
        raise SystemExit("direct/raw and compact input streams are not exact")

    casa_host_product = first_difference(
        rows,
        "casa_product_re_bits",
        "casa_product_im_bits",
        "host_product_re_bits",
        "host_product_im_bits",
    )
    casa_host_accumulator = first_difference(
        rows,
        "casa_acc_re_bits",
        "casa_acc_im_bits",
        "host_acc_re_bits",
        "host_acc_im_bits",
    )
    casa_metal_accumulator = first_difference(
        rows,
        "casa_acc_re_bits",
        "casa_acc_im_bits",
        "metal_acc_re_bits",
        "metal_acc_im_bits",
    )
    if casa_host_product is not None or casa_host_accumulator is not None:
        raise SystemExit("HostF64 diverged from the CASA Complex<Float> oracle")
    if casa_metal_accumulator is None:
        raise SystemExit("selected prefix did not expose fixed64 scale-round-readback")

    final = rows[-1]
    casa = complex(
        f64_from_bits(final["casa_acc_re_bits"]),
        f64_from_bits(final["casa_acc_im_bits"]),
    )
    host = complex(
        f64_from_bits(final["host_acc_re_bits"]),
        f64_from_bits(final["host_acc_im_bits"]),
    )
    metal = complex(
        f64_from_bits(final["metal_acc_re_bits"]),
        f64_from_bits(final["metal_acc_im_bits"]),
    )
    summary = {
        "schema": "casa-rs-vlass-aw-residual-prefix-v1",
        "receipt": str(args.receipt),
        "receipt_sha256": hashlib.sha256(payload).hexdigest(),
        "scope": {
            "formed_image": False,
            "ran_casa_tclean": False,
            "replay_block": int(input_meta["block"]),
            "source_count": int(input_meta["source_count"]),
            "role_count": int(input_meta["role_count"]),
            "phased_tap_count": int(input_meta["tap_count"]),
            "row_provenance": input_meta["row_provenance"],
            "channel_provenance": input_meta["channel_provenance"],
        },
        "source_oracle": {
            "casa_git_commit": "61020062cee290f5466cffed5ec5032e0c7a3434",
            "data_to_grid_order": "row-channel-polarization-mueller-y-x",
            "contribution_arithmetic": "Complex<Float> product promoted into DComplex grid",
        },
        "input_stream": {
            "raw_hash": int(input_meta["raw_hash"]),
            "compact_hash": int(input_meta["compact_hash"]),
            "exact_match": input_meta["raw_hash"] == input_meta["compact_hash"],
            "tap_hash_contract": input_meta["tap_hash_contract"],
        },
        "selected_prefix": {
            "grid_cell": [
                int(metadata["selected_grid_cell_x"]),
                int(metadata["selected_grid_cell_y"]),
            ],
            "contribution_count": expected_count,
            "cancellation_ratio": float(metadata["cancellation_ratio"]),
            "fixed_scale_bits": int(metadata["fixed_scale_bits"]),
            "fixed_inverse_scale_bits": int(metadata["fixed_inverse_scale_bits"]),
            "first_casa_host_product_divergence": casa_host_product,
            "first_casa_host_accumulator_divergence": casa_host_accumulator,
            "first_casa_metal_accumulator_divergence": casa_metal_accumulator,
        },
        "final_prefix": {
            "ordinal": int(final["prefix"]),
            "casa_bits": [
                int(final["casa_acc_re_bits"]),
                int(final["casa_acc_im_bits"]),
            ],
            "host_bits": [
                int(final["host_acc_re_bits"]),
                int(final["host_acc_im_bits"]),
            ],
            "metal_bits": [
                int(final["metal_acc_re_bits"]),
                int(final["metal_acc_im_bits"]),
            ],
            "casa_host_absolute_error": abs(host - casa),
            "casa_metal_absolute_error": abs(metal - casa),
            "casa_metal_relative_error": abs(metal - casa)
            / max(abs(casa), float.fromhex("0x1p-1022")),
            "casa_metal_re_ulp_distance": ulp_distance(
                final["casa_acc_re_bits"], final["metal_acc_re_bits"]
            ),
            "casa_metal_im_ulp_distance": ulp_distance(
                final["casa_acc_im_bits"], final["metal_acc_im_bits"]
            ),
        },
        "interpretation": {
            "raw_compact_inputs_excluded": True,
            "host_f64_accumulation_excluded_for_selected_prefix": True,
            "fixed64_quantization_is_secondary_delta": True,
            "selected_prefix_metal_error_matches_metadata": math.isclose(
                abs(metal - casa),
                float(metadata["metal_difference"]),
                rel_tol=1e-12,
                abs_tol=0.0,
            ),
        },
    }
    rendered = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(rendered, end="")
    else:
        args.output.write_text(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
