#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Reduce the VLASS mask-local sufficient-statistics feasibility census.

The Rust diagnostic binds the real weighted UVW stream to the promoted
4096-square full-16-SPW trajectory receipt. It reports both a constructive
axis-aligned clustering and a phase-space packing lower bound. This reducer may
retire the architecture from that lower bound, but it cannot promote a
production approximation without held-out DDE error and a matched Metal race.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
from typing import Any


AUDIT_EVENT = "awproject_mask_sufficient_statistics_census"
EXPECTED_ORDERS = [2, 3]
EXPECTED_RANKS = [1, 2, 3, 4]
DEFAULT_EXPECTED_SAMPLES = 385_862
DEFAULT_EXPECTED_COMPONENT_UPDATES = 641
DEFAULT_EXPECTED_TRAJECTORY_SHA256 = (
    "f06859c9215a26b15dd32731345b9fdb1aaf1ab0fc267938638dd016b99518a1"
)


class MaskSufficientStatisticsError(RuntimeError):
    """Raised when a census log is incomplete or changes its contract."""


def utc_now() -> str:
    """Return a stable UTC timestamp."""

    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    """Hash one file without loading it into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _required(line: str, key: str, pattern: str) -> str:
    match = re.search(rf"(?:^|\s){re.escape(key)}=({pattern})(?:\s|$)", line)
    if match is None:
        raise MaskSufficientStatisticsError(f"{AUDIT_EVENT} lacks {key}")
    return match.group(1)


def _required_int(line: str, key: str) -> int:
    return int(_required(line, key, r"[0-9]+"))


def _optional_int(line: str, key: str, default: int = 0) -> int:
    match = re.search(rf"(?:^|\s){re.escape(key)}=([0-9]+)(?:\s|$)", line)
    return default if match is None else int(match.group(1))


def _required_float(line: str, key: str) -> float:
    return float(
        _required(
            line,
            key,
            r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?",
        )
    )


def _required_token(line: str, key: str) -> str:
    return _required(line, key, r"\S+")


def _parse_event(line: str) -> dict[str, Any]:
    event: dict[str, Any] = {
        key: _required_int(line, key)
        for key in (
            "order",
            "rank",
            "samples",
            "windows",
            "source_states",
            "nterms",
            "clean_mask_pixels",
            "domain_pixels",
            "upper_clusters",
            "upper_group_cluster_pairs",
            "packing_lower_clusters",
            "shift_code",
            "moment_count",
            "cross_moment_count",
            "feature_lower",
            "feature_upper",
            "cross_moments_upper",
            "state_bytes_upper",
            "feature_limit",
            "state_limit_bytes",
            "component_updates",
            "operator_calls_retained",
            "operator_calls_targeted",
        )
    }
    for key in (
        "phase_error_budget",
        "eta",
        "remainder_bound",
        "weight_sum",
        "weight_sq_sum",
        "weight_min",
        "weight_max",
        "frequency_min_hz",
        "frequency_max_hz",
    ):
        event[key] = _required_float(line, key)
    for key in (
        "scales",
        "domain_bounds",
        "max_kernel_offsets",
        "mask_fingerprint",
        "center_lmn",
        "delta_lmn_extents",
        "trajectory_receipt_sha256",
        "factoring_contract",
        "component_contract",
        "error_contract",
        "role",
    ):
        event[key] = _required_token(line, key)
    event["greedy_packing_lower_clusters"] = _optional_int(
        line, "greedy_packing_lower_clusters"
    )
    event["residue_packing_lower_clusters"] = _optional_int(
        line, "residue_packing_lower_clusters", event["packing_lower_clusters"]
    )
    return event


def analyze_log(
    text: str,
    *,
    expected_samples: int = DEFAULT_EXPECTED_SAMPLES,
    expected_component_updates: int = DEFAULT_EXPECTED_COMPONENT_UPDATES,
    expected_trajectory_sha256: str = DEFAULT_EXPECTED_TRAJECTORY_SHA256,
) -> dict[str, Any]:
    """Validate the complete curve and make only a bound-supported decision."""

    events = [
        _parse_event(line)
        for line in text.splitlines()
        if line.startswith(f"{AUDIT_EVENT} ")
    ]
    if not events:
        raise MaskSufficientStatisticsError(f"log contains no {AUDIT_EVENT} events")
    events.sort(key=lambda event: (event["order"], event["rank"]))
    expected_pairs = [
        (order, rank) for order in EXPECTED_ORDERS for rank in EXPECTED_RANKS
    ]
    pairs = [(event["order"], event["rank"]) for event in events]
    if pairs != expected_pairs:
        raise MaskSufficientStatisticsError(
            f"order/rank curve must be {expected_pairs}, got {pairs}"
        )

    invariant_keys = (
        "samples",
        "windows",
        "source_states",
        "nterms",
        "scales",
        "clean_mask_pixels",
        "domain_pixels",
        "domain_bounds",
        "max_kernel_offsets",
        "mask_fingerprint",
        "center_lmn",
        "delta_lmn_extents",
        "phase_error_budget",
        "feature_limit",
        "state_limit_bytes",
        "component_updates",
        "trajectory_receipt_sha256",
        "weight_sum",
        "weight_sq_sum",
        "weight_min",
        "weight_max",
        "frequency_min_hz",
        "frequency_max_hz",
        "operator_calls_retained",
        "operator_calls_targeted",
        "factoring_contract",
        "component_contract",
        "error_contract",
        "role",
    )
    first = events[0]
    for event in events:
        for key in invariant_keys:
            if event[key] != first[key]:
                raise MaskSufficientStatisticsError(
                    f"census changes invariant {key} across its curve"
                )
        if event["remainder_bound"] > event["phase_error_budget"] * (1.0 + 1.0e-12):
            raise MaskSufficientStatisticsError(
                f"order {event['order']} exceeds its scalar phase error budget"
            )
        if event["feature_lower"] > event["feature_upper"]:
            raise MaskSufficientStatisticsError(
                f"order {event['order']} rank {event['rank']} has an invalid bound"
            )
    if first["samples"] != expected_samples:
        raise MaskSufficientStatisticsError(
            f"census has {first['samples']} samples, expected {expected_samples}"
        )
    if first["nterms"] != 2 or first["scales"] != "0,5,12":
        raise MaskSufficientStatisticsError("census changed MT-MFS nterms/scales")
    if first["component_updates"] != expected_component_updates:
        raise MaskSufficientStatisticsError("census changed the frozen component count")
    if first["trajectory_receipt_sha256"] != expected_trajectory_sha256.lower():
        raise MaskSufficientStatisticsError("census changed the trajectory receipt")
    if first["operator_calls_retained"] != 3 or first["operator_calls_targeted"] != 8:
        raise MaskSufficientStatisticsError("census changed the 3+8 operator contract")

    rank_one = [event for event in events if event["rank"] == 1]
    rigorously_killed_orders = [
        event["order"]
        for event in rank_one
        if event["feature_lower"] > event["feature_limit"]
    ]
    constructive_survivors = [
        event
        for event in events
        if event["feature_upper"] <= event["feature_limit"]
        and event["state_bytes_upper"] <= event["state_limit_bytes"]
    ]
    if rigorously_killed_orders == EXPECTED_ORDERS:
        decision = "retire-order-2-3-uvw-polynomial-mask-moments"
        next_step = "advance-weighted-low-rank-awb-after-exact-symmetry-quotient"
    elif constructive_survivors:
        decision = "promote-held-out-low-rank-and-metal-response-race"
        next_step = "measure-dde-rank-error-then-race-fixed-641-component-trajectory"
    else:
        decision = "inconclusive-tighten-adaptive-clustering"
        next_step = "seek-tighter-clustering-bound-before-low-rank-fallback"

    return {
        "role": "source-feasibility-and-phase-space-bound-not-performance-or-science-evidence",
        "evidence_class": "measured-weighted-uvw-occupancy-with-analytic-phase-bounds",
        "contract": {
            "imsize": 4096,
            "spws": "2~17",
            "gridder": "awproject",
            "wprojplanes": 32,
            "nterms": first["nterms"],
            "scales": [0, 5, 12],
            "samples": first["samples"],
            "windows": first["windows"],
            "source_states": first["source_states"],
            "clean_mask_pixels": first["clean_mask_pixels"],
            "scale_dilated_domain_pixels": first["domain_pixels"],
            "domain_bounds": first["domain_bounds"],
            "mask_fingerprint": first["mask_fingerprint"],
            "component_updates": first["component_updates"],
            "trajectory_receipt_sha256": first["trajectory_receipt_sha256"],
            "operator_calls_retained": first["operator_calls_retained"],
            "operator_calls_targeted": first["operator_calls_targeted"],
        },
        "weighting": {
            "weight_sum": first["weight_sum"],
            "weight_sq_sum": first["weight_sq_sum"],
            "weight_min": first["weight_min"],
            "weight_max": first["weight_max"],
            "frequency_min_hz": first["frequency_min_hz"],
            "frequency_max_hz": first["frequency_max_hz"],
        },
        "curve": events,
        "selection": {
            "decision": decision,
            "next_step": next_step,
            "rigorously_killed_orders_at_rank_one": rigorously_killed_orders,
            "constructive_survivors": [
                {"order": event["order"], "rank": event["rank"]}
                for event in constructive_survivors
            ],
        },
        "claim_boundary": (
            "The packing count is a valid lower bound for the stated scalar "
            "Taylor phase budget. A survivor still lacks measured DDE low-rank "
            "error, fixed-trajectory execution time, mask-gradient time, and "
            "end-to-end scientific comparison."
        ),
    }


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-samples", type=int, default=DEFAULT_EXPECTED_SAMPLES)
    parser.add_argument(
        "--expected-component-updates",
        type=int,
        default=DEFAULT_EXPECTED_COMPONENT_UPDATES,
    )
    parser.add_argument(
        "--expected-trajectory-sha256",
        default=DEFAULT_EXPECTED_TRAJECTORY_SHA256,
    )
    return parser.parse_args()


def main() -> int:
    """Write one immutable census receipt."""

    args = parse_args()
    result = analyze_log(
        args.log.read_text(encoding="utf-8"),
        expected_samples=args.expected_samples,
        expected_component_updates=args.expected_component_updates,
        expected_trajectory_sha256=args.expected_trajectory_sha256,
    )
    result["generated_at"] = utc_now()
    result["inputs"] = {
        "log": str(args.log.resolve()),
        "log_sha256": sha256_file(args.log),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
