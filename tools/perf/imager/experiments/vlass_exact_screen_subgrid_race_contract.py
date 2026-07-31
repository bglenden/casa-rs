#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Bind the VLASS inverse-CF subgrid race to frozen full16 evidence.

This tool runs neither imaging nor a benchmark. It corrects the earlier
cross-representation interaction-count rejection by translating the requested
whole-run speedup into an exact operator-time and throughput contract for one
target-hardware race.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any


EXPECTED_KIND = "vlass_architecture_tournament_audit"
EXPECTED_WORKLOAD_ID = "vlass-fragment-single-field-clean-4096-full-16-spw"
EXPECTED_SAMPLES = 385_862
EXPECTED_OPERATOR_CALLS = 11
SUBGRID_SIDE = 32
TARGET_TOTAL_SPEEDUP = 2.0
PROMOTION_OPERATOR_FRACTION = 0.20
PROMOTION_PEAK_BYTES = 12 * 1024**3
ABORT_PEAK_BYTES = 15_200_000_000


class RaceContractError(RuntimeError):
    """Raised when evidence cannot support the subgrid race contract."""


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp."""

    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    """Hash one file without loading it into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    """Read one JSON object."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise RaceContractError(f"cannot read architecture receipt {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise RaceContractError(
            f"architecture receipt is not valid JSON: {path}: {error}"
        ) from error
    if not isinstance(value, dict):
        raise RaceContractError("architecture receipt must contain a JSON object")
    return value


def require_dict(value: Any, *, label: str) -> dict[str, Any]:
    """Require a dictionary value."""

    if not isinstance(value, dict):
        raise RaceContractError(f"{label} must be an object")
    return value


def require_number(value: Any, *, label: str) -> float:
    """Require one finite numeric value."""

    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
    ):
        raise RaceContractError(f"{label} must be a finite number")
    return float(value)


def require_int(value: Any, *, label: str) -> int:
    """Require one integer value."""

    if not isinstance(value, int) or isinstance(value, bool):
        raise RaceContractError(f"{label} must be an integer")
    return value


def find_card(receipt: dict[str, Any], card_id: str) -> dict[str, Any]:
    """Find one architecture candidate card."""

    cards = receipt.get("candidate_cards")
    if not isinstance(cards, list):
        raise RaceContractError("candidate_cards must be an array")
    matches = [
        card
        for card in cards
        if isinstance(card, dict) and card.get("id") == card_id
    ]
    if len(matches) != 1:
        raise RaceContractError(f"expected exactly one {card_id} card")
    return matches[0]


def build_contract(receipt: dict[str, Any]) -> dict[str, Any]:
    """Build the target-hardware race contract from one frozen audit."""

    if receipt.get("kind") != EXPECTED_KIND:
        raise RaceContractError(f"receipt kind must be {EXPECTED_KIND}")
    workload = require_dict(receipt.get("workload"), label="workload")
    if workload.get("id") != EXPECTED_WORKLOAD_ID:
        raise RaceContractError(f"workload.id must be {EXPECTED_WORKLOAD_ID}")

    incumbent = require_dict(receipt.get("incumbent"), label="incumbent")
    completion = require_dict(incumbent.get("completion"), label="completion")
    trajectory = require_dict(
        incumbent.get("operator_trajectory"),
        label="operator_trajectory",
    )
    timing = require_dict(incumbent.get("timing"), label="timing")
    samples = require_int(completion.get("samples"), label="completion.samples")
    calls = require_int(
        trajectory.get("logical_expensive_operator_calls"),
        label="logical_expensive_operator_calls",
    )
    if samples != EXPECTED_SAMPLES:
        raise RaceContractError(f"expected {EXPECTED_SAMPLES} samples, got {samples}")
    if calls != EXPECTED_OPERATOR_CALLS:
        raise RaceContractError(f"expected {EXPECTED_OPERATOR_CALLS} calls, got {calls}")

    wall_seconds = require_number(
        timing.get("wall_seconds"),
        label="timing.wall_seconds",
    )
    incumbent_operator_seconds = (
        require_number(
            timing.get("operator_dominated_stage_ms"),
            label="timing.operator_dominated_stage_ms",
        )
        / 1000.0
    )
    fixed_seconds = wall_seconds - incumbent_operator_seconds
    if fixed_seconds <= 0.0:
        raise RaceContractError("fixed non-operator time must be positive")
    abort_operator_seconds = wall_seconds / TARGET_TOTAL_SPEEDUP - fixed_seconds
    promotion_operator_seconds = (
        incumbent_operator_seconds * PROMOTION_OPERATOR_FRACTION
    )
    if abort_operator_seconds <= 0.0:
        raise RaceContractError("requested total speedup leaves no operator budget")

    idg = find_card(receipt, "idg-image-domain-subgrid")
    side_sweep = idg.get("side_sweep")
    if not isinstance(side_sweep, list):
        raise RaceContractError("IDG side_sweep must be an array")
    side_matches = [
        row
        for row in side_sweep
        if isinstance(row, dict) and row.get("side") == SUBGRID_SIDE
    ]
    if len(side_matches) != 1:
        raise RaceContractError(f"expected exactly one L={SUBGRID_SIDE} row")
    side_row = side_matches[0]
    work_ratio = require_number(
        side_row.get("standalone_work_ratio"),
        label="L=32 standalone_work_ratio",
    )
    interactions = require_int(
        side_row.get("standalone_clean_interactions"),
        label="L=32 standalone_clean_interactions",
    )

    promotion_throughput = (
        work_ratio * incumbent_operator_seconds / promotion_operator_seconds
    )
    abort_throughput = (
        work_ratio * incumbent_operator_seconds / abort_operator_seconds
    )
    return {
        "schema_version": 1,
        "kind": "vlass_exact_screen_subgrid_race_contract",
        "status": "bounded-experiment-authorized-not-executed",
        "created_at": utc_now(),
        "role": {
            "runs_casa": False,
            "runs_imaging": False,
            "runs_benchmark": False,
            "development_evidence_only": True,
            "speedup_claim": False,
            "production_architecture_claim": False,
        },
        "decision": {
            "choice": "inverse-cf-throwaway-direct-subgrid-race",
            "purpose": "target-hardware viability discriminator only",
            "production_prerequisite_if_promoted": (
                "native physical A/WB/conjugate/POINTING screen generator"
            ),
            "source_correction": (
                "VLASS AWProject currently loads sampled UV CFs; only the separate "
                "analytic mosaic projector has a native image-domain screen path"
            ),
        },
        "frozen_workload": {
            "id": EXPECTED_WORKLOAD_ID,
            "samples": samples,
            "operator_calls": calls,
            "subgrid_side": SUBGRID_SIDE,
            "subgrid_interactions": interactions,
            "subgrid_to_incumbent_interaction_ratio": work_ratio,
        },
        "amdahl": {
            "incumbent_total_seconds": wall_seconds,
            "incumbent_operator_seconds": incumbent_operator_seconds,
            "fixed_non_operator_seconds": fixed_seconds,
            "target_total_speedup": TARGET_TOTAL_SPEEDUP,
            "abort_operator_seconds": abort_operator_seconds,
            "promotion_operator_seconds": promotion_operator_seconds,
            "promotion_total_seconds": fixed_seconds + promotion_operator_seconds,
            "promotion_total_speedup": (
                wall_seconds / (fixed_seconds + promotion_operator_seconds)
            ),
            "required_effective_throughput_multiplier_for_abort": abort_throughput,
            "required_effective_throughput_multiplier_for_promotion": (
                promotion_throughput
            ),
        },
        "executable_boundary": {
            "calls": (
                "replay the exact 11-call full16 operator multiset; no CLEAN, "
                "controller, restoration, product FFTs, writes, or CASA"
            ),
            "screen_source": (
                "IFFT of each exact sampled patch instance after its CF key, "
                "oversampling phase, conjugation, origin, and normalization are fixed"
            ),
            "timings": {
                "core": "preloaded screens; includes all operator setup and synchronization",
                "throwaway": "also includes bounded streaming inverse-CF construction",
            },
            "screen_ring_bytes_max": 512 * 1024**2,
            "persistent_state_must_not_scale_with": [
                "total sampled tap count",
                "total CF corpus",
                "field count",
            ],
        },
        "gates": {
            "promotion": {
                "core_seconds_max": promotion_operator_seconds,
                "normalized_rms_max": 3.0e-6,
                "normalized_peak_relative_max": 2.0e-5,
                "adjointness_error_max": 2.0e-6,
                "full_size_peak_bytes_max": PROMOTION_PEAK_BYTES,
            },
            "abort": {
                "core_seconds_above": abort_operator_seconds,
                "full_size_peak_bytes_above": ABORT_PEAK_BYTES,
            },
            "inconclusive": (
                "core time is between promotion and abort, or timing passes while "
                "the discrete-operator numerical gate fails"
            ),
        },
        "falsification_scope": {
            "can_falsify": (
                "full-rank per-sample direct-subgrid AW operators at L=32 under "
                "the actual VLASS signatures on this target hardware"
            ),
            "cannot_falsify": [
                "native physical-screen subgridding with smaller effective support",
                "low-rank or tensor operator factorization",
                "baseline, time, or frequency coalescing",
                "3D W-gridding or NUFFT",
                "reconstruction algorithms that eliminate operator calls",
            ],
        },
    }


def write_new_json(path: Path, value: dict[str, Any]) -> None:
    """Create a receipt without replacing prior evidence."""

    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(value, indent=2, sort_keys=True) + "\n"
    try:
        with path.open("x", encoding="utf-8") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as error:
        raise RaceContractError(f"refusing to replace existing receipt: {path}") from error


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--architecture-receipt", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Build one content-bound race contract."""

    args = parse_args(argv)
    receipt_path = args.architecture_receipt.expanduser().resolve()
    try:
        receipt = load_json(receipt_path)
        contract = build_contract(receipt)
        contract["inputs"] = {
            "contract_source": {
                "path": str(Path(__file__).resolve()),
                "sha256": sha256_file(Path(__file__).resolve()),
            },
            "architecture_receipt": {
                "path": str(receipt_path),
                "sha256": sha256_file(receipt_path),
            },
        }
        if args.output is None:
            print(json.dumps(contract, indent=2, sort_keys=True))
        else:
            write_new_json(args.output, contract)
            print(
                json.dumps(
                    {
                        "status": contract["status"],
                        "output": str(args.output.expanduser().resolve()),
                        "sha256": sha256_file(args.output.expanduser().resolve()),
                    },
                    sort_keys=True,
                )
            )
    except (OSError, RaceContractError) as error:
        print(f"error: {error}", file=os.sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
