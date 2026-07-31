#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused tests for the VLASS exact-screen subgrid race contract."""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
import sys
import unittest


MODULE_PATH = Path(__file__).with_name(
    "vlass_exact_screen_subgrid_race_contract.py"
)
SPEC = importlib.util.spec_from_file_location("vlass_subgrid_race_contract", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
contract = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = contract
SPEC.loader.exec_module(contract)


def synthetic_receipt() -> dict[str, object]:
    """Return the minimal frozen architecture evidence."""

    return {
        "kind": "vlass_architecture_tournament_audit",
        "workload": {
            "id": "vlass-fragment-single-field-clean-4096-full-16-spw",
        },
        "incumbent": {
            "completion": {
                "samples": 385_862,
            },
            "operator_trajectory": {
                "logical_expensive_operator_calls": 11,
            },
            "timing": {
                "wall_seconds": 42.91,
                "operator_dominated_stage_ms": 28_239.768,
            },
        },
        "candidate_cards": [
            {
                "id": "idg-image-domain-subgrid",
                "side_sweep": [
                    {
                        "side": 32,
                        "standalone_clean_interactions": 4_346_349_568,
                        "standalone_work_ratio": 1.1997394161367814,
                    }
                ],
            }
        ],
    }


class VlassExactScreenSubgridRaceContractTest(unittest.TestCase):
    def test_backpropagates_end_to_end_target(self) -> None:
        result = contract.build_contract(synthetic_receipt())
        amdahl = result["amdahl"]
        self.assertAlmostEqual(amdahl["fixed_non_operator_seconds"], 14.670232)
        self.assertAlmostEqual(amdahl["abort_operator_seconds"], 6.784768)
        self.assertAlmostEqual(amdahl["promotion_operator_seconds"], 5.6479536)
        self.assertAlmostEqual(
            amdahl["required_effective_throughput_multiplier_for_abort"],
            4.993591935959807,
        )
        self.assertAlmostEqual(
            amdahl["required_effective_throughput_multiplier_for_promotion"],
            5.998697080683907,
        )

    def test_preserves_discriminator_claim_boundary(self) -> None:
        result = contract.build_contract(synthetic_receipt())
        self.assertEqual(
            result["decision"]["choice"],
            "inverse-cf-throwaway-direct-subgrid-race",
        )
        self.assertFalse(result["role"]["production_architecture_claim"])
        self.assertIn(
            "low-rank or tensor operator factorization",
            result["falsification_scope"]["cannot_falsify"],
        )

    def test_rejects_changed_sample_contract(self) -> None:
        receipt = copy.deepcopy(synthetic_receipt())
        receipt["incumbent"]["completion"]["samples"] += 1
        with self.assertRaisesRegex(contract.RaceContractError, "expected 385862"):
            contract.build_contract(receipt)

    def test_rejects_missing_l32_candidate(self) -> None:
        receipt = copy.deepcopy(synthetic_receipt())
        receipt["candidate_cards"][0]["side_sweep"][0]["side"] = 48
        with self.assertRaisesRegex(contract.RaceContractError, "L=32"):
            contract.build_contract(receipt)


if __name__ == "__main__":
    unittest.main()
