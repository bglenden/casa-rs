#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused tests for the VLASS subgrid support audit."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest


MODULE_PATH = Path(__file__).with_name("vlass_subgrid_support_audit.py")
SPEC = importlib.util.spec_from_file_location("vlass_subgrid_support_audit", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
audit = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = audit
SPEC.loader.exec_module(audit)


def synthetic_event(
    *,
    block: int,
    samples: int,
    initial_histogram: list[int],
    prediction_histogram: list[int],
    adjoint_histogram: list[int],
) -> str:
    """Return one internally consistent support event."""

    def interactions(histogram: list[int]) -> int:
        return sum(
            references * side**2
            for references, side in zip(
                histogram,
                audit.EXPECTED_SIDES,
                strict=True,
            )
        )

    initial_refs = sum(initial_histogram)
    prediction_refs = sum(prediction_histogram)
    adjoint_refs = sum(adjoint_histogram)
    return (
        f"awproject_subgrid_support_audit block={block} window=0 "
        f"samples={samples} sides=32,48,64,96,128 "
        f"initial_plan_references={initial_refs} "
        f"initial_plan_references_by_side={','.join(map(str, initial_histogram))} "
        f"initial_tap_interactions={initial_refs * 25} "
        f"initial_subgrid_interactions={interactions(initial_histogram)} "
        f"prediction_plan_references={prediction_refs} "
        f"prediction_plan_references_by_side={','.join(map(str, prediction_histogram))} "
        f"prediction_tap_interactions={prediction_refs * 25} "
        f"prediction_subgrid_interactions={interactions(prediction_histogram)} "
        f"adjoint_plan_references={adjoint_refs} "
        f"adjoint_plan_references_by_side={','.join(map(str, adjoint_histogram))} "
        f"adjoint_tap_interactions={adjoint_refs * 25} "
        f"adjoint_subgrid_interactions={interactions(adjoint_histogram)} "
        "max_patch_width=97"
    )


def synthetic_log() -> str:
    """Return a two-window, mixed-side trajectory."""

    return "\n".join(
        (
            synthetic_event(
                block=0,
                samples=6,
                initial_histogram=[48, 24, 12, 6, 6],
                prediction_histogram=[12, 6, 3, 2, 1],
                adjoint_histogram=[12, 6, 3, 2, 1],
            ),
            synthetic_event(
                block=1,
                samples=4,
                initial_histogram=[32, 16, 8, 4, 4],
                prediction_histogram=[8, 4, 2, 1, 1],
                adjoint_histogram=[8, 4, 2, 1, 1],
            ),
        )
    )


class VlassSubgridSupportAuditTest(unittest.TestCase):
    def test_aggregates_exact_call_multiplicities_and_mixed_sides(self) -> None:
        result = audit.analyze_log(synthetic_log(), expected_samples=10)
        self.assertEqual(result["coverage"]["max_patch_width"], 97)
        self.assertEqual(result["full_trajectory"]["plan_references"], 560)
        self.assertEqual(
            result["full_trajectory"]["plan_references_by_side"],
            [280, 140, 70, 40, 30],
        )
        self.assertFalse(result["selection"]["constant_l32_is_valid"])
        self.assertGreater(
            result["full_trajectory"]["subgrid_to_tap_interaction_ratio"],
            1.0,
        )

    def test_rejects_changed_sample_contract(self) -> None:
        with self.assertRaisesRegex(audit.SubgridSupportAuditError, "expected 11"):
            audit.analyze_log(synthetic_log(), expected_samples=11)

    def test_rejects_wrong_plan_multiplicity(self) -> None:
        text = synthetic_log().replace(
            "initial_plan_references=96",
            "initial_plan_references=95",
            1,
        ).replace(
            "initial_plan_references_by_side=48,24,12,6,6",
            "initial_plan_references_by_side=47,24,12,6,6",
            1,
        )
        with self.assertRaisesRegex(audit.SubgridSupportAuditError, "initial has"):
            audit.analyze_log(text, expected_samples=10)

    def test_rejects_histogram_work_mismatch(self) -> None:
        text = synthetic_log().replace(
            "initial_subgrid_interactions=",
            "initial_subgrid_interactions=1 ignored=",
            1,
        )
        with self.assertRaisesRegex(
            audit.SubgridSupportAuditError,
            "does not reproduce",
        ):
            audit.analyze_log(text, expected_samples=10)


if __name__ == "__main__":
    unittest.main()
