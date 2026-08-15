#!/usr/bin/env python3
"""Focused tests for the compact AW CF metadata comparator."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("vlass_aw_cf_metadata_compare.py")
SPEC = importlib.util.spec_from_file_location("vlass_aw_cf_metadata_compare", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def record(*, source: int, role: int, frequency: float = 2.0e9) -> tuple:
    return (
        source,
        role,
        frequency,
        -120.0,
        0 if role == 0 else 15,
        4.0,
        100,
        200,
        -1,
        2,
        1,
        9,
        9,
        0.5,
        -0.25,
    )


class CfMetadataCompareTest(unittest.TestCase):
    def test_exact_metadata_passes_selector_contract(self) -> None:
        values = [record(source=0, role=0), record(source=0, role=1)]
        result = MODULE.compare(values, values, set())
        self.assertTrue(result["topology_exact"])
        self.assertEqual(
            result["classification"], "cf-selector-and-placement-exact"
        )
        self.assertEqual(result["normalization"]["normalized_rms"], 0.0)

    def test_frequency_mismatch_is_correlated_with_value_divergence(self) -> None:
        reference = [record(source=0, role=0), record(source=0, role=1)]
        candidate = [
            record(source=0, role=0, frequency=2.1e9),
            record(source=0, role=1),
        ]
        result = MODULE.compare(reference, candidate, {0})
        field = result["fields"]["cell_frequency_hz"]
        self.assertEqual(field["mismatch_count"], 1)
        self.assertEqual(field["mismatch_at_value_divergent_source_count"], 1)
        self.assertEqual(
            result["classification"], "cf-selector-or-placement-divergence"
        )


if __name__ == "__main__":
    unittest.main()
