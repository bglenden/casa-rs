#!/usr/bin/env python3

from __future__ import annotations

import sys
import unittest
from pathlib import Path


EXPERIMENTS = Path(__file__).resolve().parent
sys.path.insert(0, str(EXPERIMENTS))

import vlass_aw_datatogrid_value_hash_compare as subject  # noqa: E402


def casa_receipt(checkpoints: list[dict[str, int]]) -> dict:
    return {
        "schema": subject.CASA_SCHEMA,
        "status": "completed-before-grid",
        "source_count": 2,
        "role_count": 4,
        "hashes": {"value": checkpoints[-1]["value"]},
        "checkpoints": checkpoints,
    }


def casars_receipt(checkpoints: list[dict[str, int]]) -> dict:
    return {
        "schema": subject.CASARS_SCHEMA,
        "casa_datatogrid_tt0_value_boundary": {
            "contract": subject.VALUE_CONTRACT,
            "source_count": 2,
            "role_count": 4,
            "value_hash": checkpoints[-1]["value"],
            "checkpoints": checkpoints,
        },
    }


class ValueHashCompareTests(unittest.TestCase):
    def test_exact_stream_passes(self) -> None:
        checkpoints = [{"sources": 1, "value": 10}, {"sources": 2, "value": 20}]

        result = subject.compare(
            casa_receipt(checkpoints),
            casars_receipt(checkpoints),
        )

        self.assertTrue(result["passed"])
        self.assertEqual(result["classification"], "residual-value-stream-exact")

    def test_reports_first_mismatching_source(self) -> None:
        casa = [{"sources": 1, "value": 10}, {"sources": 2, "value": 20}]
        casars = [{"sources": 1, "value": 10}, {"sources": 2, "value": 21}]

        result = subject.compare(casa_receipt(casa), casars_receipt(casars))

        self.assertFalse(result["passed"])
        self.assertEqual(result["value"]["first_mismatch_source"], 2)
        self.assertEqual(result["value"]["previous_matching_source"], 1)


if __name__ == "__main__":
    unittest.main()
