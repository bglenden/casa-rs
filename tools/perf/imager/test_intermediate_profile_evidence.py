#!/usr/bin/env python3
"""Focused tests for intermediate-major profile evidence."""

from __future__ import annotations

import contextlib
import copy
import io
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import intermediate_profile_evidence as evidence


FIXTURES = Path(__file__).resolve().parent / "fixtures"
INTERMEDIATE_RECEIPT = FIXTURES / "intermediate_profile_receipt.json"
TERMINAL_RECEIPT = FIXTURES / "terminal_profile_receipt.json"
SAMPLE = FIXTURES / "intermediate_profile_sample.txt"
GROUPS = FIXTURES / "intermediate_profile_groups.json"
ISSUE540_GROUPS = FIXTURES / "issue540_intermediate_profile_groups.json"


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


class IntermediateProfileEvidenceTests(unittest.TestCase):
    def test_minor_cycle_node_classifies_completed_receipt_as_intermediate(
        self,
    ) -> None:
        result = evidence.classify_receipt(load(INTERMEDIATE_RECEIPT))

        self.assertEqual("continuing_intermediate", result["classification"])
        self.assertEqual(1, result["ordinal"])
        self.assertTrue(result["minor_cycle_node_present"])
        self.assertTrue(result["terminal_visibility_excluded"])
        self.assertAlmostEqual(153.025, result["plan_wall_seconds"])
        self.assertAlmostEqual(152.843308875, result["transaction_read_seconds"])
        self.assertAlmostEqual(0.098164792, result["minor_cycle_seconds"])

    def test_absent_minor_cycle_node_classifies_terminal_receipt(self) -> None:
        result = evidence.classify_receipt(load(TERMINAL_RECEIPT))

        self.assertEqual("terminal", result["classification"])
        self.assertEqual(2, result["ordinal"])
        self.assertFalse(result["minor_cycle_node_present"])
        self.assertFalse(result["terminal_visibility_excluded"])
        self.assertIsNone(result["minor_cycle_seconds"])

    def test_incomplete_receipt_fails_closed(self) -> None:
        receipt = copy.deepcopy(load(INTERMEDIATE_RECEIPT))
        receipt["receipt"]["status"] = "running"

        with self.assertRaisesRegex(
            evidence.ProfileEvidenceError, "status must be completed"
        ):
            evidence.classify_receipt(receipt)

    def test_sample_preserves_exclusive_counts_and_excludes_idle_ranking(self) -> None:
        result = evidence.parse_sample(SAMPLE.read_text(encoding="utf-8"))

        self.assertEqual(3172, result["main_thread_samples"])
        self.assertEqual("all_threads", result["exclusive_scope"])
        self.assertEqual(29569, result["process_id"])
        self.assertEqual(5, result["sampling_interval_milliseconds"])
        self.assertEqual(
            [6344, 2951, 478, 410, 142],
            [leaf["count"] for leaf in result["exclusive_leaves"]],
        )
        self.assertEqual(
            [478, 410, 142],
            [leaf["count"] for leaf in result["non_idle_exclusive_leaves"]],
        )
        self.assertEqual(1030, result["non_idle_exclusive_count"])

    def test_sample_accepts_macos_singular_millisecond_header(self) -> None:
        sample = SAMPLE.read_text(encoding="utf-8").replace(
            "every 5 milliseconds", "every 1 millisecond", 1
        )

        result = evidence.parse_sample(sample)

        self.assertEqual(1, result["sampling_interval_milliseconds"])

    def test_sample_accepts_unsymbolicated_leaf_with_load_address(self) -> None:
        sample = SAMPLE.read_text(encoding="utf-8").replace(
            "\nBinary Images:",
            "\n        ???  (in libsystem_m.dylib)  "
            "load address 0x19e255000 + 0x10ac  [0x19e2560ac]        8\n"
            "Binary Images:",
            1,
        )

        result = evidence.parse_sample(sample)

        unknown = next(
            leaf for leaf in result["exclusive_leaves"] if leaf["symbol"] == "???"
        )
        self.assertEqual(8, unknown["count"])

    def test_optional_groups_are_disjoint_and_deterministic(self) -> None:
        sample = evidence.parse_sample(SAMPLE.read_text(encoding="utf-8"))

        result = evidence.add_groups(sample, load(GROUPS))

        self.assertEqual(["gridding", "hashing"], list(result["exclusive_groups"]))
        self.assertEqual(410, result["exclusive_groups"]["gridding"]["count"])
        self.assertEqual(478, result["exclusive_groups"]["hashing"]["count"])
        self.assertEqual(142, result["ungrouped_non_idle_exclusive_count"])

    def test_issue540_hypothesis_groups_are_disjoint(self) -> None:
        sample = evidence.parse_sample(SAMPLE.read_text(encoding="utf-8"))

        result = evidence.add_groups(sample, load(ISSUE540_GROUPS))

        self.assertEqual(
            [
                "compensated_gridding",
                "prediction_and_stencil",
                "selected_traversal_and_projection",
                "weighting_generation_and_coverage",
            ],
            list(result["exclusive_groups"]),
        )

    def test_overlapping_groups_fail_closed(self) -> None:
        sample = evidence.parse_sample(SAMPLE.read_text(encoding="utf-8"))

        with self.assertRaisesRegex(
            evidence.ProfileEvidenceError, "matches multiple groups"
        ):
            evidence.add_groups(sample, {"all": ["sha2"], "hashing": ["sha2"]})

    def test_cli_emits_combined_deterministic_json(self) -> None:
        first = io.StringIO()
        second = io.StringIO()
        argv = [str(INTERMEDIATE_RECEIPT), str(SAMPLE), "--groups", str(GROUPS)]

        with contextlib.redirect_stdout(first):
            self.assertEqual(0, evidence.main(argv))
        with contextlib.redirect_stdout(second):
            self.assertEqual(0, evidence.main(argv))

        self.assertEqual(first.getvalue(), second.getvalue())
        result = json.loads(first.getvalue())
        self.assertEqual(evidence.SCHEMA_NAME, result["schema_name"])
        self.assertEqual("continuing_intermediate", result["receipt"]["classification"])


if __name__ == "__main__":
    unittest.main()
