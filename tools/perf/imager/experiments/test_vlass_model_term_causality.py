#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later

from __future__ import annotations

import sys
import unittest
from pathlib import Path


EXPERIMENTS = Path(__file__).resolve().parent
sys.path.insert(0, str(EXPERIMENTS))

import vlass_model_term_causality as subject  # noqa: E402


def case(label: str, passes: bool, *, signature: bool = False) -> dict:
    mismatch_count = 16 if signature else 0
    return {
        "label": label,
        "gates": {"pass": passes},
        "current_failure_signature": {
            "alpha_exact": signature,
            "alpha_error_exact": signature,
        },
        "products": {
            ".alpha": {"topology": {"mismatch_count": mismatch_count}},
            ".alpha.error": {"topology": {"mismatch_count": mismatch_count}},
        },
    }


def batch(*cases: dict) -> dict:
    return {"cases": list(cases)}


def phase_a_control_batch(*, candidate_hash: str = "candidate") -> dict:
    def numeric(*, expected: bool) -> dict:
        return {
            "count": 4,
            "bitwise_equal_count": 3,
            "bitwise_mismatch_count": 1,
            "candidate_sha256": "candidate" if expected else candidate_hash,
            "reference_sha256": "reference",
            "maximum_ulp_distance": 2,
            "difference_rms": 1.0 if expected else 1.0 + 2.0e-16,
            "first_mismatch": {
                "location": [1, 2],
                "candidate_bits": 3,
                "reference_bits": 4,
                "ulp_distance": 1,
                "candidate": 1.0 if expected else 1.0 + 2.0e-16,
            },
        }

    topology = {
        "mismatch_count": 0,
        "candidate_sha256": "mask",
        "reference_sha256": "mask",
    }
    actual_products = {
        suffix: {"numeric": numeric(expected=False)}
        for suffix in subject.IMAGE_SUFFIXES
    }
    expected_products = {
        suffix: numeric(expected=True) for suffix in subject.IMAGE_SUFFIXES
    }
    for suffix in (".alpha", ".alpha.error"):
        actual_products[suffix] = {
            "numeric": numeric(expected=False),
            "topology": topology | {"ordered_mismatch_coordinate_sha256": "empty"},
        }
        expected_products[suffix] = {
            "numeric": numeric(expected=True),
            "topology": topology,
        }
    return {
        "cases": [
            {
                "label": "control-a",
                "gates": {"pass": True},
                "products": actual_products,
            }
        ],
        "frozen_identity": {
            "phase_a_comparison_status": "completed",
            "phase_a_contract": {"products": expected_products},
        },
    }


class ModelTermCausalityTests(unittest.TestCase):
    def test_control_exact_uses_binary_array_identity_not_json_float_spelling(
        self,
    ) -> None:
        exact, checks = subject.phase_a_control_exact(phase_a_control_batch())

        self.assertTrue(exact)
        self.assertTrue(all(checks.values()))

    def test_control_exact_rejects_product_hash_difference(self) -> None:
        exact, checks = subject.phase_a_control_exact(
            phase_a_control_batch(candidate_hash="different")
        )

        self.assertFalse(exact)
        self.assertFalse(checks[".image.tt0.array-ledger"])

    def test_complete_model_pass_stops_before_hybrids(self) -> None:
        primary = batch(case("control-a", True), case("complete-rust-model", True))

        self.assertEqual(
            subject.required_batches(primary, control_exact=True),
            ("primary",),
        )
        self.assertEqual(
            subject.classify_cases(primary, None, control_exact=True),
            "final-model-not-sufficient",
        )

    def test_failed_control_invalidates_certificate(self) -> None:
        primary = batch(case("control-a", False), case("complete-rust-model", True))

        self.assertEqual(
            subject.required_batches(primary, control_exact=False),
            ("primary",),
        )
        self.assertEqual(
            subject.classify_cases(primary, None, control_exact=False),
            "invalid-phase-a-control",
        )

    def test_each_hybrid_truth_table_has_one_classification(self) -> None:
        primary = batch(case("control-a", True), case("complete-rust-model", False))
        scenarios = {
            (False, True): "tt0-model-state-sufficient",
            (True, False): "tt1-model-state-sufficient",
            (False, False): "both-model-terms-independently-sufficient",
            (True, True): "joint-model-term-interaction-required",
        }
        for (tt0_pass, tt1_pass), expected in scenarios.items():
            with self.subTest(expected=expected):
                hybrid = batch(
                    case("tt0-rust-only", tt0_pass),
                    case("tt1-rust-only", tt1_pass),
                )
                self.assertEqual(
                    subject.classify_cases(
                        primary,
                        hybrid,
                        control_exact=True,
                    ),
                    expected,
                )

    def test_single_term_ledger_requires_exact_signature_and_zero_complement(
        self,
    ) -> None:
        cases = subject.case_map(
            batch(
                case("control-a", True),
                case("complete-rust-model", False, signature=True),
            ),
            batch(
                case("tt0-rust-only", False, signature=True),
                case("tt1-rust-only", True),
            ),
        )

        authorization = subject.term_ledger_authorization(
            "tt0-model-state-sufficient",
            cases,
            control_exact=True,
        )

        self.assertTrue(authorization["authorized"])
        self.assertEqual(authorization["term"], "tt0")
        self.assertFalse(authorization["new_clean_authorized"])
        self.assertFalse(authorization["production_change_authorized"])

    def test_single_term_ledger_fails_closed_on_nonexact_signature(self) -> None:
        cases = subject.case_map(
            batch(
                case("control-a", True),
                case("complete-rust-model", False, signature=True),
            ),
            batch(
                case("tt0-rust-only", False),
                case("tt1-rust-only", True),
            ),
        )

        authorization = subject.term_ledger_authorization(
            "tt0-model-state-sufficient",
            cases,
            control_exact=True,
        )

        self.assertFalse(authorization["authorized"])


if __name__ == "__main__":
    unittest.main()
