#!/usr/bin/env python3

from __future__ import annotations

import unittest

import numpy as np

import vlass_casa_aw_divsc3_callsite_trace as subject


class CallsiteTraceTests(unittest.TestCase):
    def test_enumerates_rr_then_ll_helper_call_order(self) -> None:
        flags = np.ones((2, 4, 64), dtype=np.bool_)
        flags[0, 0, 11] = False
        flags[0, 3, 11] = False
        flags[1, 0, 19] = False
        flags[1, 3, 19] = False
        samples = [
            {
                "source_ordinal": 0,
                "row_id": 353600,
                "ddid": 2,
                "spw_id": 2,
                "channel": 11,
            },
            {
                "source_ordinal": 1,
                "row_id": 353635,
                "ddid": 2,
                "spw_id": 2,
                "channel": 19,
            },
        ]
        prior = subject.EXPECTED_SOURCE_ORDINALS
        subject.EXPECTED_SOURCE_ORDINALS = (0, 1)
        try:
            targets, census = subject.enumerate_helper_calls(
                row_ids=np.asarray([353600, 353635]),
                spw_ids=np.asarray([2, 2]),
                uv_selected=np.asarray([True, True]),
                flags=flags,
                source_samples=samples,
            )
        finally:
            subject.EXPECTED_SOURCE_ORDINALS = prior

        self.assertEqual([target["helper_call_index"] for target in targets], [0, 2])
        self.assertEqual(census["parallel_hand_helper_call_count"], 4)

    def test_rejects_asymmetric_parallel_hand_flags(self) -> None:
        flags = np.ones((1, 4, 64), dtype=np.bool_)
        flags[0, 0, 11] = False
        with self.assertRaisesRegex(RuntimeError, "flag symmetry"):
            subject.enumerate_helper_calls(
                row_ids=np.asarray([353600]),
                spw_ids=np.asarray([2]),
                uv_selected=np.asarray([True]),
                flags=flags,
                source_samples=[],
            )

    def test_classifies_callsite_outcomes(self) -> None:
        asserted = [1, 2, 3, 4]
        official = [5, 6]
        self.assertEqual(
            subject.classify_target([1, 2, 3, 9], official, asserted, official),
            "operands-differ-at-callsite",
        )
        self.assertEqual(
            subject.classify_target(
                asserted,
                subject.direct_probe.EXPECTED_RUST_SOURCE_1446,
                asserted,
                official,
            ),
            "operands-match-and-result-matches-rust-wide",
        )
        self.assertEqual(
            subject.classify_target(asserted, official, asserted, official),
            "operands-match-and-result-matches-official",
        )


if __name__ == "__main__":
    unittest.main()
