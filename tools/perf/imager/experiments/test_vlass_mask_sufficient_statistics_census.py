#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Focused tests for the VLASS mask sufficient-statistics reducer."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest


MODULE_PATH = Path(__file__).with_name("vlass_mask_sufficient_statistics_census.py")
SPEC = importlib.util.spec_from_file_location(
    "vlass_mask_sufficient_statistics_census",
    MODULE_PATH,
)
assert SPEC is not None and SPEC.loader is not None
census = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = census
SPEC.loader.exec_module(census)


def synthetic_event(
    order: int,
    rank: int,
    *,
    lower_clusters: int,
    upper_clusters: int,
) -> str:
    """Return one complete rank/order event."""

    moments = 10 if order == 2 else 20
    cross = 35 if order == 2 else 84
    feature_lower = lower_clusters * rank * moments
    feature_upper = upper_clusters * rank * moments
    cross_upper = upper_clusters * rank * rank * cross
    state_bytes = (2 * feature_upper + cross_upper) * 8
    return (
        f"awproject_mask_sufficient_statistics_census order={order} rank={rank} "
        "samples=10 windows=2 source_states=3 nterms=2 scales=0,5,12 "
        "clean_mask_pixels=16 domain_pixels=36 domain_bounds=10,15,20,25 "
        "max_kernel_offsets=2,2 mask_fingerprint=0123456789abcdef "
        "center_lmn=0.0,0.0,0.0 delta_lmn_extents=1e-3,1e-3,1e-6 "
        "phase_error_budget=1e-5 eta=1e-2 remainder_bound=9e-6 "
        f"upper_clusters={upper_clusters} upper_group_cluster_pairs={upper_clusters + 2} "
        f"packing_lower_clusters={lower_clusters} shift_code=0 "
        f"moment_count={moments} cross_moment_count={cross} "
        f"feature_lower={feature_lower} feature_upper={feature_upper} "
        f"cross_moments_upper={cross_upper} state_bytes_upper={state_bytes} "
        "feature_limit=77000 state_limit_bytes=151257904 component_updates=7 "
        "trajectory_receipt_sha256="
        f"{'a' * 64} "
        "weight_sum=10 weight_sq_sum=12 weight_min=0.5 weight_max=2 "
        "frequency_min_hz=1e9 frequency_max_hz=3e9 "
        "operator_calls_retained=3 operator_calls_targeted=8 "
        "factoring_contract=exact-mask-center-rephase-central-w-and-pointing-scalar-phase "
        "component_contract=frozen-promoted-count-and-scales-not-executed-by-this-niter0-census "
        "error_contract=scalar-phase-Taylor-remainder-bound-only-DDE-low-rank-error-not-measured "
        "role=source-feasibility-and-phase-space-bound-not-performance-or-science-evidence"
    )


def synthetic_log(*, lower_clusters: int, upper_clusters: int) -> str:
    """Return a complete two-order, four-rank curve."""

    return "\n".join(
        synthetic_event(
            order,
            rank,
            lower_clusters=lower_clusters,
            upper_clusters=upper_clusters,
        )
        for order in (2, 3)
        for rank in (1, 2, 3, 4)
    )


class VlassMaskSufficientStatisticsCensusTest(unittest.TestCase):
    def analyze(self, text: str) -> dict:
        """Analyze a synthetic ten-sample, seven-component contract."""

        return census.analyze_log(
            text,
            expected_samples=10,
            expected_component_updates=7,
            expected_trajectory_sha256="a" * 64,
        )

    def test_retires_family_only_from_rank_one_packing_lower_bound(self) -> None:
        result = self.analyze(
            synthetic_log(lower_clusters=8_000, upper_clusters=9_000)
        )
        self.assertEqual(
            result["selection"]["decision"],
            "retire-order-2-3-uvw-polynomial-mask-moments",
        )
        self.assertEqual(
            result["selection"]["rigorously_killed_orders_at_rank_one"],
            [2, 3],
        )

    def test_promotes_survivor_only_to_error_and_hardware_race(self) -> None:
        result = self.analyze(synthetic_log(lower_clusters=10, upper_clusters=100))
        self.assertEqual(
            result["selection"]["decision"],
            "promote-held-out-low-rank-and-metal-response-race",
        )
        self.assertIn("lacks measured DDE", result["claim_boundary"])

    def test_rejects_incomplete_or_changed_contract(self) -> None:
        incomplete = "\n".join(synthetic_log(lower_clusters=10, upper_clusters=100).splitlines()[:-1])
        with self.assertRaisesRegex(
            census.MaskSufficientStatisticsError,
            "order/rank curve",
        ):
            self.analyze(incomplete)
        changed = synthetic_log(lower_clusters=10, upper_clusters=100).replace(
            "scales=0,5,12",
            "scales=0,5",
            1,
        )
        with self.assertRaisesRegex(
            census.MaskSufficientStatisticsError,
            "invariant scales",
        ):
            self.analyze(changed)


if __name__ == "__main__":
    unittest.main()
