# SPDX-License-Identifier: LGPL-3.0-or-later
"""Adversarial tests for proposed measurements, not an acceptance-policy change."""

import numpy as np
import pytest

from perf_harness.taylor_moment_comparison import (
    MAX_CHUNK_CELLS,
    TaylorMomentAccumulator,
)


def arrays(g0, g1, g2):
    return tuple(np.atleast_1d(x).astype(np.float64) for x in (g0, g1, g2))


def add(acc, ref, candidate, *, mask=None, pb=None):
    if mask is None:
        mask = np.ones(ref[0].shape, dtype=bool)
    if pb is None:
        pb = mask.copy()
    acc.add(ref, candidate, (mask,) * 3, (mask,) * 3, pb)


def measure(ref, candidate, **kwargs):
    acc = TaylorMomentAccumulator(1.0, 1.0)
    add(acc, ref, candidate, **kwargs)
    return acc.result()


def test_exact_cancellation_has_defined_scaled_error_and_no_raw_nrms():
    result = measure(arrays([4, 4], [0, 0], [9, 9]),
                     arrays([4, 4], [3e-7, -3e-7], [9, 9]))
    assert result["raw_nrms"] is None
    assert result["e2"] == pytest.approx(5e-8)
    assert result["emax"] == pytest.approx(5e-8)
    assert result["rho_l2"] == result["rho_pmax"] == 0


def test_original_mosaic_zero_odd_bug_remains_visible():
    # A single-cell regression illustration using the observed peak magnitudes;
    # this is not a measurement of their full-image spatial correspondence.
    result = measure(arrays(359574.05, .00561333, 4.0311),
                     arrays(359574.05, 0, 4.0311))
    assert result["e2"] == pytest.approx(4.66247e-6, rel=2e-5)
    assert result["e2"] > 1e-6
    assert result["raw_nrms"] == 1


def test_local_spike_and_support_domain_are_distinct_from_l2_domain():
    n = 10_000
    ref = arrays(np.ones(n), np.zeros(n), np.ones(n))
    native = arrays(np.ones(n), np.zeros(n), np.ones(n))
    native[1][0] = 1e-5
    result = measure(ref, native)
    assert result["e2"] == pytest.approx(1e-7)
    assert result["emax"] == pytest.approx(1e-5)
    pb = np.ones(n, dtype=bool)
    pb[0] = False
    excluded = measure(ref, native, pb=pb)
    assert excluded["e2"] == result["e2"]
    assert excluded["emax"] == 0


@pytest.mark.parametrize("side", [0, 1])
@pytest.mark.parametrize("diagonals", [(0,), (2,), (0, 2)])
def test_each_negative_diagonal_rejected_even_when_product_positive(side, diagonals):
    pair = [arrays(1, 0, 1), arrays(1, 0, 1)]
    for term in diagonals:
        pair[side][term][0] = -1
    with pytest.raises(ValueError, match="nonnegative"):
        measure(*pair)


@pytest.mark.parametrize("side", [0, 1])
def test_zero_scale_requires_other_family_odd_zero_outside_pb(side):
    pair = [arrays([1, 1], [0, .5], [1, 1]),
            arrays([1, 1], [0, .5], [1, 1])]
    pair[side][0][1] = pair[side][1][1] = 0
    with pytest.raises(ValueError, match="zero diagonal"):
        measure(*pair, pb=np.array([True, False]))


def test_zero_diagonals_and_zero_odd_are_valid_with_other_positive_support():
    result = measure(arrays([0, 1], [0, 0], [1, 1]),
                     arrays([1, 1], [0, 0], [0, 1]))
    assert result["positive_scale_pb_cells"] == 1
    assert result["pb_cells"] == result["valid_cells"] == 2


def test_near_singular_indefinite_candidate_is_rejected():
    with pytest.raises(ValueError, match="PSD"):
        measure(arrays(1, 1 - 1e-8, 1), arrays(1, 1 - 1e-8 + 5e-7, 1))


@pytest.mark.parametrize("mask_index", range(6))
def test_any_one_of_six_masks_disagreeing_is_rejected(mask_index):
    masks = [np.array([True, True]) for _ in range(6)]
    masks[mask_index][1] = False
    family = arrays([1, 1], [0, 0], [1, 1])
    with pytest.raises(ValueError, match="masks must match"):
        TaylorMomentAccumulator(1, 1).add(family, family, masks[:3], masks[3:], masks[0])


def test_invalid_payload_is_ignored_only_with_false_mask():
    family = arrays([1, np.nan], [0, np.inf], [1, -10])
    result = measure(family, family, mask=np.array([True, False]))
    assert result["valid_cells"] == 1
    with pytest.raises(ValueError, match="finite"):
        measure(family, family)
    with pytest.raises(ValueError, match="subset"):
        measure(family, family, mask=np.array([True, False]), pb=np.array([True, True]))


@pytest.mark.parametrize("mode", ["empty", "invalid", "no_pb", "zero_pb"])
def test_meaningless_domains_fail_at_finish(mode):
    acc = TaylorMomentAccumulator(1, 1)
    if mode == "empty":
        add(acc, arrays([], [], []), arrays([], [], []))
    elif mode == "invalid":
        add(acc, arrays(1, 0, 1), arrays(1, 0, 1), mask=np.array([False]))
    elif mode == "no_pb":
        add(acc, arrays(1, 0, 1), arrays(1, 0, 1), pb=np.array([False]))
    else:
        add(acc, arrays([0, 1], [0, 0], [0, 1]),
            arrays([0, 1], [0, 0], [0, 1]), pb=np.array([True, False]))
    with pytest.raises(ValueError, match="positive-scale"):
        acc.result()


@pytest.mark.parametrize("value", [[], [1], np.ones((1, 1)), 0, -1, 1e-7,
                                    np.nan, np.inf, True, 1j])
@pytest.mark.parametrize("side", [0, 1])
def test_sum_weight_shape_type_and_casa_threshold(value, side):
    values = [1.0, 1.0]
    values[side] = value
    with pytest.raises(ValueError, match="S0"):
        TaylorMomentAccumulator(*values)


@pytest.mark.parametrize("side", [0, 1])
@pytest.mark.parametrize("term", [0, 1, 2])
@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf])
def test_nonfinite_valid_payload(side, term, bad):
    pair = [arrays(1, 0, 1), arrays(1, 0, 1)]
    pair[side][term][0] = bad
    with pytest.raises(ValueError):
        measure(*pair)


def test_overflow_reconstruction_product_and_reductions():
    with pytest.raises(ValueError):
        add(TaylorMomentAccumulator(1e308, 1), arrays(2, 0, 1), arrays(1, 0, 1))
    with pytest.raises(ValueError):
        measure(arrays(1e200, 0, 1e200), arrays(1, 0, 1))
    with pytest.raises(ValueError):
        measure(arrays([1e154] * 2, [0] * 2, [1e154] * 2),
                arrays([1] * 2, [0] * 2, [1] * 2))
    acc = TaylorMomentAccumulator(1, 1)
    family = arrays(1e154, 0, 1e154)
    add(acc, family, family)
    with pytest.raises(ValueError):
        add(acc, family, family)
    assert acc.result()["chunks"] == 1  # Failed updates are atomic.


def test_common_positive_weight_rescaling_and_separate_s0():
    results = []
    for factor in (0.01, 1.0, 1e5):
        acc = TaylorMomentAccumulator(2 * factor, 3 * factor)
        add(acc, arrays([2, 3], np.array([.1, .2]) * factor, np.array([4, 5]) * factor),
            arrays([4/3, 2], np.array([.11, .19]) * factor, np.array([4, 5]) * factor))
        results.append(acc.result())
    for key in ("e2", "emax", "rho_l2", "rho_pmax", "raw_nrms"):
        assert results[0][key] == pytest.approx(results[1][key], rel=1e-14)
        assert results[2][key] == pytest.approx(results[1][key], rel=1e-14)


@pytest.mark.parametrize("odd", [(1e-200, 1e-200), (0, 1e-200)])
def test_underflow_cannot_masquerade_as_zero_odd_or_zero_error(odd):
    with pytest.raises(ValueError, match="underflow"):
        measure(arrays(1, odd[0], 1), arrays(1, odd[1], 1))


def test_chunking_invariance_and_scalar_only_retained_state():
    rng = np.random.default_rng(17)
    ref = arrays(rng.uniform(1, 2, 1000), rng.uniform(-.1, .1, 1000),
                 rng.uniform(1, 2, 1000))
    native = (ref[0], ref[1] + 1e-5, ref[2])
    whole = measure(ref, native)
    acc = TaylorMomentAccumulator(1, 1)
    for start in range(0, 1000, 37):
        add(acc, tuple(a[start:start+37] for a in ref),
            tuple(a[start:start+37] for a in native))
    for key in ("e2", "emax", "rho_l2", "rho_pmax", "raw_nrms"):
        assert acc.result()[key] == pytest.approx(whole[key], rel=1e-14)
    assert all(isinstance(value, (int, float)) for value in vars(acc).values())


def test_chunk_guard_precedes_conversion_or_indexed_copies():
    class NoCopy(np.ndarray):
        def __getitem__(self, key):
            raise AssertionError("oversized input must not be indexed")

        def astype(self, *args, **kwargs):
            raise AssertionError("oversized input must not be converted")

    big = np.broadcast_to(np.array(1.0), (MAX_CHUNK_CELLS + 1,)).view(NoCopy)
    mask = np.broadcast_to(np.array(True), big.shape)
    with pytest.raises(ValueError, match="65536"):
        TaylorMomentAccumulator(1, 1).add((big,) * 3, (big,) * 3, (mask,) * 3,
                                         (mask,) * 3, mask)


def test_shape_dtype_and_exact_chunk_limit():
    acc = TaylorMomentAccumulator(1, 1)
    family = arrays(np.ones(MAX_CHUNK_CELLS), np.zeros(MAX_CHUNK_CELLS),
                    np.ones(MAX_CHUNK_CELLS))
    add(acc, family, family)
    assert acc.result()["max_chunk_cells"] == MAX_CHUNK_CELLS
    with pytest.raises(ValueError, match="one-dimensional"):
        add(acc, tuple(a.reshape(256, 256) for a in family), family)
    with pytest.raises(ValueError, match="shapes"):
        add(acc, family, tuple(a[:1] for a in family))
    with pytest.raises(ValueError, match="boolean"):
        add(acc, family, family, mask=np.ones(MAX_CHUNK_CELLS))


@pytest.mark.parametrize("odd", [(0, 1e-50), (1e-50, 0)])
def test_representable_root_ratio_survives_squared_ratio_underflow(odd):
    result = measure(arrays(1e150, odd[0], 1e150),
                     arrays(1e150, odd[1], 1e150))
    assert result["e2"] == pytest.approx(1e-200, rel=1e-14, abs=0)
    assert result["emax"] == pytest.approx(1e-200, rel=1e-14, abs=0)
    if odd[0]:
        assert result["rho_l2"] == pytest.approx(1e-200, rel=1e-14, abs=0)
        assert result["raw_nrms"] == 1
    else:
        assert result["rho_l2"] == 0
        assert result["raw_nrms"] is None


def test_raw_nrms_survives_squared_ratio_overflow():
    result = measure(arrays(1e150, 1e-150, 1e150),
                     arrays(1e150, 1e150, 1e150))
    assert result["raw_nrms"] == pytest.approx(1e300, rel=1e-14)
    assert result["rho_l2"] == pytest.approx(1e-300, rel=1e-14, abs=0)
