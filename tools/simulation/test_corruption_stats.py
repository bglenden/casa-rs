# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests of `corruption_stats.py`, the gain and leakage statistics of
`compare_sm_ms.py` (simulation Phase 3, #841), on synthetic DATA: each
statistic passes what CASA's corruptors simulate and fails the defect it
is there to catch. Run with `python3 tools/simulation/test_corruption_stats.py`
(or pytest); only NumPy is needed.
"""

from __future__ import annotations

import pathlib
import sys
import unittest

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from corruption_stats import gain_statistics, leakage_statistics, structure_failures

ANTENNAS = 12
INTEGRATIONS = 60
SCALE = 0.05
CHECKS = ["closure", "amplitude", "phase"]


def screens(rng: np.random.Generator, rho: float, shape: tuple[int, ...]) -> np.ndarray:
    """AR(1) series along the last axis, each scaled to unit standard
    deviation about its mean (the mean kept, as CASA's fBM screens)."""
    noise = rng.normal(size=shape)
    series = np.empty(shape)
    series[..., 0] = noise[..., 0]
    for t in range(1, shape[-1]):
        series[..., t] = rho * series[..., t - 1] + noise[..., t]
    return series / series.std(axis=-1, keepdims=True)


def gains(seed: int = 1, scale: float = SCALE, rho: float = 0.5) -> np.ndarray:
    """[antenna, receptor, integration] gains of `scale`: amplitude
    `1 + scale·a`, phase `π·scale·p`."""
    rng = np.random.default_rng(seed)
    shape = (ANTENNAS, 2, INTEGRATIONS)
    return (1.0 + scale * screens(rng, rho, shape)) * np.exp(1j * np.pi * scale * screens(rng, rho, shape))


def gain_side(g: np.ndarray, autos: bool = True) -> tuple[dict, dict]:
    """XX YY rows of one channel, `g_p[r]·conj(g_q[r])` on a model of 1, and
    the twin."""
    pairs = [(p, q) for p in range(ANTENNAS) for q in range(p if autos else p + 1, ANTENNAS)]
    rows = [(t, p, q) for t in range(INTEGRATIONS) for p, q in pairs]
    data = np.array([[[g[p, r, t] * np.conj(g[q, r, t])] for r in range(2)] for t, p, q in rows])
    columns = {
        "time": np.array([4.9e9 + 10.0 * t for t, _, _ in rows]),
        "antenna1": np.array([p for _, p, _ in rows]),
        "antenna2": np.array([q for _, _, q in rows]),
    }
    return dict(columns, data=data), dict(columns, data=np.ones_like(data))


class GainTests(unittest.TestCase):
    def test_antenna_based_gains_at_the_scale_pass(self) -> None:
        for autos in (True, False):
            side, twin = gain_side(gains(), autos)
            stats, failures = gain_statistics(side, twin, SCALE, CHECKS, [])
            self.assertEqual(failures, [], f"autos {autos}")
            self.assertAlmostEqual(stats["corr0"]["amplitude_scale"], 1.0, delta=0.05)

    def test_gains_of_another_scale_fail(self) -> None:
        side, twin = gain_side(gains(scale=1.3 * SCALE))
        _, failures = gain_statistics(side, twin, SCALE, CHECKS, [])
        self.assertTrue(any("amplitude scale" in failure for failure in failures), failures)
        self.assertTrue(any("phase scale" in failure for failure in failures), failures)

    def test_a_baseline_error_breaks_closure(self) -> None:
        side, twin = gain_side(gains())
        baseline = (side["antenna1"] == 2) & (side["antenna2"] == 5)
        side["data"][baseline] *= np.exp(0.01j)
        _, failures = gain_statistics(side, twin, SCALE, CHECKS, [])
        self.assertTrue(any("not antenna-based" in failure for failure in failures), failures)

    def test_unit_gain_antennas_are_found(self) -> None:
        g = gains()
        g[ANTENNAS - 1] = 1.0
        side, twin = gain_side(g)
        stats, failures = gain_statistics(side, twin, SCALE, CHECKS, [ANTENNAS - 1])
        self.assertEqual(failures, [])
        self.assertEqual(stats["corr0"]["unit_antennas"], [ANTENNAS - 1])
        _, failures = gain_statistics(side, twin, SCALE, CHECKS, [])
        self.assertEqual(failures, [f"unit-gain antennas [{ANTENNAS - 1}], expected []"])

    def test_screens_of_another_structure_differ(self) -> None:
        reference = gain_statistics(*gain_side(gains(seed=1)), SCALE, CHECKS, [])[0]
        alike = gain_statistics(*gain_side(gains(seed=2)), SCALE, CHECKS, [])[0]
        white = gain_statistics(*gain_side(gains(seed=2, rho=0.0)), SCALE, CHECKS, [])[0]
        self.assertEqual(structure_failures(reference, alike)[1], [])
        failures = structure_failures(reference, white)[1]
        self.assertTrue(any("lag 1" in failure for failure in failures), failures)


LEAKAGE = (0.03, 0.015)
OFFSET = 0.04 + 0.02j


def leakage_side(seed: int = 1, second_offset: complex = complex(-OFFSET.real, OFFSET.imag)) -> tuple[dict, dict]:
    """XX XY YX YY rows of two windows and three integrations each, without
    autocorrelations, corrupted by one leakage per window and antenna."""
    rng = np.random.default_rng(seed)
    antennas, windows = 40, 2
    draws = rng.normal(size=(windows, antennas, 2, 2))
    d = LEAKAGE[0] * draws[..., 0] + 1j * LEAKAGE[1] * draws[..., 1]
    d[:, :, 0] += OFFSET
    d[:, :, 1] += second_offset
    rows = [(w, t, p, q) for w in range(windows) for t in range(3) for p in range(antennas) for q in range(p + 1, antennas)]
    model = np.zeros((len(rows), 4, 1), dtype=complex)
    model[:, [0, 3], :] = 1.0
    data = model.copy()
    for row, (w, _, p, q) in enumerate(rows):
        data[row, 1, 0] = d[w, p, 0] + np.conj(d[w, q, 1])
        data[row, 2, 0] = d[w, p, 1] + np.conj(d[w, q, 0])
    columns = {
        "data_desc": np.array([w for w, _, _, _ in rows]),
        "antenna1": np.array([p for _, _, p, _ in rows]),
        "antenna2": np.array([q for _, _, _, q in rows]),
    }
    return dict(columns, data=data), dict(columns, data=model)


class LeakageTests(unittest.TestCase):
    def test_casas_leakage_passes(self) -> None:
        for seed in range(5):
            stats, failures = leakage_statistics(*leakage_side(seed), list(LEAKAGE))
            self.assertEqual(failures, [], f"seed {seed}")
            self.assertEqual(stats["draws"], 80)

    def test_an_offset_of_the_wrong_sign_fails(self) -> None:
        _, failures = leakage_statistics(*leakage_side(second_offset=OFFSET), list(LEAKAGE))
        self.assertTrue(any(failure.startswith("XY re: mean") for failure in failures), failures)

    def test_swapped_amplitudes_fail(self) -> None:
        _, failures = leakage_statistics(*leakage_side(), [LEAKAGE[1], LEAKAGE[0]])
        self.assertTrue(any("mean square" in failure for failure in failures), failures)

    def test_a_leakage_that_drifts_in_a_window_fails(self) -> None:
        side, twin = leakage_side()
        side["data"][5, 1, 0] += 1e-3
        _, failures = leakage_statistics(side, twin, list(LEAKAGE))
        self.assertTrue(any("changes within a spectral window" in failure for failure in failures), failures)

    def test_changed_parallel_hands_fail(self) -> None:
        side, twin = leakage_side()
        side["data"][7, 0, 0] *= 1.001
        _, failures = leakage_statistics(side, twin, list(LEAKAGE))
        self.assertTrue(any("parallel hands changed" in failure for failure in failures), failures)


if __name__ == "__main__":
    unittest.main()
