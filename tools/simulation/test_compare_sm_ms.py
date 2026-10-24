# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests of `compare_sm_ms.py`'s MAIN.DATA comparison (#848 and #853 reviews).

DATA must be zero on both sides and, like every other column, defined in
the same cells with the same [correlation, channel] shapes. A predicted or
noisy DATA with a NaN or infinite sample, or a statistic that is not a
number, fails. Run with
`python3 tools/simulation/test_compare_sm_ms.py` (or pytest); only NumPy is
needed.
"""

from __future__ import annotations

import pathlib
import sys
import unittest

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from compare_sm_ms import (
    compare_model_data,
    data_differences,
    grouped_variance_ratios,
    ks_failures,
    noise_classes,
    tsys_manual_sigma,
    tsys_term,
)


def zero_cells(rows: int, correlations: int = 2, channels: int = 4) -> list:
    return [np.zeros((correlations, channels), dtype=np.complex64) for _ in range(rows)]


def aspects(casa: list, casars: list) -> list[str]:
    return [key for key, _ in data_differences(casa, casars)]


class DataComparisonTests(unittest.TestCase):
    def test_zero_cells_of_one_shape_agree(self) -> None:
        self.assertEqual(data_differences(zero_cells(3), zero_cells(3)), [])

    def test_an_undefined_cell_differs(self) -> None:
        # The review's first injected defect: the first casa-rs cell undefined.
        casars = zero_cells(3)
        casars[0] = None
        self.assertEqual(aspects(zero_cells(3), casars), ["MAIN.DATA:defined"])

    def test_a_zero_cell_of_another_shape_differs(self) -> None:
        # The review's second injected defect: a zero cell of shape (1, 1).
        casars = zero_cells(3)
        casars[0] = np.zeros((1, 1))
        self.assertEqual(aspects(zero_cells(3), casars), ["MAIN.DATA:shape"])

    def test_nonzero_data_differs(self) -> None:
        casars = zero_cells(3)
        casars[2] = casars[2] + 1j
        self.assertEqual(aspects(zero_cells(3), casars), ["MAIN.DATA:nonzero", "MAIN.DATA:values"])

    def test_a_missing_row_differs(self) -> None:
        self.assertEqual(aspects(zero_cells(3), zero_cells(2)), ["MAIN.DATA:cells"])


def model_side(data: np.ndarray) -> dict:
    rows = data.shape[0]
    return {"data": data, "flag_row": np.zeros(rows, dtype=bool), "auto": np.zeros(rows, dtype=bool)}


class NonFiniteDataTests(unittest.TestCase):
    """The #853 review's injection: finite CASA DATA against all-NaN casa-rs
    DATA, shape (2, 2, 2), unflagged, returned NRMS NaN and no failures."""

    def test_finite_model_data_within_the_limit_passes(self) -> None:
        casa = np.ones((2, 2, 2), dtype=np.complex128)
        stats, failures = compare_model_data(model_side(casa), model_side(casa * (1 + 1e-6)), 1e-3)
        self.assertEqual(failures, [])
        self.assertLess(stats["nrms"], 1e-3)

    def test_nan_model_data_fails(self) -> None:
        casa = np.ones((2, 2, 2), dtype=np.complex128)
        ours = np.full((2, 2, 2), np.nan, dtype=np.complex128)
        _, failures = compare_model_data(model_side(casa), model_side(ours), 1e-3)
        self.assertEqual(failures, ["MAIN.DATA: casa-rs: 8 non-finite values"])

    def test_one_infinite_sample_fails_even_on_a_flagged_row(self) -> None:
        casa = np.ones((2, 2, 2), dtype=np.complex128)
        ours = casa.copy()
        ours[1, 0, 1] = np.inf
        side = model_side(ours)
        side["flag_row"][1] = True
        _, failures = compare_model_data(model_side(casa), side, 1e-3)
        self.assertEqual(failures, ["MAIN.DATA: casa-rs: 1 non-finite values"])

    def test_nan_noise_fails(self) -> None:
        rng = np.random.default_rng(1)
        noise = (rng.normal(size=(64, 2, 8)) + 1j * rng.normal(size=(64, 2, 8))) * 0.1
        auto = np.zeros(64, dtype=bool)
        self.assertEqual(noise_classes(noise, auto, 0.01)[1], [])
        noise[3, 1, 2] = complex(np.nan, 0.0)
        self.assertEqual(noise_classes(noise, auto, 0.01)[1], ["noise: 1 non-finite values"])

    def test_a_nan_ks_p_value_fails(self) -> None:
        self.assertEqual(ks_failures(0.01, 0.5), [])
        self.assertEqual(len(ks_failures(0.2, 1e-5)), 1)
        self.assertEqual(len(ks_failures(float("nan"), float("nan"))), 1)


def sigma(params: dict, elevations_deg: list[float], diameters: list[float] = (12.0, 12.0),
          frequency_hz: float = 230.5e9, bandwidth_hz: float = 15.625e6, exposure_s: float = 10.0) -> float:
    """One row's tsys-manual σ."""
    return float(
        tsys_manual_sigma(
            params,
            np.array([frequency_hz]),
            np.array([bandwidth_hz]),
            np.array([exposure_s]),
            np.array([diameters]),
            np.radians(np.array([elevations_deg])),
        )[0]
    )


class TsysManualSigmaTests(unittest.TestCase):
    """The tsys-manual σ of `compare_sm_ms.tsys_manual_sigma` (#841 Phase 3)."""

    def test_the_zenith_tsys_is_the_one_casa_logs(self) -> None:
        # CASA 6.7.6 logged "Zenith Tsys at center of first Spectral Window =
        # 193.986" for tau 0.3 and the defaults at 230.5 GHz (the probe's
        # 64 x 15.625 MHz window from 230 GHz). σ = c a / sqrt(Δν t) Tsys / D².
        jansky_per_kelvin = 8 * 1.38062e-16 * 1e23 * 1e-4 / (0.8 * 0.88 * np.pi) / np.sqrt(2.0)
        found = sigma({"tau": 0.3}, [90.0, 90.0])
        tsys = found * 144.0 / (jansky_per_kelvin / np.sqrt(15.625e6 * 10.0))
        self.assertAlmostEqual(tsys, 193.986, places=3)

    def test_sigma_scales_as_casa_scales_it(self) -> None:
        base = sigma({}, [60.0, 60.0])
        self.assertAlmostEqual(sigma({}, [60.0, 60.0], diameters=(12.0, 6.0)) / base, 2.0)
        self.assertAlmostEqual(sigma({}, [60.0, 60.0], exposure_s=40.0) / base, 0.5)
        self.assertAlmostEqual(sigma({}, [60.0, 60.0], bandwidth_hz=62.5e6) / base, 0.5)
        self.assertAlmostEqual(sigma({"rxtype": 1}, [60.0, 60.0]) / base, 2.0)
        self.assertAlmostEqual(sigma({"rxtype": -1}, [60.0, 60.0]) / base, 1.0)
        self.assertAlmostEqual(sigma({"senscoeff": 1.0}, [60.0, 60.0]) / base, np.sqrt(2.0))
        self.assertAlmostEqual(sigma({"tatmos": 5.0}, [60.0, 60.0]), sigma({"tatmos": 250.0}, [60.0, 60.0]))
        self.assertGreater(sigma({}, [20.0, 20.0]), base)
        # One antenna's airmass enters as the square root of its Tsys.
        mixed = sigma({}, [20.0, 60.0])
        self.assertAlmostEqual(mixed**2, base * sigma({}, [20.0, 20.0]))

    def test_an_antenna_below_the_horizon_applies_as_one(self) -> None:
        params = {"tau": 0.2}
        self.assertEqual(sigma(params, [-1.0, -1.0]), 1.0)
        above = sigma(params, [45.0, 45.0])
        self.assertAlmostEqual(sigma(params, [45.0, -1.0]), np.sqrt(above))

    def test_the_tsys_term_is_the_one_setnoise_that_returned(self) -> None:
        tsys = {"op": "setnoise", "resolved": {"mode": "tsys-manual", "tau": 0.2}, "returned": True}
        refused = {"op": "setnoise", "resolved": {"mode": "tsys-manual", "pwv": 200.0}, "returned": False}
        simple = {"op": "setnoise", "resolved": {"mode": "simplenoise"}, "returned": True}
        self.assertEqual(tsys_term({"calls": [refused, tsys]}), {"mode": "tsys-manual", "tau": 0.2})
        self.assertIsNone(tsys_term({"calls": [tsys, simple]}))
        self.assertIsNone(tsys_term({"calls": [simple]}))


class GroupedVarianceTests(unittest.TestCase):
    """The direct CASA/casa-rs variance comparison in groups of rows."""

    def noise(self, rng: np.random.Generator, sigmas: np.ndarray) -> np.ndarray:
        shape = (sigmas.size, 2, 16)
        return (rng.normal(size=shape) + 1j * rng.normal(size=shape)) * sigmas[:, None, None]

    def test_the_same_sigmas_agree_and_a_scaled_group_does_not(self) -> None:
        rng = np.random.default_rng(3)
        groups = np.repeat(np.array(["a", "b", "c"]), 400)
        sigmas = np.repeat(np.array([0.1, 1.0, 10.0]), 400)
        casa, ours = self.noise(rng, sigmas), self.noise(rng, sigmas)
        results, failures = grouped_variance_ratios(casa, ours, groups)
        self.assertEqual(failures, [])
        self.assertEqual([result["group"] for result in results], ["a", "b", "c"])
        ours[groups == "b"] *= 1.1
        _, failures = grouped_variance_ratios(casa, ours, groups)
        self.assertEqual(len(failures), 1)
        self.assertTrue(failures[0].startswith("group b:"), failures[0])

    def test_real_autocorrelations_compare_their_real_parts(self) -> None:
        rng = np.random.default_rng(4)
        groups = np.repeat(np.array(["auto"]), 500)
        casa = rng.normal(size=(500, 2, 16))
        ours = rng.normal(size=(500, 2, 16))
        self.assertEqual(grouped_variance_ratios(casa, ours, groups)[1], [])
        self.assertEqual(len(grouped_variance_ratios(casa, 1.2 * ours, groups)[1]), 1)


if __name__ == "__main__":
    unittest.main()
