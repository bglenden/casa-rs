# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tests of `atm_model.py`, casa-rs's ATM for the CASA comparison, against a
stand-in for casa-atmosphere's `at_tool_quantities` example. Run with
`python3 tools/simulation/test_atm_model.py` (or pytest); only NumPy is
needed.
"""

from __future__ import annotations

import json
import math
import os
import pathlib
import stat
import sys
import tempfile
import textwrap
import unittest

import numpy as np

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from atm_model import SETTROP_SITE, casars_sky, casars_wet_delays  # noqa: E402
from compare_sm_ms import compare_data, needs_atmosphere_model  # noqa: E402

# A stand-in for the example: the grid the `at` tool's convention gives,
# shifted by `shift` Hz, and quantities that say which channel, water column
# and airmass they are for. It records the configuration it read.
FAKE = textwrap.dedent(
    """\
    #!{python}
    import json, sys
    config = json.load(sys.stdin)
    json.dump(config, open({record!r}, "w"))
    band = config["bands"][0]
    n = band["channels"]
    step = band["res_ghz"] * 1e9
    reference = band["centre_ghz"] * 1e9 - (0.5 * step if n % 2 == 0 else 0.0)
    freq = [reference + (c - (n - 1) // 2) * step + {shift} for c in range(n)]
    sets = config["sets"]
    window = {{
        "freq_hz": freq,
        "dry": [0.01 * (c + 1) for c in range(n)],
        "set_wet": [[s["water_m"] * 10 * (c + 1) for c in range(n)] for s in sets],
        "set_tebb": [[100 * s["airmass"] + c for c in range(n)] for s in sets],
        "set_disp_phase": [[s["water_m"] * 1e3 * (c + 1) for c in range(n)] for s in sets],
        "set_nondisp_phase": [[s["water_m"] * 1e5 * (c + 1) for c in range(n)] for s in sets],
    }}
    json.dump({{"profiles": [{{"windows": [window]}}]}}, sys.stdout)
    """
)


class AtmModelTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.record = os.path.join(self.directory.name, "config.json")

    def tearDown(self):
        self.directory.cleanup()

    def model(self, shift: float = 0.0) -> str:
        path = os.path.join(self.directory.name, f"model{shift}")
        with open(path, "w") as handle:
            handle.write(FAKE.format(python=sys.executable, record=self.record, shift=shift))
        os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC)
        return path

    def config(self) -> dict:
        with open(self.record) as handle:
            return json.load(handle)

    def test_the_sky_is_the_sites_opacity_and_tebb_per_airmass_in_the_channels_order(self):
        site = {"altitude_m": 5050.0, "tground": 275.0, "pressure_mbar": 555.0, "humidity": 25.0,
                "h0_km": 1.8, "pwv_mm": 2.0}
        descending = 230.0e9 - 1.0e6 * np.arange(4)
        tau, tebb = casars_sky(self.model())(site, descending, np.array([1.2, 1.5, 2.0]))
        # Ascending channel c is descending channel 3 - c.
        np.testing.assert_allclose(tau, [0.01 * (4 - i) + 0.02 * (4 - i) for i in range(4)], rtol=1e-12)
        np.testing.assert_allclose(tebb[:, 1], [150.0 + (3 - i) for i in range(4)], rtol=1e-12)
        self.assertEqual(tebb.shape, (4, 3))
        sent = self.config()
        profile = sent["profiles"][0]
        self.assertEqual(
            (profile["altitude_m"], profile["temperature_k"], profile["pressure_mbar"], profile["humidity"],
             profile["h0_km"], profile["atmType"]),
            (5050.0, 275.0, 555.0, 25.0, 1.8, 1),
        )
        self.assertEqual([s["airmass"] for s in sent["sets"]], [1.2, 1.5, 2.0])
        self.assertTrue(all(math.isclose(s["water_m"], 2.0e-3) for s in sent["sets"]))
        band = sent["bands"][0]
        self.assertEqual(band["channels"], 4)
        # An even count's centre is half a channel above channel (n - 1) // 2.
        self.assertAlmostEqual(band["centre_ghz"], 229.9985, places=9)
        self.assertAlmostEqual(band["res_ghz"], 1e-3, places=12)

    def test_a_grid_the_model_does_not_reproduce_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "not the frequencies asked for"):
            casars_sky(self.model(shift=5.0))(
                {"altitude_m": 5000.0, "tground": 270.0, "pressure_mbar": 560.0, "humidity": 20.0,
                 "h0_km": 2.0, "pwv_mm": 1.0},
                100.0e9 + 2.0e6 * np.arange(3),
                np.array([1.0]),
            )

    def test_wet_delays_are_settrops_site_at_the_single_precision_water_column(self):
        frequencies = 183.0e9 + 1.0e8 * np.arange(5)
        dispersive, continuum = casars_wet_delays(self.model(), 1.1, frequencies)
        water = float(np.float32(1.1)) * 1e-3
        np.testing.assert_allclose(dispersive, np.radians([water * 1e3 * (c + 1) for c in range(5)]), rtol=1e-12)
        np.testing.assert_allclose(continuum, np.radians([water * 1e5 * (c + 1) for c in range(5)]), rtol=1e-12)
        sent = self.config()
        self.assertEqual(sent["sets"], [{"water_m": water, "airmass": 1.0}])
        profile = sent["profiles"][0]
        self.assertEqual(
            {key: profile[key] for key in ("altitude_m", "temperature_k", "pressure_mbar", "humidity", "h0_km")},
            SETTROP_SITE,
        )


class NeedsModelTests(unittest.TestCase):
    TSYS_ATM = {"data": "noisy", "calls": [{"op": "setnoise", "returned": True, "resolved": {"mode": "tsys-atm"}}]}
    TSYS_MANUAL = {"data": "noisy", "calls": [{"op": "setnoise", "returned": True, "resolved": {"mode": "tsys-manual"}}]}

    def test_tsys_atm_and_troposphere_scenarios_need_the_model(self):
        self.assertTrue(needs_atmosphere_model(self.TSYS_ATM))
        self.assertTrue(needs_atmosphere_model({"data": "troposphere", "calls": []}))
        self.assertFalse(needs_atmosphere_model(self.TSYS_MANUAL))
        self.assertFalse(needs_atmosphere_model({"data": "gains", "calls": []}))

    def test_a_scenario_needing_the_model_fails_without_it(self):
        stats, failures = compare_data(None, self.TSYS_ATM, {}, 1e-3)
        self.assertEqual(stats, {"kind": "noisy"})
        self.assertEqual(failures, ["needs --atmosphere-model, casa-atmosphere's at_tool_quantities example"])


if __name__ == "__main__":
    unittest.main()
