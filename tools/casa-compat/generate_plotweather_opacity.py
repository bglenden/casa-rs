#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Export the fixed CASA 6.7.6.14 VLA plotweather opacity curve.

Run with the installed CASA Python environment. Each little-endian pair is
(dry intercept, PWV slope) in nepers and nepers/mm. The grid is 2,000 channel
centres from 12.5 MHz in 25 MHz steps. Only the fixed profile settings from
the CASA task enter this table. The station, season and interpolation still
use each input MeasurementSet at runtime.
"""

from pathlib import Path

from casatasks import version_string
from casatools import atmosphere, quanta
import numpy as np


def main():
    if version_string() != "6.7.6.14":
        raise RuntimeError(f"expected CASA 6.7.6.14, got {version_string()}")
    qa = quanta()
    q = qa.quantity
    atm = atmosphere()
    water = []
    total = []
    for humidity in range(20, 401, 20):
        atm.initAtmProfile(
            q(2125, "m"), q(270, "K"), q(790, "mbar"), q(48, "km"),
            float(humidity), q(-5.6, "K/km"), q(10, "mbar"), 1.2, q(2, "km"), 1,
        )
        atm.initSpectralWindow(1, q(25, "GHz"), q(50, "GHz"), q(0.025, "GHz"))
        window = atm.getSpectralWindow()
        expected = 12_500_000.0 + 25_000_000.0 * np.arange(2000)
        np.testing.assert_array_equal(qa.convert(window, "Hz")["value"], expected)
        pwv = atm.getGroundWH2O()
        atm.setUserWH2O(pwv)
        water.append(pwv["value"][0])
        total.append(np.array(atm.getDryOpacitySpec()[1]) + np.array(atm.getWetOpacitySpec()[1]["value"]))
    coefficients = np.array(
        [np.polyfit(water, channel, 1)[::-1] for channel in np.asarray(total).T],
        dtype="<f8",
    )
    destination = (
        Path(__file__).resolve().parents[2]
        / "libs/casa/casa-calibration/src/weather/vla_opacity_6_7_6_14.bin"
    )
    destination.write_bytes(coefficients.tobytes())


if __name__ == "__main__":
    main()
