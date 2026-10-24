# SPDX-License-Identifier: LGPL-3.0-or-later
"""casa-rs's own ATM for the CASA comparison (`compare_sm_ms.py`).

casa-atmosphere corrects four of ATM's data errors (`docs/CASA (C++)
bugs.md`), so CASA's `at` tool is CASA's model but not casa-rs's. These
helpers read casa-rs's from casa-atmosphere's `at_tool_quantities` example,
which takes the `at` tool's terms (`tools/atmosphere/at_tool_check.py`
checks it against them):

  cargo build --release -p casa-atmosphere --example at_tool_quantities

`casars_sky` mirrors `compare_sm_ms.at_tool_sky` and `casars_wet_delays`
`troposphere_stats.at_tool_wet_delays`, each on a regular grid of
frequencies in either order. Needs only NumPy.
"""

from __future__ import annotations

import json
import subprocess

import numpy as np

# The atmosphere `settrop` uses (`ProfileParameters::default()`, CASA's
# `AtmosCorruptor::initAtm` defaults).
SETTROP_SITE = {
    "altitude_m": 5000.0,
    "temperature_k": 270.0,
    "pressure_mbar": 560.0,
    "humidity": 20.0,
    "h0_km": 2.0,
}


def profile(site: dict) -> dict:
    """The `at` tool's `initAtmProfile` arguments for `site` (altitude,
    ground temperature, pressure, humidity, water scale height), with the
    simulator's fixed top, lapse rate, pressure step and tropical type."""
    return {
        "name": "site",
        "altitude_m": float(site["altitude_m"]),
        "temperature_k": float(site["temperature_k"]),
        "pressure_mbar": float(site["pressure_mbar"]),
        "maxAltitude_km": 48.0,
        "humidity": float(site["humidity"]),
        "dTem_dh_k_per_km": -5.6,
        "dP_mbar": 10.0,
        "h0_km": float(site["h0_km"]),
        "atmType": 1,
    }


def window(model: str, site: dict, frequencies_hz: np.ndarray, sets: list[dict]) -> tuple[dict, np.ndarray]:
    """casa-atmosphere's quantities on the grid of `frequencies_hz` (regular,
    either order) for `site` and each (water column, airmass) of `sets`, as
    one window of `at_tool_quantities`, and the permutation that puts its
    ascending channels back in the order given. Refuses a grid the example
    does not reproduce within 1 Hz."""
    order = np.argsort(frequencies_hz)
    ascending = np.asarray(frequencies_hz, dtype=float)[order]
    n = ascending.size
    step = float(ascending[1] - ascending[0]) if n > 1 else 1.0e6
    # The tool's grid has its reference channel (n - 1) // 2 at the centre,
    # half a channel below it when n is even.
    centre = float(ascending[(n - 1) // 2]) + (0.5 * step if n % 2 == 0 else 0.0)
    config = {
        "profiles": [profile(site)],
        "bands": [{"centre_ghz": centre * 1e-9, "width_ghz": n * step * 1e-9, "res_ghz": step * 1e-9, "channels": n}],
        "sets": sets,
    }
    result = json.loads(
        subprocess.run([model], input=json.dumps(config), capture_output=True, text=True, check=True).stdout
    )
    found = result["profiles"][0]["windows"][0]
    got = np.asarray(found["freq_hz"], dtype=float)
    if got.size != n or not np.allclose(got, ascending, rtol=0.0, atol=1.0):
        raise RuntimeError("casa-atmosphere's channels are not the frequencies asked for")
    return found, np.argsort(order)


def casars_sky(model: str):
    """`compare_sm_ms.at_tool_sky` from casa-rs's ATM: for a site (as
    `compare_sm_ms.atm_site` gives it), each channel's zenith opacity (dry,
    and wet for its water column) and its sky-only Tebb at each airmass,
    [channel, airmass]."""

    def sky(site: dict, frequencies_hz: np.ndarray, airmasses: np.ndarray) -> tuple:
        water = float(site["pwv_mm"]) * 1e-3
        sets = [{"water_m": water, "airmass": float(airmass)} for airmass in airmasses]
        found, inverse = window(model, dict(site, temperature_k=site["tground"]), frequencies_hz, sets)
        tau = (np.asarray(found["dry"]) + np.asarray(found["set_wet"][0]))[inverse]
        tebb = np.asarray(found["set_tebb"], dtype=float).T[inverse]
        return tau, tebb

    return sky


def casars_wet_delays(model: str, pwv_mm: float, frequencies_hz: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """casa-rs's zenith (dispersive, non-dispersive) wet phase delay,
    radians, of `pwv_mm` (in single precision, as `settrop` keeps it) at
    `settrop`'s site, at the regular grid `frequencies_hz`."""
    water = float(np.float32(pwv_mm)) * 1e-3
    found, inverse = window(model, SETTROP_SITE, frequencies_hz, [{"water_m": water, "airmass": 1.0}])
    dispersive = np.radians(np.asarray(found["set_disp_phase"][0], dtype=float))[inverse]
    continuum = np.radians(np.asarray(found["set_nondisp_phase"][0], dtype=float))[inverse]
    return dispersive, continuum
