# SPDX-License-Identifier: LGPL-3.0-or-later
"""Tropospheric phase for `compare_sm_ms.py` (simulation Phase 4d, #841):
`sm.settrop` then `sm.corrupt`.

CASA and casa-rs draw their screens from different random streams and
follow different delay models, so each implementation is checked against
its own model, from its `Tf Jones` table and its DATA over a noise-free
twin (a 1 Jy point at the phase centre):

- **model** (`excess_from_table`). A table row's phase in channel `c` is
  `δ · φ(c)`, one fractional water-vapour excess δ per row times the
  zenith wet phase delay `φ` of `pwv` at `settrop`'s site (CASA's
  simulator defaults). CASA's `φ` is ATM's dispersive part on its grid
  half a band below each channel (`atm_frequencies(..., 'casa')`);
  casa-rs's is the dispersive and non-dispersive parts together at each
  channel's CHAN_FREQ. CASA's comes from casatools' `at` tool
  (`at_tool_wet_delays`), whose delays are for its profile's own water
  column and are scaled to `pwv` (as ATM's `SkyStatus` scales them);
  casa-rs's from casa-atmosphere at `pwv` (`atm_model.casars_wet_delays`),
  which corrects ATM's data errors. δ recovered channel by channel must be
  one value per row within `MODEL_TOLERANCE` (single precision), on the
  channels whose delay is at least a twentieth of the window's largest.
- **data** (`data_failures`). DATA over the twin is `g_p(c) · conj(g_q(c))`
  from the table at the row's time, within `DATA_TOLERANCE`.
- **excess** (`excess_statistics`). Each antenna's δ over the 10 s slots:
  its mean, its root-mean-square about it and its structure function at a
  few lags. casa-rs's `individual` series are held to `deltapwv`; CASA's
  normalise with values read past the end of their transforms
  (docs/CASA (C++) bugs.md) and are reported, as is a screen's sampled
  excess, whose root-mean-square is the whole screen's.

`test_compare_sm_ms.py` tests the pure-NumPy parts.
"""

from __future__ import annotations

import numpy as np

# δ's largest spread across a row's channels, relative to its size: the
# phases are single precision.
MODEL_TOLERANCE = 1e-4

# The largest difference between DATA over its twin and the table's
# g_p·conj(g_q), in Jy: single precision.
DATA_TOLERANCE = 1e-5

# A row's channels whose delay is at least this fraction of the window's
# largest carry the excess; near a line's dispersive zero the quotient is
# all rounding.
DELAY_FLOOR = 0.05

# casa-rs's `individual` series' root-mean-square, relative to `deltapwv`:
# the series are normalised to it exactly, so this checks the excess read
# back through casa-rs's own delays, and so the delays' absolute size
# (2.6e-8 measured in sm_trop_scenarios.json).
RMS_TOLERANCE = 1e-5

# The slots' width without `simint`, seconds (`TJones::createCorruptor`).
SLOT_S = 10.0

# Lags, in slots, of the excess's structure function.
LAGS = (1, 2, 4, 8)


def at_tool_wet_delays(pwv_mm: float, frequencies_hz: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """ATM's zenith (dispersive, non-dispersive) wet phase delay, radians,
    of `pwv_mm` at `settrop`'s site, at the regular grid `frequencies_hz`
    (ascending or descending), through casatools' `at` tool."""
    from casatools import atmosphere

    order = np.argsort(frequencies_hz)
    ascending = np.asarray(frequencies_hz, dtype=float)[order]
    at = atmosphere()
    at.initAtmProfile(
        altitude={"value": 5000.0, "unit": "m"},
        temperature={"value": 270.0, "unit": "K"},
        pressure={"value": 560.0, "unit": "mbar"},
        maxAltitude={"value": 48.0, "unit": "km"},
        humidity=20.0,
        dTem_dh={"value": -5.6, "unit": "K/km"},
        dP={"value": 10.0, "unit": "mbar"},
        dPm=1.2,
        h0={"value": 2.0, "unit": "km"},
        atmType=1,
    )
    n = ascending.size
    step = float(ascending[1] - ascending[0]) if n > 1 else 1.0e6
    centre = float(ascending[(n - 1) // 2]) + (0.5 * step if n % 2 == 0 else 0.0)
    at.initSpectralWindow(
        nbands=1,
        fCenter={"value": [centre], "unit": "Hz"},
        fWidth={"value": [n * step], "unit": "Hz"},
        fRes={"value": [step], "unit": "Hz"},
    )
    # The tool's delays are for its profile's own water column.
    scale = float(np.float32(pwv_mm)) / float(at.getGroundWH2O()["value"][0])

    def degrees(quantity) -> float:
        value = float(np.ravel(quantity["value"])[0])
        return value if quantity["unit"] == "deg" else float(np.degrees(value))

    dispersive = np.array([degrees(at.getDispersiveWetPhaseDelay(c, 0)) for c in range(n)])
    continuum = np.array([degrees(at.getNonDispersiveWetPhaseDelay(c, 0)) for c in range(n)])
    at.done()
    inverse = np.argsort(order)
    return np.radians(dispersive[inverse] * scale), np.radians(continuum[inverse] * scale)


def excess_from_table(phases: np.ndarray, delays: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Each row's excess δ and its spread across the channels, relative to
    its size, from `phases` (`[row, channel]`, radians) and the model's
    `delays` (`[channel]`), on the channels whose delay is at least
    `DELAY_FLOOR` of the largest."""
    use = np.abs(delays) >= DELAY_FLOOR * np.abs(delays).max()
    quotients = phases[:, use] / delays[use]
    excess = np.median(quotients, axis=1)
    spread = np.max(np.abs(quotients - excess[:, None]), axis=1) / np.maximum(np.abs(excess), 1e-12)
    return excess, spread


def data_failures(ratio: np.ndarray, expected: np.ndarray) -> tuple[float, list[str]]:
    """The largest difference between DATA over its twin (`ratio`) and the
    table's `g_p·conj(g_q)` (`expected`), and its failure past
    `DATA_TOLERANCE`."""
    worst = float(np.max(np.abs(ratio - expected))) if ratio.size else 0.0
    if not np.isfinite(worst) or worst > DATA_TOLERANCE:
        return worst, [f"DATA over its twin differs from the table by {worst:.3g}"]
    return worst, []


def slot_of(times: np.ndarray) -> np.ndarray:
    """Each solution's slot: the even slot of `SLOT_S` nearest its time,
    from the earliest, the earlier of two as near
    (`CalCorruptor::setCurrTime`)."""
    position = (times - times.min()) / SLOT_S - 0.5
    return np.maximum(np.ceil(position - 0.5), 0).astype(int)


def excess_statistics(excess: np.ndarray, antennas: np.ndarray, slots: np.ndarray) -> dict:
    """Each antenna's excess over the slots (one value per slot): its mean,
    root-mean-square about it, and structure function at `LAGS`."""
    found = {}
    for antenna in np.unique(antennas):
        rows = antennas == antenna
        series = {}
        for slot, value in zip(slots[rows], excess[rows]):
            series.setdefault(int(slot), float(value))
        values = np.array([series[slot] for slot in sorted(series)])
        found[int(antenna)] = {
            "slots": int(values.size),
            "mean": float(values.mean()),
            "rms": float(values.std()),
            "structure": [
                float(np.mean((values[lag:] - values[:-lag]) ** 2)) if values.size > lag else None
                for lag in LAGS
            ],
        }
    return found


def rms_failures(statistics: dict, delta_pwv: float) -> list[str]:
    """casa-rs's `individual` series' root-mean-square, each `deltapwv`
    within `RMS_TOLERANCE`."""
    return [
        f"antenna {antenna}: excess root-mean-square {found['rms']:.6g}, not {delta_pwv}"
        for antenna, found in statistics.items()
        if not abs(found["rms"] - delta_pwv) <= RMS_TOLERANCE * delta_pwv
    ]
