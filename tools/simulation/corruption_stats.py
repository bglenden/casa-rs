# SPDX-License-Identifier: LGPL-3.0-or-later
"""Statistics of antenna-based corruption for `compare_sm_ms.py` (simulation
Phase 3, #841): `sm.setgain(mode='fbm')` and `sm.setleakage` then
`sm.corrupt`.

CASA and casa-rs draw their gains and leakages from different random
streams, so each implementation's DATA is checked against what CASA's
corruptors simulate, from its quotient (gains) or difference (leakage)
with its noise-free twin, a 1 Jy unpolarised point at the phase centre:

- **gains** (`gain_statistics`). Per integration and parallel-hand
  correlation, the quotient `R_pq = g_p·conj(g_q)` is fitted by
  least squares as antenna-based: `log|R_pq| = a_p + a_q` (an
  autocorrelation `2·a_p`) and `arg R_pq = φ_p − φ_q`.
  - `closure`: the fits' largest residuals are at most `CLOSURE_TOLERANCE`
    (float32 rounding), and an autocorrelation's phase is zero.
  - `amplitude`: each antenna's `|g| = exp(a)` has, over the
    integrations, the standard deviation `gain_scale` (CASA normalises
    every fBM amplitude screen so), pooled over antennas within
    `SCALE_TOLERANCE`.
  - `phase`: each antenna's phase, referred to the antennas' mean, has the
    standard deviation `π·gain_scale` (corrected for the reference's
    share), pooled within `SCALE_TOLERANCE`.

  Antennas with unit gain throughout are left out of the scales and
  reported; casa-rs must have none. CASA has its last antenna's when
  autocorrelations are written, and others' when a screen's
  normalisation takes a huge value from past the end of its FFT's output,
  so CASA's are not checked. CASA's screens copy their last slots from
  there and normalise with them, so its scales vary from run to run
  (docs/CASA (C++) bugs.md): a scenario's `casa_gain_checks` leaves them
  out for CASA. Instead the screens' shape is compared across the two
  implementations (`structure_failures`): their autocorrelations at
  `LAGS`, which a screen's scale does not change, agree within
  `STRUCTURE_TOLERANCE`.
- **leakage** (`leakage_statistics`). The parallel hands are unchanged; the
  cross hands, over the twin's parallel hand, are `XY_pq = d_px +
  conj(d_qy)` and `YX_pq = d_py + conj(d_qx)`, constant through a
  spectral window (CASA draws one leakage per window for the whole run).
  With CASA's draws, `d = (a_re·n₁, a_im·n₂) + offset` and the second
  receptor's offset real part negated, the real and imaginary parts of
  both have mean zero, whatever the offset, and mean squares `2·a_re²` and
  `2·a_im²` (`leakage_amplitude`), each within `LEAKAGE_STANDARD_ERRORS`
  standard errors.

Only NumPy is needed; `test_corruption_stats.py` tests both.
"""

from __future__ import annotations

import numpy as np

# Largest residual of the antenna-based fits: float32 rounding of a few
# products, in natural-log amplitude and in radians.
CLOSURE_TOLERANCE = 1e-4
# Pooled screen scales pass within this fraction.
SCALE_TOLERANCE = 0.10
# A gain is unit when every |g| is within this of 1.
UNIT_TOLERANCE = 1e-6
# The screens' autocorrelations are compared at these lags (integrations)
# and agree within STRUCTURE_TOLERANCE. They leave out the last
# integrations, which CASA's screens fill from past the end of their FFT.
LAGS = (1, 2, 4)
STRUCTURE_TOLERANCE = 0.1
TRAILING_INTEGRATIONS = 2
# Leakage moments pass within this many standard errors. Over the
# baselines of N antennas' draws per window, a mean of `x_p + y_q` has
# variance about MEAN_VARIANCE·a²/N, and a mean square over 2a² about
# SQUARE_VARIANCE/N (Monte Carlo, N = 43, 1 and 4 windows).
LEAKAGE_STANDARD_ERRORS = 5.0
MEAN_VARIANCE = 3.0
SQUARE_VARIANCE = 1.5
# The parallel hands, and the cross hands through a window, are constant
# within this fraction of the model.
CONSTANT_TOLERANCE = 1e-5


def integrations(time: np.ndarray, antenna1: np.ndarray, antenna2: np.ndarray):
    """The rows as [integration, baseline] indices, the baselines as
    (antenna1, antenna2) pairs in the first integration's order, or a
    failure when integrations have other baselines."""
    times, inverse = np.unique(time, return_inverse=True)
    first = np.flatnonzero(inverse == 0)
    baselines = list(zip(antenna1[first].tolist(), antenna2[first].tolist()))
    position = {baseline: index for index, baseline in enumerate(baselines)}
    rows = np.full((times.size, len(baselines)), -1, dtype=np.int64)
    for row, (integration, p, q) in enumerate(zip(inverse, antenna1, antenna2)):
        index = position.get((int(p), int(q)))
        if index is None or rows[integration, index] >= 0:
            return None, baselines, "integrations differ in their baselines"
        rows[integration, index] = row
    if np.any(rows < 0):
        return None, baselines, "integrations differ in their baselines"
    return rows, baselines, None


def parallel_correlations(model: np.ndarray) -> list[int]:
    """The correlations where the twin has power: the parallel hands of an
    unpolarised model."""
    return [c for c in range(model.shape[1]) if np.any(model[:, c, :] != 0)]


def antenna_fits(ratio: np.ndarray, baselines: list[tuple[int, int]], antennas: list[int]):
    """Least-squares antenna-based fits of [integration, baseline] quotients:
    (log-amplitudes, phases) as [antenna, integration] and their largest
    residuals. Phases are fitted on cross-correlations, referred to the
    antennas' mean (the minimum-norm solution)."""
    index = {antenna: i for i, antenna in enumerate(antennas)}
    amplitude = np.zeros((len(baselines), len(antennas)))
    phase = np.zeros((len(baselines), len(antennas)))
    for b, (p, q) in enumerate(baselines):
        amplitude[b, index[p]] += 1.0
        amplitude[b, index[q]] += 1.0
        if p != q:
            phase[b, index[p]] += 1.0
            phase[b, index[q]] -= 1.0
    logs = np.log(np.abs(ratio)).T
    a = np.linalg.lstsq(amplitude, logs, rcond=None)[0]
    cross = np.array([p != q for p, q in baselines])
    angles = np.angle(ratio).T
    phi = np.linalg.lstsq(phase[cross], angles[cross], rcond=None)[0]
    residual = {
        "amplitude": float(np.abs(amplitude @ a - logs).max()),
        "phase": float(np.abs(phase[cross] @ phi - angles[cross]).max()),
        "auto_phase": float(np.abs(angles[~cross]).max()) if (~cross).any() else 0.0,
    }
    return a, phi, residual


def autocorrelations(series: np.ndarray) -> list[float]:
    """The [antenna, integration] screens' autocorrelations at `LAGS`,
    each screen about its mean over its standard deviation, pooled; the
    last `TRAILING_INTEGRATIONS` integrations are left out."""
    x = series[:, : series.shape[1] - TRAILING_INTEGRATIONS]
    x = x - x.mean(axis=1, keepdims=True)
    deviation = x.std(axis=1, keepdims=True)
    x = x / np.where(deviation > 0.0, deviation, 1.0)
    return [float(np.mean(np.sum(x[:, lag:] * x[:, :-lag], axis=1) / x.shape[1])) for lag in LAGS]


def screen_autocorrelations(stats: dict) -> list[float]:
    """A side's gain autocorrelations at `LAGS`, averaged over its
    correlations' amplitude and phase screens."""
    entries = [
        entry["autocorrelation"][kind]
        for key, entry in stats.items()
        if key.startswith("corr")
        for kind in ("amplitude", "phase")
    ]
    return np.mean(entries, axis=0).tolist() if entries else []


def structure_failures(casa: dict, casars: dict) -> tuple[dict, list[str]]:
    """CASA's and casa-rs's screen autocorrelations, and a failure at each
    lag where they differ by more than `STRUCTURE_TOLERANCE`."""
    a, b = screen_autocorrelations(casa), screen_autocorrelations(casars)
    stats = {"lags": list(LAGS), "casa": a, "casa-rs": b}
    if len(a) != len(LAGS) or len(b) != len(LAGS):
        return stats, ["gain screens: no autocorrelations to compare"]
    failures = [
        f"gain screens: autocorrelation at lag {lag} is {x:.3f} in CASA, {y:.3f} in casa-rs"
        for lag, x, y in zip(LAGS, a, b)
        if not abs(x - y) <= STRUCTURE_TOLERANCE
    ]
    return stats, failures


def gain_statistics(
    side: dict,
    twin: dict,
    scale: float,
    checks: list[str],
    unit_expected: list[int] | None,
) -> tuple[dict, list[str]]:
    """Gain statistics of one implementation against its twin, and the
    failures (see the module documentation); its unit-gain antennas must
    be `unit_expected` unless that is None."""
    rows, baselines, failure = integrations(side["time"], side["antenna1"], side["antenna2"])
    if failure:
        return {}, [failure]
    antennas = sorted({antenna for baseline in baselines for antenna in baseline})
    stats, failures = {"integrations": int(rows.shape[0]), "antennas": len(antennas)}, []
    unit = None
    for c in parallel_correlations(twin["data"]):
        # Gains do not depend on frequency: the channels' mean quotient.
        quotient = (side["data"][:, c, :] / twin["data"][:, c, :]).mean(axis=1)
        a, phi, residual = antenna_fits(quotient[rows], baselines, antennas)
        gains = np.exp(a + 1j * phi)
        units = [
            antenna
            for antenna, g in zip(antennas, np.abs(np.exp(a)))
            if np.all(np.abs(g - 1.0) <= UNIT_TOLERANCE)
        ]
        unit = units if unit is None else sorted(set(unit) & set(units))
        kept = [i for i, antenna in enumerate(antennas) if antenna not in units]
        amplitudes = np.abs(gains[kept])
        phases = phi[kept] - phi[kept].mean(axis=0)
        share = np.sqrt(len(kept) / max(len(kept) - 1, 1))
        entry = {
            "closure": residual,
            "amplitude_scale": float(np.sqrt(np.mean(np.var(amplitudes, axis=1))) / scale),
            "phase_scale": float(share * np.sqrt(np.mean(np.var(phases, axis=1))) / (np.pi * scale)),
            "autocorrelation": {
                "amplitude": autocorrelations(a[kept]),
                "phase": autocorrelations(phases),
            },
            "unit_antennas": units,
        }
        stats[f"corr{c}"] = entry
        if "closure" in checks:
            for key, value in residual.items():
                if not value <= CLOSURE_TOLERANCE:
                    failures.append(f"correlation {c}: gains not antenna-based ({key} residual {value:.3g})")
        for check in ("amplitude", "phase"):
            ratio = entry[f"{check}_scale"]
            if check in checks and not abs(ratio - 1.0) <= SCALE_TOLERANCE:
                failures.append(f"correlation {c}: {check} scale {ratio:.3f} of the expected")
    if unit is None:
        return stats, ["no parallel hands with model power"]
    stats["unit_antennas"] = sorted(unit)
    if unit_expected is not None and sorted(unit) != sorted(unit_expected):
        failures.append(f"unit-gain antennas {sorted(unit)}, expected {sorted(unit_expected)}")
    return stats, failures


def leakage_statistics(side: dict, twin: dict, amplitude: list[float]) -> tuple[dict, list[str]]:
    """Leakage statistics of one implementation against its twin, and the
    failures (see the module documentation)."""
    data, model = side["data"], twin["data"]
    if data.shape[1] != 4:
        return {}, ["leakage needs four correlations"]
    peak = float(np.abs(model).max())
    if peak == 0.0:
        return {}, ["the twin has no model power"]
    failures = []
    parallel = float(np.abs(data[:, [0, 3], :] - model[:, [0, 3], :]).max()) / peak
    if not parallel <= CONSTANT_TOLERANCE:
        failures.append(f"the parallel hands changed by {parallel:.3g} of the model")
    cross_rows = side["antenna1"] != side["antenna2"]
    # The cross hands over the parallel hand that leaks into them.
    leaked = (data[:, [1, 2], :] - model[:, [1, 2], :]) / model[:, [0], :]
    leaked = leaked[cross_rows].mean(axis=2)
    window = side["data_desc"][cross_rows]
    # Each window and baseline's rows against their mean.
    keys = np.stack([window, side["antenna1"][cross_rows], side["antenna2"][cross_rows]], axis=1)
    _, group = np.unique(keys, axis=0, return_inverse=True)
    group = group.ravel()
    sums = np.zeros((group.max() + 1, 2), dtype=leaked.dtype)
    np.add.at(sums, group, leaked)
    means = sums / np.bincount(group)[:, None]
    drift = float(np.abs(leaked - means[group]).max())
    if not drift <= CONSTANT_TOLERANCE:
        failures.append(f"the leakage changes within a spectral window by {drift:.3g}")
    antennas = np.unique(np.concatenate([side["antenna1"], side["antenna2"]])).size
    draws = antennas * np.unique(window).size
    stats = {"parallel_change": parallel, "window_drift": drift, "draws": int(draws)}
    for hand, label in ((0, "XY"), (1, "YX")):
        for part, values, sigma in (
            ("re", leaked[:, hand].real, amplitude[0]),
            ("im", leaked[:, hand].imag, amplitude[1]),
        ):
            mean = float(values.mean())
            square = float(np.mean(values**2)) / (2.0 * sigma**2)
            mean_error = sigma * np.sqrt(MEAN_VARIANCE / draws)
            square_error = np.sqrt(SQUARE_VARIANCE / draws)
            stats[f"{label}_{part}"] = {
                "mean": mean,
                "mean_standard_errors": mean / mean_error,
                "square_scale": square,
                "square_standard_errors": (square - 1.0) / square_error,
            }
            if not abs(mean) <= LEAKAGE_STANDARD_ERRORS * mean_error:
                failures.append(f"{label} {part}: mean {mean:.3g}, {mean / mean_error:.1f} standard errors from zero")
            if not abs(square - 1.0) <= LEAKAGE_STANDARD_ERRORS * square_error:
                failures.append(
                    f"{label} {part}: mean square {square:.3f} of 2·amplitude², "
                    f"{(square - 1.0) / square_error:.1f} standard errors"
                )
    return stats, failures
