#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare casa-rs simulator MSs with CASA's (simulation Phases 1c and 1d, #841).

Run with CASA's Python, which reads both. For every scenario in the CASA
oracle's manifest, the comparison opens `<casa>/<name>.ms` and
`<casars>/<name>.ms` and checks:

- **the tables:** which subtables exist, and their row counts;
- **the descriptors:** each table's TableInfo (type and subType), its
  non-table keywords, and every column's value type, dimensionality,
  fixed-shape option and keywords (units and MEASINFO);
- **the cells:** every column, row by row: which cells are defined, their
  shapes and their values. Floats pass within a per-column tolerance, and
  UVW's largest difference is reported.
- **DATA**: which cells are defined and their shapes, as every column; its
  values by the scenario's `data` (default `zero`):
  - `zero` (observe only): DATA is zero in both, and its cells are compared
    as every column's are.
  - `model` (predicted, no noise): the normalized RMS of casa-rs against
    CASA, NRMS = sqrt(sum |casa-rs - CASA|^2 / sum |CASA|^2) over every
    complex sample of the rows FLAG_ROW leaves unflagged in both MSs (the
    convention of Phase 1a's `predict_vs_casa`), must be at most
    `--data-nrms` (1e-3, plan Phase 1 acceptance). It is also reported per
    correlation and for cross- and autocorrelation rows apart. A
    correlation with no reference power (the cross-hands of a Stokes I
    model) is checked by its largest difference instead, which must be at
    most 1e-6 of CASA's peak; a scenario with no reference power at all
    fails. Rows flagged in both are reported by their largest value.
  - `noisy` (predicted and corrupted): the draws differ, so each
    implementation's noise is its DATA minus its noise-free `twin`
    scenario's. Per correlation, the variance of the noise's real and
    imaginary parts on cross-correlations and of its real part on
    autocorrelations must be the scenario's `noise_variance` within five
    standard errors in both, the autocorrelations' imaginary noise must be
    zero, and a two-sample Kolmogorov-Smirnov test compares the two
    implementations' normalized cross-correlation noise.

    A scenario whose noise is one `setnoise(mode='tsys-manual')` term has no
    single variance: each row's σ comes from its antennas' system
    temperatures (`tsys_manual_sigma`, CASA's `AtmosCorruptor`, with the
    elevations casatools' measures give at each row's TIME). On the rows
    unflagged in both, each implementation's noise divided by that σ must
    have unit variance per correlation and class, as above, and the KS test
    compares the normalized noise. Without the formula, CASA's and casa-rs's
    variances are compared directly in groups of rows (spectral window,
    dish pair and time bin, `grouped_variance_ratios`): each group's ratio
    must be 1 within five of its standard errors.
  - `gains` and `leakage` (`sm.setgain` or `sm.setleakage`, then
    `corrupt`; Phase 3): the draws differ, so each implementation's DATA
    is checked against what CASA's corruptors simulate, from its quotient
    or difference with its noise-free `twin` (`corruption_stats.py`
    says how).

  A NaN or infinite DATA or noise sample fails a `model`, `noisy`, `gains`
  or `leakage` scenario, as does a statistic that is not a number (#853
  review).
- **calibration tables**: a scenario's `tables`, written by `setgain` or
  `setleakage` beside each side's MS, must agree in what is not drawn
  (`table_differences`): keywords and subtype, columns, rows, TIME,
  the identifying columns, and CPARAM's shapes and FLAG.

Data managers are not compared; the plan lets them differ. A scenario's
`ignore` entries (`TABLE.COLUMN`) are skipped. The exit status is 1 if any
scenario differs.

`test_compare_sm_ms.py` tests the cell comparison and the DATA statistics'
refusal of non-finite values; it needs only NumPy, as
`casatools` is imported only to read tables.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import numpy as np

from corruption_stats import gain_statistics, leakage_statistics, structure_failures

# What a `gains` scenario checks: on casa-rs always, on CASA unless the
# scenario's `casa_gain_checks` says otherwise.
GAIN_CHECKS = ["closure", "amplitude", "phase"]

# Absolute tolerances by column, added to RELATIVE_TOLERANCE times the
# value; other float columns use DEFAULT_TOLERANCE. Times are seconds near
# 5e9, where a double's spacing is about 1e-6 s.
TOLERANCES = {
    "UVW": 1e-3,
    "TIME": 2e-6,
    "TIME_CENTROID": 2e-6,
    "TIME_RANGE": 2e-6,
    "TIME_ORIGIN": 2e-6,
    "POSITION": 1e-6,
    "DIRECTION": 1e-12,
    "TARGET": 1e-12,
    "PHASE_DIR": 1e-12,
    "DELAY_DIR": 1e-12,
    "REFERENCE_DIR": 1e-12,
}
DEFAULT_TOLERANCE = 1e-9
RELATIVE_TOLERANCE = 1e-14

# DATA of predicted scenarios: the plan's Phase 1 acceptance.
DATA_NRMS = 1e-3
# A correlation without reference power passes when its largest difference is
# at most this fraction of CASA's peak.
ZERO_POWER_FRACTION = 1e-6
# Noise variances pass within this many standard errors.
NOISE_STANDARD_ERRORS = 5.0

# Differences that are expected, with the reason. A difference whose key
# (`TABLE.COLUMN:aspect`) is listed is reported as known and does not fail.
KNOWN = {
    "MAIN.MODEL_DATA:missing": "plan decision 6: DATA only",
    "MAIN.CORRECTED_DATA:missing": "plan decision 6: DATA only",
    "MAIN.UVW:keywords": "casa-ms's UVW MEASINFO lacks casacore's default Ref ITRF (#847)",
    "POINTING.TARGET:ndim": "casa-ms's POINTING.TARGET has ndim 2, casacore's -1 (#847)",
    "SOURCE.DIRECTION:ndim": "CASA's simulator makes SOURCE arrays variable-shaped; casa-ms follows casacore's MSSource (fixed [2])",
    "SOURCE.DIRECTION:shape": "as SOURCE.DIRECTION:ndim",
    "SOURCE.DIRECTION:option": "as SOURCE.DIRECTION:ndim",
    "SOURCE.PROPER_MOTION:ndim": "as SOURCE.DIRECTION:ndim",
    "SOURCE.PROPER_MOTION:shape": "as SOURCE.DIRECTION:ndim",
    "SOURCE.PROPER_MOTION:option": "as SOURCE.DIRECTION:ndim",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--casa", required=True, help="CASA oracle output (with manifest.json)")
    parser.add_argument("--casars", required=True, help="casa-rs output directory")
    parser.add_argument("--only", nargs="*", help="compare only these scenarios")
    parser.add_argument("--uvw-tolerance-m", type=float, default=TOLERANCES["UVW"])
    parser.add_argument("--data-nrms", type=float, default=DATA_NRMS, help="largest DATA NRMS of a model scenario")
    parser.add_argument("--json", help="write the report here as JSON")
    parser.add_argument("--verbose", action="store_true", help="also list the known differences")
    return parser.parse_args()


def subtables(tb, path: str) -> dict[str, str]:
    tb.open(path)
    keywords = tb.getkeywords()
    tb.close()
    return {
        name: value.split("Table: ", 1)[1]
        for name, value in keywords.items()
        if isinstance(value, str) and value.startswith("Table: ")
    }


def describe(tb, path: str) -> dict:
    tb.open(path)
    info = tb.info()
    keywords = {
        key: value
        for key, value in tb.getkeywords().items()
        if not (isinstance(value, str) and value.startswith("Table: "))
    }
    columns = {}
    for name in tb.colnames():
        desc = tb.getcoldesc(name)
        columns[name] = {
            "valueType": desc.get("valueType"),
            "ndim": desc.get("ndim", 0),
            "shape": list(desc.get("shape", [])),
            "option": desc.get("option", 0),
            "keywords": desc.get("keywords", {}),
        }
    rows = tb.nrows()
    tb.close()
    return {"info": info, "keywords": keywords, "columns": columns, "rows": rows}


def cells(tb, path: str, column: str, rows: int):
    tb.open(path)
    try:
        if rows == 0:
            return []
        values = []
        for row in range(rows):
            if not tb.iscelldefined(column, row):
                values.append(None)
                continue
            try:
                values.append(tb.getcell(column, row))
            except RuntimeError as error:
                values.append(f"<unreadable: {error}>")
        return values
    finally:
        tb.close()


def compare_values(a, b, tolerance: float) -> tuple[str | None, float | None, str]:
    """How two cell lists differ (None if they agree, else the aspect), the
    largest numeric difference, and a note. Floats agree within `tolerance`
    plus RELATIVE_TOLERANCE of their size."""
    if len(a) != len(b):
        return "cells", None, f"{len(a)} against {len(b)} cells"
    largest = 0.0
    numeric = False
    for row, (x, y) in enumerate(zip(a, b)):
        if (x is None) != (y is None) or isinstance(x, str) != isinstance(y, str):
            return "defined", None, f"row {row}: CASA {'undefined' if x is None else repr(x)[:40]}, casa-rs {'undefined' if y is None else repr(y)[:40]}"
        if x is None:
            continue
        xa, ya = np.asarray(x), np.asarray(y)
        if xa.shape != ya.shape:
            return "shape", None, f"row {row}: shape {xa.shape} against {ya.shape}"
        if xa.dtype.kind in "fc" or ya.dtype.kind in "fc":
            numeric = True
            if xa.size:
                xc, yc = xa.astype(complex), ya.astype(complex)
                difference = np.abs(xc - yc)
                if not np.all(np.isfinite(difference)):
                    return "values", None, f"row {row}: non-finite"
                largest = max(largest, float(np.max(difference)))
                allowed = tolerance + RELATIVE_TOLERANCE * np.maximum(np.abs(xc), np.abs(yc))
                if np.any(difference > allowed):
                    index = int(np.argmax(difference - allowed))
                    return "values", largest, (
                        f"row {row}: difference {float(difference.flat[index]):.3g} over "
                        f"{float(allowed.flat[index]):.3g} ({xc.flat[index]} against {yc.flat[index]})"
                    )
        elif not np.array_equal(xa, ya):
            return "values", None, f"row {row}: {x!r} against {y!r}"
    return None, (largest if numeric else None), ""


def data_differences(casa: list, casars: list) -> list[tuple[str, str]]:
    """MAIN.DATA's differences, as (`TABLE.COLUMN:aspect`, message) pairs:
    nonzero data on either side, then the comparison every column gets
    (which cells are defined, their shapes and their values)."""
    found = []
    for side, values in (("CASA", casa), ("casa-rs", casars)):
        if any(value is not None and np.any(np.asarray(value) != 0) for value in values):
            found.append(("MAIN.DATA:nonzero", f"MAIN.DATA: {side} has nonzero data"))
    aspect, _, note = compare_values(casa, casars, TOLERANCES.get("DATA", DEFAULT_TOLERANCE))
    if aspect is not None:
        found.append((f"MAIN.DATA:{aspect}", f"MAIN.DATA: {note}"))
    return found


def data_cell_differences(casa: list, casars: list) -> list[tuple[str, str]]:
    """MAIN.DATA's cells compared by definedness and shape only, for
    scenarios whose values differ by design (predicted or noisy); their
    values are compared by NRMS or noise statistics instead."""
    for row, (a, b) in enumerate(zip(casa, casars)):
        if (a is None) != (b is None):
            return [("MAIN.DATA:defined", f"MAIN.DATA: row {row} defined {a is not None} against {b is not None}")]
        if a is not None and np.shape(a) != np.shape(b):
            return [("MAIN.DATA:shape", f"MAIN.DATA: row {row} shape {np.shape(a)} against {np.shape(b)}")]
    return []


def normalise_keywords(value):
    """Keywords as JSON-comparable values (NumPy arrays become lists)."""
    if isinstance(value, dict):
        return {key: normalise_keywords(item) for key, item in value.items()}
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.generic,)):
        return value.item()
    return value


def main_columns(tb, path: str) -> dict:
    """MAIN's DATA as [row, corr, chan], FLAG_ROW, which rows are
    autocorrelations, and the antennas, TIME and DATA_DESC_ID."""
    tb.open(path)
    try:
        data = np.transpose(tb.getcol("DATA"), (2, 0, 1))
        flag_row = np.asarray(tb.getcol("FLAG_ROW"), dtype=bool)
        antenna1 = np.asarray(tb.getcol("ANTENNA1"))
        antenna2 = np.asarray(tb.getcol("ANTENNA2"))
        time = np.asarray(tb.getcol("TIME"))
        data_desc = np.asarray(tb.getcol("DATA_DESC_ID"))
    finally:
        tb.close()
    return {
        "data": data.astype(np.complex128),
        "flag_row": flag_row,
        "auto": antenna1 == antenna2,
        "antenna1": antenna1,
        "antenna2": antenna2,
        "time": time,
        "data_desc": data_desc,
    }


def non_finite(label: str, values: np.ndarray) -> list[str]:
    """A failure when any of `values` is NaN or infinite (#853 review: a NaN
    statistic compares false against every limit, so it would pass)."""
    count = int(values.size - np.count_nonzero(np.isfinite(values)))
    return [f"{label}: {count} non-finite values"] if count else []


def nrms(ours: np.ndarray, casa: np.ndarray) -> float | None:
    """sqrt(sum |ours - casa|^2 / sum |casa|^2), or None without reference power."""
    reference = float(np.sum(np.abs(casa) ** 2))
    if reference == 0.0:
        return None
    return float(np.sqrt(np.sum(np.abs(ours - casa) ** 2) / reference))


def compare_model_data(casa: dict, ours: dict, limit: float) -> tuple[dict, list[str]]:
    """The NRMS of casa-rs's DATA against CASA's over rows unflagged in both,
    and the failures."""
    if casa["data"].shape != ours["data"].shape:
        return {}, [f"MAIN.DATA: shape {casa['data'].shape} against {ours['data'].shape}"]
    failures = non_finite("MAIN.DATA: CASA", casa["data"]) + non_finite(
        "MAIN.DATA: casa-rs", ours["data"]
    )
    if failures:
        return {}, failures
    unflagged = ~casa["flag_row"] & ~ours["flag_row"]
    both_flagged = casa["flag_row"] & ours["flag_row"]
    a, b = casa["data"][unflagged], ours["data"][unflagged]
    stats = {
        "rows_compared": int(unflagged.sum()),
        "samples_compared": int(a.size),
        "rows_flagged_in_one": int((casa["flag_row"] ^ ours["flag_row"]).sum()),
        "rows_flagged_in_both": int(both_flagged.sum()),
        "nrms": nrms(b, a),
        "casa_peak": float(np.abs(a).max()) if a.size else 0.0,
        "max_difference": float(np.abs(b - a).max()) if a.size else 0.0,
    }
    if both_flagged.any():
        stats["flagged_rows_max_abs"] = {
            "casa": float(np.abs(casa["data"][both_flagged]).max()),
            "casa-rs": float(np.abs(ours["data"][both_flagged]).max()),
        }
    if stats["nrms"] is None:
        failures.append("MAIN.DATA: CASA's DATA has no power on the rows compared")
    elif not stats["nrms"] <= limit:
        failures.append(f"MAIN.DATA: NRMS {stats['nrms']:.3e} over the {limit:g} limit")
    per_correlation = []
    for corr in range(a.shape[1]):
        value = nrms(b[:, corr, :], a[:, corr, :])
        difference = float(np.abs(b[:, corr, :] - a[:, corr, :]).max()) if a.size else 0.0
        per_correlation.append({"nrms": value, "max_difference": difference})
        if value is None and difference > ZERO_POWER_FRACTION * max(stats["casa_peak"], 1e-30):
            failures.append(
                f"MAIN.DATA: correlation {corr} has no CASA power and differs by {difference:.3g}"
            )
    stats["per_correlation"] = per_correlation
    auto = casa["auto"][unflagged]
    stats["cross_nrms"] = nrms(b[~auto], a[~auto])
    stats["auto_nrms"] = nrms(b[auto], a[auto]) if auto.any() else None
    return stats, failures


def ks_two_sample(x: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    """The two-sample Kolmogorov-Smirnov statistic and its asymptotic p-value."""
    x, y = np.sort(x), np.sort(y)
    values = np.concatenate([x, y])
    cdf_x = np.searchsorted(x, values, side="right") / x.size
    cdf_y = np.searchsorted(y, values, side="right") / y.size
    statistic = float(np.max(np.abs(cdf_x - cdf_y)))
    effective = np.sqrt(x.size * y.size / (x.size + y.size))
    lam = (effective + 0.12 + 0.11 / effective) * statistic
    terms = [2.0 * (-1.0) ** (k - 1) * np.exp(-2.0 * (k * lam) ** 2) for k in range(1, 101)]
    return statistic, float(min(1.0, max(0.0, sum(terms))))


def noise_classes(noise: np.ndarray, auto: np.ndarray, expected: float) -> tuple[dict, list[str]]:
    """Per-correlation noise variances by class, and their failures."""
    classes, failures = {}, non_finite("noise", noise)
    if failures:
        return classes, failures

    def check(label: str, values: np.ndarray) -> float:
        variance = float(np.var(values))
        tolerance = NOISE_STANDARD_ERRORS * expected * np.sqrt(2.0 / max(values.size, 1))
        if not abs(variance - expected) <= tolerance:
            failures.append(f"{label}: variance {variance:.5f}, expected {expected:.5f} +- {tolerance:.5f}")
        return variance

    for corr in range(noise.shape[1]):
        cross = noise[~auto, corr, :].ravel()
        autos = noise[auto, corr, :].ravel()
        entry = {
            "cross_re_var": check(f"correlation {corr} cross real", cross.real),
            "cross_im_var": check(f"correlation {corr} cross imaginary", cross.imag),
            "cross_re_im_corr": float(np.corrcoef(cross.real, cross.imag)[0, 1]),
            "n_cross": int(cross.size),
        }
        if autos.size:
            entry["auto_re_var"] = check(f"correlation {corr} auto real", autos.real)
            entry["auto_im_max"] = float(np.abs(autos.imag).max())
            if not entry["auto_im_max"] == 0.0:
                failures.append(f"correlation {corr}: autocorrelation imaginary noise {entry['auto_im_max']:.3g}")
        classes[f"corr{corr}"] = entry
    return classes, failures


def compare_noisy_data(paths: dict, expected: float | None) -> tuple[dict, list[str]]:
    """Noise statistics of both implementations against their twins."""
    from casatools import table

    tb = table()
    if expected is None:
        return {}, ["MAIN.DATA: a noisy scenario needs noise_variance"]
    stats, failures, normalized = {}, [], {}
    for side in ("casa", "casa-rs"):
        noisy = main_columns(tb, paths[side])
        clean = main_columns(tb, paths[side + "-twin"])
        if noisy["data"].shape != clean["data"].shape:
            failures.append(f"{side}: the twin's DATA has another shape")
            continue
        noise = noisy["data"] - clean["data"]
        classes, side_failures = noise_classes(noise, noisy["auto"], expected)
        classes["flagged_rows"] = int(noisy["flag_row"].sum())
        stats[side] = classes
        failures += [f"{side} noise: {failure}" for failure in side_failures]
        if np.all(np.isfinite(noise)):  # non-finite noise fails above
            cross = noise[~noisy["auto"]].ravel()
            normalized[side] = np.concatenate([cross.real, cross.imag]) / np.sqrt(expected)
    if len(normalized) == 2:
        statistic, p_value = ks_two_sample(normalized["casa"], normalized["casa-rs"])
        stats["ks_cross"] = {"statistic": statistic, "p_value": p_value}
        failures += ks_failures(statistic, p_value)
    return stats, failures


# CASA's sm.setnoise defaults for the tsys-manual parameters (simulator.xml).
TSYS_DEFAULTS = {
    "tau": 0.1,
    "tatmos": 250.0,
    "antefficiency": 0.8,
    "spillefficiency": 0.85,
    "correfficiency": 0.88,
    "trx": 50.0,
    "tground": 270.0,
    "tcmb": 2.73,
    "senscoeff": 0.0,
    "rxtype": 0,
}
# Time bins of the direct CASA/casa-rs variance comparison.
NOISE_TIME_BINS = 8


def tsys_manual_sigma(
    params: dict,
    frequency_hz: np.ndarray,
    channel_bandwidth_hz: np.ndarray,
    exposure_s: np.ndarray,
    diameters_m: np.ndarray,
    elevations_rad: np.ndarray,
) -> np.ndarray:
    """Each row's σ per real and imaginary draw for CASA's
    `sm.setnoise(mode='tsys-manual')`, in double precision.

    CASA scales a unit `ANoise` by the inverse of a `T` term whose antenna
    value is `D / f / sqrt(Tsys(A))` (`AtmosCorruptor::simPar`,
    `CalCorruptor.cc`), so σ_ij = s_i s_j with
    s = sqrt(c a / sqrt(Δν t) · Tsys(A)) / D, `a = 8 k 1e19 / (η_a η_q π)`,
    `c = senscoeff` (1/√2 when not positive), `Δν` the window's
    |TOTAL_BANDWIDTH| / NUM_CHAN, `t` EXPOSURE, `A = 1/sin(e)` (1000 at
    e <= 0), and `Tsys(A) = (hν/k) R (1 + rxtype)`,
    `R = η_s R(T_cmb) + e^τ (η_s (1 - e^-τ) R(T_atm) + (1 - η_s) R(T_gnd)
    + T_rx / (hν/k))`, `τ = tau·A`, `R(T) = 1/(e^(hν/kT) - 1)`, ν the
    window's REF_FREQUENCY. An antenna whose Tsys overflows single precision
    (below the horizon) applies as 1, as CASA applies a bad `T`.

    `frequency_hz`, `channel_bandwidth_hz` and `exposure_s` hold one value
    per row; `diameters_m` and `elevations_rad` (rows, 2), the row's
    antennas'."""
    p = dict(TSYS_DEFAULTS, **{key: value for key, value in params.items() if key in TSYS_DEFAULTS})
    frequency = np.asarray(frequency_hz, dtype=float)[:, None]
    h_nu_k = 4.80145e-11 * frequency

    def radiance(temperature: float) -> np.ndarray:
        return 1.0 / np.expm1(0.04799274551e-9 * frequency / temperature)

    spill = p["spillefficiency"]
    tatmos = p["tatmos"] if p["tatmos"] > 10 else 250.0
    elevations = np.asarray(elevations_rad, dtype=float)
    with np.errstate(divide="ignore", over="ignore", invalid="ignore"):
        airmass = np.where(elevations > 0, 1.0 / np.sin(elevations), 1000.0)
        tau = p["tau"] * airmass
        r = spill * radiance(p["tcmb"]) + np.exp(tau) * (
            spill * (1.0 - np.exp(-tau)) * radiance(tatmos)
            + (1.0 - spill) * radiance(p["tground"])
            + p["trx"] / h_nu_k
        )
        tsys = h_nu_k * r * (1 + max(int(p["rxtype"]), 0))
        c = p["senscoeff"] if p["senscoeff"] > 0 else 1.0 / np.sqrt(2.0)
        a = 8 * 1.38062e-16 * 1e23 * 1e-4 / (p["antefficiency"] * p["correfficiency"] * np.pi)
        factor = c * a / np.sqrt(np.asarray(channel_bandwidth_hz) * np.asarray(exposure_s))
        scale = np.sqrt(factor[:, None] * tsys) / np.asarray(diameters_m, dtype=float)
    scale = np.where(tsys > np.finfo(np.float32).max, 1.0, scale)
    return scale[:, 0] * scale[:, 1]


def grouped_variance_ratios(
    casa: np.ndarray, ours: np.ndarray, groups: np.ndarray
) -> tuple[list[dict], list[str]]:
    """casa-rs's noise variance over CASA's in each group of rows, by
    group label (one per row), over every sample's real and imaginary parts
    (`casa` and `ours`: rows of complex samples; autocorrelations are
    compared by their real parts, which the caller passes alone). Each ratio
    must be 1 within five of its standard errors, sqrt(2/n_casa + 2/n_ours)
    for normal noise."""
    results, failures = [], []
    for label in np.unique(groups):
        a, b = casa[groups == label].ravel(), ours[groups == label].ravel()
        if np.iscomplexobj(a):
            a = np.concatenate([a.real, a.imag])
            b = np.concatenate([b.real, b.imag])
        if a.size < 2:
            continue
        ratio = float(np.mean(b * b) / np.mean(a * a))
        error = float(np.sqrt(2.0 / a.size + 2.0 / b.size))
        results.append({"group": str(label), "n": int(a.size), "ratio": ratio, "standard_error": error})
        if not abs(ratio - 1.0) <= NOISE_STANDARD_ERRORS * error:
            failures.append(
                f"group {label}: casa-rs/CASA variance {ratio:.4f}, expected 1 +- "
                f"{NOISE_STANDARD_ERRORS * error:.4f} over {a.size} samples"
            )
    return results, failures


def tsys_term(scenario: dict) -> dict | None:
    """The resolved arguments of the scenario's one `tsys-manual` setnoise,
    or None when its noise is not that alone."""
    calls = [call for call in scenario.get("calls", []) if call["op"] == "setnoise"]
    terms = [call["resolved"] for call in calls if call.get("returned")]
    if len(terms) == 1 and terms[0].get("mode") == "tsys-manual":
        return terms[0]
    return None


def row_geometry(tb, me, qa, path: str) -> dict:
    """What a row's tsys-manual σ takes, from the MS at `path`: each row's
    window frequency and channel bandwidth, EXPOSURE, its antennas' dishes,
    and their elevations of the row's field at its TIME (UTC, as CASA's
    VisIter takes TIME), and the rows' window, antennas and time."""
    tb.open(path)
    try:
        a1, a2 = np.asarray(tb.getcol("ANTENNA1")), np.asarray(tb.getcol("ANTENNA2"))
        time = np.asarray(tb.getcol("TIME"))
        exposure = np.asarray(tb.getcol("EXPOSURE"))
        ddid = np.asarray(tb.getcol("DATA_DESC_ID"))
        field = np.asarray(tb.getcol("FIELD_ID"))
    finally:
        tb.close()

    def column(subtable: str, name: str) -> np.ndarray:
        tb.open(os.path.join(path, subtable))
        try:
            return np.asarray(tb.getcol(name))
        finally:
            tb.close()

    spw = column("DATA_DESCRIPTION", "SPECTRAL_WINDOW_ID")[ddid]
    reference = column("SPECTRAL_WINDOW", "REF_FREQUENCY")
    bandwidth = np.abs(column("SPECTRAL_WINDOW", "TOTAL_BANDWIDTH")) / column("SPECTRAL_WINDOW", "NUM_CHAN")
    positions = column("ANTENNA", "POSITION")
    diameters = column("ANTENNA", "DISH_DIAMETER")
    phase = column("FIELD", "PHASE_DIR")[:, 0, :]
    elevation = np.zeros((time.size, 2))
    for t, f in sorted(set(zip(time.tolist(), field.tolist()))):
        me.doframe(me.epoch("UTC", qa.quantity(t, "s")))
        direction = me.direction("J2000", qa.quantity(phase[0, f], "rad"), qa.quantity(phase[1, f], "rad"))
        antennas = []
        for antenna in range(positions.shape[1]):
            me.doframe(me.position("ITRF", *[qa.quantity(positions[k, antenna], "m") for k in range(3)]))
            antennas.append(me.measure(direction, "AZEL")["m1"]["value"])
        rows = (time == t) & (field == f)
        elevation[rows, 0] = np.asarray(antennas)[a1[rows]]
        elevation[rows, 1] = np.asarray(antennas)[a2[rows]]
    return {
        "frequency": reference[spw],
        "bandwidth": bandwidth[spw],
        "exposure": exposure,
        "diameters": np.stack([diameters[a1], diameters[a2]], axis=1),
        "elevations": elevation,
        "spw": spw,
        "time": time,
    }


def time_bins(time: np.ndarray, bins: int = NOISE_TIME_BINS) -> np.ndarray:
    """Each row's bin of `bins` equal spans of the rows' times."""
    span = max(float(time.max() - time.min()), 1e-9)
    return np.minimum(((time - time.min()) / span * bins).astype(int), bins - 1)


def compare_tsys_noise(paths: dict, term: dict) -> tuple[dict, list[str]]:
    """A tsys-manual term's noise in both implementations: normalized by
    each row's σ (`tsys_manual_sigma`), and CASA's against casa-rs's in
    groups of rows (`grouped_variance_ratios`)."""
    from casatools import measures, quanta, table

    tb, me, qa = table(), measures(), quanta()
    geometry = row_geometry(tb, me, qa, paths["casa"])
    sigma = tsys_manual_sigma(
        term,
        geometry["frequency"],
        geometry["bandwidth"],
        geometry["exposure"],
        geometry["diameters"],
        geometry["elevations"],
    )
    noise, stats, failures = {}, {}, []
    for side in ("casa", "casa-rs"):
        noisy, clean = main_columns(tb, paths[side]), main_columns(tb, paths[side + "-twin"])
        if noisy["data"].shape != clean["data"].shape or noisy["data"].shape[0] != sigma.size:
            failures.append(f"{side}: the twin's or CASA's DATA has another shape")
            return stats, failures
        noise[side] = (noisy["data"] - clean["data"], noisy["flag_row"], noisy["auto"])
    unflagged = ~noise["casa"][1] & ~noise["casa-rs"][1]
    auto = noise["casa"][2]
    stats["rows_compared"] = int(unflagged.sum())
    stats["rows_flagged"] = int((~unflagged).sum())
    stats["sigma_range_jy"] = [float(sigma[unflagged].min()), float(sigma[unflagged].max())]
    elevation = np.degrees(geometry["elevations"][unflagged])
    stats["elevation_range_deg"] = [float(elevation.min()), float(elevation.max())]
    normalized = {}
    for side in ("casa", "casa-rs"):
        values = noise[side][0][unflagged] / sigma[unflagged][:, None, None]
        classes, side_failures = noise_classes(values, auto[unflagged], 1.0)
        stats[side] = classes
        failures += [f"{side} normalized noise: {failure}" for failure in side_failures]
        cross = values[~auto[unflagged]].ravel()
        normalized[side] = np.concatenate([cross.real, cross.imag])
    if all(np.all(np.isfinite(values)) for values in normalized.values()):
        statistic, p_value = ks_two_sample(normalized["casa"], normalized["casa-rs"])
        stats["ks_cross"] = {"statistic": statistic, "p_value": p_value}
        failures += ks_failures(statistic, p_value)
    pairs = np.sort(geometry["diameters"], axis=1)
    labels = np.array(
        [
            f"spw{spw} {d1:g}m-{d2:g}m {'auto' if is_auto else 'cross'} t{bin_}"
            for spw, (d1, d2), is_auto, bin_ in zip(
                geometry["spw"], pairs, auto, time_bins(geometry["time"])
            )
        ]
    )
    casa, ours = noise["casa"][0][unflagged], noise["casa-rs"][0][unflagged]
    cross_rows, auto_rows = ~auto[unflagged], auto[unflagged]
    groups, group_failures = grouped_variance_ratios(
        casa[cross_rows], ours[cross_rows], labels[unflagged][cross_rows]
    )
    if auto_rows.any():
        auto_groups, auto_failures = grouped_variance_ratios(
            casa[auto_rows].real, ours[auto_rows].real, labels[unflagged][auto_rows]
        )
        groups += auto_groups
        group_failures += auto_failures
    stats["groups"] = groups
    pulls = [(group["ratio"] - 1.0) / group["standard_error"] for group in groups]
    stats["groups_worst_pull"] = float(max(np.abs(pulls))) if pulls else None
    stats["groups_chi2_per_group"] = float(np.mean(np.square(pulls))) if pulls else None
    failures += [f"noise: {failure}" for failure in group_failures]
    return stats, failures


def ks_failures(statistic: float, p_value: float) -> list[str]:
    """The two-sample KS test's failure: a p-value below 1e-3, or one that is
    not a number."""
    if p_value >= 1e-3:
        return []
    return [f"noise: two-sample KS p-value {p_value:.2g} (statistic {statistic:.4f})"]


def compare_corrupted_data(paths: dict, scenario: dict) -> tuple[dict, list[str]]:
    """Gain or leakage statistics of both implementations against their
    twins (`corruption_stats.py`)."""
    from casatools import table

    tb = table()
    kind = scenario["data"]
    stats, failures = {}, []
    for side in ("casa", "casa-rs"):
        corrupted = main_columns(tb, paths[side])
        twin = main_columns(tb, paths[side + "-twin"])
        if corrupted["data"].shape != twin["data"].shape:
            failures.append(f"{side}: the twin's DATA has another shape")
            continue
        side_failures = non_finite("DATA", corrupted["data"]) + non_finite("twin DATA", twin["data"])
        if not side_failures and kind == "gains":
            # CASA's defects: its unit-gain antennas, reported but not
            # checked, and the checks its gains fail (corruption_stats.py).
            unit = None if side == "casa" else []
            checks = scenario.get("casa_gain_checks", GAIN_CHECKS) if side == "casa" else GAIN_CHECKS
            stats[side], side_failures = gain_statistics(corrupted, twin, scenario["gain_scale"], checks, unit)
        elif not side_failures:
            stats[side], side_failures = leakage_statistics(corrupted, twin, scenario["leakage_amplitude"])
        failures += [f"{side} {kind}: {failure}" for failure in side_failures]
    if kind == "gains" and "casa" in stats and "casa-rs" in stats:
        stats["structure"], structure = structure_failures(stats["casa"], stats["casa-rs"])
        failures += structure
    return stats, failures


def compare_data(tb, scenario: dict, paths: dict, limit: float) -> tuple[dict, list[str]]:
    """DATA by the scenario's kind: zero, model, noisy, gains or leakage."""
    kind = scenario.get("data") or "zero"
    if kind == "zero":
        failures = []
        for side in ("casa", "casa-rs"):
            data = main_columns(tb, paths[side])["data"]
            if np.any(data != 0):
                failures.append(f"MAIN.DATA: {side} has nonzero data")
        return {"kind": kind}, failures
    if kind == "model":
        stats, failures = compare_model_data(
            main_columns(tb, paths["casa"]), main_columns(tb, paths["casa-rs"]), limit
        )
        return dict(stats, kind=kind), failures
    if kind == "noisy":
        term = tsys_term(scenario)
        if term is not None and scenario.get("noise_variance") is None:
            stats, failures = compare_tsys_noise(paths, term)
            return dict(stats, kind=kind, noise="tsys-manual"), failures
        stats, failures = compare_noisy_data(paths, scenario.get("noise_variance"))
        return dict(stats, kind=kind), failures
    if kind in ("gains", "leakage"):
        stats, failures = compare_corrupted_data(paths, scenario)
        return dict(stats, kind=kind), failures
    return {"kind": kind}, [f"unknown data kind {kind!r}"]


def compare_scenario(
    scenario: dict, casa_ms: str, casars_ms: str, args: argparse.Namespace, twins: dict | None = None
) -> dict:
    from casatools import table

    name = scenario["name"]
    ignore = set(scenario.get("ignore", []))
    uvw_tol = args.uvw_tolerance_m
    tb = table()
    report = {"name": name, "differences": [], "known": [], "largest": {}}

    def differ(key: str, message: str) -> None:
        if key in KNOWN:
            report["known"].append(f"{message} [known: {KNOWN[key]}]")
        else:
            report["differences"].append(message)

    tables = {"MAIN": ("", "")}
    casa_sub = subtables(tb, casa_ms)
    casars_sub = subtables(tb, casars_ms)
    for missing in sorted(set(casa_sub) - set(casars_sub)):
        differ(f"{missing}:missing", f"subtable {missing} missing from casa-rs")
    for extra in sorted(set(casars_sub) - set(casa_sub)):
        differ(f"{extra}:extra", f"subtable {extra} only in casa-rs")
    for sub in sorted(set(casa_sub) & set(casars_sub)):
        tables[sub] = (sub, sub)
    for label, (casa_rel, casars_rel) in tables.items():
        casa_path = os.path.join(casa_ms, casa_rel) if casa_rel else casa_ms
        casars_path = os.path.join(casars_ms, casars_rel) if casars_rel else casars_ms
        a = describe(tb, casa_path)
        b = describe(tb, casars_path)
        if a["rows"] != b["rows"]:
            differ(f"{label}:rows", f"{label}: {a['rows']} rows in CASA, {b['rows']} in casa-rs")
        for key in ("type", "subType", "readme"):
            if a["info"].get(key) != b["info"].get(key):
                differ(f"{label}:info", f"{label}: TableInfo {key} {a['info'].get(key)!r} against {b['info'].get(key)!r}")
        if normalise_keywords(a["keywords"]) != normalise_keywords(b["keywords"]):
            differ(f"{label}:keywords", f"{label}: keywords {normalise_keywords(a['keywords'])} against {normalise_keywords(b['keywords'])}")
        for column in sorted(set(a["columns"]) - set(b["columns"])):
            differ(f"{label}.{column}:missing", f"{label}.{column}: missing from casa-rs")
        for column in sorted(set(b["columns"]) - set(a["columns"])):
            differ(f"{label}.{column}:extra", f"{label}.{column}: only in casa-rs")
        for column in sorted(set(a["columns"]) & set(b["columns"])):
            qualified = f"{label}.{column}"
            if qualified in ignore:
                continue
            da, db = a["columns"][column], b["columns"][column]
            for key in ("valueType", "ndim", "shape"):
                if da[key] != db[key]:
                    differ(f"{qualified}:{key}", f"{qualified}: descriptor {key} {da[key]!r} against {db[key]!r}")
            if (da["option"] & 4) != (db["option"] & 4):
                differ(f"{qualified}:option", f"{qualified}: fixed-shape option {da['option']} against {db['option']}")
            ka, kb = normalise_keywords(da["keywords"]), normalise_keywords(db["keywords"])
            if ka != kb:
                differ(f"{qualified}:keywords", f"{qualified}: column keywords {ka} against {kb}")
            if a["rows"] != b["rows"]:
                continue
            values_a = cells(tb, casa_path, column, a["rows"])
            values_b = cells(tb, casars_path, column, b["rows"])
            if label == "MAIN" and column == "DATA":
                kind = scenario.get("data") or "zero"
                if kind == "zero":
                    # Definedness, shapes, values, and zero in both (#848 review).
                    for key, message in data_differences(values_a, values_b):
                        differ(key, message)
                    report["data"] = {"kind": kind}
                    continue
                for key, message in data_cell_differences(values_a, values_b):
                    differ(key, message)
                paths = {"casa": casa_ms, "casa-rs": casars_ms, **(twins or {})}
                report["data"], failures = compare_data(tb, scenario, paths, args.data_nrms)
                for failure in failures:
                    differ("MAIN.DATA:values", failure)
                continue
            tolerance = uvw_tol if column == "UVW" else TOLERANCES.get(column, DEFAULT_TOLERANCE)
            aspect, largest, note = compare_values(values_a, values_b, tolerance)
            if largest is not None:
                report["largest"][qualified] = largest
            if aspect is not None:
                differ(f"{qualified}:{aspect}", f"{qualified}: {note}")
    for name in scenario.get("tables", []):
        rows = {}
        for side, ms in (("casa", casa_ms), ("casa-rs", casars_ms)):
            path = os.path.join(os.path.dirname(ms), name)
            if not os.path.isdir(path):
                differ(f"{name}:missing", f"calibration table {name}: missing from {side}")
                break
            rows[side] = calibration_table(tb, path)
        else:
            for message in table_differences(rows["casa"], rows["casa-rs"]):
                differ(f"{name}:table", f"calibration table {name}: {message}")
    return report


# The calibration-table columns compared exactly: what a simulated table's
# rows are, whatever the draws (`write_simulated_jones_table`).
TABLE_ID_COLUMNS = (
    "FIELD_ID",
    "SPECTRAL_WINDOW_ID",
    "ANTENNA1",
    "ANTENNA2",
    "INTERVAL",
    "SCAN_NUMBER",
    "OBSERVATION_ID",
)


def calibration_table(tb, path: str) -> dict:
    """A calibration table's keywords, columns, TIME and identifying
    columns, and its CPARAM and FLAG cell shapes."""
    tb.open(path)
    try:
        keywords = {
            key: value
            for key, value in tb.getkeywords().items()
            if key in ("VisCal", "ParType", "PolBasis")
        }
        rows = tb.nrows()
        found = {
            "keywords": keywords,
            "subtype": tb.info().get("subType"),
            "columns": sorted(tb.colnames()),
            "TIME": np.asarray(tb.getcol("TIME")) if rows else np.zeros(0),
            "shapes": [tb.getcell("CPARAM", row).shape for row in range(rows)],
            "flags": [bool(np.any(tb.getcell("FLAG", row))) for row in range(rows)],
        }
        for column in TABLE_ID_COLUMNS:
            found[column] = np.asarray(tb.getcol(column)) if rows else np.zeros(0)
        return found
    finally:
        tb.close()


def table_differences(casa: dict, casars: dict) -> list[str]:
    """How casa-rs's calibration table differs from CASA's in what is not
    drawn: its keywords and subtype, columns, rows, TIME (within
    `TOLERANCES['TIME']`), identifying columns, cell shapes and flags."""
    found = []
    for key in ("subtype", "keywords", "columns"):
        if casa[key] != casars[key]:
            found.append(f"{key} {casa[key]!r} against {casars[key]!r}")
    if len(casa["TIME"]) != len(casars["TIME"]):
        return found + [f"{len(casa['TIME'])} rows against {len(casars['TIME'])}"]
    if casa["TIME"].size and not np.all(np.abs(casa["TIME"] - casars["TIME"]) <= TOLERANCES["TIME"]):
        found.append(f"TIME differs by up to {np.max(np.abs(casa['TIME'] - casars['TIME'])):.3g} s")
    for column in TABLE_ID_COLUMNS:
        if not np.array_equal(casa[column], casars[column]):
            row = int(np.argmax(casa[column] != casars[column]))
            found.append(f"{column} row {row}: {casa[column][row]} against {casars[column][row]}")
    for key in ("shapes", "flags"):
        if casa[key] != casars[key]:
            found.append(f"{key} differ")
    return found


def describe_data(data: dict) -> str:
    """A one-line summary of a scenario's DATA comparison."""
    kind = data.get("kind")
    if kind == "model" and data.get("nrms") is not None:
        # A total-power MS has autocorrelations only.
        parts = [
            f"{name} {value:.2e}"
            for name, value in (("cross", data.get("cross_nrms")), ("auto", data.get("auto_nrms")))
            if value is not None
        ]
        return (
            f"; DATA NRMS {data['nrms']:.2e} over {data['samples_compared']} samples"
            f" ({', '.join(parts)})"
        )
    if kind == "noisy" and data.get("noise") == "tsys-manual" and "casa" in data and "casa-rs" in data:
        normalized = lambda side: ", ".join(  # noqa: E731
            f"{entry['cross_re_var']:.4f}/{entry['cross_im_var']:.4f}"
            + (f"/{entry['auto_re_var']:.4f}" if "auto_re_var" in entry else "")
            for key, entry in data[side].items()
            if key.startswith("corr")
        )
        ks = data.get("ks_cross", {})
        return (
            f"; tsys-manual noise over sigma {data['sigma_range_jy'][0]:.4g}-"
            f"{data['sigma_range_jy'][1]:.4g} Jy, normalized cross re/im[/auto] variance "
            f"CASA [{normalized('casa')}], casa-rs [{normalized('casa-rs')}]"
            + (f", KS p {ks['p_value']:.2g}" if ks else "")
            + f"; {len(data['groups'])} groups, worst pull {data['groups_worst_pull']:.2f},"
            f" chi2/group {data['groups_chi2_per_group']:.2f}"
        )
    if kind == "noisy" and "casa" in data and "casa-rs" in data:
        variances = lambda side: ", ".join(  # noqa: E731
            f"{entry['cross_re_var']:.5f}/{entry['cross_im_var']:.5f}"
            for key, entry in data[side].items()
            if key.startswith("corr")
        )
        ks = data.get("ks_cross", {})
        return (
            f"; noise cross re/im variance CASA [{variances('casa')}], casa-rs [{variances('casa-rs')}]"
            + (f", KS p {ks['p_value']:.2g}" if ks else "")
        )
    if kind == "gains" and "casa" in data and "casa-rs" in data:
        scales = lambda side: ", ".join(  # noqa: E731
            f"{entry['amplitude_scale']:.3f}/{entry['phase_scale']:.3f}"
            for key, entry in data[side].items()
            if key.startswith("corr")
        )
        structure = data.get("structure", {})
        lags = lambda side: ", ".join(f"{value:.3f}" for value in structure.get(side, []))  # noqa: E731
        return (
            f"; gain amplitude/phase scale CASA [{scales('casa')}], casa-rs [{scales('casa-rs')}]"
            f"; screen autocorrelation at lags {structure.get('lags')} CASA [{lags('casa')}], casa-rs [{lags('casa-rs')}]"
            f"; CASA's unit-gain antennas {data['casa'].get('unit_antennas')}"
        )
    if kind == "leakage" and "casa" in data and "casa-rs" in data:
        squares = lambda side: ", ".join(  # noqa: E731
            f"{data[side][key]['square_scale']:.3f}" for key in ("XY_re", "XY_im", "YX_re", "YX_im")
        )
        return f"; leakage XY/YX re/im mean-square scale CASA [{squares('casa')}], casa-rs [{squares('casa-rs')}]"
    return ""


def main() -> int:
    args = parse_args()
    with open(os.path.join(args.casa, "manifest.json"), encoding="utf-8") as handle:
        manifest = json.load(handle)
    reports = []
    for scenario in manifest["scenarios"]:
        name = scenario["name"]
        if args.only and name not in args.only:
            continue
        casars_ms = os.path.join(args.casars, name + ".ms")
        if not os.path.isdir(casars_ms):
            reports.append({"name": name, "differences": ["casa-rs MS missing"], "known": [], "largest": {}})
            continue
        twins = None
        if scenario.get("twin"):
            twin = next(other for other in manifest["scenarios"] if other["name"] == scenario["twin"])
            twins = {"casa-twin": twin["ms"], "casa-rs-twin": os.path.join(args.casars, twin["name"] + ".ms")}
        reports.append(compare_scenario(scenario, scenario["ms"], casars_ms, args, twins))
    failed = 0
    for report in reports:
        uvw = report["largest"].get("MAIN.UVW")
        status = "OK" if not report["differences"] else f"{len(report['differences'])} differences"
        known = f", {len(report['known'])} known" if report["known"] else ""
        print(
            f"{report['name']}: {status}{known}"
            + (f" (UVW within {uvw:.3g} m)" if uvw is not None else "")
            + describe_data(report.get("data", {}))
        )
        for line in report["differences"][:40]:
            print("   ", line)
        if len(report["differences"]) > 40:
            print(f"    ... {len(report['differences']) - 40} more")
        if args.verbose:
            for line in report["known"]:
                print("    known:", line)
        failed += bool(report["differences"])
    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(reports, handle, indent=2, default=str)
            handle.write("\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
