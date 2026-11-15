# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare a casa-rs creation (#937 PR B) with CASA's concat of the same
inputs: MAIN keyed by (observation, data description, field, scan, TIME,
ANTENNA1, ANTENNA2), each identity by what it is rather than its ID, with
row multiplicity; data by normalized RMS; and the subtables' row counts.

Usage: python3.12 compare_concat.py OURS THEIRS
"""

import json
import sys
from collections import defaultdict

import numpy as np
from casatools import table

tb = table()


def column(path, name, subtable=None):
    tb.open(path if subtable is None else f"{path}/{subtable}")
    try:
        if name not in tb.colnames():
            return None
        return tb.getcol(name) if tb.nrows() > 0 else []
    finally:
        tb.close()


def rows(path, subtable):
    tb.open(f"{path}/{subtable}")
    try:
        return tb.nrows()
    finally:
        tb.close()


def identities(path):
    names = column(path, "NAME", "ANTENNA")
    stations = column(path, "STATION", "ANTENNA")
    antennas = [f"{n}@{s}" for n, s in zip(names, stations)]
    fields = list(column(path, "NAME", "FIELD"))
    tb.open(f"{path}/OBSERVATION")
    sessions = []
    for row in range(tb.nrows()):
        sessions.append(
            (
                tb.getcell("TELESCOPE_NAME", row),
                tb.getcell("PROJECT", row),
                tb.getcell("OBSERVER", row),
                tuple(np.round(tb.getcell("TIME_RANGE", row), 3)),
            )
        )
    tb.close()
    tb.open(f"{path}/SPECTRAL_WINDOW")
    windows = []
    for row in range(tb.nrows()):
        frequencies = tb.getcell("CHAN_FREQ", row)
        windows.append((len(frequencies), round(float(frequencies[0]), 3)))
    tb.close()
    tb.open(f"{path}/POLARIZATION")
    polarizations = [tuple(tb.getcell("CORR_TYPE", row)) for row in range(tb.nrows())]
    tb.close()
    tb.open(f"{path}/DATA_DESCRIPTION")
    descriptions = [
        (windows[tb.getcell("SPECTRAL_WINDOW_ID", row)], polarizations[tb.getcell("POLARIZATION_ID", row)])
        for row in range(tb.nrows())
    ]
    tb.close()
    return antennas, fields, sessions, descriptions


def main_rows(path):
    antennas, fields, sessions, descriptions = identities(path)
    tb.open(path)
    try:
        columns = {
            name: tb.getcol(name)
            for name in [
                "OBSERVATION_ID",
                "DATA_DESC_ID",
                "FIELD_ID",
                "SCAN_NUMBER",
                "TIME",
                "ANTENNA1",
                "ANTENNA2",
                "WEIGHT",
                "UVW",
            ]
        }
        keyed = defaultdict(list)
        for row in range(tb.nrows()):
            key = (
                sessions[columns["OBSERVATION_ID"][row]],
                descriptions[columns["DATA_DESC_ID"][row]],
                fields[columns["FIELD_ID"][row]],
                int(columns["SCAN_NUMBER"][row]),
                round(float(columns["TIME"][row]), 6),
                antennas[columns["ANTENNA1"][row]],
                antennas[columns["ANTENNA2"][row]],
            )
            keyed[key].append(row)
        return keyed, columns
    finally:
        tb.close()


def cells(path, name, row_list):
    tb.open(path)
    try:
        return [tb.getcell(name, row) for row in row_list]
    finally:
        tb.close()


def subtable_content(ours, theirs, name):
    """Compare every column and cell; report differences, including metadata.

    HISTORY is deliberately task-specific. Source validity changes caused by
    CASA's documented merge bug remain visible in this report.
    """
    def read(path):
        tb.open(f"{path}/{name}")
        try:
            values = {}
            for column_name in tb.colnames():
                values[column_name] = [
                    tb.getcell(column_name, row) if tb.iscelldefined(column_name, row) else None
                    for row in range(tb.nrows())
                ]
            if name == "FEED":
                order = sorted(range(tb.nrows()), key=lambda row: tuple(
                    values[key][row] for key in ["ANTENNA_ID", "FEED_ID", "SPECTRAL_WINDOW_ID", "TIME", "INTERVAL"]))
                values = {column: [cells[row] for row in order] for column, cells in values.items()}
            return values
        finally:
            tb.close()

    a, b = read(ours), read(theirs)
    if name == "OBSERVATION":
        for columns in [a, b]:
            for column_name in ["LOG", "SCHEDULE"]:
                if column_name in columns:
                    columns[column_name] = [np.array([], dtype=str) if value is None or np.asarray(value).size == 0
                        or np.all(np.asarray(value) == "") else value for value in columns[column_name]]
    if name == "SOURCE":
        for columns in [a, b]:
            if "POSITION" in columns and all(value is None or np.asarray(value).size == 0 for value in columns["POSITION"]):
                del columns["POSITION"]
    differences = {}
    for column_name in sorted(set(a) | set(b)):
        if column_name not in a or column_name not in b:
            differences[column_name] = "column absent"
            continue
        left, right = a[column_name], b[column_name]
        if len(left) != len(right):
            differences[column_name] = {"rows": [len(left), len(right)]}
            continue
        bad, difference, power, maximum = 0, 0.0, 0.0, 0.0
        for x, y in zip(left, right):
            if x is None or y is None:
                bad += int((x is None) != (y is None))
                continue
            if isinstance(x, dict) or isinstance(y, dict):
                # Record values are checked recursively, including arrays.
                def same(p, q):
                    if isinstance(p, dict) and isinstance(q, dict):
                        return p.keys() == q.keys() and all(same(p[k], q[k]) for k in p)
                    return np.array_equal(p, q, equal_nan=True) if np.asarray(p).dtype.kind in "fc" else np.array_equal(p, q)
                bad += int(not same(x, y))
                continue
            x, y = np.asarray(x), np.asarray(y)
            if column_name in ["DIRECTION", "DELAY_DIR", "PHASE_DIR", "REFERENCE_DIR"] and x.size and y.size:
                x, y = x.copy(), y.copy()
                x.flat[0] %= 2 * np.pi
                y.flat[0] %= 2 * np.pi
            if x.shape != y.shape:
                bad += 1
            elif x.dtype.kind in "fciub" and y.dtype.kind in "fciub":
                equal_nan = np.isnan(x) & np.isnan(y)
                delta = np.where(equal_nan, 0.0, np.abs(x.astype(complex) - y.astype(complex)))
                bad += int(np.any(~np.isfinite(delta)))
                difference += float(np.nansum(delta ** 2))
                power += float(np.nansum(np.abs(y.astype(complex)) ** 2))
                maximum = max(maximum, float(np.max(delta, initial=0.0)))
            else:
                bad += int(not np.array_equal(x, y))
        if bad or maximum:
            differences[column_name] = {
                "different_or_undefined_cells": bad,
                "max_abs": maximum,
                "nrms": (difference / power) ** 0.5 if power else difference ** 0.5,
            }
    return differences


def compare(ours, theirs):
    our_rows, our_columns = main_rows(ours)
    their_rows, their_columns = main_rows(theirs)
    our_keys, their_keys = set(our_rows), set(their_rows)
    common = sorted(our_keys & their_keys)
    multiplicity = sum(1 for key in common if len(our_rows[key]) != len(their_rows[key]))
    ours_first = [our_rows[key][0] for key in common]
    theirs_first = [their_rows[key][0] for key in common]
    flags = (cells(ours, "FLAG", ours_first), cells(theirs, "FLAG", theirs_first))

    def nrms(name):
        if column(ours, name) is None or column(theirs, name) is None:
            return None
        pair = (cells(ours, name, ours_first), cells(theirs, name, theirs_first))
        difference = sum(float(np.sum(np.abs(a - b) ** 2)) for a, b in zip(*pair))
        power = sum(float(np.sum(np.abs(b) ** 2)) for b in pair[1])
        return (difference / power) ** 0.5 if power > 0 else difference

    data_columns = {name: nrms(name) for name in ["DATA", "CORRECTED_DATA", "MODEL_DATA"]}
    our_sigma = cells(ours, "SIGMA", ours_first)
    their_sigma = cells(theirs, "SIGMA", theirs_first)
    sigma_relative = max(
        [
            float(np.max(np.abs(a - b) / np.maximum(np.abs(b), 1e-30)))
            for a, b in zip(our_sigma, their_sigma)
        ]
        or [0.0]
    )
    weights = np.max(
        [
            np.max(np.abs(our_columns["WEIGHT"][:, r] - their_columns["WEIGHT"][:, s]))
            for r, s in zip(ours_first, theirs_first)
        ]
        or [0.0]
    )
    # Where CASA's output keeps WEIGHT_SPECTRUM, casa-rs writes its (channel-
    # constant) values as WEIGHT: compare those.
    spectral = column(theirs, "WEIGHT_SPECTRUM")
    spectrum_weights = None
    if spectral is not None and len(spectral) > 0:
        their_spectrum = cells(theirs, "WEIGHT_SPECTRUM", theirs_first)
        spectrum_weights = float(
            np.max(
                [
                    np.max(np.abs(our_columns["WEIGHT"][:, r] - spectrum[:, 0]))
                    for r, spectrum in zip(ours_first, their_spectrum)
                ]
                or [0.0]
            )
        )
    uvw = np.max(
        [
            np.max(np.abs(our_columns["UVW"][:, r] - their_columns["UVW"][:, s]))
            for r, s in zip(ours_first, theirs_first)
        ]
        or [0.0]
    )
    subtables = {}
    for name in [
        "ANTENNA",
        "FEED",
        "FIELD",
        "SOURCE",
        "SPECTRAL_WINDOW",
        "POLARIZATION",
        "DATA_DESCRIPTION",
        "STATE",
        "OBSERVATION",
        "PROCESSOR",
        "POINTING",
        "HISTORY",
    ]:
        try:
            subtables[name] = [rows(ours, name), rows(theirs, name)]
        except Exception as error:  # a subtable one output lacks
            subtables[name] = repr(error)
    return {
        "rows": [sum(map(len, our_rows.values())), sum(map(len, their_rows.values()))],
        "keys_only_ours": len(our_keys - their_keys),
        "keys_only_theirs": len(their_keys - our_keys),
        "examples_only_ours": [str(key) for key in sorted(our_keys - their_keys)[:3]],
        "examples_only_theirs": [str(key) for key in sorted(their_keys - our_keys)[:3]],
        "multiplicity_differs": multiplicity,
        "data_nrms": data_columns["DATA"],
        "data_columns_nrms": data_columns,
        "uncertainty_columns_nrms": {name: nrms(name) for name in ["WEIGHT", "SIGMA", "WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"]},
        "sigma_max_relative": sigma_relative,
        "flags_differ": sum(int(np.any(a != b)) for a, b in zip(*flags)),
        "weight_max_abs": float(weights),
        "weight_vs_their_spectrum_max_abs": spectrum_weights,
        "uvw_max_abs_m": float(uvw),
        "subtables_ours_theirs": subtables,
        "subtable_content_differences": {
            name: subtable_content(ours, theirs, name)
            for name, counts in subtables.items()
            if isinstance(counts, list) and name != "HISTORY"
        },
    }


if __name__ == "__main__":
    print(json.dumps(compare(sys.argv[1], sys.argv[2]), indent=2))
