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
    their_sigma = (
        [s[:, 0] for s in cells(theirs, "SIGMA_SPECTRUM", theirs_first)]
        if column(theirs, "SIGMA_SPECTRUM") is not None and len(column(theirs, "SIGMA_SPECTRUM")) > 0
        else cells(theirs, "SIGMA", theirs_first)
    )
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
        "sigma_max_relative": sigma_relative,
        "flags_differ": sum(int(np.any(a != b)) for a, b in zip(*flags)),
        "weight_max_abs": float(weights),
        "weight_vs_their_spectrum_max_abs": spectrum_weights,
        "uvw_max_abs_m": float(uvw),
        "subtables_ours_theirs": subtables,
    }


if __name__ == "__main__":
    print(json.dumps(compare(sys.argv[1], sys.argv[2]), indent=2))
