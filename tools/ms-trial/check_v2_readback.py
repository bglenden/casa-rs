# SPDX-License-Identifier: LGPL-3.0-or-later
"""casacore (casatools) read-back of an MSv2 the trial converter wrote.

Usage, with a CASA install's Python (casatools is an oracle here only):
    python check_v2_readback.py ORIGINAL.ms CONVERTED.ms

The converted MSv2 is compared with the one it was converted from (through
MSv4 and back). See docs/measurement-set/trial/README.md, "Converter".

Rows are matched per spectral window (by CHAN_FREQ, over every data
description of it) on (TIME, ANTENNA1, ANTENNA2); every carried column is
compared exactly. So is the subtable content each matched row points at:
its spectral window's channels and reference, its polarization's
CORR_TYPE and CORR_PRODUCT, its field's PHASE_DIR, its antennas' positions
and dish diameters, its session's TELESCOPE_NAME and a TIME_RANGE that
covers it; and every field with a source has a SOURCE row in each of its
spectral windows.
"""
import sys

import numpy as np
from casatools import table

original, converted = sys.argv[1], sys.argv[2]
tb = table()


def subtable(ms, name, columns):
    tb.open(f"{ms}/{name}")
    out = {c: tb.getcol(c) for c in columns}
    tb.close()
    return out


def windows(ms):
    spw = subtable(ms, "SPECTRAL_WINDOW", ["CHAN_FREQ"])
    dd = subtable(ms, "DATA_DESCRIPTION", ["SPECTRAL_WINDOW_ID"])
    return {
        ddid: tuple(spw["CHAN_FREQ"][:, s]) for ddid, s in enumerate(dd["SPECTRAL_WINDOW_ID"])
    }


def stations(ms):
    return list(subtable(ms, "ANTENNA", ["STATION"])["STATION"])


def fields(ms):
    return list(subtable(ms, "FIELD", ["NAME"])["NAME"])


def descriptions(ms):
    """Each data description's spectral window and polarization rows."""
    dd = subtable(ms, "DATA_DESCRIPTION", ["SPECTRAL_WINDOW_ID", "POLARIZATION_ID"])
    return list(zip(dd["SPECTRAL_WINDOW_ID"], dd["POLARIZATION_ID"]))


def column_rows(ms, name, column):
    """Each row's cell of `column` (cells may differ in shape)."""
    tb.open(f"{ms}/{name}")
    out = [tb.getcell(column, r) for r in range(tb.nrows())]
    tb.close()
    return out


def same(a, b, what):
    assert np.shape(a) == np.shape(b) and np.array_equal(a, b), (what, a, b)


def rows(ms, ddids, columns):
    tb.open(ms)
    sel = tb.query(
        f"DATA_DESC_ID IN [{','.join(map(str, ddids))}]", sortlist="TIME,ANTENNA1,ANTENNA2"
    )
    out = {c: sel.getcol(c) for c in columns}
    sel.close()
    tb.close()
    return out


tb.open(original)
n_original = tb.nrows()
tb.close()
tb.open(converted)
n_converted = tb.nrows()
casa_columns = tb.colnames()
tb.close()
print(f"rows: original {n_original}, converted {n_converted}")
print(f"converted columns: {casa_columns}")
assert n_original == n_converted

wo, wc = windows(original), windows(converted)
so, sc = stations(original), stations(converted)
fo, fc = fields(original), fields(converted)
columns = [
    "TIME", "ANTENNA1", "ANTENNA2", "FIELD_ID", "SCAN_NUMBER", "DATA", "FLAG",
    "WEIGHT", "SIGMA", "UVW", "TIME_CENTROID", "INTERVAL", "EXPOSURE", "FLAG_ROW",
    "DATA_DESC_ID", "OBSERVATION_ID",
]
do, dc = descriptions(original), descriptions(converted)
spw_columns = ["CHAN_FREQ", "CHAN_WIDTH", "REF_FREQUENCY", "MEAS_FREQ_REF", "TOTAL_BANDWIDTH"]
spw_o = {c: column_rows(original, "SPECTRAL_WINDOW", c) for c in spw_columns}
spw_c = {c: column_rows(converted, "SPECTRAL_WINDOW", c) for c in spw_columns}
pol_columns = ["CORR_TYPE", "CORR_PRODUCT"]
pol_o = {c: column_rows(original, "POLARIZATION", c) for c in pol_columns}
pol_c = {c: column_rows(converted, "POLARIZATION", c) for c in pol_columns}
dir_o, dir_c = column_rows(original, "FIELD", "PHASE_DIR"), column_rows(converted, "FIELD", "PHASE_DIR")
ant_columns = ["POSITION", "DISH_DIAMETER"]
ant_o = {c: column_rows(original, "ANTENNA", c) for c in ant_columns}
ant_c = {c: column_rows(converted, "ANTENNA", c) for c in ant_columns}
obs_o = subtable(original, "OBSERVATION", ["TELESCOPE_NAME"])["TELESCOPE_NAME"]
obs_c = subtable(converted, "OBSERVATION", ["TELESCOPE_NAME", "TIME_RANGE"])
subtable_checks = 0
compared = 0
worst_sigma = 0.0
windows_o = sorted(set(wo.values()))
assert windows_o == sorted(set(wc.values())), "the same spectral windows"
for freqs in windows_o:
    ddid_o = [d for d, f in wo.items() if f == freqs]
    a = rows(original, ddid_o, columns)
    b = rows(converted, [d for d, f in wc.items() if f == freqs], columns)
    keys = list(zip(a["TIME"], a["ANTENNA1"], a["ANTENNA2"]))
    assert len(set(keys)) == len(keys), f"{ddid_o}: one row per time and baseline"
    assert len(a["TIME"]) == len(b["TIME"]), ddid_o
    assert np.array_equal(a["TIME"], b["TIME"]), ddid_o
    assert [so[i] for i in a["ANTENNA1"]] == [sc[i] for i in b["ANTENNA1"]]
    assert [so[i] for i in a["ANTENNA2"]] == [sc[i] for i in b["ANTENNA2"]]
    # XRADIO's names: NAME_ID.
    assert [f"{fo[i]}_{i}" for i in a["FIELD_ID"]] == [fc[i] for i in b["FIELD_ID"]]
    for c in ["SCAN_NUMBER", "DATA", "FLAG", "WEIGHT", "UVW", "TIME_CENTROID", "INTERVAL", "EXPOSURE"]:
        if c == "FLAG":
            # Row flags fold into the element flags.
            expected = a["FLAG"] | a["FLAG_ROW"][np.newaxis, np.newaxis, :]
            assert np.array_equal(expected, b["FLAG"]), (ddid_o, c)
        else:
            assert np.array_equal(a[c], b[c]), (ddid_o, c)
    assert not b["FLAG_ROW"].any()
    # The subtable rows each matched row points at, once per distinct pair.
    pairs = set(zip(a["DATA_DESC_ID"], b["DATA_DESC_ID"]))
    pairs |= {("field", x, y) for x, y in zip(a["FIELD_ID"], b["FIELD_ID"])}
    pairs |= {("antenna", x, y) for x, y in zip(a["ANTENNA1"], b["ANTENNA1"])}
    pairs |= {("antenna", x, y) for x, y in zip(a["ANTENNA2"], b["ANTENNA2"])}
    pairs |= {("session", x, y) for x, y in zip(a["OBSERVATION_ID"], b["OBSERVATION_ID"])}
    for pair in pairs:
        if pair[0] == "field":
            same(dir_o[pair[1]], dir_c[pair[2]], ("PHASE_DIR", pair))
        elif pair[0] == "antenna":
            for c in ant_columns:
                same(ant_o[c][pair[1]], ant_c[c][pair[2]], (c, pair))
        elif pair[0] == "session":
            assert obs_o[pair[1]] == obs_c["TELESCOPE_NAME"][pair[2]], ("TELESCOPE_NAME", pair)
        else:
            (sw_o, pl_o), (sw_c, pl_c) = do[pair[0]], dc[pair[1]]
            for c in spw_columns:
                same(spw_o[c][sw_o], spw_c[c][sw_c], (c, pair))
            for c in pol_columns:
                same(pol_o[c][pl_o], pol_c[c][pl_c], (c, pair))
        subtable_checks += 1
    for session in np.unique(b["OBSERVATION_ID"]):
        start, end = obs_c["TIME_RANGE"][:, session]
        times = b["TIME"][b["OBSERVATION_ID"] == session]
        half = b["INTERVAL"][b["OBSERVATION_ID"] == session] / 2
        assert start <= np.min(times - half) and np.max(times + half) <= end, ("TIME_RANGE", session)
    w = b["WEIGHT"]
    derived = np.where(w > np.finfo(np.float32).tiny, 1.0 / np.sqrt(w), -1.0).astype(np.float32)
    assert np.array_equal(derived, b["SIGMA"]), ddid_o
    good = a["SIGMA"] > 0
    worst_sigma = max(
        worst_sigma,
        float(np.max(np.abs(b["SIGMA"][good] - a["SIGMA"][good]) / a["SIGMA"][good], initial=0.0)),
    )
    compared += len(a["TIME"])
field_sources = subtable(converted, "FIELD", ["SOURCE_ID"])["SOURCE_ID"]
if any(field_sources >= 0):
    sources = subtable(converted, "SOURCE", ["SOURCE_ID", "SPECTRAL_WINDOW_ID"])
    held = set(zip(sources["SOURCE_ID"], sources["SPECTRAL_WINDOW_ID"]))
    tb.open(converted)
    for ddid, (window, _) in enumerate(dc):
        sel = tb.query(f"DATA_DESC_ID == {ddid}")
        for field in np.unique(sel.getcol("FIELD_ID")) if sel.nrows() else []:
            if field_sources[field] >= 0:
                assert (field_sources[field], window) in held, ("SOURCE row", field, window)
        sel.close()
    tb.close()
print(f"compared {compared} rows over {len(windows_o)} windows ({len(wo)} and {len(wc)} data descriptions): equal")
print(f"compared {subtable_checks} subtable rows the matched rows point at: equal")
print(f"largest relative difference, derived SIGMA vs original SIGMA: {worst_sigma:.3g}")
