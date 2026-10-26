# SPDX-License-Identifier: LGPL-3.0-or-later
"""casacore (casatools) read-back of an MSv2 the trial converter wrote.

Usage, with a CASA install's Python (casatools is an oracle here only):
    python check_v2_readback.py ORIGINAL.ms CONVERTED.ms

The converted MSv2 is compared with the one it was converted from (through
MSv4 and back). See docs/measurement-set/trial/README.md, "Converter".

Rows are matched per spectral window (by CHAN_FREQ, over every data
description of it) on (TIME, ANTENNA1, ANTENNA2); every carried column is
compared exactly.
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
]
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
    w = b["WEIGHT"]
    derived = np.where(w > np.finfo(np.float32).tiny, 1.0 / np.sqrt(w), -1.0).astype(np.float32)
    assert np.array_equal(derived, b["SIGMA"]), ddid_o
    good = a["SIGMA"] > 0
    worst_sigma = max(
        worst_sigma,
        float(np.max(np.abs(b["SIGMA"][good] - a["SIGMA"][good]) / a["SIGMA"][good], initial=0.0)),
    )
    compared += len(a["TIME"])
print(f"compared {compared} rows over {len(windows_o)} windows ({len(wo)} and {len(wc)} data descriptions): equal")
print(f"largest relative difference, derived SIGMA vs original SIGMA: {worst_sigma:.3g}")
