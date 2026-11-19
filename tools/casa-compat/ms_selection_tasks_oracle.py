#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare selected MeasurementSets from the public split and mstransform APIs.

Run with CASA 6.7.6.14, python-casacore and the editable casars package in
the same Python environment. Set CASARS_MSTRANSFORM_BIN to the built task.
"""

from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table as core_table
from casatools import table
import numpy as np

from casars import tasks
from record_casa_returns import make_cal_ms


MAIN = ("TIME", "SCAN_NUMBER", "ANTENNA1", "ANTENNA2", "FIELD_ID", "DATA_DESC_ID",
        "DATA", "FLAG", "FLAG_ROW", "WEIGHT", "SIGMA", "UVW")
SUBTABLES = {
    "ANTENNA": ("NAME", "POSITION"),
    "DATA_DESCRIPTION": ("SPECTRAL_WINDOW_ID", "POLARIZATION_ID"),
    "FEED": ("ANTENNA_ID", "SPECTRAL_WINDOW_ID"),
    "FIELD": ("NAME", "PHASE_DIR", "SOURCE_ID"),
    "POINTING": ("ANTENNA_ID", "TIME"),
    "POLARIZATION": ("CORR_TYPE",),
    "SOURCE": ("SOURCE_ID", "SPECTRAL_WINDOW_ID", "NAME"),
    "SPECTRAL_WINDOW": ("NAME", "NUM_CHAN", "CHAN_FREQ", "CHAN_WIDTH", "RESOLUTION",
                        "EFFECTIVE_BW", "REF_FREQUENCY", "TOTAL_BANDWIDTH"),
}


@contextmanager
def casa_table(path, *, writable=False):
    tool = table()
    tool.open(str(path), nomodify=not writable)
    try:
        yield tool
    finally:
        tool.done()


def columns_from_casacore(path, names):
    with core_table(str(path), ack=False) as tool:
        return {name: tool.getcol(name) for name in names}


def compare_columns(expected, actual, names, case):
    want = columns_from_casacore(expected, names)
    got = columns_from_casacore(actual, names)
    for name, values in want.items():
        np.testing.assert_array_equal(got[name], values, err_msg=f"{case} {name}")
    # Reopen the native output in CASA as well as python-casacore.
    with casa_table(actual) as tool:
        for name, values in got.items():
            np.testing.assert_array_equal(tool.getcol(name).T, values, err_msg=f"CASA readback {case} {name}")


def calendar(seconds):
    epoch = datetime(1858, 11, 17) + timedelta(seconds=seconds)
    return epoch.strftime("%Y/%m/%d/%H:%M:%S.%f")[:-3]


def make_input(path):
    make_cal_ms(str(path))
    with casa_table(path, writable=True) as tool:
        shape = tool.getcol("DATA").shape
        # Distinguish every row, channel and correlation in the copy.
        values = np.arange(np.prod(shape)).reshape(shape, order="F")
        tool.putcol("DATA", (values + 1j * (values + 1)).astype(np.complex64))
        flags = values % 7 == 0
        flags[:, :, -1] = True
        tool.putcol("FLAG", flags)
        tool.putcol("FLAG_ROW", flags.all(axis=(0, 1)))
        sigma = np.exp2(-(values[:, 0, :] % 3)).astype(np.float32)
        tool.putcol("SIGMA", sigma)
        tool.putcol("WEIGHT", 1 / sigma**2)
        return np.unique(tool.getcol("TIME"))


def add_second_window(source):
    for name, changes in {
        "SPECTRAL_WINDOW": {"NAME": "SPW1"},
        "DATA_DESCRIPTION": {"SPECTRAL_WINDOW_ID": 1},
    }.items():
        with core_table(str(source / name), readonly=False, ack=False) as tool:
            tool.addrows(1)
            for column in tool.colnames():
                if tool.iscelldefined(column, 0):
                    tool.putcell(column, 1, changes.get(column, tool.getcell(column, 0)))
            if name == "SPECTRAL_WINDOW":
                for column in ("CHAN_FREQ", "REF_FREQUENCY"):
                    tool.putcell(column, 1, tool.getcell(column, 0) + 1e9)
                tool.addcols(maketabdesc([
                    makearrcoldesc("ASSOC_SPW_ID", 0, ndim=1, valuetype="int"),
                    makearrcoldesc("ASSOC_NATURE", "", ndim=1, valuetype="string"),
                ]))
                for row in range(2):
                    tool.putcell("ASSOC_SPW_ID", row, [0, 1])
                    tool.putcell("ASSOC_NATURE", row, ["association-0", "association-1"])
    for name, column, predicate in (
        ("", "DATA_DESC_ID", "FIELD_ID"),
        ("SOURCE", "SPECTRAL_WINDOW_ID", "SOURCE_ID"),
    ):
        with core_table(str(source / name), readonly=False, ack=False) as tool:
            tool.putcol(column, tool.getcol(predicate))


def succeeds(root, source, name, case, options, subtables=SUBTABLES):
    paths = [root / f"{name}-{case}-{side}.ms" for side in ("casa", "native")]
    for module, path in zip((casatasks, tasks), paths):
        result = getattr(module, name)(vis=str(source), outputvis=str(path), datacolumn="data", **options)
        assert result is None, (name, case, result)
    compare_columns(*paths, MAIN, f"{name} {case} MAIN")
    for subtable, names in subtables.items():
        compare_columns(*(path / subtable for path in paths), names, f"{name} {case} {subtable}")


def empty_selection(root, source, name, time):
    for module in (casatasks, tasks):
        path = root / f"{name}-empty-{module.__name__}.ms"
        try:
            getattr(module, name)(vis=str(source), outputvis=str(path), datacolumn="data",
                                  field="FLUX", timerange=f">{calendar(time)}")
        except RuntimeError as error:
            assert "MSSelectionNullSelection" in str(error), (name, str(error))
        else:
            raise AssertionError(f"{module.__name__}.{name} accepted an empty selection")
        assert not path.exists(), path


def main():
    assert casatasks.version() == [6, 7, 6, 14], casatasks.version()
    with TemporaryDirectory(prefix="ms-task-selection-") as scratch:
        root = Path(scratch)
        source = root / "input.ms"
        times = make_input(source)
        before = columns_from_casacore(source, MAIN)
        time = f"{calendar(times[2] - 1)}~{calendar(times[3] + 1)}"
        cases = {
            "field": {"field": "PH*"},
            "antenna": {"antenna": "A2&A4"},
            "time": {"timerange": time},
            "scan": {"scan": ">1"},
            "stride": {"spw": "SPW0:1~5^2"},
            "segments": {"spw": "SPW0:0~1;3~6"},
            "repeated-window": {"spw": "0:5~7,0:0~1"},
            "clipped-channel": {"spw": "0:5~90"},
            "combined": {"field": "FL*", "antenna": "A2&A4", "timerange": time, "spw": "SPW0:0~3"},
        }
        for name in ("split", "mstransform"):
            for case, options in cases.items():
                succeeds(root, source, name, case, options)
            empty_selection(root, source, name, times[-1] + 1)
        for column, values in columns_from_casacore(source, MAIN).items():
            np.testing.assert_array_equal(values, before[column], err_msg=f"input {column}")
        add_second_window(source)
        subtables = SUBTABLES | {
            "SPECTRAL_WINDOW": (*SUBTABLES["SPECTRAL_WINDOW"], "ASSOC_SPW_ID", "ASSOC_NATURE"),
        }
        for name in ("split", "mstransform"):
            succeeds(root, source, name, "unused-window", {"field": "PHASE"}, subtables)
            succeeds(root, source, name, "compact-second-window", {"spw": "SPW1:2~6"}, subtables)
        print(f"{(len(cases) + 2) * 2} selected products and 2 rejected calls matched CASA 6.7.6.14")


if __name__ == "__main__":
    main()
