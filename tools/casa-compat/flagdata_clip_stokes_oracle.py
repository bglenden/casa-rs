#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare clipping of derived Stokes products through public flagdata."""

from contextlib import chdir
from json import loads
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_summary_oracle import snapshot
from ms_selection_tasks_oracle import casa_table
from record_casa_returns import make_cal_ms

DATA = np.array([[4, 3, 3, 0], [1, 2j, -2j, 1], [0, 3, -3, 4], [2, -2, -2, 2]], dtype="c8")
PREEXISTING = np.zeros(DATA.shape, dtype=bool)
PREEXISTING[1, 0] = PREEXISTING[2, 1] = True
# XX = I + Q, XY = U + iV, YX = U - iV and YY = I - Q.
# RR = I + V, RL = Q + iU, LR = Q - iU and LL = I - V.
MATH = {
    "linear": {
        "I": ((0, 3), (0.5, 0.5)), "Q": ((0, 3), (0.5, -0.5)),
        "U": ((1, 2), (0.5, 0.5)), "V": ((1, 2), (-0.5j, 0.5j)),
        "XX": ((0,), (1,)),
    },
    "circular": {
        "I": ((0, 3), (0.5, 0.5)), "Q": ((1, 2), (0.5, 0.5)),
        "U": ((1, 2), (-0.5j, 0.5j)), "V": ((0, 3), (0.5, -0.5)),
    },
    "wvr": {"I": ((0,), (1,)), "Q": ((1, 3), (0.5, -0.5))},
}
CASA_DEFECTS = {
    ("linear", "U"): ((1, 2), (0.5, -0.5)),
    ("circular", "Q"): ((1, 2), (0.5, -0.5)),
    ("circular", "V"): ((0, 3), (-0.5j, 0.5j)),
}


def make_inputs():
    for basis, labels, frequency in (("linear", "XX XY YX YY", "100GHz"),
                                     ("circular", "RR RL LR LL", "110GHz")):
        path = f"{basis}.ms"
        make_cal_ms(path, channels=4, scan_seconds=20, stokes=labels, frequency=frequency)
        with table(path, readonly=False, ack=False) as ms:
            shape = ms.getcol("DATA").shape
            ms.putcol("DATA", np.broadcast_to(DATA, shape).copy())
            ms.putcol("CORRECTED_DATA", np.broadcast_to(DATA[::-1], shape).copy())
            ms.putcol("FLAG", np.broadcast_to(PREEXISTING, shape).copy())
            ms.putcol("FLAG_ROW", np.zeros(ms.nrows(), dtype=bool))
    casatasks.concat(vis=["linear.ms", "circular.ms"], concatvis="mixed.ms", freqtol="1Hz")
    copytree("linear.ms", "float.ms")
    with table("float.ms", readonly=False, ack=False) as ms:
        ms.addcols(maketabdesc(makearrcoldesc("FLOAT_DATA", 0.0, ndim=2, valuetype="float")))
        ms.putcol("FLOAT_DATA", np.broadcast_to(DATA.real, ms.getcol("DATA").shape).copy())
    copytree("linear.ms", "wvr.ms")
    with table("wvr.ms/POLARIZATION", readonly=False, ack=False) as pol:
        pol.putcell("CORR_TYPE", 0, np.array([1, 9, 10, 12], dtype=np.int32))
        pol.putcell("CORR_PRODUCT", 0, np.array([[0, 0], [0, 0], [0, 1], [1, 1]], dtype=np.int32))


def expected_flags(basis, products, samples, metric, bounds, channels=slice(None), *, casa=False):
    expected = PREEXISTING.copy()
    for product in products:
        planes, coefficients = MATH[basis][product]
        if casa:
            planes, coefficients = CASA_DEFECTS.get((basis, product), (planes, coefficients))
        projected = metric(samples[:, planes] @ coefficients)
        selected = np.zeros(len(samples), dtype=bool)
        selected[channels] = (projected[channels] >= bounds[0]) & (projected[channels] <= bounds[1])
        for plane in planes:
            expected[selected, plane] = True
    return expected


def check_product(path, expected_by_dd, selected_column, selected_data, *, observed=DATA):
    with table(path, ack=False) as ms, casa_table(path) as casa:
        for row, ddid in enumerate(ms.getcol("DATA_DESC_ID")):
            expected = expected_by_dd[ddid]
            np.testing.assert_array_equal(ms.getcell("FLAG", row), expected)
            np.testing.assert_array_equal(casa.getcell("FLAG", row).T, expected)
            np.testing.assert_array_equal(ms.getcell("DATA", row), observed)
            np.testing.assert_array_equal(ms.getcell(selected_column, row), selected_data)
            assert not ms.getcell("FLAG_ROW", row)


def compare_products():
    squared_magnitude = lambda values: np.abs(values)**2
    cases = [
        (basis, f"REAL_{product}", "DATA", np.real, (1, 2) if product == "I" else (-0.5, 0.5),
         {0: (basis, (product,))}, slice(None))
        for basis in ("linear", "circular") for product in "IQUV"
    ]
    # These bounds distinguish the sign and factor of two in the corrected products.
    cases.extend(
        (basis, f"REAL_{product}", "DATA", np.real, (1.9, 3.1), {0: (basis, (product,))}, slice(None))
        for basis, product in CASA_DEFECTS
    )
    cases.extend((
        ("linear", "ABS_I", "CORRECTED", np.abs, (1, 2), {0: ("linear", ("I",))}, slice(1, 3)),
        ("linear", "IMAG_I", "DATA", np.imag, (-0.5, 0.5), {0: ("linear", ("I",))}, slice(None)),
        ("linear", "ARG_I", "DATA", np.angle, (-0.5, 0.5), {0: ("linear", ("I",))}, slice(None)),
        ("linear", "NORM_Q", "DATA", squared_magnitude, (3, 5), {0: ("linear", ("Q",))}, slice(None)),
        ("linear", "ARG_V", "DATA", np.angle, (-2, -1), {0: ("linear", ("V",))}, slice(None)),
        ("linear", "XX,QQ", "DATA", np.abs, (0, 2), {0: ("linear", ("XX", "Q"))}, slice(None)),
        ("float", "ABS_I", "FLOAT_DATA", np.real, (1, 2), {0: ("linear", ("I",))}, slice(None)),
        ("mixed", "REAL_I", "DATA", np.real, (1, 2),
         {0: ("linear", ("I",)), 1: ("circular", ("I",))}, slice(None)),
        ("mixed", "REAL_XX,I", "DATA", np.real, (0, 2),
         {0: ("linear", ("XX", "I")), 1: ("circular", ())}, slice(None)),
        ("wvr", "REAL_WVR,Q", "DATA", np.real, (0, 2), {0: ("wvr", ("I", "Q"))}, slice(None)),
    ))
    columns = {"DATA": "DATA", "CORRECTED": "CORRECTED_DATA", "FLOAT_DATA": "FLOAT_DATA"}
    samples = {"DATA": DATA, "CORRECTED": DATA[::-1], "FLOAT_DATA": DATA.real}
    for index, (source, expression, column, metric, bounds, selection, channels) in enumerate(cases):
        options = dict(mode="clip", correlation=expression, datacolumn=column,
                       clipminmax=list(bounds), clipoutside=False, flagbackup=False)
        if channels.start is not None:
            options["spw"] = f"0:{channels.start}~{channels.stop - 1}"
        expected = {}
        for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
            path = f"result-{index}-{name}.ms"
            copytree(f"{source}.ms", path)
            assert task(vis=path, **options) == {}, options
            expected[name] = {
                ddid: expected_flags(basis, products, samples[column], metric, bounds, channels,
                                     casa=name == "casa")
                for ddid, (basis, products) in selection.items()
            }
            check_product(path, expected[name], columns[column], samples[column])
        if (source, expression[-1]) in CASA_DEFECTS:
            assert any(not np.array_equal(expected["casa"][ddid], expected["native"][ddid])
                       for ddid in selection), options
    return len(cases)


def compare_precision_edges():
    huge = float(np.finfo(np.float32).max) * 0.75
    epsilon = np.finfo(np.float32).eps
    cases = [
        (basis, f"ARG_{product}", {"clipminmax": [-3.2, -3.0], "clipoutside": False},
         (1, 2), (complex(0.0, -2), 2j))
        for basis, product in (("linear", "V"), ("circular", "U"))
    ]
    cases.extend((
        ("linear", "REAL_V", {}, (1, 2), (complex(np.nan, 4), 0)),
        ("linear", "REAL_V", {"clipminmax": [huge * 0.49, huge * 0.51], "clipoutside": False},
         (1, 2), (complex(0, huge), 0)),
        ("linear", "XX,I", {"clipzeros": True}, (0, 3), (2 * complex(epsilon, epsilon * 0.0003), 0)),
    ))
    for index, (basis, expression, options, planes, samples) in enumerate(cases):
        observed = np.tile(np.array([1, 3, 1, 1], dtype="c8"), (4, 1))
        observed[0, list(planes)] = samples
        expected = np.zeros(observed.shape, dtype=bool)
        expected[0, list(planes)] = True
        for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
            path = f"precision-{index}-{name}.ms"
            copytree(f"{basis}.ms", path)
            with table(path, readonly=False, ack=False) as ms:
                shape = ms.getcol("DATA").shape
                ms.putcol("DATA", np.broadcast_to(observed, shape).copy())
                ms.putcol("FLAG", np.zeros(shape, dtype=bool))
            assert task(vis=path, mode="clip", correlation=expression, flagbackup=False,
                        **options) == {}, expression
            check_product(path, {0: expected}, "DATA", observed, observed=observed)
    return len(cases)


def compare_preview_and_repeat():
    vis = "linear.ms"
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    products, bounds = ("XX", "I", "Q"), (0, 3)
    expression = dict(mode="clip", correlation="REAL_XX,I,Q", clipminmax=list(bounds),
                      clipoutside=False)
    expected = expected_flags("linear", products, DATA, np.real, bounds)
    assert np.any(expected & ~PREEXISTING)
    assert casatasks.flagdata(vis=vis, action="calculate", **expression) == {}
    preview = tasks.run("flagdata", vis=vis, action="calculate", **expression)
    assert preview.value == {}
    report = loads(preview.stdout)["result"]
    assert report["changed_rows"] == len(before)
    assert report["changed_samples"] == len(before) * int((expected & ~PREEXISTING).sum())
    assert report["backup_version"] is None
    assert_preserved(vis, before, auxiliary)

    for path, first, second in (("native-first.ms", tasks.flagdata, casatasks.flagdata),
                                ("casa-first.ms", casatasks.flagdata, tasks.flagdata)):
        copytree(vis, path)
        for task in (first, second):
            assert task(vis=path, flagbackup=False, **expression) == {}
            check_product(path, {0: expected}, "DATA", DATA)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-clip-stokes-") as work, chdir(work):
        make_inputs()
        cases = compare_products()
        cases += compare_precision_edges()
        compare_preview_and_repeat()
    print(f"{cases} Stokes clip products, a preview and cross-tool repeat applications passed")


if __name__ == "__main__":
    main()
