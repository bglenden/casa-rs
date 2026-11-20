#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare clip time bins and their propagation to stored MeasurementSet flags."""

from contextlib import chdir
from json import loads
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from flagdata_clip_channelavg_oracle import UNCHANGED
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_summary_oracle import snapshot
from ms_selection_tasks_oracle import casa_table
from record_casa_returns import make_cal_ms

COMMON = {"mode": "clip", "timeavg": True, "timebin": "20s", "correlation": "REAL_XX",
          "clipoutside": False, "flagbackup": False}
TASKS = (("casa", casatasks.flagdata), ("native", tasks.flagdata))


def make_cases():
    make_cal_ms("visibility.ms", channels=4, scan_seconds=40)
    with table("visibility.ms", readonly=False, ack=False) as ms:
        times = ms.getcol("TIME")
        offsets = (times - times.min()).round().astype("i4")
        np.testing.assert_array_equal(np.unique(offsets), np.arange(0, 80, 10))
        indices = offsets // 10
        shape = ms.getcol("DATA").shape
        observed = np.array([1, 3, 7, 9, 11, 13, 17, 19], dtype="f4")[indices]
        for column, values in (("DATA", observed), ("CORRECTED_DATA", observed - 1),
                               ("MODEL_DATA", observed * 0.5 - 1)):
            ms.putcol(column, np.broadcast_to(values[:, None, None], shape).astype("c8"))
        ms.putcol("FLAG", np.zeros(shape, dtype=bool))
        ms.putcol("FLAG_ROW", np.zeros(ms.nrows(), dtype=bool))
        for column, values in (("SIGMA_SPECTRUM", [1, 0.5, 2, 1] * 2),
                               ("WEIGHT_SPECTRUM", [3, 1, 2, 4] * 2)):
            if column not in ms.colnames():
                ms.addcols(maketabdesc(makearrcoldesc(column, 0., ndim=2, valuetype="float")))
            cell = np.array(values, dtype="f4")[indices, None, None]
            ms.putcol(column, np.broadcast_to(cell, shape).copy())
    copytree("visibility.ms", "preflagged.ms")
    with table("preflagged.ms", readonly=False, ack=False) as ms:
        flags = ms.getcol("FLAG")
        flags[0, 0, 0] = True
        ms.putcol("FLAG", flags)
    copytree("visibility.ms", "float.ms")
    with table("float.ms", readonly=False, ack=False) as ms:
        ms.addcols(maketabdesc(makearrcoldesc("FLOAT_DATA", 0., ndim=2, valuetype="float")))
        ms.putcol("FLOAT_DATA", ms.getcol("DATA").real.copy())
        ms.removecols("DATA")


def check_flags(source, vis, times, channels=(0, 1, 2, 3), *, baseline=None, planes=(0,)):
    with table(source, ack=False) as before, table(vis, ack=False) as after, casa_table(vis) as casa:
        timestamps = before.getcol("TIME")
        offsets = (timestamps - timestamps.min()).round().astype("i4")
        expected = before.getcol("FLAG")
        selected = np.isin(offsets, times)
        if baseline is not None:
            selected &= (before.getcol("ANTENNA1") == baseline[0]) & (before.getcol("ANTENNA2") == baseline[1])
        expected[np.ix_(selected, channels, planes)] = True
        np.testing.assert_array_equal(after.getcol("FLAG"), expected)
        for row in range(after.nrows()):
            np.testing.assert_array_equal(casa.getcell("FLAG", row).T, expected[row])
            assert casa.getcell("FLAG_ROW", row) == before.getcell("FLAG_ROW", row)
        for column in (*UNCHANGED, "TIME"):
            if column in before.colnames():
                np.testing.assert_array_equal(after.getcol(column), before.getcol(column), err_msg=column)


def compare_products():
    # Observed [1, 3] with inverse variance [1, 4] averages to 2.6.
    # Corrected [0, 2] with [3, 1] gives 0.5, and that weighted model
    # [-0.5, 0.5] averages to -0.25. The model used alone averages to 0.
    cases = (
        ((0, 10), (2.59, 2.61), {}),
        ((), (2.59, 2.61), {"timebin": "10s"}),
        ((), (2.59, 2.61), {"timebin": "10.01s"}),
        ((0, 10), (2.59, 2.61), {"timebin": "10.02s"}),
        ((0, 10), (2.59, 2.61), {"timebin": "20"}),
        ((0, 10), (2.59, 2.61), {"timebin": "20000ms"}),
        ((0, 10), (2.59, 2.61), {"timebin": "0.333333333333min"}),
        ((0, 10, 20, 30), (3.79, 3.81), {"timebin": "10GHz"}),
        ((), (0, 100), {"timebin": "two integrations"}),
        ((0,), (0.9, 1.1), {"timebin": "0s"}),
        ((0,), (0.9, 1.1), {"timebin": "-1s"}),
        ((0,), (0.9, 1.1), {"timeavg": False}),
        ((0, 10), (0.49, 0.51), {"datacolumn": "CORRECTED"}),
        ((0, 10), (-0.01, 0.01), {"datacolumn": "MODEL"}),
        ((0, 10), (0.74, 0.76), {"datacolumn": "RESIDUAL"}),
        ((0, 10), (2.59, 2.61), {"channelavg": True, "chanbin": 2, "spw": "0:1~3"}),
        ((0, 10), (2.59, 2.61), {"field": "FLUX"}),
        ((), (2.59, 2.61), {"field": "PHASE"}),
        ((0, 10), (2.59, 2.61), {"correlation": ""}),
    )
    for index, (times, bounds, options) in enumerate(cases):
        channels = (1, 2, 3) if "spw" in options else (0, 1, 2, 3)
        planes = (0, 1) if options.get("correlation") == "" else (0,)
        for name, task in TASKS:
            path = f"time-{index}-{name}.ms"
            copytree("visibility.ms", path)
            arguments = {**COMMON, **options, "clipminmax": list(bounds)}
            assert task(vis=path, **arguments) == {}, (name, arguments)
            try:
                check_flags("visibility.ms", path, times, channels, planes=planes)
            except AssertionError as error:
                raise AssertionError((name, arguments)) from error
    return len(cases)


def check_averaged_operands():
    # Compare to independently derived weighted results for columns the
    # pinned clipping iterator substitutes with zero in its time-averaged path.
    cases = (
        ("visibility.ms", "RESIDUAL_DATA", [2.29, 2.31], (0, 10)),
        ("float.ms", "FLOAT_DATA", [2.59, 2.61], (0, 10)),
        ("visibility.ms", "WEIGHT", [3.99, 4.01], (0, 10, 40, 50)),
        ("visibility.ms", "WEIGHT_SPECTRUM", [3.99, 4.01], (0, 10, 40, 50)),
    )
    for index, (source, column, limits, times) in enumerate(cases):
        for name, task in TASKS:
            vis = f"time-operands-{index}-{name}.ms"
            copytree(source, vis)
            assert task(vis=vis, **COMMON, datacolumn=column, clipminmax=limits) == {}
            check_flags(source, vis, times if name == "native" else ())
        vis = f"time-zero-{index}.ms"
        copytree(source, vis)
        assert casatasks.flagdata(vis=vis, **COMMON, datacolumn=column, clipminmax=[0, 0.001]) == {}
        check_flags(source, vis, tuple(range(0, 80, 10)))


def compare_preflags_and_refusals():
    with table("preflagged.ms", ack=False) as source:
        baseline = tuple(source.getcell(name, 0) for name in ("ANTENNA1", "ANTENNA2"))
    for name, task in TASKS:
        path = f"preflags-{name}.ms"
        copytree("preflagged.ms", path)
        assert task(vis=path, **COMMON, clipminmax=[2.99, 3.01]) == {}
        check_flags("preflagged.ms", path, (0, 10), (0,), baseline=baseline)

    cases = (
        ({"timeavg": [True]}, RuntimeError, "RecordInterface::asBool - invalid data type"),
        ({"timeavg": "true"}, AssertionError, "{'timeavg': ['must be of cBool type', 'must be of cBoolVec type']}"),
        ({"timebin": []}, AssertionError, "{'timebin': ['must be of cStr type']}"),
    )
    for index, (kwargs, error_type, message) in enumerate(cases):
        for name, task in TASKS:
            path = f"time-refusal-{index}-{name}.ms"
            copytree("visibility.ms", path)
            original, auxiliary = snapshot(path), auxiliary_content(path)
            try:
                task(vis=path, **{**COMMON, **kwargs})
            except error_type as failure:
                assert type(failure) is error_type and str(failure) == message, (name, failure)
            else:
                raise AssertionError((name, kwargs))
            assert_preserved(path, original, auxiliary)


def compare_preview_and_backups():
    options = {**COMMON, "clipminmax": [2.59, 2.61]}
    source = "visibility.ms"
    before, auxiliary = snapshot(source), auxiliary_content(source)
    assert casatasks.flagdata(vis=source, action="calculate", **options) == {}
    preview = tasks.run("flagdata", vis=source, action="calculate", **options)
    report = loads(preview.stdout)["result"]
    with table(source, ack=False) as ms:
        times = ms.getcol("TIME")
        first_integration, second_integration = np.unique(times)[:2]
        count = np.count_nonzero(np.isin(times, [first_integration, second_integration]))
    assert preview.value == {} and report["changed_samples"] == 4 * count
    assert report["changed_rows"] == count and report["backup_version"] is None
    assert_preserved(source, before, auxiliary)
    for name, task in TASKS:
        path = f"time-backup-{name}.ms"
        copytree(source, path)
        assert task(vis=path, **{**options, "flagbackup": True}) == {}
        check_flags(source, path, (0, 10))
        opposite = tasks.flagdata if name == "casa" else casatasks.flagdata
        assert opposite(vis=path, **options) == {}
        check_flags(source, path, (0, 10))
        assert casatasks.flagmanager(vis=path, mode="restore", versionname="flagdata_1") is None
        check_flags(source, path, ())


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-clip-timeavg-") as work, chdir(work):
        make_cases()
        results = compare_products()
        check_averaged_operands()
        compare_preflags_and_refusals()
        compare_preview_and_backups()
    print(f"{results + 1} time-averaged products, four operand protections, three refusals, a preview "
          "and two backups verified")


if __name__ == "__main__":
    main()
