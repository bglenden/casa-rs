#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check channel-averaged clipping and exact propagation to stored flags."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from signal import SIGABRT
from subprocess import run
from sys import executable
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from flagdata_clip_columns_oracle import make_inputs
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_summary_oracle import snapshot
from ms_selection_tasks_oracle import add_second_window, casa_table

COMMON = {"mode": "clip", "correlation": "XX", "channelavg": True,
          "clipoutside": False, "flagbackup": False}
TASKS = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
UNCHANGED = ("DATA", "FLOAT_DATA", "CORRECTED_DATA", "MODEL_DATA", "WEIGHT", "SIGMA",
             "WEIGHT_SPECTRUM", "SIGMA_SPECTRUM", "DATA_DESC_ID", "FLAG_ROW")


def make_cases():
    make_inputs()
    with table("weighted.ms", readonly=False, ack=False) as ms:
        ms.addcols(maketabdesc(makearrcoldesc("SIGMA_SPECTRUM", 0.0, ndim=2, valuetype="float")))
        for column, values in (("WEIGHT_SPECTRUM", [3, 1, 2, 4]), ("SIGMA_SPECTRUM", [1, 0.5, 2, 1])):
            values = np.array(values, dtype="f4")[None, :, None]
            ms.putcol(column, np.broadcast_to(values, ms.getcol("DATA").shape).copy())
    copytree("weighted.ms", "two.ms")
    add_second_window(Path("two.ms"))
    copytree("two.ms", "unused.ms")
    with table("unused.ms", readonly=False, ack=False) as ms:
        ms.putcol("DATA_DESC_ID", np.zeros(ms.nrows(), dtype="i4"))
    copytree("weighted.ms", "preflagged.ms")
    with table("preflagged.ms", readonly=False, ack=False) as ms:
        flags = ms.getcol("FLAG")
        flags[:, 0, 0] = True
        ms.putcol("FLAG", flags)
    copytree("weighted.ms", "float.ms")
    with table("float.ms", readonly=False, ack=False) as ms:
        ms.addcols(maketabdesc(makearrcoldesc("FLOAT_DATA", 0.0, ndim=2, valuetype="float")))
        ms.putcol("FLOAT_DATA", ms.getcol("DATA").real.copy())
        ms.removecols("DATA")


def check_flags(source, output, masks, planes=(0,)):
    with table(source, ack=False) as before, table(output, ack=False) as after, casa_table(output) as casa:
        assert before.nrows() == after.nrows() == casa.nrows()
        expected = before.getcol("FLAG")
        ddids = before.getcol("DATA_DESC_ID")
        for ddid, channels in masks.items():
            expected[np.ix_(ddids == ddid, channels, planes)] = True
        np.testing.assert_array_equal(after.getcol("FLAG"), expected)
        for row in range(after.nrows()):
            np.testing.assert_array_equal(casa.getcell("FLAG", row).T, expected[row])
            assert casa.getcell("FLAG_ROW", row) == before.getcell("FLAG_ROW", row)
        for name in UNCHANGED:
            if name in before.colnames():
                np.testing.assert_array_equal(after.getcol(name), before.getcol(name), err_msg=name)


def compare_products():
    # The first pair of observed samples [1, 3] has inverse variance [1, 4]
    # and corrected [2, 1] has weights [3, 1]. The second model pair [1, 3]
    # has arithmetic mean 2. Subtract it after reducing each visibility column.
    cases = (
        ("data", {0: (0, 1)}, (1.9, 2.1), {"chanbin": 2}),
        ("data", {0: (0, 1)}, (1.9, 2.1), {"chanbin": (2,)}),
        ("data", {0: (0, 1)}, (1.9, 2.1), {"chanbin": np.array([2])}),
        ("data", {0: (3,)}, (3.9, 4.1), {"chanbin": 3}),
        ("data", {0: (0,)}, (0.9, 1.1), {"chanbin": 0}),
        ("data", {0: (0,)}, (0.9, 1.1), {"chanbin": 2, "channelavg": False}),
        ("data", {0: (0,)}, (0.9, 1.1), {"chanbin": 2, "channelavg": np.bool_(True)}),
        ("data", {0: (0, 1, 2, 3)}, (3.7, 3.8), {}),
        ("data", {0: (0, 1, 2, 3)}, (3.7, 3.8), {"chanbin": []}),
        ("data", {0: (0, 1, 2, 3)}, (3.7, 3.8), {"chanbin": -1}),
        ("data", {0: (0, 1, 2, 3)}, (3.7, 3.8), {"chanbin": 7}),
        ("data", {0: (0, 1, 2, 3)}, (3.7, 3.8), {"chanbin": np.int64(2)}),
        ("data", {0: (0, 3)}, (2.45, 2.55), {"chanbin": 2, "spw": "0:0;3"}),
        ("data", {0: (0, 2)}, (3.95, 4.05), {"chanbin": 2, "spw": "0:0;2"}),
        ("weighted", {0: (0, 1)}, (2.59, 2.61), {"chanbin": 2}),
        ("weighted", {0: (0, 1)}, (1.74, 1.76), {"chanbin": 2, "datacolumn": "CORRECTED"}),
        ("weighted", {0: (0, 1, 2, 3)}, (1.99, 2.01), {"chanbin": 2, "datacolumn": "MODEL"}),
        ("weighted", {0: (2, 3)}, (2.99, 3.01), {"chanbin": 2, "datacolumn": "RESIDUAL"}),
        ("weighted", {0: (2, 3)}, (2.59, 2.61), {"chanbin": 2, "datacolumn": "RESIDUAL_DATA"}),
        ("weighted", {0: (0, 1)}, (3.99, 4.01), {"chanbin": 2, "datacolumn": "WEIGHT"}),
        ("weighted", {0: (0, 1)}, (3.99, 4.01), {"chanbin": 2, "datacolumn": "WEIGHT_SPECTRUM"}),
        ("data", {0: (0, 1, 2, 3)}, (1.99, 2.01), {"chanbin": 2, "datacolumn": "WEIGHT"}),
        ("float", {0: (0, 1)}, (2.59, 2.61),
         {"chanbin": 2, "datacolumn": "FLOAT_DATA", "correlation": "REAL_XX"}),
        ("preflagged", {0: (1,)}, (2.99, 3.01), {"chanbin": 2}),
        ("two", {0: (0, 1)}, (2.59, 2.61), {"chanbin": [2, 0]}),
        ("two", {1: (0, 1)}, (2.59, 2.61), {"chanbin": [0, 2]}),
        ("two", {0: (0, 1), 1: (0, 1)}, (2.59, 2.61), {"chanbin": [2, 2]}),
        ("two", {0: (0, 1)}, (2.59, 2.61), {"chanbin": [2, 0], "spw": "0"}),
        ("two", {1: (0, 1)}, (2.59, 2.61), {"chanbin": [0, 2], "field": "1"}),
        ("unused", {0: (0, 1)}, (2.59, 2.61), {"chanbin": [2, 0]}),
    )
    for index, (source, masks, bounds, options) in enumerate(cases):
        source += ".ms"
        arguments = {**COMMON, "clipminmax": list(bounds), **options}
        for name, task in TASKS:
            vis = f"{name}-{index}.ms"
            copytree(source, vis)
            assert task(vis=vis, **arguments) == {}, (name, arguments)
            try:
                check_flags(source, vis, masks)
            except AssertionError as error:
                raise AssertionError((name, arguments)) from error
    return len(cases)


def compare_average_of_complex_visibilities():
    source = "cancel.ms"
    copytree("data.ms", source)
    with table(source, readonly=False, ack=False) as ms:
        values = ms.getcol("DATA")
        values[:, :2, 0] = [1 + 2j, -1 - 2j]
        ms.putcol("DATA", values)
    for name, task in TASKS:
        vis = f"cancel-{name}.ms"
        copytree(source, vis)
        assert task(vis=vis, **{**COMMON, "correlation": "ABS_XX"},
                    chanbin=2, clipzeros=True) == {}
        check_flags(source, vis, {0: (0, 1)})


def compare_average_normalization():
    for label, value in (("weak", 1e-25), ("negative", -1.0), ("zero", 0.0)):
        source = f"statistical-weight-{label}.ms"
        copytree("weighted.ms", source)
        with table(source, readonly=False, ack=False) as ms:
            weights = ms.getcol("WEIGHT_SPECTRUM")
            weights[:] = value
            ms.putcol("WEIGHT_SPECTRUM", weights)
        for name, task in TASKS:
            vis = f"statistical-weight-{label}-{name}.ms"
            copytree(source, vis)
            assert task(vis=vis, **COMMON, datacolumn="CORRECTED", chanbin=2, clipzeros=True) == {}
            # All corrected means are strictly positive for nonzero constant
            # weights. Pinned CASA loses the weak positive result to underflow.
            clips = label == "zero" or (name == "casa" and label == "weak")
            check_flags(source, vis, {0: (0, 1, 2, 3)} if clips else {})


def check_width_boundaries():
    source = "zero-width.ms"
    copytree("weighted.ms", source)
    with table(source, readonly=False, ack=False) as ms:
        sigma = ms.getcol("SIGMA_SPECTRUM")
        sigma[:, 0, 0] = 0
        ms.putcol("SIGMA_SPECTRUM", sigma)
    for name, task in TASKS:
        for enabled in (False, True):
            vis = f"zero-width-{name}-{enabled}.ms"
            copytree(source, vis)
            assert task(vis=vis, **{**COMMON, "channelavg": enabled}, chanbin=0, clipzeros=True) == {}
            check_flags(source, vis, {0: (0,)} if name == "casa" and enabled else {})

    two_windows = "two-unweighted.ms"
    copytree("data.ms", two_windows)
    add_second_window(Path(two_windows))
    # The first two observed samples average to 2.0 and a whole window to
    # 3.75. The large width must never wrap into an unintended smaller bin.
    for index, (source, width) in enumerate((("data.ms", 2**32 + 2), (two_windows, [2**32 + 2, 0]))):
        for name, task in TASKS:
            vis = f"wide-{index}-{name}.ms"
            copytree(source, vis)
            assert task(vis=vis, **COMMON, chanbin=width, clipminmax=[1.9, 2.1]) == {}
            check_flags(source, vis, {0: (0, 1)} if name == "casa" else {})


def compare_clip_cli():
    # Verify the ordinary CLI's text parsing as well as the Python runner's
    # typed argument. The reference has different selected bins per window.
    binary = tasks.run("flagdata", vis="two.ms", action="calculate").command[0]
    for index, (width, masks) in enumerate((('2', {0: (0, 1), 1: (0, 1)}), ('[2,0]', {0: (0, 1)}))):
        vis = f"cli-{index}.ms"
        copytree("two.ms", vis)
        run([binary, "--vis", vis, "--mode", "clip", "--correlation", "XX",
             "--channelavg", "--chanbin", width, "--clipminmax", "2.59,2.61",
             "--no-clipoutside", "--no-flagbackup"], check=True, capture_output=True, text=True)
        check_flags("two.ms", vis, masks)


def compare_refusals():
    cases = (
        ({"chanbin": [2, 3]}, RuntimeError, "Error parsing ChannelAverageTVI configuration"),
        ({"chanbin": np.array(2)}, RuntimeError, "Error parsing ChannelAverageTVI configuration"),
        ({"channelavg": [True]}, RuntimeError, "RecordInterface::asBool - invalid data type"),
        ({"channelavg": 1}, AssertionError, "{'channelavg': ['must be of cBool type', 'must be of cBoolVec type']}"),
        ({"chanbin": [2.0]}, AssertionError, "{'chanbin': ['must be of cInt type', 'must be of cIntVec type']}"),
        ({"chanbin": None}, AssertionError, "{'chanbin': ['null value not allowed', 'null value not allowed', 'null value not allowed']}"),
    )
    for index, (options, kind, message) in enumerate(cases):
        for name, task in TASKS:
            vis = f"refusal-{name}-{index}.ms"
            copytree("data.ms", vis)
            before, auxiliary = snapshot(vis), auxiliary_content(vis)
            try:
                task(vis=vis, **{**COMMON, **options})
            except kind as error:
                assert type(error) is kind and str(error) == message, (name, options, type(error), str(error))
            else:
                raise AssertionError((name, options))
            assert_preserved(vis, before, auxiliary)
    return len(cases)


def compare_preview_and_restore():
    arguments = {**COMMON, "chanbin": 2, "clipminmax": [2.59, 2.61]}
    source = "weighted.ms"
    before, auxiliary = snapshot(source), auxiliary_content(source)
    assert casatasks.flagdata(vis=source, action="calculate", **arguments) == {}
    run = tasks.run("flagdata", vis=source, action="calculate", **arguments)
    assert run.value == {}
    report = loads(run.stdout)["result"]
    assert (report["changed_samples"], report["changed_rows"], report["backup_version"]) == (2 * len(before), len(before), None)
    assert_preserved(source, before, auxiliary)
    for name, task in TASKS:
        vis = f"backup-{name}.ms"
        copytree(source, vis)
        assert task(vis=vis, **{**arguments, "flagbackup": True}) == {}
        check_flags(source, vis, {0: (0, 1)})
        other = tasks.flagdata if name == "casa" else casatasks.flagdata
        assert other(vis=vis, **arguments) == {}
        check_flags(source, vis, {0: (0, 1)})
        assert casatasks.flagmanager(vis=vis, mode="restore", versionname="flagdata_1") is None
        check_flags(source, vis, {})


def check_strided_clip():
    options = {**COMMON, "chanbin": 2, "clipminmax": [3.95, 4.05]}
    reference, native = "stride-reference.ms", "stride-native.ms"
    for vis in (reference, native):
        copytree("data.ms", vis)
    # The pinned iterator aborts for the stride. Keep the parent oracle alive
    # and verify [0, 2] by both an independent mean and a CASA explicit list.
    task = f"import casatasks; casatasks.flagdata(**{dict(options, vis=reference, spw='0:0~3^2')!r})"
    command = "import resource; resource.setrlimit(resource.RLIMIT_CORE, (0, 0)); " + task
    result = run([executable, "-c", command], capture_output=True, text=True)
    assert result.returncode == -SIGABRT and "ArraySlicerError" in result.stderr, result.stderr
    assert casatasks.flagdata(vis=reference, spw="0:0;2", **options) == {}
    assert tasks.flagdata(vis=native, spw="0:0~3^2", **options) == {}
    for vis in (reference, native):
        check_flags("data.ms", vis, {0: (0, 2)})


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-clip-channelavg-") as work, chdir(work):
        make_cases()
        products = compare_products()
        refusals = compare_refusals()
        compare_preview_and_restore()
        check_strided_clip()
        compare_average_of_complex_visibilities()
        compare_average_normalization()
        check_width_boundaries()
        compare_clip_cli()
    print(f"{products} flag products, {refusals} refusals, a preview and two backups verified with CASA, "
          "plus complex cancellation, weight limits, width boundaries, and two CLI products")


if __name__ == "__main__":
    main()
