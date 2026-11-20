#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Verify named products in routine flagging expressions and their stored flags."""

from contextlib import chdir
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_clip_wvr_oracle import make_inputs
from flagdata_quack_oracle import assert_equal
from flagdata_summary_oracle import snapshot
from record_casa_returns import make_cal_ms


def compare_named_planes():
    make_cal_ms("linear-four.ms", channels=4, scan_seconds=20, stokes="XX XY YX YY")
    before = snapshot("linear-four.ms")
    cases = (("ABS_XX", "XX", (0,)), ("REALXX", "XX", (0,)), ("XY_IMAG", "XY", (1,)),
             ("ABS_XX,ARG_YY", "XX,YY", (0, 3)), ("NORM_UNKNOWN", "UNKNOWN", ()))
    for mode in ("manual", "quack"):
        options = {"mode": mode, "spw": "0:1~2", "flagbackup": False}
        if mode == "quack":
            options["quackinterval"] = 5.0
        for index, (expression, selections, planes) in enumerate(cases):
            paths = [f"{mode}-{index}-{suffix}.ms" for suffix in ("casa-expression", "casa-planes", "native")]
            for path in paths:
                copytree("linear-four.ms", path)
            assert casatasks.flagdata(vis=paths[0], correlation=expression, **options) == {}
            assert casatasks.flagdata(vis=paths[1], correlation=selections, **options) == {}
            assert tasks.flagdata(vis=paths[2], correlation=expression, **options) == {}
            assert_equal(paths[1], paths[2])
            native = snapshot(paths[2])
            decorated = snapshot(paths[0])
            outside_planes = sorted(set(range(4)) - set(planes))
            assert any(flags[:, outside_planes].any() for _, flags, _ in decorated)
            for (row_flag, flags, data), initial in zip(native, before, strict=True):
                assert row_flag == initial[0]
                np.testing.assert_array_equal(flags[[0, 3]], initial[1][[0, 3]])
                np.testing.assert_array_equal(flags[:, outside_planes], initial[1][:, outside_planes])
                np.testing.assert_array_equal(data, initial[2])
            assert bool(planes) == any(flags.any() for _, flags, _ in native)
    return len(cases) * 2


def compare_wvr_and_all():
    make_inputs()
    cases = (("stored-i", "WVR", "WVR"), ("stored-i", "WVR_I", "WVR_I"),
             ("mixed", "WVR,XX", "WVR,XX"), ("mixed", "REAL_WVR", "I"),
             ("mixed", "ABS_ALL", "ABS_ALL"))
    for index, (source, expression, reference) in enumerate(cases):
        casa, native = f"wvr-{index}-casa.ms", f"wvr-{index}-native.ms"
        for path in (casa, native):
            copytree(f"{source}.ms", path)
        assert casatasks.flagdata(vis=casa, correlation=reference, flagbackup=False) == {}
        assert tasks.flagdata(vis=native, correlation=expression, flagbackup=False) == {}
        if expression == "REAL_WVR":
            for (_, casa_flags, casa_data), (_, native_flags, native_data) in zip(
                snapshot(casa), snapshot(native), strict=True
            ):
                np.testing.assert_array_equal(casa_flags, native_flags)
                np.testing.assert_array_equal(casa_data, native_data)
            with table(f"{native}/POLARIZATION", ack=False) as pol:
                labels = [tuple(pol.getcell("CORR_TYPE", row)) for row in range(pol.nrows())]
            with table(f"{native}/DATA_DESCRIPTION", ack=False) as desc:
                descriptions = [labels[index] for index in desc.getcol("POLARIZATION_ID")]
            with table(native, ack=False) as ms:
                for row, ddid in enumerate(ms.getcol("DATA_DESC_ID")):
                    selected = descriptions[ddid] == (1,)
                    assert np.all(ms.getcell("FLAG", row) == selected)
                    assert bool(ms.getcell("FLAG_ROW", row)) == selected
        else:
            assert_equal(casa, native)
    return len(cases)


def compare_fully_selected_rows():
    make_cal_ms("stored-xx.ms", channels=4, scan_seconds=20, stokes="XX")
    cases = (("stored-xx", "ABS_XX", "manual", {}, 40),
             ("stored-xx", "ABS_XX", "unflag", {}, 0),
             ("stored-i", "REAL_WVR", "manual", {}, 40),
             ("stored-i", "REAL_WVR", "quack", {"quackinterval": 5.0}, 20),
             ("stored-q", "ABS_Q", "manual", {}, 40),
             ("stored-q", "ABS_Q", "quack", {"quackinterval": 5.0}, 20))
    for index, (source, correlation, mode, options, row_count) in enumerate(cases):
        paths = [f"full-{index}-{name}.ms" for name in ("casa", "native")]
        for path, task in zip(paths, (casatasks.flagdata, tasks.flagdata), strict=True):
            copytree(f"{source}.ms", path)
            with table(path, readonly=False, ack=False) as ms:
                ms.putcol("FLAG_ROW", np.full(ms.nrows(), mode == "unflag"))
                ms.putcol("FLAG", np.full(ms.getcol("FLAG").shape, mode == "unflag"))
            assert task(vis=path, mode=mode, correlation=correlation, flagbackup=False, **options) == {}
            with table(path, ack=False) as ms:
                assert ms.getcol("FLAG_ROW").sum() == row_count
                assert ms.getcol("FLAG").sum() == 4 * row_count
        assert_equal(*paths)
    return len(cases)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-routine-expression-") as directory, chdir(directory):
        changed = compare_named_planes()
        paired = compare_wvr_and_all() + compare_fully_selected_rows()
    print(f"{changed} scoped routine expressions and {paired} paired products verified against CASA")


if __name__ == "__main__":
    main()
