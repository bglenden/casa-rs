#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public scan-edge flagging with CASA 6.7.6.14."""

from contextlib import chdir
from json import loads
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content, changed_samples
from flagdata_summary_oracle import snapshot
from record_casa_returns import make_cal_ms

EDGE_FLAGS = {
    "beg": [True, True, False, False, False, False],
    "endb": [False, False, False, False, True, True],
    "end": [True, True, True, True, False, False],
    "tail": [False, False, True, True, True, True],
}


def make_fixtures():
    make_cal_ms("source.ms", channels=4, scan_seconds=60)
    with table("source.ms", readonly=False, ack=False) as ms:
        ms.putcol("FLAG", np.zeros_like(ms.getcol("FLAG")))
        ms.putcol("FLAG_ROW", np.zeros_like(ms.getcol("FLAG_ROW")))
    for seed in ("whole", "selected", "row", "all-flagged", "repeated-scan"):
        path = f"{seed}.ms"
        copytree("source.ms", path)
        with table(path, readonly=False, ack=False) as ms:
            scans, times = ms.getcol("SCAN_NUMBER"), ms.getcol("TIME")
            edge_rows = np.zeros(ms.nrows(), dtype=bool)
            for scan in np.unique(scans):
                scan_times = times[scans == scan]
                edge_rows |= (scans == scan) & ((times == scan_times.min()) | (times == scan_times.max()))
            if seed == "row":
                ms.putcol("FLAG_ROW", edge_rows)
            elif seed == "all-flagged":
                ms.putcol("FLAG", np.ones_like(ms.getcol("FLAG")))
            elif seed == "repeated-scan":
                with table(f"{path}/OBSERVATION", readonly=False, ack=False) as observation:
                    observation.addrows(1)
                    for column in observation.colnames():
                        if observation.iscelldefined(column, 0):
                            observation.putcell(column, 1, observation.getcell(column, 0))
                ms.putcol("OBSERVATION_ID", (scans != scans.min()).astype(np.int32))
                ms.putcol("SCAN_NUMBER", np.full_like(scans, scans.min()))
            else:
                flags = ms.getcol("FLAG")
                flags[edge_rows, :4 if seed == "whole" else 2, :] = True
                ms.putcol("FLAG", flags)


def assert_equal(reference, native):
    for expected, actual in zip(snapshot(reference), snapshot(native), strict=True):
        for column, wanted, got in zip(("FLAG_ROW", "FLAG", "DATA"), expected, actual, strict=True):
            np.testing.assert_array_equal(got, wanted, err_msg=column)


def verify_edges(vis, mode):
    with table(vis, ack=False) as ms:
        scans, times = ms.getcol("SCAN_NUMBER"), ms.getcol("TIME")
        expected = np.zeros(ms.nrows(), dtype=bool)
        for scan in np.unique(scans):
            rows = scans == scan
            ordered_times = np.unique(times[rows])
            assert len(ordered_times) == 6
            flagged_times = ordered_times[np.asarray(EDGE_FLAGS[mode])]
            expected[rows] = np.isin(times[rows], flagged_times)
        np.testing.assert_array_equal(ms.getcol("FLAG_ROW"), expected)
        np.testing.assert_array_equal(ms.getcol("FLAG"), np.broadcast_to(expected[:, None, None], ms.getcol("FLAG").shape))


def compare_apply():
    cases = [
        *[("source", {"quackmode": mode}) for mode in EDGE_FLAGS],
        *[("whole", {"quackmode": mode, "quackincrement": True}) for mode in EDGE_FLAGS],
        ("whole", {"quackincrement": False}),
        ("whole", {"quackincrement": np.bool_(True)}),
        ("whole", {"spw": "0", "quackincrement": True}),
        ("whole", {"spw": "0:0~3", "quackincrement": True}),
        ("all-flagged", {"quackincrement": True}),
        ("all-flagged", {"quackincrement": False}),
        ("selected", {"spw": "0:0~1", "quackincrement": True}),
        ("selected", {"spw": "0:0~1", "quackmode": "endb", "quackincrement": True}),
        ("row", {"quackincrement": True}),
        ("row", {"quackmode": "endb", "quackincrement": True}),
        ("row", {"spw": "0:0~1", "quackincrement": True}),
        ("repeated-scan", {"quackincrement": True}),
        ("source", {"scan": "2", "quackmode": "end"}),
        ("source", {"field": "0", "spw": "0:1~2", "quackmode": "tail"}),
        *[("source", {"quackinterval": width, "quackmode": "tail"}) for width in (0, -1, np.nan)],
        *[("source", {"quackinterval": width}) for width in (np.inf, -np.inf, True, np.float32(10), np.int64(10))],
        *[("whole", {"quackincrement": value}) for value in ([True], [], np.array(True))],
        *[("source", {"quackmode": value}) for value in ("BEG", "", ["beg"], np.array("beg"), np.str_("beg"))],
        *[("source", {"quackinterval": value}) for value in ([10], [], np.array(10))],
    ]
    for index, (seed, custom) in enumerate(cases):
        reference, native = f"reference-{index}.ms", f"native-{index}.ms"
        args = {"mode": "quack", "quackinterval": 10, "flagbackup": False, **custom}
        for vis, task in ((reference, casatasks.flagdata), (native, tasks.flagdata)):
            copytree(f"{seed}.ms", vis)
            assert task(vis=vis, **args) == {}, args
        try:
            assert_equal(reference, native)
        except AssertionError as error:
            raise AssertionError((seed, custom)) from error
        if index < len(EDGE_FLAGS):
            verify_edges(native, custom["quackmode"])
    return len(cases)


def compare_repeated_and_preview():
    reference, native = "increment-reference.ms", "increment-native.ms"
    for vis in (reference, native):
        copytree("source.ms", vis)
    args = {"mode": "quack", "quackinterval": 10, "quackincrement": True}
    for _ in range(3):
        original, auxiliary = snapshot(native), auxiliary_content(native)
        assert tasks.flagdata(vis=native, action="calculate", flagbackup=True, **args) == {}
        completed = tasks.run("flagdata", vis=native, action="calculate", flagbackup=True, **args)
        assert completed.value == {}
        assert_preserved(native, original, auxiliary)

        assert casatasks.flagdata(vis=reference, flagbackup=False, **args) == {}
        expected = changed_samples(original, snapshot(reference))
        assert sum(expected) > 0
        report = loads(completed.stdout)["result"]
        assert report["changed_samples"] == sum(expected)
        assert report["changed_rows"] == sum(delta != 0 for delta in expected)
        assert report["backup_version"] is None
        assert tasks.flagdata(vis=native, flagbackup=False, **args) == {}
        assert_equal(reference, native)


def compare_invalid():
    cases = (
        ("quackmode", None), ("quackmode", 1), ("quackmode", [1]),
        ("quackinterval", None), ("quackinterval", "10"), ("quackinterval", np.array(10.0)),
        ("quackinterval", np.bool_(True)), ("quackinterval", [1, "2"]),
        ("quackincrement", None), ("quackincrement", 1), ("quackincrement", [1]),
        ("quackmode", np.array("beg", dtype=object)),
        ("quackinterval", np.array(10, dtype=object)),
        ("quackinterval", np.array(10.0, dtype=object)),
        ("quackincrement", np.array(True, dtype=object)),
        ("clipoutside", np.array(True, dtype=object)),
    )
    original, auxiliary = snapshot("source.ms"), auxiliary_content("source.ms")
    for parameter, value in cases:
        errors = []
        for task in (casatasks.flagdata, tasks.flagdata):
            try:
                task(vis="source.ms", mode="quack", **{parameter: value})
            except AssertionError as error:
                errors.append(str(error))
            else:
                raise AssertionError(f"{parameter} accepted {value!r}")
        assert errors[0] == errors[1], (parameter, value, errors)
        assert_preserved("source.ms", original, auxiliary)
    return len(cases)


def compare_provider_options():
    reference, native = "provider-reference.ms", "provider-native.ms"
    for vis in (reference, native):
        copytree("source.ms", vis)
    args = {"mode": "quack", "quackinterval": True, "flagbackup": False}
    assert casatasks.flagdata(vis=reference, **args) == {}
    assert tasks.run("flagdata", vis=native, **args).value == {}
    assert_equal(reference, native)

    before, auxiliary = snapshot("source.ms"), auxiliary_content("source.ms")
    for parameter, value in (("quackinterval", "typo"), ("quackmode", 42), ("quackincrement", 1)):
        try:
            tasks.run("flagdata", vis="source.ms", mode="quack", **{parameter: value})
        except tasks.TaskParameterError as error:
            assert error.completion.parameter == parameter, str(error)
        else:
            raise AssertionError(f"native provider accepted {parameter}={value!r}")
        assert_preserved("source.ms", before, auxiliary)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-quack-") as root, chdir(root):
        make_fixtures()
        count = compare_apply()
        compare_repeated_and_preview()
        rejected = compare_invalid()
        compare_provider_options()
    print(f"{count} scan-edge products, three successive previews and applies, and {rejected} rejections compared with CASA")
    print("Native provider checked against CASA for Boolean intervals and for three pre-write type refusals")


if __name__ == "__main__":
    main()
