#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare clip bounds, data columns and argument handling with CASA."""

from contextlib import chdir
from json import loads
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content, changed_samples
from flagdata_summary_oracle import snapshot
from record_casa_returns import make_cal_ms


FLOAT_SAMPLES = np.array(
    [[-3, -2], [-1, -1e-8], [-0.0, 1.5], [float("inf"), float("nan")]], dtype="f4"
)


def make_input(path, *, float_data=False):
    make_cal_ms(path, channels=4, scan_seconds=20)
    with table(path, readonly=False, ack=False) as storage:
        shape = storage.getcell("DATA", 0).shape
        if float_data:
            storage.addcols(maketabdesc(makearrcoldesc(
                "FLOAT_DATA", np.float32(0), ndim=2, valuetype="float",
            )))
        for row in range(storage.nrows()):
            data = np.ones(shape, dtype="c8")
            if float_data:
                storage.putcell("FLOAT_DATA", row, FLOAT_SAMPLES)
            storage.putcell("DATA", row, data)
        if not float_data:
            samples = np.array(
                [[0, .4+.4j], [1, 2], [3, -2], [complex(float("nan"), 0), float("inf")]],
                dtype="c8",
            )
            storage.putcell("DATA", 0, samples)
            maximum = float(np.finfo("f4").max)
            samples = np.ones(shape, dtype="c8")
            samples[3] = (complex(maximum, maximum), maximum)
            storage.putcell("DATA", 1, samples)


def reset_flags(path):
    with table(path, readonly=False, ack=False) as storage:
        flags = np.zeros_like(storage.getcol("FLAG"))
        flags[5, 0, 1] = True
        storage.putcol("FLAG", flags)
        row_flags = np.zeros_like(storage.getcol("FLAG_ROW"))
        row_flags[2] = True
        storage.putcol("FLAG_ROW", row_flags)


def compare_clip(reference, native, options):
    for path in (reference, native):
        reset_flags(path)
    before_casa, before_native = snapshot(reference), snapshot(native)
    for task, path in ((casatasks.flagdata, reference), (tasks.flagdata, native)):
        assert task(vis=path, mode="clip", flagbackup=False, **options) == {}
        assert not Path(f"{path}.flagversions").exists()
    casa, materialized = snapshot(reference), snapshot(native)
    for old_casa, old_native, wanted, actual in zip(
        before_casa, before_native, casa, materialized, strict=True
    ):
        np.testing.assert_array_equal(wanted[2], old_casa[2])
        np.testing.assert_array_equal(actual[2], old_native[2])
        for expected, got in zip(wanted[:2], actual[:2], strict=True):
            np.testing.assert_array_equal(got, expected, err_msg=repr(options))
    if options.get("datacolumn") == "FLOAT_DATA":
        for path in (reference, native):
            with table(path, ack=False) as storage:
                np.testing.assert_array_equal(
                    storage.getcol("FLOAT_DATA"),
                    np.broadcast_to(FLOAT_SAMPLES, (storage.nrows(), *FLOAT_SAMPLES.shape)),
                )
    return casa


def compare_rejections(path):
    before, auxiliary = snapshot(path), auxiliary_content(path)
    for options in (
        {"clipminmax": None},
        {"clipminmax": True},
        {"clipminmax": ""},
        {"clipminmax": [1, "2"]},
        {"clipminmax": [[1], [2]]},
        {"clipminmax": np.array(1.5)},
        {"clipminmax": np.array([1, 2], dtype="u4")},
        {"clipminmax": np.array([1, 2], dtype="f2")},
        {"clipminmax": np.array([1, 2], dtype="c8")},
        {"clipoutside": 0},
        {"clipoutside": None},
        {"clipoutside": [0]},
        {"clipzeros": [False]},
    ):
        messages = []
        for task in (casatasks.flagdata, tasks.flagdata):
            try:
                task(vis=path, mode="clip", action="calculate", **options)
            except AssertionError as error:
                messages.append(str(error))
            else:
                raise AssertionError(f"clip unexpectedly accepted {options!r}")
        assert messages[0] == messages[1], (options, messages)
    assert_preserved(path, before, auxiliary)


def check_explicit_data(reference, native):
    for path in (reference, native):
        reset_flags(path)
    before_native, auxiliary = snapshot(native), auxiliary_content(native)
    options = {"mode": "clip", "datacolumn": "DATA", "clipminmax": [1, 1], "flagbackup": False}
    assert casatasks.flagdata(vis=reference, **options) == {}
    assert tasks.flagdata(vis=native, **options) == {}
    assert_preserved(native, before_native, auxiliary)
    with table(reference, ack=False) as storage:
        # CASA substitutes abs(FLOAT_DATA) for the explicitly requested DATA.
        np.testing.assert_array_equal(
            storage.getcell("FLAG", 0), abs(FLOAT_SAMPLES) != 1.0
        )


def check_float_zero_detection(reference, native):
    for path in (reference, native):
        reset_flags(path)
    options = {"mode": "clip", "datacolumn": "FLOAT_DATA", "clipzeros": True, "flagbackup": False}
    assert casatasks.flagdata(vis=reference, **options) == {}
    assert tasks.flagdata(vis=native, **options) == {}
    limit = np.finfo("f4").eps
    for path, zero in ((reference, FLOAT_SAMPLES <= limit), (native, abs(FLOAT_SAMPLES) <= limit)):
        with table(path, ack=False) as storage:
            np.testing.assert_array_equal(
                storage.getcell("FLAG", 0), zero | ~np.isfinite(FLOAT_SAMPLES)
            )


def check_disabled_agent_backup(reference, native):
    for task, path in ((casatasks.flagdata, reference), (tasks.flagdata, native)):
        reset_flags(path)
        before = snapshot(path)
        assert task(
            vis=path, mode="clip", clipoutside=[False], clipminmax=[1, 2], flagbackup=True
        ) == {}
        for wanted, actual in zip(before, snapshot(path), strict=True):
            for expected, got in zip(wanted, actual, strict=True):
                np.testing.assert_array_equal(got, expected)
        directory = Path(f"{path}.flagversions")
        assert [item.name for item in directory.glob("flags.*")] == ["flags.flagdata_1"]
        with table(str(directory / "flags.flagdata_1"), ack=False) as backup:
            for row, (flag_row, flags, _) in enumerate(before):
                np.testing.assert_array_equal(backup.getcell("FLAG_ROW", row), flag_row)
                np.testing.assert_array_equal(backup.getcell("FLAG", row), flags)


def main():
    assert tuple(casatasks.version()) == (6, 7, 6, 14)
    with TemporaryDirectory(prefix="flagdata-clip-range-") as directory, chdir(directory):
        reference, native = "reference.ms", "native.ms"
        for path in (reference, native):
            make_input(path)
        for options in (
            {},
            {"clipminmax": [1, 2]},
            {"clipminmax": [1, 2], "clipoutside": False},
            {"clipminmax": [1, 2], "clipoutside": False, "clipzeros": True},
            {"clipminmax": [1, 2], "clipoutside": np.bool_(False)},
            {"clipminmax": [1, 2], "clipoutside": [False]},
            {"clipminmax": [1, 2], "clipoutside": [True]},
            {"clipminmax": [1, 2], "clipoutside": [False, True]},
            {"clipminmax": [1, 2], "clipoutside": []},
            {"clipminmax": [1, 2], "clipoutside": (False,)},
            {"clipminmax": [1, 2], "clipoutside": np.array([False])},
            {"clipminmax": [1, 2], "clipoutside": np.array(False)},
            {"clipminmax": [2, 1]},
            {"clipminmax": [2, 1], "clipoutside": False},
            {"clipminmax": [1]},
            {"clipminmax": [1, 2, 3]},
            {"clipminmax": 1.5},
            {"clipminmax": (1, 2)},
            {"clipminmax": np.array([1, 2])},
            {"clipminmax": np.array([1, 2], dtype="f4")},
            {"clipminmax": [True, np.int64(2)]},
            {"clipminmax": range(1, 3)},
            {"clipminmax": b"12"},
            {"clipminmax": [float("nan"), 2]},
            {"clipminmax": [float("nan"), 2], "clipoutside": False},
            {"clipminmax": [-float("inf"), float("inf")]},
            {"clipminmax": [1 + 2**-26, 2]},
            {"clipminmax": [1 + 2**-21, 2]},
            {"clipminmax": [0, 1 - 2**-26]},
            {"clipminmax": [1, 2], "spw": "0:1~2"},
        ):
            compare_clip(reference, native, options)
        compare_rejections(native)

        reset_flags(reference)
        reset_flags(native)
        before, auxiliary = snapshot(native), auxiliary_content(native)
        options = {"mode": "clip", "clipminmax": [1, 2], "flagbackup": False}
        casatasks.flagdata(vis=reference, **options)
        expected_samples = sum(changed_samples(before, snapshot(reference)))
        result = tasks.run("flagdata", vis=native, action="calculate", **options)
        assert result.value == {}
        assert loads(result.stdout)["result"]["changed_samples"] == expected_samples
        assert_preserved(native, before, auxiliary)

        float_casa, float_native = "float-casa.ms", "float-native.ms"
        for path in (float_casa, float_native):
            make_input(path, float_data=True)
        for settings in (
            {"clipminmax": [1, 2]},
            {"clipminmax": [-2, -1], "clipoutside": False},
            {"clipzeros": np.bool_(True)},
        ):
            compare_clip(float_casa, float_native, {"datacolumn": "FLOAT_DATA", **settings})
        check_explicit_data(float_casa, float_native)
        check_float_zero_detection(float_casa, float_native)
        check_disabled_agent_backup(reference, native)
    print("Thirty-five clip comparisons, thirteen rejections and two documented corrections verified")


if __name__ == "__main__":
    main()
