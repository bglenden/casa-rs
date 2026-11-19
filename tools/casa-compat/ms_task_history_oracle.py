#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare split and mstransform provenance with pinned CASA."""

from pathlib import Path
from tempfile import TemporaryDirectory
from time import time

import casatasks
from casacore.tables import makearrcoldesc, makescacoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from ms_selection_tasks_oracle import casa_table, make_input

MJD_UNIX_EPOCH_S = 40_587 * 86_400
HISTORY = {
    "APP_PARAMS": ["origin_input"],
    "CLI_COMMAND": ["simobserve"],
    "APPLICATION": "test-simulator",
    "MESSAGE": "Original input history",
    "OBJECT_ID": 2,
    "OBSERVATION_ID": 0,
    "ORIGIN": "history-fixture",
    "PRIORITY": "NORMAL",
    "TIME": 5_000_000_000.0,
    "EXTRA": 42,
    "EXTRA_FIXED": [2, 3],
    "EXTRA_VARIABLE": [4, 5],
}


def seed_history(source):
    with table(str(source / "HISTORY"), readonly=False, ack=False) as history:
        history.addcols(maketabdesc([
            makescacoldesc("EXTRA", 0),
            makearrcoldesc("EXTRA_FIXED", 0, shape=[2]),
            makearrcoldesc("EXTRA_VARIABLE", 0, ndim=1),
        ]))
        history.addrows(1)
        for name, value in HISTORY.items():
            history.putcell(name, 0, value)


def compare_layout(paths, task):
    with (table(str(paths[0]), ack=False) as reference, table(str(paths[1]), ack=False) as actual,
          casa_table(paths[1]) as reopen):
        assert actual.info() == reference.info(), (task, actual.info(), reference.info())
        assert actual.nrows() == reference.nrows(), task
        names = reference.keywordnames()
        assert set(actual.keywordnames()) == set(reopen.keywordnames()) == set(names), task
        for name in names:
            expected = reference.getkeyword(name)
            got = actual.getkeyword(name)
            if isinstance(expected, str) and expected.startswith("Table: "):
                target = Path(got.removeprefix("Table: "))
                assert target == paths[1] / Path(expected.removeprefix("Table: ")).name, (task, name, got)
                with casa_table(target):
                    pass
            else:
                np.testing.assert_array_equal(got, expected, err_msg=f"{task} keyword {name}")


def compare_history(inputs, outputs, task, wall_time):
    paths = [output / "HISTORY" for output in outputs]
    with (table(str(paths[0]), ack=False) as reference, table(str(paths[1]), ack=False) as actual,
          casa_table(paths[1]) as reopen):
        assert set(reference.colnames()) == set(actual.colnames()) == set(HISTORY)
        assert reference.nrows() == actual.nrows(), (task, reference.nrows(), actual.nrows())
        time_columns = [history.getcol("TIME") for history in (reference, actual)]
        for time_values in time_columns:
            assert time_values[0] == HISTORY["TIME"]
            assert np.all(time_values[1:] >= wall_time[0])
            assert np.all(time_values[1:] <= wall_time[1])
            assert np.all(np.diff(time_values) >= 0)
        for row in range(reference.nrows()):
            for name in reference.colnames():
                if name == "EXTRA_VARIABLE" and row > 0:
                    assert not reference.iscelldefined(name, row)
                    assert not actual.iscelldefined(name, row)
                    continue
                expected = reference.getcell(name, row)
                value = actual.getcell(name, row)
                np.testing.assert_array_equal(value, reopen.getcell(name, row), err_msg=f"CASA reopen {name} {row}")
                if row == 0:
                    np.testing.assert_array_equal(value, HISTORY[name])
                elif name == "TIME":
                    continue
                elif name == "EXTRA_FIXED":
                    # CASA leaves new, unwritten fixed arrays uninitialized.
                    assert value.shape == expected.shape
                    np.testing.assert_array_equal(value, [0, 0])
                elif name == "MESSAGE" and row == 2:
                    assert expected.startswith("version: 6.7.6-14"), expected
                    assert value.startswith("version: casa-rs "), value
                elif name == "MESSAGE" and row in (3, 4):
                    key = "vis" if row == 3 else "outputvis"
                    paths_for_row = inputs if row == 3 else outputs
                    assert expected == f'{key:<11} = "{paths_for_row[0]}"', expected
                    assert value == f'{key:<11} = "{paths_for_row[1]}"', value
                else:
                    np.testing.assert_array_equal(value, expected, err_msg=f"{task} {name} {row}")


def main():
    assert tuple(casatasks.version()) == (6, 7, 6, 14)
    with TemporaryDirectory(prefix="ms-task-history-") as work:
        root = Path(work)
        source = root / "input.ms"
        make_input(source, channels=4)
        seed_history(source)
        cases = (
            ("split", {}, {}),
            ("mstransform", {}, {}),
            ("split", {"width": [2], "timebin": "25s", "combine": "scan",
                       "correlation": ["xx", "yy"]}, {"msselect": ""}),
            ("mstransform", {"chanaverage": True, "chanbin": [2], "timeaverage": True,
                            "timebin": "25s", "timespan": "scan", "correlation": "xx,yy",
                            "feed": np.int32(0)}, {}),
            ("split", {"width": "2"}, {}),
        )
        for number, (task, arguments, native_only) in enumerate(cases):
            outputs = [root / f"{task}-{number}-{implementation}.ms"
                       for implementation in ("casa", "native")]
            inputs = [source, source]
            before = time() + MJD_UNIX_EPOCH_S
            for module, vis, output in zip((casatasks, tasks), inputs, outputs):
                kwargs = arguments | (native_only if module is tasks else {})
                result = getattr(module, task)(vis=str(vis), outputvis=str(output), datacolumn="data",
                                               spw="0:0~3", **kwargs)
                assert result is None, (task, module.__name__, result)
                for collision in (output, source):
                    try:
                        getattr(module, task)(vis=str(vis), outputvis=str(collision),
                                              datacolumn="data", spw="0:0~3", **kwargs)
                    except OSError as error:
                        assert str(error) == f"Output MS {collision} already exists - will not overwrite it."
                    else:
                        raise AssertionError((task, module.__name__, collision))
            compare_history(inputs, outputs, task, (before, time() + MJD_UNIX_EPOCH_S))
            compare_layout(outputs, task)
        with table(str(source / "HISTORY"), ack=False) as history:
            assert history.nrows() == 1
            for name, value in HISTORY.items():
                np.testing.assert_array_equal(history.getcell(name, 0), value)
        print("Five HISTORY products and ten repeated-call collisions compared with CASA 6.7.6.14")


if __name__ == "__main__":
    main()
