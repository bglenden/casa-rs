#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the individual HISTORY records written by public flagdata."""

from contextlib import chdir
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory
from time import time

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_summary_oracle import snapshot
from ms_selection_tasks_oracle import casa_table
from ms_task_history_oracle import MJD_UNIX_EPOCH_S, seed_history
from record_casa_returns import make_cal_ms

TASKS = (("casa", casatasks.flagdata), ("native", tasks.flagdata))


def compare_new_rows(paths, original, calls, wall_time, subtable="HISTORY"):
    history_paths = [f"{path}/{subtable}" for path in paths]
    with (table(history_paths[0], ack=False) as casa,
          table(history_paths[1], ack=False) as native,
          casa_table(history_paths[1]) as reopen):
        assert set(casa.colnames()) == set(native.colnames()) == set(reopen.colnames())
        assert casa.nrows() == native.nrows() == reopen.nrows() == original + 78 * calls
        for row in range(casa.nrows()):
            for name in casa.colnames():
                if not casa.iscelldefined(name, row):
                    assert not native.iscelldefined(name, row), (name, row)
                    continue
                expected = casa.getcell(name, row)
                actual = native.getcell(name, row)
                if row >= original and name == "TIME":
                    assert all(wall_time[0] <= value <= wall_time[1] for value in (actual, expected))
                elif row >= original and name == "EXTRA_FIXED":
                    np.testing.assert_array_equal(actual, [0, 0])
                    assert np.shape(expected) == np.shape(actual)
                elif row >= original and name == "MESSAGE":
                    position = (row - original) % 78
                    if position == 1:
                        assert expected.startswith("version: 6.7.6-14")
                        assert actual.startswith("version: casa-rs ")
                    elif position == 2:
                        assert expected == f'vis         = "{paths[0]}"'
                        assert actual == f'vis         = "{paths[1]}"'
                    else:
                        assert actual == expected, (row, actual, expected)
                else:
                    np.testing.assert_array_equal(actual, expected, err_msg=f"{name} {row}")
                np.testing.assert_array_equal(reopen.getcell(name, row), actual,
                                              err_msg=f"CASA reopen {name} {row}")


def compare_applies(source):
    cases = (
        ({}, 2),
        ({"mode": "manualflag", "scan": "1"}, 1),
        ({"mode": "unflag", "spw": "0:1~2"}, 1),
        ({"mode": "clip", "datacolumn": "corrected", "clipminmax": [2, 3]}, 1),
        ({"mode": "quack", "quackmode": "endb", "quackinterval": 4}, 1),
        ({"action": "apply", "writeflags": False}, 1),
        ({"mode": "clip", "channelavg": True, "chanbin": [2]}, 1),
        ({"mode": "clip", "channelavg": True, "chanbin": 2, "flagbackup": True}, 1),
        ({"mode": "clip", "channelavg": True, "chanbin": []}, 1),
        ({"mode": "clip", "channelavg": True, "chanbin": np.int64(2)}, 1),
        ({"spw": " 0:1 ~ 2 "}, 1),
        ({"mode": "rflag"}, 1),
        ({"mode": "rflag", "timedev": 2, "freqdev": 3}, 1),
    )
    with table(f"{source}/HISTORY", ack=False) as history:
        original = history.nrows()
    for index, (options, calls) in enumerate(cases):
        paths = [f"apply-{index}-{name}.ms" for name, _ in TASKS]
        start = time() + MJD_UNIX_EPOCH_S
        for path, (_, task) in zip(paths, TASKS, strict=True):
            copytree(source, path)
            for _ in range(calls):
                assert task(vis=path, **{"flagbackup": False, **options}) == {}
        end = time() + MJD_UNIX_EPOCH_S
        compare_new_rows(paths, original, calls, (start, end))
    return len(cases)


def compare_relocated_history(source):
    paths = [f"relocated-{name}.ms" for name, _ in TASKS]
    with table(f"{source}/HISTORY", ack=False) as history:
        original = history.nrows()
    start = time() + MJD_UNIX_EPOCH_S
    for path, (_, task) in zip(paths, TASKS, strict=True):
        copytree(source, path)
        history = Path(path, "HISTORY")
        relocated = history.rename(history.with_name("PROVENANCE"))
        with table(path, readonly=False, ack=False) as ms:
            ms.putkeyword("HISTORY", f"Table: {relocated.resolve()}")
        copytree(f"{source}/HISTORY", history)
        assert task(vis=path, flagbackup=False) == {}
        with table(str(history), ack=False) as unused:
            assert unused.nrows() == original
    end = time() + MJD_UNIX_EPOCH_S
    compare_new_rows(paths, original, 1, (start, end), subtable="PROVENANCE")


def check_retained_reader(source):
    for name, task in TASKS:
        vis = f"retained-history-{name}.ms"
        copytree(source, vis)
        with table(f"{vis}/HISTORY", lockoptions="usernoread", ack=False) as reader:
            original = list(reader.getcol("MESSAGE"))
            managers = {key: value["TYPE"] for key, value in reader.getdminfo().items()}
            assert task(vis=vis, flagbackup=False) == {}
            reader.resync()
            assert {key: value["TYPE"] for key, value in reader.getdminfo().items()} == managers
            current = list(reader.getcol("MESSAGE"))
            assert current[:len(original)] == original and len(current) == len(original) + 78
            assert current[len(original)] == "taskname=flagdata"
            other = casatasks.flagdata if name == "native" else tasks.flagdata
            assert other(vis=vis, field="FLUX", flagbackup=False) == {}
            reader.resync()
            final = list(reader.getcol("MESSAGE"))
            assert final[:len(current)] == current and len(final) == len(current) + 78
            assert final[len(current)] == "taskname=flagdata"


def compare_unchanged(source):
    cases = ({"mode": "summary"}, {"mode": "clip", "action": "calculate"},
             {"action": "none"}, {"action": ""}, {"field": "unavailable"})
    for index, options in enumerate(cases):
        for name, task in TASKS:
            path = f"inactive-{index}-{name}.ms"
            copytree(source, path)
            before, auxiliary = snapshot(path), auxiliary_content(path)
            try:
                task(vis=path, flagbackup=False, **options)
            except RuntimeError:
                assert "field" in options
            else:
                assert "field" not in options
            assert_preserved(path, before, auxiliary)
    return len(cases)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-history-") as work, chdir(work):
        source = Path("history-source.ms")
        make_cal_ms(str(source), channels=4, scan_seconds=20)
        seed_history(source)
        applied = compare_applies(source)
        compare_relocated_history(source)
        check_retained_reader(source)
        unchanged = compare_unchanged(source)
    print(f"{applied + 1} flagdata HISTORY products and {unchanged} unchanged inputs verified against CASA")


if __name__ == "__main__":
    main()
