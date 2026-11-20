#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check iterator time chunks and headless display controls with pinned CASA."""

from contextlib import chdir
from pathlib import Path
from shutil import copytree
import subprocess
import sys
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_autocorr_oracle import assert_same_visibilities
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_summary_oracle import snapshot
from ms_selection_tasks_oracle import casa_table
from record_casa_returns import make_cal_ms


def flags(path):
    with table(path, ack=False) as ms, casa_table(path) as casa:
        samples, rows = ms.getcol("FLAG"), ms.getcol("FLAG_ROW")
        np.testing.assert_array_equal(casa.getcol("FLAG").transpose(2, 1, 0), samples)
        np.testing.assert_array_equal(casa.getcol("FLAG_ROW"), rows)
        return samples, rows


def check_time_chunks():
    cases = (("scan", 324), (1, 674), (20., 364), (40, 324),
             ("20s", 364), ("1min", 324), ("2ms", 324), ("0s", 324),
             ("", 324), (np.int64(20), 364), (True, 674), (float("inf"), 324))
    for index, (ntime, count) in enumerate(cases):
        masks = []
        for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
            path = f"time-{index}-{name}.ms"
            copytree("source.ms", path)
            options = dict(vis=path, mode="clip", ntime=ntime, timeavg=True, timebin="40s",
                           clipminmax=[1.2, 2.2], flagbackup=False)
            before, auxiliary = snapshot(path), auxiliary_content(path)
            assert task(action="calculate", **options) == {}
            assert_preserved(path, before, auxiliary)
            assert task(**options) == {}
            samples, rows = flags(path)
            assert samples.sum() == count, (name, ntime, samples.sum(), count)
            if name == "native":
                # The averaged clip's row flag describes all original samples.
                np.testing.assert_array_equal(rows, samples.all(axis=(1, 2)))
            masks.append(samples)
            for prior, after in zip(before, snapshot(path), strict=True):
                np.testing.assert_array_equal(after[2], prior[2])
        np.testing.assert_array_equal(*masks)

    # CASA can use an uninitialized interval for these inputs. Derive the
    # stable result from its explicit scan request, including the whole mask.
    expected, _ = flags("time-0-casa.ms")
    for index, (ntime, history_value) in enumerate((("-1s", '"-1s"'), (float("nan"), "nan"))):
        path = f"scan-default-{index}.ms"
        copytree("source.ms", path)
        assert tasks.flagdata(vis=path, mode="clip", ntime=ntime, timeavg=True, timebin="40s",
                              clipminmax=[1.2, 2.2], flagbackup=False) == {}
        samples, rows = flags(path)
        np.testing.assert_array_equal(samples, expected)
        np.testing.assert_array_equal(rows, samples.all(axis=(1, 2)))
        with table(f"{path}/HISTORY", ack=False) as history:
            assert [line for line in history.getcol("MESSAGE")[-78:] if line.startswith("ntime ")] == [
                f"ntime       = {history_value}"
            ]

    for mode in ("manual", "unflag", "summary", "quack"):
        paths = [f"{mode}-{name}.ms" for name in ("casa", "native")]
        results = []
        for path, task in zip(paths, (casatasks.flagdata, tasks.flagdata), strict=True):
            copytree("source.ms", path)
            options = {"quackinterval": 5.0} if mode == "quack" else {}
            results.append(task(vis=path, mode=mode, ntime=20, flagbackup=False, **options))
        assert results[0] == results[1]
        for before, after in zip(flags(paths[0]), flags(paths[1]), strict=True):
            np.testing.assert_array_equal(after, before)
    return len(cases) + 6


def check_time_gaps():
    copytree("source.ms", "gaps.ms")
    with table("gaps.ms", readonly=False, ack=False) as ms:
        field = ms.getcol("FIELD_ID") == 0
        time, centroid, data = [ms.getcol(name) for name in ("TIME", "TIME_CENTROID", "DATA")]
        times = np.unique(time[field])
        for old, offset, visibility in zip(times, [0, 10, 20, 73, 87, 95],
                                           [0, 2, 6, 0, 2, 6], strict=True):
            rows = field & (time == old)
            time[rows] = centroid[rows] = times[0] + offset
            data[rows] = visibility
        for name, values in (("TIME", time), ("TIME_CENTROID", centroid), ("DATA", data)):
            ms.putcol(name, values)
        baseline = (ms.getcol("ANTENNA1") == 0) & (ms.getcol("ANTENNA2") == 1)

    cases = ((25, "", field), (25, "0&1", field & baseline),
             (20, "", np.zeros_like(field)), (40, "", field))
    for index, (ntime, antenna, selected_rows) in enumerate(cases):
        expected = np.broadcast_to(selected_rows[:, None, None], data.shape)
        for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
            path = f"gap-{index}-{name}.ms"
            copytree("gaps.ms", path)
            assert task(vis=path, mode="clip", field="0", antenna=antenna,
                        ntime=ntime, timeavg=True, timebin="200s", clipoutside=False,
                        clipminmax=[2.6, 2.7], flagbackup=False) == {}
            samples, rows = flags(path)
            np.testing.assert_array_equal(samples, expected)
            if name == "native":
                np.testing.assert_array_equal(rows, selected_rows)
    return len(cases)


def check_time_errors():
    cases = ((0, "apply", ValueError, True), (np.float64(-1), "apply", ValueError, True),
             ("nonsense", "apply", RuntimeError, True), ("SCAN", "calculate", RuntimeError, False),
             (0, "none", ValueError, False), ([2], "apply", AssertionError, False),
             (np.array(2), "apply", AssertionError, False))
    for index, (ntime, action, typ, backup) in enumerate(cases):
        for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
            path = f"bad-time-{index}-{name}.ms"
            copytree("source.ms", path)
            before = snapshot(path)
            try:
                task(vis=path, ntime=ntime, action=action)
            except typ:
                pass
            else:
                raise AssertionError((name, ntime, action))
            assert_same_visibilities(path, before)
            assert Path(f"{path}.flagversions/flags.flagdata_1").exists() == backup
    return len(cases)


def check_displays():
    # CASA attempts to start its separate viewer in the headless wheel.
    execute = "import sys; from casatasks import flagdata; assert flagdata(vis=sys.argv[1], display=sys.argv[2], flagbackup=False) == {}"
    for index, display in enumerate(("data", "report", "both", "unrecognized")):
        casa, native = (f"display-{index}-{name}.ms" for name in ("casa", "native"))
        for path in (casa, native):
            copytree("source.ms", path)
        subprocess.run([sys.executable, "-c", execute, casa, display], check=True, timeout=10)
        assert tasks.flagdata(vis=native, display=display, flagbackup=False) == {}
        for expected, actual in zip(flags(casa), flags(native), strict=True):
            np.testing.assert_array_equal(actual, expected)

    summary = casatasks.flagdata(vis="source.ms", mode="summary")
    original, auxiliary = snapshot("source.ms"), auxiliary_content("source.ms")
    for display in ("data", "report", "both"):
        assert tasks.flagdata(vis="source.ms", mode="summary", display=display) == summary
        assert_preserved("source.ms", original, auxiliary)
    for display in (None, False, ["data"], np.array("")):
        for task in (casatasks.flagdata, tasks.flagdata):
            try:
                task(vis="source.ms", display=display)
            except AssertionError as error:
                assert "display" in str(error)
            else:
                raise AssertionError(display)
            assert_preserved("source.ms", original, auxiliary)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-routine-controls-") as directory, chdir(directory):
        make_cal_ms("source.ms", channels=4, scan_seconds=60)
        products = check_time_chunks() + check_time_gaps()
        errors = check_time_errors()
        check_displays()
    print(f"{products} timed products, {errors} failures and the headless display controls verified")


if __name__ == "__main__":
    main()
