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
             ("20s", 364), ("1min", 324), ("2ms", 324), ("-1s", 324),
             ("", 324), (np.int64(20), 364))
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
            masks.append(samples)
            for prior, after in zip(before, snapshot(path), strict=True):
                np.testing.assert_array_equal(after[2], prior[2])
        np.testing.assert_array_equal(*masks)

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
    return len(cases) + 4


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


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-routine-controls-") as directory, chdir(directory):
        make_cal_ms("source.ms", channels=4, scan_seconds=60)
        products = check_time_chunks()
        errors = check_time_errors()
        check_displays()
    print(f"{products} timed products, {errors} failures and the headless display controls verified")


if __name__ == "__main__":
    main()
