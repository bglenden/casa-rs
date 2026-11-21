#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check TFCrop channel averaging and its physical flag destinations."""

from contextlib import chdir
from json import dumps, loads
from pathlib import Path
from shutil import copytree
from signal import SIGABRT
from subprocess import run
import sys
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import auxiliary_content
from flagdata_routine_controls_oracle import flags
from flagdata_tfcrop_fit_oracle import make_fixture
from flagdata_tfcrop_sources_oracle import check_science_state, put_samples, science_state


def check_products(libraries):
    empty, narrow, broad = make_fixture("averaging-source.ms")
    with table("averaging-source.ms", readonly=False, ack=False) as main:
        main.putcol("CORRECTED_DATA", main.getcol("DATA"))
    cases = (
        (0, "freq", "", "data", "", False, (16,)),
        (1, "freq", "", "data", "", False, ()),
        (2, "freq", "", "data", "", False, (16, 17)),
        ([2], "freq", "", "data", "", False, (16, 17)),
        (4, "freq", "", "data", "", False, ()),
        (5, "freq", "", "data", "", False, ()),
        (2, "freq", "0:8~31", "data", "", False, (16, 17)),
        (2, "freq", "0:8~15;16~31", "data", "", False, (16, 17)),
        (2, "freq", "0:6~15;18~31", "data", "", False, ()),
        (2, "time", "0:8~19", "data", "", False, range(8, 20)),
        (4, "time", "0:8~19", "data", "", False, range(8, 20)),
        (1, "time", "", "corrected", "", False, range(32)),
        (2, "freq", "", "data", "preflag", False, ()),
        (2, "freq", "", "data", "SIGMA_SPECTRUM", False, ()),
        (2, "freq", "", "data", "WEIGHT_SPECTRUM", False, (16, 17)),
        (2, "freq", "", "corrected", "WEIGHT_SPECTRUM", False, ()),
        (2, "freq", "", "corrected", "SIGMA_SPECTRUM", False, (16, 17)),
        (2, "freq", "", "data", "phase-cancellation", False, ()),
        (2, "freq", "", "data", "", True, (16, 17)),
        (2, "freq", "0:8~23", "data", "", True, ()),
    )
    for index, (width, direction, spw, column, seed, extend, channels) in enumerate(cases):
        for name, task in libraries:
            path = f"average-{index}-{name}.ms"
            copytree("averaging-source.ms", path)
            seeded = empty.copy()
            with table(path, readonly=False, ack=False) as main:
                if seed == "preflag":
                    seeded[narrow, 16, 1] = True
                    main.putcol("FLAG", seeded)
                elif seed.endswith("_SPECTRUM"):
                    values = np.ones(empty.shape, dtype=np.float32)
                    values[narrow, 16, 1] = 1e10 if seed.startswith("SIGMA") else 1e-12
                    put_samples(main, seed, values)
                elif seed == "phase-cancellation":
                    data = main.getcol("DATA")
                    data[narrow, 17, 1] = -28.
                    main.putcol("DATA", data)
            expected = seeded.copy()
            for channel in channels:
                if direction == "time":
                    expected[broad, channel, 0] = True
                elif extend:
                    expected[narrow, channel, :] = True
                else:
                    expected[narrow, channel, 1] = True
            original, auxiliary = science_state(path), auxiliary_content(path)
            options = dict(vis=path, mode="tfcrop", antenna="0&1", datacolumn=column, spw=spw,
                           flagdimension=direction, channelavg=True, chanbin=width, freqcutoff=1.5,
                           timecutoff=2.5, timefit="poly", maxnpieces=3, extendflags=extend)
            assert task(action="calculate", **options) == {}
            if name == "native":
                calculated = tasks.run("flagdata", action="calculate", **options)
                assert loads(calculated.stdout)["result"]["changed_samples"] == expected.sum() - seeded.sum()
            verify_flags(path, seeded)
            check_science_state(path, original)
            assert auxiliary_content(path) == auxiliary
            assert task(**options) == {}
            verify_flags(path, expected)
            check_science_state(path, original)
            verify_flags(str(Path(path + ".flagversions", "flags.flagdata_1")), seeded)
    return len(cases), broad


def verify_flags(path, expected):
    actual, rows = flags(path)
    np.testing.assert_array_equal(actual, expected)
    assert not rows.any()


def check_stride(broad, native):
    safe = "explicit-channel-list.ms"
    copytree("averaging-source.ms", safe)
    original, _ = flags(safe)
    expected = original.copy()
    for channel in range(8, 24, 2):
        expected[broad, channel, 0] = True
    common = dict(mode="tfcrop", antenna="0&1", datacolumn="data", flagdimension="time",
                  timefit="poly", timecutoff=2.5, maxnpieces=3, channelavg=True, chanbin=2,
                  extendflags=False, flagbackup=False)
    assert casatasks.flagdata(vis=safe, spw="0:8;10;12;14;16;18;20;22", **common) == {}
    verify_flags(safe, expected)

    failing = "pinned-stride.ms"
    copytree("averaging-source.ms", failing)
    execute = """
import json
import resource
import sys
from casatasks import flagdata

resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
flagdata(**json.loads(sys.argv[1]))
"""
    completed = run([sys.executable, "-c", execute, dumps(dict(vis=failing, spw="0:8~23^2", **common))],
                    capture_output=True, text=True, timeout=30)
    assert completed.returncode == -SIGABRT and "ArraySlicerError" in completed.stderr, completed.stderr

    if native:
        path = "native-stride.ms"
        copytree("averaging-source.ms", path)
        assert native(vis=path, spw="0:8~23^2", **common) == {}
        verify_flags(path, expected)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    libraries = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory(prefix="flagdata-tfcrop-channelavg-") as root, chdir(root):
        products, broad = check_products(libraries)
        check_stride(broad, tasks.flagdata)
    print(f"{products} TFCrop channel averaging products and a strided selection verified")


if __name__ == "__main__":
    main()
