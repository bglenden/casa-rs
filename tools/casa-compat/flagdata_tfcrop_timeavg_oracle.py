#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare TFCrop time averaging and flags on the contributing integrations."""

from contextlib import chdir
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import auxiliary_content
from flagdata_tfcrop_channelavg_oracle import run_product, verify_flags
from flagdata_tfcrop_fit_oracle import make_fixture
from flagdata_tfcrop_sources_oracle import check_science_state, science_state


OPTIONS = dict(mode="tfcrop", antenna="0&1", timeavg=True, flagdimension="time", timefit="poly",
               timecutoff=2.5, freqcutoff=1.5, maxnpieces=3, extendflags=False)


def check_time_products(libraries):
    source = "tfcrop-time.ms"
    empty, narrow, _ = make_fixture(source)
    with table(source, ack=False) as main:
        times = main.getcol("TIME")
        integration = np.searchsorted(np.unique(times), times)
    full = range(32)
    cases = (
        (dict(timebin="0s"), (6,), full, (0,)),
        (dict(timebin="10s"), (6,), full, (0,)),
        (dict(timebin="20s"), (6, 7), full, (0,)),
        (dict(timebin="30s"), (), full, (0,)),
        (dict(timebin="40s"), (), full, (0,)),
        (dict(timebin="240s"), (), full, (0,)),
        (dict(timebin=""), (6,), full, (0,)),
        (dict(timebin="-10s"), (6,), full, (0,)),
        (dict(timebin="garbage"), (), full, (0,)),
        (dict(timebin="20s", spw="0:8~19"), (6, 7), range(8, 20), (0,)),
        (dict(timebin="20s", spw="0:8~19", channelavg=True, chanbin=2), (6, 7), range(8, 20), (0,)),
        (dict(timebin="20s", spw="0:8~19", extendflags=True), (6, 7), range(8, 20), (0, 1)),
        (dict(timebin="20s", flagdimension="freq"), range(21), (16,), (1,)),
        (dict(timebin="20s", flagdimension="freq", channelavg=True, chanbin=2),
         range(21), (16, 17), (1,)),
        (dict(timebin="20s", ntime="65s"), (), full, (0,)),
        (dict(timebin="20s", timeavg=np.bool_(True)), (6,), full, (0,)),
    )
    for index, (overrides, integrations, channels, planes) in enumerate(cases):
        rows = narrow & np.isin(integration, integrations)
        expected = empty.copy()
        for plane in planes:
            for channel in channels:
                expected[rows, channel, plane] = True
        for name, task in libraries:
            path = f"time-{index}-{name}.ms"
            copytree(source, path)
            run_product(name, task, OPTIONS | dict(vis=path) | overrides, empty, expected)
    return len(cases)


def check_vector_refusal(libraries):
    for name, task in libraries:
        path = f"time-vector-{name}.ms"
        copytree("tfcrop-time.ms", path)
        with table(path, ack=False) as main:
            seeded = main.getcol("FLAG")
        original, auxiliary = science_state(path), auxiliary_content(path)
        for action in ("calculate", "apply"):
            options = dict(vis=path, timeavg=[True], timebin="20s", action=action)
            try:
                task(**(OPTIONS | options))
            except RuntimeError as error:
                assert str(error) == "RecordInterface::asBool - invalid data type"
            else:
                raise AssertionError((name, options))
            verify_flags(path, seeded)
            check_science_state(path, original)
            if action == "calculate":
                assert auxiliary_content(path) == auxiliary
            else:
                verify_flags(str(Path(path + ".flagversions", "flags.flagdata_1")), seeded)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    libraries = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory(prefix="flagdata-tfcrop-timeavg-") as root, chdir(root):
        products = check_time_products(libraries)
        check_vector_refusal(libraries)
    print(f"{products} TFCrop time averaging products and the vector refusal verified")


if __name__ == "__main__":
    main()
