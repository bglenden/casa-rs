#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check clipping against CASA's virtual and materialized Fourier models."""

from contextlib import chdir
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_summary_oracle import snapshot
from imager_prediction_options_oracle import write_model
from ms_selection_tasks_oracle import casa_table
from record_casa_returns import make_cal_ms


def prepare_models():
    make_cal_ms("source.ms", channels=4, scan_seconds=20)
    with table("source.ms/FIELD", ack=False) as fields:
        direction = fields.getcell("PHASE_DIR", 0).ravel()
    write_model(Path("point.im"), direction, 100e9, 2, cell=5)
    for name, scratch in (("virtual", False), ("scratch", True)):
        path = f"{name}.ms"
        copytree("source.ms", path)
        casatasks.ft(vis=path, model="point.im", field="0", usescratch=scratch)
    with table("virtual.ms", ack=False) as virtual, table("scratch.ms", ack=False) as scratch:
        assert not np.array_equal(virtual.getcol("MODEL_DATA"), scratch.getcol("MODEL_DATA"))


def compare_models():
    cases = (
        ("MODEL", {}, 24),
        ("RESIDUAL_DATA", {}, 10),
        ("RESIDUAL", {}, 10),
        ("MODEL", {"spw": "0:1~2"}, 12),
    )
    for case, (column, extra, flagged) in enumerate(cases):
        result = None
        for name, source, task in (("scratch", "scratch", casatasks.flagdata),
                                   ("casa", "virtual", casatasks.flagdata),
                                   ("native", "virtual", tasks.flagdata)):
            path = f"case-{case}-{name}.ms"
            copytree(f"{source}.ms", path)
            before, auxiliary = snapshot(path), auxiliary_content(path)
            options = dict(vis=path, field="0", datacolumn=column, mode="clip",
                           correlation="REAL_XX", clipminmax=[0.5, 1.5],
                           clipoutside=False, flagbackup=False, **extra)
            with table(path, ack=False) as ms:
                model = ms.getcol("MODEL_DATA")
                corrected = ms.getcol("CORRECTED_DATA")
                observed = ms.getcol("DATA")
            if name != "scratch":
                assert task(action="calculate", **options) == {}
                assert_preserved(path, before, auxiliary)
            assert task(**options) == {}
            with table(path, ack=False) as ms, casa_table(path) as casa:
                flags = ms.getcol("FLAG")
                row_flags = ms.getcol("FLAG_ROW")
                np.testing.assert_array_equal(flags, casa.getcol("FLAG").transpose(2, 1, 0))
                np.testing.assert_array_equal(row_flags, casa.getcol("FLAG_ROW"))
                np.testing.assert_array_equal(ms.getcol("MODEL_DATA"), model)
                np.testing.assert_array_equal(ms.getcol("CORRECTED_DATA"), corrected)
                np.testing.assert_array_equal(ms.getcol("DATA"), observed)
                assert not flags[ms.getcol("FIELD_ID") != 0].any()
                assert not flags[:, :, 1].any()
                assert flags.sum() == flagged
                if result is None:
                    result = (flags, row_flags)
                else:
                    np.testing.assert_array_equal(flags, result[0])
                    np.testing.assert_array_equal(row_flags, result[1])
    return len(cases)


def compare_overlapping_scans():
    paths = [f"overlap-{name}.ms" for name in ("casa", "native")]
    for path, task in zip(paths, (casatasks.flagdata, tasks.flagdata), strict=True):
        copytree("virtual.ms", path)
        with table(path, readonly=False, ack=False) as ms:
            scans = ms.getcol("SCAN_NUMBER")
            scans[1::2] += 10
            ms.putcol("SCAN_NUMBER", scans)
        assert task(vis=path, field="0", mode="clip", datacolumn="MODEL",
                    correlation="REAL_XX", clipminmax=[0.99, 1.01],
                    clipoutside=False, flagbackup=False) == {}
        with table(path, ack=False) as ms, casa_table(path) as casa:
            flags = ms.getcol("FLAG")
            expected = np.zeros_like(flags)
            expected[ms.getcol("SCAN_NUMBER") == 11, :, 0] = True
            assert expected.sum() == 40
            np.testing.assert_array_equal(flags, expected)
            np.testing.assert_array_equal(casa.getcol("FLAG").transpose(2, 1, 0), expected)
            assert not ms.getcol("FLAG_ROW").any()


def compare_partly_flagged_channel():
    for column, bounds in (("outside", [0.5, 1.5]), ("zero", [-0.01, 0.01])):
        for representation in ("scratch", "virtual"):
            masks = []
            for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
                path = f"preflagged-{column}-{representation}-{name}.ms"
                copytree(f"{representation}.ms", path)
                with table(path, ack=False, readonly=False) as ms:
                    flags = ms.getcell("FLAG", 1)
                    flags[1, 1] = True
                    ms.putcell("FLAG", 1, flags)
                assert task(vis=path, field="0", mode="clip", datacolumn="MODEL",
                            correlation="REAL_XX", clipminmax=bounds,
                            clipoutside=False, flagbackup=False) == {}
                with table(path, ack=False) as ms, casa_table(path) as casa:
                    flags = ms.getcol("FLAG")
                    matches = (representation == "virtual") == (column == "zero")
                    np.testing.assert_array_equal(flags[1, 1], [matches, True])
                    np.testing.assert_array_equal(casa.getcol("FLAG").transpose(2, 1, 0), flags)
                    masks.append((flags, ms.getcol("FLAG_ROW")))
            for expected, actual in zip(*masks, strict=True):
                np.testing.assert_array_equal(actual, expected)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-virtual-fourier-") as directory, chdir(directory):
        prepare_models()
        products = compare_models()
        compare_overlapping_scans()
        compare_partly_flagged_channel()
    print(f"{products} ordinary products plus simultaneous scans and four preflagged products agree with CASA")


if __name__ == "__main__":
    main()
