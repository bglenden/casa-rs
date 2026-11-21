#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check public TFCrop correlation expressions and physical flag destinations."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import auxiliary_content
from flagdata_routine_controls_oracle import flags
from flagdata_tfcrop_sources_oracle import check_science_state, put_samples, science_state
from record_casa_returns import make_cal_ms


def seed_layout(path, stokes):
    make_cal_ms(path, channels=32, scan_seconds=210, stokes=stokes)
    with table(path, readonly=False, ack=False) as main:
        data = np.ones_like(main.getcol("DATA"))
        baseline = (main.getcol("ANTENNA1") == 0) & (main.getcol("ANTENNA2") == 1)
        if stokes == "XX YY":
            baseline &= main.getcol("FIELD_ID") == 0
            data[:, :, 0] = 2.
        data[baseline, 16, 0] = 30.
        if data.shape[2] > 1:
            data[baseline, 20 if stokes == "XX YY" else 21, -1] = 35.
        if data.shape[2] == 4:
            data[:, :, 1] = 2.
            data[baseline, 9, 1] += 20.
        main.putcol("DATA", data)
        if stokes == "XX YY":
            corrected = np.full(data.shape, 1. + 1j, dtype=np.complex64)
            corrected[baseline, 12, 1] = 1. + 30j
            put_samples(main, "CORRECTED_DATA", corrected)
        empty = np.zeros(data.shape, dtype=bool)
        main.putcol("FLAG", empty)
        main.putcol("FLAG_ROW", np.zeros(main.nrows(), dtype=bool))
    return empty, baseline


def check_products(libraries):
    datasets = {name: seed_layout(f"{name}.ms", stokes) for name, stokes in (
        ("linear", "XX YY"), ("four", "XX XY YX YY"), ("circular", "RR LL"),
        ("stored-i", "I"), ("stored-q", "Q"),
    )}
    cases = (
        ("linear", "data", "", ((16, 0), (20, 1))),
        ("linear", "data", "XX", ((16, 0),)),
        ("linear", "data", "YY", ((20, 1),)),
        ("linear", "data", "YY,XX", ((16, 0),)),
        ("linear", "data", "fooXXbar", ((16, 0),)),
        ("linear", "data", "ABS_XX,ABS_YY", ((16, 0),)),
        ("linear", "data", "ABS_I", ((16, 0), (20, 0), (16, 1), (20, 1))),
        ("linear", "data", "ABS_Q", ((16, 0), (20, 0), (16, 1), (20, 1))),
        ("linear", "data", "ABS_XY", ()),
        ("linear", "data", "unrecognized", ()),
        ("linear", "data", "IMAG_ALL", ()),
        ("linear", "data", "ALL", ((16, 0), (20, 1))),
        ("linear", "corrected", "ABS_YY", ((12, 1),)),
        ("linear", "corrected", "REAL_YY", ()),
        ("linear", "corrected", "IMAG_YY", ((12, 1),)),
        ("linear", "corrected", "ARG_YY", ((12, 1),)),
        ("linear", "corrected", "NORM_YY", ((12, 1),)),
        ("four", "data", "ABS_ALL", ((16, 0), (9, 1), (21, 3))),
        ("four", "data", "ABS_U", ((9, 1), (9, 2))),
        ("four", "data", "ABS_V", ((9, 1), (9, 2))),
        ("circular", "data", "ABS_RR", ((16, 0),)),
        ("circular", "data", "ABS_LL", ((21, 1),)),
        ("stored-i", "data", "", ()),
        ("stored-i", "data", "ABS_I", ()),
        ("stored-i", "data", "ABS_WVR", ((16, 0),)),
        ("stored-i", "data", "WVR_I", ((16, 0),)),
        ("stored-q", "data", "ABS_ALL", ((16, 0),)),
        ("stored-q", "data", "ABS_Q", ((16, 0),)),
    )
    for index, (dataset, column, expression, coordinates) in enumerate(cases):
        empty, baseline = datasets[dataset]
        expected = empty.copy()
        for channel, plane in coordinates:
            expected[baseline, channel, plane] = True
        for name, task in libraries:
            path = f"products-{index}-{name}.ms"
            copytree(f"{dataset}.ms", path)
            science, auxiliary = science_state(path), auxiliary_content(path)
            options = dict(vis=path, mode="tfcrop", datacolumn=column, flagdimension="freq",
                           antenna="0&1", correlation=expression, extendflags=False)
            assert task(action="calculate", **options) == {}
            if name == "native":
                calculated = tasks.run("flagdata", action="calculate", **options)
                assert loads(calculated.stdout)["result"]["changed_samples"] == expected.sum()
            assert auxiliary_content(path) == auxiliary
            preview, preview_rows = flags(path)
            np.testing.assert_array_equal(preview, empty)
            assert not preview_rows.any()
            assert task(**options) == {}
            actual, row_flags = flags(path)
            np.testing.assert_array_equal(actual, expected, err_msg=str(options))
            assert not row_flags.any()
            backup, backup_rows = flags(str(Path(path + ".flagversions", "flags.flagdata_1")))
            np.testing.assert_array_equal(backup, empty)
            assert not backup_rows.any()
            check_science_state(path, science)
    return len(cases)


def check_unknown_operator_product(libraries):
    message = "Unknown polarization requested, (REAL) supported types are: XX,YY,XY,YX,RR,LL,RL,LR,I,Q,U,V"
    for name, task in libraries:
        path = f"unknown-product-{name}.ms"
        copytree("linear.ms", path)
        initial = auxiliary_content(path)
        try:
            task(vis=path, mode="tfcrop", action="calculate", datacolumn="data", correlation="REAL",
                 flagdimension="freq", antenna="0&1", extendflags=False)
        except RuntimeError as error:
            assert str(error) == message
        else:
            raise AssertionError(name)
        assert auxiliary_content(path) == initial
        assert not any(mask.any() for mask in flags(path))


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    libraries = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory(prefix="flagdata-tfcrop-products-") as root, chdir(root):
        products = check_products(libraries)
        check_unknown_operator_product(libraries)
    print(f"{products} TFCrop correlation products and an invalid expression verified")


if __name__ == "__main__":
    main()
