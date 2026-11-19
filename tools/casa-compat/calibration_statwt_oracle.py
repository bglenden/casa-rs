#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare classical scalar statwt previews and persisted MS columns."""

import argparse
import inspect
import os
from pathlib import Path

import casatasks as casa
import numpy as np

from applycal_oracle import copy_ms
from calibration_initweights_oracle import opened
from record_casa_returns import make_cal_ms


COLUMNS = ("DATA", "CORRECTED_DATA", "MODEL_DATA", "WEIGHT", "SIGMA", "FLAG", "FLAG_ROW")


def fixture(path):
    make_cal_ms(str(path))
    with opened(path, True) as table:
        rows = table.nrows()
        values = np.array([[-3, -2, -1, 0, 1, 2, 3, 4], [0, 4, -1, 2, 3, 1, -5, 0]], dtype=np.complex64)
        values += 1j * np.array([[0, 0, 1, 1, -1, 0, 2, -3], [1, 0, 6, 2, 0, -2, -2, 4]])
        cube = values[:, :, None] + np.reshape(np.arange(rows) // 10, (1, 1, rows))
        flags = np.zeros(cube.shape, dtype=bool)
        flags[:, 0, 1] = True
        flags[0, :, 2] = True
        flags[1, 1:, 2] = True
        flags[:, :, 3] = True
        table.putcol("DATA", cube)
        table.putcol("FLAG", flags)
        table.putcol("FLAG_ROW", np.all(flags, axis=(0, 1)))
        table.putcell("FLAG_ROW", 4, True)
        table.putcol("CORRECTED_DATA", cube * 2)
        table.putcol("MODEL_DATA", cube * .25)
        table.putcol("SIGMA", np.broadcast_to([[2.], [4.]], (2, rows)).astype(np.float32).copy())
        for column in ("WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"):
            if column in table.colnames():
                table.removecols(column)


def read(path):
    with opened(path) as table:
        return {name: table.getcol(name) for name in COLUMNS}


def compare_case(root, base, native, name, options):
    products = []
    reports = []
    for api, engine in ((casa, "casa"), (native, "native")):
        vis = root / f"{engine}-{name}.ms"
        copy_ms(base, vis)
        before = read(vis)
        preview = api.statwt(str(vis), flagbackup=True, preview=True, **options)
        assert list(preview) == ["mean", "variance"]
        assert not Path(str(vis) + ".flagversions").exists()
        for column, values in read(vis).items():
            np.testing.assert_array_equal(values, before[column], err_msg=column)
        report = api.statwt(str(vis), flagbackup=False, **options)
        assert report == preview
        reports.append(report)
        result = read(vis)
        for column in ("DATA", "CORRECTED_DATA", "MODEL_DATA"):
            np.testing.assert_array_equal(result[column], before[column], err_msg=column)
        if options.get("field") and options.get("selectdata", True):
            with opened(vis) as table:
                excluded = table.getcol("FIELD_ID") != 0
            for column in ("WEIGHT", "SIGMA", "FLAG", "FLAG_ROW"):
                np.testing.assert_array_equal(result[column][..., excluded], before[column][..., excluded])
        products.append(result)
    for key in reports[0]:
        np.testing.assert_allclose(reports[1][key], reports[0][key], rtol=2e-7, atol=0, err_msg=key)
    for column in COLUMNS:
        np.testing.assert_allclose(products[1][column], products[0][column], rtol=2e-7, atol=0,
                                   err_msg=column)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", required=True, type=Path)
    parser.add_argument("--native-bin", required=True, type=Path)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    os.environ["CASARS_CALIBRATE_BIN"] = str(args.native_bin.resolve())
    os.environ["CASARS_REQUIRE_TASK_BINARIES"] = "1"
    from casars import tasks as native

    assert casa.version_string() == "6.7.6.14"
    source = inspect.signature(casa.statwt)
    candidate = inspect.signature(native.statwt)
    assert [(p.name, p.kind, p.default) for p in source.parameters.values()] == [
        (p.name, p.kind, p.default) for p in candidate.parameters.values()
    ]
    base = root / "base.ms"
    fixture(base)
    cases = [
        ("corrected", {}),
        ("data", {"datacolumn": "DATA"}),
        ("correlations", {"datacolumn": "data", "combine": "corr"}),
        ("selected-field", {"field": "FLUX"}),
        ("selectdata-disabled", {"field": "unused", "selectdata": False}),
        ("minimum-and-range", {"minsamp": 8, "wtrange": [0.04, 0.08]}),
        ("residual-data", {"datacolumn": "residual_data"}),
        ("residual-corrected", {"datacolumn": "residual"}),
    ]
    for name, options in cases:
        compare_case(root, base, native, name, options)
    print(f"{len(cases)} matched CASA classical statwt workflows")


if __name__ == "__main__":
    main()
