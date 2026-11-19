#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public correlation-selection tasks with CASA 6.7.6.14.

Use the same Python environment and native binary as the selection oracle.
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from ms_selection_tasks_oracle import (
    MAIN,
    add_second_window,
    casa_table,
    columns_from_casacore,
    compare_columns,
    make_input,
)


def make_input_with_two_polarizations(source):
    make_input(source, stokes="XX XY YX YY")
    add_second_window(source)
    with table(str(source / "POLARIZATION"), readonly=False, ack=False) as pol:
        pol.addrows(1)
        for name in pol.colnames():
            pol.putcell(name, 1, [5, 6, 7, 8] if name == "CORR_TYPE" else pol.getcell(name, 0))
    with table(str(source / "DATA_DESCRIPTION"), readonly=False, ack=False) as dd:
        dd.putcell("POLARIZATION_ID", 1, 1)
    with table(str(source), readonly=False, ack=False) as ms:
        flags = ms.getcol("FLAG")
        flags[:4, :, 0] = True
        ms.putcol("FLAG", flags)
        ms.putcol("FLAG_ROW", flags.all(axis=(1, 2)))
        category = np.stack((flags, ~flags), axis=1)
        ms.putcol("FLAG_CATEGORY", category)
        ms.putcolkeyword("FLAG_CATEGORY", "CATEGORY", ["sample flag", "test category"])
        ms.putcol("CORRECTED_DATA", (1000 + ms.getcol("DATA")).astype(np.complex64))


def compare_main(want_path, got_path, *, reverse_ddids=()):
    with (table(str(want_path), ack=False) as want,
          table(str(got_path), ack=False) as got,
          casa_table(got_path) as readback):
        assert got.nrows() == want.nrows()
        for row in range(got.nrows()):
            for name in MAIN:
                expected = want.getcell(name, row)
                if name in ("DATA", "FLAG", "WEIGHT", "SIGMA"):
                    if want.getcell("DATA_DESC_ID", row) in reverse_ddids:
                        expected = expected[..., ::-1]
                np.testing.assert_array_equal(got.getcell(name, row), expected,
                                              err_msg=f"{got_path.name} row {row} {name}")
                np.testing.assert_array_equal(np.asarray(readback.getcell(name, row)).T, expected)


def compare_polarization(want_path, got_path):
    with (table(str(want_path / "POLARIZATION"), ack=False) as want,
          table(str(got_path / "POLARIZATION"), ack=False) as got,
          casa_table(got_path / "POLARIZATION") as readback):
        assert got.nrows() == want.nrows()
        for row in range(got.nrows()):
            for name in ("NUM_CORR", "CORR_TYPE", "CORR_PRODUCT", "FLAG_ROW"):
                expected = want.getcell(name, row)
                np.testing.assert_array_equal(got.getcell(name, row), expected)
                np.testing.assert_array_equal(np.asarray(readback.getcell(name, row)).T, expected)


def succeeds(root, source, name, case, options, *, reverse_ddids=(), missing_metadata=False):
    paths = [root / f"{name}-{case}-{side}.ms" for side in ("casa", "native")]
    for module, path in zip((casatasks, tasks), paths):
        arguments = dict(vis=str(source), outputvis=str(path), datacolumn="data") | options
        assert getattr(module, name)(**arguments) is None
    compare_main(*paths, reverse_ddids=reverse_ddids)
    for subtable, columns in {
        "DATA_DESCRIPTION": ("SPECTRAL_WINDOW_ID", "POLARIZATION_ID"),
        "SPECTRAL_WINDOW": ("NUM_CHAN", "CHAN_FREQ"),
    }.items():
        compare_columns(*(path / subtable for path in paths), columns, case)
    if missing_metadata:
        with table(str(paths[0] / "POLARIZATION"), ack=False) as casa:
            assert not casa.iscelldefined("CORR_TYPE", 0)
        with (table(str(paths[1] / "POLARIZATION"), ack=False) as native,
              casa_table(paths[1] / "POLARIZATION") as readback):
            np.testing.assert_array_equal(native.getcell("CORR_TYPE", 0), [9, 10, 12])
            np.testing.assert_array_equal(native.getcell("CORR_PRODUCT", 0), [[0, 0], [0, 1], [1, 1]])
            assert native.getcell("NUM_CORR", 0) == 3
            np.testing.assert_array_equal(readback.getcell("CORR_TYPE", 0), [9, 10, 12])
    else:
        compare_polarization(*paths)
    return paths


def reject(root, source, name, case, options, error_type, diagnostic):
    for module in (casatasks, tasks):
        path = root / f"{name}-{case}-{module.__name__}.ms"
        try:
            getattr(module, name)(vis=str(source), outputvis=str(path), datacolumn="data", **options)
        except error_type as error:
            assert diagnostic in str(error), (name, options, str(error))
        else:
            raise AssertionError(f"{module.__name__}.{name} accepted {options!r}")
        assert not path.exists(), path


def main():
    assert casatasks.version() == [6, 7, 6, 14], casatasks.version()
    with TemporaryDirectory(prefix="ms-correlation-") as scratch:
        root = Path(scratch)
        source = root / "input.ms"
        make_input_with_two_polarizations(source)
        before = columns_from_casacore(source, (*MAIN, "FLAG_CATEGORY", "CORRECTED_DATA"))
        count = 0
        rejected = 0
        for name in ("split", "mstransform"):
            cases = {
                "xx": {"correlation": "XX"},
                "rr": {"correlation": "RR"},
                "multi": {"correlation": "XX;RR"},
                "spw-qualified": {"correlation": "1:RR;SPW0:XX,YY"},
                "channels": {"correlation": "xx, yy", "spw": "SPW0:2~5"},
                "corrected": {"correlation": "YY", "datacolumn": "corrected", "field": "FLUX"},
                "keepflags": {"correlation": "XX", "keepflags": False},
                "duplicate": {"correlation": ",XX,XX,"},
                "reverse": {"correlation": "YY,XX"},
                "three": {"correlation": "XX,XY,YY"},
            }
            if name == "split":
                cases["list"] = {"correlation": ["XX", "YY"]}
                cases["np-list"] = {"correlation": np.array(["XX", "YY"])}
                cases["empty-list"] = {"correlation": []}
            for case, options in cases.items():
                casa, native = succeeds(
                    root, source, name, case, options,
                    reverse_ddids=(0,) if case == "reverse" else (),
                    missing_metadata=case == "three",
                )
                if case == "xx":
                    with table(str(casa), ack=False) as casa_main:
                        assert not casa_main.iscelldefined("FLAG_CATEGORY", 0)
                    with (table(str(native), ack=False) as native_main, casa_table(native) as readback):
                        expected = before["FLAG_CATEGORY"][:native_main.nrows(), ..., [0]]
                        np.testing.assert_array_equal(
                            native_main.getcol("FLAG_CATEGORY"), expected,
                        )
                        np.testing.assert_array_equal(readback.getcol("FLAG_CATEGORY").T, expected)
                count += 1
            failures = [
                ({"correlation": "I"}, RuntimeError, "No match for polarization ID"),
                ({"correlation": "XX,RR"}, RuntimeError, "No match for polarization ID"),
                ({"correlation": "XX YY"}, RuntimeError, "No match for polarization ID"),
                ({"correlation": " xx "}, RuntimeError, "No match for polarization ID"),
                ({"correlation": "SPW0:XX", "spw": "1"}, RuntimeError, "MSSelectionNullSelection"),
                ({"correlation": None}, AssertionError, "null value not allowed"),
                ({"correlation": 1}, AssertionError, "must be of cStr type"),
                ({"correlation": np.array("XX")}, RuntimeError, "used for field correlation with type unknown"),
            ]
            if name == "mstransform":
                failures.append(({"correlation": ["XX", "YY"]}, RuntimeError,
                                 "used for field correlation with type unknown"))
            for index, (options, kind, message) in enumerate(failures):
                reject(root, source, name, f"rejected-{index}", options, kind, message)
                rejected += 1
        for column, values in columns_from_casacore(source, before).items():
            np.testing.assert_array_equal(values, before[column], err_msg=f"input {column}")
        print(f"{count} correlation products and {rejected} rejected calls compared with CASA 6.7.6.14")


if __name__ == "__main__":
    main()
