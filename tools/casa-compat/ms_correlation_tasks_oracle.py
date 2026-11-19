#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public correlation-selection tasks with CASA 6.7.6.14.

Use the same Python environment and native binary as the selection oracle.
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table
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


def replace_array_descriptor(tool, name, *, fixed):
    values = tool.getcol(name)
    keywords = tool.getcolkeywords(name)
    datatype = tool.getcoldesc(name)["valueType"]
    options = {"shape": values.shape[1:]} if fixed else {}
    tool.removecols([name])
    tool.addcols(maketabdesc(makearrcoldesc(
        name, 0, ndim=values.ndim - 1, valuetype=datatype, **options,
    )))
    tool.putcol(name, values)
    tool.putcolkeywords(name, keywords)


def fix_column_shapes(source):
    with table(str(source / "DATA_DESCRIPTION"), readonly=False, ack=False) as dd:
        dd.putcell("POLARIZATION_ID", 1, 0)
    for subtable, columns in {
        "": ("DATA", "FLAG", "FLAG_CATEGORY", "WEIGHT", "SIGMA"),
        "POLARIZATION": ("CORR_TYPE", "CORR_PRODUCT"),
        "SPECTRAL_WINDOW": ("CHAN_FREQ", "CHAN_WIDTH", "RESOLUTION", "EFFECTIVE_BW"),
    }.items():
        with table(str(source / subtable), readonly=False, ack=False) as tool:
            for name in columns:
                replace_array_descriptor(tool, name, fixed=True)


def shorten_first_window(source):
    with table(str(source / "SPECTRAL_WINDOW"), readonly=False, ack=False) as windows:
        for name in ("CHAN_FREQ", "CHAN_WIDTH", "RESOLUTION", "EFFECTIVE_BW"):
            windows.putcell(name, 0, windows.getcell(name, 0)[:1])
        windows.putcell("NUM_CHAN", 0, 1)
        windows.putcell("TOTAL_BANDWIDTH", 0, windows.getcell("CHAN_WIDTH", 0)[0])
    with table(str(source), readonly=False, ack=False) as main:
        columns = ("DATA", "CORRECTED_DATA", "MODEL_DATA", "FLAG", "FLAG_CATEGORY")
        for name in columns:
            replace_array_descriptor(main, name, fixed=False)
        for row in np.where(main.getcol("DATA_DESC_ID") == 0)[0]:
            for name in columns:
                cell = main.getcell(name, int(row))
                main.putcell(name, int(row), cell[..., :1, :])


def compare_rows(want_path, got_path, names, *, reverse_ddids=()):
    with (table(str(want_path), ack=False) as want,
          table(str(got_path), ack=False) as got,
          casa_table(got_path) as readback):
        assert got.nrows() == want.nrows()
        for row in range(got.nrows()):
            for name in names:
                expected = want.getcell(name, row)
                if reverse_ddids and name in ("DATA", "FLAG", "WEIGHT", "SIGMA"):
                    if want.getcell("DATA_DESC_ID", row) in reverse_ddids:
                        expected = expected[..., ::-1]
                np.testing.assert_array_equal(got.getcell(name, row), expected,
                                              err_msg=f"{got_path.name} row {row} {name}")
                np.testing.assert_array_equal(np.asarray(readback.getcell(name, row)).T, expected)


def succeeds(root, source, name, case, options, *, reverse_ddids=(), missing_metadata=False):
    paths = [root / f"{name}-{case}-{side}.ms" for side in ("casa", "native")]
    for module, path in zip((casatasks, tasks), paths):
        arguments = dict(vis=str(source), outputvis=str(path), datacolumn="data") | options
        assert getattr(module, name)(**arguments) is None
    compare_rows(*paths, MAIN, reverse_ddids=reverse_ddids)
    compare_columns(*(path / "DATA_DESCRIPTION" for path in paths),
                    ("SPECTRAL_WINDOW_ID", "POLARIZATION_ID"), case)
    compare_rows(*(path / "SPECTRAL_WINDOW" for path in paths), ("NUM_CHAN", "CHAN_FREQ"))
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
        compare_rows(*(path / "POLARIZATION" for path in paths),
                     ("NUM_CORR", "CORR_TYPE", "CORR_PRODUCT", "FLAG_ROW"))
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
        fixed = root / "fixed.ms"
        make_input_with_two_polarizations(fixed)
        fix_column_shapes(fixed)
        mixed = root / "mixed.ms"
        make_input_with_two_polarizations(mixed)
        shorten_first_window(mixed)
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
                cases["object-list"] = {"correlation": np.array(["XX", "YY"], dtype=object)}
                cases["upper-list-window"] = {"correlation": ["spw0:xx"]}
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
            for index, qualifier in enumerate(("(0)", "(SPW0)")):
                succeeds(root, fixed, name, f"fixed-{index}", {"correlation": f"{qualifier}:XX"})
                count += 1
            succeeds(root, fixed, name, "fixed-channel", {"correlation": "XX,YY", "spw": "0:2~5"})
            casa, native = succeeds(root, mixed, name, "mixed-category-shapes", {"correlation": "XX;RR"})
            with (table(str(mixed), ack=False) as original,
                  table(str(casa), ack=False) as reference,
                  table(str(native), ack=False) as got,
                  casa_table(native) as readback):
                assert not reference.iscelldefined("FLAG_CATEGORY", 0)
                for row in range(got.nrows()):
                    expected = original.getcell("FLAG_CATEGORY", row)[..., [0]]
                    np.testing.assert_array_equal(got.getcell("FLAG_CATEGORY", row), expected)
                    np.testing.assert_array_equal(readback.getcell("FLAG_CATEGORY", row).T, expected)
            count += 2
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
                failures.append(({"correlation": np.array(["XX", "YY"], dtype=object)}, RuntimeError,
                                 "used for field correlation with type unknown"))
            else:
                failures.extend([
                    ({"correlation": ["", "YY"]}, RuntimeError, "No match for polarization ID"),
                    ({"correlation": ["YY", ""]}, RuntimeError, "No match for polarization ID"),
                ])
            for index, (options, kind, message) in enumerate(failures):
                reject(root, source, name, f"rejected-{index}", options, kind, message)
                rejected += 1
        for column, values in columns_from_casacore(source, before).items():
            np.testing.assert_array_equal(values, before[column], err_msg=f"input {column}")
        print(f"{count} correlation products and {rejected} rejected calls compared with CASA 6.7.6.14")


if __name__ == "__main__":
    main()
