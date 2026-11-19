#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare ordinary initweights modes and persisted MS columns against CASA 6.7.6.14."""

import argparse
from contextlib import contextmanager
import inspect
import os
from pathlib import Path

import casatasks as casa
from casatools import table
import numpy as np

from applycal_oracle import copy_ms
from calibration_gencal_specify_oracle import with_second_window
from calibration_scratch_oracle import referenced_tables
from record_casa_returns import make_cal_ms


SPECTRA = ("WEIGHT_SPECTRUM", "SIGMA_SPECTRUM")
UNCHANGED = ("DATA", "FLAG", "FLAG_ROW", "ANTENNA1", "ANTENNA2", "EXPOSURE", "DATA_DESC_ID")


@contextmanager
def opened(path, write=False):
    tool = table()
    assert tool.open(str(path), nomodify=not write)
    try:
        yield tool
    finally:
        tool.done()


def fixture(path):
    make_cal_ms(str(path))
    with_second_window(path)
    with opened(path, write=True) as main:
        weight = np.broadcast_to(np.array([[4.], [9.]], np.float32), (2, main.nrows())).copy()
        sigma = np.broadcast_to(np.array([[2.], [4.]], np.float32), weight.shape).copy()
        sigma[:, 1] = [0., -2.]
        sigma[:, 2] = [np.nan, np.inf]
        sigma[:, 3] = [0.1, 0.3]
        main.putcol("WEIGHT", weight)
        main.putcol("SIGMA", sigma)
        main.putcell("ANTENNA2", 0, main.getcell("ANTENNA1", 0))
        main.putcol("EXPOSURE", np.resize([3., 5., 0., -1.], main.nrows()))
        main.putcell("FLAG_ROW", 0, True)
        flags = main.getcell("FLAG", 1)
        flags[0, 0] = True
        main.putcell("FLAG", 1, flags)
    with opened(path / "SPECTRAL_WINDOW", write=True) as spw:
        spw.putcell("EFFECTIVE_BW", 0, np.arange(1., 9.) * 1e6)
        spw.putcell("EFFECTIVE_BW", 1, np.arange(1., 9.) * 2e6)


def add_spectra(path, contents):
    if not contents:
        return
    with opened(path, write=True) as main:
        for index, column in enumerate(SPECTRA):
            assert main.addcols({column: {"valueType": "float", "ndim": 2}})
            cell = np.arange(16, dtype=np.float32).reshape(2, 8) + 1 + index * 100
            for row in range(0, main.nrows(), 2 if contents == "partial" else 1):
                main.putcell(column, row, cell)


def main_columns(path):
    with opened(path) as main:
        present = set(main.colnames())
        values = {column: main.getcol(column)
                  for column in ("WEIGHT", "SIGMA", *UNCHANGED, "FLOAT_DATA") if column in present}
        for column in SPECTRA:
            if column not in present:
                continue
            values[column] = [
                main.getcell(column, row) if main.iscelldefined(column, row) else None
                for row in range(main.nrows())
            ]
        with opened(path / "HISTORY") as history:
            values["HISTORY"] = history.getcol("MESSAGE")
        return present, values


def compare(expected, actual, *, rtol=2e-7):
    assert expected[0] == actual[0], expected[0] ^ actual[0]
    for column, reference in expected[1].items():
        value = actual[1][column]
        if column in SPECTRA:
            for index, (left, right) in enumerate(zip(reference, value, strict=True)):
                if left is None or right is None:
                    assert left is right, (column, index)
                else:
                    np.testing.assert_allclose(right, left, rtol=rtol, atol=0, err_msg=f"{column} row {index}")
        elif column in ("WEIGHT", "SIGMA"):
            np.testing.assert_allclose(value, reference, rtol=rtol, atol=0, err_msg=column)
        else:
            np.testing.assert_array_equal(value, reference, err_msg=column)


def compare_case(root, base, native, mode, spectrum, contents):
    expected, outputs = None, {}
    for name, tasks in (("casa", casa), ("native", native)):
        vis = root / f"{name}-{mode}-{int(spectrum)}-{contents or 'absent'}.ms"
        copy_ms(base, vis)
        add_spectra(vis, contents)
        before = main_columns(vis)
        if mode == "weight" and not spectrum:
            try:
                tasks.initweights(str(vis), mode, dowtsp=spectrum)
            except RuntimeError:
                compare(before, main_columns(vis))
                continue
            raise AssertionError(f"{name} accepted weight with dowtsp=False")
        if mode == "nyq" and not spectrum:
            assert tasks.initweights(str(vis)) is None
        else:
            assert tasks.initweights(str(vis), mode, "unused", "FLUX", "nearest", np.array([1, 0]), spectrum) is None
        after = main_columns(vis)
        for column in (*UNCHANGED, "HISTORY"):
            np.testing.assert_array_equal(after[1][column], before[1][column], err_msg=column)
        if name == "casa":
            expected = after
        else:
            compare(expected, after)
        outputs[name] = after[1]
    if mode == "nyq":
        # The first row is flagged and autocorrelated, the second is a
        # cross-correlation in the second SPW. Neither flag changes the weight.
        for result in outputs.values():
            np.testing.assert_array_equal(result["WEIGHT"][:, :2], [[13.5e6, 90.e6]] * 2)
    if mode == "sigma":
        for result in outputs.values():
            assert result["WEIGHT"][0, 1] == np.finfo(np.float32).eps


def compare_mms(root, base, native):
    for spectrum in (False, True):
        outputs = []
        for name, api in (("casa", casa), ("native", native)):
            mms = root / f"{name}-partition-{spectrum}.mms"
            casa.partition(str(base), str(mms), separationaxis="scan", numsubms=2,
                           disableparallel=True, flagbackup=False, datacolumn="all")
            parts = referenced_tables(mms)
            assert len(parts) == 2
            before = [main_columns(path)[1]["HISTORY"] for path in (mms, *parts)]
            assert api.initweights(str(mms), "ones", dowtsp=spectrum) is None
            after = [main_columns(path) for path in (mms, *parts)]
            for history, (_, columns) in zip(before, after, strict=True):
                np.testing.assert_array_equal(columns.pop("HISTORY"), history)
            outputs.append(after)
        for expected, actual in zip(*outputs, strict=True):
            compare(expected, actual)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    args = parser.parse_args()
    args.work.mkdir(exist_ok=False)
    os.environ["CASARS_CALIBRATE_BIN"] = str(args.native_bin.resolve())
    os.environ["CASARS_REQUIRE_TASK_BINARIES"] = "1"
    from casars import tasks as native

    assert casa.version_string() == "6.7.6.14"
    reference = inspect.signature(casa.initweights)
    candidate = inspect.signature(native.initweights)
    assert [(p.name, p.kind, p.default) for p in reference.parameters.values()] == [
        (p.name, p.kind, p.default) for p in candidate.parameters.values()
    ]
    base = args.work / "base.ms"
    fixture(base)
    cases = [(mode, spectrum, contents)
             for mode in ("nyq", "ones", "sigma", "weight", "delwtsp", "delsigsp")
             for spectrum in (False, True)
             for contents in ("", "partial", "full")]
    for case in cases:
        compare_case(args.work, base, native, *case)
    compare_mms(args.work, base, native)
    print(f"{len(cases) + 2} matched CASA initweights cases")


if __name__ == "__main__":
    main()
