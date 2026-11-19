#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare Tsys-based initweights against CASA 6.7.6.14."""

import argparse
import os
from pathlib import Path

import casatasks as casa
from casatools import table
import numpy as np

from applycal_oracle import copy_ms
from calibration_initweights_oracle import UNCHANGED, add_spectra, compare, fixture, main_columns, opened
from record_casa_returns import make_cal_ms


def make_table(root, vis):
    syscal = root / "syscal.tbl"
    raw = root / "syscal.txt"
    with opened(vis) as main:
        times = np.unique(main.getcol("TIME"))
    records = []
    for index, time in enumerate((times[0], times[-1])):
        for spw in range(2):
            for antenna in range(5):
                values = [100 + 5 * spw + 25 * index + 10 * antenna + 4 * receptor + 7 * channel
                          for channel in range(8) for receptor in range(2)]
                records.append(" ".join(map(str, [antenna, 0, spw, time, 5, *values, "false"])))
    raw.write_text("\n".join(records) + "\n")
    handle = table()
    assert handle.fromascii(
        str(syscal), str(raw), sep=" ",
        columnnames=["ANTENNA_ID", "FEED_ID", "SPECTRAL_WINDOW_ID", "TIME", "INTERVAL",
                     "TSYS_SPECTRUM", "TSYS_FLAG"],
        datatypes=["I", "I", "I", "D", "D", "R2,8", "B"],
    )
    handle.done()
    with opened(syscal, write=True) as handle:
        handle.putcolkeywords("TIME", {"QuantumUnits": ["s"], "MEASINFO": {"type": "epoch", "Ref": "UTC"}})
        handle.putcolkeywords("INTERVAL", {"QuantumUnits": ["s"]})
        handle.putcolkeywords("TSYS_SPECTRUM", {"QuantumUnits": ["K"]})
    with opened(vis, write=True) as handle:
        if "SYSCAL" in handle.keywordnames():
            handle.removekeyword("SYSCAL")
        handle.putkeyword("SYSCAL", f"Table: {syscal}")
    cal = root / "temperature.cal"
    casa.gencal(str(vis), str(cal), "tsys")
    return cal


def flagged_table(cal, result):
    copy_ms(cal, result)
    with opened(result, write=True) as handle:
        for row, antenna in enumerate(handle.getcol("ANTENNA1")):
            flags = handle.getcell("FLAG", row)
            values = handle.getcell("FPARAM", row)
            if antenna == 0:
                flags[0, 0] = True
                values[:, 4] = [-5., 0.]
            if antenna == 2:
                flags[:] = True
            if antenna == 3:
                values[:] = 0.
            handle.putcell("FLAG", row, flags)
            handle.putcell("FPARAM", row, values)


def test_case(root, base, native, index, options, existing, shift, exception=None, float_only=False):
    files = [root / f"{name}-{index}.ms" for name in ("casa", "native")]
    for api, vis in zip((casa, native), files, strict=True):
        copy_ms(base, vis)
        add_spectra(vis, existing)
        if float_only:
            with opened(vis, write=True) as handle:
                assert handle.addcols({"FLOAT_DATA": {"valueType": "float", "ndim": 2}})
                handle.putcol("FLOAT_DATA", handle.getcol("DATA").real)
                handle.removecols("DATA")
        if shift:
            with opened(vis / "SPECTRAL_WINDOW", write=True) as handle:
                for spw in range(handle.nrows()):
                    handle.putcell("CHAN_FREQ", spw, handle.getcell("CHAN_FREQ", spw) + 0.4e6)
        before = main_columns(vis)
        try:
            result = api.initweights(str(vis), **options)
        except Exception as error:
            assert exception and isinstance(error, exception), (api, error)
            if exception is Exception:
                assert str(error) == f"Tsys calibration table {options['tsystable']} not found"
            if options.get("gainfield") == "999" or options.get("interp") == "bad":
                # CASA can create the optional columns before failing setup.
                for name in ("WEIGHT", "SIGMA", *UNCHANGED, "HISTORY"):
                    np.testing.assert_array_equal(main_columns(vis)[1][name], before[1][name])
            else:
                compare(before, main_columns(vis))
        else:
            assert exception is None and result is None, (api, options, result)
            after = main_columns(vis)
            unchanged = ("FLOAT_DATA", *UNCHANGED[1:]) if float_only else UNCHANGED
            for name in (*unchanged, "HISTORY"):
                np.testing.assert_array_equal(after[1][name], before[1][name])
    if exception is None:
        # Three f32 ulps can accumulate in the scalar temperature mean and
        # reciprocal baseline factors. This is tighter than the applycal
        # Tsys weight tolerance.
        compare(*(main_columns(vis) for vis in files), rtol=5e-7)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir(exist_ok=False)
    os.environ["CASARS_CALIBRATE_BIN"] = str(args.native_bin.resolve())
    os.environ["CASARS_REQUIRE_TASK_BINARIES"] = "1"
    from casars import tasks as native

    assert casa.version_string() == "6.7.6.14"
    base = root / "base.ms"
    fixture(base)
    cal = make_table(root, base)
    flagged = root / "flagged.cal"
    flagged_table(cal, flagged)
    missing_window = root / "one-window.cal"
    copy_ms(cal, missing_window)
    with opened(missing_window, write=True) as handle:
        handle.removerows(np.flatnonzero(handle.getcol("SPECTRAL_WINDOW_ID") == 1))
    cases = []
    for mode in ("tsys", "tinttsys"):
        for create, existing in ((False, ""), (True, ""), (False, "partial"), (True, "full")):
            cases.append((dict(wtmode=mode, tsystable=str(cal), dowtsp=create), existing, False, None))
    for create in (False, True):
        cases.append((dict(wtmode="tsys", tsystable=str(flagged), dowtsp=create), "", False, None))
    for create, existing in ((False, ""), (True, ""), (False, "partial")):
        cases.append((dict(wtmode="tsys", tsystable=str(missing_window), dowtsp=create), existing,
                      False, None))
    for field, interp, mapping in (
        ("1", "nearest,linear", [1, 0]),
        ("nearest", "linear,linear", [0, 1]),
        ("FLUX", "nearest,spline", [-1]),
        ("", "linear,nearest", [0, 1]),
        ("", "linear,linearflag", [0, 1]),
        ("", "linearperscan,cubic", [0, 1]),
        ("", "nearestperobs,spline", [0, 1]),
        ("", "linear,linearrel", [1, 0]),
        ("", "linear", [0]),
    ):
        cases.append((dict(wtmode="tsys", tsystable=str(cal), gainfield=field, interp=interp,
                           spwmap=mapping, dowtsp=True), "", True, None))
    cases.extend([
        (dict(wtmode="tsys", tsystable=str(cal), dowtsp=True), "", False, None, True),
        (dict(wtmode="tsys", tsystable="~/" + os.path.relpath(cal, Path.home())), "", False, None),
        (dict(wtmode="tsys", tsystable=str(root / "missing.cal")), "", False, Exception),
        (dict(wtmode="tsys", tsystable=str(cal), gainfield="999", dowtsp=True), "", False, RuntimeError),
        (dict(wtmode="tsys", tsystable=str(cal), interp="bad", dowtsp=True), "", False, RuntimeError),
    ])
    for index, options in enumerate(cases):
        test_case(root, base, native, index, *options)
    stokes_i = root / "stokes-i.ms"
    make_cal_ms(str(stokes_i), stokes="I")
    for index, create in enumerate((False, True), start=len(cases)):
        test_case(root, stokes_i, native, index, dict(wtmode="tsys", tsystable=str(cal),
                  dowtsp=create), "", False)
    print(f"{len(cases) + 2} matched CASA Tsys initweights cases")


if __name__ == "__main__":
    main()
