#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check CASA 6.7.6.14 offline additive noise and consume native outputs (#1292).

Use CASA's Python. --out records independent CASA controls; --verify-native
consumes the three cases exported by CASA_RS_OFFLINE_NOISE_OUT. Random streams
need not match: the reference is the saved field, its statistics and reuse.
"""

import argparse
import json
from pathlib import Path

import numpy as np
from casatools import simulator, table
from sm_multiple_models_oracle import make_session


def read_data(path):
    tb = table()
    tb.open(str(path))
    values = np.asarray([tb.getcell("DATA", row) for row in range(tb.nrows())])
    tb.close()
    return values


def row_key(tb, row):
    return (float(tb.getcell("TIME", row)), int(tb.getcell("ANTENNA1", row)),
            int(tb.getcell("ANTENNA2", row)))


def parameters_for_rows(path, noise):
    tb = table()
    tb.open(str(noise))
    assert tb.info()["subType"] == "A Noise"
    values = {}
    for row in range(tb.nrows()):
        key = row_key(tb, row)
        parameters = tb.getcell("CPARAM", row)
        assert parameters.shape == (4, 4)
        assert not np.any(tb.getcell("FLAG", row))
        values[key] = parameters
    tb.close()
    tb.open(str(path))
    result = [values[row_key(tb, row)] for row in range(tb.nrows())]
    tb.close()
    return np.asarray(result)


def nrms(actual, expected):
    return float(np.linalg.norm(actual - expected) / max(np.linalg.norm(expected), 1e-30))


def verify_native(root):
    cases = sorted(root.glob("*/case.json"))
    assert len(cases) == 3, len(cases)
    for case in cases:
        directory = case.parent
        stokes = json.loads(case.read_text())["stokes"]
        indices = {"XX": [0], "XX YY": [0, 3], "XX XY YX YY": [0, 1, 2, 3]}[stokes]
        path, noise = directory / "data.ms", directory / "noise.A.cal"
        saved = parameters_for_rows(path, noise)[:, indices, :]
        before = read_data(path)
        assert nrms(before, -saved) < 1e-3, stokes
        expected = before - saved
        sm = simulator()
        assert sm.openfromms(str(path))
        assert sm.setapply(table=str(noise), interp="nearest")
        assert sm.corrupt()
        sm.close()
        residual = nrms(read_data(path), expected)
        assert residual < 1e-3, (stokes, residual)
        print("CASA consumed native A Noise", stokes, "DATA NRMS", residual, flush=True)


def record_controls(root):
    root.mkdir(parents=True, exist_ok=True)
    for label, stokes, named in [("one", "XX", True), ("two", "XX YY", True),
                                  ("four", "XX XY YX YY", True),
                                  ("unsaved", "XX XY YX YY", False)]:
        directory = root / label
        directory.mkdir()
        path, noise = directory / "data.ms", directory / "noise.A.cal"
        sm = make_session(path, stokes)
        assert sm.setseed(19)
        assert sm.setnoise(mode="simplenoise", simplenoise="0.25Jy", OTF=False,
                           table=str(directory / "noise.cal") if named else "")
        assert sm.corrupt()
        once = read_data(path)
        assert sm.corrupt()
        twice = read_data(path)
        sm.close()
        residual = nrms(twice, 2 * once)
        assert residual < 1e-3, residual
        result = dict(stokes=stokes, named=named, repeat_nrms=residual)
        if named:
            indices = {"XX": [0], "XX YY": [0, 3], "XX XY YX YY": [0, 1, 2, 3]}[stokes]
            saved = parameters_for_rows(path, noise)
            result["table_nrms"] = nrms(once, -saved[:, indices, :])
            assert result["table_nrms"] < 1e-3, result
            result["rows"] = len(saved)
        (directory / "result.json").write_text(json.dumps(result, indent=2))
        print("CASA offline noise", result, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--out", type=Path)
    group.add_argument("--verify-native", type=Path)
    args = parser.parse_args()
    if args.verify_native:
        verify_native(args.verify_native)
    else:
        record_controls(args.out)


if __name__ == "__main__":
    main()
