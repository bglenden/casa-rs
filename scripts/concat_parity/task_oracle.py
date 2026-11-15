#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA 6.7.6-14 task references on small slices of its public concat data.

Run with CASA Python: task_oracle.py PUBLIC_DATA WORK FIXTURES.
The full, unmodified m51c and unittest cases are verified separately.
"""
import gzip
import json
from pathlib import Path
import shutil
import sys
import tarfile

import numpy as np
from casatasks import concat
from casatools import table

tb = table()


def thin(source, target, rows=12):
    tb.open(str(source))
    selected = tb.query(f"rownumber() < {rows}")
    copied = selected.copy(str(target), deep=True, valuecopy=True)
    copied.close()
    selected.close()
    tb.close()
    tb.open(str(target))
    times, intervals, observations = (tb.getcol(name) for name in ["TIME", "INTERVAL", "OBSERVATION_ID"])
    tb.close()
    tb.open(str(target / "OBSERVATION"), nomodify=False)
    for observation in np.unique(observations):
        selected = observations == observation
        tb.putcell("TIME_RANGE", int(observation), np.array([
            np.min(times[selected] - intervals[selected] / 2),
            np.max(times[selected] + intervals[selected] / 2)]))
    tb.close()
    # The oracle checks opaque record preservation with a small nested record;
    # the full public m51c run also checks its original FTMachine records.
    tb.open(str(target / "SOURCE"), nomodify=False)
    if "SOURCE_MODEL" in tb.colnames():
        for row in range(tb.nrows()):
            tb.putcell("SOURCE_MODEL", row, {"numft": 0, "fixture": {"name": target.name, "v": np.array([2., 3.])}})
        for key in list(tb.getkeywords()):
            if key.startswith(("definedmodel_field_", "model_")):
                tb.removekeyword(key)
    tb.close()
    tb.open(str(target / "POINTING"), nomodify=False)
    if tb.nrows() > 12:
        tb.removerows(list(range(12, tb.nrows())))
    if tb.nrows():
        # Distinct names/targets and a false tracking bit distinguish a copy
        # from a reconstruction of just the direction.
        tb.putcell("NAME", 0, "preserve-this-pointing")
        tb.putcell("TRACKING", 0, False)
        direction = tb.getcell("TARGET", 0)
        direction[0, 0] += 1.e-7
        tb.putcell("TARGET", 0, direction)
    tb.close()


def encode(value):
    if isinstance(value, dict):
        return {"record": {key: encode(item) for key, item in value.items()}}
    if isinstance(value, np.ndarray):
        return {"shape": list(value.shape), "values": [encode(x) for x in value.flatten()]}
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, complex):
        return {"complex": [value.real, value.imag]}
    if isinstance(value, float) and not np.isfinite(value):
        return str(value)
    return value


def add_sigma_spectrum(path):
    tb.open(str(path), nomodify=False)
    description = tb.getcoldesc("DATA")
    description["valueType"] = "float"
    description["dataManagerGroup"] = "ConcatSigmaSpectrum"
    tb.addcols({"SIGMA_SPECTRUM": description})
    for row in range(tb.nrows()):
        sigma = tb.getcell("SIGMA", row)
        channels = tb.getcell("DATA", row).shape[1]
        tb.putcell("SIGMA_SPECTRUM", row, np.repeat(sigma[:, None], channels, axis=1))
    tb.close()


def snapshot(path):
    result = {}
    for name in ["MAIN", "ANTENNA", "DATA_DESCRIPTION", "FEED", "FIELD", "FLAG_CMD", "OBSERVATION",
                 "POINTING", "POLARIZATION", "PROCESSOR", "SOURCE", "SPECTRAL_WINDOW", "STATE"]:
        source = path if name == "MAIN" else path / name
        if not source.exists():
            continue
        tb.open(str(source))
        columns = {}
        for column in tb.colnames():
            values = [encode(tb.getcell(column, row)) if tb.iscelldefined(column, row) else None
                      for row in range(tb.nrows())]
            if any(value is not None for value in values):
                columns[column] = values
        result[name] = {"rows": tb.nrows(), "columns": columns}
        tb.close()
    return result


def main(public, work, fixtures):
    inputs = work / "inputs"
    inputs.mkdir(parents=True, exist_ok=True)
    pair = public / "regression/sim_multi_arrays_and_TP/m51c_reference"
    for name, source in [("twelve.ms", pair / "m51c.alma_0.5arcsec.ms"),
                         ("seven.ms", pair / "m51c.aca.i.ms"),
                         ("weighted.ms", public / "measurementset/other/part1.ms")]:
        target = inputs / name
        shutil.rmtree(target, ignore_errors=True)
        thin(source, target)
        if name in ["twelve.ms", "seven.ms"]:
            add_sigma_spectrum(target)
    late = inputs / "late.ms"
    shutil.rmtree(late, ignore_errors=True)
    shutil.copytree(inputs / "weighted.ms", late)
    tb.open(str(late), nomodify=False)
    for column in ["TIME", "TIME_CENTROID"]:
        tb.putcol(column, tb.getcol(column) + 1000.)
    # Preserve sigma independently of weights: recalibrating weights does not
    # license concat to replace an existing uncertainty estimate.
    tb.putcol("SIGMA", tb.getcol("SIGMA") * 1.7)
    tb.putcell("FLAG_ROW", 0, True)
    tb.putcell("FLAG", 0, np.zeros_like(tb.getcell("FLAG", 0)))
    for column in ["WEIGHT", "WEIGHT_SPECTRUM"]:
        value = tb.getcell(column, 0)
        value[:] = np.nan
        tb.putcell(column, 0, value)
    # A stored spectrum need not equal its row summary in every channel.
    value = tb.getcell("WEIGHT_SPECTRUM", 1)
    value[:, 1] *= 1.5
    tb.putcell("WEIGHT_SPECTRUM", 1, value)
    tb.close()
    tb.open(str(late / "OBSERVATION"), nomodify=False)
    tb.putcol("TIME_RANGE", tb.getcol("TIME_RANGE") + 1000.)
    tb.close()
    for path in [inputs / "weighted.ms", late]:
        tb.open(str(path))
        centre = float(np.min(tb.getcol("TIME")))
        tb.close()
        tb.open(str(path / "FEED"), nomodify=False)
        tb.putcol("TIME", np.repeat(centre, tb.nrows()))
        tb.putcol("INTERVAL", np.repeat(2000., tb.nrows()))
        tb.close()
    cases = {
        "pair": dict(vis=["twelve.ms", "seven.ms"], visweightscale=[4., .34]),
        "reversed": dict(vis=["seven.ms", "twelve.ms"], visweightscale=[.34, 4.]),
        "without_pointing": dict(vis=["twelve.ms", "seven.ms"], copypointing=False),
        "weighted": dict(vis=["late.ms", "weighted.ms"], visweightscale=[2., 3.]),
    }
    fixtures.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fixtures / "concat_inputs.tar.gz", "w:gz") as archive:
        for path in inputs.iterdir():
            archive.add(path, arcname=path.name)
    references = {}
    for name, parameters in cases.items():
        output = work / f"{name}.ms"
        shutil.rmtree(output, ignore_errors=True)
        # CASA mutates appended input HISTORY; keep the archived inputs clean.
        copies = work / f"{name}-inputs"
        shutil.rmtree(copies, ignore_errors=True)
        shutil.copytree(inputs, copies)
        arguments = {**parameters, "vis": [str(copies / path) for path in parameters["vis"]]}
        concat(concatvis=str(output), **arguments)
        references[name] = {"parameters": parameters, "tables": snapshot(output)}
        if name in ["pair", "reversed"]:
            # CASA scales SIGMA but omits SIGMA_SPECTRUM for the first MS.
            # Keep the actual reference intact and record the analytic
            # correction the native task must apply to its uncertainties.
            tb.open(str(inputs / "twelve.ms"))
            first_rows = tb.nrows()
            original = tb.getcol("SIGMA_SPECTRUM")
            tb.close()
            tb.open(str(output))
            actual = tb.getcol("SIGMA_SPECTRUM", nrow=first_rows)
            tb.close()
            if not np.array_equal(actual, original):
                raise RuntimeError("CASA's first-input sigma-spectrum behavior changed")
            references[name]["first_sigma_spectrum_correction"] = {"rows": first_rows, "scale": 4.0}
        print(name, "recorded", flush=True)
    with gzip.open(fixtures / "concat_casa.json.gz", "wt") as stream:
        json.dump({"casa_version": "6.7.6.14", "cases": references}, stream, allow_nan=False)


if __name__ == "__main__":
    main(*(Path(arg).resolve() for arg in sys.argv[1:]))
