#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the `MODEL_DATA` tclean writes, cell by cell, with CASA's (issue 1081).

Run with CASA's Python, on a data root holding the MeasurementSets below with
any `XSym` links resolved (`tclean_case_matrix.py` says why):

    /Applications/CASA.app/Contents/MacOS/python3 -I \
        tools/casa-compat/tclean_model_column_compare.py \
        --data-root <root> --casars-python <venv with the wheel> \
        --imager-binary <binary> --output <new dir> [--case NAME ...]

Each side images its own copy of the MeasurementSet. Both create `MODEL_DATA`
with CASA's initial model, one in every cell, so a cell still holding exactly
one was not written. The summary counts, per channel, the cells only one side
wrote, and gives the normalized RMS of the cells both wrote. `predictModel`
also writes zero into the channels no plane reaches, which casa-rs doesn't read
(issue 1081): those cells are counted apart and don't fail a case. Native runs
at `gridprecision='f64'`.
"""
import argparse
import json
import shutil
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tclean_compare import run_native  # noqa: E402

POINT = "unittest/tclean/refim_point.ms"
CUBE = {
    "field": "0", "imsize": 100, "cell": "8.0arcsec", "niter": 10, "specmode": "cube",
    "nchan": 10, "restfreq": ["1.25GHz"], "phasecenter": "J2000 19:59:28.500 +40.44.01.50",
    "deconvolver": "hogbom", "spw": "0", "start": 0, "width": 1, "veltype": "radio",
    "interpolation": "linear", "savemodel": "modelcolumn",
}
CASES = {
    # test_cube_1's cube, whose edge channels keep few of their times.
    "topo": (POINT, dict(CUBE, outframe="TOPO")),
    "lsrk": (POINT, dict(CUBE, outframe="LSRK")),
    "lsrk-middle": (POINT, dict(CUBE, outframe="LSRK", start=5)),
    "named-channels": (POINT, dict(CUBE, outframe="LSRK", spw="0:3~14", start=5, nchan=8)),
    # tclean's final write is `predictModel` under these.
    "topo-nsigma": (POINT, dict(CUBE, outframe="TOPO", nsigma=0.5)),
    "topo-auto": (POINT, dict(CUBE, outframe="TOPO", usemask="auto-multithresh")),
    "lsrk-nsigma": (POINT, dict(CUBE, outframe="LSRK", nsigma=0.5)),
    # A first call solves, without a model column; a second only predicts it.
    "topo-predict": (POINT, dict(CUBE, outframe="TOPO", savemodel="none"),
                     dict(CUBE, outframe="TOPO", niter=0, calcres=False, calcpsf=False)),
}
NRMS_LIMIT = 1e-3


def read_model(table, path):
    """`MODEL_DATA`, `[product, channel, row]`, in the first data description."""
    table.open(str(path))
    try:
        ids = table.getcol("DATA_DESC_ID")
        rows = np.nonzero(ids == 0)[0]
        return table.getcolslice("MODEL_DATA", [0, 0], [-1, -1], [1, 1], int(rows[0]),
                                 int(rows[-1] - rows[0] + 1))
    finally:
        table.close()


def compare(casa, native):
    """The cells one side wrote alone, by channel, and the NRMS of the rest."""
    written = casa != 1, native != 1
    both = written[0] & written[1]
    only_casa = written[0] & ~written[1]
    zero = only_casa & (casa == 0)
    gap = casa[both] - native[both]
    scale = np.sqrt(np.mean(np.abs(casa[both]) ** 2)) if both.any() else 1.0
    return {
        "cells": int(casa.size),
        "casa_wrote": written[0].sum(axis=(0, 2)).tolist(),
        "native_wrote": written[1].sum(axis=(0, 2)).tolist(),
        "only_casa": int((only_casa & ~zero).sum()),
        "only_casa_zero": int(zero.sum()),
        "only_native": int((written[1] & ~written[0]).sum()),
        "only_native_by_channel": (written[1] & ~written[0]).sum(axis=(0, 2)).tolist(),
        "only_casa_by_channel": (only_casa & ~zero).sum(axis=(0, 2)).tolist(),
        "nrms": float(np.sqrt(np.mean(np.abs(gap) ** 2)) / scale) if both.any() else 0.0,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--casars-python", type=Path, required=True)
    parser.add_argument("--imager-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", action="append", choices=sorted(CASES))
    args = parser.parse_args()
    args.output.mkdir(parents=True)

    import casatasks
    import casatools

    casatasks.casalog.setlogfile(str(args.output / "casa.log"))
    table = casatools.table()
    summary = {}
    for case in args.case or CASES:
        source, *calls = CASES[case]
        folder = (args.output / case).resolve()
        folder.mkdir()
        models = {}
        for side in ("casa", "native"):
            ms = folder / f"{side}.ms"
            shutil.copytree(args.data_root / source, ms)
            for values in calls:
                call = dict(values, vis=str(ms), imagename=str(folder / side))
                if side == "casa":
                    casatasks.tclean(**call, parallel=False)
                else:
                    reply = run_native(args.casars_python, args.imager_binary,
                                       dict(call, backend="cpu", gridprecision="f64"), folder)
                    if "result" not in reply:
                        raise RuntimeError(f"{case}: {json.dumps(reply)[:1500]}")
            models[side] = read_model(table, ms)
        result = compare(models["casa"], models["native"])
        result["passed"] = (sum(result["casa_wrote"]) > 0 and result["only_casa"] == 0
                            and result["only_native"] == 0 and result["nrms"] < NRMS_LIMIT)
        summary[case] = result
        print(case, "passed" if result["passed"] else "DIFFERS", f"only casa {result['only_casa']}",
              f"only native {result['only_native']}", f"zero only casa {result['only_casa_zero']}",
              f"nrms {result['nrms']:.2e}", flush=True)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=1))
    print(f"{sum(item['passed'] for item in summary.values())}/{len(summary)} passed", flush=True)


if __name__ == "__main__":
    main()
