#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA reads the virtual models of a tclean case matrix run (CASA 6.7.6.14).

Evidence driver for `savemodel='virtual'` (#823), not a runtime dependency.
Run it with CASA's Python (`-I`) on the output directory of
`tclean_case_matrix.py`. For every case whose MeasurementSets the calls
mutated, for CASA's copy and casa-rs's: where the model is filed, its
record's structure, and the model visibilities CASA's visibility iterator
predicts from it (`mstransform(datacolumn='model')`, in a copy whose
`MODEL_DATA` is zero, the column added through the table tool, which clears
nothing, when the MeasurementSet has none). casa-rs's prediction is compared
with CASA's (NRMS), and, when casa-rs's copy also holds a `MODEL_DATA` column
an earlier call wrote, with that column. The report is
`virtual_verify.json` beside the receipt.

    CASA-python -I tools/casa-compat/tclean_virtual_model_verify.py OUTPUT [CASE ...]
"""

import json
import os
import shutil
import sys
import tempfile

import numpy as np
from casatasks import mstransform
from casatools import table

tb = table()


def keywords(path):
    tb.open(path)
    kw = tb.getkeywords()
    tb.close()
    return kw


def plain(value):
    if isinstance(value, dict):
        return {k: plain(v) for k, v in value.items()}
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    return value


def describe(value):
    if isinstance(value, dict):
        return {k: describe(v) for k, v in value.items()}
    if isinstance(value, np.ndarray):
        return f"array<{value.dtype}>{list(value.shape)}"
    return type(value).__name__


def model_record(vis):
    """Field 0's pointer, key, where the record is, and the record."""
    source = vis + "/SOURCE"
    src_kw = keywords(source) if os.path.isdir(source) else {}
    tb.open(source)
    rows = tb.nrows()
    tb.close()
    main_kw = keywords(vis)
    pointers = {}
    for where, kw in (("SOURCE", src_kw), ("MAIN", main_kw)):
        for name, value in kw.items():
            if name.startswith("definedmodel_field_"):
                pointers[where + ":" + name] = value
    out = {"pointers": pointers, "records": {}}
    for where, kw in (("SOURCE", src_kw), ("MAIN", main_kw)):
        for name, value in kw.items():
            if not name.startswith("model_"):
                continue
            if where == "SOURCE" and rows > 0:
                tb.open(source)
                record = tb.getcell("SOURCE_MODEL", int(value))
                tb.close()
            else:
                record = value
            out["records"][where + ":" + name] = record
    return out


def entries(record):
    found = []
    for k in range(int(record.get("numft", 0))):
        entry = record[f"ft_{k}"]
        container = entry["container"]
        images = []
        if "diskimage" in container:
            images.append(container["diskimage"])
        for key in sorted(container):
            if key.startswith("subftm_"):
                images.append(container[key]["diskimage"])
        found.append(
            {
                "name": container["name"],
                "indexcombination": np.asarray(entry["indexcombination"]).tolist(),
                "channelselection": np.asarray(entry["channelselection"]).tolist(),
                "images": images,
                "images_exist": [os.path.isdir(image) for image in images],
                "container": describe(container),
            }
        )
    return found


def predicted(vis, scratch):
    """CASA's model visibilities of `vis` as its iterator predicts them."""
    copy = os.path.join(scratch, os.path.basename(vis))
    shutil.rmtree(copy, ignore_errors=True)
    shutil.copytree(vis, copy, symlinks=True)
    tb.open(copy, nomodify=False)
    if "MODEL_DATA" not in tb.colnames():
        tb.addcols({"MODEL_DATA": tb.getcoldesc("DATA")})
    shape = tb.getcell("DATA", 0).shape
    tb.putcol("MODEL_DATA", np.zeros(shape + (tb.nrows(),), dtype=np.complex64))
    tb.close()
    out = os.path.join(scratch, "model_" + os.path.basename(vis))
    shutil.rmtree(out, ignore_errors=True)
    mstransform(vis=copy, outputvis=out, datacolumn="model")
    tb.open(out)
    data = tb.getcol("DATA")
    tb.close()
    shutil.rmtree(out, ignore_errors=True)
    shutil.rmtree(copy, ignore_errors=True)
    return data


def column(vis, name):
    tb.open(vis)
    values = tb.getcol(name) if name in tb.colnames() else None
    tb.close()
    return values


def nrms(a, b):
    return float(
        np.sqrt(np.mean(np.abs(a - b) ** 2))
        / max(np.sqrt(np.mean(np.abs(b) ** 2)), 1e-30)
    )


def main():
    root = sys.argv[1]
    wanted = set(sys.argv[2:])
    report = {}
    scratch = tempfile.mkdtemp(prefix="verify-")
    for case in sorted(os.listdir(os.path.join(root, "cases"))):
        if wanted and case not in wanted:
            continue
        folder = os.path.join(root, "cases", case)
        visdir = os.path.join(folder, "casa", "vis")
        if not os.path.isdir(visdir):
            continue
        for ms in sorted(os.listdir(visdir)):
            row = {}
            vis = {
                impl: os.path.join(folder, impl, "vis", ms)
                for impl in ("casa", "native")
            }
            for impl, path in vis.items():
                found = model_record(path)
                row[impl] = {
                    "pointers": found["pointers"],
                    "records": {
                        key: {"top": list(rec), "entries": entries(rec)}
                        for key, rec in found["records"].items()
                    },
                    "values": {
                        key: plain(rec["ft_0"]["container"])
                        for key, rec in found["records"].items()
                        if "ft_0" in rec
                    },
                    "model_data_column": column(path, "MODEL_DATA") is not None,
                }
            has = [bool(row[impl]["records"]) for impl in ("casa", "native")]
            row["both_virtual"] = all(has)
            if any(has):
                try:
                    casa = predicted(vis["casa"], scratch)
                    native = predicted(vis["native"], scratch)
                    row["nrms_native_vs_casa_virtual"] = nrms(native, casa)
                    row["max_abs"] = [
                        float(np.abs(casa).max()),
                        float(np.abs(native).max()),
                    ]
                    own = column(vis["native"], "MODEL_DATA")
                    if own is not None and has[1]:
                        row["nrms_native_virtual_vs_native_model_data"] = nrms(
                            native, own
                        )
                except Exception as error:  # noqa: BLE001 - CASA raises several types; report, do not stop
                    row["prediction_error"] = repr(error)
            report[case + "/" + ms] = row
            print(
                "case",
                case,
                ms,
                json.dumps(
                    {
                        k: v
                        for k, v in row.items()
                        if k.startswith("nrms")
                        or k in ("both_virtual", "prediction_error")
                    }
                ),
                flush=True,
            )
    shutil.rmtree(scratch, ignore_errors=True)
    with open(os.path.join(root, "virtual_verify.json"), "w") as handle:
        json.dump(report, handle, indent=1, default=str)
    print("REPORT", os.path.join(root, "virtual_verify.json"))


if __name__ == "__main__":
    main()
