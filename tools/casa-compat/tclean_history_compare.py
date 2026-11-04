#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the history and metadata tclean leaves in its images with CASA's
(issue 1068).

Run with CASA's Python, on a data root as `tclean_case_matrix.py` describes:

    /Applications/CASA.app/Contents/MacOS/python3 -I \
        tools/casa-compat/tclean_history_compare.py \
        --data-root <root> --casars-python <venv with the wheel> \
        --imager-binary <binary> --output <new dir>

For each product of each case it compares the object name, image type, the
`miscinfo` keys `INSTRUME`, `distance`, `mpiprocs` and `chnchnks`, each history
line of a parameter in CASA's signature by what it means and in CASA's order,
and each `logtable` row's `PRIORITY` and `LOCATION`, with `TIME` in MJD seconds.
A product on one side only is a difference. `memreq` and `memavail` describe
CASA's run, and the version line names each implementation.

Its `casa-then-native` case has casa-rs restart on an image CASA wrote, and reads
the result back with CASA.
"""
import argparse
import ast
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tclean_compare import run_native  # noqa: E402

tb = None
LINE = "refim_point_withline.ms"
COMMON = {"imsize": 100, "cell": "8.0arcsec", "gain": 0.2, "spw": "0:0~7"}
CASES = {
    "mfs": [dict(COMMON, niter=10)],
    "mtmfs": [dict(COMMON, niter=10, deconvolver="mtmfs")],
    "cube": [dict(COMMON, niter=10, specmode="cube", spw="0", nchan=3, start=8, width=1)],
    "dirty": [dict(COMMON, niter=0)],
    "restart": [dict(COMMON, niter=5), dict(COMMON, niter=10)],
    "mask": [dict(COMMON, niter=5, usemask="user", mask="box[[40pix,40pix],[60pix,60pix]]",
                  threshold="1mJy", weighting="briggs", robust=0.5, uvtaper=["10arcsec"])],
}


def rows(path):
    """The `logtable`'s rows, as CASA's table tool reads them."""
    table = tb()
    table.open(str(path / "logtable"))
    try:
        return [
            {name: table.getcell(name, row) for name in ("TIME", "PRIORITY", "LOCATION", "MESSAGE")}
            for row in range(table.nrows())
        ]
    finally:
        table.close()


def describe(reader, path):
    reader.open(str(path))
    try:
        summary = reader.summary(list=False, verbose=False)["messages"][0]
        misc = reader.miscinfo()
        history = list(reader.history(list=False))
    finally:
        reader.close()
    labels = {}
    for line in summary.split("\n"):
        for label in ("Object name", "Image quantity"):
            if line.startswith(label):
                labels[label] = line.split(":", 1)[1].strip()
    return {
        "labels": labels,
        "misc": {key: misc.get(key) for key in ("INSTRUME", "distance", "mpiprocs", "chnchnks")},
        "history": history,
        "rows": rows(path),
    }


def canonical(text):
    """A history value as it means: the contract gives a scalar as a list of
    one, a number as text, and a quantity as its shortest number in Jy."""
    try:
        value = ast.literal_eval(text)
    except (ValueError, SyntaxError):
        return text
    while isinstance(value, list) and len(value) == 1:
        value = value[0]
    if isinstance(value, list):
        return [canonical(repr(item)) for item in value]
    if isinstance(value, str):
        match = re.fullmatch(r"([0-9.eE+-]+)([A-Za-z]*)", value)
        if match:
            try:
                number = float(match.group(1))
            except ValueError:
                return value
            scale, unit = (1e-3, "Jy") if match.group(2) == "mJy" else (1.0, match.group(2))
            return (round(number * scale, 12), unit)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return (round(float(value), 12), "")
    return value


def split(lines):
    """The history's parameters by name, per call: `taskname=` opens a call."""
    calls = []
    for line in lines:
        if line.startswith("taskname="):
            calls.append({"taskname": line})
        elif " = " in line and calls:
            name, value = line.split(" = ", 1)
            calls[-1][name.strip()] = value
    return calls


def row_differences(product, casa, native):
    found = []
    if len(native) != len(casa) and not native:
        found.append(f"{product}: no logtable rows")
    for row in native:
        if row["PRIORITY"] != "INFO" or row["LOCATION"] != "::tclean":
            found.append(f"{product}: row {row['PRIORITY']} {row['LOCATION']}")
            break
    # This century, in MJD seconds; a row's time doesn't move on later calls.
    times = [row["TIME"] for row in native]
    if any(not 4.5e9 < time < 6.0e9 for time in times) or times != sorted(times):
        found.append(f"{product}: TIME {times[:2]} is not ascending MJD seconds")
    return found


def casa_then_native(args, casatasks, reader, vis):
    """CASA's image, restarted by casa-rs, read back by CASA."""
    folder = args.output / "casa-then-native"
    (folder / "run").mkdir(parents=True)
    values = dict(COMMON, niter=5, vis=vis, imagename="run/im")
    previous = os.getcwd()
    os.chdir(folder)
    try:
        casatasks.tclean(**values)
    finally:
        os.chdir(previous)
    before = {path.name: rows(path) for path in (folder / "run").iterdir()
              if path.name.startswith("im.") and path.is_dir()}
    reply = run_native(args.casars_python, args.imager_binary,
                       dict(values, niter=10, backend="cpu", gridprecision="f64"), folder)
    if "result" not in reply:
        return [f"casa-rs refused the restart: {json.dumps(reply)[:400]}"]
    found = []
    for name, earlier in before.items():
        path = folder / "run" / name
        now = rows(path)
        if now[: len(earlier)] != earlier:
            found.append(f"{name}: CASA's rows changed")
        added = now[len(earlier):]
        if not added or added[0]["MESSAGE"] != "taskname=tclean":
            found.append(f"{name}: no history added: {len(earlier)} then {len(now)} rows")
        history = describe(reader, path)["history"]
        if len(history) != len(now):
            found.append(f"{name}: ia.history reads {len(history)} of {len(now)} rows")
    return found


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--casars-python", type=Path, required=True)
    parser.add_argument("--imager-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    global tb
    import casatasks
    from casatools import image, table as tb

    args.output.mkdir(parents=True)
    casatasks.casalog.setlogfile(str(args.output / "casa.log"))
    reader = image()
    vis = str(args.data_root / "unittest/tclean" / LINE)
    summary = {}
    for case, calls in CASES.items():
        folders = {side: args.output / side for side in ("casa", "native")}
        for folder in folders.values():
            (folder / case).mkdir(parents=True)
        for call in calls:
            values = dict(call, vis=vis, imagename=f"{case}/im")
            previous = os.getcwd()
            os.chdir(folders["casa"])
            try:
                casatasks.tclean(**values)
            finally:
                os.chdir(previous)
            reply = run_native(args.casars_python, args.imager_binary,
                               dict(values, backend="cpu", gridprecision="f64"), folders["native"])
            if "result" not in reply:
                raise RuntimeError(f"{case}: {json.dumps(reply)[:1500]}")
        found = []
        products = sorted(path.name for path in (folders["casa"] / case).iterdir()
                          if path.is_dir() and path.name.startswith("im."))
        for product in products:
            if not (folders["native"] / case / product).exists():
                found.append(f"{product}: only on CASA")
                continue
            casa = describe(reader, folders["casa"] / case / product)
            native = describe(reader, folders["native"] / case / product)
            for key in ("labels", "misc"):
                if casa[key] != native[key]:
                    found.append(f"{product} {key}: {casa[key]} against {native[key]}")
            found.extend(row_differences(product, casa["rows"], native["rows"]))
            theirs, ours = split(casa["history"]), split(native["history"])
            if len(theirs) != len(ours):
                found.append(f"{product}: {len(theirs)} calls against {len(ours)}")
            for one, other in zip(theirs, ours):
                order = [name for name in other if name in one]
                if order != list(one):
                    found.append(f"{product}: parameters in another order")
                for name, value in one.items():
                    if name == "vis":
                        continue
                    if name not in other:
                        found.append(f"{product} {name}: no line")
                    elif canonical(other[name]) != canonical(value):
                        found.append(f"{product} {name}: {value} against {other[name]}")
        ours_only = sorted(path.name for path in (folders["native"] / case).iterdir()
                           if path.is_dir() and path.name.startswith("im.")
                           and path.name not in products)
        found.extend(f"{product}: only on casa-rs" for product in ours_only)
        summary[case] = {"passed": not found, "differences": found}
        print(case, "passed" if not found else "DIFFERS", flush=True)
        for line in found[:40]:
            print("   ", line[:240], flush=True)
    found = casa_then_native(args, casatasks, reader, vis)
    summary["casa-then-native"] = {"passed": not found, "differences": found}
    print("casa-then-native", "passed" if not found else "DIFFERS", flush=True)
    for line in found[:40]:
        print("   ", line[:240], flush=True)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
