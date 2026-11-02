#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run a matrix of tclean calls through CASA and through casa-rs, and compare.

Evidence driver for #980, not a runtime dependency. Run it with CASA 6.7.6.14's
Python (`-I`). A case file names input MeasurementSets (relative to
`--data-root`), parameters common to every case, and the cases:

    {"inputs": {"point": "unittest/tclean/refim_point.ms"},
     "common": {"imsize": 100, "cell": "8.0arcsec", "datacolumn": "data"},
     "cases": {"superuniform": {"vis": "point",
                                "params": {"weighting": "superuniform"}}}}

Each input is copied once into the output directory, and both CASA's
`casatasks.tclean` and casa-rs's public Python `casars.tasks.tclean` (in the
`--casars-python` interpreter, with `CASARS_IMAGER_BIN` naming the binary)
image it with the same keyword arguments into separate folders. A case CASA
refuses passes when casa-rs refuses it too and writes no image. A case CASA
images passes when casa-rs writes the same image products, each within the
T1.5 limits (`tclean_compare.compare_product`), with exact iterdone,
nmajordone and stopcode, and maxpsfsidelobe and cyclethreshold within 1e-6
relative plus 1e-8 absolute. A case may list `"skip_scalars": true` when its
return values are not part of what it checks, with the reason in `"note"`.
"""

import argparse
import json
import platform
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tclean_compare import compare_product, load_reader, members, run_native, sha256  # noqa: E402

SCALARS = ("maxpsfsidelobe", "cyclethreshold")
COUNTS = ("iterdone", "nmajordone", "stopcode")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", type=Path, required=True, help="case-matrix JSON")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--casars-python", type=Path, required=True)
    parser.add_argument("--imager-binary", type=Path, required=True)
    parser.add_argument("--revision", required=True, help="40-character source revision")
    parser.add_argument("--backend", default="cpu", choices=("cpu", "gpu"))
    parser.add_argument("--precision", default="f64", choices=("f32", "f64"))
    parser.add_argument("--output", type=Path, required=True, help="new directory")
    parser.add_argument("--case", action="append", help="run only these cases")
    return parser.parse_args()


def plain(value):
    if isinstance(value, dict):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    if hasattr(value, "tolist"):
        return value.tolist()
    return value


def run_casa(casatasks, values):
    started = time.monotonic()
    try:
        record = casatasks.tclean(**values)
    except Exception as error:  # CASA's refusals are exceptions of several types
        reply = {"refused": {"type": type(error).__name__, "message": str(error)}}
    else:
        reply = {"result": plain(record)}
    reply["driver_elapsed_seconds"] = time.monotonic() - started
    return reply


def compare(case, casa_folder, native_folder, casa, native, tools):
    row = {}
    if "refused" in casa:
        row["passed"] = "refused" in native and not members(native_folder)
        row["native_products"] = members(native_folder)
        return row
    if "result" not in native:
        return {"passed": False}
    row["inventory"] = {
        "native": members(native_folder),
        "casa": members(casa_folder),
        "passed": members(native_folder) == members(casa_folder),
    }
    row["products"] = {
        product: compare_product(native_folder / product, casa_folder / product, product, *tools)
        for product in members(casa_folder)
        if (native_folder / product).is_dir()
    }
    row["other_files"] = sorted(path.name for path in native_folder.iterdir() if not path.is_dir())
    expected, actual = casa["result"], native["result"]
    row["counts"] = {key: actual.get(key) == expected.get(key) for key in COUNTS}
    row["scalars"] = {
        key: {
            "native": actual.get(key),
            "casa": expected.get(key),
            "passed": actual.get(key) is not None
            and expected.get(key) is not None
            and abs(actual[key] - expected[key]) <= 1e-8 + 1e-6 * abs(expected[key]),
        }
        for key in SCALARS
    }
    scalars = case.get("skip_scalars") or all(entry["passed"] for entry in row["scalars"].values())
    row["passed"] = bool(
        row["inventory"]["passed"]
        and all(entry["passed"] for entry in row["products"].values())
        and all(row["counts"].values())
        and scalars
    )
    return row


def main():
    args = parse_args()
    import casatasks
    import casatools
    import numpy

    assert casatasks.version_string() == "6.7.6.14", casatasks.version_string()
    assert casatools.version_string() == "6.7.6-14", casatools.version_string()
    matrix = json.loads(args.cases.read_text())
    args.output.mkdir(parents=True)
    staged = args.output / "inputs"
    staged.mkdir()
    inputs = {}
    for name, relative in matrix["inputs"].items():
        source = (args.data_root / relative).resolve(strict=True)
        inputs[name] = staged / source.name
        shutil.copytree(source, inputs[name], symlinks=True)
    casatasks.casalog.setlogfile(str(args.output / "casa.log"))
    tools = (load_reader(), casatools, numpy)
    receipt = {
        "driver": "tools/casa-compat/tclean_case_matrix.py",
        "case_file": str(args.cases),
        "case_file_sha256": sha256(args.cases),
        "casa_tasks": casatasks.version_string(),
        "casa_tools": casatools.version_string(),
        "native_revision": args.revision,
        "imager_binary_sha256": sha256(args.imager_binary),
        "backend": args.backend,
        "precision": args.precision,
        "host": platform.platform(),
        "inputs": matrix["inputs"],
        "cases": {},
    }

    def write():
        (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")

    for name, case in matrix["cases"].items():
        if args.case and name not in args.case:
            continue
        values = dict(matrix.get("common", {}), vis=str(inputs[case["vis"]]), **case["params"])
        casa_folder = args.output / "casa" / name
        native_folder = args.output / "native" / name
        casa_folder.mkdir(parents=True)
        native_folder.mkdir(parents=True)
        casa = run_casa(casatasks, dict(values, imagename=str(casa_folder / "image")))
        native_values = dict(values, imagename=str(native_folder / "image"))
        native_values.update(backend=args.backend, gridprecision=args.precision)
        native = run_native(args.casars_python, args.imager_binary, native_values, native_folder)
        row = {"parameters": values, "note": case.get("note"), "casa": casa, "native": native}
        row |= compare(case, casa_folder, native_folder, casa, native, tools)
        receipt["cases"][name] = row
        write()
        print("case", name, row["passed"], flush=True)
    rows = list(receipt["cases"].values())
    receipt["passed"] = sum(bool(row["passed"]) for row in rows)
    receipt["total"] = len(rows)
    write()
    print(f"{receipt['passed']}/{receipt['total']} passed", flush=True)
    return 0 if receipt["passed"] == receipt["total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
