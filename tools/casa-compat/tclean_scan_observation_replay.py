#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare casa-rs's public Python tclean scan/observation selection with CASA.

Evidence driver for #980, not a runtime dependency. Run it with CASA 6.7.6.14's
Python (`-I`) against an extracted `scan-observation-engine-20261101.tar`: its
`input.ms` and `input.fixture.json`, CASA's science products under
`oracle-science/` and CASA's grammar receipts under `oracle-grammar-*/`.

Each case calls `casars.tasks.tclean` with keyword arguments, as a user does,
in a separate interpreter that has the casars package (`--casars-python`), with
`CASARS_IMAGER_BIN` naming the task binary. Science cases compare every image
product with CASA's: exact product inventory, masks, finite support, NRMS at
most 1e-3 (`.sumwt` within 5e-5 of CASA's peak), WCS, rest frequency, units,
Stokes and frames, beam axes within 1e-3 and position angle within 0.01 deg,
exact iterdone, nmajordone and stopcode, and maxpsfsidelobe and cyclethreshold
within 1e-6 relative plus 1e-8 absolute. Grammar cases check that each selector
CASA's tclean refused raises in casa-rs too and writes no image, and that each
one CASA accepted writes CASA's product inventory.
"""

import argparse
import hashlib
import importlib.util
import json
import math
import os
import platform
import subprocess
import time
from pathlib import Path

SCIENCE = {
    "all_true": {"selectdata": True},
    "all_false": {"selectdata": False},
    "scan_one": {"scan": "1"},
    "scan_two_selectdata_false": {"scan": "2", "selectdata": False},
    "observation_zero": {"observation": "0"},
    "observation_one_selectdata_false": {"observation": "1", "selectdata": False},
    "scan_one_observation_one": {"scan": "1", "observation": "1"},
    "scan_two_observation_zero_false": {"scan": "2", "observation": "0", "selectdata": False},
    "scan_list_observation_range": {"scan": "1,2", "observation": "0~1"},
    "scan_range_observation_one_false": {"scan": "1~2", "observation": "1", "selectdata": False},
    "duplicate_scan_observation_one": {"scan": "1,1", "observation": "1"},
    "wide_range_false": {"scan": "0~10", "observation": "0~1", "selectdata": False},
    "clean_conjunction": {"scan": "1", "observation": "1", "niter": 10},
}
GRAMMAR = ("oracle-grammar-task", "oracle-grammar-sentinels", "oracle-grammar-lexical")

# Runs in the casars interpreter: tclean's keyword arguments on stdin, its
# record or its refusal on stdout.
CHILD = r"""
import json, sys
values = json.load(sys.stdin)
def plain(value):
    if isinstance(value, dict):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    if hasattr(value, "tolist"):
        return value.tolist()
    return value
from casars import tasks
try:
    record = tasks.tclean(**values)
except Exception as error:
    print(json.dumps({"refused": {"type": type(error).__name__, "message": str(error)}}))
else:
    print(json.dumps({"result": plain(record)}))
"""


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--casars-python", type=Path, required=True)
    parser.add_argument("--imager-binary", type=Path, required=True)
    parser.add_argument("--revision", required=True, help="40-character source revision")
    parser.add_argument("--backend", default="cpu", choices=("cpu", "gpu"))
    parser.add_argument("--precision", default="f64", choices=("f32", "f64"))
    parser.add_argument("--output", type=Path, required=True, help="new directory")
    parser.add_argument("--case", action="append", help="science case; repeat")
    parser.add_argument("--skip-grammar", action="store_true")
    return parser.parse_args()


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def load_reader():
    source = Path(__file__).resolve().parents[2] / "tools/imaging/t15_imager_additions.py"
    spec = importlib.util.spec_from_file_location("t15_imager_additions", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def members(folder):
    return sorted(
        path.name for path in folder.iterdir() if path.name.startswith("image.") and path.is_dir()
    )


def metadata(path, casatools):
    image = casatools.image()
    image.open(str(path))
    try:
        coords = image.coordsys()
        try:
            record = coords.torecord()
            return {
                "unit": image.brightnessunit(),
                "stokes": list(coords.stokes()),
                "units": list(coords.units()),
                "codes": list(coords.referencecode()),
                "referencevalue": coords.referencevalue()["numeric"].tolist(),
                "referencepixel": coords.referencepixel()["numeric"].tolist(),
                "increment": coords.increment()["numeric"].tolist(),
                "restfreq": next(
                    value["restfreq"] for key, value in record.items() if key.startswith("spectral")
                ),
            }
        finally:
            coords.done()
    finally:
        image.done()


def compare_product(native, casa, name, reader, casatools, numpy):
    a, am, ab = reader.read(str(native))
    b, bm, bb = reader.read(str(casa))
    entry = {"shape_equal": a.shape == b.shape}
    if not entry["shape_equal"]:
        return entry | {"passed": False}
    valid = am & bm
    finite = bool(valid.any() and numpy.isfinite(a[am]).all() and numpy.isfinite(b[bm]).all())
    entry.update(mask_mismatch=int((am != bm).sum()), finite=finite)
    if not finite:
        return entry | {"passed": False}
    av, bv = a[valid], b[valid]
    power = float(numpy.sum(bv * bv))
    error = float(numpy.sum((av - bv) ** 2))
    entry["nrms"] = math.sqrt(error / power) if power else (0.0 if error == 0 else math.inf)
    if name == "image.sumwt":
        numeric = bool(numpy.max(numpy.abs(av - bv)) <= 5e-5 * numpy.max(numpy.abs(bv)))
    else:
        numeric = entry["nrms"] <= 1e-3
    left, right = metadata(native, casatools), metadata(casa, casatools)
    checks = {key: left[key] == right[key] for key in ("unit", "stokes", "units", "codes")}
    for key, rtol, atol in (
        ("referencevalue", 2e-10, 2e-10),
        ("referencepixel", 0, 1e-8),
        ("increment", 2e-10, 2e-10),
        ("restfreq", 2e-10, 2e-10),
    ):
        checks[key] = bool(numpy.allclose(left[key], right[key], rtol=rtol, atol=atol))
    beam = True
    if ab or bb:
        beam = bool(ab and bb) and all(abs(x - y) <= 1e-3 * abs(y) for x, y in zip(ab[:2], bb[:2]))
        if beam and bb[0] / bb[1] > 1.001:
            beam = abs((ab[2] - bb[2] + 90) % 180 - 90) <= 0.01
    entry.update(numeric_passed=numeric, metadata=checks, beam=[ab, bb], beam_passed=beam)
    entry["passed"] = bool(numeric and all(checks.values()) and beam and entry["mask_mismatch"] == 0)
    return entry


def run_native(args, values, folder):
    environment = {
        name: value for name, value in os.environ.items() if name != "CASA_RS_IMAGER_RUN_SUMMARY"
    }
    environment["CASARS_IMAGER_BIN"] = str(args.imager_binary)
    started = time.monotonic()
    run = subprocess.run(
        [str(args.casars_python), "-I", "-c", CHILD],
        input=json.dumps(values),
        cwd=folder,
        env=environment,
        capture_output=True,
        text=True,
        timeout=600,
    )
    (folder / "stderr.log").write_text(run.stderr)
    reply = json.loads(run.stdout.strip().splitlines()[-1]) if run.stdout.strip() else {}
    reply["exit_code"] = run.returncode
    reply["driver_elapsed_seconds"] = time.monotonic() - started
    return reply


def science_case(args, name, common, reference, tools):
    reader, casatools, numpy = tools
    folder = args.output / "science" / name
    folder.mkdir(parents=True)
    values = dict(common, imagename=str(folder / "image"), **SCIENCE[name])
    values.update(backend=args.backend, gridprecision=args.precision)
    row = {"parameters": values} | run_native(args, values, folder)
    casa_folder = args.archive_root / "oracle-science" / name
    if "result" not in row:
        return row | {"passed": False}
    inventory = members(folder) == members(casa_folder)
    row["products"] = {
        product: compare_product(folder / product, casa_folder / product, product, *tools)
        for product in members(casa_folder)
        if (folder / product).is_dir()
    }
    row["inventory"] = {"native": members(folder), "casa": members(casa_folder), "passed": inventory}
    row["other_files"] = sorted(path.name for path in folder.iterdir() if not path.is_dir())
    expected = reference["cases"][name]["result"]
    row["exact_integers"] = {
        key: row["result"][key] == expected[key] for key in ("iterdone", "nmajordone", "stopcode")
    }
    row["scalars"] = {
        key: {
            "native": row["result"][key],
            "casa": expected[key],
            "passed": abs(row["result"][key] - expected[key]) <= 1e-8 + 1e-6 * abs(expected[key]),
        }
        for key in ("maxpsfsidelobe", "cyclethreshold")
    }
    row["passed"] = bool(
        inventory
        and all(entry["passed"] for entry in row["products"].values())
        and all(row["exact_integers"].values())
        and all(entry["passed"] for entry in row["scalars"].values())
    )
    return row


def grammar_case(args, receipt, name, case, common):
    folder = args.output / "grammar" / f"{receipt}-{name}"
    folder.mkdir(parents=True)
    selectors = {
        key: case["parameters"][key]
        for key in ("scan", "observation", "selectdata")
        if key in case["parameters"]
    }
    values = dict(common, imagename=str(folder / "image"), **selectors)
    values.update(backend=args.backend, gridprecision=args.precision)
    row = {"selectors": selectors, "casa_succeeded": case["succeeded"]}
    row |= run_native(args, values, folder)
    native = members(folder)
    if case["succeeded"]:
        row["passed"] = "result" in row and native == sorted(case["image_products"])
    else:
        row["passed"] = "refused" in row and not native
    row["native_products"] = native
    return row


def main():
    args = parse_args()
    import casatasks
    import casatools
    import numpy

    assert casatasks.version_string() == "6.7.6.14", casatasks.version_string()
    assert casatools.version_string() == "6.7.6-14", casatools.version_string()
    args.archive_root = args.archive_root.resolve(strict=True)
    args.output.mkdir(parents=True)
    fixture = json.loads((args.archive_root / "input.fixture.json").read_text())
    common = dict(
        vis=str(args.archive_root / "input.ms"),
        datacolumn="data",
        imsize=fixture["imsize"],
        cell=fixture["cell"],
        phasecenter="J2000 " + " ".join(f"{value}rad" for value in fixture["phasecenter_rad"]),
        weighting="natural",
        niter=0,
        pblimit=0.2,
        threshold="0Jy",
        gain=0.1,
    )
    reference = json.loads((args.archive_root / "oracle-science/receipt.json").read_text())
    tools = (load_reader(), casatools, numpy)
    receipt = {
        "driver": "tools/casa-compat/tclean_scan_observation_replay.py",
        "casa_tasks": casatasks.version_string(),
        "casa_tools": casatools.version_string(),
        "native_revision": args.revision,
        "imager_binary_sha256": sha256(args.imager_binary),
        "casars_python": str(args.casars_python),
        "backend": args.backend,
        "precision": args.precision,
        "host": platform.platform(),
        "science": {},
        "grammar": {},
    }
    write = lambda: (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    for name in args.case or SCIENCE:
        row = science_case(args, name, common, reference, tools)
        receipt["science"][name] = row
        write()
        print("science", name, row.get("passed"), flush=True)
    if not args.skip_grammar:
        for source in GRAMMAR:
            cases = json.loads((args.archive_root / source / "receipt.json").read_text())["cases"]
            for name, case in cases.items():
                row = grammar_case(args, source, name, case, common)
                receipt["grammar"][f"{source}/{name}"] = row
                write()
                print("grammar", source, name, row["passed"], flush=True)
    rows = [*receipt["science"].values(), *receipt["grammar"].values()]
    receipt["passed"] = sum(bool(row.get("passed")) for row in rows)
    receipt["total"] = len(rows)
    write()
    print(f"{receipt['passed']}/{receipt['total']} passed", flush=True)
    return 0 if receipt["passed"] == receipt["total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
