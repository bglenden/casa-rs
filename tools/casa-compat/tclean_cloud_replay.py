#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Replay the six saved tclean handoff comparisons from an extracted archive.

This is an evidence driver, not a runtime dependency. It uses the existing
T1.5 product comparator from the separately archived harness and records
additional finite-pixel, metadata and selected-return checks. The native extra
summary sidecar remains a separately reported, undecided file difference.
"""

import argparse
import hashlib
import importlib.util
import json
import math
import re
import subprocess
import sys
from pathlib import Path

CASES = {
    "point-mfs-hogbom": ("refim", "refim_point.ms"),
    "point-mfs-clark-briggs": ("refim", "refim_point.ms"),
    "point-mfs-multiscale-uniform": ("refim", "refim_point.ms"),
    "point-cube-briggs": ("refim", "refim_point.ms"),
    "squint-wproject-hogbom": ("kernels", "vla_wideband_2ptg_w_squint.ms"),
    "withline-cube-wproject-hogbom": ("kernels", "refim_point_withline.ms"),
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--archive-root",
        type=Path,
        required=True,
        help="extracted tclean-handoff-20261101.tar",
    )
    parser.add_argument(
        "--harness-root",
        type=Path,
        required=True,
        help="directory with refim/t15.py, kernels/t15.py and IF-7/catalog_request.py",
    )
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument(
        "--revision", required=True, help="40-character native source revision"
    )
    parser.add_argument(
        "--expected-binary-sha256", help="optional exact executable hash"
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="new directory; never overwritten"
    )
    parser.add_argument(
        "--case", choices=CASES, action="append", help="repeat; defaults to all six"
    )
    return parser.parse_args()


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def decode(value):
    if value["kind"] == "dict":
        return {entry["key"]: decode(entry["value"]) for entry in value["entries"]}
    if value["kind"] == "list":
        return [decode(item) for item in value["items"]]
    return float(value["value"]) if value["kind"] == "float" else value.get("value")


def load_harness(root, group):
    sys.path.insert(0, str(root / "IF-7"))
    source = root / group / "t15.py"
    spec = importlib.util.spec_from_file_location("tclean_handoff_" + group, source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def image_metadata(path, casatools):
    image = casatools.image()
    image.open(str(path))
    try:
        coords = image.coordsys()
        try:
            record = coords.torecord()
            rest = next(
                value["restfreq"]
                for key, value in record.items()
                if key.startswith("spectral")
            )
            return {
                "unit": image.brightnessunit(),
                "stokes": list(coords.stokes()),
                "units": list(coords.units()),
                "codes": list(coords.referencecode()),
                "referencevalue": coords.referencevalue()["numeric"].tolist(),
                "referencepixel": coords.referencepixel()["numeric"].tolist(),
                "increment": coords.increment()["numeric"].tolist(),
                "restfreq": rest,
            }
        finally:
            coords.done()
    finally:
        image.done()


def metadata_checks(left, right, numpy):
    checks = {
        key: left[key] == right[key] for key in ("unit", "stokes", "units", "codes")
    }
    for key, rtol, atol in (
        ("referencevalue", 2e-10, 2e-10),
        ("referencepixel", 0, 1e-8),
        ("increment", 2e-10, 2e-10),
        ("restfreq", 2e-10, 2e-10),
    ):
        checks[key] = bool(numpy.allclose(left[key], right[key], rtol=rtol, atol=atol))
    return checks


def reference_version(reference, casatools):
    table = casatools.table()
    table.open(str(reference / "image.residual" / "logtable"))
    try:
        messages = table.getcol("MESSAGE").tolist()
    finally:
        table.close()
    versions = [value for value in messages if "version:" in value.lower()]
    if not any("6.7.6-14" in value for value in versions):
        raise ValueError(f"unverified reference CASA version: {reference}")
    return versions


def compare_products(harness, native, reference, casatools, numpy):
    products = harness.compare_products(str(native / "image"), str(reference / "image"))
    sumwt_metric = harness.sumwt_metric(products)
    product_checks = {}
    for suffix, entry in products.items():
        if entry["status"] != "ok":
            continue
        left = native / ("image" + suffix)
        right = reference / ("image" + suffix)
        a, am = harness.read(str(left))[:2]
        b, bm = harness.read(str(right))[:2]
        valid = am & bm
        finite = bool(
            valid.any() and numpy.isfinite(a[am]).all() and numpy.isfinite(b[bm]).all()
        )
        metadata = metadata_checks(
            image_metadata(left, casatools), image_metadata(right, casatools), numpy
        )
        metric = sumwt_metric if suffix.startswith(".sumwt") else entry["nrms"]
        limit = 5e-5 if suffix.startswith(".sumwt") else 1e-3
        product_checks[suffix] = {
            "finite_valid_pixels": finite,
            "mask_mismatch": entry["mask_mismatch"],
            "numeric_metric": metric,
            "numeric_limit": limit,
            "numeric_passed": metric is not None
            and math.isfinite(metric)
            and metric <= limit,
            "beam_axes_passed": entry.get("beam_relative", 0.0) <= 1e-3,
            "metadata": metadata,
        }
    image_inventory = all(
        entry["status"] == "ok"
        for suffix, entry in products.items()
        if suffix != ".summary.json"
    )
    required = {".image", ".mask", ".model", ".pb", ".psf", ".residual", ".sumwt"}
    scoped_pass = (
        image_inventory
        and required.issubset(product_checks)
        and all(
            check["finite_valid_pixels"]
            and check["mask_mismatch"] == 0
            and check["numeric_passed"]
            and check["beam_axes_passed"]
            and all(check["metadata"].values())
            for check in product_checks.values()
        )
    )
    return {
        "raw_framework_comparison": products,
        "additional_checks": product_checks,
        "sumwt_metric": sumwt_metric,
        "image_directory_inventory_matches": image_inventory,
        "extra_sidecar": products.get(".summary.json"),
        "beam_position_angle_checked": False,
        "scoped_products_passed": scoped_pass,
    }


def run_case(args, name, harness, casatools, numpy):
    group, dataset = CASES[name]
    reference = args.archive_root / "t15" / group / "casa" / name
    measurement_set = args.archive_root / "inputs" / dataset
    if not reference.is_dir() or not measurement_set.is_dir():
        raise FileNotFoundError(f"missing archived reference or input for {name}")
    source = reference / "request.json"
    values = json.loads(source.read_text())
    if values.get("datacolumn") != "data" or Path(values["vis"]).name != dataset:
        raise ValueError(f"unexpected saved CASA input in {source}")
    folder = args.output / name
    folder.mkdir()
    values.update(
        vis=str(measurement_set),
        imagename=str(folder / "image"),
        backend="cpu",
        gridprecision="f64",
    )
    request = folder / "request.json"
    request.write_text(
        json.dumps({"surface": "tclean", "values": values}, indent=2) + "\n"
    )
    run = subprocess.run(
        [str(args.binary), "--json-run", str(request)],
        cwd=folder,
        capture_output=True,
        text=True,
        timeout=120,
    )
    (folder / "stdout.json").write_text(run.stdout)
    (folder / "stderr.log").write_text(run.stderr)
    row = {
        "exit": run.returncode,
        "reference_history": reference_version(reference, casatools),
        "reference_request_sha256": sha256(source),
        "parameters": values,
    }
    if run.returncode:
        row["error"] = run.stdout + run.stderr
        return row
    actual = decode(json.loads(run.stdout)["value"])
    expected = json.loads((reference / "result.json").read_text())["task_result"]
    row["comparison"] = compare_products(harness, folder, reference, casatools, numpy)
    row["selected_counts"] = {
        key: {
            "native": actual[key],
            "casa": expected[key],
            "passed": actual[key] == expected[key],
        }
        for key in ("iterdone", "nmajordone", "stopcode")
    }
    row["selected_scalars"] = {
        key: {
            "native": actual[key],
            "casa": expected[key],
            "passed": abs(actual[key] - expected[key])
            <= 1e-8 + 1e-6 * abs(expected[key]),
        }
        for key in ("maxpsfsidelobe", "cyclethreshold")
    }
    summary = folder / "image.summary.json"
    row["execution"] = json.loads(summary.read_text()) if summary.is_file() else None
    return row


def main():
    args = parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        raise ValueError("--revision must be a full 40-character lowercase commit hash")
    args.archive_root = args.archive_root.resolve(strict=True)
    args.harness_root = args.harness_root.resolve(strict=True)
    args.binary = args.binary.resolve(strict=True)
    args.output = args.output.resolve()
    binary_hash = sha256(args.binary)
    if args.expected_binary_sha256 and binary_hash != args.expected_binary_sha256:
        raise ValueError("executable SHA256 does not match --expected-binary-sha256")
    for group in {CASES[name][0] for name in (args.case or CASES)}:
        if not (args.harness_root / group / "t15.py").is_file():
            raise FileNotFoundError(f"missing {group}/t15.py")
    if not (args.harness_root / "IF-7" / "catalog_request.py").is_file():
        raise FileNotFoundError("missing IF-7/catalog_request.py")
    from casaconfig import config

    config.measures_auto_update = False
    config.data_auto_update = False
    import casatasks
    import casatools
    import numpy

    if (
        casatasks.version_string() != "6.7.6.14"
        or casatools.version_string() != "6.7.6-14"
    ):
        raise ValueError(
            "this comparison requires CASA tasks 6.7.6.14 / tools 6.7.6-14"
        )
    args.output.mkdir(parents=True, exist_ok=False)
    record = {
        "native_revision": args.revision,
        "binary_sha256": binary_hash,
        "oracle": {
            "casatasks": casatasks.version_string(),
            "casatools": casatools.version_string(),
        },
        "scope": "CPU/f64 saved CASA requests; scoped products/selected returns only. "
        "Whole-task, full file inventory and beam position angle are not established.",
        "cases": {},
    }
    for name in args.case or CASES:
        group = CASES[name][0]
        harness = load_harness(args.harness_root, group)
        row = run_case(args, name, harness, casatools, numpy)
        record["cases"][name] = row
        (args.output / "receipt.json").write_text(json.dumps(record, indent=2) + "\n")
        counts = row.get("selected_counts", {})
        scalars = row.get("selected_scalars", {})
        print(
            name,
            "exit",
            row["exit"],
            "products",
            row.get("comparison", {}).get("scoped_products_passed"),
            "counts",
            bool(counts) and all(x["passed"] for x in counts.values()),
            "scalars",
            bool(scalars) and all(x["passed"] for x in scalars.values()),
            flush=True,
        )
    passed = all(
        row["exit"] == 0
        and row["comparison"]["scoped_products_passed"]
        and all(x["passed"] for x in row["selected_counts"].values())
        and all(x["passed"] for x in row["selected_scalars"].values())
        for row in record["cases"].values()
    )
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
