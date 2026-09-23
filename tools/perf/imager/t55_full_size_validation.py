#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""One-shot corrected Q-band validation; invoke each stage under an RSS guard."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

REPO = Path(__file__).resolve().parents[3]
FULL_ROWS = 4_094_064
PRODUCTS = ("image", "residual", "model", "psf", "pb", "mask", "sumwt")


def save(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2)


def preflight(args):
    import numpy as np
    from casatools import table

    tb = table()
    tb.open(str(args.input), nomodify=True)
    try:
        assert tb.nrows() == args.rows
        flags = 0
        for start in range(0, args.rows, 65536):
            count = min(65536, args.rows - start)
            for column in ("FIELD_ID", "DATA_DESC_ID", "OBSERVATION_ID"):
                assert np.all(tb.getcol(column, start, count) == 0), column
            flags += int(np.count_nonzero(tb.getcol("FLAG_ROW", start, count)))
        samples = []
        for row in (0, args.rows // 2, args.rows - 1):
            values = tb.getcell("DATA", row)
            assert values.shape == (2, 512) and np.isfinite(values).all()
            assert tb.getcell("FLAG", row).shape == values.shape
            samples.append({"row": row, "time": float(tb.getcell("TIME", row))})
    finally:
        tb.close()
    tb.open(str(args.input / "SPECTRAL_WINDOW"), nomodify=True)
    try:
        assert tb.nrows() == 1
        frequencies = tb.getcell("CHAN_FREQ", 0).tolist()
        assert frequencies == [44e9 + i * 2e6 for i in range(512)]
    finally:
        tb.close()
    tb.open(str(args.input / "POLARIZATION"), nomodify=True)
    try:
        assert tb.nrows() == 1
        correlations = tb.getcell("CORR_TYPE", 0).tolist()
        assert len(correlations) == 2
    finally:
        tb.close()
        tb.done()
    result = dict(status="passed", input=str(args.input), rows=args.rows,
                  channels=512, correlations=correlations, flagged_rows=flags,
                  samples=samples, first_hz=frequencies[0], last_hz=frequencies[-1],
                  logical_file_bytes=sum(p.stat().st_size for p in args.input.rglob("*")
                                         if p.is_file()),
                  sample_validation="bounded DATA samples; complete scalar-ID/row-flag scan")
    save(args.records / f"{args.label}-input.json", result)
    print(json.dumps(result), flush=True)


def stage_input(args):
    output = args.outputs / args.label
    output.mkdir()
    copied = output / "input.ms"
    print(f"stage=fixture-copy source={args.input} destination={copied}", flush=True)
    started = time.perf_counter()
    shutil.copytree(args.input, copied, ignore=shutil.ignore_patterns("table.lock"))
    save(args.records / f"{args.label}-staging.json",
         dict(input=str(args.input), copy=str(copied), seconds=time.perf_counter() - started,
              timed_boundary="fixture copy excluded from both imaging task timings"))
    return output, copied


def native(args):
    assert args.binary and args.initializer
    assert args.workers in (1, 4)
    output, copied = stage_input(args)
    subprocess.run([str(args.initializer), str(copied)], check=True)
    environment = dict(os.environ, CASA_RS_T55_REAL_MS=str(copied),
                       CASA_RS_T55_ARTIFACT_ROOT=str(output / "products"),
                       CASA_RS_T55_NATIVE_MEMORY_BYTES=str(16 << 30),
                       CASA_RS_T55_PREFLIGHT_ROWS=str(args.rows),
                       CASA_RS_T55_PREFLIGHT_IMAGE_SIZE="512",
                       CASA_RS_T55_PREFLIGHT_WORKERS=str(args.workers),
                       CASA_RS_T55_FULL_WORKERS=str(args.workers),
                       CASA_RS_T55_PUBLICATION_PROBE="1", CASA_RS_PROFILE_CUBE="1")
    test = ("t55_full_dataset_clark_timing" if args.rows == FULL_ROWS
            else "t55_q_band_rebaseline_preflight")
    command = [str(args.binary), f"t55_real_cube::{test}", "--exact", "--ignored",
               "--nocapture", "--test-threads=1"]
    save(args.records / f"{args.label}-binary.json",
         dict(binary=str(args.binary), sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              command=command, workers=args.workers, rows=args.rows,
              native_memory_bytes=16 << 30, source_head=subprocess.check_output(
                  ["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()))
    print(f"stage=native-imaging rows={args.rows} workers={args.workers}", flush=True)
    subprocess.run(command, cwd=REPO, env=environment, check=True)
    result = json.loads((output / "products/summary.json").read_text())
    assert result["rows"] == args.rows and result["image_size"] == 512
    assert result["native_memory_bytes"] == 16 << 30
    assert result["requested_workers"] == args.workers
    assert result["execution_route"] == "native-streaming-cube"
    save(args.records / f"{args.label}-summary.json", result)
    print(json.dumps(result), flush=True)


def casa(args):
    assert args.config and args.casarc
    os.environ["CASARCFILES"] = str(args.casarc)
    from casatasks import tclean, casalog, version_string
    from casatools.utils import utils

    assert float(utils().getrc("system.resources.memory")) == 16384
    config = json.loads(args.config.read_text())
    assert version_string() == config["casa_version"]
    output, copied = stage_input(args)
    os.chdir(output)
    casalog.setlogfile(str(output / "casa-task.log"))
    kwargs = dict(config["casa_kwargs"], vis=str(copied), imagename=str(output / "image"),
                  spw="0", imsize=512, cell="0.35arcsec", nchan=512, start=0, width=1)
    save(args.records / f"{args.label}-request.json", kwargs)
    print(f"stage=casa-imaging rows={args.rows} workers=1", flush=True)
    started = time.perf_counter()
    returned = tclean(**kwargs)
    result = dict(seconds=time.perf_counter() - started, rows=args.rows, channels=512,
                  image_size=512, workers=1, casa_version=version_string(),
                  major_cycles=int(returned["nmajordone"]),
                  minor_iterations=int(returned["iterdone"]),
                  memory_planning="CASA system.resources.memory=16384MiB; external16GiB RSS guard",
                  timed_boundary="tclean including publication; excludes imports, fixture copy, validation")
    assert all((output / ("image." + suffix)).is_dir() for suffix in PRODUCTS)
    save(args.records / f"{args.label}-result.json", result)
    print(json.dumps(result), flush=True)


def compare(args):
    from perf_harness.image_compare import compare_products

    assert args.left and args.right and args.casa_python
    directory = args.records / args.label
    directory.mkdir()
    contract = json.loads((REPO / "tools/perf/imager/workloads/t55-clark-cube-development.json")
                          .read_text())["comparison"]
    result = compare_products(casa_python=str(args.casa_python), cwd=directory,
                              artifact_prefix=directory / "comparison", request={
                                  **contract, "left_prefix": str(args.left),
                                  "right_prefix": str(args.right), "left_label": args.left_label,
                                  "right_label": "CASA serial", "panel_dir": str(directory / "panels"),
                                  "structure_workspace_dir": str(directory / "structure")})
    save(directory / "result.json", result)
    print(json.dumps(dict(status=result["status"],
                          tolerances=result.get("tolerance_evaluation"))), flush=True)
    assert result["status"] == "completed"
    assert result["tolerance_evaluation"]["status"] == "passed"
    assert len(result["products"]) == 7


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("preflight", "native", "casa", "compare"))
    parser.add_argument("--label", required=True)
    parser.add_argument("--records", type=Path, required=True)
    parser.add_argument("--outputs", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=FULL_ROWS)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--initializer", type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--casarc", type=Path)
    parser.add_argument("--casa-python", type=Path)
    parser.add_argument("--left", type=Path)
    parser.add_argument("--right", type=Path)
    parser.add_argument("--left-label", default="casa-rs")
    args = parser.parse_args()
    assert args.rows == FULL_ROWS or (0 < args.rows <= 84240 and args.rows % 351 == 0)
    assert args.label and Path(args.label).name == args.label and args.label not in (".", "..")
    for path in (args.records, args.outputs, args.input):
        assert path.is_absolute()
        assert not any(part in ("private", "tmp", "temp") for part in path.resolve().parts)
    args.records.mkdir(parents=True, exist_ok=True)
    args.outputs.mkdir(parents=True, exist_ok=True)
    globals()[args.stage](args)


if __name__ == "__main__":
    main()
