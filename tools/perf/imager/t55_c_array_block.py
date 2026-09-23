#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Matched C-array natural/Clark application diagnostic (block or full cube).

Each stage is launched separately under the existing sampled RSS guard. This
does not run the full-32GB dataset or change the production imaging route.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

from stage_t55_c_array import save


PIXELS = 1024
CELL_ARCSEC = 0.06


def selection(args):
    first, count = args.first_channel, args.channels
    assert first >= 0 and count > 0 and first + count <= 512
    return first, count, min(first + count, 511), 20_000 * count


def native_name(args):
    return f"native-w{args.workers}" + (f"-{args.run_id}" if args.run_id else "")


def casa(args):
    from casatasks import casalog, tclean, version_string
    from casatools import image

    first, count, last_input, niter = selection(args)

    output = args.outputs / "casa"
    output.mkdir()
    os.chdir(output)
    casalog.setlogfile(str(output / "casa.log"))
    prefix = output / "image"
    kwargs = dict(
        vis=str(args.casa_input), imagename=str(prefix), datacolumn="data", field="0",
        spw=f"0:{first}~{last_input}", imsize=PIXELS,
        cell=f"{CELL_ARCSEC}arcsec", stokes="I", specmode="cube", outframe="LSRK",
        nchan=count, start=f"{44e9 + first * 2e6:.0f}Hz",
        width="2000000Hz", interpolation="linear", gridder="standard",
        weighting="natural", perchanweightdensity=True, deconvolver="clark",
        niter=niter, cycleniter=1000, gain=0.1, threshold="0.0005Jy",
        usemask="user", mask="circle[[512pix,512pix],27.5arcsec]",
        interactive=False, calcres=True, calcpsf=True, restoration=True,
        pbcor=False, savemodel="none", parallel=False, pblimit=-0.2,
        restart=False, fullsummary=True,
    )
    save(args.records / "casa-request.json", kwargs)
    started = time.perf_counter()
    result = tclean(**kwargs)
    seconds = time.perf_counter() - started
    handle = image()
    handle.open(str(prefix) + ".image")
    try:
        assert tuple(handle.shape()) == (PIXELS, PIXELS, 1, count)
        cs = handle.coordsys()
        try:
            for plane in (0, count // 2, count - 1):
                observed_hz = cs.toworld([512., 512., 0., float(plane)])["numeric"][3]
                expected_hz = 44e9 + (first + plane) * 2e6
                assert abs(observed_hz - expected_hz) < 1, (plane, observed_hz, expected_hz)
        finally:
            cs.done()
    finally:
        handle.done()
    for suffix in ("image", "model", "residual", "psf", "pb", "mask", "sumwt"):
        assert (output / f"image.{suffix}").is_dir(), suffix
    receipt = dict(prefix=str(prefix), seconds=seconds, workers=1, first_channel=first,
                   channels=count, input_channels=last_input - first + 1,
                   iterations=int(result["iterdone"]), majors=int(result["nmajordone"]),
                   stopcode=int(result["stopcode"]), casa_version=version_string(),
                   timing_boundary="tclean through publication; excludes input preparation and comparison")
    save(args.records / "casa-result.json", receipt)
    print(json.dumps(receipt), flush=True)


def mask(args):
    """Extract the common spatial user mask as one input direction plane."""
    import numpy as np
    from casatools import image, regionmanager

    _, count, _, _ = selection(args)
    source = args.outputs / "casa/image.mask"
    destination = args.outputs / "casa-mask-single.image"
    assert not destination.exists() and source.is_dir()
    handle = image()
    handle.open(str(source))
    try:
        assert tuple(handle.shape()) == (PIXELS, PIXELS, 1, count)
        baseline = handle.getchunk(blc=[0, 0, 0, 0], trc=[PIXELS - 1, PIXELS - 1, 0, 0])
        for plane in range(1, count):
            values = handle.getchunk(blc=[0, 0, 0, plane],
                                     trc=[PIXELS - 1, PIXELS - 1, 0, plane])
            if not np.array_equal(values, baseline):
                raise ValueError(f"CASA user mask differs in channel {plane}")
        selected = handle.subimage(outfile=str(destination), region=regionmanager().box(
            blc=[0, 0, 0, 0], trc=[PIXELS - 1, PIXELS - 1, 0, 0]))
        try:
            assert tuple(selected.shape()) == (PIXELS, PIXELS, 1, 1)
        finally:
            selected.done()
    finally:
        handle.done()
    save(args.records / "mask-single-result.json", dict(source=str(source),
         destination=str(destination), channels_checked=count,
         nonzero_pixels=int(np.count_nonzero(baseline))))
    print(json.dumps(dict(destination=str(destination), channels_checked=count)), flush=True)


def native(args):
    first, count, _, niter = selection(args)
    assert args.workers in (1, 4) and args.binary.is_file()
    name = native_name(args)
    output = args.outputs / name
    assert not output.exists(), "fresh native output required"
    environment = dict(
        os.environ,
        CASA_RS_C_ARRAY_MS=str(args.native_input),
        CASA_RS_C_ARRAY_OUTPUT=str(output),
        CASA_RS_C_ARRAY_CHANNEL=str(first),
        CASA_RS_C_ARRAY_OUTPUT_CHANNELS=str(count),
        CASA_RS_C_ARRAY_WORKERS=str(args.workers),
        CASA_RS_C_ARRAY_NITER=str(niter),
        CASA_RS_C_ARRAY_MASK=str(args.outputs / "casa-mask-single.image"),
        CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND="1000000000",
        CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND="1000000000",
    )
    command = [str(args.binary), "t55_c_array_turnaround::contiguous_spectral_block",
               "--exact", "--ignored", "--nocapture", "--test-threads=1"]
    save(args.records / f"app-{name}-command.json", dict(
        command=command, environment={key: value for key, value in environment.items()
                                      if key.startswith("CASA_RS_C_ARRAY_")
                                      or key.startswith("CASA_RS_IMAGING_SPILL_")},
        binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest()))
    with (args.records / f"app-{name}.log").open("x") as log:
        subprocess.run(command, env=environment, stdout=log, stderr=subprocess.STDOUT,
                       check=True)
    summary = json.loads((output / "summary.json").read_text())
    assert summary["workers"] == args.workers
    assert summary["output_channels"] == count
    assert summary["execution_route"] == "native-streaming-cube"
    save(args.records / f"app-{name}-result.json", summary)
    print(json.dumps(summary), flush=True)


def compare(args):
    from perf_harness.image_compare import compare_products

    assert args.workers in (1, 4) and args.casa_python.is_file()
    name = native_name(args)
    directory = args.records / f"comparison-{name}"
    directory.mkdir()
    contract = json.loads((args.repo / "tools/perf/imager/workloads/t55-clark-cube-development.json")
                          .read_text())["comparison"]
    result = compare_products(casa_python=str(args.casa_python), cwd=directory,
                              artifact_prefix=directory / "comparison", request={
                                  **contract,
                                  "left_prefix": str(args.outputs / name / "image"),
                                  "right_prefix": str(args.outputs / "casa/image"),
                                  "left_label": f"casa-rs W{args.workers}",
                                  "right_label": "CASA serial",
                                  "panel_dir": str(directory / "panels"),
                                  "structure_workspace_dir": str(directory / "structure"),
                              })
    save(directory / "result.json", result)
    assert len(result["products"]) == 7
    assert all(product["full_array"]["coverage_complete"]
               for product in result["products"].values())
    print(json.dumps(dict(status=result["status"], reason=result.get("reason"),
                          tolerances=result["tolerance_evaluation"]["status"])), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("casa", "mask", "native", "compare"))
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--records", type=Path, required=True)
    parser.add_argument("--outputs", type=Path, required=True)
    parser.add_argument("--casa-input", type=Path)
    parser.add_argument("--native-input", type=Path)
    parser.add_argument("--casa-python", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--workers", type=int)
    parser.add_argument("--first-channel", type=int, default=240)
    parser.add_argument("--channels", type=int, default=32)
    parser.add_argument("--run-id", default="")
    args = parser.parse_args()
    if args.run_id and not re.fullmatch(r"[a-z0-9][a-z0-9-]*", args.run_id):
        raise ValueError("run ID must be a simple durable label")
    selection(args)
    for path in (args.records, args.outputs):
        assert path.is_absolute() and path.is_dir()
        assert not any(part in ("private", "tmp", "temp") for part in path.resolve().parts)
    if args.stage == "casa":
        assert args.casa_input and args.casa_input.is_dir()
    elif args.stage == "native":
        assert args.native_input and args.native_input.is_dir() and args.binary
    elif args.stage == "compare":
        assert args.repo and args.casa_python
    {"casa": casa, "mask": mask, "native": native, "compare": compare}[args.stage](args)


if __name__ == "__main__":
    main()
