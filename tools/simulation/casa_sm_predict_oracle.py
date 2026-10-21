#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA oracle for simulation Phase 1a (#841): `sm.predict` into a CASA-made MS.

Run with CASA's Python (for example `/Applications/CASA.app/Contents/MacOS/python3`).
For each case, the script:

1. writes a deterministic model image;
2. creates an MS with the `sm` tool (`setconfig`, `setspwindow`, `setfeed`,
   `setfield`, `settimes`, `observe`);
3. predicts the model into DATA with the default `GridFT` FTMachine, once with
   no primary beam and once after `setvp(dovp=True, usedefaultvp=False)`, as
   `simobserve` does.

The casa-rs prediction spike reads these MSs and compares its prediction with
CASA's DATA sample by sample, on CASA's geometry. A JSON manifest records the
inputs and the time CASA spent in each `sm` call.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time

import numpy as np
from casatasks.private import simutil
from casatools import coordsys, image, measures, simulator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="output directory (replaced)")
    parser.add_argument(
        "--config",
        default=os.path.expanduser("~/.casa/data/alma/simmos/alma.cycle10.3.cfg"),
        help="CASA antenna configuration file",
    )
    parser.add_argument("--freq-ghz", type=float, default=230.0, help="centre of channel 0")
    parser.add_argument("--chan-width-mhz", type=float, default=15.625)
    parser.add_argument("--nchan", type=int, default=4)
    parser.add_argument("--imsize", type=int, default=256)
    parser.add_argument("--cell-arcsec", type=float, default=0.05)
    parser.add_argument("--integration-s", type=float, default=30.0)
    parser.add_argument("--duration-s", type=float, default=600.0)
    parser.add_argument("--ra", default="10h00m00.0")
    parser.add_argument("--dec", default="-30d00m00.0")
    parser.add_argument("--refdate", default="2026/03/21/00:00:00")
    parser.add_argument("--stokes", default="XX YY", help="correlations, e.g. 'XX YY' or 'RR LL'")
    parser.add_argument(
        "--offset-arcsec",
        type=float,
        nargs=2,
        default=(0.0, 0.0),
        help="model reference direction minus the field direction, in the tangent plane",
    )
    parser.add_argument(
        "--crpix-shift",
        type=int,
        nargs=2,
        default=(0, 0),
        help="model reference pixel minus (n/2, n/2)",
    )
    parser.add_argument(
        "--mfs-width-mhz",
        type=float,
        default=None,
        help="width of the one-plane model (default: the whole band)",
    )
    parser.add_argument(
        "--spectral-unit",
        default="Hz",
        help="unit of the model's spectral axis (CASA normalizes it to Hz)",
    )
    parser.add_argument("--autocorr", action="store_true", help="write autocorrelations")
    parser.add_argument("--no-vp", action="store_true", help="skip the setvp cases")
    return parser.parse_args()


def write_model(path: str, args: argparse.Namespace, planes: int) -> None:
    """Gaussian blobs and points, offset from the centre, a few Jy in total."""
    cs = coordsys()
    csys = cs.newcoordsys(direction=True, spectral=True, stokes="I")
    csys.setreferencecode("J2000", type="direction")
    # Units first: setreferencevalue and setincrement read plain numbers in the
    # coordinate system's current units.
    csys.setunits(["rad", "rad", "", "Hz"])
    me = measures()
    direction = me.direction("J2000", args.ra, args.dec)
    ra = direction["m0"]["value"]
    dec = direction["m1"]["value"]
    dx, dy = (value / 206264.80624709636 for value in args.offset_arcsec)
    csys.setreferencevalue([ra + dx / np.cos(dec), dec + dy], type="direction")
    csys.setincrement(
        [-args.cell_arcsec / 206264.80624709636, args.cell_arcsec / 206264.80624709636],
        type="direction",
    )
    n = args.imsize
    shift_x, shift_y = args.crpix_shift
    csys.setreferencepixel([n // 2 + shift_x, n // 2 + shift_y, 0, 0])
    width_hz = args.chan_width_mhz * 1e6
    if planes == 1:
        centre = args.freq_ghz * 1e9 + 0.5 * (args.nchan - 1) * width_hz
        mfs_width_hz = (
            args.nchan * width_hz if args.mfs_width_mhz is None else args.mfs_width_mhz * 1e6
        )
        csys.setreferencevalue([centre], type="spectral")
        csys.setincrement([mfs_width_hz], type="spectral")
    else:
        csys.setreferencevalue([args.freq_ghz * 1e9], type="spectral")
        csys.setincrement([width_hz], type="spectral")
    csys.setreferencecode("LSRK", type="spectral")
    # setunits converts the stored values, so the model is unchanged physically.
    csys.setunits(["rad", "rad", "", args.spectral_unit])

    y, x = np.mgrid[0:n, 0:n]
    pixels = np.zeros((n, n, 1, planes), dtype=np.float32)
    for plane in range(planes):
        scale = 1.0 + 0.1 * plane
        blob = lambda cx, cy, sigma, peak: peak * np.exp(  # noqa: E731
            -((x - cx) ** 2 + (y - cy) ** 2) / (2.0 * sigma**2)
        )
        sky = blob(n * 0.40, n * 0.55, 6.0, 0.02 * scale) + blob(n * 0.62, n * 0.45, 3.0, 0.03)
        sky[int(n * 0.30), int(n * 0.70)] += 0.5
        sky[int(n * 0.75), int(n * 0.35)] += 0.3 * scale
        pixels[:, :, 0, plane] = sky.T  # CASA axes are [x, y]
    ia = image()
    ia.fromarray(outfile=path, pixels=pixels, csys=csys.torecord(), overwrite=True)
    ia.setbrightnessunit("Jy/pixel")
    ia.done()
    csys.done()


def simulate(ms: str, model: str, args: argparse.Namespace, with_vp: bool) -> dict:
    util = simutil.simutil()
    x, y, z, diam, pads, names, telescope, posobs = util.readantenna(args.config)
    me = measures()
    sm = simulator()
    timings = {}
    started = time.perf_counter()
    sm.open(ms)
    sm.setconfig(
        telescopename=telescope,
        x=x,
        y=y,
        z=z,
        dishdiameter=diam.tolist(),
        mount=["alt-az"],
        antname=names,
        padname=pads,
        coordsystem="global",
        referencelocation=posobs,
    )
    sm.setspwindow(
        spwname="spw0",
        freq=f"{args.freq_ghz}GHz",
        deltafreq=f"{args.chan_width_mhz}MHz",
        freqresolution=f"{args.chan_width_mhz}MHz",
        nchannels=args.nchan,
        stokes=args.stokes,
    )
    sm.setfeed(mode="perfect X Y" if "X" in args.stokes else "perfect R L")
    sm.setfield(sourcename="target", sourcedirection=me.direction("J2000", args.ra, args.dec))
    sm.setauto(autocorrwt=1.0 if args.autocorr else 0.0)
    sm.settimes(
        integrationtime=f"{args.integration_s}s",
        usehourangle=True,
        referencetime=me.epoch("UTC", args.refdate),
    )
    timings["setup_s"] = time.perf_counter() - started
    started = time.perf_counter()
    sm.observe(
        "target",
        "spw0",
        starttime=f"{-args.duration_s / 2}s",
        stoptime=f"{args.duration_s / 2}s",
    )
    timings["observe_s"] = time.perf_counter() - started
    # simobserve selects the observed fields before setting the voltage pattern.
    sm.setdata(fieldid=[0])
    if with_vp:
        sm.setvp(dovp=True, usedefaultvp=False)
    started = time.perf_counter()
    sm.predict(imagename=model)
    timings["predict_s"] = time.perf_counter() - started
    sm.close()
    return timings


def main() -> int:
    args = parse_args()
    shutil.rmtree(args.out, ignore_errors=True)
    os.makedirs(args.out)
    cases = []
    for planes in (args.nchan, 1):
        for with_vp in (False,) if args.no_vp else (False, True):
            name = f"model{planes}{'-vp' if with_vp else ''}"
            # predict rewrites the model image's units and frame, so each case
            # predicts from its own copy.
            model = os.path.join(args.out, f"{name}.image")
            write_model(model, args, planes)
            pristine = os.path.join(args.out, f"{name}.input.image")
            shutil.copytree(model, pristine)
            ms = os.path.join(args.out, f"{name}.ms")
            timings = simulate(ms, model, args, with_vp)
            cases.append(
                {
                    "name": name,
                    "model_planes": planes,
                    "vp": with_vp,
                    "ms": ms,
                    "model_input": pristine,
                    "model_after_predict": model,
                    "timings": timings,
                }
            )
            print(name, json.dumps(timings))
    manifest = {
        "schema_version": 1,
        "purpose": "simulation Phase 1a prediction spike oracle (#841)",
        "args": vars(args),
        "cases": cases,
    }
    with open(os.path.join(args.out, "manifest.json"), "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
