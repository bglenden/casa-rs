#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record CASA 6.7.6.14 simulation plot data and quick PSFs (#1053/#1025).

Run under CASA's Python with --out in task scratch. Inputs are synthetic;
the reference contains the actual Matplotlib artists and CASA image pixels,
not a casa-rs rendering. PNG raster differences are not numerical evidence.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
from pathlib import Path
import shutil

os.environ.setdefault("MPLBACKEND", "Agg")

import numpy as np


def inputs(root):
    from casatools import componentlist, coordsys, image

    model = root / "model.image"
    cs = coordsys().newcoordsys(direction=True, spectral=True, stokes="I")
    cs.setunits(["rad", "rad", "", "Hz"])
    cs.setreferencecode("J2000", type="direction")
    cs.setreferencevalue([2.6179938779914944, -0.5235987755982988], type="direction")
    cs.setincrement([-2e-6, 2e-6], type="direction")
    cs.setreferencepixel([32, 32, 0, 0])
    cs.setreferencecode("LSRK", type="spectral")
    cs.setreferencevalue([230e9], type="spectral")
    cs.setincrement([1e9], type="spectral")
    pixels = np.zeros((64, 64, 1, 1), dtype=np.float32)
    pixels[32, 32, 0, 0] = 1.25
    pixels[41, 28, 0, 0] = 0.75
    ia = image()
    ia.fromarray(outfile=str(model), pixels=pixels, csys=cs.torecord(), overwrite=True)
    ia.setbrightnessunit("Jy/pixel")
    ia.done()
    cs.done()
    cl = componentlist()
    cl.addcomponent(dir="J2000 10h00m00 -30d00m00", flux=1.0, freq="230GHz")
    shutil.rmtree(root / "point.cl", ignore_errors=True)
    cl.rename(str(root / "point.cl"))
    cl.done()
    pads = [(-40, -25), (30, -40), (60, 10), (-50, 45), (10, 60), (0, 0), (-80, -70), (90, 80)]
    config = root / "small.cfg"
    config.write_text("# observatory=ALMA\n# coordsys=XYZ\n" + "".join(
        f"{2225061.1636+x:.6f} {-5440057.36994+y:.6f} -2481681.150540 12.0 A{i:02d}\n"
        for i, (x, y) in enumerate(pads)))
    return model, config


def artists(figure):
    from matplotlib.patches import Circle

    panels = []
    for axis in figure.axes:
        panels.append({
            "title": axis.get_title(), "xlabel": axis.get_xlabel(), "ylabel": axis.get_ylabel(),
            "xlim": list(axis.get_xlim()), "ylim": list(axis.get_ylim()), "aspect": axis.get_aspect(),
            "lines": [{"x": line.get_xdata().tolist(), "y": line.get_ydata().tolist(),
                       "color": line.get_color(), "marker": line.get_marker(),
                       "linestyle": line.get_linestyle()} for line in axis.lines],
            "images": [{"pixels": np.asarray(im.get_array()).tolist(), "extent": list(im.get_extent()),
                        "clim": list(im.get_clim()), "origin": im.origin,
                        "interpolation": im.get_interpolation(), "cmap": im.get_cmap().name}
                       for im in axis.images],
            "circles": [{"center": list(p.center), "radius": p.radius, "fill": p.get_fill()}
                        for p in axis.patches if isinstance(p, Circle)],
            "texts": [{"position": list(t.get_position()), "text": t.get_text()}
                      for t in axis.texts],
        })
    return panels


def main():
    import casatasks
    from casatasks import simobserve
    from casatools import image
    from matplotlib import pyplot as plt
    from matplotlib.figure import Figure

    if casatasks.version_string() != "6.7.6.14":
        raise RuntimeError(f"Expected CASA 6.7.6.14, got {casatasks.version_string()}")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=True)
    model, config = inputs(root)
    captured = {}
    original = Figure.savefig

    def savefig(figure, filename, *a, **kw):
        captured[Path(filename).name] = artists(figure)
        return original(figure, filename, *a, **kw)

    Figure.savefig = savefig
    cases = []
    previous = Path.cwd()
    os.chdir(root)
    try:
        for name, extra in [
            ("int_image", {}),
            ("int_components", {"skymodel": "", "complist": str(root / "point.cl"), "compwidth": "1GHz"}),
            ("sd_image", {"obsmode": "sd", "antennalist": "", "sdantlist": str(config), "maptype": "square", "incell": "2arcsec"}),
        ]:
            shutil.rmtree(name, ignore_errors=True)
            params = dict(skymodel=str(model), antennalist=str(config), mapsize="1arcsec",
                          integration="10s", totaltime="40s", thermalnoise="", graphics="file")
            params.update(extra)
            simobserve(project=name, **params)
            case = {"name": name, "parameters": params,
                    "products": sorted(p.name for p in Path(name).iterdir()),
                    "figures": {key: value for key, value in captured.items() if key.startswith(name + ".")}}
            psfs = list(Path(name).glob("*.quick.psf"))
            if psfs:
                ia = image()
                ia.open(str(psfs[0]))
                cs = ia.coordsys()
                case["psf"] = {"shape": ia.shape().tolist(), "unit": ia.brightnessunit(),
                               "reference": cs.referencevalue()["numeric"].tolist(),
                               "increment": cs.increment()["numeric"].tolist(),
                               "pixels": ia.getchunk().tolist(), "beam": ia.restoringbeam()}
                cs.done()
                ia.done()
            cases.append(case)
            payload = {"casa_version": casatasks.version_string(), "recipe": __file__, "cases": cases}
            with (root / "graphics-reference.json.gz").open("wb") as raw:
                with gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as zipped:
                    zipped.write((json.dumps(payload, separators=(",", ":"), allow_nan=False) + "\n").encode())
            print(name, "figures", len(case["figures"]), "quick PSFs", len(psfs), flush=True)
            plt.close("all")
    finally:
        os.chdir(previous)
        Figure.savefig = original
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
