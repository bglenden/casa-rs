#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Record the shape of what CASA's tasks return, as versioned fixtures (#931).

Run with CASA's own Python, which has `casatasks` and `casatools`:

    "$CASA_PYTHON" -I tools/casa-compat/record_casa_returns.py \
        --out resources/casa-compat/casa-6.7.6.14/returns.json

Each case calls one task on small images or small MeasurementSets built
here with `casatools` and records the structure of the value it returns: for a dictionary its keys in
order and the type of each key, for a NumPy array its dtype and shape, for
anything else its Python type. Values are not recorded; the casa-rs tests
compare the structure of casa-rs's return values with these, and the
numbers are compared elsewhere (`TESTING.md`). A case whose call raises
records the exception's type instead.

`--check FILE` compares the installed CASA with FILE and exits 1 when they
differ. The script exits 2 when it is not run with CASA's Python, or CASA is
not the pinned version.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

PINNED_VERSION = "6.7.6.14"


def describe(value: Any) -> dict[str, Any]:
    """The structure of `value`, without its numbers."""
    import numpy

    if value is None:
        return {"type": "None"}
    if isinstance(value, numpy.ndarray):
        return {"type": "ndarray", "dtype": str(value.dtype), "shape": list(value.shape)}
    if isinstance(value, dict):
        return {
            "type": "dict",
            "items": [[key, type(key).__name__, describe(item)] for key, item in value.items()],
        }
    if isinstance(value, (list, tuple)):
        return {"type": type(value).__name__, "items": [describe(item) for item in value]}
    return {"type": f"{type(value).__module__}.{type(value).__name__}".removeprefix("builtins.")}


def make_image(path: str, *, beam: bool = True) -> None:
    """A 16 x 16 x 1 x 8 (RA, Dec, Stokes, frequency) image of ramped values."""
    import numpy
    from casatools import image

    tool = image()
    tool.fromshape(path, [16, 16, 1, 8], overwrite=True)
    data = numpy.arange(16 * 16 * 8, dtype=numpy.float32).reshape(16, 16, 1, 8) / 100.0
    tool.putchunk(data)
    tool.setbrightnessunit("Jy/beam")
    if beam:
        tool.setrestoringbeam(major="3arcsec", minor="2arcsec", pa="10deg")
    tool.done()


def make_ms(path: str) -> None:
    """A MeasurementSet of four antennas, one field and one 8-channel window
    in XX and YY: one 60 s scan of 10 s integrations, simulated with no
    noise."""
    from casatools import measures, simulator

    me = measures()
    sm = simulator()
    sm.open(path)
    sm.setconfig(
        telescopename="ALMA",
        x=[0.0, 30.0, 0.0, 30.0],
        y=[0.0, 0.0, 30.0, 30.0],
        z=[0.0] * 4,
        dishdiameter=[12.0] * 4,
        mount=["alt-az"] * 4,
        antname=["A0", "A1", "A2", "A3"],
        padname=["P0", "P1", "P2", "P3"],
        coordsystem="local",
        referencelocation=me.observatory("ALMA"),
    )
    sm.setspwindow(
        spwname="SPW0", freq="100GHz", deltafreq="1MHz", freqresolution="1MHz", nchannels=8, stokes="XX YY"
    )
    sm.setfeed(mode="perfect X Y")
    sm.setfield(sourcename="SRC", sourcedirection=me.direction("J2000", "0h0m0s", "-30d0m0s"))
    sm.setlimits(shadowlimit=0.001, elevationlimit="8.0deg")
    sm.setauto(autocorrwt=0.0)
    sm.settimes(integrationtime="10s", usehourangle=True, referencetime=me.epoch("UTC", "2020/01/01/00:00:00"))
    sm.observe("SRC", "SPW0", starttime="0s", stoptime="60s")
    sm.close()


def ms_cases(root: Path) -> dict[str, Callable[[], Any]]:
    """Each MeasurementSet case's name and call, on a MeasurementSet under
    `root`; they run in order, so `flagmanager list` sees the versions the
    cases before it saved."""
    import casatasks as ct

    vis = str(root / "sim.ms")
    make_ms(vis)
    return {
        "flagdata summary": lambda: ct.flagdata(vis=vis, mode="summary"),
        "flagdata manual": lambda: ct.flagdata(vis=vis, mode="manual", antenna="A0"),
        "flagdata action none": lambda: ct.flagdata(vis=vis, mode="manual", action=""),
        "flagdata unflag": lambda: ct.flagdata(vis=vis, mode="unflag"),
        "flagmanager save": lambda: ct.flagmanager(vis=vis, mode="save", versionname="v1"),
        "flagmanager list": lambda: ct.flagmanager(vis=vis, mode="list"),
        "split": lambda: ct.split(vis=vis, outputvis=str(root / "split.ms"), datacolumn="data"),
        "mstransform": lambda: ct.mstransform(
            vis=vis, outputvis=str(root / "mstransform.ms"), datacolumn="data", chanaverage=True, chanbin=2
        ),
    }


def make_cal_ms(path: str) -> None:
    """A MeasurementSet of five antennas, two fields (FLUX, then PHASE) and
    one 8-channel window in XX and YY: a 60 s scan of each, 10 s
    integrations, predicted from point sources of 2 and 1 Jy with no
    noise."""
    from casatools import componentlist, measures, simulator

    me = measures()
    components = path + ".cl"
    cl = componentlist()
    cl.addcomponent(dir="J2000 0h0m0s -30d0m0s", flux=2.0, fluxunit="Jy", freq="100GHz", shape="point")
    cl.addcomponent(dir="J2000 1h0m0s -30d0m0s", flux=1.0, fluxunit="Jy", freq="100GHz", shape="point")
    cl.rename(components)
    cl.close()
    sm = simulator()
    sm.open(path)
    sm.setconfig(
        telescopename="ALMA",
        x=[0.0, 30.0, 0.0, 30.0, 60.0],
        y=[0.0, 0.0, 30.0, 30.0, 10.0],
        z=[0.0] * 5,
        dishdiameter=[12.0] * 5,
        mount=["alt-az"] * 5,
        antname=["A0", "A1", "A2", "A3", "A4"],
        padname=["P0", "P1", "P2", "P3", "P4"],
        coordsystem="local",
        referencelocation=me.observatory("ALMA"),
    )
    sm.setspwindow(
        spwname="SPW0", freq="100GHz", deltafreq="1MHz", freqresolution="1MHz", nchannels=8, stokes="XX YY"
    )
    sm.setfeed(mode="perfect X Y")
    sm.setfield(sourcename="FLUX", sourcedirection=me.direction("J2000", "0h0m0s", "-30d0m0s"))
    sm.setfield(sourcename="PHASE", sourcedirection=me.direction("J2000", "1h0m0s", "-30d0m0s"))
    sm.setlimits(shadowlimit=0.001, elevationlimit="8.0deg")
    sm.setauto(autocorrwt=0.0)
    sm.settimes(integrationtime="10s", usehourangle=True, referencetime=me.epoch("UTC", "2020/01/01/00:00:00"))
    sm.observe("FLUX", "SPW0", starttime="0s", stoptime="60s")
    sm.observe("PHASE", "SPW0", starttime="60s", stoptime="120s")
    sm.predict(complist=components)
    sm.close()


def cal_cases(root: Path) -> dict[str, Callable[[], Any]]:
    """Each calibration case's name and call, on a MeasurementSet under
    `root`; they run in order, so a later case reads an earlier case's
    table."""
    import casatasks as ct

    vis = str(root / "cal.ms")
    make_cal_ms(vis)
    gain = str(root / "g.cal")
    return {
        "gaincal": lambda: ct.gaincal(vis=vis, caltable=gain, refant="A0", solint="int"),
        "gaincal pre-applied": lambda: ct.gaincal(
            vis=vis, caltable=str(root / "g2.cal"), refant="A0", gaintable=[gain]
        ),
        "bandpass": lambda: ct.bandpass(vis=vis, caltable=str(root / "b.cal"), refant="A0", field="FLUX"),
        "fluxscale": lambda: ct.fluxscale(
            vis=vis, caltable=gain, fluxtable=str(root / "f.cal"), reference=["FLUX"], transfer=["PHASE"]
        ),
        "applycal": lambda: ct.applycal(vis=vis, gaintable=[gain]),
        "gencal": lambda: ct.gencal(vis=vis, caltable=str(root / "o.cal"), caltype="opac", parameter=[0.05]),
        **tclean_cases(root, vis),
    }


def tclean_cases(root: Path, vis: str) -> dict[str, Callable[[], Any]]:
    """Each tclean case's name and call, imaging field FLUX of the
    calibration MeasurementSet `vis` under `root`."""
    import casatasks as ct

    def tclean(name: str, **parameters: Any) -> Callable[[], Any]:
        image = str(root / name)
        return lambda: ct.tclean(vis=vis, imagename=image, imsize=[64], cell="2arcsec", field="0", **parameters)

    return {
        "tclean niter0": tclean("t0", niter=0),
        "tclean niter100": tclean("t100", niter=100),
        "tclean fullsummary": tclean("tfull", niter=100, fullsummary=True),
        "tclean cube": tclean("tcube", niter=30, specmode="cube", nchan=3),
    }


def cases(root: Path) -> dict[str, Callable[[], Any]]:
    """Each case's name and call, on images and a MeasurementSet under
    `root`."""
    import casatasks as ct

    image = str(root / "ramp.im")
    other = str(root / "other.im")
    unbeamed = str(root / "unbeamed.im")
    make_image(image)
    make_image(other)
    make_image(unbeamed, beam=False)
    fits = str(root / "ramp.fits")
    ct.exportfits(imagename=image, fitsimage=fits)

    def put() -> Any:
        return ct.imhead(imagename=other, mode="put", hdkey="object", hdvalue="M1")

    def add() -> Any:
        return ct.imhead(imagename=other, mode="add", hdkey="extra", hdvalue="casa-rs")

    def delete() -> Any:
        return ct.imhead(imagename=other, mode="del", hdkey="extra")

    return {
        "exportfits": lambda: ct.exportfits(imagename=image, fitsimage=str(root / "x.fits")),
        "feather": lambda: ct.feather(imagename=str(root / "f.im"), highres=image, lowres=other),
        "imhead summary": lambda: ct.imhead(imagename=image),
        "imhead list": lambda: ct.imhead(imagename=image, mode="list"),
        "imhead get crval1": lambda: ct.imhead(imagename=image, mode="get", hdkey="crval1"),
        "imhead get bunit": lambda: ct.imhead(imagename=image, mode="get", hdkey="bunit"),
        "imhead get beammajor": lambda: ct.imhead(imagename=image, mode="get", hdkey="beammajor"),
        "imhead put": put,
        "imhead add": add,
        "imhead del": delete,
        "imhead history": lambda: ct.imhead(imagename=image, mode="history"),
        "immath": lambda: ct.immath(imagename=image, outfile=str(root / "m.im"), expr="IM0*2"),
        "immoments": lambda: ct.immoments(imagename=image, outfile=str(root / "mom.im")),
        "impbcor": lambda: ct.impbcor(imagename=image, pbimage=other, outfile=str(root / "pb.im")),
        "importfits": lambda: ct.importfits(fitsimage=fits, imagename=str(root / "i.im")),
        "impv": lambda: ct.impv(
            imagename=image, outfile=str(root / "pv.im"), start=[2, 2], end=[12, 12]
        ),
        "imregrid get": lambda: ct.imregrid(imagename=image),
        "imregrid template": lambda: ct.imregrid(
            imagename=image, template=other, output=str(root / "r.im")
        ),
        "imstat": lambda: ct.imstat(imagename=image),
        "imstat axes": lambda: ct.imstat(imagename=image, axes=[0, 1]),
        "imstat no beam": lambda: ct.imstat(imagename=unbeamed, box="0,0,3,3"),
        "imsubimage": lambda: ct.imsubimage(imagename=image, outfile=str(root / "s.im"), box="0,0,7,7"),
        **ms_cases(root),
        **cal_cases(root),
    }


def run(name: str, call: Callable[[], Any]) -> dict[str, Any]:
    try:
        return {"returns": describe(call())}
    except BaseException as error:
        return {"raises": type(error).__name__}


def records() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="casa-returns-") as directory:
        previous = os.getcwd()
        os.chdir(directory)
        try:
            calls = cases(Path(directory))
            # In order: a later case may read an earlier case's product.
            return {name: run(name, call) for name, call in calls.items()}
        finally:
            os.chdir(previous)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--out", type=Path, help="write the fixture to this file")
    action.add_argument("--check", type=Path, help="compare the installed CASA with this fixture")
    args = parser.parse_args()
    try:
        import casatasks
    except ImportError as error:
        print(f"record_casa_returns: casatasks is not importable ({error}); run with CASA's Python", file=sys.stderr)
        return 2
    version = str(casatasks.version_string())
    if version != PINNED_VERSION:
        print(f"record_casa_returns: CASA {version} is installed; the fixture is pinned to {PINNED_VERSION}", file=sys.stderr)
        return 2
    document = {"casa_version": version, "cases": records()}
    text = json.dumps(document, indent=2) + "\n"
    if args.out:
        args.out.write_text(text, encoding="utf-8")
        print(f"record_casa_returns: wrote {len(document['cases'])} cases to {args.out}")
        return 0
    committed = json.loads(args.check.read_text(encoding="utf-8"))
    found = [
        name
        for name in sorted(set(committed.get("cases", {})) | set(document["cases"]))
        if committed.get("cases", {}).get(name) != document["cases"].get(name)
    ]
    if committed.get("casa_version") != version:
        found.insert(0, f"casa_version {committed.get('casa_version')}")
    for name in found:
        print(f"record_casa_returns: {name} differs from the installed CASA", file=sys.stderr)
    if found:
        return 1
    print(f"record_casa_returns: {len(document['cases'])} cases match CASA {version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
