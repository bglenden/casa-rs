#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""One actual CASA direct-image prediction layout control per invocation."""
import argparse
import copy
import gzip
import json
from pathlib import Path
import shutil
import subprocess
import sys

import numpy as np
from casatasks import version_string
from casatools import coordsys, ctsys, image, measures, regionmanager, simulator, table, vpmanager

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_multiple_models_oracle import CENTRE, make_session

EXTRA_FRAMES = ("SUPERGAL", "ECLIPTIC", "JMEAN", "JTRUE", "JNAT", "APP",
    "MECLIPTIC", "TECLIPTIC", "HADEC", "AZEL", "AZELSW", "AZELGEO", "AZELSWGEO",
    "ITRF", "TOPO", "B1950_VLA", "BMEAN", "BTRUE")
FRAME_COMBINATIONS = tuple(f"frame_{frame}_{mode}" for frame in ("b1950", "galactic", "icrs")
    for mode in ("beam", "mosaic")) + ("frame_j2000_mosaic",)
FRAME_COMBINATIONS += ("frame_hadec_mosaic",)
FRAME_COMBINATIONS += tuple(f"frame_{frame.lower()}_beam" for frame in
    ("JMEAN", "APP", "HADEC", "AZEL"))
FRAME_COMBINATIONS += tuple(f"frame_{frame.lower()}_mosaic" for frame in ("JMEAN", "APP", "AZEL"))
HORIZON_FRAMES = ("HADEC", "AZEL", "AZELSW", "AZELGEO", "AZELSWGEO", "ITRF")


def geocentre_limit(me):
    # MVPosition::readjust(0) turns zero diurnal aberration into 1e-6.
    # A micrometre radius avoids that CASA defect and represents its zero limit.
    return me.position("ITRF", "0.000001m", "0m", "0m")


def irregular_mosaic_control(root, case, kind, location):
    """Keep raw DATA and correct the two mislabelled beam frequencies with CASA runs."""
    corrected = copy.deepcopy(case["after"]["rows"])
    controls = []
    for channel, frequency in ((2, 210e9), (3, 300e9)):
        out = root / f"beam-frequency-{channel}"
        fixture = root / f"beam-frequency-{channel}.json.gz"
        command = [sys.executable, str(Path(__file__).resolve()),
            "--case", kind, "--out", str(out), "--fixture", str(fixture),
            "--constant-frequency", str(frequency)]
        if location:
            command += ["--location", location]
        result = subprocess.run(command, check=False)
        # MosaicFT's known cleanup abort occurs after the complete fixture is saved.
        assert result.returncode in (0, -6) and fixture.is_file()
        with gzip.open(fixture, "rt") as stream:
            control = json.load(stream)["cases"][0]
        assert control["returned"] and len(control["after"]["rows"]) == len(corrected)
        for row, reference in zip(corrected, control["after"]["rows"]):
            if row["field"] == 0:
                for column in ("data", "corrected_data"):
                    row[column][4*channel:4*(channel+1)] = reference[column][4*channel:4*(channel+1)]
        controls.append(dict(channel=channel, frequency_hz=frequency))
    case["corrected_rows"] = corrected
    case["beam_frequency_controls"] = controls
    case["casa_defect"] = "HetArrayConvFunc makes an affine beam spectral axis from the first two frequencies: [100,150,210,300] GHz becomes [100,150,200,250]. Independent constant-frequency CASA controls supply the 210/300 GHz columns; raw DATA is retained."


def earth_orientation(before):
    """Actual IERS input knots used by CASA, with their native units."""
    tb = table()
    tb.open(ctsys.resolve("geodetic/IERSeop2000"))
    days = tb.getcol("MJD")
    first = before["rows"][0]["time"]/86400.
    start = int(np.searchsorted(days, first, side="right"))-1
    samples = [[float(tb.getcell(name, row)) for name in ("MJD", "dUT1", "x", "y", "dX", "dY")]
        for row in (start, start+1)]
    tb.close()
    return samples


def make_model(path, kind, location=None, full_stokes=False):
    polarized = kind.startswith("iquv") or full_stokes
    cs = coordsys().newcoordsys(direction=True, spectral=True, stokes=["I", "Q", "U", "V"] if polarized else ["I"])
    cs.setunits(["rad", "rad", "", "Hz"])
    cs.setreferencecode("J2000", type="direction")
    cs.setreferencevalue(CENTRE, type="direction")
    if location is not None and kind.endswith("_mosaic"):
        cs.setepoch(measures().epoch("UTC", "2024/06/01/00:00:05"))
        cs.settelescope("ALMA")
        record = cs.torecord()
        record["telescopeposition"] = location
        assert cs.fromrecord(record)
    frame = None
    if kind.startswith("frame_") and kind != "frame_conversion_b1950":
        frame = kind.removeprefix("frame_").removesuffix("_beam").removesuffix("_mosaic").upper()
    if frame:
        me = measures()
        if kind not in ("frame_j2000", "frame_b1950", "frame_galactic", "frame_icrs"):
            me.doframe(me.epoch("UTC", "2024/06/01/00:00:05"))
            me.doframe(location if location is not None else
                (geocentre_limit(me) if frame in HORIZON_FRAMES else me.observatory("ALMA")))
        ra = CENTRE[0] + (4e-5 if kind == "frame_hadec_mosaic" else 0.)
        direction = me.measure(me.direction("J2000", f"{ra}rad", f"{CENTRE[1]}rad"), frame)
        cs.setreferencecode(frame, type="direction", adjust=False)
        cs.setreferencevalue([direction["m0"]["value"], direction["m1"]["value"]], type="direction")
    if kind == "frame_conversion_b1950":
        cs.setconversiontype(direction="B1950")
    cs.setincrement([-2e-6, 2e-6], type="direction")
    cs.setreferencepixel([32, 32, 0, 0])
    cs.setreferencecode("LSRK", type="spectral")
    cs.setreferencevalue([100e9], type="spectral")
    cs.setincrement([200e9 / 3], type="spectral")
    pixels = np.zeros((64, 64, 4 if polarized else 1, 4), dtype=np.float32)
    for channel in range(4):
        pixels[32, 32, 0, channel] = channel + 1.
        if polarized:
            pixels[32, 32, 1:, channel] = [.2 * (channel + 1.), -.1 * (channel + 1.), .05 * (channel + 1.)]
        if kind == "iquv_beam":
            pixels[35, 30, :, channel] = np.asarray([.4, .1, -.02, .03]) * (channel + 1.)
        if kind.startswith("frame_"):
            pixels[35, 30, 0, channel] = .4 * (channel + 1.)
    ia = image()
    raw = path.with_name("canonical.image")
    ia.fromarray(outfile=str(raw), pixels=pixels, csys=cs.torecord())
    ia.setbrightnessunit("Jy/pixel")
    if kind in ("spectral_before_stokes", "direction_last"):
        result = ia.transpose(outfile=str(path), order="0132" if kind == "spectral_before_stokes" else "3201")
        result.done()
    elif kind == "no_stokes":
        result = ia.subimage(outfile=str(path), dropdeg=True)
        result.done()
    elif kind == "direction_only":
        region = regionmanager().box(blc=[0, 0, 0, 0], trc=[63, 63, 0, 0])
        result = ia.subimage(outfile=str(path), region=region, dropdeg=True)
        result.done()
    else:
        ia.done()
        shutil.copytree(raw, path)
    ia.done()
    cs.done()
    shutil.rmtree(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--constant-frequency", type=float,
        help="Independent one-frequency control for CASA's irregular mosaic beam-axis defect")
    parser.add_argument("--location", choices=("ALMA", "ARRAY"),
        help="Explicit setoptions reference position for horizon-frame controls")
    parser.add_argument("--refresh-beam", action="store_true")
    parser.add_argument("--squint", action="store_true")
    parser.add_argument("--all-fields", action="store_true")
    parser.add_argument("--long-track", action="store_true")
    parser.add_argument("--msselect", default="")
    parser.add_argument("--reverse-integrations", action="store_true")
    parser.add_argument("--case", required=True, choices=("canonical", "spectral_before_stokes", "direction_last", "no_stokes", "direction_only", "iquv", "iquv_beam", "frame_j2000", "frame_b1950", "frame_galactic", "frame_icrs", "frame_conversion_b1950", *("frame_"+frame.lower() for frame in EXTRA_FRAMES), *FRAME_COMBINATIONS))
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    source = Path(__file__).resolve().parents[2] / "libs/casa/casa-simulation/tests/data/sm_irregular_channels/ascending.ms"
    if args.long_track:
        from sm_apply_parameters_oracle import initialize
        fresh = make_session(inputs / "base.ms", stokes="XX XY YX YY", frequency=100e9, channel=1e6)
        assert fresh.observe(sourcename="target", spwname="band6", starttime="3s", stoptime="13s")
        fresh.close()
        initialize(inputs / "base.ms")
    else:
        shutil.copytree(source, inputs / "base.ms", ignore=shutil.ignore_patterns("table.lock"))
    if args.squint:
        tb = table()
        tb.open(str(inputs / "base.ms" / "POLARIZATION"), nomodify=False)
        tb.putcell("CORR_TYPE", 0, np.asarray([5, 6, 7, 8], dtype=np.int32))
        tb.close()
        tb.open(str(inputs / "base.ms" / "FEED"), nomodify=False)
        for row in range(tb.nrows()):
            tb.putcell("POLARIZATION_TYPE", row, np.asarray(["R", "L"]))
        tb.close()
        vp = vpmanager()
        assert vp.reset()
        assert vp.setpbgauss(telescope="ALMA", halfwidth="8arcsec", maxrad="60arcsec",
            reffreq="100GHz", squintdir=measures().direction("AZEL", "-4arcsec", "2arcsec"),
            squintreffreq="100GHz", dosquint=True)
        assert vp.saveastable(str(inputs / "squint.vp"))
    if args.constant_frequency is not None:
        tb = table()
        tb.open(str(inputs / "base.ms" / "SPECTRAL_WINDOW"), nomodify=False)
        tb.putcell("CHAN_FREQ", 0, np.full(int(tb.getcell("NUM_CHAN", 0)), args.constant_frequency))
        tb.putcell("REF_FREQUENCY", 0, args.constant_frequency)
        tb.close()
    if args.reverse_integrations:
        tb = table()
        tb.open(str(inputs / "base.ms"), nomodify=False)
        order = list(range(36, 72)) + list(range(36)) + list(range(72, tb.nrows()))
        for column in tb.colnames():
            if not tb.iscelldefined(column, 0):
                continue
            values = [tb.getcell(column, row) for row in range(tb.nrows())]
            for row, source_row in enumerate(order):
                tb.putcell(column, row, values[source_row])
        tb.close()
    model = inputs / "model.image"
    if args.location == "ARRAY":
        tb = table()
        tb.open(str(inputs / "base.ms" / "ANTENNA"))
        position = tb.getcell("POSITION", 0)
        tb.close()
        location = measures().position("ITRF", *(f"{value}m" for value in position))
    else:
        location = measures().observatory(args.location) if args.location else None
    make_model(model, args.case, location, args.squint)
    before = snapshot(inputs / "base.ms")
    path = root / "predicted.ms"
    shutil.copytree(inputs / "base.ms", path, ignore=shutil.ignore_patterns("table.lock"))
    run_model = root / "run.image"
    shutil.copytree(model, run_model, ignore=shutil.ignore_patterns("table.lock"))
    sm = simulator()
    assert sm.openfromms(str(path))
    assert sm.setdata(fieldid=[] if args.all_fields else [0], msselect=args.msselect)
    beam = args.case == "iquv_beam" or args.case.endswith("_beam")
    mosaic = args.case.endswith("_mosaic")
    if beam:
        settings = dict(dovp=True, dosquint=args.squint,
            parangleinc="0deg" if args.refresh_beam else "360deg")
        if args.squint:
            settings.update(usedefaultvp=False, vptable=str(inputs / "squint.vp"))
        assert sm.setvp(**settings)
    horizon = args.case.removeprefix("frame_").removesuffix("_mosaic").removesuffix("_beam").upper() in HORIZON_FRAMES
    options = dict(ftmachine="mosaic") if mosaic else {}
    if location is not None:
        options["location"] = location
    if horizon:
        options["location"] = location if location is not None else geocentre_limit(measures())
    if options:
        assert sm.setoptions(**options)
    returned = bool(sm.predict(imagename=str(run_model)))
    case = dict(name=args.case, operation="image", returned=returned, beam=beam, mosaic=mosaic,
        eop=earth_orientation(before),
        before=before, after=snapshot(path))
    if horizon:
        case["casa_defect"] = "Compare at the geocentre limit: MVPosition::readjust(0) adds spurious 1e-6 diurnal aberration at exactly zero radius. Model and prediction use the same limit frame."
    if location is not None:
        itrf = measures().measure(location, "ITRF")
        lon, lat, radius = [itrf[f"m{i}"]["value"] for i in range(3)]
        case["location_itrf_m"] = [radius*np.cos(lat)*np.cos(lon),
            radius*np.cos(lat)*np.sin(lon), radius*np.sin(lat)]
        case["name"] += "_at_" + args.location.lower()
        case.pop("casa_defect", None)
    if beam:
        case["beam_increment_deg"] = 0. if args.refresh_beam else 360.
        if args.refresh_beam:
            case["name"] += "_refresh"
    if args.squint:
        case.update(squint=True, circular=True, vp_table="squint.vp")
        case["name"] += "_squint"
    if args.all_fields:
        case["field_ids"] = []
        case["all_fields_selected"] = True
        case["name"] += "_all_fields"
    if args.long_track:
        case["name"] += "_long"
    if args.msselect:
        case["msselect"] = args.msselect
        case["name"] += "_selected_antenna"
    if args.reverse_integrations:
        case["name"] += "_reordered"
    if args.constant_frequency is not None:
        case["constant_frequency_hz"] = args.constant_frequency
    elif mosaic:
        irregular_mosaic_control(root, case, args.case, args.location)
    # Preserve completed DATA before CASA's documented mosaic cleanup abort.
    if mosaic:
        write(args.fixture, dict(casa_version=version_string(), cases=[case]))
    sm.close()
    if args.case == "frame_conversion_b1950":
        control_ms, control_image = root / "physical-control.ms", root / "physical-control.image"
        shutil.copytree(inputs / "base.ms", control_ms, ignore=shutil.ignore_patterns("table.lock"))
        shutil.copytree(model, control_image, ignore=shutil.ignore_patterns("table.lock"))
        ia = image()
        ia.open(str(control_image))
        cs = ia.coordsys()
        assert cs.setconversiontype(direction="J2000")
        assert ia.setcoordsys(cs.torecord())
        cs.done()
        ia.done()
        control = simulator()
        assert control.openfromms(str(control_ms))
        assert control.setdata(fieldid=[0])
        assert control.predict(imagename=str(control_image))
        control.close()
        case["direction_frame_corrected_rows"] = snapshot(control_ms)["rows"]
        case["casa_defect"] = "DirectionCoordinate::toWorld(MDirection) labels converted values with the native type; a display conversion must preserve physical pixel directions."
    write(args.fixture, dict(casa_version=version_string(), cases=[case]))
    print(args.case, "returned", returned, flush=True)


if __name__ == "__main__":
    main()
