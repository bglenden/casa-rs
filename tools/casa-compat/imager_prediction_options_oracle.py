#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare configured native imager predictions with installed CASA."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import shutil

import casatasks
from casatools import image, imager, table
import numpy as np

from bandpass_oracle import source_data
from calibration_ft_oracle import coordinates
from gaincal_oracle import relative_rms


BEAM_CASES = {"vp", "all-vp", "no-vp", "pointing-vp", "dish-vp", "vpvirtual", "custom-vp"}
CLOSE_MODEL_CASES = BEAM_CASES | {"image-distance", "definition-distance"}
VIRTUAL_CASES = ["wvirtual", "vpvirtual"]


def write_model(path, direction, reference, flux, cell=70):
    ra, dec = direction
    pixels = np.zeros((32, 32, 1, 1), np.float32)
    pixels[26, 7, 0, 0] = flux
    tool = image()
    assert tool.fromarray(outfile=str(path), pixels=pixels)
    coords = tool.coordsys()
    assert coords.setunits(["rad", "rad", "", "Hz"])
    assert coords.setreferencevalue(value=f"{ra}rad {dec}rad", type="direction")
    assert coords.setincrement(value=f"-{cell}arcsec {cell}arcsec", type="direction")
    assert coords.setreferencepixel(value=[16., 16.], type="direction")
    assert coords.setreferencecode("TOPO", type="spectral", adjust=False)
    assert coords.setreferencepixel(value=[0.], type="spectral")
    assert coords.setreferencevalue(value=f"{reference}Hz", type="spectral")
    assert coords.setincrement(value="30MHz", type="spectral")
    assert tool.setcoordsys(coords.torecord())
    assert tool.setbrightnessunit("Jy/pixel")
    coords.done()
    tool.done()


def configure_data(path, case=""):
    source_data(path, stokes="XX XY YX YY")
    tb = table()
    assert tb.open(str(path), nomodify=False)
    try:
        rows = tb.nrows()
        uvw = np.array([
            np.linspace(-2., 2., rows), np.full(rows, .8), np.linspace(.05, 1.8, rows)
        ])
        if case == "automatic-w":
            # Exercise several automatic planes. Pinned CASA can return a
            # nonpositive convolution integral on the short-baseline grid.
            uvw[2] *= 10.
        tb.putcol("UVW", uvw)
        fields = tb.getcol("FIELD_ID")
        times = tb.getcol("TIME")
    finally:
        tb.close()
    if case.startswith("phase-"):
        assert tb.open(str(path / "FIELD"), nomodify=False)
        try:
            tb.putcell("TIME", 0, float(np.min(times[fields == 0])))
            tb.putcell("NUM_POLY", 0, 1)
            for column in ["PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"]:
                position = tb.getcell(column, 0)[:, 0]
                tb.putcell(column, 0, np.column_stack((position, [2e-5, 0.])))
        finally:
            tb.close()
    if case == "pointing-vp":
        assert tb.open(str(path / "POINTING"), nomodify=False)
        try:
            directions = tb.getcol("DIRECTION")
            directions[0, 0, :] += 0.000111963
            tb.putcol("DIRECTION", directions)
        finally:
            tb.close()
    if case == "dish-vp":
        assert tb.open(str(path / "OBSERVATION"), nomodify=False)
        try:
            tb.putcell("TELESCOPE_NAME", 0, "CUSTOM_ARRAY")
        finally:
            tb.close()


def prediction(path, images, reference, case, factory=imager, custom_vp=None):
    cell = 2 if case in CLOSE_MODEL_CASES else 70
    phase_time = -1.
    if case == "phase-time":
        tb = table()
        assert tb.open(str(path))
        try:
            phase_time = float(tb.getcell("TIME", tb.nrows() - 1))
        finally:
            tb.close()
    tool = factory()
    assert tool.open(str(path), usescratch=case not in VIRTUAL_CASES)
    assert tool.selectvis(field="0,1" if case in {"all-vp", "vpvirtual"} else "0")
    assert tool.defineimage(nx=32, ny=32, cellx=f"{cell}arcsec", celly=f"{cell}arcsec",
                            phasecenter=0, outframe="TOPO",
                            distance="10m" if case == "definition-distance" else "0m")
    telescope = "THIS_IS_NOT_AN_OBSERVATORY" if case == "vp" else ""
    if case == "custom-vp":
        assert tool.setvp(dovp=True, usedefaultvp=False, vptable=str(custom_vp), verbose=False)
    else:
        assert tool.setvp(dovp=case in BEAM_CASES - {"no-vp"}, telescope=telescope)
    if case in {"wproject", "wvirtual", "automatic-w"}:
        planes = -1 if case == "automatic-w" else 16
        assert tool.setoptions(ftmachine="wproject", padding=1.4, wprojplanes=planes, numthreads=1)
        assert tool.ft(model=images[:1]), (case, path)
    elif case == "boxcar":
        assert tool.setoptions(gridfunction="BOX", singleprecisiononly=True, cache=131072,
                               tile=8, freqinterp="linear-interpolation")
        assert tool.ft(model=images[:1])
    elif case in {"taylor", "changed"}:
        assert tool.settaylorterms(2, reffreq=reference * (.99 if case == "taylor" else .98))
        assert tool.ft(model=images)
    else:
        if case == "grid":
            assert tool.setoptions(ftmachine="ft", padding=1.4)
        assert tool.ft(model=images[:1], phasecentertime=phase_time)
    assert tool.done()


def samples(path, column="MODEL_DATA", field=0):
    tb = table()
    assert tb.open(str(path))
    try:
        fields = tb.getcol("FIELD_ID")
        selected = np.ones_like(fields, dtype=bool) if field is None else fields == field
        return tb.getcol(column)[:, :, selected]
    finally:
        tb.close()


def empty_image(path):
    tool = image()
    assert tool.open(str(path))
    try:
        assert not np.any(tool.getchunk())
        metadata = {
            "shape": tool.shape().tolist(),
            "units": tool.brightnessunit(),
            "misc": tool.miscinfo(),
        }
    finally:
        tool.done()
    tb = table()
    assert tb.open(str(path))
    try:
        metadata["type"] = tb.info()["subType"]
        metadata["image"] = tb.getkeyword("imageinfo")["objectname"]
    finally:
        tb.close()
    return metadata


def definition_cases(root, reference_ms, factories):
    definitions = {
        "defaults": {},
        "rectangular": dict(nx=40, ny=32, cellx="3arcsec", celly="2arcsec",
                            stokes="IQ", phasecenter=1, projection="TAN", distance="10m"),
        "selected-spw": dict(nx=32, spw=[-1], restfreq="100GHz", outframe="TOPO"),
        "channel": dict(nx=32, mode="channel", nchan=4, start=2, step=2, outframe="TOPO"),
        "frequency": dict(nx=32, mode="frequency", nchan=3, start="100GHz", step="20MHz"),
        "restless-mfs": dict(nx=32),
        "restless-frequency": dict(nx=32, mode="frequency", nchan=3, start="100.003GHz",
                                   step="2MHz"),
        "native-frame": dict(nx=32, mode="channel", nchan=3, start=2, step=2, outframe=""),
        "velocity-default": dict(nx=32, mode="opticalvelocity", nchan=4, start="3km/s",
                                 step="5km/s", restfreq="100.006GHz", outframe="TOPO"),
        "velocity-optical": dict(nx=32, mode="velocity", nchan=4, start="3km/s", step="5km/s",
                                 veltype="optical", restfreq="100.006GHz", outframe="TOPO"),
        "north-pole": dict(nx=32, phasecenter=-1),
    }
    reports = {}
    for name, arguments in definitions.items():
        records = []
        predictions = []
        for engine, factory in factories.items():
            path = root / f"definition-{name}-{engine}.ms"
            shutil.copytree(reference_ms, path)
            if name.startswith("restless-") or name == "native-frame":
                with_table = table()
                subtable = "SPECTRAL_WINDOW" if name == "native-frame" else "SOURCE"
                assert with_table.open(str(path / subtable), nomodify=False)
                try:
                    if name == "native-frame":
                        with_table.putcell("MEAS_FREQ_REF", 0, 1)  # LSRK
                    else:
                        with_table.removecols(["REST_FREQUENCY"])
                finally:
                    with_table.close()
            model = root / f"definition-{name}-{engine}.im"
            tool = factory()
            assert tool.open(str(path), usescratch=True)
            assert tool.selectvis(field=0)
            assert tool.defineimage(**arguments), (name, engine)
            if name == "defaults":
                for convolution in ("gauss", "gjinc"):
                    try:
                        tool.setoptions(ftmachine="wproject", gridfunction=convolution)
                    except RuntimeError as error:
                        assert "only for single dish imaging" in str(error)
                    else:
                        raise AssertionError((engine, convolution))
                assert tool.setoptions()
            if name == "channel" and engine == "native":
                assert tool.setoptions(freqinterp="linear")
                before = samples(path)
                assert tool.ft(str(model)) is False
                assert not model.exists()
                np.testing.assert_array_equal(samples(path), before)
                assert tool.setoptions(freqinterp="nearest")
            assert tool.ft(str(model)), (name, engine)
            assert tool.done()
            observed = empty_image(model)
            reader = image()
            assert reader.open(str(model))
            try:
                coordinate = reader.coordsys()
                observed.update(stokes=coordinate.stokes(), frames=coordinate.referencecode(),
                                reference_pixel=coordinate.referencepixel()["numeric"],
                                reference_value=coordinate.referencevalue()["numeric"],
                                increment=coordinate.increment()["numeric"],
                                rest=coordinate.restfrequency()["value"])
                assert coordinate.done()
                if name == "channel":
                    pixels = reader.getchunk()
                    pixels[21, 12, 0, :] = [2., .5, 1.5, .75]
                    assert reader.putchunk(pixels)
            finally:
                assert reader.done()
            records.append(observed)
            if name == "channel":
                assert tool.open(str(path), usescratch=True)
                assert tool.selectvis(field=0)
                if engine == "native":
                    assert tool.defineimage(**arguments)
                    before = samples(path)
                    for interpolation in ("linear", "cubic", "spline"):
                        assert tool.setoptions(freqinterp=interpolation)
                        assert tool.ft(str(model)) is False
                        np.testing.assert_array_equal(samples(path), before)
                    assert tool.setoptions(freqinterp="nearest")
                assert tool.ft(str(model))
                assert tool.done()
                predictions.append(samples(path))
        expected, actual = records
        for name_of_axis in ("reference_value", "increment"):
            reference = expected.pop(name_of_axis)
            result = actual.pop(name_of_axis)
            np.testing.assert_allclose(result[:-1], reference[:-1], rtol=0, atol=1e-14)
            np.testing.assert_allclose(result[-1], reference[-1], rtol=0, atol=0.002)
            reports[f"{name}-{name_of_axis}-hz"] = float(abs(result[-1] - reference[-1]))
        # The absent MFS rest frequency uses the synthesized center, which
        # has the same sub-millihertz measures precision as the frequency axis.
        if name == "restless-mfs":
            np.testing.assert_allclose(actual.pop("rest"), expected.pop("rest"), rtol=0, atol=0.002)
        np.testing.assert_equal(actual, expected)
        if predictions:
            assert np.max(np.abs(predictions[0])) > 1.
            error = relative_rms(predictions[1], predictions[0])
            assert error < 1e-3, (name, error)
            reports["channel_cube_nrms"] = error
    reports.update(reopened_channel_cube(root, reference_ms, factories))
    return reports


def reopened_channel_cube(root, reference_ms, factories):
    """The fresh tool uses the stored frequency axis for a different MS selection."""
    model = root / "definition-channel-casa.im"
    reports = {}
    for label, shift, field in (("other_field", 0., 1), ("other_epoch", 180. * 86400., 0)):
        predictions = []
        for engine, factory in factories.items():
            path = root / f"cube-{label}-{engine}.ms"
            shutil.copytree(reference_ms, path)
            if shift:
                stored = table()
                assert stored.open(str(path), nomodify=False)
                try:
                    for column in ("TIME", "TIME_CENTROID"):
                        stored.putcol(column, stored.getcol(column) + shift)
                finally:
                    stored.close()
            tool = factory()
            assert tool.open(str(path), usescratch=True)
            assert tool.selectvis(field=field)
            assert tool.ft(str(model)) and tool.done()
            predictions.append(samples(path, field=field))
        expected, actual = predictions
        assert np.max(np.abs(expected)) > 1.
        error = relative_rms(actual, expected)
        assert error < 1e-3, (label, error)
        reports[f"channel_cube_{label}_nrms"] = error
    return reports


def virtual_interchange(root, native_bin, case):
    expected = None
    evidence = {}
    for writer in ["casa", "native"]:
        published = root / f"{writer}-{case}.ms"
        fields = [0, 1] if case == "vpvirtual" else [0]
        for reader in ["casa", "native"]:
            destination = root / f"{case}-{writer}-{reader}-uvsub.ms"
            shutil.copytree(published, destination)
            before = [samples(destination, "CORRECTED_DATA", field) for field in fields]
            if reader == "casa":
                assert casatasks.uvsub(str(destination)) is None
            elif native_bin is None:
                from casars.tools import ms

                native = ms()
                assert native.open(str(destination), nomodify=False)
                assert native.uvsub() and native.done()
            else:
                subprocess.run(
                    [str(native_bin), "uvsub", "--vis", str(destination),
                     "--workspace", str(root), "--no-save-last"],
                    check=True,
                )
            actual = [value - samples(destination, "CORRECTED_DATA", field)
                      for value, field in zip(before, fields, strict=True)]
            if expected is None:
                assert np.max(np.abs(actual[0])) > (1.5 if case == "wvirtual" else 1.)
                if case == "vpvirtual":
                    assert np.max(np.abs(actual[1])) > 1.
                expected = actual
            errors = [relative_rms(value, reference)
                      for value, reference in zip(actual, expected, strict=True)]
            evidence[f"{writer}-{reader}"] = {
                "complex_samples": sum(value.size for value in actual),
                "normalized_rms": max(errors),
            }
            print(f"{case} {writer}-{reader}", evidence[f"{writer}-{reader}"], flush=True)
            assert max(errors) < 1e-3, (case, writer, reader, errors)
    return evidence


def run(root, native_test=None, native_bin=None):
    assert casatasks.version_string() == "6.7.6.14"
    root.mkdir(parents=True)
    cases = ["wproject", "grid", "taylor", "changed", "vp", "all-vp", "pointing-vp",
             "dish-vp", "no-vp", "phase-default", "phase-time", "image-distance",
             "definition-distance"]
    custom_vp = None
    if native_test is None:
        from casatools import vpmanager

        custom_vp = root / "gaussian-vp.table"
        tool = vpmanager()
        tool.setpbgauss(telescope="ALMA", halfwidth="40arcsec", maxrad="120arcsec", reffreq="100GHz")
        assert tool.saveastable(str(custom_vp))
        cases.extend(["automatic-w", "boxcar", "custom-vp"])
    for case in [*cases, *VIRTUAL_CASES]:
        for engine in ["casa", "native"]:
            configure_data(root / f"{engine}-{case}.ms", case)
    configure_data(root / "native-defineimage.ms")
    configure_data(root / "casa-defineimage.ms")
    reference_ms = root / "casa-wproject.ms"
    tb = table()
    assert tb.open(str(reference_ms / "SPECTRAL_WINDOW"))
    reference = tb.getcell("CHAN_FREQ", 0)[0]
    tb.close()
    paths = [root / "off-axis.im", root / "term-1.im"]
    for path, flux in zip(paths, [2., .5], strict=True):
        write_model(path, coordinates(reference_ms), reference, flux)
    beam_target = root / "beam-target.im"
    write_model(beam_target, coordinates(reference_ms), reference, 2., cell=2)
    focused_image = root / "focus-target.im"
    shutil.copytree(beam_target, focused_image)
    tool = image()
    assert tool.open(str(focused_image))
    assert tool.setmiscinfo({"distance": 10.})
    assert tool.done()
    images = [str(path) for path in paths]
    factories = {"casa": imager}
    if native_test is None:
        from casars.tools import imager as native

        factories["native"] = native
    for case in [*cases, *VIRTUAL_CASES]:
        if case == "image-distance":
            models = [str(focused_image)]
        elif case in CLOSE_MODEL_CASES:
            models = [str(beam_target)]
        else:
            models = images
        for engine, factory in factories.items():
            prediction(root / f"{engine}-{case}.ms", models, reference, case, factory, custom_vp)
    for engine, factory in factories.items():
        prediction(root / f"{engine}-defineimage.ms", [str(root / f"new-{engine}.im")], reference,
                   "defineimage", factory)
    if native_test is not None:
        subprocess.run(
            [str(native_test), "--ignored", "--exact", "installed_casa_prediction_settings"],
            env=dict(os.environ, CASARS_IMAGER_PREDICTION_WORK=str(root)),
            check=True,
        )
    native_image = empty_image(root / "new-native.im")
    casa_image = empty_image(root / "new-casa.im")
    assert native_image == casa_image, (native_image, casa_image)
    assert relative_rms(samples(root / "casa-wproject.ms"),
                        samples(root / "casa-grid.ms")) > 1e-3
    assert not np.array_equal(samples(root / "casa-taylor.ms"),
                              samples(root / "casa-changed.ms"))
    assert relative_rms(samples(root / "casa-phase-time.ms"),
                        samples(root / "casa-phase-default.ms")) > 0.1
    assert 0.5 < np.max(np.abs(samples(root / "casa-vp.ms"))) < 1.5
    assert relative_rms(samples(root / "casa-pointing-vp.ms"),
                        samples(root / "casa-vp.ms")) > 0.1
    assert relative_rms(samples(root / "casa-dish-vp.ms"),
                        samples(root / "casa-vp.ms")) > 0.1
    assert np.max(np.abs(samples(root / "casa-no-vp.ms"))) > 1.9
    np.testing.assert_array_equal(samples(root / "casa-definition-distance.ms"),
                                  samples(root / "casa-no-vp.ms"))
    assert relative_rms(samples(root / "casa-image-distance.ms"),
                        samples(root / "casa-no-vp.ms")) > 0.1
    evidence = {}
    for case in cases:
        field = None if case == "all-vp" else 0
        expected = samples(root / f"casa-{case}.ms", field=field)
        actual = samples(root / f"native-{case}.ms", field=field)
        error = relative_rms(actual, expected)
        evidence[case] = {
            "complex_samples": actual.size,
            "nonzero": int(np.count_nonzero(expected)),
            "normalized_rms": error,
        }
        print(case, evidence[case], flush=True)
        assert error < 1e-3, (case, error)
    for case in VIRTUAL_CASES:
        evidence[case] = virtual_interchange(root, native_bin, case)
    if native_test is None:
        evidence["image_definitions"] = definition_cases(root, reference_ms, factories)
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-test", type=Path)
    parser.add_argument("--native-bin", type=Path)
    parser.add_argument("--public-python", action="store_true")
    args = parser.parse_args()
    root = args.work.resolve()
    if args.public_python:
        if args.native_test is not None or args.native_bin is not None:
            parser.error("--public-python uses the installed public package")
        evidence = run(root)
    else:
        if args.native_test is None or args.native_bin is None:
            parser.error("supply --public-python or both --native-test and --native-bin")
        evidence = run(root, args.native_test.resolve(), args.native_bin.resolve())
    (root / "results.json").write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    main()
