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


def configure_data(path, polynomial=False):
    source_data(path, stokes="XX XY YX YY")
    tb = table()
    assert tb.open(str(path), nomodify=False)
    try:
        rows = tb.nrows()
        uvw = np.array([
            np.linspace(-2., 2., rows), np.full(rows, .8), np.linspace(.05, 1.8, rows)
        ])
        tb.putcol("UVW", uvw)
        fields = tb.getcol("FIELD_ID")
        times = tb.getcol("TIME")
    finally:
        tb.close()
    if polynomial:
        assert tb.open(str(path / "FIELD"), nomodify=False)
        try:
            tb.putcell("TIME", 0, float(np.min(times[fields == 0])))
            tb.putcell("NUM_POLY", 0, 1)
            for column in ["PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"]:
                position = tb.getcell(column, 0)[:, 0]
                tb.putcell(column, 0, np.column_stack((position, [2e-5, 0.])))
        finally:
            tb.close()


def prediction(path, images, reference, case):
    cell = 2 if case in {"vp", "no-vp", "all-vp"} else 70
    phase_time = -1.
    if case == "phase-time":
        tb = table()
        assert tb.open(str(path))
        try:
            phase_time = float(tb.getcell("TIME", tb.nrows() - 1))
        finally:
            tb.close()
    tool = imager()
    assert tool.open(str(path), usescratch=case != "wvirtual")
    assert tool.selectvis(field="0,1" if case == "all-vp" else "0")
    assert tool.defineimage(nx=32, ny=32, cellx=f"{cell}arcsec", celly=f"{cell}arcsec",
                            phasecenter=0, outframe="TOPO")
    assert tool.setvp(dovp=case in {"vp", "all-vp"}, telescope="ALMA")
    if case in {"wproject", "wvirtual"}:
        assert tool.setoptions(ftmachine="wproject", padding=1.4, wprojplanes=16, numthreads=1)
        assert tool.ft(model=images[:1])
    elif case in {"taylor", "changed"}:
        assert tool.settaylorterms(2, reffreq=reference * (.99 if case == "taylor" else .98))
        assert tool.ft(model=images)
    else:
        if case == "grid":
            assert tool.setoptions(ftmachine="ft", padding=1.4)
        assert tool.ft(model=images[:1], phasecentertime=phase_time)
    assert tool.done()


def samples(path, virtual=False, all_fields=False):
    tb = table()
    assert tb.open(str(path))
    try:
        fields = tb.getcol("FIELD_ID")
        selected = np.ones_like(fields, dtype=bool) if all_fields else fields == 0
        if virtual:
            return (tb.getcol("DATA") - tb.getcol("CORRECTED_DATA"))[:, :, selected]
        return tb.getcol("MODEL_DATA")[:, :, selected]
    finally:
        tb.close()


def virtual_interchange(root, native_bin):
    expected = None
    evidence = {}
    for writer in ["casa", "native"]:
        published = root / f"{writer}-wvirtual.ms"
        for reader in ["casa", "native"]:
            destination = root / f"{writer}-{reader}-uvsub.ms"
            shutil.copytree(published, destination)
            if reader == "casa":
                assert casatasks.uvsub(str(destination)) is None
            else:
                subprocess.run(
                    [str(native_bin), "uvsub", "--vis", str(destination),
                     "--workspace", str(root), "--no-save-last"],
                    check=True,
                )
            actual = samples(destination, virtual=True)
            if expected is None:
                assert np.max(np.abs(actual)) > 1.5
                expected = actual
            error = relative_rms(actual, expected)
            evidence[f"{writer}-{reader}"] = {
                "complex_samples": actual.size, "normalized_rms": error
            }
            print(f"virtual {writer}-{reader}", evidence[f"{writer}-{reader}"], flush=True)
            assert error < 1e-3, (writer, reader, error)
    return evidence


def run(root, native_test, native_bin):
    assert casatasks.version_string() == "6.7.6.14"
    root.mkdir(parents=True)
    cases = ["wproject", "grid", "taylor", "changed", "vp", "all-vp", "no-vp", "phase-default", "phase-time"]
    for case in [*cases, "wvirtual"]:
        for engine in ["casa", "native"]:
            configure_data(root / f"{engine}-{case}.ms", polynomial=case.startswith("phase-"))
    configure_data(root / "native-defineimage.ms")
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
    images = [str(path) for path in paths]
    for case in [*cases, "wvirtual"]:
        models = [str(beam_target)] if case in {"vp", "no-vp", "all-vp"} else images
        prediction(root / f"casa-{case}.ms", models, reference, case)
    subprocess.run(
        [str(native_test), "--ignored", "--exact", "installed_casa_prediction_settings"],
        env=dict(os.environ, CASARS_IMAGER_PREDICTION_WORK=str(root)),
        check=True,
    )
    model = image()
    assert model.open(str(root / "new-native.im"))
    np.testing.assert_array_equal(model.shape(), [32, 32, 1, 1])
    assert not np.any(model.getchunk())
    model.done()
    assert relative_rms(samples(root / "casa-wproject.ms"),
                        samples(root / "casa-grid.ms")) > 1e-3
    assert not np.array_equal(samples(root / "casa-taylor.ms"),
                              samples(root / "casa-changed.ms"))
    assert relative_rms(samples(root / "casa-phase-time.ms"),
                        samples(root / "casa-phase-default.ms")) > 0.1
    assert 0.5 < np.max(np.abs(samples(root / "casa-vp.ms"))) < 1.5
    assert np.max(np.abs(samples(root / "casa-no-vp.ms"))) > 1.9
    evidence = {}
    for case in cases:
        expected = samples(root / f"casa-{case}.ms", all_fields=case == "all-vp")
        actual = samples(root / f"native-{case}.ms", all_fields=case == "all-vp")
        error = relative_rms(actual, expected)
        evidence[case] = {
            "complex_samples": actual.size,
            "nonzero": int(np.count_nonzero(expected)),
            "normalized_rms": error,
        }
        print(case, evidence[case], flush=True)
        assert error < 1e-3, (case, error)
    evidence["virtual"] = virtual_interchange(root, native_bin)
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-test", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    evidence = run(root, args.native_test.resolve(), args.native_bin.resolve())
    (root / "results.json").write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    main()
