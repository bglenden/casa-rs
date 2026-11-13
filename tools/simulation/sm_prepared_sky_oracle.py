# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA sky overrides followed by selected simulator image prediction."""
import argparse
import gzip
import json
from pathlib import Path
import shutil

import numpy as np
from casatasks import version_string
from casatasks.private.simutil import simutil
from casatools import componentlist, image, simulator, table

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_multiple_models_oracle import CENTRE, make_session

CASES = {
    "brightness_cube": dict(source="canonical", inbright="2Jy/pixel"),
    "coordinate_spectral_cube": dict(source="canonical", inbright="0.8Jy/pixel",
        indirection="J2000 10h00m00.003 +34d00m00.04", incell="0.5arcsec",
        incenter="230.03125GHz", inwidth="31.25MHz"),
    "combined_stokes_cube": dict(source="iquv", inbright="2Jy/pixel",
        indirection="J2000 10h00m00.003 +34d00m00.04", incell="0.5arcsec",
        incenter="230.03125GHz", inwidth="31.25MHz", components=True),
}


def prepared_image(path):
    ia = image()
    ia.open(str(path))
    cs = ia.coordsys()
    shape = ia.shape().tolist()
    points = [[0, 0, 0, 0], [32, 32, 0, 2], [33, 31, shape[2]-1, 3]]
    result = dict(shape=shape, unit=ia.brightnessunit(), units=cs.units(),
        pixels=ia.getchunk().flatten(order="F").astype(float).tolist(),
        worlds=[dict(pixel=p, world=cs.toworld(p)["numeric"].tolist()) for p in points])
    cs.done()
    ia.done()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--check", type=Path, help="CASA C++ reads native prepared images")
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    if args.check:
        with gzip.open(args.fixture, "rt") as stream:
            cases = json.load(stream)["cases"]
        for case in cases:
            for prefix in ("w1", "w3", "metal3"):
                path = args.check/f"{prefix}-{case['name']}.image"
                if prefix == "metal3" and not path.exists():
                    continue
                actual, expected = prepared_image(path), case["prepared"]
                for key in ("shape", "unit", "units"):
                    assert actual[key] == expected[key], (path, key)
                peak = max(abs(v) for v in expected["pixels"])
                np.testing.assert_allclose(actual["pixels"], expected["pixels"], rtol=0., atol=1e-6*peak)
                for observed, reference in zip(actual["worlds"], expected["worlds"]):
                    np.testing.assert_allclose(observed["world"], reference["world"], rtol=1e-12, atol=1e-12)
                print("CASA C++ reads", path.name, "pixels/coordinates agree", flush=True)
        return
    if not args.out:
        parser.error("--out is required without --check")
    root = args.out.resolve()
    root.mkdir(parents=True)
    inputs = root/"inputs"
    inputs.mkdir()
    base = inputs/"base.ms"
    sm = make_session(base)
    sm.close()
    tb = table()
    tb.open(str(base), nomodify=False)
    for row in range(tb.nrows()):
        shape = tb.getcell("DATA", row).shape
        for name, value in (("DATA", 3+4j), ("MODEL_DATA", 7-2j), ("CORRECTED_DATA", -5+6j)):
            tb.putcell(name, row, np.full(shape, value, dtype=np.complex64))
    tb.close()
    source = Path(__file__).resolve().parents[2]/"crates/casa-simulation/tests/data/sm_image_layouts"
    cases = []
    for name, spec in CASES.items():
        work = root/name
        work.mkdir()
        model = work/"prepared.image"
        overrides = {key: spec.get(key, "") for key in ("inbright", "indirection", "incell", "incenter", "inwidth")}
        result = simutil().modifymodel(str(source/(spec["source"]+".image")), str(model),
            overrides["inbright"], overrides["indirection"], overrides["incell"],
            overrides["incenter"], overrides["inwidth"], -1, flatimage=False)
        assert result is not False
        prepared = prepared_image(model)
        path = work/"predicted.ms"
        shutil.copytree(base, path, ignore=shutil.ignore_patterns("table.lock"))
        sm = simulator()
        assert sm.openfromms(str(path))
        assert sm.setdata(fieldid=[0])
        arguments = dict(imagename=str(model))
        if spec.get("components"):
            cl = componentlist()
            assert cl.addcomponent(dir=f"J2000 {CENTRE[0]}rad {CENTRE[1]}rad",
                flux=[.25, .05, -.02, .01], freq="230GHz", polarization="Stokes")
            component = work/"point.cl"
            cl.rename(str(component))
            cl.done()
            arguments["complist"] = str(component)
        assert sm.predict(**arguments)
        sm.close()
        cases.append(dict(name=name, spec=spec, overrides=overrides, prepared=prepared,
            before=snapshot(base), after=snapshot(path)))
        print(name, "predicted", flush=True)
    write(args.fixture, dict(casa_version=version_string(), cases=cases))


if __name__ == "__main__":
    main()
