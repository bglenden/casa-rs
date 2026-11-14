# SPDX-License-Identifier: LGPL-3.0-or-later
"""Capture simulator finite-distance image prediction in CASA 6.7.6.14."""
import argparse
from pathlib import Path

from casatasks import version_string
from casatools import componentlist, image, measures, simulator

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_multiple_models_oracle import CENTRE, make_session, model


CASES = {
    "zero": {},
    "positive": {"distance": 1e6},
    "negative": {"distance": -1e6},
    "last_zero": {"earlier_distance": 1e6},
    "components": {"distance": 1e6, "components": True},
    "multiple": {"distance": 1e6, "multiple": True},
    "metadata": {"metadata_distance": 1e6},
    "wproject": {"distance": 1e6, "machine": "wproject"},
    "mosaic": {"distance": 1e6, "machine": "mosaic"},
    "selected": {"distance": 1e6, "selected": True},
    "stored_metadata": {"metadata_distance": 1e6, "reopen": True},
    "two_distances": {"distance": 1e6, "second_distance": 2e6},
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--case", choices=CASES, required=True)
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    root.mkdir(parents=True)
    spec = CASES[args.case]
    images = [root / "a.image"]
    model(images[0], "a")
    if spec.get("multiple"):
        images.append(root / "b.image")
        model(images[1], "b")
    if "metadata_distance" in spec:
        ia = image()
        ia.open(str(images[0]))
        info = ia.miscinfo()
        info["distance"] = spec["metadata_distance"]
        assert ia.setmiscinfo(info)
        ia.done()
    path = root / "predicted.ms"
    sm = make_session(path)
    me = measures()
    direction = me.direction("J2000", f"{CENTRE[0]}rad", f"{CENTRE[1]}rad")
    if spec.get("earlier_distance"):
        assert sm.setfield(sourcename="earlier", sourcedirection=direction,
                           distance=f"{spec['earlier_distance']}m")
    assert sm.setfield(sourcename="last", sourcedirection=direction,
                       distance=f"{spec.get('distance', 0)}m")
    assert sm.setoptions(ftmachine=spec.get("machine", "ft"), wprojplanes=16)
    if spec.get("reopen"):
        sm.close()
        sm = simulator()
        assert sm.openfromms(str(path))
    if spec.get("selected"):
        assert sm.setdata(fieldid=[0], msselect="ANTENNA1 == 0 && ANTENNA2 > 2")
    components = root / "point.cl"
    if spec.get("components"):
        cl = componentlist()
        assert cl.addcomponent(dir=f"J2000 {CENTRE[0]}rad {CENTRE[1]}rad", flux=.25, freq="230GHz")
        cl.rename(str(components))
        cl.done()
    before = snapshot(path)
    arguments = dict(imagename=[str(p) for p in images])
    if spec.get("components"):
        arguments["complist"] = str(components)
    returned = bool(sm.predict(**arguments))
    if "second_distance" in spec:
        assert sm.setfield(sourcename="second", sourcedirection=direction,
                           distance=f"{spec['second_distance']}m")
        assert sm.predict(**arguments, incremental=True)
    case = dict(name=args.case, operation="image", returned=returned, spec=spec,
                before=before, after=snapshot(path))
    if spec.get("selected"):
        # setdata(fieldid=...) accidentally shrinks nField without updating
        # distance_p. Select the identical rows through TaQL alone to retain
        # the requested focus, independently of casa-rs.
        control = root / "physical-control.ms"
        corrected = make_session(control)
        assert corrected.setfield(sourcename="last", sourcedirection=direction,
                                  distance=f"{spec['distance']}m")
        assert corrected.setdata(fieldid=[], msselect="FIELD_ID == 0 && ANTENNA1 == 0 && ANTENNA2 > 2")
        assert corrected.predict(**arguments)
        case["field_distance_corrected_rows"] = snapshot(control)["rows"]
        corrected.close()
    write(args.fixture, dict(casa_version=version_string(), cases=[case]))
    print(args.case, "returned", returned, flush=True)
    # Mosaic may abort during CASA cleanup; its independently written fixture
    # must already exist, as in sm_multiple_models_oracle.py.
    sm.close()
    me.done()


if __name__ == "__main__":
    main()
