#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Named simulator telescope defaults beyond ALMA/VLA/SMA/ATA/ngVLA."""
import argparse
from pathlib import Path

import numpy as np
from casatasks import version_string
from casatools import componentlist, image, measures

from sm_apply_time_oracle import write
from sm_existing_append_oracle import snapshot
from sm_image_layouts_oracle import earth_orientation
from sm_multiple_models_oracle import CENTRE, make_session, model

# One image and one component calculation per distinct default fit/band.
PROFILES = [("hatcreek", "HATCREEK", 86e9, 6.), ("iram_pdb", "IRAMPDB", 100e9, 15.),
    ("iram_30m", "IRAM30M", 100e9, 15.), ("ovro", "OVRO", 100e9, 10.4),
    ("gmrt", "GMRT", 1.4e9, 45.), ("nrao_12m", "NRAO12M", 86e9, 12.),
    ("nrao_140ft", "NRAO140FT", 1.4e9, 43.), ("gbt", "GBT", 1.4e9, 100.),
    ("wsrt_high", "WSRT", 1.4e9, 25.), ("wsrt_low", "WSRT", .7e9, 25.),
    ("atca_16", "ATCA", 1.4e9, 22.), ("atca_cx", "ATCA", 6e9, 22.),
    ("atca_kq", "ATCA", 20e9, 22.), ("atca_w", "ATCA", 95e9, 22.),
    ("atca_fallback", "ATCA", 60e9, 22.), ("atca_ri", "ATCA_C_RI", 5.5e9, 22.),
    ("bima_refused", "BIMA", 86e9, 6.), ("none_refused", "NONE", 100e9, 12.), ("gbt_outer", "GBT", 1.4e9, 100.),
    *[(f"atca_16_{mhz}", "ATCA", mhz*1e6, 22.) for mhz in (1100, 1700, 2000, 2300, 2600, 2800, 3200)]]


def inputs_for(inputs, name, frequency, diameter):
    scale = 230e9 / frequency * 10.7 / diameter
    if name == "gbt_outer":
        cl = componentlist()
        offset = np.deg2rad(20.39 / 60.) / (frequency / 1e9)
        assert cl.addcomponent(flux=1., freq=f"{frequency}Hz",
            dir=f"J2000 {CENTRE[0]}rad {CENTRE[1]+offset}rad")
        cl.rename(str(inputs / f"{name}.cl"))
        cl.done()
        return
    image_path = inputs / f"{name}.image"
    model(image_path, "a")
    ia = image()
    ia.open(str(image_path))
    cs = ia.coordsys()
    cs.setincrement([-2e-6 * scale, 2e-6 * scale], type="direction")
    cs.setreferencevalue([frequency * 1.03], type="spectral")
    cs.setincrement([frequency * .16], type="spectral")
    assert ia.setcoordsys(cs.torecord())
    cs.done()
    ia.done()
    cl = componentlist()
    for flux, shift in ((1., 0.), (.7, 6e-5 * scale)):
        assert cl.addcomponent(flux=flux, freq=f"{frequency}Hz",
            dir=f"J2000 {CENTRE[0]+shift}rad {CENTRE[1]-shift/2}rad")
    cl.rename(str(inputs / f"{name}.cl"))
    cl.done()


def observatory(telescope):
    me = measures()
    try:
        value = me.observatory(telescope)
        if not value:
            return None
        value = me.measure(value, "ITRF")
        lon, lat, radius = (value[f"m{i}"]["value"] for i in range(3))
        return [float(radius*np.cos(lat)*np.cos(lon)), float(radius*np.cos(lat)*np.sin(lon)),
            float(radius*np.sin(lat))]
    finally:
        me.done()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--profiles", nargs="*")
    args = parser.parse_args()
    assert version_string() == "6.7.6.14"
    root = args.out.resolve()
    inputs = root / "inputs"
    inputs.mkdir(parents=True)
    cases = []
    for profile, telescope, frequency, diameter in PROFILES:
        if args.profiles and profile not in args.profiles:
            continue
        inputs_for(inputs, profile, frequency, diameter)
        location = observatory(telescope)
        for operation in (("components",) if profile == "gbt_outer" else ("image", "components")):
            name = f"{profile}_{operation}"
            path = root / f"{name}.ms"
            # Unshadowed physical 12m dishes isolate the named default fit.
            sm = make_session(path, stokes="XX YY", telescope=telescope,
                dishes=[12.], frequency=frequency, channel=frequency*.02)
            before = snapshot(path)
            assert sm.setvp(dovp=True, dosquint=False)
            accepted = sm.predict(**{"imagename" if operation == "image" else "complist":
                str(inputs / f"{profile}.{'image' if operation == 'image' else 'cl'}")}, incremental=False)
            sm.close()
            after = snapshot(path)
            assert all(np.isfinite(v) for row in after["rows"] for pair in row["data"] for v in pair), name
            assert accepted == (telescope not in {"BIMA", "NRAO140FT", "NONE"}), name
            if profile == "gbt_outer":
                assert not any(v for row in after["rows"] for pair in row["data"] for v in pair), name
            elif accepted:
                assert any(v for row in after["rows"] for pair in row["data"] for v in pair), name
            cases.append(dict(name=name, profile=profile, telescope=telescope,
                frequency_hz=frequency, channel_hz=frequency*.02, operation=operation, accepted=accepted,
                observatory_itrf_m=location, eop=earth_orientation(before),
                native_created=True, all_fields_selected=True, before=before, after=after))
            write(args.fixture, dict(casa_version=version_string(),
                recipe="tools/simulation/sm_default_beams_oracle.py", cases=cases))
            print(name, "passed", flush=True)


if __name__ == "__main__":
    main()
