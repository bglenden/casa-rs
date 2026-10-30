#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA's sky-model preparation on small inputs, for casa-rs's port (#841 Phase 5b).

Run with CASA's Python (for example `/Applications/CASA.app/Contents/MacOS/python3 -I`):

    python3 -I tools/simulation/skymodel_oracle.py \\
        --out crates/casa-simulation/tests/data/skymodel

It writes, into `--out`:
- `skymodel_inputs.tar.gz`: the images (CASA images and FITS files) and
  component lists it makes, which casa-rs's tests read as CASA wrote them;
- `skymodel_casa.json.gz`: what CASA makes of them:
  - `simutil.modifymodel` for each input and a set of `inbright`,
    `indirection`, `incell`, `incenter` and `inwidth` overrides: the returned
    tuple, or the exception, and the image written;
  - `simutil.flatimage` of those images, with and without components;
  - `image.modify` (casacore `ComponentImager::project`) of point, Gaussian
    and disk components, elongated and rotated, in other frames, with
    spectra and polarisation, and on images in other units;
  - `simobserve`'s component-only model (`.compskymodel` and its flat
    image), by the lines of `task_simobserve.py` that make it.

Each image is recorded by its shape, brightness unit, restoring beam,
coordinate-system record (`coordsys.torecord()`) and its pixels in Fortran
order.

`--check DIR` instead opens the images casa-rs wrote in DIR (named as the
cases are) and reports where CASA reads a coordinate system or pixels other
than those recorded, apart from casacore's bookkeeping and the documented
departures.

`--evidence CASATESTDATA` records, into `--out`, the evidence cases instead:
casatestdata's ppdisk model (with tutorial-like overrides) and simobserve's
test models and component lists, copied to `--out/inputs`.
casa-rs's tests compare them with `CASA_RS_SKYMODEL_FIXTURE=<out>`:

    python3 -I tools/simulation/skymodel_oracle.py --out EV --evidence /Volumes/home/casatestdata
    CASA_RS_SKYMODEL_FIXTURE=EV CASA_RS_SKYMODEL_OUT=EV/casa-rs \\
        cargo nextest run -p casa-simulation --test skymodel_casa
    python3 -I tools/simulation/skymodel_oracle.py --out EV --check EV/casa-rs
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import re
import shutil
import sys
import tarfile
import tempfile

import numpy as np

import casatools
from casatasks.private.simutil import simutil

# Run with `python3 -I`, which leaves the script's directory off the path.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from skymodel_compare import pixel_problems, record_differences  # noqa: E402

qa = casatools.quanta()
ia = casatools.image()
cl = casatools.componentlist()
po = casatools.imagepol()

# The direction every input is centred near, and its pixel grid.
RA_DEG = 285.1234567
DEC_DEG = -23.4567891
NX, NY = 16, 12
CELL_ARCSEC = 0.25
FREQ_HZ = 230.0e9
CHAN_HZ = 2.0e6


def jsonable(value):
    """`value` with NumPy types as plain lists and numbers, NaN as None."""
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if isinstance(value, np.ndarray):
        return jsonable(value.tolist())
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        value = float(value)
        return None if math.isnan(value) else value
    if isinstance(value, map):
        return jsonable(list(value))
    return value


def float32_values(data):
    """`data` in Fortran order, each value the shortest decimal that reads
    back as the same float32, NaN as None."""
    values = np.asarray(data, dtype=np.float32).flatten(order="F")
    return [None if math.isnan(v) else float(str(v)) for v in values]


def coordinates(spec):
    """A coordinate-system tool built from `spec`.

    `spec["axes"]` lists the pixel axes in order: "lon", "lat", "stokes",
    "spectral" or "linear".
    """
    axes = spec["axes"]
    cs = casatools.coordsys()
    stokes = spec.get("stokes", [])
    cs.newcoordsys(
        direction="lon" in axes,
        spectral="spectral" in axes,
        stokes=stokes,
        linear=axes.count("linear"),
    )
    if "lon" in axes:
        cs.setunits(["rad", "rad"], type="direction", overwrite=True)
        cs.setreferencecode(spec.get("refcode", "J2000"), type="direction", adjust=False)
        cs.setprojection(spec.get("projection", "SIN"))
        lon, lat = spec.get("crval_deg", [RA_DEG, DEC_DEG])
        cs.setreferencevalue([math.radians(lon), math.radians(lat)], type="direction")
        cell = spec.get("cdelt_arcsec", [-CELL_ARCSEC, CELL_ARCSEC])
        cs.setincrement([math.radians(c / 3600.0) for c in cell], type="direction")
        cs.setreferencepixel(spec.get("crpix", [7.3, 5.6]), type="direction")
        if "rotation_deg" in spec:
            angle = math.radians(spec["rotation_deg"])
            xform = np.array(
                [[math.cos(angle), -math.sin(angle)], [math.sin(angle), math.cos(angle)]]
            )
            cs.setlineartransform("direction", xform)
    if "spectral" in axes:
        cs.setspectral(refcode=spec.get("frame", "LSRK"), restfreq=f"{spec.get('restfreq_hz', FREQ_HZ)}Hz")
        cs.setreferencevalue(spec.get("freq_hz", FREQ_HZ), type="spectral")
        cs.setincrement(spec.get("chan_hz", CHAN_HZ), type="spectral")
        cs.setreferencepixel(spec.get("chan_refpix", 0.0), type="spectral")
        if "conversion" in spec:
            # A conversion layer with a real frame: the telescope's position,
            # an epoch and the direction's reference value.
            telescope, epoch, frame = spec["conversion"]
            cs.settelescope(telescope)
            cs.setepoch(casatools.measures().epoch("UTC", epoch))
            assert cs.setconversiontype(spectral=frame)
    return cs


COORDINATE_KEY = re.compile(r"(direction|stokes|spectral|linear)(\d+)$")
AXIS_MAP_KEYS = ("worldmap", "pixelmap", "worldreplace", "pixelreplace")


def coordinate_record(spec):
    """`coordinates(spec)` as a record whose coordinates come in the order
    `spec["axes"]` lists, each over consecutive axes, as an image imported
    from FITS has them.

    `newcoordsys` makes the coordinates in its own order, and a transpose
    would leave them in that order with permuted axis maps. casa-rs reads
    that form with its coordinates renumbered in axis order (#1001), so its
    outputs would number them unlike CASA's, and the record comparisons
    here go by coordinate field name.
    """
    cs = coordinates(spec)
    record = cs.torecord()
    cs.done()
    kinds = {"lon": "direction", "lat": "direction", "stokes": "stokes", "spectral": "spectral", "linear": "linear"}
    order = []
    for axis in spec["axes"]:
        if not order or order[-1] != kinds[axis]:
            order.append(kinds[axis])
    old = {}
    for key in record:
        match = COORDINATE_KEY.match(key)
        if match:
            old[match.group(1)] = int(match.group(2))
    reordered = {key: value for key, value in record.items() if not COORDINATE_KEY.match(key) and not any(key.startswith(prefix) for prefix in AXIS_MAP_KEYS)}
    axis = 0
    for index, kind in enumerate(order):
        before = old[kind]
        reordered[f"{kind}{index}"] = record[f"{kind}{before}"]
        count = len(record[f"worldmap{before}"])
        reordered[f"worldmap{index}"] = np.arange(axis, axis + count, dtype=np.int32)
        reordered[f"pixelmap{index}"] = np.arange(axis, axis + count, dtype=np.int32)
        reordered[f"worldreplace{index}"] = record[f"worldreplace{before}"]
        reordered[f"pixelreplace{index}"] = record[f"pixelreplace{before}"]
        axis += count
    return reordered


def pixels(shape, nan_at=()):
    """Deterministic, distinct pixel values, with NaN at `nan_at`."""
    grid = np.indices(shape, dtype=float)
    weights = [0.7, 1.3, 0.37, 0.11][: len(shape)]
    phase = sum(w * g for w, g in zip(weights, grid))
    values = 1.0e-3 * (1.0 + 0.5 * np.sin(phase))
    centre = tuple(n // 2 for n in shape)
    values[centre] += 4.0e-3
    for position in nan_at:
        values[tuple(position)] = np.nan
    return values.astype(np.float32)


def masked_pixels(shape, masked):
    """The values and mask of `masked`: in its `channel`, every pixel whose
    `x + y` is a multiple of `every` masked and set to `value`; at each of
    `both_at`, every channel masked."""
    values = pixels(shape)
    mask = np.ones(shape, dtype=bool)
    for x in range(shape[0]):
        for y in range(shape[1]):
            if (x + y) % masked["every"] == 0:
                mask[x, y, :, masked["channel"]] = False
                values[x, y, :, masked["channel"]] = masked["value"]
    for x, y in masked["both_at"]:
        mask[x, y, :, :] = False
    return values, mask


def make_image(path, spec):
    shape = spec["shape"]
    values = pixels(shape, spec.get("nan_at", ()))
    mask = None
    if "masked" in spec:
        values, mask = masked_pixels(shape, spec["masked"])
    ia.fromarray(outfile=path, pixels=values, csys=coordinate_record(spec), overwrite=True)
    types = ia.coordsys().axiscoordinatetypes()
    expected = [{"lon": "Direction", "lat": "Direction"}.get(axis, axis.capitalize()) for axis in spec["axes"]]
    assert types == expected, f"{path}: axes {types}, wanted {expected}"
    ia.setbrightnessunit(spec.get("unit", "Jy/pixel"))
    if "beam_arcsec" in spec:
        major, minor, pa = spec["beam_arcsec"]
        ia.setrestoringbeam(major=f"{major}arcsec", minor=f"{minor}arcsec", pa=f"{pa}deg")
    if mask is not None:
        ia.calcmask("T", name="mask0", asdefault=True)
        ia.putregion(pixelmask=mask)
    ia.done()


def describe(path):
    """The image at `path`: shape, unit, beam, coordinate record, pixels and,
    when some pixels are masked, the default mask (both in Fortran order)."""
    if not os.path.exists(path):
        return None
    ia.open(path)
    cs = ia.coordsys()
    data = ia.getchunk()
    mask = ia.getchunk(getmask=True)
    info = {
        "shape": jsonable(ia.shape()),
        "unit": ia.brightnessunit(),
        "beam": jsonable(ia.restoringbeam()),
        "csys": jsonable(cs.torecord()),
        "types": cs.axiscoordinatetypes(),
        "pixels": float32_values(data),
        "masked": int(np.size(mask) - np.count_nonzero(mask)),
    }
    if info["masked"]:
        info["mask"] = [bool(value) for value in np.asarray(mask).flatten(order="F")]
    cs.done()
    ia.close()
    return info


def failure(error):
    return {"type": type(error).__name__, "message": str(error)}


DIRECTION_ONLY = {"axes": ["lon", "lat"], "shape": [NX, NY]}
INPUTS = {
    # Direction, Stokes I, one channel: simobserve's own layout.
    "dsf.im": {"axes": ["lon", "lat", "stokes", "spectral"], "stokes": ["I"], "shape": [NX, NY, 1, 1], "nan_at": [[2, 3, 0, 0]]},
    # FITS order (spectral before Stokes), three channels, TOPO.
    "dfs_cube.im": {"axes": ["lon", "lat", "spectral", "stokes"], "stokes": ["I"], "shape": [NX, NY, 3, 1], "frame": "TOPO", "chan_refpix": 1.0, "nan_at": [[0, 0, 1, 0], [5, 6, 2, 0]]},
    "iquv_cube.im": {"axes": ["lon", "lat", "stokes", "spectral"], "stokes": ["I", "Q", "U", "V"], "shape": [NX, NY, 4, 3]},
    "iquv_1ch.im": {"axes": ["lon", "lat", "stokes", "spectral"], "stokes": ["I", "Q", "U", "V"], "shape": [NX, NY, 4, 1]},
    "plain2d.im": dict(DIRECTION_ONLY),
    "dir_freq.im": {"axes": ["lon", "lat", "spectral"], "shape": [NX, NY, 3], "chan_hz": -CHAN_HZ},
    # A third axis of three planes and no Stokes coordinate: modifymodel
    # labels it IQV.
    "dir_freq_linear.im": {"axes": ["lon", "lat", "spectral", "linear"], "shape": [NX, NY, 2, 3]},
    # Spectral first: a cyclic permutation of simobserve's axes.
    "freq_first.im": {"axes": ["spectral", "lon", "lat", "stokes"], "stokes": ["I"], "shape": [3, NX, NY, 1]},
    "galactic.im": {"axes": ["lon", "lat", "stokes", "spectral"], "stokes": ["I"], "shape": [NX, NY, 1, 1], "refcode": "GALACTIC", "crval_deg": [10.25, 2.5]},
    "ra_right.im": {"axes": ["lon", "lat", "stokes", "spectral"], "stokes": ["I"], "shape": [NX, NY, 1, 1], "cdelt_arcsec": [CELL_ARCSEC, CELL_ARCSEC]},
    "rotated.im": {"axes": ["lon", "lat", "stokes", "spectral"], "stokes": ["I"], "shape": [NX, NY, 1, 1], "rotation_deg": 10.0},
    "linear2d.im": {"axes": ["linear", "linear"], "shape": [NX, NY]},
    "southpole.im": {"axes": ["lon", "lat", "stokes", "spectral"], "stokes": ["I"], "shape": [NX, NY, 1, 1], "crval_deg": [359.99999, -89.5]},
    "iv.im": {"axes": ["lon", "lat", "stokes", "spectral"], "stokes": ["I", "V"], "shape": [NX, NY, 2, 3]},
    "jybeam.im": {"axes": ["lon", "lat", "stokes", "spectral"], "stokes": ["I"], "shape": [NX, NY, 1, 1], "unit": "Jy/beam", "beam_arcsec": [1.0, 0.6, 20.0]},
    "jybeam_nobeam.im": {"axes": ["lon", "lat", "stokes", "spectral"], "stokes": ["I"], "shape": [NX, NY, 1, 1], "unit": "Jy/beam"},
    "kelvin.im": {"axes": ["lon", "lat", "stokes", "spectral"], "stokes": ["I"], "shape": [NX, NY, 1, 1], "unit": "K"},
    "dirstokes.im": {"axes": ["lon", "lat", "stokes"], "stokes": ["I"], "shape": [NX, NY, 1]},
    # LSRD, with an LSRK conversion layer of a real frame (the VLA, an
    # epoch, the image's direction).
    "lsrd.im": {"axes": ["lon", "lat", "stokes", "spectral"], "stokes": ["I"], "shape": [NX, NY, 1, 1], "frame": "LSRD", "conversion": ["VLA", "2025/06/01/12:00", "LSRK"]},
    # Two channels with masked samples (set to 9) and pixels masked in both.
    "masked_cube.im": {"axes": ["lon", "lat", "stokes", "spectral"], "stokes": ["I"], "shape": [NX, NY, 1, 2], "masked": {"channel": 1, "every": 3, "value": 9.0, "both_at": [[2, 2], [5, 7]]}},
}
FITS_INPUTS = {"dfs_cube.fits": "dfs_cube.im", "plain2d.fits": "plain2d.im"}

UNSET = dict(inbright="", indirection="", incell="", incenter="", inwidth="")
MODIFY_CASES = [
    ("dsf", "dsf.im", {}),
    ("dsf_inbright_jy_pixel", "dsf.im", {"inbright": "2mJy/pixel"}),
    ("dsf_inbright_number", "dsf.im", {"inbright": "0.004"}),
    ("dsf_inbright_jy_beam", "dsf.im", {"inbright": "3Jy/beam"}),
    ("dsf_overrides", "dsf.im", {"indirection": "J2000 19h30m00.123 -40d00m00.45", "incell": "0.1arcsec", "incenter": "345GHz", "inwidth": "10MHz"}),
    ("dsf_indirection_b1950", "dsf.im", {"indirection": "B1950 280.0deg -30.0deg"}),
    ("dsf_indirection_list", "dsf.im", {"indirection": ["J2000 10h00m00 +10d00m00"]}),
    ("dsf_indirection_two", "dsf.im", {"indirection": ["J2000 10h00m00 +10d00m00", "J2000 11h00m00 +10d00m00"]}),
    ("dsf_incell_list", "dsf.im", {"incell": ["0.1arcsec", "0.2arcsec"]}),
    ("dsf_incell_mas", "dsf.im", {"incell": "-50mas"}),
    ("dsf_incell_hz", "dsf.im", {"incell": "5GHz"}),
    ("dsf_incenter_negative", "dsf.im", {"incenter": "-5GHz"}),
    ("dsf_inwidth_angle", "dsf.im", {"inwidth": "1arcsec"}),
    ("dsf_incenter_mhz", "dsf.im", {"incenter": "345000.5MHz", "inwidth": "0.5MHz"}),
    ("dfs_cube", "dfs_cube.im", {}),
    ("dfs_cube_inbright", "dfs_cube.im", {"inbright": "1Jy/pixel"}),
    ("iquv_cube", "iquv_cube.im", {}),
    ("iquv_1ch", "iquv_1ch.im", {}),
    ("plain2d", "plain2d.im", {}),
    ("plain2d_spectral", "plain2d.im", {"incenter": "100GHz", "inwidth": "50MHz"}),
    ("dir_freq", "dir_freq.im", {}),
    ("dir_freq_linear", "dir_freq_linear.im", {}),
    ("iv", "iv.im", {}),
    ("freq_first", "freq_first.im", {}),
    ("galactic", "galactic.im", {}),
    ("galactic_indirection", "galactic.im", {"indirection": "J2000 12h00m00 -10d00m00"}),
    ("ra_right", "ra_right.im", {}),
    ("rotated", "rotated.im", {}),
    ("linear2d", "linear2d.im", {}),
    ("linear2d_overrides", "linear2d.im", {"indirection": "J2000 01h00m00 +05d00m00", "incell": "0.3arcsec", "incenter": "90GHz", "inwidth": "1MHz"}),
    ("southpole", "southpole.im", {}),
    ("dirstokes", "dirstokes.im", {}),
    ("fits_cube", "dfs_cube.fits", {}),
    ("fits_plain2d", "plain2d.fits", {"incenter": "100GHz", "inwidth": "50MHz"}),
    # Zero increments, which CASA's coordinate setters refuse.
    ("dsf_incell_zero", "dsf.im", {"incell": "0arcsec"}),
    ("dsf_inwidth_zero", "dsf.im", {"inwidth": "0Hz"}),
]

# name, component list, and the input flattened: the modifymodel output of
# the case of that name, or (third) an input image itself.
FLAT_CASES = [
    ("dsf", None, None),
    ("dfs_cube", None, None),
    ("iquv_cube", None, None),
    ("iquv_1ch", None, None),
    ("dir_freq_linear", None, None),
    ("dsf", "shapes.cl", None),
    ("dfs_cube", "spectral.cl", None),
    # Masked samples, and a Jy/beam image whose beam the flat image keeps.
    ("masked_cube", None, "masked_cube.im"),
    ("jybeam", None, "jybeam.im"),
    ("jybeam", "shapes.cl", "jybeam.im"),
]

COMPONENT_LISTS = {
    # Offsets from the image centre (arcsec) and shapes.
    "shapes.cl": [
        dict(flux=0.5, dx=0.0, dy=0.0, shape="point"),
        dict(flux=0.25, dx=0.83, dy=-0.61, shape="point"),
        dict(flux=1.5, dx=-0.9, dy=0.45, shape="Gaussian", majoraxis="1.2arcsec", minoraxis="0.6arcsec", positionangle="30deg"),
        dict(flux=0.75, dx=1.1, dy=0.2, shape="Gaussian", majoraxis="1.5arcsec", minoraxis="0.5arcsec", positionangle="120deg"),
        dict(flux=2.0, dx=0.3, dy=-0.4, shape="disk", majoraxis="1.4arcsec", minoraxis="0.7arcsec", positionangle="30deg"),
        dict(flux=1.0, dx=-1.2, dy=-0.5, shape="disk", majoraxis="1.0arcsec", minoraxis="0.45arcsec", positionangle="120deg"),
    ],
    "spectral.cl": [
        dict(flux=1.0, dx=0.4, dy=0.3, shape="Gaussian", majoraxis="0.9arcsec", minoraxis="0.7arcsec", positionangle="-15deg", spectrumtype="spectral index", index=-0.8, freq="229.5GHz"),
        dict(flux=[0.6, 0.12, -0.05, 0.02], dx=-0.7, dy=-0.2, shape="point", spectrumtype="spectral index", index=2.0, freq="231GHz"),
    ],
    "polarised.cl": [
        dict(flux=[1.0, 0.2, 0.1, 0.05], dx=0.2, dy=0.1, shape="Gaussian", majoraxis="1.0arcsec", minoraxis="0.8arcsec", positionangle="45deg"),
        dict(flux=[0.4, -0.1, 0.03, -0.02], dx=-0.5, dy=0.6, shape="point"),
    ],
    # A galactic Gaussian and point near the J2000 inputs' centre.
    "galactic.cl": "galactic",
    # Points about a pixel off each edge of the 16 x 12 grid.
    "edges.cl": "edges",
    # A component whose spectrum is in TOPO, on LSRK images.
    "topo.cl": [
        dict(flux=1.0, dx=0.1, dy=-0.1, shape="Gaussian", majoraxis="0.8arcsec", minoraxis="0.8arcsec", positionangle="0deg", spectrumtype="spectral index", index=1.0, freq="TOPO 230GHz"),
    ],
    "rest.cl": [
        dict(flux=1.0, dx=0.1, dy=-0.1, shape="point", spectrumtype="spectral index", index=1.0, freq="REST 230GHz"),
    ],
    # LSRD spectra on lsrd.im, whose conversion layer presents LSRK: a 1 Jy
    # point at the reference frequency and an extended Gaussian.
    "lsrd.cl": [
        dict(flux=1.0, dx=0.0, dy=0.0, shape="point", spectrumtype="spectral index", index=2.0, freq="LSRD 230GHz"),
        dict(flux=0.5, dx=0.6, dy=-0.4, shape="Gaussian", majoraxis="1.0arcsec", minoraxis="0.6arcsec", positionangle="40deg", spectrumtype="spectral index", index=-1.5, freq="LSRD 229GHz"),
    ],
    # simobserve's component-only model: a core and five points over 4".
    "core5ps.cl": [
        dict(flux=0.4, dx=0.0, dy=0.0, shape="Gaussian", majoraxis="0.6arcsec", minoraxis="0.4arcsec", positionangle="60deg", freq="345GHz"),
        dict(flux=0.05, dx=1.5, dy=0.3, shape="point", freq="345GHz"),
        dict(flux=0.04, dx=-1.1, dy=1.6, shape="point", freq="345GHz"),
        dict(flux=0.03, dx=-0.4, dy=-1.9, shape="point", freq="345GHz"),
        dict(flux=0.02, dx=2.0, dy=-1.2, shape="point", freq="345GHz"),
        dict(flux=0.01, dx=-1.8, dy=-0.7, shape="point", freq="345GHz"),
    ],
    "core5ps_index.cl": [
        dict(flux=0.4, dx=0.0, dy=0.0, shape="Gaussian", majoraxis="0.6arcsec", minoraxis="0.4arcsec", positionangle="60deg", freq="345GHz", spectrumtype="spectral index", index=-0.7),
        dict(flux=0.05, dx=1.5, dy=0.3, shape="point", freq="345GHz", spectrumtype="spectral index", index=1.5),
    ],
}

PROJECT_CASES = [
    ("dsf_shapes", "dsf.im", "shapes.cl"),
    ("dfs_cube_spectral", "dfs_cube.im", "spectral.cl"),
    # The same spectra in the image's own frame: LSRK on an LSRK cube, TOPO
    # on the TOPO cube.
    ("iquv_cube_spectral", "iquv_cube.im", "spectral.cl"),
    ("dfs_cube_topo", "dfs_cube.im", "topo.cl"),
    ("iquv_cube_polarised", "iquv_cube.im", "polarised.cl"),
    ("iv_polarised", "iv.im", "polarised.cl"),
    ("dsf_galactic", "dsf.im", "galactic.cl"),
    ("dsf_edges", "dsf.im", "edges.cl"),
    ("ra_right_shapes", "ra_right.im", "shapes.cl"),
    ("dirstokes_spectral", "dirstokes.im", "spectral.cl"),
    ("plain2d_shapes", "plain2d.im", "shapes.cl"),
    ("dsf_topo", "dsf.im", "topo.cl"),
    ("dsf_rest", "dsf.im", "rest.cl"),
    ("lsrd_converted", "lsrd.im", "lsrd.cl"),
    ("jybeam_shapes", "jybeam.im", "shapes.cl"),
    ("jybeam_nobeam_shapes", "jybeam_nobeam.im", "shapes.cl"),
    ("kelvin_shapes", "kelvin.im", "shapes.cl"),
]

COMPONENT_MODEL_CASES = [
    # name, list, compwidth, comp_nchan, psfsize (arcsec)
    ("one_channel", "core5ps.cl", "10MHz", 1, 3.7321),
    ("eight_channels", "core5ps_index.cl", "10MHz", 8, 3.7321),
    ("width_unset", "core5ps.cl", "", 2, 2.5),
    ("shapes", "shapes.cl", "1GHz", 1, 3.0),
    # A zero width, which CASA's coordinate setters refuse.
    ("zero_width", "core5ps.cl", "0Hz", 1, 3.7321),
]


def offset_direction(dx_arcsec, dy_arcsec, refcode="J2000", lon_deg=RA_DEG, lat_deg=DEC_DEG):
    """The direction `dx` east and `dy` north of the inputs' centre."""
    me = casatools.measures()
    centre = me.direction(refcode, f"{lon_deg}deg", f"{lat_deg}deg")
    moved = me.shift(centre, offset=f"{math.hypot(dx_arcsec, dy_arcsec)}arcsec", pa=f"{math.degrees(math.atan2(dx_arcsec, dy_arcsec))}deg")
    lon = qa.formxxx(moved["m0"], format="hms", prec=6)
    lat = qa.formxxx(moved["m1"], format="dms", prec=6)
    if refcode == "GALACTIC":
        lon = qa.formxxx(moved["m0"], format="deg", prec=8) + "deg"
        lat = qa.formxxx(moved["m1"], format="deg", prec=8) + "deg"
    return f"{refcode} {lon} {lat}"


def image_centre():
    """The J2000 direction of pixel (NX/2, NY/2) of the J2000 inputs."""
    ia.open(INPUT_DIR + "/dsf.im")
    world = ia.toworld([NX / 2, NY / 2], "m")["measure"]["direction"]
    ia.close()
    return world


def write_component_list(path, entries):
    cl.done()
    if entries == "galactic":
        me = casatools.measures()
        centre = me.measure(image_centre(), "GALACTIC")
        lon, lat = math.degrees(centre["m0"]["value"]), math.degrees(centre["m1"]["value"])
        entries = [
            dict(flux=1.0, dx=0.6, dy=-0.3, shape="Gaussian", majoraxis="1.1arcsec", minoraxis="0.5arcsec", positionangle="75deg", frame=("GALACTIC", lon, lat)),
            dict(flux=0.5, dx=-0.8, dy=0.4, shape="point", frame=("GALACTIC", lon, lat)),
        ]
    if entries == "edges":
        # Points 0.7 pixel beyond the left and right edges, and 0.3 pixel
        # inside the bottom edge. The pixel centres are found from the image.
        ia.open(INPUT_DIR + "/dsf.im")
        cs = ia.coordsys()
        entries = []
        for label, pixel, flux in (("left", [-0.7, 5.0], 0.3), ("right", [NX - 0.3, 7.0], 0.2), ("bottom", [9.0, -0.3], 0.1)):
            world = cs.toworld(pixel + [0, 0], "m")["measure"]["direction"]
            entries.append(dict(flux=flux, shape="point", dir=world, label=label))
        cs.done()
        ia.close()
    for entry in entries:
        if "dir" in entry:
            direction = entry["dir"]
        elif "frame" in entry:
            refcode, lon, lat = entry["frame"]
            direction = offset_direction(entry["dx"], entry["dy"], refcode, lon, lat)
        else:
            centre = image_centre()
            lon, lat = math.degrees(centre["m0"]["value"]), math.degrees(centre["m1"]["value"])
            direction = offset_direction(entry["dx"], entry["dy"], "J2000", lon, lat)
        args = dict(flux=entry["flux"], fluxunit="Jy", polarization="Stokes", dir=direction, shape=entry["shape"], freq=entry.get("freq", "230GHz"), label=entry.get("label", ""))
        if entry["shape"] != "point":
            args.update(majoraxis=entry["majoraxis"], minoraxis=entry["minoraxis"], positionangle=entry["positionangle"])
        if "spectrumtype" in entry:
            args.update(spectrumtype=entry["spectrumtype"], index=entry["index"])
        cl.addcomponent(**args)
    cl.rename(path)
    cl.done()


def run_modify(work, name, inimage, overrides):
    util = simutil()
    params = dict(UNSET, **overrides)
    outimage = os.path.join(work, name + ".skymodel")
    case = {"name": name, "input": inimage, "params": params}
    try:
        result = util.modifymodel(os.path.join(INPUT_DIR, inimage), outimage, params["inbright"], params["indirection"], params["incell"], params["incenter"], params["inwidth"], -1, flatimage=False)
        case["result"] = jsonable(result)
    except Exception as error:  # noqa: BLE001 - CASA raises plain Exceptions
        case["error"] = failure(error)
    case["image"] = describe(outimage)
    return case


def run_flat(work, name, complist, image=None):
    """flatimage of the modifymodel output `name`, or of a copy of the input
    `image` when one is given."""
    util = simutil()
    model = os.path.join(work, name + ".skymodel")
    case = {"name": name, "complist": complist}
    if image:
        model = os.path.join(work, f"{name}+{complist}.input" if complist else f"{name}.input")
        if os.path.exists(model):
            shutil.rmtree(model)
        shutil.copytree(os.path.join(INPUT_DIR, image), model)
        case["input"] = image
    flat = model + ".flat"
    if os.path.exists(flat):
        shutil.rmtree(flat)
    try:
        util.flatimage(model, complist=os.path.join(INPUT_DIR, complist) if complist else "", verbose=False)
    except Exception as error:  # noqa: BLE001
        case["error"] = failure(error)
    case["image"] = describe(flat)
    if os.path.exists(flat):
        shutil.rmtree(flat)
    return case


def run_project(work, name, image, complist):
    target = os.path.join(work, name + ".im")
    shutil.copytree(os.path.join(INPUT_DIR, image), target)
    case = {"name": name, "input": image, "complist": complist}
    try:
        cl.open(os.path.join(INPUT_DIR, complist))
        record = cl.torecord()
        cl.done()
        ia.open(target)
        ia.modify(record, subtract=False)
        ia.close()
    except Exception as error:  # noqa: BLE001
        ia.close()
        case["error"] = failure(error)
    case["image"] = describe(target)
    return case


def run_component_model(work, name, complist, compwidth, comp_nchan, psfsize):
    """`task_simobserve.py`'s component-only model, line for line."""
    util = simutil()
    complist = os.path.join(INPUT_DIR, complist)
    case = {"name": name, "complist": os.path.basename(complist), "compwidth": compwidth, "comp_nchan": comp_nchan, "psfsize": psfsize}
    try:
        compdirs = []
        cl.done()
        cl.open(complist)
        for i in range(cl.length()):
            compdirs.append(util.dir_m2s(cl.getrefdir(i)))
        model_refdir, coffs = util.average_direction(compdirs)
        model_specrefval = cl.getspectrum(0)["frequency"]["m0"]
        if util.isquantity(compwidth, halt=False):
            model_width = compwidth
        else:
            model_width = "2GHz"
        model_nchan = comp_nchan
        model_specrefpix = 0.5 * (comp_nchan - 1)
        model_width = qa.div(model_width, model_nchan)
        model_stokes = "I"
        cmax = 0.0014
        for i in range(coffs.shape[1]):
            xc = np.absolute(coffs[0, i])
            yc = np.absolute(coffs[1, i])
            if xc > cmax:
                cmax = xc
            if yc > cmax:
                cmax = yc
        model_size = ["%fdeg" % (3 * cmax), "%fdeg" % (3 * cmax)]
        model_cell = [qa.quantity(str(psfsize / 20) + "arcsec"), qa.quantity(str(psfsize / 20) + "arcsec")]
        case["parameters"] = jsonable(dict(model_refdir=model_refdir, model_specrefval=model_specrefval, model_width=model_width, model_nchan=model_nchan, model_specrefpix=model_specrefpix, model_stokes=model_stokes, model_size=model_size, model_cell=model_cell, compdirs=compdirs))
        newmodel = os.path.join(work, name + ".compskymodel")
        modimsize = int((qa.convert(model_size[0], "arcsec")["value"]) / (qa.convert(model_cell[0], "arcsec")["value"]))
        case["parameters"]["modimsize"] = modimsize
        newepoch, newlat, newlon = util.direction_splitter(model_refdir)
        csmodel = ia.newimagefromshape(newmodel, [modimsize, modimsize, 1, 1])
        modelcsys = csmodel.coordsys()
        cell0_rad = qa.convert(model_cell[0], "rad")["value"]
        cell1_rad = qa.convert(model_cell[1], "rad")["value"]
        modelcsys.setdirection(refpix=[modimsize / 2, modimsize / 2], refval=[qa.tos(newlat), qa.tos(newlon)], refcode=newepoch)
        modelcsys.setincrement([-cell0_rad, cell1_rad], "direction")
        modelcsys.setreferencevalue(type="spectral", value=qa.convert(model_specrefval, "Hz")["value"])
        modelcsys.setreferencepixel(type="spectral", value=model_specrefpix)
        modelcsys.setrestfrequency(model_specrefval)
        modelcsys.setincrement(type="spectral", value=compwidth)
        csmodel.setcoordsys(modelcsys.torecord())
        modelcsys.done()
        cl.open(complist)
        csmodel.setbrightnessunit("Jy/pixel")
        csmodel.modify(cl.torecord(), subtract=False)
        cl.done()
        csmodel.done()
        util.flatimage(newmodel, verbose=False)
    except Exception as error:  # noqa: BLE001
        case["error"] = failure(error)
    finally:
        # A refusal leaves the list and the image open; CASA aborts at exit
        # on a table removed while open.
        cl.done()
        if "csmodel" in locals():
            csmodel.done()
    case["image"] = describe(os.path.join(work, name + ".compskymodel"))
    case["flat"] = describe(os.path.join(work, name + ".compskymodel.flat"))
    return case


def make_inputs():
    if os.path.exists(INPUT_DIR):
        shutil.rmtree(INPUT_DIR)
    os.makedirs(INPUT_DIR)
    for name, spec in INPUTS.items():
        make_image(os.path.join(INPUT_DIR, name), spec)
    for fits, image in FITS_INPUTS.items():
        ia.open(os.path.join(INPUT_DIR, image))
        ia.tofits(os.path.join(INPUT_DIR, fits), velocity=False, history=False, overwrite=True)
        ia.close()
    for name, entries in COMPONENT_LISTS.items():
        write_component_list(os.path.join(INPUT_DIR, name), entries)


def pack_inputs(out):
    """`inputs/` as `skymodel_inputs.tar.gz`, without the tables' lock files
    (which are not part of an image)."""
    for root, _, files in os.walk(INPUT_DIR):
        for file in files:
            if file == "table.lock":
                os.remove(os.path.join(root, file))
    with tarfile.open(os.path.join(out, "skymodel_inputs.tar.gz"), "w:gz") as archive:
        for name in sorted(os.listdir(INPUT_DIR)):
            archive.add(os.path.join(INPUT_DIR, name), arcname=f"inputs/{name}")


KINDS = ("modify", "flat", "project", "component_model")

# The evidence cases (`--evidence`): casatestdata's models, read from a
# casatestdata clone and recorded with their inputs as a directory rather
# than checked in. The ppdisk model with tutorial-like overrides (the VLA
# tutorial's 44 GHz, 128 MHz and 3e-5 Jy/pixel, with a direction and cell
# chosen here), and CASA's own simobserve test models and component lists
# with its overrides.
EVIDENCE_INPUTS = {
    "input50pc_672GHz.fits": "regression/sim_components_and_skymodel/input50pc_672GHz.fits",
    "core5ps.skymodel": "unittest/simobserve/core5ps.skymodel",
    "core5ps.skymodel4mod": "unittest/simobserve/core5ps.skymodel4mod",
    "flatone.model": "unittest/simobserve/flatone.model",
    "core5ps.clist": "unittest/simobserve/core5ps.clist",
    "ps5.clist": "unittest/simobserve/ps5.clist",
    "point.cl": "unittest/simobserve/point.cl",
}
EVIDENCE_MODIFY = [
    ("ppdisk_vla", "input50pc_672GHz.fits", {"inbright": "3e-5Jy/pixel", "indirection": "J2000 18h00m00.031s -22d59m59.6s", "incell": "0.1arcsec", "incenter": "44GHz", "inwidth": "128MHz"}),
    ("ppdisk", "input50pc_672GHz.fits", {"incenter": "672GHz", "inwidth": "8GHz"}),
    ("core5ps", "core5ps.skymodel", {}),
    ("core5ps_test", "core5ps.skymodel4mod", {"inbright": "5.95565834e-05Jy/pixel", "indirection": "J2000 19h00m00 -23d00m00", "incell": "0.043080964943481216arcsec", "incenter": "345GHz", "inwidth": "10MHz"}),
    ("flatone", "flatone.model", {}),
]
EVIDENCE_FLAT = [("ppdisk_vla", None, None), ("core5ps", "core5ps.clist", None), ("core5ps_test", "ps5.clist", None), ("flatone", None, None)]
EVIDENCE_PROJECT = [("core5ps_point", "core5ps.skymodel", "point.cl"), ("core5ps_ps5", "core5ps.skymodel", "ps5.clist")]
# Component lists, widths and channels as simobserve's tests give them; the
# PSF is approxBeam's for alma.out01 at the list's frequency, as simobserve
# finds it.
EVIDENCE_COMPONENT_MODEL = [
    ("core5ps_one", "core5ps.clist", "10MHz", 1),
    ("core5ps_eight", "core5ps.clist", "10MHz", 8),
    ("ps5", "ps5.clist", "8GHz", 1),
]


def approx_psf(complist):
    """simobserve's PSF size for alma.out01 at `complist`'s first frequency."""
    util = simutil()
    cl.open(complist)
    frequency = qa.convert(cl.getspectrum(0)["frequency"]["m0"], "GHz")["value"]
    cl.done()
    config = casatools.ctsys.resolve("alma/simmos/alma.out01.cfg")
    return util.approxBeam(config, frequency)


def run_cases(work, modify, flat, project, component_model):
    record = {"casa": casatools.version_string(), **{kind: [] for kind in KINDS}}
    for name, image, overrides in modify:
        record["modify"].append(run_modify(work, name, image, overrides))
    for name, complist, image in flat:
        record["flat"].append(run_flat(work, name, complist, image))
    for name, image, complist in project:
        record["project"].append(run_project(work, name, image, complist))
    for name, complist, compwidth, comp_nchan, psfsize in component_model:
        record["component_model"].append(run_component_model(work, name, complist, compwidth, comp_nchan, psfsize))
    return record


def write_record(out, record):
    with gzip.open(os.path.join(out, "skymodel_casa.json.gz"), "wt") as handle:
        json.dump(record, handle, separators=(",", ":"))
        handle.write("\n")
    failures = [c["name"] for kind in KINDS for c in record[kind] if "error" in c]
    print(f"recorded {sum(len(record[k]) for k in KINDS)} cases; CASA raised in {failures}")


def oracle(out, work):
    make_inputs()
    record = run_cases(work, MODIFY_CASES, FLAT_CASES, PROJECT_CASES, COMPONENT_MODEL_CASES)
    record["inputs"] = INPUTS
    pack_inputs(out)
    write_record(out, record)


def evidence(out, work, casatestdata):
    """The evidence cases, with their inputs copied from `casatestdata` to
    `out/inputs`, for `CASA_RS_SKYMODEL_FIXTURE=out` to compare."""
    if os.path.exists(INPUT_DIR):
        shutil.rmtree(INPUT_DIR)
    os.makedirs(INPUT_DIR)
    for name, path in EVIDENCE_INPUTS.items():
        source = os.path.join(casatestdata, path)
        (shutil.copytree if os.path.isdir(source) else shutil.copy)(source, os.path.join(INPUT_DIR, name))
    component_model = [(name, complist, compwidth, nchan, approx_psf(os.path.join(INPUT_DIR, complist)))
                       for name, complist, compwidth, nchan in EVIDENCE_COMPONENT_MODEL]
    record = run_cases(work, EVIDENCE_MODIFY, EVIDENCE_FLAT, EVIDENCE_PROJECT, component_model)
    write_record(out, record)


# casacore's bookkeeping, which casa-rs does not reproduce: the replacement
# values recorded for axes never removed (CASA keeps `ia.fromshape`'s
# defaults), and the telescope position casacore looks up for the name.
BOOKKEEPING = ("worldreplace", "pixelreplace", "telescopeposition")

# The documented departures (`casa_simulation::simutil::skymodel`): where a
# difference is expected, and what tolerance the pixels take.
DEPARTURES = {
    ("modify", "dir_freq_linear"): {"csys": ["stokes1.stokes"]},
    ("modify", "iv"): {"csys": ["stokes1.stokes"]},
    ("component_model", "width_unset"): {"csys": ["spectral2.wcs.cdelt"]},
    ("project", "dsf_topo"): {"pixels": 2e-4},
    ("project", "dfs_cube_spectral"): {"pixels": 2e-4},
}


def output_name(kind, case):
    """The name casa-rs's test writes a case's image under: the case's, and
    for a flat image made with components, the list's too."""
    if kind == "flat" and case.get("complist"):
        return f"{case['name']}+{case['complist']}"
    return case["name"]


def check(out, directory):
    """Read the images casa-rs wrote in `directory/<kind>/<case>` in CASA and
    report where they differ from what CASA made, apart from casacore's
    bookkeeping and the documented departures. Every image CASA made for a
    case it did not refuse must be there."""
    with gzip.open(os.path.join(out, "skymodel_casa.json.gz"), "rt") as handle:
        record = json.load(handle)
    problems = []
    departures = []
    compared = 0
    for kind in KINDS:
        for case in record[kind]:
            if "error" in case:
                continue
            expected_departures = DEPARTURES.get((kind, case["name"]), {})
            for key in ("image", "flat"):
                expected = case.get(key)
                if expected is None:
                    continue
                name = output_name(kind, case)
                flat = key == "flat" or kind == "flat"
                where = f"{kind}/{name}" + (".flat" if flat else "")
                path = os.path.join(directory, kind, name + (".flat" if flat else ""))
                if not os.path.exists(path):
                    problems.append(f"{where}: missing from {directory}")
                    continue
                compared += 1
                actual = describe(path)
                for field in ("shape", "unit", "types", "beam"):
                    if actual[field] != expected[field]:
                        problems.append(f"{where}: {field} {actual[field]}, CASA {expected[field]}")

                def report(problem, expected_csys=expected_departures.get("csys", [])):
                    expected_here = any(f" csys.{field}" in problem for field in expected_csys)
                    (departures if expected_here else problems).append(problem)

                record_differences(actual["csys"], expected["csys"], f"{where} csys", report, BOOKKEEPING)
                found, worst = pixel_problems(
                    where, actual["pixels"], expected["pixels"], actual.get("mask"), expected.get("mask"))
                print(f"{where}: shape {actual['shape']}, largest pixel difference {worst:.1e} of the peak")
                tolerance = expected_departures.get("pixels")
                for problem in found:
                    within = tolerance is not None and "differ by" in problem and worst <= tolerance
                    (departures if within else problems).append(problem)
    if compared == 0:
        problems.append(f"no images compared in {directory}")
    for departure in departures:
        print(f"documented departure: {departure}")
    for problem in problems:
        print(problem)
    print(f"{compared} images read; {len(departures)} documented departures; {len(problems)} other differences")
    return len(problems)


def main():
    global INPUT_DIR
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True)
    parser.add_argument("--work", default=None, help="scratch directory for CASA's outputs")
    parser.add_argument("--check", default=None, help="a directory of casa-rs outputs to read in CASA")
    parser.add_argument("--evidence", default=None, help="a casatestdata clone: record the evidence cases instead")
    args = parser.parse_args()
    if args.check:
        sys.exit(1 if check(args.out, args.check) else 0)
    work = tempfile.mkdtemp(dir=args.work)
    INPUT_DIR = os.path.join(args.out if args.evidence else work, "inputs")
    try:
        if args.evidence:
            evidence(args.out, work, args.evidence)
        else:
            oracle(args.out, work)
    finally:
        shutil.rmtree(work)


INPUT_DIR = ""

if __name__ == "__main__":
    main()
