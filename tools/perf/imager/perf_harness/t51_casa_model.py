# SPDX-License-Identifier: LGPL-3.0-or-later
"""Bounded, diagnostic-only CASA Float representation of Rust model authority."""

from contextlib import ExitStack
import json
from pathlib import Path

import numpy as np

from .t51_model_export import MAX_CHUNK_RECORDS, f64_bits, inspect_export
from .casa_image_compare import coordinate_records_equivalent


def verify_coordinates(manifest, shape, coordinates, csys):
    """Check direction exactly in native form and spectral values by shared policy.

    DirectionCoordinate stores angles internally in degrees and solves the
    celestial pole. Its saved record is not bit-identical to the input record.
    Apply that same native conversion to the authority, never a tolerance or
    a regrid, before comparing it with the image tool's saved coordinate record.
    Spectral numeric fields use the existing coordinate-equivalence contract;
    frames, axis topology, and support are not made approximate.
    """
    domain, = manifest["domains"]
    direction = coordinates["direction0"]
    spectral = coordinates["spectral2"]
    exported_spectral = manifest["spectral"]
    frames = {"Topocentric": "TOPO", "Lsrk": "LSRK", "Barycentric": "BARY", "Rest": "REST"}
    expected = {
        "shape": [*domain["pixels"], 1, 1],
        "system": domain["frame"], "conversionSystem": domain["frame"],
        "projection": "SIN", "projection_parameters": [0.0, 0.0],
        "units": ["rad", "rad"],
        "crval": [f64_bits(x) for x in domain["reference_direction_rad_bits"]],
        "crpix": [f64_bits(x) for x in domain["reference_pixel_bits"]],
        "cdelt": [f64_bits(x) for x in domain["increment_rad_bits"]],
        "pc": [[f64_bits(x) for x in row] for row in domain["pc_bits"]],
        "longpole": f64_bits(domain["pole_deg_bits"][0]),
        "latpole": f64_bits(domain["pole_deg_bits"][1]),
    }
    if domain["projection"] != "Sin" or domain["frame"] != "J2000":
        raise ValueError("unexpected T51 direction frame/projection")
    if not np.array_equal(shape, expected.pop("shape")):
        raise ValueError("authoritative model coordinate mismatch: shape")
    if not csys.fromrecord({**coordinates, "direction0": {**direction, **expected}}):
        raise ValueError("CASA could not represent the authoritative coordinate law")
    canonical_direction = csys.torecord()["direction0"]
    actual = {name: direction[name] for name in expected}
    expected = {name: canonical_direction[name] for name in expected}
    for name, wanted in expected.items():
        if not np.array_equal(actual[name], wanted):
            raise ValueError(f"authoritative model coordinate mismatch: {name}")
    for prefix in ("pixelmap", "worldmap"):
        for ordinal, axes in enumerate(([0, 1], [2], [3])):
            if not np.array_equal(coordinates[f"{prefix}{ordinal}"], axes):
                raise ValueError("model coordinate axis mapping differs")
    if not np.array_equal(coordinates["stokes1"]["stokes"], ["I"]):
        raise ValueError("model polarization differs")
    if (spectral["unit"] != "Hz"
            or spectral["system"] != frames[exported_spectral["output_frame"]]
            or spectral["conversion"]["system"] != spectral["system"]):
        raise ValueError("model spectral frame differs")
    for key, exported in (("crpix", "reference_pixel_bits"),
                          ("crval", "reference_frequency_hz_bits"),
                          ("cdelt", "increment_hz_bits")):
        if not coordinate_records_equivalent(
            float(spectral["wcs"][key]), f64_bits(exported_spectral[exported])
        ):
            raise ValueError(f"model spectral coordinate differs: {key}")


def image_slab(offset, count, shape):
    """Map the x-fastest export stream to a bounded CASA [x,y,pol,freq] slab."""
    width, height = shape
    plane = width * height
    term, within = divmod(offset, plane)
    row, column = divmod(within, width)
    if term not in (0, 1) or column or count % width or within + count > plane:
        raise ValueError("model chunk is not an aligned Taylor image slab")
    rows = count // width
    return term, [0, row, 0, 0], [width - 1, row + rows - 1, 0, 0], (width, rows, 1, 1)


def open_verified(stack, path, manifest, image_factory):
    tool = image_factory()
    stack.callback(tool.done)
    if not tool.open(str(path)):
        raise ValueError(f"cannot open model image: {path}")
    csys = tool.coordsys()
    try:
        coordinates = csys.torecord()
        verify_coordinates(manifest, tool.shape(), coordinates, csys)
    finally:
        csys.done()
    return tool, coordinates


def create_start_images(manifest_path, *, templates, destinations,
                        absolute_budget, relative_budget, image_factory, region_factory):
    """Write isolated Float images; never use template pixels as model authority."""
    manifest = json.loads(Path(manifest_path).read_text())
    shape = manifest["domains"][0]["pixels"]
    if (len(templates) != 2 or len(destinations) != 2
            or len(set(map(str, destinations))) != 2
            or MAX_CHUNK_RECORDS % shape[0]
            or shape[0] * shape[1] % MAX_CHUNK_RECORDS):
        raise ValueError("unexpected fixed-model image/chunk layout")
    if any(Path(path).exists() for path in destinations):
        raise ValueError("starting image destinations must be new")
    with ExitStack() as stack:
        targets = []
        region = region_factory()
        stack.callback(region.done)
        for source, destination in zip(templates, destinations, strict=True):
            template, coordinates = open_verified(stack, source, manifest, image_factory)
            target = image_factory()
            stack.callback(target.done)
            if not target.fromshape(outfile=str(destination), shape=template.shape(),
                                    csys=coordinates, overwrite=False, type="f"):
                raise ValueError("CASA could not create Float starting image")
            if not target.setbrightnessunit("Jy/pixel"):
                raise ValueError("CASA could not set starting model units")
            targets.append(target)

        def consume(offset, values, support):
            term, blc, trc, slab = image_slab(offset, len(values), shape)
            if not targets[term].putregion(
                pixels=values.reshape(slab, order="F"),
                pixelmask=support.astype(bool).reshape(slab, order="F"),
                region=region.box(blc=blc, trc=trc), usemask=False, locking=True,
            ):
                raise ValueError("CASA starting-model write failed")

        result = inspect_export(manifest_path, expected_shape=shape,
                                absolute_budget=absolute_budget, relative_budget=relative_budget,
                                consume=consume)
    result["coordinates_verified"] = True
    return result


def check_physical_images(manifest_path, *, paths, absolute_budget,
                          relative_budget, image_factory):
    """Read actual prediction images after native divide/scatter, before the core."""
    manifest = json.loads(Path(manifest_path).read_text())
    shape = manifest["domains"][0]["pixels"]
    if len(paths) != 2:
        raise ValueError("exactly two physical Taylor images are required")
    with ExitStack() as stack:
        images = [open_verified(stack, path, manifest, image_factory)[0] for path in paths]

        def read(offset, count):
            term, blc, trc, slab = image_slab(offset, count, shape)
            values = images[term].getchunk(blc=blc, trc=trc, dropdeg=False, getmask=False)
            support = images[term].getchunk(blc=blc, trc=trc, dropdeg=False, getmask=True)
            if values.shape != slab or support.shape != slab:
                raise ValueError("CASA physical model read returned a different slab")
            return values.ravel(order="F"), support.ravel(order="F")

        result = inspect_export(manifest_path, expected_shape=shape,
                                absolute_budget=absolute_budget, relative_budget=relative_budget,
                                read_physical=read)
    result["coordinates_verified"] = True
    return result
