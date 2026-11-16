#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Regenerate CASA 6.7.6.14 multi-array and total-power simanalyze evidence.

Run with CASA's Python and --out target/simanalyze-multi-oracle. The only
task modification is the documented full-precision fidelity floor control.
"""
import argparse
import gzip
import json
import os
from pathlib import Path
import shutil
import tarfile

os.environ.setdefault("MPLBACKEND", "Agg")
from simobserve_graphics_oracle import inputs, image_record
from simanalyze_task_oracle import corrected_simanalyze


def record_image(path):
    from casatools import image
    result = image_record(path)
    ia = image()
    ia.open(str(path))
    cs = ia.coordsys()
    result.update(mask=ia.getchunk(getmask=True).tolist(), telescope=cs.telescope(), observer=cs.observer())
    cs.done()
    ia.done()
    return result


def physical_sf_beam():
    """Use CASA's actual C++ gridder kernel in its beam estimator.

    The accepted Phase 6c correction replaces sdbeamutil's approximate SciPy
    spheroid with the kernel SDGrid uses, including its float table lookup.
    No casa-rs code or output participates in this independent control.
    """
    import ctypes
    import casatools
    import numpy as np
    from casatasks.private import sdbeamutil

    library = next((Path(casatools.__file__).parent / "__casac__" / "lib").glob("libcasa_scimath_f.so*"))
    grdsf = ctypes.CDLL(str(library)).grdsf_
    grdsf.argtypes = [ctypes.POINTER(ctypes.c_double), ctypes.POINTER(ctypes.c_double)]
    grdsf.restype = None

    def kernel(self, axis, convsupport, cell_size):
        support = 3 if convsupport == -1 else convsupport
        table = np.zeros(100 * (2 * support + 1), dtype=np.float32)
        for index in range(100 * support):
            nu, value = ctypes.c_double(index / (100 * support)), ctypes.c_double()
            grdsf(ctypes.byref(nu), ctypes.byref(value))
            table[index] = (1 - nu.value**2) * value.value
        indices = (np.abs(axis) / cell_size * 100).astype(int)
        return np.where(indices < len(table), table[np.minimum(indices, len(table)-1)], 0).astype(float)

    sdbeamutil.TheoreticalBeam.get_sf_kernel = kernel


def main():
    from casatasks import simobserve, version_string

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--legacy-sf", action="store_true", help="retain CASA's documented approximate beam")
    args = parser.parse_args()
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=True)
    os.chdir(root)
    assert version_string() == "6.7.6.14"
    for name in ["multi", "tp", "seed", "feather"]:
        shutil.rmtree(root / name, ignore_errors=True)
    model, config = inputs(root)
    # Single-dish prediction requires a model wider than 2.5 primary beams.
    import numpy as np
    from casatools import image
    ia = image()
    ia.open(str(model))
    cs = ia.coordsys()
    ia.done()
    cs.setreferencepixel([96, 96], type="direction")
    pixels = np.zeros((192, 192, 1, 1), dtype=np.float32)
    pixels[96, 96, 0, 0], pixels[105, 92, 0, 0] = 1.25, 0.75
    ia.fromarray(outfile=str(model), pixels=pixels, csys=cs.torecord(), overwrite=True)
    ia.setbrightnessunit("Jy/pixel")
    ia.done()
    cs.done()
    compact = root / "compact.cfg"
    compact.write_text(config.read_text().replace("12.0 A", "7.0 A").replace("observatory=ALMA", "observatory=ACA"))
    common = dict(project="multi", skymodel=str(model), integration="10s",
                  totaltime="40s", thermalnoise="", graphics="none", overwrite=True)
    for antenna in [config, compact]:
        simobserve(**common, antennalist=str(antenna), mapsize="36arcsec")
    simobserve(**dict(common, integration="1s", totaltime="180s"),
               obsmode="sd", sdantlist=str(config), sdant=0,
               mapsize=["36arcsec", "36arcsec"], maptype="square", pointingspacing="4arcsec")
    for name in ["tp", "seed", "feather"]:
        destination = root / name
        destination.mkdir(exist_ok=True)
        for path in (root / "multi").iterdir():
            include = path.name.endswith((".ms", ".skymodel"))
            if name == "tp":
                include = path.name.endswith(".sd.ms") or path.name == "multi.small.skymodel"
            if include:
                shutil.copytree(path, destination / path.name.replace("multi.", name+".", 1))
    archive = root / "simanalyze_multi_inputs.tar.gz"
    with tarfile.open(archive, "w:gz") as output:
        for name in ["multi", "tp", "seed", "feather"]:
            for path in sorted((root / name).iterdir()):
                if path.name.endswith((".ms", ".skymodel")):
                    output.add(path, arcname=str(path.relative_to(root)))
    # CASA 6.7.6.14 left a CASA 5 factory call in get_concatweights.
    # Supply only that missing factory; retain the upstream weight algorithm.
    import importlib
    from casatools import table, msmetadata
    module = importlib.import_module("casatasks.private.task_simanalyze")
    assert "gentools" not in module.__dict__
    module.gentools = lambda names: [{"tb": table, "msmd": msmetadata}[name]() for name in names]
    if not args.legacy_sf:
        physical_sf_beam()
    if not callable(module.feather):
        from casatasks import feather
        module.feather = feather
    task, defaults = corrected_simanalyze()
    cases = {
        "multi": dict(project="multi", vis="multi.small.ms,multi.compact.ms",
                      imsize=[128, 128], niter=20, threshold="1e-7Jy"),
        "tp": dict(project="tp", vis="default", imsize=[128, 128]),
        "seed": dict(project="seed", vis="seed.small.ms,seed.small.sd.ms", imsize=[128, 128],
                     modelimage="$project.sd.image"),
        "feather": dict(project="feather", vis="feather.small.ms", imsize=[128, 128],
                        featherimage="tp/tp.sd.image"),
    }
    result = dict(casa_version=version_string(), fidelity_control="full-precision floor",
                  concat_control="restore missing gentools factory", feather_control="restore public callable shadowed by simutil module",
                  sf_beam_control="legacy approximation" if args.legacy_sf else "CASA C++ SDGrid kernel", cases={})
    for name, parameters in cases.items():
        parameters = dict(defaults, **dict(parameters, analyze=True, graphics="none", overwrite=True))
        task(**parameters)
        # CASA relabels its TP seed in place; the accepted native task keeps
        # that input unchanged. Restore only the reference's known label.
        if name == "seed":
            from casatools import image
            ia = image()
            ia.open(str(root / name / (name+".sd.image")))
            assert ia.brightnessunit() == "Jy/pixel"
            ia.setbrightnessunit("Jy/beam")
            ia.done()
        images = {str(path.relative_to(root / name)): record_image(path)
                  for path in sorted((root / name).iterdir())
                  if path.is_dir() and (path / "table.dat").exists()
                  and not path.name.endswith(".ms") and not path.name.endswith(".skymodel")
                  and (not path.name.endswith(".skymodel.flat") or Path(str(path)+".regrid").exists())}
        result["cases"][name] = dict(parameters=parameters, images=images)
    with gzip.open(root / "simanalyze_multi_casa.json.gz", "wt") as output:
        json.dump(result, output, separators=(",", ":"))
    print("Recorded CASA multi-array and TP task products", flush=True)


if __name__ == "__main__":
    main()
