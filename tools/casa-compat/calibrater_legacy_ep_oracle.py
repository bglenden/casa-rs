#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare legacy EP setmodel and bidirectional cache reuse with CASA 6.7.6.14."""

import argparse
import hashlib
from itertools import product
import json
from pathlib import Path
import shutil
from time import perf_counter

import casatasks
from casatools import calibrater as reference, image, imager, table
from casars.tools import calibrater as native
import numpy as np

from gaincal_oracle import relative_rms
from record_casa_returns import make_cal_ms


ENGINES = {"casa": reference, "native": native}
PRODUCTS = ("CF0_0", "CFWT0_0", "avgPB")


def reject(call, text):
    try:
        call()
    except RuntimeError as error:
        assert text.lower() in str(error).lower(), str(error)
    else:
        raise AssertionError(f"expected RuntimeError containing {text!r}")


def setup(dataset, model, cache, factory, *, retry=False, painc=360.):
    cb = factory()
    assert cb.setvi(old=True, quiet=True)
    assert cb.open(str(dataset))
    try:
        if retry:
            reject(lambda: cb.setmodel(str(model)), "called before Calibrater::setsolve()")
        assert cb.setsolve(type="EP", table=str(cache.with_suffix(".EP")), cfcache=str(cache),
                           painc=painc)
        if retry:
            assert not any(cache.iterdir())
            missing = model.with_name("missing-ep-model.im")
            reject(lambda: cb.setmodel(str(missing)), "does not exist")
            assert not any(cache.iterdir())
        start = perf_counter()
        assert cb.setmodel(str(model))
        return perf_counter() - start
    finally:
        assert cb.done()


def read_image(path):
    reader = image()
    assert reader.open(str(path))
    try:
        coordinate = reader.coordsys()
        try:
            metadata = (coordinate.names(), coordinate.units(), coordinate.stokes(),
                        coordinate.referencecode(), coordinate.referencevalue()["numeric"],
                        coordinate.referencepixel()["numeric"], coordinate.increment()["numeric"])
        finally:
            assert coordinate.done()
        pixels = reader.getchunk()
        assert np.all(np.isfinite(pixels)) and np.max(np.abs(pixels)) > 0.
        return pixels, metadata
    finally:
        assert reader.done()


def snapshot(cache):
    return {str(path.relative_to(cache)): (hashlib.sha256(path.read_bytes()).hexdigest(),
                                           path.stat().st_mtime_ns)
            for path in cache.rglob("*") if path.is_file() and not path.name.endswith(".lock")}


def controlled_refusals(root, datasets, model):
    for factory in ENGINES.values():
        cb = factory()
        assert cb.open(str(datasets["casa"]))
        try:
            reject(lambda: cb.setsolve(type="EP", table=str(root / "unsupported.EP"),
                                      cfcache=str(root / "unsupported-cache")), "Error in Calibrater::setsolve.")
        finally:
            assert cb.done()

    for label, factory in ENGINES.items():
        cb = factory()
        assert cb.setvi(old=True, quiet=True) and cb.open(str(datasets[label]))
        try:
            assert cb.setsolve(type="G", table=str(root / f"{label}-model-refusal.G"))
            reject(lambda: cb.setmodel(str(model)), "Internal error: setModel() not yet supported for non EPJones type.")
            assert cb.setsolve(type="EP", painc=-1., cfcache=str(root / f"{label}-negative-pa-cache"))
        finally:
            assert cb.done()

    alma = root / "alma.ms"
    make_cal_ms(str(alma))
    for name, factory in ENGINES.items():
        cb = factory()
        assert cb.setvi(old=True, quiet=True) and cb.open(str(alma))
        try:
            assert cb.setsolve(type="EP", cfcache=str(root / f"{name}-alma-cache"))
            reject(lambda: cb.setmodel(str(model)), "VLA")
        finally:
            assert cb.done()

    unsupported = root / "single-stokes.im"
    reader = image()
    assert reader.fromarray(str(unsupported), pixels=np.zeros((256, 256, 1, 1), np.float32))
    assert reader.done()
    cb = native()
    assert cb.setvi(old=True, quiet=True) and cb.open(str(datasets["native"]))
    try:
        cache = root / "single-stokes-cache"
        assert cb.setsolve(type="EP", cfcache=str(cache))
        reject(lambda: cb.setmodel(str(unsupported)), "IQUV")
        assert not any(cache.iterdir())
    finally:
        assert cb.done()


def run(root, dataset, model):
    assert casatasks.version_string() == "6.7.6.14"
    root.mkdir(parents=True)
    datasets = {label: root / f"{label}.ms" for label in ENGINES}
    caches = {label: root / f"{label}-cache" for label in ENGINES}
    result = {}
    for label, factory in ENGINES.items():
        shutil.copytree(dataset, datasets[label])
        result[f"{label}_cold_seconds"] = setup(datasets[label], model, caches[label], factory,
                                                retry=True)
    casa_aux, native_aux = [(cache / "aux.dat").read_text().split() for cache in caches.values()]
    assert casa_aux[:2] == native_aux[:2] == ["1", "1"]
    assert abs(float(casa_aux[2]) - float(native_aux[2])) < 1e-3
    assert casa_aux[3:] == native_aux[3:] == ["3", "3", "20"]
    result["angle_deg"] = float(native_aux[2])
    result["support"] = 3
    result["sampling"] = 20
    for name in PRODUCTS:
        expected, expected_coordinates = read_image(caches["casa"] / name)
        actual, actual_coordinates = read_image(caches["native"] / name)
        assert actual.shape == expected.shape
        np.testing.assert_equal(actual_coordinates[:4], expected_coordinates[:4])
        for observed, reference_record in zip(actual_coordinates[4:], expected_coordinates[4:],
                                              strict=True):
            np.testing.assert_allclose(observed, reference_record, rtol=1e-12, atol=1e-12)
        nrms = relative_rms(actual, expected)
        assert nrms < 1e-3, (name, nrms)
        result[name] = {"shape": list(expected.shape), "nrms": nrms}
    for writer, reader in product(ENGINES, repeat=2):
        cache = caches[writer]
        before = snapshot(cache)
        result[f"{writer}_{reader}_warm_seconds"] = setup(datasets[reader], model, cache,
                                                          ENGINES[reader])
        assert len(before) == 22
        assert snapshot(cache) == before
    controlled_refusals(root, datasets, model)
    result["angle_append"] = append_angle(root, datasets, caches, model)
    expected = None
    for writer in ENGINES:
        dataset, cache = datasets[writer], caches[writer]
        before = snapshot(cache)
        predicted = first_buffer(dataset, cache, model)
        assert snapshot(cache) == before
        if expected is None:
            expected = predicted
        nrms = relative_rms(predicted, expected)
        assert nrms < 1e-3, (writer, nrms)
        result[f"{writer}_casa_first_buffer_nrms"] = nrms
    result["first_buffer_reference_ms"] = str(datasets["casa"])
    return result


def append_angle(root, datasets, caches, model):
    reports = {}
    for label, factory in ENGINES.items():
        path = root / f"{label}-append-cache"
        shutil.copytree(caches["casa"], path)
        aux = path / "aux.dat"
        aux.write_text(aux.read_text().replace("74.4609", "64.4609"))
        original = snapshot(path)
        if label == "native":
            # Reproduce a process exit after promoting the first image of
            # the second slot, before committing the new auxiliary file.
            stage = path / ".ep-cache-interrupted"
            stage.mkdir()
            (stage / "pending-slot").write_text("1")
            (stage / "CFWT0_1").mkdir()
            copied = path / "CF0_1"
            copied.mkdir()
            (copied / "incomplete").write_text("abandoned projection")
        setup(datasets[label], model, path, factory, painc=.1)
        assert not (path / ".ep-cache-interrupted").exists()
        recorded = aux.read_text().split()
        assert recorded[:2] == ["2", "1"]
        assert recorded[2:6] == ["64.4609", "3", "3", "20"]
        assert recorded[6:] == ["74.4609", "3", "3", "20"]
        kept = snapshot(path)
        assert all(kept[name] == value for name, value in original.items() if name != "aux.dat")
        for prefix in ("CF", "CFWT"):
            expected, _ = read_image(caches["casa"] / f"{prefix}0_0")
            actual, _ = read_image(path / f"{prefix}0_1")
            nrms = relative_rms(actual, expected)
            assert nrms < 1e-3, (label, prefix, nrms)
            reports[f"{label}_{prefix}_nrms"] = nrms
    return reports


def first_buffer(dataset, cache, model):
    # The legacy imager exposes the EP solver's nPBWProjectFT prediction
    # with zero pointing offsets for the retained first-buffer comparison.
    tool = imager()
    assert tool.open(str(dataset), usescratch=True)
    assert tool.selectvis(field=0)
    assert tool.defineimage(nx=256, ny=256, cellx="15arcsec", celly="15arcsec",
                            stokes="IQUV", outframe="TOPO")
    assert tool.setoptions(ftmachine="pbwproject", wprojplanes=1,
                           cfcachedirname=str(cache), applypointingoffsets=False,
                           dopbgriddingcorrections=True)
    assert tool.ft(str(model)) and tool.done()
    reader = table()
    assert reader.open(str(dataset))
    try:
        time = reader.getcol("TIME")
        rows = np.flatnonzero(time == time.min())
        assert np.all(np.diff(rows) == 1)
        visibilities = reader.getcol("MODEL_DATA", startrow=int(rows[0]), nrow=len(rows))
        assert np.max(np.abs(visibilities)) > 1. and np.max(np.abs(visibilities.imag)) > 0.1
        return visibilities
    finally:
        reader.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--ms", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    result = run(root, args.ms.resolve(), args.model.resolve())
    (root / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
