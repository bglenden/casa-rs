#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Run each stage of ft, gaincal and uvsub with CASA or Rust on the same input."""

import argparse
from itertools import product
import json
from pathlib import Path
import shutil

import casatasks as casa
from casatools import table
from casars import tasks as native
import numpy as np

from calibration_ft_oracle import component, coordinates
from calibration_scratch_oracle import MAIN, prepare, state
from calibration_smoothcal_oracle import opened
from gaincal_oracle import relative_rms, solve
from imager_prediction_options_oracle import write_model
from record_casa_returns import make_cal_ms


ENGINES = {"casa": casa, "native": native}
GAIN_IDENTITIES = ("FLAG", "FIELD_ID", "ANTENNA1", "ANTENNA2", "SPECTRAL_WINDOW_ID",
                   "SCAN_NUMBER", "OBSERVATION_ID")


def column(path, name, row=None):
    reader = table()
    assert reader.open(str(path))
    try:
        return reader.getcol(name) if row is None else reader.getcell(name, row)
    finally:
        reader.close()


def compare_gains(actual, expected):
    for col in GAIN_IDENTITIES:
        np.testing.assert_array_equal(actual[col], expected[col], err_msg=col)
    for col in ("TIME", "INTERVAL"):
        np.testing.assert_allclose(actual[col], expected[col], rtol=0., atol=1e-5, err_msg=col)
    good = ~expected["FLAG"]
    assert np.count_nonzero(good) == 60
    assert np.any(np.abs(expected["CPARAM"][good]) > 0.1)
    return relative_rms(actual["CPARAM"][good], expected["CPARAM"][good])


def unchanged(actual, original, columns):
    assert actual.keys() == original.keys()
    for col in columns:
        if col in original:
            np.testing.assert_array_equal(actual[col], original[col], err_msg=col)


def check_undefined_uvw(seed, path):
    shutil.copytree(seed, path)
    missing = 3
    with opened(path, True) as handle:
        uvw = handle.getcol("UVW")
        keywords = handle.getcolkeywords("UVW")
        handle.renamecol("UVW", "ORIGINAL_UVW")
        assert handle.addcols({"UVW": {"valueType": "double", "ndim": 1}})
        for key, value in keywords.items():
            handle.putcolkeyword("UVW", key, value)
        for row in range(handle.nrows()):
            if row != missing:
                handle.putcell("UVW", row, uvw[:, row])
        assert not handle.iscelldefined("UVW", missing)
    try:
        native.setjy(str(path), field="0", standard="manual", fluxdensity=[2., 0., 0., 0.], usescratch=True)
    except RuntimeError as error:
        assert f'column "UVW" not found in row {missing}' in str(error), error
    else:
        raise AssertionError("Scratch prediction accepted an undefined UVW cell")
    print("Scratch prediction reports the missing UVW cell's row", flush=True)


def publish(scenario, api, seed, path, models, fields):
    scratch = scenario == "scratch"
    prepare(seed, path, corrected=True, model=scratch)
    before, _ = state(path)

    def ft(**options):
        assert api.ft(str(path), **options) is None

    if scratch:
        ft(field="0", spw="0:2~8^2", complist=str(models["replacement"]), usescratch=True)
    else:
        ft(field="0", complist=str(models["superseded"]))
        ft(field="1", complist=str(models["other"]))
        if scenario == "virtual-overlap":
            ft(field="0", complist=str(models["replacement"]))
        else:
            ft(field="0", model=str(models["image"]))

    ready, recipe = state(path)
    unchanged(ready, before, (*MAIN, "CORRECTED_DATA"))
    if scratch:
        selected = (fields == 0)[None, :] & np.isin(np.arange(12), [2, 4, 6, 8])[:, None]
        np.testing.assert_allclose(ready["MODEL_DATA"][:, selected], 5., atol=2e-4, rtol=0.)
        np.testing.assert_array_equal(ready["MODEL_DATA"][:, ~selected],
                                      before["MODEL_DATA"][:, ~selected])
    else:
        assert "MODEL_DATA" not in ready
        pointers = set(recipe[0]) | set(recipe[1])
        assert {"definedmodel_field_0", "definedmodel_field_1"} <= pointers
    return ready, recipe


def run(root):
    assert casa.version_string() == "6.7.6.14"
    root.mkdir(parents=True)
    seed = root / "seed.ms"
    make_cal_ms(str(seed), channels=12)
    fields = column(seed, "FIELD_ID")
    phase = coordinates(seed)
    other_phase = column(seed / "FIELD", "PHASE_DIR", 1)[:, 0]
    models = {name: root / f"{name}.cl" for name in ("superseded", "replacement", "other")}
    for name, direction, flux in (("superseded", phase, 2.), ("replacement", phase, 5.),
                                  ("other", other_phase, 3.)):
        component(models[name], direction, flux)
    models["image"] = root / "off-axis.im"
    frequency = float(column(seed / "SPECTRAL_WINDOW", "CHAN_FREQ", 0)[0])
    write_model(models["image"], phase, frequency, 2., cell=3.)

    evidence = {}
    for scenario in ("scratch", "virtual-overlap", "virtual-image"):
        expected = None
        for writer, writer_api in ENGINES.items():
            published = root / f"{scenario}-{writer}.ms"
            ready, recipe = publish(scenario, writer_api, seed, published, models, fields)
            for solver, subtractor in product(ENGINES, repeat=2):
                label = f"{scenario}-{writer}-{solver}-{subtractor}"
                path = root / f"{label}.ms"
                shutil.copytree(published, path)
                gains = solve(ENGINES[solver], root, label, path, smodel=(), field="0",
                              spw="0:2~8^2", minblperant=2, solint="int")
                after_solve, kept = state(path)
                unchanged(after_solve, ready, ready)
                np.testing.assert_equal(kept, recipe)

                uvsub = ENGINES[subtractor].uvsub
                assert uvsub(str(path)) is None
                residual, kept = state(path)
                unchanged(residual, ready, (*MAIN, "MODEL_DATA"))
                np.testing.assert_equal(kept, recipe)
                measured = ready["CORRECTED_DATA"] - residual["CORRECTED_DATA"]
                if scenario == "scratch":
                    np.testing.assert_allclose(measured, ready["MODEL_DATA"], atol=2e-6, rtol=0.)
                if scenario == "virtual-overlap":
                    np.testing.assert_allclose(measured[:, :, fields == 0], 5., atol=2e-4, rtol=0.)
                    np.testing.assert_allclose(measured[:, :, fields == 1], 3., atol=2e-4, rtol=0.)
                assert np.count_nonzero(measured) > 0
                assert uvsub(str(path), reverse=True) is None
                restored, kept = state(path)
                unchanged(restored, ready, (*MAIN, "MODEL_DATA"))
                np.testing.assert_equal(kept, recipe)
                np.testing.assert_allclose(restored["CORRECTED_DATA"], ready["CORRECTED_DATA"],
                                           atol=3e-6, rtol=0.)

                if expected is None:
                    expected = gains, measured
                gain_nrms = compare_gains(gains, expected[0])
                model_nrms = relative_rms(measured, expected[1])
                assert gain_nrms < 1e-3 and model_nrms < 1e-3, (label, gain_nrms, model_nrms)
                evidence[label] = {"gains": gains["CPARAM"].size, "gain_nrms": gain_nrms,
                                   "model_samples": measured.size, "model_nrms": model_nrms}
                print(label, evidence[label], flush=True)
    check_undefined_uvw(seed, root / "undefined-uvw.ms")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    evidence = run(root)
    (root / "results.json").write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    main()
