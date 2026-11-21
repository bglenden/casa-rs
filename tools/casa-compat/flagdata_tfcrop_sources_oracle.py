#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check unaveraged TFCrop visibility and weight sources against pinned CASA."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import auxiliary_content
from flagdata_routine_controls_oracle import flags
from flagdata_tfcrop_fit_oracle import make_fixture
from imager_prediction_options_oracle import write_model


def put_samples(main, column, data):
    if column not in main.colnames():
        is_real = data.dtype.kind == "f"
        descriptor = makearrcoldesc(column, 0. if is_real else 0j, ndim=2,
                                     valuetype="float" if is_real else "complex")
        main.addcols(maketabdesc(descriptor))
    main.putcol(column, data)


def seed_sources():
    empty, narrow, broad = make_fixture("complex.ms")
    with table("complex.ms", readonly=False, ack=False) as main:
        model = np.ones_like(main.getcol("DATA"))
        model[narrow, 10, 1] = 35.
        observed = model + 1.
        observed[narrow, 16, 0] += 30.
        corrected = model + (1. + 1j)
        corrected[narrow, 12, 1] += 30j
        weight_spectrum = np.ones(model.shape, dtype=np.float32)
        weight_spectrum[narrow, 18, 1] = 42.
        for column, data in (("DATA", observed), ("CORRECTED_DATA", corrected),
                             ("MODEL_DATA", model), ("WEIGHT_SPECTRUM", weight_spectrum)):
            put_samples(main, column, data)
        weight = np.ones_like(main.getcol("WEIGHT"))
        weight[broad, 0] = 30.
        main.putcol("WEIGHT", weight)
    for variant in ("scalar", "float", "float-only"):
        path = f"{variant}.ms"
        copytree("complex.ms", path)
        with table(path, readonly=False, ack=False) as main:
            if variant == "scalar":
                main.removecols("WEIGHT_SPECTRUM")
                continue
            real = np.ones(model.shape, dtype=np.float32)
            real[narrow, 14, 1] = 39.
            put_samples(main, "FLOAT_DATA", real)
            if variant == "float-only":
                main.removecols("DATA")
    copytree("float.ms", "signed.ms")
    with table("signed.ms", readonly=False, ack=False) as main:
        signed = np.full(model.shape, -1., dtype=np.float32)
        signed[narrow, 14, 1] = 1.
        main.putcol("FLOAT_DATA", signed)
    return empty, narrow, broad


def science_state(path):
    names = {"DATA", "CORRECTED_DATA", "MODEL_DATA", "FLOAT_DATA", "WEIGHT", "WEIGHT_SPECTRUM",
             "SIGMA", "SIGMA_SPECTRUM"}
    with table(path, ack=False) as main:
        return {column: main.getcol(column) for column in names.intersection(main.colnames())}


def check_science_state(path, original):
    stored = science_state(path)
    assert stored.keys() == original.keys()
    for column, values in original.items():
        np.testing.assert_array_equal(stored[column], values, err_msg=column)


def check_sources(libraries):
    empty, narrow, broad = seed_sources()
    cases = (
        ("complex", "data", ((16, 0), (10, 1))),
        ("complex", "corrected", ((10, 1), (12, 1))),
        ("complex", "model", ((10, 1),)),
        ("complex", "residual", ((12, 1),)),
        ("complex", "residual_data", ((16, 0),)),
        ("complex", "weight", ((18, 1),)),
        ("complex", "weight_spectrum", ((18, 1),)),
        ("scalar", "weight", None),
        ("float", "data", ((14, 1),)),
        ("float", "residual_data", ()),
        ("float", "float_data", ((14, 1),)),
        ("float-only", "float_data", ((14, 1),)),
        ("float-only", "residual_data", ()),
        ("signed", "float_data", ((14, 1),)),
        ("signed", "data", ()),
    )
    # The native DATA selection keeps the complex source when both observed
    # columns exist. The complex-only cases verify these same masks in CASA.
    complex_source_masks = {
        ("float", "data"): ((16, 0), (10, 1)),
        ("signed", "data"): ((16, 0), (10, 1)),
        ("float", "residual_data"): ((16, 0),),
    }
    for index, (variant, column, coordinates) in enumerate(cases):
        dimension = "time" if coordinates is None else "freq"
        for name, task in libraries:
            expected = empty.copy()
            selected = complex_source_masks.get((variant, column), coordinates) if name == "native" else coordinates
            if selected is None:
                expected[broad, :, 0] = True
            else:
                for channel, plane in selected:
                    expected[narrow, channel, plane] = True
            path = f"source-{index}-{name}.ms"
            copytree(f"{variant}.ms", path)
            original, auxiliary = science_state(path), auxiliary_content(path)
            options = dict(vis=path, mode="tfcrop", datacolumn=column, antenna="0&1", extendflags=False,
                           flagdimension=dimension, timefit="poly", timecutoff=2.5, maxnpieces=3)
            assert task(action="calculate", **options) == {}
            if name == "native":
                calculated = tasks.run("flagdata", action="calculate", **options)
                assert loads(calculated.stdout)["result"]["changed_samples"] == expected.sum()
            check_science_state(path, original)
            assert auxiliary_content(path) == auxiliary
            for actual, original_flags in zip(flags(path), (empty, np.zeros(len(empty), dtype=bool)), strict=True):
                np.testing.assert_array_equal(actual, original_flags)
            assert task(**options) == {}
            actual, rows = flags(path)
            np.testing.assert_array_equal(actual, expected, err_msg=str(options))
            assert not rows.any()
            prior, prior_rows = flags(str(Path(path + ".flagversions", "flags.flagdata_1")))
            np.testing.assert_array_equal(prior, empty)
            assert not prior_rows.any()
            check_science_state(path, original)

    refusals = (("scalar", "weight_spectrum"), ("float-only", "data"),
                ("complex", "corrected_data"), ("complex", "fparam"))
    for index, (variant, column) in enumerate(refusals):
        for name, task in libraries:
            path = f"missing-{index}-{name}.ms"
            copytree(f"{variant}.ms", path)
            original, auxiliary = science_state(path), auxiliary_content(path)
            options = dict(vis=path, mode="tfcrop", datacolumn=column)
            for action in ("calculate", "apply"):
                try:
                    task(action=action, **options)
                except ValueError as error:
                    assert str(error) == "Failed to parse parameters for mode tfcrop"
                else:
                    raise AssertionError((name, action, options))
                check_science_state(path, original)
                actual, rows = flags(path)
                np.testing.assert_array_equal(actual, empty)
                assert not rows.any()
                if action == "calculate":
                    assert auxiliary_content(path) == auxiliary
            prior, prior_rows = flags(str(Path(path + ".flagversions", "flags.flagdata_1")))
            np.testing.assert_array_equal(prior, empty)
            assert not prior_rows.any()
    return len(cases), len(refusals)


def seed_fourier_models():
    empty, narrow, _ = make_fixture("model-source.ms")
    with table("model-source.ms/FIELD", ack=False) as fields:
        direction = fields.getcell("PHASE_DIR", 0).ravel()
    write_model(Path("point.im"), direction, 100e9, 2, cell=5)
    for name, use_scratch in (("virtual", False), ("scratch", True)):
        path = f"model-{name}.ms"
        copytree("model-source.ms", path)
        casatasks.ft(vis=path, field="0", model="point.im", usescratch=use_scratch)
    with table("model-scratch.ms", ack=False) as materialized:
        model = materialized.getcol("MODEL_DATA")
    with table("model-virtual.ms", ack=False) as virtual:
        assert not np.array_equal(virtual.getcol("MODEL_DATA"), model)
    return empty, narrow, model


def check_virtual_residuals(libraries):
    empty, narrow, model = seed_fourier_models()
    cases = (("residual_data", 16, 0, 30.), ("residual", 12, 1, 30.),
             ("residual_data", 16, 0, 0.0001), ("residual", 12, 1, 0.0001))
    for index, (column, channel, plane, amplitude) in enumerate(cases):
        observed = model + 1.
        corrected = model + 1.
        source_samples = observed if column == "residual_data" else corrected
        source_samples[narrow, channel, plane] += amplitude
        expected = empty.copy()
        expected[narrow, channel, plane] = True
        for source in ("scratch", "virtual"):
            for name, task in libraries:
                path = f"{column}-{index}-{source}-{name}.ms"
                copytree(f"model-{source}.ms", path)
                with table(path, readonly=False, ack=False) as main:
                    put_samples(main, "DATA", observed)
                    put_samples(main, "CORRECTED_DATA", corrected)
                original = science_state(path)
                assert task(vis=path, mode="tfcrop", field="0", antenna="0&1", datacolumn=column,
                            flagdimension="freq", extendflags=False, flagbackup=False) == {}
                actual, rows = flags(path)
                np.testing.assert_array_equal(actual, expected, err_msg=f"{source} {name} {column} {amplitude}")
                assert not rows.any()
                check_science_state(path, original)
    # An exactly zero prediction leaves even a very small signal available
    # to the statistical fit. This band has no Fourier model coverage.
    assert not model[narrow, 16:, :].any()
    row = np.flatnonzero(narrow)[0]
    observed = model + 1.
    observed[row, 23, 0] += 8 * np.finfo(np.float32).eps
    expected = empty.copy()
    expected[row, 23, 0] = True
    for source in ("scratch", "virtual"):
        for name, task in libraries:
            path = f"zero-model-{source}-{name}.ms"
            copytree(f"model-{source}.ms", path)
            with table(path, readonly=False, ack=False) as main:
                put_samples(main, "DATA", observed)
            assert task(vis=path, mode="tfcrop", field="0", antenna="0&1", spw="0:16~31",
                        datacolumn="residual_data", flagdimension="freq", freqfit="poly", maxnpieces=1,
                        freqcutoff=2., extendflags=False, flagbackup=False) == {}
            actual, rows = flags(path)
            np.testing.assert_array_equal(actual, expected, err_msg=f"{source} {name}")
            assert not rows.any()
    return len(cases) + 1


def check_disabled_model(libraries):
    for index, reason in enumerate(("invalid-fit", "missing-corrected")):
        for name, task in libraries:
            path = f"disabled-{index}-{name}.ms"
            copytree("model-virtual.ms", path)
            with table(f"{path}/SOURCE", readonly=False, ack=False) as source:
                record = source.getcell("SOURCE_MODEL", 0)
                del record["ft_0"]
                source.putcell("SOURCE_MODEL", 0, record)
            if reason == "missing-corrected":
                with table(path, readonly=False, ack=False) as main:
                    main.removecols("CORRECTED_DATA")
            science, auxiliary = science_state(path), auxiliary_content(path)
            options = dict(vis=path, mode="tfcrop", field="0", antenna="0&1", datacolumn="residual",
                           maxnpieces=0 if reason == "invalid-fit" else 7,
                           extendflags=False, flagbackup=False)
            assert task(action="calculate", **options) == {}
            assert auxiliary_content(path) == auxiliary
            assert task(**options) == {}
            assert not any(mask.any() for mask in flags(path))
            check_science_state(path, science)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    libraries = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory(prefix="flagdata-tfcrop-sources-") as root, chdir(root):
        products, refusals = check_sources(libraries)
        virtual = check_virtual_residuals(libraries)
        check_disabled_model(libraries)
    print(f"{products} TFCrop stored-source products, {virtual} virtual residual products and "
          f"{refusals} source refusals and two disabled models verified")


if __name__ == "__main__":
    main()
