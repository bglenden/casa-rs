#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check public TFCrop correlation expressions and physical flag destinations."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import auxiliary_content
from flagdata_routine_controls_oracle import flags
from flagdata_tfcrop_sources_oracle import check_science_state, put_samples, science_state, seed_sources
from record_casa_returns import make_cal_ms


def seed_layout(path, stokes):
    make_cal_ms(path, channels=32, scan_seconds=210, stokes=stokes)
    with table(path, readonly=False, ack=False) as main:
        data = np.ones_like(main.getcol("DATA"))
        baseline = (main.getcol("ANTENNA1") == 0) & (main.getcol("ANTENNA2") == 1)
        if stokes == "XX YY":
            baseline &= main.getcol("FIELD_ID") == 0
            data[:, :, 0] = 2.
        data[baseline, 16, 0] = 30.
        if data.shape[2] > 1:
            data[baseline, 20 if stokes == "XX YY" else 21, -1] = 35.
        if data.shape[2] == 4:
            data[:, :, 1] = 2.
            data[baseline, 9, 1] += 20.
        main.putcol("DATA", data)
        if stokes == "XX YY":
            corrected = np.full(data.shape, 1. + 1j, dtype=np.complex64)
            corrected[baseline, 12, 1] = 1. + 30j
            put_samples(main, "CORRECTED_DATA", corrected)
        empty = np.zeros(data.shape, dtype=bool)
        main.putcol("FLAG", empty)
        main.putcol("FLAG_ROW", np.zeros(main.nrows(), dtype=bool))
    return empty, baseline


def check_products(libraries):
    datasets = {name: seed_layout(f"{name}.ms", stokes) for name, stokes in (
        ("linear", "XX YY"), ("four", "XX XY YX YY"), ("circular", "RR LL"),
        ("stored-i", "I"), ("stored-q", "Q"),
    )}
    cases = (
        ("linear", "data", "", ((16, 0), (20, 1))),
        ("linear", "data", "XX", ((16, 0),)),
        ("linear", "data", "YY", ((20, 1),)),
        ("linear", "data", "YY,XX", ((16, 0),)),
        ("linear", "data", "fooXXbar", ((16, 0),)),
        ("linear", "data", "ABS_XX,ABS_YY", ((16, 0),)),
        ("linear", "data", "ABS_I", ((16, 0), (20, 0), (16, 1), (20, 1))),
        ("linear", "data", "ABS_Q", ((16, 0), (20, 0), (16, 1), (20, 1))),
        ("linear", "data", "ABS_XY", ()),
        ("linear", "data", "unrecognized", ()),
        ("linear", "data", "IMAG_ALL", ()),
        ("linear", "data", "ALL", ((16, 0), (20, 1))),
        ("linear", "corrected", "ABS_YY", ((12, 1),)),
        ("linear", "corrected", "REAL_YY", ()),
        ("linear", "corrected", "IMAG_YY", ((12, 1),)),
        ("linear", "corrected", "ARG_YY", ((12, 1),)),
        ("linear", "corrected", "NORM_YY", ((12, 1),)),
        ("four", "data", "ABS_ALL", ((16, 0), (9, 1), (21, 3))),
        ("four", "data", "ABS_U", ((9, 1), (9, 2))),
        ("four", "data", "ABS_V", ((9, 1), (9, 2))),
        ("circular", "data", "ABS_RR", ((16, 0),)),
        ("circular", "data", "ABS_LL", ((21, 1),)),
        ("stored-i", "data", "", ()),
        ("stored-i", "data", "ABS_I", ()),
        ("stored-i", "data", "WVR", ()),
        ("stored-i", "data", "ABS_WVR", ((16, 0),)),
        ("stored-i", "data", "WVR_I", ((16, 0),)),
        ("stored-q", "data", "ABS_ALL", ((16, 0),)),
        ("stored-q", "data", "ALL", ()),
        ("stored-q", "data", "ABS_Q", ((16, 0),)),
    )
    for index, (dataset, column, expression, coordinates) in enumerate(cases):
        empty, baseline = datasets[dataset]
        expected = empty.copy()
        for channel, plane in coordinates:
            expected[baseline, channel, plane] = True
        for name, task in libraries:
            path = f"products-{index}-{name}.ms"
            copytree(f"{dataset}.ms", path)
            science, auxiliary = science_state(path), auxiliary_content(path)
            options = dict(vis=path, mode="tfcrop", datacolumn=column, flagdimension="freq",
                           antenna="0&1", correlation=expression, extendflags=False)
            assert task(action="calculate", **options) == {}
            if name == "native":
                calculated = tasks.run("flagdata", action="calculate", **options)
                assert loads(calculated.stdout)["result"]["changed_samples"] == expected.sum()
            assert auxiliary_content(path) == auxiliary
            preview, preview_rows = flags(path)
            np.testing.assert_array_equal(preview, empty)
            assert not preview_rows.any()
            assert task(**options) == {}
            actual, row_flags = flags(path)
            np.testing.assert_array_equal(actual, expected, err_msg=str(options))
            assert not row_flags.any()
            backup, backup_rows = flags(str(Path(path + ".flagversions", "flags.flagdata_1")))
            np.testing.assert_array_equal(backup, empty)
            assert not backup_rows.any()
            check_science_state(path, science)
    return len(cases)


def assert_unknown_product(task, path, column, expression, reported):
    message = f"Unknown polarization requested, ({reported}) supported types are: XX,YY,XY,YX,RR,LL,RL,LR,I,Q,U,V"
    initial = auxiliary_content(path)
    try:
        task(vis=path, mode="tfcrop", action="calculate", datacolumn=column, correlation=expression,
             flagdimension="freq", antenna="0&1", extendflags=False)
    except RuntimeError as error:
        assert str(error) == message
    else:
        raise AssertionError((path, expression))
    assert auxiliary_content(path) == initial
    assert not any(mask.any() for mask in flags(path))


def check_unknown_operator_product(libraries):
    seed_sources()
    for name, task in libraries:
        for source, column, expression, reported in (("linear", "data", "REAL", "REAL"),
                ("complex", "weight_spectrum", "YY_ABS", "REAL ABS")):
            path = f"unknown-{source}-{name}.ms"
            copytree(f"{source}.ms", path)
            assert_unknown_product(task, path, column, expression, reported)
        path = f"real-source-{name}.ms"
        copytree("complex.ms", path)
        assert task(vis=path, mode="tfcrop", antenna="0&1", datacolumn="weight_spectrum",
                    correlation="ABS_YY", flagdimension="freq", extendflags=False, flagbackup=False) == {}
        actual, rows = flags(path)
        expected = np.zeros_like(actual)
        with table(path, ack=False) as main:
            expected[:, 18, 1] = main.getcol("WEIGHT_SPECTRUM")[:, 18, 1] == 42.
        np.testing.assert_array_equal(actual, expected)
        assert not rows.any()


def check_related_flag_state(libraries):
    with table("linear.ms", ack=False) as main:
        baseline = ((main.getcol("FIELD_ID") == 0) & (main.getcol("ANTENNA1") == 0)
                    & (main.getcol("ANTENNA2") == 1))
    row = np.flatnonzero(baseline)[0]
    cases = (("ABS_I", False, 0), ("ABS_I", False, 1), ("XX", True, 0), ("XX", True, 1),
             ("XX,YY", True, 0))
    for index, (expression, extend, plane) in enumerate(cases):
        for name, task in libraries:
            path = f"related-{index}-{name}.ms"
            copytree("linear.ms", path)
            seeded, rows = flags(path)
            seeded[row, 9, plane] = True
            with table(path, readonly=False, ack=False) as main:
                main.putcol("DATA", np.ones_like(main.getcol("DATA")))
                main.putcol("FLAG", seeded)
            expected = seeded.copy()
            # Stokes I covers both contributing planes. For an explicit XX
            # fit, native extension may update the selected physical plane.
            if expression != "XX" or name == "casa" or plane == 1:
                expected[row, 9, :] = True
            before = auxiliary_content(path)
            options = dict(vis=path, mode="tfcrop", antenna="0&1", datacolumn="data",
                           correlation=expression, flagdimension="freq", extendflags=extend)
            assert task(action="calculate", **options) == {}
            if name == "native":
                calculated = tasks.run("flagdata", action="calculate", **options)
                assert loads(calculated.stdout)["result"]["changed_samples"] == expected.sum() - seeded.sum()
            assert auxiliary_content(path) == before
            assert task(**options) == {}
            actual, actual_rows = flags(path)
            np.testing.assert_array_equal(actual, expected)
            np.testing.assert_array_equal(actual_rows, rows)
    return len(cases)


def check_global_products(libraries):
    source = "global.ms"
    copytree("linear.ms", source)
    with table("stored-i.ms/POLARIZATION", ack=False) as extra:
        with table(f"{source}/POLARIZATION", readonly=False, ack=False) as pol:
            row = pol.nrows()
            pol.addrows(1)
            extra.copyrows(pol, startrowin=0, startrowout=row, nrow=1)
    seeded, original_rows = flags(source)
    with table(source, readonly=False, ack=False) as main:
        pair = (main.getcol("ANTENNA1") == 0) & (main.getcol("ANTENNA2") == 1)
        row = np.flatnonzero(pair)[0]
        seeded[row, 9, 0] = True
        main.putcol("DATA", np.ones_like(main.getcol("DATA")))
        main.putcol("FLAG", seeded)
    expected = seeded.copy()
    expected[row, 9, 1] = True
    for index, expression in enumerate(("", "ABS_ALL")):
        for name, task in libraries:
            path = f"global-{index}-{name}.ms"
            copytree(source, path)
            assert task(vis=path, mode="tfcrop", antenna="0&1", datacolumn="data",
                        correlation=expression, flagdimension="freq", extendflags=False, flagbackup=False) == {}
            actual, rows = flags(path)
            np.testing.assert_array_equal(actual, expected)
            np.testing.assert_array_equal(rows, original_rows)
    return 2


def check_imaginary_products(libraries):
    source = "imaginary.ms"
    copytree("linear.ms", source)
    with table(source, readonly=False, ack=False) as main:
        rows = ((main.getcol("FIELD_ID") == 0) & (main.getcol("ANTENNA1") == 0)
                & (main.getcol("ANTENNA2") == 1))
        data = np.ones_like(main.getcol("DATA"))
        data[:, :, 0] *= 2j
        data[:, :, 1] *= 1j
        data[rows, 16, :] += 30j
        main.putcol("DATA", data)
    empty, original_rows = flags(source)
    for index, expression in enumerate(("IMAG_I", "IMAG_Q", "IMAG")):
        for name, task in libraries:
            path = f"imaginary-{index}-{name}.ms"
            copytree(source, path)
            if expression == "IMAG" and name == "native":
                assert_unknown_product(task, path, "data", expression, expression)
                continue
            assert task(vis=path, mode="tfcrop", antenna="0&1", datacolumn="data", correlation=expression,
                        flagdimension="freq", extendflags=False, flagbackup=False) == {}
            expected = empty.copy()
            if name == "casa" or expression == "IMAG_I":
                expected[rows, 16, :] = True
            actual, flags_row = flags(path)
            np.testing.assert_array_equal(actual, expected)
            np.testing.assert_array_equal(flags_row, original_rows)
    return 3


def check_mixed_layouts(libraries):
    casatasks.concat(vis=["linear.ms", "circular.ms"], concatvis="mixed.ms", freqtol="1Hz")
    with table("mixed.ms/DATA_DESCRIPTION", ack=False) as descriptions:
        assert descriptions.nrows() == 2
        assert len(set(descriptions.getcol("SPECTRAL_WINDOW_ID"))) == 1
        assert len(set(descriptions.getcol("POLARIZATION_ID"))) == 2
    with table("mixed.ms", ack=False) as main:
        data = main.getcol("DATA")
    empty, empty_rows = flags("mixed.ms")
    linear = data[:, 20, 1].real > 20.
    circular = data[:, 21, 1].real > 20.
    cases = ("XX,RR", "ABS_RR,XX", "ABS_I")
    for index, expression in enumerate(cases):
        expected = empty.copy()
        if expression == "ABS_RR,XX":
            expected[linear, 16, 0] = True
        elif expression == "ABS_I":
            expected[linear, 16, :] = True
            expected[linear, 20, :] = True
            expected[circular, 16, :] = True
            expected[circular, 21, :] = True
        for name, task in libraries:
            path = f"mixed-{index}-{name}.ms"
            copytree("mixed.ms", path)
            assert task(vis=path, mode="tfcrop", antenna="0&1", datacolumn="data",
                        correlation=expression, flagdimension="freq", extendflags=False, flagbackup=False) == {}
            actual, rows = flags(path)
            np.testing.assert_array_equal(actual, expected, err_msg=expression)
            np.testing.assert_array_equal(rows, empty_rows)
    return len(cases)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    libraries = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory(prefix="flagdata-tfcrop-products-") as root, chdir(root):
        products = check_products(libraries)
        check_unknown_operator_product(libraries)
        related = check_related_flag_state(libraries)
        mixed = check_mixed_layouts(libraries)
        global_products = check_global_products(libraries)
        imaginary = check_imaginary_products(libraries)
    print(f"{products} TFCrop correlation products, {related} related flag products, {mixed} mixed-layout "
          f"products, {global_products} global product sets, {imaginary} imaginary expressions and real-source "
          "operator refusals verified")


if __name__ == "__main__":
    main()
