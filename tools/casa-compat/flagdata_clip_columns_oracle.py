#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare physical visibility and weight clipping through public flagdata."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_summary_oracle import snapshot
from ms_selection_tasks_oracle import add_second_window, casa_table
from record_casa_returns import make_cal_ms

OBSERVED = np.array([[1, 1], [3, 3], [7, 7], [4, 4]], dtype="c8")
CORRECTED = np.array([[2, 2], [1, 1], [7, 7], [4, 4]], dtype="c8")
MODEL = np.array([[2, 2], [2, 2], [1, 1], [3, 3]], dtype="c8")
SCALAR_WEIGHTS = np.array([1, 2], dtype="f4")
SPECTRUM = np.array([[-2, 3], [1, 1], [2, 2], [1, 1]], dtype="f4")
BEFORE_FLAGS = np.zeros(OBSERVED.shape, dtype=bool)
BEFORE_FLAGS[3, 1] = True


def make_inputs():
    make_cal_ms("data.ms", channels=4, scan_seconds=20)
    with table("data.ms", readonly=False, ack=False) as ms:
        shape = ms.getcol("DATA").shape
        for name, values in (("DATA", OBSERVED), ("CORRECTED_DATA", CORRECTED), ("MODEL_DATA", MODEL),
                             ("FLAG", BEFORE_FLAGS)):
            ms.putcol(name, np.broadcast_to(values, shape).copy())
        ms.putcol("FLAG_ROW", np.zeros(ms.nrows(), dtype=bool))
        ms.putcol("WEIGHT", np.broadcast_to(SCALAR_WEIGHTS, (ms.nrows(), 2)).copy())
    copytree("data.ms", "weighted.ms")
    with table("weighted.ms", readonly=False, ack=False) as ms:
        ms.addcols(maketabdesc(makearrcoldesc("WEIGHT_SPECTRUM", 0.0, ndim=2, valuetype="float")))
        ms.putcol("WEIGHT_SPECTRUM", np.broadcast_to(SPECTRUM, shape).copy())
    copytree("data.ms", "minimal.ms")
    with table("minimal.ms", readonly=False, ack=False) as ms:
        ms.removecols(["CORRECTED_DATA", "MODEL_DATA"])


def check_product(path, flags, *, spectral=False):
    with table(path, ack=False) as ms, casa_table(path) as casa:
        for row in range(ms.nrows()):
            np.testing.assert_array_equal(ms.getcell("FLAG", row), flags)
            np.testing.assert_array_equal(casa.getcell("FLAG", row).T, flags)
            assert not ms.getcell("FLAG_ROW", row)
            np.testing.assert_array_equal(ms.getcell("DATA", row), OBSERVED)
            np.testing.assert_array_equal(ms.getcell("CORRECTED_DATA", row), CORRECTED)
            np.testing.assert_array_equal(ms.getcell("MODEL_DATA", row), MODEL)
            np.testing.assert_array_equal(ms.getcell("WEIGHT", row), SCALAR_WEIGHTS)
            if spectral:
                np.testing.assert_array_equal(ms.getcell("WEIGHT_SPECTRUM", row), SPECTRUM)


def compare_products():
    cases = (
        ("data", "model", MODEL, np.abs, "", (0.9, 1.1), slice(None), slice(None)),
        ("data", "RESIDUAL", CORRECTED - MODEL, np.abs, "", (0.9, 1.1), slice(None), slice(None)),
        ("data", "residual_data", OBSERVED - MODEL, np.abs, "", (0.9, 1.1), slice(None), slice(None)),
        ("data", "RESIDUAL", CORRECTED - MODEL, np.real, "REAL_XX", (-1.1, -0.9), slice(1, 3), slice(0, 1)),
        ("data", "WEIGHT", np.broadcast_to(SCALAR_WEIGHTS, OBSERVED.shape), np.real,
         "", (0.9, 1.1), slice(None), slice(None)),
        ("weighted", "weight", SPECTRUM, np.real, "", (0.9, 1.1), slice(None), slice(None)),
        ("weighted", "WEIGHT_SPECTRUM", SPECTRUM, np.real, "", (0.9, 1.1), slice(None), slice(None)),
        ("weighted", "WEIGHT_SPECTRUM", SPECTRUM, np.real, "NORM_XX", (1.9, 2.1), slice(None), slice(0, 1)),
        ("weighted", "WEIGHT", SPECTRUM, np.real, "REAL_XX", (-2.1, -1.9), slice(None), slice(0, 1)),
    )
    for index, (source, column, samples, metric, expression, bounds, channels, correlations) in enumerate(cases):
        options = dict(mode="clip", datacolumn=column, correlation=expression,
                       clipminmax=list(bounds), clipoutside=False, flagbackup=False)
        if channels.start is not None:
            options["spw"] = f"0:{channels.start}~{channels.stop - 1}"
        values = metric(samples)[channels, correlations]
        expected = BEFORE_FLAGS.copy()
        expected[channels, correlations] |= (values >= bounds[0]) & (values <= bounds[1])
        assert np.any(expected & ~BEFORE_FLAGS), options
        for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
            path = f"result-{index}-{name}.ms"
            copytree(f"{source}.ms", path)
            assert task(vis=path, **options) == {}, options
            check_product(path, expected, spectral=source == "weighted")
    return len(cases)


def compare_preview():
    vis = "data.ms"
    original, auxiliary = snapshot(vis), auxiliary_content(vis)
    options = dict(vis=vis, mode="clip", datacolumn="RESIDUAL", clipminmax=[0.9, 1.1],
                   clipoutside=False, action="calculate")
    assert casatasks.flagdata(**options) == {}
    preview = tasks.run("flagdata", **options)
    assert preview.value == {}
    report = loads(preview.stdout)["result"]
    expected = np.isclose(np.abs(CORRECTED - MODEL), 1.0) & ~BEFORE_FLAGS
    assert report["changed_samples"] == len(original) * int(expected.sum())
    assert report["changed_rows"] == len(original)
    assert report["backup_version"] is None
    assert_preserved(vis, original, auxiliary)


def compare_unavailable_columns():
    cases = (
        ("data", "WEIGHT_SPECTRUM"), ("data", "MODEL_DATA"), ("data", "SIGMA"), ("data", "junk"),
        ("minimal", "MODEL"), ("minimal", "RESIDUAL"), ("minimal", "RESIDUAL_DATA"),
        ("minimal", "CORRECTED"), ("minimal", "FLOAT_DATA"),
    )
    cases = [(source, column, {}) for source, column in cases]
    cases.append(("minimal", "MODEL", {"clipoutside": [True]}))
    for index, (source, column, extra) in enumerate(cases):
        for action in ("apply", "calculate"):
            for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
                path = f"unavailable-{index}-{action}-{name}.ms"
                copytree(f"{source}.ms", path)
                original = snapshot(path)
                try:
                    task(vis=path, mode="clip", datacolumn=column, action=action, **extra)
                except ValueError as error:
                    assert str(error) == "Failed to parse parameters for mode clip", str(error)
                else:
                    raise AssertionError((name, action, column))
                for before, after in zip(original, snapshot(path), strict=True):
                    for want, got in zip(before, after, strict=True):
                        np.testing.assert_array_equal(want, got)
                backup = Path(f"{path}.flagversions/flags.flagdata_1")
                assert backup.is_dir() == (action == "apply"), (name, action, column)
                if backup.is_dir():
                    with table(str(backup), ack=False) as saved:
                        for row, (_, flags, _) in enumerate(original):
                            np.testing.assert_array_equal(saved.getcell("FLAG", row), flags)
    return 2 * len(cases)


def compare_sparse_spectra():
    scalar_flags = BEFORE_FLAGS | (np.broadcast_to(SCALAR_WEIGHTS, OBSERVED.shape) == 1)
    spectral_flags = BEFORE_FLAGS | (SPECTRUM == 1)
    assert np.any(scalar_flags & ~spectral_flags)
    for case, defined in (("absent-values", ()), ("defined-later", (1, 2)), ("missing-later", (0, 1))):
        source = f"{case}.ms"
        copytree("data.ms", source)
        with table(source, readonly=False, ack=False) as ms:
            ms.addcols(maketabdesc(makearrcoldesc("WEIGHT_SPECTRUM", 0.0, ndim=2, valuetype="float")))
            for row in defined:
                ms.putcell("WEIGHT_SPECTRUM", row, SPECTRUM)
        for column in ("WEIGHT", "WEIGHT_SPECTRUM"):
            options = dict(mode="clip", datacolumn=column, clipminmax=[0.9, 1.1],
                           clipoutside=False, flagbackup=False)
            for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
                path = f"{case}-{column}-{name}.ms"
                copytree(source, path)
                if case == "missing-later":
                    original, auxiliary = snapshot(path), auxiliary_content(path)
                    try:
                        task(vis=path, **options)
                    except RuntimeError as error:
                        assert "WEIGHT_SPECTRUM" in str(error) and "row 2" in str(error)
                    else:
                        raise AssertionError((name, column))
                    assert_preserved(path, original, auxiliary)
                    continue
                assert task(vis=path, **options) == {}
                with table(path, ack=False) as ms, casa_table(path) as casa:
                    for row in range(ms.nrows()):
                        expected = spectral_flags if row in defined and name == "native" else scalar_flags
                        np.testing.assert_array_equal(ms.getcell("FLAG", row), expected)
                        np.testing.assert_array_equal(casa.getcell("FLAG", row).T, expected)
                        np.testing.assert_array_equal(ms.getcell("DATA", row), OBSERVED)
                        assert not ms.getcell("FLAG_ROW", row)
                        assert ms.iscelldefined("WEIGHT_SPECTRUM", row) == (row in defined)
                        if row in defined:
                            np.testing.assert_array_equal(ms.getcell("WEIGHT_SPECTRUM", row), SPECTRUM)


def compare_spectrum_windows():
    copytree("data.ms", "windows.ms")
    add_second_window(Path("windows.ms"))
    scalar_flags = BEFORE_FLAGS | (np.broadcast_to(SCALAR_WEIGHTS, OBSERVED.shape) == 1)
    spectral_flags = BEFORE_FLAGS | (SPECTRUM == 1)
    for defined_window in (0, 1):
        source = f"spectrum-window-{defined_window}.ms"
        copytree("windows.ms", source)
        with table(source, readonly=False, ack=False) as ms:
            ms.addcols(maketabdesc(makearrcoldesc("WEIGHT_SPECTRUM", 0.0, ndim=2, valuetype="float")))
            for row, ddid in enumerate(ms.getcol("DATA_DESC_ID")):
                if ddid == defined_window:
                    ms.putcell("WEIGHT_SPECTRUM", row, SPECTRUM)
        for column in ("WEIGHT", "WEIGHT_SPECTRUM"):
            for selection in ("", "0", "1"):
                for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
                    target = f"{defined_window}-{column}-{selection}-{name}.ms"
                    copytree(source, target)
                    assert task(vis=target, mode="clip", datacolumn=column, spw=selection,
                                clipminmax=[0.9, 1.1], clipoutside=False, flagbackup=False) == {}
                    with table(target, ack=False) as ms:
                        for row, ddid in enumerate(ms.getcol("DATA_DESC_ID")):
                            selected = selection in ("", str(ddid))
                            expected = spectral_flags if ddid == defined_window else scalar_flags
                            np.testing.assert_array_equal(ms.getcell("FLAG", row),
                                                          expected if selected else BEFORE_FLAGS)
                            np.testing.assert_array_equal(ms.getcell("DATA", row), OBSERVED)
                            assert not ms.getcell("FLAG_ROW", row)
    return 12


def compare_residual_operands():
    copytree("data.ms", "dual-observed.ms")
    float_values = np.full(OBSERVED.shape, 3, dtype="f4")
    model = MODEL + np.complex64(1j) * (MODEL == 2)
    with table("dual-observed.ms", readonly=False, ack=False) as ms:
        shape = ms.getcol("DATA").shape
        ms.addcols(maketabdesc(makearrcoldesc("FLOAT_DATA", 0.0, ndim=2, valuetype="float")))
        ms.putcol("FLOAT_DATA", np.broadcast_to(float_values, shape).copy())
        ms.putcol("MODEL_DATA", np.broadcast_to(model, shape).copy())
    copytree("dual-observed.ms", "float-observed.ms")
    with table("float-observed.ms", readonly=False, ack=False) as ms:
        ms.removecols(["DATA"])
    bounds = [0.9, 1.6]
    for source in ("float-observed", "dual-observed"):
        for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
            target = f"{source}-{name}.ms"
            copytree(f"{source}.ms", target)
            observed = OBSERVED if source == "dual-observed" and name == "native" else float_values
            amplitudes = np.abs(observed - model)
            expected = BEFORE_FLAGS | ((amplitudes >= bounds[0]) & (amplitudes <= bounds[1]))
            assert task(vis=target, mode="clip", datacolumn="RESIDUAL_DATA", clipminmax=bounds,
                        clipoutside=False) == {}
            with table(target, ack=False) as ms, casa_table(target) as casa, table(
                f"{target}.flagversions/flags.flagdata_1", ack=False,
            ) as saved:
                for row in range(ms.nrows()):
                    np.testing.assert_array_equal(ms.getcell("FLAG", row), expected)
                    np.testing.assert_array_equal(casa.getcell("FLAG", row).T, expected)
                    np.testing.assert_array_equal(ms.getcell("FLOAT_DATA", row), float_values)
                    np.testing.assert_array_equal(ms.getcell("MODEL_DATA", row), model)
                    np.testing.assert_array_equal(saved.getcell("FLAG", row), BEFORE_FLAGS)
                    if source == "dual-observed":
                        np.testing.assert_array_equal(ms.getcell("DATA", row), OBSERVED)
                    assert not ms.getcell("FLAG_ROW", row)
    float_amplitudes = np.abs(float_values - model)
    assert np.any(np.isclose(np.abs(OBSERVED - model), 1.0)
                  & ((float_amplitudes < bounds[0]) | (float_amplitudes > bounds[1])))

    copytree("data.ms", "model-without-corrected.ms")
    with table("model-without-corrected.ms", readonly=False, ack=False) as ms:
        ms.removecols(["CORRECTED_DATA"])
    for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
        target = f"model-without-corrected-{name}.ms"
        copytree("model-without-corrected.ms", target)
        before = snapshot(target)
        assert task(vis=target, mode="clip", datacolumn="RESIDUAL", clipminmax=bounds,
                    clipoutside=False) == {}
        with table(f"{target}.flagversions/flags.flagdata_1", ack=False) as saved:
            for row, (_, flags, _) in enumerate(before):
                np.testing.assert_array_equal(saved.getcell("FLAG", row), flags)
        for before_row, after_row in zip(before, snapshot(target), strict=True):
            for want, got in zip(before_row, after_row, strict=True):
                np.testing.assert_array_equal(want, got)


def compare_virtual_model_selection():
    vis = "virtual-model.ms"
    copytree("data.ms", vis)
    casatasks.setjy(vis=vis, field="FLUX", standard="manual", fluxdensity=[1, 0, 0, 0],
                   usescratch=False)
    with table(vis, ack=False) as ms:
        np.testing.assert_array_equal(ms.getcell("MODEL_DATA", 0), MODEL)
        fields = ms.getcol("FIELD_ID")
    options = dict(mode="clip", clipminmax=[0.9, 1.1], clipoutside=False, flagbackup=False)
    copytree(vis, "virtual-reference.ms")
    assert casatasks.flagdata(vis="virtual-reference.ms", datacolumn="MODEL", field="FLUX", **options) == {}
    for field, (_, flags, _) in zip(fields, snapshot("virtual-reference.ms"), strict=True):
        np.testing.assert_array_equal(flags, np.ones_like(flags) if field == 0 else BEFORE_FLAGS)

    original, auxiliary = snapshot(vis), auxiliary_content(vis)
    for column in ("MODEL", "RESIDUAL", "RESIDUAL_DATA"):
        try:
            tasks.flagdata(vis=vis, datacolumn=column, field="FLUX", **options)
        except RuntimeError as error:
            assert "virtual model for FIELD_ID 0" in str(error) and "#1617" in str(error)
        else:
            raise AssertionError(column)
        assert_preserved(vis, original, auxiliary)

    expected = BEFORE_FLAGS | (np.abs(MODEL) == 1)
    for name, task in (("casa", casatasks.flagdata), ("native", tasks.flagdata)):
        path = f"physical-field-{name}.ms"
        copytree(vis, path)
        assert task(vis=path, datacolumn="MODEL", field="PHASE", **options) == {}
        for field, (_, flags, _) in zip(fields, snapshot(path), strict=True):
            np.testing.assert_array_equal(flags, expected if field == 1 else BEFORE_FLAGS)


def compare_repeat_applications():
    options = dict(mode="clip", datacolumn="RESIDUAL", clipminmax=[0.9, 1.1], clipoutside=False,
                   flagbackup=False)
    expected = BEFORE_FLAGS | (np.abs(CORRECTED - MODEL) == 1)
    for path, first, second in (("native-first.ms", tasks.flagdata, casatasks.flagdata),
                                ("casa-first.ms", casatasks.flagdata, tasks.flagdata)):
        copytree("data.ms", path)
        for task in (first, second):
            assert task(vis=path, **options) == {}
            check_product(path, expected)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-clip-columns-") as work, chdir(work):
        make_inputs()
        products = compare_products()
        compare_preview()
        refusals = compare_unavailable_columns()
        compare_sparse_spectra()
        products += compare_spectrum_windows()
        compare_residual_operands()
        compare_virtual_model_selection()
        compare_repeat_applications()
    print(f"{products + 5} clip data-column products, one preview, {refusals + 2} refusals and "
          "cross-tool repeats matched CASA. Verified two sparse-spectrum protections, explicit "
          "observed-column precedence and the virtual-model guard.")


if __name__ == "__main__":
    main()
