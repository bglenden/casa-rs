#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare clip visibility operators and sample masks with CASA 6.7.6.14."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from flagdata_autocorr_oracle import assert_same_visibilities
from flagdata_correlation_products_oracle import mixed_products_fixture
from flagdata_preview_oracle import assert_preserved, auxiliary_content, changed_samples
from flagdata_quack_oracle import assert_equal
from flagdata_summary_oracle import snapshot
from record_casa_returns import make_cal_ms

SAMPLES = np.array([[-3-4j, 10+2j], [.5-.5j, -1-5j], [2j, 2j], [1, 7+8j]], dtype="c8")
FLOAT_SAMPLES = np.array([[-3, -2], [-1, -1e-8], [0, 1.5], [2, 2]], dtype="f4")


def prepare(path, *, floats=False):
    with table(path, readonly=False, ack=False) as ms:
        shape = ms.getcol("DATA").shape
        data = np.broadcast_to(SAMPLES, shape)
        corrected = np.broadcast_to(SAMPLES[:, ::-1], shape)
        ms.putcol("DATA", data.copy())
        ms.putcol("CORRECTED_DATA", corrected.copy())
        ms.putcol("FLAG", np.zeros(shape, dtype=bool))
        ms.putcol("FLAG_ROW", np.zeros(ms.nrows(), dtype=bool))
        if floats:
            ms.addcols(maketabdesc(makearrcoldesc("FLOAT_DATA", 0.0, ndim=2, valuetype="float")))
            ms.putcol("FLOAT_DATA", np.broadcast_to(FLOAT_SAMPLES, shape).copy())
        return ms.getcol("DATA_DESC_ID")


def compare_products(ddids):
    sources = dict(DATA=SAMPLES, CORRECTED=SAMPLES[:, ::-1], FLOAT_DATA=FLOAT_SAMPLES)
    squared = lambda data: data.real**2 + data.imag**2
    cases = (
        ("linear", "ABS_XX", "DATA", np.abs, (1, 2), {0: (0,)}, slice(None)),
        ("linear", "ABSXX", "DATA", np.abs, (1, 2), {0: (0,)}, slice(None)),
        ("linear", "ABS_XX,RR", "DATA", np.abs, (1, 2), {0: (0,)}, slice(None)),
        ("linear", "ABS_ALL", "DATA", np.abs, (1, 2), {0: (0, 1)}, slice(None)),
        ("linear", "REAL XX", "DATA", np.real, (-1, 1), {0: (0,)}, slice(None)),
        ("linear", " REAL_XX", "DATA", np.real, (-1, 1), {0: (0,)}, slice(None)),
        ("linear", "XX_REAL", "DATA", np.real, (-1, 1), {0: (0,)}, slice(None)),
        ("linear", "IMAG_XX", "DATA", np.imag, (-0.6, -0.4), {0: (0,)}, slice(None)),
        ("linear", "ARG_XX", "DATA", np.angle, (-2, 2), {0: (0,)}, slice(None)),
        ("linear", "NORM_XX", "DATA", squared, (1, 2), {0: (0,)}, slice(None)),
        ("linear", "REAL_YY", "CORRECTED", np.real, (-1, 1), {0: (1,)}, slice(1, 3)),
        ("mixed", "ABS_ALL", "DATA", np.abs, (1, 2), {0: (0, 1)}, slice(None)),
        ("mixed", "NORM_RR", "DATA", squared, (1, 5), {1: (1,)}, slice(None)),
        ("floats", "REAL_XX", "FLOAT_DATA", np.real, (-1, 1), {0: (0,)}, slice(None)),
        ("floats", "ABS_XX", "FLOAT_DATA", np.real, (-1, 1), {0: (0,)}, slice(None)),
        ("floats", "IMAG_XX", "FLOAT_DATA", np.real, (-1, 1), {0: (0,)}, slice(None)),
        ("floats", "ARG_XX", "FLOAT_DATA", np.real, (-1, 1), {0: (0,)}, slice(None)),
        ("floats", "NORM_XX", "FLOAT_DATA", np.real, (-1, 1), {0: (0,)}, slice(None)),
        ("floats", "ABSXX YY", "FLOAT_DATA", np.real, (-1, 1), {0: (1,)}, slice(None)),
    )
    for index, (source, expression, column, metric, bounds, planes, channels) in enumerate(cases):
        first = f"reference-{index}.ms"
        second = f"native-{index}.ms"
        options = dict(mode="clip", correlation=expression, datacolumn=column,
                       clipminmax=list(bounds), clipoutside=False,
                       flagbackup=expression.startswith(" "))
        if channels.start is not None:
            options["spw"] = f"0:{channels.start}~{channels.stop - 1}"
        for path, task in ((first, casatasks.flagdata), (second, tasks.flagdata)):
            copytree(f"{source}.ms", path)
            assert task(vis=path, **options) == {}, options
            if options["flagbackup"]:
                assert Path(f"{path}.flagversions/flags.flagdata_1").is_dir()
        assert_equal(first, second)
        values = metric(sources[column])
        within = (values >= bounds[0]) & (values <= bounds[1])
        for ddid, (flag_row, flags, data) in zip(ddids[source], snapshot(second), strict=True):
            selected = list(planes.get(ddid, ()))
            expected = np.zeros_like(flags)
            expected[channels, selected] = within[channels, selected]
            np.testing.assert_array_equal(flags, expected, err_msg=str(options))
            assert not flag_row
            np.testing.assert_array_equal(data, SAMPLES)
        if column != "DATA":
            with table(second, ack=False) as ms:
                physical_column = "CORRECTED_DATA" if column == "CORRECTED" else column
                np.testing.assert_array_equal(
                    ms.getcol(physical_column),
                    np.broadcast_to(sources[column], (ms.nrows(), 4, 2)),
                )
    return len(cases)


def compare_protections():
    cases = (
        (dict(correlation="IMAG_XX", clipminmax=[-1, 1], clipoutside=False),
         [(1, 0), (3, 0)], [(0, 0), (0, 1)]),
        (dict(correlation="REAL_XX", clipzeros=True), [(2, 0)], [(0, 0)]),
    )
    for index, (options, scientific, casa_extra) in enumerate(cases):
        expected = np.zeros((4, 2), dtype=bool)
        for channel, plane in scientific:
            expected[channel, plane] = True
        casa_flags = expected.copy()
        for channel, plane in casa_extra:
            casa_flags[channel, plane] = True
        for name, task, mask in (("reference", casatasks.flagdata, casa_flags),
                                 ("native", tasks.flagdata, expected)):
            path = f"protection-{name}-{index}.ms"
            copytree("linear.ms", path)
            assert task(vis=path, mode="clip", flagbackup=False, **options) == {}
            for flag_row, flags, data in snapshot(path):
                np.testing.assert_array_equal(flags, mask, err_msg=str(options))
                assert not flag_row
                np.testing.assert_array_equal(data, SAMPLES)
    return len(cases)


def compare_operator_errors():
    cases = (("floats", "FLOAT_DATA", "ABSXX"),
             ("floats", "FLOAT_DATA", "NORMXX"),
             ("linear", "DATA", "ABS_BAD"))
    for index, (source, column, expression) in enumerate(cases):
        before = snapshot(f"{source}.ms")
        messages = []
        for name, task in (("reference", casatasks.flagdata), ("native", tasks.flagdata)):
            path = f"float-error-{name}-{index}.ms"
            copytree(f"{source}.ms", path)
            try:
                task(vis=path, mode="clip", datacolumn=column, correlation=expression)
            except RuntimeError as error:
                completion = getattr(error, "completion", None)
                messages.append(completion.message if completion else str(error))
            else:
                raise AssertionError(f"{name} accepted unknown clip expression {expression!r}")
            assert_same_visibilities(path, before)
            assert Path(f"{path}.flagversions/flags.flagdata_1").is_dir()
        assert messages[1].endswith(messages[0]), (expression, messages)
    return len(cases)


def compare_amplitude_rounding():
    epsilon = np.finfo(np.float32).eps
    value = np.complex64(complex(epsilon, epsilon * 0.0003))
    assert np.abs(value) == epsilon
    expected = np.zeros((4, 2), dtype=bool)
    expected[0, 0] = True
    for name, task in (("reference", casatasks.flagdata), ("native", tasks.flagdata)):
        path = f"rounded-{name}.ms"
        copytree("linear.ms", path)
        with table(path, readonly=False, ack=False) as ms:
            data = ms.getcol("DATA")
            data[:, 0, 0] = value
            ms.putcol("DATA", data)
        assert task(vis=path, mode="clip", correlation="ABS_XX", clipzeros=True, flagbackup=False) == {}
        for flag_row, flags, stored in snapshot(path):
            assert not flag_row
            np.testing.assert_array_equal(flags, expected)
            assert stored[0, 0] == value


def compare_skipped_unknown_product():
    make_cal_ms("stored-i.ms", channels=4, scan_seconds=20, stokes="I", frequency="105GHz")
    before = snapshot("stored-i.ms")
    for name, task in (("reference", casatasks.flagdata), ("native", tasks.flagdata)):
        path = f"stored-i-{name}.ms"
        copytree("stored-i.ms", path)
        assert task(vis=path, mode="clip", correlation="ABS_BAD", spw="0") == {}
        assert_same_visibilities(path, before)
        assert Path(f"{path}.flagversions/flags.flagdata_1").is_dir()


def compare_preview():
    path = "linear.ms"
    before, auxiliary = snapshot(path), auxiliary_content(path)
    options = dict(mode="clip", correlation="NORM_XX", clipminmax=[1, 2], clipoutside=False)
    copytree(path, "preview-reference.ms")
    assert casatasks.flagdata(vis="preview-reference.ms", flagbackup=False, **options) == {}
    expected = changed_samples(before, snapshot("preview-reference.ms"))
    assert casatasks.flagdata(vis=path, action="calculate", **options) == {}
    result = tasks.run("flagdata", vis=path, action="calculate", **options)
    assert result.value == {}
    report = loads(result.stdout)["result"]
    assert report["changed_samples"] == sum(expected)
    assert report["changed_rows"] == sum(change > 0 for change in expected)
    assert report["backup_version"] is None
    assert_preserved(path, before, auxiliary)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-clip-operators-") as directory, chdir(directory):
        make_cal_ms("linear.ms", channels=4, scan_seconds=20)
        copytree("linear.ms", "floats.ms")
        mixed_products_fixture("mixed.ms")
        with table("mixed.ms/DATA_DESCRIPTION", readonly=False, ack=False) as descriptions:
            descriptions.putcell("FLAG_ROW", 1, True)
        ddids = {path: prepare(path + ".ms", floats=(path == "floats"))
                 for path in ("linear", "floats", "mixed")}
        products = compare_products(ddids)
        protections = compare_protections()
        errors = compare_operator_errors()
        compare_amplitude_rounding()
        compare_skipped_unknown_product()
        compare_preview()
    print(f"{products + 1} clip operator products, one preview, {errors} error backups and one "
          f"skipped buffer backup matched CASA 6.7.6.14, with {protections} verified protections")


if __name__ == "__main__":
    main()
