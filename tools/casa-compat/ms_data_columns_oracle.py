#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare physical visibility layouts through the public MS tasks."""

from pathlib import Path
import signal
import subprocess
import sys
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from ms_selection_tasks_oracle import MAIN, casa_table, columns_from_casacore, make_input

VISIBILITY = {"DATA", "CORRECTED_DATA", "MODEL_DATA", "FLOAT_DATA", "LAG_DATA"}
CHANNELS = slice(2, 5)


def clone(source, path, remove=()):
    with table(str(source), ack=False) as original:
        original.copy(str(path), deep=True, valuecopy=True).close()
    if remove:
        with table(str(path), readonly=False, ack=False) as copy:
            copy.removecols(list(remove))


def set_complex_columns(path):
    make_input(path)
    with table(str(path), readonly=False, ack=False) as ms:
        data = ms.getcol("DATA")
        ms.putcol("CORRECTED_DATA", data + np.complex64(1000 + 30j))
        ms.putcol("MODEL_DATA", data + np.complex64(2000 + 60j))
        # Deliberately independent calibrated weights and observed sigmas.
        indices = np.arange(ms.nrows() * data.shape[-1]).reshape(-1, data.shape[-1])
        ms.putcol("WEIGHT", (1 + indices % 7).astype(np.float32))
        ms.putcol("SIGMA", ((1 + indices % 5) / 2).astype(np.float32))


def add_float_and_lag(path):
    with table(str(path), readonly=False, ack=False) as ms:
        shape = ms.getcol("DATA").shape
        for name, dtype in (("FLOAT_DATA", "float"), ("LAG_DATA", "complex")):
            ms.addcols(maketabdesc(makearrcoldesc(name, 0, ndim=2, valuetype=dtype)))
        samples = np.arange(np.prod(shape), dtype=np.float32).reshape(shape)
        ms.putcol("FLOAT_DATA", samples + 3000)
        lag_shape = (shape[0], shape[1] + 4, shape[2])
        samples = np.arange(np.prod(lag_shape), dtype=np.float32).reshape(lag_shape)
        ms.putcol("LAG_DATA", ((samples + 4000) - 1j * samples).astype(np.complex64))


def compare_case(root, source, task, case, selector, *, preserve_complex=False, preserve_lag=False):
    paths = [root / f"{task}-{case}-{side}.ms" for side in ("casa", "native")]
    for module, target in zip((casatasks, tasks), paths):
        assert getattr(module, task)(vis=str(source), outputvis=str(target),
                                     datacolumn=selector, spw="0:2~4") is None
    with (table(str(paths[0]), ack=False) as reference,
          table(str(paths[1]), ack=False) as output,
          table(str(source), ack=False) as original,
          casa_table(paths[1]) as readback):
        expect_columns = VISIBILITY.intersection(reference.colnames())
        assert VISIBILITY.intersection(output.colnames()) == expect_columns
        assert output.nrows() == reference.nrows()
        columns = (set(MAIN) - {"DATA"}) | expect_columns
        for name in sorted(columns):
            if name == "DATA" and preserve_complex:
                expected = original.getcol("DATA")[:, CHANNELS, :]
                assert not np.array_equal(expected, reference.getcol(name))
            elif name == "LAG_DATA" and preserve_lag:
                assert not reference.iscelldefined(name, 0)
                expected = original.getcol(name)
            elif name == "FLAG" and preserve_lag and not reference.iscelldefined(name, 0):
                expected = original.getcol(name)[:, CHANNELS, :]
            else:
                expected = reference.getcol(name)
            if name in ("WEIGHT", "SIGMA"):
                np.testing.assert_allclose(output.getcol(name), expected, rtol=2e-7, atol=0,
                                           err_msg=f"{task} {case} {name}")
                np.testing.assert_array_equal(readback.getcol(name).T, output.getcol(name))
            else:
                np.testing.assert_array_equal(output.getcol(name), expected, err_msg=f"{task} {case} {name}")
                np.testing.assert_array_equal(readback.getcol(name).T, expected)


def reject_missing(root, source, task, selector, column):
    for module in (casatasks, tasks):
        output = root / f"{task}-{source.stem}-{selector}-{module.__name__}.ms"
        try:
            getattr(module, task)(vis=str(source), outputvis=str(output), datacolumn=selector)
        except RuntimeError as error:
            assert f"Desired column ({column}) not found" in str(error), str(error)
        else:
            raise AssertionError(f"{module.__name__}.{task} accepted a missing {column} column")
        assert not output.exists()


def copy_corrected_only_all(root, source, task):
    columns = (*(name for name in MAIN if name != "DATA"), "CORRECTED_DATA")
    expected = columns_from_casacore(source, columns)
    reference = root / f"{task}-corrected-only-all-casa.ms"
    code = (
        "import casatasks, resource, sys; "
        "resource.setrlimit(resource.RLIMIT_CORE, (0, 0)); "
        "getattr(casatasks, sys.argv[1])(vis=sys.argv[2], outputvis=sys.argv[3], datacolumn='all')"
    )
    result = subprocess.run([sys.executable, "-c", code, task, str(source), str(reference)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=25)
    assert result.returncode == -signal.SIGSEGV, (task, result.returncode)
    output = root / f"{task}-corrected-only-all-native.ms"
    assert getattr(tasks, task)(vis=str(source), outputvis=str(output), datacolumn="all") is None
    with (table(str(output), ack=False) as native, casa_table(output) as readback):
        assert VISIBILITY.intersection(native.colnames()) == {"CORRECTED_DATA"}
        for name, values in expected.items():
            np.testing.assert_array_equal(native.getcol(name), values, err_msg=name)
            np.testing.assert_array_equal(readback.getcol(name).T, values, err_msg=name)
    for name, values in columns_from_casacore(source, columns).items():
        np.testing.assert_array_equal(values, expected[name], err_msg=f"input {name}")


def reject_invalid_layout(root, source, task, index, value, message):
    for module in (casatasks, tasks):
        path = root / f"{task}-invalid-layout-{index}-{module.__name__}.ms"
        try:
            getattr(module, task)(vis=str(source), outputvis=str(path), datacolumn=value)
        except AssertionError as error:
            assert message in str(error), str(error)
        else:
            raise AssertionError(f"{module.__name__}.{task} accepted {value!r}")
        assert not path.exists()


def main():
    assert casatasks.version() == [6, 7, 6, 14], casatasks.version()
    with TemporaryDirectory(prefix="ms-column-layouts-") as scratch:
        root = Path(scratch)
        complex_ms = root / "complex.ms"
        set_complex_columns(complex_ms)
        all_ms = root / "all.ms"
        clone(complex_ms, all_ms)
        add_float_and_lag(all_ms)
        observed_only = root / "observed.ms"
        clone(complex_ms, observed_only, ("MODEL_DATA", "CORRECTED_DATA"))
        float_only = root / "float.ms"
        clone(all_ms, float_only, ("DATA", "CORRECTED_DATA", "MODEL_DATA", "LAG_DATA"))
        corrected_only = root / "corrected.ms"
        clone(complex_ms, corrected_only, ("DATA", "MODEL_DATA"))
        virtual_ms = root / "virtual.ms"
        clone(complex_ms, virtual_ms, ("MODEL_DATA",))
        casatasks.setjy(vis=str(virtual_ms), field="FLUX", standard="manual",
                       fluxdensity=[2.0, 0.0, 0.0, 0.0], usescratch=False)
        with table(str(virtual_ms / "SOURCE"), ack=False) as source:
            assert "SOURCE_MODEL" in source.colnames()

        snapshot = columns_from_casacore(all_ms, (*MAIN, *sorted(VISIBILITY - {"DATA"})))
        cases = [
            (complex_ms, "data", "data", {}),
            (complex_ms, "corrected", "corrected", {}),
            (complex_ms, "model", "model", {}),
            (complex_ms, "complex-combination", "data,model,corrected", {}),
            (complex_ms, "all-complex", "all", {}),
            (observed_only, "all-observed", "all", {}),
            (corrected_only, "rename-without-data", "corrected", {}),
            (float_only, "float-single", "float_data", {}),
            (all_ms, "float-and-data", "float_data,data", {"preserve_complex": True}),
            (all_ms, "lag-single", "lag_data", {"preserve_lag": True}),
            (all_ms, "lag-and-data", "lag_data,data", {"preserve_complex": True, "preserve_lag": True}),
            (all_ms, "all-types", "all", {"preserve_complex": True, "preserve_lag": True}),
        ]
        rejected = [
            (observed_only, "model", "MODEL_DATA"),
            (observed_only, "corrected", "CORRECTED_DATA"),
            (observed_only, "data,model,corrected", "MODEL_DATA"),
            (observed_only, "float_data,data", "FLOAT_DATA"),
            (observed_only, "lag_data", "LAG_DATA"),
            (virtual_ms, "model", "MODEL_DATA"),
        ]
        invalid_layouts = [
            ("junk", "unallowed value junk"),
            ("Model", "unallowed value Model"),
            (None, "null value not allowed"),
            (1, "must be of cStr type"),
            (np.array("data"), "must be of cStr type"),
        ]
        for task in ("split", "mstransform"):
            for source, case, selector, checks in cases:
                compare_case(root, source, task, case, selector, **checks)
            for source, selector, column in rejected:
                reject_missing(root, source, task, selector, column)
            copy_corrected_only_all(root, corrected_only, task)
            for index, (value, message) in enumerate(invalid_layouts):
                reject_invalid_layout(root, complex_ms, task, index, value, message)
        for name, values in columns_from_casacore(all_ms, snapshot).items():
            np.testing.assert_array_equal(values, snapshot[name])
        print(f"{2 * len(cases)} visibility-layout products and {2 * (len(rejected) + len(invalid_layouts))} rejections compared with CASA, "
              "and two corrected-only copies checked against their input after CASA crashed")


if __name__ == "__main__":
    main()
