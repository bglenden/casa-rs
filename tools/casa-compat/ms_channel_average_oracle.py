#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare flag-aware channel averaging through public split and mstransform."""

from pathlib import Path
from hashlib import blake2b
import signal
import subprocess
import sys
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from ms_data_columns_oracle import add_float_and_lag, clone
from ms_selection_tasks_oracle import MAIN, add_second_window, casa_table, make_input

SPECTRA = ("WEIGHT_SPECTRUM", "SIGMA_SPECTRUM")
CORRECTED_WEIGHT = "CORRECTED_WEIGHT_SPECTRUM"
COMPLEX = ("DATA", "CORRECTED_DATA", "MODEL_DATA")
SPECTRAL_WINDOW = ("NAME", "NUM_CHAN", "CHAN_FREQ", "CHAN_WIDTH", "EFFECTIVE_BW", "RESOLUTION",
                   "REF_FREQUENCY", "TOTAL_BANDWIDTH")


def add_statistics(path):
    with table(str(path), readonly=False, ack=False) as ms:
        shape = ms.getcol("DATA").shape
        sequence = np.arange(np.prod(shape), dtype=np.float32).reshape(shape)
        for index, name in enumerate(COMPLEX):
            ms.putcol(name, ((1000 * index + sequence) + 1j * (sequence + 1)).astype(np.complex64))
        flags = (sequence % 13 == 4)
        flags[0, 1:4, 0] = True
        flags[-1, :, :] = True
        ms.putcol("FLAG", flags)
        ms.putcol("FLAG_ROW", flags.all(axis=(1, 2)))
        weights = (1 + sequence % 5).astype(np.float32)
        sigmas = ((1 + sequence % 7) / 2).astype(np.float32)
        # First bin, second correlation has no valid weight even though it is unflagged.
        weights[0, :2, 1] = 0
        sigmas[0, :2, 1] = 0
        for name, values in zip(SPECTRA, (weights, sigmas)):
            ms.addcols(maketabdesc(makearrcoldesc(name, 0, ndim=2, valuetype="float")))
            ms.putcol(name, values)
        ms.putcol("WEIGHT", np.maximum(weights[:, 0, :], 1))
        ms.putcol("SIGMA", np.maximum(sigmas[:, 0, :], 1))


def compare_array(label, actual, expected):
    if np.issubdtype(actual.dtype, np.inexact):
        np.testing.assert_allclose(actual, expected, rtol=1e-6, atol=1e-7, err_msg=label)
    else:
        np.testing.assert_array_equal(actual, expected, err_msg=label)


def channel_parameters(task, width, average=True):
    return {"width": width} if task == "split" else {"chanaverage": average, "chanbin": width}


def compare(root, source, task, selector, width, spw="", *, native_width=None, force_spectra=False,
            field="", reference_source=None):
    case = f"{task}-{source.stem}-{selector}-{width!r}-{spw}-{field}-{force_spectra}-{native_width!r}"
    name = blake2b(case.encode(), digest_size=8).hexdigest()
    targets = [root / f"{task}-{name}-{side}.ms" for side in ("casa", "native")]
    for module, target in zip((casatasks, tasks), targets):
        vis = reference_source if module == casatasks and reference_source else source
        arg_width = width if module == casatasks or native_width is None else native_width
        params = channel_parameters(task, arg_width, average=not isinstance(width, int) or width > 1)
        if force_spectra:
            params["usewtspectrum"] = True
        result = getattr(module, task)(vis=str(vis), outputvis=str(target), datacolumn=selector,
                                       spw=spw, field=field, **params)
        assert result is None, (case, module.__name__, result)
    with (table(str(targets[0]), ack=False) as want,
          table(str(targets[1]), ack=False) as got,
          casa_table(targets[1]) as interoperable):
        optional = set(COMPLEX[1:] + SPECTRA + (CORRECTED_WEIGHT,))
        names = optional.intersection(want.colnames())
        assert optional.intersection(got.colnames()) == names, case
        assert want.nrows() == got.nrows(), case
        for name in sorted(set(MAIN) | names):
            for row in range(got.nrows()):
                if name in SPECTRA and not want.iscelldefined(name, row):
                    assert not got.iscelldefined(name, row), (case, name, row)
                    continue
                actual = np.asarray(got.getcell(name, row))
                compare_array(f"{case} {name} row {row}", actual, want.getcell(name, row))
                np.testing.assert_array_equal(np.asarray(interoperable.getcell(name, row)).T, actual,
                                              err_msg=f"{case} {name} row {row}")
    with (table(str(targets[0] / "SPECTRAL_WINDOW"), ack=False) as want,
          table(str(targets[1] / "SPECTRAL_WINDOW"), ack=False) as got,
          casa_table(targets[1] / "SPECTRAL_WINDOW") as interoperable):
        assert want.nrows() == got.nrows(), case
        for row in range(want.nrows()):
            for name in SPECTRAL_WINDOW:
                actual = got.getcell(name, row)
                expected = want.getcell(name, row)
                if name in ("NAME", "NUM_CHAN"):
                    np.testing.assert_array_equal(actual, expected, err_msg=f"{case} {name}")
                else:
                    np.testing.assert_allclose(actual, expected, rtol=0, atol=2e-5, err_msg=f"{case} {name}")
                np.testing.assert_array_equal(interoperable.getcell(name, row), actual,
                                              err_msg=f"{case} {name}")
    with (table(str(targets[0] / "DATA_DESCRIPTION"), ack=False) as want,
          table(str(targets[1] / "DATA_DESCRIPTION"), ack=False) as got):
        for name in ("SPECTRAL_WINDOW_ID", "POLARIZATION_ID"):
            np.testing.assert_array_equal(got.getcol(name), want.getcol(name), err_msg=f"{case} {name}")


def reject(root, source, task, width, spw, kind, diagnostic):
    for module in (casatasks, tasks):
        target = root / f"{task}-{spw}-{width}-{module.__name__}.ms"
        try:
            getattr(module, task)(vis=str(source), outputvis=str(target), datacolumn="data",
                                  spw=spw, **channel_parameters(task, width))
        except kind as error:
            assert diagnostic in str(error), (task, width, str(error))
        else:
            raise AssertionError((task, width, module.__name__))
        assert not target.exists(), target


def verify_width_crash(root, source, task, case, signal_number, spw):
    reference = root / f"{task}-{case}-casa-crash.ms"
    worker = """
import casatasks, numpy as np, resource, sys
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
task, case, source, output, spw = sys.argv[1:]
value = {'scalar': np.array(2), 'zero': [0, 2], 'negative': [-1, 2], 'empty': []}[case]
params = {'width': value} if task == 'split' else {'chanaverage': True, 'chanbin': value}
getattr(casatasks, task)(vis=source, outputvis=output, spw=spw, datacolumn='data', **params)
"""
    result = subprocess.run([sys.executable, "-c", worker, task, case, str(source), str(reference), spw],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=25)
    assert result.returncode == -signal_number, (task, case, result.returncode)
    assert reference.exists(), (task, case)
    if case == "scalar":
        compare(root, source, task, "data", 2, spw, native_width=np.array(2))
        return
    value = {"zero": [0, 2], "negative": [-1, 2], "empty": []}[case]
    output = root / f"{task}-{case}-native-rejected.ms"
    try:
        getattr(tasks, task)(vis=str(source), outputvis=str(output), spw=spw, datacolumn="data",
                             **channel_parameters(task, value))
    except ValueError as error:
        assert ("positive integers" if value else "at least one width") in str(error), str(error)
    else:
        raise AssertionError((task, case))
    assert not output.exists(), output


def sparse_spectra(source, target, missing_row, columns=SPECTRA):
    clone(source, target)
    with table(str(target), readonly=False, ack=False) as ms:
        for name in columns:
            values = ms.getcol(name)
            ms.removecols(name)
            ms.addcols(maketabdesc(makearrcoldesc(name, 0, ndim=2, valuetype="float")))
            for row, value in enumerate(values):
                if row != missing_row:
                    ms.putcell(name, row, value)


def reject_missing_spectrum(root, source, task, selector, width, name):
    label = f"{task}-{source.stem}-{width}-{selector}"
    for module in (casatasks, tasks):
        target = root / f"{label}-{module.__name__}.ms"
        try:
            getattr(module, task)(vis=str(source), outputvis=str(target), datacolumn=selector,
                                  **channel_parameters(task, width, average=width > 1))
        except RuntimeError as error:
            assert f"no array in row 1 in column {name}" in str(error), (label, str(error))
        else:
            raise AssertionError((label, module.__name__))
        if module == tasks:
            assert not target.exists(), target


def verify_sparse_selection_and_segmented_widths(root, sparse, spectra, windows, task):
    worker = """
import casatasks, resource, sys
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
task, source, output, case = sys.argv[1:]
width = 2 if case == 'spectra' else [2, 2]
opts = {'field': '1', 'datacolumn': 'corrected'} if case == 'spectra' else {
    'spw': '1:0~3;4~5,0:2~5', 'datacolumn': 'data',
}
opts.update({'width': width} if task == 'split' else {'chanaverage': True, 'chanbin': width})
getattr(casatasks, task)(vis=source, outputvis=output, **opts)
"""
    for source, case in ((sparse, "spectra"), (windows, "ranges")):
        reference = root / f"{task}-{case}-casa-crash.ms"
        result = subprocess.run([sys.executable, "-c", worker, task, str(source), str(reference), case],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=25)
        assert result.returncode == -signal.SIGSEGV, (task, case, result.returncode)
        assert reference.exists(), (task, case)
    compare(root, sparse, task, "corrected", 2, field="1", reference_source=spectra)

    output = root / f"{task}-segmented-native.ms"
    getattr(tasks, task)(vis=str(windows), outputvis=str(output), datacolumn="data",
                         spw="1:0~3;4~5,0:2~5", **channel_parameters(task, [2, 2]))
    with (table(str(output), ack=False) as native,
          table(str(output / "DATA_DESCRIPTION"), ack=False) as dd,
          table(str(output / "SPECTRAL_WINDOW"), ack=False) as windows_table,
          casa_table(output) as readback):
        np.testing.assert_array_equal(windows_table.getcol("NAME"), ["SPW1", "SPW0"])
        np.testing.assert_array_equal(windows_table.getcol("NUM_CHAN"), [3, 2])
        descriptions = dd.getcol("SPECTRAL_WINDOW_ID")[native.getcol("DATA_DESC_ID")]
        for spw, expression in enumerate(("1:0~3;4~5", "0:2~5")):
            expected = root / f"{task}-segmented-{spw}-casa.ms"
            getattr(casatasks, task)(vis=str(windows), outputvis=str(expected), datacolumn="data",
                                     spw=expression, **channel_parameters(task, 2))
            with (table(str(expected), ack=False) as reference,
                  table(str(expected / "SPECTRAL_WINDOW"), ack=False) as ref_spw):
                indices = np.flatnonzero(descriptions == spw)
                assert reference.nrows() == len(indices)
                for name in ("TIME", "DATA", "FLAG", "WEIGHT", "SIGMA", *SPECTRA):
                    original = reference.getcol(name)
                    for i, row in enumerate(indices):
                        actual = np.asarray(native.getcell(name, int(row)))
                        compare_array(f"{task} {expression} {name} row {i}", actual, original[i])
                        np.testing.assert_array_equal(np.asarray(readback.getcell(name, int(row))).T, actual)
                for name in SPECTRAL_WINDOW:
                    np.testing.assert_array_equal(windows_table.getcell(name, spw), ref_spw.getcell(name, 0))


def verify_sparse_statistics(root, source, task, visibility):
    selector = visibility.lower()
    targets = [root / f"{task}-{selector}-statistics-{side}.ms" for side in ("casa", "native")]
    for module, target in zip((casatasks, tasks), targets):
        getattr(module, task)(vis=str(source), outputvis=str(target), datacolumn=selector,
                              **channel_parameters(task, 2))
    with (table(str(source), ack=False) as original,
          table(str(targets[0]), ack=False) as reference,
          table(str(targets[1]), ack=False) as output,
          casa_table(targets[1]) as readback):
        for name in ("ANTENNA1", "ANTENNA2", "TIME"):
            np.testing.assert_array_equal(output.getcol(name), original.getcol(name))
        raw_flags = original.getcol("FLAG")[:, :6, :].reshape(output.nrows(), 3, 2, 2)
        bin_flags = raw_flags.all(axis=2)
        include = raw_flags == bin_flags[:, :, None, :]
        weights = original.getcol("WEIGHT_SPECTRUM")[:, :6, :].reshape(raw_flags.shape)
        spectral_weight = np.where(include, weights, np.float32(0)).sum(axis=2)
        spectral_sigma = np.full(spectral_weight.shape, -1, dtype=np.float32)
        np.divide(1, np.sqrt(spectral_weight), out=spectral_sigma, where=spectral_weight > 0)
        use_bin = bin_flags == bin_flags.all(axis=1)[:, None, :]
        total = use_bin.sum(axis=1)
        expected = {
            "WEIGHT": np.where(use_bin, spectral_weight, 0).sum(axis=1) / total,
            "SIGMA": np.where(use_bin, spectral_sigma, 0).sum(axis=1) / total,
            "WEIGHT_SPECTRUM": spectral_weight,
            "SIGMA_SPECTRUM": spectral_sigma,
        }
        for name in ("WEIGHT", "SIGMA", *SPECTRA):
            assert not reference.iscelldefined(name, 0), (task, visibility, name)
            compare_array(f"{task} {visibility} {name}", output.getcol(name), expected[name])
            np.testing.assert_array_equal(readback.getcol(name).T, output.getcol(name))
        for name in (visibility, "FLAG"):
            if visibility == "LAG_DATA":
                assert not reference.iscelldefined(name, 0), (task, name)
                values = original.getcol(name) if name == visibility else bin_flags
            else:
                values = reference.getcol(name)
            compare_array(f"{task} {name}", output.getcol(name), values)
            np.testing.assert_array_equal(readback.getcol(name).T, output.getcol(name))


def main():
    assert tuple(casatasks.version()) == (6, 7, 6, 14)
    with TemporaryDirectory(prefix="ms-channel-average-") as work:
        root = Path(work)
        spectra = root / "spectra.ms"
        make_input(spectra, channels=7)
        add_statistics(spectra)
        extra = root / "extra.ms"
        clone(spectra, extra)
        add_float_and_lag(extra)
        float_only = root / "float-only.ms"
        lag_only = root / "lag-only.ms"
        clone(extra, float_only, (*COMPLEX, "LAG_DATA"))
        clone(extra, lag_only, (*COMPLEX, "FLOAT_DATA"))
        scalars = root / "scalars.ms"
        clone(spectra, scalars, SPECTRA)
        weight_only = root / "weight-only.ms"
        clone(spectra, weight_only, ("SIGMA_SPECTRUM",))
        sigma_only = root / "sigma-only.ms"
        clone(spectra, sigma_only, ("WEIGHT_SPECTRUM",))
        missing_first = root / "first-spectrum-undefined.ms"
        sparse_spectra(spectra, missing_first, 0)
        missing_later = root / "later-spectrum-undefined.ms"
        sparse_spectra(spectra, missing_later, 1)
        missing_sigma = root / "later-sigma-undefined.ms"
        sparse_spectra(spectra, missing_sigma, 1, ("SIGMA_SPECTRUM",))
        corrected_weight = root / "with-corrected-weight.ms"
        clone(spectra, corrected_weight)
        with table(str(corrected_weight), readonly=False, ack=False) as ms:
            ms.addcols(maketabdesc(makearrcoldesc(CORRECTED_WEIGHT, 0, ndim=2, valuetype="float")))
            ms.putcol(CORRECTED_WEIGHT, ms.getcol("WEIGHT_SPECTRUM"))
        windows = root / "two-spws.ms"
        clone(spectra, windows)
        add_second_window(windows)
        descending = root / "descending.ms"
        clone(spectra, descending)
        with table(str(descending / "SPECTRAL_WINDOW"), readonly=False, ack=False) as spw:
            spw.putcell("CHAN_FREQ", 0, 100e9 - np.array([0, 1, 3, 6, 10, 11, 13]) * 1e6)
            spw.putcell("RESOLUTION", 0, np.array([1, 2, 3, 1, 2, 2, 3]) * 1e6)
        with table(str(spectra), ack=False) as original:
            source_flags = original.getcol("FLAG")
            source_weights = original.getcol("WEIGHT_SPECTRUM")
        columns = ("data", "corrected", "model", "all")
        cases = [(spectra, col, width, "") for col in columns for width in (1, 2, 3, 100)]
        cases += [(scalars, col, 2, "") for col in columns]
        cases += [(weight_only, col, width, "") for col in ("data", "corrected") for width in (1, 2)]
        cases += [(sigma_only, "data", width, "") for width in (1, 2)]
        cases += [(corrected_weight, "all", width, "") for width in (1, 2)]
        cases += [(missing_first, "corrected", 2, ""), (missing_sigma, "corrected", 2, "")]
        cases += [(spectra, "data", 2, spw) for spw in ("0:2~6", "0:0~6^2", "0:0~2;4~6")]
        cases += [(windows, "all", 3, "0,1"), (windows, "all", 1, "1,0"),
                  (descending, "data", 2, "")]
        for task in ("split", "mstransform"):
            for source, selector, width, spw in cases:
                compare(root, source, task, selector, width, spw)
            for value, spw in (([2], "0,1"), ([2, 3], "0,1"), ([2, 3], "1,0"),
                               ([1, 2], "0,1"), ((2, 3), "0,1"), (np.array([2, 3]), "0,1"),
                               (np.int64(2), "0"), ([1], "0")):
                compare(root, windows, task, "data", value, spw)
            name = "width" if task == "split" else "chanbin"
            reject(root, windows, task, [2, 3], "0", ValueError,
                   f"Number of {name} is different from the number of spw")
            reject(root, windows, task, 2.5, "0", AssertionError, "must be of cInt type")
            dtype = np.int16 if task == "split" else np.uint32
            reject(root, windows, task, np.array([2], dtype=dtype), "0", AssertionError, "must be of cIntVec type")
            reject_missing_spectrum(root, missing_later, task, "corrected", 2, "WEIGHT_SPECTRUM")
            reject_missing_spectrum(root, missing_sigma, task, "data", 1, "SIGMA_SPECTRUM")
            verify_sparse_selection_and_segmented_widths(root, missing_first, spectra, windows, task)
            verify_sparse_statistics(root, float_only, task, "FLOAT_DATA")
            verify_sparse_statistics(root, lag_only, task, "LAG_DATA")
            for case, signal_number, spw in (("scalar", signal.SIGFPE, "0"),
                                             ("zero", signal.SIGFPE, "0,1"),
                                             ("negative", signal.SIGSEGV, "0,1")):
                verify_width_crash(root, windows, task, case, signal_number, spw)
        reject(root, windows, "split", [], "0", IndexError, "list index out of range")
        for width in ("2", "bad", 0, range(2, 3)):
            compare(root, windows, "split", "data", width, "0")
        reject(root, windows, "mstransform", 1, "0", ValueError, "must be > 1")
        verify_width_crash(root, windows, "mstransform", "empty", signal.SIGSEGV, "0")
        for selector in ("data", "corrected"):
            for width in (1, 2):
                compare(root, scalars, "mstransform", selector, width, force_spectra=True)
        with table(str(spectra), ack=False) as source:
            np.testing.assert_array_equal(source.getcol("FLAG"), source_flags)
            np.testing.assert_array_equal(source.getcol("WEIGHT_SPECTRUM"), source_weights)
        print(f"{2 * len(cases) + 34} channel-averaging outputs and 12 rejections compared with CASA 6.7.6.14, "
              "including four sparse-statistics products and 11 CASA crash cases")


if __name__ == "__main__":
    main()
