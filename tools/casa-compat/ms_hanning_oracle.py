#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public mstransform Hanning with CASA 6.7.6.14.

Run with CASA, python-casacore, the current casars package, and
CASARS_MSTRANSFORM_BIN pointing to the built native task.
"""

from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from ms_data_columns_oracle import VISIBILITY, add_float_and_lag, clone
from ms_selection_tasks_oracle import MAIN, SUBTABLES, casa_table
from record_casa_returns import make_cal_ms


SPECTRA = ("WEIGHT_SPECTRUM", "SIGMA_SPECTRUM")
NOISE = {"WEIGHT", "SIGMA", *SPECTRA}


def fixture(path, channels, *, spectra=False, frequency="100GHz"):
    make_cal_ms(str(path), channels=channels, frequency=frequency)
    with table(str(path), readonly=False, ack=False) as ms:
        rows, _, correlations = ms.getcol("DATA").shape
        r, ch, c = np.ogrid[:rows, :channels, :correlations]
        data = (r + ch**2 + c + 1j * (ch**3 + 3 * c - 2 * r)).astype(np.complex64)
        ms.putcol("DATA", data)
        for name, values in (("CORRECTED_DATA", 2 * data + 15), ("MODEL_DATA", -3 * data + 5j)):
            if name not in ms.colnames():
                ms.addcols(maketabdesc(makearrcoldesc(name, 0, ndim=2, valuetype="complex")))
            ms.putcol(name, values)
        flags = np.zeros(data.shape, dtype=bool)
        flags[1, channels // 2, 0] = True
        flags[2, ::2] = True
        flags[3] = True
        row_flags = np.zeros(rows, dtype=bool)
        row_flags[4] = True
        ms.putcol("FLAG", flags)
        ms.putcol("FLAG_ROW", row_flags)
        ms.putcol("WEIGHT", (2 + r[:, 0] % 3 + c[0]).astype(np.float32))
        ms.putcol("SIGMA", (1 + 0.25 * (r[:, 0] % 5 + c[0])).astype(np.float32))
        if spectra:
            add_spectra(ms, data.shape)
    with table(str(path / "FLAG_CMD"), readonly=False, ack=False) as commands:
        commands.addrows(1)
        commands.putcell("APPLIED", 0, True)
        commands.putcell("COMMAND", 0, "mode='manual' spw='0:0'")


def add_spectra(ms, shape, *, start=0, sigma=True):
    r, ch, c = np.ogrid[:shape[0], :shape[1], :shape[2]]
    cells = [(SPECTRA[0], (1 + r % 4 + (ch + 1) ** 2 + c).astype(np.float32))]
    if sigma:
        cells.append((SPECTRA[1], (0.5 + 0.25 * (r % 3 + ch + 2 * c)).astype(np.float32)))
    for name, values in cells:
        ms.addcols(maketabdesc(makearrcoldesc(name, 0, ndim=2, valuetype="float")))
        ms.putcol(name, values[start:], startrow=start, nrow=shape[0] - start)


def compare_table(reference, native, columns, case, deviations=None):
    with (
        table(str(reference), ack=False) as expected,
        table(str(native), ack=False) as actual,
        casa_table(native) as readback,
    ):
        common = set(expected.colnames()) & set(actual.colnames())
        assert set(columns) <= common, (case, set(columns) - common)
        assert expected.nrows() == actual.nrows() == readback.nrows(), case
        for row in range(actual.nrows()):
            for name in columns:
                label = f"{case} {name} row {row}"
                got = actual.getcell(name, row)
                np.testing.assert_array_equal(np.asarray(readback.getcell(name, row)).T, got,
                                              err_msg=label)
                if deviations is not None and name in deviations:
                    deviations[name](row, got, expected)
                elif name in VISIBILITY | NOISE | {"TIME", "UVW"}:
                    np.testing.assert_allclose(got, expected.getcell(name, row), rtol=3e-6,
                                               atol=5e-7, err_msg=label)
                else:
                    np.testing.assert_array_equal(got, expected.getcell(name, row), err_msg=label)


def paired(root, source, case, *, datacolumn="data", hanning=True, deviations=None, **options):
    reference, native = (root / f"{case}-{side}.ms" for side in ("casa", "native"))
    for engine, path in ((casatasks, reference), (tasks, native)):
        assert engine.mstransform(vis=str(source), outputvis=str(path),
                                 datacolumn=datacolumn, hanning=hanning, **options) is None
    compare = VISIBILITY | NOISE
    with table(str(reference), ack=False) as expected, table(str(native), ack=False) as actual:
        present = compare & set(expected.colnames())
        assert present == compare & set(actual.colnames()), case
    compare_table(reference, native, (set(MAIN) - {"DATA"}) | present, case, deviations)
    for name in ("SPECTRAL_WINDOW", "DATA_DESCRIPTION", "POLARIZATION", "FIELD"):
        compare_table(reference / name, native / name, SUBTABLES[name], f"{case} {name}")
    compare_table(reference / "FLAG_CMD", native / "FLAG_CMD", ("COMMAND", "APPLIED"), case)
    with table(str(native / "HISTORY"), ack=False) as history:
        assert any("".join(message.split()) == "hanning=True"
                   for message in history.getcol("MESSAGE")), case
    print(f"passed {case}", flush=True)
    return reference, native


def mixed_windows(root, wide):
    narrow = root / "narrow.ms"
    fixture(narrow, 2, frequency="101GHz")
    mixed = root / "mixed.ms"
    casatasks.concat(vis=[str(wide), str(narrow)], concatvis=str(mixed))
    return mixed


def terminal_stride(root, source):
    # CASA abandons SPW metadata when a stride's nominal footprint extends
    # past the final input channel. The native footprint ends at that channel.
    reference, native = (root / f"tail-stride-{side}.ms" for side in ("casa", "native"))
    for engine, path in ((casatasks, reference), (tasks, native)):
        assert engine.mstransform(vis=str(source), outputvis=str(path), datacolumn="data",
                                 spw="0:0~8^2", hanning=True) is None
    compare_table(reference, native, MAIN, "terminal stride MAIN")
    with table(str(reference / "SPECTRAL_WINDOW"), ack=False) as casa_spw:
        assert casa_spw.getcell("NAME", 0) == ""
    with casa_table(native / "SPECTRAL_WINDOW") as spw:
        assert spw.getcell("NAME", 0) == "SPW0"
        assert spw.getcell("NUM_CHAN", 0) == 5
        np.testing.assert_array_equal(spw.getcell("CHAN_FREQ", 0),
                                      1e11 + 1e6 * np.array([0.5, 2.5, 4.5, 6.5, 8]))
        np.testing.assert_array_equal(spw.getcell("CHAN_WIDTH", 0), 1e6 * np.array([2, 2, 2, 2, 1]))
    print("passed terminal stride", flush=True)


def sparse_failure(root, source):
    sparse = root / "sparse.ms"
    clone(source, sparse)
    with table(str(sparse), readonly=False, ack=False) as ms:
        add_spectra(ms, ms.getcol("DATA").shape, sigma=False)
        # Put one undefined cell immediately after the visibility buffer's
        # defined first cell using a freshly added array descriptor.
        weights = ms.getcol(SPECTRA[0])
        ms.removecols(SPECTRA[0])
        ms.addcols(maketabdesc(makearrcoldesc(SPECTRA[0], 0, ndim=2, valuetype="float")))
        for row in (0, *range(2, ms.nrows())):
            ms.putcell(SPECTRA[0], row, weights[row])
    for engine in (casatasks, tasks):
        path = root / f"sparse-{engine.__name__}.ms"
        try:
            engine.mstransform(vis=str(sparse), outputvis=str(path),
                              datacolumn="corrected", hanning=True)
        except RuntimeError as error:
            assert "WEIGHT_SPECTRUM" in str(error), str(error)
            if engine is tasks:
                assert not path.exists(), path
        else:
            raise AssertionError(f"{engine.__name__} accepted an undefined spectrum in a buffer")
    print("passed sparse-spectrum refusal", flush=True)


def unused_sigma_spectrum(root, source):
    sparse = root / "unused-sigma.ms"
    clone(source, sparse)
    with table(str(sparse), readonly=False, ack=False) as ms:
        sigmas = ms.getcol("SIGMA_SPECTRUM")
        ms.removecols("SIGMA_SPECTRUM")
        ms.addcols(maketabdesc(makearrcoldesc("SIGMA_SPECTRUM", 0, ndim=2, valuetype="float")))
        for row in (0, *range(2, ms.nrows())):
            ms.putcell("SIGMA_SPECTRUM", row, sigmas[row])
    paired(root, sparse, "model-ignores-undefined-sigma-spectrum", datacolumn="model")


def nonpositive_and_unknown_weights(root, source):
    for label, value in (("zero", 0.), ("nan", np.nan)):
        vis = root / f"{label}.ms"
        clone(source, vis)
        with table(str(vis), readonly=False, ack=False) as ms:
            weights = ms.getcell("WEIGHT_SPECTRUM", 0)
            weights[3, 0] = value
            ms.putcell("WEIGHT_SPECTRUM", 0, weights)
        paired(root, vis, f"{label}-weight", datacolumn="corrected")


def invalid_switches(root, source):
    for value, diagnostic in ((None, "null value not allowed"),
                              (1, "must be of cBool type"),
                              ("true", "must be of cBool type")):
        for engine in (casatasks, tasks):
            path = root / f"bad-hanning-{engine.__name__}.ms"
            try:
                engine.mstransform(vis=str(source), outputvis=str(path),
                                  datacolumn="data", hanning=value)
            except AssertionError as error:
                assert str(error) == str({"hanning": [diagnostic]}), error
                assert not path.exists()
            else:
                raise AssertionError(f"{engine.__name__} accepted hanning={value!r}")
    print("passed invalid Hanning switches", flush=True)


def float_and_lag(root, source):
    mixed = root / "float-and-lag.ms"
    clone(source, mixed)
    add_float_and_lag(mixed)
    with table(str(mixed), readonly=False, ack=False) as ms:
        values = ms.getcol("FLOAT_DATA")
        values += np.arange(values.shape[1], dtype=np.float32)[None, :, None] ** 2
        ms.putcol("FLOAT_DATA", values)
        observed = ms.getcol("DATA")
        expected_data = observed.copy()
        expected_data[:, 1:-1] = (observed[:, :-2] + 2 * observed[:, 1:-1] + observed[:, 2:]) / 4
        input_lag = ms.getcol("LAG_DATA")
        calibrated = ms.getcol("WEIGHT")
        flags = ms.getcol("FLAG")
    rolled = flags.copy()
    rolled[:, 0] = rolled[:, -1] = True
    rolled[:, 1:-1] = flags[:, :-2] | flags[:, 1:-1] | flags[:, 2:]
    # CASA leaves noise arrays undefined in its float-only product. Check the
    # native extension of the calibrated convention analytically.
    spectrum = np.broadcast_to(calibrated[:, None, :], flags.shape).copy()
    spectrum[:, 1:-1] /= np.float32(0.375)
    used = ~rolled | rolled.all(axis=1, keepdims=True)
    weights = (spectrum * used).sum(axis=1) / used.sum(axis=1)
    sigmas = (used / np.sqrt(spectrum)).sum(axis=1) / used.sum(axis=1)

    def defined_in_native_only(name, values):
        def compare(row, actual, reference):
            assert not reference.iscelldefined(name, row), (name, row)
            np.testing.assert_allclose(actual, values[row], rtol=3e-6)
        return compare

    noise = {"WEIGHT": defined_in_native_only("WEIGHT", weights),
             "SIGMA": defined_in_native_only("SIGMA", sigmas)}
    paired(root, mixed, "float", datacolumn="float_data", deviations=noise)
    paired(root, mixed, "lag", datacolumn="lag_data",
           deviations={**noise, "LAG_DATA": defined_in_native_only("LAG_DATA", input_lag),
                       "FLAG": defined_in_native_only("FLAG", rolled)})

    def preserves_complex(row, actual, reference):
        assert (reference.getcell("DATA", row).imag == 0).all()
        np.testing.assert_array_equal(actual, expected_data[row])

    paired(root, mixed, "float-and-data", datacolumn="float_data,data",
           deviations={"DATA": preserves_complex})


def main():
    assert casatasks.version() == [6, 7, 6, 14], casatasks.version()
    with TemporaryDirectory(prefix="mstransform-hanning-") as scratch:
        root = Path(scratch)
        for channels in (1, 2, 3, 5):
            source = root / f"window-{channels}.ms"
            fixture(source, channels)
            paired(root, source, f"full-{channels}")
        source = root / "wide.ms"
        fixture(source, 9)
        for case, selector in (
            ("slice", "0:2~6"), ("stride", "0:1~6^2"), ("disjoint", "0:0~1;3~6"),
        ):
            paired(root, source, case, spw=selector, correlation="YY", field="FLUX")
        terminal_stride(root, source)
        paired(root, source, "input-flags-filter", keepflags=False)
        paired(root, source, "numpy-switch", hanning=np.bool_(True))
        for case, options in (
            ("channel-average", dict(chanaverage=True, chanbin=3)),
            ("time-average", dict(timeaverage=True, timebin="20s")),
            ("both-averages", dict(chanaverage=True, chanbin=2, timeaverage=True, timebin="20s")),
        ):
            paired(root, source, case, **options)
        paired(root, source, "forced-spectra", datacolumn="corrected", usewtspectrum=True)
        spectral = root / "spectral.ms"
        fixture(spectral, 9, spectra=True)
        for layout in ("data", "corrected", "model", "data,model,corrected", "all"):
            paired(root, spectral, f"spectral-{layout}", datacolumn=layout)
        paired(root, spectral, "weighted-both-averages", datacolumn="all",
               chanaverage=True, chanbin=2, timeaverage=True, timebin="20s")
        mixed = mixed_windows(root, source)
        pair = paired(root, mixed, "mixed-window-sizes")
        for product in pair:
            out = subprocess.check_output(
                ["taql", f"select NUM_CHAN from [{product}/SPECTRAL_WINDOW]"], text=True
            )
            assert out.splitlines()[-2:] == ["9", "2"], out
        for side, product in zip(("casa", "native"), pair):
            paired(root, product, f"reopen-{side}-product")
        sparse_failure(root, source)
        unused_sigma_spectrum(root, spectral)
        nonpositive_and_unknown_weights(root, spectral)
        invalid_switches(root, source)
        float_and_lag(root, source)


if __name__ == "__main__":
    main()
