#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Paired public mstransform spectral regridding against CASA 6.7.6.14."""

from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory

import casatasks
from casatools import table as casa_table_tool
from casacore.tables import makescacoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from ms_combine_spws_oracle import make_pair
from ms_data_columns_oracle import add_float_and_lag
from ms_hanning_oracle import NOISE, compare_table, fixture
from ms_selection_tasks_oracle import MAIN, SUBTABLES


def paired(root, source, case, *, datacolumn="data", usewtspectrum=True, deviations=None,
           casa_options=None, **options):
    reference, native = (root / f"{case}-{side}.ms" for side in ("casa", "native"))
    for engine, path, parameters in (
        (casatasks, reference, options | (casa_options or {})), (tasks, native, options)
    ):
        assert engine.mstransform(vis=str(source), outputvis=str(path), datacolumn=datacolumn,
                                 regridms=True, usewtspectrum=usewtspectrum, **parameters) is None
    with table(str(reference), ack=False) as expected, table(str(native), ack=False) as actual:
        extras = NOISE | {"CORRECTED_DATA", "MODEL_DATA", "FLOAT_DATA"}
        present = extras & set(expected.colnames())
        assert present == extras & set(actual.colnames()), case
        compare = (set(MAIN) - {"DATA"} if datacolumn == "float_data" else set(MAIN)) | present
        for name in NOISE & compare:
            if not expected.iscelldefined(name, 0):
                assert all(not expected.iscelldefined(name, row) for row in range(expected.nrows())), (case, name)
                if not deviations or name not in deviations:
                    assert all(not actual.iscelldefined(name, row) for row in range(actual.nrows())), (case, name)
                    compare.remove(name)
    # CASA and casacore builds use independently evaluated Measures tables and
    # arithmetic. Compare subchannel offsets, not the absolute sky frequency.
    def grid_tolerance(column):
        return lambda row, got, ref: np.testing.assert_allclose(
            got, ref.getcell(column, row), atol=0.3, rtol=0,
            err_msg=f"{case} {column} row {row}",
        )

    compare_table(reference, native, compare, case, deviations)
    for name in ("SPECTRAL_WINDOW", "DATA_DESCRIPTION", "POLARIZATION", "FIELD", "FEED", "SOURCE"):
        columns = SUBTABLES[name]
        tolerances = None
        if name == "SPECTRAL_WINDOW":
            columns = (*columns, "MEAS_FREQ_REF")
            tolerances = {column: grid_tolerance(column) for column in
                          ("CHAN_FREQ", "CHAN_WIDTH", "REF_FREQUENCY", "TOTAL_BANDWIDTH",
                           "EFFECTIVE_BW", "RESOLUTION")}
        compare_table(reference / name, native / name, columns, f"{case} {name}", tolerances)
    print(f"passed {case}", flush=True)
    return reference, native


def scientific_tolerance(name):
    def compare(row, got, ref):
        expected = ref.getcell(name, row)
        error = np.linalg.norm(got - expected)
        assert error <= 0.001 * np.linalg.norm(expected) + 1e-7, (name, row, error)
    return compare

def unpadded_split(root, source):
    def spectrum(name, pad):
        def compare(row, got, ref):
            expected = ref.getcell(name, row)
            count = got.shape[0]
            np.testing.assert_allclose(got, expected[:count], atol=5e-7, rtol=3e-6)
            if row % 2:
                assert expected.shape[0] == count + 1
                np.testing.assert_array_equal(expected[count:], pad)
        return compare

    def scalar(name):
        def compare(row, got, ref):
            flags = ref.getcell("FLAG", row)
            data = ref.getcell(f"{name}_SPECTRUM", row)[:len(flags)]
            # CASA's padded values enter its scalar reducer when the
            # entire lane is flagged. Check native statistics on real samples.
            keep = flags == np.all(flags, axis=0)
            expected = np.sum(np.where(keep, data, 0), axis=0) / np.sum(keep, axis=0)
            np.testing.assert_allclose(got, expected, atol=5e-7, rtol=3e-6)
        return compare

    return paired(root, source, "split-odd", nspw=2, deviations={
        "WEIGHT_SPECTRUM": spectrum("WEIGHT_SPECTRUM", 0),
        "SIGMA_SPECTRUM": spectrum("SIGMA_SPECTRUM", -1),
        "WEIGHT": scalar("WEIGHT"), "SIGMA": scalar("SIGMA"),
    })


def partial_hanning_bin(root, source):
    # CASA's averaging kernel uses chanbin=2 on the final one-sample bin
    # before smoothing. Compute the result independently using only real
    # samples and inverse-variance weights.
    with table(str(source), ack=False) as ms:
        flags = ms.getcol("FLAG")
        data = ms.getcol("DATA")
        weights = 1 / ms.getcol("SIGMA_SPECTRUM") ** 2
    select = flags.copy()
    for begin in range(0, 9, 2):
        lanes = slice(begin, begin + 2)
        select[:, lanes] = flags[:, lanes] == np.all(flags[:, lanes], axis=1, keepdims=True)
    contribution = np.where(select, weights, 0)
    weight_bins = np.stack([contribution[:, i:i+2].sum(axis=1) for i in range(0, 9, 2)], axis=1)
    observed = contribution * data
    bins = np.stack([observed[:, i:i+2].sum(axis=1) for i in range(0, 9, 2)], axis=1) / weight_bins
    smooth_data = bins.copy()
    smooth_data[:, 1:-1] = 0.25 * (bins[:, :-2] + 2 * bins[:, 1:-1] + bins[:, 2:])
    smooth_weight = weight_bins.copy()
    smooth_weight[:, 1:-1] = 1 / ((1 / weight_bins[:, :-2] + 1 / weight_bins[:, 2:]) / 16
                                 + 1 / weight_bins[:, 1:-1] / 4)
    expected_data = 0.75 * smooth_data[:, :3] + 0.25 * smooth_data[:, 1:4]
    expected_weight = 0.75 * smooth_weight[:, :3] + 0.25 * smooth_weight[:, 1:4]
    checks = {
        "DATA": expected_data,
        "WEIGHT_SPECTRUM": expected_weight,
        "SIGMA_SPECTRUM": 1 / np.sqrt(expected_weight),
    }

    def independently(name):
        def compare(row, got, ref):
            expected = checks[name][row]
            np.testing.assert_allclose(got, expected, atol=2e-6, rtol=3e-6)
        return compare

    # Scalar reductions use the output flags, which are compared exactly.
    for name in ("WEIGHT", "SIGMA"):
        spectrum = checks[f"{name}_SPECTRUM"]
        # Output centres interpolate adjacent smoothed channels. Each
        # smoothing endpoint is flagged, and an internal flag ORs 3 bins.
        bin_flags = np.stack([flags[:, i:i+2].all(axis=1) for i in range(0, 9, 2)], axis=1)
        smoothed_flags = np.ones_like(bin_flags)
        smoothed_flags[:, 1:-1] = bin_flags[:, :-2] | bin_flags[:, 1:-1] | bin_flags[:, 2:]
        final = smoothed_flags[:, :3] | smoothed_flags[:, 1:4]
        keep = final == np.all(final, axis=1, keepdims=True)
        checks[name] = np.sum(np.where(keep, spectrum, 0), axis=1) / np.sum(keep, axis=1)
    paired(root, source, "hanning-average-odd", mode="frequency", start="100.001GHz",
           width="2MHz", nchan=3, hanning=True, chanaverage=True, chanbin=2,
           deviations={name: independently(name) for name in checks})


def terminal_channel_bounds(root, source):
    # Ask CASA for the two real channels. Requesting a third can read past
    # the input array, so that invocation is unsuitable as a test oracle.
    paired(root, source, "terminal-channel-bounds", mode="channel_b", start=7,
           width=1, nchan=3, casa_options={"nchan": 2})


def float_visibilities(root, source):
    with table(str(source), ack=False) as ms:
        selectors = np.s_[:, 2:7, :]
        weight = ms.getcol("WEIGHT_SPECTRUM")[selectors]
        flags = ms.getcol("FLAG")[selectors]
    keep = flags == np.all(flags, axis=1, keepdims=True)
    sigma = 1 / np.sqrt(weight)
    checks = {
        "WEIGHT_SPECTRUM": weight,
        "SIGMA_SPECTRUM": sigma,
        "WEIGHT": np.sum(np.where(keep, weight, 0), axis=1) / np.sum(keep, axis=1),
        "SIGMA": np.sum(np.where(keep, sigma, 0), axis=1) / np.sum(keep, axis=1),
    }

    def verify(name):
        def compare(row, got, ref):
            assert not ref.iscelldefined(name, row)
            np.testing.assert_allclose(got, checks[name][row], rtol=3e-6, atol=5e-7)
        return compare

    paired(root, source, "float", datacolumn="float_data", start=2, nchan=5,
           deviations={name: verify(name) for name in checks})

def attached_ephemeris(source, radial_velocity=None):
    with table(str(source), ack=False) as ms:
        start = ms.getcell("TIME", 0)
    field = source / "FIELD"
    with table(str(field), ack=False, readonly=False) as fields:
        fields.addcols(maketabdesc(makescacoldesc("EPHEMERIS_ID", -1)))
        fields.putcol("EPHEMERIS_ID", [-1] * fields.nrows())
        fields.putcell("EPHEMERIS_ID", 0, 0)
        fields.putcell("TIME", 0, start)
        for name in ["PHASE_DIR", "REFERENCE_DIR", "DELAY_DIR"]:
            fields.putcell(name, 0, np.zeros_like(fields.getcell(name, 0)))
    names = ("MJD", "RA", "DEC", "Rho", "RadVel")
    columns = maketabdesc([makescacoldesc(name, 0.) for name in names])
    ephemeris = casa_table_tool()
    try:
        assert ephemeris.create(str(field / "EPHEM0_TEST.tab"), tabledesc=columns, nrow=5)
        epoch = np.floor(start / 86400.)
        ephemeris.putinfo({"type": "IERS", "subType": "ephemeris"})
        for name, unit, values in zip(names, ("d", "deg", "deg", "AU", "AU/d"), [
            np.arange(epoch - 2, epoch + 3), 15 + np.arange(5) * 0.1,
            -30 + np.arange(5) * 0.1, np.full(5, 2.),
            np.linspace(0.001, 0.002, 5) if radial_velocity is None else np.full(5, radial_velocity),
        ]):
            ephemeris.putcolkeyword(name, "UNIT", unit)
            ephemeris.putcol(name, values)
        for key, value in {
            "VS_CREATE": "2026/11/22/00:00", "VS_DATE": "2026/11/22/00:00",
            "VS_VERSION": "0001.0001", "VS_TYPE": "Table of comet/planetary positions",
            "NAME": "TEST", "posrefsys": "ICRF/ICRS", "obsloc": "GEOCENTRIC", "GeoDist": 0.,
            "GeoLong": 0., "GeoLat": 0., "dMJD": 1., "MJD0": epoch - 3,
        }.items():
            ephemeris.putkeyword(key, value)
    finally:
        ephemeris.done()


def negligible_source_velocity(root):
    grids = []
    for label, velocity in (("zero", 0.), ("tiny", 4.6e-13)):
        moving = root / f"moving-{label}.ms"
        fixture(moving, 9, spectra=True)
        attached_ephemeris(moving, radial_velocity=velocity)
        products = paired(root, moving, f"source-{label}-velocity", outframe="SOURCE", field="0")
        for engine, product in enumerate(products):
            with table(str(product / "SPECTRAL_WINDOW"), ack=False) as spw:
                axis = [spw.getcol(name) for name in ("CHAN_FREQ", "CHAN_WIDTH")]
            if label == "zero":
                grids.append(axis)
            else:
                for actual, expected in zip(axis, grids[engine]):
                    np.testing.assert_array_equal(actual, expected)


def refusal(root, source, case, options, *, error_type=RuntimeError, compare_casa=True):
    for engine in ((casatasks, tasks) if compare_casa else (tasks,)):
        output = root / f"{case}-{engine.__name__}.ms"
        try:
            engine.mstransform(vis=str(source), outputvis=str(output), datacolumn="data",
                              regridms=True, **options)
        except error_type:
            if engine is tasks:
                assert not output.exists()
        else:
            raise AssertionError(f"{case}: {engine.__name__} accepted {options}")
    print(f"passed {case}", flush=True)

def main(cases):
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="mstransform-regrid-") as scratch:
        root = Path(scratch)
        source = root / "spectral.ms"
        fixture(source, 9, spectra=True)
        inputs = [
            ("default", {}),
            ("channel-crop", dict(start=2, nchan=4)),
            ("channel-width", dict(start=1, width=2, nchan=3)),
            ("channel-negative", dict(start=7, width=-2, nchan=3)),
            ("channel-bounds", dict(mode="channel_b", start=1, width=2, nchan=3)),
            ("selection-step", dict(spw="0:0~6^2", start=1, nchan=2, datacolumn="corrected")),
            ("frequency", dict(mode="frequency", start="100.0005GHz", width="1MHz", nchan=5)),
            ("wide-frequency", dict(mode="frequency", start="100.0005GHz", width="3MHz", nchan=3)),
            ("preaverage", dict(mode="frequency", start="100.0005GHz", width="3MHz", nchan=3,
                                preaverage=True)),
            ("reverse-frequency", dict(mode="frequency", start="100.007GHz", width="-1MHz", nchan=5)),
            ("radio", dict(mode="velocity", start="-21km/s", width="3km/s", nchan=5,
                           restfreq="100GHz", veltype="radio")),
            ("optical", dict(mode="velocity", start="-21km/s", width="3km/s", nchan=5,
                             restfreq="100GHz", veltype="optical")),
            ("nearest", dict(interpolation="nearest")),
            ("cubic", dict(interpolation="cubic")),
            ("spline", dict(interpolation="spline")),
            ("fftshift", dict(interpolation="fftshift")),
            ("lsrk", dict(outframe="LSRK")),
            ("field", dict(outframe="LSRK", phasecenter=1)),
            ("direction", dict(outframe="LSRK", phasecenter="J2000 1h0m0s -30d0m0s")),
            ("barycentric", dict(outframe="BARY")),
            ("cross-frame-wide", dict(outframe="BARY", mode="frequency", start="100.010GHz",
                                      width="1.5MHz", nchan=4)),
            ("frame-channel-average", dict(outframe="LSRK", chanaverage=True, chanbin=3, start=1, nchan=2)),
            ("frame-preaverage", dict(outframe="LSRK", preaverage=True, mode="frequency",
                                      start="100.012GHz", width="3MHz", nchan=2)),
            ("gapped-channel-bounds", dict(spw="0:0~1;7~8", mode="channel_b", start=0, width=1)),
            ("geo", dict(outframe="GEO")),
            ("source-direction", dict(outframe="SOURCE", phasecenter="J2000 1h0m0s -30d0m0s")),
            ("fft-frame", dict(outframe="LSRK", interpolation="fftshift")),
            ("time-average", dict(timeaverage=True, timebin="20s", start=2, nchan=5)),
            ("data-model-corrected", dict(datacolumn="data,model,corrected", start=2, nchan=5)),
            ("corrected", dict(datacolumn="corrected", start=2, nchan=5)),
            ("model", dict(datacolumn="model", start=2, nchan=5)),
            ("split-even", dict(nspw=2, nchan=8)),
        ]
        for case, options in inputs:
            if cases and case not in cases:
                continue
            if case in {"nearest", "cubic", "spline", "fftshift"}:
                options.update(mode="frequency", start="100.0005GHz", width="1MHz", nchan=5)
            paired(root, source, case, **options)
        if not cases or "hanning-average" in cases:
            even = root / "even.ms"
            fixture(even, 10, spectra=True)
            paired(root, even, "hanning-average", mode="frequency", start="100.001GHz",
                   width="2MHz", nchan=3, hanning=True, chanaverage=True, chanbin=2)
        if not cases or "time-frame" in cases:
            # Stay off exact channel centres so the documented input-epoch
            # difference does not move a sample across a flag boundary.
            paired(root, source, "time-frame", timeaverage=True, timebin="20s", outframe="LSRK",
                   field="0", mode="frequency", start="100.012GHz", width="1.01MHz", nchan=4,
                   deviations={name: scientific_tolerance(name) for name in NOISE | {"DATA"}})
        if not cases or "hanning-average-odd" in cases:
            partial_hanning_bin(root, source)
        if not cases or "terminal-channel-bounds" in cases:
            terminal_channel_bounds(root, source)
        if not cases or "split-odd" in cases:
            unpadded_split(root, source)
        if not cases or "float" in cases:
            floating = root / "float.ms"
            fixture(floating, 9, spectra=True)
            add_float_and_lag(floating)
            float_visibilities(root, floating)
        if not cases or cases & {"source-ephemeris", "source-fft"}:
            moving = root / "moving.ms"
            fixture(moving, 9, spectra=True)
            attached_ephemeris(moving)
            if not cases or "source-ephemeris" in cases:
                paired(root, moving, "source-ephemeris", outframe="SOURCE", field="0")
            if not cases or "source-fft" in cases:
                paired(root, moving, "source-fft", outframe="SOURCE", field="0", interpolation="fftshift",
                       deviations={name: scientific_tolerance(name) for name in NOISE | {"DATA"}})
        if not cases or "source-velocity-cutoff" in cases:
            negligible_source_velocity(root)
        if not cases or "failures" in cases:
            refusal(root, source, "bad-quantity", dict(mode="frequency", width="3km/s"), error_type=TypeError)
            refusal(root, source, "no-rest-frequency", dict(mode="velocity", width="3km/s"), error_type=ValueError)
            refusal(root, source, "bad-mode", dict(mode="INVALID"), error_type=AssertionError)
            refusal(root, source, "bad-frame", dict(outframe="INVALID"), error_type=AssertionError)
            refusal(root, source, "missing-source-ephemeris", dict(outframe="SOURCE"))
            refusal(root, source, "empty-split", dict(nspw=4), compare_casa=False)
        if not cases or cases & {"independent", "combine-regrid", "combine-split", "combine-hanning"}:
            pair = make_pair(root, "paired-windows", 6, spectra=True)
            for case, options in [
                ("independent", dict(start=1, nchan=4)),
                ("combine-regrid", dict(start=1, nchan=6, combinespws=True)),
                ("combine-hanning", dict(start=1, nchan=8, combinespws=True, hanning=True)),
                ("combine-split", dict(start=1, nchan=8, nspw=2)),
            ]:
                if not cases or case in cases:
                    paired(root, pair, case, **options)
        if not cases or cases & {"mixed-size", "descending-cubic", "descending-spline"}:
            mixed = make_pair(root, "mixed-input", 4, counts=(7, 9), descending=True, spectra=True)
            if not cases or "mixed-size" in cases:
                paired(root, mixed, "mixed-size", start=1, nchan=4)
            for method in ("cubic", "spline"):
                case = f"descending-{method}"
                if not cases or case in cases:
                    paired(root, mixed, case, spw="1", interpolation=method, start=1, nchan=5)
        if not cases or "reopen" in cases:
            for product in unpadded_split(root, source) if cases else (
                root / f"split-odd-{side}.ms" for side in ("casa", "native")
            ):
                result = subprocess.check_output(
                    ["taql", f"select NUM_CHAN from [{product}/SPECTRAL_WINDOW]"], text=True
                )
                assert result.splitlines()[-2:] == ["5", "4"], result
            print("passed standalone-casacore-reopen", flush=True)


if __name__ == "__main__":
    main(set(sys.argv[1:]))
