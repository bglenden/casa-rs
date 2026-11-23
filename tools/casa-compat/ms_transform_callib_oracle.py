#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public mstransform callibrary correction against CASA 6.7.6.14."""

from pathlib import Path
import hashlib
import subprocess
import sys
from tempfile import TemporaryDirectory

import casatasks
from casatools import table as casa_table
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from ms_combine_spws_oracle import make_pair
from ms_hanning_oracle import NOISE, VISIBILITY, compare_table
from ms_selection_tasks_oracle import MAIN, SUBTABLES
from ms_transform_mms_oracle import compare_members


def library(root, source, name, *, calwt=True, flagged=False):
    gain = root / f"{name}.cal"
    casatasks.gencal(vis=str(source), caltable=str(gain), caltype="amp", parameter=[2.0])
    tool = casa_table()
    try:
        tool.open(str(gain), nomodify=False)
        for name in ("FIELD_ID", "SPECTRAL_WINDOW_ID", "OBSERVATION_ID"):
            ids = tool.getcol(name)
            tool.putcol(name, np.zeros_like(ids))
        if flagged:
            visibility = tool.getcol("FLAG")
            visibility[..., 0] = True
            tool.putcol("FLAG", visibility)
    finally:
        tool.close()
    path = gain.with_suffix(".callib")
    path.write_text(
        f"caltable='{gain}' fldmap=[0,0] spwmap=[0,0] tinterp='nearest' "
        f"finterp='nearest' calwt={'T' if calwt else 'F'}\n"
    )
    return path


def snapshot(path):
    return {
        entry.relative_to(path): hashlib.sha256(entry.read_bytes()).digest()
        for entry in path.rglob("*") if entry.is_file() and entry.name != "table.lock"
    }


def paired(root, source, label, callib, *, datacolumn="corrected", usewtspectrum=True, **options):
    reference, native = (root / f"{label}-{side}.ms" for side in ("casa", "native"))
    before = snapshot(source)
    for engine, output in ((casatasks, reference), (tasks, native)):
        assert engine.mstransform(
            vis=str(source), outputvis=str(output), datacolumn=datacolumn, docallib=True,
            callib=str(callib), usewtspectrum=usewtspectrum, **options,
        ) is None, label
        assert snapshot(source) == before, (label, "the input changed")
    with table(str(reference), ack=False) as expected, table(str(native), ack=False) as actual:
        expected_arrays = (VISIBILITY | NOISE) & set(expected.colnames())
        assert expected_arrays == (VISIBILITY | NOISE) & set(actual.colnames()), label
    compare_table(reference, native, (set(MAIN) - {"DATA"}) | expected_arrays, label)
    for name in ("SPECTRAL_WINDOW", "DATA_DESCRIPTION", "POLARIZATION", "FIELD", "FEED", "SOURCE"):
        compare_table(reference / name, native / name, SUBTABLES[name], f"{label} {name}")
    with table(str(native / "HISTORY"), ack=False) as history:
        lines = {"".join(str(line).split()) for line in history.getcol("MESSAGE")}
        assert "docallib=True" in lines
        assert f'callib="{callib}"' in lines
    print(f"passed {label}", flush=True)
    return reference, native


def retries(root, source, valid):
    bad = root / "malformed.callib"
    bad.write_text("not a callibrary assignment\n")
    for engine in (casatasks, tasks):
        output = root / f"retry-{engine.__name__}.ms"
        args = dict(vis=str(source), outputvis=str(output), datacolumn="corrected", docallib=True)
        for missing in ("", str(root / "missing.callib")):
            try:
                engine.mstransform(**args, callib=missing)
            except FileNotFoundError as error:
                assert error.filename == missing
                assert not output.exists()
            else:
                raise AssertionError(f"missing callibrary succeeded in {engine.__name__}")
        try:
            engine.mstransform(**args, callib=str(bad))
        except Exception as error:
            assert "Problem parsing cal library line" in str(error), str(error)
            assert not output.exists()
        else:
            raise AssertionError(f"malformed callibrary succeeded in {engine.__name__}")
        assert engine.mstransform(**args, callib=str(valid)) is None
        assert output.is_dir()
    print("passed invalid callibrary and retry", flush=True)


def irregular_spectra(root, gain):
    undefined = make_pair(root, "undefined-source", 7, counts=(7, 7), spectra=False)
    with table(str(undefined), ack=False, readonly=False) as ms:
        if "WEIGHT_SPECTRUM" in ms.colnames():
            ms.removecols("WEIGHT_SPECTRUM")
        ms.addcols(maketabdesc(makearrcoldesc("WEIGHT_SPECTRUM", 0, ndim=2, valuetype="float")))
    paired(root, undefined, "undefined-weight-spectrum", gain, usewtspectrum=False)

    sparse = make_pair(root, "sparse-source", 7, counts=(7, 7), spectra=True)
    with table(str(sparse), ack=False, readonly=False) as ms:
        sigmas = ms.getcol("SIGMA_SPECTRUM")
        ms.removecols("SIGMA_SPECTRUM")
        ms.addcols(maketabdesc(makearrcoldesc("SIGMA_SPECTRUM", 0, ndim=2, valuetype="float")))
        for row in range(ms.nrows()):
            if row != 1:
                ms.putcell("SIGMA_SPECTRUM", row, sigmas[row])
    before = snapshot(sparse)
    for engine in (casatasks, tasks):
        output = root / f"sparse-{engine.__name__}.ms"
        try:
            engine.mstransform(
                vis=str(sparse), outputvis=str(output), datacolumn="corrected",
                docallib=True, callib=str(gain),
            )
        except RuntimeError as error:
            assert "SIGMA_SPECTRUM" in str(error) and "row 1" in str(error), str(error)
            if engine is tasks:
                assert not output.exists()
        else:
            raise AssertionError(f"undefined spectral noise succeeded in {engine.__name__}")
        assert snapshot(sparse) == before
    with table(str(sparse), ack=False, readonly=False) as ms:
        ms.putcell("SIGMA_SPECTRUM", 1, sigmas[1])
    assert tasks.mstransform(
        vis=str(sparse), outputvis=str(output), datacolumn="corrected",
        docallib=True, callib=str(gain),
    ) is None
    print("passed undefined sigma spectrum and retry", flush=True)


def diagonal_edge_case(root, source, label):
    interpolated = label == "interpolated-bandpass"
    bandpass = label != "unflagged-zero-gain"
    callib = library(root, source, label, calwt=not bandpass)
    cal = callib.with_suffix(".cal")
    if interpolated:
        callib.write_text(callib.read_text().replace("tinterp='nearest'", "tinterp='linear'"))
        with (
            table(str(source), ack=False) as ms,
            table(str(source / "SPECTRAL_WINDOW"), ack=False) as spw,
            table(str(cal / "SPECTRAL_WINDOW"), ack=False, readonly=False) as target,
        ):
            start = ms.getcol("TIME").min() - 10
            for column in ("NUM_CHAN", "CHAN_FREQ", "CHAN_WIDTH",
                           "EFFECTIVE_BW", "RESOLUTION", "TOTAL_BANDWIDTH"):
                target.putcell(column, 0, spw.getcell(column, 0))
            channels = int(spw.getcell("NUM_CHAN", 0))
    tool = casa_table()
    try:
        tool.open(str(cal), nomodify=False)
        if bandpass:
            tool.putinfo(tool.info() | {"subType": "B Jones"})
            tool.putkeyword("VisCal", "B Jones")
        if interpolated:
            rows = tool.nrows()
            tool.copyrows(str(cal))
            assert tool.nrows() == 2 * rows
            for row in range(2 * rows):
                lower = row < rows
                first_gain = 0.25j if lower else 0.25 + 2j
                gain = first_gain if tool.getcell("ANTENNA1", row) == 0 else 1
                tool.putcell("TIME", row, start if lower else start + 20)
                tool.putcell("CPARAM", row, np.full((2, channels), gain, np.complex64))
                tool.putcell("FLAG", row, np.zeros((2, channels), bool))
        else:
            antenna = tool.getcol("ANTENNA1")
            gain = 1 + 1j if bandpass else 0
            values = np.broadcast_to(np.where(antenna == 0, gain, 1),
                                     tool.getcol("CPARAM").shape)
            tool.putcol("CPARAM", values.astype(np.complex64))
    finally:
        tool.done()
    reference, native = paired(root, source, label, callib, spw="0")
    with table(str(reference), ack=False) as expected, table(str(native), ack=False) as actual:
        if bandpass:
            np.testing.assert_allclose(actual.getcol("DATA"), expected.getcol("DATA"),
                                       rtol=0, atol=1e-6)
        else:
            weight = expected.getcol("WEIGHT")
            assert np.any(weight == 0)
            np.testing.assert_array_equal(actual.getcol("WEIGHT"), weight)
    print(f"passed exact {label}", flush=True)


def main(cases):
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="mstransform-callib-") as scratch:
        root = Path(scratch)
        source = make_pair(root, "input", 7, counts=(7, 7), spectra=True)
        gain = library(root, source, "amplitude")
        no_weights = library(root, source, "no-weight-change", calwt=False)
        flagged = library(root, source, "flagged-gain", flagged=True)
        selected = lambda *names: not cases or any(name in cases for name in names)
        for case, callib, options in (
            ("corrected", gain, {}),
            ("corrected-calwt-false", no_weights, {}),
            ("data", gain, dict(datacolumn="data")),
            ("all", gain, dict(datacolumn="all")),
            ("selected-channels", gain, dict(spw="1:0~3", field="0")),
            ("qualified-correlation", gain, dict(correlation="0:XX")),
            ("array-inequality", gain, dict(array="<2")),
            ("hanning", gain, dict(hanning=True)),
            ("channel-average", gain, dict(chanaverage=True, chanbin=2)),
            ("time-average", gain, dict(timeaverage=True, timebin="24s")),
            ("combine", gain, dict(combinespws=True)),
            ("regrid", gain, dict(regridms=True, start=1, nchan=3)),
            ("flagged-solution", flagged, {}),
        ):
            if not selected(case):
                continue
            reference, native = paired(root, source, case, callib, **options)
            if case == "corrected":
                with table(str(source), ack=False) as given, table(str(reference), ack=False) as expected:
                    np.testing.assert_allclose(expected.getcell("DATA", 0), given.getcell("DATA", 0) / 4)
                assert subprocess.check_output(
                    ["taql", f"select DATA_DESC_ID from [{native}] limit 1"], text=True
                ).splitlines()[-1] == "0"
                print("passed standalone calibrated readback", flush=True)
        if selected("unmapped"):
            unmapped = root / "unmapped.callib"
            unmapped.write_text(gain.read_text().replace("spwmap=[0,0]", "spwmap=[]"))
            paired(root, source, "unmapped", unmapped)
            unmapped.write_text(unmapped.read_text() + no_weights.read_text())
            paired(root, source, "unmapped-chain", unmapped)
        if selected("irregular-spectra"):
            irregular_spectra(root, gain)
        diagonal = ("one-channel-bandpass", "interpolated-bandpass", "unflagged-zero-gain")
        if selected(*diagonal):
            edge = make_pair(root, "diagonal-edge", 7, counts=(7, 7), spectra=True)
            with table(str(edge), ack=False, readonly=False) as ms:
                ms.putcol("DATA", np.full_like(ms.getcol("DATA"), 32 + 1j))
            for case in diagonal:
                if selected(case):
                    diagonal_edge_case(root, edge, case)
        if selected("virtual"):
            virtual = make_pair(root, "virtual", 7, counts=(7, 7), spectra=False)
            tool = casa_table()
            try:
                tool.open(str(virtual), nomodify=False)
                tool.removecols(["CORRECTED_DATA"])
            finally:
                tool.close()
            paired(root, virtual, "virtual", gain)
        if selected("empty-library"):
            empty = root / "empty.callib"
            empty.write_text("# No calibration terms.\n")
            paired(root, source, "empty-library", empty, datacolumn="data")
        if selected("scalar-sigma"):
            scalar = make_pair(root, "scalar-sigma", 7, counts=(7, 7), spectra=True)
            tool = casa_table()
            try:
                tool.open(str(scalar), nomodify=False)
                tool.removecols(["SIGMA_SPECTRUM"])
            finally:
                tool.close()
            paired(root, scalar, "scalar-sigma", gain)
        if selected("mms", "retained"):
            reference, native = (root / f"mms-{side}.ms" for side in ("casa", "native"))
            for engine, output in ((casatasks, reference), (tasks, native)):
                assert engine.mstransform(
                    vis=str(source), outputvis=str(output), datacolumn="corrected", docallib=True,
                    callib=str(gain), usewtspectrum=True, createmms=True, separationaxis="scan",
                    numsubms=2, disableparallel=True,
                ) is None
            compare_members(reference, native, "mms")
            print("passed calibrated mms", flush=True)
            if selected("retained"):
                outputs = (root / "retained-casa.ms", root / "retained-native.ms")
                for engine, input_ms, output in zip((casatasks, tasks), (reference, native), outputs):
                    assert engine.mstransform(
                        vis=str(input_ms), outputvis=str(output), datacolumn="corrected",
                        docallib=True, callib=str(gain), disableparallel=True,
                    ) is True
                compare_members(*outputs, "retained")
                print("passed retained calibrated mms", flush=True)
        if selected("retry"):
            retries(root, source, gain)


if __name__ == "__main__":
    main(set(sys.argv[1:]))
