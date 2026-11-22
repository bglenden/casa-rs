#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare native continuum subtraction with CASA 6.7.6.14's compiled fitter."""

import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import casatasks
from casatasks.private import task_mstransform as implementation
from casacore.tables import table
import numpy as np

from casars import tasks
from ms_combine_spws_oracle import make_pair
from ms_data_columns_oracle import add_float_and_lag
from ms_hanning_oracle import NOISE, VISIBILITY, compare_table
from ms_selection_tasks_oracle import MAIN, SUBTABLES
from ms_transform_callib_oracle import library, snapshot
from ms_transform_mms_oracle import member_paths


def reference(**arguments):
    constructor = implementation.mstransformer

    class FitSpecAdapter:
        def __init__(self, *args, **kwargs):
            self.tool = constructor(*args, **kwargs)

        def __getattr__(self, name):
            return getattr(self.tool, name)

        def config(self, options):
            if "uvcontsublib" in options:
                options["uvcontsublib"]["fitspec"] = options["uvcontsublib"].pop("fitspw")
            return self.tool.config(options)

    with patch.object(implementation, "mstransformer", FitSpecAdapter):
        return casatasks.mstransform(**arguments)


def compare_products(expected, actual, case, *, metadata=None, analytic=None):
    with table(str(expected), ack=False) as casa_ms, table(str(actual), ack=False) as native_ms:
        present = (VISIBILITY | NOISE) & set(casa_ms.colnames())
        assert present == (VISIBILITY | NOISE) & set(native_ms.colnames()), case
        for name in present & VISIBILITY:
            baseline, result = (ms.getcol(name) for ms in (casa_ms, native_ms))
            if analytic is not None:
                independent = analytic[name]
                assert np.linalg.norm(independent - baseline) > 0.001 * np.linalg.norm(independent)
                baseline = independent
            scale = max(np.linalg.norm(baseline.astype(np.complex128)), 1.0)
            rms = np.linalg.norm((result - baseline).astype(np.complex128)) / scale
            assert rms <= 0.001, (case, name, rms)
    paired = (set(MAIN) - VISIBILITY) | present
    # Check scientific error over each visibility column. Retain exact
    # independent readback of native cells and the ordinary scalar/noise checks.
    compare_table(metadata or expected, actual, paired, case,
                  {name: lambda *_: None for name in present & VISIBILITY})
    for name in ("SPECTRAL_WINDOW", "DATA_DESCRIPTION", "POLARIZATION", "FIELD", "FEED"):
        compare_table(expected / name, actual / name, SUBTABLES[name], f"{case} {name}")


def paired(root, source, case, **options):
    expected, actual = (root / f"{case}-{side}.ms" for side in ("casa", "native"))
    common = dict(vis=str(source), datacolumn="data", douvcontsub=True,
                  fitspw="0:1~4,1:1~4", usewtspectrum=True) | options
    before = snapshot(source)
    assert reference(outputvis=str(expected), **common) is None
    assert tasks.mstransform(outputvis=str(actual), **common) is None
    assert snapshot(source) == before, case
    metadata = None
    analytic = None
    if case in ("time-average", "robust", "float"):
        public = root / f"{case}-unchanged.ms"
        assert casatasks.mstransform(outputvis=str(public), **common) is None
        if case == "time-average":
            # The compiled fitter aliases the averaging iterator's FLAG cube.
            # The unmodified wrapper independently records flags and noise
            # from the same chain without changing line channels into flags.
            metadata = public
        else:
            analytic = intended_fit(public, common["fitorder"], common.get("niter", 1), case)
    compare_products(expected, actual, case, metadata=metadata, analytic=analytic)
    with table(str(actual / "HISTORY"), ack=False) as history:
        lines = {"".join(line.split()) for line in history.getcol("MESSAGE")}
        assert "douvcontsub=True" in lines
        assert f'fitspw="{common["fitspw"]}"' in lines
    print(f"passed {case}", flush=True)
    return expected, actual


def intended_fit(path, order, iterations, case):
    """Independent NumPy weighted polynomial over the line-free channels 1–4."""
    name = "FLOAT_DATA" if case == "float" else "DATA"
    with table(str(path), ack=False) as ms:
        data, flags = (ms.getcol(column) for column in (name, "FLAG"))
        weights = (ms.getcol("WEIGHT_SPECTRUM") if case == "float"
                   else 1 / ms.getcol("SIGMA_SPECTRUM") ** 2)
    matrix = np.polynomial.polynomial.polyvander(np.linspace(1, -1, data.shape[1]), order)
    result = data.copy()
    for row in range(data.shape[0]):
        for corr in range(data.shape[2]):
            valid = ~flags[row, :, corr]
            valid[[0, 5, 6]] = False
            terms = min(order + 1, np.count_nonzero(valid))
            if terms == 0:
                continue
            basis = matrix[:, :terms]
            spectral_weight = weights[row, valid, corr].astype(np.float64)

            def fitted(sample):
                weight = spectral_weight.copy()
                selected = sample[valid].astype(np.float64)
                for _ in range(iterations):
                    root = np.sqrt(weight)
                    coefficient = np.linalg.lstsq(basis[valid] * root[:, None],
                                                 selected * root, rcond=None)[0]
                    residual = np.where(weight > 0, np.abs(selected - basis[valid] @ coefficient), 0)
                    largest = residual.max()
                    if largest <= abs(selected).max() * 64 * np.finfo(np.float64).eps:
                        break
                    weight = np.where((residual > 0) & (residual < largest),
                                      (largest - residual) / largest, 0)
                return basis @ coefficient

            spectrum = data[row, :, corr]
            model = fitted(spectrum.real)
            if np.iscomplexobj(data):
                model = model + 1j * fitted(spectrum.imag)
            result[row, :, corr] -= model.astype(data.dtype)
    return {name: result}


def wrapper_bug(root, source):
    baseline, ignored = (root / f"public-{case}.ms" for case in ("disabled", "enabled"))
    for active, path in ((False, baseline), (True, ignored)):
        assert casatasks.mstransform(vis=str(source), outputvis=str(path), datacolumn="data",
                                     douvcontsub=active, fitspw="0:1~4,1:1~4") is None
    with table(str(baseline), ack=False) as disabled, table(str(ignored), ack=False) as enabled:
        np.testing.assert_array_equal(disabled.getcol("DATA"), enabled.getcol("DATA"))
    print("passed reproduction of public wrapper no-op", flush=True)


def invalid_and_retry(root, source):
    args = dict(vis=str(source), datacolumn="data", douvcontsub=True)
    for name, engine in (("casa", reference), ("native", tasks.mstransform)):
        output = root / f"invalid-{name}.ms"
        try:
            engine(outputvis=str(output), fitspw="99", **args)
        except RuntimeError:
            if name == "native":
                assert not output.exists()
                assert engine(outputvis=str(output), fitspw="0:1~4", **args) is None
        else:
            raise AssertionError(f"{name} accepted a missing fit window")
    print("passed invalid fit window and same-path native retry", flush=True)


def selected_channels(root, source):
    selected = root / "selected.ms"
    expected = root / "selected-reference.ms"
    actual = root / "selected-native.ms"
    # CASA's combined selection/continuum path can attempt to reshape an
    # already allocated spectrum. Select first with CASA and fit on the
    # equivalent local channel indices in a separate CASA transform.
    assert casatasks.mstransform(vis=str(source), outputvis=str(selected), datacolumn="data",
                                 spw="0:1~5", usewtspectrum=True) is None
    common = dict(datacolumn="data", douvcontsub=True, fitorder=1, usewtspectrum=True)
    assert reference(vis=str(selected), outputvis=str(expected), fitspw="0:1~3", **common) is None
    assert tasks.mstransform(vis=str(source), outputvis=str(actual), spw="0:1~5",
                             fitspw="0:2~4", **common) is None
    compare_products(expected, actual, "selected-channels")
    print("passed selected-channels", flush=True)


def multi_ms(root, source):
    partitioned = root / "partitioned.ms"
    actual = root / "continuum.mms"
    partition = dict(createmms=True, separationaxis="scan", numsubms=2, disableparallel=True)
    common = dict(datacolumn="data", usewtspectrum=True)
    fit = dict(douvcontsub=True, fitspw="0:1~4,1:1~4", fitorder=1)
    assert casatasks.mstransform(vis=str(source), outputvis=str(partitioned), **partition, **common) is None
    assert tasks.mstransform(vis=str(source), outputvis=str(actual), **partition, **common, **fit) is None
    expected_members, actual_members = member_paths(partitioned), member_paths(actual)
    assert len(expected_members) == len(actual_members) == 2
    for index, (member, result) in enumerate(zip(expected_members, actual_members)):
        expected = root / f"continuum-member-{index}.ms"
        assert reference(vis=str(member), outputvis=str(expected), **common, **fit) is None
        compare_products(expected, result, f"continuum-member-{index}")
    print("passed multi-ms", flush=True)


def main(cases):
    assert casatasks.version() == [6, 7, 6, 14]
    selected = lambda name: not cases or name in cases
    with TemporaryDirectory(prefix="mstransform-continuum-") as temporary:
        root = Path(temporary)
        source = make_pair(root, "input", 7, counts=(7, 7), spectra=True)
        for case, options in (
            ("constant", {}),
            ("linear", dict(fitorder=1)),
            ("quadratic", dict(fitorder=2)),
            ("robust", dict(fitorder=1, niter=3)),
            ("no-iterations", dict(fitorder=1, niter=0)),
            ("legacy", dict(fitorder=1, denoising_lib=False, niter=5)),
            ("parallel", dict(fitorder=1, nthreads=2)),
            ("continuum-only", dict(fitorder=1, want_cont=True)),
            ("corrected", dict(fitorder=1, datacolumn="corrected")),
            ("all", dict(fitorder=1, datacolumn="all")),
            ("none", dict(fitorder=1, fitspw="NONE")),
            ("full-window", dict(fitorder=1, fitspw="")),
            ("one-window", dict(fitorder=1, fitspw="1:1~4")),
            ("reduce-order", dict(fitorder=3, fitspw="0:2,1:2")),
            ("hanning", dict(fitorder=1, hanning=True)),
            ("channel-average", dict(fitorder=1, chanaverage=True, chanbin=2)),
            ("time-average", dict(fitorder=1, timeaverage=True, timebin="24s")),
            ("combine", dict(fitorder=1, combinespws=True)),
            ("regrid", dict(fitorder=1, regridms=True, start=1, nchan=4)),
        ):
            if selected(case):
                expected, actual = paired(root, source, case, **options)
                if case == "linear":
                    with table(str(expected), ack=False) as fitted:
                        assert abs(fitted.getcell("DATA", 0)[0, 0]) > 1
                    assert subprocess.check_output(
                        ["taql", f"select DATA_DESC_ID from [{actual}] limit 1"], text=True,
                    ).splitlines()[-1] == "0"
        if selected("joint"):
            gain = library(root, source, "amplitude")
            paired(root, source, "joint", fitorder=1, datacolumn="corrected",
                   docallib=True, callib=str(gain))
        if selected("scalar"):
            scalar = make_pair(root, "scalar-input", 7, counts=(7, 7), spectra=False)
            for column in ("data", "corrected"):
                paired(root, scalar, f"scalar-{column}", fitorder=1,
                       datacolumn=column, usewtspectrum=False)
        if selected("float"):
            floats = make_pair(root, "float-input", 7, counts=(7, 7), spectra=True)
            add_float_and_lag(floats)
            paired(root, floats, "float", fitorder=1, datacolumn="float_data")
        if selected("selection"):
            selected_channels(root, source)
        if selected("multi-ms"):
            multi_ms(root, source)
        if selected("wrapper"):
            wrapper_bug(root, source)
        if selected("retry"):
            invalid_and_retry(root, source)


if __name__ == "__main__":
    main(set(sys.argv[1:]))
