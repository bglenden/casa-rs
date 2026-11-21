#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check MeasurementSet RFlag parity against pinned CASA 6.7.6.14."""

import ast
from contextlib import chdir
from json import dumps
from pathlib import Path
from shutil import copytree
from signal import SIGABRT
import subprocess
import sys
from tempfile import TemporaryDirectory

import casatasks
from casatasks.private.flaghelper import Parser
from casacore.tables import makearrcoldesc, maketabdesc, table
import numpy as np

from casars import tasks
from flagdata_calibration_rfi_oracle import compare_report
from flagdata_preview_oracle import auxiliary_content
from flagdata_routine_controls_oracle import flags
from flagdata_tfcrop_fit_oracle import make_fixture
from flagdata_tfcrop_sources_oracle import check_science_state, put_samples, science_state
from imager_prediction_options_oracle import write_model
from record_casa_returns import make_cal_ms


def outcome(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        return (type(exc).__name__, str(exc))


def check_thresholds_and_files(libraries):
    empty, narrow, broad = make_fixture("thresh-source.ms")
    with table("thresh-source.ms", readonly=False, ack=False) as main:
        data = main.getcol("DATA")
        # Inject distinct time and spectral outliers in field 0 and field 1.
        data[narrow, 11, 0] += 25.0 + 10.0j
        data[broad, 6, 1] += 18.0 - 12.0j
        f1_rows = (main.getcol("FIELD_ID") == 1) & (main.getcol("ANTENNA1") == 0) & (main.getcol("ANTENNA2") == 1)
        data[f1_rows, 14, 0] += 30.0
        main.putcol("DATA", data)

    products = {}
    for name, task in libraries:
        case = Path(f"calc-{name}")
        case.mkdir()
        vis = case / "vis.ms"
        copytree("thresh-source.ms", vis)
        with chdir(case):
            original, aux = science_state("vis.ms"), auxiliary_content("vis.ms")
            default = task(vis="vis.ms", mode="rflag", action="calculate",
                           extendflags=False, flagbackup=False)
            assert default["report0"]["name"] == "Rflag"
            assert default["report0"]["type"] == "rflag"
            for axis in ("timedev", "freqdev"):
                path = Path(f"rflag_output_thresholds_{axis}0.txt")
                record = ast.literal_eval(path.read_text())
                assert record["name"] == "Rflag"
                np.testing.assert_allclose(record[axis], default["report0"][axis], rtol=1e-6, atol=1e-8)
            named = task(vis="vis.ms", mode="rflag", action="calculate",
                         timedev="tdev.txt", freqdev="fdev.txt",
                         extendflags=False, flagbackup=False)
            compare_report(default, named)
            actual, rows = flags("vis.ms")
            np.testing.assert_array_equal(actual, empty)
            assert not rows.any()
            check_science_state("vis.ms", original)
            assert auxiliary_content("vis.ms") == aux
            products[name] = (default, case / "tdev.txt", case / "fdev.txt")
    compare_report(products["casa"][0], products["native"][0])

    # Cross-engine threshold file and matrix replay in action='apply'.
    task_map = dict(libraries)
    expected_mask = None
    for writer, reader in (("casa", "native"), ("native", "casa")):
        for use_files in (True, False):
            vis = f"replay-{writer}-{reader}-{use_files}.ms"
            copytree("thresh-source.ms", vis)
            original = science_state(vis)
            if use_files:
                supplied = dict(timedev=str(products[writer][1].resolve()),
                                freqdev=str(products[writer][2].resolve()))
            else:
                supplied = dict(timedev=products[writer][0]["report0"]["timedev"],
                                freqdev=products[writer][0]["report0"]["freqdev"])
            assert task_map[reader](vis=vis, mode="rflag", extendflags=False,
                                    flagbackup=False, **supplied) == {}
            actual, rows = flags(vis)
            assert not rows.any()
            if expected_mask is None:
                expected_mask = actual
                assert expected_mask.any()
            else:
                np.testing.assert_array_equal(actual, expected_mask)
            check_science_state(vis, original)

    # Saved commands with calculate/apply inline threshold substitution into outfile and FLAG_CMD.
    save_cases = (
        ("calc-file", dict(action="calculate", timedev="s-time.txt", freqdev="s-freq.txt", outfile="saved.txt")),
        ("calc-table", dict(action="calculate", outfile="")),
        ("apply-matrix", dict(action="apply", timedev=[[0, 0, 0.5]], freqdev=[[0, 0, 1.2]], outfile="saved.txt")),
    )
    for label, opts in save_cases:
        saved_cmds, masks = {}, {}
        for name, task in libraries:
            case = Path(f"save-{label}-{name}")
            case.mkdir()
            vis = case / "vis.ms"
            copytree("thresh-source.ms", vis)
            with chdir(case):
                res = task(vis="vis.ms", mode="rflag", savepars=True, cmdreason="ms-rflag",
                           extendflags=False, flagbackup=False, **opts)
                if opts["outfile"]:
                    cmd_text = Path(opts["outfile"]).read_text().strip()
                else:
                    with table("vis.ms/FLAG_CMD", ack=False) as fcmd:
                        cmd_text = fcmd.getcol("COMMAND")[-1].strip()
                saved_cmds[name] = (res, Parser(" ", "=").parse2Dictionary(cmd_text))
                masks[name] = flags("vis.ms")[0]
        compare_report(saved_cmds["casa"][0], saved_cmds["native"][0])
        np.testing.assert_array_equal(masks["native"], masks["casa"])
        assert saved_cmds["casa"][1].keys() == saved_cmds["native"][1].keys()
        for key, want_val in saved_cmds["casa"][1].items():
            have_val = saved_cmds["native"][1][key]
            if key in ("timedev", "freqdev") and isinstance(want_val, list):
                np.testing.assert_allclose(have_val, want_val, rtol=1e-3, atol=1e-8)
            else:
                assert have_val == want_val, (label, key, have_val, want_val)

    # Matrix, scale, spectralmin/spectralmax, winsize, and extension boundary controls.
    param_cases = (
        ("scales", dict(timedev=0.5, freqdev=0.8, timedevscale=2.0, freqdevscale=1.5, extendflags=False)),
        ("neg-scale-disable", dict(timedevscale=-1.0, freqdevscale=-1.0, extendflags=False)),
        ("neg-thresh-disable", dict(timedev=-1.0, freqdev=-1.0, extendflags=False)),
        ("unmatched-matrix", dict(timedev=[[99, 0, 0.01]], freqdev=[[0, 0, 0.8], [0, 0, 0.4]], extendflags=False)),
        ("spectralmax", dict(timedev=100.0, freqdev=100.0, spectralmax=-0.1, extendflags=False)),
        ("spectralmin", dict(timedev=100.0, freqdev=100.0, spectralmin=0.05, extendflags=False)),
        ("winsize-1", dict(winsize=1, extendflags=False)),
        ("winsize-5", dict(winsize=5, extendflags=False)),
        ("winsize-wrap", dict(winsize=2**32 + 3, extendflags=False)),
        ("winsize-np", dict(winsize=np.int64(5), timedev=np.float32(0.5), extendflags=False)),
        ("winsize-3-timedev", dict(winsize=3, timedev=0.5, extendflags=False)),
        ("winsize-5-timedev", dict(winsize=5, timedev=0.5, extendflags=False)),
        ("winsize-bool", dict(winsize=True, extendflags=False)),
        ("winsize-npbool", dict(winsize=np.bool_(True), extendflags=False)),
        ("extend-full", dict(extendflags=True)),
        ("corr-xx-ext", dict(correlation="XX", extendflags=True)),
    )
    for label, opts in param_cases:
        results, masks = {}, {}
        for name, task in libraries:
            vis = f"param-{label}-{name}.ms"
            copytree("thresh-source.ms", vis)
            results[name] = outcome(task, vis=vis, mode="rflag", flagbackup=False, **opts)
            masks[name] = flags(vis)[0]
        compare_report(results["casa"], results["native"])
        np.testing.assert_array_equal(masks["native"], masks["casa"], err_msg=label)
        if label in ("neg-thresh-disable", "winsize-1", "winsize-bool", "winsize-npbool"):
            np.testing.assert_array_equal(masks["native"], empty, err_msg=label)
        elif label in (
            "scales",
            "unmatched-matrix",
            "neg-scale-disable",
            "spectralmax",
            "spectralmin",
            "winsize-5",
            "winsize-wrap",
            "winsize-np",
            "winsize-3-timedev",
            "winsize-5-timedev",
            "extend-full",
            "corr-xx-ext",
        ):
            assert masks["native"].any(), label
        if label in ("winsize-5", "winsize-np"):
            msgs = {}
            for name, _ in libraries:
                with table(f"param-{label}-{name}.ms/HISTORY", ack=False) as history:
                    msgs[name] = [
                        m for m in history.getcol("MESSAGE")
                        if m.split("=", 1)[0].strip() == "winsize"
                    ]
            assert len(msgs["native"]) == 1 and msgs["native"] == msgs["casa"], (label, msgs)
            assert msgs["native"][0].split("=", 1)[1].strip() == "5", (label, msgs)
    # NumPy integer winsize falls back to default winsize=3 while preserving winsize=5 in HISTORY.
    np_mask = flags("param-winsize-np-native.ms")[0]
    w3_mask = flags("param-winsize-3-timedev-native.ms")[0]
    w5_mask = flags("param-winsize-5-timedev-native.ms")[0]
    np.testing.assert_array_equal(np_mask, w3_mask)
    assert not np.array_equal(w3_mask, w5_mask)

    # Selected-channel extension boundary: native confines extended flags to selected channels.
    for name, task in libraries:
        vis = f"selected-ext-{name}.ms"
        copytree("thresh-source.ms", vis)
        assert task(vis=vis, mode="rflag", spw="0:8~15", timedev=100.0, freqdev=0.1,
                    extendflags=True, flagbackup=False) == {}
    casa_sel, _ = flags("selected-ext-casa.ms")
    native_sel, _ = flags("selected-ext-native.ms")
    assert native_sel[:, 8:16, :].any()
    np.testing.assert_array_equal(native_sel[:, 8:16, :], casa_sel[:, 8:16, :])
    assert not native_sel[:, :8, :].any()
    assert not native_sel[:, 16:, :].any()

    # Overflowing winsize (2**70) fails in parseAgentParameters after creating flagbackup,
    # while nonpositive winsize and positive threshold + nonpositive scale are refused safely.
    task_map = dict(libraries)
    for name, task in libraries:
        vis = f"overflow-win-{name}.ms"
        copytree("thresh-source.ms", vis)
        original = science_state(vis)
        res = outcome(task, vis=vis, mode="rflag", winsize=2**70, flagbackup=True)
        assert res == (
            "SystemError",
            "<built-in function agentflagger_parseagentparameters> returned a result with an exception set",
        ), (name, res)
        assert Path(f"{vis}.flagversions/flags.flagdata_1").exists()
        np.testing.assert_array_equal(flags(vis)[0], empty)
        check_science_state(vis, original)

    for index, (bad, expect_backup, param, reason) in enumerate((
        (dict(winsize=0), False, "winsize", "winsize must be positive"),
        (dict(winsize=-1), False, "winsize", "winsize must be positive"),
        (dict(timedev=1.0, timedevscale=0.0), False, "timedevscale",
         "timedevscale must be positive when its threshold is positive"),
        (dict(freqdev=1.0, freqdevscale=-1.0), False, "freqdevscale",
         "freqdevscale must be positive when its threshold is positive"),
        (dict(timedev=[[0, 0, 1.0]], timedevscale=0.0), True, "timedevscale",
         "timedevscale must be positive when its threshold is positive"),
        (dict(freqdev=[[0, 0, 1.0]], freqdevscale=-2.0), True, "freqdevscale",
         "freqdevscale must be positive when its threshold is positive"),
    )):
        vis = f"crash-refusal-{index}.ms"
        copytree("thresh-source.ms", vis)
        original = science_state(vis)
        res = outcome(task_map["native"], vis=vis, mode="rflag", flagbackup=True, **bad)
        assert res == (
            "TaskParameterError",
            f"flagdata refused its parameters (parameter '{param}'): {reason}",
        ), (bad, res)
        assert Path(f"{vis}.flagversions").exists() == expect_backup
        if expect_backup:
            np.testing.assert_array_equal(flags(f"{vis}.flagversions/flags.flagdata_1")[0], empty)
        fl, rfl = flags(vis)
        np.testing.assert_array_equal(fl, empty)
        assert not rfl.any()
        check_science_state(vis, original)
    return 4 + len(save_cases) + len(param_cases) + 2


def check_sources_and_models(libraries):
    empty, narrow, broad = make_fixture("rflag-complex.ms")
    with table("rflag-complex.ms", readonly=False, ack=False) as main:
        shape = main.getcol("DATA").shape
        times = main.getcol("TIME")
        ordinal = np.searchsorted(np.unique(times), times)
        chan_ramp = (np.arange(shape[1], dtype=np.float32) * 0.01)[None, :, None]
        time_ramp = (ordinal.astype(np.float32) * 0.01)[:, None]
        model = np.ones(shape, dtype=np.complex64) + chan_ramp
        model[narrow, 10, 1] = 35.0
        observed = model + 1.0 + chan_ramp
        observed[narrow, 16, 0] += 30.0
        corrected = model + (1.0 + 1.0j) + chan_ramp
        corrected[narrow, 12, 1] += 30.0j
        weight_spectrum = np.ones(shape, dtype=np.float32) + chan_ramp
        weight_spectrum[narrow, 18, 1] = 42.0
        for col, arr in (("DATA", observed), ("CORRECTED_DATA", corrected),
                         ("MODEL_DATA", model), ("WEIGHT_SPECTRUM", weight_spectrum)):
            put_samples(main, col, arr)
        weight = np.ones_like(main.getcol("WEIGHT")) + time_ramp
        weight[broad, 0] = 30.0
        main.putcol("WEIGHT", weight)
    for variant in ("scalar", "float-only"):
        path = f"rflag-{variant}.ms"
        copytree("rflag-complex.ms", path)
        with table(path, readonly=False, ack=False) as main:
            if variant == "scalar":
                main.removecols("WEIGHT_SPECTRUM")
            else:
                real = np.ones(shape, dtype=np.float32) + chan_ramp
                real[narrow, 14, 1] = 39.0
                put_samples(main, "FLOAT_DATA", real)
                main.removecols("DATA")

    source_cases = (
        ("rflag-complex", "data", ((16, 0), (10, 1))),
        ("rflag-complex", "corrected", ((10, 1), (12, 1))),
        ("rflag-complex", "model", ((10, 1),)),
        ("rflag-complex", "residual", ((12, 1),)),
        ("rflag-complex", "residual_data", ((16, 0),)),
        ("rflag-complex", "weight", ((18, 1),)),
        ("rflag-complex", "weight_spectrum", ((18, 1),)),
        ("rflag-scalar", "weight", None),
        ("rflag-float-only", "float_data", ((14, 1),)),
        ("rflag-float-only", "residual_data", ()),
    )
    for index, (variant, column, coords) in enumerate(source_cases):
        calcs, masks = {}, {}
        for name, task in libraries:
            path = f"rsrc-{index}-{name}.ms"
            copytree(f"{variant}.ms", path)
            original = science_state(path)
            opts = dict(vis=path, mode="rflag", datacolumn=column, antenna="0&1",
                        extendflags=False, flagbackup=False)
            calcs[name] = task(action="calculate", **opts)
            assert task(**opts) == {}
            masks[name], rows = flags(path)
            assert not rows.any()
            check_science_state(path, original)
        compare_report(calcs["casa"], calcs["native"])
        np.testing.assert_array_equal(masks["native"], masks["casa"], err_msg=f"{variant} {column}")
        expected = empty.copy()
        if coords is None:
            # winsize=3 flags the three 3-step windows containing the broad time outlier.
            broad_idx = int(np.flatnonzero(broad)[0])
            narrow_rows = np.flatnonzero(narrow)
            pos = int(np.searchsorted(narrow_rows, broad_idx))
            expected[narrow_rows[pos - 2:pos + 3], :, 0] = True
        else:
            for ch, pl in coords:
                expected[narrow, ch, pl] = True
        np.testing.assert_array_equal(masks["native"], expected, err_msg=f"{variant} {column}")

    refusals = (("rflag-scalar", "weight_spectrum"), ("rflag-float-only", "data"),
                ("rflag-complex", "corrected_data\0ignored"), ("rflag-complex", "fparam"))
    for index, (variant, column) in enumerate(refusals):
        for name, task in libraries:
            path = f"rmiss-{index}-{name}.ms"
            copytree(f"{variant}.ms", path)
            original, auxiliary = science_state(path), auxiliary_content(path)
            before_flags, before_rows = flags(path)
            for action in ("calculate", "apply"):
                res = outcome(task, vis=path, mode="rflag", datacolumn=column,
                              action=action, flagbackup=False)
                assert res == ("ValueError", "Failed to parse parameters for mode rflag"), (name, column, action, res)
                after_flags, after_rows = flags(path)
                np.testing.assert_array_equal(after_flags, before_flags)
                np.testing.assert_array_equal(after_rows, before_rows)
                check_science_state(path, original)
                assert auxiliary_content(path) == auxiliary

    # Virtual Fourier model parity across MODEL, RESIDUAL_DATA, and RESIDUAL.
    with table("rflag-complex.ms/FIELD", ack=False) as fields:
        direction = fields.getcell("PHASE_DIR", 0).ravel()
    write_model(Path("rflag-point.im"), direction, 100e9, 2, cell=5)
    for tag, use_scratch in (("virtual", False), ("scratch", True)):
        path = f"rmodel-{tag}.ms"
        copytree("rflag-complex.ms", path)
        casatasks.ft(vis=path, field="0", model="rflag-point.im", usescratch=use_scratch)
    with table("rmodel-scratch.ms", ack=False) as materialized:
        ft_model = materialized.getcol("MODEL_DATA")

    model_cases = (
        ("model", 10, 1, 0.0),
        ("residual_data", 16, 0, 25.0),
        ("residual", 12, 1, 25.0),
        ("residual_data", 16, 0, 0.0005),
        ("residual", 12, 1, 0.0005),
    )
    for index, (column, ch, pl, amp) in enumerate(model_cases):
        observed = ft_model + 1.0
        corrected = ft_model + 1.0
        if column == "residual_data":
            observed[narrow, ch, pl] += amp
        elif column == "residual":
            corrected[narrow, ch, pl] += amp
        reference_mask = None
        reference_calc = None
        for tag in ("scratch", "virtual"):
            for name, task in libraries:
                path = f"rmod-{index}-{tag}-{name}.ms"
                copytree(f"rmodel-{tag}.ms", path)
                with table(path, readonly=False, ack=False) as main:
                    put_samples(main, "DATA", observed)
                    put_samples(main, "CORRECTED_DATA", corrected)
                opts = dict(vis=path, mode="rflag", field="0", antenna="0&1",
                            datacolumn=column, extendflags=False, flagbackup=False)
                calc = task(action="calculate", **opts)
                assert task(**opts) == {}
                actual, rows = flags(path)
                assert not rows.any()
                if reference_mask is None:
                    reference_mask = actual
                    reference_calc = calc
                else:
                    compare_report(reference_calc, calc)
                    np.testing.assert_array_equal(actual, reference_mask, err_msg=f"{column} {tag} {name}")

    # Missing CORRECTED_DATA with datacolumn='residual' disables the RFlag agent without opening virtual model.
    for name, task in libraries:
        path = f"rdisabled-{name}.ms"
        copytree("rmodel-virtual.ms", path)
        with table(f"{path}/SOURCE", readonly=False, ack=False) as src:
            rec = src.getcell("SOURCE_MODEL", 0)
            del rec["ft_0"]
            src.putcell("SOURCE_MODEL", 0, rec)
        with table(path, readonly=False, ack=False) as main:
            main.removecols("CORRECTED_DATA")
        aux = auxiliary_content(path)
        opts = dict(vis=path, mode="rflag", field="0", antenna="0&1",
                    datacolumn="residual", extendflags=False, flagbackup=False)
        assert task(action="calculate", **opts) == {}
        assert auxiliary_content(path) == aux
        assert task(**opts) == {}
        assert not flags(path)[0].any()
    return len(source_cases) + len(refusals) + len(model_cases) + 1


def check_correlations(libraries):
    empty, narrow, _ = make_fixture("rcorr-source.ms")
    with table("rcorr-source.ms", readonly=False, ack=False) as main:
        shape = main.getcol("DATA").shape
        chan_ramp = (np.arange(shape[1], dtype=np.float32) * 0.01)[None, :, None]
        data = np.ones(shape, dtype=np.complex64) * (1.0 + 1.0j) + chan_ramp * (1.0 + 1.0j)
        # Spike XX real/imag at channel 8 and YY imag at channel 20.
        data[narrow, 8, 0] += 30.0 + 35.0j
        data[narrow, 20, 1] += 40.0j
        main.putcol("DATA", data)
    copytree("rcorr-source.ms", "rcorr-float.ms")
    with table("rcorr-float.ms", readonly=False, ack=False) as main:
        real = np.ones(shape, dtype=np.float32) + chan_ramp
        real[narrow, 14, 1] += 45.0
        put_samples(main, "FLOAT_DATA", real)
        main.removecols("DATA")

    corr_cases = (
        ("default", "data", ""),
        ("xx", "data", "XX"),
        ("yy", "data", "YY"),
        ("xxyy", "data", "XX,YY"),
        ("abs-all", "data", "ABS_ALL"),
        ("real-all", "data", "REAL_ALL"),
        ("imag-all", "data", "IMAG_ALL"),
        ("arg-all", "data", "ARG_ALL"),
        ("norm-all", "data", "NORM_ALL"),
        ("real-xx", "data", "REAL_XX"),
        ("imag-xx", "data", "IMAG_XX"),
        ("imag-yy", "data", "IMAG_YY"),
        ("imag-zz", "data", "IMAG_ZZ"),
        ("abs-i", "data", "ABS_I"),
        ("abs-q", "data", "ABS_Q"),
        ("abs-u", "data", "ABS_U"),
        ("abs-sol1", "data", "ABS_SOL1"),
        ("abs-wvr", "data", "ABS_WVR"),
        ("bare-all-skip", "data", "ALL"),
        ("bare-i-skip", "data", "I"),
        ("bare-wvr-skip", "data", "WVR"),
        ("bare-bogus-skip", "data", "BOGUS"),
        ("float-imag-fallback", "float_data", "IMAG_YY"),
    )
    for label, column, corr in corr_cases:
        calcs, masks = {}, {}
        for name, task in libraries:
            path = f"rcorr-{label}-{name}.ms"
            src = "rcorr-float.ms" if column == "float_data" else "rcorr-source.ms"
            copytree(src, path)
            opts = dict(vis=path, mode="rflag", datacolumn=column, correlation=corr,
                        antenna="0&1", extendflags=False, flagbackup=False)
            calcs[name] = task(action="calculate", **opts)
            assert task(**opts) == {}
            masks[name] = flags(path)[0]
        compare_report(calcs["casa"], calcs["native"])
        np.testing.assert_array_equal(masks["native"], masks["casa"], err_msg=f"{label} ({corr})")
        if label == "imag-xx":
            # IMAG_XX evaluates XX then I; at timestep 0 (before the time window starts)
            # XX flags chan 8 before I checks modified flags, while from timestep >= delta
            # onward the sliding time pass runs on I and flags both XX and YY.
            narrow_rows = np.flatnonzero(narrow)
            assert masks["native"][narrow, 8, 0].all()
            assert not masks["native"][narrow_rows[0], 8, 1] and masks["native"][narrow_rows[1:], 8, 1].all()
            assert masks["native"][narrow, 20, 0].all() and masks["native"][narrow, 20, 1].all()
        elif label == "imag-yy":
            # IMAG_YY evaluates YY then I; YY has no spike at chan 8, so I sees the XX spike and flags both!
            assert masks["native"][narrow, 8, 0].all() and masks["native"][narrow, 8, 1].all()
            assert masks["native"][narrow, 20, 1].all() and not masks["native"][narrow, 20, 0].any()

    for index, bad_corr in enumerate(("ABS_ZZ", "REAL_NONE", "ABS_", "ABS")):
        for name, task in libraries:
            path = f"rcorr-err-{index}-{name}.ms"
            copytree("rcorr-source.ms", path)
            for action in ("calculate", "apply"):
                res = outcome(task, vis=path, mode="rflag", correlation=bad_corr,
                              action=action, flagbackup=False)
                assert res[0] == "RuntimeError" and "Unknown polarization requested" in res[1], (name, bad_corr, res)

    # Edge cases across mixed DDIDs, flagged DDID/SPW rows, circular bare ALL, unused I, stored I, and 4-pol U/V.
    def seed_noisy(path, *, stokes="XX YY", freq="100GHz", imag_outlier=False):
        make_cal_ms(path, channels=32, scan_seconds=210, stokes=stokes, frequency=freq)
        rng = np.random.default_rng(7)
        with table(path, readonly=False, ack=False) as main:
            data = main.getcol("DATA")
            noise = rng.normal(0.0, 0.25, size=data.shape) + 1j * rng.normal(0.0, 0.25, size=data.shape)
            data = (np.ones_like(data) + noise).astype(np.complex64)
            r01 = np.flatnonzero((main.getcol("FIELD_ID") == 0) & (main.getcol("ANTENNA1") == 0)
                                 & (main.getcol("ANTENNA2") == 1))
            data[r01[4:7], 5, 0] += 40j if imag_outlier else 40.0
            data[r01[12:15], 10, -1] += 45j if imag_outlier else 45.0
            if data.shape[2] == 4:
                data[:, :, 2] = 0.0 + 0.0j
                data[r01[8:11], 9, 1] += 35.0
            main.putcol("DATA", data)
            main.putcol("FLAG", np.zeros(data.shape, dtype=bool))
            main.putcol("FLAG_ROW", np.zeros(main.nrows(), dtype=bool))

    for name, stokes, freq, imag_out in (
        ("rc_lin", "XX YY", "100GHz", False),
        ("rc_cir", "RR LL", "100GHz", False),
        ("rc_cir_spw1", "RR LL", "101GHz", False),
        ("rc_cir_im", "RR LL", "100GHz", True),
        ("rc_si", "I", "100GHz", False),
        ("rc_four", "XX XY YX YY", "100GHz", False),
    ):
        seed_noisy(f"{name}.ms", stokes=stokes, freq=freq, imag_outlier=imag_out)
    casatasks.concat(vis=["rc_lin.ms", "rc_cir.ms"], concatvis="rc_mixed1.ms", freqtol="1Hz")
    casatasks.concat(vis=["rc_lin.ms", "rc_cir_spw1.ms"], concatvis="rc_mixed2.ms", freqtol="1Hz")
    casatasks.concat(vis=["rc_cir.ms", "rc_lin.ms"], concatvis="rc_cir_first.ms", freqtol="1Hz")

    copytree("rc_mixed1.ms", "rc_flagddid0.ms")
    with table("rc_flagddid0.ms/DATA_DESCRIPTION", readonly=False, ack=False) as dd:
        fr = dd.getcol("FLAG_ROW")
        fr[0] = True
        dd.putcol("FLAG_ROW", fr)
    copytree("rc_mixed1.ms", "rc_flagddid1.ms")
    with table("rc_flagddid1.ms/DATA_DESCRIPTION", readonly=False, ack=False) as dd:
        fr = dd.getcol("FLAG_ROW")
        fr[1] = True
        dd.putcol("FLAG_ROW", fr)
    copytree("rc_lin.ms", "rc_flagspw.ms")
    with table("rc_flagspw.ms/SPECTRAL_WINDOW", readonly=False, ack=False) as spw_tbl:
        spw_tbl.putcol("FLAG_ROW", np.ones(spw_tbl.nrows(), dtype=bool))
    copytree("rc_lin.ms", "rc_unused_i.ms")
    with table("rc_si.ms/POLARIZATION", ack=False) as extra:
        with table("rc_unused_i.ms/POLARIZATION", readonly=False, ack=False) as pol_tbl:
            r = pol_tbl.nrows()
            pol_tbl.addrows(1)
            extra.copyrows(pol_tbl, startrowin=0, startrowout=r, nrow=1)

    edge_cases = []
    for ds in ("rc_mixed1", "rc_mixed2", "rc_cir_first"):
        for expr, expect_any in (
            ("", True), ("ALL", False), ("ABS_ALL", True), ("IMAG_ALL", True),
            ("ABS_XX,RR", True), ("ABS_RR,XX", True), ("ABS_YY,LL", True),
            ("ABS_XX,YY", True), ("ABS_RR,LL", True), ("XX,RR", False),
            ("RR,XX", False), ("XX,YY", True), ("RR,LL", True), ("ABS_I", True),
        ):
            edge_cases.append((ds, expr, expect_any))
    for ds in ("rc_cir", "rc_cir_im"):
        edge_cases.extend([(ds, "ALL", True), (ds, "ABS_ALL", True)])
    edge_cases.extend([
        ("rc_flagddid0", "XX", False), ("rc_flagddid0", "RR", True),
        ("rc_flagddid1", "RR", False), ("rc_flagddid0", "ABS_ALL", True),
        ("rc_flagspw", "XX", False), ("rc_flagspw", "ABS_XX", True),
        ("rc_unused_i", "", True), ("rc_unused_i", "ABS_ALL", True),
        ("rc_unused_i", "ABS_ALL,WVR", True), ("rc_unused_i", "WVR", False),
        ("rc_unused_i", "ALL", False),
        ("rc_si", "ABS_WVR", True), ("rc_si", "WVR_I", True),
        ("rc_si", "IMAG_WVR", True), ("rc_si", "WVR", False),
        ("rc_si", "ABS_I", False), ("rc_si", "ALL", False),
        ("rc_four", "ABS_U", True), ("rc_four", "ABS_V", True),
        ("rc_four", "IMAG_U", True), ("rc_four", "IMAG_V", True),
        ("rc_four", "ALL", False),
    ])
    for idx, (ds, corr, expect_any) in enumerate(edge_cases):
        calcs, masks = {}, {}
        safe = f"{ds}-{idx}".replace(",", "_")
        for name, task in libraries:
            path = f"rcorr-edge-{safe}-{name}.ms"
            copytree(f"{ds}.ms", path)
            opts = dict(vis=path, mode="rflag", datacolumn="data", correlation=corr,
                        antenna="0&1", extendflags=False, flagbackup=False)
            calcs[name] = task(action="calculate", **opts)
            assert task(**opts) == {}
            masks[name] = flags(path)[0]
        compare_report(calcs["casa"], calcs["native"])
        if ds in ("rc_mixed1", "rc_cir_first") and corr == "ALL":
            assert calcs["native"]["nreport"] == 0
        elif ds in ("rc_mixed1", "rc_cir_first") and corr in ("ABS_ALL", "ABS_RR,XX"):
            assert calcs["native"]["nreport"] == 1
        np.testing.assert_array_equal(masks["native"], masks["casa"], err_msg=f"{ds} {corr}")
        assert bool(masks["native"].any()) == expect_any, (ds, corr)

    # Analytic linear Stokes U control (XY == YX): CASA's (XY - YX)/2 cancels to 0, while native's
    # physical (XY + YX)/2 flags the common spike on both contributing cross-hand planes.
    copytree("rc_four.ms", "rc_four_u_sign.ms")
    with table("rc_four_u_sign.ms", readonly=False, ack=False) as main:
        d = main.getcol("DATA")
        d[:, :, 2] = d[:, :, 1]
        expected_u = np.zeros(d.shape, dtype=bool)
        r01 = np.flatnonzero((main.getcol("FIELD_ID") == 0) & (main.getcol("ANTENNA1") == 0)
                             & (main.getcol("ANTENNA2") == 1))
        # winsize=3 windows centered at timesteps 7..=11 overlap the spiked rows 8..10.
        expected_u[r01[6:13], 9, 1:3] = True
        main.putcol("DATA", d)
    for name, task in libraries:
        u_path = f"rcorr-u-sign-{name}.ms"
        copytree("rc_four_u_sign.ms", u_path)
        assert task(vis=u_path, mode="rflag", datacolumn="data", correlation="ABS_U",
                    antenna="0&1", extendflags=False, flagbackup=False) == {}
        fl, _ = flags(u_path)
        if name == "casa":
            assert not fl.any()
        else:
            np.testing.assert_array_equal(fl, expected_u)
    return len(corr_cases) + 4 + len(edge_cases) + 1


def check_averaging_and_chunks(libraries):
    empty, narrow, broad = make_fixture("ravg-source.ms")
    with table("ravg-source.ms", readonly=False, ack=False) as main:
        data = main.getcol("DATA")
        # Channel-pair spike at channels 12 & 13 and time-pair spike on narrow baseline.
        data[narrow, 12:14, 0] += 18.0
        narrow_rows = np.flatnonzero(narrow)
        data[narrow_rows[4:6], 5, 1] += 22.0
        main.putcol("DATA", data)
        scans = main.getcol("SCAN_NUMBER")
        times = main.getcol("TIME")
        unique_times = np.unique(times)
        scans[times > unique_times[5]] = 2
        main.putcol("SCAN_NUMBER", scans)

    avg_cases = (
        ("chanavg-2", dict(channelavg=True, chanbin=2, extendflags=False)),
        ("chanavg-vec", dict(channelavg=True, chanbin=[2], extendflags=False)),
        ("chanavg-spw", dict(channelavg=True, chanbin=2, spw="0:4~23", extendflags=False)),
        ("timeavg-20s", dict(timeavg=True, timebin="20s", extendflags=False)),
        ("timeavg-comb", dict(timeavg=True, timebin="20s", combinescans=True, ntime="200s", extendflags=False)),
        ("both-avg", dict(timeavg=True, timebin="20s", channelavg=True, chanbin=2, extendflags=False)),
        ("scans-separate", dict(combinescans=False, winsize=5, extendflags=False)),
        ("scans-combined", dict(combinescans=True, ntime="200s", winsize=5, extendflags=False)),
        ("surviving-ext-corr", dict(correlation="BOGUS", timeavg=True, timebin="20s", channelavg=True, chanbin=2, extendflags=True)),
        ("surviving-ext-timebin", dict(timeavg=True, timebin="bad", channelavg=True, chanbin=2, extendflags=True)),
    )
    for label, opts in avg_cases:
        calcs, masks = {}, {}
        for name, task in libraries:
            path = f"ravg-{label}-{name}.ms"
            copytree("ravg-source.ms", path)
            run_opts = dict(vis=path, mode="rflag", antenna="0&1", flagbackup=False, **opts)
            if "timeavg" not in opts or label == "timeavg-20s":
                calcs[name] = task(action="calculate", **run_opts)
            assert task(**run_opts) == {}
            masks[name] = flags(path)[0]
        if calcs:
            compare_report(calcs["casa"], calcs["native"])
        np.testing.assert_array_equal(masks["native"], masks["casa"], err_msg=label)

    # chanbin=0 + channelavg=True + spw slice + extendflags=True (with and without timeavg),
    # and multi-baseline antenna="" + ntime splitting scans across time chunks.
    make_cal_ms("ravg-cb0.ms", channels=32, scan_seconds=210, stokes="XX YY")
    with table("ravg-cb0.ms", readonly=False, ack=False) as main:
        d = np.ones_like(main.getcol("DATA"))
        r01 = np.flatnonzero((main.getcol("FIELD_ID") == 0) & (main.getcol("ANTENNA1") == 0)
                             & (main.getcol("ANTENNA2") == 1))
        d[r01[5], 8:15, 0] = 40.0
        d[r01[:11], 10, 0] = 35.0
        main.putcol("DATA", d)
        main.putcol("FLAG", np.zeros(d.shape, dtype=bool))
        main.putcol("FLAG_ROW", np.zeros(main.nrows(), dtype=bool))
    for label, opts in (
        ("cb0-no-tavg", dict(antenna="0&1", spw="0:8~15", channelavg=True, chanbin=0, extendflags=True)),
        ("cb0-tavg", dict(antenna="0&1", spw="0:8~15", channelavg=True, chanbin=0,
                          timeavg=True, timebin="30s", extendflags=True)),
    ):
        masks = {}
        for name, task in libraries:
            path = f"ravg-{label}-{name}.ms"
            copytree("ravg-cb0.ms", path)
            assert task(vis=path, mode="rflag", flagbackup=False, **opts) == {}
            masks[name] = flags(path)[0]
        assert masks["native"].any(), label
        np.testing.assert_array_equal(masks["native"], masks["casa"], err_msg=label)

    make_cal_ms("ravg-mb.ms", channels=16, scan_seconds=210, stokes="XX YY")
    rng = np.random.default_rng(12345)
    with table("ravg-mb.ms", readonly=False, ack=False) as main:
        d = main.getcol("DATA")
        noise = rng.normal(0.0, 0.25, size=d.shape) + 1j * rng.normal(0.0, 0.25, size=d.shape)
        d = (np.ones_like(d) + noise).astype(np.complex64)
        f0 = main.getcol("FIELD_ID") == 0
        a1, a2 = main.getcol("ANTENNA1"), main.getcol("ANTENNA2")
        r01 = np.flatnonzero(f0 & (a1 == 0) & (a2 == 1))
        r23 = np.flatnonzero(f0 & (a1 == 2) & (a2 == 3))
        d[r01[4:7], 5, 0] += 40.0
        d[r01[12:15], 10, 1] += 45.0
        d[r23[3:6], 8, 0] += 50.0
        d[r23[14:17], 13, 1] += 55.0
        main.putcol("DATA", d)
        main.putcol("FLAG", np.zeros(d.shape, dtype=bool))
        main.putcol("FLAG_ROW", np.zeros(main.nrows(), dtype=bool))
    for label, opts in (
        ("mb-ntime90", dict(antenna="", ntime="90s", extendflags=False)),
        ("mb-ntime100f", dict(antenna="", ntime=100.0, combinescans=True, extendflags=True)),
    ):
        calcs, masks = {}, {}
        for name, task in libraries:
            path = f"ravg-{label}-{name}.ms"
            copytree("ravg-mb.ms", path)
            run_opts = dict(vis=path, mode="rflag", flagbackup=False, **opts)
            calcs[name] = task(action="calculate", **run_opts)
            assert task(**run_opts) == {}
            masks[name] = flags(path)[0]
        compare_report(calcs["casa"], calcs["native"])
        assert masks["native"].any(), label
        np.testing.assert_array_equal(masks["native"], masks["casa"], err_msg=label)

    # Strided SPW with channelavg=True: CASA aborts with SIGABRT (ArraySlicerError);
    # native matches CASA's explicit channel-list equivalent.
    copytree("ravg-source.ms", "ravg-stride-crash.ms")
    stride_abort = """
import json
import resource
import sys
from casatasks import flagdata

resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
flagdata(**json.loads(sys.argv[1]))
"""
    stride_opts = dict(vis="ravg-stride-crash.ms", mode="rflag", antenna="0&1",
                       spw="0:0~30^2", channelavg=True, chanbin=2,
                       extendflags=False, flagbackup=False)
    aborted = subprocess.run(
        [sys.executable, "-c", stride_abort, dumps(stride_opts)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert aborted.returncode == -SIGABRT and "ArraySlicerError" in aborted.stderr, aborted.stderr

    copytree("ravg-source.ms", "ravg-stride-native.ms")
    copytree("ravg-source.ms", "ravg-stride-casa-ref.ms")
    task_map = dict(libraries)
    explicit_spw = "0:" + ";".join(str(ch) for ch in range(0, 32, 2))
    assert task_map["casa"](vis="ravg-stride-casa-ref.ms", mode="rflag", antenna="0&1",
                            spw=explicit_spw, channelavg=True, chanbin=2,
                            extendflags=False, flagbackup=False) == {}
    assert task_map["native"](vis="ravg-stride-native.ms", mode="rflag", antenna="0&1",
                              spw="0:0~30^2", channelavg=True, chanbin=2,
                              extendflags=False, flagbackup=False) == {}
    casa_stride_ref, _ = flags("ravg-stride-casa-ref.ms")
    native_stride, _ = flags("ravg-stride-native.ms")
    assert native_stride[:, ::2, :].any()
    np.testing.assert_array_equal(native_stride, casa_stride_ref)

    # CASA FlagDataHandler::setTimeAverageIter substitutes zeros for residual_data,
    # float_data, weight, and weight_spectrum when timeavg=True; native averages the
    # physical values accurately and preserves backup/FLAG_ROW invariants.
    z_empty, z_narrow, _ = make_fixture("ravg-zsub.ms", broadband=False)
    with table("ravg-zsub.ms", readonly=False, ack=False) as main:
        shape = main.getcol("DATA").shape
        times = main.getcol("TIME")
        integration = np.searchsorted(np.unique(times), times)
        chan_ramp = (np.arange(shape[1], dtype=np.float32) * 0.01)[None, :, None]
        model = np.ones(shape, dtype=np.complex64) + chan_ramp
        observed = model + 1.0 + chan_ramp
        observed[z_narrow, 16, 0] += 30.0
        put_samples(main, "MODEL_DATA", model)
        put_samples(main, "DATA", observed)
        ws = np.ones(shape, dtype=np.float32) + chan_ramp
        baseline = (main.getcol("ANTENNA1") == 0) & (main.getcol("ANTENNA2") == 1)
        trailing = baseline & np.isin(integration, (20, 41))
        ws[trailing, :, :] *= 2.0
        ws[z_narrow, 18, 1] *= 20.0
        put_samples(main, "WEIGHT_SPECTRUM", ws)
        w = np.ones_like(main.getcol("WEIGHT"))
        w[trailing, :] = 2.0
        w[z_narrow & np.isin(integration, (6, 7)), 0] = 30.0
        main.putcol("WEIGHT", w)
    copytree("ravg-zsub.ms", "ravg-zsub-float.ms")
    with table("ravg-zsub-float.ms", readonly=False, ack=False) as main:
        real = np.ones(shape, dtype=np.float32) + chan_ramp
        real[z_narrow, 14, 1] += 39.0
        put_samples(main, "FLOAT_DATA", real)
        main.removecols("DATA")

    z_narrow_rows = np.flatnonzero(z_narrow)
    z_cases = (
        ("residual_data", {}, (z_narrow, 16, 0)),
        ("float_data", {}, (z_narrow, 14, 1)),
        ("weight_spectrum", {}, (z_narrow, 18, 1)),
        ("weight", dict(timedev=0.5, freqdev=100.0, timedevscale=1.0, freqdevscale=1.0),
         (z_narrow_rows[2:12], slice(None), 0)),
    )
    for col, extra, coords in z_cases:
        expected_native = z_empty.copy()
        expected_native[coords] = True
        for name, task in libraries:
            path = f"ravg-zsub-{col}-{name}.ms"
            copytree("ravg-zsub-float.ms" if col == "float_data" else "ravg-zsub.ms", path)
            if col == "weight":
                with table(path, readonly=False, ack=False) as main:
                    main.removecols("WEIGHT_SPECTRUM")
            assert task(vis=path, mode="rflag", datacolumn=col, antenna="0&1",
                        timeavg=True, timebin="20s", extendflags=False, flagbackup=False,
                        **extra) == {}
            actual, actual_rows = flags(path)
            assert not actual_rows.any()
            np.testing.assert_array_equal(actual, expected_native if name == "native" else z_empty,
                                          err_msg=f"timeavg zero-sub {col} ({name})")

    # Verify FLAG_ROW update when time-averaged RFlag flags all channels/correlations of a bin,
    # backup restoration via flagmanager, and standalone casacore taql readback.
    copytree("ravg-source.ms", "ravg-flagrow.ms")
    with table("ravg-flagrow.ms", readonly=False, ack=False) as main:
        d = main.getcol("DATA")
        d[narrow_rows[:2], :, :] += 80.0
        main.putcol("DATA", d)
    assert task_map["native"](vis="ravg-flagrow.ms", mode="rflag", antenna="0&1",
                              timeavg=True, timebin="20s", timedev=0.5, freqdev=100.0,
                              extendflags=True, flagbackup=True) == {}
    actual_flags, actual_rows = flags("ravg-flagrow.ms")
    assert actual_rows[narrow_rows[:2]].all()
    taql_out = subprocess.check_output(
        ["taql", "select gsum(ntrue(FLAG)), gsum(iif(FLAG_ROW, 1, 0)) from [ravg-flagrow.ms]"],
        text=True,
    )
    assert f"{int(actual_flags.sum())}" in taql_out
    tasks.flagmanager(vis="ravg-flagrow.ms", mode="restore", versionname="flagdata_1")
    restored_flags, restored_rows = flags("ravg-flagrow.ms")
    np.testing.assert_array_equal(restored_flags, empty)
    assert not restored_rows.any()
    return len(avg_cases) + len(z_cases) + 6


def main():
    assert casatasks.version_string() == "6.7.6.14"
    libraries = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory() as directory, chdir(directory):
        c1 = check_thresholds_and_files(libraries)
        c2 = check_sources_and_models(libraries)
        c3 = check_correlations(libraries)
        c4 = check_averaging_and_chunks(libraries)
    print(f"Verified {c1} threshold/file cases, {c2} source/Fourier-model cases, "
          f"{c3} correlation cases, and {c4} averaging/chunk/interop cases.")


if __name__ == "__main__":
    main()
