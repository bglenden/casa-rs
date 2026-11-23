#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check MeasurementSet flagdata(mode='list') parity against pinned CASA 6.7.6.14."""

from ast import literal_eval
from contextlib import chdir
from datetime import datetime, timedelta
from json import loads
import os
from pathlib import Path
from shutil import copytree, rmtree
import subprocess
import sys
from tempfile import TemporaryDirectory
from time import time

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_calibration_rfi_oracle import compare_report
from flagdata_history_oracle import MJD_UNIX_EPOCH_S, compare_new_rows
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_routine_controls_oracle import flags
from flagdata_summary_oracle import snapshot
from flagdata_tfcrop_fit_oracle import make_fixture
from imager_prediction_options_oracle import write_model
from ms_selection_tasks_oracle import casa_table
from record_casa_returns import make_cal_ms


def outcome(fn, *args, **kwargs):
    try:
        return ("OK", fn(*args, **kwargs))
    except Exception as exc:
        return ("ERR", type(exc).__name__, str(exc))


def make_list_fixture(path, *, with_corrected=True):
    make_fixture(path, integrations=10, broadband=False)
    with table(path, ack=False, readonly=False) as main:
        if not with_corrected:
            for col in ("CORRECTED_DATA", "MODEL_DATA"):
                if col in main.colnames():
                    main.removecols(col)
            return
        data = main.getcol("DATA")
        b01 = (main.getcol("ANTENNA1") == 0) & (main.getcol("ANTENNA2") == 1)
        f0 = main.getcol("FIELD_ID") == 0
        f1 = main.getcol("FIELD_ID") == 1
        # Channel spikes for TFCrop/RFlag and clip contrast across fields/DDIDs.
        data[b01, 12:16, 1] = 28.0
        data[b01 & f1, 4, 0] = 24.0
        main.putcol("DATA", data)
        if "CORRECTED_DATA" in main.colnames():
            main.putcol("CORRECTED_DATA", data * 1.0)


def make_fourier_fixture(path):
    make_list_fixture(path)
    with table(path, ack=False, readonly=False) as main:
        if "MODEL_DATA" in main.colnames():
            main.removecols("MODEL_DATA")
    with table(f"{path}/FIELD", ack=False) as fields:
        direction = fields.getcell("PHASE_DIR", 0).ravel()
    model_im = Path("list-point.im")
    if model_im.exists():
        rmtree(model_im)
    write_model(model_im, direction, 100e9, 2, cell=5)
    casatasks.ft(vis=path, field="0", model=str(model_im), usescratch=False)
    with table(path, ack=False, readonly=False) as main:
        data = main.getcol("DATA")
        # Inject an outlier channel in DATA so RESIDUAL_DATA exceeds 0.2.
        data[(main.getcol("ANTENNA1") == 0) & (main.getcol("ANTENNA2") == 1), 6:8, 0] += 5.0
        main.putcol("DATA", data)


def check_inputs_and_selectors(libraries):
    make_list_fixture("inputs.ms")
    with table("inputs.ms", ack=False) as main:
        times = np.unique(main.getcol("TIME"))
    stamp = lambda sec: (
        datetime(1858, 11, 17) + timedelta(seconds=float(sec))
    ).strftime("%Y/%m/%d/%H:%M:%S.%f")[:-3]
    tr = f"{stamp(times[1])}~{stamp(times[2])}"

    cases = [
        ("file", lambda: dict(inpfile="cmds_a.txt")),
        ("files", lambda: dict(inpfile=["cmds_a.txt", "cmds_b.txt"])),
        ("list-lowercase-bool", lambda: dict(inpfile=[
            "# comment",
            "",
            "mode='manual'  antenna=A1 spw='0:2~3' autocorr=false reason='sci'",
            "mode='summary' name='mid' reason='inspect'",
            "mode='unflag' antenna='A1' spw='0:3' reason='undo'",
            "mode='summary' reason='inspect'",
        ])),
        ("tuple-reason", lambda: dict(
            inpfile=("mode='manual' spw='0:0' reason='a'", "mode='summary' reason='b'", "mode='unflag' reason='c'"),
            reason=("a", "b"),
        )),
        ("ndarray-inputs", lambda: dict(
            inpfile=np.asarray(["mode='manual' spw='0:1' reason='r'", "mode='summary' reason='r'"]),
            reason=np.asarray(["r"]),
        )),
        ("empty-reason", lambda: dict(
            inpfile=["mode='manual' spw='0:0' reason='r'", "mode='summary'"],
            reason=[],
        )),
        ("zero-reason", lambda: dict(
            inpfile=["mode='manual' spw='0:0'", "mode='summary' reason=''"],
            reason="",
        )),
        ("tbuff-scalar", lambda: dict(inpfile=[f"mode='manual' timerange='{tr}' spw='0:0'", "mode='summary'"], tbuff=11.0)),
        ("tbuff-one", lambda: dict(inpfile=[f"mode='manual' timerange='{tr}' spw='0:0'", "mode='summary'"], tbuff=[11.0])),
        ("tbuff-asym", lambda: dict(inpfile=[f"mode='manual' timerange='{tr}' spw='0:0'", "mode='summary'"], tbuff=[11.0, 21.0])),
        ("tbuff-empty", lambda: dict(inpfile=[f"mode='manual' timerange='{tr}' spw='0:0'", "mode='summary'"], tbuff=[])),
        ("tbuff-zero", lambda: dict(inpfile=[f"mode='manual' timerange='{tr}' spw='0:0'", "mode='summary'"], tbuff=0.0)),
        ("tbuff-numpy", lambda: dict(inpfile=[f"mode='manual' timerange='{tr}' spw='0:0'", "mode='summary'"], tbuff=np.asarray([11, 21]))),
        ("tbuff-inline-str", lambda: dict(inpfile=f"mode='manual' timerange='{tr}' spw='0:1'", tbuff=11.0)),
        ("calculate", lambda: dict(inpfile="cmds_a.txt", action="calculate")),
    ]
    checked = 0
    for label, make_opts in cases:
        outputs, states = {}, {}
        for name, task in libraries:
            case = Path(f"in-{label}-{name}")
            case.mkdir()
            vis = case / "vis.ms"
            copytree("inputs.ms", vis)
            before, aux = snapshot(str(vis)), auxiliary_content(str(vis))
            with chdir(case):
                Path("cmds_a.txt").write_text(
                    "# header comment\n\n"
                    "mode='manualflag' antenna='A1' spw='0:0~2' autocorr=false reason='sci'\n"
                    "mode='summary' name='first'\n"
                    "mode='unflag' antenna='A1' spw='0:1' reason='undo'\n"
                    "mode='summary'\n"
                )
                Path("cmds_b.txt").write_text("mode='clip' clipminmax=[0.0, 10.0]\nmode='summary' name='end'\n")
                opts = make_opts()
                outputs[name] = outcome(task, vis="vis.ms", mode="list", flagbackup=False, **opts)
            assert outputs[name][0] == "OK", (label, name, outputs[name])
            states[name] = flags(str(vis))
            if opts.get("action") == "calculate":
                assert_preserved(str(vis), before, aux)
        assert outputs["native"][1] == outputs["casa"][1], (label, outputs)
        np.testing.assert_array_equal(states["native"][0], states["casa"][0])
        np.testing.assert_array_equal(states["native"][1], states["casa"][1])
        checked += 1
    return checked


def check_sequential_all_modes(libraries):
    make_list_fixture("modes.ms")
    make_fourier_fixture("fourier.ms")

    # 1. All geometry + automatic + manual + unflag + extend + summary modes in one program.
    cmds = [
        "mode='manual' spw='0:0~1'",
        "mode='summary' name='s_named'",
        "mode='summary' name=''",
        "mode='unflag' spw='0:1'",
        "mode='quack' quackinterval=10.0 quackmode='beg' quackincrement=False",
        "mode='elevation' lowerlimit=0.0 upperlimit=89.9",
        "mode='shadow' tolerance=0.5 addantenna='extra_ant.txt'",
        "mode='shadow' tolerance=0.5 addantenna={'0': {'name': 'A9', 'diameter': 12.0, 'position': [1.0, 2.0, 3.0]}}",
        "mode='clip' datacolumn='DATA' clipminmax=[0.0, 20.0] clipoutside=True",
        "mode='tfcrop' antenna='0&1' datacolumn='DATA' freqcutoff=2.0 extendflags=False",
        "mode='extend' extendpols=True growfreq=50.0 growtime=0.0",
        "mode='antint' antint_ref_antenna='A0' minchanfrac=0.5 verbose=False",
        "mode='rflag' winsize=3 timedev=[[0, 0, 0.5], [1, 0, 0.5]] freqdev=[[0, 0, 0.5], [1, 0, 0.5]]",
        "mode='summary' correlation='XX'",
        "mode='summary'",
    ]
    res_map, mask_map = {}, {}
    for name, task in libraries:
        case = Path(f"all-{name}")
        case.mkdir()
        vis = case / "vis.ms"
        copytree("modes.ms", vis)
        with chdir(case):
            Path("extra_ant.txt").write_text("name = A8\ndiameter = 12.0\nposition = [1.0, 2.0, 3.0]\n")
            res_map[name] = task(vis="vis.ms", mode="list", flagbackup=False, minrel=0.01, inpfile=cmds)
        mask_map[name] = flags(str(vis))
    assert res_map["native"] == res_map["casa"], (res_map["native"], res_map["casa"])
    np.testing.assert_array_equal(mask_map["native"][0], mask_map["casa"][0])
    np.testing.assert_array_equal(mask_map["native"][1], mask_map["casa"][1])

    # 2. Virtual Fourier residual model clipping & averaged TFCrop in list mode.
    f_masks = {}
    f_cmds = [
        "mode='clip' datacolumn='RESIDUAL_DATA' clipminmax=[0.0, 0.2] channelavg=True chanbin=2",
        "mode='extend' extendpols=False growfreq=50.0 growtime=0.0",
    ]
    for name, task in libraries:
        vis = f"fourier-{name}.ms"
        copytree("fourier.ms", vis)
        assert task(vis=vis, mode="list", flagbackup=False, inpfile=f_cmds) == {}
        f_masks[name] = flags(vis)
    np.testing.assert_array_equal(f_masks["native"][0], f_masks["casa"][0])
    np.testing.assert_array_equal(f_masks["native"][1], f_masks["casa"][1])

    # 3. Top-level vs per-command ntime precedence for averaged clip and extend.
    make_cal_ms("ntime-src.ms", channels=4, scan_seconds=60)
    with table("ntime-src.ms", readonly=False, ack=False) as tb:
        d = tb.getcol("DATA")
        t = tb.getcol("TIME")
        t0, t1 = np.unique(t)[:2]
        d[t <= t1] = 1.0
        d[t > t1, 1] = 10.0
        tb.putcol("DATA", d)
    ntime_cases = (
        ("clip-top40-cmd20", dict(ntime=40.0), ["mode='clip' timeavg=True timebin='40s' clipminmax=[0.5, 2.0] ntime=20.0"]),
        ("clip-top20-cmd40", dict(ntime=20.0), ["mode='clip' timeavg=True timebin='40s' clipminmax=[0.5, 2.0] ntime=40.0"]),
        ("ext-top60-cmd20", dict(ntime=60.0), ["mode='manual' scan='1' spw='0:0' timerange='0~1'", "mode='extend' growtime=50.0 growfreq=0.0 extendpols=False ntime=20.0"]),
        ("ext-top20-cmd60", dict(ntime=20.0), ["mode='manual' antenna='A0&A1' spw='0:0'", "mode='extend' growtime=50.0 growfreq=0.0 extendpols=False ntime=60.0"]),
    )
    for label, kw, ncmds in ntime_cases:
        ms_pair = {}
        for name, task in libraries:
            vis = f"nt-{label}-{name}.ms"
            copytree("ntime-src.ms", vis)
            assert task(vis=vis, mode="list", flagbackup=False, inpfile=ncmds, **kw) == {}
            ms_pair[name] = flags(vis)
        np.testing.assert_array_equal(ms_pair["native"][0], ms_pair["casa"][0], err_msg=label)
        np.testing.assert_array_equal(ms_pair["native"][1], ms_pair["casa"][1], err_msg=label)

    # 4. RFlag calculate in list mode + cross-engine threshold replay + mixed summary.
    calc_files = {}
    for name, task in libraries:
        case = Path(f"rf-calc-{name}")
        case.mkdir()
        vis = case / "vis.ms"
        copytree("modes.ms", vis)
        before, aux = snapshot(str(vis)), auxiliary_content(str(vis))
        with chdir(case):
            out = task(
                vis="vis.ms",
                mode="list",
                action="calculate",
                flagbackup=False,
                savepars=True,
                outfile="saved.txt",
                inpfile=[
                    "mode='manual' spw='0:0'",
                    "mode='rflag' timedev='t_ignore.txt' freqdev=[] extendflags=False",
                    "mode='summary' name='staged_only'",
                    "mode='rflag' timedev=[] freqdev='freq_named.txt' extendflags=False",
                ],
            )
            files = {p.name: literal_eval(p.read_text()) for p in Path(".").glob("*.txt") if p.name != "saved.txt"}
            saved = Path("saved.txt").read_text()
        assert_preserved(str(vis), before, aux)
        calc_files[name] = (out, files, saved, case.resolve())
    assert calc_files["native"][0] == calc_files["casa"][0]
    assert calc_files["native"][2] == calc_files["casa"][2]
    assert calc_files["native"][1].keys() == calc_files["casa"][1].keys()
    for fname, expected in calc_files["casa"][1].items():
        actual = calc_files["native"][1][fname]
        assert actual["name"] == expected["name"]
        axis = "timedev" if "timedev" in actual else "freqdev"
        np.testing.assert_allclose(actual[axis], expected[axis], rtol=1e-6, atol=1e-8)

    # Cross-engine replay from CASA-calculated files -> native apply, and vice versa.
    replay_masks = []
    for reader, writer in ((tasks.flagdata, "casa"), (casatasks.flagdata, "native")):
        vis = f"rf-replay-{writer}.ms"
        copytree("modes.ms", vis)
        wdir = calc_files[writer][3]
        rcmds = [
            "mode='manual' spw='0:0'",
            f"mode='rflag' timedev='{wdir / 'rflag_output_thresholds_timedev1.txt'}' freqdev='{wdir / 'rflag_output_thresholds_freqdev1.txt'}' extendflags=False",
            f"mode='rflag' timedev='{wdir / 'rflag_output_thresholds_timedev3.txt'}' freqdev='{wdir / 'freq_named.txt'}' extendflags=False",
        ]
        assert reader(vis=vis, mode="list", flagbackup=False, inpfile=rcmds) == {}
        replay_masks.append(flags(vis))
    np.testing.assert_array_equal(replay_masks[0][0], replay_masks[1][0])
    np.testing.assert_array_equal(replay_masks[0][1], replay_masks[1][1])
    return len(ntime_cases) + 4


def check_savepars_history_backup_display_cli(libraries):
    make_list_fixture("meta.ms")

    # 1. FLAG_CMD savepars (outfile='') across actions, cmdreason, and summary skip.
    cmd_batch = [
        "mode='manual' spw='0:0' reason='r1'",
        "mode='summary' name='ignored'",
        "mode='unflag' spw='0:0'",
    ]
    for action, applied, reason_kw in (
        ("none", False, dict()),
        ("calculate", False, dict(cmdreason="override")),
        ("apply", True, dict(cmdreason="applied")),
    ):
        results, table_rows = {}, {}
        for name, task in libraries:
            vis = f"fcmd-{action}-{name}.ms"
            copytree("meta.ms", vis)
            results[name] = task(
                vis=vis,
                mode="list",
                action=action,
                flagbackup=False,
                savepars=True,
                outfile="",
                inpfile=cmd_batch,
                **reason_kw,
            )
            with table(f"{vis}/FLAG_CMD", ack=False) as fc, casa_table(f"{vis}/FLAG_CMD") as cfc:
                assert fc.nrows() == cfc.nrows() == 2
                rows = [
                    {col: fc.getcell(col, r) for col in ("APPLIED", "COMMAND", "REASON")}
                    for r in range(fc.nrows())
                ]
                assert rows == [
                    {col: cfc.getcell(col, r) for col in ("APPLIED", "COMMAND", "REASON")}
                    for r in range(cfc.nrows())
                ]
                table_rows[name] = rows
        assert results["native"] == results["casa"], (action, results)
        assert table_rows["native"] == table_rows["casa"], (action, table_rows)
        assert all(r["APPLIED"] is applied and "reason=" not in r["COMMAND"] for r in table_rows["native"])

    # Summary-only command list with outfile='' leaves FLAG_CMD openable and empty.
    for name, task in libraries:
        vis = f"fcmd-sumonly-{name}.ms"
        copytree("meta.ms", vis)
        task(vis=vis, mode="list", action="calculate", flagbackup=False, savepars=True, outfile="", inpfile=["mode='summary'"])
        with table(f"{vis}/FLAG_CMD", ack=False) as fc:
            assert fc.nrows() == 0

    # 2. ASCII outfile overwrite=False/True across apply/calculate/none + shadow addantenna expansion.
    occupied = Path("occupied.txt")
    occupied.write_text("existing\n")
    for action, exc_type in (("apply", RuntimeError), ("calculate", RuntimeError), ("none", ValueError)):
        for name, task in libraries:
            vis = f"occ-{action}-{name}.ms"
            copytree("meta.ms", vis)
            res = outcome(
                task, vis=vis, mode="list", action=action, flagbackup=False,
                savepars=True, outfile=str(occupied), overwrite=False,
                inpfile=["mode='manual' spw='0:0'"],
            )
            assert res[0] == "ERR" and res[1] == exc_type.__name__, (action, name, res)
            assert occupied.read_text() == "existing\n"

    shadow_saved = {}
    Path("ant.txt").write_text("name = A7\ndiameter = 14.0\nposition = [2.0, 3.0, 4.0]\n")
    for name, task in libraries:
        vis = f"shsave-{name}.ms"
        copytree("meta.ms", vis)
        out_path = Path(f"shsave-{name}.txt")
        out_path.write_text("stale\n")
        assert task(
            vis=vis, mode="list", action="", flagbackup=False, savepars=True,
            outfile=str(out_path), overwrite=True, cmdreason="sh",
            inpfile=["mode='shadow' addantenna='ant.txt'", "mode='summary'"],
        ) == {}
        shadow_saved[name] = out_path.read_text()
    assert shadow_saved["native"] == shadow_saved["casa"]

    # 3. HISTORY parity (inpfile str with tbuff=0, tbuff>0, and list inpfile).
    Path("hist_cmds.txt").write_text("mode='manual' spw='0:0'\n")
    with table("meta.ms/HISTORY", ack=False) as h:
        orig_hist = h.nrows()
    for label, opts in (
        ("str-notbuff", dict(inpfile="hist_cmds.txt", tbuff=0.0)),
        ("str-tbuff", dict(inpfile="hist_cmds.txt", tbuff=2.0)),
        ("list-inp", dict(inpfile=["mode='manual' spw='0:0'"])),
    ):
        paths = [f"hist-{label}-{name}.ms" for name, _ in libraries]
        t0 = time() + MJD_UNIX_EPOCH_S
        for p, (_, task) in zip(paths, libraries, strict=True):
            copytree("meta.ms", p)
            assert task(vis=p, mode="list", flagbackup=False, **opts) == {}
        t1 = time() + MJD_UNIX_EPOCH_S
        compare_new_rows(paths, orig_hist, 1, (t0, t1))

    # 4. flagbackup=True + flagmanager restore + display='report' disabling savepars.
    initial = flags("meta.ms")
    bk_vis = "backup-native.ms"
    copytree("meta.ms", bk_vis)
    assert tasks.flagdata(vis=bk_vis, mode="list", flagbackup=True, inpfile=["mode='manual' spw='0:0'"]) == {}
    assert np.any(flags(bk_vis)[0] != initial[0])
    assert Path(f"{bk_vis}.flagversions/flags.flagdata_1").exists()
    tasks.flagmanager(vis=bk_vis, mode="restore", versionname="flagdata_1")
    np.testing.assert_array_equal(flags(bk_vis)[0], initial[0])
    np.testing.assert_array_equal(flags(bk_vis)[1], initial[1])

    # Headless display='report' disables savepars and respects list datacolumn error.
    disp_flags = {}
    for name, task in libraries:
        disp_vis = f"disp-{name}.ms"
        copytree("meta.ms", disp_vis)
        assert task(
            vis=disp_vis, mode="list", flagbackup=False, display="report",
            savepars=True, outfile="", inpfile=["mode='manual' spw='0:0'"],
        ) == {}
        with table(f"{disp_vis}/FLAG_CMD", ack=False) as fc:
            assert fc.nrows() == 0, name
        disp_flags[name] = flags(disp_vis)
    np.testing.assert_array_equal(disp_flags["native"][0], disp_flags["casa"][0])
    np.testing.assert_array_equal(disp_flags["native"][1], disp_flags["casa"][1])
    bad_disp = outcome(
        tasks.flagdata, vis="disp-native.ms", mode="list", flagbackup=False,
        display="report", datacolumn=["DATA"], inpfile=["mode='manual'"],
    )
    casa_bad_disp = outcome(
        casatasks.flagdata, vis="disp-casa.ms", mode="list", flagbackup=False,
        display="report", datacolumn=["DATA"], inpfile=["mode='manual'"],
    )
    assert bad_disp[:2] == casa_bad_disp[:2] == ("ERR", "AttributeError")

    # 5. CLI --mode list + standalone C++ casacore 3.7.1 taql readback.
    binary = os.environ.get("CASARS_FLAGDATA_BIN")
    if binary:
        cli_vis = "cli-native.ms"
        copytree("meta.ms", cli_vis)
        Path("cli_cmds.txt").write_text("mode='manual' spw='0:0'\nmode='summary'\n")
        completed = subprocess.run(
            [binary, "--vis", cli_vis, "--mode", "list", "--inpfile", "cli_cmds.txt",
             "--no-flagbackup", "--savepars", "--outfile", "", "--no-save-last"],
            capture_output=True, text=True, check=True,
        )
        reports = loads(completed.stdout)
        assert len(reports) == 2 and reports[1]["mode"] == "summary"
        fl, flr = flags(cli_vis)
        taql_main = subprocess.check_output(
            ["taql", f"select gsum(ntrue(FLAG)), gsum(iif(FLAG_ROW, 1, 0)) from [{cli_vis}]"],
            text=True,
        )
        assert f"{int(fl.sum())}" in taql_main
        assert f"{int(flr.sum())}" in taql_main
        taql_cmd = subprocess.check_output(
            ["taql", f"select COMMAND from [{cli_vis}/FLAG_CMD]"],
            text=True,
        )
        assert "mode='manual' spw='0:0'" in taql_cmd
        taql_hist = subprocess.check_output(
            ["taql", f"select MESSAGE from [{cli_vis}/HISTORY] where ORIGIN == 'flagdata'"],
            text=True,
        )
        assert "taskname=flagdata" in taql_hist
    return 12


def check_skips_and_refusals(libraries):
    make_list_fixture("refusal-full.ms", with_corrected=True)
    make_list_fixture("refusal-nocol.ms", with_corrected=False)
    copytree("refusal-full.ms", "preflagged.ms")
    casatasks.flagdata(vis="preflagged.ms", mode="manual", spw="0:0", correlation="XX", flagbackup=False)

    skip_to_summary = (
        ("missing-col-clip", "refusal-nocol.ms", ["mode='clip' datacolumn='CORRECTED' clipminmax=[0.0,1.0]", "mode='summary'"]),
        ("missing-col-tfcrop", "refusal-nocol.ms", ["mode='tfcrop' datacolumn='CORRECTED'", "mode='summary'"]),
        ("missing-col-rflag", "refusal-nocol.ms", ["mode='rflag' datacolumn='CORRECTED'", "mode='summary'"]),
        ("missing-col-rflag-badcorr", "refusal-nocol.ms", ["mode='rflag' datacolumn='CORRECTED' correlation='BOGUS'", "mode='summary'"]),
        ("missing-antint", "refusal-full.ms", ["mode='antint' antint_ref_antenna='NO_ANT'", "mode='summary'"]),
        ("unknown-mode", "refusal-full.ms", ["mode='bogus'", "mode='summary'"]),
        ("bad-quack", "refusal-full.ms", ["mode='quack' quackmode='bogus'", "mode='summary'"]),
        ("bad-corr-manual", "refusal-full.ms", ["mode='manual' correlation='BOGUS'", "mode='summary'"]),
        ("bad-corr-tfcrop-ext", "preflagged.ms", ["mode='tfcrop' correlation='BOGUS' extendflags=True", "mode='summary'"]),
        ("bad-corr-tfcrop-noext", "preflagged.ms", ["mode='tfcrop' correlation='BOGUS' extendflags=False", "mode='summary'"]),
        ("bad-corr-rflag-ext", "preflagged.ms", ["mode='rflag' correlation='BOGUS' extendflags=True", "mode='summary'"]),
        ("bad-corr-rflag-noext", "preflagged.ms", ["mode='rflag' correlation='BOGUS' extendflags=False", "mode='summary'"]),
        ("bad-corr-tfcrop-alone-ext", "preflagged.ms", ["mode='tfcrop' correlation='BOGUS' extendflags=True"]),
        ("bad-corr-rflag-alone-ext", "preflagged.ms", ["mode='rflag' correlation='BOGUS' extendflags=True"]),
        ("bad-corr-summary", "refusal-full.ms", ["mode='summary' correlation='BOGUS'", "mode='summary' name='ok'"]),
        ("local-missing-ant", "refusal-full.ms", ["mode='manual' antenna='NO_SUCH'", "mode='summary'"]),
        ("local-missing-field", "refusal-full.ms", ["mode='manual' field='NO_SUCH'", "mode='summary'"]),
    )
    for label, src, cmds in skip_to_summary:
        res, masks = {}, {}
        for name, task in libraries:
            vis = f"skip-{label}-{name}.ms"
            copytree(src, vis)
            res[name] = outcome(task, vis=vis, mode="list", flagbackup=False, inpfile=cmds)
            masks[name] = flags(vis)
        assert res["native"] == res["casa"] and res["native"][0] == "OK", (label, res)
        np.testing.assert_array_equal(masks["native"][0], masks["casa"][0], err_msg=label)
        np.testing.assert_array_equal(masks["native"][1], masks["casa"][1], err_msg=label)

    # Single-mode controls: missing CORRECTED_DATA before correlation skip, and surviving extendflags=True.
    single_err = {}
    for name, task in libraries:
        vis = f"single-missing-rflag-{name}.ms"
        copytree("refusal-nocol.ms", vis)
        single_err[name] = outcome(
            task, vis=vis, mode="rflag", datacolumn="CORRECTED", correlation="BOGUS", flagbackup=False,
        )
    assert single_err["native"] == single_err["casa"] == (
        "ERR", "ValueError", "Failed to parse parameters for mode rflag"
    ), single_err
    for mode_name in ("tfcrop", "rflag"):
        for ext in (True, False):
            s_masks = {}
            for name, task in libraries:
                vis = f"single-{mode_name}-ext{ext}-{name}.ms"
                copytree("preflagged.ms", vis)
                assert task(
                    vis=vis, mode=mode_name, correlation="BOGUS", extendflags=ext, flagbackup=False,
                ) == {}
                s_masks[name] = flags(vis)
            np.testing.assert_array_equal(s_masks["native"][0], s_masks["casa"][0], err_msg=(mode_name, ext))
            np.testing.assert_array_equal(s_masks["native"][1], s_masks["casa"][1], err_msg=(mode_name, ext))

    errors = (
        ("empty-str", "refusal-full.ms", dict(inpfile="")),
        ("empty-list", "refusal-full.ms", dict(inpfile=[])),
        ("missing-file-bk", "refusal-full.ms", dict(inpfile="no_such.txt", flagbackup=True)),
        ("space-line", "refusal-full.ms", dict(inpfile=["   "])),
        ("malformed", "refusal-full.ms", dict(inpfile=["mode='manual' spw"])),
        ("disallowed-key", "refusal-full.ms", dict(inpfile=["mode='manual' vis='x.ms'"])),
        ("unknown-param", "refusal-full.ms", dict(inpfile=["mode='manual' unknown_par=1"])),
        ("int-timecutoff", "refusal-full.ms", dict(inpfile=["mode='tfcrop' timecutoff=4"])),
        ("list-chanbin", "refusal-full.ms", dict(inpfile=["mode='clip' channelavg=True chanbin=[2]"])),
        ("bad-clipminmax", "refusal-full.ms", dict(inpfile=["mode='clip' clipminmax=[1.0]"])),
        ("float-timedev", "refusal-full.ms", dict(inpfile=["mode='rflag' timedev=0.5"])),
        ("scalar-array-reason", "refusal-full.ms", dict(inpfile=["mode='manual'"], reason=np.asarray("r"))),
        ("scalar-array-tbuff", "refusal-full.ms", dict(inpfile=["mode='manual'"], tbuff=np.asarray(1.0))),
        ("ntime-zero-bk", "refusal-full.ms", dict(inpfile=["mode='manual'"], ntime=0.0, flagbackup=True)),
        ("alone-calc-nreport", "refusal-full.ms", dict(action="calculate", inpfile=["mode='antint' antint_ref_antenna='NO_ANT'"])),
        ("alone-missing-antint", "refusal-full.ms", dict(inpfile=["mode='antint' antint_ref_antenna='NO_ANT'"])),
        ("alone-unknown-mode", "refusal-full.ms", dict(inpfile=["mode='bogus'"])),
        ("alone-bad-corr-sum", "refusal-full.ms", dict(inpfile=["mode='summary' correlation='BOGUS'"])),
        ("alone-bad-corr-tfcrop-noext", "preflagged.ms", dict(inpfile=["mode='tfcrop' correlation='BOGUS' extendflags=False"])),
        ("alone-bad-corr-rflag-noext", "preflagged.ms", dict(inpfile=["mode='rflag' correlation='BOGUS' extendflags=False"])),
        ("alone-bad-field", "refusal-full.ms", dict(inpfile=["mode='manual' field='NO_SUCH'"])),
        ("tfcrop-avg+manual", "refusal-full.ms", dict(inpfile=["mode='tfcrop' channelavg=True chanbin=2", "mode='manual'"], flagbackup=True)),
        ("clip-avg+summary", "refusal-full.ms", dict(inpfile=["mode='clip' timeavg=True timebin='10s'", "mode='summary'"])),
        ("rflag-avg+quack", "refusal-full.ms", dict(inpfile=["mode='rflag' channelavg=True chanbin=2", "mode='quack' quackinterval=1.0"])),
    )
    apply_nreports = {
        "malformed",
        "alone-missing-antint",
        "alone-unknown-mode",
        "alone-bad-corr-sum",
        "alone-bad-corr-tfcrop-noext",
        "alone-bad-corr-rflag-noext",
    }
    for label, src, opts in errors:
        res = {}
        for name, task in libraries:
            vis = f"err-{label}-{name}.ms"
            copytree(src, vis)
            before, aux = snapshot(vis), auxiliary_content(vis)
            with table(f"{vis}/HISTORY", ack=False) as h:
                src_hist = h.nrows()
            res[name] = outcome(task, vis=vis, mode="list", **{"flagbackup": False, **opts})
            assert Path(f"{vis}.flagversions").exists() == bool(opts.get("flagbackup")), (label, name)
            if not opts.get("flagbackup") and label not in apply_nreports:
                assert auxiliary_content(vis) == aux, (label, name)
            elif label in apply_nreports:
                with table(f"{vis}/HISTORY", ack=False) as h, table(f"{vis}/FLAG_CMD", ack=False) as fc:
                    assert h.nrows() == src_hist + 78, (label, name, h.nrows(), src_hist)
                    assert fc.nrows() == 0, (label, name, fc.nrows())
            # Flagversions/HISTORY may have been updated; visibility data must be unchanged.
            for b, a in zip(before, snapshot(vis), strict=True):
                np.testing.assert_array_equal(a[0], b[0])
                np.testing.assert_array_equal(a[1], b[1])
                np.testing.assert_array_equal(a[2], b[2])
        assert res["native"][0] == "ERR" and res["native"][1] == res["casa"][1], (label, res)
        if label == "alone-bad-field":
            assert "NO_SUCH" in res["native"][2] and "NO_SUCH" in res["casa"][2], (label, res)
        else:
            assert res["native"][2] == res["casa"][2], (label, res)

    # Allowed averaging combinations: + extend, + antint, + display.
    for name, task in libraries:
        vis = f"avg-allowed-{name}.ms"
        copytree("refusal-full.ms", vis)
        assert task(
            vis=vis, mode="list", flagbackup=False, display="report",
            inpfile=[
                "mode='tfcrop' channelavg=True chanbin=4 freqcutoff=2.0 extendflags=False",
                "mode='extend' growfreq=50.0 growtime=0.0 extendpols=False",
                "mode='antint' antint_ref_antenna='A0' minchanfrac=0.5",
            ],
        ) == {}
    np.testing.assert_array_equal(flags("avg-allowed-native.ms")[0], flags("avg-allowed-casa.ms")[0])
    return len(skip_to_summary) + 5 + len(errors) + 1


def check_agent_time_intervals(libraries):
    make_list_fixture("agent-time.ms")
    with table("agent-time.ms", ack=False, readonly=False) as main:
        original = main.getcol("TIME")
        unique, integrations = np.unique(original, return_inverse=True)
        point = np.floor(unique[0] / 86400) * 86400
        next_point = point + 43200
        # Midnight and noon are exactly representable both in MJD days and
        # seconds. Shift the midnight integration by one representable step.
        times = np.where(integrations < 2, point - 10, next_point + 10 * (integrations - 2))
        times[integrations == 1] = np.nextafter(point, -np.inf)
        main.putcol("TIME", times)

    def stamp(value):
        return (datetime(1858, 11, 17) + timedelta(seconds=float(value))).strftime(
            "%Y/%m/%d/%H:%M:%S.%f"
        )[:-3]

    shifted = times == np.nextafter(point, -np.inf)
    empty = np.zeros_like(times, dtype=bool)
    lower = point + (next_point - point) / 2
    cases = (
        ("global-point", stamp(point), None, shifted),
        ("one-agent-point", stamp(point), False, empty),
        ("two-agent-point", stamp(point), True, empty),
        ("two-agent-range", f"0.0[{stamp(lower)}~{stamp(next_point)}]", True,
         times == next_point),
    )
    for label, timerange, multi, expected in cases:
        for name, task in libraries:
            vis = f"{label}-{name}.ms"
            copytree("agent-time.ms", vis)
            if multi is None:
                options = dict(mode="manual", timerange=timerange)
            else:
                commands = [f"mode='manual' timerange='{timerange}'"]
                if multi:
                    commands.append("mode='summary'")
                options = dict(mode="list", inpfile=commands)
            task(vis=vis, flagbackup=False, **options)
            flagged, row_flags = flags(vis)
            np.testing.assert_array_equal(
                flagged, np.broadcast_to(expected[:, None, None], flagged.shape),
                err_msg=f"{label} {name} FLAG",
            )
            np.testing.assert_array_equal(row_flags, expected, err_msg=f"{label} {name} FLAG_ROW")
    for name, task in libraries:
        vis = f"empty-agent-time-{name}.ms"
        copytree("agent-time.ms", vis)
        result = task(
            vis=vis, flagbackup=False, mode="list",
            inpfile=[
                f"mode='summary' name='empty' timerange='{stamp(point)}'",
                "mode='summary' name='all'",
            ],
        )
        assert [(report["name"], report["total"]) for report in result.values()] == [
            ("empty", 0),
            ("all", flags(vis)[0].size),
        ], (name, result)
    return len(cases) + 1



def check_averaged_agent_time(libraries):
    source = "averaged-agent-time.ms"
    make_list_fixture(source)
    with table(source, ack=False, readonly=False) as main:
        times = main.getcol("TIME")
        first, second = np.unique(times)[:2]
        data = np.full_like(main.getcol("DATA"), 8.)
        main.putcol("DATA", data)
        for column in ("FLAG", "FLAG_ROW"):
            main.putcol(column, np.zeros_like(main.getcol(column)))

    def stamp(value):
        return (datetime(1858, 11, 17) + timedelta(seconds=float(value))).strftime(
            "%Y/%m/%d/%H:%M:%S.%f"
        )[:-3]

    midpoint = first + (second - first) / 2
    assert not np.any(np.abs(times - midpoint) <= 1)
    timerange = f"{stamp(midpoint - 1)}~{stamp(midpoint + 1)}"
    expected_rows = np.isin(times, [first, second])
    expected = np.broadcast_to(expected_rows[:, None, None], data.shape)
    assert np.count_nonzero(expected) == 1280
    for name, task in libraries:
        vis = f"averaged-agent-time-{name}.ms"
        copytree(source, vis)
        assert task(
            vis=vis, mode="list", flagbackup=False,
            inpfile=[
                f"mode='clip' datacolumn='DATA' timerange='{timerange}' "
                "timeavg=True timebin='20s' clipminmax=[0,6]",
                "mode='antint' antint_ref_antenna='A0' minchanfrac=0.01",
            ],
        ) == {}
        samples, rows = flags(vis)
        np.testing.assert_array_equal(samples, expected, err_msg=f"{name} averaged FLAG")
        np.testing.assert_array_equal(rows, expected_rows, err_msg=f"{name} averaged FLAG_ROW")
    return 1


def check_agent_time_defaults(libraries):
    make_list_fixture("agent-default-date.ms")
    with table("agent-default-date.ms", ack=False, readonly=False) as main:
        unique, integration = np.unique(main.getcol("TIME"), return_inverse=True)
        day = np.floor(unique[0] / 86400) * 86400
        offsets = [21600, 21600, 86400, 129600] + [
            151200 + 10 * i for i in range(len(unique) - 4)
        ]
        times = day + np.asarray(offsets)[integration]
        first_scan = integration < 2
        main.putcol("TIME", times)
        main.putcol("FIELD_ID", np.where(first_scan, 0, 1).astype("int32"))
        main.putcol("SCAN_NUMBER", np.where(first_scan, 1, 2).astype("int32"))

    cases = (
        ("single-agent-date", ["mode='manual' timerange='>12:00:00'"], times >= day + 129600),
        ("field-agent-date", ["mode='manual' field='1' timerange='12:00:00'", "mode='summary'"],
         integration == 3),
        ("initial-agent-date", ["mode='manual' scan='1'", "mode='unflag' timerange='12:00:00'"],
         integration == 0),
    )
    for label, commands, expected in cases:
        for name, task in libraries:
            vis = f"{label}-{name}.ms"
            copytree("agent-default-date.ms", vis)
            if label == "initial-agent-date":
                with table(vis, ack=False, readonly=False) as main:
                    times_with_noon = times.copy()
                    times_with_noon[integration == 1] = day + 43200
                    main.putcol("TIME", times_with_noon)
            task(vis=vis, mode="list", inpfile=commands, flagbackup=False)
            flagged, row_flags = flags(vis)
            np.testing.assert_array_equal(
                flagged, np.broadcast_to(expected[:, None, None], flagged.shape),
                err_msg=f"{label} {name} FLAG",
            )
            np.testing.assert_array_equal(row_flags, expected, err_msg=f"{label} {name} FLAG_ROW")
    return len(cases)


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    libraries = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory(prefix="flagdata-list-ms-") as root, chdir(root):
        c1 = check_inputs_and_selectors(libraries)
        c1 += check_agent_time_intervals(libraries)
        c1 += check_agent_time_defaults(libraries)
        c1 += check_averaged_agent_time(libraries)
        c2 = check_sequential_all_modes(libraries)
        c3 = check_savepars_history_backup_display_cli(libraries)
        c4 = check_skips_and_refusals(libraries)
        c4 += check_averaging_admission(libraries)
    print(f"{c1} input/selector cases, {c2} multi-mode/ntime/rflag cases, {c3} metadata/interop cases, "
          f"and {c4} skip/refusal cases verified against CASA 6.7.6.14.")


def check_averaging_admission(libraries):
    make_list_fixture("averaging-admission.ms", with_corrected=False)
    make_list_fixture("averaging-shared.ms")
    with table("averaging-shared.ms", ack=False, readonly=False) as main:
        values = main.getcol("DATA")
        values[:, 0::2, :] = 2
        values[:, 1::2, :] = 8
        main.putcol("DATA", values)
        for column in ("FLAG", "FLAG_ROW"):
            main.putcol(column, np.zeros_like(main.getcol(column)))
    for name, task in libraries:
        invalid = f"invalid-averaging-{name}.ms"
        copytree("averaging-admission.ms", invalid)
        before = flags(invalid)
        result = outcome(
            task, vis=invalid, mode="list", flagbackup=False,
            inpfile=["mode='manual'", "mode='display' datacolumn='DATA' timeavg=True"],
        )
        assert result == (
            "ERR", "RuntimeError",
            "Cannot use timeavg or channelavg in mode display. timeavg=1, channelavg=0. "
            "These averaging options are only allowed for auto-flagging methods. "
            "Refusing this configuration.",
        ), (name, result)
        for original, current in zip(before, flags(invalid), strict=True):
            np.testing.assert_array_equal(current, original)
        assert task(
            vis=invalid, mode="list", flagbackup=False,
            inpfile=["mode='manual'", "mode='antint' datacolumn='CORRECTED' timeavg=True "
                     "antint_ref_antenna='A0'"],
        ) == {}
        assert flags(invalid)[0].all()

        shared = f"shared-averaging-{name}.ms"
        copytree("averaging-shared.ms", shared)
        before = flags(shared)
        result = outcome(
            task, vis=shared, mode="list", flagbackup=False,
            inpfile=["mode='clip' clipminmax=[0,100] channelavg=True chanbin=2",
                     "mode='clip' clipminmax=[0,6]"],
        )
        if name == "casa":
            assert result == ("OK", {}), result
        else:
            assert result == (
                "ERR", "RuntimeError",
                "Averaging with multiple automatic flagging agents requires a shared "
                "averaged visibility iterator (#978).",
            ), result
        for original, current in zip(before, flags(shared), strict=True):
            np.testing.assert_array_equal(current, original)
    return 3


if __name__ == "__main__":
    main()
