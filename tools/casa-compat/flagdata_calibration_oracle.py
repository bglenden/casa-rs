#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare native manual flagging, clipping and summaries on calibration tables."""

import argparse
from contextlib import chdir
from datetime import datetime, timedelta
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys

import casatasks as casa
import numpy as np

from calibration_gencal_tsys_oracle import attach_syscal
from calibration_smoothcal_oracle import opened
from flagmanager_calibration_oracle import assert_flags, original_columns, preserved
from record_casa_returns import make_cal_ms


def fixture(root):
    vis = root / "source.ms"
    make_cal_ms(str(vis), channels=8, scan_seconds=80)
    products = [(root / "bandpass.cal", "CPARAM"), (root / "tsys.cal", "FPARAM")]
    casa.bandpass(str(vis), str(products[0][0]), refant="A0", minsnr=0.0)
    attach_syscal(vis, root / "syscal.txt", spectrum=True)
    with opened(vis / "SYSCAL", True) as table:
        table.removerows(np.flatnonzero(table.getcol("ANTENNA_ID") >= 5).tolist())
    casa.gencal(str(vis), str(products[1][0]), "tsys")
    for path, column in products:
        with opened(path, True) as table:
            flags = table.getcol("FLAG")
            flags[:] = False
            flags[0, 0, 0] = flags[1, -1, 1] = True
            table.putcol("FLAG", flags)
            data = table.getcol(column)
            data[:, :4, :] = np.array([0.0, 1.0, 2.0, -1.0])[None, :, None]
            table.putcol(column, data)
    return products


def outcome(engine, path, **settings):
    try:
        return engine(str(path), **settings)
    except Exception as error:
        return type(error).__name__, str(error).replace(str(path), "<table>")


def flags(path):
    with opened(path) as table:
        return table.getcol("FLAG")


def cases(column, first_time):
    yield "manual", dict(mode="manual", antenna="A2")
    yield "channels", dict(mode="manual", antenna="A1", spw="0:2~3")
    yield "negation", dict(mode="manual", antenna="!A1", spw="0:1~5")
    yield "autocorr", dict(mode="manual", autocorr=True)
    yield "unflag", dict(mode="unflag", spw="0:0~7")
    yield "field", dict(mode="manual", field="PHASE")
    yield "scan", dict(mode="manual", scan="1", observation="0")
    yield "scan-comparison", dict(mode="manual", scan=">1")
    yield "scan-comma", dict(mode="manual", scan=",")
    yield "scan-reversed", dict(mode="manual", scan="2~1")
    yield "observation-comma", dict(mode="manual", observation=",")
    yield "time", dict(mode="manual", timerange=first_time)
    yield "preview", dict(mode="manual", action="calculate", antenna="A1")
    yield "summary", dict(mode="summary", name="user summary")
    yield "summary-axes", dict(mode="summary", spw="0:2~4", spwchan=True,
                              spwcorr=True, basecnt=True, fieldcnt=True)
    yield "summary-filter", dict(mode="summary", minabs=1, maxrel=0.1, spwcorr=True)
    yield "clip", dict(mode="clip", datacolumn=column, clipminmax=[0.1, 1.5], spw="0:0~2")
    yield "clip-calculate", dict(mode="clip", datacolumn=column, clipzeros=True, action="calculate")
    yield "clip-vector", dict(mode="clip", datacolumn=column, clipoutside=[False], clipminmax=[0.1, 1.5])
    yield "clip-decimal", dict(mode="clip", datacolumn=column, clipminmax=[0.0, 0.1])
    yield "clip-solution", dict(mode="clip", datacolumn=column, correlation="Sol1", clipminmax=[0.1, 1.5])
    yield "clip-real-solution", dict(mode="clip", datacolumn=column, correlation="REAL_SOL2", clipzeros=True)
    yield "clip-signed-zero", dict(mode="clip", datacolumn=column, correlation="REAL_ALL", clipzeros=True)
    yield "clip-postfix", dict(mode="clip", datacolumn=column, correlation="ABS_SOL1", clipminmax=[0.1, 1.5])
    yield "clip-imaginary", dict(mode="clip", datacolumn=column, correlation="IMAG_ALL", clipminmax=[0.1, 1.5])
    yield "clip-bare", dict(mode="clip", datacolumn=column, correlation="ABS", clipminmax=[0.1, 1.5])
    yield "clip-unknown", dict(mode="clip", datacolumn=column, correlation="ARBITRARY", clipminmax=[0.1, 1.5])
    yield "clip-parameter-error", dict(mode="clip", datacolumn="PARAMERR", clipminmax=[0.1, 1.5])
    yield "clip-channelavg", dict(mode="clip", datacolumn=column, channelavg=True, chanbin=2, clipminmax=[0.1, 1.5])
    yield "clip-timeavg", dict(mode="clip", datacolumn=column, timeavg=True, timebin="10s", clipminmax=[0.1, 1.5])
    yield "clip-missing", dict(mode="clip", datacolumn="DATA")
    yield "select-none", dict(mode="manual", antenna="missing")
    yield "invalid-array", dict(mode="manual", array="0")
    yield "correlation", dict(mode="manual", correlation="Sol1")
    yield "summary-correlation", dict(mode="summary", correlation="Sol1")
    for action in ("apply", "calculate", "none"):
        yield f"saved-{action}", dict(mode="manual", antenna="A1", savepars=True, action=action)
    yield "backup", dict(mode="manual", antenna="A2", flagbackup=True)


def exercise(root, reference, native, fixture_path, column):
    total, written = 0, 0
    with opened(fixture_path) as table:
        time = datetime(1858, 11, 17) + timedelta(seconds=float(table.getcell("TIME", 0)))
    for name, settings in cases(column, time.strftime("%Y/%m/%d/%H:%M:%S.%f")):
        expected, actual = (root / f"{extension}-{column}-{name}.cal"
                            for extension in ("casa", "native"))
        for path in (expected, actual):
            shutil.copytree(fixture_path, path)
            if name == "clip-decimal":
                with opened(path, True) as table:
                    values = table.getcol(column)
                    values[:] = 0.1
                    table.putcol(column, values)
        initial = flags(actual)
        metadata = original_columns(actual)
        settings.setdefault("flagbackup", False)
        want = outcome(reference, expected, **settings)
        have = outcome(native, actual, **settings)
        assert want == have, (column, name, want, have)
        observed = flags(expected)
        assert_flags(actual, observed)
        preserved(actual, metadata)
        if settings["flagbackup"]:
            for path in (expected, actual):
                version = root / f"{path.name}.flagversions" / "flags.flagdata_1"
                assert_flags(version, initial, version=True)
        elif settings.get("action") == "calculate":
            assert not (root / f"{actual.name}.flagversions").exists()
            np.testing.assert_array_equal(observed, initial)
        count = int(np.count_nonzero(observed != initial))
        if name in ("manual", "channels", "field", "scan", "time", "clip", "backup"):
            assert count > 0, (name, column)
        if name in ("scan-comma", "scan-reversed", "observation-comma", "clip-vector", "clip-decimal", "clip-bare", "clip-parameter-error"):
            assert count == 0, (name, column)
        written += count
        total += 1
        print(f"{column} {name}: {count} changed cells", flush=True)
    return total, written


def stride_safety(root, native, products):
    for source, column in products:
        for label, engine, channels in (("casa", casa.flagdata, [1, 2, 3, 4, 5]),
                                        ("native", native, [1, 3, 5])):
            path = root / f"{label}-{column}-stride.cal"
            shutil.copytree(source, path)
            expected = flags(path)
            expected[:, channels, :] = True
            assert engine(str(path), mode="manual", spw="0:1~5^2", flagbackup=False) == {}
            assert_flags(path, expected)


def wildcard_summary(root, native):
    delay = root / "wildcard-delay.cal"
    casa.gencal(str(root / "source.ms"), str(delay), "sbd", parameter=[2.5])
    before = flags(delay)
    probe = """
import resource
import sys
from casatasks import flagdata
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
flagdata(sys.argv[1], mode='summary', flagbackup=False)
"""
    reference = subprocess.run([sys.executable, "-c", probe, str(delay)],
                               capture_output=True, timeout=30)
    assert reference.returncode == -signal.SIGSEGV.value, reference.stderr.decode()
    report = native(str(delay), mode="summary", flagbackup=False)
    assert report["total"] == float(before.size)
    assert report["field"] == {"-1": {"flagged": float(before.sum()), "total": float(before.size)}}
    assert_flags(delay, before)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    os.environ["CASARS_FLAGDATA_BIN"] = str(args.native_bin.resolve())
    from casars.tasks import flagdata as native

    assert casa.version_string() == "6.7.6.14"
    comparisons, changed = 0, 0
    with chdir(root):
        products = fixture(root)
        for path, column in products:
            count, edits = exercise(root, casa.flagdata, native, path, column)
            comparisons += count
            changed += edits
        stride_safety(root, native, products)
        wildcard_summary(root, native)
    print(f"{comparisons} paired calibration flagdata cases and {changed} exact changed cells. "
          "Both stride cases and the wildcard-field summary checked.")


if __name__ == "__main__":
    main()
