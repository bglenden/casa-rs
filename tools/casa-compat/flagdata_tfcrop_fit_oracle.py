#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare TFCrop model and sliding-window controls with pinned CASA."""

from contextlib import chdir
from json import loads
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_preview_oracle import assert_preserved, auxiliary_content
from flagdata_routine_controls_oracle import flags
from flagdata_summary_oracle import snapshot
from record_casa_returns import make_cal_ms


def make_fixture(path, integrations=21, broadband=True):
    make_cal_ms(path, channels=32, scan_seconds=10 * integrations)
    with table(path, ack=False, readonly=False) as main:
        visibilities = np.ones_like(main.getcol("DATA"))
        times = main.getcol("TIME")
        ordinal = np.searchsorted(np.unique(times), times)
        baseline = (main.getcol("ANTENNA1") == 0) & (main.getcol("ANTENNA2") == 1)
        narrow_rows = baseline & (ordinal < integrations)
        broad_rows = baseline & (ordinal == 6)
        if broadband:
            visibilities[broad_rows, :, 0] = 30.
        visibilities[narrow_rows, 16, 1] = 30.
        main.putcol("DATA", visibilities)
        empty = np.zeros(visibilities.shape, dtype=bool)
        main.putcol("FLAG", empty)
        main.putcol("FLAG_ROW", np.zeros(len(times), dtype=bool))
    return empty, narrow_rows, broad_rows


def check_fits(libraries):
    empty, narrow, broad = make_fixture("source.ms")
    cases = (
        ({}, (16,), False),
        ({"flagdimension": "freq"}, (16,), False),
        ({"flagdimension": "time"}, (), False),
        ({"flagdimension": "timefreq"}, (16,), False),
        ({"flagdimension": "freq", "freqfit": "line"}, (), False),
        ({"flagdimension": "freq", "maxnpieces": np.int64(1)}, (16,), False),
        ({"flagdimension": "time", "timefit": "poly", "maxnpieces": 3, "timecutoff": 2.5}, (), True),
        ({"flagdimension": "freq", "usewindowstats": "sum", "halfwin": 1}, range(15, 18), False),
        ({"flagdimension": "freq", "usewindowstats": "std", "halfwin": 2}, range(14, 19), False),
        ({"flagdimension": "freq", "usewindowstats": "std", "halfwin": np.int32(2)}, range(15, 18), False),
        ({"flagdimension": "freq", "usewindowstats": "both", "halfwin": 3}, range(13, 20), False),
        ({"freqfit": "LINE"}, (), False),
        ({"flagdimension": "invalid"}, (), False),
        ({"usewindowstats": "invalid"}, (), False),
        ({"maxnpieces": 0}, (), False),
        ({"halfwin": 4}, (), False),
        ({"halfwin": True}, (), False),
    )
    for index, (settings, active_channels, flag_broadband) in enumerate(cases):
        expected = empty.copy()
        for channel in active_channels:
            expected[narrow, channel, 1] = True
        if flag_broadband:
            expected[broad, :, 0] = True
        for name, task in libraries:
            path = f"fit-{index}-{name}.ms"
            copytree("source.ms", path)
            original, auxiliary = snapshot(path), auxiliary_content(path)
            options = dict(vis=path, mode="tfcrop", antenna="0&1", datacolumn="data",
                           extendflags=False, **settings)
            assert task(action="calculate", **options) == {}
            assert_preserved(path, original, auxiliary)
            if name == "native":
                numpy_defaults = {"maxnpieces": 7, "halfwin": 1}
                request = {key: numpy_defaults[key] if isinstance(value, np.integer) else value
                           for key, value in options.items()}
                outcome = tasks.run("flagdata", action="calculate", **request)
                assert loads(outcome.stdout)["result"]["changed_samples"] == expected.sum(), options
                assert_preserved(path, original, auxiliary)
            assert task(**options) == {}
            samples, row_flags = flags(path)
            np.testing.assert_array_equal(samples, expected, err_msg=repr(options))
            assert not row_flags.any()
            prior, prior_rows = flags(str(Path(path + ".flagversions", "flags.flagdata_1")))
            np.testing.assert_array_equal(prior, empty)
            assert not prior_rows.any()
            for before, after in zip(original, snapshot(path), strict=True):
                np.testing.assert_array_equal(after[2], before[2])
    return len(cases)


def check_invalid_schema(libraries):
    cases = (
        ("timefit", None, "null value not allowed"),
        ("freqfit", 1, "must be of cStr type"),
        ("flagdimension", ["freq"], "must be of cStr type"),
        ("usewindowstats", True, "must be of cStr type"),
        ("halfwin", np.array(2), "must be of cInt type"),
        ("maxnpieces", "2", "must be of cInt type"),
    )
    for name, task in libraries:
        path = f"invalid-fit-{name}.ms"
        copytree("source.ms", path)
        original, auxiliary = snapshot(path), auxiliary_content(path)
        for key, value, reason in cases:
            try:
                task(vis=path, mode="tfcrop", **{key: value})
            except AssertionError as error:
                assert str(error) == str({key: [reason]})
            else:
                raise AssertionError((name, key, value))
            assert_preserved(path, original, auxiliary)
    return len(cases)


def check_equal_axis_lengths(libraries):
    for integrations in (32, 33):
        source = f"axes-{integrations}.ms"
        empty, narrow, _ = make_fixture(source, integrations, broadband=False)
        for name, task in libraries:
            path = f"axes-{integrations}-{name}.ms"
            copytree(source, path)
            assert task(vis=path, mode="tfcrop", datacolumn="data", antenna="0&1",
                        flagdimension="time", timefit="poly", timecutoff=3.,
                        extendflags=False, flagbackup=False) == {}
            expected = empty.copy()
            # The narrowband signal is stationary. CASA incorrectly runs
            # the frequency fit for a square time/frequency buffer.
            if name == "casa" and integrations == 32:
                expected[narrow, 16, 1] = True
            samples, row_flags = flags(path)
            np.testing.assert_array_equal(samples, expected)
            assert not row_flags.any()


def check_full_row(libraries):
    empty, _, broad = make_fixture("broadband-all.ms")
    with table("broadband-all.ms", readonly=False, ack=False) as main:
        data = np.ones_like(main.getcol("DATA"))
        data[broad, :, :] = 30.
        main.putcol("DATA", data)
    expected = empty.copy()
    expected[broad, :, :] = True
    for name, task in libraries:
        path = f"broadband-all-{name}.ms"
        copytree("broadband-all.ms", path)
        assert task(vis=path, mode="tfcrop", datacolumn="data", antenna="0&1",
                    flagdimension="time", timefit="poly", timecutoff=2.5, maxnpieces=3,
                    extendflags=False, flagbackup=False) == {}
        samples, rows = flags(path)
        np.testing.assert_array_equal(samples, expected)
        assert expected.sum() == 64 and not rows.any()


def check_commands(libraries):
    written = []
    for name, task in libraries:
        path, out = f"command-{name}.ms", Path(f"command-{name}.txt")
        copytree("source.ms", path)
        original, auxiliary = snapshot(path), auxiliary_content(path)
        options = dict(vis=path, mode="tfcrop", datacolumn="data", antenna="0&1", savepars=True,
                       extendflags=False, flagdimension="timefreq", maxnpieces=3, timefit="poly",
                       usewindowstats="std", halfwin=np.int32(2))
        assert task(action="none", outfile=str(out), **options) == {}
        assert_preserved(path, original, auxiliary)
        assert task(flagbackup=False, **options) == {}
        with table(f"{path}/FLAG_CMD", ack=False) as commands:
            assert commands.nrows() == 1 and commands.getcell("APPLIED", 0)
            command = commands.getcell("COMMAND", 0)
        assert "datacolumn='DATA'" in command and "timefit='poly'" in command
        assert "maxnpieces=3" in command and "usewindowstats='std'" in command and "halfwin=2" in command
        written.append((out.read_text(), command))
    if len(written) == 2:
        assert written[0] == written[1], written


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    libraries = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory(prefix="flagdata-tfcrop-fit-") as directory, chdir(directory):
        fits = check_fits(libraries)
        refusals = check_invalid_schema(libraries)
        check_equal_axis_lengths(libraries)
        check_full_row(libraries)
        check_commands(libraries)
    print(f"{fits} TFCrop fitting and window products, {refusals} refusals, equal-axis controls "
          "and saved commands verified")


if __name__ == "__main__":
    main()
