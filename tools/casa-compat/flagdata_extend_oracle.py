#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare flag extension against pinned CASA and exact selected sample masks."""

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


def make_fixture():
    make_cal_ms("source.ms", channels=8, scan_seconds=40)
    with table("source.ms", ack=False, readonly=False) as main:
        main.putcol("FIELD_ID", np.zeros(main.nrows(), dtype=np.int32))
        mask = (main.getcol("ANTENNA1") == 0) & (main.getcol("ANTENNA2") == 1)
        rows = np.flatnonzero(mask)
        times = main.getcol("TIME")[rows]
        return rows[np.argsort(times)]


def seed(path, samples, true_rows=()):
    copytree("source.ms", path)
    with table(path, ack=False, readonly=False) as main:
        flags = np.zeros_like(main.getcol("FLAG"))
        for row, channel, correlation in samples:
            flags[row, channel, correlation] = True
        row_flags = np.zeros(len(flags), dtype=bool)
        for row in true_rows:
            row_flags[row] = True
        main.putcol("FLAG", flags)
        main.putcol("FLAG_ROW", row_flags)
    return flags, row_flags


def check_products(libraries):
    rows = make_fixture()
    assert len(rows) == 8
    cases = (
        ({"flagneartime": True}, [(1, 3)], [(r, 3) for r in (0, 1, 2)]),
        ({"flagnearfreq": True}, [(1, 3)], [(1, c) for c in (2, 3, 4)]),
        ({"growaround": True}, [(0, 2), (0, 3), (0, 4), (1, 2), (1, 4)],
         [(r, c) for r in (0, 1) for c in (2, 3, 4)]),
        ({"flagnearfreq": True, "growfreq": 25.}, [(1, 3)], [(1, c) for c in range(8)]),
        ({"flagneartime": True, "spw": "0:1~2;4~5"}, [(1, 4)], [(r, 4) for r in (0, 1, 2)]),
        ({"flagnearfreq": True, "spw": "0:1~2;4~5"}, [(1, 4)], [(1, c) for c in (2, 4, 5)]),
        ({"growtime": 25.}, [(1, 3)], [(r, 3) for r in range(4)]),
        ({"growtime": 25., "combinescans": True}, [(1, 3)], [(1, 3)]),
        ({"growtime": 40.}, [(1, 3)], [(1, 3)]),
        ({"growtime": 40., "ntime": "20s"}, [(1, 3)], [(0, 3), (1, 3)]),
        ({"growtime": -1., "flagneartime": True}, [(1, 3)], [(1, 3)]),
        ({"growfreq": 101., "flagneartime": True}, [(1, 3)], [(1, 3)]),
        ({"growtime": float("nan"), "flagneartime": True}, [(1, 3)], [(r, 3) for r in (0, 1, 2)]),
        ({"flagneartime": True, "combinescans": True}, [(3, 3)], [(r, 3) for r in (2, 3, 4)]),
        ({"flagneartime": True}, [(3, 3)], [(r, 3) for r in (2, 3)]),
        ({"flagnearfreq": np.bool_(True)}, [(1, 3)], [(1, 3)]),
        ({"flagneartime": np.bool_(True)}, [(1, 3)], [(1, 3)]),
        ({"growaround": np.bool_(True)}, [(0, 2), (0, 3), (0, 4), (1, 2), (1, 4)],
         [(0, 2), (0, 3), (0, 4), (1, 2), (1, 4)]),
        ({"flagneartime": True, "combinescans": np.bool_(True)}, [(3, 3)], [(r, 3) for r in (2, 3)]),
    )
    for index, (case_options, seeds, wanted) in enumerate(cases):
        for name, task in libraries:
            path = f"extend-{index}-{name}.ms"
            seeded, seeded_rows = seed(path, [(rows[t], c, 0) for t, c in seeds])
            original, before = snapshot(path), auxiliary_content(path)
            expected = np.zeros_like(seeded)
            for time, channel in wanted:
                expected[rows[time], channel, 0] = True
            options = {"vis": path, "mode": "extend", "antenna": "0&1", "correlation": "XX",
                       "extendpols": False, "growtime": 0., "growfreq": 0., **case_options}
            assert task(action="calculate", **options) == {}
            assert_preserved(path, original, before)
            if name == "native":
                run = tasks.run("flagdata", action="calculate", **options)
                assert loads(run.stdout)["result"]["changed_samples"] == (expected != seeded).sum()
                assert_preserved(path, original, before)
            assert task(**options) == {}
            samples, row_flags = flags(path)
            np.testing.assert_array_equal(samples, expected, err_msg=repr(options))
            np.testing.assert_array_equal(row_flags, seeded_rows)
            backup, backup_rows = flags(str(Path(path + ".flagversions", "flags.flagdata_1")))
            np.testing.assert_array_equal(backup, seeded)
            np.testing.assert_array_equal(backup_rows, seeded_rows)
            for prior, after in zip(original, snapshot(path), strict=True):
                np.testing.assert_array_equal(prior[2], after[2])
            for parameter, value in case_options.items():
                if isinstance(value, np.bool_):
                    with table(f"{path}/HISTORY", ack=False) as history:
                        messages = history.getcol("MESSAGE")[-78:]
                    values = [line.split("=", 1)[1].strip() for line in messages
                              if line.startswith(parameter + " ")]
                    assert values == ["True"]
    return rows, len(cases)


def check_polarizations(libraries, rows):
    cases = (
        (True, "", (8, 8)),
        (False, "", (8, 0)),
        (True, "XX", (8, 0)),
        (True, "ABS_ALL", (8, 8)),
        (True, "ABS_XX", (8, 0)),
        ([True], "", (4, 0)),
        (1, "", (4, 0)),
        ("true", "", (4, 0)),
        (np.array(True), "", (4, 0)),
    )
    row = rows[1]
    for index, (extendpols, correlation, expected) in enumerate(cases):
        for name, task in libraries:
            path = f"polarization-{index}-{name}.ms"
            seeded, seeded_rows = seed(path, [(row, c, 0) for c in range(4)],
                                       true_rows=(rows[0],))
            options = dict(vis=path, mode="extend", antenna="0&1", correlation=correlation,
                           extendpols=extendpols, growtime=0., growfreq=50.)
            assert task(**options) == {}
            wanted = seeded.copy()
            # The public CASA agent widens ABS_XX to all stored planes.
            # The scientifically scoped result is the plain XX control.
            xx, yy = (8, 8) if name == "casa" and correlation == "ABS_XX" else expected
            wanted[row, :xx, 0] = True
            wanted[row, :yy, 1] = True
            samples, row_flags = flags(path)
            np.testing.assert_array_equal(samples, wanted, err_msg=repr(options))
            np.testing.assert_array_equal(row_flags, seeded_rows)
            assert (Path(path + ".flagversions") / "flags.flagdata_1").exists()
    return len(cases)


def check_invalid_inputs(libraries):
    invalid = (
        ("growtime", None, "null value not allowed"),
        ("growtime", "50", "must be of cFloat type"),
        ("growfreq", np.array(50.), "must be of cFloat type"),
        ("extendpols", None, "null value not allowed"),
        ("extendpols", np.bool_(True), "must be of cVariant type"),
        ("growaround", np.array(True), "must be of cBool type"),
        ("combinescans", [True], "must be of cBool type"),
        ("flagnearfreq", 1, "must be of cBool type"),
    )
    for name, task in libraries:
        path = f"invalid-{name}.ms"
        copytree("source.ms", path)
        original, auxiliary = snapshot(path), auxiliary_content(path)
        for parameter, value, reason in invalid:
            try:
                task(vis=path, mode="extend", **{parameter: value})
            except AssertionError as error:
                assert str(error) == str({parameter: [reason]})
            else:
                raise AssertionError((name, parameter, value))
            assert_preserved(path, original, auxiliary)
    return len(invalid)


def check_commands(libraries, row):
    written = []
    for name, task in libraries:
        path, out = f"commands-{name}.ms", Path(f"commands-{name}.txt")
        seed(path, [(row, 3, 0)])
        original, auxiliary = snapshot(path), auxiliary_content(path)
        options = dict(vis=path, mode="extend", antenna="0&1", correlation="XX",
                       extendpols=False, ntime="20s", combinescans=True, growtime=25.,
                       growfreq=np.float32(50), flagnearfreq=np.bool_(True), savepars=True)
        assert task(action="none", outfile=str(out), **options) == {}
        assert_preserved(path, original, auxiliary)
        assert task(flagbackup=False, **options) == {}
        with table(f"{path}/FLAG_CMD", ack=False) as commands:
            assert commands.nrows() == 1 and commands.getcell("APPLIED", 0)
            command = commands.getcell("COMMAND", 0)
        assert "ntime=20.0" in command and "extendpols=False" in command
        assert "growtime=25.0" in command and "flagnearfreq=True" in command
        written.append((out.read_text(), command))
    if len(written) == 2:
        assert written[0] == written[1]


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    libraries = (("casa", casatasks.flagdata), ("native", tasks.flagdata))
    with TemporaryDirectory(prefix="flagdata-extension-") as directory, chdir(directory):
        rows, count = check_products(libraries)
        polarizations = check_polarizations(libraries, rows)
        invalid = check_invalid_inputs(libraries)
        check_commands(libraries, rows[1])
    print(f"{count} extension products, {polarizations} polarization products, "
          f"{invalid} refusals, previews, backups and saved commands verified")


if __name__ == "__main__":
    main()
