#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Verify flagdata command text and write ordering against pinned CASA."""

from contextlib import chdir
from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_summary_oracle import snapshot
from ms_selection_tasks_oracle import casa_table
from record_casa_returns import make_cal_ms

TASKS = (("casa", casatasks.flagdata), ("native", tasks.flagdata))


def command_row(text, *, reason="", applied=False):
    return {"APPLIED": applied, "COMMAND": text, "INTERVAL": 0.0, "LEVEL": 0,
            "REASON": reason, "SEVERITY": 0, "TIME": 0.0, "TYPE": ""}


def inspect(vis, original, commands, history_calls):
    with table(f"{vis}/FLAG_CMD", ack=False) as saved, casa_table(f"{vis}/FLAG_CMD") as reopened:
        assert saved.nrows() == reopened.nrows() == len(commands)
        for row, expected in enumerate(commands):
            assert {name: saved.getcell(name, row) for name in expected} == expected
            assert {name: reopened.getcell(name, row) for name in expected} == expected
    with table(f"{vis}/HISTORY", ack=False) as history:
        assert history.nrows() == 78 * history_calls
    current = snapshot(vis)
    for before, after in zip(original, current, strict=True):
        np.testing.assert_array_equal(after[2], before[2])
    assert not Path(f"{vis}.flagversions").exists()
    return current


def compare_snapshots(left, right):
    for left_row, right_row in zip(left, right, strict=True):
        for expected, actual in zip(left_row, right_row, strict=True):
            np.testing.assert_array_equal(actual, expected)


def compare_table_commands(original):
    for name, task in TASKS:
        vis = f"table-{name}.ms"
        copytree("source.ms", vis)
        common = {"vis": vis, "savepars": True, "flagbackup": False}
        expected = []
        options = (
            (dict(field="FLUX", spw="0:0", cmdreason="chosen field"),
             command_row("field='FLUX' spw='0:0' autocorr=False mode='manual'",
                         reason="chosen field", applied=True)),
            (dict(mode="manualflag", autocorr=True, action="none"),
             command_row("autocorr=True mode='manual'")),
            (dict(mode="unflag", action="", cmdreason="undo"),
             command_row("mode='unflag'", reason="undo")),
            (dict(mode="clip", action="calculate", clipminmax=[1, 3], datacolumn="corrected",
                  correlation="xx"),
             command_row("datacolumn='CORRECTED' clipoutside=True channelavg=False chanbin=1 "
                         "timeavg=False timebin='0s' clipzeros=False clipminmax=[1.0, 3.0] "
                         "mode='clip' correlation='XX'")),
            (dict(action="none", field="no matching rows"),
             command_row("field='no matching rows' autocorr=False mode='manual'")),
        )
        for index, (kwargs, row) in enumerate(options):
            expected.append(row)
            assert task(**common, **kwargs) == {}
            current = inspect(vis, original, expected, history_calls=1)
            if index == 0:
                after_apply = current
                assert any(np.any(before[1] != after[1])
                           for before, after in zip(original, after_apply, strict=True))
            else:
                compare_snapshots(after_apply, current)
    compare_snapshots(snapshot("table-casa.ms"), snapshot("table-native.ms"))
    return len(options)


def compare_ascii_commands(original):
    for name, task in TASKS:
        vis = f"ascii-{name}.ms"
        copytree("source.ms", vis)
        common = {"vis": vis, "savepars": True, "flagbackup": False}
        path = Path(f"clip-{name}.txt")
        assert task(**common, action="none", mode="clip", channelavg=True, chanbin=2,
                    outfile=str(path), cmdreason="chosen range", overwrite=False) == {}
        assert path.read_text() == ("datacolumn='DATA' clipoutside=True channelavg=True chanbin=2 "
                                    "timeavg=False timebin='0s' clipzeros=False mode='clip' "
                                    "reason='chosen range'\n")
        compare_snapshots(inspect(vis, original, [], history_calls=0), original)
        summary = task(**common, mode="summary", outfile=f"summary-{name}.txt")
        assert summary["flagged"] == 0
        assert not Path(f"summary-{name}.txt").exists()
        assert task(**common, mode="quack", quackinterval=4, quackmode="endb", outfile=str(path)) == {}
        assert path.read_text() == "quackmode='endb' quackinterval=4.0 quackincrement=False mode='quack'\n"
        inspect(vis, original, [], history_calls=1)
    compare_snapshots(snapshot("ascii-casa.ms"), snapshot("ascii-native.ms"))


def compare_occupied_file(original):
    cases = (("apply", RuntimeError), ("calculate", RuntimeError), ("none", ValueError))
    path = Path("occupied.txt")
    path.write_text("original\n")
    message = "You have set overwrite to False. Remove occupied.txt before saving the flag commands"
    for action, error_type in cases:
        products = []
        for name, task in TASKS:
            vis = f"occupied-{action}-{name}.ms"
            copytree("source.ms", vis)
            try:
                task(vis=vis, flagbackup=False, savepars=True, action=action, outfile=str(path),
                     overwrite=False)
            except error_type as error:
                assert type(error) is error_type and str(error) == message
            else:
                raise AssertionError((name, action))
            assert path.read_text() == "original\n"
            products.append(inspect(vis, original, [], history_calls=0))
        compare_snapshots(*products)
        if action == "apply":
            assert all(row[1].all() for row in products[0])
        else:
            compare_snapshots(products[0], original)
    return len(cases)


def compare_original_arguments():
    common = {"vis": "source.ms", "savepars": True, "action": "none"}
    cases = (
        ({"field": ["FLUX"], "spw": ["0"], "scan": ("1", "2")},
         "field=['FLUX'] spw=['0'] scan=['1', '2'] autocorr=False mode='manual'\n"),
        ({"mode": "clip", "clipoutside": np.bool_(False), "channelavg": np.bool_(True),
          "clipzeros": np.bool_(True), "chanbin": np.array(2), "clipminmax": [float("nan"), float("inf")]},
         "datacolumn='DATA' clipoutside=False channelavg=True chanbin=2 timeavg=False "
         "timebin='0s' clipzeros=True clipminmax=[nan, inf] mode='clip'\n"),
    )
    for index, (options, command) in enumerate(cases):
        for name, task in TASKS:
            path = Path(f"arguments-{index}-{name}.txt")
            assert task(**common, **options, outfile=str(path)) == {}
            assert path.read_text() == command


def check_reader_resync():
    vis = "reader.ms"
    copytree("source.ms", vis)
    options = {"vis": vis, "savepars": True, "action": "none"}
    casatasks.flagdata(**options, field="FLUX")
    command_table = f"{vis}/FLAG_CMD"
    with table(command_table, lockoptions="usernoread", ack=False) as reader:
        def bindings():
            return [{key: item[key] for key in ("NAME", "TYPE", "SEQNR", "COLUMNS")}
                    for item in reader.getdminfo().values()]
        managers = bindings()
        assert list(reader.getcol("COMMAND")) == ["field='FLUX' autocorr=False mode='manual'"]
        tasks.flagdata(**options, field="PHASE")
        reader.resync()
        assert bindings() == managers
        assert list(reader.getcol("COMMAND")) == [
            "field='FLUX' autocorr=False mode='manual'", "field='PHASE' autocorr=False mode='manual'",
        ]


def check_output_paths():
    empty = Path("empty-directory.ms")
    empty.mkdir()
    options = {"savepars": True, "action": "none"}
    for name, task in TASKS:
        path = Path(f"invalid-source-{name}.txt")
        for vis, expected in (("missing.ms", AssertionError), (str(empty), ValueError)):
            try:
                task(**options, vis=vis, outfile=str(path))
            except expected:
                pass
            else:
                raise AssertionError((name, vis))
            assert not path.exists()
        outfile = f"missing-parent/{name}.txt"
        try:
            task(**options, vis="source.ms", outfile=outfile)
        except FileNotFoundError as failure:
            assert failure.filename == outfile
        else:
            raise AssertionError((name, outfile))


def main():
    assert casatasks.version() == [6, 7, 6, 14]
    with TemporaryDirectory(prefix="flagdata-savepars-") as directory, chdir(directory):
        make_cal_ms("source.ms", channels=4, scan_seconds=20)
        original = snapshot("source.ms")
        commands = compare_table_commands(original)
        compare_ascii_commands(original)
        occupied = compare_occupied_file(original)
        compare_original_arguments()
        check_reader_resync()
        check_output_paths()
    print(f"{commands} matched FLAG_CMD rows, two command files, ignored summary output and "
          f"{occupied} existing-file failures verified against CASA")


if __name__ == "__main__":
    main()
