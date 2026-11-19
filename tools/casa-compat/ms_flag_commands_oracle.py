#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare copied FLAG_CMD selectors after channel selection and averaging."""

from pathlib import Path
from shutil import copytree
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from ms_channel_average_oracle import channel_parameters
from ms_selection_tasks_oracle import add_second_window, casa_table, make_input

COMMANDS = (
    "mode='manual' spw='1:0~3' reason='numeric'",
    "mode='manual' spw='0:1;3' reason='disjoint'",
    "mode='manual' spw='1:0' reason='excluded-channel'",
    "mode='manual' spw='0' reason='window'",
    "mode='manual' spw='*' reason='all'",
    "mode='manual' spw = 1:2 reason='spacing'",
    'mode=\'manual\' spw="SPW1:1~3" reason=\'named-command\'',
    "mode='manual' antenna='A1' reason='no-window'",
    "mode='manual' spw='1:2~5' reason='limit'",
    "mode='manual' spw='0:2,1:0' reason='some-overlap'",
    "mode='manual' spw='0:2,1:2' reason='two-windows'",
    "mode='manual' spw='0:2,99:0' reason='unknown-window'",
    "mode='manual' spw='0:2~3' reason='inactive-averaging-width'",
)


def add_commands(vis, commands):
    with table(str(vis / "FLAG_CMD"), readonly=False, ack=False) as flags:
        flags.addrows(len(commands))
        for row, command in enumerate(commands):
            values = dict(APPLIED=bool(row % 2), COMMAND=command, INTERVAL=0.0, LEVEL=row,
                          REASON="stable metadata", SEVERITY=1, TIME=5e9, TYPE="FLAG")
            for name, value in values.items():
                flags.putcell(name, row, value)


def compare(case, paths, original, *, check_truncation=False):
    with (table(str(paths[0]), ack=False) as want, table(str(paths[1]), ack=False) as got,
          casa_table(paths[1]) as reopen):
        assert len(original) == want.nrows() == got.nrows(), case
        assert set(got.colnames()) == set(want.colnames()), case
        for name in want.colnames():
            actual = got.getcol(name)
            expected = want.getcol(name)
            if name == "COMMAND" and check_truncation:
                desired = "mode='manual' spw='0:-2~-1' reason='inactive-averaging-width'"
                assert expected[-1] == desired[:max(map(len, original))]
                assert actual[-1] == desired
                np.testing.assert_array_equal(actual[:-1], expected[:-1], err_msg=f"{case} {name}")
            else:
                np.testing.assert_array_equal(actual, expected, err_msg=f"{case} {name}")
            np.testing.assert_array_equal(actual, reopen.getcol(name), err_msg=f"{case} {name} CASA reopen")


def main():
    assert tuple(casatasks.version()) == (6, 7, 6, 14)
    with TemporaryDirectory(prefix="ms-flag-commands-") as work:
        root = Path(work)
        source = root / "source.ms"
        make_input(source, channels=4)
        add_second_window(source)
        total = 0
        for first, position in (("numeric", 0), ("named", 6), ("no-window", 7)):
            base = root / f"{first}.ms"
            copytree(source, base)
            commands = list(COMMANDS)
            commands[0], commands[position] = commands[position], commands[0]
            add_commands(base, commands)
            for name, selection, width in (
                ("full", "", 1), ("only-window", "1", 1), ("partial", "1:1~3", 1),
                ("named", "SPW1:1~3", 1), ("averaged", "", 2),
                ("partial-avg", "0:1~3,1:1~3", 2), ("reordered", "1:1~3,0:1~3", 1),
            ):
                for task in ("split", "mstransform"):
                    case = f"{task}-{first}-{name}"
                    paths = [root / f"{case}-{side}.ms" for side in ("casa", "native")]
                    params = channel_parameters(task, width, average=width > 1)
                    for module, target in zip((casatasks, tasks), paths):
                        result = getattr(module, task)(vis=str(base), outputvis=str(target),
                                                       spw=selection, datacolumn="data", **params)
                        assert result is None, (case, module.__name__, result)
                    compare(case, [path / "FLAG_CMD" for path in paths], commands)
                    total += 1
            if first == "numeric":
                for task, options in (
                    ("mstransform", {"chanaverage": False, "chanbin": 2}),
                    ("mstransform", {"chanaverage": False, "chanbin": 0}),
                    ("mstransform", {"chanaverage": False, "chanbin": -2}),
                    ("split", {"width": [0, 1]}),
                ):
                    case = f"{task}-inactive-width-{total}"
                    paths = [root / f"{case}-{side}.ms" for side in ("casa", "native")]
                    for module, target in zip((casatasks, tasks), paths):
                        getattr(module, task)(vis=str(base), outputvis=str(target), spw="0~1",
                                              datacolumn="data", **options)
                    compare(case, [path / "FLAG_CMD" for path in paths], commands,
                            check_truncation=options.get("chanbin") == -2)
                    total += 1
            with table(str(base / "FLAG_CMD"), ack=False) as flags:
                np.testing.assert_array_equal(flags.getcol("COMMAND"), commands)
        print(f"{total} FLAG_CMD products compared with CASA 6.7.6.14")


if __name__ == "__main__":
    main()
