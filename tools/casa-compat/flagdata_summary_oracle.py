#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare named flagdata summaries and optional SPW breakdowns with CASA."""

from contextlib import chdir
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from record_casa_returns import make_cal_ms


def seed_flags(path):
    with table(path, readonly=False, ack=False) as storage:
        for row in range(storage.nrows()):
            flags = storage.getcell("FLAG", row)
            flags[:] = False
            flags[1, 0] = row % 3 == 0
            flags[-1, 1] = row % 5 == 1
            storage.putcell("FLAG", row, flags)
            storage.putcell("FLAG_ROW", row, False)


def snapshot(path):
    with table(path, ack=False) as storage:
        return [
            (storage.getcell("FLAG_ROW", row), storage.getcell("FLAG", row),
             storage.getcell("DATA", row))
            for row in range(storage.nrows())
        ]


def assert_unchanged(path, before):
    assert not Path(f"{path}.flagversions").exists()
    for original, current in zip(before, snapshot(path), strict=True):
        for wanted, got in zip(original, current, strict=True):
            np.testing.assert_array_equal(got, wanted)


def compare(path, cases):
    before = snapshot(path)
    for options in cases:
        args = {"vis": path, "mode": "summary", **options}
        expected = casatasks.flagdata(**args)
        actual = tasks.flagdata(**args)
        assert actual == expected, (options, expected, actual)
        assert list(actual) == list(expected), options
        if "spw:channel" in actual:
            assert sum(value["flagged"] for value in actual["spw:channel"].values()) == actual["flagged"]
            assert sum(value["total"] for value in actual["spw:channel"].values()) == actual["total"]
        if "spw:correlation" in actual:
            assert sum(value["flagged"] for value in actual["spw:correlation"].values()) == actual["flagged"]
            assert sum(value["total"] for value in actual["spw:correlation"].values()) == actual["total"]
        assert_unchanged(path, before)


def compare_rejections(path):
    before = snapshot(path)
    invalid = (
        {"name": None},
        {"name": 5},
        {"name": b"bytes"},
        {"name": np.array("text")},
        {"name": ["one"]},
        {"spwchan": None},
        {"spwchan": 1},
        {"spwchan": np.array(True)},
        {"spwcorr": "true"},
        {"spwcorr": [True]},
        {"name": 5, "spwchan": 1},
    )
    for options in invalid:
        messages = []
        for task in (casatasks.flagdata, tasks.flagdata):
            try:
                task(vis=path, mode="summary", **options)
            except AssertionError as error:
                messages.append(str(error))
            else:
                raise AssertionError(f"flagdata accepted {options!r}")
        assert messages[0] == messages[1], (options, messages)
    assert_unchanged(path, before)


def main():
    assert tuple(casatasks.version()) == (6, 7, 6, 14)
    with TemporaryDirectory(prefix="flagdata-summary-") as work, chdir(work):
        make_cal_ms("linear.ms", channels=4, scan_seconds=20, frequency="100GHz")
        make_cal_ms(
            "circular.ms", channels=6, scan_seconds=20, frequency="101GHz", stokes="RR LL"
        )
        assert casatasks.concat(vis=["linear.ms", "circular.ms"], concatvis="mixed.ms") is None
        seed_flags("linear.ms")
        seed_flags("mixed.ms")
        compare("linear.ms", (
            {},
            {"name": "preflight"},
            {"name": ""},
            {"name": "  audit  "},
            {"name": "none"},
            {"name": np.str_("numpy summary")},
            {"name": "per-channel", "spwchan": True},
            {"name": "per-correlation", "spwcorr": True},
            {"spwchan": np.bool_(True), "spwcorr": False},
            {"spwchan": True, "spwcorr": np.bool_(True)},
        ))
        both = {"spwchan": True, "spwcorr": True}
        compare("mixed.ms", (
            both,
            {**both, "spw": "1:4~5", "name": "window suffix"},
            {**both, "spw": "0:1~2,1:3~5"},
        ))
        expected = casatasks.flagdata(
            vis="mixed.ms", mode="summary", spw="1:4~5", **both
        )
        assert set(expected["spw:channel"]) == {"1:0", "1:1"}
        assert set(expected["spw:correlation"]) == {"1:LL", "1:RR"}
        compare_rejections("mixed.ms")
    print("Thirteen flagdata summaries and eleven rejections matched CASA 6.7.6.14")


if __name__ == "__main__":
    main()
