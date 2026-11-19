#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare flagdata field, baseline and threshold summaries with CASA."""

from contextlib import chdir
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_summary_oracle import assert_unchanged, seed_flags, snapshot
from record_casa_returns import make_cal_ms


def compare(path, **options):
    arguments = {"vis": path, "mode": "summary", **options}
    before = snapshot(path)
    expected = casatasks.flagdata(**arguments)
    actual = tasks.flagdata(**arguments)
    assert actual == expected, (options, expected, actual)
    assert list(actual) == list(expected), options
    assert_unchanged(path, before)
    return actual


def seed_baselines(path):
    seed_flags(path)
    with table(path, readonly=False, ack=False) as storage:
        first, second = (storage.getcell(key, 0) for key in ("ANTENNA1", "ANTENNA2"))
        storage.putcell("ANTENNA1", 0, second)
        storage.putcell("ANTENNA2", 0, first)
        storage.putcell("ANTENNA2", 1, storage.getcell("ANTENNA1", 1))
        for row in range(storage.nrows()):
            if storage.getcell("FIELD_ID", row) == 1:
                flags = storage.getcell("FLAG", row)
                flags[0] = True
                storage.putcell("FLAG", row, flags)


def set_field_names(path, names):
    with table(f"{path}/FIELD", readonly=False, ack=False) as fields:
        for row, name in enumerate(names):
            fields.putcell("NAME", row, name)


def compare_rejections(path):
    before = snapshot(path)
    rejected = (
        {"basecnt": 1},
        {"basecnt": None},
        {"fieldcnt": [True]},
        {"minrel": np.array(0.1)},
        {"maxrel": None},
        {"maxrel": "0.5"},
        {"minabs": 1.0},
        {"maxabs": np.uint32(2)},
        {"maxabs": np.array(2)},
    )
    for options in rejected:
        messages = []
        for task in (casatasks.flagdata, tasks.flagdata):
            try:
                task(vis=path, mode="summary", **options)
            except AssertionError as error:
                messages.append(str(error))
            else:
                raise AssertionError(f"summary unexpectedly accepted {options!r}")
        assert messages[0] == messages[1], (options, messages)
    assert_unchanged(path, before)


def compare_reserved_field(path, reserved):
    set_field_names(path, (reserved, "other"))
    before = snapshot(path)
    messages = []
    for task in (casatasks.flagdata, tasks.flagdata):
        try:
            task(vis=path, mode="summary", fieldcnt=True)
        except RuntimeError as error:
            completion = getattr(error, "completion", None)
            messages.append(completion.message if completion else str(error))
        else:
            raise AssertionError(f"summary unexpectedly accepted the field {reserved!r}")
    assert messages[1].endswith(messages[0]), messages
    assert "incorrect data type used for field" in messages[0], messages
    assert_unchanged(path, before)


def main():
    assert tuple(casatasks.version()) == (6, 7, 6, 14)
    with TemporaryDirectory(prefix="flagdata-summary-filters-") as work, chdir(work):
        path = "input.ms"
        make_cal_ms(path, channels=4, scan_seconds=20)
        seed_baselines(path)
        report = compare(path, basecnt=True)
        assert {"A0&&A0", "A0&&A1", "A1&&A0"} <= report["baseline"].keys()
        assert sum(value["total"] for value in report["baseline"].values()) == report["total"]

        detailed = {"basecnt": True, "spwchan": True, "spwcorr": True}
        for options in (
            {"fieldcnt": True},
            {"fieldcnt": True, "field": "1"},
            {**detailed, "fieldcnt": True, "spw": "0:1~2", "name": "per field"},
            {**detailed, "minabs": 3, "maxabs": 5},
            {"basecnt": True, "minrel": 0.04, "maxrel": 0.08},
            {"basecnt": True, "minrel": 0.0625, "maxrel": 0.0625},
            {"minabs": -1, "maxabs": -2},
            {**detailed, "maxabs": 0},
            {"minrel": 0.8, "maxrel": 0.2},
            {"minrel": float("nan"), "maxrel": float("nan")},
            {"minrel": float("inf")},
            {"minrel": -float("inf"), "maxrel": float("inf")},
            {"minrel": np.float32(0.1), "maxrel": np.float64(0.6)},
            {"minrel": True, "maxrel": np.int64(1)},
            {"minabs": np.int32(2), "maxabs": np.int64(11)},
            {"minabs": 1 << 100},
            {"maxabs": -(1 << 100)},
            {"basecnt": np.bool_(True), "fieldcnt": np.bool_(True)},
        ):
            compare(path, **options)
        fields = compare(path, **detailed, fieldcnt=True, minabs=20)
        assert set(fields) == {"PHASE", "name", "type"}
        assert fields["PHASE"]["spw:channel"]["0:1"]["flagged"] < 20
        assert set(compare(path, fieldcnt=True, minabs=1 << 100)) == {"name", "type"}
        set_field_names(path, ("zeta", "alpha"))
        compare(path, fieldcnt=True)
        set_field_names(path, ("same", "same"))
        duplicate = compare(path, fieldcnt=True)
        assert duplicate["same"]["total"] == report["total"]
        compare_rejections(path)
        for reserved in ("type", "name"):
            compare_reserved_field(path, reserved)
    print("Twenty-three filtered or grouped summaries and eleven rejections matched CASA 6.7.6.14")


if __name__ == "__main__":
    main()
