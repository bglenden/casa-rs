#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the public flagmanager rename workflow with installed CASA."""

from contextlib import chdir
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from record_casa_returns import make_cal_ms


REJECTIONS = (
    ({"oldname": "missing", "versionname": "second"}, "No such flagversions: missing"),
    ({"oldname": "first", "versionname": "second"}, "Flagversions second already exists!"),
    ({"oldname": "first", "versionname": "first"}, "Flagversions first already exists!"),
    ({"oldname": "missing", "versionname": "missing"}, "No such flagversions: missing"),
    ({"oldname": "first"}, "Illegal versionname: ''"),
    ({"versionname": "third"}, "Illegal oldname: ''"),
    ({}, "Illegal versionname: ''"),
)


def read_flags(path):
    with table(str(path), ack=False) as storage:
        return storage.getcol("FLAG"), storage.getcol("FLAG_ROW")


def set_flags(path, row):
    with table(str(path), readonly=False, ack=False) as storage:
        flags = np.zeros_like(storage.getcol("FLAG"))
        flag_rows = np.zeros_like(storage.getcol("FLAG_ROW"))
        flags[row] = True
        flag_rows[row] = True
        flags[row + 2, 0, 0] = True
        storage.putcol("FLAG", flags)
        storage.putcol("FLAG_ROW", flag_rows)
    return flags, flag_rows


def assert_flags(path, expected):
    for actual, original in zip(read_flags(path), expected, strict=True):
        np.testing.assert_array_equal(actual, original)


def versions(task, vis):
    listed = task(vis=str(vis), mode="list")
    assert Path(listed.pop("MS")).resolve() == vis.resolve()
    return listed


def exercise(task, vis):
    make_cal_ms(str(vis), channels=4, scan_seconds=20)
    with table(str(vis), ack=False) as storage:
        data = storage.getcol("DATA")
    first_flags = set_flags(vis, 0)
    assert task(vis=str(vis), mode="save", versionname="first", comment="original") is None
    second_flags = set_flags(vis, 1)
    assert task(vis=str(vis), mode="save", versionname="second", comment="current") is None

    sidecar = Path(f"{vis}.flagversions")
    catalog = sidecar / "FLAG_VERSION_LIST"
    original_catalog = catalog.read_bytes()
    original_versions = versions(task, vis)
    for arguments, expected in REJECTIONS:
        try:
            task(vis=str(vis), mode="rename", comment="must not replace a comment", **arguments)
        except ValueError as error:
            completion = getattr(error, "completion", None)
            assert (completion.message if completion else str(error)) == expected, arguments
        else:
            raise AssertionError(f"rename unexpectedly succeeded with {arguments!r}")
        assert catalog.read_bytes() == original_catalog
        assert versions(task, vis) == original_versions
        assert not (sidecar / "flags.third").exists()
        assert_flags(vis, second_flags)
        assert_flags(sidecar / "flags.first", first_flags)
        assert_flags(sidecar / "flags.second", second_flags)

    assert task(
        vis=str(vis), mode="rename", oldname="first", versionname="third", comment="updated"
    ) is None
    assert not (sidecar / "flags.first").exists()
    listed = versions(task, vis)
    assert listed == {
        0: {"name": "third", "comment": "updated"},
        1: {"name": "second", "comment": "current"},
    }
    assert_flags(vis, second_flags)
    assert_flags(sidecar / "flags.third", first_flags)
    assert_flags(sidecar / "flags.second", second_flags)
    assert task(vis=str(vis), mode="restore", versionname="third") is None
    assert_flags(vis, first_flags)
    with table(str(vis), ack=False) as storage:
        np.testing.assert_array_equal(storage.getcol("DATA"), data)


def main():
    assert tuple(casatasks.version()) == (6, 7, 6, 14)
    with TemporaryDirectory(prefix="flagmanager-rename-") as work, chdir(work):
        exercise(casatasks.flagmanager, Path("casa.ms"))
        exercise(tasks.flagmanager, Path("native.ms"))
    print("Rename, restore and seven unchanged rejection cases matched CASA 6.7.6.14")


if __name__ == "__main__":
    main()
