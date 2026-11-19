#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare public flagdata previews and apply write control with CASA."""

from contextlib import chdir
from hashlib import sha256
from json import loads
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import table
import numpy as np

from casars import tasks
from flagdata_summary_oracle import seed_flags, snapshot
from record_casa_returns import make_cal_ms


def make_fixture(path):
    make_cal_ms(path, channels=4, scan_seconds=20)
    seed_flags(path)
    with table(path, readonly=False, ack=False) as storage:
        data = storage.getcell("DATA", 2)
        data[1, 0] = 0
        storage.putcell("DATA", 2, data)


def auxiliary_content(path):
    roots = [Path(path, "HISTORY"), Path(path, "FLAG_CMD"), Path(f"{path}.flagversions")]
    files = {
        entry: sha256(entry.read_bytes()).digest()
        for root in roots
        for entry in root.rglob("*")
        if entry.is_file() and entry.name != "table.lock"
    }
    return tuple(root.is_dir() for root in roots), files


def assert_preserved(path, original, auxiliary):
    for before, after in zip(original, snapshot(path), strict=True):
        for expected, actual in zip(before, after, strict=True):
            np.testing.assert_array_equal(actual, expected)
    assert auxiliary_content(path) == auxiliary


def changed_samples(original, final):
    return [
        int(np.count_nonzero(before[1] != after[1]))
        for before, after in zip(original, final, strict=True)
    ]


def check_candidates():
    cases = (
        {"mode": "manual", "writeflags": True},
        {"mode": "manual", "writeflags": False},
        {"mode": "unflag"},
        {"mode": "clip", "clipzeros": True},
        {"mode": "quack", "quackinterval": 5.0},
    )
    for index, options in enumerate(cases):
        reference, native = f"reference-{index}.ms", f"native-{index}.ms"
        for path in (reference, native):
            make_fixture(path)
            if index == 1:
                casatasks.flagmanager(vis=path, mode="save", versionname="retained")
        before_reference, before_native = snapshot(reference), snapshot(native)
        reference_auxiliary, native_auxiliary = auxiliary_content(reference), auxiliary_content(native)
        common = {"spw": "0:1~2", **options}
        expected = casatasks.flagdata(vis=reference, action="calculate", flagbackup=True, **common)
        assert expected == {}
        assert tasks.flagdata(vis=native, action="calculate", flagbackup=True, **common) == expected
        assert_preserved(reference, before_reference, reference_auxiliary)
        assert_preserved(native, before_native, native_auxiliary)

        # CASA's applied product is an independent check of the candidate report.
        casatasks.flagdata(vis=reference, action="apply", flagbackup=False, **common)
        changes = changed_samples(before_reference, snapshot(reference))
        assert sum(changes) > 0, options
        completed = tasks.run("flagdata", vis=native, action="calculate", flagbackup=True, **common)
        assert completed.value == expected
        report = loads(completed.stdout)["result"]
        assert report["changed_samples"] == sum(changes), (options, report)
        assert report["changed_rows"] == sum(delta != 0 for delta in changes)
        assert report["backup_version"] is None
        assert_preserved(native, before_native, native_auxiliary)


def check_summary_preview():
    vis = "summary.ms"
    make_fixture(vis)
    before, auxiliary = snapshot(vis), auxiliary_content(vis)
    for action in ("calculate", "", "none"):
        arguments = {"vis": vis, "mode": "summary", "action": action, "spwchan": True}
        expected = casatasks.flagdata(**arguments)
        assert tasks.flagdata(**arguments) == expected
        assert_preserved(vis, before, auxiliary)
    for value in (None, 1, "false"):
        messages = []
        for task in (casatasks.flagdata, tasks.flagdata):
            try:
                task(vis=vis, mode="summary", action="calculate", writeflags=value)
            except AssertionError as error:
                messages.append(str(error))
            else:
                raise AssertionError(f"flagdata unexpectedly accepted writeflags={value!r}")
        assert messages[0] == messages[1]
        assert_preserved(vis, before, auxiliary)


def check_apply_writeflags():
    reference, native = "apply-reference.ms", "apply-native.ms"
    for path in (reference, native):
        make_fixture(path)
    before = snapshot(reference)
    options = {
        "mode": "manual", "action": "apply", "writeflags": False,
        "spw": "0:1~2", "flagbackup": True,
    }
    assert tasks.flagdata(vis=native, **options) == casatasks.flagdata(vis=reference, **options)
    expected = snapshot(reference)
    assert sum(changed_samples(before, expected)) > 0
    for casa, materialized in zip(expected, snapshot(native), strict=True):
        for wanted, actual in zip(casa, materialized, strict=True):
            np.testing.assert_array_equal(actual, wanted)
    for path in (reference, native):
        sidecar = Path(f"{path}.flagversions")
        versions = list(sidecar.glob("flags.*"))
        assert [version.name for version in versions] == ["flags.flagdata_1"]
        with table(str(versions[0]), ack=False) as backup:
            for row, (flag_row, flags, _) in enumerate(before):
                np.testing.assert_array_equal(backup.getcell("FLAG_ROW", row), flag_row)
                np.testing.assert_array_equal(backup.getcell("FLAG", row), flags)


def main():
    assert tuple(casatasks.version()) == (6, 7, 6, 14)
    with TemporaryDirectory(prefix="flagdata-preview-") as directory, chdir(directory):
        check_candidates()
        check_summary_preview()
        check_apply_writeflags()
    print("Nine flagdata preview and write-control cases and three rejections matched CASA 6.7.6.14")


if __name__ == "__main__":
    main()
