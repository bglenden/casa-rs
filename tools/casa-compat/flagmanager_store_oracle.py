#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare flag-version catalog initialization and restore merges with CASA."""

from contextlib import chdir
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
import numpy as np

from casars import tasks
from flagmanager_rename_oracle import assert_flags, set_flags, versions
from record_casa_returns import make_cal_ms


def check_catalog(task, prefix):
    cases = (
        ("list", {}, None, None, True),
        ("save", {}, ValueError, "Illegal empty versionname: ''", False),
        ("restore", {}, ValueError, "Illegal versionname: ''", False),
        ("delete", {}, ValueError, "Illegal versionname: ''", False),
        ("restore", {"versionname": "missing"}, RuntimeError, None, True),
        ("delete", {"versionname": "missing"}, None, None, True),
        ("rename", {"oldname": "missing", "versionname": "new"}, ValueError,
         "No such flagversions: missing", False),
    )
    for index, (mode, options, expected_error, message, creates_catalog) in enumerate(cases):
        vis = Path(f"{prefix}-{index}.ms")
        make_cal_ms(str(vis), channels=4, scan_seconds=20)
        initial_flags = set_flags(vis, 0)
        sidecar = Path(f"{vis}.flagversions")
        assert not sidecar.exists()
        try:
            result = task(vis=str(vis), mode=mode, **options)
        except Exception as error:
            assert expected_error and isinstance(error, expected_error), (mode, error)
            if message:
                completion = getattr(error, "completion", None)
                assert (completion.message if completion else str(error)) == message, mode
        else:
            assert expected_error is None, mode
            if mode == "list":
                assert set(result) == {"MS"}
                assert Path(result["MS"]).resolve() == vis.resolve()
            else:
                assert result is None
        assert sidecar.exists() == creates_catalog, mode
        if creates_catalog:
            catalog = sidecar / "FLAG_VERSION_LIST"
            assert set(sidecar.iterdir()) == {catalog}
            assert catalog.read_bytes() == b""
        assert_flags(vis, initial_flags)


def check_merges(task, vis):
    make_cal_ms(str(vis), channels=4, scan_seconds=20)
    saved = set_flags(vis, 0, shared_row=4)
    assert task(vis=str(vis), mode="save", versionname="first", comment="retained") is None
    sidecar = Path(f"{vis}.flagversions")
    catalog = sidecar / "FLAG_VERSION_LIST"
    initial_catalog = catalog.read_bytes()
    for merge in ("replace", "or", "and", "", "OR", "unrecognized", "$literal"):
        working = set_flags(vis, 1, shared_row=4)
        assert task(vis=str(vis), mode="restore", versionname="first", merge=merge) is None
        if merge == "or":
            expected = tuple(np.logical_or(a, b) for a, b in zip(saved, working, strict=True))
        elif merge == "and":
            expected = tuple(np.logical_and(a, b) for a, b in zip(saved, working, strict=True))
        else:
            expected = saved
        assert_flags(vis, expected)
        assert_flags(sidecar / "flags.first", saved)
        assert catalog.read_bytes() == initial_catalog
        assert versions(task, vis) == {0: {"name": "first", "comment": "retained"}}
    assert task(vis=str(vis), mode="save", versionname="copy", merge="unrecognized") is None
    assert_flags(sidecar / "flags.copy", saved)


def main():
    assert tuple(casatasks.version()) == (6, 7, 6, 14)
    with TemporaryDirectory(prefix="flagmanager-store-") as work, chdir(work):
        for task, prefix in ((casatasks.flagmanager, "reference"), (tasks.flagmanager, "native")):
            check_catalog(task, prefix)
            check_merges(task, Path(f"{prefix}-merge.ms"))
    print("Seven flag-version catalog cases, seven restore choices and fallback save checked against CASA 6.7.6.14")


if __name__ == "__main__":
    main()
