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
        ("restore", {"versionname": "missing"}, RuntimeError, "Flag version missing does not exist", True),
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
            assert type(error) is expected_error, (mode, error)
            if message:
                assert str(error) == message, mode
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
    for merge in ("replace", "or", "and", "", "OR", "unrecognized"):
        working = set_flags(vis, 1, shared_row=4)
        if task is tasks.flagmanager and merge not in ("replace", "or", "and"):
            try:
                task(vis=str(vis), mode="restore", versionname="first", merge=merge)
            except ValueError:
                pass
            else:
                raise AssertionError(f"restore accepted an unknown merge choice {merge!r}")
            expected = working
        else:
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


def check_literal_lookup(task, vis):
    make_cal_ms(str(vis), channels=4, scan_seconds=20)
    sidecar = Path(f"{vis}.flagversions")
    catalog = sidecar / "FLAG_VERSION_LIST"
    saved = set_flags(vis, 0, shared_row=4)
    for name in ("seed", "none", "auto"):
        assert task(vis=str(vis), mode="save", versionname=name) is None
        assert_flags(sidecar / f"flags.{name}", saved)
    for name in ("none", "auto"):
        set_flags(vis, 1, shared_row=4)
        assert task(vis=str(vis), mode="restore", versionname=name) is None
        assert_flags(vis, saved)
    assert task(vis=str(vis), mode="rename", oldname="none", versionname="renamed") is None
    assert_flags(sidecar / "flags.renamed", saved)

    working = set_flags(vis, 1, shared_row=4)
    for name in (" seed ", "seed"):
        if name == "seed":
            # An orphaned on-disk version is not a member of the catalog.
            catalog.write_bytes(b"")
        before = catalog.read_bytes()
        try:
            task(vis=str(vis), mode="restore", versionname=name)
        except RuntimeError as error:
            assert type(error) is RuntimeError
            assert str(error) == f"Flag version {name} does not exist"
        else:
            raise AssertionError(name)
        assert_flags(vis, working)
        assert_flags(sidecar / "flags.seed", saved)
        assert catalog.read_bytes() == before


def main():
    assert tuple(casatasks.version()) == (6, 7, 6, 14)
    with TemporaryDirectory(prefix="flagmanager-store-") as work, chdir(work):
        for task, prefix in ((casatasks.flagmanager, "reference"), (tasks.flagmanager, "native")):
            check_catalog(task, prefix)
            check_merges(task, Path(f"{prefix}-merge.ms"))
            check_literal_lookup(task, Path(f"{prefix}-lookup.ms"))
    print("Catalog initialization, six restore choices, and literal version lookup verified with CASA 6.7.6.14")


if __name__ == "__main__":
    main()
