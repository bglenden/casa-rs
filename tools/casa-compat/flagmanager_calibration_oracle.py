#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Exercise calibration flag versions and bidirectional CASA interchange."""

import argparse
from contextlib import chdir
import os
from pathlib import Path
import shutil

import casatasks as casa
import numpy as np

from calibration_smoothcal_oracle import opened
from flagmanager_rename_oracle import versions
from record_casa_returns import make_cal_ms


def original_columns(path):
    with opened(path) as table:
        return (table.info(), table.getkeywords(), tuple(table.colnames()),
                {name: [table.getcell(name, row) if table.iscelldefined(name, row) else None
                        for row in range(table.nrows())]
                 for name in table.colnames() if name != "FLAG"})


def preserved(path, original):
    actual = original_columns(path)
    assert actual[:3] == original[:3], (path, actual[:3], original[:3])
    for name, expected in original[3].items():
        for cell, reference in zip(actual[3][name], expected, strict=True):
            np.testing.assert_array_equal(cell, reference, err_msg=name)


def set_flags(path, row):
    with opened(path, True) as table:
        flags = np.zeros_like(table.getcol("FLAG"))
        flags[:, :, row] = True
        flags[0, 0, row + 2] = True
        flags[-1, -1, 4] = True
        table.putcol("FLAG", flags)
    return flags


def assert_flags(path, expected, *, version=False):
    with opened(path) as table:
        assert "FLAG_ROW" not in table.colnames(), path
        if version:
            assert table.colnames() == ["FLAG"], table.colnames()
        np.testing.assert_array_equal(table.getcol("FLAG"), expected, err_msg=str(path))


def exercise(path, writer, peer):
    sidecar = Path(f"{path}.flagversions")
    before = original_columns(path)
    assert versions(writer, path) == {}
    assert (sidecar / "FLAG_VERSION_LIST").read_bytes() == b""
    initial = set_flags(path, 0)
    assert writer(path.as_posix(), "save", versionname="initial", comment="captured") is None
    snapshot = sidecar / "flags.initial"
    assert_flags(snapshot, initial, version=True)
    assert (sidecar / "FLAG_VERSION_LIST").read_text() == "initial : captured\n"
    assert versions(peer, path) == {0: {"name": "initial", "comment": "captured"}}

    merges = {"replace": lambda source, _: source, "or": np.logical_or, "and": np.logical_and}
    for restorer in (writer, peer):
        for merge, combine in merges.items():
            current = set_flags(path, 1)
            assert restorer(path.as_posix(), "restore", versionname="initial", merge=merge) is None
            assert_flags(path, combine(initial, current))
            assert_flags(snapshot, initial, version=True)
            preserved(path, before)

    current = set_flags(path, 1)
    try:
        writer(path.as_posix(), "restore", versionname="missing")
    except RuntimeError as error:
        assert str(error) == "Flag version missing does not exist"
    else:
        raise AssertionError("missing version was restored")
    assert_flags(path, current)
    assert writer(path.as_posix(), "rename", oldname="initial", versionname="renamed",
                  comment="updated") is None
    assert not snapshot.exists()
    assert versions(peer, path) == {0: {"name": "renamed", "comment": "updated"}}
    assert peer(path.as_posix(), "restore", versionname="renamed") is None
    assert_flags(path, initial)
    assert writer(path.as_posix(), "delete", versionname="renamed") is None
    assert versions(peer, path) == {}
    assert {entry.name for entry in sidecar.iterdir()} == {"FLAG_VERSION_LIST"}
    preserved(path, before)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir()
    os.environ["CASARS_FLAGMANAGER_BIN"] = str(args.native_bin.resolve())
    from casars.tasks import flagmanager as native

    assert casa.version_string() == "6.7.6.14"
    with chdir(root):
        make_cal_ms("source.ms", channels=8, scan_seconds=40)
        casa.bandpass("source.ms", "bandpass.cal", refant="A0", minsnr=0.0)
        casa.gencal("source.ms", "delay.cal", "sbd", parameter=[2.5])
        for name in ("bandpass", "delay"):
            for label, writer, reader in (("casa", casa.flagmanager, native),
                                         ("native", native, casa.flagmanager)):
                path = Path(f"{label}-{name}.cal")
                shutil.copytree(f"{name}.cal", path)
                exercise(path.resolve(), writer, reader)
    print("Four calibration version workflows, 24 merge restores and four renamed restores. "
          "Both directions read FLAG-only versions and preserved all other calibration columns.")


if __name__ == "__main__":
    main()
