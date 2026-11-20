#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Check flagmanager against CASA when opening a MeasurementSet by alias."""

from contextlib import chdir
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks

from casars import tasks
from flagmanager_rename_oracle import assert_flags, set_flags, versions
from record_casa_returns import make_cal_ms


def make_alias(path):
    path.mkdir()
    physical = path / "physical.ms"
    alias = path / "alias.ms"
    make_cal_ms(str(physical), channels=4, scan_seconds=20)
    alias.symlink_to(physical.name, target_is_directory=True)
    return physical, alias


def check_shared_versions(task, directory):
    physical, alias = make_alias(directory)
    saved = set_flags(physical, 0)
    assert task(vis=str(physical), mode="save", versionname="seed", comment="original") is None
    working = set_flags(physical, 1)

    listed = task(vis=str(alias), mode="list")
    assert Path(listed.pop("MS")) == alias.absolute()
    assert listed == {0: {"name": "seed", "comment": "original"}}
    assert task(vis=str(alias), mode="restore", versionname="seed") is None
    assert_flags(physical, saved)
    try:
        task(vis=str(alias), mode="rename", oldname="seed", versionname="moved")
    except ValueError as error:
        completion = getattr(error, "completion", None)
        assert (completion.message if completion else str(error)) == "No such flagversions: seed"
    else:
        raise AssertionError("rename unexpectedly changed the target's flag catalog")

    set_flags(physical, 1)
    assert task(vis=str(alias), mode="save", versionname="new", comment="via alias") is None
    assert task(vis=str(alias), mode="delete", versionname="seed") is None
    assert versions(task, physical) == {0: {"name": "new", "comment": "via alias"}}
    assert_flags(Path(f"{physical}.flagversions") / "flags.new", working)
    assert_flags(physical, working)
    assert not Path(f"{alias}.flagversions").exists()


def check_repeated_save(task, directory, *, preserve_archived):
    physical, alias = make_alias(directory)
    saved = set_flags(physical, 0)
    assert task(vis=str(physical), mode="save", versionname="seed", comment="original") is None
    working = set_flags(physical, 1)
    assert task(vis=str(alias), mode="save", versionname="seed", comment="new") is None
    entries = list(versions(task, physical).values())
    sidecar = Path(f"{physical}.flagversions")
    assert entries[-1] == {"name": "seed", "comment": "new"}
    assert_flags(sidecar / "flags.seed", working)
    assert not Path(f"{alias}.flagversions").exists()
    if preserve_archived:
        archive, _ = entries
        assert archive["name"].startswith("seed.old.")
        assert archive["name"].removeprefix("seed.old.").isdigit()
        assert archive["comment"] == "original"
        assert_flags(sidecar / f"flags.{archive['name']}", saved)
        assert task(vis=str(physical), mode="restore", versionname=archive["name"]) is None
        assert_flags(physical, saved)
    else:
        assert sorted(path.name for path in sidecar.glob("flags.*")) == ["flags.seed"]
        assert len(entries) == 1


def check_legacy_alias(directory):
    task = tasks.flagmanager
    physical, alias = make_alias(directory)
    saved = set_flags(physical, 0)
    assert task(vis=str(physical), mode="save", versionname="legacy") is None
    sidecar = Path(f"{physical}.flagversions")
    legacy_sidecar = Path(f"{alias}.flagversions")
    sidecar.rename(legacy_sidecar)
    working = set_flags(physical, 1)
    assert task(vis=str(physical), mode="save", versionname="physical") is None
    assert versions(task, alias) == {0: {"name": "legacy", "comment": ""}}
    assert task(vis=str(alias), mode="restore", versionname="legacy") is None
    assert_flags(physical, saved)
    assert_flags(sidecar / "flags.physical", working)


def main():
    assert tuple(casatasks.version()) == (6, 7, 6, 14)
    with TemporaryDirectory(prefix="flagmanager-alias-") as work, chdir(work):
        check_shared_versions(casatasks.flagmanager, Path("casa"))
        check_shared_versions(tasks.flagmanager, Path("native"))
        check_repeated_save(casatasks.flagmanager, Path("replace-casa"), preserve_archived=False)
        check_repeated_save(tasks.flagmanager, Path("replace-native"), preserve_archived=True)
        check_legacy_alias(Path("legacy"))
    print("Shared alias flag versions checked against CASA 6.7.6.14, including repeated saves and legacy catalogs")


if __name__ == "__main__":
    main()
