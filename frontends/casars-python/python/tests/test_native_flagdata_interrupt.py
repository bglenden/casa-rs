# SPDX-License-Identifier: LGPL-3.0-or-later
"""Interrupt the public flagdata task with a real signal at controlled stages."""

import shutil

import numpy as np
import pytest

from casars import _core, tasks, tools
from _table_readback import open_table
from test_tools_msmetadata import measures, native_dataset
from test_native_transform_interrupt import messages


pytestmark = pytest.mark.skipif(
    not hasattr(_core, "_arm_flagdata_test"), reason="requires flagdata-test-hooks on POSIX"
)
INTERRUPTED = "flagdata interrupted task=flagdata reason=flagdata interrupted"


@pytest.fixture
def flag_input(native_dataset, tmp_path, monkeypatch):
    source = tmp_path / "flags.ms"
    shutil.copytree(native_dataset, source)
    writer = tools.table()
    assert writer.open(str(source), nomodify=False)
    try:
        assert writer.nrows() > 2
        assert not np.any(writer.getcol("DATA"))
        flags = np.zeros_like(writer.getcol("FLAG"))
        flags[0, 0, 0] = True
        flags[..., -1] = True
        flag_rows = np.zeros(writer.nrows(), dtype=bool)
        flag_rows[-1] = True
        assert writer.putcol("FLAG", flags)
        assert writer.putcol("FLAG_ROW", flag_rows)
    finally:
        assert writer.done()
    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "off")
    monkeypatch.setenv("CASA_RS_LOG_TABLE_PRIORITY", "DEBUG")
    monkeypatch.chdir(tmp_path)
    return source, flags, flag_rows


def assert_flags(path, flags, flag_rows):
    table = open_table(path)
    try:
        np.testing.assert_array_equal(table.getcol("FLAG"), flags)
        np.testing.assert_array_equal(table.getcol("FLAG_ROW"), flag_rows)
    finally:
        assert table.done()


def assert_backups(source, flags, flag_rows, names):
    versions = tasks.flagmanager(vis=source, mode="list")
    assert sorted(entry["name"] for key, entry in versions.items() if key != "MS") == names
    store = source.with_suffix(".ms.flagversions")
    for name in names:
        assert_flags(store / f"flags.{name}", flags, flag_rows)
    assert not list(store.glob(".flag-version-*"))


def task_log(tmp_path, monkeypatch, name):
    log = tmp_path / f"{name}.logtable"
    monkeypatch.setenv("CASA_RS_LOG_TABLE", str(log))
    return log


def checkpoints(log):
    prefix = "flagdata test checkpoint "
    return [line.removeprefix(prefix) for line in log if line.startswith(prefix)]


@pytest.mark.parametrize("list_mode", [False, True], ids=["clip", "list"])
def test_sigint_stops_next_clip_batch_and_discards_staged_flags(
    flag_input, tmp_path, monkeypatch, list_mode
):
    source, flags, flag_rows = flag_input
    history = messages(source / "HISTORY")
    parameters = (
        {"mode": "list", "inpfile": ["mode='manual' spw='0:0'", "mode='clip' clipzeros=True"]}
        if list_mode
        else {"mode": "clip", "clipzeros": True}
    )
    log = task_log(tmp_path, monkeypatch, "interrupt")
    _core._arm_flagdata_test("clip")
    with pytest.raises(KeyboardInterrupt):
        tasks.flagdata(vis=source, **parameters)

    log = messages(log)
    assert INTERRUPTED in log
    assert checkpoints(log) == ["ClipBatchStarted", "ClipBatchStaged"]
    assert_flags(source, flags, flag_rows)
    assert messages(source / "HISTORY") == history
    assert_backups(source, flags, flag_rows, ["flagdata_1"])

    retry = task_log(tmp_path, monkeypatch, "retry")
    _core._arm_flagdata_test("observe")
    tasks.flagdata(vis=source, **parameters)
    assert checkpoints(messages(retry))[:4] == ["ClipBatchStarted", "ClipBatchStaged"] * 2
    assert_flags(source, np.ones_like(flags), flag_rows)
    assert_backups(source, flags, flag_rows, ["flagdata_1", "flagdata_2"])


@pytest.mark.parametrize("accepted", [False, True], ids=["before-commit", "commit-accepted"])
def test_sigint_respects_main_commit_boundary(flag_input, tmp_path, monkeypatch, accepted):
    source, flags, flag_rows = flag_input
    history = messages(source / "HISTORY")
    log = task_log(tmp_path, monkeypatch, "boundary")
    _core._arm_flagdata_test("commit_accepted" if accepted else "before_commit")
    with pytest.raises(KeyboardInterrupt):
        tasks.flagdata(vis=source, mode="manual")

    log = messages(log)
    assert_backups(source, flags, flag_rows, ["flagdata_1"])
    if accepted:
        assert checkpoints(log) == ["BeforeCommit", "CommitAccepted"]
        assert "flagdata completed task=flagdata" in log
        assert INTERRUPTED not in log
        assert_flags(source, np.ones_like(flags), np.ones_like(flag_rows))
        assert len(messages(source / "HISTORY")) > len(history)
    else:
        assert checkpoints(log) == ["BeforeCommit"]
        assert INTERRUPTED in log
        assert_flags(source, flags, flag_rows)
        assert messages(source / "HISTORY") == history
