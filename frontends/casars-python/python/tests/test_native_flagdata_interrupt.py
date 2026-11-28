# SPDX-License-Identifier: LGPL-3.0-or-later
"""Interrupt the public flagdata task with a real signal at controlled stages."""

import os
import shutil
import subprocess
import sys

import numpy as np
import pytest

from casars import _core, tasks, tools
from _table_readback import open_table
from test_tools_msmetadata import measures, native_dataset
from test_native_transform_interrupt import messages


requires_flagdata_hooks = pytest.mark.skipif(
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


def checkpoints(log, stage):
    prefix = "flagdata test checkpoint "
    return [line.removeprefix(prefix) for line in log if line.startswith(prefix + stage)]


@requires_flagdata_hooks
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
    assert checkpoints(log, "ClipBatch") == ["ClipBatchStarted", "ClipBatchStaged"]
    assert_flags(source, flags, flag_rows)
    assert messages(source / "HISTORY") == history
    assert_backups(source, flags, flag_rows, ["flagdata_1"])

    retry = task_log(tmp_path, monkeypatch, "retry")
    _core._arm_flagdata_test("observe")
    tasks.flagdata(vis=source, **parameters)
    assert checkpoints(messages(retry), "ClipBatch")[:4] == ["ClipBatchStarted", "ClipBatchStaged"] * 2
    assert_flags(source, np.ones_like(flags), flag_rows)
    assert_backups(source, flags, flag_rows, ["flagdata_1", "flagdata_2"])


@requires_flagdata_hooks
@pytest.mark.parametrize("accepted", [False, True], ids=["before-commit", "main-committed"])
def test_sigint_respects_main_commit_boundary(flag_input, tmp_path, monkeypatch, accepted):
    source, flags, flag_rows = flag_input
    history = messages(source / "HISTORY")
    log = task_log(tmp_path, monkeypatch, "boundary")
    _core._arm_flagdata_test("main_committed" if accepted else "before_commit")
    with pytest.raises(KeyboardInterrupt):
        tasks.flagdata(vis=source, mode="manual")

    log = messages(log)
    assert_backups(source, flags, flag_rows, ["flagdata_1"])
    if accepted:
        assert checkpoints(log, "BeforeCommit") == ["BeforeCommit"]
        assert checkpoints(log, "MainCommitted") == ["MainCommitted"]
        assert "flagdata completed task=flagdata" in log
        assert INTERRUPTED not in log
        assert_flags(source, np.ones_like(flags), np.ones_like(flag_rows))
        assert len(messages(source / "HISTORY")) > len(history)
    else:
        assert checkpoints(log, "BeforeCommit") == ["BeforeCommit"]
        assert not checkpoints(log, "MainCommitted")
        assert INTERRUPTED in log
        assert_flags(source, flags, flag_rows)
        assert messages(source / "HISTORY") == history


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX table locks and SIGINT")
def test_sigint_ends_wait_for_external_main_read_lock(flag_input, tmp_path, monkeypatch):
    source, flags, flag_rows = flag_input
    history = messages(source / "HISTORY")
    log = task_log(tmp_path, monkeypatch, "contended")
    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "WARN")
    held_reader = """
import sys
from casars import tools

reader = tools.table()
assert reader.open(sys.argv[1], lockoptions={'option': 'user'})
assert reader.lock(write=False)
print('held', flush=True)
sys.stdin.readline()
assert reader.done()
"""
    interrupt_on_wait = """
import os
import signal
import sys
from casars import tasks

class InterruptOnWait:
    sent = False

    def write(self, text):
        if not self.sent and 'waiting for the write lock' in text:
            self.sent = True
            os.kill(os.getpid(), signal.SIGINT)
        return sys.__stderr__.write(text)

    def flush(self):
        sys.__stderr__.flush()

terminal = InterruptOnWait()
sys.stderr = terminal
try:
    tasks.flagdata(vis=sys.argv[1], mode='manual', flagbackup=False)
except KeyboardInterrupt:
    assert terminal.sent
else:
    raise AssertionError('flagdata completed despite SIGINT')
"""
    with subprocess.Popen(
        [sys.executable, "-c", held_reader, str(source)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    ) as holder:
        try:
            assert holder.stdout.readline().strip() == "held"
            worker = subprocess.run(
                [sys.executable, "-c", interrupt_on_wait, str(source)],
                capture_output=True, text=True, timeout=8,
            )
            assert worker.returncode == 0, worker.stderr
            assert holder.poll() is None
            assert INTERRUPTED in messages(log)
            assert_flags(source, flags, flag_rows)
            assert messages(source / "HISTORY") == history
        finally:
            holder.communicate(input="release\n", timeout=8)
        assert holder.returncode == 0

    task_log(tmp_path, monkeypatch, "retry")
    tasks.flagdata(vis=source, mode="manual", flagbackup=False)
    assert_flags(source, np.ones_like(flags), np.ones_like(flag_rows))
    assert len(messages(source / "HISTORY")) > len(history)


@requires_flagdata_hooks
@pytest.mark.parametrize("existing_outfile", [False, True], ids=["new", "existing"])
def test_sigint_during_calculate_report_stops_and_preserves_outfile(
    flag_input, tmp_path, monkeypatch, existing_outfile
):
    source, flags, flag_rows = flag_input
    history = messages(source / "HISTORY")
    outfile = tmp_path / "calculated.txt"
    if existing_outfile:
        outfile.write_text("keep this file\n")
    parameters = dict(
        vis=source, mode="manual", action="calculate", savepars=True, outfile=str(outfile),
        overwrite=True,
    )
    log = task_log(tmp_path, monkeypatch, "calculate")
    _core._arm_flagdata_test("report")
    with pytest.raises(KeyboardInterrupt):
        tasks.flagdata(**parameters)

    if existing_outfile:
        assert outfile.read_text() == "keep this file\n"
    else:
        assert not outfile.exists()
    log = messages(log)
    assert INTERRUPTED in log
    assert checkpoints(log, "ReportRow") == ["ReportRowStarted", "ReportRowFinished"]
    assert_flags(source, flags, flag_rows)
    assert messages(source / "HISTORY") == history

    task_log(tmp_path, monkeypatch, "retry")
    tasks.flagdata(**parameters)
    assert "mode='manual'" in outfile.read_text()
    assert_flags(source, flags, flag_rows)


@requires_flagdata_hooks
def test_sigint_stops_next_manual_flag_staging_batch(flag_input, tmp_path, monkeypatch):
    source, flags, flag_rows = flag_input
    history = messages(source / "HISTORY")
    log = task_log(tmp_path, monkeypatch, "manual")
    _core._arm_flagdata_test("flag_stage")
    with pytest.raises(KeyboardInterrupt):
        tasks.flagdata(vis=source, mode="manual", flagbackup=False)

    log = messages(log)
    assert INTERRUPTED in log
    first_batch = ["FlagStageStarted(2)", "FlagStageFinished(2)"]
    assert checkpoints(log, "FlagStage") == first_batch
    assert not checkpoints(log, "BeforeCommit")
    assert_flags(source, flags, flag_rows)
    assert messages(source / "HISTORY") == history

    retry = task_log(tmp_path, monkeypatch, "retry")
    _core._arm_flagdata_test("observe")
    tasks.flagdata(vis=source, mode="manual", flagbackup=False)
    assert checkpoints(messages(retry), "FlagStage")[:4] == first_batch * 2
    assert_flags(source, np.ones_like(flags), np.ones_like(flag_rows))
