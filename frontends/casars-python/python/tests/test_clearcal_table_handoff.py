"""Exercise clearcal while its caller keeps a MeasurementSet table open."""

from concurrent.futures import ThreadPoolExecutor
import os
import shutil
import signal
import subprocess
import sys
import time

import numpy as np
import pytest
from casars import tasks, tools
from casars.tasks import _runner

from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


@pytest.mark.skipif(os.name != "posix", reason="requires a POSIX table lock and SIGINT")
def test_clearcal_interrupts_a_wait_for_an_external_write_lock(native_dataset, monkeypatch):
    before = (native_dataset / "table.dat").read_bytes()
    locked_table = """
import select
import sys
from casars.tools import table

tool = table()
assert tool.open(sys.argv[1], nomodify=False, lockoptions={'option': 'user'})
assert tool.lock(write=True)
print('held', flush=True)
if select.select([sys.stdin], [], [], 15)[0]:
    sys.stdin.readline()
assert tool.done()
"""
    stderr = sys.stderr
    sent = False

    class Terminal:
        def write(self, message):
            nonlocal sent
            if not sent and "waiting for the write lock" in message:
                sent = True
                os.kill(os.getpid(), signal.SIGINT)
            return stderr.write(message)

        def flush(self):
            stderr.flush()

    with subprocess.Popen([sys.executable, "-c", locked_table, str(native_dataset)],
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True) as peer:
        assert peer.stdout.readline().strip() == "held"
        try:
            with monkeypatch.context() as patch:
                patch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "WARN")
                patch.setattr(sys, "stderr", Terminal())
                with pytest.raises(KeyboardInterrupt):
                    tasks.clearcal(str(native_dataset), addmodel=True)
            assert sent and peer.poll() is None
        finally:
            if peer.poll() is None:
                peer.stdin.write("release\n")
                peer.stdin.flush()
            assert peer.wait(timeout=10) == 0
    assert (native_dataset / "table.dat").read_bytes() == before
    assert tasks.clearcal(str(native_dataset), addmodel=True) is None


@pytest.mark.skipif(os.name != "posix", reason="requires a POSIX table lock")
def test_clearcal_debug_stderr_can_reenter_the_same_table(native_dataset, monkeypatch):
    read_main = """
import sys
from casars import tools

table = tools.table()
assert table.open(sys.argv[1])
assert table.nrows() > 0
assert table.done()
"""
    stderr = sys.stderr
    reads = []

    class Terminal:
        def write(self, line):
            if not reads and f"opening CASA table path={native_dataset}\n" in line:
                # clearcal opens MAIN after taking its write lock. A separate
                # process waits for that lock, even while stderr is running.
                try:
                    result = subprocess.run(
                        [sys.executable, "-c", read_main, str(native_dataset)],
                        capture_output=True, text=True, timeout=15,
                    )
                    reads.append((result.returncode, result.stderr))
                except subprocess.TimeoutExpired:
                    reads.append(("timeout", "stderr delivery blocked MAIN lock release"))
            return stderr.write(line)

        def flush(self):
            stderr.flush()

    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "DEBUG")
    monkeypatch.setattr(sys, "stderr", Terminal())
    assert tasks.clearcal(str(native_dataset), addmodel=True) is None
    assert reads == [(0, "")]


def test_clearcal_does_not_wait_for_an_unrelated_busy_table(native_dataset, tmp_path):
    pinned_casa()
    unrelated = tmp_path / "unrelated.ms"
    shutil.copytree(native_dataset, unrelated)
    table = tools.table()
    assert table.open(str(unrelated))
    row = table.row()
    assert table.unlock()
    lock = """
import sys
from casatools import table
tool = table()
assert tool.open(sys.argv[1], nomodify=False, lockoptions={'option': 'user'})
assert tool.lock(write=True)
print('held', flush=True)
sys.stdin.readline()
assert tool.done()
"""
    with subprocess.Popen([sys.executable, "-c", lock, str(unrelated)],
                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True) as peer:
        assert peer.stdout.readline().strip() == "held"
        with ThreadPoolExecutor(max_workers=2) as threads:
            reading = threads.submit(row.get, 0)
            try:
                # The request-count word changes after the row tool takes
                # the shared root mutex and starts waiting for the peer.
                lockfile = unrelated / "table.lock"
                deadline = time.monotonic() + 10
                while not int.from_bytes(lockfile.read_bytes()[:4], "big"):
                    assert time.monotonic() < deadline
                    time.sleep(0.01)
                assert not reading.done()
                assert threads.submit(tasks.clearcal, str(native_dataset), addmodel=True).result(timeout=10) is None
                assert not reading.done()
            finally:
                peer.stdin.write("release\n")
                peer.stdin.flush()
                assert peer.wait(timeout=10) == 0
            assert "DATA" in reading.result(timeout=10)
    row.done()
    assert table.done()


def test_clearcal_updates_an_open_table(native_dataset, tmp_path):
    casa = pinned_casa()
    import casatasks

    snapshots = []
    for label, backend, clearcal in (
        ("casa", casa, casatasks.clearcal), ("native", tools, tasks.clearcal),
    ):
        vis = tmp_path / f"{label}.ms"
        shutil.copytree(native_dataset, vis)
        opened_path = vis
        if label == "native":
            opened_path = tmp_path / "native-alias.ms"
            opened_path.symlink_to(vis.name, target_is_directory=True)
        table = backend.table()
        other = backend.table()
        assert table.open(str(opened_path), nomodify=False)
        assert other.open(str(native_dataset))
        try:
            assert table.haslock(write=False)
            assert other.haslock(write=False)
            clearcal(str(vis), addmodel=True)
            corrected = table.getcol("CORRECTED_DATA")
            np.testing.assert_array_equal(corrected, table.getcol("DATA"))
            model = table.getcol("MODEL_DATA")
            overwrite = np.full_like(corrected, 7 - 2j)
            assert np.any(overwrite != corrected)
            assert table.putcol("CORRECTED_DATA", overwrite)
            clearcal(str(vis))
            np.testing.assert_array_equal(table.getcol("CORRECTED_DATA"), corrected)
            # CASA releases automatic locks process-wide. The native task
            # releases only the table it is about to change.
            assert other.haslock(write=False) == (label == "native")
        finally:
            assert table.done()
            assert other.done()
        for reader in (casa, tools):
            table = reader.table()
            assert table.open(str(vis))
            try:
                np.testing.assert_array_equal(table.getcol("CORRECTED_DATA"), corrected)
                np.testing.assert_array_equal(table.getcol("MODEL_DATA"), model)
            finally:
                assert table.done()
        snapshots.append((corrected, model))

    np.testing.assert_array_equal(snapshots[0][0], snapshots[1][0])
    np.testing.assert_array_equal(snapshots[0][1], snapshots[1][1])


def test_clearcal_keeps_attached_reads_out_until_the_provider_returns(native_dataset, monkeypatch):
    table = tools.table()
    history = tools.table()
    assert table.open(str(native_dataset))
    assert history.open(str(native_dataset / "HISTORY"))
    history_rows = history.nrows()
    run = _runner._native().run_native_task

    def run_after_read_attempt(*args, **kwargs):
        # Both tables are reserved before the native worker takes its lock.
        with pytest.raises(RuntimeError, match="task provider is using this table"):
            table.getcol("DATA")
        with pytest.raises(RuntimeError, match="task provider is using this table"):
            history.nrows()
        return run(*args, **kwargs)

    monkeypatch.setattr(_runner._native(), "run_native_task", run_after_read_attempt)
    try:
        tasks.clearcal(str(native_dataset), addmodel=True)
        np.testing.assert_array_equal(table.getcol("CORRECTED_DATA"), table.getcol("DATA"))
        assert history.nrows() > history_rows
    finally:
        assert table.done()
        assert history.done()


def test_clearcal_yields_the_base_lock_of_a_reopened_saved_view(native_dataset, tmp_path):
    base = tools.table()
    saved = tmp_path / "view.table"
    assert base.open(str(native_dataset))
    view = base.selectrows([0], name=str(saved))
    assert view.done() and base.done()
    assert view.open(str(saved))
    try:
        data = view.getcol("DATA")
        assert view.haslock(write=False)
        tasks.clearcal(str(native_dataset), addmodel=True)
        np.testing.assert_array_equal(view.getcol("DATA"), data)
        assert base.open(str(native_dataset))
        np.testing.assert_array_equal(base.getcell("CORRECTED_DATA", 0), data[..., 0])
    finally:
        assert base.done()
        assert view.done()


def test_clearcal_hands_off_an_open_multi_ms_member(native_dataset, tmp_path):
    casa = pinned_casa()
    import casatasks

    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    try:
        times = table.getcol("TIME")
        midpoint = np.unique(times)[1]
        assert table.putcol("SCAN_NUMBER", np.where(times < midpoint, 1, 2).astype(np.int32))
    finally:
        assert table.done()

    for label, backend, clearcal in (
        ("casa", casa, casatasks.clearcal), ("native", tools, tasks.clearcal),
    ):
        ms = tmp_path / f"{label}.mms"
        casatasks.partition(str(native_dataset), str(ms), separationaxis="scan",
                            numsubms=2, disableparallel=True, flagbackup=False, datacolumn="all")
        parts = sorted((ms / "SUBMSS").glob("*.ms"))
        assert len(parts) == 2
        aggregate = backend.table()
        member = backend.table()
        assert aggregate.open(str(ms))
        assert member.open(str(parts[0]))
        try:
            assert member.haslock(write=False)
            clearcal(str(ms), addmodel=True)
            np.testing.assert_array_equal(member.getcol("CORRECTED_DATA"), member.getcol("DATA"))
            # CASA's existing aggregate schema is a snapshot. Reopening
            # the aggregate reloads its columns from the updated members.
            assert "MODEL_DATA" not in aggregate.colnames()
        finally:
            assert member.done()
            assert aggregate.done()
        for reader in (casa, tools):
            reopened = reader.table()
            assert reopened.open(str(ms))
            try:
                np.testing.assert_array_equal(reopened.getcol("CORRECTED_DATA"), reopened.getcol("DATA"))
                assert "MODEL_DATA" in reopened.colnames()
            finally:
                assert reopened.done()


def test_clearcal_preserves_a_user_lock(native_dataset, tmp_path):
    casa = pinned_casa()
    import casatasks

    reference = tmp_path / "casa-user-lock.ms"
    shutil.copytree(native_dataset, reference)
    reference_table = casa.table()
    assert reference_table.open(str(reference), nomodify=False, lockoptions={"option": "user"})
    try:
        assert reference_table.lock(write=True)
        casatasks.clearcal(str(reference), addmodel=True)
        assert "MODEL_DATA" in reference_table.colnames()
        assert not reference_table.haslock(write=True)
    finally:
        assert reference_table.done()

    table = tools.table()
    assert table.open(str(native_dataset), nomodify=False, lockoptions={"option": "user"})
    try:
        assert table.lock(write=True)
        columns = table.colnames()
        with pytest.raises(RuntimeError, match="table lock held by this Python process"):
            tasks.clearcal(str(native_dataset), addmodel=True)
        assert table.haslock(write=True)
        assert table.colnames() == columns
    finally:
        assert table.done()
