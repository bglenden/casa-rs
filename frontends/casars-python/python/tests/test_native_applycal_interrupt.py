# SPDX-License-Identifier: LGPL-3.0-or-later
"""Exercise applycal's bounded staging and accepted publication with public SIGINT."""

import os
import subprocess
import sys

import numpy as np
import pytest

from casars import _core, tasks, tools
from test_native_imstat_interrupt import image_contents
from test_native_transform_interrupt import messages
from test_tools_simulator import configure, measures, needs_measures

pytestmark = pytest.mark.skipif(
    not hasattr(_core, "_arm_solve_test"), reason="requires solve-test-hooks on POSIX"
)
COLUMNS = (
    "DATA", "FLAG", "FLAG_ROW", "FLAG_CATEGORY", "WEIGHT", "WEIGHT_SPECTRUM", "CORRECTED_DATA",
)
INTERRUPTED = "applycal interrupted task=applycal reason=applycal interrupted"
COMPLETED = "applycal completed task=applycal"
PREFIX = "solve test checkpoint "


def read_columns(path, columns=COLUMNS):
    table = tools.table()
    assert table.open(str(path))
    try:
        return {column: table.getcol(column) for column in columns if column in table.colnames()}
    finally:
        assert table.done()


def filled(table, name, value):
    return np.full_like(table.getcol(name), value)


@pytest.fixture
def apply_fixture(tmp_path, monkeypatch):
    def create(existing, *, rows="900s", regions=False, stokes="RR LL"):
        source = tmp_path / "input.ms"
        gain = tmp_path / "gain.cal"
        sm = tools.simulator()
        assert sm.open(str(source))
        configure(sm, autocorrelations=False, stokes=stokes)
        if regions:
            assert sm.setspwindow(
                spwname="other", freq="1.5GHz", deltafreq="1MHz",
                freqresolution="1MHz", nchannels=4, stokes="RR LL",
            )
        assert sm.observe("F", "L", starttime="0s", stoptime=rows)
        if regions:
            assert sm.observe("F", "other", starttime="1000s", stoptime="1900s")
        assert sm.close()
        tasks.gencal(vis=source, caltable=gain, caltype="amp", parameter=[2.0],
                     spw="0" if regions else "")

        writer = tools.table()
        assert writer.open(str(gain), nomodify=False)
        try:
            if regions:
                missing = np.flatnonzero(writer.getcol("SPECTRAL_WINDOW_ID") == 1)
                assert missing.size > 0
                assert writer.removerows(missing.tolist())
            unusable = writer.getcol("ANTENNA1") == 0
            assert unusable.any() and not unusable.all()
            flags = np.broadcast_to(unusable, writer.getcol("FLAG").shape).copy()
            assert writer.putcol("FLAG", flags)
        finally:
            assert writer.done()

        assert writer.open(str(source), nomodify=False)
        try:
            assert writer.nrows() > 512
            assert writer.putcol("DATA", filled(writer, "DATA", 8 + 4j))
            flags = filled(writer, "FLAG", False)
            flags[0, 0, 0] = True
            assert writer.putcol("FLAG", flags)
            flag_rows = filled(writer, "FLAG_ROW", False)
            flag_rows[0] = True
            assert writer.putcol("FLAG_ROW", flag_rows)
            assert writer.putcol("FLAG_CATEGORY", np.stack((flags, ~flags), axis=2))
            assert writer.putcol("SIGMA", filled(writer, "SIGMA", 2.0))
            assert writer.putcol("WEIGHT", filled(writer, "WEIGHT", 3.0))
            spectral = ("SIGMA_SPECTRUM", "WEIGHT_SPECTRUM")
            assert writer.addcols({name: {"valueType": "float", "ndim": 2} for name in spectral})
            for name, value in zip(spectral, (4.0, 5.0), strict=True):
                assert writer.putcol(name, np.full(flags.shape, value, dtype=np.float32))
            if existing:
                assert writer.addcols({"CORRECTED_DATA": {"valueType": "complex", "ndim": 2}})
                assert writer.putcol("CORRECTED_DATA", filled(writer, "DATA", -3 + 9j))
        finally:
            assert writer.done()

        monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "off")
        monkeypatch.setenv("CASA_RS_LOG_TABLE_PRIORITY", "DEBUG")
        return source, gain

    return create


def read_log(path):
    log = messages(path)
    return log, [line.removeprefix(PREFIX) for line in log if line.startswith(PREFIX)]


def assert_calibration(source, before):
    columns = read_columns(source, COLUMNS + ("ANTENNA1", "ANTENNA2"))
    bad = (columns["ANTENNA1"] == 0) | (columns["ANTENNA2"] == 0)
    assert bad.any() and not bad.all()
    np.testing.assert_array_equal(columns["FLAG"][:, 1:3, bad], True)
    np.testing.assert_array_equal(columns["FLAG"][:, 1:3, ~bad], False)
    np.testing.assert_array_equal(columns["FLAG"][:, [0, 3]], before["FLAG"][:, [0, 3]])
    np.testing.assert_array_equal(columns["FLAG_ROW"], before["FLAG_ROW"])
    np.testing.assert_array_equal(columns["FLAG_CATEGORY"], before["FLAG_CATEGORY"])
    np.testing.assert_array_equal(columns["DATA"], before["DATA"])
    np.testing.assert_allclose(columns["CORRECTED_DATA"][:, 1:3, ~bad], 2 + 1j, rtol=1e-6)
    unselected = before.get("CORRECTED_DATA", before["DATA"])
    np.testing.assert_array_equal(columns["CORRECTED_DATA"][:, [0, 3]], unselected[:, [0, 3]])
    np.testing.assert_allclose(columns["WEIGHT"][:, ~bad], 1.0, rtol=1e-6)
    np.testing.assert_allclose(columns["WEIGHT_SPECTRUM"][:, 1:3, ~bad], 1.0, rtol=1e-6)
    np.testing.assert_array_equal(
        columns["WEIGHT_SPECTRUM"][:, [0, 3]], before["WEIGHT_SPECTRUM"][:, [0, 3]],
    )


def assert_backup(store, version, before):
    columns = read_columns(store / f"flags.{version}", ("FLAG", "FLAG_ROW"))
    for name, values in columns.items():
        np.testing.assert_array_equal(values, before[name], err_msg=name)
    assert columns.keys() == {"FLAG", "FLAG_ROW"}
    assert not list(store.glob(".flag-version-*"))


@needs_measures
@pytest.mark.parametrize(("stage", "existing"), [
    ("apply-batch", False),
    ("apply-before-commit", False),
    ("apply-before-commit", True),
    ("apply-main-committed", True),
    ("apply-trial-batch", True),
])
def test_applycal_sigint_and_retry(tmp_path, monkeypatch, apply_fixture, stage, existing):
    source, gain = apply_fixture(existing)
    store = source.with_suffix(".ms.flagversions")
    trial = stage == "apply-trial-batch"
    accepted = stage == "apply-main-committed"
    if existing:
        tasks.flagmanager(vis=source, mode="save", versionname="before")
    original = image_contents(source)
    original_store = image_contents(store) if store.exists() else None
    original_gain = image_contents(gain)
    prior_history = messages(source / "HISTORY")
    old = read_columns(source)
    assert ("CORRECTED_DATA" in old) == existing
    request = dict(vis=source, gaintable=[gain], spw="0:1~2",
                   applymode="trial" if trial else "calflag", calwt=[True])
    trace = tmp_path / "interrupt.logtable"
    monkeypatch.setenv("CASA_RS_LOG_TABLE", str(trace))
    _core._arm_solve_test(stage, 0)
    with pytest.raises(KeyboardInterrupt):
        tasks.applycal(**request)

    log, stages = read_log(trace)
    if accepted:
        assert stages[-2:] == ["ApplyBeforeCommit", "ApplyMainCommitted"]
        assert COMPLETED in log and INTERRUPTED not in log
        assert_calibration(source, old)
        assert_backup(store, "applycal_1", old)
        assert messages(source / "HISTORY").count("taskname=applycal") == prior_history.count("taskname=applycal") + 1
    else:
        expected = {
            "apply-batch": ["ApplyBatchStarted(0, 0)", "ApplyBatchFinished(0, 0)"],
            "apply-trial-batch": ["ApplyTrialBatchStarted(0)", "ApplyTrialBatchFinished(0)"],
        }
        if stage in expected:
            assert stages == expected[stage]
        else:
            assert stages[-1] == "ApplyBeforeCommit"
        assert image_contents(source) == original
        assert (image_contents(store) if store.exists() else None) == original_store
        assert INTERRUPTED in log
    assert image_contents(gain) == original_gain

    before_retry = read_columns(source)
    assert tasks.applycal(**request) is None
    if trial:
        assert "applycal_1" not in (store / "FLAG_VERSION_LIST").read_text()
        current = read_columns(source)
        assert current.keys() == old.keys()
        for name in old:
            np.testing.assert_array_equal(current[name], old[name], err_msg=name)
    else:
        assert_calibration(source, old)
        assert_backup(store, "applycal_2" if accepted else "applycal_1", before_retry)
    assert messages(source / "HISTORY").count("taskname=applycal") == prior_history.count("taskname=applycal") + 1 + int(accepted)


@needs_measures
def test_applycal_strict_sigint_discards_both_mutations(tmp_path, monkeypatch, apply_fixture):
    source, gain = apply_fixture(True, regions=True)
    original = image_contents(source)
    prior_history = messages(source / "HISTORY")
    old = read_columns(source)
    request = dict(vis=source, gaintable=[gain], spw="0:1~2,1:1~2",
                   applymode="calflagstrict", calwt=[True])
    trace = tmp_path / "strict.logtable"
    monkeypatch.setenv("CASA_RS_LOG_TABLE", str(trace))
    _core._arm_solve_test("apply-strict-batch", 0)
    with pytest.raises(KeyboardInterrupt):
        tasks.applycal(**request)

    log, stages = read_log(trace)
    assert "ApplyBatchFinished(0, 1)" in stages
    assert [s for s in stages if s.startswith("ApplyBatch") and "(1," in s] == [
        "ApplyBatchStarted(1, 0)", "ApplyBatchFinished(1, 0)",
    ]
    assert image_contents(source) == original
    store = source.with_suffix(".ms.flagversions")
    assert not store.exists()
    assert INTERRUPTED in log

    assert tasks.applycal(**request) is None
    current = read_columns(source, COLUMNS + ("DATA_DESC_ID", "ANTENNA1", "ANTENNA2"))
    unavailable = current["DATA_DESC_ID"] == 1
    usable = (~unavailable) & (current["ANTENNA1"] != 0) & (current["ANTENNA2"] != 0)
    assert unavailable.sum() > 256 and usable.any()
    np.testing.assert_allclose(current["CORRECTED_DATA"][:, 1:3, usable], 2 + 1j, rtol=1e-6)
    np.testing.assert_array_equal(current["CORRECTED_DATA"][..., unavailable], old["CORRECTED_DATA"][..., unavailable])
    np.testing.assert_array_equal(current["FLAG"][:, 1:3, unavailable], True)
    np.testing.assert_array_equal(current["FLAG"][:, [0, 3]], old["FLAG"][:, [0, 3]])
    np.testing.assert_array_equal(current["FLAG_ROW"], old["FLAG_ROW"])
    np.testing.assert_array_equal(current["FLAG_CATEGORY"], old["FLAG_CATEGORY"])
    assert_backup(store, "applycal_1", old)
    assert len(messages(source / "HISTORY")) > len(prior_history)


@needs_measures
@pytest.mark.skipif(os.name != "posix", reason="requires POSIX table locks and SIGINT")
def test_applycal_sigint_ends_commit_lock_wait(tmp_path, monkeypatch, apply_fixture):
    source, gain = apply_fixture(False)
    original = image_contents(source)
    old = read_columns(source)
    trace = tmp_path / "waiting.logtable"
    monkeypatch.setenv("CASA_RS_LOG_TABLE", str(trace))
    monkeypatch.setenv("CASA_RS_LOG_STDERR_PRIORITY", "WARN")
    read_lock = """
import sys
from casars import tools
table = tools.table()
assert table.open(sys.argv[1], lockoptions={"option": "user"})
assert table.lock(write=False)
print("held", flush=True)
sys.stdin.readline()
assert table.done()
"""
    signal_on_wait = """
import os
import signal
import sys
from casars import tasks

class InterruptOnWait:
    sent = False
    def write(self, line):
        if not self.sent and "waiting for the write lock" in line:
            self.sent = True
            os.kill(os.getpid(), signal.SIGINT)
        return sys.__stderr__.write(line)
    def flush(self):
        sys.__stderr__.flush()

terminal = InterruptOnWait()
sys.stderr = terminal
try:
    tasks.applycal(vis=sys.argv[1], gaintable=[sys.argv[2]], spw="0:1~2")
except KeyboardInterrupt:
    assert terminal.sent
else:
    raise AssertionError("applycal completed despite SIGINT")
"""
    with subprocess.Popen(
        [sys.executable, "-c", read_lock, str(source)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    ) as holder:
        try:
            assert holder.stdout.readline().strip() == "held"
            worker = subprocess.run(
                [sys.executable, "-c", signal_on_wait, str(source), str(gain)],
                capture_output=True, text=True, timeout=15,
            )
            assert worker.returncode == 0, worker.stderr
            assert holder.poll() is None
            assert INTERRUPTED in messages(trace)
            assert image_contents(source) == original
            store = source.with_suffix(".ms.flagversions")
            assert not store.exists()
        finally:
            holder.communicate(input="release\n", timeout=8)
        assert holder.returncode == 0

    assert tasks.applycal(vis=source, gaintable=[gain], spw="0:1~2") is None
    assert_calibration(source, old)
    assert_backup(store, "applycal_1", old)


@needs_measures
@pytest.mark.parametrize("parang", [False, True], ids=["selection", "calculation"])
def test_applycal_refusal_preserves_casa_setup(apply_fixture, parang):
    source, gain = apply_fixture(False, stokes="XX YY" if parang else "RR LL")
    old = read_columns(source)
    history = messages(source / "HISTORY")
    options = {"parang": True} if parang else {"gaintable": [gain], "spw": "does-not-exist"}
    with pytest.raises(RuntimeError):
        tasks.applycal(vis=source, **options)
    current = read_columns(source)
    for name in old:
        np.testing.assert_array_equal(current[name], old[name], err_msg=name)
    np.testing.assert_array_equal(current["CORRECTED_DATA"], old["DATA"])
    assert messages(source / "HISTORY") == history
    assert_backup(source.with_suffix(".ms.flagversions"), "applycal_1", old)


@needs_measures
@pytest.mark.skipif(os.name != "posix", reason="requires POSIX table locks")
def test_applycal_preserves_a_locked_backup_missing_from_the_list(apply_fixture):
    source, gain = apply_fixture(False)
    tasks.flagmanager(vis=source, mode="save", versionname="applycal_1")
    store = source.with_suffix(".ms.flagversions")
    orphan = store / "flags.applycal_1"
    (store / "FLAG_VERSION_LIST").write_text("")
    original = image_contents(orphan)

    writer = tools.table()
    assert writer.open(str(source), nomodify=False)
    try:
        flags = writer.getcol("FLAG")
        flags[0, 0, 0] = False
        assert writer.putcol("FLAG", flags)
    finally:
        assert writer.done()
    working_flags = read_columns(source, ("FLAG", "FLAG_ROW"))

    reader = tools.table()
    assert reader.open(str(orphan), lockoptions={"option": "user"})
    try:
        assert reader.lock(write=False)
        assert tasks.applycal(vis=source, gaintable=[gain], spw="0:1~2") is None
    finally:
        assert reader.done()
    assert image_contents(orphan) == original
    assert_backup(store, "applycal_2", working_flags)
    assert (store / "FLAG_VERSION_LIST").read_text().split(" : ")[0] == "applycal_2"
