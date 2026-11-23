"""Exercise clearcal while its caller keeps a MeasurementSet table open."""

import shutil

import numpy as np
import pytest
from casars import tasks, tools
from casars.tasks import _runner

from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


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
    launch = _runner.subprocess.Popen

    def launch_after_read_attempt(*args, **kwargs):
        # This happens after the handoff and before the child can take its
        # write lock. A successful getcol would leave that child waiting.
        with pytest.raises(RuntimeError, match="task provider is using this table"):
            table.getcol("DATA")
        with pytest.raises(RuntimeError, match="task provider is using this table"):
            history.nrows()
        return launch(*args, **kwargs)

    monkeypatch.setattr(_runner.subprocess, "Popen", launch_after_read_attempt)
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
