"""Exercise clearcal while its caller keeps a MeasurementSet table open."""

import shutil

import numpy as np
import pytest
from casars import tasks, tools

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
        alias = tmp_path / f"{label}-alias.ms"
        alias.symlink_to(vis.name, target_is_directory=True)
        table = backend.table()
        other = backend.table()
        assert table.open(str(alias), nomodify=False)
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
            assert other.haslock(write=False)
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


def test_clearcal_preserves_a_user_lock(native_dataset):
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
