"""Cache ownership for CASA calibrater handles on linked MeasurementSets."""

import shutil

import pytest
from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


@pytest.mark.parametrize("reference_view", [False, True])
def test_calibrater_caches_linked_subtables(native_dataset, tmp_path, reference_view):
    casa = pinned_casa()
    parts = [tmp_path / name for name in ("a.ms", "b.ms")]
    for part in parts:
        shutil.copytree(native_dataset, part)

    combined = tmp_path / "observations.mms"
    ms = casa.ms()
    assert ms.createmultims(
        str(combined), list(map(str, parts)), ["ANTENNA", "DATA_DESCRIPTION", "FEED", "FIELD"],
    )
    assert ms.done()
    assert not (combined / "FIELD").exists()

    dataset = combined
    if reference_view:
        dataset = tmp_path / "selected.ms"
        table = casa.table()
        assert table.open(str(combined))
        selected = table.selectrows([0], str(dataset))
        assert selected.done() and table.done()

    member_fields = {str(combined / "SUBMSS" / part.name / "FIELD") for part in parts}
    inventories = []
    for backend in (casa, tools):
        table = backend.table()
        calibrater = backend.calibrater()
        before = set(table.showcache(verbose=False))
        try:
            assert calibrater.open(str(dataset), addcorr=False, addmodel=False)
            paths = set(table.showcache(verbose=False)) - before
            assert member_fields <= paths
            assert all(table.isopened(path) for path in member_fields)
            inventories.append(paths)
        finally:
            assert calibrater.done()
        assert set(table.showcache(verbose=False)) == before

    assert inventories[0] == inventories[1]
