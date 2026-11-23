"""Public MeasurementSet tool attachment and CASA storage interoperability."""

import shutil

import numpy as np
import pytest

from casars import tools
from test_tools_msmetadata import native_dataset, measures


def pinned_casa():
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    return casa


def test_ms_attachment_reports_logical_name_and_selected_rows(native_dataset, tmp_path, monkeypatch):
    casa = pinned_casa()
    alias = tmp_path / "linked.ms"
    alias.symlink_to(native_dataset.name, target_is_directory=True)
    table = tools.table()
    assert table.open(str(native_dataset))
    first, second = table.getcol("ANTENNA1"), table.getcol("ANTENNA2")
    selected_rows = np.count_nonzero(((first == 0) & (second == 1)) | ((first == 1) & (second == 0)))
    assert 0 < selected_rows < len(first)
    assert table.done()

    for module in (casa, tools):
        ms = module.ms()
        assert ms.name() == "none"
        assert ms.nrow() == 0
        assert ms.nrow(selected=True) == 0
        assert ms.iswritable() is False
        assert ms.ismultims() is False
        assert ms.getreferencedtables() == []
        assert ms.open(str(alias))
        assert ms.name() == str(alias)
        assert ms.getreferencedtables() == [str(alias)]
        assert ms.iswritable() is False
        assert ms.ismultims() is False
        assert ms.nrow() == len(first)
        for invalid in (0, 1, np.bool_(True), None):
            with pytest.raises(TypeError):
                ms.nrow(selected=invalid)
        assert ms.msselect({"baseline": "A1&A2"})
        assert ms.nrow() == len(first)
        assert ms.nrow(selected=True) == selected_rows
        assert ms.getreferencedtables() == [str(alias)]
        assert ms.done()
        assert ms.name() == "none"

        with monkeypatch.context() as context:
            context.chdir(native_dataset)
            assert ms.open(nomodify=False)
            assert ms.name() == str(native_dataset)
            assert ms.iswritable() is True
            assert ms.nrow(selected=True) == len(first)
            assert ms.done()


def test_ms_nrow_observes_rows_published_by_another_handle(native_dataset):
    casa = pinned_casa()
    for module in (casa, tools):
        ms = module.ms()
        assert ms.open(str(native_dataset))
        before = ms.nrow()
        table = casa.table()
        assert table.open(str(native_dataset), nomodify=False)
        assert table.addrows(2)
        assert table.done()
        assert ms.nrow() == before + 2
        assert ms.nrow(selected=True) == before + 2
        assert ms.done()


def test_ms_reports_casa_and_native_reference_tables(native_dataset, tmp_path):
    casa = pinned_casa()
    for name, maker in (("casa", casa), ("native", tools)):
        table = maker.table()
        view_path = tmp_path / f"{name}.ms"
        assert table.open(str(native_dataset))
        view = table.selectrows(np.asarray([4, 1, 8], dtype=np.int64), str(view_path))
        assert view.nrows() == 3
        assert view.done()
        assert table.done()

        for module in (casa, tools):
            ms = module.ms()
            assert ms.open(str(view_path))
            assert ms.name() == str(view_path)
            assert ms.getreferencedtables() == [str(native_dataset)]
            assert ms.nrow() == ms.nrow(selected=True) == 3
            assert ms.ismultims() is False
            assert ms.done()


def test_ms_reads_ordered_casa_multi_ms_parts(native_dataset, tmp_path):
    casa = pinned_casa()
    parts = []
    for name in ("part-b.ms", "part-a.ms"):
        part = tmp_path / name
        shutil.copytree(native_dataset, part)
        parts.append(str(part))
    output = tmp_path / "observations.mms"
    tool = casa.ms()
    assert tool.createmultims(str(output), parts, ["ANTENNA", "DATA_DESCRIPTION", "FEED"])
    assert tool.done()

    before = tools.table()
    assert before.open(str(native_dataset))
    expected_rows = 2 * before.nrows()
    assert before.done()

    assert tool.open(str(output))
    references = tool.getreferencedtables()
    assert len(references) == len(parts)
    assert references[0].endswith("part-b.ms")
    assert references[1].endswith("part-a.ms")
    assert tool.nrow() == expected_rows
    assert tool.ismultims() is True
    assert tool.done()

    native = tools.ms()
    assert native.open(str(output))
    assert native.name() == str(output)
    assert native.getreferencedtables() == references
    assert native.nrow() == native.nrow(selected=True) == expected_rows
    assert native.ismultims() is True
    assert native.iswritable() is False
    assert native.done()
