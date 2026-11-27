"""Exercise the independent in-memory MSSelector view against pinned CASA."""

from contextlib import closing
from shutil import copytree

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


@pytest.mark.parametrize("giving", ['AS MEMORY', '"ignored.ms" AS [type="memory"]'])
def test_legacy_taql_memory_owns_its_values_and_keeps_the_modern_attachment(
    native_dataset, tmp_path, monkeypatch, giving,
):
    casa = pinned_casa()
    monkeypatch.chdir(tmp_path)
    with closing(casa.table()) as table:
        assert table.open(str(native_dataset))
        original_rows = table.nrows()
        data = table.getcol("DATA", 3, 7)
        flags = table.getcol("FLAG", 3, 7)
    for api in (casa, tools):
        with closing(api.ms()) as selector:
            assert selector.open(str(native_dataset))
            assert selector.selecttaqlold(f"True LIMIT 7 OFFSET 3 GIVING {giving}")
            assert selector.name() == str(native_dataset)
            assert selector.nrow() == original_rows and selector.nrowold(True) == 7
            assert selector.iswritable() is False
            np.testing.assert_array_equal(selector.rangeold("rows")["rows"], np.arange(7))
            rec = selector.getdataold(["data", "flag", "flag_row"])
            np.testing.assert_array_equal(rec["data"], data.astype(np.complex128), strict=True)
            np.testing.assert_array_equal(rec["flag"], flags, strict=True)
            update = {name: np.logical_not(rec[name]) for name in ("flag", "flag_row")}
            assert selector.putdataold(update)
            current = selector.getdataold(["flag", "flag_row"])
            for name, values in update.items():
                np.testing.assert_array_equal(current[name], values, strict=True)
            assert selector.selecttaqlold("True LIMIT 2 OFFSET 1 GIVING AS MEMORY")
            np.testing.assert_array_equal(selector.rangeold("rows")["rows"], [1, 2])
            assert selector.iterinitold(["TIME"], maxrows=1, adddefaultsortcolumns=False)
            assert selector.iteroriginold()
            assert selector.getdataold(["data"])["data"].shape[-1] == 1
            assert selector.iterendold()
            assert selector.selectinitold(reset=True)
            assert selector.nrowold(True) == original_rows
        assert not (tmp_path / "ignored.ms").exists()
    with closing(casa.table()) as table:
        assert table.open(str(native_dataset))
        np.testing.assert_array_equal(table.getcol("FLAG", 3, 7), flags, strict=True)


def test_legacy_taql_memory_copies_metadata_independently(native_dataset, tmp_path):
    casa = pinned_casa()
    for api in (casa, tools):
        source = tmp_path / f"{api.__name__}-source.ms"
        output = tmp_path / f"{api.__name__}-output.ms"
        copytree(native_dataset, source)
        with closing(api.ms()) as selector:
            assert selector.open(str(source))
            assert selector.selecttaqlold("True LIMIT 4 GIVING AS MEMORY")
            with closing(casa.table()) as antenna:
                assert antenna.open(str(source / "ANTENNA"), nomodify=False)
                name = antenna.getcell("NAME", 0)
                antenna.putcell("NAME", 0, "EDITED_ORIGINAL")
            assert selector.selecttaqlold(f'True LIMIT 2 GIVING "{output}" AS PLAIN')
            np.testing.assert_array_equal(selector.rangeold("rows")["rows"], [0, 1])
        with closing(casa.table()) as antenna:
            assert antenna.open(str(output / "ANTENNA"))
            assert antenna.getcell("NAME", 0) == name


def test_legacy_taql_memory_empty_selector_can_reset(native_dataset):
    for api in (pinned_casa(), tools):
        with closing(api.ms()) as selector:
            assert selector.open(str(native_dataset))
            original = selector.nrow()
            assert not selector.selecttaqlold("False GIVING AS MEMORY")
            assert selector.nrowold(True) == 0
            assert selector.getdataold(["data"]) == {}
            assert selector.selectinitold(reset=True)
            assert selector.nrowold(True) == original
