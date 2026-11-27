"""Exercise the independent in-memory MSSelector view against pinned CASA."""

from contextlib import closing
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pathlib import Path
from shutil import copytree
import subprocess
import sys

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


def test_legacy_taql_memory_subtable_name_leaves_no_unopenable_disk_keyword(native_dataset, tmp_path):
    casa = pinned_casa()
    for api in (casa, tools):
        parent = tmp_path / f"{api.__name__}-parent.ms"
        copytree(native_dataset, parent)
        with closing(api.ms()) as selector:
            assert selector.open(str(native_dataset))
            assert selector.selecttaqlold(f'True LIMIT 2 GIVING "{parent}::RESULT" AS MEMORY')
            np.testing.assert_array_equal(selector.rangeold("rows")["rows"], [0, 1])
        assert not (parent / "RESULT").exists()
        with closing(casa.table()) as table:
            assert table.open(str(parent))
            links = table.getkeywords()
            if api is casa:
                assert not Path(links["RESULT"].removeprefix("Table: ")).exists()
            else:
                assert "RESULT" not in links


def test_legacy_taql_untyped_output_from_memory_reopens_independently(native_dataset, tmp_path):
    casa = pinned_casa()
    target = tmp_path / "result.ms"
    with closing(casa.table()) as source:
        assert source.open(str(native_dataset))
        expected = source.getcol("DATA", 4, 2)
    with closing(tools.ms()) as selector:
        assert selector.open(str(native_dataset))
        assert selector.selecttaqlold("True LIMIT 5 OFFSET 3 GIVING AS MEMORY")
        np.testing.assert_array_equal(selector.getdataold(["data"])["data"][..., 1:3], expected, strict=True)
        assert selector.selecttaqlold(f'True LIMIT 2 OFFSET 1 GIVING "{target}"')
        np.testing.assert_array_equal(selector.rangeold("rows")["rows"], [1, 2])
    for api in (casa, tools):
        with closing(api.table()) as reader:
            assert reader.open(str(target))
            np.testing.assert_array_equal(reader.getcol("DATA"), expected, strict=True)


def test_legacy_taql_memory_detaches_table_and_column_keyword_tables(native_dataset, tmp_path):
    casa = pinned_casa()
    for api in (casa, tools):
        source = tmp_path / f"{api.__name__}-source.ms"
        target = tmp_path / f"{api.__name__}-output.ms"
        extra = source / "AUXILIARY"
        child = extra / "SPECTRAL_WINDOW"
        copytree(native_dataset, source)
        copytree(source / "ANTENNA", extra)
        copytree(source / "SPECTRAL_WINDOW", child)
        with closing(casa.table()) as table:
            assert table.open(str(source), nomodify=False)
            table.putkeyword("AUX", f"Table: {extra}")
            table.putcolkeyword("DATA", "AUX", f"Table: {extra}")
            assert table.open(str(extra), nomodify=False)
            original_name = table.getcell("NAME", 0)
            table.putkeyword("CHILD", f"Table: {child}")
        with closing(api.ms()) as selector:
            assert selector.open(str(source))
            assert selector.selecttaqlold("True LIMIT 4 GIVING AS MEMORY")
            with closing(casa.table()) as table:
                assert table.open(str(extra), nomodify=False)
                table.putcell("NAME", 0, "EDITED_ORIGINAL")
            assert selector.selecttaqlold(f'True LIMIT 2 GIVING "{target}" AS PLAIN')
        with closing(casa.table()) as table:
            assert table.open(str(target))
            assert table.getkeyword("AUX") == f"Table: {target / 'AUXILIARY'}"
            assert table.getcolkeyword("DATA", "AUX") == f"Table: {target / 'AUXILIARY'}"
            assert table.open(str(target / "AUXILIARY"))
            assert table.getcell("NAME", 0) == original_name
            assert table.getkeyword("CHILD") == f"Table: {target / 'AUXILIARY' / 'SPECTRAL_WINDOW'}"


@pytest.mark.parametrize("parent", [".", "::ANTENNA", ".DATA::AUX"])
def test_legacy_taql_memory_resolves_implicit_parents_in_the_detached_view(native_dataset, parent):
    casa = pinned_casa()
    antenna = native_dataset / "ANTENNA"
    with closing(casa.table()) as source:
        assert source.open(str(native_dataset), nomodify=False)
        source.putcolkeyword("DATA", "AUX", f"Table: {antenna}")
    for api in (casa, tools):
        with closing(api.ms()) as selector:
            assert selector.open(str(native_dataset))
            assert selector.selecttaqlold("True LIMIT 4 GIVING AS MEMORY")
            query = f'True LIMIT 2 GIVING "{parent}::RESULT"'
            with pytest.raises(RuntimeError):
                selector.selecttaqlold(query + " AS PLAIN")
            assert selector.nrowold(True) == 4
            assert selector.selecttaqlold(query + " AS MEMORY")
            np.testing.assert_array_equal(selector.rangeold("rows")["rows"], [0, 1])
    for path in (native_dataset, antenna):
        assert not (path / "RESULT").exists()
        with closing(casa.table()) as source:
            assert source.open(str(path))
            assert "RESULT" not in source.getkeywords()


def test_legacy_taql_memory_protects_external_subtables_and_can_write_after_source_moves(native_dataset, tmp_path):
    casa = pinned_casa()
    source = tmp_path / "source.ms"
    copytree(native_dataset, source)
    antenna = (source / "ANTENNA").rename(tmp_path / "external-antenna")
    with closing(casa.table()) as reader:
        assert reader.open(str(antenna))
        names = reader.getcol("NAME")
        assert reader.open(str(source), nomodify=False)
        reader.putkeyword("ANTENNA", f"Table: {antenna}")
        expected = reader.getcol("DATA", 0, 2)
    target = tmp_path / "independent.ms"
    with closing(tools.ms()) as selector:
        assert selector.open(str(source))
        assert selector.selecttaqlold("True LIMIT 4 GIVING AS MEMORY")
        with pytest.raises(RuntimeError, match="overlaps a source table"):
            selector.selecttaqlold(f'True LIMIT 2 GIVING "{antenna}"')
        assert selector.nrowold(True) == 4
        source.rename(tmp_path / "moved.ms")
        assert selector.selecttaqlold(f'True LIMIT 2 GIVING "{target}" AS PLAIN')
    with closing(casa.table()) as reader:
        assert reader.open(str(target))
        np.testing.assert_array_equal(reader.getcol("DATA"), expected, strict=True)
        assert reader.open(str(antenna))
        np.testing.assert_array_equal(reader.getcol("NAME"), names)


@pytest.mark.parametrize("mode", ["", " AS PLAIN"])
def test_legacy_taql_memory_preserves_main_metadata_when_saved(native_dataset, tmp_path, mode):
    casa = pinned_casa()
    with closing(casa.table()) as writer:
        assert writer.open(str(native_dataset), nomodify=False)
        writer.putcolkeyword("TIME", "QuantumUnits", ["min"])
        writer.putkeyword("TRACE", {"note": "keep independently"})
        units = writer.getcolkeyword("TIME", "QuantumUnits")
    target = tmp_path / "selected.ms"
    with closing(tools.ms()) as selector:
        assert selector.open(str(native_dataset))
        assert selector.selecttaqlold("True LIMIT 4 GIVING AS MEMORY")
        assert selector.selecttaqlold(f'True LIMIT 2 GIVING "{target}"{mode}')
    with closing(casa.table()) as reader:
        assert reader.open(str(target))
        assert reader.getkeyword("TRACE") == {"note": "keep independently"}
        np.testing.assert_array_equal(reader.getcolkeyword("TIME", "QuantumUnits"), units, strict=True)


def test_legacy_taql_memory_reopens_nested_table_and_column_keyword_links(native_dataset, tmp_path):
    casa = pinned_casa()
    source = tmp_path / "source.ms"
    copytree(native_dataset, source)
    extra = source / "AUXILIARY"
    copytree(source / "ANTENNA", extra)
    with closing(casa.table()) as writer:
        assert writer.open(str(source), nomodify=False)
        writer.putkeyword("META.CAL", f"Table: {extra}", makesubrecord=True)
        writer.putcolkeyword("DATA", "META", {"NOTE": "preserve"})
        writer.putcolkeyword("DATA", "META.CAL", f"Table: {extra}")
    target = tmp_path / "result.ms"
    with closing(tools.ms()) as selector:
        assert selector.open(str(source))
        assert selector.selecttaqlold("True LIMIT 4 GIVING AS MEMORY")
        source.rename(tmp_path / "moved.ms")
        assert selector.selecttaqlold(f'True LIMIT 2 GIVING "{target}" AS PLAIN')
    with closing(casa.table()) as reader, closing(casa.table()) as child:
        assert reader.open(str(target))
        for link in (reader.getkeyword("META.CAL"), reader.getcolkeyword("DATA", "META.CAL")):
            assert Path(link.removeprefix("Table: ")) == target / "AUXILIARY"
            assert child.open(link.removeprefix("Table: ")) and child.nrows() > 0


def test_legacy_taql_memory_waits_for_the_subtable_writer(native_dataset, tmp_path):
    casa = pinned_casa()
    antenna = native_dataset / "ANTENNA"
    runner = """
from casatools import table
from contextlib import closing
import sys
with closing(table()) as parent:
    assert parent.open(sys.argv[1], nomodify=False, lockoptions={'option': 'permanent'})
    parent.putcell('NAME', 0, 'FIRST')
    print('HELD', flush=True)
    sys.stdin.readline()
    parent.putcell('NAME', 1, 'SECOND')
"""
    target = tmp_path / "saved.ms"
    with subprocess.Popen(
        [sys.executable, "-c", runner, str(antenna)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    ) as writer, closing(tools.ms()) as selector, ThreadPoolExecutor(max_workers=1) as workers:
        assert writer.stdout.readline().strip() == "HELD"
        assert selector.open(str(native_dataset))
        copied = workers.submit(selector.selecttaqlold, "True LIMIT 4 GIVING AS MEMORY")
        try:
            with pytest.raises(TimeoutError):
                copied.result(timeout=0.2)
        finally:
            writer.stdin.write("release\n")
            writer.stdin.flush()
        assert writer.wait(timeout=10) == 0
        assert copied.result(timeout=10)
        assert selector.selecttaqlold(f'True LIMIT 2 GIVING "{target}" AS PLAIN')
    with closing(casa.table()) as reader:
        assert reader.open(str(target / "ANTENNA"))
        assert [reader.getcell("NAME", i) for i in (0, 1)] == ["FIRST", "SECOND"]
