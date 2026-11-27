"""Publish named legacy TaQL selections through a parent table keyword."""

from contextlib import closing
from pathlib import Path
from shutil import copytree

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


@pytest.mark.parametrize("giving", ['""', 'AS [overwrite=false]', 'AS [storage="multifile"]'])
def test_legacy_taql_unnamed_reference_preserves_original_row_numbers(native_dataset, giving):
    for api in (pinned_casa(), tools):
        with closing(api.ms()) as selector:
            assert selector.open(str(native_dataset))
            assert selector.selecttaqlold(f"True LIMIT 2 OFFSET 3 GIVING {giving}")
            np.testing.assert_array_equal(selector.rangeold("rows")["rows"], [3, 4])


@pytest.mark.parametrize("mode", ["", " AS PLAIN", " AS SCRATCH"])
def test_legacy_taql_publishes_the_subtable_that_the_parent_links_to(native_dataset, tmp_path, mode):
    casa = pinned_casa()
    with closing(casa.table()) as main:
        assert main.open(str(native_dataset))
        original_rows = main.nrows()
        expected = main.getcol("DATA", 3, 2)
    for api in (casa, tools):
        parent = tmp_path / f"{api.__name__}.ms"
        copytree(native_dataset, parent)
        with closing(api.ms()) as selector:
            assert selector.open(str(native_dataset))
            assert selector.selecttaqlold(f'True LIMIT 2 OFFSET 3 GIVING "{parent}::RESULT"{mode}')
            assert selector.name() == str(native_dataset)
            assert selector.nrow() == original_rows and selector.nrowold(True) == 2
            expected_ids = [0, 1] if mode else [3, 4]
            np.testing.assert_array_equal(selector.rangeold("rows")["rows"], expected_ids)
        with closing(casa.table()) as child:
            assert child.open(str(parent / "RESULT"))
            np.testing.assert_array_equal(child.getcol("DATA"), expected, strict=True)
        with closing(casa.table()) as parent_tool:
            assert parent_tool.open(str(parent))
            target = Path(parent_tool.getkeyword("RESULT").removeprefix("Table: "))
        if api is casa and mode:
            # The pinned deep copy leaves the keyword pointing to a temporary view.
            assert not target.exists()
            continue
        assert target == parent / "RESULT"
        moved = parent.rename(tmp_path / f"{api.__name__}-moved.ms")
        with closing(casa.table()) as parent_tool, closing(tools.table()) as child:
            assert parent_tool.open(str(moved))
            new_target = parent_tool.getkeyword("RESULT").removeprefix("Table: ")
            assert Path(new_target) == moved / "RESULT"
            assert child.open(new_target)
            np.testing.assert_array_equal(child.getcol("DATA"), expected, strict=True)


def test_legacy_taql_can_link_a_result_on_the_original_ms(native_dataset, tmp_path):
    root = tmp_path / "source.ms"
    copytree(native_dataset, root)
    with closing(tools.ms()) as reader:
        assert reader.open(str(root))
        original_rows = reader.nrow()
        assert reader.selecttaqlold(f'True LIMIT 2 GIVING "{root}::QUERY_RESULT"')
        assert reader.nrow() == original_rows and reader.nrowold(True) == 2
    casa = pinned_casa()
    with closing(casa.table()) as parent, closing(casa.table()) as child:
        assert parent.open(str(root))
        assert parent.getkeyword("QUERY_RESULT") == f"Table: {root / 'QUERY_RESULT'}"
        assert child.open(str(root / "QUERY_RESULT"))
        assert child.nrows() == 2
    moved = root.rename(tmp_path / "moved.ms")
    with closing(casa.table()) as child:
        assert child.open(str(moved / "QUERY_RESULT"))
        assert child.nrows() == 2


def test_legacy_taql_self_reference_respects_overwrite_and_open_peers(native_dataset, tmp_path):
    casa = pinned_casa()
    root = tmp_path / "source.ms"
    copytree(native_dataset, root)
    output = root / "RESULT"
    with closing(tools.ms()) as selector:
        assert selector.open(str(root), nomodify=False, lock=True)
        assert selector.selecttaqlold('True LIMIT 2 GIVING ".::RESULT"')
        assert selector.selectinitold(reset=True)
        with pytest.raises(RuntimeError, match="already exists"):
            selector.selecttaqlold('True LIMIT 3 GIVING ".::RESULT" AS [overwrite=false]')
        with closing(tools.table()) as peer:
            assert peer.open(str(output))
            with pytest.raises(RuntimeError, match="still open"):
                selector.selecttaqlold('True LIMIT 3 GIVING ".::RESULT"')
            assert peer.nrows() == 2
        assert selector.selecttaqlold('True LIMIT 3 GIVING ".::RESULT"')
    with closing(casa.table()) as child:
        assert child.open(str(output))
        assert child.nrows() == 3


@pytest.mark.parametrize("destination", ["parent.ms::ANTENNA::RESULT", ".DATA::ANT_LINK::RESULT"])
def test_legacy_taql_resolves_a_nested_parent(native_dataset, tmp_path, monkeypatch, destination):
    casa = pinned_casa()
    for api in (casa, tools):
        cwd = tmp_path / api.__name__
        cwd.mkdir()
        monkeypatch.chdir(cwd)
        copytree(native_dataset, "source.ms")
        parent = cwd / "parent.ms"
        copytree(native_dataset, parent)
        antenna = parent / "ANTENNA"
        with closing(casa.table()) as source:
            assert source.open("source.ms", nomodify=False)
            source.putcolkeyword("DATA", "ANT_LINK", f"Table: {antenna}")
        with closing(api.ms()) as selector:
            assert selector.open("source.ms")
            assert selector.selecttaqlold(f"True LIMIT 3 GIVING {destination}")
            assert selector.nrowold(True) == 3
        with closing(casa.table()) as reader:
            assert reader.open(str(antenna))
            assert reader.getkeyword("RESULT") == f"Table: {antenna / 'RESULT'}"


def test_legacy_taql_reference_parent_persists_the_keyword_on_its_base(native_dataset, tmp_path):
    casa = pinned_casa()
    for api in (casa, tools):
        root = tmp_path / f"{api.__name__}-parent.ms"
        copytree(native_dataset, root)
        reference = tmp_path / f"{api.__name__}-ref.ms"
        with closing(casa.table()) as parent:
            assert parent.open(str(root))
            with closing(parent.query("TRUE", name=str(reference))):
                pass
        with closing(api.ms()) as selector:
            assert selector.open(str(native_dataset))
            assert selector.selecttaqlold(f'True LIMIT 3 GIVING "{reference}::RESULT"')
        for path in (reference, root):
            with closing(casa.table()) as parent:
                assert parent.open(str(path))
                assert parent.getkeyword("RESULT") == f"Table: {reference / 'RESULT'}"


def test_legacy_taql_kind_conflict_preserves_parent_and_leaves_no_child(native_dataset, tmp_path):
    casa = pinned_casa()
    parent = tmp_path / "parent.ms"
    copytree(native_dataset, parent)
    with closing(casa.table()) as reader:
        assert reader.open(str(parent), nomodify=False)
        reader.putkeyword("RESULT", 7)
    with closing(tools.ms()) as selector:
        assert selector.open(str(native_dataset))
        assert selector.selecttaqlold("True LIMIT 8")
        with pytest.raises(RuntimeError, match="incorrect data type"):
            selector.selecttaqlold(f'True LIMIT 3 GIVING "{parent}::RESULT"')
        assert selector.nrowold(True) == 8
    assert not (parent / "RESULT").exists()
    with closing(casa.table()) as reader:
        assert reader.open(str(parent))
        assert reader.getkeyword("RESULT") == 7


def test_legacy_taql_can_copy_into_a_linked_subtable_without_copying_the_stage(native_dataset, tmp_path):
    casa = pinned_casa()
    root = tmp_path / "source.ms"
    copytree(native_dataset, root)
    antenna = root / "ANTENNA"
    with closing(tools.ms()) as selector:
        assert selector.open(str(root))
        assert selector.selecttaqlold(f'True LIMIT 2 GIVING "{root}::ANTENNA::RESULT" AS PLAIN')
        assert selector.nrowold(True) == 2
    with closing(casa.table()) as parent, closing(casa.table()) as child:
        assert parent.open(str(antenna))
        published = parent.getkeyword("RESULT").removeprefix("Table: ")
        assert published == str(antenna / "RESULT")
        assert child.open(published)
        assert child.nrows() == 2
    assert not any(path.name.startswith(".taql-ms-") for path in root.rglob("*"))


def test_legacy_taql_rejects_unpersistable_concatenated_parent(native_dataset, tmp_path):
    casa = pinned_casa()
    parts = [tmp_path / "first.ms", tmp_path / "second.ms"]
    for part in parts:
        copytree(native_dataset, part)
    parent = tmp_path / "parent.ms"
    with closing(casa.ms()) as builder:
        assert builder.createmultims(str(parent), list(map(str, parts)), [])
    command = f'True LIMIT 2 GIVING "{parent}::RESULT"'
    with closing(tools.ms()) as selector:
        assert selector.open(str(native_dataset))
        original = selector.nrowold()
        with pytest.raises(RuntimeError, match="cannot persist TaQL result keywords"):
            selector.selecttaqlold(command)
        assert selector.nrowold() == original
    assert not (parent / "RESULT").exists()
    with closing(casa.ms()) as selector:
        assert selector.open(str(native_dataset))
        assert selector.selecttaqlold(command)
    assert (parent / "RESULT").is_dir()
    with closing(casa.table()) as reader:
        assert reader.open(str(parent))
        assert "RESULT" not in reader.keywordnames()
