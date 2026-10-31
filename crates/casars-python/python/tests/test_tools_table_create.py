"""Safety and persistence laws for table creation (#1028)."""

from pathlib import Path

import pytest

from casars import tools


DESCRIPTION = {"id": {"valueType": "int"}}


def contents(path: Path) -> dict[str, bytes]:
    return {
        str(file.relative_to(path)): file.read_bytes()
        for file in path.rglob("*")
        if file.is_file() and file.name != "table.lock"
    }


def test_invalid_lock_does_not_replace_existing_table(tmp_path: Path) -> None:
    path = tmp_path / "existing.table"
    tool = tools.table()
    assert tool.create(str(path), DESCRIPTION, nrow=1)
    assert tool.putcell("id", 0, 42)
    assert tool.close()
    before = contents(path)

    with pytest.raises(RuntimeError, match="unknown lock option"):
        tool.create(str(path), DESCRIPTION, lockoptions={"option": "not-a-lock"})

    assert contents(path) == before
    assert tool.open(str(path))
    assert tool.getcell("id", 0) == 42
    tool.done()


def test_create_refuses_to_replace_an_open_table(tmp_path: Path) -> None:
    path = tmp_path / "open.table"
    owner, other = tools.table(), tools.table()
    assert owner.create(str(path), DESCRIPTION, nrow=1)
    assert owner.putcell("id", 0, 42)
    before = contents(path)

    with pytest.raises(RuntimeError, match="still open"):
        other.create(str(path), DESCRIPTION)

    assert contents(path) == before
    assert owner.getcell("id", 0) == 42
    owner.done()
    other.done()


def test_create_does_not_delete_an_unrelated_directory(tmp_path: Path) -> None:
    path = tmp_path / "not-a-table"
    path.mkdir()
    marker = path / "keep.txt"
    marker.write_text("keep this")
    tool = tools.table()

    with pytest.raises(RuntimeError, match="not a true table directory"):
        tool.create(str(path), DESCRIPTION)

    assert marker.read_text() == "keep this"
    tool.done()


def test_create_rejects_unsupported_manager_before_replacement(tmp_path: Path) -> None:
    path = tmp_path / "existing.table"
    tool = tools.table()
    assert tool.create(str(path), DESCRIPTION, nrow=1)
    assert tool.putcell("id", 0, 42)
    assert tool.close()
    before = contents(path)
    manager = {"*1": {"TYPE": "StandardStMan", "NAME": "custom", "COLUMNS": ["id"], "SPEC": {}}}

    with pytest.raises(NotImplementedError, match="dminfo"):
        tool.create(str(path), DESCRIPTION, dminfo=manager)

    assert contents(path) == before
    assert tool.open(str(path))
    assert tool.getcell("id", 0) == 42
    tool.done()


def test_memory_table_structural_writes_stay_in_memory(tmp_path: Path) -> None:
    path = tmp_path / "memory.table"
    tool = tools.table()
    assert tool.create(str(path), DESCRIPTION, memtype="memory", nrow=2)
    assert tool.renamecol("id", "renamed")
    assert tool.addcols({"v": {"valueType": "double"}})
    assert tool.colnames() == ["renamed", "v"]
    assert tool.putcell("v", 0, 3.5)
    assert tool.getcell("v", 0) == 3.5
    assert tool.removerows([1])
    assert tool.nrows() == 1
    assert tool.removecols("v")
    assert tool.colnames() == ["renamed"]
    assert tool.done()
    assert not path.exists()


def test_multitable_does_not_replace_or_nest_its_input(tmp_path: Path) -> None:
    path = tmp_path / "input.table"
    tool = tools.table()
    assert tool.create(str(path), DESCRIPTION, nrow=1)
    assert tool.putcell("id", 0, 42)
    assert tool.close()
    before = contents(path)

    assert not tool.createmultitable(str(path), [str(path)], "")
    assert not tool.createmultitable(str(path / "nested"), [str(path)], "")
    assert contents(path) == before
    assert tool.open(str(path))
    assert tool.getcell("id", 0) == 42
    tool.done()


def test_multitable_move_rejects_duplicate_names_without_losing_input(tmp_path: Path) -> None:
    paths = [tmp_path / "one" / "input.table", tmp_path / "two" / "input.table"]
    tool = tools.table()
    for index, path in enumerate(paths):
        path.parent.mkdir()
        assert tool.create(str(path), DESCRIPTION, nrow=1)
        assert tool.putcell("id", 0, index + 40)
        assert tool.close()
    before = [contents(path) for path in paths]

    assert not tool.createmultitable(str(tmp_path / "joined"), [str(path) for path in paths], "parts")
    assert [contents(path) for path in paths] == before
    assert not (tmp_path / "joined").exists()
    tool.done()


def test_multitable_move_rejects_open_input(tmp_path: Path) -> None:
    path = tmp_path / "input.table"
    owner, other = tools.table(), tools.table()
    assert owner.create(str(path), DESCRIPTION, nrow=1)
    assert owner.putcell("id", 0, 42)
    before = contents(path)

    assert not other.createmultitable(str(tmp_path / "joined"), [str(path)], "parts")
    assert contents(path) == before
    assert owner.getcell("id", 0) == 42
    owner.done()
    other.done()


def test_multitable_move_restores_inputs_if_writing_output_fails(tmp_path: Path) -> None:
    path = tmp_path / "input.table"
    tool = tools.table()
    assert tool.create(str(path), DESCRIPTION, nrow=1)
    assert tool.putcell("id", 0, 42)
    assert tool.close()
    before = contents(path)

    # This subdirectory collides with the new table's required control file.
    assert not tool.createmultitable(str(tmp_path / "joined"), [str(path)], "table.dat")
    assert contents(path) == before
    assert not (tmp_path / "joined").exists()
    assert tool.open(str(path))
    assert tool.getcell("id", 0) == 42
    tool.done()
