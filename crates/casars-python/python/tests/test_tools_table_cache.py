"""Process-wide table cache behavior observed in CASA (#1028)."""

import sys
import tarfile
from pathlib import Path

from casars import tools

from _casa_tools import DATA


def create_table(path: Path) -> None:
    tool = tools.table()
    assert tool.create(str(path), {"id": {"valueType": "int"}}, nrow=1)
    assert tool.done()


def test_cache_keeps_symbolic_link_names_and_expands_variables(tmp_path, monkeypatch) -> None:
    original, alias = tmp_path / "original.table", tmp_path / "alias.table"
    create_table(original)
    alias.symlink_to(original, target_is_directory=True)
    tool = tools.table()
    assert tool.open(str(alias))
    monkeypatch.setenv("CASARS_TEST_TABLE", str(alias))
    assert tool.isopened("$CASARS_TEST_TABLE")
    assert not tool.isopened(str(original))
    assert str(alias) in tool.showcache(False)
    tool.done()


def test_empty_cache_name_is_the_current_directory(tmp_path, monkeypatch) -> None:
    path = tmp_path / "current.table"
    create_table(path)
    monkeypatch.chdir(path)
    tool = tools.table()
    assert tool.open(".")
    assert tool.isopened("")
    assert tool.isopened(".")
    tool.done()


def test_cache_lists_concat_parts_and_not_the_concat_or_memory_table(tmp_path) -> None:
    first, second, joined = (tmp_path / name for name in ("a.table", "b.table", "joined.table"))
    create_table(first)
    create_table(second)
    tool, memory = tools.table(), tools.table()
    assert tool.createmultitable(str(joined), [str(first), str(second)], "")
    assert tool.open(str(joined))
    assert memory.create(str(tmp_path / "memory"), {"id": {"valueType": "int"}}, memtype="memory")
    names = tool.showcache(False)
    assert names == sorted(names)
    assert str(first) in names and str(second) in names
    assert str(joined) not in names and str(tmp_path / "memory") not in names
    assert tool.isopened(str(first)) and tool.isopened(str(second))
    assert not tool.isopened(str(joined)) and not tool.isopened(str(tmp_path / "memory"))
    memory.done()
    tool.done()


def test_cache_lists_the_plain_root_of_an_open_reference(tmp_path) -> None:
    with tarfile.open(DATA / "reftable_nested.tgz", "r:gz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    root = tmp_path / "reftable-nested"
    tool = tools.table()
    assert tool.open(str(root / "nested.table"))
    assert tool.isopened(str(root / "parent.table"))
    assert not tool.isopened(str(root / "nested.table"))
    assert str(root / "parent.table") in tool.showcache(False)
    tool.done()


def test_listlocks_prints_locked_table_names(tmp_path, capfd) -> None:
    path = tmp_path / "locked.table"
    create_table(path)
    tool = tools.table()
    assert tool.open(str(path), nomodify=False)
    assert tool.lock(write=True, nattempts=1)
    assert tool.listlocks()
    text = capfd.readouterr().out
    assert "Locked tables:" in text and str(path) in text and "--" in text
    assert tool.unlock()
    assert tool.listlocks()
    assert str(path) not in capfd.readouterr().out
    tool.done()


def test_listlocks_reports_locked_concat_constituents(tmp_path, capfd) -> None:
    first, second, joined = (tmp_path / name for name in ("a.table", "b.table", "joined.table"))
    create_table(first)
    create_table(second)
    tool = tools.table()
    assert tool.createmultitable(str(joined), [str(first), str(second)], "")
    assert tool.open(str(joined), nomodify=False)
    assert tool.lock(write=True, nattempts=1)
    assert tool.listlocks()
    output = capfd.readouterr().out
    assert str(first) in output and str(second) in output and str(joined) not in output
    assert tool.unlock()
    assert tool.listlocks()
    output = capfd.readouterr().out
    assert str(first) not in output and str(second) not in output
    tool.done()
