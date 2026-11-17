"""`casars.tools.table`: the cases Astra's review of #1039 raised for `copy`,
`copyrows` and the calls that move a table's lock (#1025's table part, #1028).

What CASA 6.7.6.14 does in the cases it can be asked is replayed from
`data/casa_table_tool.json` (`copyrows_order`, `copyrows_types`,
`self_copies`, `flush_publishes`, `clearlocks`, in `test_tools_table.py`).
These tests hold the rest: the places where casa-rs refuses or does more than
CASA does, to keep a table from being lost, and the cases that need two
processes or a table CASA wrote.
"""

from __future__ import annotations

import shutil
import stat
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

from casars import tools

from _casa_tools import DATA
from test_tools_table import HOLDER, encode_value, fixtures, table_files  # noqa: F401 - `fixtures` is a fixture

READER = """
import sys
import time
from casars import tools

tool = tools.table()
assert tool.open(sys.argv[1])
print("held", flush=True)
time.sleep(float(sys.argv[2]))
"""


def fixture_copy(fixtures: dict[str, Path], where: Path, name: str) -> Path:  # noqa: F811
    path = where.resolve() / name
    shutil.copytree(fixtures["table"], path)
    return path


def staging_leftovers(directory: Path) -> list[str]:
    """Names of the half-made copies a failed or finished copy left beside its target."""
    return [path.name for path in directory.iterdir() if ".copying-" in path.name]


def test_a_link_cannot_hide_a_copy_inside_the_table_or_the_table_inside_the_copy(
    fixtures: dict[str, Path], tmp_path: Path  # noqa: F811
) -> None:
    """Only the places the names resolve to are compared: a link to a table
    that lies inside another must not let a copy onto the other remove both."""
    parent = fixture_copy(fixtures, tmp_path, "parent.table")
    child = parent / "child.table"
    shutil.copytree(fixtures["table"], child)
    alias = tmp_path.resolve() / "source_alias"
    alias.symlink_to(child)
    alias_of_parent = tmp_path.resolve() / "parent_alias"
    alias_of_parent.symlink_to(parent)
    kept = table_files(parent)
    tool = tools.table()
    assert tool.open(str(alias))
    # The table is inside the copy's name.
    with pytest.raises(RuntimeError, match="one inside the other"):
        tool.copy(str(parent))
    with pytest.raises(RuntimeError, match="one inside the other"):
        tool.copy(str(alias_of_parent))
    # The copy is inside the table, by a name that does not exist yet.
    with pytest.raises(RuntimeError, match="one inside the other"):
        tool.copy(str(alias / "inner.table"))
    assert table_files(parent) == kept
    assert not (child / "inner.table").exists()
    assert staging_leftovers(tmp_path) == []
    # A link to the table itself is the table: a copy onto it changes nothing.
    again = tool.copy(str(alias))
    assert again.nrows() == 3
    again.done()
    assert table_files(parent) == kept
    tool.done()


@pytest.mark.parametrize("held", ["writer", "reader"])
def test_a_table_another_tool_has_open_is_not_replaced(
    held: str, fixtures: dict[str, Path], tmp_path: Path  # noqa: F811
) -> None:
    """The tool would go on writing its rows into the copy that replaced its
    table (casacore refuses: the target is in its table cache)."""
    source = fixture_copy(fixtures, tmp_path, "source.table")
    target = fixture_copy(fixtures, tmp_path, "target.table")
    keeper = tools.table()
    assert keeper.open(str(target), nomodify=held == "reader")
    if held == "writer":
        assert keeper.putcell("id", 0, 99)
    kept = table_files(target)
    tool = tools.table()
    assert tool.open(str(source))
    with pytest.raises(RuntimeError, match="is still open"):
        tool.copy(str(target))
    with pytest.raises(RuntimeError, match="is still open"):
        tool.copy(str(target), norows=True)
    assert table_files(target) == kept
    assert staging_leftovers(tmp_path) == []
    if held == "writer":
        assert keeper.putcell("id", 1, 98)
        assert keeper.getcell("id", 1) == 98
    keeper.close()
    copied = tool.copy(str(target))
    assert copied.getcell("id", 0) == 1
    # The tool the copy returned has the table open in its turn.
    with pytest.raises(RuntimeError, match="is still open"):
        tool.copy(str(target))
    copied.done()
    assert tool.copy(str(target)).done()
    tool.done()


def test_a_table_another_process_has_open_is_not_replaced(
    fixtures: dict[str, Path], tmp_path: Path  # noqa: F811
) -> None:
    """casacore checks its own table cache only; a table another process has
    open would be replaced under its feet."""
    source = fixture_copy(fixtures, tmp_path, "source.table")
    target = fixture_copy(fixtures, tmp_path, "target.table")
    tool = tools.table()
    assert tool.open(str(source))
    holder = subprocess.Popen(
        [sys.executable, "-c", READER, str(target), "5"], stdout=subprocess.PIPE, text=True
    )
    try:
        assert holder.stdout is not None and holder.stdout.readline().strip() == "held"
        kept = table_files(target)
        with pytest.raises(RuntimeError, match="open in another process"):
            tool.copy(str(target))
        assert table_files(target) == kept
        assert staging_leftovers(tmp_path) == []
    finally:
        holder.wait(timeout=60)
    assert tool.copy(str(target)).done()
    tool.done()


def test_resync_refreshes_a_handle_that_keeps_its_read_lock(
    fixtures: dict[str, Path], tmp_path: Path  # noqa: F811
) -> None:
    """A handle that holds its read lock does not look at the table again
    until it is told to: `resync` (and `datachanged`, which reads the counter
    the lock file publishes) see what another tool of the process has written."""
    source = fixture_copy(fixtures, tmp_path, "source.table")
    out = tmp_path.resolve() / "out.table"
    maker = tools.table()
    assert maker.open(str(source))
    maker.copy(str(out), norows=True).done()
    maker.done()
    reader = tools.table()
    assert reader.open(str(out))
    assert reader.nrows() == 0
    assert reader.haslock(write=False)
    writer = tools.table()
    assert writer.open(str(source))
    assert writer.copyrows(str(out))
    assert reader.nrows() == 0
    assert reader.datachanged()
    assert not reader.datachanged()
    assert reader.resync()
    assert reader.nrows() == 3
    assert reader.haslock(write=False), "resync keeps the lock it found"
    writer.done()
    reader.done()


def test_resync_that_cannot_lock_the_table_says_so(
    fixtures: dict[str, Path], tmp_path: Path  # noqa: F811
) -> None:
    """With the table write-locked by another process and one attempt allowed,
    nothing could be synchronized: the call fails where it would have answered
    True."""
    path = fixture_copy(fixtures, tmp_path, "python_fixture.table")
    tool = tools.table()
    assert tool.open(str(path), lockoptions={"option": "user", "maxwait": 1})
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(path), "4"], stdout=subprocess.PIPE, text=True
    )
    try:
        assert holder.stdout is not None and holder.stdout.readline().strip() == "held"
        with pytest.raises(RuntimeError, match="could not be locked"):
            tool.resync()
        assert not tool.haslock(write=False)
    finally:
        holder.wait(timeout=60)
    assert tool.resync()
    tool.done()


def test_clearlocks_of_a_tool_with_no_table_releases_the_automatic_locks_of_every_tool(
    fixtures: dict[str, Path], tmp_path: Path  # noqa: F811
) -> None:
    """casacore's `relinquishAutoLocks` reaches every table of the process; a
    lock the user took stays."""
    first = tools.table()
    second = tools.table()
    kept = tools.table()
    assert first.open(str(fixture_copy(fixtures, tmp_path, "first.table")))
    assert second.open(str(fixture_copy(fixtures, tmp_path, "second.table")))
    assert kept.open(
        str(fixture_copy(fixtures, tmp_path, "kept.table")), lockoptions={"option": "user"}
    )
    assert kept.lock(write=False)
    first.getcell("id", 0)
    second.getcell("id", 0)
    assert first.haslock(write=False) and second.haslock(write=False)
    assert tools.table().clearlocks()
    assert not first.haslock(write=False)
    assert not second.haslock(write=False)
    assert kept.haslock(write=False)
    # A tool that is read from takes its lock again.
    first.getcell("id", 1)
    assert first.haslock(write=False)
    for tool in (first, second, kept):
        tool.done()


@pytest.fixture
def reference_tables(tmp_path: Path) -> Path:
    """`ref.table`, a persisted selection of two rows of the big-endian
    `big.table` beside it, written by CASA 6.7.6.14 (`tb.selectrows`)."""
    with tarfile.open(DATA / "reftable_big.tgz", "r:gz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path.resolve(), **extract)
    return tmp_path.resolve() / "reftable"


def test_a_reference_table_reports_the_byte_order_of_the_table_it_refers_to(
    reference_tables: Path,
) -> None:
    tool = tools.table()
    assert tool.open(str(reference_tables / "ref.table"))
    assert tool.nrows() == 2
    assert [tool.getcell("id", row) for row in range(2)] == [1, 3]
    assert tool.endianformat() == "big"
    tool.done()


def test_reference_rownumbers_return_the_parent_rows(
    reference_tables: Path,
) -> None:
    tool = tools.table()
    assert tool.open(str(reference_tables / "ref.table"))
    assert tool.rownumbers().tolist() == [0, 2]
    tool.done()


def test_simple_reference_description_preserves_root_bindings(reference_tables: Path) -> None:
    """CASA's simple reference retains the root's declared and actual bindings."""
    parent, view = tools.table(), tools.table()
    parent_path = str(reference_tables / "big.table")
    view_path = str(reference_tables / "ref.table")
    assert parent.open(parent_path)
    assert view.open(view_path)
    for actual in (True, False):
        expected = encode_value(parent.getdesc(actual), parent_path)
        assert encode_value(view.getdesc(actual), view_path) == expected
    assert encode_value(view.getcoldesc("id"), view_path) == encode_value(parent.getcoldesc("id"), parent_path)
    view.done()
    parent.done()


def test_reference_description_distinguishes_declared_and_actual_managers(tmp_path: Path) -> None:
    """CASA bound StManAipsIO to a column declaring StandardStMan."""
    with tarfile.open(DATA / "reftable_declared.tgz", "r:gz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    root = tmp_path / "reftable-declared"
    for name in ("parent.table", "ref.table"):
        tool = tools.table()
        assert tool.open(str(root / name))
        actual, declared = tool.getdesc(True)["id"], tool.getdesc(False)["id"]
        assert (actual["dataManagerType"], actual["dataManagerGroup"]) == ("StManAipsIO", "")
        assert (declared["dataManagerType"], declared["dataManagerGroup"]) == ("StandardStMan", "StandardStMan")
        tool.done()


def test_projected_reference_descriptions_refuse_instead_of_inventing_bindings(tmp_path: Path) -> None:
    """CASA wrote a reference that projects the parent's id and label columns."""
    with tarfile.open(DATA / "reftable_projected.tgz", "r:gz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    tool = tools.table()
    assert tool.open(str(tmp_path / "reftable-projected" / "projected.table"))
    with pytest.raises(NotImplementedError, match="reference"):
        tool.getdesc()
    with pytest.raises(NotImplementedError, match="reference"):
        tool.getcoldesc("id")
    tool.done()


def test_nested_reference_rownumbers_keep_order_and_duplicates(tmp_path: Path) -> None:
    """CASA 6.7.6.14 wrote views [2, 0, 2] and then [2, 1] of that view."""
    with tarfile.open(DATA / "reftable_nested.tgz", "r:gz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    root = tmp_path / "reftable-nested"
    for name, expected in (("parent.table", [0, 1, 2]), ("view.table", [2, 0, 2]),
                           ("nested.table", [2, 0])):
        tool = tools.table()
        assert tool.open(str(root / name))
        assert tool.rownumbers().tolist() == expected
        tool.done()


def test_nested_reference_descriptions_match_casa(tmp_path: Path) -> None:
    with tarfile.open(DATA / "reftable_nested.tgz", "r:gz") as archive:
        extract = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(tmp_path, **extract)
    root = tmp_path / "reftable-nested"
    for name in ("view.table", "nested.table"):
        tool = tools.table()
        assert tool.open(str(root / name))
        for actual in (True, False):
            desc = tool.getdesc(actual)
            assert desc["_define_hypercolumn_"] == {}
            assert desc["_keywords_"] == {}
            assert desc["_private_keywords_"] == {}
            assert desc["id"]["dataManagerType"] == "StandardStMan"
            assert desc["id"]["dataManagerGroup"] == "StandardStMan"
        tool.done()


def test_a_plain_copy_of_a_reference_table_is_still_a_reference_table(
    reference_tables: Path,
) -> None:
    """As CASA's: the directory is copied, and the copy refers to the same
    table. (`deep` alone changes nothing for a plain table; for a reference
    table it would make a table of its own: see the next test.)"""
    tool = tools.table()
    assert tool.open(str(reference_tables / "ref.table"))
    copied = tool.copy(str(reference_tables / "copy.table"))
    files = {path.name for path in (reference_tables / "copy.table").iterdir()}
    assert files - {"table.lock"} == {"table.dat", "table.info"}
    assert copied.nrows() == 2
    assert copied.endianformat() == "big"
    assert [copied.getcell("id", row) for row in range(2)] == [1, 3]
    copied.done()
    tool.done()


@pytest.mark.parametrize(
    "options", [{"deep": True}, {"valuecopy": True}, {"norows": True}, {"deep": True, "norows": True}]
)
def test_a_copy_that_would_make_a_table_of_its_own_of_a_reference_table_changes_nothing(
    options: dict[str, bool], reference_tables: Path
) -> None:
    """CASA makes a plain table of it; casa-tables cannot write the table CASA
    would, so the copy is refused before the target is touched."""
    tool = tools.table()
    assert tool.open(str(reference_tables / "ref.table"))
    existing = reference_tables / "existing.table"
    shutil.copytree(reference_tables / "big.table", existing)
    kept = table_files(existing)
    with pytest.raises(NotImplementedError, match="reference table"):
        tool.copy(str(reference_tables / "new.table"), **options)
    with pytest.raises(NotImplementedError, match="reference table"):
        tool.copy(str(existing), **options)
    assert not (reference_tables / "new.table").exists()
    assert table_files(existing) == kept
    tool.done()


def test_a_copy_of_files_the_owner_cannot_write_can_be_updated(
    fixtures: dict[str, Path], tmp_path: Path  # noqa: F811
) -> None:
    """casacore's `Directory::copy` adds the owner's write permission to the
    files it copies (a 0444 file is 0644 in the copy, as CASA 6.7.6.14 does);
    the original is left as it was."""
    path = fixture_copy(fixtures, tmp_path, "readonly.table")
    for file in path.iterdir():
        if file.is_file():
            file.chmod(0o444)
    tool = tools.table()
    assert tool.open(str(path))
    target = tmp_path.resolve() / "writable.table"
    tool.copy(str(target)).done()
    for file in target.iterdir():
        if file.is_file():
            assert stat.S_IMODE(file.stat().st_mode) == 0o644, file.name
    for file in path.iterdir():
        if file.is_file() and file.name != "table.lock":
            assert stat.S_IMODE(file.stat().st_mode) == 0o444, file.name
    writer = tools.table()
    assert writer.open(str(target), nomodify=False)
    assert writer.putcell("id", 0, 9)
    assert writer.getcell("id", 0) == 9
    writer.done()
    tool.done()


def test_a_copy_that_fails_part_way_leaves_the_table_it_was_to_replace(
    fixtures: dict[str, Path], tmp_path: Path  # noqa: F811
) -> None:
    """The copy is made beside the target and put in its place when complete:
    casacore removes the target first, and a copy that fails then loses it."""
    source = fixture_copy(fixtures, tmp_path, "source.table")
    target = fixture_copy(fixtures, tmp_path, "target.table")
    maker = tools.table()
    assert maker.open(str(target), nomodify=False)
    assert maker.putcell("id", 0, 77)
    maker.done()
    kept = table_files(target)
    tool = tools.table()
    assert tool.open(str(source))
    unreadable = source / "table.f0"
    unreadable.chmod(0)
    try:
        with pytest.raises(RuntimeError):
            tool.copy(str(target))
    finally:
        unreadable.chmod(0o644)
    assert table_files(target) == kept
    assert staging_leftovers(tmp_path) == []
    tool.done()


def test_a_copyrows_refused_for_the_type_of_a_column_changes_nothing(
    fixtures: dict[str, Path], tmp_path: Path  # noqa: F811
) -> None:
    """CASA adds the rows first and fails at the first cell, leaving the rows
    it added with cells nobody set (`copyrows_types` shows its refusals);
    casa-rs refuses before the output is touched."""
    source = fixture_copy(fixtures, tmp_path, "source.table")
    out = tmp_path.resolve() / "out.table"
    maker = tools.table()
    assert maker.open(str(source))
    maker.copy(str(out), norows=True).done()
    maker.done()
    editor = tools.table()
    assert editor.open(str(out), nomodify=False)
    assert editor.removecols(["gain"])
    assert editor.addcols({"gain": {"valueType": "double"}})
    editor.done()
    kept = table_files(out)
    tool = tools.table()
    assert tool.open(str(source))
    with pytest.raises(RuntimeError, match="RecordInterface::asDouble - invalid data type"):
        tool.copyrows(str(out))
    assert table_files(out) == kept
    reader = tools.table()
    assert reader.open(str(out))
    assert reader.nrows() == 0
    reader.done()
    tool.done()


def test_a_copy_onto_the_name_of_a_table_open_for_update_under_permanent_locking_is_refused(
    fixtures: dict[str, Path], tmp_path: Path  # noqa: F811
) -> None:
    """CASA's second tool shares the first's write lock; here it would have to
    take a write lock the first holds."""
    path = fixture_copy(fixtures, tmp_path, "python_fixture.table")
    tool = tools.table()
    assert tool.open(str(path), nomodify=False, lockoptions={"option": "permanent"})
    with pytest.raises(NotImplementedError, match="permanent locking"):
        tool.copy(str(path))
    tool.done()


def test_a_copyrows_of_an_integer_that_does_not_fit_the_column_is_refused_where_casa_wraps_it(
    fixtures: dict[str, Path], tmp_path: Path  # noqa: F811
) -> None:
    """CASA 6.7.6.14 puts a Short -5 into a uChar column as 251, as it wraps an
    int beyond 32 bits in `putcell`; casa-rs refuses the value, before the
    output is touched, rather than store a wrong number."""
    source = fixture_copy(fixtures, tmp_path, "source.table")
    out = tmp_path.resolve() / "out.table"
    maker = tools.table()
    assert maker.open(str(source), nomodify=False)
    assert maker.putcell("id", 1, 2**40)
    maker.copy(str(out), norows=True).done()
    editor = tools.table()
    assert editor.open(str(out), nomodify=False)
    assert editor.removecols(["id"])
    assert editor.addcols({"id": {"valueType": "int"}})
    editor.done()
    kept = table_files(out)
    with pytest.raises(RuntimeError, match="out of range for a column of Int"):
        maker.copyrows(str(out))
    assert table_files(out) == kept
    # Rows that fit are copied.
    assert maker.copyrows(str(out), 0, -1, 1)
    maker.done()
    reader = tools.table()
    assert reader.open(str(out))
    assert reader.nrows() == 1 and reader.getcell("id", 0) == 1
    reader.done()
