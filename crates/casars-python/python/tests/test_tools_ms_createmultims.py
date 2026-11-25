"""MultiMS publication, relocation and readback through both tool packages."""

from shutil import copytree

import numpy as np
import pytest

from casars import tools
from test_tools_msmetadata import measures, native_dataset


@pytest.fixture(params=["casa", "native"])
def creator(request):
    if request.param == "native":
        return tools
    return pinned_casa()


def pinned_casa():
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    return casa


def members(dataset, scratch):
    scratch.mkdir()
    paths = [scratch / "first.ms", scratch / "second.ms"]
    for path in paths:
        copytree(dataset, path)
    return paths


def edit_antenna(api, path, name):
    table = api.table()
    assert table.open(str(path / "ANTENNA"), nomodify=False)
    assert table.putcell("NAME", 0, name)
    assert table.done()


@pytest.mark.parametrize("output_kind", ["absent", "empty", "existing"])
def test_publication_and_relocated_readback(native_dataset, tmp_path, creator, output_kind):
    source = creator.table()
    assert source.open(str(native_dataset))
    nrows = source.nrows()
    expected_data = source.getcell("DATA", 1)
    expected_flag = source.getcell("FLAG", 1)
    assert source.done()
    parts = members(native_dataset, tmp_path / "parts")
    output = tmp_path / "out.ms"
    if output_kind == "empty":
        output.mkdir()
    elif output_kind == "existing":
        copytree(native_dataset, output)
    tool = creator.ms()
    assert tool.createmultims(
        outputTableName=bytes(output), tables=list(map(str, parts)), subtables=[],
        nomodify=False, lock=True,
    )
    assert tool.name() == str(output)
    assert tool.ismultims()
    assert tool.iswritable()
    assert tool.nrow() == 2 * nrows
    assert tool.getreferencedtables() == [str(output / "SUBMSS" / part.name) for part in parts]
    assert all(not part.exists() for part in parts)
    assert tool.done()
    relocated = output.rename(tmp_path / "relocated.ms")
    for api in (pinned_casa(), tools):
        reader = api.ms()
        assert reader.open(str(relocated))
        assert reader.ismultims()
        assert not reader.iswritable()
        assert reader.nrow() == 2 * nrows
        assert reader.getreferencedtables() == [str(relocated / "SUBMSS" / part.name) for part in parts]
        assert reader.done()
        table = api.table()
        assert table.open(str(relocated))
        assert table.getcoldesc("DATA")["valueType"] == "complex"
        for row in (1, nrows + 1):
            np.testing.assert_array_equal(table.getcell("DATA", row), expected_data, strict=True)
            np.testing.assert_array_equal(table.getcell("FLAG", row), expected_flag, strict=True)
        assert table.done()


@pytest.mark.parametrize("omit", [[], "ANTENNA"])
def test_subtable_copy_and_join(native_dataset, tmp_path, creator, omit):
    first, second = members(native_dataset, tmp_path / "parts")
    edit_antenna(creator, first, "FIRST")
    edit_antenna(creator, second, "SECOND")
    output = tmp_path / "out.ms"
    tool = creator.ms()
    assert tool.createmultims(
        str(output), [str(first), str(second)], "ANTENNA",
        copysubtables=True, omitsubtables=omit,
    )
    assert tool.done()
    relocated = output.rename(tmp_path / "relocated.ms")
    for api in (pinned_casa(), tools):
        reader = api.ms()
        assert reader.open(str(relocated))
        metadata = reader.metadata()
        assert metadata.nantennas() == 8
        assert metadata.done()
        assert reader.done()
        for member, expected in ((first, "FIRST"), (second, "SECOND" if omit else "FIRST")):
            table = api.table()
            assert table.open(str(relocated / "SUBMSS" / member.name / "ANTENNA"))
            assert table.getcell("NAME", 0) == expected
            assert table.done()


@pytest.mark.parametrize("form", ["scalar", "repeated", "tuple", "none", "ndarray"])
def test_member_argument_forms(native_dataset, tmp_path, creator, form):
    first, _ = members(native_dataset, tmp_path / "parts")
    output = tmp_path / "out.ms"
    tool = creator.ms()
    values = {
        "scalar": str(first),
        "repeated": [str(first), str(first)],
        "tuple": (str(first),),
        "none": None,
        "ndarray": np.array([str(first)]),
    }[form]
    if form in ("scalar", "repeated"):
        assert tool.createmultims(str(output), values, None)
        member = str(output / "SUBMSS" / first.name)
        assert tool.getreferencedtables() == [member] * (2 if form == "repeated" else 1)
        assert not first.exists()
        assert tool.done()
    else:
        assert tool.createmultims(str(output), values, []) is False
        assert first.is_dir()
        assert not output.exists()


def test_arguments_and_failed_creation_keep_attachment(native_dataset, tmp_path, creator):
    first, second = members(native_dataset, tmp_path / "parts")
    tool = creator.ms()
    assert tool.open(str(native_dataset))
    output = tmp_path / "out.ms"
    for args, kwargs in (
        ((output, [str(first)], []), {}),
        ((str(output), [str(first), 1], []), {}),
        ((str(output), [str(first)]), {}),
        ((str(output), [str(first)], []), {"lock": 1}),
        ((str(output), [str(first)], []), {"omitsubtables": ["ANTENNA", 1]}),
    ):
        with pytest.raises(TypeError):
            tool.createmultims(*args, **kwargs)
    assert tool.createmultims(str(output), list(map(str, (first, second))), ["ABSENT"]) is False
    assert tool.createmultims(str(output), [str(first), str(tmp_path / "missing.ms")], []) is False
    assert tool.name() == str(native_dataset)
    assert tool.nrow() > 0
    assert first.is_dir() and second.is_dir()
    assert not output.exists()
    assert tool.done()


def test_conflicting_paths_preserve_originals(native_dataset, tmp_path):
    first, second = members(native_dataset, tmp_path / "parts")
    collision = tmp_path / first.name
    second.rename(collision)
    output = tmp_path / "existing"
    output.mkdir()
    sentinel = output / "keep.txt"
    sentinel.write_bytes(b"existing unrelated data")
    tool = tools.ms()
    assert tool.createmultims(str(output), [str(first), str(collision)], []) is False
    assert first.is_dir() and collision.is_dir()
    assert sentinel.read_bytes() == b"existing unrelated data"
    assert tool.createmultims(str(output), [str(first)], []) is False
    assert first.is_dir()
    assert sentinel.read_bytes() == b"existing unrelated data"
    new = tmp_path / "new.ms"
    assert tool.createmultims(str(new), [str(first), str(collision)], []) is False
    assert first.is_dir() and collision.is_dir()
    assert not new.exists()


@pytest.mark.parametrize("layout", ["shape", "reorder"])
def test_member_columns_with_different_shapes(native_dataset, tmp_path, creator, layout):
    casa = pinned_casa()
    parts = members(native_dataset, tmp_path / "parts")
    rows = []
    for index, part in enumerate(parts, start=1):
        table = casa.table()
        assert table.open(str(part), nomodify=False)
        length = index if layout == "shape" else 1
        columns = [
            ("CUSTOM", {"valueType": "float", "ndim": 1, "shape": [length], "option": 4}),
            ("EXTRA", {"valueType": "int"}),
        ]
        if layout == "reorder" and index == 2:
            columns.reverse()
        for name, description in columns:
            assert table.addcols({name: description})
        values = np.full(length, float(index), dtype=np.float32)
        assert table.putcell("CUSTOM", 0, values)
        rows.append((table.nrows(), values.astype(np.float64)))
        assert table.done()
    output = tmp_path / "out.ms"
    tool = creator.ms()
    assert tool.createmultims(str(output), list(map(str, parts)), [])
    assert tool.done()
    for api in (casa, tools):
        table = api.table()
        assert table.open(str(output))
        description = table.getcoldesc("CUSTOM")
        assert description["valueType"] == "float"
        assert description["ndim"] == -1
        assert description["option"] == 0
        for row, (_, expected) in zip((0, rows[0][0]), rows, strict=True):
            np.testing.assert_array_equal(table.getcell("CUSTOM", row), expected, strict=True)
        assert table.done()


@pytest.mark.parametrize(
    ("external", "copy"),
    [(False, False), (True, False), (False, True), (True, True)],
)
def test_saved_reference_member(native_dataset, tmp_path, creator, external, copy):
    casa = pinned_casa()
    first = tmp_path / "first.ms"
    copytree(native_dataset, first)
    parent = native_dataset if external else first
    view_name = tmp_path / "selected.ms"
    table = casa.table()
    assert table.open(str(parent))
    view = table.selectrows([2, 0, 2], str(view_name))
    assert view.done()
    expected = [table.getcell("DATA", row) for row in (2, 0, 2)]
    offset = table.nrows()
    assert table.done()
    output = tmp_path / "out.ms"
    tool = creator.ms()
    if external and creator is casa:
        pytest.skip("CASA moves the member and strands its relative parent link")
    assert tool.createmultims(
        str(output), [str(first), str(view_name)], [], copysubtables=copy,
    )
    assert tool.nrow() == offset + 3
    assert tool.done()
    assert not first.exists()
    assert not view_name.exists()
    relocated = output.rename(tmp_path / "relocated.ms")
    for api in (casa, tools):
        table = api.table()
        assert table.open(str(relocated))
        for row, value in enumerate(expected, start=offset):
            np.testing.assert_array_equal(table.getcell("DATA", row), value, strict=True)
        assert table.done()
        if copy and not external:
            # The member view still shares its parent's keyword set. The
            # ANTENNA copy inside the selected member serves both tables.
            link = "Table: " + str(relocated / "SUBMSS" / view_name.name / "ANTENNA")
            for member in (first, view_name):
                assert table.open(str(relocated / "SUBMSS" / member.name))
                assert table.getkeyword("ANTENNA") == link
                assert table.done()
        if copy and external:
            assert table.open(str(parent))
            assert table.getkeyword("ANTENNA") == "Table: " + str(parent / "ANTENNA")
            assert table.done()


def test_relative_member_symlink_and_existing_attachment(native_dataset, tmp_path):
    first, second = members(native_dataset, tmp_path / "parts")
    alias = second.with_name("alias.ms")
    alias.symlink_to(second.name, target_is_directory=True)
    output = tmp_path / "out.ms"
    tool = tools.ms()
    assert tool.open(str(first), nomodify=False, lock=True)
    assert tool.createmultims(str(output), [str(first), str(alias)], [], lock=True)
    assert tool.nrow() > 0
    assert tool.done()
    assert not alias.is_symlink()
    assert second.is_dir()
    published = output / "SUBMSS" / alias.name
    assert published.is_symlink() and published.resolve() == second.resolve()
    for api in (pinned_casa(), tools):
        reader = api.ms()
        assert reader.open(str(output))
        assert reader.ismultims()
        assert reader.done()


def test_subtable_copy_preserves_nested_multims(native_dataset, tmp_path, creator):
    first, second = members(native_dataset, tmp_path / "parts")
    third = second.with_name("third.ms")
    copytree(native_dataset, third)
    edit_antenna(creator, first, "FIRST")
    edit_antenna(creator, second, "SECOND")
    inner = tmp_path / "inner.ms"
    tool = creator.ms()
    assert tool.createmultims(str(inner), [str(second), str(third)], [])
    assert tool.done()
    output = tmp_path / "out.ms"
    assert tool.createmultims(
        str(output), [str(first), str(inner)], [], copysubtables=True,
    )
    assert tool.done()
    nested = output / "SUBMSS" / inner.name
    for api in (pinned_casa(), tools):
        tool = api.ms()
        assert tool.open(str(nested))
        assert tool.ismultims()
        assert tool.getreferencedtables() == [
            str(nested / "SUBMSS" / part.name) for part in (second, third)
        ]
        assert tool.done()
        table = api.table()
        assert table.open(str(nested))
        inherited = nested / "SUBMSS" / second.name / "ANTENNA"
        assert table.getkeyword("ANTENNA") == "Table: " + str(inherited)
        assert table.done()
        for path, name in ((nested / "ANTENNA", "FIRST"), (inherited, "SECOND")):
            assert table.open(str(path))
            assert table.getcell("NAME", 0) == name
            assert table.done()
