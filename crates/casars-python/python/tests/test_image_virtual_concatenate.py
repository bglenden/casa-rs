"""Native imageconcat manifest interoperability and strided access."""

import json
import os
import subprocess
import sys

import numpy as np
import pytest

from test_image_removal import _backend
from test_image_cross_device_rename import other_filesystem


KINDS = [
    ("float", np.float32, "f", "float"),
    ("double", np.float64, "d", "double"),
    ("complex", np.complex64, "f", "Complex"),
    ("dcomplex", np.complex128, "d", "DComplex"),
]


@pytest.mark.parametrize(("kind", "dtype", "precision", "stored_type"), KINDS, ids=[row[0] for row in KINDS])
@pytest.mark.parametrize("tempclose", [False, True])
@pytest.mark.parametrize("writer", ["casa", "casars"])
@pytest.mark.parametrize("reader", ["casa", "casars"])
@pytest.mark.parametrize("mode", ["nomove", "copy", "move"])
def test_virtual_manifest_and_write_through(tmp_path, kind, dtype, precision, stored_type, tempclose, writer, reader, mode):
    writer_api, reader_api = _backend(writer), _backend(reader)
    source = writer_api.image()
    reader_tool, operand = reader_api.image(), reader_api.image()
    paths = [tmp_path / "left.im", tmp_path / "right.im"]
    output = tmp_path / "joint.im"
    members = paths if mode == "nomove" else [output / path.name for path in paths]
    joined = None
    factor = 1. + 2.j if np.issubdtype(dtype, np.complexfloating) else 1.
    expected = np.concatenate([np.full((3, 4), factor), np.full((3, 4), 2 * factor)])
    try:
        for number, path in enumerate(paths, start=1):
            assert source.fromarray(str(path), np.full((3, 4), number * factor, dtype=dtype), linear=True, type=precision)
            assert source.done()
        originals = [{entry.name: entry.stat() for entry in path.iterdir() if entry.is_file()}
                     for path in paths]
        original_dirs = [path.stat() for path in paths]
        joined = source.imageconcat(str(output), list(map(str, paths)), axis=0, relax=True, tempclose=tempclose, mode=mode)
        assert joined.pixeltype() == kind
        if kind in {"float", "complex"}:
            assert any("ia.imageconcat" in row for row in joined.history())
        np.testing.assert_array_equal(joined.shape(), [6, 4])
        children = [] if mode == "nomove" else members
        assert set(output.iterdir()) == {output / "imageconcat.json", *children}
        assert all(path.exists() == (mode != "move") for path in paths)
        if writer == "casars" and mode == "move":
            for directory, payload, member in zip(original_dirs, originals, members):
                assert os.path.samestat(directory, member.stat())
                assert all(os.path.samestat(stat, (member / name).stat()) for name, stat in payload.items())
        manifest = json.loads((output / "imageconcat.json").read_text())
        assert manifest == {
            "Version": 1,
            "DataType": stored_type,
            "Axis": 0,
            "TempClose": tempclose,
            "Images": [(("./" if mode == "nomove" else "././") + path.name) for path in paths],
            "MiscInfo": {},
        }
        assert joined.done()
        assert reader_tool.open(str(output))
        assert reader_tool.pixeltype() == kind
        if kind in {"float", "complex"}:
            assert reader_tool.ispersistent()
            assert not any("ia.imageconcat" in row for row in reader_tool.history())
            np.testing.assert_array_equal(reader_tool.haslock(), [not tempclose, False])
        else:
            with pytest.raises(RuntimeError, match="ispersistent does not support"):
                reader_tool.ispersistent()
        subsection = reader_tool.getchunk([1, 0], [5, 3], [2, 1])
        assert subsection.dtype == expected.dtype
        np.testing.assert_array_equal(subsection, expected[1:6:2, :])
        values = np.full((3, 1), 9 * factor, dtype=dtype)
        assert reader_tool.putchunk(values, blc=[1, 0], inc=[2, 1])
        expected[1:6:2, 0] = 9 * factor
        np.testing.assert_array_equal(reader_tool.getchunk(), expected)
        if kind in {"float", "complex"}:
            assert reader_tool.unlock()
            np.testing.assert_array_equal(reader_tool.haslock(), [False, False])
            assert reader_tool.lock(True, 1)
            np.testing.assert_array_equal(reader_tool.haslock(), [not tempclose, not tempclose])
            np.testing.assert_array_equal(reader_tool.getchunk(), expected)
            np.testing.assert_array_equal(reader_tool.haslock(), [not tempclose, not tempclose])
            assert reader_tool.unlock()
        assert reader_tool.setmiscinfo({"intent": "write-through"})
        if kind in {"float", "complex"}:
            np.testing.assert_array_equal(reader_tool.haslock(), [tempclose, False])
        assert reader_tool.done()
        assert json.loads((output / "imageconcat.json").read_text())["MiscInfo"] == {
            "intent": "write-through",
        }
        assert joined.open(str(output))
        if kind in {"float", "complex"}:
            assert joined.miscinfo() == {"intent": "write-through"}
        else:
            with pytest.raises(RuntimeError, match="miscinfo does not support"):
                joined.miscinfo()
        np.testing.assert_array_equal(joined.getchunk(), expected)
        assert joined.done()
        for index, path in enumerate(members):
            assert operand.open(str(path))
            np.testing.assert_array_equal(operand.getchunk(), expected[index * 3:(index + 1) * 3, :])
            assert operand.done()
        if mode == "copy":
            for number, path in enumerate(paths, start=1):
                assert operand.open(str(path))
                np.testing.assert_array_equal(operand.getchunk(), np.full((3, 4), number * factor))
                assert operand.done()
    finally:
        if joined is not None:
            joined.done()
        operand.done()
        reader_tool.done()
        source.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("tempclose", [False, True])
def test_virtual_concat_keeps_mixed_mask_stride(tmp_path, backend, tempclose):
    api = _backend(backend)
    source = api.image()
    paths = [tmp_path / "masked.im", tmp_path / "valid.im"]
    output = tmp_path / "joint.im"
    joined = None
    try:
        for number, path in enumerate(paths):
            assert source.fromarray(str(path), np.full((3, 4), number + 1.), linear=True)
            if number == 0:
                assert source.calcmask("F", "invalid")
            assert source.done()
        joined = source.imageconcat(str(output), list(map(str, paths)), axis=0, relax=True, tempclose=tempclose, mode="nomove")
        validity = np.ones((3, 4), dtype=bool)
        validity[0, :] = False
        np.testing.assert_array_equal(
            joined.getchunk([1, 0], [5, 3], [2, 1], getmask=True), validity,
        )
        assert joined.done()
        assert source.open(str(output))
        np.testing.assert_array_equal(
            source.getchunk([1, 0], [5, 3], [2, 1], getmask=True), validity,
        )
    finally:
        if joined is not None:
            joined.done()
        source.done()


def test_virtual_concat_rejects_an_active_graph_cycle(tmp_path):
    api = _backend("casars")
    first = tmp_path / "first.im"
    second = tmp_path / "second.im"
    for root, source in ((first, second), (second, first)):
        root.mkdir()
        (root / "imageconcat.json").write_text(json.dumps({
            "DataType": "float",
            "Axis": 0,
            "Images": [f"./{source.name}", f"./{source.name}"],
        }))
    tool = api.image()
    try:
        with pytest.raises(RuntimeError, match="Cycle in virtual image concatenation"):
            tool.open(str(first))
        assert not tool.isopen()
    finally:
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("mode", ["nomove", "copy"])
def test_virtual_concat_repeated_operand_reads_one_table(tmp_path, backend, mode):
    api = _backend(backend)
    a, b = tmp_path / "a.im", tmp_path / "b.im"
    image, out = api.image(), None
    try:
        assert image.fromarray(str(a), np.ones((3, 4)), linear=True)
        assert image.done()
        assert image.fromarray(str(b), np.full((3, 4), 2.), linear=True)
        assert image.done()
        output = tmp_path / "repeat.im"
        out = image.imageconcat(str(output), list(map(str, [a, b, a])),
                                axis=0, relax=True, mode=mode, tempclose=False)
        np.testing.assert_array_equal(out.getchunk()[:, 0], [1, 1, 1, 2, 2, 2, 1, 1, 1])
        if backend == "casars":
            assert out.putchunk(np.array([[9.], [8.]]), blc=[0, 0], inc=[6, 1])
            np.testing.assert_array_equal(out.getchunk()[:, 0], [8, 1, 1, 2, 2, 2, 8, 1, 1])
    finally:
        if out is not None:
            out.done()
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("mode", ["copy", "move"])
def test_owned_virtual_concat_can_nest_a_virtual_member(tmp_path, backend, mode):
    api = _backend(backend)
    tool, inner, outer = api.image(), None, None
    members = [tmp_path / f"source-{i}.im" for i in [1, 2, 3]]
    inner_path, output = tmp_path / "inner.im", tmp_path / "outer.im"
    try:
        for number, source in enumerate(members, 1):
            assert tool.fromarray(str(source), np.full((2, 2), number), linear=True)
            assert tool.done()
        inner = tool.imageconcat(str(inner_path), list(map(str, members[:2])), axis=0,
                                 mode="copy", relax=True)
        assert inner.done()
        outer = tool.imageconcat(str(output), [str(inner_path), str(members[2])], axis=0,
                                 mode=mode, relax=True)
        assert outer.done()
        assert inner_path.exists() == (mode == "copy")
        assert tool.open(str(output))
        np.testing.assert_array_equal(tool.getchunk()[:, 0], [1, 1, 2, 2, 3, 3])
        assert tool.done()
        assert tool.open(str(output / "inner.im"))
        np.testing.assert_array_equal(tool.getchunk()[:, 0], [1, 1, 2, 2])
    finally:
        if outer is not None:
            outer.done()
        if inner is not None:
            inner.done()
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("mode", ["copy", "move"])
def test_owned_virtual_concat_handles_absolute_symlink_member(tmp_path, backend, mode):
    api = _backend(backend)
    tool, joined = api.image(), None
    physical, alias, other, output = [tmp_path / f"{n}.im" for n in ["physical", "link", "other", "join"]]
    try:
        for path, value in [(physical, 1.), (other, 2.)]:
            assert tool.fromarray(str(path), np.full((2, 2), value), linear=True)
            assert tool.done()
        alias.symlink_to(physical, target_is_directory=True)
        joined = tool.imageconcat(str(output), [str(alias), str(other)], axis=0,
                                   mode=mode, relax=True)
        assert physical.is_dir()
        assert alias.is_symlink() == (mode == "copy")
        copied = output / alias.name
        assert copied.is_symlink() == (mode == "move")
        if copied.is_symlink():
            assert copied.readlink() == physical
        np.testing.assert_array_equal(joined.getchunk()[:, 0], [1, 1, 2, 2])
        assert joined.done()
        assert tool.open(str(output))
        np.testing.assert_array_equal(tool.getchunk()[:, 0], [1, 1, 2, 2])
    finally:
        if joined is not None:
            joined.done()
        tool.done()


@pytest.mark.parametrize("mode", ["copy", "move"])
def test_owned_concat_rejects_changed_binding_of_nested_manifest(tmp_path, mode):
    api = _backend("casars")
    tool, inner = api.image(), None
    original, replacements = tmp_path / "first", tmp_path / "second"
    original.mkdir()
    replacements.mkdir()
    path, output = original / "inner.im", tmp_path / "join.im"
    originals = [original / f"{name}.im" for name in ("a", "b")]
    others = [replacements / item.name for item in originals]
    try:
        for value, member in enumerate([*originals, *others], 1):
            assert tool.fromarray(str(member), np.full((2, 2), value), linear=True)
            assert tool.done()
        inner = tool.imageconcat(str(path), list(map(str, originals)), axis=0, mode="nomove", relax=True)
        assert inner.done()
        before = {member: member.stat() for member in [path, *originals, *others]}
        with pytest.raises(RuntimeError, match="operand .* would resolve to.*different image"):
            tool.imageconcat(str(output), [str(path), *map(str, others)], axis=0, mode=mode, relax=True)
        assert not output.exists()
        assert all(os.path.samestat(stat, member.stat()) for member, stat in before.items())
        assert tool.open(str(path))
        np.testing.assert_array_equal(tool.getchunk()[:, 0], [1, 1, 2, 2])
    finally:
        if inner is not None:
            inner.done()
        tool.done()


def test_owned_concat_move_preserves_a_relative_symlink_chain(tmp_path):
    api = _backend("casars")
    tool, joined = api.image(), None
    real, first, second, output = [tmp_path / f"{n}.im" for n in ("real", "first", "second", "join")]
    try:
        assert tool.fromarray(str(real), np.full((2, 2), 6.), linear=True)
        assert tool.done()
        first.symlink_to(real.name, target_is_directory=True)
        second.symlink_to(first.name, target_is_directory=True)
        members = [first, real, second]
        joined = tool.imageconcat(str(output), list(map(str, members)), axis=0, mode="move", relax=True)
        assert not any(member.exists() or member.is_symlink() for member in members)
        assert str((output / first.name).readlink()) == real.name
        assert str((output / second.name).readlink()) == first.name
        assert joined.done()
        assert tool.open(str(output))
        np.testing.assert_array_equal(tool.getchunk(), np.full((6, 2), 6.))
    finally:
        if joined is not None:
            joined.done()
        tool.done()


@pytest.mark.parametrize("mode", ["copy", "move"])
def test_owned_concat_overwrite_respects_unlocked_table_tool(tmp_path, mode):
    api = _backend("casars")
    tool, table, joined = api.image(), api.table(), None
    first, second, output = [tmp_path / f"{n}.im" for n in ("first", "second", "join")]
    try:
        for path, value in [(first, 1.), (second, 2.)]:
            assert tool.fromarray(str(path), np.full((2, 2), value), linear=True)
            assert tool.done()
        original = tool.imageconcat(str(output), [str(first), str(second)], axis=0, mode="copy", relax=True)
        assert original.done()
        old_member = output / first.name
        original_identity = old_member.stat()
        assert table.open(str(old_member))
        table.unlock()
        with pytest.raises(RuntimeError, match="already opened"):
            joined = tool.imageconcat(str(output), [str(first), str(second)], axis=0, mode=mode,
                                       overwrite=True, relax=True)
        assert first.exists() and second.exists()
        assert os.path.samestat(original_identity, old_member.stat())
        assert table.nrows() == 1
    finally:
        if joined is not None:
            joined.done()
        table.done()
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_native_virtual_manifest_resolution_and_missing_miscinfo(tmp_path, monkeypatch, backend):
    api = _backend(backend)
    image = api.image()
    working, parent = tmp_path / "cwd", tmp_path / "parent"
    working.mkdir()
    parent.mkdir()
    output = parent / "joined.im"
    output.mkdir()
    monkeypatch.chdir(working)
    try:
        for path, value, record in [
            (working / "plain.im", 1., {"working": True}),
            (parent / "plain.im", 8., {"unused": True}),
            (parent / "sibling.im", 2., {"sibling": True}),
        ]:
            assert image.fromarray(str(path), np.full((3, 4), value), linear=True)
            assert image.setmiscinfo(record)
            assert image.done()
        path = output / "imageconcat.json"
        header = {"DataType": "float", "Axis": 0, "Images": ["plain.im", "./sibling.im"]}
        path.write_text(json.dumps(header))
        assert image.open(str(output))
        np.testing.assert_array_equal(image.haslock(), [False, False])
        np.testing.assert_array_equal(image.getchunk()[:, 0], [1, 1, 1, 2, 2, 2])
        assert image.miscinfo() == {"working": True, "sibling": True}
        assert image.done()
        path.write_text(json.dumps(dict(header, MiscInfo={})))
        assert image.open(str(output))
        assert image.miscinfo() == {}
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_virtual_concat_fits_member_is_read_only(tmp_path, backend):
    api = _backend(backend)
    a, b = tmp_path / "a.im", tmp_path / "b.fits"
    source = api.image()
    out = None
    try:
        assert source.fromarray(str(a), np.ones((3, 4)), linear=True)
        assert source.done()
        assert source.fromarray(pixels=np.full((3, 4), 2.), linear=True)
        assert source.tofits(str(b))
        assert source.done()
        out = source.imageconcat(str(tmp_path / "fit-joined.im"), list(map(str, [a, b])),
                                 axis=0, relax=True, mode="nomove")
        np.testing.assert_array_equal(out.getchunk()[:, 0], [1, 1, 1, 2, 2, 2])
        with pytest.raises(RuntimeError, match="not writable"):
            out.putchunk(np.full((3, 1), 9.), blc=[1, 0], inc=[2, 1])
        assert out.done()
        assert source.open(str(a))
        np.testing.assert_array_equal(source.getchunk(), np.ones((3, 4)))
    finally:
        if out is not None:
            out.done()
        source.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("mode", ["nomove", "copy", "move"])
def test_virtual_concat_combines_restoring_beams_on_the_spectral_axis(tmp_path, backend, mode):
    api = _backend(backend)
    source, reader = api.image(), api.image()
    paths = [tmp_path / "left.im", tmp_path / "right.im"]
    target = tmp_path / "channels.im"
    out, coordinates = None, None
    try:
        for i, path in enumerate(paths):
            assert source.fromshape(str(path), [3, 4, 1, 2])
            if i == 1:
                coordinates = source.coordsys()
                frequency = coordinates.referencevalue()["numeric"]
                frequency[-1] += 2 * coordinates.increment()["numeric"][-1]
                assert coordinates.setreferencevalue(frequency)
                assert source.setcoordsys(coordinates.torecord())
                assert coordinates.done()
            assert source.setrestoringbeam(f"{2 + i}arcsec", "1arcsec", "0deg")
            assert source.done()
        out = source.imageconcat(str(target), list(map(str, paths)), axis=3, mode=mode)
        assert out.done()
        assert reader.open(str(target))
        beams = [reader.restoringbeam(channel=channel, polarization=0) for channel in range(4)]
        qa = api.quanta()
        try:
            np.testing.assert_allclose(
                [qa.convert(beam["major"], "arcsec")["value"] for beam in beams],
                [2, 2, 3, 3],
            )
            assert reader.setrestoringbeam("4arcsec", "1arcsec", "0deg", channel=2, polarization=0)
            assert reader.done()
            member = paths[1] if mode == "nomove" else target / paths[1].name
            for path, expected in [(target, [2, 2, 4, 3]), (member, [4, 3])]:
                assert reader.open(str(path))
                beams = [reader.restoringbeam(channel=channel, polarization=0) for channel in range(len(expected))]
                np.testing.assert_allclose(
                    [qa.convert(beam["major"], "arcsec")["value"] for beam in beams], expected,
                )
                assert reader.done()
        finally:
            qa.done()
    finally:
        if out is not None:
            out.done()
        if coordinates is not None:
            coordinates.done()
        reader.done()
        source.done()


@pytest.mark.parametrize("mode", ["nomove", "copy", "move"])
def test_virtual_shape_admission_preserves_named_output_and_inputs(tmp_path, mode):
    api = _backend("casars")
    paths = [tmp_path / "left.im", tmp_path / "right.im"]
    destination = tmp_path / "previous.im"
    image = api.image()
    try:
        for path, data in [
            (paths[0], np.ones((3, 4))),
            (paths[1], np.full((3, 5), 2.)),
            (destination, np.full((2, 2), 7.)),
        ]:
            assert image.fromarray(str(path), data, linear=True)
            assert image.done()
        original_entries = [path.stat() for path in [*paths, destination]]
        with pytest.raises(RuntimeError, match="Lattice shapes inconsistent"):
            image.imageconcat(str(destination), list(map(str, paths)), axis=0,
                              mode=mode, overwrite=True, relax=True)
        assert image.open(str(destination))
        np.testing.assert_array_equal(image.getchunk(), np.full((2, 2), 7.))
        assert all(os.path.samestat(contents, path.stat())
                   for contents, path in zip(original_entries, [*paths, destination]))
        assert not (destination / "imageconcat.json").exists()
    finally:
        image.done()


@pytest.mark.parametrize("existing_output", [False, True])
def test_virtual_move_rejects_broken_link_preserving_sources(tmp_path, existing_output):
    api = _backend("casars")
    source, referent, alias, output = [
        tmp_path / f"{name}.im" for name in ("a", "referent", "alias", "join")
    ]
    tool = api.image()
    try:
        originals = [(source, 1.), (referent, 2.)]
        if existing_output:
            originals.append((output, 7.))
        for path, value in originals:
            assert tool.fromarray(str(path), np.full((2, 2), value), linear=True)
            assert tool.done()
        alias.symlink_to(referent.name, target_is_directory=True)
        before = {path: path.lstat() for path in [source, referent, alias, *([output] if existing_output else [])]}
        contents = set(tmp_path.iterdir())
        with pytest.raises(RuntimeError, match="operand .* would resolve to .*(not found|No such file)"):
            tool.imageconcat(str(output), [str(source), str(alias)], axis=0, relax=True,
                             mode="move", overwrite=existing_output)
        assert set(tmp_path.iterdir()) == contents
        assert str(alias.readlink()) == referent.name
        assert all(os.path.samestat(stat, path.lstat()) for path, stat in before.items())
        for path, value in originals:
            assert tool.open(str(path))
            np.testing.assert_array_equal(tool.getchunk(), np.full((2, 2), value))
            assert tool.done()
    finally:
        tool.done()


@pytest.mark.parametrize("mode", ["copy", "move"])
def test_owned_virtual_concat_rejects_basename_collision(tmp_path, mode):
    api = _backend("casars")
    tool = api.image()
    paths = [tmp_path / name / "member.im" for name in ("one", "two")]
    output = tmp_path / "join.im"
    try:
        for index, path in enumerate(paths):
            path.parent.mkdir()
            assert tool.fromarray(str(path), np.full((2, 2), index + 1.), linear=True)
            assert tool.done()
        before = [path.stat() for path in paths]
        with pytest.raises(RuntimeError, match="same.*basename|inputs have the destination basename"):
            tool.imageconcat(str(output), list(map(str, paths)), axis=0, relax=True, mode=mode)
        assert not output.exists()
        for index, (original, path) in enumerate(zip(before, paths)):
            assert os.path.samestat(original, path.stat())
            assert tool.open(str(path))
            np.testing.assert_array_equal(tool.getchunk(), np.full((2, 2), index + 1.))
            assert tool.done()
    finally:
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("mode", ["copy", "move"])
def test_owned_virtual_concat_can_rename_to_another_parent(tmp_path, backend, mode):
    api = _backend(backend)
    holder = tmp_path / "holder"
    holder.mkdir()
    paths = [holder / "one.im", holder / "two.im"]
    output = holder / "out.im"
    tool, joined = api.image(), None
    try:
        for index, path in enumerate(paths):
            assert tool.fromarray(str(path), np.full((2, 2), index + 1.), linear=True)
            assert tool.done()
        joined = tool.imageconcat(str(output), list(map(str, paths)), axis=0, relax=True, mode=mode)
        relocated = tmp_path / "relocated"
        relocated.mkdir()
        assert joined.rename(str(relocated / output.name))
        np.testing.assert_array_equal(joined.getchunk()[:, 0], [1, 1, 2, 2])
        assert not output.exists()
        assert joined.done()
        assert tool.open(str(relocated / output.name))
        np.testing.assert_array_equal(tool.getchunk()[:, 0], [1, 1, 2, 2])
        assert tool.done()
        for index, path in enumerate(paths):
            assert tool.open(str(relocated / output.name / path.name))
            np.testing.assert_array_equal(tool.getchunk(), np.full((2, 2), index + 1.))
            assert tool.done()
    finally:
        if joined is not None:
            joined.done()
        tool.done()


def test_virtual_nomove_rename_cannot_use_unrelated_siblings(tmp_path):
    api = _backend("casars")
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    tool, joined = api.image(), None
    output = first / "join.im"
    unwanted = second / "join.im"
    try:
        for directory, offset in [(first, 0), (second, 8)]:
            for i in [1, 2]:
                assert tool.fromarray(str(directory / f"member-{i}.im"), np.full((2, 2), offset+i), linear=True)
                assert tool.done()
        sources = [str(first / f"member-{i}.im") for i in [1, 2]]
        joined = tool.imageconcat(str(output), sources, axis=0, relax=True, mode="nomove")
        with pytest.raises(RuntimeError, match="operand .* would resolve to"):
            joined.rename(str(unwanted))
        assert not unwanted.exists()
        assert tool.open(str(output))
        np.testing.assert_array_equal(tool.getchunk()[:, 0], [1, 1, 2, 2])
    finally:
        if joined is not None:
            joined.done()
        tool.done()


def test_virtual_owned_rename_cannot_redirect_a_relative_symlink(tmp_path):
    api = _backend("casars")
    tool = api.image()
    before, after = tmp_path / "before", tmp_path / "after"
    before.mkdir()
    after.mkdir()
    original, target = before / "join.im", after / "join.im"
    original.mkdir()
    try:
        for member, value in [(before / "ref.im", 1.), (after / "ref.im", 9.), (original / "rest.im", 2.)]:
            assert tool.fromarray(str(member), np.full((2, 2), value), linear=True)
            assert tool.done()
        (original / "link.im").symlink_to("../ref.im", target_is_directory=True)
        (original / "imageconcat.json").write_text(json.dumps({
            "Version": 1, "DataType": "float", "Axis": 0, "TempClose": True,
            "Images": ["././link.im", "././rest.im"],
        }))
        assert tool.open(str(original))
        with pytest.raises(RuntimeError, match="operand .* would resolve to.*different image"):
            tool.rename(str(target))
        assert not target.exists()
        assert tool.open(str(original))
        np.testing.assert_array_equal(tool.getchunk()[:, 0], [1, 1, 2, 2])
    finally:
        tool.done()


@pytest.mark.parametrize("writer", ["casa", "casars"])
@pytest.mark.parametrize("reader", ["casa", "casars"])
def test_virtual_owned_move_across_filesystems(tmp_path, other_filesystem, writer, reader):
    writer_api, reader_api = _backend(writer), _backend(reader)
    original = [tmp_path / f"member-{i}.im" for i in [1, 2]]
    output = other_filesystem / "join.im"
    relocated = tmp_path / "relocated.im"
    tool, sensor, joined = writer_api.image(), reader_api.image(), None
    try:
        for number, path in enumerate(original, 1):
            pixels = np.arange(4., dtype=np.float32).reshape(2, 2) + 4 * number
            assert tool.fromarray(str(path), pixels, linear=True)
            assert tool.calcmask(f'\"{path}\" > 6', "valid")
            assert tool.done()
        joined = tool.imageconcat(str(output), list(map(str, original)), axis=0, relax=True, mode="move")
        assert not any(path.exists() for path in original)
        assert tmp_path.stat().st_dev != output.stat().st_dev
        assert joined.setmiscinfo({"kind": "owned"})
        assert joined.rename(str(relocated))
        assert joined.done()
        assert not output.exists()
        assert sensor.open(str(relocated))
        expected = np.arange(4., 12., dtype=np.float32).reshape(4, 2)
        np.testing.assert_array_equal(sensor.getchunk(), expected)
        np.testing.assert_array_equal(sensor.getchunk(getmask=True), expected > 6)
        assert sensor.miscinfo() == {"kind": "owned"}
    finally:
        if joined is not None:
            joined.done()
        sensor.done()
        tool.done()


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX directory permissions")
def test_virtual_owned_move_across_filesystems_checks_original_removal_access(tmp_path, other_filesystem):
    api = _backend("casars")
    tool = api.image()
    originals = [tmp_path / name for name in ("left.im", "right.im")]
    output = other_filesystem / "join.im"
    extra = originals[1] / "extra"
    try:
        paths = [*originals, output]
        for number, path in enumerate(paths, 1):
            assert tool.fromarray(str(path), np.full((2, 2), number), linear=True)
            assert tool.done()
        extra.mkdir()
        (extra / "data").write_text("retained original")
        extra.chmod(0o577)
        if os.access(extra, os.W_OK):
            pytest.skip("requires a process without write access to the source directory")
        identities = [path.stat() for path in paths]
        with pytest.raises(RuntimeError, match="Cannot move native image .* across filesystems"):
            tool.imageconcat(str(output), list(map(str, originals)), axis=0,
                             mode="move", overwrite=True, relax=True)
        for number, (path, identity) in enumerate(zip(paths, identities), 1):
            assert os.path.samestat(identity, path.stat())
            assert tool.open(str(path))
            np.testing.assert_array_equal(tool.getchunk(), np.full((2, 2), number))
            assert tool.done()
        assert (extra / "data").read_text() == "retained original"
    finally:
        if extra.exists():
            extra.chmod(0o755)
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_virtual_concat_metadata_and_mask_handler_state(tmp_path, backend):
    api = _backend(backend)
    source, image = api.image(), api.image()
    paths = [tmp_path / "a.im", tmp_path / "b.im"]
    output = tmp_path / "virtual.im"
    out = None
    try:
        for path in paths:
            assert source.fromarray(str(path), np.ones((3, 4)), linear=True)
            assert source.setbrightnessunit("Jy")
            assert source.done()
        out = source.imageconcat(str(output), list(map(str, paths)), axis=0,
                                 mode="nomove", relax=True)
        assert out.brightnessunit() == "Jy"
        np.testing.assert_array_equal(out.haslock(), [False, False])
        assert out.setbrightnessunit("K")
        np.testing.assert_array_equal(out.haslock(), [False, False])
        assert out.brightnessunit() == "K"
        with pytest.raises(RuntimeError, match="Cannot make requested mask.*ImageConcat"):
            out.calcmask("F", "unsupported")
        assert list(out.maskhandler("get")) == ["T"]
        with pytest.raises(RuntimeError, match="setDefaultMask cannot be used"):
            out.maskhandler("set", [])
        assert list(out.maskhandler("default")) == [""]
        with pytest.raises(RuntimeError, match="table is not writable"):
            out.remove(verbose=False)
        assert out.done()
        assert image.open(str(output))
        assert image.brightnessunit() == "Jy"
        np.testing.assert_array_equal(image.getchunk(), np.ones((6, 4)))
    finally:
        if out is not None:
            out.done()
        image.done()
        source.done()


@pytest.mark.parametrize("action", ["write", "rename"])
def test_attached_virtual_concat_refuses_mutation_after_replacement(tmp_path, action):
    api = _backend("casars")
    source = api.image()
    paths = [tmp_path / "a.im", tmp_path / "b.im"]
    original, backup, destination = [tmp_path / f"{name}.im" for name in ("original", "backup", "renamed")]
    attached = None
    try:
        for path in paths:
            assert source.fromarray(str(path), np.ones((3, 4)), linear=True)
            assert source.done()
        attached = source.imageconcat(str(original), list(map(str, paths)), axis=0,
                                      mode="nomove", relax=True)
        original.rename(backup)
        assert source.fromarray(str(original), np.full((2, 2), 9.))
        assert source.done()
        with pytest.raises(RuntimeError, match="changed"):
            if action == "write":
                attached.putchunk([[5.]], blc=[0, 0])
            else:
                attached.rename(str(destination))
        assert (backup / "imageconcat.json").is_file()
        assert not destination.exists()
        assert source.open(str(original))
        np.testing.assert_array_equal(source.getchunk(), np.full((2, 2), 9.))
        assert source.done()
        assert source.open(str(paths[0]))
        np.testing.assert_array_equal(source.getchunk(), np.ones((3, 4)))
    finally:
        if attached is not None:
            attached.done()
        source.done()


@pytest.mark.parametrize("tempclose", [False, True])
@pytest.mark.parametrize("backend", ["casa", "casars"])
@pytest.mark.parametrize("mode", ["nomove", "copy", "move"])
def test_virtual_concat_putregion_writes_physical_default_masks(tmp_path, backend, tempclose, mode):
    api = _backend(backend)
    tool, joined = api.image(), None
    sources = [tmp_path / "one.im", tmp_path / "two.im"]
    target = tmp_path / "join.im"
    valid = np.ones((4, 2), dtype=bool)
    valid[1:3, 0] = False
    try:
        for source in sources:
            assert tool.fromarray(str(source), np.ones((2, 2)), linear=True)
            assert tool.calcmask("T", "valid")
            assert tool.done()
        joined = tool.imageconcat(str(target), list(map(str, sources)),
                                   axis=0, relax=True, tempclose=tempclose, mode=mode)
        assert joined.putregion(pixelmask=valid)
        np.testing.assert_array_equal(joined.getchunk(getmask=True), valid)
        assert joined.done()
        for index, source in enumerate(sources):
            member = source if mode == "nomove" else target / source.name
            assert tool.open(str(member))
            np.testing.assert_array_equal(tool.getchunk(getmask=True), valid[2 * index:2 * index + 2])
            np.testing.assert_array_equal(tool.getchunk(), np.ones((2, 2)))
            assert tool.done()
    finally:
        if joined is not None:
            joined.done()
        tool.done()


@pytest.mark.parametrize("tempclose", [False, True])
def test_attached_virtual_concat_pins_physical_member_before_retarget(tmp_path, tempclose):
    api = _backend("casars")
    tool, joined = api.image(), None
    one, two, other, alias = (tmp_path / name for name in ("one.im", "two.im", "other.im", "alias.im"))
    try:
        for path, value in [(one, 1.), (two, 2.), (other, 9.)]:
            assert tool.fromarray(str(path), np.full((2, 2), value), linear=True)
            assert tool.done()
        alias.symlink_to(one.name, target_is_directory=True)
        joined = tool.imageconcat(str(tmp_path / "join.im"), [str(alias), str(two)],
                                   axis=0, relax=True, tempclose=tempclose, mode="nomove")
        alias.unlink()
        alias.symlink_to(other.name, target_is_directory=True)
        np.testing.assert_array_equal(joined.getchunk()[:, 0], [1, 1, 2, 2])
        assert joined.putchunk([[5.]], blc=[0, 0])
        assert joined.done()
        for path, expected in [(one, [[5, 1], [1, 1]]), (other, [[9, 9], [9, 9]])]:
            assert tool.open(str(path))
            np.testing.assert_array_equal(tool.getchunk(), expected)
            assert tool.done()
    finally:
        if joined is not None:
            joined.done()
        tool.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_published_single_member_and_miscinfo_vectors(tmp_path, backend):
    api = _backend(backend)
    tool = api.image()
    source, joined = tmp_path / "one.im", tmp_path / "join.im"
    output = joined / "imageconcat.json"
    try:
        original = np.arange(6., dtype=np.float32).reshape(3, 2)
        assert tool.fromarray(str(source), original, linear=True)
        assert tool.done()
        joined.mkdir()
        output.write_text(json.dumps({"Version": 1, "DataType": "float", "Axis": 0,
                                      "TempClose": True, "Images": ["./one.im"]}))
        assert tool.open(str(joined))
        np.testing.assert_array_equal(tool.getchunk(), original)
        record = {"channels": [1, 2, 3], "nested": {"offsets": [0.25, 0.5]}}
        assert tool.setmiscinfo(record)
        assert tool.done()
        assert json.loads(output.read_text())["MiscInfo"] == record
        assert tool.open(str(joined))
        read = tool.miscinfo()
        np.testing.assert_array_equal(read["channels"], record["channels"])
        np.testing.assert_array_equal(read["nested"]["offsets"], record["nested"]["offsets"])
    finally:
        tool.done()


@pytest.mark.skipif(os.name != "posix", reason="requires a POSIX file descriptor limit")
@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_tempclose_admission_under_file_descriptor_limit(tmp_path, backend):
    api = _backend(backend)
    tool = api.image()
    paths = [tmp_path / f"member-{i}.im" for i in range(48)]
    destination = tmp_path / "join.im"
    try:
        for i, path in enumerate(paths):
            assert tool.fromarray(str(path), np.full((2, 2), i + 1., dtype=np.float32), linear=True)
            assert tool.done()
    finally:
        tool.done()
    module = {"casa": "casatools", "casars": "casars.tools"}[backend]
    subprocess.run(
        [sys.executable, "-c", """
import importlib
import resource
import sys

import numpy as np

api = importlib.import_module(sys.argv[1])
_, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (min(64, hard) if hard >= 0 else 64, hard))
tool = api.image()
joined = tool.imageconcat(sys.argv[2], sys.argv[3:], axis=0, relax=True, tempclose=True, mode="nomove")
np.testing.assert_array_equal(joined.shape(), [96, 2])
np.testing.assert_array_equal(joined.getchunk()[:, 0], np.repeat(np.arange(1, 49), 2))
assert joined.done()
assert tool.done()
""", module, str(destination), *map(str, paths)],
        check=True, timeout=60,
    )
