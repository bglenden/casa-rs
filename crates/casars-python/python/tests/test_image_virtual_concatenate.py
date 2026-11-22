"""Native imageconcat manifest interoperability and strided access."""

import json

import numpy as np
import pytest

from test_image_removal import _backend


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
def test_nomove_manifest_and_write_through(tmp_path, kind, dtype, precision, stored_type, tempclose, writer, reader):
    writer_api, reader_api = _backend(writer), _backend(reader)
    source = writer_api.image()
    reader_tool, operand = reader_api.image(), reader_api.image()
    paths = [tmp_path / "left.im", tmp_path / "right.im"]
    output = tmp_path / "joint.im"
    joined = None
    factor = 1. + 2.j if np.issubdtype(dtype, np.complexfloating) else 1.
    expected = np.concatenate([np.full((3, 4), factor), np.full((3, 4), 2 * factor)])
    try:
        for number, path in enumerate(paths, start=1):
            assert source.fromarray(str(path), np.full((3, 4), number * factor, dtype=dtype), linear=True, type=precision)
            assert source.done()
        joined = source.imageconcat(str(output), list(map(str, paths)), axis=0, relax=True, tempclose=tempclose, mode="nomove")
        assert joined.pixeltype() == kind
        np.testing.assert_array_equal(joined.shape(), [6, 4])
        assert set(output.iterdir()) == {output / "imageconcat.json"}
        manifest = json.loads((output / "imageconcat.json").read_text())
        assert manifest == {
            "Version": 1,
            "DataType": stored_type,
            "Axis": 0,
            "TempClose": tempclose,
            "Images": ["./left.im", "./right.im"],
            "MiscInfo": {},
        }
        assert joined.done()
        assert reader_tool.open(str(output))
        assert reader_tool.pixeltype() == kind
        if kind in {"float", "complex"}:
            assert reader_tool.ispersistent()
            np.testing.assert_array_equal(reader_tool.haslock(), [not tempclose, False])
        subsection = reader_tool.getchunk([1, 0], [5, 3], [2, 1])
        assert subsection.dtype == expected.dtype
        np.testing.assert_array_equal(subsection, expected[1:6:2, :])
        values = np.full((3, 1), 9 * factor, dtype=dtype)
        assert reader_tool.putchunk(values, blc=[1, 0], inc=[2, 1])
        expected[1:6:2, 0] = 9 * factor
        np.testing.assert_array_equal(reader_tool.getchunk(), expected)
        assert reader_tool.setmiscinfo({"intent": "write-through"})
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
        for index, path in enumerate(paths):
            assert operand.open(str(path))
            np.testing.assert_array_equal(operand.getchunk(), expected[index * 3:(index + 1) * 3, :])
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
def test_nomove_concat_repeated_operand_reads_one_table(tmp_path, backend):
    api = _backend(backend)
    a, b = tmp_path / "a.im", tmp_path / "b.im"
    image, out = api.image(), None
    try:
        assert image.fromarray(str(a), np.ones((3, 4)), linear=True)
        assert image.done()
        assert image.fromarray(str(b), np.full((3, 4), 2.), linear=True)
        assert image.done()
        out = image.imageconcat(str(tmp_path / "repeat.im"), list(map(str, [a, b, a])),
                                axis=0, relax=True, mode="nomove", tempclose=False)
        np.testing.assert_array_equal(out.getchunk()[:, 0], [1, 1, 1, 2, 2, 2, 1, 1, 1])
        if backend == "casars":
            assert out.putchunk(np.array([[9.], [8.]]), blc=[0, 0], inc=[6, 1])
            np.testing.assert_array_equal(out.getchunk()[:, 0], [8, 1, 1, 2, 2, 2, 8, 1, 1])
    finally:
        if out is not None:
            out.done()
        image.done()


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
def test_virtual_concat_combines_restoring_beams_on_the_spectral_axis(tmp_path, backend):
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
        out = source.imageconcat(str(target), list(map(str, paths)), axis=3, mode="nomove")
        assert out.done()
        assert reader.open(str(target))
        beams = [reader.restoringbeam(channel=channel, polarization=0) for channel in range(4)]
        qa = api.quanta()
        try:
            np.testing.assert_allclose(
                [qa.convert(beam["major"], "arcsec")["value"] for beam in beams],
                [2, 2, 3, 3],
            )
        finally:
            qa.done()
    finally:
        if out is not None:
            out.done()
        if coordinates is not None:
            coordinates.done()
        reader.done()
        source.done()


def test_virtual_nomove_shape_admission_preserves_named_output_and_inputs(tmp_path):
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
        with pytest.raises(RuntimeError, match="Lattice shapes inconsistent"):
            image.imageconcat(str(destination), list(map(str, paths)), axis=0,
                              mode="nomove", overwrite=True, relax=True)
        assert image.open(str(destination))
        np.testing.assert_array_equal(image.getchunk(), np.full((2, 2), 7.))
        assert all(path.is_dir() for path in paths)
        assert not (destination / "imageconcat.json").exists()
    finally:
        image.done()


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
        assert out.setbrightnessunit("K")
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
