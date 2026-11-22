"""Native component-list image products and their opening routes."""

from pathlib import Path

import numpy as np
import pytest

from test_image_component_list import _component_record
from test_image_removal import _backend


@pytest.mark.parametrize("writer", ["casa", "casars"])
@pytest.mark.parametrize("reader", ["casa", "casars"])
@pytest.mark.parametrize("cache", [False, True])
def test_named_component_image_writer_reader(tmp_path, writer, reader, cache):
    writer_api, reader_api = _backend(writer), _backend(reader)
    path = tmp_path / "source.im"
    source, reopened = writer_api.image(), reader_api.image()
    table = reader_api.table()
    record = _component_record(writer_api)
    expected = np.zeros((6, 8))
    expected[3, 4] = 2.
    try:
        assert source.fromcomplist(outfile=str(path), shape=[6, 8], cl=record, cache=cache)
        assert source.name() == str(path)
        assert source.ispersistent()
        np.testing.assert_array_equal(source.getchunk(), expected)
        assert source.done()
        assert table.open(str(path))
        assert table.info()["type"] == "Component List"
        assert set(table.keywordnames()) >= {"shape", "coords", "units", "logtable"}
        assert table.getkeyword("units") == "Jy/pixel"
        np.testing.assert_array_equal(table.getkeyword("shape"), [6, 8])
        assert Path(table.getkeyword("logtable").removeprefix("Table: ")).is_dir()
        assert table.done()
        assert reopened.open(str(path), cache=cache)
        np.testing.assert_array_equal(reopened.getchunk(), expected)
        assert reopened.name() == str(path)
        assert reopened.ispersistent()
        with pytest.raises(RuntimeError, match="pixel values cannot be modified"):
            reopened.putchunk(np.zeros((1, 1), dtype=np.float32))
        assert reopened.isopen()
    finally:
        table.done()
        reopened.done()
        source.done()


@pytest.mark.parametrize("writer", ["casa", "casars"])
@pytest.mark.parametrize("reader", ["casa", "casars"])
@pytest.mark.parametrize("named", [False, True])
def test_component_list_table_input(tmp_path, writer, reader, named):
    writer_api, reader_api = _backend(writer), _backend(reader)
    table_path = tmp_path / "source.cl"
    destination = str(tmp_path / "created.im") if named else ""
    source, image = writer_api.componentlist(), reader_api.image()
    expected = np.zeros((6, 8))
    expected[3, 4] = 2.
    try:
        assert source.fromrecord(_component_record(writer_api))
        assert source.rename(str(table_path))
        assert source.done()
        assert image.fromcomplist(destination, [6, 8], str(table_path))
        assert image.ispersistent() == named
        np.testing.assert_array_equal(image.getchunk(), expected)
        assert table_path.is_dir()
    finally:
        image.done()
        source.done()


@pytest.mark.parametrize("writer", ["casa", "casars"])
@pytest.mark.parametrize("reader", ["casa", "casars"])
def test_named_component_metadata_survives_reopen(tmp_path, writer, reader):
    writer_api, reader_api = _backend(writer), _backend(reader)
    path = tmp_path / "editable.im"
    source, restored = writer_api.image(), reader_api.image()
    coords = None
    expected = np.zeros((6, 8))
    expected[1, 2] = 2.
    try:
        assert source.fromcomplist(str(path), [6, 8], _component_record(writer_api))
        coords = source.coordsys()
        assert coords.setreferencepixel([1., 2.])
        assert source.setcoordsys(coords.torecord())
        assert source.setmiscinfo({"purpose": "interoperability"})
        assert source.sethistory("component-test", ["saved history"])
        np.testing.assert_array_equal(source.getchunk(), expected)
        assert source.done()
        assert restored.open(str(path))
        assert restored.miscinfo() == {"purpose": "interoperability"}
        assert "saved history" in restored.history(list=False)
        np.testing.assert_array_equal(restored.getchunk(), expected)
        reference = restored.coordsys()
        try:
            np.testing.assert_array_equal(reference.referencepixel()["numeric"], [1., 2.])
        finally:
            reference.done()
    finally:
        if coords is not None:
            coords.done()
        source.done()
        restored.done()


@pytest.mark.parametrize("reader", ["casa", "casars"])
def test_open_reads_existing_native_component_mask(tmp_path, reader):
    casa, reader_api = _backend("casa"), _backend(reader)
    path = tmp_path / "masked.im"
    source, restored = casa.image(), reader_api.image()
    expected = np.zeros((6, 8), dtype=bool)
    expected[3, 4] = True
    try:
        assert source.fromcomplist(str(path), [6, 8], _component_record(casa))
        assert source.calcmask(f'"{path}" > 0', "point")
        assert source.done()
        assert restored.open(str(path))
        assert list(restored.maskhandler("get")) == ["point"]
        np.testing.assert_array_equal(restored.getchunk(getmask=True), expected)
        np.testing.assert_array_equal(
            restored.getchunk([1, 2], [5, 6], [2, 2], getmask=True),
            expected[1:6:2, 2:7:2],
        )
    finally:
        source.done()
        restored.done()


def test_failed_named_component_creation_preserves_existing_target(tmp_path):
    api = _backend("casars")
    path = tmp_path / "existing.im"
    source, restored = api.image(), api.image()
    values = np.full((3, 4), 7.)
    try:
        assert source.fromarray(str(path), values)
        assert source.done()
        assert source.fromshape(shape=[2, 2])
        with pytest.raises(RuntimeError, match="must have 2, 3, or 4 dimensions"):
            source.fromcomplist(str(path), [4], _component_record(api), overwrite=True)
        assert not source.isopen()
        assert restored.open(str(path))
        np.testing.assert_array_equal(restored.getchunk(), values)
    finally:
        source.done()
        restored.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_native_component_image_rename_and_remove(tmp_path, backend):
    api = _backend(backend)
    source, reopened = api.image(), api.image()
    path, moved = tmp_path / "source.im", tmp_path / "moved.im"
    expected = np.zeros((6, 8))
    expected[3, 4] = 2.
    try:
        assert source.fromcomplist(str(path), [6, 8], _component_record(api))
        assert source.setmiscinfo({"kept": "during rename"})
        assert source.rename(str(moved))
        assert source.name() == str(moved)
        assert not path.exists()
        assert source.done()
        assert reopened.open(str(moved))
        assert reopened.miscinfo() == {"kept": "during rename"}
        assert "Ran ia.rename" in reopened.history(list=False)
        np.testing.assert_array_equal(reopened.getchunk(), expected)
        assert reopened.remove(verbose=False)
        assert not reopened.isopen() and not moved.exists()
    finally:
        source.done()
        reopened.done()
