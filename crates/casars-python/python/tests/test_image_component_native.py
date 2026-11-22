"""Native component-list image products and their opening routes."""

from copy import deepcopy
from pathlib import Path

import numpy as np
import pytest

from test_image_component_list import _component_record
from test_componentlist_review import OFFSET
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


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_native_component_creation_preserves_output_alias_and_lexical_parent(tmp_path, backend):
    api = _backend(backend)
    image = api.image()
    record = _component_record(api)
    real, alias = tmp_path / "real.im", tmp_path / "alias.im"
    alias.symlink_to(real, target_is_directory=True)
    left, right = tmp_path / "lexical", tmp_path / "physical"
    left.mkdir()
    (right / "nested").mkdir(parents=True)
    (left / "link").symlink_to(right / "nested", target_is_directory=True)
    physical = right / "target.im"
    lexical = left / "target.im"
    try:
        with pytest.raises(RuntimeError, match="is not a true table directory"):
            image.fromcomplist(str(alias), [6, 8], record, overwrite=True)
        assert alias.is_symlink() and not real.exists()
        assert image.fromarray(str(real), [17., 19.])
        assert image.done()
        assert image.fromcomplist(str(alias), [6, 8], record, overwrite=True)
        assert alias.is_symlink() and real.is_dir()
        assert image.done()
        assert image.open(str(real))
        assert image.getchunk()[3, 4] == 2.
        assert image.done()
        assert image.fromarray(str(physical), [41., 42.])
        assert image.done()
        assert image.fromcomplist(str(left / "link" / ".." / "target.im"), [6, 8], record, overwrite=True)
        assert image.name() == str(lexical)
        assert image.done()
        assert image.open(str(physical))
        np.testing.assert_array_equal(image.getchunk(), [41., 42.])
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_named_component_image_can_be_created_beside_its_input_rows(tmp_path, backend):
    api = _backend(backend)
    cl, image = api.componentlist(), api.image()
    path = tmp_path / "source.cl"
    try:
        assert cl.fromrecord(_component_record(api))
        assert cl.rename(str(path))
        assert cl.done()
        nested = path / "nested.im"
        assert image.fromcomplist(str(nested), [6, 8], str(path))
        assert image.getchunk()[3, 4] == 2.
        assert image.done()
        assert cl.open(str(path))
        assert cl.length() == 1
        assert cl.done()
        assert image.fromcomplist(str(path), [6, 8], str(path), overwrite=True)
        assert image.getchunk()[3, 4] == 2.
    finally:
        cl.done()
        image.done()


@pytest.mark.parametrize("writer", ["casa", "casars"])
@pytest.mark.parametrize("reader", ["casa", "casars"])
@pytest.mark.parametrize("use_table", [False, True])
def test_native_component_reference_frequency_records(tmp_path, writer, reader, use_table):
    writer_api, reader_api = _backend(writer), _backend(reader)
    cl, image, table = writer_api.componentlist(), writer_api.image(), reader_api.table()
    record = {"nelements": 1, "component0": deepcopy(OFFSET)}
    path = tmp_path / "source.cl"
    target = tmp_path / "image.im"
    try:
        if use_table:
            assert cl.fromrecord(record)
            assert cl.rename(str(path))
            assert cl.done()
            assert table.open(str(path), nomodify=False)
            table.putcell("Frequency_Error", 0, 1.e6)
            assert table.done()
            record = str(path)
        assert image.fromcomplist(str(target), [6, 8], record)
        assert image.done()
        assert table.open(str(target))
        frequency = table.getcell("Spectral_Record", 0)["frequency"]
        assert table.getcell("Reference_Frequency", 0) == pytest.approx(2.4e9)
        assert table.getcell("Frequency_Error", 0) == 0.
        assert frequency["m0"]["unit"] == "GHz"
        assert frequency["m0"]["value"] == pytest.approx(2.4 if use_table else 1.4)
        if not use_table:
            assert frequency["offset"]["m0"] == {"unit": "Hz", "value": 1.e9}
        else:
            assert "offset" not in frequency
    finally:
        table.done()
        image.done()
        cl.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_component_image_record_defaults(tmp_path, backend):
    api = _backend(backend)
    image = api.image()
    try:
        path = tmp_path / "default.im"
        assert image.fromcomplist(str(path), [6, 8, 2], {"nelements": 1, "component0": {}})
        assert image.getchunk().shape == (6, 8, 2)
        np.testing.assert_array_equal(image.getchunk(), np.zeros((6, 8, 2)))
    finally:
        image.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_component_image_history_is_inherited_by_derivations(tmp_path, backend):
    api = _backend(backend)
    path = tmp_path / "source.im"
    source, derived = api.image(), api.image()
    subimage = None
    try:
        assert source.fromcomplist(str(path), [6, 8], _component_record(api))
        assert source.sethistory("provenance", ["exposure calibration"])
        subimage = source.subimage()
        assert "exposure calibration" in subimage.history(list=False)
        assert source.done()
        assert derived.fromimage(infile=str(path))
        assert "exposure calibration" in derived.history(list=False)
    finally:
        if subimage is not None:
            subimage.done()
        source.done()
        derived.done()


@pytest.mark.parametrize("reader", ["casa", "casars"])
@pytest.mark.parametrize("wrong_shape", [False, True])
def test_component_image_checks_the_default_mask_on_open(tmp_path, reader, wrong_shape):
    casa, api = _backend("casa"), _backend(reader)
    source, reopened, table = casa.image(), api.image(), casa.table()
    path = tmp_path / "source.im"
    try:
        assert source.fromcomplist(str(path), [6, 8], _component_record(casa))
        assert source.calcmask("F", "point")
        assert source.done()
        assert table.open(str(path), nomodify=False)
        if wrong_shape:
            table.putkeyword("shape", np.array([7, 9], dtype=np.int32))
        else:
            table.putkeyword("Image_defaultmask", "missing")
        assert table.done()
        if wrong_shape:
            with pytest.raises(RuntimeError, match="does not cover the full image"):
                reopened.open(str(path))
            assert not reopened.isopen()
        else:
            assert reopened.open(str(path))
            assert list(reopened.maskhandler("default")) == ["missing"]
            assert np.all(reopened.getchunk(getmask=True))
    finally:
        table.done()
        reopened.done()
        source.done()


def test_clear_default_mask_written_by_casa(tmp_path):
    casa, api = _backend("casa"), _backend("casars")
    source, reopened = casa.image(), api.image()
    path = tmp_path / "source.im"
    try:
        assert source.fromcomplist(str(path), [6, 8], _component_record(casa))
        assert source.calcmask("F", "none")
        assert source.done()
        assert reopened.open(str(path))
        assert list(reopened.maskhandler("set", [])) == ["T"]
        assert list(reopened.maskhandler("default")) == [""]
        assert np.all(reopened.getchunk(getmask=True))
        assert reopened.done()
        assert source.open(str(path))
        assert list(source.maskhandler("default")) == [""]
        assert list(source.maskhandler("get")) == ["none"]
    finally:
        reopened.done()
        source.done()


@pytest.mark.parametrize("cache", [True, False])
def test_reopened_native_component_evaluates_a_small_section(tmp_path, cache):
    api = _backend("casars")
    path = tmp_path / "large.im"
    small = tmp_path / "small.im"
    image = api.image()
    expected = np.zeros((3, 3))
    expected[1, 1] = 2.

    def stored_sizes(directory):
        return {
            entry.relative_to(directory): entry.stat().st_size
            for entry in directory.rglob("*")
            if entry.is_file() and entry.name != "table.lock"
        }

    try:
        record = _component_record(api)
        assert image.fromcomplist(str(small), [6, 8], record, cache=cache)
        assert image.done()
        assert image.fromcomplist(str(path), [50_000, 50_000], record, cache=cache)
        assert image.done()
        assert image.open(str(path), cache=cache)
        for _ in range(2):
            actual = image.getchunk([24_998, 24_998], [25_002, 25_002], [2, 2])
            np.testing.assert_array_equal(actual, expected)
        assert stored_sizes(path) == stored_sizes(small)
    finally:
        image.done()
