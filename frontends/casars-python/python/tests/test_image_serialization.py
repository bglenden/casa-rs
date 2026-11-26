"""CASA image records carry pixels, coordinates and image info."""

import numpy as np
import pytest

from casars import tools
from test_image_fits import _hdu
from test_image_workflow import _assert_value


@pytest.fixture
def oracle():
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    return casa


@pytest.mark.parametrize("persistent", [False, True])
@pytest.mark.parametrize("kind", ["f", "d", "c", "cd"])
def test_image_records_cross_tool_round_trip(tmp_path, oracle, kind, persistent):
    source = np.arange(6).reshape(2, 3) + 0.125000012
    if kind.startswith("c"):
        source = source + (1.062500012 - 2j)
        source = source + np.arange(6).reshape(2, 3) * 1j
    outputs = []
    for index, backend in enumerate([oracle, tools]):
        image = backend.image()
        assert image.torecord() == {}
        path = str(tmp_path / f"source-{index}.im") if persistent else ""
        assert image.fromshape(path, [2, 3], type=kind)
        assert image.putchunk(source)
        if kind in ("c", "f"):
            assert image.setbrightnessunit("Jy/beam")
            assert image.setmiscinfo({"extra": 73})
            assert image.setrestoringbeam("4arcsec", "2arcsec", "20deg", log=False)
        record = image.torecord()
        assert set(record) == {"coordsys", "imagearray", "imageinfo", "shape"}
        assert set(record["coordsys"]) == {"coordsys"}
        assert image.done()
        outputs.append(record)
    _assert_value(*outputs)
    target_dtype = np.complex64 if kind.startswith("c") else np.float32
    expected = source.astype(target_dtype).astype(np.complex128 if kind.startswith("c") else np.float64)
    for index, (backend, opposite) in enumerate([(oracle, tools), (tools, oracle)]):
        target = backend.image()
        path = str(tmp_path / f"target-{index}.im") if persistent else ""
        assert target.fromrecord(outputs[1 - index], outfile=path)
        _assert_value(target.getchunk(), expected)
        assert target.ispersistent() is persistent
        assert target.brightnessunit() == ""
        assert target.miscinfo() == {}
        assert target.history()[-2:] == ["Ran ia.fromrecord", f'ia.fromrecord(record={{...}}, outfile="{path}")']
        if kind in ("c", "f"):
            _assert_value(target.restoringbeam(), outputs[0]["imageinfo"]["restoringbeam"])
        assert target.done()
        if persistent:
            reader = opposite.image()
            assert reader.open(path)
            _assert_value(reader.getchunk(), expected)
            _assert_value(reader.coordsys().torecord(), outputs[0]["coordsys"]["coordsys"])
            assert reader.done()


def test_fromrecord_scalar_replaces_own_native_attachment(tmp_path, oracle):
    for backend, name in [(oracle, "casa"), (tools, "casars")]:
        path = str(tmp_path / f"{name}.im")
        image = backend.image()
        assert image.fromshape(path, [2, 3])
        record = image.torecord()
        record["imagearray"] = 7j
        assert image.fromrecord(record=record, outfile=path)
        assert image.pixeltype() == "complex"
        _assert_value(image.getchunk(), np.array([[7j, 0j, 0j], [0j, 0j, 0j]]))
        assert image.done()


@pytest.mark.parametrize("missing", ["shape", "coordsys", "imagearray", "imageinfo"])
def test_invalid_record_detaches_and_preserves_native_output(tmp_path, oracle, missing):
    for backend, name in [(oracle, "casa"), (tools, "casars")]:
        target = str(tmp_path / f"{name}.im")
        image = backend.image()
        assert image.fromshape(target, [2, 3])
        record = image.torecord()
        del record[missing]
        with pytest.raises(RuntimeError, match=f"field {missing} is unknown"):
            image.fromrecord(record, target)
        assert not image.isopen()
        assert image.open(target)
        _assert_value(image.getchunk(), np.zeros((2, 3)))
        assert image.done()


@pytest.mark.parametrize("problem", ["missing nested coordinates", "incompatible native rank"])
def test_failed_native_record_restore_preserves_existing_image(tmp_path, problem):
    target = str(tmp_path / "saved.im")
    image = tools.image()
    assert image.fromshape(target, [2, 3])
    assert image.putchunk(np.full((2, 3), 7.0))
    assert image.sethistory("input", ["original image"])
    history = image.history()
    record = image.torecord()
    if problem == "missing nested coordinates":
        record["coordsys"] = {}
        diagnostic = "invalid coordinate system"
    else:
        record["shape"] = [2, 3, 2]
        diagnostic = "Failed AlwaysAssert setCoordinateInfo"
    with pytest.raises(RuntimeError, match=diagnostic):
        image.fromrecord(record, target)
    assert not image.isopen()
    assert image.open(target)
    np.testing.assert_array_equal(image.getchunk(), np.full((2, 3), 7.0))
    assert image.history() == history
    assert image.done()


def test_record_restoration_with_short_input_and_mismatched_coordinates(tmp_path, oracle):
    payload = np.array([[2, 4], [3, 5]], dtype=np.int32)
    matrices = []
    for backend, label in [(oracle, "casa"), (tools, "casars")]:
        image = backend.image()
        assert image.fromshape(shape=[2, 3])
        record = image.torecord()
        record["shape"] = [3, 4]
        record["imagearray"] = payload
        assert image.fromrecord(record)
        expected = np.zeros((3, 4))
        expected[:2, :2] = payload
        _assert_value(image.getchunk(), expected)
        record["shape"] = [2, 3, 2]
        assert image.fromrecord(record)
        assert image.shape().tolist() == [2, 3, 2]
        matrices.append(image.torecord())
        with pytest.raises(RuntimeError, match="referencePixel and newLatticeShape"):
            image.getchunk()
        with pytest.raises(RuntimeError, match="itsCSys.nWorldAxes"):
            image.pixelvalue([1, 1, 0])
        with pytest.raises(RuntimeError, match="Failed AlwaysAssert setCoordinateInfo"):
            image.fromrecord(record, str(tmp_path / f"{label}.im"))
        assert not image.isopen()
    _assert_value(*matrices)


def test_serialize_direct_fits_pixels_and_image_info(tmp_path, oracle):
    path = tmp_path / "image.fits"
    path.write_bytes(_hdu(True, [1, -999, 3, 4]))
    records = []
    for backend in [oracle, tools]:
        image = backend.image()
        assert image.open(str(path))
        record = image.torecord()
        records.append(record)
        assert record["imageinfo"]["objectname"] == "sample"
        _assert_value(record["imagearray"], np.array([[1., 3.], [np.nan, 4.]]))
        assert image.done()
    _assert_value(*records)


def test_swapped_coordinate_record_names_remaining_axis_work(oracle):
    reference = oracle.image()
    assert reference.fromshape(shape=[2, 3])
    swapped = reference.transpose(order="10")
    record = swapped.torecord()
    assert record["coordsys"]["coordsys"]["pixelmap0"].tolist() == [1, 0]
    assert swapped.done()
    assert reference.done()
    with pytest.raises(RuntimeError, match="casa-rs issue #1002"):
        tools.image().fromrecord(record)
