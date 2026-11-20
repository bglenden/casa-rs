"""Persistent and temporary copies returned by the CASA image factories."""

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


def _attach_copy(parent, method, **arguments):
    result = getattr(parent, method)(**arguments)
    if method == "fromimage":
        assert result is True
        return parent
    assert parent.shape().tolist() == [1]
    assert parent.done()
    return result


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
@pytest.mark.parametrize("kind", ["f", "d", "c", "cd"])
def test_native_copy_factory_reopens_with_opposite_backend(tmp_path, oracle, method, kind):
    source = str(tmp_path / "source.im")
    data = np.arange(12).reshape(3, 4) + 0.125000001
    if kind.startswith("c"):
        data = data + 1j*(2 + data)
    writer = oracle.image()
    assert writer.fromshape(source, [3, 4], type=kind)
    assert writer.putchunk(data)
    expected = writer.getchunk()
    coordinates = writer.coordsys().torecord()
    assert writer.done()
    for index, (module, reader_module) in enumerate([(oracle, tools), (tools, oracle)]):
        parent = module.image()
        assert parent.fromshape(shape=[1])
        output = str(tmp_path / f"copy-{index}.im")
        returned = _attach_copy(parent, method, infile=source, outfile=output)
        if kind in ("f", "c"):
            assert returned.name() == output
        else:
            with pytest.raises(RuntimeError, match="name does not support images with"):
                returned.name()
        _assert_value(returned.getchunk(), expected)
        _assert_value(returned.coordsys().torecord(), coordinates)
        assert returned.history()[-2] == f"Ran ia.{method}"
        assert returned.done()
        reader = reader_module.image()
        assert reader.open(output)
        _assert_value(reader.getchunk(), expected)
        _assert_value(reader.coordsys().torecord(), coordinates)
        assert reader.done()


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
def test_copy_strided_region_default_mask_and_expression(tmp_path, oracle, method):
    source = str(tmp_path / "source.im")
    writer = oracle.image()
    assert writer.fromarray(source, np.arange(20).reshape(4, 5))
    assert writer.calcmask(f'"{source}" > 4', "active")
    assert writer.calcmask("T", "alternate", False)
    assert writer.setbrightnessunit("Jy/pixel")
    assert writer.setmiscinfo({"sample": "metadata"})
    assert writer.setrestoringbeam("10arcsec", "5arcsec", "12deg")
    assert writer.sethistory("::experiment", ["source provenance"])
    history = writer.history()
    assert writer.done()
    rg = oracle.regionmanager()
    region = rg.box([1, 1], [3, 4], inc=[2, 2])
    assert rg.done()
    outputs = []
    for module in [oracle, tools]:
        parent = module.image()
        assert parent.fromshape(shape=[1])
        parent.dohistory(False)
        result = _attach_copy(parent, method, infile=source, region=region, mask=f'"{source}" < 13')
        assert not result.ispersistent()
        _assert_value(result.getchunk(), np.array([[6., 8.], [16., 18.]]))
        _assert_value(result.getchunk(getmask=True), np.array([[True, True], [False, False]]))
        assert result.maskhandler("get") == ["mask0"]
        assert result.maskhandler("default") == ["mask0"]
        assert result.brightnessunit() == "Jy/pixel"
        assert result.miscinfo() == {"sample": "metadata"}
        assert result.history() == history
        outputs.append((result.coordsys().torecord(), result.restoringbeam()))
        assert result.done()
    for actual, expected in zip(*outputs):
        _assert_value(actual, expected)


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
def test_copy_blank_fits_with_read_only_source(tmp_path, oracle, method):
    source = tmp_path / "source.fits"
    payload = _hdu(True, [1, -999, 3, 4])
    source.write_bytes(payload)
    records = []
    for module in [oracle, tools]:
        parent = module.image()
        assert parent.fromshape(shape=[1])
        result = _attach_copy(parent, method, infile=str(source))
        _assert_value(result.getchunk(), np.array([[1., 3.], [np.nan, 4.]]))
        _assert_value(result.getchunk(getmask=True), np.array([[True, True], [False, True]]))
        assert result.brightnessunit() == "Jy/beam"
        assert result.miscinfo() == {"foobar": 73}
        assert result.maskhandler("default") == ["mask0"]
        assert result.history()[0] == "first custom history"
        records.append(result.torecord())
        assert result.done()
    _assert_value(*records)
    assert source.read_bytes() == payload


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
def test_copy_cannot_replace_its_open_source(tmp_path, oracle, method):
    for name, module in [("casa", oracle), ("casars", tools)]:
        source = str(tmp_path / f"{name}.im")
        parent = module.image()
        assert parent.fromshape(source, [2, 3])
        assert parent.done()
        assert parent.fromshape(shape=[1])
        with pytest.raises(RuntimeError, match="already opened"):
            getattr(parent, method)(infile=source, outfile=source, overwrite=True)
        assert parent.shape().tolist() == [1]
        assert parent.open(source)
        assert parent.shape().tolist() == [2, 3]
        assert parent.done()


def test_drop_whole_singleton_coordinates(tmp_path, oracle):
    source = str(tmp_path / "cube.im")
    data = np.arange(12).reshape(3, 4, 1, 1)
    writer = oracle.image()
    assert writer.fromarray(source, data)
    assert writer.calcmask(f'"{source}" > 4')
    assert writer.done()
    records = []
    for module in [oracle, tools]:
        result = module.image().newimagefromimage(infile=source, dropdeg=True)
        _assert_value(result.getchunk(), data[:, :, 0, 0].astype(np.float64))
        _assert_value(result.getchunk(getmask=True), data[:, :, 0, 0] > 4)
        records.append(result.coordsys().torecord())
        assert result.done()
    _assert_value(*records)


def test_copy_region_clips_to_image_bounds(tmp_path, oracle):
    source = str(tmp_path / "source.im")
    data = np.arange(12).reshape(3, 4)
    writer = oracle.image()
    assert writer.fromarray(source, data)
    assert writer.done()
    rg = oracle.regionmanager()
    clipped = rg.box([-5, 1], [9, 10])
    disjoint = rg.box([7, 0], [10, 3])
    assert rg.done()
    for backend in [oracle, tools]:
        image = backend.image()
        assert image.fromimage(infile=source, region=clipped)
        _assert_value(image.getchunk(), data[:, 1:].astype(np.float64))
        with pytest.raises(RuntimeError, match=r"LCSlicer::toSlicer - blc .* must be <= trc"):
            image.fromimage(infile=source, region=disjoint)
        _assert_value(image.getchunk(), data[:, 1:].astype(np.float64))
        assert image.done()


def test_copy_keeps_source_log_when_outfile_overlaps(tmp_path):
    source = tmp_path / "source.im"
    tool = tools.image()
    assert tool.fromshape(str(source), [2, 3])
    assert tool.sethistory("experiment", ["keep this entry"])
    old = tool.history()
    assert tool.done()
    alias = tmp_path / "alias.im"
    alias.symlink_to(source / "logtable", target_is_directory=True)
    for destination in [source / "logtable", alias]:
        with pytest.raises(RuntimeError, match="part of it"):
            tool.fromimage(infile=str(source), outfile=str(destination), overwrite=True)
        assert tool.open(str(source))
        _assert_value(tool.getchunk(), np.zeros((2, 3)))
        assert tool.history() == old
        assert tool.done()


def test_copy_refuses_destination_open_in_table_tool(tmp_path, oracle):
    for label, backend in [("casa", oracle), ("casars", tools)]:
        source = str(tmp_path / f"{label}-source.im")
        target = str(tmp_path / f"{label}-target.im")
        writer = backend.image()
        assert writer.fromshape(source, [3, 4])
        assert writer.done()
        assert writer.fromshape(target, [2, 2])
        assert writer.done()
        table = backend.table()
        assert table.open(target, nomodify=False)
        try:
            with pytest.raises(RuntimeError, match="already opened"):
                writer.fromimage(infile=source, outfile=target, overwrite=True)
        finally:
            assert table.done()
        assert writer.open(target)
        assert writer.shape().tolist() == [2, 2]
        assert writer.done()


def test_copy_crops_tabular_spectral_lookup_starting_at_zero(tmp_path, oracle):
    source = str(tmp_path / "source.im")
    coordinate_tool = oracle.coordsys()
    coordinates = coordinate_tool.newcoordsys(stokes="I Q U V", spectral=True)
    frequencies = np.array([1.4e9, 1.401e9, 1.4025e9, 1.4043e9])
    assert coordinates.setspectral(frequencies={"unit": "Hz", "value": frequencies})
    writer = oracle.image()
    assert writer.fromshape(source, [4, 4], csys=coordinates.torecord())
    assert coordinates.done()
    assert coordinate_tool.done()
    assert writer.done()
    region_tool = oracle.regionmanager()
    region = region_tool.box([1, 0], [2, 2])
    assert region_tool.done()
    records = []
    for backend in [oracle, tools]:
        image = backend.image().newimagefromimage(infile=source, region=region)
        assert image.shape().tolist() == [2, 3]
        lookup = image.coordsys().torecord()["spectral1"]["tabular"]
        _assert_value(lookup["worldvalues"], frequencies[:3])
        _assert_value(lookup["cdelt"], np.array([1.25e6]))
        records.append(lookup)
        assert image.done()
    _assert_value(*records)


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
@pytest.mark.parametrize("case", ["strided", "one plane", "remove singleton axes"])
def test_copy_selects_tabular_frequencies_and_original_beam_planes(tmp_path, oracle, method, case):
    source = str(tmp_path / "per-plane.im")
    shape = (3, 4, 4, 6)
    pixels = np.arange(np.prod(shape), dtype=np.float64).reshape(shape)
    frequencies = np.array([1.4e9, 1.401e9, 1.4025e9, 1.4043e9, 1.407e9, 1.412e9])
    coordinates = oracle.coordsys().newcoordsys(direction=True, stokes="I Q U V", spectral=True)
    assert coordinates.setspectral(frequencies={"unit": "Hz", "value": frequencies})
    writer = oracle.image()
    assert writer.fromshape(source, list(shape), csys=coordinates.torecord())
    assert coordinates.done()
    assert writer.putchunk(pixels)
    for channel in range(shape[3]):
        for polarization in range(shape[2]):
            assert writer.setrestoringbeam(
                f"{(10 + channel) * 60 + polarization}arcsec",
                "2arcsec", "12deg", channel=channel, polarization=polarization, log=False,
            )
    assert writer.done()

    region_tool = oracle.regionmanager()
    if case == "strided":
        region = region_tool.box([0, 0, 1, 1], [2, 3, 3, 5], inc=[1, 1, 2, 2])
        expected = pixels[:, :, 1:4:2, 1:6:2]
        expected_frequencies = frequencies[1:6:2]
    else:
        region = region_tool.box([0, 0, 2, 3], [2, 3, 2, 3])
        expected = pixels[:, :, 2:3, 3:4]
        expected_frequencies = frequencies[3:4]
    assert region_tool.done()
    drop_axes = case == "remove singleton axes"
    if drop_axes:
        expected = expected[:, :, 0, 0]

    beam_records = []
    for writer_module in (oracle, tools):
        parent = writer_module.image()
        assert parent.fromshape(shape=[1])
        path = str(tmp_path / f"{writer_module.__name__}.im")
        result = _attach_copy(parent, method, infile=source, outfile=path, region=region, dropdeg=drop_axes)
        assert result.done()
        for reader_module in (oracle, tools):
            reader = reader_module.image()
            assert reader.open(path)
            _assert_value(reader.getchunk(), expected)
            coords = reader.coordsys()
            spectral = next((value for key, value in coords.torecord().items() if key.startswith("spectral")), None)
            if drop_axes:
                assert spectral is None
            else:
                lookup = spectral["tabular"]
                _assert_value(lookup["worldvalues"], expected_frequencies)
                _assert_value(lookup["pixelvalues"], np.arange(len(expected_frequencies), dtype=np.float64))
                increment = (expected_frequencies[-1] - expected_frequencies[0]) / max(1, len(expected_frequencies) - 1)
                _assert_value(lookup["cdelt"], np.array([increment]))
            assert coords.done()
            beam = reader.restoringbeam()
            if case == "strided":
                assert (beam["nChannels"], beam["nStokes"]) == (3, 2)
                assert beam["beams"]["*2"]["*1"]["major"] == {"value": 903.0, "unit": "arcsec"}
            else:
                assert beam["major"] == {"value": 782.0, "unit": "arcsec"}
            beam_records.append(beam)
            assert reader.done()
    for beam in beam_records[1:]:
        _assert_value(beam, beam_records[0])
