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

    beam_records, coordinate_records = [], []
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
            record = coords.torecord()
            coordinate_records.append(record)
            spectral = next((value for key, value in record.items() if key.startswith("spectral")), None)
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
    for record in coordinate_records[1:]:
        _assert_value(record, coordinate_records[0])


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
@pytest.mark.parametrize("axis", ["channels", "polarizations"])
def test_copy_rejects_a_beam_grid_that_does_not_cover_the_source(tmp_path, oracle, method, axis):
    source = str(tmp_path / "malformed-beams.im")
    shape = [3, 4, 1, 6] if axis == "channels" else [3, 4, 4, 1]
    writer = oracle.image()
    assert writer.fromshape(source, shape)
    assert writer.setrestoringbeam("4arcsec", "2arcsec", "12deg", channel=0, polarization=0)
    assert writer.done()
    table = oracle.table()
    assert table.open(source, nomodify=False)
    info = table.getkeyword("imageinfo")
    beams = info["perplanebeams"]
    info["perplanebeams"] = {
        "nChannels": 2 if axis == "channels" else 1,
        "nStokes": 1 if axis == "channels" else 2,
        "*0": beams["*0"],
        "*1": beams["*1"],
    }
    assert table.putkeyword("imageinfo", info)
    assert table.done()
    region_tool = oracle.regionmanager()
    start = [0, 0, 0, 1] if axis == "channels" else [0, 0, 1, 0]
    region = region_tool.box(start, [length - 1 for length in shape])
    assert region_tool.done()
    outfile = tmp_path / "copy.im"
    for backend in (oracle, tools):
        with pytest.raises(RuntimeError, match=f"Number of {axis} is not consistent"):
            getattr(backend.image(), method)(outfile=str(outfile), infile=source, region=region)
        assert not outfile.exists()


def test_copy_uses_the_first_stored_spectral_coordinate_for_restoring_beams(tmp_path, oracle):
    source = str(tmp_path / "transposed.im")
    coords = oracle.coordsys().newcoordsys(direction=True, spectral=True)
    assert coords.addcoordinate(spectral=True)
    writer = oracle.image()
    assert writer.fromshape(str(tmp_path / "original.im"), [3, 4, 6, 6], csys=coords.torecord())
    assert coords.done()
    for channel in range(6):
        unit = "arcmin" if channel == 1 else "arcsec"
        assert writer.setrestoringbeam(f"{20 + channel}{unit}", "2arcsec", "12deg", channel=channel)
    transposed = writer.transpose(outfile=source, order="0132")
    assert transposed.done()
    assert writer.done()
    region_tool = oracle.regionmanager()
    region = region_tool.box([0, 0, 0, 1], [2, 3, 5, 5], inc=[1, 1, 1, 2])
    assert region_tool.done()

    for backend in (oracle, tools):
        path = str(tmp_path / f"{backend.__name__}.im")
        result = backend.image().newimagefromimage(infile=source, outfile=path, region=region)
        assert result.done()
        reader = oracle.image()
        assert reader.open(path)
        assert reader.shape().tolist() == [3, 4, 6, 3]
        assert reader.restoringbeam()["nChannels"] == 3
        for channel, major in enumerate((21.0, 23.0, 25.0)):
            unit = "arcmin" if channel == 0 else "arcsec"
            assert reader.restoringbeam(channel=channel)["major"] == {"value": major, "unit": unit}
        assert reader.done()


def test_copy_preserves_units_from_a_legacy_native_beam_grid(tmp_path, oracle):
    source = str(tmp_path / "legacy.im")
    writer = tools.image()
    assert writer.fromshape(source, [3, 4, 1, 2])
    assert writer.setrestoringbeam("21arcmin", "2arcmin", "0.2rad", channel=1, polarization=0)
    assert writer.done()
    table = oracle.table()
    assert table.open(source, nomodify=False)
    info = table.getkeyword("imageinfo")
    beams = info["perplanebeams"]
    beams["*0_0"] = beams.pop("*0")
    beams["*1_0"] = beams.pop("*1")
    expected = beams["*1_0"]
    assert table.putkeyword("imageinfo", info)
    assert table.done()

    region_tool = oracle.regionmanager()
    region = region_tool.box([0, 0, 0, 1], [2, 3, 0, 1])
    assert region_tool.done()
    path = str(tmp_path / "copy.im")
    image = tools.image().newimagefromimage(infile=source, outfile=path, region=region)
    assert image.done()
    for backend in (tools, oracle):
        reader = backend.image()
        assert reader.open(path)
        _assert_value(reader.restoringbeam(), expected)
        assert reader.done()


@pytest.mark.parametrize("method", ["fromimage", "newimagefromimage"])
@pytest.mark.parametrize("kind", ["worldbox", "polygon", "difference", "fractional", "relative", "latticebox"])
def test_copy_preserves_region_values_masks_and_native_geometry(tmp_path, oracle, method, kind):
    shape = (21, 23, 1, 3)
    x, y, _, channel = np.ogrid[:21, :23, :1, :3]
    pixels = (x + 10 * y + 100 * channel).astype(np.float32)
    names = {}
    record = None
    for api in (oracle, tools):
        name = str(tmp_path / f"{api.__name__}-source.im")
        image = api.image()
        coords = None
        try:
            assert image.fromarray(name, pixels)
            assert image.calcmask(f'"{name}" < 290', "valid")
            if record is None:
                coords = image.coordsys()
                record = coords.torecord()
        finally:
            if coords is not None:
                coords.done()
            image.done()
        names[api] = name

    lower, upper = {
        "fractional": ((4, 6), (12, 15)),
        "relative": ((8, 9), (13, 15)),
    }.get(kind, ((4, 5), (10, 12)))
    x, y = np.ogrid[lower[0]:upper[0] + 1, lower[1]:upper[1] + 1]
    manager = oracle.regionmanager()
    try:
        outer = manager.wbox(["4pix", "5pix"], ["10pix", "12pix"], [0, 1], record)
        inner = manager.wbox(["6pix", "7pix"], ["9pix", "10pix"], [0, 1], record)
        if kind == "fractional":
            region = manager.box([0.2, 0.25], [0.6, 0.7], frac=True)
        elif kind == "relative":
            region = manager.box([-2, -2], [3, 4], absrel="relref")
        elif kind == "polygon":
            region = manager.fromtext("poly[[4pix,5pix],[10pix,5pix],[4pix,12pix]]", list(shape), record)
        elif kind == "difference":
            region = manager.difference(outer, inner)
        elif kind == "latticebox":
            region = {
                "name": "LCBox", "isRegion": 1, "comment": "", "oneRel": True,
                "blc": np.array([5, 6, 1, 1], dtype=np.float32),
                "trc": np.array([11, 13, 1, 3], dtype=np.float32),
                "shape": np.array(shape, dtype=np.int32),
            }
        else:
            region = outer
    finally:
        manager.done()
    if kind == "polygon":
        membership = 7 * (x - 4) + 6 * (y - 5) <= 42
    elif kind == "difference":
        membership = (x < 6) | (x > 9) | (y < 7) | (y > 10)
    else:
        membership = np.ones(np.broadcast_shapes(x.shape, y.shape), dtype=bool)
    expected = pixels[lower[0]:upper[0] + 1, lower[1]:upper[1] + 1]
    mask = membership[..., None, None] & (expected > 80) & (expected < 290)
    reference = oracle.coordsys()
    try:
        assert reference.fromrecord(record)
        reference_origin = reference.toworld([*lower, 0, 0])["numeric"]
    finally:
        reference.done()

    dropdeg = kind == "polygon"
    if dropdeg:
        expected = expected.squeeze(axis=2)
        mask = mask.squeeze(axis=2)
        reference_origin = reference_origin[[0, 1, 3]]

    for factory, source in ((oracle, names[tools]), (tools, names[oracle])):
        destination = str(tmp_path / f"{factory.__name__}-result.im")
        parent = factory.image()
        assert parent.fromshape(shape=[1])
        result = _attach_copy(
            parent, method, infile=source, outfile=destination,
            region=region, mask=f'"{source}" > 80', dropdeg=dropdeg,
        )
        assert result.done()
        for reader in (oracle, tools):
            product = reader.image()
            coords = None
            try:
                assert product.open(destination)
                np.testing.assert_array_equal(product.getchunk(), expected)
                np.testing.assert_array_equal(product.getchunk(getmask=True), mask)
                coords = product.coordsys()
                np.testing.assert_allclose(
                    coords.toworld([0.0] * expected.ndim)["numeric"],
                    reference_origin,
                    atol=2e-8,
                    rtol=1e-10,
                )
            finally:
                if coords is not None:
                    coords.done()
                product.done()
