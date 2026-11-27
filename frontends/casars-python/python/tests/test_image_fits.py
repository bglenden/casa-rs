"""Direct FITS attachments and native products from the image tool."""

import gzip
import os

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


def _card(key, value):
    if isinstance(value, str):
        text = f"{value!r:<20}"
    elif type(value) is bool:
        text = f"{'T' if value else 'F':>20}"
    else:
        text = f"{value:>20}"
    return f"{key:8}= {text}".ljust(80).encode("ascii")


def _hdu(primary, pixels, *, alternate=False, bitpix=16, scaling=()):
    extension = [("SIMPLE", True)] if primary else [("XTENSION", "IMAGE   ")]
    shape = [("NAXIS1", 2), ("NAXIS2", 2)] if pixels is not None else []
    keys = extension + [("BITPIX", bitpix), ("NAXIS", len(shape))] + shape
    if not primary:
        keys += [("PCOUNT", 0), ("GCOUNT", 1)]
    if pixels is not None:
        keys += [
            ("CTYPE1", "RA---SIN"), ("CTYPE2", "DEC--SIN"),
            ("CRPIX1", 1.0), ("CRPIX2", 1.0), ("CRVAL1", 5.0), ("CRVAL2", 6.0),
            ("CDELT1", -0.02), ("CDELT2", 0.03),
            ("BUNIT", "Jy/BEAM"), ("FOOBAR", 73), ("OBJECT", "sample"),
        ]
        if bitpix > 0:
            keys.append(("BLANK", -999))
    if alternate:
        keys += [
            ("CTYPE1A", "RA---TAN"), ("CTYPE2A", "DEC--TAN"),
            ("CRPIX1A", 1.0), ("CRPIX2A", 1.0), ("CRVAL1A", 35.0), ("CRVAL2A", 36.0),
            ("CDELT1A", -0.05), ("CDELT2A", 0.06),
        ]
    keys += scaling
    header = b"".join(_card(key, value) for key, value in keys)
    header += b"HISTORY first custom history".ljust(80) + b"END".ljust(80)
    values = b"" if pixels is None else np.asarray(pixels, dtype=">i2" if bitpix > 0 else ">f4").tobytes()
    return header.ljust((-len(header) % 2880) + len(header)) + values.ljust((-len(values) % 2880) + len(values), b"\0")


@pytest.fixture
def oracle():
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    return casa


@pytest.fixture
def gzip_fits(tmp_path):
    path = tmp_path / "source.fits.gz"
    path.write_bytes(gzip.compress(_hdu(True, [1, -999, 3, 4]), mtime=0))
    return path


@pytest.mark.parametrize("method", [
    "open", "newimage", "newimagefromfile", "fromimage", "newimagefromimage",
])
def test_gzip_fits_attachment_and_copy_extension(gzip_fits, method):
    contents = gzip_fits.read_bytes()
    image = tools.image()
    product = image
    coordinates = None
    try:
        assert image.fromshape(shape=[3])
        outcome = getattr(image, method)(infile=str(gzip_fits))
        if method in ("open", "fromimage"):
            assert outcome
        else:
            product = outcome
            np.testing.assert_array_equal(image.shape(), [3])
        persistent = method in ("open", "newimage", "newimagefromfile")
        assert product.ispersistent() is persistent
        if persistent:
            assert product.name() == str(gzip_fits)
        _assert_value(product.getchunk(), np.array([[1., 3.], [np.nan, 4.]]))
        _assert_value(product.getchunk(getmask=True), np.array([[True, True], [False, True]]))
        assert product.brightnessunit() == "Jy/beam"
        assert product.history()[0] == "first custom history"
        coordinates = product.coordsys()
        assert coordinates.setunits(["deg", "deg"])
        np.testing.assert_allclose(coordinates.toworld([0, 0])["numeric"], [5., 6.], atol=1e-10)
    finally:
        if coordinates is not None:
            coordinates.done()
        if product is not image:
            product.done()
        image.done()
    assert gzip_fits.read_bytes() == contents


def test_pinned_casa_requires_explicit_import_of_gzip_fits(oracle, gzip_fits):
    image = oracle.image()
    try:
        for method in ("open", "newimage", "newimagefromfile", "fromimage", "newimagefromimage"):
            with pytest.raises(RuntimeError, match="Unable to open image"):
                getattr(image, method)(infile=str(gzip_fits))
        for api in (oracle, tools):
            source = api.image()
            product = None
            try:
                assert source.fromfits(infile=str(gzip_fits))
                product = source.newimagefromfits(infile=str(gzip_fits))
                for imported in (source, product):
                    _assert_value(imported.getchunk(), np.array([[1., 3.], [np.nan, 4.]]))
                    _assert_value(
                        imported.getchunk(getmask=True), np.array([[True, True], [False, True]]),
                    )
                    assert imported.brightnessunit() == "Jy/beam"
            finally:
                if product is not None:
                    product.done()
                source.done()
    finally:
        image.done()


@pytest.mark.parametrize("kind, reason", [
    ("unrecognized", "Unable to open image"),
    ("false-simple", "Unable to open image"),
    ("corrupt-header", "Error opening FITS input"),
    ("shifted-equals", "Error opening FITS input"),
    ("unspaced-value", "Error reading initial record"),
    ("header-only", "No data in the zeroth or first extension"),
    ("missing-primary-record", "No data in the zeroth or first extension"),
    ("short-primary-record", "Error reading initial record"),
    ("short-extension-record", "No data in the zeroth or first extension"),
    ("later-image", "No data in the zeroth or first extension"),
])
def test_direct_fits_rejects_invalid_initial_image_before_attachment(tmp_path, oracle, kind, reason):
    primary = _hdu(True, [1, 2, 3, 4])
    empty = _hdu(True, None)
    extension = _hdu(False, [5, 6, 7, 8])
    contents = {
        "unrecognized": b"plain text\n",
        "false-simple": _card("SIMPLE", False) + primary[80:],
        "corrupt-header": primary.replace(b"NAXIS2", b"OTHER2", 1),
        "shifted-equals": b"SIMPLE = T".ljust(80) + primary[80:],
        "unspaced-value": b"SIMPLE  =T".ljust(80) + primary[80:2888],
        "header-only": empty,
        "missing-primary-record": primary[:2880],
        "short-primary-record": primary[:2888],
        "short-extension-record": empty + extension[:2888],
        "later-image": empty + _hdu(False, None) + extension,
    }[kind]
    path = tmp_path / f"{kind}.fits"
    path.write_bytes(contents)
    for api in (oracle, tools):
        for method in ("open", "newimage", "newimagefromfile", "fromimage", "newimagefromimage"):
            image = api.image()
            try:
                assert image.fromarray(pixels=[7., 11., 13.])
                with pytest.raises(RuntimeError, match=reason):
                    getattr(image, method)(infile=str(path))
                assert image.isopen() == (method != "open")
                if image.isopen():
                    np.testing.assert_array_equal(image.getchunk(), [7., 11., 13.])
            finally:
                image.done()
    assert path.read_bytes() == contents


def test_explicit_fits_import_can_select_an_image_after_two_empty_hdus(tmp_path, oracle):
    path = tmp_path / "later.fits"
    path.write_bytes(_hdu(True, None) + _hdu(False, None) + _hdu(False, [5, 6, 7, 8]))
    for api in (oracle, tools):
        image = api.image()
        try:
            assert image.fromfits(infile=str(path), whichhdu=-1)
            np.testing.assert_array_equal(image.getchunk(), [[5., 7.], [6., 8.]])
        finally:
            image.done()


def test_direct_fits_open_only_requires_the_initial_data_record(tmp_path, oracle):
    path = tmp_path / "one-record.fits"
    cards = [
        ("SIMPLE", True), ("BITPIX", 16), ("NAXIS", 2), ("NAXIS1", 256), ("NAXIS2", 256),
    ]
    header = b"".join(_card(name, value) for name, value in cards) + b"END".ljust(80)
    path.write_bytes(header.ljust(2880) + bytes(2880))
    for api in (oracle, tools):
        image = api.image()
        try:
            assert image.open(str(path))
            np.testing.assert_array_equal(image.shape(), [256, 256])
        finally:
            image.done()


def test_subimage_cannot_replace_its_direct_fits_source(tmp_path):
    source = tmp_path / "image.fits"
    content = _hdu(True, [1, 2, 3, 4])
    source.write_bytes(content)
    tool = tools.image()
    assert tool.open(str(source))
    with pytest.raises(RuntimeError, match="matches or contains input"):
        tool.subimage(str(source), overwrite=True)
    assert source.read_bytes() == content
    np.testing.assert_array_equal(tool.getchunk(), [[1, 3], [2, 4]])
    assert tool.done()


@pytest.mark.parametrize("bitpix,blank", [(16, -999), (-32, np.nan)])
def test_subimage_of_attached_fits_survives_parent_close(tmp_path, oracle, bitpix, blank):
    source = tmp_path / "input.fits"
    content = _hdu(True, [1, blank, 3, 4], bitpix=bitpix)
    source.write_bytes(content)
    expected_pixels = np.array([[1., 3.], [np.nan, 4.]])
    expected_mask = np.array([[True, True], [False, True]])
    records = []
    for index, (writer, reader) in enumerate(((oracle, tools), (tools, oracle))):
        parent = writer.image()
        assert parent.open(str(source))
        child = parent.subimage(list=False)
        assert parent.done()
        _assert_value(child.getchunk(), expected_pixels)
        _assert_value(child.getchunk(getmask=True), expected_mask)
        assert child.brightnessunit() == "Jy/beam"
        assert child.history()[0] == "first custom history"
        assert child.done()
        assert parent.open(str(source))
        saved = tmp_path / f"section-{index}.im"
        region = oracle.regionmanager().box([0, 1], [1, 1])
        result = parent.subimage(
            str(saved), region=region, mask=f'"{source}" > 3',
            dropdeg=True, keepaxes=[1], wantreturn=False, list=False,
        )
        assert not result.isopen() and result.done() and parent.done()
        reopened = reader.image()
        assert reopened.open(str(saved))
        _assert_value(reopened.getchunk(), expected_pixels[:, 1:])
        _assert_value(reopened.getchunk(getmask=True), np.array([[False], [True]]))
        assert reopened.brightnessunit() == "Jy/beam"
        assert reopened.history()[-2] == "Ran ia.subimage"
        coordinates = reopened.coordsys()
        _assert_value(coordinates.referencepixel()["numeric"], np.array([0.0, -1.0]))
        records.append(coordinates.torecord())
        assert coordinates.done()
        assert reopened.done()
    _assert_value(*records)
    assert source.read_bytes() == content


@pytest.mark.parametrize("writer", ["casa", "casars"])
@pytest.mark.parametrize("bitpix", [16, -32])
@pytest.mark.parametrize("zero", [False, True])
def test_fits_import_and_native_readback(tmp_path, oracle, writer, bitpix, zero):
    source, product = str(tmp_path / "image.fits"), str(tmp_path / "image.im")
    blank = -999 if bitpix > 0 else np.nan
    (tmp_path / "image.fits").write_bytes(_hdu(True, [1, blank, 3, 4], bitpix=bitpix))
    module, reader_module = (oracle, tools) if writer == "casa" else (tools, oracle)
    tool = module.image()
    assert tool.fromfits(product, source, zeroblanks=zero)
    assert tool.ispersistent()
    assert tool.pixeltype() == "float"
    expected = np.array([[1, 3], [0 if zero else np.nan, 4]], dtype=np.float64)
    _assert_value(tool.getchunk(), expected)
    _assert_value(tool.getchunk(getmask=True), np.array([[True, True], [False, True]]))
    assert tool.maskhandler("default") == ["mask0"]
    assert tool.miscinfo() == {"foobar": 73, "object": "sample"}
    assert tool.brightnessunit() == "Jy/beam"
    assert tool.history()[0] == "first custom history"
    assert tool.history()[-2] == "Ran ia.fromfits"
    assert f"zeroBlanks={str(zero).lower()}" in tool.history()[-1]
    record = tool.coordsys().torecord()
    assert tool.done()
    reader = reader_module.image()
    assert reader.open(product)
    _assert_value(reader.getchunk(), expected)
    _assert_value(reader.coordsys().torecord(), record)
    assert reader.maskhandler("default") == ["mask0"]
    assert reader.history()[0] == "first custom history"
    assert reader.done()


def test_fits_returned_tool_selects_wcs_and_image_hdu(tmp_path, oracle):
    path = str(tmp_path / "selected.fits")
    (tmp_path / "selected.fits").write_bytes(
        _hdu(True, None) + _hdu(False, [5, 6, 7, 8], alternate=True)
    )
    with pytest.raises(RuntimeError, match="No image at specified location"):
        tools.image().fromfits(infile=path, whichhdu=0)
    for which_hdu, which_rep in [(-1, 0), (1, 1), (1, 30)]:
        expected = oracle.image().newimagefromfits(infile=path, whichhdu=which_hdu, whichrep=which_rep)
        tool = tools.image()
        assert tool.fromshape(shape=[3])
        returned = tool.newimagefromfits(infile=path, whichhdu=which_hdu, whichrep=which_rep)
        assert tool.shape().tolist() == [3]
        assert returned.isopen() and not returned.ispersistent()
        _assert_value(returned.getchunk(), expected.getchunk())
        _assert_value(returned.coordsys().torecord(), expected.coordsys().torecord())
        assert returned.history() == expected.history() == ["first custom history"]
        assert returned.done()
        assert expected.done()
        assert tool.done()


def test_rejected_fits_import_keeps_current_image_and_output(tmp_path, oracle):
    source = tmp_path / "one.fits"
    source.write_bytes(_hdu(True, [1, 2, 3, 4]))
    target = tmp_path / "existing.im"
    tool = tools.image()
    assert tool.fromshape(str(target), [2, 3])
    assert tool.done()
    assert tool.fromshape(shape=[3])
    with pytest.raises(RuntimeError, match="No image at specified location"):
        tool.fromfits(str(target), str(source), whichhdu=2, overwrite=True)
    with pytest.raises(RuntimeError, match="No image at specified location"):
        tool.fromfits(str(target), str(source), whichhdu=2**32, overwrite=True)
    with pytest.raises(RuntimeError, match="index must be non-negative"):
        tool.fromfits(str(target), str(source), whichrep=-1, overwrite=True)
    assert tool.shape().tolist() == [3]
    reader = oracle.image()
    assert reader.open(str(target))
    assert reader.shape().tolist() == [2, 3]
    assert reader.done()


def test_import_casa_written_spectral_stokes_cube(tmp_path, oracle):
    cube = str(tmp_path / "cube.im")
    fits = str(tmp_path / "cube.fits")
    out = str(tmp_path / "reimport.im")
    image = oracle.image()
    values = np.arange(64, dtype=np.float64).reshape(4, 4, 2, 2)
    assert image.fromarray(cube, values)
    assert image.setbrightnessunit("Jy/beam")
    assert image.setrestoringbeam("6arcsec", "2arcsec", "16deg", channel=0, polarization=0, log=False)
    assert image.setrestoringbeam("5arcsec", "3arcsec", "12deg", channel=1, polarization=1, log=False)
    assert image.tofits(fits, history=True)
    assert image.done()
    expected = oracle.image()
    assert expected.fromfits(infile=fits)
    actual = tools.image()
    assert actual.fromfits(out, fits)
    _assert_value(actual.getchunk(), expected.getchunk())
    _assert_value(actual.coordsys().torecord(), expected.coordsys().torecord())
    _assert_value(actual.restoringbeam(), expected.restoringbeam())
    _assert_value(actual.miscinfo(), expected.miscinfo())
    assert actual.history()[:-2] == expected.history()[:-2]
    assert actual.done()
    assert image.open(out)
    _assert_value(image.coordsys().torecord(), expected.coordsys().torecord())
    _assert_value(image.restoringbeam(), expected.restoringbeam())
    assert image.done()
    assert expected.done()


def _raw_fits(path, shape, keys=(), history=()):
    structural = [("SIMPLE", True), ("BITPIX", -32), ("NAXIS", len(shape))]
    structural += [(f"NAXIS{axis}", length) for axis, length in enumerate(shape, 1)]
    cards = b"".join(_card(key, value) for key, value in (*structural, *keys))
    cards += b"".join(f"HISTORY {entry}".ljust(80).encode("ascii") for entry in history)
    cards += b"END".ljust(80)
    data = np.arange(np.prod(shape), dtype=">f4").tobytes()
    path.write_bytes(cards.ljust(len(cards) + (-len(cards) % 2880)) + data.ljust(len(data) + (-len(data) % 2880), b"\0"))


@pytest.mark.parametrize("shape, keys", [
    ((3, 2, 2), [
        ("CTYPE1", "FREQ"), ("CUNIT1", "Hz"), ("CRPIX1", 1),
        ("CRVAL1", 1e9), ("CDELT1", 1e6),
        ("CTYPE2", "RA---TAN"), ("CTYPE3", "DEC--TAN"),
        ("CRPIX2", 1), ("CRPIX3", 1), ("CRVAL2", 5), ("CRVAL3", 6),
        ("CDELT2", -0.02), ("CDELT3", 0.03),
    ]),
    ((2, 2), [
        ("CTYPE1", "RA---TAN"), ("CTYPE2", "DEC--TAN"), ("CRVAL1", 5), ("CRVAL2", 6),
        ("CDELT1", -0.02), ("CDELT2", 0.03), ("PC1_2", 0.5),
    ]),
    ((3, 2), [
        ("CTYPE1", "VOPT"), ("CTYPE2", "LINEAR"), ("CUNIT1", "m/s"),
        ("CRVAL1", 100.0), ("CDELT1", 100000.0), ("RESTFRQ", 1e9),
    ]),
    ((2, 2), []),
    ((2, 2), [
        ("CTYPE1", "STOKES"), ("CTYPE2", "LINEAR"),
        ("CRPIX1", 1), ("CRVAL1", -1), ("CDELT1", -1),
    ]),
])
def test_fits_axis_maps_and_wcs_variants(tmp_path, oracle, shape, keys):
    source = tmp_path / "coordinates.fits"
    result = tmp_path / "coordinates.im"
    _raw_fits(source, shape, keys)
    expected = oracle.image()
    assert expected.fromfits(infile=str(source))
    actual = tools.image()
    assert actual.fromfits(str(result), str(source))
    _assert_value(actual.getchunk(), expected.getchunk())
    _assert_value(actual.coordsys().torecord(), expected.coordsys().torecord())
    assert actual.done()
    assert actual.open(str(result))
    _assert_value(actual.coordsys().torecord(), expected.coordsys().torecord())
    assert actual.done()
    assert expected.done()


def test_fits_import_preserves_full_history_rows(tmp_path, oracle):
    file = tmp_path / "log.fits"
    _raw_fits(file, (2, 2), history=[
        "CASA START LOGTABLE",
        "2000-01-01T00:00:00 WARN SRCCODE='::test location' OBJID='obj42'",
        "history from FITS",
        "CASA END LOGTABLE",
    ])
    for module, name in [(oracle, "casa.im"), (tools, "casars.im")]:
        image = module.image()
        result = tmp_path / name
        assert image.fromfits(str(result), str(file))
        assert image.done()
        table = oracle.table()
        assert table.open(str(result / "logtable"))
        assert table.getcell("MESSAGE", 0) == "history from FITS"
        assert table.getcell("PRIORITY", 0) == "WARN"
        assert table.getcell("OBJECT_ID", 0) == "obj42"
        assert table.getcell("LOCATION", 0) == "::test location' OBJID='obj42"
        assert table.getcell("TIME", 0) == 51544 * 86400.0
        assert table.done()


@pytest.mark.parametrize("method", ["open", "newimage", "newimagefromfile"])
def test_read_fits_pixels_and_mask_without_importing(tmp_path, oracle, method):
    # FITS detection uses the file content. This path has no extension.
    path = tmp_path / "direct-image"
    path.write_bytes(_hdu(True, [1, -999, 3, 4]))
    parent, expected_parent = tools.image(), oracle.image()
    assert parent.fromshape(shape=[3])
    assert expected_parent.fromshape(shape=[3])
    if method == "open":
        assert parent.open(str(path))
        assert expected_parent.open(str(path))
        image, expected = parent, expected_parent
    else:
        image = getattr(parent, method)(str(path))
        expected = getattr(expected_parent, method)(str(path))
        assert parent.shape().tolist() == [3]
        assert expected_parent.shape().tolist() == [3]
        assert parent.done()
        assert expected_parent.done()
    assert image.ispersistent() and image.name() == str(path)
    assert image.pixeltype() == "float"
    _assert_value(image.haslock(), expected.haslock())
    assert image.unlock() and expected.unlock()
    assert image.lock(writelock=True, nattempts=1) and expected.lock(writelock=True, nattempts=1)
    _assert_value(image.haslock(), expected.haslock())
    assert image.miscinfo() == expected.miscinfo() == {"foobar": 73}
    assert image.history() == expected.history() == ["first custom history"]
    assert image.maskhandler("get") == expected.maskhandler("get") == ["T"]
    assert image.maskhandler("default") == expected.maskhandler("default") == [""]
    _assert_value(image.coordsys().torecord(), expected.coordsys().torecord())
    for args in [{}, {"inc": [2, 1], "dropdeg": True}, {"axes": [0]}, {"blc": [1, 0], "trc": [1, 1]}]:
        _assert_value(image.getchunk(**args), expected.getchunk(**args))
        _assert_value(image.getchunk(getmask=True, **args), expected.getchunk(getmask=True, **args))
    _assert_value(image.pixelvalue([0, 0]), expected.pixelvalue([0, 0]))
    null = image.pixelvalue([1, 0])
    assert null["mask"] is False and np.isnan(null["value"]["value"])
    assert image.done()
    assert expected.done()


@pytest.mark.parametrize("backend", ["casa", "casars"])
def test_fits_metadata_edits_are_per_attachment(tmp_path, oracle, backend):
    backend = oracle if backend == "casa" else tools
    path = tmp_path / "edit.fits"
    payload = _hdu(True, [1, -999, 3, 4])
    path.write_bytes(payload)
    image, independent = backend.image(), backend.image()
    assert image.open(str(path))
    assert independent.open(str(path))
    assert image.setbrightnessunit("K")
    assert image.setmiscinfo({"comment": "this attachment"})
    assert image.sethistory("source", ["this attachment"])
    assert image.setrestoringbeam("4arcsec", "2arcsec", "15deg", log=False)
    coordinates = image.coordsys()
    assert coordinates.setreferencepixel([1.0, 1.0])
    assert image.setcoordsys(coordinates.torecord())
    assert image.pixelvalue()["value"] == {"unit": "K", "value": 4.0}
    assert image.miscinfo() == {"comment": "this attachment"}
    assert image.commonbeam()["major"] == {"unit": "arcsec", "value": 4.0}
    assert "this attachment" in image.history()
    assert independent.brightnessunit() == "Jy/beam"
    assert independent.miscinfo() == {"foobar": 73}
    assert independent.restoringbeam() == {}
    assert independent.history() == ["first custom history"]
    assert independent.done()
    with pytest.raises(RuntimeError, match="FITSImage is not writable"):
        image.putchunk(np.zeros((2, 2)))
    with pytest.raises(RuntimeError, match="Cannot make requested mask"):
        image.calcmask("T")
    with pytest.raises(RuntimeError, match="setDefaultMask cannot be used"):
        image.maskhandler("set", "missing")
    with pytest.raises(RuntimeError, match="renameRegion cannot be used"):
        image.maskhandler("rename", ["missing", "new"])
    with pytest.raises(RuntimeError, match="cannot create mask for virtual image"):
        image.maskhandler("copy", ["missing", "new"])
    with pytest.raises(RuntimeError, match="table is not writable"):
        image.remove()
    assert not image.isopen()
    assert path.read_bytes() == payload
    assert image.open(str(path))
    assert image.brightnessunit() == "Jy/beam"
    assert image.done()


@pytest.mark.skipif(os.name != "posix", reason="Replacing an open FITS path requires POSIX")
@pytest.mark.parametrize("cache", [True, False])
def test_fits_stream_opens_on_first_pixel_read(tmp_path, oracle, cache):
    for index, backend in enumerate([oracle, tools]):
        directory = tmp_path / str(index)
        directory.mkdir()
        path, moved = directory / "source.fits", directory / "moved.fits"
        path.write_bytes(_hdu(True, [1, 2, 3, 4]))
        image = backend.image()
        assert image.open(str(path), cache=cache)
        path.rename(moved)
        path.write_bytes(_hdu(True, [5, 6, 7, 8]))
        expected = np.array([[5., 7.], [6., 8.]])
        _assert_value(image.getchunk(), expected)
        moved.unlink()
        path.rename(moved)
        _assert_value(image.getchunk(), expected)
        path.write_bytes(_hdu(True, [9, 10, 11, 12]))
        moved.unlink()
        _assert_value(image.getchunk(), expected)
        assert image.done()


def test_direct_fits_selects_image_extension_and_preserves_infinities(tmp_path, oracle):
    path = tmp_path / "extension.fits"
    path.write_bytes(
        _hdu(True, None)
        + _hdu(False, [1, np.inf, np.nan, -np.inf], bitpix=-32)
    )
    for backend in [oracle, tools]:
        image = backend.image()
        assert image.open(str(path))
        _assert_value(image.getchunk(), np.array([[1., np.nan], [np.inf, -np.inf]]))
        _assert_value(image.getchunk(getmask=True), np.array([[True, False], [True, True]]))
        assert image.done()


def test_direct_fits_scaled_integer_blanks_without_scratch(tmp_path, monkeypatch, oracle):
    path = tmp_path / "scaled.fits"
    path.write_bytes(_hdu(True, [1, -999, 3, 4], scaling=[("BSCALE", 0.5), ("BZERO", 12)]))
    monkeypatch.setenv("TMPDIR", str(path))
    for backend in [oracle, tools]:
        image = backend.image()
        assert image.open(str(path))
        _assert_value(image.getchunk(), np.array([[12.5, 13.5], [np.nan, 14.0]]))
        _assert_value(image.getchunk(getmask=True), np.array([[True, True], [False, True]]))
        blank = image.pixelvalue([1, 0])
        assert blank["mask"] is False and np.isnan(blank["value"]["value"])
        assert image.done()
