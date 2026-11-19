"""Native products from the image tool's FITS factories."""

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


def _hdu(primary, pixels, *, alternate=False, bitpix=16):
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
    header = b"".join(_card(key, value) for key, value in keys)
    header += b"HISTORY first custom history".ljust(80) + b"END".ljust(80)
    values = b"" if pixels is None else np.asarray(pixels, dtype=">i2" if bitpix > 0 else ">f4").tobytes()
    return header.ljust((-len(header) % 2880) + len(header)) + values.ljust((-len(values) % 2880) + len(values), b"\0")


@pytest.fixture
def oracle():
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    return casa


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
