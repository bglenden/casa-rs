"""Public image.makecomplex interop with CASA 6.7.6.14."""

import numpy as np
import pytest

from casars import tools
from test_image_fits import _hdu


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


@pytest.mark.parametrize("kind,result_kind,result_dtype", [
    ("f", "complex", np.complex64), ("d", "dcomplex", np.complex128),
])
def test_makecomplex_native_crop_persistence_and_provenance(casa, tmp_path, kind, result_kind, result_dtype):
    values = np.arange(12, dtype=np.float64).reshape(3, 4)
    if kind == "d":
        values += 1e-9
    region = casa.regionmanager().box([1, 1], [2, 3])
    for module in (casa, tools):
        real_path = str(tmp_path / f"{module.__name__}-real.im")
        imag_path = str(tmp_path / f"{module.__name__}-imag.im")
        out_path = str(tmp_path / f"{module.__name__}-product.im")
        real = module.image()
        imag = module.image()
        assert real.fromarray(real_path, values, type=kind)
        assert imag.fromarray(imag_path, values + 20, type=kind)
        assert real.setmiscinfo({"origin": "real metadata"})
        assert real.sethistory("test", ["real history marker"])
        assert imag.sethistory("test", ["imaginary history marker"])
        if kind == "f":
            assert real.setbrightnessunit("Jy/beam")
            assert real.setrestoringbeam("0.1arcmin", "2arcsec", "2rad")
            expected_beam = real.restoringbeam()
            assert real.calcmask(f'"{real_path}" > 4', name="real", asdefault=True)
            assert imag.calcmask(f'"{imag_path}" < 31', name="imag", asdefault=True)
        assert imag.done()
        before = real.history()
        assert real.makecomplex(out_path, imag_path, region)
        assert real.makecomplex("", imag_path)
        if kind == "f":
            assert real.name() == real_path
        assert real.isopen()
        assert real.pixeltype() == {"f": "float", "d": "double"}[kind]
        assert real.history() == before
        assert real.done()

        expected = (values + 1j * (values + 20))[1:3, 1:4].astype(result_dtype)
        valid = np.ones(expected.shape, dtype=bool)
        if kind == "f":
            valid &= (expected.real > 4) & (expected.imag < 31)
        for reader_module in (casa, tools):
            reader = reader_module.image()
            assert reader.open(out_path)
            assert reader.pixeltype() == result_kind
            pixels = reader.getchunk()
            assert pixels.dtype == np.complex128
            np.testing.assert_array_equal(pixels, expected)
            np.testing.assert_array_equal(reader.getchunk(getmask=True), valid)
            if kind == "f":
                assert reader.miscinfo() == {"origin": "real metadata"}
            else:
                table = reader_module.table()
                assert table.open(out_path)
                assert table.getkeyword("miscinfo") == {"origin": "real metadata"}
                assert table.done()
            history = reader.history()
            assert "real history marker" in history
            assert "imaginary history marker" not in history
            assert history[-2] == "Ran ia.makecomplex"
            assert history[-1].endswith("region={...}, overwrite=false)")
            if kind == "f":
                assert reader.brightnessunit() == "Jy/beam"
                assert reader.restoringbeam() == expected_beam
                assert reader.maskhandler("default") == ["mask0"]
            assert reader.done()


def test_makecomplex_reads_direct_fits_inputs(casa, tmp_path):
    real_path = tmp_path / "real.fits"
    imag_path = tmp_path / "imag.fits"
    real_path.write_bytes(_hdu(True, [1, 2, 3, 4]))
    imag_path.write_bytes(_hdu(True, [8, 9, 10, 11]))
    for module in (casa, tools):
        source = module.image()
        output = str(tmp_path / f"{module.__name__}.im")
        assert source.open(str(real_path))
        assert source.makecomplex(output, str(imag_path))
        assert source.isopen()
        assert source.done()
        for reader_module in (casa, tools):
            reader = reader_module.image()
            assert reader.open(output)
            np.testing.assert_array_equal(reader.getchunk(), [[1 + 8j, 3 + 10j], [2 + 9j, 4 + 11j]])
            assert reader.done()


@pytest.mark.parametrize("length", [102, 104])
def test_makecomplex_resolves_relative_region_in_each_input(casa, tmp_path, length):
    region = casa.regionmanager().box([0], [1], absrel="relref")
    for module in (casa, tools):
        source, imaginary, reader = (module.image() for _ in range(3))
        path = str(tmp_path / f"{module.__name__}-imaginary.im")
        output = str(tmp_path / f"{module.__name__}-result.im")
        values = np.arange(length, dtype=np.float32)
        assert source.fromarray(pixels=values, linear=True)
        assert imaginary.fromarray(path, values, linear=True)
        for tool, reference in ((source, 100.49998), (imaginary, 100.50002)):
            coordinates = tool.coordsys()
            assert coordinates.setreferencepixel([reference])
            assert tool.setcoordsys(coordinates.torecord())
            assert coordinates.done()
        assert imaginary.done()
        if length == 102:
            with pytest.raises(RuntimeError, match="different shapes"):
                source.makecomplex(output, path, region=region)
        else:
            assert source.makecomplex(output, path, region=region)
            assert reader.open(output)
            np.testing.assert_array_equal(reader.getchunk(), [100 + 101j, 101 + 102j])
            assert reader.done()
        assert source.done()


def test_makecomplex_overwrite_preserves_an_open_table(tmp_path):
    real, imaginary, result = (tools.image() for _ in range(3))
    output = str(tmp_path / "output.im")
    imag_path = str(tmp_path / "imaginary.im")
    assert real.fromshape(shape=[3, 4])
    assert imaginary.fromshape(imag_path, [3, 4])
    assert imaginary.done()
    assert result.fromarray(output, np.ones((3, 4), dtype=np.float32))
    assert result.done()
    table = tools.table()
    assert table.open(output, nomodify=False)
    with pytest.raises(RuntimeError, match="in use|open"):
        real.makecomplex(output, imag_path, overwrite=True)
    assert table.done()
    assert result.open(output)
    np.testing.assert_array_equal(result.getchunk(), np.ones((3, 4)))
    assert result.done()
    assert real.done()


def test_makecomplex_accepts_a_saved_ellipsoid_record(casa, tmp_path):
    shape = [7, 7]
    values = np.arange(49, dtype=np.float32).reshape(shape)
    template = casa.image()
    assert template.fromshape(shape=shape)
    coordinates = template.coordsys()
    region = casa.regionmanager().fromtext(
        "ellipse[[3pix, 3pix], [2pix, 1pix], 0deg]", shape, coordinates.torecord()
    )
    assert region["name"] == "WCEllipsoid"
    assert coordinates.done()
    assert template.done()
    products = []
    for module in (casa, tools):
        real, imaginary, reader = (module.image() for _ in range(3))
        path = str(tmp_path / f"{module.__name__}-imaginary.im")
        output = str(tmp_path / f"{module.__name__}-result.im")
        assert real.fromarray(pixels=values)
        assert imaginary.fromarray(path, values + 10)
        assert imaginary.done()
        assert real.makecomplex(output, path, region=region)
        assert real.done()
        assert reader.open(output)
        products.append((reader.getchunk(), reader.getchunk(getmask=True)))
        assert reader.done()
    expected, mask = products[0]
    assert mask.any() and not mask.all()
    np.testing.assert_array_equal(products[1][1], mask)
    np.testing.assert_array_equal(products[1][0][mask], expected[mask])


def test_makecomplex_preserves_units_of_cropped_per_plane_beams(casa, tmp_path):
    region = casa.regionmanager().box([0, 0, 0, 1], [2, 3, 0, 2])
    for module in (casa, tools):
        real, imaginary, reader = (module.image() for _ in range(3))
        path = str(tmp_path / f"{module.__name__}-imaginary.im")
        output = str(tmp_path / f"{module.__name__}-result.im")
        assert real.fromshape(shape=[3, 4, 1, 3])
        assert real.setrestoringbeam("4arcsec", "2arcsec", "0deg", channel=0, polarization=0)
        assert real.setrestoringbeam("0.15arcmin", "3arcsec", "2rad", channel=1, polarization=0)
        assert real.setrestoringbeam("0.2arcmin", "3arcsec", "20deg", channel=2, polarization=0)
        expected = [real.restoringbeam(channel, 0) for channel in (1, 2)]
        assert imaginary.fromshape(path, [3, 4, 1, 3])
        assert imaginary.done()
        assert real.makecomplex(output, path, region=region)
        assert real.done()
        assert reader.open(output)
        assert [reader.restoringbeam(channel, 0) for channel in (0, 1)] == expected
        assert reader.done()


def test_makecomplex_rejected_inputs_and_overwrite(casa, tmp_path):
    values = np.arange(12, dtype=np.float32).reshape(3, 4)
    for module in (casa, tools):
        prefix = str(tmp_path / module.__name__)
        real_path, imag_path, output = (f"{prefix}-{suffix}.im" for suffix in ("real", "imag", "output"))
        real, imag, result = (module.image() for _ in range(3))
        assert real.makecomplex(output, imag_path) is False
        assert real.fromarray(real_path, values)
        assert imag.fromshape(imag_path, [3, 5])
        assert imag.done()
        with pytest.raises(RuntimeError, match="shapes are not identical"):
            real.makecomplex(output, imag_path)
        assert imag.fromshape(imag_path, [3, 4], type="d", overwrite=True)
        assert imag.done()
        with pytest.raises(RuntimeError, match="same precision"):
            real.makecomplex(output, imag_path)
        assert imag.fromshape(imag_path, [3, 4], linear=True, overwrite=True)
        assert imag.done()
        with pytest.raises(RuntimeError, match="not conformant"):
            real.makecomplex(output, imag_path)
        assert imag.fromarray(imag_path, values + 10, overwrite=True)
        assert imag.done()
        assert real.makecomplex(output, imag_path)
        with pytest.raises(RuntimeError, match="exists"):
            real.makecomplex(output, imag_path)
        assert imag.fromarray(imag_path, values + 30, overwrite=True)
        assert imag.done()
        assert real.makecomplex(output, imag_path, overwrite=True)
        assert real.done()
        assert result.open(output)
        np.testing.assert_array_equal(result.getchunk().imag, values + 30)
        assert result.done()


def test_makecomplex_uses_temporary_real_pixels_and_protects_the_imaginary_file(tmp_path):
    values = np.arange(12, dtype=np.float32).reshape(3, 4)
    real, imaginary, result = (tools.image() for _ in range(3))
    imag_path = str(tmp_path / "imaginary.im")
    output = str(tmp_path / "result.im")
    assert real.fromarray(pixels=values * 2)
    assert real.dohistory(False)
    assert imaginary.fromarray(imag_path, values * -3)
    assert imaginary.done()
    with pytest.raises(RuntimeError, match="matches or contains input"):
        real.makecomplex(imag_path, imag_path, overwrite=True)
    assert imaginary.open(imag_path)
    np.testing.assert_array_equal(imaginary.getchunk(), values * -3)
    assert imaginary.done()
    assert real.makecomplex(outfile=output, imag=imag_path)
    assert result.open(output)
    np.testing.assert_array_equal(result.getchunk(), values * (2 - 3j))
    assert "Ran ia.makecomplex" not in result.history()
    np.testing.assert_array_equal(real.getchunk(), values * 2)
    assert result.done()
    assert real.done()
