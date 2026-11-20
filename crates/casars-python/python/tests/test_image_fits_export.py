"""Pinned CASA evidence for FITS files written by the public image tool."""

import os
import subprocess
import sys
import time

import numpy as np
import pytest

from casars import tools
from test_exportfits import _parse_fits_primary
from test_image_fits import _hdu


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
@pytest.mark.parametrize("cropped", [False, True])
def test_export_uses_the_open_tools_beams_and_reacquires_read_lock(casa, tmp_path, cropped):
    selection = {"region": casa.regionmanager().box([0, 0], [1, 2])} if cropped else {}
    for backend in (casa, tools):
        source = str(tmp_path / f"{backend.__name__}.im")
        target = tmp_path / f"{backend.__name__}.fits"
        writer, reader = backend.image(), backend.image()
        assert writer.fromshape(source, [3, 4])
        assert writer.setrestoringbeam("4arcsec", "2arcsec", "12deg")
        assert reader.open(source)
        assert writer.setrestoringbeam("8arcsec", "4arcsec", "27deg")
        assert reader.unlock()
        np.testing.assert_array_equal(reader.haslock(), [False, False])
        assert reader.tofits(str(target), **selection)
        np.testing.assert_array_equal(reader.haslock(), [True, False])
        header, _, _ = _parse_fits_primary(target)
        assert header["BMAJ"] == pytest.approx(4 / 3600)
        assert header["BMIN"] == pytest.approx(2 / 3600)
        assert header["BPA"] == pytest.approx(12)
        assert (header["NAXIS1"], header["NAXIS2"]) == ((2, 3) if cropped else (3, 4))
        assert reader.done()
        assert writer.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_lazy_imagecalc_export_reads_native_operand(casa, tmp_path):
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    for backend, peer in ((casa, tools), (tools, casa)):
        path = str(tmp_path / f"{backend.__name__}.im")
        destination = str(tmp_path / f"{backend.__name__}.fits")
        source = backend.image()
        assert source.fromarray(path, pixels)
        assert source.done()
        result = source.imagecalc(pixels=f'"{path}" + 1')
        assert result.unlock()
        np.testing.assert_array_equal(result.haslock(), [False, False])
        assert result.tofits(destination)
        np.testing.assert_array_equal(result.haslock(), [True, False])
        reader = peer.image()
        assert reader.open(destination)
        np.testing.assert_array_equal(reader.getchunk(), pixels + 1)
        assert reader.done()
        assert result.done()


def test_lazy_export_preserves_native_and_fits_operands(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    source = tools.image()
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    assert source.fromarray("source.im", pixels)
    assert source.tofits("source.fits")
    assert source.done()

    for operand, target, message in (
        ("source.im", tmp_path / "source.im" / "table.dat", "inside input image"),
        ("source.fits", tmp_path / "source.fits", "matches or contains input image"),
    ):
        original = target.read_bytes()
        result = source.imagecalc(pixels=f'"{operand}" + 1')
        with pytest.raises(RuntimeError, match=message):
            result.tofits(str(target), overwrite=True)
        assert target.read_bytes() == original
        np.testing.assert_array_equal(result.getchunk(), pixels + 1)
        assert result.done()


@pytest.mark.skipif(os.name != "posix", reason="casacore file locks require POSIX")
def test_export_rejects_invalid_requests_while_a_writer_holds_the_lock(tmp_path):
    path = tmp_path / "source.im"
    target = tmp_path / "existing.fits"
    target.write_bytes(b"existing output")
    writer = tools.image()
    assert writer.fromshape(str(path), [3, 4])
    assert writer.unlock()
    code = """
import sys
from pathlib import Path

from casars import tools

reader = tools.image()
assert reader.open("source.im")
assert reader.unlock()
Path("ready").touch()
assert sys.stdin.readline().strip() == "locked"
for options, message in (
    ({}, "FITS output already exists"),
    ({"bitpix": 8, "overwrite": True}, "unsupported bitpix"),
):
    try:
        reader.tofits("existing.fits", **options)
    except RuntimeError as error:
        assert message in str(error), error
    else:
        raise AssertionError("accepted invalid FITS request")
assert reader.haslock().tolist() == [False, False]
assert reader.done()
"""
    with subprocess.Popen(
        [sys.executable, "-c", code], cwd=tmp_path,
        stdin=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ) as child:
        try:
            deadline = time.monotonic() + 10
            ready = tmp_path / "ready"
            while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
                time.sleep(0.02)
            assert ready.exists()
            assert writer.lock(writelock=True, nattempts=1)
            _, stderr = child.communicate("locked\n", timeout=10)
            assert child.returncode == 0, stderr
            assert target.read_bytes() == b"existing output"
        finally:
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=5)
            writer.done()


@pytest.mark.parametrize("persistent", [False, True])
@pytest.mark.parametrize("bitpix", [-32, 16])
def test_export_cross_reads_pixels_and_metadata(casa, tmp_path, persistent, bitpix):
    values = np.arange(3 * 4 * 2 * 5, dtype=np.float32).reshape(3, 4, 2, 5)
    headers = []
    for writer_module in (casa, tools):
        writer = writer_module.image()
        infile = str(tmp_path / f"{writer_module.__name__}.im") if persistent else ""
        assert writer.fromarray(infile, values)
        assert writer.setbrightnessunit("Jy/beam")
        assert writer.setrestoringbeam("4arcsec", "2arcsec", "120deg")
        assert writer.setmiscinfo({"product": "example"})
        assert writer.sethistory("test", ["FITS export marker"])
        output = tmp_path / f"{writer_module.__name__}.fits"
        assert writer.tofits(str(output), bitpix=bitpix, stokeslast=False)
        assert writer.isopen()
        header, history, _ = _parse_fits_primary(output)
        headers.append(header)
        assert header["BITPIX"] == bitpix
        assert header["CTYPE3"] == "STOKES"
        assert header["CTYPE4"] == "FREQ"
        assert header["BPA"] == pytest.approx(-60)
        assert header["PRODUCT"] == "example"
        assert any("FITS export marker" in line for line in history)
        for reader_module in (casa, tools):
            reader = reader_module.image()
            assert reader.open(str(output))
            assert reader.brightnessunit() == "Jy/beam"
            assert reader.restoringbeam()["major"]["value"] == pytest.approx(4)
            np.testing.assert_allclose(reader.getchunk(), values, atol=0.002, rtol=0)
            np.testing.assert_array_equal(reader.getchunk(getmask=True), np.ones(values.shape, dtype=bool))
            assert reader.done()
        assert writer.done()
    for key in ("NAXIS", "NAXIS1", "NAXIS2", "NAXIS3", "NAXIS4", "VELREF", "SPECSYS"):
        assert headers[0][key] == headers[1][key], key
    for key in ("CRPIX1", "CRPIX2", "CDELT1", "CDELT2", "CRVAL4", "CDELT4", "BMAJ", "BMIN", "BPA"):
        assert headers[1][key] == pytest.approx(headers[0][key], rel=1e-9), key


@pytest.mark.parametrize("options,ctype", [
    ({"velocity": True}, "VOPT"),
    ({"velocity": True, "optical": False}, "VRAD"),
    ({"wavelength": True}, "WAVE"),
    ({"wavelength": True, "airwavelength": True}, "AWAV"),
    ({"airwavelength": True}, "FREQ"),
    ({"dropstokes": True}, "FREQ"),
    ({"deglast": True, "stokeslast": False}, "FREQ"),
    ({"dropdeg": True}, "FREQ"),
])
def test_export_spectral_and_axis_options(casa, tmp_path, options, ctype):
    outputs = []
    for module in (casa, tools):
        tool = module.image()
        assert tool.fromshape(shape=[3, 4, 1, 5])
        output = tmp_path / f"{module.__name__}.fits"
        assert tool.tofits(str(output), history=False, **options)
        header, history, _ = _parse_fits_primary(output)
        assert not history
        assert header["CTYPE3"] == ctype
        outputs.append(header)
        assert tool.done()
    for key in ("NAXIS", "NAXIS1", "NAXIS2", "NAXIS3", "NAXIS4", "CTYPE4", "CUNIT3", "VELREF"):
        assert outputs[0].get(key) == outputs[1].get(key), key
    for key in ("CRPIX3", "CRVAL3", "CDELT3", "RESTFRQ"):
        assert outputs[1][key] == pytest.approx(outputs[0][key], rel=5e-9), key


def test_export_preserves_nonzero_slant_sin_parameters(casa, tmp_path):
    shape = [10, 8]
    oracle = casa.image()
    assert oracle.fromshape(shape=shape)
    record = oracle.coordsys().torecord()
    direction = record["direction0"]
    direction["projection_parameters"] = np.array([0.5, -0.3])
    direction["cdelt"] = np.array([-60.0, 60.0])
    assert oracle.setcoordsys(record)
    pixels = [[1, 2], [5, 3], [7, 6]]
    native_coordinates = oracle.coordsys()
    assert native_coordinates.setunits(["rad", "rad"])
    expected = [native_coordinates.toworld(pixel)["numeric"] for pixel in pixels]
    assert native_coordinates.done()
    assert oracle.done()

    for module in (casa, tools):
        writer = module.image()
        assert writer.fromshape(shape=shape, csys=record)
        output = tmp_path / f"{module.__name__}.fits"
        assert writer.tofits(str(output), history=False)
        header, _, _ = _parse_fits_primary(output)
        assert (header["PV2_1"], header["PV2_2"]) == pytest.approx((0.5, -0.3))
        assert writer.done()
        reader = casa.image()
        assert reader.open(str(output))
        fits_coordinates = reader.coordsys()
        assert fits_coordinates.setunits(["rad", "rad"])
        actual = [fits_coordinates.toworld(pixel)["numeric"] for pixel in pixels]
        np.testing.assert_allclose(actual, expected, atol=1e-10, rtol=0)
        assert fits_coordinates.done()
        assert reader.done()


def test_export_moves_stokes_past_spectral_and_linear_axes(casa, tmp_path):
    shape = (3, 4, 2, 5, 7)
    values = np.arange(np.prod(shape), dtype=np.float32).reshape(shape)
    for module in (casa, tools):
        coordinates = module.coordsys().newcoordsys(direction=True, stokes="I Q", spectral=True, linear=1)
        tool = module.image()
        assert tool.fromarray(pixels=values, csys=coordinates.torecord())
        output = tmp_path / f"{module.__name__}.fits"
        assert tool.tofits(str(output), stokeslast=True, history=False)
        header, _, _ = _parse_fits_primary(output)
        assert [header[f"NAXIS{axis}"] for axis in range(1, 6)] == [3, 4, 7, 5, 2]
        assert [header[f"CTYPE{axis}"].upper() for axis in range(3, 6)] == ["LINAXIS0", "FREQ", "STOKES"]
        raw = output.read_bytes()
        end = next(i for i in range(0, len(raw), 80) if raw[i:i + 8] == b"END     ") + 80
        written = np.frombuffer(raw, dtype=">f4", count=values.size, offset=((end + 2879) // 2880) * 2880)
        np.testing.assert_array_equal(written, values.transpose(0, 1, 4, 3, 2).ravel(order="F"))
        assert tool.done()
        assert coordinates.done()


def test_export_combines_order_flags_without_changing_pixel_count(casa, tmp_path):
    shape = [3, 4, 2, 1]
    values = np.arange(24, dtype=np.float32).reshape(shape)
    for module, spectral_extent in [(casa, 2), (tools, 1)]:
        writer = module.image()
        path = tmp_path / f"{module.__name__}.fits"
        assert writer.fromarray(pixels=values)
        assert writer.tofits(str(path), deglast=True, stokeslast=True)
        header, _, _ = _parse_fits_primary(path)
        assert [header[f"NAXIS{axis}"] for axis in range(1, 5)] == [3, 4, spectral_extent, 2]
        assert header["CTYPE3"] == "FREQ"
        assert writer.done()
    for module in (casa, tools):
        reader = module.image()
        assert reader.open(str(tmp_path / "casars.tools.fits"))
        np.testing.assert_array_equal(reader.getchunk(), values.transpose(0, 1, 3, 2))
        assert reader.done()


def test_export_moves_a_leading_singleton_linear_axis(casa, tmp_path):
    values = np.arange(8, dtype=np.float32).reshape(1, 4, 2)
    casa_writer = casa.image()
    assert casa_writer.fromarray(pixels=values, linear=True)
    with pytest.raises(RuntimeError, match="Unknown error copying image to FITS file"):
        casa_writer.tofits(str(tmp_path / "casa.fits"), deglast=True)
    assert casa_writer.done()

    output = str(tmp_path / "casars.fits")
    writer = tools.image()
    assert writer.fromarray(pixels=values, linear=True)
    assert writer.tofits(output, deglast=True)
    assert writer.done()
    reader = casa.image()
    assert reader.open(output)
    np.testing.assert_array_equal(reader.getchunk(), values.transpose(1, 2, 0))
    assert reader.done()


@pytest.mark.parametrize("stokes,first,step", [("XX YY", -5, -1), ("I V", 1, 3)])
def test_export_preserves_polarization_codes(casa, tmp_path, stokes, first, step):
    for module in (casa, tools):
        writer = module.image()
        assert writer.fromshape(shape=[3, 4, 2, 3])
        coordinates = writer.coordsys()
        assert coordinates.setstokes(stokes)
        assert writer.setcoordsys(coordinates.torecord())
        assert coordinates.done()
        output = tmp_path / f"{module.__name__}.fits"
        assert writer.tofits(str(output), history=False)
        header, _, _ = _parse_fits_primary(output)
        assert (header["CRVAL4"], header["CDELT4"]) == (first, step)
        assert writer.done()
        reader = casa.image()
        assert reader.open(str(output))
        assert list(reader.coordsys().stokes()) == stokes.split()
        assert reader.done()


def test_dropstokes_keeps_a_sole_stokes_coordinate(casa, tmp_path):
    for module in (casa, tools):
        writer = module.image()
        coordinates = module.coordsys().newcoordsys(stokes="I")
        assert writer.fromshape(shape=[1], csys=coordinates.torecord())
        assert coordinates.done()
        output = tmp_path / f"{module.__name__}.fits"
        assert writer.tofits(str(output), dropstokes=True)
        header, _, _ = _parse_fits_primary(output)
        assert (header["NAXIS"], header["NAXIS1"], header["CTYPE1"]) == (1, 1, "STOKES")
        assert writer.done()


def test_export_crops_and_wraps_per_plane_beams(casa, tmp_path):
    region = casa.regionmanager().box([0, 0, 0, 1], [2, 3, 0, 2])
    for module in (casa, tools):
        writer = module.image()
        assert writer.fromshape(shape=[3, 4, 1, 3])
        for channel, angle in enumerate([10, 120, 155]):
            assert writer.setrestoringbeam(
                f"{4 + channel}arcsec", "2arcsec", f"{angle}deg", channel=channel, polarization=0
            )
        output = tmp_path / f"{module.__name__}.fits"
        assert writer.tofits(str(output), region=region, history=False)
        header, _, _ = _parse_fits_primary(output)
        assert header["CASAMBM"] is True
        assert writer.done()
        for reader_module in (casa, tools):
            reader = reader_module.image()
            assert reader.open(str(output))
            for channel, angle in enumerate([-60, -25]):
                beam = reader.restoringbeam(channel=channel, polarization=0)
                assert beam["major"]["value"] == pytest.approx(5 + channel)
                assert beam["positionangle"]["value"] == pytest.approx(angle)
            assert reader.done()


def test_export_region_mask_and_short_scaling(casa, tmp_path):
    source = str(tmp_path / "source.im")
    values = np.arange(20, dtype=np.float32).reshape(4, 5)
    writer = casa.image()
    assert writer.fromarray(source, values)
    assert writer.calcmask(f'"{source}" < 18', name="valid", asdefault=True)
    assert writer.done()
    region = casa.regionmanager().box([1, 1], [3, 3])
    expected = values[1:4, 1:4]
    valid = (expected > 8) & (expected < 18)
    headers = []
    for module in (casa, tools):
        source_tool = module.image()
        assert source_tool.open(source)
        output = tmp_path / f"{module.__name__}.fits"
        assert source_tool.tofits(str(output), region=region, mask=f'"{source}" > 8', bitpix=16, minpix=8, maxpix=16)
        header, _, raw = _parse_fits_primary(output)
        headers.append(header)
        np.testing.assert_array_equal(raw == header["BLANK"], ~valid.ravel(order="F"))
        for reader_module in (casa, tools):
            reader = reader_module.image()
            assert reader.open(str(output))
            np.testing.assert_array_equal(reader.getchunk(getmask=True), valid)
            np.testing.assert_allclose(reader.getchunk()[valid], np.minimum(expected[valid], 16), atol=0.001, rtol=0)
            assert reader.done()
        assert source_tool.done()
    for key in ("CRPIX1", "CRPIX2", "BLANK", "DATAMIN", "DATAMAX", "BSCALE", "BZERO"):
        assert headers[1][key] == pytest.approx(headers[0][key], rel=1e-9), key


def test_export_direct_fits_and_its_integer_blanks(casa, tmp_path):
    source = tmp_path / "source.fits"
    source.write_bytes(_hdu(True, [1, -999, 3, 4]))
    for module in (casa, tools):
        writer = module.image()
        assert writer.open(str(source))
        output = str(tmp_path / f"{module.__name__}.fits")
        assert writer.tofits(output)
        for reader_module in (casa, tools):
            reader = reader_module.image()
            assert reader.open(output)
            np.testing.assert_array_equal(reader.getchunk(getmask=True), [[True, True], [False, True]])
            np.testing.assert_allclose(reader.getchunk(), [[1, 3], [np.nan, 4]], equal_nan=True)
            assert reader.done()
        assert writer.done()


def test_export_stretches_a_direction_mask_across_planes(casa, tmp_path):
    mask_path = tmp_path / "mask.im"
    mask_pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    mask = casa.image()
    assert mask.fromarray(str(mask_path), mask_pixels)
    assert mask.done()
    shape = [3, 4, 2, 3]
    expected = np.broadcast_to((mask_pixels > 4)[:, :, None, None], shape)
    for module in (casa, tools):
        writer = module.image()
        assert writer.fromshape(shape=shape)
        output = tmp_path / f"{module.__name__}.fits"
        expression = f'"{mask_path}" > 4'
        with pytest.raises(RuntimeError):
            writer.tofits(str(output), mask=expression, stokeslast=False)
        assert not output.exists()
        assert writer.tofits(str(output), mask=expression, stokeslast=False, stretch=True)
        assert writer.done()
        reader = casa.image()
        assert reader.open(str(output))
        np.testing.assert_array_equal(reader.getchunk(getmask=True), expected)
        assert reader.done()


def test_export_rejects_invalid_names_types_and_preserves_an_existing_product(tmp_path):
    source = tmp_path / "source.im"
    destination = tmp_path / "image.fits"
    tool = tools.image()
    assert tool.tofits() is False
    assert tool.fromshape(str(source), [3, 4])
    with pytest.raises(RuntimeError, match="specified"):
        tool.tofits()
    with pytest.raises(RuntimeError, match="Invalid fitsfile name"):
        tool.tofits("..")
    assert tool.tofits(str(destination))
    original = destination.read_bytes()
    with pytest.raises(RuntimeError, match="already exists"):
        tool.tofits(str(destination))
    with pytest.raises(RuntimeError, match="bitpix"):
        tool.tofits(str(destination), bitpix=8, overwrite=True)
    with pytest.raises(RuntimeError, match="inside input image"):
        tool.tofits(str(source / "table.dat"), overwrite=True)
    assert destination.read_bytes() == original
    assert tool.done()

    assert tool.fromshape(shape=[1, 1, 1, 1])
    with pytest.raises(RuntimeError, match="cannot drop all axes"):
        tool.tofits(str(destination), dropdeg=True, overwrite=True)
    assert destination.read_bytes() == original
    assert tool.done()

    assert tool.fromshape(shape=[3, 4], type="d")
    with pytest.raises(RuntimeError, match="Only writing float-valued"):
        tool.tofits(str(destination), overwrite=True)
    assert tool.done()


def test_rejected_export_leaves_the_fits_reader_lazy(tmp_path):
    source = tmp_path / "source.fits"
    source.write_bytes(_hdu(True, [1, 2, 3, 4]))
    destination = tmp_path / "existing.fits"
    destination.write_bytes(b"existing product")
    writer = tools.image()
    assert writer.open(str(source))
    with pytest.raises(RuntimeError, match="already exists"):
        writer.tofits(str(destination))
    replacement = tmp_path / "replacement.fits"
    replacement.write_bytes(_hdu(True, [5, 6, 7, 8]))
    os.replace(replacement, source)
    np.testing.assert_array_equal(writer.getchunk(), [[5, 7], [6, 8]])
    assert writer.done()


def test_buffered_export_write_failure_preserves_destination(tmp_path):
    pytest.importorskip("resource")
    destination = tmp_path / "existing.fits"
    destination.write_bytes(b"existing product")
    code = """
import resource
import signal
import sys
from casars import tools

image = tools.image()
assert image.fromshape(shape=[3, 4])
signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
_, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
resource.setrlimit(resource.RLIMIT_FSIZE, (3200, hard))
try:
    image.tofits(sys.argv[1], overwrite=True, history=False)
except RuntimeError as error:
    assert 'finalizing staged FITS output' in str(error), str(error)
else:
    raise AssertionError('FITS export succeeded past the process file-size limit')
"""
    process = subprocess.run(
        [sys.executable, "-c", code, str(destination)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert process.returncode == 0, process.stderr
    assert destination.read_bytes() == b"existing product"
    assert not list(tmp_path.glob(".exportfits.*"))
