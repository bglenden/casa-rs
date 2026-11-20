"""Pixel and world region bounds from the CASA 6.7.6.14 image tool."""

from pathlib import Path
from shutil import copytree

import numpy as np
import pytest

from casars import tools
from test_image_fits import _hdu
from test_image_workflow import _assert_value


@pytest.fixture
def casa():
    package = pytest.importorskip("casatools")
    assert package.version_string().replace("-", ".") == "6.7.6.14"
    return package


@pytest.mark.parametrize("kind", ["f", "c"])
def test_pixel_stride_and_world_corner_order(casa, kind):
    shape = [10, 12, 5]
    rust, oracle = tools.image(), casa.image()
    region = casa.regionmanager().box([1, 2, 0], [8, 9, 4], [3, 3, 3])
    for image in (rust, oracle):
        assert image.boundingbox() == {}
        assert image.fromshape(shape=shape, type=kind)
    actual = rust.boundingbox(region=region)
    _assert_value(actual, oracle.boundingbox(region))
    expected = {
        "bbShape": [7, 7, 4], "blc": [1, 2, 0], "trc": [7, 8, 3],
        "imageShape": shape, "inc": [3, 3, 3], "regionShape": [3, 3, 2],
    }
    for field, values in expected.items():
        _assert_value(actual[field], np.array(values, dtype=np.int64))
    for full_image in ({}, [True], ""):
        _assert_value(rust.boundingbox(full_image), oracle.boundingbox())
    for image in (rust, oracle):
        with pytest.raises(RuntimeError, match="Unsupported type for region"):
            image.boundingbox(True)
        assert image.shape().tolist() == shape
        assert image.done()


@pytest.mark.parametrize("kind,precision", [("d", "double"), ("cd", "complex<double>")])
def test_double_precision_rejected(casa, kind, precision):
    for module in (tools, casa):
        image = module.image()
        assert image.fromshape(shape=[3, 4], type=kind)
        with pytest.raises(RuntimeError, match=f"boundingbox does not support images with {precision} precision"):
            image.boundingbox()
        assert image.done()


def test_one_dimensional_pixel_regions_without_a_direction_coordinate(casa):
    rust, oracle = tools.image(), casa.image()
    for image in (rust, oracle):
        assert image.fromshape(shape=[15])
    for step in (1, 3):
        region = casa.regionmanager().box([1], [8], [step])
        _assert_value(rust.boundingbox(region), oracle.boundingbox(region))
    assert rust.done() and oracle.done()


def test_compound_world_regions(casa):
    rust, oracle = tools.image(), casa.image()
    shape = [10, 12, 5]
    for image in (rust, oracle):
        assert image.fromshape(shape=shape)
    rg = casa.regionmanager()
    coords = oracle.coordsys().torecord()
    members = {
        "0": rg.fromtext("box [[1pix, 2pix], [4pix, 5pix]]", shape, coords),
        "1": rg.fromtext("box [[3pix, 4pix], [7pix, 9pix]]", shape, coords),
    }
    for compound in (rg.makeunion(members), rg.intersection(members)):
        _assert_value(rust.boundingbox(compound), oracle.boundingbox(compound))
    assert rust.done() and oracle.done()


def test_reference_relative_polygon_as_record_and_file(casa, tmp_path):
    fixtures = Path(__file__).resolve().parents[3] / "casa-images" / "tests" / "data" / "imsubimage"
    table = tmp_path / "source.im"
    copytree(fixtures / "cube.im", table)
    region_file = str(fixtures / "saved_relative_polygon_ref.rgn")
    record = casa.regionmanager().fromfiletorecord(region_file, verbose=False)
    rust, oracle = tools.image(), casa.image()
    for tool in (rust, oracle):
        assert tool.open(str(table))
    expected = oracle.boundingbox(region_file)
    for tool in (rust, oracle):
        _assert_value(tool.boundingbox(record), expected)
        _assert_value(tool.boundingbox(region_file), expected)
        assert tool.done()


def test_galactic_fits_bounds_use_fixed_decimal_degrees(casa, tmp_path):
    path = tmp_path / "galactic.fits"
    path.write_bytes(
        _hdu(True, [1, 2, 3, 4]).replace(b"RA---SIN", b"GLON-SIN").replace(b"DEC--SIN", b"GLAT-SIN")
    )
    rust, oracle = tools.image(), casa.image()
    for tool in (rust, oracle):
        assert tool.open(str(path))
    bounds = rust.boundingbox()
    _assert_value(bounds, oracle.boundingbox())
    assert bounds["blcf"] == "5.000000deg, 6.000000deg"
    assert rust.done() and oracle.done()


def test_crtf_region_and_direct_fits_attachment(casa, tmp_path):
    path = tmp_path / "image.fits"
    content = _hdu(True, [1, -999, 3, 4])
    path.write_bytes(content)
    rust, oracle = tools.image(), casa.image()
    for image in (rust, oracle):
        assert image.open(str(path))
    crtf = "box [[1pix, 0pix], [1pix, 1pix]]"
    _assert_value(rust.boundingbox(), oracle.boundingbox())
    _assert_value(rust.boundingbox(crtf), oracle.boundingbox(crtf))
    assert rust.done() and oracle.done()
    assert path.read_bytes() == content


@pytest.mark.parametrize("native_writer", ["casa", "rust"])
def test_saved_region_strings(casa, tmp_path, monkeypatch, native_writer):
    monkeypatch.chdir(tmp_path)
    rust, oracle = tools.image(), casa.image()
    writer = oracle if native_writer == "casa" else rust
    assert writer.fromshape("source.im", [10, 12, 5]) and writer.done()
    rg = casa.regionmanager()
    selection = rg.box([1, 2, 0], [8, 9, 4], [3, 3, 3])
    assert rg.tofile("stride.rgn", selection)
    assert rg.fromrecordtotable("source.im", "box1", selection) == "box1"
    for tool in (rust, oracle):
        assert tool.open("source.im")
    expected = oracle.boundingbox(selection)
    for value in ("stride.rgn", "box1", "source.im:box1"):
        _assert_value(oracle.boundingbox(value), expected)
        _assert_value(rust.boundingbox(value), expected)
    assert rust.done() and oracle.done()
