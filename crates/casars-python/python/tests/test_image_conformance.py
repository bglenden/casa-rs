"""Conformability checks against the public CASA 6.7.6.14 tool."""

from pathlib import Path

import numpy as np
import pytest

from casars import tools
from test_image_fits import _card, _hdu


@pytest.fixture
def casa():
    module = pytest.importorskip("casatools")
    assert module.version_string().replace("-", ".") == "6.7.6.14"
    return module


@pytest.mark.parametrize("change,expected", [
    ("same", True),
    ("shape", False),
    ("telescope", True),
    ("name", False),
    ("near", True),
    ("reference_pixel", False),
    ("matrix", False),
    ("rescaled_direction_matrix", True),
    ("direction_display_units", False),
    ("omitted_latitude_pole", True),
    ("conversion", False),
    ("spectral_rest_frequency", False),
    ("spectral_matrix", False),
    ("tabular_spectral_matrix", True),
    ("legacy_velocity", False),
    ("stokes_labels", True),
])
def test_isconform_coordinates_match_casa(casa, tmp_path, change, expected):
    for module in (casa, tools):
        shape = [3, 4, 2, 5]
        source = module.image()
        assert source.fromshape(shape=shape)
        coordinates = source.coordsys()
        record = coordinates.torecord()
        target_shape = shape.copy()
        if change == "shape":
            target_shape[0] += 1
        elif change == "telescope":
            record["telescope"] = "VLA"
        elif change == "name":
            record["direction0"]["axes"] = ["RA", "Declination"]
        elif change in ("near", "reference_pixel"):
            offset = 1e-7 if change == "near" else 1e-3
            record["direction0"]["crpix"][0] += offset
        elif change == "matrix":
            record["direction0"]["pc"] = np.array([[0.9, 0.1], [-0.1, 0.9]])
        elif change == "rescaled_direction_matrix":
            record["direction0"]["pc"][0, 0] = 2.0
            record["direction0"]["cdelt"][0] /= 2.0
        elif change == "direction_display_units":
            record["direction0"]["units"] = ["deg", "deg"]
            for field in ("cdelt", "crval"):
                record["direction0"][field] /= 60.0
        elif change == "omitted_latitude_pole":
            del record["direction0"]["latpole"]
        elif change == "conversion":
            record["direction0"]["conversionSystem"] = "GALACTIC"
        elif change == "spectral_rest_frequency":
            record["spectral2"]["restfreq"] += 1e6
            record["spectral2"]["restfreqs"][0] += 1e6
        elif change == "spectral_matrix":
            record["spectral2"]["wcs"]["pc"] = 2.0
        elif change == "tabular_spectral_matrix":
            spectral = record["spectral2"]
            # CASA's tabular spectral constructor starts its samples at pixel zero.
            spectral["wcs"]["crpix"] = 0.0
            assert source.setcoordsys(record)
            wcs = spectral.pop("wcs")
            pixels = np.arange(shape[3], dtype=np.float64)
            worlds = wcs["crval"] + pixels * wcs["cdelt"]
            spectral["tabular"] = {
                "axes": [spectral["name"]], "units": ["Hz"],
                "crpix": np.array([wcs["crpix"]]), "crval": np.array([wcs["crval"]]),
                "cdelt": np.array([wcs["cdelt"]]), "pc": np.array([[wcs["pc"]]]),
                "pixelvalues": pixels,
                "worldvalues": worlds,
            }
        elif change == "legacy_velocity":
            spectral = record["spectral2"]
            del spectral["velType"]
            spectral["prefVelType"] = 1
        elif change == "stokes_labels":
            record["stokes1"]["stokes"] = ["RR", "LL"]
        coordinates.done()

        path = str(tmp_path / f"{module.__name__}.im")
        writer = module.image()
        assert writer.fromshape(path, target_shape, csys=record)
        assert writer.setbrightnessunit("Jy")
        assert writer.done()
        assert source.isconform(other=path) is expected
        assert source.isopen()
        assert source.done()


@pytest.mark.parametrize("pixeltype,accepted", [("f", True), ("c", True), ("d", False), ("cd", False)])
def test_isconform_target_pixel_types(casa, tmp_path, pixeltype, accepted):
    for module in (casa, tools):
        source = module.image()
        assert source.fromshape(shape=[3, 2])
        writer = module.image()
        path = str(tmp_path / f"{module.__name__}.im")
        assert writer.fromshape(path, [3, 2], type=pixeltype)
        assert writer.done()
        if accepted:
            assert source.isconform(path)
        else:
            with pytest.raises(RuntimeError, match="Unsupported image pixel data type"):
                source.isconform(path)
        assert source.done()


def test_isconform_reads_a_direct_fits_attachment(casa, tmp_path):
    path = tmp_path / "image.fits"
    original = _hdu(True, [1, 2, 3, 4])
    path.write_bytes(original)
    other = tmp_path / "shifted.fits"
    other.write_bytes(original.replace(_card("CRVAL1", 5.0), _card("CRVAL1", 7.0), 1))
    for module in (casa, tools):
        source = module.image()
        assert source.open(str(path))
        assert source.isconform(str(path))
        assert not source.isconform(str(other))
        assert source.done()


def test_isconform_reads_native_tabular_spectral_coordinates(casa, tmp_path):
    shape = [3, 4, 2, 5]
    writer = casa.image()
    assert writer.fromshape(shape=shape)
    record = writer.coordsys().torecord()
    spectral = record["spectral2"]
    wcs = spectral.pop("wcs")
    pixels = wcs["crpix"] + np.arange(shape[3], dtype=np.float64)
    spectral["tabular"] = {
        "axes": [spectral["name"]], "units": ["Hz"],
        "crpix": np.array([wcs["crpix"]]), "crval": np.array([wcs["crval"]]),
        "cdelt": np.array([wcs["cdelt"]]), "pc": np.array([[wcs["pc"]]]),
        "pixelvalues": pixels,
        "worldvalues": wcs["crval"] + (pixels - wcs["crpix"]) * wcs["cdelt"],
    }
    assert writer.done()
    path = str(tmp_path / "shifted.im")
    assert writer.fromshape(path, shape, csys=record)
    assert writer.done()
    for module in (casa, tools):
        source = module.image()
        assert source.fromshape(shape=shape)
        assert not source.isconform(path)
        assert source.done()
        assert source.open(path)
        assert source.isconform(path)
        assert source.done()


def test_isconform_detached_and_unreadable_target(casa, tmp_path):
    path = tmp_path / "unreadable"
    path.write_text("not an image", encoding="utf-8")
    for module in (casa, tools):
        source = module.image()
        assert source.isconform("") is False
        assert source.fromshape(shape=[3, 2])
        errors = {
            "": "File name is empty",
            str(tmp_path / "missing.im"): "does not exist",
            str(path): "Unable to open image",
        }
        for other, message in errors.items():
            with pytest.raises(RuntimeError, match=message):
                source.isconform(other)
        assert source.done()


def test_isconform_compares_linear_geometry_without_reading_pixels(tmp_path):
    source = tools.image()
    assert source.fromshape(shape=[4, 5], linear=True)
    values = np.arange(20, dtype=np.float32).reshape(4, 5)
    target = str(Path(tmp_path) / "linear.im")
    writer = tools.image()
    assert writer.fromarray(target, values, linear=True)
    assert source.isconform(target)
    record = writer.coordsys().torecord()
    record["linear0"]["crval"][1] += 10
    assert writer.setcoordsys(record)
    assert writer.done()
    assert not source.isconform(target)
    assert source.done()


@pytest.mark.parametrize("different", [False, True])
def test_isconform_compares_tabular_samples(casa, tmp_path, different):
    for module in (casa, tools):
        coordinates = module.coordsys()
        assert coordinates.newcoordsys(tabular=True)
        record = coordinates.torecord()
        record["tabular0"]["pixelvalues"] = np.arange(5, dtype=np.float64)
        record["tabular0"]["worldvalues"] = np.array([10, 12, 14, 18, 22], dtype=np.float64)
        source = module.image()
        assert source.fromshape(shape=[5], csys=record)
        if different:
            record["tabular0"]["worldvalues"][3] += 2
        path = str(tmp_path / f"{module.__name__}.im")
        writer = module.image()
        assert writer.fromshape(path, [5], csys=record)
        assert writer.done()
        assert source.isconform(path) is not different
        assert source.done()
        coordinates.done()


def test_isconform_rejects_a_record_with_an_unread_axis(tmp_path):
    path = str(tmp_path / "native.im")
    writer = tools.image()
    assert writer.fromshape(path, [3, 2])
    assert writer.done()
    source = tools.image()
    assert source.fromshape(shape=[3, 2])
    record = source.coordsys().torecord()
    record["coordinate0"] = record.pop("direction0")
    record["coordinate0"]["coordinate_type"] = "Direction"
    record["linear1"] = {}
    assert source.setcoordsys(record)
    with pytest.raises(RuntimeError, match="#1002"):
        source.isconform(path)
    assert source.isopen()
    assert source.done()


def test_isconform_checks_a_removed_axis_of_a_retained_coordinate(casa, tmp_path):
    for module in (casa, tools):
        coordinates = module.coordsys()
        assert coordinates.newcoordsys(linear=2)
        record = coordinates.torecord()
        record["pixelmap0"] = np.array([0, -2], dtype=np.int32)
        record["worldmap0"] = np.array([0, -2], dtype=np.int32)
        source = module.image()
        assert source.fromshape(shape=[4], csys=record)
        record["linear0"]["crpix"][1] += 7.0
        path = str(tmp_path / f"{module.__name__}.im")
        writer = module.image()
        assert writer.fromshape(path, [4], csys=record)
        assert writer.done()
        assert source.isconform(path) is False
        assert source.done()
        coordinates.done()
