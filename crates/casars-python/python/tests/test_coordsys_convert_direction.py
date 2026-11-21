"""Conversion of a native celestial grid and its reported rotation."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def test_native_galactic_conversion_matches_projected_angular_rotation():
    # Columns of casacore MeasData::J2000toGAL.
    rotation = np.array([
        [-0.0548777621, -0.8734369591, -0.4838350026],
        [0.4941083214, -0.4448308610, 0.7469822433],
        [-0.8676666398, -0.1980741871, 0.4559837919],
    ])
    x, y, z = rotation[:, 0]
    longitude = np.arctan2(y, x)
    latitude = np.arctan2(z, np.hypot(x, y))
    north = latitude + np.deg2rad(1.0)
    offset = rotation.T @ [np.cos(north) * np.cos(longitude), np.cos(north) * np.sin(longitude), np.sin(north)]
    # For a SIN plane at RA=Dec=0 with negative longitude increment, the
    # projected pixel direction is proportional to [-equatorial_y, equatorial_z].
    expected = np.arctan2(offset[1], offset[2])
    cs = tools.coordsys().newcoordsys(direction=True)
    result = cs.convertdirection(frame=b"gal")
    assert result["unit"] == "rad"
    assert result["value"] == pytest.approx(expected, abs=1.e-12)
    assert cs.getconversiontype("direction", False) == "GALACTIC"
    np.testing.assert_allclose(cs.referencevalue()["numeric"], [longitude, latitude], atol=1.e-12)
    np.testing.assert_array_equal(cs.names(), ["Longitude", "Latitude"])


@pytest.mark.parametrize(("projection", "frame"), [("SIN", "GALACTIC"), ("SIN", "J2000"), ("TAN", "ECLIPTIC"), ("CAR", "GALACTIC"), ("SFL", "GALACTIC")])
def test_custom_native_grid_matches_pinned_casa(casa, projection, frame):
    native = casa.coordsys().newcoordsys(direction=True, spectral=True)
    assert native.setprojection(projection)
    assert native.setunits(["deg", "deg"], "direction")
    assert native.setreferencepixel([11.0, -14.0], "direction")
    assert native.setreferencevalue([24.0, 32.0], "direction")
    assert native.setincrement([-0.2, 0.2], "direction")
    spin = 0.31
    matrix = np.array([[np.cos(spin), -np.sin(spin)], [np.sin(spin), np.cos(spin)]])
    assert native.setlineartransform("direction", matrix)
    assert native.transpose([1, 2, 0])
    record = native.torecord()
    actual = tools.coordsys()
    assert actual.fromrecord(record)
    expected = native.convertdirection(frame)
    rotated = actual.convertdirection(frame)
    assert rotated["unit"] == expected["unit"] == "rad"
    # The finite latitude offset involves two spherical conversions through
    # a rounded frame matrix. A nanoradian is about 0.2 milliarcseconds.
    assert rotated["value"] == pytest.approx(expected["value"], abs=1.e-9, rel=0.0)
    _assert_value(actual.torecord()["direction0"], native.torecord()["direction0"])
    np.testing.assert_array_equal(actual.torecord()["pixelmap0"], [2, 0])
    _assert_value(actual.torecord()["worldreplace0"], native.torecord()["worldreplace0"])
    _assert_value(actual.torecord()["spectral1"], native.torecord()["spectral1"])
    sample = native.referencepixel()["numeric"] + [0.5, 2.0, 1.5]
    np.testing.assert_allclose(actual.toworld(sample)["numeric"], native.toworld(sample)["numeric"], atol=1.e-10)


def test_native_frame_rotation_preserves_signed_axis_maps(casa):
    source = casa.coordsys().newcoordsys(direction=True, linear=1)
    assert source.setunits(["deg", "deg"], "direction")
    record = source.torecord()
    for domain in ("pixel", "world"):
        record[f"{domain}map0"] = np.array([-1, 0], dtype=np.int32)
        record[f"{domain}map1"] = np.array([1], dtype=np.int32)
    record["pixelreplace0"] = np.array([0.75, 0.0])
    record["worldreplace0"] = np.array([12.0, 0.0])
    states = []
    for api in (tools, casa):
        cs = api.coordsys()
        assert cs.fromrecord(record)
        angle = cs.convertdirection("GALACTIC")
        saved = cs.torecord()
        np.testing.assert_array_equal(saved["pixelmap0"], [-1, 0])
        np.testing.assert_array_equal(saved["worldmap0"], [-1, 0])
        np.testing.assert_allclose(saved["worldreplace0"], [np.deg2rad(12.0), 0.0])
        np.testing.assert_allclose(saved["pixelreplace0"], [0.75, 0.0])
        states.append((angle, cs.toworld([1.25, 4.0])["numeric"]))
    _assert_value(states[0][0], states[1][0])
    _assert_value(states[0][1], states[1][1])


def test_exact_negative_pixel_meridian_returns_positive_half_turn(casa):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(direction=True)
        assert cs.setunits(["deg", "deg"])
        assert cs.setreferencevalue([0.0, -30.0])
        assert cs.setincrement([1.0, -1.0])
        angle = cs.convertdirection("J2000")
        assert angle == {"unit": "rad", "value": np.pi}


def test_square_pixel_check_absorbs_pc_row_scale(casa):
    original = casa.coordsys().newcoordsys(direction=True)
    assert original.setunits(["rad", "rad"])
    source = original.torecord()
    original_step = source["direction0"]["cdelt"].copy()
    source["direction0"]["pc"] = np.diag([2.0, 1.0])
    for api in (tools, casa):
        rejected = api.coordsys()
        assert rejected.fromrecord(source)
        before = rejected.torecord()
        with pytest.raises(RuntimeError, match="square pixels"):
            rejected.convertdirection("GALACTIC")
        _assert_value(rejected.torecord(), before)
    source["direction0"]["cdelt"][0] *= 0.5
    for api in (tools, casa):
        accepted = api.coordsys()
        assert accepted.fromrecord(source)
        assert accepted.convertdirection("GALACTIC")["value"] == pytest.approx(-0.4097959745752)
        np.testing.assert_allclose(accepted.torecord()["direction0"]["cdelt"], original_step)


def test_rejected_rotation_preserves_the_previous_grid(casa):
    for api in (tools, casa):
        cs = api.coordsys().newcoordsys(linear=1)
        with pytest.raises(RuntimeError, match="does not have a direction coordinate"):
            cs.convertdirection("GALACTIC")
        cs = api.coordsys().newcoordsys(direction=True)
        for frame, exception, message in (
            (None, TypeError, "_frame must be a string"),
            ("unknown", RuntimeError, "Unknown frame specifier unknown"),
            ("AZEL", RuntimeError, None),
        ):
            before = cs.torecord()
            with pytest.raises(exception, match=message):
                cs.convertdirection(frame)
            _assert_value(cs.torecord(), before)
        assert cs.setincrement([-1.e-3, 2.e-3])
        before = cs.torecord()
        with pytest.raises(RuntimeError, match="square pixels"):
            cs.convertdirection("GALACTIC")
        _assert_value(cs.torecord(), before)


def test_rotated_native_frame_survives_both_native_writers_and_readers(casa, tmp_path):
    world_reference = np.array([1.6814072365, -1.0504894071])
    for writer in (tools, casa):
        coords = writer.coordsys().newcoordsys(direction=True)
        assert coords.convertdirection("GALACTIC")["unit"] == "rad"
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        assert image.fromshape(path, [5, 6], coords.torecord())
        image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            try:
                reopened = image.coordsys()
                assert reopened.getconversiontype("direction", False) == "GALACTIC"
                np.testing.assert_allclose(reopened.toworld([0.0, 0.0])["numeric"], world_reference, atol=1.e-8)
                np.testing.assert_allclose(reopened.topixel(world_reference)["numeric"], [0.0, 0.0], atol=4.e-5)
            finally:
                image.done()
