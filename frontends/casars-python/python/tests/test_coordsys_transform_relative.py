"""Relative pixel and spherical world offsets with a pinned CASA oracle."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def _linear(api):
    cs = api.coordsys().newcoordsys(linear=2)
    assert cs.setreferencepixel([5.0, 7.0])
    assert cs.setreferencevalue([110.0, 190.0])
    return cs


def test_scalar_offset_records_defaults_and_overrides(casa):
    for backend in (tools, casa):
        cs = _linear(backend)
        assert cs.toabs([2.0, 3.0]) == {}
        with pytest.raises(RuntimeError, match="Should be world value"):
            cs.torel([2.0, 3.0])
        for pixel in ([6, 8], [6.0, 8.0, 20.0], {"numeric": [6.0, 8.0], "pw_type": "pixel"}):
            relative = cs.torel(value=pixel, isworld=0)
            assert relative["pw_type"] == "pixel"
            assert relative["ar_type"] == "relative"
            np.testing.assert_array_equal(relative["numeric"], [1, 1])
            absolute = cs.toabs(relative)
            assert absolute["ar_type"] == "absolute"
            np.testing.assert_array_equal(absolute["numeric"], [6, 8])
        np.testing.assert_array_equal(cs.torel(isworld=0)["numeric"], [0, 0])
        np.testing.assert_array_equal(cs.torel([112.0], 2)["numeric"], [2, 0])
        np.testing.assert_array_equal(cs.toabs([2.0], -2)["numeric"], [112, 190])
        with pytest.raises(RuntimeError, match="value is relative"):
            cs.torel(relative)
        with pytest.raises(RuntimeError, match="value is absolute"):
            cs.toabs(absolute)


def test_world_offsets_reuse_supplied_record_representations(casa):
    left, right = (_linear(backend) for backend in (tools, casa))
    quantity = {"*1": {"value": 112.0, "unit": "km"}, "*2": {"value": 193.0, "unit": "km"}}
    for argument in (
        "2km", "2km 3km", ["2km"], ["112km", "193km"],
        {"quantity": quantity}, {"measure": {"linear": quantity}},
        {"numeric": [112.0, 193.0], "string": ["100km", "50km"], "attribute": 8.0},
    ):
        _assert_value(left.torel(argument, 1), right.torel(argument, 1))
        _assert_value(left.toabs(argument, 1), right.toabs(argument, 1))


def test_scalar_pixel_records_keep_numeric_values_alongside_strings(casa):
    for backend in (tools, casa):
        cs = _linear(backend)
        pixel = {"pw_type": "pixel", "numeric": [6.0, 8.0], "string": ["x", "y"]}
        np.testing.assert_array_equal(cs.torel(pixel)["numeric"], [1, 1])
        np.testing.assert_array_equal(cs.toabs(pixel)["numeric"], [11, 15])
        assert cs.toabs({"pw_type": "pixel"}) == {}
        del pixel["numeric"]
        for method in (cs.toabs, cs.torel):
            with pytest.raises(RuntimeError, match="Input does not appear to be a pixel coordinate"):
                method(pixel)
        for numeric in (np.array([[6.0, 8.0]]), np.array([[6.0], [8.0]])):
            np.testing.assert_array_equal(cs.torel({"numeric": numeric}, 1)["numeric"], [-104, -182])


def test_stokes_pixel_offsets_require_a_defined_polarization(casa):
    for backend in (tools, casa):
        cs = backend.coordsys().newcoordsys(stokes="I,Q")
        for method in (cs.toabs, cs.torel, cs.toabsmany, cs.torelmany):
            many = method.__name__.endswith("many")
            for value, index in ((2.0, 2), (-1.6, -1)):
                pixel = np.array([[value]] if many else [value])
                with pytest.raises(RuntimeError, match=rf"Absolute pixel {index} is out of range \[0\.\.1\]"):
                    method(pixel, 0)
            pixel = np.array([[-1.4]] if many else [-1.4])
            np.testing.assert_array_equal(method(pixel, 0)["numeric"], pixel)


@pytest.mark.parametrize("axes", [[0, -2], [-2, 0]])
def test_world_offsets_of_a_sliced_direction_axis(casa, axes):
    original = casa.coordsys().newcoordsys(direction=True, spectral=True)
    original.setunits(["deg", "deg"], "direction")
    original.setreferencevalue([40.0, 23.0], "direction")
    record = original.torecord()
    record["pixelmap0"] = record["worldmap0"] = np.array(axes, dtype=np.int32)
    record["pixelmap1"] = record["worldmap1"] = np.array([1], dtype=np.int32)
    left, right = (backend.coordsys() for backend in (tools, casa))
    assert left.fromrecord(record) and right.fromrecord(record)
    world = np.array([38.0, 1.4e9])
    for name in ("toabs", "torel", "toabsmany", "torelmany"):
        value = world[:, None] if name.endswith("many") else world
        _assert_value(getattr(left, name)(value, 1), getattr(right, name)(value, 1))


def test_absolute_offset_measures_keep_the_native_frames(casa):
    original = casa.coordsys().newcoordsys(direction=True, spectral=True)
    record = original.torecord()
    record["direction0"]["conversionSystem"] = "GALACTIC"
    record["spectral1"]["conversion"]["system"] = "BARY"
    left, right = (backend.coordsys() for backend in (tools, casa))
    assert left.fromrecord(record) and right.fromrecord(record)
    value = {"numeric": [0.1, 0.2, 3.0], "measure": {}}
    actual = left.toabs(value, 1)
    assert actual["measure"]["direction"]["refer"] == "J2000"
    assert actual["measure"]["spectral"]["frequency"]["refer"] == "LSRK"
    _assert_value(actual, right.toabs(value, 1))


def test_spherical_offsets_near_the_pole(casa):
    for backend in (tools, casa):
        cs = backend.coordsys().newcoordsys(direction=True)
        cs.setunits(["deg", "deg"])
        np.testing.assert_allclose(
            cs.torel([0.0, 89.999999], 1)["numeric"],
            [0.0, 89.99999914622636],
            rtol=0.0,
            atol=1e-11,
        )


def _mixed(api):
    cs = api.coordsys().newcoordsys(direction=True, stokes=["I", "Q"], spectral=True)
    assert cs.setunits(["deg", "deg"], "direction")
    assert cs.setunits(["MHz"], "spectral")
    assert cs.setreferencevalue([40.0, 23.0], "direction")
    assert cs.setreferencevalue([1415.0], "spectral")
    return cs


def test_declared_angle_offsets_follow_the_analytic_local_spherical_basis(casa):
    coordinates = np.array([[39.5, 40.4], [23.4, 23.1], [2.0, 1.0], [1400.0, 1450.0]])
    delta_lon = np.deg2rad(coordinates[0] - 40.0)
    lat = np.deg2rad(coordinates[1])
    reference_lat = np.deg2rad(23.0)
    east = np.cos(lat) * np.sin(delta_lon)
    north = np.cos(reference_lat) * np.sin(lat) - np.sin(reference_lat) * np.cos(lat) * np.cos(delta_lon)
    normal = np.sin(reference_lat) * np.sin(lat) + np.cos(reference_lat) * np.cos(lat) * np.cos(delta_lon)
    latitude = np.arcsin(north)
    longitude = np.arctan2(east, normal) * np.cos(latitude)
    expected = np.vstack((np.rad2deg(longitude), np.rad2deg(latitude), coordinates[2], coordinates[3] - 1415.0))
    for backend in (tools, casa):
        cs = _mixed(backend)
        relative = cs.torelmany(coordinates)
        assert relative["pw_type"] == "world" and relative["ar_type"] == "relative"
        np.testing.assert_allclose(relative["numeric"], expected, rtol=0, atol=1e-11)
        np.testing.assert_allclose(cs.toabsmany(relative)["numeric"], coordinates, rtol=0, atol=1e-11)
        for column in range(coordinates.shape[1]):
            single = cs.torel(coordinates[:, column], 1)
            np.testing.assert_allclose(single["numeric"], expected[:, column], rtol=0, atol=1e-11)
            np.testing.assert_allclose(cs.toabs(single)["numeric"], coordinates[:, column], rtol=0, atol=1e-11)


def test_offset_matrices_keep_the_oracle_layout_and_domain_defaults(casa):
    for backend in (tools, casa):
        cs = _linear(backend)
        positions = np.array([[6.0, 7.0], [8.0, 9.0], [15.0, 20.0]])
        np.testing.assert_array_equal(cs.torelmany(positions, isworld=0)["numeric"], [[1, 2], [1, 2], [15, 20]])
        np.testing.assert_array_equal(cs.torelmany(positions)["numeric"], [[-104, -103], [-182, -181], [15, 20]])
        relative = cs.torelmany(positions, 0)
        assert cs.toabsmany(relative["numeric"]) == {}
        np.testing.assert_array_equal(cs.toabsmany(relative)["numeric"], positions)
        assert cs.torelmany(np.empty((2, 0)), 0)["numeric"].shape == (2, 0)
        with pytest.raises(RuntimeError, match="getDoubleVec.*intvec"):
            cs.torelmany([[6, 7], [8, 9]], 0)
        with pytest.raises(RuntimeError, match="valueIn.shape"):
            cs.torelmany(np.array([6.0, 8.0]), 0)


def test_offsets_handle_casa_termination_cases_without_mutating_state():
    cs = _linear(tools)
    np.testing.assert_array_equal(cs.toabs(isworld=0)["numeric"], [5, 7])
    np.testing.assert_array_equal(cs.toabs(isworld=1)["numeric"], [110, 190])
    for method in (cs.toabsmany, cs.torelmany):
        with pytest.raises(RuntimeError, match="at least 2 coordinate rows"):
            method(np.array([[0.0, 1.0]]), 0)
    np.testing.assert_array_equal(cs.referencepixel()["numeric"], [5, 7])


def test_native_coordinate_records_keep_offsets_in_all_four_directions(casa, tmp_path):
    positions = np.array([[39.5, 40.4], [23.4, 23.1], [2.0, 1.0], [1400.0, 1450.0]])
    baseline = _mixed(casa).torelmany(positions)
    for index, writer in enumerate((tools, casa)):
        path = str(tmp_path / f"relative-{index}.im")
        image = writer.image()
        assert image.fromshape(path, [5, 6, 2, 7], csys=_mixed(writer).torecord())
        assert image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            cs = image.coordsys()
            _assert_value(cs.torelmany(positions), baseline)
            np.testing.assert_allclose(cs.toabsmany(baseline)["numeric"], positions, atol=1e-11)
            assert image.done()
