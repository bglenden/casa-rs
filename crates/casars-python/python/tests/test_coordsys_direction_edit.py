"""Constructing a direction coordinate through the public CASA tool."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def original(api):
    coords = api.coordsys().newcoordsys(direction=True, spectral=True)
    assert coords.setunits(["deg", "deg"], "direction")
    assert coords.setreferencevalue([12., 23.], "direction")
    assert coords.setreferencepixel([1., 4.], "direction")
    assert coords.setincrement([-0.5, 1.0], "direction")
    return coords


def test_direction_defaults_rebuild_the_axis_at_its_system_axes(casa):
    source = original(casa)
    assert source.setnames(["Custom RA", "Custom Dec"], "direction")
    assert source.setconversiontype(direction="GALACTIC")
    assert source.setlineartransform("direction", np.array([[0.8, -0.6], [0.6, 0.8]]))
    assert source.transpose([1, 2, 0])
    record = source.torecord()
    results = []
    for api in (tools, casa):
        coords = api.coordsys()
        assert coords.fromrecord(record)
        assert coords.setdirection()
        result = coords.torecord()
        entry = result["direction0"]
        np.testing.assert_array_equal(entry["crpix"], [2., 0.])
        np.testing.assert_array_equal(entry["pc"], np.eye(2))
        np.testing.assert_array_equal(entry["units"], ["rad", "rad"])
        np.testing.assert_array_equal(entry["axes"], ["Right Ascension", "Declination"])
        np.testing.assert_allclose(entry["crval"], np.deg2rad([12., 23.]))
        np.testing.assert_allclose(entry["cdelt"], np.deg2rad([-0.5, 1.0]))
        assert entry["system"] == entry["conversionSystem"] == "J2000"
        _assert_value(result["spectral1"], record["spectral1"])
        _assert_value(result["pixelmap0"], record["pixelmap0"])
        results.append(result)
    _assert_value(*results)


def test_direction_constructor_distinguishes_numpy_arrays_from_integer_scalars():
    coords = original(tools)
    with pytest.raises(RuntimeError, match="Unrecognized format for world coordinate"):
        coords.setdirection(refval=np.int64(25))
    assert coords.setdirection(refval=np.array(25), incr=np.float32(-0.25))
    direction = coords.torecord()["direction0"]
    np.testing.assert_allclose(direction["crval"], np.deg2rad([25., 23.]))
    np.testing.assert_allclose(direction["cdelt"], np.deg2rad([-0.25, 1.]))


@pytest.mark.parametrize("parameters", [
    {"refval": [25.], "incr": [-0.4]},
    {"refval": ["25deg"], "incr": ["-0.4deg"]},
    {"refval": np.array(25), "incr": "-0.4deg"},
    {"refval": np.uint64(25), "incr": np.bool_(True), "poles": ["1m", "0deg"]},
    {"refval": {"numeric": [25., 35.]}, "incr": {"numeric": [-.4, .9]}},
    {"refcode": "GAL", "proj": "SIN", "projpar": [0.2, 0.3], "refpix": [11., 12., 20.],
     "refval": ["24deg", "32deg"], "incr": ["-0.5deg", "1deg"],
     "xform": np.array([[0.8, -0.6], [0.6, 0.8]], dtype=np.float32), "poles": "180deg,23deg"},
    {"proj": "CAR", "poles": ["0deg", "90deg"], "xform": [[1., 0.], [0., 1.]]},
    {"proj": "CAR", "poles": ["0deg", "-90deg"]},
    {"proj": "CAR", "poles": ["999deg", "999deg"]},
])
def test_direction_argument_forms_and_projected_values_match_casa(casa, parameters):
    values = []
    for api in (tools, casa):
        coords = original(api)
        assert coords.setdirection(**parameters)
        entry = coords.torecord()["direction0"]
        pixel = [entry["crpix"][0] + 0.25, entry["crpix"][1] + 0.5, 1.]
        world = coords.toworld(pixel)["numeric"]
        np.testing.assert_allclose(coords.topixel(world)["numeric"], pixel, rtol=0, atol=1.e-10)
        values.append((entry, world))
    _assert_value(values[0][0], values[1][0])
    np.testing.assert_allclose(values[0][1], values[1][1], rtol=0, atol=1.e-10)


def test_rejected_direction_edits_preserve_state(casa, caplog):
    for api in (tools, casa):
        coords = original(api)
        initial = coords.torecord()
        assert coords.setdirection(xform=np.eye(2, dtype=np.int32)) is False
        assert coords.setdirection(xform=[False]) is False
        for args, message in (
            ({"refcode": "missing"}, "Invalid direction code"),
            ({"refval": np.int64(25)}, "Unrecognized format for world coordinate"),
            ({"proj": "car"}, "unknown projection"),
            ({"proj": "CAR"}, "Ill-conditioned coordinate transformation"),
            ({"refval": ["12deg", "100deg"]}, "Ill-conditioned coordinate transformation"),
            ({"poles": [180, 0]}, "pole parameter is not understood"),
            ({"xform": np.array([[1., 0.], [1., 0.]])}, "singular"),
        ):
            with pytest.raises(RuntimeError, match=message):
                coords.setdirection(**args)
        _assert_value(coords.torecord(), initial)
    assert "xform paramater invalid" in caplog.text
    coords = original(tools)
    initial = coords.torecord()
    with pytest.raises(RuntimeError, match="requires two components"):
        coords.setdirection(refpix=[1.0])
    _assert_value(coords.torecord(), initial)


def test_direction_constructor_retains_signed_maps_and_replacements(casa):
    record = original(casa).torecord()
    for domain in ("pixel", "world"):
        record[f"{domain}map0"] = np.array([-1, 0], dtype=np.int32)
        record[f"{domain}map1"] = np.array([1], dtype=np.int32)
    record["pixelreplace0"] = np.array([0.75, 0.0])
    record["worldreplace0"] = np.array([12.0, 0.0])
    result = []
    for api in (tools, casa):
        coords = api.coordsys()
        assert coords.fromrecord(record)
        assert coords.setdirection(refpix=[2., 3.], refval=["24deg", "32deg"])
        actual = coords.torecord()
        np.testing.assert_array_equal(actual["pixelmap0"], [-1, 0])
        np.testing.assert_array_equal(actual["worldmap0"], [-1, 0])
        np.testing.assert_allclose(actual["worldreplace0"], [np.deg2rad(12.), 0.])
        result.append(coords.toworld([3.25, 1.])["numeric"])
    np.testing.assert_allclose(*result, rtol=0, atol=1.e-10)


def test_tangent_grid_in_all_native_writer_reader_pairs(casa, tmp_path):
    longitude_step, latitude_step = np.deg2rad([1., 2.])
    x, y = 2. * longitude_step, 3. * latitude_step
    expected = [np.arctan(x), np.arctan2(y, np.hypot(1., x))]
    for writer in (tools, casa):
        coords = writer.coordsys().newcoordsys(direction=True)
        assert coords.setdirection(proj="TAN", refpix=[0., 0.], refval=["0deg", "0deg"], incr=["1deg", "2deg"])
        path = str(tmp_path / f"{writer.__name__}.im")
        image = writer.image()
        assert image.fromshape(path, [7, 8], coords.torecord())
        image.done()
        for reader in (tools, casa):
            image = reader.image()
            assert image.open(path)
            try:
                coords = image.coordsys()
                assert coords.projection()["type"] == "TAN"
                np.testing.assert_allclose(coords.toworld([2., 3.])["numeric"], expected, rtol=0, atol=1.e-12)
                np.testing.assert_allclose(coords.topixel(expected)["numeric"], [2., 3.], rtol=0, atol=1.e-12)
            finally:
                image.done()
