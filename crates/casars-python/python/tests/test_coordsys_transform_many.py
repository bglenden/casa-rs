"""Batch coordinate transforms against CASA and analytic linear axes."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    package = pytest.importorskip("casatools")
    assert package.version_string().replace("-", ".") == "6.7.6.14"
    return package


def test_linear_batch_uses_one_coordinate_per_column():
    cs = tools.coordsys().newcoordsys(linear=2)
    assert cs.setreferencepixel([1.5, -2.0])
    pixels = np.arange(18, dtype=np.float64).reshape(2, 9)[:, ::-2]
    world = cs.toworldmany(value=pixels)
    _assert_value(world, {
        "numeric": pixels - np.array([[1.5], [-2.0]]),
        "pw_type": "world",
        "ar_type": "absolute",
    })
    _assert_value(cs.topixelmany(world["numeric"]), {
        "numeric": pixels, "pw_type": "pixel", "ar_type": "absolute",
    })


def test_batches_preserve_byte_order_and_reject_ragged_lists():
    cs = tools.coordsys().newcoordsys(linear=2)
    values = np.asarray([[0.25, 1.25], [3.5, 4.5]], dtype=np.dtype(np.float64).newbyteorder())
    np.testing.assert_array_equal(cs.toworldmany(values)["numeric"], values)
    with pytest.raises(RuntimeError):
        cs.topixelmany([[3.0], [4.0, 5.0]])


def test_world_to_pixel_batch_reuses_a_coupled_linear_transform(casa):
    rust = tools.coordsys().newcoordsys(linear=2)
    oracle = casa.coordsys()
    record = rust.torecord()
    axis = record["linear0"]
    pc = np.array([[2.0, 0.5], [-0.75, 1.5]])
    # CASA's coordinate record stores the input index first.
    axis["pc"] = pc.T.copy()
    axis["crpix"] = [1.5, 2.25]
    axis["crval"] = [-1.0, 0.25]
    axis["cdelt"] = [2.5, -1.5]
    assert rust.fromrecord(record) and oracle.fromrecord(record)
    pixels = np.arange(20, dtype=np.float64).reshape(2, 10)[:, 8:0:-2]
    ref = np.asarray(axis["crpix"])[:, None]
    values = np.asarray(axis["crval"])[:, None] + np.asarray(axis["cdelt"])[:, None] * (pc @ (pixels - ref))
    _assert_value(rust.topixelmany(values), oracle.topixelmany(values))
    np.testing.assert_allclose(rust.topixelmany(values)["numeric"], pixels, rtol=0, atol=1e-12)


@pytest.mark.parametrize("array_type", [list, tuple, np.float32, np.float64, np.longdouble])
def test_mixed_coordinates_use_casas_native_units(casa, array_type):
    kwargs = dict(direction=True, spectral=True, stokes="I,Q", tabular=True)
    actual = tools.coordsys().newcoordsys(**kwargs)
    expected = casa.coordsys().newcoordsys(**kwargs)
    pixels = np.array([
        [-2.0, -1.0, 0.0], [0.25, 1.25, 2.25], [0, 1, 0],
        [3.5, 4.5, 5.5], [1.25, 2.5, 3.75],
    ])
    if array_type is list:
        argument = pixels.tolist()
    elif array_type is tuple:
        argument = tuple(tuple(row) for row in pixels)
    else:
        argument = pixels.astype(array_type, order="F")
    want = expected.toworldmany(argument)
    got = actual.toworldmany(value=argument)
    _assert_value(got, want)
    _assert_value(actual.topixelmany(want["numeric"]), expected.topixelmany(want["numeric"]))


@pytest.mark.parametrize("dtype", [np.longlong, np.ulonglong])
def test_numpy_long_long_arrays_are_double_variants(casa, dtype):
    actual = tools.coordsys().newcoordsys(linear=2)
    expected = casa.coordsys().newcoordsys(linear=2)
    values = np.asarray([[1, 2], [3, 4]], dtype=dtype)
    _assert_value(actual.toworldmany(values), expected.toworldmany(values))
    _assert_value(actual.topixelmany(values), expected.topixelmany(values))


def test_stokes_batch_preserves_discrete_pixel_and_world_rules(casa):
    actual = tools.coordsys().newcoordsys(stokes="I,Q")
    expected = casa.coordsys().newcoordsys(stokes="I,Q")
    pixels = [[-1.49999, -0.5, 0.5]]
    worlds = [[1.0 + 1e-14, 2.0]]
    _assert_value(actual.toworldmany(pixels), expected.toworldmany(pixels))
    _assert_value(actual.topixelmany(worlds), expected.topixelmany(worlds))
    for cs in (expected, actual):
        with pytest.raises(RuntimeError):
            cs.toworldmany([[0.0, -1.5]])
        with pytest.raises(RuntimeError):
            cs.topixelmany([[1.0, 1.49]])


@pytest.mark.parametrize("method", ["toworldmany", "topixelmany"])
def test_empty_batch_preserves_its_shape(casa, method):
    args = np.empty((2, 0), dtype=np.float64)
    actual = tools.coordsys().newcoordsys(linear=2)
    expected = casa.coordsys().newcoordsys(linear=2)
    _assert_value(getattr(actual, method)(args), getattr(expected, method)(args))


@pytest.mark.parametrize(
    "method,value,error",
    [
        ("toworldmany", None, "provide a vector of doubles"),
        ("toworldmany", np.array([[3], [4]], dtype="l"), "provide a vector of doubles"),
        ("topixelmany", np.array([[3], [4]], dtype="l"), "getDoubleVec called for type intvec"),
        ("topixelmany", np.array([[3], [4]], dtype="L"), "getDoubleVec called for type intvec"),
        ("topixelmany", np.ones((2, 2), dtype=np.bool_), "getDoubleVec called for type boolvec"),
        ("topixelmany", np.ones((2, 2), dtype=np.float16), "getDoubleVec called for type boolvec"),
        ("topixelmany", np.ones((2, 2), dtype=object), "getDoubleVec called for type boolvec"),
        ("topixelmany", [[], []], "getDoubleVec called for type boolvec"),
        ("topixelmany", [np.ones(2), np.ones(2)], "getDoubleVec called for type boolvec"),
        ("topixelmany", 1.0, "getDoubleVec called for type double"),
        ("topixelmany", "one", "getDoubleVec called for type string"),
        ("topixelmany", {"numeric": np.ones((2, 2))}, "getDoubleVec called for type ErRoR"),
        ("toworldmany", [3.0, 4.0], "pixel.shape.*nelements"),
        ("topixelmany", np.array(1.0), "world.shape.*nelements"),
        ("topixelmany", np.ones((2, 2, 1)), "world.shape.*nelements"),
        ("toworldmany", np.ones((3, 2)), "nPixelAxes.*pixel.nrow"),
        ("topixelmany", np.ones((1, 2)), "nWorldAxes.*world.nrow"),
    ],
)
def test_batch_conversion_refuses_invalid_axes_or_variant(casa, method, value, error):
    for backend in (casa.coordsys, tools.coordsys):
        convert = getattr(backend().newcoordsys(linear=2), method)
        with pytest.raises(RuntimeError, match=error):
            convert(value)
