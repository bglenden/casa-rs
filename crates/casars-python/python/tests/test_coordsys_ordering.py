"""CASA axis transposition and coordinate creation-order changes."""

import platform

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(scope="module")
def casa():
    api = pytest.importorskip("casatools")
    assert api.version_string().replace("-", ".") == "6.7.6.14"
    return api


def source(casa):
    cs = casa.coordsys().newcoordsys(direction=True, spectral=True, linear=1)
    assert cs.setunits(["rad", "rad", "Hz", "km"])
    assert cs.setreferencepixel([1.0, 2.0, 3.0, 4.0])
    assert cs.setreferencevalue([0.9, 0.3, 1.4e9, 37.0])
    return cs


def compare(left, right, positions):
    assert left.coordinatetype() == right.coordinatetype()
    assert left.names() == right.names()
    _assert_value(left.referencepixel(), right.referencepixel())
    _assert_value(left.referencevalue(), right.referencevalue())
    for point in positions:
        _assert_value(left.toworld(point, "nq"), right.toworld(point, "nq"))
    saved = right.torecord()
    actual = left.torecord()
    for field in saved:
        if field.startswith(("pixelmap", "worldmap", "pixelreplace", "worldreplace")):
            _assert_value(actual[field], saved[field], field)


@pytest.mark.parametrize("repeat", [False, True])
def test_transpose_and_reorder_keep_the_selected_local_transforms(casa, repeat):
    right = source(casa)
    left = tools.coordsys()
    assert left.fromrecord(right.torecord())
    original_point = [2.0, 1.5, 4.0, 7.0]
    original_world = right.toworld(original_point)["numeric"]
    order = [3, 1, 2, 0]
    new_point = np.array(original_point)[order]
    for implementation in (left, right):
        assert implementation.transpose(np.array(order))
    np.testing.assert_allclose(left.toworld(new_point)["numeric"], original_world[order])
    compare(left, right, [new_point])
    _assert_value(left.findcoordinate("direction"), right.findcoordinate("direction"))

    order = [1, 0, 0] if repeat else [2, 0, 1]
    for implementation in (left, right):
        assert implementation.reorder(order)
    point = [4.0, 2.0, 1.5, 2.0, 1.5] if repeat else [7.0, 2.0, 1.5, 4.0]
    wanted = original_world[[2, 0, 1, 0, 1] if repeat else [3, 0, 1, 2]]
    np.testing.assert_allclose(left.toworld(point)["numeric"], wanted)
    compare(left, right, [point])
    reread = casa.coordsys()
    assert reread.fromrecord(left.torecord())
    compare(reread, right, [point])


@pytest.mark.parametrize("method", ["transpose", "reorder"])
def test_order_argument_errors_are_atomic(method):
    cs = tools.coordsys().newcoordsys(direction=True, spectral=True)
    before = cs.torecord()
    call = getattr(cs, method)
    bad = [0, 0, 2] if method == "transpose" else [0, 2]
    message = "Failed AlwaysAssert"
    with pytest.raises(RuntimeError, match=message):
        call(bad)
    with pytest.raises(RuntimeError, match="nelements|length"):
        call(None)
    with pytest.raises(TypeError, match="argument _order must not be a string"):
        call("0")
    with pytest.raises(SystemError, match="with an exception set"):
        call(tuple(bad))
    _assert_value(cs.torecord(), before)


@pytest.mark.parametrize("method", ["transpose", "reorder"])
@pytest.mark.parametrize("order", [False, [0.5], ["0"], [np.nan], [-np.inf], np.array([[0]])])
def test_scalar_and_coerced_order_arguments(casa, method, order):
    for implementation in (tools, casa):
        cs = implementation.coordsys().newcoordsys(linear=1)
        assert getattr(cs, method)(order=order)
        assert cs.naxes() == 1



@pytest.mark.parametrize("method", ["transpose", "reorder"])
@pytest.mark.parametrize("backend", ["casatools", "casars"])
@pytest.mark.parametrize(
    "argument, expected",
    [
        pytest.param([np.inf], "overflow", id="positive-infinity"),
        pytest.param([np.float32(np.inf)], "overflow", id="numpy-float-list"),
        pytest.param(np.array([np.inf]), "overflow", id="float-array"),
        pytest.param([1.e30], "overflow", id="wide-float"),
        pytest.param([2.**63], "overflow", id="native-long-limit"),
        pytest.param([np.nan], "identity", id="nan"),
        pytest.param([-1.e30], "identity", id="negative-wide-float"),
        pytest.param([2.**32], "identity", id="low-32-bits"),
        pytest.param([2**72], "range-error", id="wide-python-int"),
        pytest.param([-(2**72)], "range-error", id="negative-wide-python-int"),
        pytest.param([str(2**72)], "range-error", id="wide-numeric-text"),
        pytest.param([str(-(2**72))], "identity", id="negative-wide-text"),
        pytest.param([np.uint64(1)], "identity", id="unsigned-list-item"),
        pytest.param([[np.uint64(1)]], "identity", id="unsigned-nested-list-item"),
        pytest.param(np.array([2**63], dtype="u8"), "identity", id="wide-unsigned-array"),
        pytest.param(np.array([2**63+1], dtype="u8"), "range-error", id="unsigned-array-low-bits"),
        pytest.param(np.uint64(0), "length-error", id="bare-unsigned"),
        pytest.param(np.float32(0), "length-error", id="bare-float32"),
        pytest.param(np.array([0], dtype=object), "length-error", id="object-array"),
        pytest.param([1+2j], "range-error", id="complex-real-component"),
        pytest.param([np.complex64(1+2j)], "identity", id="narrow-complex-list-item"),
    ],
)
def test_order_binding_preserves_native_coercion_and_failed_state(method, backend, argument, expected):
    api = tools if backend == "casars" else pytest.importorskip("casatools")
    if backend == "casatools":
        assert api.version_string().replace("-", ".") == "6.7.6.14"
    if expected == "overflow":
        expected = "range-error" if platform.machine() in ("aarch64", "arm64") else "identity"
    coordinates = api.coordsys().newcoordsys(linear=1)
    try:
        before = coordinates.torecord()
        world_before = coordinates.toworld([3.0])["numeric"]
        call = getattr(coordinates, method)
        if expected == "identity":
            assert call(argument)
            assert coordinates.naxes() == 1
            _assert_value(coordinates.toworld([3.0])["numeric"], world_before)
        else:
            message = "which" if expected == "range-error" else "nelements|length"
            with pytest.raises(RuntimeError, match=message):
                call(argument)
            _assert_value(coordinates.torecord(), before)
    finally:
        coordinates.done()


def test_transpose_keeps_a_hidden_slot_and_reorder_restores_its_coordinate(casa):
    original = casa.coordsys().newcoordsys(linear=2, tabular=True)
    assert original.setreferencepixel([0.5, 1.5, 2.0])
    assert original.setreferencevalue([3.0, 17.0, 31.0])
    record = original.torecord()
    for domain in ("world", "pixel"):
        record[f"{domain}map0"] = np.array([-1, 0], dtype=np.int32)
        record[f"{domain}map1"] = np.array([1], dtype=np.int32)
    left, right = (api.coordsys() for api in (tools, casa))
    for implementation in (left, right):
        assert implementation.fromrecord(record)
        assert implementation.transpose([1, 0])
        assert implementation.naxes() == 2
    compare(left, right, [[2.25, 1.0]])
    np.testing.assert_array_equal(left.torecord()["worldmap0"], [-1, 1])
    for implementation in (left, right):
        assert implementation.reorder([0, 1])
        assert implementation.naxes() == 3
    compare(left, right, [[0.0, 1.0, 2.25]])


def test_transpose_keeps_a_sampled_tabular_coordinate_live():
    cs = tools.coordsys().newcoordsys(linear=1, tabular=True)
    record = cs.torecord()
    record["tabular1"]["pixelvalues"] = np.array([0.0, 2.0, 5.0])
    record["tabular1"]["worldvalues"] = np.array([10.0, 14.0, 30.0])
    assert cs.fromrecord(record)
    matrix = np.array([[2.0]])
    assert cs.setlineartransform("tabular", matrix)
    assert cs.transpose([1, 0])
    np.testing.assert_array_equal(cs.lineartransform("tabular"), matrix)
    np.testing.assert_array_equal(cs.toworld([1.0, 5.0])["numeric"], [14.0, 5.0])
    reloaded = tools.coordsys()
    assert reloaded.fromrecord(cs.torecord())
    np.testing.assert_array_equal(reloaded.toworld([1.0, 5.0])["numeric"], [14.0, 5.0])
