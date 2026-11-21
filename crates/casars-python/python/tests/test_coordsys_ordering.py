"""CASA axis transposition and coordinate creation-order changes."""

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
@pytest.mark.parametrize("order", [False, [0.5], ["0"], [np.nan], [np.inf], np.array([[0]])])
def test_scalar_and_coerced_order_arguments(casa, method, order):
    for implementation in (tools, casa):
        cs = implementation.coordsys().newcoordsys(linear=1)
        assert getattr(cs, method)(order=order)
        assert cs.naxes() == 1
