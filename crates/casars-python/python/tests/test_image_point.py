"""Pixel-value records and constant arrays through the public image tools."""

import numpy as np
import pytest

from casars import tools
from test_image_workflow import _assert_value


@pytest.fixture(params=["casars", "casa"])
def tool(request):
    module = tools if request.param == "casars" else pytest.importorskip("casatools")
    return module.image()


@pytest.mark.parametrize("complex_pixels", [False, True])
def test_pixel_value_selects_coordinates_and_returns_units(tool, complex_pixels):
    assert tool.pixelvalue() == {}
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    if complex_pixels:
        pixels = pixels.astype(np.complex64) + 2j * pixels[::-1]
    assert tool.fromarray(pixels=pixels)
    assert tool.setbrightnessunit("Jy/pixel")
    coords = tool.coordsys()
    assert coords.setreferencepixel([1.5, 1.25])
    assert tool.setcoordsys(coords.torecord())
    for pixel, expected in [
        ([], [2, 1]),
        ([-1], [2, 1]),
        ([0], [0, 1]),
        ([2, 3, 500], [2, 3]),
        ([1.75, 2.50], [1, 2]),
    ]:
        _assert_value(tool.pixelvalue(pixel), {
            "pixel": np.asarray(expected, dtype=np.int64),
            "mask": True,
            "value": {"unit": "Jy/pixel", "value": pixels[tuple(expected)].item()},
        })
    assert tool.pixelvalue([0, -1]) == {}
    assert tool.pixelvalue([3, 0]) == {}
    assert tool.done()


def test_pixel_value_reads_native_mask(tmp_path):
    casa = pytest.importorskip("casatools")
    path = str(tmp_path / "mask.im")
    source = casa.image()
    assert source.fromarray(path, np.array([[0.0, -1.0], [3.0, 4.0]]))
    assert source.calcmask(f'"{path}" > 0', name="positive")
    assert source.done()
    native = tools.image()
    assert native.open(path)
    assert native.pixelvalue([0, 1])["mask"] is False
    assert native.pixelvalue([1, 1])["mask"] is True
    assert native.pixelvalue([0, 1])["value"]["value"] == -1.0
    assert native.done()


@pytest.mark.parametrize("kind", ["d", "cd"])
def test_pixel_value_rejects_double_precision(tool, kind):
    assert tool.fromshape(shape=[2, 3], type=kind)
    with pytest.raises(RuntimeError, match="pixelvalue does not support images with .* precision"):
        tool.pixelvalue()


def test_makearray_accepts_scalar_and_shaped_requests(tool):
    _assert_value(tool.makearray(), np.empty(0, dtype=np.float64))
    _assert_value(tool.makearray(2.5, [2, 3]), np.full((2, 3), 2.5))
    _assert_value(tool.makearray(2, [0, 3]), np.empty((0, 3)))
    _assert_value(tool.makearray(2, []), np.array([2.0]))
    _assert_value(tool.makearray(True, 3), np.array([1.0, 1.0, 1.0]))
    with pytest.raises(SystemError, match="image_makearray"):
        tool.makearray(1 + 2j, [1])
