"""Analytic binary transport and rendering checks, independent of native data."""

import struct

import numpy as np
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
from matplotlib import pyplot as plt

from casars._plot_document import array_view, render_document
from casars.tasks import _runner
from casars.tasks._runner import _save_figure


def view(offset, shape, dtype="f64_le", strides=()):
    return {"offset_bytes": offset, "shape": shape, "dtype": dtype, "strides_bytes": strides}


def test_little_endian_strides_broadcast_and_images_are_views():
    payload = bytearray(struct.pack("<7d4f", 1, 10, 2, 20, 3, 30, 2.5, 1, 2, 3, 4))
    x = array_view(payload, view(0, [3], strides=[16]))
    y = array_view(payload, view(8, [3], strides=[16]))
    radius = array_view(payload, view(48, [3], strides=[0]))
    image = array_view(payload, view(56, [2, 2], "f32_le"))
    np.testing.assert_array_equal(x, [1, 2, 3])
    np.testing.assert_array_equal(y, [10, 20, 30])
    np.testing.assert_array_equal(radius, [2.5, 2.5, 2.5])
    np.testing.assert_array_equal(image, [[1, 2], [3, 4]])
    assert np.shares_memory(x, np.frombuffer(payload, dtype="u1"))
    assert np.shares_memory(image, np.frombuffer(payload, dtype="u1"))
    struct.pack_into("<d", payload, 16, -7)
    assert x[1] == -7


def test_renderer_preserves_all_native_artists_and_display_choices():
    payload = struct.pack("<7d4f", 1, 10, 2, 20, 3, 30, 2.5, 1, 2, 3, 4)
    points = {"kind": "points", "x": view(0, [3], strides=[16]), "y": view(8, [3], strides=[16])}
    base = {"title": "", "line_width": 2, "opacity": 1}
    layers = [
        {**base, "kind": "line", "data": points, "style": {"color": "b"}},
        {**base, "kind": "circles", "data": {**points, "kind": "circles", "radii": view(48, [3], strides=[0]), "filled": False}, "style": {"color": "r"}},
        {**base, "kind": "image", "data": {
            "kind": "image", "pixels": view(56, [2, 2], "f32_le"),
            "extent": [2, -2, -3, 3], "range": [1, 4], "color_map": "jet",
            "origin": "lower", "interpolation": "bilinear", "color_bar": True,
        }},
    ]
    document = {"title": "", "show_legend": False, "panels": [{
        "title": "native", "axes": [
            {"label": "x", "lower": 4, "upper": -4},
            {"label": "y", "lower": -5, "upper": 5},
        ], "layers": layers, "aspect": "equal",
        "annotations": [{"x": 0.05, "y": 0.95, "text": "native stats", "style": {"axes_relative": True}}],
    }]}
    figure, axis = render_document(document, payload=payload)
    try:
        np.testing.assert_array_equal(axis.lines[0].get_xdata(), [1, 2, 3])
        np.testing.assert_array_equal(axis.lines[0].get_ydata(), [10, 20, 30])
        assert len(axis.patches) == 3
        assert [patch.get_radius() for patch in axis.patches] == [2.5] * 3
        image = axis.images[0]
        np.testing.assert_array_equal(image.get_array(), [[1, 2], [3, 4]])
        assert image.get_clim() == (1, 4)
        assert image.get_extent() == [2, -2, -3, 3]
        assert image.origin == "lower" and image.get_interpolation() == "bilinear"
        assert image.get_cmap().name == "jet"
        assert axis.get_xlim() == (4, -4) and axis.get_ylim() == (-5, 5)
        assert axis.get_aspect() == 1
        assert axis.texts[0].get_transform() is axis.transAxes
        assert len(figure.axes) == 2  # the complete image's colour scale
    finally:
        plt.close(figure)


def test_failed_png_publication_preserves_the_previous_figure(tmp_path):
    destination = tmp_path / "plot.png"
    destination.write_bytes(b"previous")

    class FailedFigure:
        def savefig(self, path):
            path.write_bytes(b"incomplete")
            raise OSError("renderer failed")

    with pytest.raises(OSError, match="renderer failed"):
        _save_figure(FailedFigure(), destination)
    assert destination.read_bytes() == b"previous"
    assert list(tmp_path.iterdir()) == [destination]


def test_failed_publication_closes_even_a_requested_screen_figure(tmp_path, monkeypatch):
    payload = tmp_path / "arrays.bin"
    payload.write_bytes(struct.pack("<d", 0))
    plots = [{"output_file": "plot.png", "show": True, "document": {
        "title": "", "show_legend": False,
        "panels": [{"title": "", "axes": [], "layers": []}],
    }}]

    def fail(*args):
        raise OSError("cannot publish PNG")

    monkeypatch.setattr(_runner, "_save_figure", fail)
    before = plt.get_fignums()
    with pytest.raises(OSError, match="cannot publish PNG"):
        _runner._render_task_plots(plots, payload, tmp_path)
    assert plt.get_fignums() == before
