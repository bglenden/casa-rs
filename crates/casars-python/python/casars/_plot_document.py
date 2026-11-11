"""Draw native plot documents; all scientific preparation belongs to Rust."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np


def array_view(payload: Any, descriptor: Mapping[str, Any]) -> np.ndarray:
    """Map a Rust-validated little-endian view without copying its bytes."""

    dtype = {"f32_le": np.dtype("<f4"), "f64_le": np.dtype("<f8")}[descriptor["dtype"]]
    return np.ndarray(
        tuple(descriptor["shape"]),
        dtype=dtype,
        buffer=payload,
        offset=descriptor["offset_bytes"],
        strides=tuple(descriptor["strides_bytes"]) or None,
    )


def render_document(document: Mapping[str, Any], *, payload: Any = None, figure: Any = None, use_native_limits: bool = True, axes: Any = None):
    """Return editable Matplotlib figures from native-prepared artists."""

    from matplotlib import pyplot as plt

    if axes is not None:
        if len(axes) != len(document["panels"] or [document]):
            raise ValueError("one supplied axes is required for each panel")
        if figure is None:
            figure = axes[0].figure
        if any(axis.figure is not figure for axis in axes):
            raise ValueError("supplied axes must belong to the supplied figure")
    owned = figure is None
    if owned:
        figure = plt.figure()
    try:
        return _render_document(document, payload, figure, use_native_limits, axes)
    except BaseException:
        if owned:
            plt.close(figure)
        raise


def _render_document(document, payload, figure, use_native_limits, axes):
    panels = document["panels"] or [document]
    layout = document.get("layout") or {}
    rows, columns = layout.get("rows", len(panels)), layout.get("columns", 1)
    supplied_axes = axes is not None
    if axes is None:
        axes = [figure.add_subplot(rows, columns, index + 1) for index in range(len(panels))]
    for panel, axis in zip(panels, axes, strict=True):
        for layer in panel["layers"]:
            _draw_layer(figure, axis, layer, payload)
        for annotation in panel.get("annotations", ()):
            style = annotation.get("style") or {}
            kwargs = {}
            if style.get("axes_relative"):
                kwargs["transform"] = axis.transAxes
            for field, keyword in (("horizontal_alignment", "ha"), ("vertical_alignment", "va"), ("font_size", "fontsize")):
                if style.get(field) is not None:
                    kwargs[keyword] = style[field]
            if style.get("background"):
                kwargs["bbox"] = {"facecolor": style["background"], "alpha": 0.7}
            axis.text(annotation["x"], annotation["y"], annotation["text"], **kwargs)
        font = {} if layout.get("font_size") is None else {"fontsize": layout["font_size"]}
        title_font = font if panel.get("title_font_size") is None else {"fontsize": panel["title_font_size"]}
        axis.set_title(panel.get("title", ""), **title_font)
        if panel.get("frame_visible") is not None:
            axis.set_frame_on(panel["frame_visible"])
        if font:
            axis.tick_params(labelsize=layout["font_size"])
        if panel.get("axes"):
            x = panel["axes"][0]
            axis.set_xlabel(x["label"], **font)
            if use_native_limits:
                axis.set_xlim(x["lower"], x["upper"])
            axis.set_xscale(x.get("scale", "linear"))
        if len(panel.get("axes", ())) > 1:
            y = panel["axes"][1]
            axis.set_ylabel(y["label"], **font)
            if use_native_limits:
                axis.set_ylim(y["lower"], y["upper"])
            axis.set_yscale(y.get("scale", "linear"))
        aspect = panel.get("aspect") or layout.get("aspect")
        if aspect == "equal_data_limits":
            axis.set_aspect("equal", adjustable="datalim")
        elif aspect:
            axis.set_aspect(aspect)
        if document["show_legend"] and len(panel["layers"]) > 1:
            axis.legend()
    if not supplied_axes:
        figure.suptitle(document.get("title", ""))
    if layout.get("margins"):
        left, right, bottom, top, wspace, hspace = layout["margins"]
        figure.subplots_adjust(left=left, right=right, bottom=bottom, top=top, wspace=wspace, hspace=hspace)
    return figure, axes[0] if len(axes) == 1 else axes


def render_current(document, payload_path, preserve_title=False):
    """Draw one native simutil panel on Matplotlib's current axes."""

    from matplotlib import pyplot as plt

    axis = plt.gca()
    title = axis.get_title()
    payload = np.memmap(payload_path, dtype="u1", mode="r")
    render_document(document, payload=payload, figure=axis.figure, axes=[axis])
    if preserve_title:
        axis.set_title(title)


def _draw_layer(figure, axis, layer, payload):
    from matplotlib.patches import Circle

    data = layer.get("data")
    style = layer.get("style") or {}
    color = style.get("color")
    if data and data["kind"] == "image":
        image = axis.imshow(
            array_view(payload, data["pixels"]), extent=data["extent"],
            vmin=data["range"][0], vmax=data["range"][1], cmap=data["color_map"],
            interpolation=data["interpolation"], origin=data["origin"],
        )
        if data["color_bar"]:
            bar = figure.colorbar(image, ax=axis, pad=0)
            if data.get("color_bar_font_size") is not None:
                bar.ax.tick_params(labelsize=data["color_bar_font_size"])
        return
    if data:
        x, y = array_view(payload, data["x"]), array_view(payload, data["y"])
    else:
        x, y = layer["x_values"], layer["y_values"]
    if data and data["kind"] == "circles":
        radii = array_view(payload, data["radii"])
        box = None
        if data.get("anchored"):
            from matplotlib.offsetbox import AnchoredOffsetbox, AuxTransformBox

            box = AuxTransformBox(axis.transData)
            box.set_alpha(0.7)
        for xc, yc, radius in zip(x, y, radii, strict=True):
            circle_color = {"facecolor": color} if data["filled"] else {"edgecolor": color}
            circle = Circle((xc, yc), radius=radius, fill=data["filled"], linewidth=layer["line_width"], **circle_color)
            if box is None:
                axis.add_patch(circle)
            else:
                box.add_artist(circle)
        if box is not None:
            anchored = AnchoredOffsetbox(loc=3, pad=0.2, borderpad=0, child=box, frameon=False)
            anchored.set_alpha(0.7)
            axis.add_artist(anchored)
        return
    if layer["kind"] == "interval":
        for start, end, lane in zip(layer["interval_x_start"], layer["interval_x_end"], layer["interval_y"], strict=True):
            axis.plot([start, end], [lane, lane], color=color, linewidth=layer["line_width"])
    elif data or layer["kind"] == "line":
        axis.plot(
            x, y, label=layer["title"], color=color,
            marker=style.get("marker") or "None",
            linestyle=style.get("line_style") or ("-" if layer["kind"] == "line" else "None"),
            linewidth=layer["line_width"], alpha=layer["opacity"],
        )
    else:
        axis.scatter(x, y, label=layer["title"], s=layer["symbol_size"], color=color, alpha=layer["opacity"])
