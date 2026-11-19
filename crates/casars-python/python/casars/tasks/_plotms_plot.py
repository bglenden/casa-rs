# SPDX-License-Identifier: LGPL-3.0-or-later
"""Apply Matplotlib presentation controls to a native PlotMS figure."""

_LEGEND = {
    "upperRight": ("upper right", None),
    "upperLeft": ("upper left", None),
    "lowerRight": ("lower right", None),
    "lowerLeft": ("lower left", None),
    "exteriorRight": ("center left", (1.02, 0.5)),
    "exteriorLeft": ("center right", (-0.02, 0.5)),
    "exteriorTop": ("lower center", (0.5, 1.02)),
    "exteriorBottom": ("upper center", (0.5, -0.02)),
}
_MARKERS = {
    "nosymbol": "None", "autoscaling": "o", "circle": "o",
    "square": "s", "diamond": "D", "pixel": ",",
}
_STROKES = {"": "-", "solid": "-", "dot": ":", "dash": "--"}


def configure(figure, document, options):
    """Set figure formatting after the shared renderer draws the native layers."""

    font = options.get("titlefont")
    if font > 0 and document["title"]:
        figure.suptitle(document["title"], fontsize=font)
    for axis in figure.axes:
        for name, label in (("x", axis.xaxis.label), ("y", axis.yaxis.label)):
            size = options.get(f"{name}axisfont")
            if size > 0:
                label.set_size(size)
                axis.tick_params(axis=name, labelsize=size)
        for which in ("major", "minor"):
            style = options.get(f"{which}style")
            if options.get(f"show{which}grid") and style != "none":
                if which == "minor":
                    axis.minorticks_on()
                axis.grid(
                    True, which=which, linestyle=_STROKES[style],
                    linewidth=options.get(f"{which}width"),
                    color=_color(options.get(f"{which}color")),
                )
    layers = {
        layer["id"]: layer
        for panel in (document["panels"] or [document])
        for layer in panel["layers"]
    }
    for axis in figure.axes:
        for line in axis.lines:
            layer = layers.get(line.get_gid())
            if layer is not None:
                _line(line, layer, options)
    if options.get("showlegend") and figure.axes:
        handles, labels = [], []
        for axis in figure.axes:
            these_handles, these_labels = axis.get_legend_handles_labels()
            for handle, label in zip(these_handles, these_labels, strict=True):
                if label not in labels:
                    handles.append(handle)
                    labels.append(label)
            legend = axis.get_legend()
            if legend is not None:
                legend.remove()
        location, anchor = _LEGEND[options.get("legendposition") or "upperRight"]
        if handles:
            figure.axes[0].legend(handles, labels, loc=location, bbox_to_anchor=anchor)


def _line(line, layer, options):
    index = layer["id"].partition("-")[0]
    series = int(index) if index.isdigit() else 0
    flagged = layer["color_group"] == "flagged"
    prefix = "flaggedsymbol" if flagged else "symbol"
    custom = "customflaggedsymbol" if flagged else "customsymbol"
    if _per_series(options.get(custom), series, False):
        shape = _per_series(options.get(f"{prefix}shape"), series, "circle" if flagged else "autoscaling")
        color = _per_series(options.get(f"{prefix}color"), series, "ff0000" if flagged else "0000ff")
        fill = _per_series(options.get(f"{prefix}fill"), series, "fill")
        outline = _per_series(options.get(f"{prefix}outline"), series, False)
        size = _per_series(options.get(f"{prefix}size"), series, 2)
        line.set_marker(_MARKERS[shape.lower()])
        line.set_markersize(size)
        line.set_markeredgecolor("black" if outline else _color(color))
        line.set_markerfacecolor("none" if fill.lower() == "nofill" else _color(color))
    else:
        line.set_markersize(layer["symbol_size"])
    if options.get("xconnector") == "step":
        line.set_linestyle("-")
        line.set_drawstyle("steps-mid")


def _per_series(value, series, default):
    if isinstance(value, (list, tuple)):
        return value[series] if series < len(value) else default
    return default if value is None else value


def _color(value):
    if len(value) in (3, 6, 8) and all(character in "0123456789abcdefABCDEF" for character in value):
        return "#" + value
    return value
