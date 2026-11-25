# SPDX-License-Identifier: LGPL-3.0-or-later
"""Apply Matplotlib presentation controls to a native PlotMS figure."""

from collections import defaultdict
from datetime import datetime, timedelta
from functools import lru_cache

_MJD_EPOCH = datetime(1858, 11, 17)

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


def validate(arguments):
    """Check active CASA figure controls before creating any export."""

    for custom, prefix in (("customsymbol", "symbol"), ("customflaggedsymbol", "flaggedsymbol")):
        values = _typed_values(custom, arguments[custom], bool, "bool")
        if not any(values):
            continue
        _typed_values(f"{prefix}size", arguments[f"{prefix}size"], int, "int")
        for suffix, allowed in (
            ("shape", _MARKERS),
            ("color", ()),
            ("fill", ("fill", "mesh1", "mesh2", "mesh3", "nofill")),
        ):
            name = f"{prefix}{suffix}"
            value = arguments[name]
            if value is None or (isinstance(value, (str, list, tuple)) and len(value) == 0):
                continue
            for item in _typed_values(name, value, str, "string"):
                if allowed and item not in allowed:
                    raise ValueError(f"invalid {name} value '{item}'")
    limits = arguments["plotrange"]
    if isinstance(limits, (list, tuple)) or hasattr(limits, "__len__"):
        if len(limits) not in (0, 4):
            raise ValueError("plotrange parameter has incorrect number of elements.")
        try:
            list(map(float, limits))
        except (TypeError, ValueError):
            raise TypeError("plotrange elements must be numeric") from None
    if arguments["rowindex"] >= arguments["gridrows"] or arguments["colindex"] >= arguments["gridcols"]:
        raise ValueError("row/col index out of range")
    if arguments["iteraxis"]:
        for axis in ("x", "y"):
            if arguments[f"{axis}sharedaxis"] and not arguments[f"{axis}selfscale"]:
                article = "an" if axis == "x" else "a"
                raise ValueError(f"plots cannot share {article} {axis}-axis unless they use the same {axis}-axis scale.")


def _typed_values(name, value, expected, description):
    if isinstance(value, expected):
        return [value]
    if isinstance(value, (list, tuple)) and all(isinstance(item, expected) for item in value):
        return value
    raise TypeError(f"invalid {name} type, should be {description} or list")


def configure(figure, panel_axes, document, options):
    """Set figure formatting after the shared renderer draws the native layers."""

    headers = document["header_lines"]
    positioned = (document.get("layout") or {}).get("first_panel") is not None
    title_y = None
    if headers:
        title_y = _layout_page_headers(figure, headers)
        if document["title"] and not positioned:
            figure.suptitle(document["title"], y=title_y)
    font = options.get("titlefont")
    if font > 0 and document["title"]:
        if positioned:
            axes = panel_axes if isinstance(panel_axes, list) else [panel_axes]
            for axis in axes:
                axis.title.set_fontsize(font)
        else:
            figure.suptitle(document["title"], fontsize=font, y=title_y)
    measurement_set = document["id"] == "plotms-visibility"
    if measurement_set:
        primary = panel_axes if isinstance(panel_axes, list) else [panel_axes]
        _format_measurement_set_axes(primary, document, options)
        if not document["panels"] and not positioned:
            panel_axes.set_title("")
    layout = document.get("layout")
    if layout and document["panels"]:
        if measurement_set:
            _share_measurement_set_axes(figure, primary, layout, options)
        elif isinstance(panel_axes, list) and len(panel_axes) > 1:
            _share_axes(panel_axes, layout, options)
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
                _line(line, layer, options, measurement_set=measurement_set)
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
        position = options.get("legendposition") or "upperRight"
        location, anchor = _LEGEND[position]
        if measurement_set and position in ("exteriorRight", "exteriorLeft"):
            location = "upper left" if position == "exteriorRight" else "upper right"
            anchor = (anchor[0], 1.0)
        if handles:
            figure.axes[0].legend(handles, labels, loc=location, bbox_to_anchor=anchor)
    # Positioned pages reserve their grid cells even when some are empty.
    if measurement_set and not positioned:
        figure.tight_layout(rect=(0, 0, 1, title_y if title_y is not None else 1))


def _layout_page_headers(figure, headers):
    labels = [figure.text(0.025, 0.98, line, va="top", fontsize=9) for line in headers]
    boxes = [label.get_window_extent() for label in labels]
    width, height = figure.bbox.size
    scale = min(1.0, width * 0.95 / max(box.width for box in boxes),
                height * 0.23 / (len(labels) * max(box.height for box in boxes) * 1.25))
    for label in labels:
        label.set_fontsize(9 * scale)
    row_height = 1.25 * max(label.get_window_extent().height for label in labels) / height
    for index, label in enumerate(labels):
        label.set_y(0.98 - index * row_height)
    top = 0.98 - len(labels) * row_height
    figure.subplots_adjust(top=min(figure.subplotpars.top, top - 0.06))
    return top


def _format_measurement_set_axes(primary, document, options):
    averaged = options.get("averagedata")
    binned_channels = averaged and float(options.get("avgchannel") or 0) > 1
    binned_time = averaged and float(options.get("avgtime") or 0) > 0
    for axis, panel in zip(primary, document["panels"] or [document], strict=True):
        for dimension, data in zip("xy", panel["axes"]):
            scale = data["scale"]
            kind = options.get(f"{dimension}axis")
            if isinstance(scale, dict):
                _format_clock(axis, dimension, scale["mjd_seconds_clock"], options, binned_time)
            elif binned_channels and isinstance(kind, str) and kind.lower() in ("chan", "channel"):
                set_label = getattr(axis, f"set_{dimension}label")
                set_label(f"Average {data['label']}")


def _format_clock(axis, dimension, clock, options, averaged):
    from matplotlib.ticker import FuncFormatter, MaxNLocator

    midnight = clock["reference_mjd_seconds"]
    date = _MJD_EPOCH + timedelta(seconds=midnight)

    def tick(value, _):
        if dimension == "x":
            elapsed = round(value - midnight, 6)
            hours, remainder = divmod(abs(elapsed), 3600)
            minutes, seconds = divmod(remainder, 60)
            seconds = f"{seconds:09.6f}".rstrip("0").rstrip(".")
            sign = "-" if elapsed < 0 else ""
            return f"{sign}{int(hours):02d}:{int(minutes):02d}:{seconds}"
        timestamp = _MJD_EPOCH + timedelta(seconds=value)
        return timestamp.strftime("%Y/%m/%d\n%H:%M:%S")

    rendered_axis = getattr(axis, f"{dimension}axis")
    rendered_axis.set_major_locator(MaxNLocator(nbins=8, steps=[1, 2, 2.5, 5, 10]))
    rendered_axis.set_major_formatter(FuncFormatter(tick))
    label = options.get(f"{dimension}label")
    if not label:
        date_label = f" (from {date:%Y/%m/%d})" if midnight else ""
        label = f"Time{date_label} (hh:mm:ss)"
    if averaged:
        label = f"Average {label}"
    getattr(axis, f"set_{dimension}label")(label)


def _share_axes(axes, layout, options):
    for dimension, edge, count in (("x", "is_last_row", layout["rows"]), ("y", "is_first_col", layout["columns"])):
        if count < 2 or not options.get(f"{dimension}sharedaxis"):
            continue
        for axis in axes[1:]:
            getattr(axis, f"share{dimension}")(axes[0])
        for axis in axes:
            if not getattr(axis.get_subplotspec(), edge)():
                if dimension == "x":
                    axis.tick_params(axis="x", labelbottom=False)
                else:
                    axis.tick_params(axis="y", labelleft=False)
                getattr(axis, f"set_{dimension}label")("")


def _share_measurement_set_axes(figure, primary, layout, options):
    if layout["rows"] > 1 and options.get("xsharedaxis"):
        columns = defaultdict(list)
        for axis in primary:
            columns[axis.get_subplotspec().colspan.start].append(axis)
        for group in columns.values():
            bottom = max(group, key=lambda axis: axis.get_subplotspec().rowspan.start)
            for axis in group:
                if axis is not bottom:
                    axis.sharex(bottom)
                    axis.tick_params(axis="x", labelbottom=False)
                    axis.set_xlabel("")
    if layout["columns"] > 1 and options.get("ysharedaxis"):
        rows = defaultdict(list)
        for axis in figure.axes:
            side = axis.yaxis.get_label_position()
            rows[axis.get_subplotspec().rowspan.start, side].append(axis)
        for (_, side), group in rows.items():
            exterior = max if side == "right" else min
            edge = exterior(group, key=lambda axis: axis.get_subplotspec().colspan.start)
            for axis in group:
                if axis is not edge:
                    axis.sharey(edge)
                    axis.tick_params(axis="y", **{f"label{side}": False})
                    axis.set_ylabel("")


def _line(line, layer, options, *, measurement_set):
    index = layer["id"].partition("-")[0]
    series = int(index) if index.isdigit() else 0
    group = layer["color_group"]
    if measurement_set:
        group = group.rsplit("::", 1)[-1]
    flagged = group == "flagged"
    prefix = "flaggedsymbol" if flagged else "symbol"
    custom = "customflaggedsymbol" if flagged else "customsymbol"

    def option(name, default):
        return _per_series(options.get(name), series, default, broadcast=not measurement_set)

    if option(custom, False):
        shape = option(f"{prefix}shape", "circle" if flagged else "autoscaling")
        color = option(f"{prefix}color", "ff0000" if flagged else "0000ff")
        fill = option(f"{prefix}fill", "fill").lower()
        outline = option(f"{prefix}outline", False)
        size = option(f"{prefix}size", 2)
        # Explicit PlotMS marker colors carry their own opacity.
        line.set_alpha(None)
        marker = _MARKERS[shape.lower()]
        if measurement_set and group not in ("", "all", "flagged"):
            color = line.get_color()
        else:
            color = _color(color)
        line.set_marker(marker)
        line.set_markersize(size)
        line.set_markeredgecolor("black" if outline else color)
        patterned = measurement_set and fill in ("mesh1", "mesh2", "mesh3") and marker in ("o", "s", "D")
        line.set_markerfacecolor("none" if patterned or fill == "nofill" else color)
        if patterned:
            line.axes.scatter(
                line.get_xdata(), line.get_ydata(),
                marker=_mesh_marker(marker, fill), s=size ** 2, facecolor=color,
                linewidth=0, zorder=line.get_zorder() - 0.01,
            )
    else:
        line.set_markersize(layer["symbol_size"])
    if not measurement_set and options.get("xconnector") == "step":
        line.set_linestyle("-")
        line.set_drawstyle("steps-mid")


@lru_cache(maxsize=9)
def _mesh_marker(marker, fill):
    import numpy as np
    from matplotlib.markers import MarkerStyle
    from matplotlib.path import Path
    from matplotlib.transforms import Affine2D, Bbox

    shape = MarkerStyle(marker)
    outline = shape.get_path().transformed(shape.get_transform())
    bounds = outline.get_extents()
    xs = np.linspace(bounds.xmin, bounds.xmax, 17)
    ys = np.linspace(bounds.ymin, bounds.ymax, 17)
    regions = []
    for row, (bottom, top) in enumerate(zip(ys, ys[1:])):
        for column, (left, right) in enumerate(zip(xs, xs[1:])):
            if fill == "mesh2":
                filled = (row + column) % 2 == 0
            else:
                sparse = row % 2 == 0 and (column + row) % 4 == 0
                filled = not sparse if fill == "mesh1" else sparse
            if filled:
                clipped = outline.clip_to_bbox(Bbox.from_extents(left, bottom, right, top))
                if len(clipped.vertices):
                    regions.append(clipped)
    path = Path.make_compound_path(*regions)
    # Keep the size of the underlying marker when Matplotlib normalizes a custom path.
    extent = np.abs(path.vertices).max()
    return MarkerStyle(path, transform=Affine2D().scale(2 * extent))


def _per_series(value, series, default, *, broadcast):
    if isinstance(value, (list, tuple)):
        value = value[series] if series < len(value) else None
    elif series and not broadcast:
        return default
    return default if value is None else value


def _color(value):
    if len(value) in (3, 6, 8) and all(character in "0123456789abcdefABCDEF" for character in value):
        return "#" + value
    return value
