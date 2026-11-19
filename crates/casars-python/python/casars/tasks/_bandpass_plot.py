"""Save and display native calibration panels using the shared renderer."""

from .._plot_document import render_document


def render_bandpass(pages, workspace):
    from matplotlib import pyplot as plt

    options = {"axes.formatter.useoffset": False, "axes.grid": True}
    with plt.style.context("classic"), plt.rc_context(options):
        for page in pages:
            draw_page(plt, page, workspace)


def draw_page(plt, page, workspace):
    plot = page["figure"]
    document = plot["document"]
    figure, axes = render_document(document, use_native_limits=False)
    try:
        if len(document["panels"]) == 1:
            axes = [axes]
        for axis, panel, y_limits in zip(axes, document["panels"], page["y_limits"], strict=True):
            axis.set_xlim(panel["axes"][0]["lower"], panel["axes"][0]["upper"])
            if y_limits is not None:
                axis.set_ylim(*y_limits)
            for artist, layer in zip(axis.lines, panel["layers"], strict=True):
                artist.set_markersize(layer["symbol_size"])
        if plot["output_file"] is not None:
            path = workspace / plot["output_file"]
            path.parent.mkdir(parents=True, exist_ok=True)
            figure.savefig(path, format="png", dpi=page["density"])
        if plot["show"]:
            figure.show()
    finally:
        if not plot["show"]:
            plt.close(figure)
