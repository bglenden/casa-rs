"""Save and display native calibration panels using the shared renderer."""

import subprocess

from .._plot_document import render_document


def render_bandpass(reply, workspace):
    from matplotlib import pyplot as plt

    pages = reply["pages"]
    if not pages:
        return
    options = {"axes.formatter.useoffset": False, "axes.grid": True}
    with plt.style.context("classic"), plt.rc_context(options):
        products = [draw_page(plt, page, workspace) for page in pages]
    if reply["pdf"] is not None:
        make_pdf(reply["pdf"], [path for path in products if path is not None], workspace)


def draw_page(plt, page, workspace):
    plot = page["figure"]
    document = plot["document"]
    figure, axes = render_document(document, use_native_limits=False)
    path = None
    try:
        if len(document["panels"]) == 1:
            axes = [axes]
        for axis, panel, y_limits, atmosphere in zip(
            axes, document["panels"], page["y_limits"], page["atmosphere"], strict=True,
        ):
            axis.set_xlim(panel["axes"][0]["lower"], panel["axes"][0]["upper"])
            if y_limits is not None:
                axis.set_ylim(*y_limits)
            for artist, layer in zip(axis.lines, panel["layers"], strict=True):
                artist.set_markersize(layer["symbol_size"])
            if atmosphere:
                with plt.rc_context({"axes.grid": True}):
                    draw_atmosphere(axis, atmosphere, document["layout"])
        if plot["output_file"] is not None:
            path = workspace / plot["output_file"]
            path.parent.mkdir(parents=True, exist_ok=True)
            figure.savefig(path, format="png", dpi=page["density"])
        if plot["show"]:
            figure.show()
    finally:
        if not plot["show"]:
            plt.close(figure)
    return path


def draw_atmosphere(axis, curves, layout):
    import numpy as np

    for curve in curves:
        x_lower, x_upper = axis.get_xlim()
        lower, upper = axis.get_ylim()
        data_range = upper - lower
        upper += data_range * (0.125 if curve["image"] else 0.25)
        expanded = upper - lower
        bottom = upper - expanded * 0.2
        gap = 0.5 if curve["image"] else 0.6
        top = upper - gap * expanded * 0.2
        samples = np.asarray(curve["y"])
        floor, ceil = np.min(samples), np.max(samples)
        value_range = max(ceil - floor, 0.05)
        if 1 < value_range < 10:
            value_range = 10.0
        y = top - (top - bottom) * (ceil - samples) / value_range
        color = "k" if curve["image"] else "m"
        axis.plot(curve["x"], y, color=color, marker="." if curve["show_points"] else "None",
                  linestyle="None" if curve["show_points"] else "-")
        padding = (x_upper - x_lower) * 0.01
        axis.set_xlim(x_lower - padding, x_upper + padding)
        axis.set_ylim(lower, upper)
        if not curve["image"]:
            axis.text(0.25, 1.0 - 0.04 * layout["rows"], curve["label"], color=color,
                      transform=axis.transAxes)
        index = 0 if curve["image"] else -1
        unit = "K" if curve["sky_temperature"] else "%"
        edge = samples[index] if unit == "K" else 100.0 * samples[index]
        axis.text(-0.1 * layout["columns"] if curve["image"] else 1.01,
                  (y[index] - lower) / expanded - 0.01 * layout["rows"],
                  f"{edge:.0f}{unit}", color=color, transform=axis.transAxes)


def make_pdf(options, png_paths, workspace):
    pages = []
    for png in sorted(set(png_paths)):
        product = png.with_suffix(".pdf")
        if not run_pdf_command(options["convert"], "-density", str(options["density"]), str(png), str(product)):
            print("ImageMagick's convert command not found, no PDF built")
            return
        pages.append(product)
    if not pages or (len(pages) == 1 and options["sequential"]):
        return
    target = workspace / options["output"]
    sources = [str(path) for path in pages]
    combined = run_pdf_command(options["pdftk"], *sources, "cat", "output", str(target))
    if not combined:
        combined = run_pdf_command(
            options["ghostscript"], "-q", "-sPAPERSIZE=letter", "-dNOPAUSE", "-dBATCH",
            "-sDEVICE=pdfwrite", f"-sOutputFile={target}", *sources,
        )
    if combined:
        for page in pages:
            if page != target:
                page.unlink(missing_ok=True)
        print(f"PDF left in {target}")
    else:
        print("Both pdftk and ghostscript are missing, so no PDF built.")


def run_pdf_command(program, *arguments):
    try:
        return subprocess.run([program, *arguments], check=False).returncode == 0
    except OSError:
        return False
