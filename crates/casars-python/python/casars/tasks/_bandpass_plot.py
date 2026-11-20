"""Save and display native calibration panels using the shared renderer."""

import subprocess

from .._plot_document import render_document


def render_bandpass(reply, workspace):
    for message in reply["diagnostics"]:
        print(message)
    pages = reply["pages"]
    if not pages:
        return 0, False
    from matplotlib import pyplot as plt

    options = {"axes.formatter.useoffset": False, "axes.grid": True}
    with plt.style.context("classic"), plt.rc_context(options):
        products, completed, immediate = browse_pages(plt, reply, workspace)
    if immediate:
        return completed, True
    if len(products) == 1 and reply["sequential"] and not reply["polynomial"]:
        product = products[0]
        name = product.name.split(".000.png", 1)[0] + ".png"
        target = product.with_name(name)
        product.replace(target)
        products[0] = target
    if reply["pdf"] is not None:
        make_pdf(reply["pdf"], products, workspace)
    return completed, False


def browse_pages(plt, reply, workspace):
    pages = reply["pages"]
    interactive = pages[0]["figure"]["show"]
    figure = plt.figure() if interactive else None
    products = []
    completed = current = 0
    while current < len(pages):
        page = pages[current]
        for message in page["diagnostics"]:
            print(message)
        path = draw_page(plt, page, workspace, figure, reply, len(products))
        if path is not None:
            products.append(path)
        completed = max(completed, current + 1)
        command = input("Press return for next page (b for backwards, q to quit): ") if page["prompt"] else ""
        if "q" in command:
            if page["quit_immediately"]:
                return products, completed, True
            if path is not None:
                products.append(save_page(figure, page, workspace, reply, len(products)))
            break
        if "b" in command:
            current = max(0, current - 1)
        else:
            current += 1
        if figure is not None and current < len(pages):
            figure.clear()
    return products, completed, False


def save_page(figure, page, workspace, reply, sequence):
    path = workspace / page["figure"]["output_file"]
    if reply["sequential"]:
        ending = f'.resample{reply["resample"]}.png' if reply["resample"] > 1 else ".png"
        stem = path.name.removesuffix(ending).rsplit(".", 1)[0]
        path = path.with_name(f"{stem}.{sequence:03}{ending}")
    path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(path, format="png", dpi=page["density"])
    return path


def draw_page(plt, page, workspace, figure, reply, sequence):
    plot = page["figure"]
    document = plot["document"]
    figure, axes = render_document(document, figure=figure, use_native_limits=False)
    path = None
    try:
        if len(document["panels"]) == 1:
            axes = [axes]
        for axis, panel, y_limits, atmosphere, fdm, legend in zip(
            axes, document["panels"], page["y_limits"], page["atmosphere"], page["fdm"],
            page["legends"], strict=True,
        ):
            axis.set_xlim(panel["axes"][0]["lower"], panel["axes"][0]["upper"])
            if y_limits is not None:
                axis.set_ylim(*y_limits)
            for artist, layer in zip(axis.lines, panel["layers"], strict=True):
                artist.set_markersize(layer["symbol_size"])
            if axis is axes[0]:
                header = document["header_lines"][0]
                if len(header) > 90 and not header.startswith("c1=") and "degamp=" not in header:
                    header = f"...{header[-90:]}"
                axis.text(0.07, 0.955, header, transform=figure.transFigure)
                for item in legend["identities"]:
                    axis.text(item["x"], item["y"], item["text"], color=item["color"],
                              transform=figure.transFigure)
            for marker in legend["markers"]:
                axis.plot([marker["x"]], [marker["y"]], color="k", marker=marker["marker"], linestyle="None",
                          transform=axis.transAxes, scalex=False, scaley=False,
                          markersize=panel["layers"][0]["symbol_size"])
            if atmosphere:
                with plt.rc_context({"axes.grid": True}):
                    draw_atmosphere(axis, atmosphere, document["layout"])
            if fdm is not None:
                draw_fdm_windows(axis, fdm)
        axes[-1].text(0.04, 0.02, page["footer"], transform=figure.transFigure)
        if plot["output_file"] is not None:
            path = save_page(figure, page, workspace, reply, sequence)
        if plot["show"]:
            figure.show()
            figure.canvas.draw_idle()
            figure.canvas.flush_events()
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
        anchor = -0.1 * layout["columns"] if curve["image"] else 1.01
        axis.text(anchor, (y[index] - lower) / expanded - 0.01 * layout["rows"],
                  f"{edge:.0f}{unit}", color=color, transform=axis.transAxes)
        portion = 0.25 * (1.0 - gap)
        floor_value = max(0.0, ceil - value_range * (1.0 + portion) / portion)
        floor_label = floor_value if unit == "K" else 100.0 * floor_value
        axis.text(anchor, 0.0, f"{floor_label:.0f}{unit}", color=color, transform=axis.transAxes)
    if len(curves) > 1:
        axis.text(0.88, 1.02, "Signal SB", color="m", transform=axis.transAxes)
        axis.text(0.02, 1.02, "Image SB", color="k", transform=axis.transAxes)


def draw_fdm_windows(axis, windows):
    x_lower, x_upper = axis.get_xlim()
    lower, upper = axis.get_ylim()
    lower -= 0.25 * (upper - lower)
    height = upper - lower
    width = x_upper - x_lower
    palette = [(0, 0, 0), (1, 0, 0), (0, 0, 1), (0, 0.5, 0)]
    for index, window in enumerate(windows):
        first, last = window["frequencies"]
        first, last = max(first, x_lower), min(last, x_upper)
        if first < last:
            y = lower + height * (0.03 + 0.04 * index)
            axis.plot([first, last], [y, y], linewidth=4, color=palette[index % len(palette)])
            axis.text(last + 0.02 * width, y - 0.016 * height, window["label"], size=7)
    axis.set_ylim(lower, upper)


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
