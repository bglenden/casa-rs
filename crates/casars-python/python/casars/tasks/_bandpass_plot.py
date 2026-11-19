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
    return path


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
