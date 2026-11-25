#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair headless MeasurementSet file formats, sizing and publication."""

import argparse
from contextlib import chdir
from importlib.metadata import version
import os
from pathlib import Path
import subprocess
from unittest.mock import patch

import numpy as np
from PIL import Image

from plotms_ms_columns_oracle import casa, compare_text, plotms_launcher, reference, request, rows
from plotms_ms_iteration_axes_oracle import data_fixture
from plotms_ms_layout_oracle import occupied_marker_cells


SIZE = (500, 360)
MARKER = np.array([221, 34, 85])


def paired(root, name, native, options, suffix):
    outputs = [root / f"{name}-{engine}{suffix}" for engine in ("casa", "native")]
    for task, file in zip((reference, native), outputs, strict=True):
        assert task(**options, plotfile=str(file)) is None
        assert file.stat().st_size, file
    return outputs


def rasterize(file, header):
    assert file.read_bytes().startswith(header), file
    pattern = file.name + ".raster-%02d.png"
    subprocess.run(["gs", "-q", "-dSAFER", "-dBATCH", "-dNOPAUSE", "-r96", "-sDEVICE=png16m",
                    f"-sOutputFile={file.parent / pattern}", str(file)], check=True, capture_output=True)
    outputs = list(file.parent.glob(file.name + ".raster-*.png"))
    assert len(outputs) == 1, (file, outputs)
    return outputs[0]


def marker_bands(file):
    with Image.open(file) as image:
        pixels = np.asarray(image.convert("RGB"), dtype=np.int16)
    marked = np.max(np.abs(pixels - MARKER), axis=-1) < 50
    columns = np.flatnonzero(marked.sum(axis=0) >= 2)
    bands = np.split(columns, np.flatnonzero(np.diff(columns) > 1) + 1)
    assert len(bands) == 2 and all(len(band) >= 2 for band in bands), (file, bands)
    first_y = np.nonzero(marked[:, bands[0]])[0]
    second_y = np.nonzero(marked[:, bands[1]])[0]
    assert first_y.min() > second_y.max(), file


def check_raster(path, format, size, dpi):
    with Image.open(path) as image:
        assert (image.format, image.size) == (format, size), (path, image.format, image.size)
        np.testing.assert_allclose(image.info["dpi"], (dpi, dpi), atol=0.025, rtol=0)
    marker_bands(path)


def compare_formats(root, native, base):
    for fmt in ("txt", "png", "jpg", "pdf", "ps"):
        for explicit in (False, True):
            options = base | (dict(expformat=fmt) if explicit else {})
            suffix = ".opaque" if explicit else f".{fmt}"
            pinned, actual = paired(root, f"{fmt}-{'explicit' if explicit else 'inferred'}",
                                    native, options, suffix)
            if fmt == "txt":
                assert compare_text(actual, pinned) == 12
                assert all(len(sample) == 14 for sample in rows(pinned))
                assert "MJD(seconds) GHz" in pinned.read_text()
            elif fmt in ("png", "jpg"):
                for path in (pinned, actual):
                    check_raster(path, {"png": "PNG", "jpg": "JPEG"}[fmt], SIZE, 100)
            else:
                # Qt emits PDF for PostScript requests. The native writer uses the requested encoding.
                pinned_image = rasterize(pinned, b"%PDF-")
                actual_image = rasterize(actual, b"%PDF-" if fmt == "pdf" else b"%!PS-Adobe")
                for file in (pinned_image, actual_image):
                    marker_bands(file)
            print(f"{fmt} {'explicit' if explicit else 'inferred'}: paired output and selected plot content", flush=True)

    quiet, native_quiet = paired(root, "quiet-text-alias", native, base | dict(verbose=False), ".text")
    assert compare_text(native_quiet, quiet) == 12
    assert all(len(sample) == 2 for sample in rows(quiet))
    assert "# Channel Amp\n" in quiet.read_text()
    for image in paired(root, "jpeg-alias", native, base, ".JPEG"):
        check_raster(image, "JPEG", SIZE, 100)


def compare_sizes(root, native, base):
    cases = (
        ("width-only", dict(height=-1), (500, 567), 100),
        ("height-only", dict(width=-1), (751, 360), 100),
        ("highres-explicit-size", dict(highres=True, dpi=-1), SIZE, 96),
        ("highres-explicit-dpi", dict(highres=True, dpi=110), SIZE, 110),
        ("highres-natural-size", dict(highres=True, dpi=-1, width=-1, height=-1), (751, 567), 96),
        ("default-canvas", dict(dpi=-1, width=-1, height=-1), (751, 567), 96),
        ("with-header", dict(headeritems="filename"), (500, 473), 100),
    )
    for name, extra, size, dpi in cases:
        for image in paired(root, name, native, base | extra, ".PNG"):
            check_raster(image, "PNG", size, dpi)
        print(f"{name}: {size[0]} x {size[1]} at {dpi} DPI", flush=True)


def compare_pages(root, native, base):
    controls = base | dict(field="", iteraxis="scan", gridrows=1, gridcols=2)
    names = ("plot_Scan1,2.png", "plot_Scan3,4_2.png", "plot_Scan5_3.png")
    cells = ({(0, 0), (0, 1)}, {(0, 0), (0, 1)}, {(0, 0)})
    for range_, count in (("current", 1), ("all", 3)):
        for engine, task in (("casa", reference), ("native", native)):
            directory = root / f"{range_}-{engine}"
            directory.mkdir()
            assert task(**controls, exprange=range_, plotfile=str(directory / "plot.png")) is None
            assert {file.name for file in directory.iterdir() if file.suffix == ".png"} == set(names[:count])
            for name, expected in zip(names[:count], cells[:count], strict=True):
                assert occupied_marker_cells(directory / name, (1, 2)) == expected
    print("current/all scan pages match the pinned names and occupied canvas cells", flush=True)


def refused(task, options, destination, match):
    try:
        task(**options, plotfile=str(destination))
    except Exception as error:
        assert match in str(error).lower(), error
    else:
        raise AssertionError(f"accepted invalid request for {destination}")


def check_publication(root, native, base):
    cases = (
        ("no-extension", "", {}, "format"),
        ("invalid-extension", ".svg", {}, "format"),
        ("invalid-format", ".png", dict(expformat="bogus"), "expformat"),
        ("empty-selection-source", ".png", dict(vis=""), "empty plot"),
    )
    for engine, task in (("casa", reference), ("native", native)):
        for name, suffix, controls, match in cases:
            output = root / f"{name}-{engine}{suffix}"
            refused(task, base | controls, output, match)
            assert not output.exists()
        existing = root / f"png-inferred-{engine}.png"
        original = existing.read_bytes()
        refused(task, base | dict(overwrite=False), existing, "overwrite")
        refused(task, base | dict(expformat="bogus", overwrite=True), existing, "expformat")
        assert existing.read_bytes() == original

    from matplotlib.figure import Figure

    def interrupted_save(figure, path, **options):
        Path(path).write_bytes(b"incomplete renderer output")
        raise OSError("interrupted figure save")

    dest = root / "png-inferred-native.png"
    previous = dest.read_bytes()
    with patch.object(Figure, "savefig", interrupted_save):
        refused(native, base, dest, "interrupted figure save")
    assert dest.read_bytes() == previous
    assert not any(root.glob(f".{dest.name}.*"))
    print("invalid requests publish nothing and failed overwrites preserve the previous figure", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--app-run", type=Path)
    parser.add_argument("--only", choices=("formats", "sizes", "pages", "publication"))
    args = parser.parse_args()
    assert casa.version_string() == "6.7.6.14"
    assert version("casaplotms") == "2.9.1"
    from casars.tasks import plotms as native

    root = args.work.resolve()
    root.mkdir(parents=True)
    os.environ["CASARS_MSEXPLORE_BIN"] = str(args.native_bin.resolve())
    with chdir(root), plotms_launcher(args.app_run.resolve() if args.app_run else None):
        vis, _ = data_fixture(root)
        base = request(vis) | dict(
            xaxis="channel", xdatacolumn="", yaxis="amp", ydatacolumn="data", title="Selected amplitudes",
            customsymbol=True, symbolcolor="dd2255", symbolsize=7, plotrange=[0, 4, 0, 6],
            width=500, height=360, dpi=100,
        )
        if args.only in (None, "formats", "publication"):
            compare_formats(root, native, base)
        if args.only in (None, "sizes"):
            compare_sizes(root, native, base)
        if args.only in (None, "pages"):
            compare_pages(root, native, base)
        if args.only in (None, "publication"):
            check_publication(root, native, base)


if __name__ == "__main__":
    main()
