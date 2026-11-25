#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Pair static MeasurementSet page headers and selection metadata with pinned CASA."""

import argparse
from contextlib import chdir
from importlib.metadata import version
from io import BytesIO
import os
from pathlib import Path
import subprocess
from unittest.mock import patch

import numpy as np
from PIL import Image

from plotms_ms_columns_oracle import casa, opened, plotms_launcher, reference, request
from plotms_ms_iteration_axes_oracle import data_fixture


WIDTH, HEIGHT = 700, 450
TITLE = "Selected targets"
HEADERS = "filename,observer,projid,targname,targdir"
FLUX = ("Clara", "FLUX-PROJECT", "FLUX", "00:00:00.000000 -30.00.00.00000 J2000")
PHASE = ("Diego", "PHASE-PROJECT", "PHASE", "01:00:00.000000 -30.00.00.00000 J2000")


def row_values(table, row):
    return {name: table.getcell(name, row) for name in table.colnames()
            if table.iscelldefined(name, row)}


def write_values(table, row, values):
    for name, value in values.items():
        table.putcell(name, row, value)


def prepare_metadata(vis):
    with opened(vis / "OBSERVATION", True) as table:
        assert table.nrows() == 1
        original = row_values(table, 0)
        table.addrows(1)
        for row, (observer, project, *_) in enumerate((FLUX, PHASE)):
            write_values(table, row, original | dict(OBSERVER=observer, PROJECT=project))
        table.putcell("TIME_RANGE", 1, np.array([5_000_000_000.0, 5_000_000_030.0]))
        table.putcell("TELESCOPE_NAME", 1, "ALMA")
    with opened(vis, True) as table:
        table.putcol("OBSERVATION_ID", table.getcol("FIELD_ID"))
    with opened(vis / "SOURCE", True) as table:
        assert table.nrows() == 2
        # The earliest compatible source row wins. FIELD directions are decoys.
        phase = row_values(table, 1)
        write_values(table, 1, phase | dict(TIME=200.0, DIRECTION=np.deg2rad([45.0, -20.0])))
        table.addrows(2)
        write_values(table, 2, phase | dict(TIME=100.0, SPECTRAL_WINDOW_ID=-1))
        write_values(table, 3, phase | dict(TIME=0.0, SPECTRAL_WINDOW_ID=1, DIRECTION=np.deg2rad([90.0, -10.0])))
    with opened(vis / "FIELD", True) as table:
        for row in range(table.nrows()):
            table.putcell("PHASE_DIR", row, np.deg2rad([[60.0], [5.0]]))


def labels(vis, values):
    observer, project, field, direction = values
    return (f"File: {vis.name}", f"Observer: {observer}", f"Project ID: {project}",
            f"Target name: {field}", f"Target RA,Dec: {direction}".rstrip())


def ocr(image, region):
    crop = image.crop(region)
    sample = BytesIO()
    crop.resize((crop.width * 3, crop.height * 3)).save(sample, format="PNG")
    output = subprocess.run(["tesseract", "stdin", "stdout", "--psm", "6"],
                            input=sample.getvalue(), capture_output=True, check=True).stdout.decode()
    return " ".join(output.split())


def check_pinned(path, expected):
    with Image.open(path) as image:
        assert image.width == WIDTH
        # At this width Qt arranges the requested items in two columns.
        regions = [(392 if index % 2 else 0, 4 + 30 * (index // 2),
                    WIDTH if index % 2 else 385, 29 + 30 * (index // 2))
                   for index in range(len(expected))]
        fields = tuple(ocr(image, region) for region in regions)
        assert fields == expected, (path, fields, expected)
        assert TITLE in ocr(image, (0, 100, WIDTH, 150)), path
        gap = np.asarray(image.crop((0, 91, WIDTH, 112)).convert("RGB"))
        assert np.all(gap == 255), path


def compare_pages(root, native, name, controls, suffixes, expected):
    from casars.tasks import _plotms_plot

    pinned, actual = (root / f"{name}-{engine}" for engine in ("casa", "native"))
    for directory in (pinned, actual):
        directory.mkdir()
    assert reference(**controls, plotfile=str(pinned / "plot.png")) is None
    names = [f"plot{suffix}.png" for suffix in suffixes]
    assert {file.name for file in pinned.glob("*.png")} == set(names)
    for filename in names:
        check_pinned(pinned / filename, expected)

    configure = _plotms_plot.configure
    seen = []

    def capture(figure, axes, document, options):
        configure(figure, axes, document, options)
        lines = document["header_lines"]
        normalized = " ".join(" ".join(lines).split())
        assert normalized == " ".join(expected), normalized
        figure.canvas.draw()
        headers = [text for text in figure.texts if text.get_text() in lines]
        titles = [text for text in (*figure.texts, *(axis.title for axis in figure.axes))
                  if text.get_text().startswith(TITLE) and text.get_visible()]
        assert len(headers) == len(lines) and titles
        bounds = [text.get_window_extent() for text in headers]
        assert all(0 <= box.x0 < box.x1 <= WIDTH and 0 <= box.y0 < box.y1 <= HEIGHT for box in bounds)
        assert all(upper.y0 > lower.y1 for upper, lower in zip(bounds, bounds[1:]))
        assert all(title.get_window_extent().y1 < bounds[-1].y0 for title in titles)
        seen.append(normalized)

    with patch.object(_plotms_plot, "configure", side_effect=capture):
        assert native(**controls, plotfile=str(actual / "plot.png")) is None
    assert len(seen) == len(names)
    assert {file.name for file in actual.glob("*.png")} == set(names)
    for filename in names:
        with Image.open(actual / filename) as image:
            assert image.size == (WIDTH, HEIGHT)
    print(f"{name}: {len(names)} paired pages, {expected}", flush=True)
    return len(names)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--native-bin", type=Path, required=True)
    parser.add_argument("--app-run", type=Path, required=True)
    args = parser.parse_args()
    assert casa.version_string() == "6.7.6.14"
    assert version("casaplotms") == "2.9.1"
    from casars.tasks import plotms as native

    root = args.work.resolve()
    root.mkdir(parents=True)
    os.environ["CASARS_MSEXPLORE_BIN"] = str(args.native_bin.resolve())
    with chdir(root), plotms_launcher(args.app_run.resolve()):
        vis, _ = data_fixture(root)
        prepare_metadata(vis)
        controls = request(vis) | dict(
            xaxis="channel", xdatacolumn="", yaxis="amp", ydatacolumn="data", field="",
            spw="0:1", exprange="all", gridrows=1, gridcols=1, title=TITLE,
            headeritems=HEADERS, width=WIDTH, height=HEIGHT, dpi=100, expformat="png",
        )
        pages = (
            ("field", dict(iteraxis="field"), ("_FieldFLUX", "_FieldPHASE_2"), FLUX),
            ("scan", dict(iteraxis="scan", gridcols=2), ("_Scan1,2", "_Scan3,4_2", "_Scan5_3"), FLUX),
            ("selected-field", dict(field="PHASE"), ("",), PHASE),
        )
        count = sum(compare_pages(root, native, name, controls | extra, names, labels(vis, values))
                    for name, extra, names, values in pages)
        metadata = controls | dict(field="PHASE", headeritems="obsdate,obstime,telescope,ycolumn",
                                   yaxis=["amp", "phase"], ydatacolumn=["data", "corrected"])
        count += compare_pages(root, native, "observation-and-ordinates", metadata, ("",), (
            "Observation date: 27 Apr 2017", "Observation time: 08:53 UTC", "Telescope: ALMA",
            "Y Column(s): Amp:data, Phase:corrected",
        ))
        with opened(vis / "SOURCE", True) as table:
            table.putcol("SPECTRAL_WINDOW_ID", np.full(table.nrows(), 1, dtype=np.int32))
        count += compare_pages(root, native, "absent-direction", controls | dict(field="PHASE"),
                               ("",), labels(vis, (*PHASE[:3], "")))
        assert count == 8
        print(f"Public PlotMS page headers match the pinned reference across {count} PNG pairs.", flush=True)


if __name__ == "__main__":
    main()
