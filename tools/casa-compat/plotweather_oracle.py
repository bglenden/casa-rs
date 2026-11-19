#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare installed CASA and native plotweather results and drawn artists."""

import argparse
from contextlib import chdir
import inspect
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from unittest.mock import patch

import casatasks as casa
from casatools import table
import numpy as np
from matplotlib.figure import Figure
from matplotlib.image import imread

from calibration_smoothcal_oracle import opened
from record_casa_returns import make_cal_ms


def fixtures(root):
    source = root / "station.ms"
    make_cal_ms(str(source))
    with opened(source) as t:
        time_zero = t.getcol("TIME").min()
    with opened(source / "SPECTRAL_WINDOW", True) as t:
        references = [1.44e9, 22.235e9, 33.14e9, 49.9875e9, 100e9]
        t.addrows(len(references) - t.nrows())
        t.putcol("REF_FREQUENCY", references)
    missing = root / "missing.ms"
    shutil.copytree(source, missing)
    columns = {
        "TIME": np.array([0.0, 900, 1800, 2699.9]) + time_zero,
        "TEMPERATURE": [270.5, 274.25, 275.0, 275.5],
        "DEW_POINT": [265.1, 267.4, 270.1, 271.3],
        "WIND_SPEED": [1.0, 3.0, 5.0, 2.0],
        "WIND_DIRECTION": [0.0, np.pi / 2, np.pi, 3 * np.pi / 2],
        "PRESSURE": [790.0] * 4,
        "REL_HUMIDITY": [60.0] * 4,
    }
    description = {k: {"valueType": "double" if k == "TIME" else "float"} for k in columns}
    tool = table()
    assert tool.create(str(source / "WEATHER"), description, nrow=4)
    for name, values in columns.items():
        tool.putcol(name, values)
    tool.close()
    subsecond = root / "subsecond.ms"
    shutil.copytree(source, subsecond)
    with opened(subsecond / "WEATHER", True) as t:
        t.putcol("TIME", [5084545936.5, 5084545937.5, 5084545938.5, 5084545939.5])
    empty = root / "empty.ms"
    shutil.copytree(missing, empty)
    assert tool.create(str(empty / "WEATHER"), description, nrow=0)
    tool.close()
    broken = root / "broken.ms"
    shutil.copytree(missing, broken)
    (broken / "WEATHER").mkdir()
    short = root / "low.ms"
    shutil.copytree(missing, short)
    with opened(short / "SPECTRAL_WINDOW", True) as t:
        t.putcell("REF_FREQUENCY", 0, 0.02e9)


def artists(figure):
    return [
        {
            "labels": [axis.get_title(), axis.get_xlabel(), axis.get_ylabel()],
            "x_ticks": axis.get_xticks().tolist(),
            "x_labels": [label.get_text() for label in axis.get_xticklabels()],
            "limits": [list(axis.get_xlim()), list(axis.get_ylim())],
            "position": list(axis.get_position().bounds),
            "labels_y": [axis.yaxis.get_label_position(), axis.yaxis.label.get_rotation()],
            "legend": [] if axis.legend_ is None else [t.get_text() for t in axis.legend_.get_texts()],
            "lines": [
                {
                    "x": np.asarray(line.get_xdata(), dtype=float).tolist(),
                    "y": np.asarray(line.get_ydata(), dtype=float).tolist(),
                    "color": line.get_color(),
                    "marker": line.get_marker(),
                    "width": line.get_linewidth(),
                }
                for line in axis.lines
            ],
            "winds": [
                [*text.get_position(), text.get_rotation(), text.get_text(), text.get_color()]
                for text in axis.texts
            ],
        }
        for axis in figure.axes
    ]


def evaluate(task):
    def invoke(args, keywords, product):
        received = []
        original_save = Figure.savefig

        def capture(figure, path, **kwargs):
            assert kwargs["dpi"] == 150
            received.extend(artists(figure))
            original_save(figure, path, **kwargs)

        with patch.object(Figure, "savefig", capture):
            start = time.perf_counter()
            values = task(*args, **keywords)
            seconds = time.perf_counter() - start
        assert isinstance(values, list) and all(type(value) is np.float64 for value in values)
        result = {"opacity": [float(value) for value in values], "seconds": seconds}
        if product:
            assert len(received) == 5
            assert imread(product).shape[:2] == (1500, 1275)
            Path(product).unlink()
            result["artists"] = received
        else:
            assert not received
        return result

    cases = [
        ("explicit", ("station.ms", 0.25, True, "weather.png"), {}, "weather.png"),
        ("defaults", (), {"vis": "station.ms"}, "station.ms.plotweather.png"),
        ("subsecond", ("subsecond.ms", 0.25, True, "subsecond.png"), {}, "subsecond.png"),
        ("station", ("station.ms", 0.0, False, "unused.png"), {}, None),
        ("seasonal", ("station.ms", True, np.bool_(False)), {}, None),
        ("extrapolated", (), {"vis": "station.ms", "seasonal_weight": np.float32(1.25), "doPlot": False}, None),
        ("nonfinite", ("station.ms", float("nan"), False), {}, None),
        ("missing", ("missing.ms", 0.0, True), {}, None),
        ("empty", ("empty.ms", 0.0, True), {}, None),
        ("broken", ("broken.ms", 0.0, True), {}, None),
    ]
    output = {}
    for name, args, keywords, product in cases:
        result = invoke(args, keywords, product)
        print(f"{name} {result['seconds']:.3f} s", flush=True)
        output[name] = result
    assert not any(Path(path).exists() for path in [
        "unused.png", "missing.ms.plotweather.png", "empty.ms.plotweather.png", "broken.ms.plotweather.png",
    ])
    errors = [
        ((), {}, AssertionError),
        ((None,), {}, AssertionError),
        ((Path("station.ms"),), {}, AssertionError),
        (("./does-not-exist.ms",), {}, AssertionError),
        (("station.ms", None), {}, AssertionError),
        (("station.ms", "0.5"), {}, AssertionError),
        (("station.ms", np.int16(1)), {}, AssertionError),
        (("station.ms", 0.5, 1), {}, AssertionError),
        (("station.ms", 0.5, False, Path("bad.png")), {}, AssertionError),
        (("station.ms", 0.5, False, None), {}, AssertionError),
        (("low.ms", 0.5, False), {}, ValueError),
        (("station.ms", float("nan"), True, "nan.png"), {}, ValueError),
        (("station.ms", "bad", None, 13), {}, AssertionError),
    ]
    output["errors"] = []
    for args, keywords, expected in errors:
        try:
            task(*args, **keywords)
        except expected as error:
            output["errors"].append([type(error).__name__, str(error)])
        else:
            raise AssertionError(f"invalid plotweather call succeeded: {args}")
    assert not Path("nan.png").exists()
    return output


def compare_plots(native, casa_plot):
    for panel, (actual, expected) in enumerate(zip(native, casa_plot, strict=True)):
        for label in ("labels", "x_labels", "labels_y", "legend"):
            assert actual[label] == expected[label], (label, actual[label], expected[label])
        for key in ("x_ticks", "limits", "position"):
            np.testing.assert_allclose(actual[key], expected[key], rtol=0, atol=1e-9)
        for actual_line, expected_line in zip(actual["lines"], expected["lines"], strict=True):
            for key in ("color", "marker", "width"):
                assert actual_line[key] == expected_line[key], (key, actual_line[key], expected_line[key])
            np.testing.assert_allclose(actual_line["x"], expected_line["x"], rtol=0, atol=1e-9)
            tolerance = 1e-6 if panel == 0 else 1e-12
            np.testing.assert_allclose(actual_line["y"], expected_line["y"], rtol=0, atol=tolerance)
        for actual_text, expected_text in zip(actual["winds"], expected["winds"], strict=True):
            assert actual_text[3:] == expected_text[3:]
            np.testing.assert_allclose(actual_text[:3], expected_text[:3], rtol=0, atol=1e-9)


def compare(native, casa_results):
    assert native.keys() == casa_results.keys()
    assert native["errors"] == casa_results["errors"], (native["errors"], casa_results["errors"])
    for name in native.keys() - {"errors"}:
        actual, expected = native[name], casa_results[name]
        np.testing.assert_allclose(actual["opacity"], expected["opacity"], rtol=5e-13, atol=1e-14)
        assert actual["opacity"][-1] == actual["opacity"][-2] or name == "nonfinite"
        if "artists" in expected:
            compare_plots(actual["artists"], expected["artists"])
    seasonal = np.array(native["seasonal"]["opacity"])
    station = np.array(native["station"]["opacity"])
    np.testing.assert_allclose(native["explicit"]["opacity"], 0.25 * seasonal + 0.75 * station, rtol=5e-13)
    np.testing.assert_allclose(native["extrapolated"]["opacity"], 1.25 * seasonal - 0.25 * station, rtol=5e-13)
    np.testing.assert_array_equal(native["missing"]["opacity"], seasonal)
    np.testing.assert_array_equal(native["empty"]["opacity"], seasonal)
    np.testing.assert_array_equal(native["broken"]["opacity"], seasonal)
    print("10 paired opacity cases, 15 matched panels, 13 exact errors, 5 numerical laws", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", required=True, type=Path)
    parser.add_argument("--native-bin", type=Path)
    parser.add_argument("--engine", choices=["both", "casa", "native"], default="both")
    args = parser.parse_args()
    root = args.work.resolve()
    assert casa.version_string() == "6.7.6.14"
    if args.engine in {"both", "casa"}:
        root.mkdir()
        fixtures(root)
        with chdir(root):
            results = evaluate(casa.plotweather)
        (root / "reference.json").write_text(json.dumps(results))
    if args.engine in {"both", "native"}:
        assert args.native_bin
        os.environ["CASARS_CALIBRATE_BIN"] = str(args.native_bin.resolve())
        os.environ["CASARS_REQUIRE_TASK_BINARIES"] = "1"
        from casars.tasks import plotweather
        assert list(inspect.signature(casa.plotweather).parameters.values()) == [
            p.replace(annotation=inspect.Parameter.empty) for p in inspect.signature(plotweather).parameters.values()
        ]
        with chdir(root):
            native = evaluate(plotweather)
        compare(native, json.loads((root / "reference.json").read_text()))
        cli = subprocess.run(
            [str(args.native_bin.resolve()), "plotweather", "--vis", "station.ms",
             "--plot-name", "cli.png", "--no-save-last"],
            cwd=root, capture_output=True, text=True, check=True,
        )
        assert "Use casars.tasks.plotweather for the weather figure." in cli.stderr
        assert not (root / "cli.png").exists()
        np.testing.assert_allclose(json.loads(cli.stdout), native["defaults"]["opacity"])


if __name__ == "__main__":
    main()
