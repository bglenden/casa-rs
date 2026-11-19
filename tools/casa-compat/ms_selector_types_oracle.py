#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare the input types of basic MeasurementSet row selectors with pinned CASA."""

from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
import numpy as np

from casars import tasks
from ms_selection_tasks_oracle import MAIN, add_second_window, compare_columns, make_input

SELECTORS = (("field", 0), ("spw", 0), ("scan", 1), ("antenna", 0), ("timerange", 0))


def compare(root, source, task, name, case, argument):
    targets = [root / f"{task}-{name}-{case}-{side}.ms" for side in ("casa", "native")]
    for module, output in zip((casatasks, tasks), targets):
        result = getattr(module, task)(vis=str(source), outputvis=str(output), datacolumn="data",
                                       **{name: argument})
        assert result is None, (task, name, case, module.__name__, result)
    compare_columns(*targets, MAIN, f"{task} {name} {case}")


def reject(root, source, task, name, case, value, kind, message):
    targets = [root / f"{task}-{name}-{case}-{side}.ms" for side in ("casa", "native")]
    for module, output in zip((casatasks, tasks), targets):
        try:
            getattr(module, task)(vis=str(source), outputvis=str(output), datacolumn="data",
                                  **{name: value})
        except kind as error:
            if message is not None:
                assert message.lower() in str(error).lower(), (task, name, case, module.__name__, str(error))
        else:
            raise AssertionError((task, name, case, module.__name__))
        assert not output.exists(), output


def main():
    assert tuple(casatasks.version()) == (6, 7, 6, 14)
    with TemporaryDirectory(prefix="ms-selector-types-") as work:
        root = Path(work)
        source = root / "input.ms"
        make_input(source, channels=4)
        add_second_window(source)
        products = rejections = 0
        for task in ("split", "mstransform"):
            for name, number in SELECTORS:
                for case, value in (("np32", np.int32(number)), ("np64", np.int64(number))):
                    compare(root, source, task, name, case, value)
                    products += 1
                if task == "split" and name != "timerange":
                    compare(root, source, task, name, "integer", number)
                    compare(root, source, task, name, "zero-dimensional", np.array(number))
                    products += 2
                    for case, value in (
                        ("bool", bool(number)), ("int-list", [number]), ("string-list", [str(number)]),
                        ("tuple", (number,)), ("array", np.array([number])), ("empty", []),
                    ):
                        reject(root, source, task, name, case, value, RuntimeError, None)
                        rejections += 1
                else:
                    for case, value, record_type in (
                        ("integer", number, "Int"), ("bool", bool(number), "Bool"),
                        ("zero-dimensional", np.array(number), "unknown"),
                        ("int-list", [number], "unknown"), ("string-list", [str(number)], "unknown"),
                        ("tuple", (number,), "unknown"), ("array", np.array([number]), "unknown"),
                        ("empty", [], "unknown"),
                    ):
                        reject(root, source, task, name, case, value, RuntimeError,
                               f"used for field {name} with type {record_type}")
                        rejections += 1
                reject(root, source, task, name, "float", float(number), AssertionError,
                       "must be of cInt type")
                reject(root, source, task, name, "null", None, AssertionError, "null value not allowed")
                rejections += 2
        print(f"{products} typed selector products and {rejections} rejections compared with CASA 6.7.6.14")


if __name__ == "__main__":
    main()
