#!/usr/bin/env python3
# SPDX-License-Identifier: LGPL-3.0-or-later
"""Compare selected MeasurementSets from the public split and mstransform APIs.

Run with CASA 6.7.6.14, python-casacore and the editable casars package in
the same Python environment. Set CASARS_MSTRANSFORM_BIN to the built task.
"""

from contextlib import contextmanager
from datetime import datetime, timedelta
from enum import IntEnum
from pathlib import Path
from tempfile import TemporaryDirectory

import casatasks
from casacore.tables import makearrcoldesc, maketabdesc, table as core_table
from casatools import table
import numpy as np

from casars import tasks
from record_casa_returns import make_cal_ms


class Observation(IntEnum):
    SECOND = 1


MAIN = ("TIME", "SCAN_NUMBER", "ANTENNA1", "ANTENNA2", "FIELD_ID", "DATA_DESC_ID",
        "OBSERVATION_ID", "STATE_ID", "ARRAY_ID", "FEED1", "FEED2",
        "DATA", "FLAG", "FLAG_ROW", "WEIGHT", "SIGMA", "UVW")
SUBTABLES = {
    "ANTENNA": ("NAME", "POSITION"),
    "DATA_DESCRIPTION": ("SPECTRAL_WINDOW_ID", "POLARIZATION_ID"),
    "FEED": ("ANTENNA_ID", "SPECTRAL_WINDOW_ID", "FEED_ID"),
    "FIELD": ("NAME", "PHASE_DIR", "SOURCE_ID"),
    "OBSERVATION": ("PROJECT", "TIME_RANGE"),
    "POINTING": ("ANTENNA_ID", "TIME"),
    "POLARIZATION": ("CORR_TYPE",),
    "SOURCE": ("SOURCE_ID", "SPECTRAL_WINDOW_ID", "NAME"),
    "STATE": ("OBS_MODE", "FLAG_ROW"),
    "SPECTRAL_WINDOW": ("NAME", "NUM_CHAN", "CHAN_FREQ", "CHAN_WIDTH", "RESOLUTION",
                        "EFFECTIVE_BW", "REF_FREQUENCY", "TOTAL_BANDWIDTH"),
}


@contextmanager
def casa_table(path, *, writable=False):
    tool = table()
    tool.open(str(path), nomodify=not writable)
    try:
        yield tool
    finally:
        tool.done()


def columns_from_casacore(path, names):
    with core_table(str(path), ack=False) as tool:
        result = {}
        for name in names:
            value = tool.getcol(name)
            # python-casacore represents string arrays as a shape and flat list.
            if isinstance(value, dict):
                value = np.asarray(value["array"]).reshape(value["shape"])
            result[name] = value
        return result


def compare_columns(expected, actual, names, case):
    want = columns_from_casacore(expected, names)
    got = columns_from_casacore(actual, names)
    for name, values in want.items():
        np.testing.assert_array_equal(got[name], values, err_msg=f"{case} {name}")
    # Reopen the native output in CASA as well as python-casacore.
    with casa_table(actual) as tool:
        for name, values in got.items():
            np.testing.assert_array_equal(tool.getcol(name).T, values, err_msg=f"CASA readback {case} {name}")


def calendar(seconds):
    epoch = datetime(1858, 11, 17) + timedelta(seconds=seconds)
    return epoch.strftime("%Y/%m/%d/%H:%M:%S.%f")[:-3]


def make_input(path):
    make_cal_ms(str(path))
    with casa_table(path, writable=True) as tool:
        shape = tool.getcol("DATA").shape
        # Distinguish every row, channel and correlation in the copy.
        values = np.arange(np.prod(shape)).reshape(shape, order="F")
        tool.putcol("DATA", (values + 1j * (values + 1)).astype(np.complex64))
        flags = values % 7 == 0
        flags[:, :, -1] = True
        tool.putcol("FLAG", flags)
        tool.putcol("FLAG_ROW", flags.all(axis=(0, 1)))
        sigma = np.exp2(-(values[:, 0, :] % 3)).astype(np.float32)
        tool.putcol("SIGMA", sigma)
        tool.putcol("WEIGHT", 1 / sigma**2)
        return np.unique(tool.getcol("TIME"))


def add_second_window(source):
    for name, changes in {
        "SPECTRAL_WINDOW": {"NAME": "SPW1"},
        "DATA_DESCRIPTION": {"SPECTRAL_WINDOW_ID": 1},
    }.items():
        with core_table(str(source / name), readonly=False, ack=False) as tool:
            tool.addrows(1)
            for column in tool.colnames():
                if tool.iscelldefined(column, 0):
                    tool.putcell(column, 1, changes.get(column, tool.getcell(column, 0)))
            if name == "SPECTRAL_WINDOW":
                for column in ("CHAN_FREQ", "REF_FREQUENCY"):
                    tool.putcell(column, 1, tool.getcell(column, 0) + 1e9)
                tool.addcols(maketabdesc([
                    makearrcoldesc("ASSOC_SPW_ID", 0, ndim=1, valuetype="int"),
                    makearrcoldesc("ASSOC_NATURE", "", ndim=1, valuetype="string"),
                ]))
                for row in range(2):
                    tool.putcell("ASSOC_SPW_ID", row, [0, 1])
                    tool.putcell("ASSOC_NATURE", row, ["association-0", "association-1"])
    for name, column, predicate in (
        ("", "DATA_DESC_ID", "FIELD_ID"),
        ("SOURCE", "SPECTRAL_WINDOW_ID", "SOURCE_ID"),
    ):
        with core_table(str(source / name), readonly=False, ack=False) as tool:
            tool.putcol(column, tool.getcol(predicate))


def add_observation_and_intent(source, *, interleaved=False):
    for name, changes in {
        "OBSERVATION": {"PROJECT": "Second observation"},
        "STATE": {"OBS_MODE": "CALIBRATE_PHASE.ON_SOURCE"},
    }.items():
        with core_table(str(source / name), readonly=False, ack=False) as tool:
            count = 2 if interleaved else 1
            tool.addrows(count)
            for index in range(1, 1 + count):
                for column in tool.colnames():
                    if tool.iscelldefined(column, 0):
                        tool.putcell(column, index, changes.get(column, tool.getcell(column, 0)))
    with core_table(str(source), readonly=False, ack=False) as tool:
        fields = tool.getcol("FIELD_ID")
        indices = np.arange(tool.nrows())
        tool.putcol("OBSERVATION_ID", indices % 3 if interleaved else fields)
        tool.putcol("STATE_ID", (indices // 3) % 3 if interleaved else fields)


def add_arrays_and_feeds(source):
    with core_table(str(source / "FEED"), readonly=False, ack=False) as tool:
        old_rows = tool.nrows()
        tool.addrows(old_rows)
        for old in range(old_rows):
            for column in tool.colnames():
                if tool.iscelldefined(column, old):
                    value = 2 if column == "FEED_ID" else tool.getcell(column, old)
                    tool.putcell(column, old + old_rows, value)
    with core_table(str(source), readonly=False, ack=False) as tool:
        indices = np.arange(tool.nrows())
        tool.putcol("ARRAY_ID", 2 + 2 * (indices % 4))
        tool.putcol("FEED1", 2 * (indices % 2))
        tool.putcol("FEED2", 2 * ((indices // 2) % 2))
        # 14.9 m is below 5 klambda at 100 GHz and above it at 101 GHz.
        # Give these samples a nonzero V component and a larger W component.
        edge = indices % 8 == 3
        u = np.where(edge, 8.94, 5 * (indices % 8))
        v = np.where(edge, 11.92, 0)
        tool.putcol("UVW", np.stack([u, v, 100 + indices], axis=1))


def succeeds(root, source, name, case, options, subtables=SUBTABLES, *, via_run=False):
    paths = [root / f"{name}-{case}-{side}.ms" for side in ("casa", "native")]
    for module, path in zip((casatasks, tasks), paths):
        arguments = dict(vis=str(source), outputvis=str(path), datacolumn="data", **options)
        result = tasks.run(name, **arguments).value if via_run and module is tasks else getattr(module, name)(**arguments)
        assert result is None, (name, case, result)
    compare_columns(*paths, MAIN, f"{name} {case} MAIN")
    for subtable, names in subtables.items():
        compare_columns(*(path / subtable for path in paths), names, f"{name} {case} {subtable}")
    return paths[1]


def empty_selection(root, source, name, time):
    for module in (casatasks, tasks):
        path = root / f"{name}-empty-{module.__name__}.ms"
        try:
            getattr(module, name)(vis=str(source), outputvis=str(path), datacolumn="data",
                                  field="FLUX", timerange=f">{calendar(time)}")
        except RuntimeError as error:
            assert "MSSelectionNullSelection" in str(error), (name, str(error))
        else:
            raise AssertionError(f"{module.__name__}.{name} accepted an empty selection")
        assert not path.exists(), path


def malformed_spw(root, source, name):
    for module in (casatasks, tasks):
        path = root / f"{name}-spw-error-{module.__name__}.ms"
        try:
            getattr(module, name)(vis=str(source), outputvis=str(path), datacolumn="data",
                                  spw="0:bogus")
        except RuntimeError as error:
            assert "bogus" in str(error), (name, str(error))
        else:
            raise AssertionError(f"{module.__name__}.{name} accepted a malformed channel selector")
        assert not path.exists(), path


def rejected_selector_types(root, source, name):
    cases = (
        ("observation", None, AssertionError, "null value not allowed"),
        ("observation", 1.5, AssertionError, "must be of cInt type"),
        ("observation", [1], RuntimeError, "["),
        ("observation", np.array([1]), RuntimeError, "["),
        ("observation", np.array([1], dtype=np.int16), AssertionError, "must be of cIntVec type"),
        ("observation", True, RuntimeError, "T"),
        ("intent", True, RuntimeError, "used for field intent with type Bool"),
        ("intent", 1, RuntimeError, "used for field intent with type Int"),
        ("intent", ["OBSERVE*"], RuntimeError, "used for field intent with type unknown"),
        ("intent", np.array("OBSERVE*"), RuntimeError, "used for field intent with type unknown"),
        ("intent", ["OBSERVE*", 1], AssertionError, "must be of cStrVec type"),
    )
    for index, (parameter, value, error_type, message) in enumerate(cases):
        for module in (casatasks, tasks):
            path = root / f"{name}-invalid-{index}-{module.__name__}.ms"
            try:
                getattr(module, name)(vis=str(source), outputvis=str(path), datacolumn="data", **{parameter: value})
            except error_type as error:
                assert message in str(error), str(error)
            else:
                raise AssertionError(f"{module.__name__}.{name} accepted {parameter}={value!r}")
            assert not path.exists(), path
    return len(cases)


def compare_array_uv_feed(root, source, name, subtables):
    first_time = columns_from_casacore(source, ("TIME",))["TIME"].min()
    cases = {
        "array-ordered": {"array": "8,2"},
        "array-repeated": {"array": "8,2,8"},
        "array-with-range": {"array": "8,2~4"},
        "array-with-bound": {"array": "8,>3"},
        "array-both-bounds": {"array": ">=2&<=6"},
        "array-strict-bounds": {"array": ">2&<6"},
        "array-numpy-int": {"array": np.int64(2)},
        "uv-less-inclusive": {"uvrange": "<10m"},
        "uv-greater-strict": {"uvrange": ">10m"},
        "uv-distance-union": {"uvrange": "0~5m,0.025~0.035km"},
        "uv-scientific-bounds": {"uvrange": "2.5e1~3.5e1m"},
        "uv-exponent-without-mantissa": {"uvrange": "e-3~5m"},
        "uv-wavelength-bounds": {"uvrange": "5~10klambda"},
        "uv-mixed-units": {"uvrange": "0~5m,5~10klambda"},
        "uv-percentage": {"uvrange": "5~10m:50%"},
        "uv-single-percentage": {"uvrange": "5m:100%"},
        "uv-numpy-int": {"uvrange": np.int64(10)},
        "feed-either": {"feed": "0"},
        "feed-cross": {"feed": "0&2"},
        "feed-include-equal": {"feed": "0,2&&"},
        "feed-equal": {"feed": "2&&&"},
        "feed-partial-id-list": {"feed": "0,1"},
        "feed-range": {"feed": "0~2&&"},
        "feed-negation": {"feed": "!0;2"},
        "feed-remove": {"feed": "0&&&;!2&&&"},
        "feed-numpy-int": {"feed": np.int64(0)},
        "array-uv-feed": {"array": "8,2~6", "uvrange": "5~30m", "feed": "0&2"},
        "array-uv-feed-time": {"array": "8,2~6", "uvrange": "5~30m", "feed": "0&2",
                               "timerange": f">{calendar(first_time - 1)}"},
    }
    if name == "split":
        cases.update({"array-int": {"array": 2}, "feed-int": {"feed": 0},
                      "array-scalar-array": {"array": np.array(2)}})
    else:
        cases["array-uv-feed-taql"] = cases["array-uv-feed"] | {"taql": "SCAN_NUMBER >= 0"}
        cases["uv-mixed-units-taql"] = cases["uv-mixed-units"] | {"taql": "SCAN_NUMBER >= 0"}
    for case, options in cases.items():
        selected = succeeds(root, source, name, case, options, subtables)
        if case == "uv-wavelength-bounds":
            output = columns_from_casacore(selected, ("UVW", "DATA_DESC_ID"))
            edges = output["DATA_DESC_ID"][output["UVW"][:, 1] != 0]
            assert len(edges) > 0 and (edges == 1).all(), (name, edges)
    provider_cases = {
        "provider-array-numpy-int": {"array": np.int32(2)},
        "provider-feed-numpy-int": {"feed": np.int32(0)},
    }
    for case, options in provider_cases.items():
        succeeds(root, source, name, case, options, subtables, via_run=True)

    rejections = [
        ("array", " ", RuntimeError, "Array Expression"),
        ("array", "4~4", RuntimeError, "Array Expression"),
        ("array", "<0", RuntimeError, "MSSelectionNullSelection"),
        ("array", None, AssertionError, "null value not allowed"),
        ("uvrange", "<=10m", RuntimeError, "UV Distribution Expression"),
        ("uvrange", "5m~10m", RuntimeError, "UV Distribution Expression"),
        ("uvrange", "10~5m", RuntimeError, "MSSelectionNullSelection"),
        ("uvrange", "10m", RuntimeError, "MSSelectionNullSelection"),
        ("uvrange", "5~10KM", RuntimeError, "Unrecognized units k"),
        ("uvrange", 10, RuntimeError, "used for field uvrange with type Int"),
        ("uvrange", [">10m"], RuntimeError, "used for field uvrange with type unknown"),
        ("feed", "0&", RuntimeError, "MSSelectionNullSelection"),
        ("feed", "1", RuntimeError, "No match found for requested feeds"),
        ("feed", "*", RuntimeError, "Feed Expression"),
        ("feed", 1.5, AssertionError, "must be of cInt type"),
        ("array", np.array(2, dtype=np.int16), AssertionError, "must be of cInt type"),
        ("array", np.array(2, dtype=np.uint8), AssertionError, "must be of cInt type"),
    ]
    if name == "split":
        rejections.append(("feed", [0], RuntimeError, "Feed Expression"))
    else:
        rejections.extend([
            ("array", 2, RuntimeError, "used for field array with type Int"),
            ("feed", 0, RuntimeError, "used for field feed with type Int"),
        ])
    for index, (parameter, value, error_type, message) in enumerate(rejections):
        for module in (casatasks, tasks):
            path = root / f"{name}-array-uv-feed-error-{index}-{module.__name__}.ms"
            try:
                getattr(module, name)(vis=str(source), outputvis=str(path), datacolumn="data", **{parameter: value})
            except error_type as error:
                assert message in str(error), (name, parameter, value, str(error))
            else:
                raise AssertionError(f"{module.__name__}.{name} accepted {parameter}={value!r}")
            assert not path.exists(), path
    for dtype in (np.int16, np.uint8):
        path = root / f"{name}-provider-array-{dtype.__name__}.ms"
        try:
            tasks.run(name, vis=str(source), outputvis=str(path), datacolumn="data",
                      array=np.array(2, dtype=dtype))
        except ValueError as error:
            assert "must be of cInt type" in str(error), str(error)
        else:
            raise AssertionError(f"tasks.run accepted ARRAY_ID as {dtype.__name__}")
        assert not path.exists(), path
    return len(cases) + len(provider_cases), len(rejections)


def main():
    assert casatasks.version() == [6, 7, 6, 14], casatasks.version()
    with TemporaryDirectory(prefix="ms-task-selection-") as scratch:
        root = Path(scratch)
        source = root / "input.ms"
        times = make_input(source)
        before = columns_from_casacore(source, MAIN)
        time = f"{calendar(times[2] - 1)}~{calendar(times[3] + 1)}"
        cases = {
            "field": {"field": "PH*"},
            "antenna": {"antenna": "A2&A4"},
            "time": {"timerange": time},
            "scan": {"scan": ">1"},
            "stride": {"spw": "SPW0:1~5^2"},
            "segments": {"spw": "SPW0:0~1;3~6"},
            "repeated-window": {"spw": "0:5~7,0:0~1"},
            "clipped-channel": {"spw": "0:5~90"},
            "drop-flagged": {"field": "PHASE", "spw": "SPW0:1~3", "keepflags": False},
            "combined": {"field": "FL*", "antenna": "A2&A4", "timerange": time, "spw": "SPW0:0~3"},
        }
        for name in ("split", "mstransform"):
            for case, options in cases.items():
                succeeds(root, source, name, case, options)
            empty_selection(root, source, name, times[-1] + 1)
            malformed_spw(root, source, name)
        for column, values in columns_from_casacore(source, MAIN).items():
            np.testing.assert_array_equal(values, before[column], err_msg=f"input {column}")
        add_second_window(source)
        subtables = SUBTABLES | {
            "SPECTRAL_WINDOW": (*SUBTABLES["SPECTRAL_WINDOW"], "ASSOC_SPW_ID", "ASSOC_NATURE"),
        }
        for name in ("split", "mstransform"):
            succeeds(root, source, name, "unused-window", {"field": "PHASE"}, subtables)
            selected = succeeds(root, source, name, "compact-second-window", {"spw": "SPW1:2~6"}, subtables)
            succeeds(root, selected, name, "native-input", {"spw": "SPW1:0~2"}, subtables)
        add_observation_and_intent(source)
        observation_cases = {
            "observation-id": {"observation": 1},
            "observation-np-id": {"observation": np.int64(1)},
            "observation-enum": {"observation": Observation.SECOND},
            "observation-scalar-array": {"observation": np.array(1)},
            "observation-scalar-text-array": {"observation": np.array("1")},
            "observation-range": {"observation": "1~2"},
            "observation-and-field": {"observation": "0,1", "field": "PH*"},
            "intent-pattern": {"intent": "CALIBRATE*"},
            "intent-np-id": {"intent": np.int32(1)},
            "intent-and-spw": {"intent": "CALIBRATE*,OBSERVE*", "spw": "SPW1:2~6"},
        }
        interleaved = root / "interleaved.ms"
        make_input(interleaved)
        add_observation_and_intent(interleaved, interleaved=True)
        rejected = 4
        for name in ("split", "mstransform"):
            for case, options in observation_cases.items():
                succeeds(root, source, name, case, options, subtables)
            succeeds(root, source, name, "provider-np-observation", {"observation": np.int32(1)}, subtables, via_run=True)
            succeeds(root, source, name, "provider-scalar-array", {"observation": np.array(1)}, subtables, via_run=True)
            succeeds(root, interleaved, name, "interleaved-observations-and-states", {"observation": "2,0", "intent": "2,1"})
            rejected += rejected_selector_types(root, source, name)
        products = (len(cases) + len(observation_cases) + 6) * 2
        add_arrays_and_feeds(source)
        before = columns_from_casacore(source, MAIN)
        for name in ("split", "mstransform"):
            more_products, more_rejections = compare_array_uv_feed(root, source, name, subtables)
            products += more_products
            rejected += more_rejections
        for column, values in columns_from_casacore(source, MAIN).items():
            np.testing.assert_array_equal(values, before[column], err_msg=f"input {column}")
        print(f"{products} selected products and {rejected} rejected calls matched CASA 6.7.6.14")


if __name__ == "__main__":
    main()
