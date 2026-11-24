"""Paired controls for CASA's record-driven MeasurementSet selector."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset


def check_rows(selector, raw, rows):
    assert selector.nrow(True) == len(rows)
    record = selector.getdata("data")
    if len(rows):
        np.testing.assert_array_equal(record["data"], raw["data"][..., rows], strict=True)
    else:
        assert not record


def test_ms_select_record_field_order_and_integer_forms(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    first = raw["antenna1"]
    antenna_zero = np.flatnonzero(first == 0)
    table = casa.table()
    assert table.open(str(native_dataset))
    columns = {
        name: table.getcol(name)
        for name in ("ANTENNA2", "ARRAY_ID", "FEED1", "FEED2", "FIELD_ID", "SCAN_NUMBER", "DATA_DESC_ID")
    }
    assert table.done()
    cases = [
        ({"antenna1": 1}, np.flatnonzero(first == 1)),
        ({"aNtEnNa1": np.array([[1, 0, 1]], dtype=np.int64)}, np.flatnonzero(first <= 1)),
        ({"antenna1": np.array([1], dtype=np.uint64)}, np.flatnonzero(first == 1)),
        ({"antenna1": np.array([2**63 + 1], dtype=np.uint64)}, np.flatnonzero(first == 1)),
        ({"antenna1": np.int64(1)}, np.arange(first.size)),
        ({"antenna1": [True, 1]}, np.flatnonzero(first == 1)),
        ({"antenna1": np.array([2**32 + 1], dtype=np.int64)}, np.flatnonzero(first == 1)),
        ({"rows": [2, 0, -1, 1, 2, 200]}, np.array([0, 1, 2])),
        ({b"rows": [1]}, np.array([1])),
        ({"rows": [2, 4, 6], "antenna1": [0]}, antenna_zero[[2, 4, 6]]),
        ({"ROWS": [2, 4, 6], "antenna1": [0]}, np.array([2])),
        ({"ifr_number": [1002, 0]}, np.flatnonzero((first == 1) & (raw["antenna2"] == 2))),
    ]
    cases.extend(
        ({name.lower(): [int(values[-1])]}, np.flatnonzero(values == values[-1]))
        for name, values in columns.items()
    )
    for api in (casa, tools):
        selector = api.ms()
        assert selector.select({"antenna1": [1]})
        assert selector.open(str(native_dataset))
        for record, rows in cases:
            assert selector.select(record)
            check_rows(selector, raw, rows)
            assert selector.reset()
        assert selector.select({"antenna1": [99]}) is False
        check_rows(selector, raw, [])
        assert selector.done()


def test_ms_select_record_over_saved_row_order(baseline_fixture):
    casa, view, raw = baseline_fixture
    source_rows = np.array([16, 4, 28, 16, 6, 3])
    path = view("record.ms", source_rows)
    after_antenna = source_rows[raw["antenna1"][source_rows] == 1]
    for api in (casa, tools):
        selector = api.ms()
        assert selector.open(path)
        assert selector.select({"rows": [1, 0, 2], "antenna1": [1]})
        check_rows(selector, raw, after_antenna[:3])
        assert selector.select({"rows": [2, 0]})
        check_rows(selector, raw, after_antenna[[0, 2]])
        assert selector.done()


def test_ms_select_record_time_and_projected_baseline_ranges(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    times = np.unique(raw["time"])
    cases = [
        ({"time": times[:2]}, np.flatnonzero(raw["time"] <= times[1])),
        ({"times": [float(times[0]), float(times[2])]}, np.flatnonzero(raw["time"] != times[1])),
        ({"times": float(times[0])}, np.flatnonzero(raw["time"] == times[0])),
        ({"times": np.float32(times[0])}, np.arange(raw["time"].size)),
        ({"times": np.array(times[0])}, np.array([], dtype=np.int64)),
        ({"times": [np.nan, float(times[0])]}, np.flatnonzero(raw["time"] == times[0])),
        ({"times": np.array([], dtype=np.float64)}, np.array([], dtype=np.int64)),
        ({"times": [float(times[0] + 0.1)]}, np.array([], dtype=np.int64)),
    ]
    for mode, samples in zip(("u", "v", "w", "uvdist"), (*raw["uvw"], np.hypot(*raw["uvw"][:2]))):
        distinct = np.unique(samples)
        first, last = np.sort(np.argsort(np.diff(distinct))[-2:])
        lower = float((distinct[first] + distinct[first + 1]) / 2)
        upper = float((distinct[last] + distinct[last + 1]) / 2)
        cases.append(({mode: [lower, upper]}, np.flatnonzero((samples >= lower) & (samples <= upper))))
    for api in (casa, tools):
        selector = api.ms()
        assert selector.open(str(native_dataset))
        for record, rows in cases:
            assert selector.select(record)
            check_rows(selector, raw, rows)
            assert selector.reset()
        assert selector.done()


def test_ms_select_record_return_and_error_state(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    all_rows = np.arange(raw["data"].shape[-1])
    for api in (casa, tools):
        selector = api.ms()
        assert selector.select()
        assert selector.open(str(native_dataset))
        for empty in (
            {},
            {1: [1]},
            {"unknown": [b"a"]},
            {"antenna1": None},
            {"antenna1": np.array([1], dtype=object)},
            {"antenna1": np.array([b"1"], dtype="S1")},
            {"u": np.array([-5, 5], dtype=np.float16)},
        ):
            assert selector.select(empty)
            check_rows(selector, raw, all_rows)
        for invalid in (None, []):
            with pytest.raises(TypeError, match="_items is not a dictionary"):
                selector.select(invalid)
            check_rows(selector, raw, all_rows)
        for invalid in (
            {"antenna1": []},
            {"antenna1": [1.25]},
            {"antenna1": [False, True]},
            {"antenna1": "1"},
            {"antenna1": np.str_("1")},
            {"antenna1": b"1"},
        ):
            with pytest.raises(RuntimeError):
                selector.select(invalid)
            check_rows(selector, raw, all_rows)
        with pytest.raises(RuntimeError):
            selector.select({"rows": np.array(1)})
        check_rows(selector, raw, all_rows)
        assert selector.select({"u": [0.0], "antenna1": [1]}) is False
        check_rows(selector, raw, np.flatnonzero(raw["antenna1"] == 1))
        assert selector.done()


@pytest.mark.parametrize("record", [
    {"ifr_number": [0]},
    {"time": [5.2e9, 5.3e9]},
    {"uvdist": [1e7, 1e8], "w": "wrong type"},
])
def test_ms_select_record_structured_zero_rows_installs_empty_view(baseline_fixture, native_dataset, record):
    casa, _, raw = baseline_fixture
    for api in (casa, tools):
        selector = api.ms()
        assert selector.open(str(native_dataset))
        with pytest.raises(RuntimeError, match="MSSelectionNullSelection"):
            selector.select(record)
        check_rows(selector, raw, [])
        assert selector.nrow(False) == raw["time"].size
        assert selector.reset()
        check_rows(selector, raw, np.arange(raw["time"].size))
        assert selector.done()


def test_ms_select_record_rejects_nonvector_arrays(baseline_fixture, native_dataset):
    _, _, raw = baseline_fixture
    selector = tools.ms()
    assert selector.open(str(native_dataset))
    with pytest.raises(RuntimeError, match="requires a vector"):
        selector.select({"antenna1": [1], "rows": np.array([[1, 2], [3, 4]])})
    check_rows(selector, raw, np.flatnonzero(raw["antenna1"] == 1))
    assert selector.reset()
    with pytest.raises(RuntimeError, match="requires a vector"):
        selector.select({"u": np.array([[-500.0, 500.0], [-200.0, 200.0]])})
    check_rows(selector, raw, np.arange(raw["time"].size))
    assert selector.done()


def test_ms_select_record_reads_non_native_byte_order(baseline_fixture, native_dataset):
    _, _, raw = baseline_fixture
    endian = ">" if np.little_endian else "<"
    selector = tools.ms()
    assert selector.open(str(native_dataset))
    antenna = np.array([1], dtype=endian + "i8")
    assert selector.select({"antenna1": antenna})
    check_rows(selector, raw, np.flatnonzero(raw["antenna1"] == 1))
    assert selector.reset()
    time = raw["time"][0]
    assert selector.select({"times": np.array([time], dtype=endian + "f8")})
    check_rows(selector, raw, np.flatnonzero(raw["time"] == time))
    assert selector.done()


def test_ms_select_record_exact_times_keeps_auxiliary_indices(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    times = np.unique(raw["time"])
    for api in (casa, tools):
        selector = api.ms()
        assert selector.open(str(native_dataset))
        assert selector.selectinit()
        assert selector.msselect({"scan": "0"}, True)
        scan = selector.msselectedindices()["scan"]
        np.testing.assert_array_equal(scan, [0])
        assert selector.select({"times": float(times[0])})
        np.testing.assert_array_equal(selector.msselectedindices()["scan"], scan, strict=True)
        check_rows(selector, raw, np.flatnonzero(raw["time"] == times[0]))
        assert selector.reset()
        with pytest.raises(RuntimeError):
            selector.select({"time": [float(times[1]), float(times[0])]})
        with pytest.raises(RuntimeError):
            selector.msselectedindices()
        assert selector.done()


def test_ms_select_record_exact_times_preserves_working_metadata(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    time = raw["time"][0]
    rows = np.flatnonzero(raw["time"] == time)
    for api in (casa, tools):
        selector = api.ms()
        assert selector.open(str(native_dataset))
        assert selector.selectinit()
        earlier = selector.metadata()
        assert selector.select({"times": [float(time)]})
        later = selector.metadata()
        for metadata in (earlier, later):
            assert metadata.nrows(autoc=True) == raw["time"].size
        check_rows(selector, raw, rows)
        assert selector.select({"times": [float(time)]})
        assert selector.select({"rows": [0]})
        check_rows(selector, raw, rows[:1])
        for metadata in (earlier, later):
            assert metadata.nrows(autoc=True) == 1
            assert metadata.done()
        assert selector.done()


def test_ms_select_record_exact_times_then_row_independent_selection(baseline_fixture, native_dataset):
    casa, _, raw = baseline_fixture
    time = float(raw["time"][0])
    expected = int(np.count_nonzero(raw["time"] == time))
    for api in (casa, tools):
        selector = api.ms()
        assert selector.open(str(native_dataset))
        for method, value, status in (
            ("selecttaql", "T", True),
            ("selecttaql", "", False),
            ("msselect", {"taql": "T"}, True),
            ("msselect", {}, False),
        ):
            assert selector.reset()
            assert selector.selectinit()
            assert selector.select({"times": time})
            metadata = selector.metadata()
            assert metadata.nrows(autoc=True) == raw["time"].size
            assert getattr(selector, method)(value) is status
            assert selector.nrow(True) == expected
            assert metadata.nrows(autoc=True) == expected
            assert metadata.done()
        assert selector.done()
