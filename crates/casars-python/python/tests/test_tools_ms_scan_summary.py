"""Pinned CASA and analytic controls for the public scan-summary record."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


@pytest.fixture
def scan_fixture(native_dataset, tmp_path):
    casa = pinned_casa()
    table = casa.table()
    for subtable in ("FIELD", "DATA_DESCRIPTION"):
        assert table.open(str(native_dataset / subtable), nomodify=False)
        first_row = {
            column: table.getcell(column, 0)
            for column in table.colnames()
            if table.iscelldefined(column, 0)
        }
        assert table.addrows(1)
        for column, value in first_row.items():
            assert table.putcell(column, 1, value)
        assert table.done()

    assert table.open(str(native_dataset), nomodify=False)
    start = float(table.getcell("TIME", 0))
    for name, values in {
        "ARRAY_ID": [0] * 9 + [1],
        "SCAN_NUMBER": [7] * 8 + [8, 7],
        "FIELD_ID": [1, 0, 1, 0, 1, 0, 0, 1, 0, 0],
        "DATA_DESC_ID": [1, 0, 1, 0, 1, 0, 0, 1, 0, 0],
        "STATE_ID": [2, -1, 2, -1, 3, -1, -1, 3, -1, -1],
        "TIME": [start + dt for dt in (0, 0, 10, 10, 20, 20, 30, 30, 35, 37)],
        "EXPOSURE": [2.0, 4.0, 6.0, 8.0, 10.0, 14.0, 16.0, 18.0, 22.0, 24.0],
    }.items():
        for row, value in enumerate(values):
            assert table.putcell(name, row, value)

    def view(name, rows):
        path = str(tmp_path / (name + ".ms"))
        output = table.selectrows(np.array(rows, dtype=np.int64), path)
        assert output.done()
        return path

    ordered = view("ordered", range(9))
    repeated = view("repeated", [7, 1, 0, 3, 2, 5, 4, 6, 8, 7])
    both_arrays = view("two-arrays", range(10))
    assert table.done()
    return casa, start, ordered, repeated, both_arrays


def subscan(start, begin, end, exposure, rows, ddids, state):
    return {
        "BeginTime": (start + begin) / 86400.0,
        "DDIds": np.array(ddids, dtype=np.int64),
        "EndTime": (start + end) / 86400.0,
        "FieldId": ddids[0],
        "IntegrationTime": exposure,
        "SpwIds": np.zeros(len(ddids), dtype=np.int64),
        "StateId": state,
        "nRow": rows,
    }


def check_record(actual, expected):
    assert list(actual) == list(expected)
    for key, value in expected.items():
        if isinstance(value, dict):
            check_record(actual[key], value)
        elif isinstance(value, np.ndarray):
            np.testing.assert_array_equal(actual[key], value, strict=True)
        else:
            assert type(actual[key]) is type(value)
            assert actual[key] == value, key


def test_ms_getscansummary_subscan_order_reference_rows_and_working_view(scan_fixture):
    casa, start, ordered, repeated, _ = scan_fixture
    later = {"0": subscan(start, 35, 35, 22.0, 1, [0], -1)}
    expected = {
        ordered: {
            "7": {
                "0": subscan(start, 0, 10, 5.0, 4, [1, 0], 2),
                "1": subscan(start, 20, 20, 12.0, 2, [1, 0], 3),
                "2": subscan(start, 30, 30, 17.0, 2, [0, 1], -1),
            },
            "8": later,
        },
        repeated: {
            "7": {
                "0": subscan(start, 0, 10, 5.0, 4, [0, 1], -1),
                "1": subscan(start, 20, 20, 12.0, 2, [0, 1], -1),
                "2": subscan(start, 30, 30, 52.0 / 3.0, 3, [1, 0], 3),
            },
            "8": later,
        },
    }
    for api in (casa, tools):
        selector = api.ms()
        assert selector.getscansummary() == {}
        for path, record in expected.items():
            assert selector.open(path)
            check_record(selector.getscansummary(), record)
            assert selector.select({"times": start})
            assert selector.nrow(True) == 2
            check_record(selector.getscansummary(), record)
            assert selector.selecttaql("T")
            order = [1, 0] if path == ordered else [0, 1]
            state = 2 if path == ordered else -1
            check_record(
                selector.getscansummary(),
                {"7": {"0": subscan(start, 0, 0, 3.0, 2, order, state)}},
            )
            assert selector.select({"rows": [100]}) is False
            assert selector.getscansummary() == {}
            assert selector.reset()
            check_record(selector.getscansummary(), record)
            assert selector.done()


def test_ms_getscansummary_reuses_scan_numbers_across_arrays(scan_fixture):
    casa, start, _, _, both_arrays = scan_fixture
    expected = {
        "7": {
            "0": subscan(start, 37, 37, 24.0, 1, [0], -1),
            "1": subscan(start, 20, 20, 12.0, 2, [1, 0], 3),
            "2": subscan(start, 30, 30, 17.0, 2, [0, 1], -1),
        },
        "8": {"0": subscan(start, 35, 35, 22.0, 1, [0], -1)},
    }
    for api in (casa, tools):
        selector = api.ms()
        assert selector.open(both_arrays)
        check_record(selector.getscansummary(), expected)
        assert selector.done()
