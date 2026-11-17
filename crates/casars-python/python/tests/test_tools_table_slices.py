"""The two getcellslice cases from CASA's test_tool_table.py at cee5117dce.

A fixed-shape table supplies the same 3-by-4 array shape as the CASA image.
The other 11 upstream cases are in test_tools_table_row_iter.py.
"""

import numpy as np
import pytest

from casars import tools


@pytest.fixture
def array_cell(tmp_path):
    pixels = np.arange(12, dtype=np.float32).reshape(3, 4)
    tb = tools.table()
    description = {"map": {"valueType": "float", "ndim": 2, "shape": [3, 4]}}
    assert tb.create(str(tmp_path / "pixels.tab"), description, nrow=1)
    assert tb.putcell("map", 0, pixels)
    yield tb, pixels
    tb.done()


def test_getcellslice_whole_and_partial_cells(array_cell):
    tb, pixels = array_cell
    np.testing.assert_array_equal(tb.getcellslice("map", 0, -1, -1), pixels)
    np.testing.assert_array_equal(tb.getcellslice("map", 0, [-1], [-1]), pixels)
    np.testing.assert_array_equal(tb.getcellslice("map", 0, [0, 0], -1), pixels)
    np.testing.assert_array_equal(tb.getcellslice("map", 0, -1, [2, 3]), pixels)
    np.testing.assert_array_equal(tb.getcellslice("map", 0, -1, -1, [1, 1]), pixels)
    np.testing.assert_array_equal(tb.getcellslice("map", 0, [1, 1], -1), pixels[1:, 1:])
    np.testing.assert_array_equal(tb.getcellslice("map", 0, -1, [1, 1]), pixels[:2, :2])
    np.testing.assert_array_equal(tb.getcellslice("map", 0, -1, -1, [2, 2]), pixels[::2, ::2])


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (("bogus", 0, -1, -1), "Table column bogus is unknown"),
        (("map", 1, -1, -1), "row number 1 exceeds #rows 1"),
        (("map", 0, [0, 0, 0], -1), "blc must have length of 2"),
        (("map", 0, [0, -1], -1), "All elements of blc must be greater than or equal to 0"),
        (("map", 0, [0, 4], -1), "Element 1 of blc must be less than 4"),
        (("map", 0, -1, [5, 5, 5]), "trc must have length of 2"),
        (("map", 0, -1, [2, -1]), "All elements of trc must be greater than or equal to 0"),
        (("map", 0, -1, [4, 4]), "Element 0 of trc must be less than 3"),
        (
            ("map", 0, [1, 2], [2, 0]),
            "All elements of trc must be greater than or equal to their corresponding blc elements",
        ),
        (("map", 0, -1, -1, [1, 1, 1]), "incr must have length of 2"),
        (("map", 0, -1, -1, [2]), "incr must have length of 2"),
        (("map", 0, -1, -1, [1, 0]), "All elements of incr must be greater than 0"),
    ],
)
def test_getcellslice_reports_invalid_coordinates(array_cell, args, message):
    tb, _ = array_cell
    kwargs = dict(zip(("columnname", "rownr", "blc", "trc", "incr"), args))
    with pytest.raises(RuntimeError, match=message):
        tb.getcellslice(**kwargs)
