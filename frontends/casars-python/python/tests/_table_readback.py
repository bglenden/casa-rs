"""Read native tables in simulator tests without keeping a read lock."""

import numpy as np

from casars import tools


def open_table(path):
    reader = tools.table()
    assert reader.open(str(path), lockoptions={"option": "autonoread"})
    return reader


def read_column(reader, column):
    """Give tests one array entry per row, with the cell's axis order intact."""
    values = reader.getcol(column)
    if isinstance(values, np.ndarray) and values.ndim > 1:
        return np.moveaxis(values, -1, 0)
    if isinstance(values, np.ndarray) and values.dtype.kind in "SU":
        return values.tolist()
    return values
