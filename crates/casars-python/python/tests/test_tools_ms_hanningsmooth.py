"""In-place Hanning smoothing through the public MS tool."""

import numpy as np
import pytest

from casars import tools
from test_tools_ms_putdata import writer_api
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset


OBSERVED = np.array([0, 2, 8, 20], dtype=np.complex64) + 1j
SMOOTHED = np.array([1, 3, 9.5, 14], dtype=np.complex64) + 1j
CORRECTED = np.array([5, 9, 21, 45], dtype=np.complex64) - 2j
SMOOTHED_CORRECTED = np.array([7, 11, 24, 33], dtype=np.complex64) - 2j
SPECTRUM = np.array([[4, 0, 16, 8], [1, 2, 3, 4]], dtype=np.float32)


def write_input(casa, dataset, corrected=False, spectrum=False):
    table = casa.table()
    assert table.open(str(dataset), nomodify=False)
    shape = table.getcol("DATA").shape
    data = np.broadcast_to(OBSERVED[None, :, None], shape).copy()
    assert table.putcol("DATA", data)
    flags = np.zeros(shape, dtype=np.bool_)
    flags[0, 1, 0] = True
    assert table.putcol("FLAG", flags)
    if corrected:
        assert table.addcols({"CORRECTED_DATA": {"valueType": "complex", "ndim": 2}})
        for row in range(table.nrows()):
            assert table.putcell("CORRECTED_DATA", row, np.broadcast_to(CORRECTED, (2, 4)).copy())
    if spectrum:
        assert table.addcols({"WEIGHT_SPECTRUM": {"valueType": "float", "ndim": 2}})
        for row in range(table.nrows()):
            assert table.putcell("WEIGHT_SPECTRUM", row, SPECTRUM)
    sigma = table.getcell("SIGMA", 0)
    weight = table.getcell("WEIGHT", 0)
    assert table.done()
    return sigma, weight


@pytest.mark.parametrize("mode", ["data", "corrected", "all"])
@pytest.mark.parametrize("spectrum", [False, True])
def test_hanningsmooth_science(native_dataset, writer_api, mode, spectrum):
    casa = pytest.importorskip("casatools")
    assert casa.version_string().replace("-", ".") == "6.7.6.14"
    sigma, weight = write_input(casa, native_dataset, corrected=mode == "all", spectrum=spectrum)
    tool = writer_api.ms()
    assert tool.hanningsmooth() is False
    assert tool.open(str(native_dataset))
    assert tool.hanningsmooth(mode) is False
    assert tool.done()
    assert tool.open(str(native_dataset), nomodify=False)
    if mode == "corrected":
        assert tool.hanningsmooth()
    else:
        assert tool.hanningsmooth(mode)
    assert tool.done()

    for api in (casa, tools):
        table = api.table()
        assert table.open(str(native_dataset))
        expected_data = OBSERVED if mode == "corrected" else SMOOTHED
        np.testing.assert_array_equal(
            table.getcell("DATA", 0), np.broadcast_to(expected_data, (2, 4)), strict=True,
        )
        np.testing.assert_array_equal(
            table.getcell("FLAG", 0),
            [[True, True, True, True], [True, False, False, True]],
            strict=True,
        )
        np.testing.assert_array_equal(table.getcell("SIGMA", 0), sigma, strict=True)
        if spectrum:
            np.testing.assert_array_equal(table.getcell("WEIGHT", 0), weight, strict=True)
            np.testing.assert_allclose(
                table.getcell("WEIGHT_SPECTRUM", 0),
                np.array([[0, 0, 0, 0], [0, 4.8, 7.68, 0]], dtype=np.float32),
                atol=0, rtol=1e-7, strict=True,
            )
        else:
            np.testing.assert_allclose(
                table.getcell("WEIGHT", 0), weight * np.float32(8 / 3), atol=0, rtol=1e-7,
                strict=True,
            )
        if mode != "data":
            corrected = SMOOTHED_CORRECTED if mode == "all" else SMOOTHED
            np.testing.assert_array_equal(
                table.getcell("CORRECTED_DATA", 0), np.broadcast_to(corrected, (2, 4)), strict=True,
            )
        if mode == "corrected":
            np.testing.assert_array_equal(
                table.getcell("MODEL_DATA", 0), np.ones((2, 4), dtype=np.complex64), strict=True,
            )
        assert table.done()


def test_hanningsmooth_uppercase_data(native_dataset):
    casa = pytest.importorskip("casatools")
    write_input(casa, native_dataset)
    tool = tools.ms()
    assert tool.open(str(native_dataset), nomodify=False)
    assert tool.hanningsmooth("DATA")
    assert tool.done()
    table = casa.table()
    assert table.open(str(native_dataset))
    np.testing.assert_array_equal(table.getcell("DATA", 0), np.broadcast_to(SMOOTHED, (2, 4)), strict=True)
    assert "CORRECTED_DATA" not in table.colnames()
    assert table.done()
