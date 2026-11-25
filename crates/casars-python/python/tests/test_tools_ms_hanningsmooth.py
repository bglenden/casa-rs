"""In-place Hanning smoothing through the public MS tool."""

from shutil import copytree

import numpy as np
import pytest

from casars import tools
from test_tools_ms_putdata import writer_api
from test_tools_ms_visibility_ifraxis import baseline_fixture
from test_tools_msmetadata import measures, native_dataset
from test_tools_simulator import configure


OBSERVED = np.array([0, 2, 8, 20], dtype=np.complex64) + 1j
SMOOTHED = np.array([1, 3, 9.5, 14], dtype=np.complex64) + 1j
CORRECTED = np.array([5, 9, 21, 45], dtype=np.complex64) - 2j
SMOOTHED_CORRECTED = np.array([7, 11, 24, 33], dtype=np.complex64) - 2j
SPECTRUM = np.array([[4, 0, 16, 8], [1, 2, 3, 4]], dtype=np.float32)


def expected_visibilities(spectrum):
    # Both table tools expose on-disk Complex32 arrays as Complex128.
    return np.broadcast_to(spectrum.astype(np.complex128), (2, 4))


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
        assert table.getcoldesc("DATA")["valueType"] == "complex"
        np.testing.assert_array_equal(
            table.getcell("DATA", 0), expected_visibilities(expected_data), strict=True,
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
                np.array([[0, 0, 0, 0], [0, 4.8, 7.68, 0]], dtype=np.float64),
                atol=0, rtol=1e-7, strict=True,
            )
        else:
            np.testing.assert_allclose(
                table.getcell("WEIGHT", 0), weight * np.float32(8 / 3), atol=0, rtol=1e-7,
                strict=True,
            )
        if mode != "data":
            assert table.getcoldesc("CORRECTED_DATA")["valueType"] == "complex"
            corrected = SMOOTHED_CORRECTED if mode == "all" else SMOOTHED
            np.testing.assert_array_equal(
                table.getcell("CORRECTED_DATA", 0), expected_visibilities(corrected), strict=True,
            )
        if mode == "corrected":
            np.testing.assert_array_equal(
                table.getcell("MODEL_DATA", 0), np.ones((2, 4), dtype=np.complex128), strict=True,
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
    np.testing.assert_array_equal(table.getcell("DATA", 0), expected_visibilities(SMOOTHED), strict=True)
    assert "CORRECTED_DATA" not in table.colnames()
    assert table.done()


@pytest.mark.parametrize("kind", ["reference", "multims"])
def test_hanningsmooth_persists_through_a_saved_view(native_dataset, tmp_path, writer_api, kind):
    casa = pytest.importorskip("casatools")
    write_input(casa, native_dataset)
    target = tmp_path / "view.ms"
    tool = casa.ms()
    table = casa.table()
    if kind == "reference":
        assert table.open(str(native_dataset))
        view = table.selectrows(np.array([2, 0, 2], dtype=np.int64), str(target))
        assert view.done()
        assert table.done()
        parts = (str(native_dataset),)
    else:
        peer = tmp_path / "other.ms"
        copytree(native_dataset, peer)
        assert tool.createmultims(
            str(target), list(map(str, (native_dataset, peer))), ["ANTENNA", "DATA_DESCRIPTION", "FEED"]
        )
        assert tool.done()
        assert tool.open(str(target))
        parts = tool.getreferencedtables()
        assert tool.done()

    tool = writer_api.ms()
    assert tool.open(str(target), nomodify=False)
    assert tool.hanningsmooth("data")
    assert tool.done()
    for part in parts:
        for api in (casa, tools):
            t = api.table()
            assert t.open(part)
            for row in range(t.nrows()):
                smooth = kind == "multims" or row in (0, 2)
                expected = expected_visibilities(SMOOTHED if smooth else OBSERVED)
                np.testing.assert_array_equal(t.getcell("DATA", row), expected, strict=True)
                expected_weight = np.float32(8 / 3) if smooth else 1.0
                np.testing.assert_array_equal(t.getcell("WEIGHT", row), np.full(2, expected_weight, dtype=np.float64), strict=True)
            assert t.done()


def test_hanningsmooth_preserves_existing_model_when_adding_corrected(native_dataset):
    casa = pytest.importorskip("casatools")
    write_input(casa, native_dataset)
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    assert table.addcols({"MODEL_DATA": {"valueType": "complex", "ndim": 2}})
    model = np.full((2, 4), 42j, dtype=np.complex64)
    for row in range(table.nrows()):
        assert table.putcell("MODEL_DATA", row, model)
    assert table.done()
    tool = tools.ms()
    assert tool.open(str(native_dataset), nomodify=False)
    assert tool.hanningsmooth()
    assert tool.done()
    assert table.open(str(native_dataset))
    np.testing.assert_array_equal(table.getcell("MODEL_DATA", 0), model.astype(np.complex128), strict=True)
    np.testing.assert_array_equal(table.getcell("CORRECTED_DATA", 0), expected_visibilities(SMOOTHED), strict=True)
    assert table.done()


def test_hanningsmooth_updates_the_physical_column_in_a_renamed_view(
    native_dataset, tmp_path, writer_api,
):
    casa = pytest.importorskip("casatools")
    write_input(casa, native_dataset, corrected=True)
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    columns = [name for name in table.colnames() if name not in ("DATA", "CORRECTED_DATA")]
    columns.append("CORRECTED_DATA AS DATA")
    path = str(tmp_path / "alias.ms")
    view = table.query("", name=path, columns=", ".join(columns))
    assert view.done()
    assert table.done()
    tool = writer_api.ms()
    assert tool.open(path, nomodify=False)
    assert tool.hanningsmooth("data")
    assert tool.done()
    for api in (casa, tools):
        table = api.table()
        assert table.open(str(native_dataset))
        np.testing.assert_array_equal(
            table.getcell("DATA", 0), expected_visibilities(OBSERVED), strict=True,
        )
        np.testing.assert_array_equal(
            table.getcell("CORRECTED_DATA", 0),
            expected_visibilities(SMOOTHED_CORRECTED),
            strict=True,
        )
        assert table.done()


def test_hanningsmooth_creates_scratch_only_for_selected_rows(
    native_dataset, tmp_path, writer_api,
):
    casa = pytest.importorskip("casatools")
    write_input(casa, native_dataset)
    table = casa.table()
    assert table.open(str(native_dataset))
    path = str(tmp_path / "subset.ms")
    view = table.selectrows(np.array([2, 0, 2], dtype=np.int64), path)
    assert view.done()
    assert table.done()
    tool = writer_api.ms()
    assert tool.open(path, nomodify=False)
    assert tool.hanningsmooth()
    assert tool.done()
    for api in (casa, tools):
        table = api.table()
        assert table.open(str(native_dataset))
        for row in range(table.nrows()):
            selected = row in (0, 2)
            for column in ("CORRECTED_DATA", "MODEL_DATA"):
                assert table.iscelldefined(column, row) == selected
            if selected:
                np.testing.assert_array_equal(
                    table.getcell("CORRECTED_DATA", row),
                    expected_visibilities(SMOOTHED),
                    strict=True,
                )
                np.testing.assert_array_equal(
                    table.getcell("MODEL_DATA", row),
                    np.ones((2, 4), dtype=np.complex128),
                    strict=True,
                )
        assert table.done()


@pytest.mark.parametrize("rows", [[1, 0, 1], [0, 1, 0]])
def test_hanningsmooth_checks_spectrum_at_first_logical_row(
    native_dataset, tmp_path, writer_api, rows,
):
    casa = pytest.importorskip("casatools")
    write_input(casa, native_dataset)
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    assert table.addcols({"WEIGHT_SPECTRUM": {"valueType": "float", "ndim": 2}})
    assert table.putcell("WEIGHT_SPECTRUM", 1, SPECTRUM)
    path = str(tmp_path / "spectrum.ms")
    view = table.selectrows(np.array(rows, dtype=np.int64), path)
    assert view.done()
    assert table.done()

    tool = writer_api.ms()
    assert tool.open(path, nomodify=False)
    defined_first = rows[0] == 1
    if defined_first:
        with pytest.raises(RuntimeError, match="WEIGHT_SPECTRUM"):
            tool.hanningsmooth("data")
    else:
        assert tool.hanningsmooth("data")
    assert tool.done()
    for api in (casa, tools):
        table = api.table()
        assert table.open(str(native_dataset))
        expected = 1.0 if defined_first else np.float32(8 / 3)
        for row in (0, 1):
            np.testing.assert_array_equal(
                table.getcell("WEIGHT", row), np.full(2, expected, dtype=np.float64), strict=True,
            )
        np.testing.assert_array_equal(
            table.getcell("DATA", 0),
            expected_visibilities(OBSERVED if defined_first else SMOOTHED),
            strict=True,
        )
        np.testing.assert_array_equal(
            table.getcell("WEIGHT_SPECTRUM", 1), SPECTRUM.astype(np.float64), strict=True,
        )
        assert table.done()


def test_hanningsmooth_writes_float_data(native_dataset, writer_api):
    casa = pytest.importorskip("casatools")
    write_input(casa, native_dataset)
    table = casa.table()
    assert table.open(str(native_dataset), nomodify=False)
    values = table.getcol("DATA").real.copy()
    assert table.addcols({"FLOAT_DATA": {"valueType": "float", "ndim": 2}})
    for row in range(table.nrows()):
        assert table.putcell("FLOAT_DATA", row, values[:, :, row].astype(np.float32))
    assert table.removecols("DATA")
    assert table.done()
    tool = writer_api.ms()
    assert tool.open(str(native_dataset), nomodify=False)
    assert tool.hanningsmooth("data")
    assert tool.done()
    for api in (casa, tools):
        table = api.table()
        assert table.open(str(native_dataset))
        assert table.getcoldesc("FLOAT_DATA")["valueType"] == "float"
        np.testing.assert_array_equal(
            table.getcell("FLOAT_DATA", 0),
            np.broadcast_to(SMOOTHED.real.astype(np.float64), (2, 4)),
            strict=True,
        )
        assert table.done()


def test_hanningsmooth_writes_compressed_corrected_cells(native_dataset, writer_api):
    casa = pytest.importorskip("casatools")
    write_input(casa, native_dataset)
    calibrater = casa.calibrater()
    assert calibrater.open(str(native_dataset), compress=True, addcorr=True, addmodel=False)
    assert calibrater.close()
    table = casa.table()
    assert table.open(str(native_dataset))
    raw_before = table.getcell("CORRECTED_DATA_COMPRESSED", 0)
    assert table.done()

    tool = writer_api.ms()
    assert tool.open(str(native_dataset), nomodify=False)
    assert tool.hanningsmooth("corrected")
    assert tool.done()
    for api in (casa, tools):
        table = api.table()
        assert table.open(str(native_dataset))
        np.testing.assert_array_equal(
            table.getcell("DATA", 0), expected_visibilities(OBSERVED), strict=True,
        )
        np.testing.assert_allclose(
            table.getcell("CORRECTED_DATA", 0), expected_visibilities(SMOOTHED),
            rtol=0, atol=3e-4, strict=True,
        )
        raw = table.getcell("CORRECTED_DATA_COMPRESSED", 0)
        assert raw.shape == raw_before.shape and raw.dtype == raw_before.dtype
        assert not np.array_equal(raw, raw_before)
        assert table.done()


def test_hanningsmooth_leaves_two_channel_data_unchanged(native_dataset, tmp_path, writer_api):
    casa = pytest.importorskip("casatools")
    write_input(casa, native_dataset)
    path = str(tmp_path / "short.ms")
    source = casa.ms()
    assert source.open(str(native_dataset))
    assert source.split(path, spw="0:0~1")
    assert source.done()
    columns = ("DATA", "FLAG", "WEIGHT")
    table = casa.table()
    assert table.open(path)
    before = [table.getcol(name) for name in columns]
    assert before[0].shape[1] == 2
    assert table.done()
    tool = writer_api.ms()
    assert tool.open(path, nomodify=False)
    assert tool.hanningsmooth("data")
    assert tool.done()
    for api in (casa, tools):
        table = api.table()
        assert table.open(path)
        for name, values in zip(columns, before):
            np.testing.assert_array_equal(table.getcol(name), values, strict=True)
        assert table.done()


def test_hanningsmooth_initializes_full_polarization_model(tmp_path, measures, writer_api):
    casa = pytest.importorskip("casatools")
    path = str(tmp_path / "four-polarizations.ms")
    simulator = tools.simulator()
    assert simulator.open(path)
    configure(simulator, stokes="RR RL LR LL")
    assert simulator.observe("F", "L", starttime="0s", stoptime="30s")
    assert simulator.close()
    write_input(casa, path)

    tool = writer_api.ms()
    assert tool.open(path, nomodify=False)
    assert tool.hanningsmooth()
    assert tool.done()
    for api in (casa, tools):
        table = api.table()
        assert table.open(path)
        model = np.broadcast_to(
            np.array([1, 0, 0, 1], dtype=np.complex128)[:, None], (4, 4),
        )
        np.testing.assert_array_equal(table.getcell("MODEL_DATA", 0), model, strict=True)
        np.testing.assert_array_equal(
            table.getcell("CORRECTED_DATA", 0),
            np.broadcast_to(SMOOTHED.astype(np.complex128), (4, 4)),
            strict=True,
        )
        assert table.done()
