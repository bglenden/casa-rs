"""FITS-IDI tool behaviour with the upstream e-MERLIN and VLBA fixtures."""

from contextlib import closing
import os
from pathlib import Path
import struct

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_fits import fits_binary_header, fits_header, read


FIXTURES = [
    ("emerlin_multiuv.IDI1", 504, (1, 512), 4),
    ("n09q2_1_1-shortened.IDI1", 8496, (1, 16), 8),
    ("VLBA_TL015A_tl015arecor_BIN0_SRC0_1_201020T164655.idifits", 15211, (4, 256), 1),
]


def idi_fixture(name):
    root = os.environ.get("CASA_RS_TESTDATA_ROOT")
    if not root:
        pytest.skip("CASA test data directory is unavailable")
    path = Path(root) / "unittest" / "importfitsidi" / name
    if not path.is_file():
        pytest.skip(f"Missing upstream FITS-IDI fixture {name}")
    return path


@pytest.mark.parametrize("name,rows,cell,windows", FIXTURES)
def test_fromfitsidi_stock_exchange(name, rows, cell, windows, tmp_path):
    casa = pinned_casa()
    input_path = idi_fixture(name)
    left = tmp_path / "casa.ms"
    right = tmp_path / "native.ms"
    for api, path in ((casa, left), (tools, right)):
        with closing(api.ms()) as importer:
            assert importer.fromfitsidi(str(path), str(input_path), nomodify=False,
                                         lock=True, obstype=1)
            assert importer.name() == str(path)
            assert importer.nrow() == rows and importer.iswritable()
    residual = energy = 0.0
    with closing(casa.table()) as ref, closing(casa.table()) as cpp, closing(tools.table()) as native:
        assert ref.open(str(left)) and cpp.open(str(right)) and native.open(str(left))
        assert ref.nrows() == cpp.nrows() == native.nrows() == rows
        assert set(ref.colnames()) == set(cpp.colnames()) == set(native.colnames())
        columns = ["DATA", "FLAG", "FLAG_CATEGORY", "FLAG_ROW", "WEIGHT", "SIGMA", "UVW",
                   "ANTENNA1", "ANTENNA2", "ARRAY_ID", "DATA_DESC_ID", "FIELD_ID", "TIME",
                   "TIME_CENTROID", "INTERVAL", "EXPOSURE", "SCAN_NUMBER"]
        if "WEIGHT_SPECTRUM" in ref.colnames():
            columns.extend(["WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"])
        assert ref.getcell("DATA", 0).shape == cell
        assert ref.getcell("FLAG_CATEGORY", 0).shape == (*cell, 3)
        for start in range(0, rows, 512):
            count = min(512, rows - start)
            for column in columns:
                expected = ref.getcol(column, start, count)
                actual = cpp.getcol(column, start, count)
                np.testing.assert_array_equal(native.getcol(column, start, count), expected,
                                              strict=True, err_msg=column)
                assert expected.shape == actual.shape and expected.dtype == actual.dtype
                if column == "DATA":
                    difference = actual - expected
                    residual += np.vdot(difference, difference).real
                    energy += np.vdot(expected, expected).real
                else:
                    np.testing.assert_array_equal(actual, expected, strict=True, err_msg=column)
    assert (residual / energy) ** 0.5 < 1e-6
    for subtable, columns in [
        ("ANTENNA", ("NAME", "POSITION", "MOUNT")),
        ("SPECTRAL_WINDOW", ("CHAN_FREQ", "MEAS_FREQ_REF", "NUM_CHAN")),
        ("FEED", ("ANTENNA_ID", "RECEPTOR_ANGLE", "TIME", "SPECTRAL_WINDOW_ID")),
        ("FIELD", ("NAME", "PHASE_DIR", "SOURCE_ID")),
        ("POLARIZATION", ("CORR_TYPE", "CORR_PRODUCT")),
        ("OBSERVATION", ("TIME_RANGE", "TELESCOPE_NAME")),
    ]:
        baseline = read(casa, left / subtable, columns)
        result = read(casa, right / subtable, columns)
        for column, expected in baseline.items():
            if column == "PHASE_DIR":
                np.testing.assert_allclose(result[column], expected, atol=2e-15, rtol=0, strict=True)
            else:
                np.testing.assert_array_equal(result[column], expected, strict=True, err_msg=column)
    assert read(casa, right / "SPECTRAL_WINDOW", ["NUM_CHAN"])["NUM_CHAN"].size == windows
    if rows == 15211:
        compare_vlba_calibration(casa, left, right)


def compare_vlba_calibration(casa, reference, output):
    for subtable, columns in [
        ("SYSCAL", ("TIME", "INTERVAL", "TSYS", "ANTENNA_ID", "SPECTRAL_WINDOW_ID")),
        ("WEATHER", ("TIME", "INTERVAL", "TEMPERATURE", "DEW_POINT", "PRESSURE", "WIND_DIRECTION", "WIND_SPEED")),
        ("FLAG_CMD", ("COMMAND", "REASON", "SEVERITY", "APPLIED")),
        ("GAIN_CURVE", ("TIME", "INTERVAL", "GAIN", "SENSITIVITY", "NUM_POLY", "TYPE")),
        ("EARTH_ORIENTATION", ("TIME", "UT1_UTC", "PM", "TYPE")),
    ]:
        lhs = read(casa, reference / subtable, columns)
        rhs = read(casa, output / subtable, columns)
        for column, expected in lhs.items():
            if column == "PM":
                np.testing.assert_allclose(rhs[column], expected, atol=2e-21, rtol=0, strict=True)
            else:
                np.testing.assert_array_equal(rhs[column], expected, strict=True, err_msg=column)
    with closing(casa.table()) as table:
        assert table.open(str(output / "WEATHER"))
        assert not ({"H2O", "IONOS_ELECTRON"} & set(table.colnames()))


def test_fromfitsidi_replacement_and_failed_input_preserve_state(tmp_path):
    casa = pinned_casa()
    source = idi_fixture(FIXTURES[0][0])
    second = idi_fixture(FIXTURES[1][0])
    output = tmp_path / "result.ms"
    with closing(tools.ms()) as importer:
        assert importer.fromfitsidi(str(output), str(source))
        assert not importer.iswritable()
        with pytest.raises(RuntimeError):
            importer.fromfitsidi(str(output), str(second))
        assert importer.name() == str(output) and importer.nrow() == 504
        assert importer.close()
        invalid = tmp_path / "invalid.idi"
        invalid.write_bytes(source.read_bytes()[:2880])
        with pytest.raises(RuntimeError):
            importer.fromfitsidi(str(output), str(invalid), nomodify=False)
        assert read(casa, output, ["TIME"])["TIME"].size == 504
        assert importer.fromfitsidi(str(output), str(second), nomodify=False)
        assert importer.nrow() == 8496 and importer.iswritable()
    assert read(casa, output, ["TIME"])["TIME"].size == 8496


def test_fromfitsidi_phase_calibration_subtable(tmp_path):
    casa = pinned_casa()
    fixture = idi_fixture(FIXTURES[0][0])
    source = tmp_path / "phase.fits"
    definitions = [("TIME", "1D"), ("TIME_INTERVAL", "1E"), ("ANTENNA_NO", "1J"),
                   ("ARRAY", "1J"), ("FREQID", "1J"), ("CABLE_CAL", "1D")]
    wire = struct.pack(">df3id", 0.5, 1 / 24, 2, 1, 1, 1e-6)
    for hand in (1, 2):
        definitions.extend([(f"PC_FREQ_{hand}", "8D"), (f"PC_REAL_{hand}", "8E"),
                            (f"PC_IMAG_{hand}", "8E")])
        wire += struct.pack(">8d16f", *(np.arange(8) + hand * 100),
                            *(np.arange(8) + hand * 10), *(-np.arange(8) - hand * 5))
    header = fits_binary_header("PHASE-CAL", len(wire), 1, definitions)
    header.extend([("NO_BAND", 4), ("NO_TONES", 2)])
    source.write_bytes(fixture.read_bytes() + fits_header(header) + wire.ljust(2880, b"\0"))
    columns = ("ANTENNA_ID", "FEED_ID", "SPECTRAL_WINDOW_ID", "TIME", "INTERVAL", "NUM_TONES",
               "TONE_FREQUENCY", "PHASE_CAL", "CABLE_CAL")
    results = []
    for api, engine in ((casa, "casa"), (tools, "native")):
        target = tmp_path / f"{engine}.ms"
        with closing(api.ms()) as importer:
            assert importer.fromfitsidi(str(target), str(source))
        results.append(read(casa, target / "PHASE_CAL", columns))
    for column, expected in results[0].items():
        np.testing.assert_array_equal(results[1][column], expected, strict=True, err_msg=column)
    np.testing.assert_array_equal(results[1]["TONE_FREQUENCY"][..., 0], [[100, 101], [200, 201]])
