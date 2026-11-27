"""FITS-IDI tool behaviour with the upstream e-MERLIN and VLBA fixtures."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from math import prod
import os
from pathlib import Path
import struct
import time

import numpy as np
import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_ms_fits import fits_binary_header, fits_columns, fits_extension, fits_header, read


FIXTURES = [
    ("emerlin_multiuv.IDI1", 504, (1, 512), 4),
    ("n09q2_1_1-shortened.IDI1", 8496, (1, 16), 8),
    ("VLBA_TL015A_tl015arecor_BIN0_SRC0_1_201020T164655.idifits", 15211, (4, 256), 1),
    ("VLBA_VSN000397_file69.uvfits", 79000, (1, 16), 4),
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
        for reader in (ref, cpp, native):
            assert reader.getcolkeyword("UVW", "MEASINFO") == {"Ref": "ITRF", "type": "uvw"}
        columns = ["DATA", "FLAG", "FLAG_CATEGORY", "FLAG_ROW", "WEIGHT", "SIGMA", "UVW",
                   "ANTENNA1", "ANTENNA2", "ARRAY_ID", "DATA_DESC_ID", "FIELD_ID", "TIME",
                   "TIME_CENTROID", "INTERVAL", "EXPOSURE", "SCAN_NUMBER"]
        if "WEIGHT_SPECTRUM" in ref.colnames():
            columns.extend(["WEIGHT_SPECTRUM", "SIGMA_SPECTRUM"])
        for column in columns:
            assert ref.coldatatype(column) == cpp.coldatatype(column) == native.coldatatype(column)
        assert ref.getcell("DATA", 0).shape == cell
        assert ref.getcell("FLAG_CATEGORY", 0).shape == (*cell, 3)
        block_rows = min(4096, 524288 // prod(cell))
        for start in range(0, rows, block_rows):
            count = min(block_rows, rows - start)
            for column in columns:
                expected = ref.getcol(column, start, count)
                actual = cpp.getcol(column, start, count)
                np.testing.assert_array_equal(native.getcol(column, start, count), expected,
                                              strict=True, err_msg=column)
                assert expected.shape == actual.shape and expected.dtype == actual.dtype
                if column == "DATA":
                    finite = np.isfinite(expected)
                    np.testing.assert_array_equal(np.isfinite(actual), finite)
                    np.testing.assert_array_equal(actual.real[~finite], expected.real[~finite])
                    np.testing.assert_array_equal(actual.imag[~finite], expected.imag[~finite])
                    difference = actual[finite] - expected[finite]
                    residual += np.vdot(difference, difference).real
                    energy += np.vdot(expected[finite], expected[finite]).real
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
        with pytest.raises(RuntimeError, match="still open"):
            importer.fromfitsidi(str(output), str(second), nomodify=False)
        assert importer.name() == str(output) and importer.nrow() == 504
        assert importer.close()
        for factory in (tools.ms, tools.table):
            with closing(factory()) as holder:
                assert holder.open(str(output))
                with pytest.raises(RuntimeError, match="still open"):
                    importer.fromfitsidi(str(output), str(second), nomodify=False)
        invalid = tmp_path / "invalid.idi"
        invalid.write_bytes(source.read_bytes()[:2880])
        with pytest.raises(RuntimeError):
            importer.fromfitsidi(str(output), str(invalid), nomodify=False)
        assert read(casa, output, ["TIME"])["TIME"].size == 504
        assert importer.fromfitsidi(str(output), str(second), nomodify=False)
        assert importer.nrow() == 8496 and importer.iswritable()
    assert read(casa, output, ["TIME"])["TIME"].size == 8496


def test_fromfitsidi_refuses_to_replace_an_unrelated_directory(tmp_path):
    source = idi_fixture(FIXTURES[0][0])
    output = tmp_path / "unrelated.ms"
    output.mkdir()
    (output / "table.dat").write_text("not a casacore table")
    (output / "important.txt").write_text("keep this")
    with closing(tools.ms()) as importer, pytest.raises(RuntimeError):
        importer.fromfitsidi(str(output), str(source), nomodify=False)
    assert (output / "table.dat").read_text() == "not a casacore table"
    assert (output / "important.txt").read_text() == "keep this"


@pytest.mark.parametrize("already_present", [False, True])
def test_fromfitsidi_protects_a_handle_opened_while_staging(tmp_path, already_present):
    source = idi_fixture(FIXTURES[0][0])
    larger = idi_fixture(FIXTURES[3][0])
    output = tmp_path / "result.ms"
    original = output if already_present else tmp_path / "pending.ms"
    with closing(tools.ms()) as importer:
        assert importer.fromfitsidi(str(original), str(source))
    with ThreadPoolExecutor(max_workers=1) as executor, closing(tools.ms()) as holder:
        def replace():
            with closing(tools.ms()) as importer:
                return importer.fromfitsidi(str(output), str(larger), nomodify=False)

        conversion = executor.submit(replace)
        deadline = time.monotonic() + 10
        while not any(tmp_path.glob(".fits-idi-*/visibility.ms")):
            assert not conversion.done() and time.monotonic() < deadline
            time.sleep(0.001)
        if not already_present:
            original.rename(output)
        assert holder.open(str(output)) and holder.nrow() == 504
        with pytest.raises(RuntimeError, match="still open"):
            conversion.result(timeout=30)
        assert holder.nrow() == 504


def replace_card(raw, position, name, value):
    if isinstance(value, str):
        card = f"{name:8}= '{value:<8}'".encode().ljust(80)
    else:
        card = fits_header([(name, value)])[:80]
    raw[position:position + 80] = card


@pytest.mark.parametrize("problem", ["frequency shape", "flux shape", "source id"])
def test_fromfitsidi_rejects_inconsistent_metadata(problem, tmp_path):
    stock = idi_fixture(FIXTURES[0][0])
    raw = bytearray(stock.read_bytes())
    if problem == "frequency shape":
        cards, _ = fits_extension(raw, "FREQUENCY")
        replace_card(raw, cards["NO_CHAN"][1], "NO_CHAN", 511)
        message = "FREQUENCY and UV_DATA shapes disagree"
    elif problem == "flux shape":
        cards, _ = fits_extension(raw, "UV_DATA")
        replace_card(raw, cards["MAXIS3"][1], "MAXIS3", 511)
        message = "FLUX repeat"
    else:
        cards, start = fits_extension(raw, "UV_DATA")
        source, _ = fits_columns(cards, start)["SOURCE_ID"]
        struct.pack_into(">i", raw, source, 311)
        message = "SOURCE_ID has no matching FIELD row"
    damaged = tmp_path / "damaged.fits"
    damaged.write_bytes(raw)
    output = tmp_path / "result.ms"
    with closing(tools.ms()) as importer:
        assert importer.fromfitsidi(str(output), str(stock))
        assert importer.close()
        with pytest.raises(RuntimeError, match=message):
            importer.fromfitsidi(str(output), str(damaged), nomodify=False)
        assert importer.open(str(output)) and importer.nrow() == 504


def two_channel_idi(no_interval):
    raw = bytearray(idi_fixture(FIXTURES[0][0]).read_bytes())
    fq, _ = fits_extension(raw, "FREQUENCY")
    replace_card(raw, fq["NO_CHAN"][1], "NO_CHAN", 2)
    uv, start = fits_extension(raw, "UV_DATA")
    columns = fits_columns(uv, start)
    flux, form = columns["FLUX"]
    row_size = int(uv["NAXIS1"][0])
    # Preserve the original row's scalar metadata and substitute two channels
    # per band with unequal weights in the three-component representation.
    rows = bytearray()
    for index in range(2):
        row = raw[start:flux] + struct.pack(">24f", *([3, 2, 1, 5, 7, 9] * 4))
        row.extend(raw[flux + 4096 * 4:start + row_size])
        time_offset = columns["TIME"][0] - start
        initial_time, = struct.unpack_from(">d", row, time_offset)
        struct.pack_into(">d", row, time_offset, initial_time + index * 4.0 / 86400.0)
        rows.extend(row)
    replace_card(raw, form, "TFORM11", "24E")
    replace_card(raw, uv["TDIM11"][1], "TDIM11", "(3,1,2,4,1,1)")
    replace_card(raw, uv["NAXIS1"][1], "NAXIS1", len(rows) // 2)
    replace_card(raw, uv["NAXIS2"][1], "NAXIS2", 2)
    replace_card(raw, uv["MAXIS1"][1], "MAXIS1", 3)
    replace_card(raw, uv["MAXIS3"][1], "MAXIS3", 2)
    replace_card(raw, uv["CRVAL2"][1], "CRVAL2", 1.0)
    if no_interval:
        for name, (value, position) in uv.items():
            if name.startswith("TTYPE") and value == "INTTIM":
                replace_card(raw, position, name, "UNUSED")
                break
    return raw[:start] + rows.ljust(len(rows) + (-len(rows) % 2880), b"\0")


@pytest.mark.parametrize("no_interval", [False, True])
def test_fromfitsidi_two_channel_correlation_weight_and_interval(no_interval, tmp_path):
    casa = pinned_casa()
    source = tmp_path / "narrow.fits"
    source.write_bytes(two_channel_idi(no_interval))
    paths = [tmp_path / "casa.ms", tmp_path / "native.ms"]
    for api, output in zip((casa, tools), paths, strict=True):
        with closing(api.ms()) as importer:
            assert importer.fromfitsidi(str(output), str(source))
            assert importer.nrow() == 8
    we, wn = (read(casa, path, ("WEIGHT_SPECTRUM", "SIGMA_SPECTRUM", "WEIGHT", "SIGMA",
                              "INTERVAL", "EXPOSURE", "TIME", "TIME_CENTROID")) for path in paths)
    for path in paths:
        pol = read(casa, path / "POLARIZATION", ("CORR_TYPE", "CORR_PRODUCT"))
        np.testing.assert_array_equal(pol["CORR_TYPE"], [[9]])
        np.testing.assert_array_equal(pol["CORR_PRODUCT"], [[[0]], [[0]]])
    if no_interval:
        np.testing.assert_allclose(wn["INTERVAL"], 4.0, atol=1e-5, rtol=0)
        np.testing.assert_array_equal(wn["EXPOSURE"], wn["INTERVAL"])
        np.testing.assert_array_equal(wn["TIME_CENTROID"], wn["TIME"] + wn["INTERVAL"] / 2)
        np.testing.assert_array_equal(wn["WEIGHT"][..., :4], wn["WEIGHT"][..., 4:])
        assert we["TIME_CENTROID"][0] - we["TIME"][0] == 0.5
    else:
        for name in wn:
            np.testing.assert_array_equal(wn[name], we[name], strict=True, err_msg=name)
    np.testing.assert_array_equal(wn["WEIGHT"], wn["WEIGHT_SPECTRUM"][:, 0, :])
    np.testing.assert_array_equal(wn["SIGMA"], wn["SIGMA_SPECTRUM"][:, 1, :])


def test_fromfitsidi_converts_each_source_to_the_field_reference(tmp_path):
    casa = pinned_casa()
    raw = bytearray(idi_fixture(FIXTURES[1][0]).read_bytes())
    source_cards, first = fits_extension(raw, "SOURCE")
    equinox, _ = fits_columns(source_cards, first)["EQUINOX"]
    raw[equinox:equinox + 8] = b"J2000   "
    second = equinox + int(source_cards["NAXIS1"][0])
    raw[second:second + 8] = b"1950.0B "
    source = tmp_path / "mixed-source.fits"
    source.write_bytes(raw)
    outputs = [tmp_path / "casa.ms", tmp_path / "native.ms"]
    for api, path in zip((casa, tools), outputs, strict=True):
        with closing(api.ms()) as importer:
            assert importer.fromfitsidi(str(path), str(source))
    with closing(casa.table()) as ref, closing(casa.table()) as native:
        assert ref.open(str(outputs[0] / "FIELD")) and native.open(str(outputs[1] / "FIELD"))
        for column in ("DELAY_DIR", "PHASE_DIR", "REFERENCE_DIR"):
            assert ref.getcolkeyword(column, "MEASINFO") == native.getcolkeyword(column, "MEASINFO")
            np.testing.assert_allclose(native.getcol(column), ref.getcol(column), atol=5e-8, rtol=0)


def test_fromfitsidi_weather_columns_and_flag_channel_range(tmp_path):
    casa = pinned_casa()
    fixture = idi_fixture(FIXTURES[0][0])
    source = tmp_path / "auxiliary.fits"
    weather_columns = [
        ("TIME", "1D"), ("TIME_INTERVAL", "1E"), ("ANTENNA_NO", "1J"),
        ("DEWPOINT", "1E"), ("TEMPERATURE", "1E"), ("PRESSURE", "1E"),
        ("WIND_VELOCITY", "1E"), ("WIND_DIRECTION", "1E"), ("WVR_H2O", "1E"),
        ("IONOS_ELECTRON", "1E"),
    ]
    weather = struct.pack(">dfi7f", 0.5, 1 / 24, 2, 7, 13, 1013, 4, 90, 1.25, 2.5)
    flag_columns = [
        ("SOURCE_ID", "1J"), ("ARRAY", "1J"), ("ANTS", "2J"), ("FREQID", "1J"),
        ("TIMERANG", "2E"), ("BANDS", "4J"), ("CHANS", "2J"), ("PFLAGS", "4J"),
        ("REASON", "16A"), ("SEVERITY", "1J"),
    ]
    flag = struct.pack(">5i2f10i16si", 0, 0, 0, 0, 1, 0.4, 0.5, 1, 1, 1, 1, 2, 4,
                       1, 1, 1, 1, b"channel range   ", 3)
    flag_hdu = fits_binary_header("FLAG", len(flag), 1, flag_columns)
    flag_hdu.extend([("NO_BAND", 4), ("NO_STKD", 1), ("STK_1", -1)])
    source.write_bytes(
        fixture.read_bytes()
        + fits_header(fits_binary_header("WEATHER", len(weather), 1, weather_columns))
        + weather.ljust(2880, b"\0")
        + fits_header(flag_hdu)
        + flag.ljust(2880, b"\0")
    )
    output = tmp_path / "auxiliary.ms"
    reference = tmp_path / "reference.ms"
    with closing(tools.ms()) as importer:
        assert importer.fromfitsidi(str(output), str(source))
    with closing(casa.ms()) as importer:
        assert importer.fromfitsidi(str(reference), str(source))
    expected = read(casa, reference / "WEATHER", ("H2O", "IONOS_ELECTRON"))
    result = read(casa, output / "WEATHER", ("H2O", "IONOS_ELECTRON"))
    np.testing.assert_array_equal(result["H2O"], np.array([1.25], dtype=expected["H2O"].dtype),
                                  strict=True)
    np.testing.assert_array_equal(expected["H2O"], expected["IONOS_ELECTRON"], strict=True)
    np.testing.assert_array_equal(
        result["IONOS_ELECTRON"], expected["IONOS_ELECTRON"], strict=True
    )
    assert read(casa, reference / "FLAG_CMD", ("COMMAND",))["COMMAND"][-1] == "spw='*:1~3''"
    command = read(casa, output / "FLAG_CMD", ("COMMAND",))["COMMAND"][-1]
    assert command == "spw='0:1~3,1:1~3,2:1~3,3:1~3'"
    with closing(casa.ms()) as selector:
        indices = selector.msseltoindex(vis=str(output), spw=command[5:-1])
    np.testing.assert_array_equal(indices["channel"], [[window, 1, 3, 1] for window in range(4)])


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
