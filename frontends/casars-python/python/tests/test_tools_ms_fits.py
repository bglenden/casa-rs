"""Visibility UVFITS exchange against the pinned CASA readers and writers."""

from contextlib import closing
import os
from pathlib import Path
from shutil import copytree
import struct

import numpy as np
import pytest

from casars import tools

from test_tools_ms import pinned_casa


@pytest.fixture
def fits_stock():
    root = os.environ.get("CASA_RS_TESTDATA_ROOT")
    if not root:
        pytest.skip("CASA MS FITS test data directory is unavailable")
    path = Path(root) / "unittest" / "mstool"
    if not (path / "uvfits_test.ms").is_dir():
        pytest.skip("CASA MS FITS stock fixture is unavailable")
    return path


def read(api, path, names):
    with closing(api.table()) as table:
        assert table.open(str(path))
        return {column: table.getcol(column) for column in names}


MAIN = ("DATA", "FLAG", "FLAG_ROW", "WEIGHT", "WEIGHT_SPECTRUM", "SIGMA", "UVW",
        "ANTENNA1", "ANTENNA2", "ARRAY_ID", "DATA_DESC_ID", "FIELD_ID", "TIME", "EXPOSURE")


def compare(reference, actual, *, comparison="stock-import"):
    for name, expected in reference.items():
        value = actual[name]
        assert value.shape == expected.shape and value.dtype == expected.dtype
        if name == "FLAG_ROW" and comparison == "import-pinned-export":
            # The signed per-sample weights determine whether an entire row
            # is flagged. Pinned CASA loses that state on some initial rows.
            np.testing.assert_array_equal(value, actual["FLAG"].all(axis=(0, 1)), strict=True)
        elif name in ("WEIGHT", "SIGMA"):
            np.testing.assert_allclose(value, expected, rtol=2e-6, atol=0, strict=True, err_msg=name)
        else:
            np.testing.assert_array_equal(value, expected, strict=True, err_msg=name)


@pytest.mark.parametrize("fixture", ["3c273.fits7", "CTR_CHI_TR2.RWYCP-10rows-ANT-DIAMTER-0",
                                    "name10rows+000"])
def test_fromfits_stock_exchange(fits_stock, fixture, tmp_path):
    casa = pinned_casa()
    source = fits_stock / fixture
    if not source.is_file():
        pytest.skip(f"Missing upstream FITS fixture {fixture}")
    results = []
    for api, engine in ((casa, "casa"), (tools, "native")):
        location = tmp_path / f"{engine}.ms"
        with closing(api.ms()) as converter:
            assert converter.fromfits(str(location), str(source), obstype=1, nomodify=False,
                                      lock=True, antnamescheme="NeW")
            assert converter.name() == str(location) and converter.iswritable()
            assert converter.nrow() > 0
        payload = read(casa, location, MAIN)
        for name, values in read(tools, location, MAIN).items():
            np.testing.assert_array_equal(values, payload[name], strict=True)
        results.append(payload)
    compare(*results)
    fields = ("PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR", "SOURCE_ID", "NAME")
    reference = read(casa, tmp_path / "casa.ms" / "FIELD", fields)
    native = read(casa, tmp_path / "native.ms" / "FIELD", fields)
    for name, expected in reference.items():
        if name.endswith("_DIR"):
            np.testing.assert_allclose(native[name], expected, atol=2e-15, rtol=0, strict=True)
        else:
            np.testing.assert_array_equal(native[name], expected, strict=True)
    antennas = ("NAME", "STATION", "POSITION", "OFFSET", "DISH_DIAMETER")
    compare(read(casa, tmp_path / "casa.ms" / "ANTENNA", antennas),
            read(casa, tmp_path / "native.ms" / "ANTENNA", antennas))


@pytest.mark.parametrize("selection", [
    {},
    {"spw": "1:1~2", "multisource": True},
    {"spw": [1], "baseline": "1", "writesyscal": True},
    {"combinespw": True},
    {"combinespw": True, "padwithflags": True},
    {"field": [0], "taql": "ANTENNA1 == 0", "scan": [5], "time": ">2001/01/01"},
    {"taql": "ANTENNA1 == 0", "uvrange": "0~1000m", "writestation": True},
])
def test_tofits_stock_exchange(fits_stock, tmp_path, selection):
    casa = pinned_casa()
    source = fits_stock / "uvfits_test.ms"
    original = read(casa, source, ("DATA", "FLAG", "WEIGHT", "SIGMA"))
    results = []
    for api, engine in ((casa, "casa"), (tools, "native")):
        fits = tmp_path / f"{engine}.uvfits"
        with closing(api.ms()) as writer:
            assert writer.open(str(source))
            assert writer.tofits(str(fits), column="corrected", **selection)
            assert writer.name() == str(source) and not writer.iswritable()
        location = tmp_path / f"{engine}.ms"
        with closing(casa.ms()) as reader:
            assert reader.fromfits(str(location), str(fits))
            assert reader.nrow() > 0
        results.append(read(casa, location, MAIN))
        with closing(tools.ms()) as reader:
            native = tmp_path / f"{engine}-native-reader.ms"
            assert reader.fromfits(str(native), str(fits))
            assert reader.nrow() == results[-1]["DATA"].shape[-1]
        compare(results[-1], read(casa, native, MAIN), comparison="import-pinned-export")
    compare(*results)
    compare(original, read(casa, source, tuple(original)))


def test_uvfits_failed_operation_preserves_attachment_and_existing_output(fits_stock, tmp_path):
    casa = pinned_casa()
    source = tmp_path / "attached.ms"
    copytree(fits_stock / "uvfits_test.ms", source)
    for api in (casa, tools):
        with closing(api.ms()) as tool:
            assert tool.open(str(source))
            assert tool.tofits(overwrite=True)
            output = source.with_suffix(".fits")
            payload = output.read_bytes()
            with pytest.raises(RuntimeError):
                tool.tofits()
            assert output.read_bytes() == payload
            with pytest.raises(RuntimeError, match="zero rows"):
                tool.tofits(overwrite=True, taql="FALSE")
            assert output.read_bytes() == payload
            with pytest.raises(RuntimeError):
                tool.fromfits(str(tmp_path / "missing.ms"), str(tmp_path / "invalid.fits"))
            assert tool.name() == str(source) and not tool.iswritable()
            assert not (tmp_path / "missing.ms").exists()


def test_tofits_wsrt_stride_preserves_frequency_and_itrf_positions(fits_stock, tmp_path):
    casa = pinned_casa()
    source = tmp_path / "wsrt.ms"
    copytree(fits_stock / "uvfits_test.ms", source)
    with closing(casa.table()) as table:
        assert table.open(str(source / "OBSERVATION"), nomodify=False)
        assert table.putcell("TELESCOPE_NAME", 0, "WSRT")
    with closing(casa.table()) as table:
        assert table.open(str(source / "SPECTRAL_WINDOW"), nomodify=False)
        for row in range(table.nrows()):
            assert table.putcell("MEAS_FREQ_REF", row, 5)
        expected_frequency = np.mean(table.getcell("CHAN_FREQ", 1)[:2])
    expected_positions = read(casa, source / "ANTENNA", ["POSITION"])["POSITION"]
    out = tmp_path / "wsrt.uvfits"
    with closing(tools.ms()) as writer:
        assert writer.open(str(source))
        assert writer.tofits(str(out), spw="1:0~2^2")
    reread = tmp_path / "wsrt-reread.ms"
    with closing(casa.ms()) as reader:
        assert reader.fromfits(str(reread), str(out))
        assert reader.nrow() == 315
    spw = read(casa, reread / "SPECTRAL_WINDOW", ["CHAN_FREQ", "MEAS_FREQ_REF"])
    np.testing.assert_allclose(spw["CHAN_FREQ"], [[expected_frequency]], atol=0.01, rtol=0)
    np.testing.assert_array_equal(spw["MEAS_FREQ_REF"], [5])
    positions = read(casa, reread / "ANTENNA", ["POSITION"])["POSITION"]
    np.testing.assert_allclose(positions, expected_positions, atol=1e-7, rtol=0)


def fits_header(fields):
    cards = []
    for name, value in fields:
        if isinstance(value, str):
            value = f"'{value}'"
        if isinstance(value, bool):
            value = "T" if value else "F"
        cards.append(f"{name:8}= {value:>20}".encode().ljust(80))
    raw = b"".join(cards) + b"END".ljust(80)
    return raw.ljust(len(raw) + (-len(raw) % 2880))


def fits_binary_header(title, row_bytes, rows, columns):
    fields = [
        ("XTENSION", "BINTABLE"), ("BITPIX", 8), ("NAXIS", 2),
        ("NAXIS1", row_bytes), ("NAXIS2", rows), ("PCOUNT", 0),
        ("GCOUNT", 1), ("TFIELDS", len(columns)), ("EXTNAME", title),
    ]
    for index, (name, kind) in enumerate(columns, 1):
        fields.extend([(f"TTYPE{index}", name), (f"TFORM{index}", kind)])
    return fields


def test_fromfits_system_power_extension(fits_stock, tmp_path):
    casa = pinned_casa()
    source = tmp_path / "power.uvfits"
    with closing(casa.ms()) as tool:
        assert tool.open(str(fits_stock / "uvfits_test.ms"))
        assert tool.tofits(str(source))
    columns = [
        ("TIME", "1D"), ("TIME INTERVAL", "1E"), ("ANTENNA NO.", "1J"), ("FREQ ID", "1J"),
        ("POWER DIF1", "1E"), ("POWER SUM1", "1E"), ("POST GAIN1", "1E"),
        ("POWER DIF2", "1E"), ("POWER SUM2", "1E"), ("POST GAIN2", "1E"),
    ]
    system_power = fits_binary_header("AIPS SY", 44, 2, columns) + [("NO_IF", 1), ("NO_POL", 2)]
    power = struct.pack(
        ">df2i6fdf2i6f", 53500, .125, 1, 1, 2, 4, 6, 3, 5, 7,
        53501, .25, 2, 2, 8, 10, 12, 9, 11, 13,
    )
    with source.open("ab") as file:
        file.write(fits_header(system_power) + power.ljust(2880, b"\0"))
    names = ("ANTENNA_ID", "FEED_ID", "SPECTRAL_WINDOW_ID", "TIME", "INTERVAL",
             "SWITCHED_DIFF", "SWITCHED_SUM", "REQUANTIZER_GAIN")
    results = []
    for api, label in ((casa, "casa"), (tools, "native")):
        path = tmp_path / f"{label}.ms"
        with closing(api.ms()) as tool:
            assert tool.fromfits(str(path), str(source))
        with closing(casa.table()) as table:
            assert table.open(str(path))
            assert Path(table.getkeyword("SYSPOWER").removeprefix("Table: ")) == path / "SYSPOWER"
        results.append(read(casa, path / "SYSPOWER", names))
    compare(*results)
    np.testing.assert_array_equal(results[-1]["SWITCHED_DIFF"], [[2, 8], [3, 9]])


def test_fromfits_aips_binary_visibility_groups(fits_stock, tmp_path):
    casa = pinned_casa()
    base = tmp_path / "stock.uvfits"
    with closing(casa.ms()) as tool:
        assert tool.open(str(fits_stock / "uvfits_test.ms"))
        assert tool.tofits(str(base))
    raw = base.read_bytes()
    extensions = next(offset for offset in range(2880, len(raw), 2880)
                      if raw[offset:offset + 8] == b"XTENSION")
    primary = fits_header([
        ("SIMPLE", True), ("BITPIX", 8), ("NAXIS", 2), ("NAXIS1", 777777701),
        ("NAXIS2", 0), ("EXTEND", True), ("DATE", "2005-09-11T00:00:00"),
    ])
    columns = [(name, "1E") for name in ("UU", "VV", "WW", "BASELINE", "DATE", "FREQSEL")]
    groups = fits_binary_header("AIPS UV", 60, 2, columns + [("VISIBILITIES", "9E")])
    groups.extend([
        ("TELESCOP", "SMA"), ("OBSERVER", "fixture"), ("OBJECT", "TEST"),
        ("DATE-OBS", "2005-09-11T00:00:00"), ("SPECSYS", "LSRK"),
        ("TDIM7", "(3,1,3,1,1,1)"), ("TZERO5", 2453624.0),
    ])
    axes = [("COMPLEX", 1, 1, 1), ("STOKES", -1, 1, 1), ("FREQ", 230.5e9, 2, 1e6),
            ("IF", 1, 1, 1), ("RA", 10, 1, 1), ("DEC", 15, 1, 1)]
    for index, axis in enumerate(axes, 1):
        groups.extend((f"{index}{key}7", value)
                      for key, value in zip(("CTYP", "CRVL", "CRPX", "CDLT"), axis))
    samples = struct.pack(
        ">30f", 1e-8, 3e-8, -2e-8, 258, .875, 1, 1, 2, 3, 4, 5, -2, 7, 8, 1,
        2e-8, 1e-8, 1e-8, 258, .9, 1, 2, 3, 4, 5, 6, 3, 8, 9, 1,
    )
    source = tmp_path / "binary.uvfits"
    source.write_bytes(primary + raw[extensions:] + fits_header(groups) + samples.ljust(2880, b"\0"))
    target = tmp_path / "binary.ms"
    with closing(tools.ms()) as tool:
        assert tool.fromfits(str(target), str(source))
    result = read(casa, target, ("DATA", "FLAG", "WEIGHT_SPECTRUM", "ANTENNA1", "ANTENNA2"))
    np.testing.assert_array_equal(result["DATA"], [[[1+2j, 2+3j], [4+5j, 5+6j], [7+8j, 8+9j]]])
    np.testing.assert_array_equal(result["FLAG"], [[[False, False], [True, False], [False, False]]])
    np.testing.assert_array_equal(result["WEIGHT_SPECTRUM"], [[[3, 4], [2, 3], [1, 1]]])
    np.testing.assert_array_equal(result["ANTENNA1"], [0, 0])
    np.testing.assert_array_equal(result["ANTENNA2"], [1, 1])
