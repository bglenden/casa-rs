"""Visibility UVFITS exchange against the pinned CASA readers and writers."""

from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import re
from shutil import copytree, rmtree
import struct
from threading import Barrier

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
    with closing(casa.table()) as ref, closing(casa.table()) as native:
        assert ref.open(str(tmp_path / "casa.ms" / "SOURCE"))
        assert native.open(str(tmp_path / "native.ms" / "SOURCE"))
        for column in ("REST_FREQUENCY", "SYSVEL", "POSITION"):
            assert native.getcolkeyword(column, "MEASINFO") == ref.getcolkeyword(column, "MEASINFO")
            np.testing.assert_array_equal(native.getcolkeyword(column, "QuantumUnits"),
                                          ref.getcolkeyword(column, "QuantumUnits"))


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


@pytest.mark.parametrize("stokes,correlation", [(-1, 5), (1, 1)])
def test_fromfits_aips_binary_visibility_groups(fits_stock, tmp_path, stokes, correlation):
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
    axes = [("COMPLEX", 1, 1, 1), ("STOKES", stokes, 1, 1), ("FREQ", 230.5e9, 2, 1e6),
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
    result = read(casa, target, ("DATA", "FLAG", "FLAG_CATEGORY", "WEIGHT_SPECTRUM", "ANTENNA1", "ANTENNA2"))
    np.testing.assert_array_equal(result["DATA"], [[[1+2j, 2+3j], [4+5j, 5+6j], [7+8j, 8+9j]]])
    np.testing.assert_array_equal(result["FLAG"], [[[False, False], [True, False], [False, False]]])
    np.testing.assert_array_equal(result["WEIGHT_SPECTRUM"], [[[3, 4], [2, 3], [1, 1]]])
    np.testing.assert_array_equal(result["ANTENNA1"], [0, 0])
    np.testing.assert_array_equal(result["ANTENNA2"], [1, 1])
    categories = np.zeros((1, 3, 3, 2), dtype=bool)
    categories[:, :, 0, :] = result["FLAG"]
    np.testing.assert_array_equal(result["FLAG_CATEGORY"], categories, strict=True)
    polarization = read(casa, target / "POLARIZATION", ["CORR_TYPE"])
    np.testing.assert_array_equal(polarization["CORR_TYPE"], [[correlation]])
    for api in (casa, tools):
        with closing(api.table()) as table:
            assert table.open(str(target))
            np.testing.assert_array_equal(table.getcolkeyword("FLAG_CATEGORY", "CATEGORY"),
                                          ["FLAG_CMD", "ORIGINAL", "USER"])
            assert table.iscelldefined("FLAG_CATEGORY", 0)


def fits_hdu_cards(raw, offset):
    cards = {}
    for position in range(offset, len(raw), 80):
        card = raw[position:position + 80].decode("ascii")
        name = card[:8].strip()
        if name == "END":
            data_start = position + 80
            return cards, data_start + (-data_start % 2880)
        if card[8:10] == "= ":
            value = card[10:].strip()
            word = value.split("'", 2)[1] if value.startswith("'") else value.split("/", 1)[0]
            cards[name] = (word.strip(), position)
    raise ValueError("missing FITS END card")


def fits_extension(raw, name):
    for offset in range(2880, len(raw), 2880):
        if raw[offset:offset + 8] == b"XTENSION":
            cards, data = fits_hdu_cards(raw, offset)
            if cards.get("EXTNAME", ("",))[0] == name:
                return cards, data
    raise ValueError(f"missing {name} extension")


def fits_columns(cards, data):
    columns = {}
    for index in range(1, int(cards["TFIELDS"][0]) + 1):
        name = cards[f"TTYPE{index}"][0]
        form, position = cards[f"TFORM{index}"]
        count, kind = re.fullmatch(r"([0-9]*)([ADEFJK])", form).groups()
        columns[name] = data, position
        data += int(count or 1) * {"A": 1, "E": 4, "J": 4, "D": 8, "F": 4, "K": 8}[kind]
    return columns


def stock_uvfits(casa, fits_stock, output, **options):
    with closing(casa.ms()) as tool:
        assert tool.open(str(fits_stock / "uvfits_test.ms"))
        assert tool.tofits(str(output), **options)
    return bytearray(output.read_bytes())


def add_test_weight_spectrum(table):
    column = variable_test_column(table, "DATA") | {"valueType": "float", "keywords": {}}
    assert table.addcols({"WEIGHT_SPECTRUM": column})


def variable_test_column(table, name):
    column = table.getcoldesc(name) | {
        "option": 0, "dataManagerType": "StandardStMan", "dataManagerGroup": "TestArray",
    }
    column.pop("shape", None)
    return column


@pytest.mark.parametrize("format", ["1C", "1PE(2)"])
def test_fromfits_checks_text_types_and_skips_unrelated_hdus(fits_stock, tmp_path, format):
    casa = pinned_casa()
    source = tmp_path / "stock.uvfits"
    original = stock_uvfits(casa, fits_stock, source)
    unexpected = fits_binary_header("UNRELATED", 8, 1, [("VALUE", format)])
    samples = struct.pack(">2f", 1, 2)
    if format.startswith("1P"):
        unexpected = [(name, 8 if name == "PCOUNT" else value) for name, value in unexpected]
        samples = struct.pack(">2i", 2, 0) + samples
    source.write_bytes(original + fits_header(unexpected) + samples.ljust(2880, b"\0"))
    with closing(tools.ms()) as tool:
        assert tool.fromfits(str(tmp_path / "valid.ms"), str(source))
        before = tool.name()
        card = fits_columns(*fits_extension(original, "AIPS AN"))["ANNAME"][1]
        name = original[card:card + 8].decode().strip()
        original[card:card + 80] = fits_header([(name, "8B")])[:80]
        source.write_bytes(original)
        target = tmp_path / "invalid.ms"
        with pytest.raises(RuntimeError, match="fixed-width text"):
            tool.fromfits(str(target), str(source))
        assert tool.name() == before and not target.exists()
        source.write_bytes(original[:2880])
        with pytest.raises(RuntimeError):
            tool.fromfits(str(target), str(source))
        assert tool.name() == before and not target.exists()


def test_fromfits_concurrent_publication_leaves_one_output_and_allows_retry(fits_stock, tmp_path):
    source = fits_stock / "3c273.fits7"
    destination = tmp_path / "result.ms"
    start = Barrier(2)

    def import_once():
        with closing(tools.ms()) as tool:
            before = tool.name()
            start.wait(timeout=5)
            try:
                return tool.fromfits(str(destination), str(source))
            except RuntimeError:
                assert tool.name() == before
                return False

    with ThreadPoolExecutor(max_workers=2) as workers:
        outcomes = list(workers.map(lambda _: import_once(), range(2)))
    assert sorted(outcomes) == [False, True]
    assert sorted(tmp_path.iterdir()) == [destination]
    assert read(pinned_casa(), destination, ["TIME"])["TIME"].shape == (2925,)
    rmtree(destination)
    with closing(tools.ms()) as tool:
        assert tool.fromfits(str(destination), str(source))
        assert tool.nrow() == 2925


def test_fromfits_converts_source_epoch(fits_stock, tmp_path):
    casa = pinned_casa()
    source = tmp_path / "epoch.uvfits"
    content = stock_uvfits(casa, fits_stock, source, multisource=True)
    columns = fits_columns(*fits_extension(content, "AIPS SU"))
    struct.pack_into(">d", content, columns["EPOCH"][0], 1950.0)
    source.write_bytes(content)
    results = []
    for api, label in ((casa, "casa"), (tools, "native")):
        target = tmp_path / f"{label}.ms"
        with closing(api.ms()) as tool:
            assert tool.fromfits(str(target), str(source))
        with closing(casa.table()) as table:
            assert table.open(str(target / "FIELD"))
            assert table.getcolkeyword("PHASE_DIR", "MEASINFO")["Ref"] == "J2000"
            results.append(table.getcell("PHASE_DIR", 0))
    np.testing.assert_allclose(results[1], results[0], atol=1e-9, rtol=0, strict=True)


def test_fromfits_converts_apparent_source_position_and_motion(fits_stock, tmp_path):
    casa = pinned_casa()
    source = tmp_path / "motion.uvfits"
    content = stock_uvfits(casa, fits_stock, source, multisource=True)
    header, _ = fits_hdu_cards(content, 0)
    columns = fits_columns(*fits_extension(content, "AIPS SU"))
    degrees = (120.0, 25.0)
    rates = (0.01, 0.02)
    for key, value in zip(("RAAPP", "DECAPP", "PMRA", "PMDEC"), (*degrees, *rates)):
        struct.pack_into(">d", content, columns[key][0], value)
    source.write_bytes(content)
    with closing(tools.ms()) as ms:
        target = tmp_path / "moving.ms"
        assert ms.fromfits(str(target), str(source))
    oracle = casa.measures()
    try:
        day = header["DATE-OBS"][0].replace("-", "/").replace("T", "/")
        assert oracle.doframe(oracle.epoch("UTC", day))

        def convert(position):
            result = oracle.measure(oracle.direction("APP", *(f"{axis}deg" for axis in position)), "J2000")
            return np.array([result["m0"]["value"], result["m1"]["value"]])

        angle = convert(degrees)
        ahead = convert(np.array(degrees) + np.array(rates) / 16)
        behind = convert(np.array(degrees) - np.array(rates) / 16)
    finally:
        oracle.done()
    derivative = (ahead - behind) / 10_800
    with closing(casa.table()) as table:
        assert table.open(str(target / "FIELD"))
        assert table.getcolkeyword("PHASE_DIR", "MEASINFO")["Ref"] == "J2000"
        assert table.getcell("NUM_POLY", 0) == 1
        actual = table.getcell("PHASE_DIR", 0)
    np.testing.assert_allclose(actual[:, 0], angle, atol=2e-6, rtol=0)
    np.testing.assert_allclose(actual[:, 1], derivative, atol=0, rtol=2e-6)


def test_tofits_wsrt_line_metadata_and_weighted_samples(fits_stock, tmp_path):
    casa = pinned_casa()
    source = tmp_path / "line.ms"
    copytree(fits_stock / "uvfits_test.ms", source)
    with closing(casa.table()) as table:
        assert table.open(str(source / "OBSERVATION"), nomodify=False)
        assert table.putcell("TELESCOPE_NAME", 0, "WSRT")
        text = table.getcoldesc("TELESCOPE_NAME")
    nfra = source / "NFRA_TMS_PARAMETERS"
    with closing(casa.table()) as table:
        assert table.create(str(nfra), {"NAME": text, "VALUE": text}, nrow=1)
        assert table.putcell("NAME", 0, "FW1.GeoSkyFreq")
        assert table.putcell("VALUE", 0, "230510.0 MHz")
    with closing(casa.table()) as table:
        assert table.open(str(source / "SPECTRAL_WINDOW"), nomodify=False)
        assert table.addcols({"NFRA_VELOCDEFINITION": text, "NFRA_CONVERSIONTYPE": text})
        assert table.putcell("NFRA_VELOCDEFINITION", 0, "lsrk")
        assert table.putcell("NFRA_CONVERSIONTYPE", 0, "radio")
        reference = table.getcell("CHAN_FREQ", 1)
    with closing(casa.table()) as table:
        assert table.open(str(source / "POLARIZATION"), nomodify=False)
        assert table.putcell("NUM_CORR", 0, 2)
        assert table.putcell("CORR_TYPE", 0, np.array([9, 12], dtype=np.int32))
        assert table.putcell("CORR_PRODUCT", 0, np.array([[0, 1], [0, 1]], dtype=np.int32))
    with closing(casa.table()) as table:
        assert table.open(str(source), nomodify=False)
        assert table.putkeyword("NFRA_TMS_PARAMETERS", f"Table: {nfra}")
        row = int(np.flatnonzero(table.getcol("DATA_DESC_ID") == 1)[0])
        for name in ("DATA", "FLAG", "WEIGHT"):
            definition = variable_test_column(table, name)
            assert table.removecols([name])
            assert table.addcols({name: definition})
        if "WEIGHT_SPECTRUM" in table.colnames():
            assert table.removecols(["WEIGHT_SPECTRUM"])
        add_test_weight_spectrum(table)
        exact_time = table.getcell("TIME", row)
        assert table.putcell("TIME_CENTROID", row, exact_time + 60.0)
        assert table.putcell("DATA", row, np.array([[1, 100, 0], [10, 30, 0]], dtype=np.complex64))
        assert table.putcell("FLAG", row, np.array([[False, True, True], [False, False, True]]))
        assert table.putcell("FLAG_ROW", row, False)
        assert table.putcell("WEIGHT_SPECTRUM", row, np.array([[2, 100, 0], [3, 7, 0]], dtype=np.float32))
        assert table.putcell("WEIGHT", row, np.array([102, 10], dtype=np.float32))
        antennas = [table.getcell(column, row) for column in ("ANTENNA1", "ANTENNA2")]
    output = tmp_path / "line.uvfits"
    with closing(tools.ms()) as writer:
        assert writer.open(str(source))
        a, b = antennas
        taql = f"TIME == {exact_time:.17g} && ANTENNA1 == {a} && ANTENNA2 == {b}"
        assert writer.tofits(str(output), column="DATA", spw="1:0~2^2", taql=taql)
    raw = output.read_bytes()
    cards, _ = fits_extension(raw, "AIPS SU")
    assert cards["VELTYP"][0] == "LSRK" and cards["VELDEF"][0] == "RADIO"
    target = tmp_path / "line-roundtrip.ms"
    with closing(tools.ms()) as reader:
        assert reader.fromfits(str(target), str(output))
        assert reader.nrow() == 1
    visibility = read(casa, target, ["DATA", "WEIGHT_SPECTRUM", "TIME"])
    np.testing.assert_array_equal(visibility["DATA"], [[[1]], [[24]]])
    np.testing.assert_array_equal(visibility["WEIGHT_SPECTRUM"], [[[2]], [[10]]])
    np.testing.assert_allclose(visibility["TIME"], [exact_time], atol=0.02, rtol=0)
    spw = read(casa, target / "SPECTRAL_WINDOW", ["CHAN_FREQ", "MEAS_FREQ_REF"])
    desired = 230510.0e6 + (reference[0] - reference[1]) / 2
    np.testing.assert_allclose(spw["CHAN_FREQ"], [[desired]], atol=0.01, rtol=0)
    np.testing.assert_array_equal(spw["MEAS_FREQ_REF"], [4])


def test_tofits_undefined_first_weight_spectrum_uses_scalar_weights(fits_stock, tmp_path):
    casa = pinned_casa()
    source = tmp_path / "partial-weights.ms"
    copytree(fits_stock / "uvfits_test.ms", source)
    with closing(casa.table()) as table:
        assert table.open(str(source), nomodify=False)
        if "WEIGHT_SPECTRUM" in table.colnames():
            assert table.removecols(["WEIGHT_SPECTRUM"])
        add_test_weight_spectrum(table)
        assert not table.iscelldefined("WEIGHT_SPECTRUM", 0)
        for row in (0, 1):
            assert table.putcell("FLAG_ROW", row, False)
            assert table.putcell("FLAG", row, np.zeros((1, 3), dtype=bool))
            assert table.putcell("WEIGHT", row, np.array([12], dtype=np.float32))
        assert table.putcell("WEIGHT_SPECTRUM", 1, np.full((1, 3), 99, dtype=np.float32))
    for api, label in ((casa, "casa"), (tools, "native")):
        output = tmp_path / f"{label}.uvfits"
        with closing(api.ms()) as writer:
            assert writer.open(str(source))
            assert writer.tofits(str(output))
        target = tmp_path / f"{label}.ms"
        with closing(casa.ms()) as reader:
            assert reader.fromfits(str(target), str(output))
        with closing(casa.table()) as table:
            assert table.open(str(target))
            values = table.getcol("WEIGHT_SPECTRUM")
            assert not np.any(values == 99)
            assert np.count_nonzero(np.all(values == 4, axis=(0, 1))) >= 2


@pytest.mark.parametrize("motion", [None, [0.01, -0.02]])
def test_tofits_optional_proper_motion_and_units(fits_stock, tmp_path, motion):
    casa = pinned_casa()
    original = tmp_path / "original.uvfits"
    stock_uvfits(casa, fits_stock, original, multisource=True)
    source = tmp_path / "source.ms"
    with closing(casa.ms()) as importer:
        assert importer.fromfits(str(source), str(original))
    with closing(casa.table()) as table:
        assert table.open(str(source / "SOURCE"), nomodify=False)
        definition = variable_test_column(table, "PROPER_MOTION")
        assert table.removecols(["PROPER_MOTION"])
        assert table.addcols({"PROPER_MOTION": definition})
        if motion is not None:
            assert table.putcell("PROPER_MOTION", 0, np.deg2rad(motion) / 86_400)
    for api, label in ((casa, "casa"), (tools, "native")):
        if label == "native" and motion is not None:
            with closing(casa.table()) as table:
                assert table.open(str(source / "SOURCE"), nomodify=False)
                assert table.putcolkeyword("PROPER_MOTION", "QuantumUnits", ["deg/d"])
                assert table.putcell("PROPER_MOTION", 0, np.array(motion))
        output = tmp_path / f"{label}.uvfits"
        with closing(api.ms()) as writer:
            assert writer.open(str(source))
            assert writer.tofits(str(output), multisource=True)
        raw = output.read_bytes()
        columns = fits_columns(*fits_extension(raw, "AIPS SU"))
        values = [struct.unpack_from(">d", raw, columns[key][0])[0] for key in ("PMRA", "PMDEC")]
        np.testing.assert_allclose(values, motion or [0.0, 0.0], atol=1e-12, rtol=0)


def test_uvfits_separate_antenna_parameters_preserve_subarray(fits_stock, tmp_path):
    casa = pinned_casa()
    source = tmp_path / "high-antenna.ms"
    copytree(fits_stock / "uvfits_test.ms", source)
    with closing(casa.table()) as table:
        assert table.open(str(source / "ANTENNA"), nomodify=False)
        assert table.putcell("NAME", 0, "300")
    with closing(casa.table()) as table:
        assert table.open(str(source))
        row_arrays = table.getcol("ARRAY_ID")
    output = tmp_path / "high-antenna.fits"
    with closing(tools.ms()) as tool:
        assert tool.open(str(source)) and tool.tofits(str(output))
        target = tmp_path / "reread.ms"
        assert tool.fromfits(str(target), str(output))
    cards, _ = fits_hdu_cards(output.read_bytes(), 0)
    names = [value[0] for key, value in cards.items() if key.startswith("PTYPE")]
    assert "SUBARRAY" in names and "ANTENNA1" in names and "BASELINE" not in names
    main = read(casa, target, ["ARRAY_ID", "ANTENNA1", "ANTENNA2"])
    np.testing.assert_array_equal(np.sort(main["ARRAY_ID"]), np.sort(row_arrays), strict=True)
    assert 299 in main["ANTENNA1"] or 299 in main["ANTENNA2"]


def test_tofits_combined_if_system_calibration(fits_stock, tmp_path):
    casa = pinned_casa()
    source = tmp_path / "calibration.ms"
    copytree(fits_stock / "uvfits_test.ms", source)
    system = source / "SYSCAL"
    with closing(casa.table()) as table:
        assert table.open(str(source))
        definitions = {
            key: table.getcoldesc(column) | {"dataManagerType": "StandardStMan", "dataManagerGroup": "SysCalTest"}
            for key, column in [("ANTENNA_ID", "ANTENNA1"), ("TIME", "TIME"), ("INTERVAL", "INTERVAL"),
                                ("FEED_ID", "FEED1"), ("SPECTRAL_WINDOW_ID", "DATA_DESC_ID"), ("TSYS", "WEIGHT")]
        }
        times = sorted(set(table.getcol("TIME")))[:2]
    with closing(casa.table()) as table:
        assert table.create(str(system), definitions, nrow=8)
        rows = [(a, t, w) for a in range(2) for t in times for w in (1, 2)]
        for row, (antenna, time, window) in enumerate(rows):
            for column, value in [("ANTENNA_ID", antenna), ("TIME", time), ("INTERVAL", 10.0),
                                  ("FEED_ID", 0), ("SPECTRAL_WINDOW_ID", window),
                                  ("TSYS", np.array([100 + row, 200 + row], dtype=np.float32))]:
                assert table.putcell(column, row, value)
    with closing(casa.table()) as table:
        assert table.open(str(source), nomodify=False)
        assert table.putkeyword("SYSCAL", f"Table: {system}")
    temperatures = []
    for api, label in ((casa, "casa"), (tools, "native")):
        output = tmp_path / f"{label}.uvfits"
        with closing(api.ms()) as tool:
            assert tool.open(str(source))
            assert tool.tofits(str(output), combinespw=True, writesyscal=True)
        raw = output.read_bytes()
        cards, data = fits_extension(raw, "AIPS TY")
        assert cards["NO_IF"][0] == "2" and cards["NO_POL"][0] == "2"
        offsets = fits_columns(cards, data)
        row_width = int(cards["NAXIS1"][0])
        rows = int(cards["NAXIS2"][0])
        values = [[struct.unpack_from(">2f", raw, offsets[f"TSYS {hand}"][0] + row * row_width)
                   for row in range(rows)] for hand in (1, 2)]
        temperatures.append(values)
        gain, _ = fits_extension(raw, "AIPS GC")
        assert gain["NO_BAND"][0] == "2"
    np.testing.assert_array_equal(temperatures[0], temperatures[1])
    np.testing.assert_array_equal(temperatures[1][0], np.arange(100, 108).reshape(4, 2))
    with closing(casa.table()) as table:
        assert table.open(str(system), nomodify=False)
        assert table.putcell("SPECTRAL_WINDOW_ID", 1, 1)
    previous = output.read_bytes()
    with closing(tools.ms()) as writer:
        assert writer.open(str(source))
        with pytest.raises(RuntimeError, match="repeated SYSCAL spectral window"):
            writer.tofits(str(output), combinespw=True, writesyscal=True, overwrite=True)
    assert output.read_bytes() == previous
