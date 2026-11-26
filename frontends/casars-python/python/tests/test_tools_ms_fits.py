"""Visibility UVFITS exchange against the pinned CASA readers and writers."""

from contextlib import closing
import os
from pathlib import Path
from shutil import copytree

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
