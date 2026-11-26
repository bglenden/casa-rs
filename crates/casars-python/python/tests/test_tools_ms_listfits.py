"""FITS HDU diagnostics through the MeasurementSet tool."""

import logging
from struct import pack

import pytest

from casars import tools
from test_tools_ms import pinned_casa
from test_tools_msmetadata import measures, native_dataset


LOGGER = "casars.tools.ms"


def fits_hdu(cards, data=b""):
    block = 2880
    header = "".join(f"{card:<80}" for card in (*cards, "END")).encode("ascii")
    return header.ljust(-(-len(header) // block) * block) + data.ljust(
        -(-len(data) // block) * block, b"\0"
    )


def image_and_tables():
    primary = fits_hdu([
        "SIMPLE  =                    T / FITS standard",
        "BITPIX  =                   16",
        "NAXIS   =                    2",
        "NAXIS1  =                    3",
        "NAXIS2  =                    2",
        "BSCALE  =                  2.0",
        "BZERO   =                 10.0",
        "COMMENT visible note",
        "HISTORY user change",
        "OBSERVER= 'Test / apostrophe '' fit' / a comment",
    ], pack(">6h", -36, 5, 0, 22, -18, 15))
    extension = fits_hdu([
        "XTENSION= 'IMAGE   '",
        "BITPIX  =                  -64",
        "NAXIS   =                    2",
        "NAXIS1  =                    2",
        "NAXIS2  =                    1",
        "PCOUNT  =                    0",
        "GCOUNT  =                    1",
        "EXTNAME = 'IMG     '",
    ], pack(">2d", 1.2345678e-5, 1234567.8))
    formats = ("3A", "2J", "E", "L", "C", "9X", "1PJ(3)", "3A")
    binary = fits_hdu([
        "XTENSION= 'BINTABLE'",
        "BITPIX  =                    8",
        "NAXIS   =                    2",
        "NAXIS1  =                   37",
        "NAXIS2  =                    1",
        "PCOUNT  =                   12",
        "GCOUNT  =                    1",
        "TFIELDS =                    8",
        *(f"TFORM{i}  = '{fmt}'" for i, fmt in enumerate(formats, 1)),
        "TNULL2  =                   -4",
        "TSCAL2  =                  3.0",
        "TZERO2  =                 11.0",
    ], pack(">3s2ifc2f2B2i3s3i", b"a  ", 123, -4, 2.5, b"T", 1.25, -0.125,
            0xAA, 0x80, 3, 0, b"z\0\0", 40, -50, 60))
    ascii_table = fits_hdu([
        "XTENSION= 'TABLE   '",
        "BITPIX  =                    8",
        "NAXIS   =                    2",
        "NAXIS1  =                   16",
        "NAXIS2  =                    1",
        "PCOUNT  =                    0",
        "GCOUNT  =                    1",
        "TFIELDS =                    3",
        "TFORM1  = 'A4      '",
        "TFORM2  = 'I4      '",
        "TFORM3  = 'F8.2    '",
        "TBCOL1  =                    1",
        "TBCOL2  =                    5",
        "TBCOL3  =                    9",
    ], b"ok    12    1.21")
    return primary + extension + binary + ascii_table


def random_groups(channels):
    return fits_hdu([
        "SIMPLE  =                    T",
        "BITPIX  =                  -32",
        "NAXIS   =                    2",
        "NAXIS1  =                    0",
        f"NAXIS2  = {channels:20}",
        "GROUPS  =                    T",
        "PCOUNT  =                    2",
        "GCOUNT  =                    2",
        "PSCAL1  =                  3.0",
        "PZERO1  =                  5.0",
        "BSCALE  =                  2.0",
        "BZERO   =                 10.0",
    ], pack(f">{2 * (channels + 2)}f", *range(-1, 2 * channels + 3)))


def listing(tool, name, caplog):
    caplog.clear()
    with caplog.at_level(logging.INFO, logger=LOGGER):
        assert tool.listfits(fitsfile=name) is True
    return [line.rstrip() for record in caplog.records if record.name == LOGGER
            and record.levelno == logging.INFO for line in record.getMessage().splitlines()]


@pytest.mark.parametrize("content", [image_and_tables(), random_groups(20)],
                         ids=["image-and-tables", "random-groups"])
def test_ms_listfits_reports_casa_headers_and_preview(tmp_path, capfd, caplog, content):
    path = tmp_path / "sample.fits"
    path.write_bytes(content)
    reference = pinned_casa().ms()
    assert reference.listfits(str(path)) is True
    captured = capfd.readouterr()
    expected = [line.split("\t")[-1].rstrip()
                for line in (captured.out + captured.err).splitlines()
                if "\tINFO\tFITSReader::" in line]
    assert expected and expected[-1] == "finish processing of Header-Data Units."
    assert any("SIMPLE" in line for line in expected)
    assert any(line.startswith(("(0,0)", "Group 0 data:")) for line in expected)
    assert listing(tools.ms(), str(path), caplog) == expected
    assert listing(tools.ms(), bytes(path) + b"\0ignored", caplog) == expected
    assert path.read_bytes() == content


def test_ms_listfits_preserves_tool_state_and_logs_errors(native_dataset, tmp_path, caplog):
    casa = pinned_casa()
    bad = tmp_path / "not-fits"
    bad.write_text("plain text")
    for tool in (casa.ms(), tools.ms()):
        assert tool.name() == "none"
        assert tool.open(str(native_dataset))
        before = (tool.name(), tool.nrow(), tool.iswritable())
        for invalid in (bad, bytearray(b"file"), 5, None):
            with pytest.raises(TypeError, match="must be a string"):
                tool.listfits(invalid)
        with pytest.raises(TypeError, match="could not be encoded as UTF-8"):
            tool.listfits("\udc80")
        for source in (str(bad), str(tmp_path / "missing.fits"), str(tmp_path), ""):
            caplog.clear()
            with caplog.at_level(logging.INFO, logger=LOGGER):
                assert tool.listfits(source) is True
            if isinstance(tool, tools.ms):
                assert any(r.getMessage() == "Error opening FITS input."
                           and r.levelno == logging.WARNING for r in caplog.records)
                assert any(r.getMessage() == "Too many errors.  Processing terminated."
                           and r.levelno == logging.ERROR for r in caplog.records)
            assert (tool.name(), tool.nrow(), tool.iswritable()) == before
        assert tool.done()
    assert bad.read_text() == "plain text"


def test_ms_listfits_bounds_short_random_group(tmp_path, caplog):
    path = tmp_path / "short-groups.fits"
    path.write_bytes(random_groups(3))
    messages = listing(tools.ms(), str(path), caplog)
    assert "Group 0 data:  12 14 16..." in messages
    assert "Group 1 data:  22 24 26..." in messages


def test_ms_listfits_reports_each_stored_hdu_once(tmp_path, caplog):
    path = tmp_path / "primary-table.fits"
    path.write_bytes(fits_hdu([
        "SIMPLE  =                    T",
        "BITPIX  =                    8",
        "NAXIS   =                    2",
        "NAXIS1  =            777777701",
        "NAXIS2  =                    0",
    ]))
    messages = listing(tools.ms(), str(path), caplog)
    assert messages.count("Primary Table HDU ------>>>") == 1
    assert messages.count("END") == 1
    assert messages[-1] == "finish processing of Header-Data Units."
