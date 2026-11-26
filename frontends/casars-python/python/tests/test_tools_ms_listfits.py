"""FITS HDU diagnostics through the MeasurementSet tool."""

import gzip
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
    formats = ("3A", "2J", "E", "3L", "C", "9X", "1PJ(3)", "1PA(4)", "3A", "1PX(1)")
    binary = fits_hdu([
        "XTENSION= 'BINTABLE'",
        "BITPIX  =                    8",
        "NAXIS   =                    2",
        "NAXIS1  =                   55",
        "NAXIS2  =                    1",
        "PCOUNT  =                   17",
        "GCOUNT  =                    1",
        "TFIELDS =                   10",
        *(f"{f'TFORM{i}':<8}= '{fmt}'" for i, fmt in enumerate(formats, 1)),
        "TNULL2  =                   -4",
        "TSCAL2  =                  3.0",
        "TZERO2  =                 11.0",
    ], pack(">3s2if3s2f2B4i3s2i3i4sB", b"a  ", 123, -4, 2.5, b"TF\0", 1.25, -0.125,
            0xAA, 0x80, 3, 0, 4, 12, b"z\0\0", 1, 16, 40, -50, 60, b"test", 0x80))
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


def random_groups(channels, cancel_first_parameter=False):
    scale = "1.234565" if cancel_first_parameter else "3.0"
    zero = "-1.234565D0" if cancel_first_parameter else "5.0"
    values = list(range(-1, 2 * channels + 3))
    if cancel_first_parameter:
        values[0] = 1
    return fits_hdu([
        "SIMPLE  =                    T",
        "BITPIX  =                  -32",
        "NAXIS   =                    2",
        "NAXIS1  =                    0",
        f"NAXIS2  = {channels:20}",
        "GROUPS  =                    T",
        "PCOUNT  =                    2",
        "GCOUNT  =                    2",
        f"PSCAL1  = {scale:>20}",
        f"PZERO1  = {zero:>20}",
        "BSCALE  =                  2.0",
        "BZERO   =                 10.0",
    ], pack(f">{2 * (channels + 2)}f", *values))


def tiled_image():
    primary = fits_hdu([
        "SIMPLE  =                    T",
        "BITPIX  =                   16",
        "NAXIS   =                    0",
        "EXTEND  =                    T",
    ])
    # Each Rice tile has two 16-bit pixels and occupies three bytes in the
    # heap. These bytes were generated independently by CFITSIO's writer.
    offsets = b"".join(pack(">ii", 3, 3 * row) for row in range(12))
    tiles = b"".join(pack(">hB", 2 * row, 25) for row in range(12))
    return primary + fits_hdu([
        "XTENSION= 'BINTABLE'", "BITPIX  =                    8",
        "NAXIS   =                    2", "NAXIS1  =                    8",
        "NAXIS2  =                   12", "PCOUNT  =                   36",
        "GCOUNT  =                    1", "TFIELDS =                    1",
        "TTYPE1  = 'COMPRESSED_DATA'", "TFORM1  = '1PB(3)  '",
        "ZIMAGE  =                    T", "ZBITPIX =                   16",
        "ZNAXIS  =                    3", "ZNAXIS1 =                    2",
        "ZNAXIS2 =                    3", "ZNAXIS3 =                    4",
        "ZTILE1  =                    2", "ZTILE2  =                    1",
        "ZTILE3  =                    1", "ZCMPTYPE= 'RICE_1  '",
        "ZNAME1  = 'BLOCKSIZE'", "ZVAL1   =                   32",
        "ZNAME2  = 'BYTEPIX '", "ZVAL2   =                    2",
    ], offsets + tiles)


def listing(tool, name, caplog):
    caplog.clear()
    with caplog.at_level(logging.INFO, logger=LOGGER):
        assert tool.listfits(fitsfile=name) is True
    return [line.rstrip() for record in caplog.records if record.name == LOGGER
            and record.levelno == logging.INFO for line in record.getMessage().splitlines()]


@pytest.mark.parametrize("content, suffix", [
    (image_and_tables(), ".fits"), (random_groups(20), "[3].fits"),
    (tiled_image(), ".fits"), (gzip.compress(random_groups(20)), ".fits.gz"),
], ids=["image-and-tables", "random-groups", "tiled-image", "gzip"])
def test_ms_listfits_reports_casa_headers_and_preview(tmp_path, capfd, caplog, content, suffix):
    path = tmp_path / ("sample" + suffix)
    path.write_bytes(content)
    reference = pinned_casa().ms()
    assert reference.listfits(str(path)) is True
    captured = capfd.readouterr()
    expected = [line.split("\t")[-1].rstrip()
                for line in (captured.out + captured.err).splitlines()
                if "\tINFO\tFITSReader::" in line]
    assert expected and expected[-1] == "finish processing of Header-Data Units."
    assert any("SIMPLE" in line for line in expected)
    assert any(line.startswith(("(0,0)", "Group 0 data:", "0: |")) for line in expected)
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


def test_ms_listfits_reports_physical_group_axes_and_parameter_precision(tmp_path, capfd, caplog):
    path = tmp_path / "scaled-group.fits"
    path.write_bytes(random_groups(20, cancel_first_parameter=True))
    assert pinned_casa().ms().listfits(str(path)) is True
    captured = capfd.readouterr()
    assert "Group 0 parms:  0 0" in captured.out + captured.err

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        assert tools.ms().listfits(str(path)) is True
    messages = [r.getMessage() for r in caplog.records if r.name == LOGGER]
    assert "Axis 1 size 20" in messages
    assert [message for message in messages if message.startswith("Axis ")] == ["Axis 1 size 20"]
    assert any("Group 0 parms:  0 0\n" in message for message in messages)


def test_ms_listfits_reports_truncated_image_data(tmp_path, capfd, caplog):
    path = tmp_path / "truncated-image.fits"
    path.write_bytes(fits_hdu([
        "SIMPLE  =                    T", "BITPIX  =                   16",
        "NAXIS   =                    3", "NAXIS1  =                  100",
        "NAXIS2  =                  100", "NAXIS3  =                    5",
    ]))
    diagnostic = "Error occured during construction process"
    assert pinned_casa().ms().listfits(str(path)) is True
    captured = capfd.readouterr()
    assert diagnostic in captured.out + captured.err

    assert tools.ms().listfits(str(path)) is True
    assert any(r.levelno == logging.ERROR and r.getMessage() == diagnostic for r in caplog.records)
