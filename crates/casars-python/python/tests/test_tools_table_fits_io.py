# SPDX-License-Identifier: LGPL-3.0-or-later
"""Test native FITS import with values and metadata recorded in CASA 6.7.6.14."""
import struct

import numpy as np
import pytest

from casars.tools import table


def _fits_header(cards):
    def card(key, value):
        if isinstance(value, str):
            return f"{key:8}= '{value:<8}'"
        if isinstance(value, bool):
            value = "T" if value else "F"
        return f"{key:8}= {value:>20}"

    return b"".join(
        string.encode("ascii").ljust(80)
        for string in [*(card(key, value) for key, value in cards), "END"]
    ).ljust(2880)


def _table_fits(path):
    primary = _fits_header([("SIMPLE", True), ("BITPIX", 8), ("NAXIS", 0), ("EXTEND", True)])
    fields = [
        ("SCALE", "1I"), ("ID", "1J"), ("LABEL", "8A"), ("DATA", "4E"),
        ("VALID", "1L"), ("Z", "1C"), ("TDIM4", "8A"), ("FLAGS", "3X"),
    ]
    rows = (
        struct.pack(">hi8s4fc2f8sB", 2, -32768, b"A/B     ", 1, 2, 3, 4,
                    b"T", 3, -4, b"(2,2)   ", 0xA0),
        struct.pack(">hi8s4fc2f8sB", 4, 9, b"last    ", 5, 6, 7, 8,
                    b"F", -2, 1, b"(1,2)   ", 0x40),
    )
    header = _fits_header([
        ("XTENSION", "BINTABLE"), ("BITPIX", 8), ("NAXIS", 2),
        ("NAXIS1", len(rows[0])), ("NAXIS2", len(rows)), ("PCOUNT", 0),
        ("GCOUNT", 1), ("TFIELDS", len(fields)),
        *((f"{k}{i}", v) for i, pair in enumerate(fields, 1) for k, v in zip(("TTYPE", "TFORM"), pair)),
        ("TSCAL1", 0.5), ("TZERO1", 10), ("TNULL2", -32768), ("TUNIT4", "K"),
        ("TDISP4", "E8.2"), ("OBJECT", "target"), ("EXTNAME", "SAMPLE"), ("CUSTOM", 5),
    ])
    path.write_bytes(primary + header + b"".join(rows).ljust(2880, b"\0"))


@pytest.mark.parametrize("storage,kind", [("standard", "StandardStMan"), ("incremental", "IncrementalStMan")])
@pytest.mark.parametrize("sdfits", [False, True])
def test_fromfits(tmp_path, storage, kind, sdfits):
    source = tmp_path / "source.fits"
    _table_fits(source)
    tb = table()
    destination = str(tmp_path / "result.table")
    out = tb.fromfits(destination, str(source), storage=storage, convention="SDFITS" if sdfits else "none")
    assert tb.name() == ""
    assert out.iswritable()  # CASA ignores nomodify and ack here.
    assert out.lockoptions()["option"] == "autonoread"
    assert out.getcoldesc("SCALE")["dataManagerType"] == kind
    assert out.getcoldesc("SCALE")["valueType"] == "double"
    np.testing.assert_array_equal(out.getcol("SCALE"), [11, 12])
    np.testing.assert_array_equal(out.getcol("ID"), [-32768, 9])
    assert out.getcolkeywords("ID") == {"NULL": -32768}
    assert list(out.getcol("LABEL")) == ["A/B", "last"]
    assert out.getcol("VALID").tolist() == [True, False]
    assert out.getcell("Z", 0) == 3-4j
    assert out.getcolkeywords("DATA") == {"UNIT": "K", "DISP": "E8.2"}
    np.testing.assert_array_equal(out.getcell("DATA", 0), [[1, 3], [2, 4]])
    np.testing.assert_array_equal(out.getcell("DATA", 1), [[5, 6]])
    np.testing.assert_array_equal(out.getcell("FLAGS", 0), [True, False, True])
    if sdfits:
        assert out.getkeywords() == {"EXTNAME": "SAMPLE"}
        assert "TDIM4" not in out.colnames()
        assert out.getcell("OBJECT", 1) == "target"
        assert out.getcell("CUSTOM", 0) == 5
    else:
        assert out.getkeywords() == {"EXTNAME": "SAMPLE", "OBJECT": "target", "CUSTOM": 5}
        assert out.getcell("TDIM4", 1) == "(1,2)"
    with pytest.raises(RuntimeError):
        tb.fromfits(destination, str(source))
    assert out.getcell("SCALE", 0) == 11


def test_fromfits_invalid_hdu_or_storage(tmp_path):
    source = tmp_path / "source.fits"
    _table_fits(source)
    target = tmp_path / "result.table"
    tb = table()
    with pytest.raises(RuntimeError, match="whichHDU is not valid"):
        tb.fromfits(str(target), str(source), 0)
    with pytest.raises(RuntimeError, match="does not have a valid binary table"):
        tb.fromfits(str(target), str(source), 2)
    with pytest.raises(RuntimeError, match="is not a valid storage manager"):
        tb.fromfits(str(target), str(source), storage="wrong")
    assert not target.exists()


def test_fromfits_memory_keeps_the_layout_only(tmp_path):
    source = tmp_path / "source.fits"
    _table_fits(source)
    path = tmp_path / "memory.table"
    tb = table()
    out = tb.fromfits(str(path), str(source), storage="memory")
    assert out.iswritable()
    assert out.lockoptions()["option"] == "autonoread"
    assert out.getcoldesc("DATA")["dataManagerType"] == "MemoryStMan"
    assert out.getdminfo()["*1"]["NAME"] == "MemSt"
    np.testing.assert_array_equal(out.getcol("SCALE"), [11, 12])
    assert out.putcell("ID", 0, 42)
    assert out.getcell("ID", 0) == 42
    assert out.done()
    assert (path / "table.dat").exists()
    assert not list(path.glob("table.f*"))
    assert tb.open(str(path))
    assert tb.nrows() == 2
    assert tb.getdminfo()["*1"]["NAME"] == "MemoryStMan"
    np.testing.assert_array_equal(tb.getcol("SCALE"), [0, 0])
    assert tb.getcell("ID", 0) == 0
    assert not tb.iscelldefined("DATA", 0)


def test_fromfits_second_hdu_ascii_and_heap(tmp_path):
    source = tmp_path / "mixed.fits"
    primary = _fits_header([("SIMPLE", True), ("BITPIX", 8), ("NAXIS", 0), ("EXTEND", True)])
    ascii_header = _fits_header([
        ("XTENSION", "TABLE"), ("BITPIX", 8), ("NAXIS", 2), ("NAXIS1", 17),
        ("NAXIS2", 2), ("PCOUNT", 0), ("GCOUNT", 1), ("TFIELDS", 3),
        ("TTYPE1", "NAME"), ("TFORM1", "A6"), ("TBCOL1", 1),
        ("TTYPE2", "ID"), ("TFORM2", "I4"), ("TBCOL2", 7),
        ("TTYPE3", "FLUX"), ("TFORM3", "F7.2"), ("TBCOL3", 11),
    ])
    data = "".join(f"{name:6}{i:4}{flux:7.2f}" for name, i, flux in [
        ("north", 3, 1.25), ("south", -3, -2.5),
    ]).encode("ascii").ljust(2880)
    heap_header = _fits_header([
        ("XTENSION", "BINTABLE"), ("BITPIX", 8), ("NAXIS", 2), ("NAXIS1", 8),
        ("NAXIS2", 2), ("PCOUNT", 16), ("GCOUNT", 1), ("TFIELDS", 1),
        ("TTYPE1", "DATA"), ("TFORM1", "1PE(3)"),
    ])
    heap_data = struct.pack(">iiii4f", 3, 0, 1, 12, 1.0, 2.0, 3.0, 7.0).ljust(2880, b"\0")
    source.write_bytes(primary + ascii_header + data + heap_header + heap_data)
    tb = table()
    ascii_tab = tb.fromfits(str(tmp_path / "ascii.table"), str(source), whichhdu=1)
    assert ascii_tab.getcoldesc("ID")["valueType"] == "int"
    assert ascii_tab.getcoldesc("FLUX")["valueType"] == "float"
    assert list(ascii_tab.getcol("NAME")) == ["north", "south"]
    np.testing.assert_array_equal(ascii_tab.getcol("ID"), [3, -3])
    np.testing.assert_array_equal(ascii_tab.getcol("FLUX"), [1.25, -2.5])
    heap_tab = tb.fromfits(str(tmp_path / "heap.table"), str(source), whichhdu=2)
    assert heap_tab.getcoldesc("DATA")["ndim"] == -1
    np.testing.assert_array_equal(heap_tab.getcell("DATA", 0), [1, 2, 3])
    np.testing.assert_array_equal(heap_tab.getcell("DATA", 1), [7])


def test_variable_length_tdim_text_uses_each_heap_record(tmp_path):
    source = tmp_path / "heap_tdim.fits"
    primary = _fits_header([("SIMPLE", True), ("BITPIX", 8), ("NAXIS", 0), ("EXTEND", True)])
    dims = b"(2,2)(4)"
    header = _fits_header([
        ("XTENSION", "BINTABLE"), ("BITPIX", 8), ("NAXIS", 2), ("NAXIS1", 24),
        ("NAXIS2", 2), ("PCOUNT", len(dims)), ("GCOUNT", 1), ("TFIELDS", 2),
        ("TTYPE1", "DATA"), ("TFORM1", "4E"),
        ("TTYPE2", "TDIM1"), ("TFORM2", "1PA"),
    ])
    rows = struct.pack(">4fii4fii", 1, 2, 3, 4, 5, 0, 5, 6, 7, 8, 3, 5)
    source.write_bytes(primary + header + (rows + dims).ljust(2880, b"\0"))
    out = table().fromfits(str(tmp_path / "shape.table"), str(source))
    np.testing.assert_array_equal(out.getcell("DATA", 0), [[1, 3], [2, 4]])
    np.testing.assert_array_equal(out.getcell("DATA", 1), [5, 6, 7, 8])
    assert list(out.getcol("TDIM1")) == ["(2,2)", "(4)"]


def test_named_memory_table_obeys_user_lock_and_keeps_empty_schema(tmp_path):
    import subprocess
    import sys

    source = tmp_path / "source.fits"
    _table_fits(source)
    path = str(tmp_path / "mem.table")
    tb = table()
    out = tb.fromfits(path, str(source), storage="memory")
    assert out.haslock(write=True)
    assert out.done()
    assert tb.open(path, lockoptions={"option": "user"}, nomodify=False)
    assert not tb.haslock(write=True)
    with pytest.raises(RuntimeError, match="should be locked"):
        tb.addrows(1)
    assert tb.lock(write=True, nattempts=1)
    probe = (
        "import sys; from casars.tools import table; t=table(); "
        "t.open(sys.argv[1], lockoptions={'option': 'user'}, nomodify=False); "
        "assert not t.lock(write=True, nattempts=1)"
    )
    subprocess.run([sys.executable, "-c", probe, path], check=True, timeout=10)
    assert tb.removecols(tb.colnames())
    assert tb.done()
    assert tb.open(path)
    assert tb.colnames() == []
    assert tb.nrows() == 2
