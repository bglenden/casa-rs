# SPDX-License-Identifier: LGPL-3.0-or-later
"""CASA 6.7.6.14's separate flat import and bracketed export formats."""

import numpy as np
import pytest

from casars.tools import table


def test_fromascii_with_keywords_and_variable_shape(tmp_path):
    header = tmp_path / "header"
    header.write_text(
        '.key\nX I 1,2,3\nY A "abc def"\n.endkey\n'
        '.key C\nUNIT A meters\n.endkey\n'
        'I,C\nI,"D0,2"\n'
    )
    data = tmp_path / "values"
    data.write_text("# note\n2,1,2,3\n4,9\n5,8\n")
    tb = table()
    assert tb.fromascii(
        str(tmp_path / "result.table"), str(data), str(header),
        sep=",", commentmarker="#", lastline=3,
    )
    assert not tb.iswritable()
    assert tb.colnames() == ["I", "C"]
    assert tb.getcoldesc("C")["ndim"] == -1
    assert tb.getcolkeywords("C") == {"UNIT": "meters"}
    assert tb.getkeyword("Y") == "abc def"
    np.testing.assert_array_equal(tb.getkeyword("X"), [1, 2, 3])
    np.testing.assert_array_equal(tb.getcol("I"), [2, 4])
    np.testing.assert_array_equal(tb.getcell("C", 0), [[1, 3], [2, 0]])
    np.testing.assert_array_equal(tb.getcell("C", 1), [[9, 0]])


def test_ascii_export_matches_casa_bytes(tmp_path):
    data = tmp_path / "legacy"
    data.write_text(
        'ID FLAT C Z W\nI D2,2 X Z A\n'
        '9 1 2 3 4 3 4 2 90 "two words"\n'
        '10 1 2 3 4 5 6 2 180 single\n'
    )
    tb = table()
    assert tb.fromascii(str(tmp_path / "result.table"), str(data), sep=" ", nomodify=False)
    assert not tb.iswritable()  # CASA ignores nomodify in fromascii.
    out = tmp_path / "export"
    assert tb.toasciifmt(str(out), sep=",")
    assert out.read_text() == (
        'ID,FLAT,C,Z,W\nI,D[2,2],X,X,A\n'
        '9,[[1,2],[3,4]],(3,4),(-8.74227766e-08,2),"two words"\n'
        '10,[[1,2],[3,4]],(5,6),(-2,-1.74845553e-07),"single"\n'
    )
    view = tb.selectrows([1, 0])
    header = tmp_path / "header"
    assert view.toasciifmt(str(out), str(header), ["W", "ID"], ";")
    assert header.read_text() == "W;ID\nA;I\n"
    assert out.read_text() == '"single";10\n"two words";9\n'
    with pytest.raises(RuntimeError, match=r"Table column missing is unknown"):
        tb.toasciifmt(str(out), columns=["missing"])


def test_fromascii_auto_header_explicit_header_and_failure_state(tmp_path):
    data = tmp_path / "data"
    data.write_text('skip this\n# skip\n1 2.5 "hi there"\n2 3 blah\n')
    tb = table()
    assert tb.toasciifmt(str(tmp_path / "unused")) is False
    dest = str(tmp_path / "result.table")
    assert tb.fromascii(
        dest, str(data), autoheader=True, autoshape=[3], sep=" ",
        commentmarker="#", firstline=2,
    )
    assert tb.colnames() == ["Column1", "Column2", "Column3"]
    assert list(tb.getcol("Column3")) == ["hi there", "blah"]
    assert tb.fromascii(
        dest, str(data), sep=" ", firstline=3, lastline=3,
        columnnames=["COUNT", "MAG", "COMMENT"], datatypes=["i", "d", "a"],
    )
    assert tb.nrows() == 1
    assert tb.getcell("COMMENT", 0) == "hi there"
    with pytest.raises(RuntimeError, match="separator must be 1 char"):
        tb.fromascii(dest, str(data), autoheader=True)
    assert tb.name() == ""


def test_toasciifmt_record_cells(tmp_path):
    tb = table()
    assert tb.create(str(tmp_path / "result.table"), {"R": {"valueType": "record"}}, nrow=2)
    assert tb.putcell(
        "R", 0, {"abc": 12, "arr": np.array([1.0, 2.0]), "deep": {"n": True},
                 "delta": 0.1, "text": "word"},
    )
    out = tmp_path / "record"
    assert tb.toasciifmt(str(out))
    assert out.read_text() == (
        'R\nREC\n{  abc: Int 12\n  arr: Double array with shape [2]\n'
        '    [1, 2]\n  deep: {\n    n: Bool 1\n  }\n  delta: Double 0.1\n'
        '  text: String "word"\n}\n{}\n'
    )


def test_fromascii_angles_use_casa_sexagesimal_rules(tmp_path):
    data = tmp_path / "angles"
    data.write_text(
        'RA DEC\nHMS DMS\n12h 12d\n12h30m 12d30m\n'
        '"12 h 30 m" "+12 d 30 m"\n"12:30:00" "-12:30:00"\n1.5 1.5\n'
    )
    tb = table()
    assert tb.fromascii(str(tmp_path / "angles.table"), str(data), sep=" ")
    np.testing.assert_allclose(tb.getcol("RA"), [np.pi, *np.deg2rad([187.5] * 3), 1.5])
    np.testing.assert_allclose(tb.getcol("DEC"), [*np.deg2rad([12, 12.5, 12.5, -12.5]), 1.5])
