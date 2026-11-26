"""Behavior and CASA-parity tests for TB-08: tb.query, tb.taql, and tb.calc."""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pytest

from casars import tools


def _create_taql_table(path: Path) -> tools.table:
    tb = tools.table()
    desc = {
        "si": {"valueType": "int"},
        "df": {"valueType": "double"},
        "sb": {"valueType": "boolean"},
        "ss": {"valueType": "string"},
        "sc": {"valueType": "dcomplex"},
        "f1": {"valueType": "int", "ndim": 1, "shape": [2]},
        "m2": {"valueType": "int", "ndim": 2, "shape": [2, 3]},
        "va": {"valueType": "double", "ndim": 1},
    }
    assert tb.create(str(path), desc, nrow=4)
    assert tb.putcol("si", [2, 1, 4, 2])
    assert tb.putcol("df", [2.5, 1.5, 4.5, 3.5])
    assert tb.putcol("sb", [True, False, True, False])
    assert tb.putcol("ss", ["b", "a", "d", "c"])
    assert tb.putcol("sc", [1 + 2j, 3 + 4j, 5 + 6j, 7 + 8j])
    assert tb.putcol("f1", np.array([[10, 20, 30, 40], [11, 21, 31, 41]], dtype=np.int32))
    m2 = np.zeros((2, 3, 4), dtype=np.int32)
    for r in range(4):
        for i in range(2):
            for j in range(3):
                m2[i, j, r] = 100 * r + 10 * i + j
    assert tb.putcol("m2", m2)
    for i, vals in enumerate([[1.0, 2.0], [3.0], [4.0, 5.0, 6.0], [7.0]]):
        assert tb.putcell("va", i, np.array(vals, dtype=np.float64))
    assert tb.flush()
    return tb


def test_query_taql_closed_and_bad_args(caplog: pytest.LogCaptureFixture) -> None:
    closed = tools.table()
    with caplog.at_level(logging.INFO, logger="casars.tools.table"):
        with pytest.raises(RuntimeError, match="Unable to create table"):
            closed.query("si > 0")
    assert any("No table opened." in r.message for r in caplog.records)
    assert any("No table specified, please open first" in r.message for r in caplog.records)

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="casars.tools.table"):
        with pytest.raises(RuntimeError, match="Unable to create table"):
            closed.taql("SELECT FROM foo")
    assert any("No table opened." in r.message for r in caplog.records)
    assert any("No table specified, please open first" in r.message for r in caplog.records)

    with pytest.raises(TypeError, match="argument _query must be a string"):
        closed.query(123)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="argument _taqlcommand must be a string"):
        closed.taql(123)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="argument _expr must be a string"):
        closed.calc(123)  # type: ignore[arg-type]


def test_query_filtering_sorting_and_projections(tmp_path: Path) -> None:
    p = tmp_path / "main.tab"
    tb = _create_taql_table(p)
    try:
        # Default 'String' fails like CASA
        with pytest.raises(RuntimeError, match="String"):
            tb.query()

        # Empty query selects all rows
        q_all = tb.query("")
        assert q_all.nrows() == 4
        assert q_all.rownumbers().tolist() == [0, 1, 2, 3]
        assert q_all.iswritable() is True
        q_all.close()

        # Multi-key sort + NODUPLICATES + UNIQUE + leading DESC
        q_multi = tb.query("", sortlist="sb ASC, si DESC")
        assert q_multi.rownumbers().tolist() == [3, 1, 2, 0]
        assert q_multi.getcol("sb").tolist() == [False, False, True, True]
        assert q_multi.getcol("si").tolist() == [2, 1, 4, 2]
        q_multi.close()

        q_nodup = tb.query("", sortlist="NODUPLICATES si")
        assert q_nodup.rownumbers().tolist() == [1, 0, 2]
        q_nodup.close()

        q_uniq = tb.query("", sortlist="UNIQUE si")
        assert q_uniq.rownumbers().tolist() == [1, 0, 2]
        q_uniq.close()

        q_lead_desc = tb.query("", sortlist="desc sb, si")
        assert q_lead_desc.rownumbers().tolist() == [2, 0, 3, 1]
        q_lead_desc.close()

        # Column projection view restricts columns (including getdesc) and allows write-through on visible columns
        q_proj = tb.query("si > 1", columns="df, si")
        assert q_proj.colnames() == ["df", "si"]
        assert list(q_proj.getdesc().keys()) == [
            "_define_hypercolumn_",
            "_keywords_",
            "_private_keywords_",
            "df",
            "si",
        ]
        assert q_proj.ncols() == 2
        assert q_proj.rownumbers().tolist() == [0, 2, 3]
        assert q_proj.getcol("si").tolist() == [2, 4, 2]
        with pytest.raises(RuntimeError, match="does not exist|unknown"):
            q_proj.getcol("sb")
        with pytest.raises(RuntimeError, match="does not exist"):
            q_proj.putcell("sb", 0, True)
        assert q_proj.putcell("df", 1, 88.5) is True
        assert tb.getcell("df", 2) == pytest.approx(88.5)
        assert tb.putcell("df", 2, 4.5) is True
        q_proj.close()

        # Persistent RefTable + registry tracking (clearlocks + open-target guard)
        out_ref = tmp_path / "ref.tab"
        q_ref = tb.query("si >= 2", name=str(out_ref))
        assert Path(q_ref.name()) == out_ref
        assert (out_ref / "table.dat").exists()
        assert q_ref.rownumbers().tolist() == [0, 2, 3]
        _ = q_ref.getcol("si")
        assert q_ref.haslock(False)
        assert tb.clearlocks() is True
        assert not q_ref.haslock(False)
        with pytest.raises(RuntimeError, match="still open|Cannot copy/rename"):
            tb.query("si >= 1", name=str(out_ref))
        q_ref.close()

        # Persistent selectrows(..., name=...) handle works with query/taql/calc without panic
        sel_ref = tmp_path / "sel_ref.tab"
        s_ref = tb.selectrows([3, 1], name=str(sel_ref))
        try:
            sq = s_ref.query("si >= 1")
            assert sq.rownumbers().tolist() == [3, 1]
            assert sq.getcol("si").tolist() == [2, 1]
            sq.close()
            st = s_ref.taql(f'SELECT FROM "{sel_ref}" WHERE si >= 1')
            assert st.rownumbers().tolist() == [3, 1]
            st.close()
            assert s_ref.calc(f'si FROM "{sel_ref}"').tolist() == [2, 1]
        finally:
            s_ref.close()

        # Nested query works on persistent RefTable, refused on transient query view
        reopened_ref = tools.table()
        assert reopened_ref.open(str(out_ref))
        q_nested = reopened_ref.query("df > 3.0")
        assert q_nested.rownumbers().tolist() == [2, 3]
        q_nested.close()
        reopened_ref.close()

        q_trans = tb.query("si >= 2")
        with pytest.raises(RuntimeError, match="does not exist"):
            q_trans.query("df > 3.0")
        q_trans.close()

        out_expr = tmp_path / "expr.tab"
        q_expr = tb.query("", columns="si + 1 AS si2, f1[0] AS f0", name=str(out_expr), style="python")
        assert q_expr.colnames() == ["si2", "f0"]
        assert q_expr.getcol("si2").tolist() == [3, 2, 5, 3]
        assert q_expr.getcol("f0").tolist() == [10, 20, 30, 40]
        assert q_expr.rownumbers().tolist() == [0, 1, 2, 3]
        assert q_expr.iswritable() is True
        q_expr.close()

        # Style checks: ROWNUMBER() and C-order vs Fortran-order
        q_rn_glish = tb.query("ROWNUMBER()==1")
        assert q_rn_glish.rownumbers().tolist() == [0]
        q_rn_glish.close()

        q_rn_py = tb.query("ROWNUMBER()==0", style="python")
        assert q_rn_py.rownumbers().tolist() == [0]
        q_rn_py.close()

        q_style_c = tb.query("m2[0,1] == 10", style="python")
        assert q_style_c.rownumbers().tolist() == [0]
        q_style_c.close()

        with pytest.raises(RuntimeError, match="invalid TaQL STYLE"):
            tb.query("si > 0", style="bogus")
    finally:
        tb.close()

    # Read-only parent query produces non-writable transient view
    tb_ro = tools.table()
    assert tb_ro.open(str(p), nomodify=True)
    try:
        q_ro = tb_ro.query("si > 1")
        assert q_ro.iswritable() is False
        assert q_ro.rownumbers().tolist() == [0, 2, 3]
        q_ro.close()
    finally:
        tb_ro.close()


def test_taql_select_count_and_mutations(tmp_path: Path) -> None:
    p = tmp_path / "main.tab"
    tb = _create_taql_table(p)
    try:
        with pytest.raises(RuntimeError, match="TaQL"):
            tb.taql()
        with pytest.raises(RuntimeError, match="TaQL"):
            tb.taql("")
        with pytest.raises(RuntimeError, match="isTable"):
            tb.taql("CALC 1 + 2")

        r_sel = tb.taql(f'SELECT FROM "{p}" WHERE si > 1')
        assert r_sel.nrows() == 3
        assert r_sel.rownumbers().tolist() == [0, 2, 3]
        r_sel.close()

        out_giving = tmp_path / "taql_out.tab"
        r_giv = tb.taql(f'SELECT si, df FROM "{p}" ORDERBY si DESC LIMIT 3 GIVING "{out_giving}"')
        assert Path(r_giv.name()) == out_giving
        assert r_giv.colnames() == ["si", "df"]
        assert r_giv.getcol("si").tolist() == [4, 2, 2]
        r_giv.close()

        r_mem = tb.taql(f'SELECT si FROM "{p}" ORDERBY si ASC LIMIT 2 GIVING memtab AS MEMORY')
        assert r_mem.nrows() == 2
        assert r_mem.getcol("si").tolist() == [1, 2]
        r_mem.close()

        r_cnt = tb.taql(f'COUNT(si) FROM "{p}"')
        assert r_cnt.nrows() == 3
        assert r_cnt.colnames() == ["si", "_COUNT_"]
        counts = dict(zip(r_cnt.getcol("si").tolist(), r_cnt.getcol("_COUNT_").tolist()))
        assert counts == {1: 1, 2: 2, 4: 1}
        r_cnt.close()

        # Self-overwriting GIVING on an external table and top-level GIVING [expr] are refused
        other = tmp_path / "other.tab"
        tb.copy(str(other), True).close()
        with pytest.raises(RuntimeError, match="same as the source table"):
            tb.taql(f'SELECT FROM "{other}" GIVING "{other}"')
        with pytest.raises(RuntimeError, match="GIVING tablename"):
            tb.taql(f'SELECT FROM "{p}" GIVING [si]')

        # taql()-returned persistent tools are registered for open-target protection
        g_open = tb.taql(f'SELECT si FROM "{p}" GIVING "{tmp_path / "held_taql.tab"}"')
        try:
            with pytest.raises(RuntimeError, match="still open|Cannot copy/rename"):
                tb.copy(g_open.name(), True)
        finally:
            g_open.close()
    finally:
        tb.close()

    # In-memory table supports taql SELECT and in-place UPDATE
    mem_tb = tools.table()
    assert mem_tb.create(
        str(tmp_path / "mem_src.tab"),
        {"si": {"valueType": "int"}, "df": {"valueType": "double"}},
        memtype="memory",
        nrow=2,
    )
    try:
        assert mem_tb.putcol("si", [10, 20])
        m_sel = mem_tb.taql(f'SELECT FROM "{tmp_path / "mem_src.tab"}" WHERE si > 10')
        assert m_sel.getcol("si").tolist() == [20]
        m_sel.close()
        m_upd = mem_tb.taql(f'UPDATE "{tmp_path / "mem_src.tab"}" SET df = 3.5 WHERE si == 20')
        m_upd.close()
        assert mem_tb.getcol("df").tolist() == [0.0, 3.5]
    finally:
        mem_tb.close()


def test_taql_count_and_update_insert_delete_persistence(tmp_path: Path) -> None:
    p = tmp_path / "mut.tab"
    tb = _create_taql_table(p)
    try:
        r_cnt = tb.taql(f'COUNT(si) FROM "{p}"')
        counts = dict(zip(r_cnt.getcol("si").tolist(), r_cnt.getcol("_COUNT_").tolist()))
        assert counts == {1: 1, 2: 2, 4: 1}
        r_cnt.close()

        r_upd = tb.taql(f'UPDATE "{p}" SET df = 99.0 WHERE si == 4')
        assert r_upd.nrows() == 1
        assert r_upd.rownumbers().tolist() == [2]
        assert r_upd.getcol("df").tolist() == [99.0]
        r_upd.close()

        # AutoLocking writable handle updates in place without needing resync()
        assert tb.getcol("df").tolist() == [2.5, 1.5, 99.0, 3.5]

        r_ins = tb.taql(f'INSERT INTO "{p}" (si, df, sb, ss, sc) VALUES (99, 9.5, False, "z", 0+0i)')
        assert r_ins.nrows() == 1
        assert r_ins.rownumbers().tolist() == [4]
        assert r_ins.getcol("si").tolist() == [99]
        r_ins.close()

        r_del = tb.taql(f'DELETE FROM "{p}" WHERE si == 99')
        r_del.close()
        assert tb.resync()
        assert tb.nrows() == 4
    finally:
        tb.close()

    # Opening nomodify=True still lets taql UPDATE persist to disk as CASA does,
    # and a mid-batch INSERT failure rolls back table.dat cleanly without corruption.
    tb_ro = tools.table()
    assert tb_ro.open(str(p), nomodify=True)
    try:
        with pytest.raises(RuntimeError):
            tb_ro.taql(f'INSERT INTO "{p}" (si, va) VALUES (55, [1.0]), (56, "bad")')
        check = tools.table()
        assert check.open(str(p))
        assert check.nrows() == 4
        check.close()

        r_up = tb_ro.taql(f'UPDATE "{p}" SET si = 77 WHERE si == 1')
        assert r_up.nrows() == 1
        assert r_up.getcol("si").tolist() == [77]
        r_up.close()
        assert tb_ro.resync()
        assert 77 in tb_ro.getcol("si").tolist()
    finally:
        tb_ro.close()


def test_calc_constants_table_exprs_and_subqueries(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, capfd: pytest.CaptureFixture[str]
) -> None:
    closed = tools.table()
    with caplog.at_level(logging.INFO, logger="casars.tools.table"):
        res = closed.calc("1 + 2")
    assert any("No table opened." in r.message for r in caplog.records)
    assert isinstance(res, np.ndarray)
    assert res.shape == (1,) and res.dtype == np.int64 and res.tolist() == [3]

    assert closed.calc("1 + 2.5").tolist() == [3.5]
    assert closed.calc('"hi"').tolist() == ["hi"]
    assert closed.calc("True").tolist() == [True]
    assert closed.calc("1 + 2i").tolist() == [1 + 2j]
    arr_lit = closed.calc("[1, 2, 3]")
    assert isinstance(arr_lit, dict) and arr_lit["0"].tolist() == [1, 2, 3]
    assert closed.calc("[10, 20][0]").tolist() == [10]

    with pytest.raises(RuntimeError, match="FROM"):
        closed.calc("si + 1")

    capfd.readouterr()
    res_show = closed.calc("1 + 2", showtaql=True)
    assert res_show.tolist() == [3]
    out_text = capfd.readouterr().out
    assert "TaQL string: using style base0, endincl, fortranorder calc 1 + 2" in out_text

    p = tmp_path / "calc.tab"
    tb = _create_taql_table(p)
    try:
        with pytest.raises(RuntimeError, match="FROM"):
            tb.calc("si + 1")

        assert tb.calc(f'si + 1 FROM "{p}"').tolist() == [3, 2, 5, 3]
        assert tb.calc(f'ss FROM "{p}"').tolist() == ["b", "a", "d", "c"]
        assert tb.calc(f'sb FROM "{p}"').tolist() == [True, False, True, False]
        assert tb.calc(f'sc FROM "{p}"').tolist() == [1 + 2j, 3 + 4j, 5 + 6j, 7 + 8j]

        f1_dict = tb.calc(f'f1 FROM "{p}"')
        assert list(f1_dict.keys()) == ["0", "1", "2", "3"]
        assert f1_dict["2"].tolist() == [30, 31]

        va_dict = tb.calc(f'va FROM "{p}"')
        assert va_dict["2"].tolist() == [4.0, 5.0, 6.0]

        # Single-element column subscripts preserve CASA shapes (1, nrows) / (1, 1, nrows)
        f1_0 = tb.calc(f'f1[0] FROM "{p}"')
        assert f1_0.shape == (1, 4) and f1_0.tolist() == [[10, 20, 30, 40]]
        f1_slice = tb.calc(f'f1[0:0] FROM "{p}"')
        assert isinstance(f1_slice, dict) and f1_slice["0"].tolist() == [10]
        assert tb.calc(f'f1[0] + 0 FROM "{p}"').shape == (4,)

        m2_f = tb.calc(f'm2[0,1] FROM "{p}"')
        assert m2_f.shape == (1, 1, 4) and m2_f.tolist() == [[[1, 101, 201, 301]]]
        m2_c = tb.calc(f'm2[0,1] FROM "{p}"', prefix="using style python")
        assert m2_c.shape == (1, 1, 4) and m2_c.tolist() == [[[10, 110, 210, 310]]]

        assert tb.calc(f'sum(f1) FROM "{p}"').tolist() == [21, 41, 61, 81]
        assert tb.calc(f'sum([select from "{p}" giving [si]])').tolist() == [9]
        assert tb.calc(f'si IN [SELECT FROM "{p}" WHERE si >= 4 GIVING [si]] FROM "{p}"').tolist() == [
            False,
            False,
            True,
            False,
        ]
        assert tb.calc(f'SUM(f1) BETWEEN 20 AND 50 FROM "{p}"').tolist() == [
            True,
            True,
            False,
            False,
        ]
        sub_arr = tb.calc(f'[select from "{p}" giving [si * 2]]')
        assert isinstance(sub_arr, dict) and sub_arr["0"].tolist() == [4, 2, 8, 4]

        with pytest.raises(RuntimeError, match="#indices mismatches array dimensionality"):
            tb.calc(f'f1[0, 1] FROM "{p}"')
        with pytest.raises(RuntimeError, match="aggregate function"):
            tb.calc(f'gmax(si) FROM "{p}"')
    finally:
        tb.close()

    pe = tmp_path / "empty.tab"
    tbe = tools.table()
    assert tbe.create(str(pe), {"si": {"valueType": "int"}}, nrow=0)
    assert tbe.flush()
    tbe.close()
    empty_si = closed.calc(f'si FROM "{pe}"')
    assert empty_si.shape == (0,) and empty_si.dtype == np.int64
    assert closed.calc(f'1 + 2 FROM "{pe}"').tolist() == [3]


def test_casa_live_parity_query_taql_calc(tmp_path: Path) -> None:
    casatools = pytest.importorskip("casatools")
    p_rs = tmp_path / "rs.tab"
    p_casa = tmp_path / "casa.tab"

    tb_rs = _create_taql_table(p_rs)
    tb_casa = casatools.table()
    tb_casa.open(str(p_rs), nomodify=True)
    # Build identical separate table for CASA mutations
    tb_rs.copy(str(p_casa), True).close()

    try:
        q_rs = tb_rs.query("si >= 2", sortlist="sb ASC, si DESC", columns="si, df")
        q_casa = tb_casa.query("si >= 2", sortlist="sb ASC, si DESC", columns="si, df")
        assert q_rs.rownumbers().tolist() == q_casa.rownumbers().tolist()
        assert q_rs.colnames() == q_casa.colnames()
        assert q_rs.getcol("si").tolist() == q_casa.getcol("si").tolist()
        assert np.allclose(q_rs.getcol("df"), q_casa.getcol("df"))
        q_rs.close()
        q_casa.close()

        for expr in [f'si + 1 FROM "{p_rs}"', f'f1[0] FROM "{p_rs}"', f'sum(f1) FROM "{p_rs}"', f'm2[0,1] FROM "{p_rs}"']:
            v_rs = tb_rs.calc(expr)
            v_casa = tb_casa.calc(expr)
            assert v_rs.shape == v_casa.shape
            assert v_rs.tolist() == v_casa.tolist()
    finally:
        tb_rs.close()
        tb_casa.close()
