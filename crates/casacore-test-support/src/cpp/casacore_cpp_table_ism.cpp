// IncrementalStMan fixtures: ism_scalar_primitives, ism_slowly_changing,
// ism_complex_scalars, ism_typed_arrays, ism_complex_arrays, ism_column_keywords.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/IncrementalStMan.h>

using casacore_shim::make_error;

// ===== ISM scalar_primitives fixture =====

extern "C" {

int32_t cpp_table_write_ism_scalar_primitives(const char* path, char** out_error) {
    try {
        casacore::TableDesc td("", casacore::TableDesc::Scratch);
        td.addColumn(casacore::ScalarColumnDesc<casacore::Bool>("col_bool"));
        td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("col_i32"));
        td.addColumn(casacore::ScalarColumnDesc<casacore::Double>("col_f64"));
        td.addColumn(casacore::ScalarColumnDesc<casacore::String>("col_str"));

        casacore::SetupNewTable setup(path, td, casacore::Table::New);
        casacore::IncrementalStMan stman;
        setup.bindAll(stman);

        casacore::Table table(setup, 3);

        casacore::ScalarColumn<casacore::Bool> colBool(table, "col_bool");
        casacore::ScalarColumn<casacore::Int> colI32(table, "col_i32");
        casacore::ScalarColumn<casacore::Double> colF64(table, "col_f64");
        casacore::ScalarColumn<casacore::String> colStr(table, "col_str");

        colBool.put(0, casacore::True); colI32.put(0, 42);
        colF64.put(0, 1.5); colStr.put(0, "hello");

        colBool.put(1, casacore::False); colI32.put(1, -7);
        colF64.put(1, -99.5); colStr.put(1, "world");

        colBool.put(2, casacore::True); colI32.put(2, 0);
        colF64.put(2, 0.0); colStr.put(2, "");

        table.rwKeywordSet().define("observer", casacore::String("test-harness"));
        table.flush();
        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_ism_scalar_primitives"); return -1; }
}

int32_t cpp_table_verify_ism_scalar_primitives(const char* path, char** out_error) {
    try {
        casacore::Table table(path, casacore::Table::Old);
        if (table.nrow() != 3)
            throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));

        casacore::ScalarColumn<casacore::Bool> colBool(table, "col_bool");
        casacore::ScalarColumn<casacore::Int> colI32(table, "col_i32");
        casacore::ScalarColumn<casacore::Double> colF64(table, "col_f64");
        casacore::ScalarColumn<casacore::String> colStr(table, "col_str");

        if (colBool(0) != casacore::True) throw std::runtime_error("row 0 col_bool mismatch");
        if (colI32(0) != 42) throw std::runtime_error("row 0 col_i32 mismatch");
        if (colF64(0) != 1.5) throw std::runtime_error("row 0 col_f64 mismatch");
        if (colStr(0) != "hello") throw std::runtime_error("row 0 col_str mismatch");

        if (colBool(1) != casacore::False) throw std::runtime_error("row 1 col_bool mismatch");
        if (colI32(1) != -7) throw std::runtime_error("row 1 col_i32 mismatch");
        if (colF64(1) != -99.5) throw std::runtime_error("row 1 col_f64 mismatch");
        if (colStr(1) != "world") throw std::runtime_error("row 1 col_str mismatch");

        if (colBool(2) != casacore::True) throw std::runtime_error("row 2 col_bool mismatch");
        if (colI32(2) != 0) throw std::runtime_error("row 2 col_i32 mismatch");
        if (colF64(2) != 0.0) throw std::runtime_error("row 2 col_f64 mismatch");
        if (colStr(2) != "") throw std::runtime_error("row 2 col_str mismatch");

        const casacore::TableRecord& kw = table.keywordSet();
        if (!kw.isDefined("observer")) throw std::runtime_error("keyword 'observer' missing");
        if (kw.asString("observer") != "test-harness")
            throw std::runtime_error("keyword 'observer' mismatch");

        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_ism_scalar_primitives"); return -1; }
}

// ===== ISM slowly_changing fixture =====

int32_t cpp_table_write_ism_slowly_changing(const char* path, char** out_error) {
    try {
        casacore::TableDesc td("", casacore::TableDesc::Scratch);
        td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("SCAN_NUMBER"));
        td.addColumn(casacore::ScalarColumnDesc<casacore::Bool>("FLAG"));

        casacore::SetupNewTable setup(path, td, casacore::Table::New);
        casacore::IncrementalStMan stman;
        setup.bindAll(stman);

        casacore::Table table(setup, 10);

        casacore::ScalarColumn<casacore::Int> colScan(table, "SCAN_NUMBER");
        casacore::ScalarColumn<casacore::Bool> colFlag(table, "FLAG");

        casacore::Int scans[]  = {0, 0, 0, 1, 1, 1, 1, 2, 2, 2};
        casacore::Bool flags[] = {casacore::True, casacore::True, casacore::True,
                                  casacore::True, casacore::True,
                                  casacore::False, casacore::False,
                                  casacore::False, casacore::True, casacore::True};

        for (casacore::uInt i = 0; i < 10; i++) {
            colScan.put(i, scans[i]);
            colFlag.put(i, flags[i]);
        }

        table.flush();
        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_ism_slowly_changing"); return -1; }
}

int32_t cpp_table_verify_ism_slowly_changing(const char* path, char** out_error) {
    try {
        casacore::Table table(path, casacore::Table::Old);
        if (table.nrow() != 10)
            throw std::runtime_error("expected 10 rows, got " + std::to_string(table.nrow()));

        casacore::ScalarColumn<casacore::Int> colScan(table, "SCAN_NUMBER");
        casacore::ScalarColumn<casacore::Bool> colFlag(table, "FLAG");

        casacore::Int expected_scans[]  = {0, 0, 0, 1, 1, 1, 1, 2, 2, 2};
        casacore::Bool expected_flags[] = {casacore::True, casacore::True, casacore::True,
                                           casacore::True, casacore::True,
                                           casacore::False, casacore::False,
                                           casacore::False, casacore::True, casacore::True};

        for (casacore::uInt i = 0; i < 10; i++) {
            if (colScan(i) != expected_scans[i])
                throw std::runtime_error("SCAN_NUMBER mismatch at row " + std::to_string(i) +
                    ": expected " + std::to_string(expected_scans[i]) +
                    ", got " + std::to_string(colScan(i)));
            if (colFlag(i) != expected_flags[i])
                throw std::runtime_error("FLAG mismatch at row " + std::to_string(i));
        }

        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_ism_slowly_changing"); return -1; }
}

// ===== ISM complex_scalars fixture =====

int32_t cpp_table_write_ism_complex_scalars(const char* path, char** out_error) {
    try {
        casacore::TableDesc td("", casacore::TableDesc::Scratch);
        td.addColumn(casacore::ScalarColumnDesc<casacore::Complex>("col_c32"));
        td.addColumn(casacore::ScalarColumnDesc<casacore::DComplex>("col_c64"));

        casacore::SetupNewTable setup(path, td, casacore::Table::New);
        casacore::IncrementalStMan stman;
        setup.bindAll(stman);

        casacore::Table table(setup, 3);

        casacore::ScalarColumn<casacore::Complex> colC32(table, "col_c32");
        casacore::ScalarColumn<casacore::DComplex> colC64(table, "col_c64");

        colC32.put(0, casacore::Complex(1.0f, 2.0f));
        colC64.put(0, casacore::DComplex(3.0, 4.0));

        colC32.put(1, casacore::Complex(0.0f, 0.0f));
        colC64.put(1, casacore::DComplex(0.0, 0.0));

        colC32.put(2, casacore::Complex(-5.5f, 7.25f));
        colC64.put(2, casacore::DComplex(-1e10, 1e-10));

        table.flush();
        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_ism_complex_scalars"); return -1; }
}

int32_t cpp_table_verify_ism_complex_scalars(const char* path, char** out_error) {
    try {
        casacore::Table table(path, casacore::Table::Old);
        if (table.nrow() != 3)
            throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));

        casacore::ScalarColumn<casacore::Complex> colC32(table, "col_c32");
        casacore::ScalarColumn<casacore::DComplex> colC64(table, "col_c64");

        {
            auto v = colC32(0);
            if (v.real() != 1.0f || v.imag() != 2.0f)
                throw std::runtime_error("row 0 col_c32 mismatch");
        }
        {
            auto v = colC64(0);
            if (v.real() != 3.0 || v.imag() != 4.0)
                throw std::runtime_error("row 0 col_c64 mismatch");
        }
        {
            auto v = colC32(1);
            if (v.real() != 0.0f || v.imag() != 0.0f)
                throw std::runtime_error("row 1 col_c32 mismatch");
        }
        {
            auto v = colC64(1);
            if (v.real() != 0.0 || v.imag() != 0.0)
                throw std::runtime_error("row 1 col_c64 mismatch");
        }
        {
            auto v = colC32(2);
            if (v.real() != -5.5f || v.imag() != 7.25f)
                throw std::runtime_error("row 2 col_c32 mismatch");
        }
        {
            auto v = colC64(2);
            if (v.real() != -1e10 || v.imag() != 1e-10)
                throw std::runtime_error("row 2 col_c64 mismatch");
        }

        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_ism_complex_scalars"); return -1; }
}

// ===== ISM typed_arrays fixture =====

int32_t cpp_table_write_ism_typed_arrays(const char* path, char** out_error) {
    try {
        casacore::TableDesc td("", casacore::TableDesc::Scratch);
        td.addColumn(casacore::ArrayColumnDesc<casacore::Int>("arr_i32",
            casacore::IPosition(1, 4), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));
        td.addColumn(casacore::ArrayColumnDesc<casacore::Double>("arr_f64",
            casacore::IPosition(2, 2, 2), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));
        td.addColumn(casacore::ArrayColumnDesc<casacore::Bool>("arr_bool",
            casacore::IPosition(1, 3), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));

        casacore::SetupNewTable setup(path, td, casacore::Table::New);
        casacore::IncrementalStMan stman;
        setup.bindAll(stman);
        casacore::Table table(setup, 3);

        casacore::ArrayColumn<casacore::Int> colI32(table, "arr_i32");
        casacore::ArrayColumn<casacore::Double> colF64(table, "arr_f64");
        casacore::ArrayColumn<casacore::Bool> colBool(table, "arr_bool");

        // Row 0
        {
            casacore::Vector<casacore::Int> v(4); v(0)=10; v(1)=20; v(2)=30; v(3)=40;
            colI32.put(0, v);
            casacore::Array<casacore::Double> a(casacore::IPosition(2, 2, 2));
            a(casacore::IPosition(2,0,0))=1.1; a(casacore::IPosition(2,1,0))=2.2;
            a(casacore::IPosition(2,0,1))=3.3; a(casacore::IPosition(2,1,1))=4.4;
            colF64.put(0, a);
            casacore::Vector<casacore::Bool> b(3); b(0)=casacore::True; b(1)=casacore::False; b(2)=casacore::True;
            colBool.put(0, b);
        }
        // Row 1
        {
            casacore::Vector<casacore::Int> v(4); v(0)=-1; v(1)=-2; v(2)=-3; v(3)=-4;
            colI32.put(1, v);
            casacore::Array<casacore::Double> a(casacore::IPosition(2, 2, 2));
            a(casacore::IPosition(2,0,0))=5.5; a(casacore::IPosition(2,1,0))=6.6;
            a(casacore::IPosition(2,0,1))=7.7; a(casacore::IPosition(2,1,1))=8.8;
            colF64.put(1, a);
            casacore::Vector<casacore::Bool> b(3, casacore::False);
            colBool.put(1, b);
        }
        // Row 2
        {
            casacore::Vector<casacore::Int> v(4, 0); colI32.put(2, v);
            casacore::Array<casacore::Double> a(casacore::IPosition(2, 2, 2), 0.0); colF64.put(2, a);
            casacore::Vector<casacore::Bool> b(3, casacore::True); colBool.put(2, b);
        }
        table.flush();
        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_ism_typed_arrays"); return -1; }
}

int32_t cpp_table_verify_ism_typed_arrays(const char* path, char** out_error) {
    try {
        casacore::Table table(path, casacore::Table::Old);
        if (table.nrow() != 3) throw std::runtime_error("expected 3 rows");

        casacore::ArrayColumn<casacore::Int> colI32(table, "arr_i32");
        casacore::ArrayColumn<casacore::Double> colF64(table, "arr_f64");
        casacore::ArrayColumn<casacore::Bool> colBool(table, "arr_bool");

        // Row 0
        { casacore::Vector<casacore::Int> v(colI32(0)); if (v(0)!=10||v(1)!=20||v(2)!=30||v(3)!=40) throw std::runtime_error("row 0 arr_i32 mismatch"); }
        { casacore::Array<casacore::Double> a=colF64(0); if (a(casacore::IPosition(2,0,0))!=1.1||a(casacore::IPosition(2,1,1))!=4.4) throw std::runtime_error("row 0 arr_f64 mismatch"); }
        { casacore::Vector<casacore::Bool> b(colBool(0)); if (b(0)!=casacore::True||b(1)!=casacore::False||b(2)!=casacore::True) throw std::runtime_error("row 0 arr_bool mismatch"); }

        // Row 1
        { casacore::Vector<casacore::Int> v(colI32(1)); if (v(0)!=-1||v(1)!=-2||v(2)!=-3||v(3)!=-4) throw std::runtime_error("row 1 arr_i32 mismatch"); }
        { casacore::Array<casacore::Double> a=colF64(1); if (a(casacore::IPosition(2,0,0))!=5.5||a(casacore::IPosition(2,1,1))!=8.8) throw std::runtime_error("row 1 arr_f64 mismatch"); }
        { casacore::Vector<casacore::Bool> b(colBool(1)); if (b(0)!=casacore::False||b(1)!=casacore::False||b(2)!=casacore::False) throw std::runtime_error("row 1 arr_bool mismatch"); }

        // Row 2
        { casacore::Vector<casacore::Int> v(colI32(2)); if (v(0)!=0||v(1)!=0||v(2)!=0||v(3)!=0) throw std::runtime_error("row 2 arr_i32 mismatch"); }
        { casacore::Array<casacore::Double> a=colF64(2); if (a(casacore::IPosition(2,0,0))!=0.0||a(casacore::IPosition(2,1,1))!=0.0) throw std::runtime_error("row 2 arr_f64 mismatch"); }
        { casacore::Vector<casacore::Bool> b(colBool(2)); if (b(0)!=casacore::True||b(1)!=casacore::True||b(2)!=casacore::True) throw std::runtime_error("row 2 arr_bool mismatch"); }

        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_ism_typed_arrays"); return -1; }
}

// ===== ISM complex_arrays fixture =====

int32_t cpp_table_write_ism_complex_arrays(const char* path, char** out_error) {
    try {
        casacore::TableDesc td("", casacore::TableDesc::Scratch);
        td.addColumn(casacore::ArrayColumnDesc<casacore::Complex>("arr_c32",
            casacore::IPosition(1, 2), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));
        td.addColumn(casacore::ArrayColumnDesc<casacore::DComplex>("arr_c64",
            casacore::IPosition(1, 2), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));

        casacore::SetupNewTable setup(path, td, casacore::Table::New);
        casacore::IncrementalStMan stman;
        setup.bindAll(stman);
        casacore::Table table(setup, 3);

        casacore::ArrayColumn<casacore::Complex> colC32(table, "arr_c32");
        casacore::ArrayColumn<casacore::DComplex> colC64(table, "arr_c64");

        // Row 0
        {
            casacore::Vector<casacore::Complex> vc(2);
            vc(0) = casacore::Complex(1.0f, 2.0f); vc(1) = casacore::Complex(3.0f, 4.0f);
            colC32.put(0, vc);
            casacore::Vector<casacore::DComplex> vd(2);
            vd(0) = casacore::DComplex(5.0, 6.0); vd(1) = casacore::DComplex(7.0, 8.0);
            colC64.put(0, vd);
        }
        // Row 1: zeros
        {
            casacore::Vector<casacore::Complex> vc(2, casacore::Complex(0,0)); colC32.put(1, vc);
            casacore::Vector<casacore::DComplex> vd(2, casacore::DComplex(0,0)); colC64.put(1, vd);
        }
        // Row 2
        {
            casacore::Vector<casacore::Complex> vc(2);
            vc(0) = casacore::Complex(-5.5f, 7.25f); vc(1) = casacore::Complex(1e3f, -1e3f);
            colC32.put(2, vc);
            casacore::Vector<casacore::DComplex> vd(2);
            vd(0) = casacore::DComplex(-1e10, 1e-10); vd(1) = casacore::DComplex(1e10, -1e-10);
            colC64.put(2, vd);
        }
        table.flush();
        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_ism_complex_arrays"); return -1; }
}

int32_t cpp_table_verify_ism_complex_arrays(const char* path, char** out_error) {
    try {
        casacore::Table table(path, casacore::Table::Old);
        if (table.nrow() != 3) throw std::runtime_error("expected 3 rows");

        casacore::ArrayColumn<casacore::Complex> colC32(table, "arr_c32");
        casacore::ArrayColumn<casacore::DComplex> colC64(table, "arr_c64");

        { casacore::Vector<casacore::Complex> v(colC32(0)); if (v(0)!=casacore::Complex(1.0f,2.0f)||v(1)!=casacore::Complex(3.0f,4.0f)) throw std::runtime_error("row 0 arr_c32 mismatch"); }
        { casacore::Vector<casacore::DComplex> v(colC64(0)); if (v(0)!=casacore::DComplex(5.0,6.0)||v(1)!=casacore::DComplex(7.0,8.0)) throw std::runtime_error("row 0 arr_c64 mismatch"); }

        { casacore::Vector<casacore::Complex> v(colC32(1)); if (v(0)!=casacore::Complex(0,0)||v(1)!=casacore::Complex(0,0)) throw std::runtime_error("row 1 arr_c32 mismatch"); }
        { casacore::Vector<casacore::DComplex> v(colC64(1)); if (v(0)!=casacore::DComplex(0,0)||v(1)!=casacore::DComplex(0,0)) throw std::runtime_error("row 1 arr_c64 mismatch"); }

        { casacore::Vector<casacore::Complex> v(colC32(2)); if (v(0)!=casacore::Complex(-5.5f,7.25f)||v(1)!=casacore::Complex(1e3f,-1e3f)) throw std::runtime_error("row 2 arr_c32 mismatch"); }
        { casacore::Vector<casacore::DComplex> v(colC64(2)); if (v(0)!=casacore::DComplex(-1e10,1e-10)||v(1)!=casacore::DComplex(1e10,-1e-10)) throw std::runtime_error("row 2 arr_c64 mismatch"); }

        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_ism_complex_arrays"); return -1; }
}

// ===== ISM column_keywords fixture =====

int32_t cpp_table_write_ism_column_keywords(const char* path, char** out_error) {
    try {
        casacore::TableDesc td("", casacore::TableDesc::Scratch);
        td.addColumn(casacore::ScalarColumnDesc<casacore::Double>("flux"));
        td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));

        casacore::SetupNewTable setup(path, td, casacore::Table::New);
        casacore::IncrementalStMan stman;
        setup.bindAll(stman);
        casacore::Table table(setup, 2);

        casacore::ScalarColumn<casacore::Double> colFlux(table, "flux");
        casacore::ScalarColumn<casacore::Int> colId(table, "id");
        colFlux.put(0, 1.5); colId.put(0, 1);
        colFlux.put(1, 2.7); colId.put(1, 2);

        table.rwKeywordSet().define("telescope", casacore::String("VLA"));
        colFlux.rwKeywordSet().define("unit", casacore::String("Jy"));
        colFlux.rwKeywordSet().define("ref_frame", casacore::String("LSRK"));
        colId.rwKeywordSet().define("description", casacore::String("source identifier"));

        table.flush();
        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_ism_column_keywords"); return -1; }
}

int32_t cpp_table_verify_ism_column_keywords(const char* path, char** out_error) {
    try {
        casacore::Table table(path, casacore::Table::Old);
        if (table.nrow() != 2) throw std::runtime_error("expected 2 rows");

        casacore::ScalarColumn<casacore::Double> colFlux(table, "flux");
        casacore::ScalarColumn<casacore::Int> colId(table, "id");
        if (colFlux(0) != 1.5 || colFlux(1) != 2.7) throw std::runtime_error("flux mismatch");
        if (colId(0) != 1 || colId(1) != 2) throw std::runtime_error("id mismatch");

        if (table.keywordSet().asString("telescope") != "VLA") throw std::runtime_error("table kw mismatch");
        if (colFlux.keywordSet().asString("unit") != "Jy") throw std::runtime_error("flux unit kw mismatch");
        if (colFlux.keywordSet().asString("ref_frame") != "LSRK") throw std::runtime_error("flux ref_frame kw mismatch");
        if (colId.keywordSet().asString("description") != "source identifier") throw std::runtime_error("id description kw mismatch");

        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_ism_column_keywords"); return -1; }
}

} // extern "C"
