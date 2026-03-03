// IncrementalStMan fixtures: ism_scalar_primitives, ism_slowly_changing,
// ism_complex_scalars.
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

} // extern "C"
