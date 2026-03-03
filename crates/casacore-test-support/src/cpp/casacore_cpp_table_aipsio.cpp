// AipsIO storage manager fixtures: scalar_primitives, fixed_array, keywords.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/StManAipsIO.h>

using casacore_shim::make_error;
static casacore_shim::TerminateGuard g_terminate_guard_aipsio;

namespace {

void write_scalar_primitives_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Bool>("col_bool"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("col_i32"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Double>("col_f64"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::String>("col_str"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 3);

    casacore::ScalarColumn<casacore::Bool> colBool(table, "col_bool");
    casacore::ScalarColumn<casacore::Int> colI32(table, "col_i32");
    casacore::ScalarColumn<casacore::Double> colF64(table, "col_f64");
    casacore::ScalarColumn<casacore::String> colStr(table, "col_str");

    colBool.put(0, casacore::True);
    colI32.put(0, 42);
    colF64.put(0, 1.5);
    colStr.put(0, "hello");

    colBool.put(1, casacore::False);
    colI32.put(1, -7);
    colF64.put(1, -99.5);
    colStr.put(1, "world");

    colBool.put(2, casacore::True);
    colI32.put(2, 0);
    colF64.put(2, 0.0);
    colStr.put(2, "");

    table.rwKeywordSet().define("observer", casacore::String("test-harness"));
    table.flush();
}

void verify_scalar_primitives_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);

    if (table.nrow() != 3) {
        throw std::runtime_error("expected 3 rows, got " +
                                 std::to_string(table.nrow()));
    }

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
}

void write_fixed_array_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    casacore::IPosition shape(2, 2, 3);
    td.addColumn(
        casacore::ArrayColumnDesc<casacore::Float>("data", shape,
                                                    casacore::ColumnDesc::Direct |
                                                    casacore::ColumnDesc::FixedShape));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 3);
    casacore::ArrayColumn<casacore::Float> colData(table, "data");

    {
        casacore::Array<casacore::Float> arr(shape);
        arr(casacore::IPosition(2, 0, 0)) = 1.0f;
        arr(casacore::IPosition(2, 1, 0)) = 2.0f;
        arr(casacore::IPosition(2, 0, 1)) = 3.0f;
        arr(casacore::IPosition(2, 1, 1)) = 4.0f;
        arr(casacore::IPosition(2, 0, 2)) = 5.0f;
        arr(casacore::IPosition(2, 1, 2)) = 6.0f;
        colData.put(0, arr);
    }
    {
        casacore::Array<casacore::Float> arr(shape);
        arr(casacore::IPosition(2, 0, 0)) = 7.0f;
        arr(casacore::IPosition(2, 1, 0)) = 8.0f;
        arr(casacore::IPosition(2, 0, 1)) = 9.0f;
        arr(casacore::IPosition(2, 1, 1)) = 10.0f;
        arr(casacore::IPosition(2, 0, 2)) = 11.0f;
        arr(casacore::IPosition(2, 1, 2)) = 12.0f;
        colData.put(1, arr);
    }
    {
        casacore::Array<casacore::Float> arr(shape, 0.0f);
        colData.put(2, arr);
    }

    table.flush();
}

void verify_fixed_array_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);

    if (table.nrow() != 3) throw std::runtime_error("expected 3 rows");

    casacore::ArrayColumn<casacore::Float> colData(table, "data");
    casacore::IPosition expectedShape(2, 2, 3);

    {
        casacore::Array<casacore::Float> arr = colData(0);
        if (!arr.shape().isEqual(expectedShape))
            throw std::runtime_error("row 0 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 1.0f)
            throw std::runtime_error("row 0 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 1)) != 4.0f)
            throw std::runtime_error("row 0 [1,1] mismatch");
    }
    {
        casacore::Array<casacore::Float> arr = colData(2);
        if (arr(casacore::IPosition(2, 0, 0)) != 0.0f)
            throw std::runtime_error("row 2 [0,0] mismatch");
    }
}

void write_keywords_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 2);
    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    colId.put(0, 1);
    colId.put(1, 2);

    table.rwKeywordSet().define("telescope", casacore::String("ALMA"));
    table.rwKeywordSet().define("version", casacore::Int(3));

    table.flush();
}

void verify_keywords_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);

    if (table.nrow() != 2) throw std::runtime_error("expected 2 rows");

    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    if (colId(0) != 1) throw std::runtime_error("row 0 id mismatch");
    if (colId(1) != 2) throw std::runtime_error("row 1 id mismatch");

    const casacore::TableRecord& kw = table.keywordSet();
    if (kw.asString("telescope") != "ALMA")
        throw std::runtime_error("keyword 'telescope' mismatch");
    if (kw.asInt("version") != 3)
        throw std::runtime_error("keyword 'version' mismatch");
}

} // anonymous namespace

extern "C" {

int32_t cpp_table_write_scalar_primitives(const char* path, char** out_error) {
    try { write_scalar_primitives_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_scalar_primitives"); return -1; }
}

int32_t cpp_table_verify_scalar_primitives(const char* path, char** out_error) {
    try { verify_scalar_primitives_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_scalar_primitives"); return -1; }
}

int32_t cpp_table_write_fixed_array(const char* path, char** out_error) {
    try { write_fixed_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_fixed_array"); return -1; }
}

int32_t cpp_table_verify_fixed_array(const char* path, char** out_error) {
    try { verify_fixed_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_fixed_array"); return -1; }
}

int32_t cpp_table_write_keywords(const char* path, char** out_error) {
    try { write_keywords_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_keywords"); return -1; }
}

int32_t cpp_table_verify_keywords(const char* path, char** out_error) {
    try { verify_keywords_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_keywords"); return -1; }
}

} // extern "C"
