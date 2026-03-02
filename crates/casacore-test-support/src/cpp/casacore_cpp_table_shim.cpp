// C++ table shim for casacore interop testing.
// Provides C ABI functions to write and verify casacore tables
// with deterministic fixture data, matching the Rust-side fixtures.

#include <casacore/casa/aips.h>
#include <casacore/casa/BasicSL/String.h>
#include <casacore/tables/Tables/Table.h>
#include <casacore/tables/Tables/TableDesc.h>
#include <casacore/tables/Tables/SetupNewTab.h>
#include <casacore/tables/Tables/ScaColDesc.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/ScalarColumn.h>
#include <casacore/tables/Tables/ArrayColumn.h>
#include <casacore/tables/Tables/TableRecord.h>
#include <casacore/tables/DataMan/StManAipsIO.h>
#include <casacore/tables/DataMan/StandardStMan.h>
#include <casacore/casa/Arrays/Array.h>
#include <casacore/casa/Arrays/IPosition.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

// Install a custom terminate handler that prints a stack trace
// before aborting, to help diagnose exceptions in destructors.
struct TerminateGuard {
    std::terminate_handler prev;
    TerminateGuard() {
        prev = std::set_terminate([] {
            std::cerr << "[casacore_cpp_table_shim] std::terminate called";
            auto eptr = std::current_exception();
            if (eptr) {
                try {
                    std::rethrow_exception(eptr);
                } catch (const std::exception& e) {
                    std::cerr << ": " << e.what();
                } catch (...) {
                    std::cerr << ": unknown exception";
                }
            }
            std::cerr << std::endl;
            std::abort();
        });
    }
    ~TerminateGuard() { std::set_terminate(prev); }
};

static TerminateGuard g_terminate_guard;

char* make_error(const std::string& msg) {
    char* result = static_cast<char*>(std::malloc(msg.size() + 1));
    if (result) {
        std::memcpy(result, msg.c_str(), msg.size() + 1);
    }
    return result;
}

// ===== scalar_primitives fixture =====
// Schema: col_bool(Bool), col_i32(Int), col_f64(Double), col_str(String)
// 3 rows with deterministic values matching Rust side.
// Table keyword: observer = "test-harness"

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

    // Row 0
    colBool.put(0, casacore::True);
    colI32.put(0, 42);
    colF64.put(0, 1.5);
    colStr.put(0, "hello");

    // Row 1
    colBool.put(1, casacore::False);
    colI32.put(1, -7);
    colF64.put(1, -99.5);
    colStr.put(1, "world");

    // Row 2
    colBool.put(2, casacore::True);
    colI32.put(2, 0);
    colF64.put(2, 0.0);
    colStr.put(2, "");

    // Table keyword
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

    // Row 0
    if (colBool(0) != casacore::True)
        throw std::runtime_error("row 0 col_bool mismatch");
    if (colI32(0) != 42) throw std::runtime_error("row 0 col_i32 mismatch");
    if (colF64(0) != 1.5) throw std::runtime_error("row 0 col_f64 mismatch");
    if (colStr(0) != "hello")
        throw std::runtime_error("row 0 col_str mismatch");

    // Row 1
    if (colBool(1) != casacore::False)
        throw std::runtime_error("row 1 col_bool mismatch");
    if (colI32(1) != -7) throw std::runtime_error("row 1 col_i32 mismatch");
    if (colF64(1) != -99.5)
        throw std::runtime_error("row 1 col_f64 mismatch");
    if (colStr(1) != "world")
        throw std::runtime_error("row 1 col_str mismatch");

    // Row 2
    if (colBool(2) != casacore::True)
        throw std::runtime_error("row 2 col_bool mismatch");
    if (colI32(2) != 0) throw std::runtime_error("row 2 col_i32 mismatch");
    if (colF64(2) != 0.0) throw std::runtime_error("row 2 col_f64 mismatch");
    if (colStr(2) != "")
        throw std::runtime_error("row 2 col_str mismatch");

    // Table keywords
    const casacore::TableRecord& kw = table.keywordSet();
    if (!kw.isDefined("observer"))
        throw std::runtime_error("keyword 'observer' missing");
    if (kw.asString("observer") != "test-harness")
        throw std::runtime_error("keyword 'observer' mismatch");
}

// ===== fixed_array fixture =====
// Schema: data(Array<Float>, shape=[2,3])
// 3 rows of deterministic data matching Rust side.

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

    // Row 0: [1,2,3,4,5,6] in column-major (Fortran order)
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
    // Row 1
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
    // Row 2 (all zeros)
    {
        casacore::Array<casacore::Float> arr(shape, 0.0f);
        colData.put(2, arr);
    }

    table.flush();
}

void verify_fixed_array_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);

    if (table.nrow() != 3)
        throw std::runtime_error("expected 3 rows");

    casacore::ArrayColumn<casacore::Float> colData(table, "data");
    casacore::IPosition expectedShape(2, 2, 3);

    // Row 0
    {
        casacore::Array<casacore::Float> arr = colData(0);
        if (!arr.shape().isEqual(expectedShape))
            throw std::runtime_error("row 0 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 1.0f)
            throw std::runtime_error("row 0 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 1)) != 4.0f)
            throw std::runtime_error("row 0 [1,1] mismatch");
    }
    // Row 2 (all zeros)
    {
        casacore::Array<casacore::Float> arr = colData(2);
        if (arr(casacore::IPosition(2, 0, 0)) != 0.0f)
            throw std::runtime_error("row 2 [0,0] mismatch");
    }
}

// ===== keywords fixture =====
// Schema: id(Int), table keywords: telescope="ALMA", version=3

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

    if (table.nrow() != 2)
        throw std::runtime_error("expected 2 rows");

    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    if (colId(0) != 1) throw std::runtime_error("row 0 id mismatch");
    if (colId(1) != 2) throw std::runtime_error("row 1 id mismatch");

    const casacore::TableRecord& kw = table.keywordSet();
    if (kw.asString("telescope") != "ALMA")
        throw std::runtime_error("keyword 'telescope' mismatch");
    if (kw.asInt("version") != 3)
        throw std::runtime_error("keyword 'version' mismatch");
}

// ===== SSM scalar_primitives fixture =====
// Same schema/data as scalar_primitives but using StandardStMan.

void write_ssm_scalar_primitives_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Bool>("col_bool"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("col_i32"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Double>("col_f64"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::String>("col_str"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 3);

    casacore::ScalarColumn<casacore::Bool> colBool(table, "col_bool");
    casacore::ScalarColumn<casacore::Int> colI32(table, "col_i32");
    casacore::ScalarColumn<casacore::Double> colF64(table, "col_f64");
    casacore::ScalarColumn<casacore::String> colStr(table, "col_str");

    // Row 0
    colBool.put(0, casacore::True);
    colI32.put(0, 42);
    colF64.put(0, 1.5);
    colStr.put(0, "hello");

    // Row 1
    colBool.put(1, casacore::False);
    colI32.put(1, -7);
    colF64.put(1, -99.5);
    colStr.put(1, "world");

    // Row 2
    colBool.put(2, casacore::True);
    colI32.put(2, 0);
    colF64.put(2, 0.0);
    colStr.put(2, "");

    // Table keyword
    table.rwKeywordSet().define("observer", casacore::String("test-harness"));

    table.flush();
}

void verify_ssm_scalar_primitives_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);

    if (table.nrow() != 3) {
        throw std::runtime_error("expected 3 rows, got " +
                                 std::to_string(table.nrow()));
    }

    casacore::ScalarColumn<casacore::Bool> colBool(table, "col_bool");
    casacore::ScalarColumn<casacore::Int> colI32(table, "col_i32");
    casacore::ScalarColumn<casacore::Double> colF64(table, "col_f64");
    casacore::ScalarColumn<casacore::String> colStr(table, "col_str");

    // Row 0
    if (colBool(0) != casacore::True)
        throw std::runtime_error("row 0 col_bool mismatch");
    if (colI32(0) != 42) throw std::runtime_error("row 0 col_i32 mismatch");
    if (colF64(0) != 1.5) throw std::runtime_error("row 0 col_f64 mismatch");
    if (colStr(0) != "hello")
        throw std::runtime_error("row 0 col_str mismatch");

    // Row 1
    if (colBool(1) != casacore::False)
        throw std::runtime_error("row 1 col_bool mismatch");
    if (colI32(1) != -7) throw std::runtime_error("row 1 col_i32 mismatch");
    if (colF64(1) != -99.5)
        throw std::runtime_error("row 1 col_f64 mismatch");
    if (colStr(1) != "world")
        throw std::runtime_error("row 1 col_str mismatch");

    // Row 2
    if (colBool(2) != casacore::True)
        throw std::runtime_error("row 2 col_bool mismatch");
    if (colI32(2) != 0) throw std::runtime_error("row 2 col_i32 mismatch");
    if (colF64(2) != 0.0) throw std::runtime_error("row 2 col_f64 mismatch");
    if (colStr(2) != "")
        throw std::runtime_error("row 2 col_str mismatch");

    // Table keywords
    const casacore::TableRecord& kw = table.keywordSet();
    if (!kw.isDefined("observer"))
        throw std::runtime_error("keyword 'observer' missing");
    if (kw.asString("observer") != "test-harness")
        throw std::runtime_error("keyword 'observer' mismatch");
}

// ===== SSM fixed_array fixture =====
// Same schema/data as fixed_array but using StandardStMan.

void write_ssm_fixed_array_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    casacore::IPosition shape(2, 2, 3);
    td.addColumn(
        casacore::ArrayColumnDesc<casacore::Float>("data", shape,
                                                    casacore::ColumnDesc::Direct |
                                                    casacore::ColumnDesc::FixedShape));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 3);
    casacore::ArrayColumn<casacore::Float> colData(table, "data");

    // Row 0: [1,2,3,4,5,6] in column-major (Fortran order)
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
    // Row 1
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
    // Row 2 (all zeros)
    {
        casacore::Array<casacore::Float> arr(shape, 0.0f);
        colData.put(2, arr);
    }

    table.flush();
}

void verify_ssm_fixed_array_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);

    if (table.nrow() != 3)
        throw std::runtime_error("expected 3 rows");

    casacore::ArrayColumn<casacore::Float> colData(table, "data");
    casacore::IPosition expectedShape(2, 2, 3);

    // Row 0
    {
        casacore::Array<casacore::Float> arr = colData(0);
        if (!arr.shape().isEqual(expectedShape))
            throw std::runtime_error("row 0 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 1.0f)
            throw std::runtime_error("row 0 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 1)) != 4.0f)
            throw std::runtime_error("row 0 [1,1] mismatch");
    }
    // Row 2 (all zeros)
    {
        casacore::Array<casacore::Float> arr = colData(2);
        if (arr(casacore::IPosition(2, 0, 0)) != 0.0f)
            throw std::runtime_error("row 2 [0,0] mismatch");
    }
}

// ===== SSM keywords fixture =====
// Same schema/data as keywords but using StandardStMan.

void write_ssm_keywords_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 2);
    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    colId.put(0, 1);
    colId.put(1, 2);

    table.rwKeywordSet().define("telescope", casacore::String("ALMA"));
    table.rwKeywordSet().define("version", casacore::Int(3));

    table.flush();
}

void verify_ssm_keywords_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);

    if (table.nrow() != 2)
        throw std::runtime_error("expected 2 rows");

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
    try {
        write_scalar_primitives_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown non-std::exception in write_scalar_primitives");
        return -1;
    }
}

int32_t cpp_table_verify_scalar_primitives(const char* path, char** out_error) {
    try {
        verify_scalar_primitives_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown non-std::exception in verify_scalar_primitives");
        return -1;
    }
}

int32_t cpp_table_write_fixed_array(const char* path, char** out_error) {
    try {
        write_fixed_array_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown non-std::exception in write_fixed_array");
        return -1;
    }
}

int32_t cpp_table_verify_fixed_array(const char* path, char** out_error) {
    try {
        verify_fixed_array_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown non-std::exception in verify_fixed_array");
        return -1;
    }
}

int32_t cpp_table_write_keywords(const char* path, char** out_error) {
    try {
        write_keywords_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown non-std::exception in write_keywords");
        return -1;
    }
}

int32_t cpp_table_verify_keywords(const char* path, char** out_error) {
    try {
        verify_keywords_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown non-std::exception in verify_keywords");
        return -1;
    }
}

int32_t cpp_table_write_ssm_scalar_primitives(const char* path, char** out_error) {
    try {
        write_ssm_scalar_primitives_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown non-std::exception in write_ssm_scalar_primitives");
        return -1;
    }
}

int32_t cpp_table_verify_ssm_scalar_primitives(const char* path, char** out_error) {
    try {
        verify_ssm_scalar_primitives_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown non-std::exception in verify_ssm_scalar_primitives");
        return -1;
    }
}

int32_t cpp_table_write_ssm_fixed_array(const char* path, char** out_error) {
    try {
        write_ssm_fixed_array_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown non-std::exception in write_ssm_fixed_array");
        return -1;
    }
}

int32_t cpp_table_verify_ssm_fixed_array(const char* path, char** out_error) {
    try {
        verify_ssm_fixed_array_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown non-std::exception in verify_ssm_fixed_array");
        return -1;
    }
}

int32_t cpp_table_write_ssm_keywords(const char* path, char** out_error) {
    try {
        write_ssm_keywords_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown non-std::exception in write_ssm_keywords");
        return -1;
    }
}

int32_t cpp_table_verify_ssm_keywords(const char* path, char** out_error) {
    try {
        verify_ssm_keywords_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown non-std::exception in verify_ssm_keywords");
        return -1;
    }
}

void cpp_table_free_error(char* ptr) {
    std::free(ptr);
}

} // extern "C"
