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
#include <casacore/tables/Tables/TableLock.h>
#include <casacore/casa/Utilities/Sort.h>
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

// ===== Mutation verify fixtures =====
// These verify tables written by Rust after in-memory mutations.
// Only verify functions needed (RC = Rust write, C++ read).

// --- remove_column: scalar_primitives with col_str removed ---
// Schema: col_bool(Bool), col_i32(Int), col_f64(Double)
// 3 rows, same values as scalar_primitives minus col_str. No keywords.

void verify_mutation_removed_column_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 3)
        throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));

    // Verify col_str is absent
    if (table.tableDesc().isColumn("col_str"))
        throw std::runtime_error("col_str should have been removed");

    casacore::ScalarColumn<casacore::Bool> colBool(table, "col_bool");
    casacore::ScalarColumn<casacore::Int> colI32(table, "col_i32");
    casacore::ScalarColumn<casacore::Double> colF64(table, "col_f64");

    if (colBool(0) != casacore::True) throw std::runtime_error("row 0 col_bool");
    if (colI32(0) != 42) throw std::runtime_error("row 0 col_i32");
    if (colF64(0) != 1.5) throw std::runtime_error("row 0 col_f64");
    if (colI32(1) != -7) throw std::runtime_error("row 1 col_i32");
    if (colI32(2) != 0) throw std::runtime_error("row 2 col_i32");
}

// --- remove_rows: scalar_primitives with row 1 removed ---
// Schema: col_bool(Bool), col_i32(Int), col_f64(Double), col_str(String)
// 2 rows: original rows 0 and 2. No keywords.

void verify_mutation_removed_rows_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 2)
        throw std::runtime_error("expected 2 rows, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::Int> colI32(table, "col_i32");
    casacore::ScalarColumn<casacore::String> colStr(table, "col_str");

    // Row 0 = original row 0
    if (colI32(0) != 42) throw std::runtime_error("row 0 col_i32");
    if (colStr(0) != "hello") throw std::runtime_error("row 0 col_str");
    // Row 1 = original row 2
    if (colI32(1) != 0) throw std::runtime_error("row 1 col_i32");
    if (colStr(1) != "") throw std::runtime_error("row 1 col_str");
}

// --- add_column: scalar_primitives + extra(Float, 42.0) ---
// Schema: col_bool(Bool), col_i32(Int), col_f64(Double), col_str(String), extra(Float)
// 3 rows, same values as scalar_primitives + extra=42.0 in all rows. No keywords.

void verify_mutation_added_column_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 3)
        throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));

    if (!table.tableDesc().isColumn("extra"))
        throw std::runtime_error("extra column missing");

    casacore::ScalarColumn<casacore::Int> colI32(table, "col_i32");
    casacore::ScalarColumn<casacore::Float> colExtra(table, "extra");

    if (colI32(0) != 42) throw std::runtime_error("row 0 col_i32");
    if (colI32(1) != -7) throw std::runtime_error("row 1 col_i32");
    for (casacore::uInt i = 0; i < 3; ++i) {
        if (colExtra(i) != 42.0f)
            throw std::runtime_error("row " + std::to_string(i) + " extra mismatch");
    }
}

// ===== lock file interop fixture =====
// Schema: col_id(Int), col_name(String)
// 1 row: (42, "from_cpp")
// Table is created/opened with PermanentLocking to produce a table.lock
// file with sync data.

void write_with_lock_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::String>("name"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    // Create with PermanentLocking — this creates a table.lock file.
    casacore::Table table(setup, casacore::TableLock(casacore::TableLock::PermanentLocking), 1);

    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    casacore::ScalarColumn<casacore::String> colName(table, "name");
    colId.put(0, 42);
    colName.put(0, "from_cpp");

    // Flush writes sync data to lock file, then table destructor releases lock.
    table.flush();
}

void verify_with_lock_impl(const std::string& path) {
    // Open with PermanentLocking — reads the lock file and sync data.
    casacore::Table table(path,
                          casacore::TableLock(casacore::TableLock::PermanentLocking),
                          casacore::Table::Old);

    if (table.nrow() < 1) {
        throw std::runtime_error("expected at least 1 row, got " +
                                 std::to_string(table.nrow()));
    }

    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    casacore::ScalarColumn<casacore::String> colName(table, "name");

    // Check first row (written by whichever side — Rust or C++).
    if (colId(0) != 42)
        throw std::runtime_error("row 0 id mismatch: expected 42, got " +
                                 std::to_string(colId(0)));
    // Accept either "from_cpp" or "from_rust" — we just verify the lock
    // file was readable and the table data is intact.
    casacore::String name = colName(0);
    if (name != "from_cpp" && name != "from_rust")
        throw std::runtime_error("row 0 name mismatch: got '" + name + "'");
}

// ===== RefTable fixture =====
// Parent table: 3 rows (id: Int, name: String).
// RefTable selects rows 0 and 2.
// The C++ write creates both parent.tbl and ref.tbl inside the given dir.
// The C++ verify opens ref.tbl and checks row count and cell values.

void write_ref_table_impl(const std::string& dir) {
    // Create parent table at dir/parent.tbl
    std::string parentPath = dir + "/parent.tbl";
    {
        casacore::TableDesc td("", casacore::TableDesc::Scratch);
        td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));
        td.addColumn(casacore::ScalarColumnDesc<casacore::String>("name"));

        casacore::SetupNewTable setup(parentPath, td, casacore::Table::New);
        casacore::Table parent(setup, 3);
        casacore::ScalarColumn<casacore::Int> colId(parent, "id");
        casacore::ScalarColumn<casacore::String> colName(parent, "name");

        colId.put(0, 10);
        colName.put(0, "alpha");
        colId.put(1, 20);
        colName.put(1, "beta");
        colId.put(2, 30);
        colName.put(2, "gamma");

        parent.flush();
    }

    // Re-open parent to create a RefTable
    casacore::Table parent(parentPath, casacore::Table::Old);

    // Select rows 0 and 2
    casacore::Vector<casacore::rownr_t> rows(2);
    rows(0) = 0;
    rows(1) = 2;

    casacore::Table refTable = parent(rows);

    // Save the RefTable
    std::string refPath = dir + "/ref.tbl";
    refTable.rename(refPath, casacore::Table::New);
    refTable.flush();
}

void verify_ref_table_impl(const std::string& dir) {
    std::string refPath = dir + "/ref.tbl";
    casacore::Table table(refPath, casacore::Table::Old);

    if (table.nrow() != 2)
        throw std::runtime_error(
            "expected 2 rows in ref table, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    casacore::ScalarColumn<casacore::String> colName(table, "name");

    // Row 0 of the RefTable → parent row 0
    if (colId(0) != 10)
        throw std::runtime_error("ref row 0 id mismatch: got " + std::to_string(colId(0)));
    casacore::String name0 = colName(0);
    if (name0 != "alpha" && name0 != "from_rust_0")
        throw std::runtime_error("ref row 0 name mismatch: got '" + name0 + "'");

    // Row 1 of the RefTable → parent row 2
    if (colId(1) != 30)
        throw std::runtime_error("ref row 1 id mismatch: got " + std::to_string(colId(1)));
    casacore::String name1 = colName(1);
    if (name1 != "gamma" && name1 != "from_rust_2")
        throw std::runtime_error("ref row 1 name mismatch: got '" + name1 + "'");
}

// ===== SortedRefTable fixture =====
// Parent table: 5 rows (id: Int, name: String, value: Float).
// Sorted descending by "id", saved as RefTable at dir/sorted.tbl.

void write_sorted_ref_table_impl(const std::string& dir) {
    std::string parentPath = dir + "/parent.tbl";
    {
        casacore::TableDesc td("", casacore::TableDesc::Scratch);
        td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));
        td.addColumn(casacore::ScalarColumnDesc<casacore::String>("name"));
        td.addColumn(casacore::ScalarColumnDesc<casacore::Float>("value"));

        casacore::SetupNewTable setup(parentPath, td, casacore::Table::New);
        casacore::Table parent(setup, 5);
        casacore::ScalarColumn<casacore::Int> colId(parent, "id");
        casacore::ScalarColumn<casacore::String> colName(parent, "name");
        casacore::ScalarColumn<casacore::Float> colValue(parent, "value");

        colId.put(0, 30); colName.put(0, "charlie"); colValue.put(0, 3.0f);
        colId.put(1, 10); colName.put(1, "alpha");   colValue.put(1, 1.0f);
        colId.put(2, 50); colName.put(2, "echo");    colValue.put(2, 5.0f);
        colId.put(3, 20); colName.put(3, "bravo");   colValue.put(3, 2.0f);
        colId.put(4, 40); colName.put(4, "delta");   colValue.put(4, 4.0f);

        parent.flush();
    }

    casacore::Table parent(parentPath, casacore::Table::Old);

    // Sort descending by id.
    casacore::Table sorted = parent.sort("id", casacore::Sort::Descending);

    std::string sortedPath = dir + "/sorted.tbl";
    sorted.rename(sortedPath, casacore::Table::New);
    sorted.flush();
}

void verify_sorted_ref_table_impl(const std::string& dir) {
    std::string sortedPath = dir + "/sorted.tbl";
    casacore::Table table(sortedPath, casacore::Table::Old);

    if (table.nrow() != 5)
        throw std::runtime_error(
            "expected 5 rows in sorted table, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::Int> colId(table, "id");

    // Should be in descending order: 50, 40, 30, 20, 10.
    int expected[] = {50, 40, 30, 20, 10};
    for (int i = 0; i < 5; i++) {
        if (colId(i) != expected[i])
            throw std::runtime_error(
                "row " + std::to_string(i) + " id mismatch: expected " +
                std::to_string(expected[i]) + ", got " + std::to_string(colId(i)));
    }
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

// --- Mutation verify C ABI wrappers ---

int32_t cpp_table_verify_mutation_removed_column(const char* path, char** out_error) {
    try {
        verify_mutation_removed_column_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in verify_mutation_removed_column");
        return -1;
    }
}

int32_t cpp_table_verify_mutation_removed_rows(const char* path, char** out_error) {
    try {
        verify_mutation_removed_rows_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in verify_mutation_removed_rows");
        return -1;
    }
}

int32_t cpp_table_verify_mutation_added_column(const char* path, char** out_error) {
    try {
        verify_mutation_added_column_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in verify_mutation_added_column");
        return -1;
    }
}

// --- Lock file interop C ABI wrappers ---

int32_t cpp_table_write_with_lock(const char* path, char** out_error) {
    try {
        write_with_lock_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in write_with_lock");
        return -1;
    }
}

int32_t cpp_table_verify_with_lock(const char* path, char** out_error) {
    try {
        verify_with_lock_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in verify_with_lock");
        return -1;
    }
}

int32_t cpp_table_write_ref_table(const char* path, char** out_error) {
    try {
        write_ref_table_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in write_ref_table");
        return -1;
    }
}

int32_t cpp_table_verify_ref_table(const char* path, char** out_error) {
    try {
        verify_ref_table_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in verify_ref_table");
        return -1;
    }
}

int32_t cpp_table_write_sorted_ref_table(const char* path, char** out_error) {
    try {
        write_sorted_ref_table_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in write_sorted_ref_table");
        return -1;
    }
}

int32_t cpp_table_verify_sorted_ref_table(const char* path, char** out_error) {
    try {
        verify_sorted_ref_table_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in verify_sorted_ref_table");
        return -1;
    }
}

void cpp_table_free_error(char* ptr) {
    std::free(ptr);
}

} // extern "C"
