// Miscellaneous fixtures: mutations, lock, ref tables, concat, deep copy,
// columns index, free_error.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/StManAipsIO.h>
#include <casacore/tables/DataMan/StandardStMan.h>
#include <casacore/tables/Tables/ColumnsIndex.h>
#include <casacore/casa/Containers/RecordField.h>
#include <casacore/casa/Utilities/Sort.h>
#include <casacore/tables/Tables/ConcatTable.h>
#include <casacore/tables/Tables/TableCopy.h>
#include <casacore/casa/Containers/Block.h>

#include <chrono>

using casacore_shim::make_error;

namespace {

// ===== Mutation verify fixtures =====

void verify_mutation_removed_column_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 3)
        throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));
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

void verify_mutation_removed_rows_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 2)
        throw std::runtime_error("expected 2 rows, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::Int> colI32(table, "col_i32");
    casacore::ScalarColumn<casacore::String> colStr(table, "col_str");

    if (colI32(0) != 42) throw std::runtime_error("row 0 col_i32");
    if (colStr(0) != "hello") throw std::runtime_error("row 0 col_str");
    if (colI32(1) != 0) throw std::runtime_error("row 1 col_i32");
    if (colStr(1) != "") throw std::runtime_error("row 1 col_str");
}

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

// ===== Lock file interop =====

void write_with_lock_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::String>("name"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, casacore::TableLock(casacore::TableLock::PermanentLocking), 1);
    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    casacore::ScalarColumn<casacore::String> colName(table, "name");
    colId.put(0, 42);
    colName.put(0, "from_cpp");
    table.flush();
}

void verify_with_lock_impl(const std::string& path) {
    casacore::Table table(path,
                          casacore::TableLock(casacore::TableLock::PermanentLocking),
                          casacore::Table::Old);
    if (table.nrow() < 1)
        throw std::runtime_error("expected at least 1 row, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    casacore::ScalarColumn<casacore::String> colName(table, "name");

    if (colId(0) != 42)
        throw std::runtime_error("row 0 id mismatch: expected 42, got " + std::to_string(colId(0)));
    casacore::String name = colName(0);
    if (name != "from_cpp" && name != "from_rust")
        throw std::runtime_error("row 0 name mismatch: got '" + name + "'");
}

// ===== RefTable =====

void write_ref_table_impl(const std::string& dir) {
    std::string parentPath = dir + "/parent.tbl";
    {
        casacore::TableDesc td("", casacore::TableDesc::Scratch);
        td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));
        td.addColumn(casacore::ScalarColumnDesc<casacore::String>("name"));

        casacore::SetupNewTable setup(parentPath, td, casacore::Table::New);
        casacore::Table parent(setup, 3);
        casacore::ScalarColumn<casacore::Int> colId(parent, "id");
        casacore::ScalarColumn<casacore::String> colName(parent, "name");

        colId.put(0, 10); colName.put(0, "alpha");
        colId.put(1, 20); colName.put(1, "beta");
        colId.put(2, 30); colName.put(2, "gamma");
        parent.flush();
    }

    casacore::Table parent(parentPath, casacore::Table::Old);
    casacore::Vector<casacore::rownr_t> rows(2);
    rows(0) = 0; rows(1) = 2;
    casacore::Table refTable = parent(rows);
    std::string refPath = dir + "/ref.tbl";
    refTable.rename(refPath, casacore::Table::New);
    refTable.flush();
}

void verify_ref_table_impl(const std::string& dir) {
    std::string refPath = dir + "/ref.tbl";
    casacore::Table table(refPath, casacore::Table::Old);
    if (table.nrow() != 2)
        throw std::runtime_error("expected 2 rows in ref table, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    casacore::ScalarColumn<casacore::String> colName(table, "name");

    if (colId(0) != 10) throw std::runtime_error("ref row 0 id mismatch: got " + std::to_string(colId(0)));
    casacore::String name0 = colName(0);
    if (name0 != "alpha" && name0 != "from_rust_0")
        throw std::runtime_error("ref row 0 name mismatch: got '" + name0 + "'");
    if (colId(1) != 30) throw std::runtime_error("ref row 1 id mismatch: got " + std::to_string(colId(1)));
    casacore::String name1 = colName(1);
    if (name1 != "gamma" && name1 != "from_rust_2")
        throw std::runtime_error("ref row 1 name mismatch: got '" + name1 + "'");
}

// ===== SortedRefTable =====

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
    casacore::Table sorted = parent.sort("id", casacore::Sort::Descending);
    std::string sortedPath = dir + "/sorted.tbl";
    sorted.rename(sortedPath, casacore::Table::New);
    sorted.flush();
}

void verify_sorted_ref_table_impl(const std::string& dir) {
    std::string sortedPath = dir + "/sorted.tbl";
    casacore::Table table(sortedPath, casacore::Table::Old);
    if (table.nrow() != 5)
        throw std::runtime_error("expected 5 rows in sorted table, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    int expected[] = {50, 40, 30, 20, 10};
    for (int i = 0; i < 5; i++) {
        if (colId(i) != expected[i])
            throw std::runtime_error("row " + std::to_string(i) + " id mismatch: expected " +
                std::to_string(expected[i]) + ", got " + std::to_string(colId(i)));
    }
}

// ===== ConcatTable =====

void write_concat_table_impl(const std::string& dir) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::String>("name"));

    std::string part0Path = dir + "/part0.tbl";
    {
        casacore::SetupNewTable newtab(part0Path, td, casacore::Table::New);
        casacore::Table table(newtab, 3);
        casacore::ScalarColumn<casacore::Int> colId(table, "id");
        casacore::ScalarColumn<casacore::String> colName(table, "name");
        colId.put(0, 1); colName.put(0, "alpha");
        colId.put(1, 2); colName.put(1, "bravo");
        colId.put(2, 3); colName.put(2, "charlie");
    }

    std::string part1Path = dir + "/part1.tbl";
    {
        casacore::SetupNewTable newtab(part1Path, td, casacore::Table::New);
        casacore::Table table(newtab, 3);
        casacore::ScalarColumn<casacore::Int> colId(table, "id");
        casacore::ScalarColumn<casacore::String> colName(table, "name");
        colId.put(0, 4); colName.put(0, "delta");
        colId.put(1, 5); colName.put(1, "echo");
        colId.put(2, 6); colName.put(2, "foxtrot");
    }

    casacore::Block<casacore::String> tableNames(2);
    tableNames[0] = part0Path;
    tableNames[1] = part1Path;
    casacore::Block<casacore::String> subTableNames;
    casacore::ConcatTable concatTable(tableNames, subTableNames, "",
        casacore::Table::Old, casacore::TableLock(), casacore::TSMOption());
    std::string concatPath = dir + "/concat.tbl";
    concatTable.rename(concatPath, casacore::Table::New);
}

void verify_concat_table_impl(const std::string& dir) {
    std::string concatPath = dir + "/concat.tbl";
    casacore::Table table(concatPath, casacore::Table::Old);
    if (table.nrow() != 6)
        throw std::runtime_error("expected 6 rows, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    int expected[] = {1, 2, 3, 4, 5, 6};
    for (int i = 0; i < 6; i++) {
        if (colId(i) != expected[i])
            throw std::runtime_error("row " + std::to_string(i) + " id mismatch");
    }
}

// ===== Deep copy =====

void write_deep_copy_impl(const std::string& dir) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::String>("name"));

    std::string originalPath = dir + "/original.tbl";
    {
        casacore::SetupNewTable newtab(originalPath, td, casacore::Table::New);
        casacore::Table table(newtab, 5);
        casacore::ScalarColumn<casacore::Int> colId(table, "id");
        casacore::ScalarColumn<casacore::String> colName(table, "name");
        for (int i = 0; i < 5; i++) {
            colId.put(i, (i + 1) * 10);
            colName.put(i, "item_" + std::to_string(i));
        }
    }

    std::string copyPath = dir + "/copy.tbl";
    {
        casacore::Table original(originalPath, casacore::Table::Old);
        casacore::Record dminfo;
        casacore::Table newTable = casacore::TableCopy::makeEmptyTable(
            copyPath, dminfo, original, casacore::Table::New,
            casacore::Table::LittleEndian, casacore::True);
        casacore::TableCopy::copyRows(newTable, original);
    }
}

void verify_deep_copy_impl(const std::string& dir) {
    std::string copyPath = dir + "/copy.tbl";
    casacore::Table table(copyPath, casacore::Table::Old);
    if (table.nrow() != 5)
        throw std::runtime_error("expected 5 rows, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    casacore::ScalarColumn<casacore::String> colName(table, "name");
    for (int i = 0; i < 5; i++) {
        int expectedId = (i + 1) * 10;
        if (colId(i) != expectedId)
            throw std::runtime_error("row " + std::to_string(i) + " id mismatch");
        casacore::String expectedName = "item_" + std::to_string(i);
        if (colName(i) != expectedName)
            throw std::runtime_error("row " + std::to_string(i) + " name mismatch");
    }
}

// ===== ColumnsIndex =====

void write_columns_index_fixture_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("antenna_id"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);

    casacore::Table tab(setup, 50);
    casacore::ScalarColumn<casacore::Int> col(tab, "antenna_id");
    for (casacore::Int i = 0; i < 50; i++) {
        col.put(i, i % 10);
    }
}

void cpp_columns_index_time_lookups_impl(
    const std::string& path,
    int32_t key_value,
    uint64_t nqueries,
    uint64_t* out_elapsed_ns,
    uint64_t* out_match_count)
{
    casacore::Table tab(path, casacore::Table::Old);
    casacore::ColumnsIndex idx(tab, casacore::String("id"));
    casacore::RecordFieldPtr<casacore::Int> keyFld(idx.accessKey(), "id");

    auto t0 = std::chrono::steady_clock::now();
    casacore::RowNumbers rows;
    for (uint64_t q = 0; q < nqueries; q++) {
        *keyFld = key_value;
        rows = idx.getRowNumbers();
    }
    auto t1 = std::chrono::steady_clock::now();

    *out_elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    *out_match_count = rows.nelements();
}

// ===== TableInfo =====

void write_table_info_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 1);
    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    colId.put(0, 1);

    table.tableInfo().setType("Measurement");
    table.tableInfo().setSubType("UVFITS");
    table.flush();
}

void verify_table_info_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != 1)
        throw std::runtime_error("expected 1 row, got " + std::to_string(table.nrow()));

    casacore::ScalarColumn<casacore::Int> colId(table, "id");
    if (colId(0) != 1)
        throw std::runtime_error("row 0 id mismatch: expected 1, got " + std::to_string(colId(0)));

    casacore::String type = table.tableInfo().type();
    if (type != "Measurement")
        throw std::runtime_error("tableInfo type mismatch: expected 'Measurement', got '" + type + "'");

    casacore::String subType = table.tableInfo().subType();
    if (subType != "UVFITS")
        throw std::runtime_error("tableInfo subType mismatch: expected 'UVFITS', got '" + subType + "'");
}

} // anonymous namespace

extern "C" {

// Mutation verifiers
int32_t cpp_table_verify_mutation_removed_column(const char* path, char** out_error) {
    try { verify_mutation_removed_column_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_mutation_removed_rows(const char* path, char** out_error) {
    try { verify_mutation_removed_rows_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_mutation_added_column(const char* path, char** out_error) {
    try { verify_mutation_added_column_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

// Lock
int32_t cpp_table_write_with_lock(const char* path, char** out_error) {
    try { write_with_lock_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_with_lock(const char* path, char** out_error) {
    try { verify_with_lock_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

// Ref tables
int32_t cpp_table_write_ref_table(const char* path, char** out_error) {
    try { write_ref_table_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_ref_table(const char* path, char** out_error) {
    try { verify_ref_table_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_write_sorted_ref_table(const char* path, char** out_error) {
    try { write_sorted_ref_table_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_sorted_ref_table(const char* path, char** out_error) {
    try { verify_sorted_ref_table_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

// Concat
int32_t cpp_table_write_concat_table(const char* path, char** out_error) {
    try { write_concat_table_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_concat_table(const char* path, char** out_error) {
    try { verify_concat_table_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

// Deep copy
int32_t cpp_table_write_deep_copy(const char* path, char** out_error) {
    try { write_deep_copy_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_deep_copy(const char* path, char** out_error) {
    try { verify_deep_copy_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

// Free error
void cpp_table_free_error(char* ptr) {
    std::free(ptr);
}

// ColumnsIndex
int32_t cpp_table_write_columns_index_fixture(const char* path, char** out_error) {
    try { write_columns_index_fixture_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

int32_t cpp_columns_index_time_lookups(
    const char* path, int32_t key_value, uint64_t nqueries,
    uint64_t* out_elapsed_ns, uint64_t* out_match_count, char** out_error)
{
    try {
        cpp_columns_index_time_lookups_impl(path, key_value, nqueries, out_elapsed_ns, out_match_count);
        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

// TableInfo
int32_t cpp_table_write_table_info(const char* path, char** out_error) {
    try { write_table_info_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_table_info(const char* path, char** out_error) {
    try { verify_table_info_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

} // extern "C"
