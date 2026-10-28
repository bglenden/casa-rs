// Miscellaneous fixtures: mutations, lock, ref tables, concat, deep copy,
// columns index, free_error.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/StManAipsIO.h>
#include <casacore/tables/DataMan/StandardStMan.h>
#include <casacore/tables/DataMan/IncrementalStMan.h>
#include <casacore/tables/DataMan/TiledShapeStMan.h>
#include <casacore/tables/Tables/ColumnsIndex.h>
#include <casacore/casa/Containers/RecordField.h>
#include <casacore/casa/Utilities/Sort.h>
#include <casacore/tables/Tables/ConcatTable.h>
#include <casacore/tables/Tables/TableCopy.h>
#include <casacore/tables/Tables/TableColumn.h>
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

// Open with UserLocking and take, release and retake an explicit read lock.
// Each lock synchronizes the table with the sync data in table.lock
// (PlainTable::lock, ColumnSet::resync), so it fails when that data does not
// describe the table on disk: a different column count, or a number of
// data-manager counters other than the table's data managers.
void lock_read_relock_impl(const std::string& path, uint64_t* out_rows,
                           uint32_t* out_columns) {
    casacore::Table table(path,
                          casacore::TableLock(casacore::TableLock::UserLocking),
                          casacore::Table::Old);
    if (!table.lock(casacore::FileLocker::Read, 1))
        throw std::runtime_error("first read lock was not acquired");
    table.unlock();
    if (!table.lock(casacore::FileLocker::Read, 1))
        throw std::runtime_error("second read lock was not acquired");
    *out_rows = table.nrow();
    *out_columns = table.tableDesc().ncolumn();
    table.unlock();
}

// Open the table that table keyword `keyword` of the table at `path` (of its
// column `column` when that is not empty) refers to, as casacore resolves the
// stored name (TableKeyword::set, Path::addDirectory), and copy that table's
// name into `buf`.
void keyword_table_name_impl(const std::string& path, const std::string& column,
                             const std::string& keyword, char* buf, int32_t bufsize) {
    casacore::Table table(path, casacore::Table::Old);
    std::string name;
    if (column.empty()) {
        name = table.keywordSet().asTable(casacore::String(keyword)).tableName();
    } else {
        casacore::TableColumn tableColumn(table, column);
        name = tableColumn.keywordSet().asTable(casacore::String(keyword)).tableName();
    }
    if (static_cast<int32_t>(name.size()) >= bufsize)
        throw std::runtime_error("table name longer than the output buffer: " + name);
    std::memcpy(buf, name.c_str(), name.size() + 1);
}

// The Double array table keyword `keyword` of the table at `path`: its shape,
// into `shape` (at most `max_ndim` axes), and its elements in casacore's
// storage order, the first axis fastest (Array::tovector), into `values` (at
// most `max_values`).
void keyword_double_array_impl(const std::string& path, const std::string& keyword,
                               int64_t* shape, int32_t max_ndim, int32_t* out_ndim,
                               double* values, int64_t max_values, int64_t* out_count) {
    casacore::Table table(path, casacore::Table::Old);
    casacore::Array<casacore::Double> array =
        table.keywordSet().asArrayDouble(casacore::String(keyword));
    const casacore::IPosition& arrayShape = array.shape();
    if (static_cast<int32_t>(arrayShape.size()) > max_ndim)
        throw std::runtime_error("keyword array has more axes than the output holds");
    if (static_cast<int64_t>(array.nelements()) > max_values)
        throw std::runtime_error("keyword array has more elements than the output holds");
    for (size_t axis = 0; axis < arrayShape.size(); ++axis)
        shape[axis] = arrayShape[axis];
    *out_ndim = static_cast<int32_t>(arrayShape.size());
    std::vector<casacore::Double> stored = array.tovector();
    std::copy(stored.begin(), stored.end(), values);
    *out_count = static_cast<int64_t>(stored.size());
}

// A reader that keeps a table and its Int column "id" open between lock
// periods, as a casacore session holding a table open does. What it read
// stays cached in the column; a later lock re-reads the table only when the
// sync data in table.lock says another process wrote it (PlainTable::lock,
// ColumnSet::resync).
struct HeldTableReader {
    casacore::Table table;
    casacore::ScalarColumn<casacore::Int> id;

    explicit HeldTableReader(const std::string& path)
        : table(path, casacore::TableLock(casacore::TableLock::UserLocking),
                casacore::Table::Old),
          id(table, "id") {}
};

// Take a read lock, read id(0) and release the lock, keeping the table open.
int32_t held_reader_read_id_impl(HeldTableReader& reader) {
    if (!reader.table.lock(casacore::FileLocker::Read, 1))
        throw std::runtime_error("read lock was not acquired");
    casacore::Int value = reader.id(0);
    reader.table.unlock();
    return value;
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

// ===== LOG table =====

void write_log_table_impl(const std::string& path) {
    casacore::TableDesc desc("", casacore::TableDesc::Scratch);
    desc.comment() = "Log message table";
    desc.addColumn(casacore::ScalarColumnDesc<casacore::Double>("TIME", "MJD in seconds"));
    casacore::ScalarColumnDesc<casacore::String> priorityDesc("PRIORITY");
    priorityDesc.setMaxLength(9);
    desc.addColumn(priorityDesc);
    desc.addColumn(casacore::ScalarColumnDesc<casacore::String>("MESSAGE"));
    desc.addColumn(casacore::ScalarColumnDesc<casacore::String>("LOCATION"));
    desc.addColumn(casacore::ScalarColumnDesc<casacore::String>("OBJECT_ID"));

    casacore::SetupNewTable setup(path, desc, casacore::Table::New);
    casacore::StandardStMan stman("SSM", 32768);
    setup.bindAll(stman);

    casacore::Table table(setup, 1);
    table.tableInfo() = casacore::TableInfo(casacore::TableInfo::LOG);
    table.tableInfo().readmeAddLine("Repository for software-generated logging messages");

    casacore::ScalarColumn<casacore::Double> time(table, "TIME");
    casacore::ScalarColumn<casacore::String> priority(table, "PRIORITY");
    casacore::ScalarColumn<casacore::String> message(table, "MESSAGE");
    casacore::ScalarColumn<casacore::String> location(table, "LOCATION");
    casacore::ScalarColumn<casacore::String> objectId(table, "OBJECT_ID");

    time.rwKeywordSet().define("UNIT", "s");
    time.rwKeywordSet().define("MEASURE_TYPE", "EPOCH");
    time.rwKeywordSet().define("MEASURE_REFERENCE", "UTC");

    time.put(0, 40587.0 * 86400.0);
    priority.put(0, "INFO");
    message.put(0, "C++ log row");
    location.put(0, "cpp::log");
    objectId.put(0, "cpp-object");
    table.flush();
}

void verify_log_table_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() < 1)
        throw std::runtime_error("expected at least 1 log row, got " + std::to_string(table.nrow()));

    casacore::String type = table.tableInfo().type();
    if (type != "LOG" && type != "Log message")
        throw std::runtime_error("tableInfo type mismatch: expected LOG-compatible type, got '" + type + "'");

    casacore::String readme = table.tableInfo().readme();
    if (readme.find("Repository for software-generated logging messages") == casacore::String::npos)
        throw std::runtime_error("tableInfo readme missing LOG description");

    if (!table.tableDesc().isColumn("TIME") ||
        !table.tableDesc().isColumn("PRIORITY") ||
        !table.tableDesc().isColumn("MESSAGE") ||
        !table.tableDesc().isColumn("LOCATION") ||
        !table.tableDesc().isColumn("OBJECT_ID"))
        throw std::runtime_error("LOG table is missing one or more required columns");

    if (table.tableDesc().columnDesc("TIME").comment() != "MJD in seconds")
        throw std::runtime_error("TIME column comment mismatch");
    if (table.tableDesc().columnDesc("PRIORITY").maxLength() != 9)
        throw std::runtime_error("PRIORITY maxLength mismatch");

    casacore::ScalarColumn<casacore::Double> time(table, "TIME");
    casacore::ScalarColumn<casacore::String> priority(table, "PRIORITY");
    casacore::ScalarColumn<casacore::String> message(table, "MESSAGE");
    casacore::ScalarColumn<casacore::String> location(table, "LOCATION");
    casacore::ScalarColumn<casacore::String> objectId(table, "OBJECT_ID");

    const casacore::TableRecord& keySet = time.keywordSet();
    if (!keySet.isDefined("UNIT") || keySet.asString("UNIT") != "s")
        throw std::runtime_error("TIME UNIT keyword mismatch");
    if (!keySet.isDefined("MEASURE_TYPE") || keySet.asString("MEASURE_TYPE") != "EPOCH")
        throw std::runtime_error("TIME MEASURE_TYPE keyword mismatch");
    if (!keySet.isDefined("MEASURE_REFERENCE") || keySet.asString("MEASURE_REFERENCE") != "UTC")
        throw std::runtime_error("TIME MEASURE_REFERENCE keyword mismatch");

    casacore::String pri = priority(0);
    if (pri != "INFO" && pri != "WARN" && pri != "SEVERE" &&
        pri != "DEBUGGING" && pri != "DEBUG2" && pri != "DEBUG1" &&
        pri != "INFO5" && pri != "INFO4" && pri != "INFO3" &&
        pri != "INFO2" && pri != "INFO1")
        throw std::runtime_error("row 0 priority is not a CASA priority: '" + pri + "'");
    if (message(0).empty())
        throw std::runtime_error("row 0 message is empty");
    (void)location(0);
    (void)objectId(0);
}

// ===== Mixed data managers =====
// A MeasurementSet-like layout: each column in its own kind of manager.
// StandardStMan holds the first column, so StManAipsIO's sequence number is
// not 0, and StManAipsIO also holds a variable array in its indirect file.
//   antenna  Int scalar             StandardStMan
//   flux     Double scalar          StManAipsIO
//   spectrum Float variable 1-D     StManAipsIO
//   time     Double scalar          IncrementalStMan
//   data     Complex variable 2-D   TiledShapeStMan ("TiledData", tile [2,3,2])
// Three rows: antenna r, flux r + 1.5, spectrum r + 1 values r*10 + k,
// time 100, 100, 110, data [2,3] with element k (Fortran order)
// = Complex(r*10 + k, -k).

void write_mixed_managers_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("antenna"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Double>("flux"));
    td.addColumn(casacore::ArrayColumnDesc<casacore::Float>("spectrum", 1));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Double>("time"));
    td.addColumn(casacore::ArrayColumnDesc<casacore::Complex>("data", 2));
    td.defineHypercolumn("TiledData", 3,
        casacore::Vector<casacore::String>(1, "data"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan ssm;
    casacore::StManAipsIO aipsio;
    casacore::IncrementalStMan ism;
    casacore::TiledShapeStMan tsm("TiledData", casacore::IPosition(3, 2, 3, 2));
    setup.bindAll(ssm);
    setup.bindColumn("flux", aipsio);
    setup.bindColumn("spectrum", aipsio);
    setup.bindColumn("time", ism);
    setup.bindColumn("data", tsm);
    casacore::Table table(setup, 3);

    casacore::ScalarColumn<casacore::Int> antenna(table, "antenna");
    casacore::ScalarColumn<casacore::Double> flux(table, "flux");
    casacore::ArrayColumn<casacore::Float> spectrum(table, "spectrum");
    casacore::ScalarColumn<casacore::Double> time(table, "time");
    casacore::ArrayColumn<casacore::Complex> data(table, "data");
    const double times[3] = {100.0, 100.0, 110.0};
    for (int r = 0; r < 3; r++) {
        antenna.put(r, r);
        flux.put(r, r + 1.5);
        casacore::Vector<casacore::Float> s(r + 1);
        for (int k = 0; k <= r; k++) s(k) = static_cast<float>(r * 10 + k);
        spectrum.put(r, s);
        time.put(r, times[r]);
        casacore::Array<casacore::Complex> d(casacore::IPosition(2, 2, 3));
        casacore::Complex* cells = d.data();
        for (int k = 0; k < 6; k++)
            cells[k] = casacore::Complex(static_cast<float>(r * 10 + k), static_cast<float>(-k));
        data.put(r, d);
    }
    table.flush();
}

// The manager type that stores `column`, from casacore's own description.
std::string manager_of(const casacore::Table& table, const std::string& column) {
    casacore::Record info = table.dataManagerInfo();
    for (casacore::uInt i = 0; i < info.nfields(); i++) {
        const casacore::Record& manager = info.subRecord(i);
        casacore::Vector<casacore::String> columns(manager.asArrayString("COLUMNS"));
        for (const casacore::String& name : columns) {
            if (static_cast<const std::string&>(name) == column) return manager.asString("TYPE");
        }
    }
    return "";
}

void verify_mixed_managers_impl(const std::string& path) {
    casacore::Table table(path);
    if (table.nrow() != 3)
        throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));
    const std::pair<const char*, const char*> managers[] = {
        {"antenna", "StandardStMan"}, {"flux", "StManAipsIO"}, {"spectrum", "StManAipsIO"},
        {"time", "IncrementalStMan"}, {"data", "TiledShapeStMan"}};
    for (const auto& [column, type] : managers) {
        std::string found = manager_of(table, column);
        if (found != type)
            throw std::runtime_error(std::string(column) + " is stored by '" + found
                                     + "', expected " + type);
    }
    casacore::ScalarColumn<casacore::Int> antenna(table, "antenna");
    casacore::ScalarColumn<casacore::Double> flux(table, "flux");
    casacore::ArrayColumn<casacore::Float> spectrum(table, "spectrum");
    casacore::ScalarColumn<casacore::Double> time(table, "time");
    casacore::ArrayColumn<casacore::Complex> data(table, "data");
    const double times[3] = {100.0, 100.0, 110.0};
    for (int r = 0; r < 3; r++) {
        std::string row = " row " + std::to_string(r);
        if (antenna(r) != r) throw std::runtime_error("antenna mismatch" + row);
        if (flux(r) != r + 1.5) throw std::runtime_error("flux mismatch" + row);
        casacore::Vector<casacore::Float> s(spectrum(r));
        if (s.nelements() != static_cast<size_t>(r + 1))
            throw std::runtime_error("spectrum length mismatch" + row);
        for (int k = 0; k <= r; k++)
            if (s(k) != static_cast<float>(r * 10 + k))
                throw std::runtime_error("spectrum value mismatch" + row);
        if (time(r) != times[r]) throw std::runtime_error("time mismatch" + row);
        casacore::Array<casacore::Complex> d = data(r);
        if (d.shape() != casacore::IPosition(2, 2, 3))
            throw std::runtime_error("data shape mismatch" + row);
        const casacore::Complex* cells = d.data();
        for (int k = 0; k < 6; k++)
            if (cells[k] != casacore::Complex(static_cast<float>(r * 10 + k), static_cast<float>(-k)))
                throw std::runtime_error("data value mismatch" + row);
    }
}

} // anonymous namespace

extern "C" {

// Mixed data managers
int32_t cpp_table_write_mixed_managers(const char* path, char** out_error) {
    try { write_mixed_managers_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_mixed_managers(const char* path, char** out_error) {
    try { verify_mixed_managers_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

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
int32_t cpp_table_lock_read_relock(const char* path, uint64_t* out_rows,
                                   uint32_t* out_columns, char** out_error) {
    try { lock_read_relock_impl(path, out_rows, out_columns); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_keyword_table_name(const char* path, const char* column,
                                     const char* keyword, char* buf, int32_t bufsize,
                                     char** out_error) {
    try { keyword_table_name_impl(path, column, keyword, buf, bufsize); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_keyword_double_array(const char* path, const char* keyword,
                                       int64_t* shape, int32_t max_ndim, int32_t* out_ndim,
                                       double* values, int64_t max_values,
                                       int64_t* out_count, char** out_error) {
    try {
        keyword_double_array_impl(path, keyword, shape, max_ndim, out_ndim, values,
                                  max_values, out_count);
        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_held_reader_open(const char* path, void** out_reader,
                                   char** out_error) {
    try { *out_reader = new HeldTableReader(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_held_reader_read_id(void* reader, int32_t* out_id,
                                      char** out_error) {
    try {
        *out_id = held_reader_read_id_impl(*static_cast<HeldTableReader*>(reader));
        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
void cpp_table_held_reader_close(void* reader) {
    delete static_cast<HeldTableReader*>(reader);
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

// LOG table
int32_t cpp_table_write_log_table(const char* path, char** out_error) {
    try { write_log_table_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_log_table(const char* path, char** out_error) {
    try { verify_log_table_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

} // extern "C"
