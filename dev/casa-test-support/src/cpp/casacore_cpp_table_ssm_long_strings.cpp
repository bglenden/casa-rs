// StandardStMan fixture: ssm_long_strings, strings longer than a string
// bucket, which SSMStringHandler continues in linked buckets; and the same
// rows laid out as casa-rs writes them, updated in place, with casacore's
// account of the buckets.
#include "casacore_cpp_common.h"
#include <casacore/casa/Arrays/Vector.h>
#include <casacore/tables/DataMan/StandardStMan.h>
#include <casacore/tables/DataMan/StandardStManAccessor.h>

#include <sstream>
#include <vector>

using casacore_shim::make_error;

namespace {

// About `bytes` bytes of text, a run of one- and two-byte characters
// starting at character `seed`: the Rust fixture's `text`.
std::string text(size_t bytes, size_t seed) {
    static const char* const characters[] = {
        "a", "\xc3\xa9", "b", "\xc3\xa7", "c", "-", "\xc3\xbc", "d"};
    std::string out;
    for (size_t index = seed; out.size() < bytes; ++index) {
        out += characters[index % 8];
    }
    return out;
}

struct Row {
    std::string name;
    std::vector<std::string> fixed;
    std::vector<std::string> variable;
};

// The Rust fixture's rows: a name of 100 kB, one just over casa-rs's
// 32 KiB bucket and a short one; array elements of tens of kB beside short
// and empty ones.
std::vector<Row> long_string_rows() {
    return {
        {text(100000, 0), {text(70000, 1), "x", text(40000, 2)}, {text(33000, 3), text(5, 4)}},
        {"short", {"", "", ""}, {"a"}},
        {text(32800, 5), {"fixed", text(32769, 6), ""}, {text(65536, 7), "", text(1000, 8)}},
    };
}

casacore::Vector<casacore::String> strings(const std::vector<std::string>& values) {
    casacore::Vector<casacore::String> out(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        out(i) = values[i];
    }
    return out;
}

void require_strings(const casacore::Vector<casacore::String>& found,
                     const std::vector<std::string>& expected,
                     const std::string& what) {
    if (found.nelements() != expected.size()) {
        throw std::runtime_error(what + " has " + std::to_string(found.nelements()) +
                                 " elements, expected " + std::to_string(expected.size()));
    }
    for (size_t i = 0; i < expected.size(); ++i) {
        if (static_cast<const std::string&>(found(i)) != expected[i]) {
            throw std::runtime_error(what + " element " + std::to_string(i) + " mismatch");
        }
    }
}

void write_ssm_long_strings_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::String>("NAME"));
    td.addColumn(casacore::ArrayColumnDesc<casacore::String>("FIXED",
        casacore::IPosition(1, 3), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));
    // ndim=1, no shape: a variable-shape column, SSMIndStringColumn.
    td.addColumn(casacore::ArrayColumnDesc<casacore::String>("VARIABLE", 1));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);
    const std::vector<Row> rows = long_string_rows();
    casacore::Table table(setup, rows.size());

    casacore::ScalarColumn<casacore::String> name(table, "NAME");
    casacore::ArrayColumn<casacore::String> fixed(table, "FIXED");
    casacore::ArrayColumn<casacore::String> variable(table, "VARIABLE");
    for (size_t row = 0; row < rows.size(); ++row) {
        name.put(row, rows[row].name);
        fixed.put(row, strings(rows[row].fixed));
        variable.put(row, strings(rows[row].variable));
    }
    table.flush();
}

// Check every cell of the table at `path` against `rows`.
void require_rows(const std::string& path, const std::vector<Row>& rows) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != rows.size()) {
        throw std::runtime_error("expected " + std::to_string(rows.size()) + " rows, found " +
                                 std::to_string(table.nrow()));
    }

    casacore::ScalarColumn<casacore::String> name(table, "NAME");
    casacore::ArrayColumn<casacore::String> fixed(table, "FIXED");
    casacore::ArrayColumn<casacore::String> variable(table, "VARIABLE");
    for (size_t row = 0; row < rows.size(); ++row) {
        const std::string at = "row " + std::to_string(row) + " ";
        const casacore::String stored = name(row);
        if (static_cast<const std::string&>(stored) != rows[row].name) {
            throw std::runtime_error(at + "NAME mismatch");
        }
        require_strings(casacore::Vector<casacore::String>(fixed(row)), rows[row].fixed, at + "FIXED");
        require_strings(casacore::Vector<casacore::String>(variable(row)), rows[row].variable,
                        at + "VARIABLE");
    }
}

void verify_ssm_long_strings_impl(const std::string& path) {
    require_rows(path, long_string_rows());
}

// The rows as casa-rs writes them, column by column, in its 32 KiB buckets:
// strings then lie in the string buckets as casa-rs lays them out.
void write_ssm_long_strings_by_column_impl(const std::string& path,
                                           casacore::Table::EndianFormat endian) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::String>("NAME"));
    td.addColumn(casacore::ArrayColumnDesc<casacore::String>("FIXED",
        casacore::IPosition(1, 3), casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape));
    td.addColumn(casacore::ArrayColumnDesc<casacore::String>("VARIABLE", 1));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman("SSM", 32768);
    setup.bindAll(stman);
    const std::vector<Row> rows = long_string_rows();
    casacore::Table table(setup, rows.size(), casacore::False, endian);

    casacore::ScalarColumn<casacore::String> name(table, "NAME");
    casacore::ArrayColumn<casacore::String> fixed(table, "FIXED");
    casacore::ArrayColumn<casacore::String> variable(table, "VARIABLE");
    for (size_t row = 0; row < rows.size(); ++row) name.put(row, rows[row].name);
    for (size_t row = 0; row < rows.size(); ++row) fixed.put(row, strings(rows[row].fixed));
    for (size_t row = 0; row < rows.size(); ++row) variable.put(row, strings(rows[row].variable));
    table.flush();
}

// The rows after `update_ssm_long_strings_impl`.
std::vector<Row> updated_long_string_rows() {
    std::vector<Row> rows = long_string_rows();
    rows[0].name = std::string(1700, 'n');
    rows[0].fixed = {std::string(13, 'f'), std::string(111, 'g'), std::string(25, 'h')};
    rows[2].variable = {std::string(250000, 'v'), std::string(20, 'w')};
    rows.push_back({std::string(50000, 'm'), {"p", "q", "r"}, {"s"}});
    return rows;
}

// Update the long-string table in place: shrink a scalar and a fixed array
// that span several buckets, grow a variable array into the buckets that
// frees, append a row of a long scalar; then reopen it and check every cell.
void update_ssm_long_strings_impl(const std::string& path) {
    {
        casacore::Table table(path, casacore::Table::Update);
        casacore::ScalarColumn<casacore::String> name(table, "NAME");
        casacore::ArrayColumn<casacore::String> fixed(table, "FIXED");
        casacore::ArrayColumn<casacore::String> variable(table, "VARIABLE");
        const std::vector<Row> rows = updated_long_string_rows();
        name.put(0, rows[0].name);
        fixed.put(0, strings(rows[0].fixed));
        variable.put(2, strings(rows[2].variable));
        table.addRow(1);
        name.put(3, rows[3].name);
        fixed.put(3, strings(rows[3].fixed));
        variable.put(3, strings(rows[3].variable));
        table.flush();
    }
    require_rows(path, updated_long_string_rows());
}

// casacore's account of the StandardStMan holding column NAME, as
// `showBaseStatistics` prints it: total buckets, free buckets, the first
// free bucket, the last string bucket in use, the index buckets, the first
// index bucket and the index's offset in it.
void ssm_statistics_impl(const std::string& path, int64_t* out) {
    casacore::Table table(path, casacore::Table::Old);
    // SSMBase reads its header when a cell is first read.
    casacore::ScalarColumn<casacore::String>(table, "NAME").get(0);
    casacore::ROStandardStManAccessor accessor(table, "NAME", casacore::True);
    std::ostringstream shown;
    accessor.showBaseStatistics(shown);
    constexpr int count = 7;
    const char* const labels[count] = {"Total buckets", "Total free buckets", "1st free bucket",
                                       "last String bucket used", "Total Index buckets",
                                       "1st Index bucket", "Index bucket offset"};
    std::istringstream lines(shown.str());
    bool found[count] = {};
    for (std::string line; std::getline(lines, line);) {
        const size_t colon = line.find(':');
        if (colon == std::string::npos) continue;
        std::string label = line.substr(0, colon);
        label.erase(label.find_last_not_of(' ') + 1);
        for (int i = 0; i < count; ++i) {
            if (label == labels[i]) {
                out[i] = std::stoll(line.substr(colon + 1));
                found[i] = true;
            }
        }
    }
    for (int i = 0; i < count; ++i) {
        if (!found[i]) throw std::runtime_error(std::string("no '") + labels[i] + "' statistic");
    }
}

} // namespace

extern "C" {

int32_t cpp_table_write_ssm_long_strings(const char* path, char** out_error) {
    try { write_ssm_long_strings_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_verify_ssm_long_strings(const char* path, char** out_error) {
    try { verify_ssm_long_strings_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_write_ssm_long_strings_by_column(const char* path, int32_t big_endian,
                                                   char** out_error) {
    try {
        write_ssm_long_strings_by_column_impl(
            path, big_endian ? casacore::Table::BigEndian : casacore::Table::LittleEndian);
        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_update_ssm_long_strings(const char* path, char** out_error) {
    try { update_ssm_long_strings_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}
int32_t cpp_table_ssm_statistics(const char* path, int64_t* out_statistics, char** out_error) {
    try { ssm_statistics_impl(path, out_statistics); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

} // extern "C"
