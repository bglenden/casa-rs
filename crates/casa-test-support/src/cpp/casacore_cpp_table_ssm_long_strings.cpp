// StandardStMan fixture: ssm_long_strings, strings longer than a string
// bucket, which SSMStringHandler continues in linked buckets.
#include "casacore_cpp_common.h"
#include <casacore/casa/Arrays/Vector.h>
#include <casacore/tables/DataMan/StandardStMan.h>

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

void verify_ssm_long_strings_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    const std::vector<Row> rows = long_string_rows();
    if (table.nrow() != rows.size()) throw std::runtime_error("expected 3 rows");

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

} // extern "C"
