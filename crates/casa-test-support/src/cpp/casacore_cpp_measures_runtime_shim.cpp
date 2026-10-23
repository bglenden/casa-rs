// SPDX-License-Identifier: LGPL-3.0-or-later
// C++ interop shim: the measures tables casacore reads for the oracle.
//
// casacore finds its measures tables (IERS, TAI_UTC, Observatories, ...)
// through MeasIERS::findTab, whose first source is the AppState measures
// directory. The oracle installs the Rust runtime's root there before any
// other casacore call, so both sides of a measures law read one table set
// (#845).

#include <cstring>
#include <string>

#include <casacore/casa/System/AppState.h>
#include <casacore/measures/Measures/MeasIERS.h>
#include <casacore/tables/Tables/Table.h>
#include <casacore/tables/Tables/TableRecord.h>

using namespace casacore;

namespace {

class SharedMeasuresState : public AppState {
public:
    explicit SharedMeasuresState(std::string directory) : directory_(std::move(directory)) {}

    std::string measuresDir() const override { return directory_; }

    bool initialized() const override { return true; }

private:
    std::string directory_;
};

void copy_out(const std::string &value, char *out, size_t capacity) {
    if (capacity == 0) {
        return;
    }
    size_t length = value.size() < capacity - 1 ? value.size() : capacity - 1;
    std::memcpy(out, value.data(), length);
    out[length] = '\0';
}

}  // namespace

extern "C" {

// Make `path` (a measures runtime root holding geodetic/ and ephemerides/)
// the directory casacore searches first for every measures table.
int measures_shim_use_measures_directory(const char *path) {
    try {
        AppStateSource::initialize(new SharedMeasuresState(path));
        return 0;
    } catch (...) {
        return -1;
    }
}

// Report the table casacore resolves for a geodetic measures table `name`,
// and its VS_DATE keyword.
int measures_shim_measures_table_identity(
    const char *name, char *path_out, size_t path_capacity, char *date_out,
    size_t date_capacity) {
    try {
        Table table;
        String table_name(name);
        if (!MeasIERS::findTab(table, nullptr, "measures." + table_name + ".directory",
                               "geodetic", table_name)) {
            return 1;
        }
        const TableRecord &keywords = table.keywordSet();
        copy_out(table.tableName(), path_out, path_capacity);
        copy_out(keywords.isDefined("VS_DATE") ? keywords.asString("VS_DATE") : String(),
                 date_out, date_capacity);
        return 0;
    } catch (...) {
        return -1;
    }
}

}  // extern "C"
