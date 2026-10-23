// SPDX-License-Identifier: LGPL-3.0-or-later
// C++ interop shim: the process-global measures state casacore reads for the
// oracle, namely its measures tables and its IAU model.
//
// casacore finds its measures tables (IERS, TAI_UTC, Observatories, ...)
// through MeasIERS::findTab, whose first source is the AppState measures
// directory. The oracle installs the Rust runtime's root there before any
// other casacore call, so both sides of a measures law read one table set.
//
// The IAU model is two process-global Aipsrc switches. The Rust oracle runs
// the operations here that set them under an exclusive lock (#845).

#include <cstring>
#include <string>

#include <casacore/casa/Quanta/MVEpoch.h>
#include <casacore/casa/Quanta/MVPosition.h>
#include <casacore/casa/Quanta/Quantum.h>
#include <casacore/casa/System/AipsrcValue.h>
#include <casacore/casa/System/AppState.h>
#include <casacore/measures/Measures/MCEpoch.h>
#include <casacore/measures/Measures/MEpoch.h>
#include <casacore/measures/Measures/MPosition.h>
#include <casacore/measures/Measures/MeasConvert.h>
#include <casacore/measures/Measures/MeasFrame.h>
#include <casacore/measures/Measures/MeasIERS.h>
#include <casacore/measures/Measures/MeasTable.h>
#include <casacore/tables/Tables/Table.h>
#include <casacore/tables/Tables/TableRecord.h>

using namespace casacore;

namespace {

// Switch casacore to IAU 2006/2000A for one scope, restoring IAU 1976/1980
// on exit, including exit by exception.
class Iau2000AScope {
public:
    Iau2000AScope() {
        // Register the switches through MeasTable first, so the ids below are
        // the ones MeasTable reads.
        (void)MeasTable::useIAU2000();
        (void)MeasTable::useIAU2000A();
        use_ = AipsrcValue<Bool>::registerRC("measures.iau2000.b_use", False);
        use_2000a_ = AipsrcValue<Bool>::registerRC("measures.iau2000.b_use2000a", False);
        AipsrcValue<Bool>::set(use_, True);
        AipsrcValue<Bool>::set(use_2000a_, True);
    }

    ~Iau2000AScope() {
        AipsrcValue<Bool>::set(use_, False);
        AipsrcValue<Bool>::set(use_2000a_, False);
    }

    bool active() const { return MeasTable::useIAU2000() && MeasTable::useIAU2000A(); }

private:
    uInt use_ = 0;
    uInt use_2000a_ = 0;
};

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

// Convert an epoch under IAU 2006/2000A, with an optional WGS84 observatory
// (all-zero coordinates mean none). Returns the full value, day count
// included.
int measures_shim_epoch_convert_iau2000a(
    double mjd_in, const char *ref_in, const char *ref_out, double obs_lon, double obs_lat,
    double obs_h, double *mjd_out) {
    try {
        Iau2000AScope model;
        if (!model.active()) {
            return -2;
        }
        MEpoch::Types type_in;
        MEpoch::Types type_out;
        if (!MEpoch::getType(type_in, String(ref_in)) ||
            !MEpoch::getType(type_out, String(ref_out))) {
            return -3;
        }
        MeasFrame frame;
        if (obs_lon != 0.0 || obs_lat != 0.0 || obs_h != 0.0) {
            frame.set(MPosition(MVPosition(Quantity(obs_h, "m"), Quantity(obs_lon, "rad"),
                                           Quantity(obs_lat, "rad")),
                                MPosition::WGS84));
        }
        MEpoch epoch(MVEpoch(mjd_in), type_in);
        *mjd_out = MEpoch::Convert(epoch, MEpoch::Ref(type_out, frame))().getValue().get();
        return 0;
    } catch (...) {
        return -1;
    }
}

}  // extern "C"
