// SPDX-License-Identifier: LGPL-3.0-or-later
// casacore's IAU model for one scope of a measures shim.
//
// The model is two process-global Aipsrc switches. The Rust oracle runs every
// shim operation that sets them under an exclusive lock (#845).
#pragma once

#include <casacore/casa/System/AipsrcValue.h>
#include <casacore/measures/Measures/MeasTable.h>

namespace casacore_shim {

// Switch casacore to IAU 2006/2000A for one scope, restoring IAU 1976/1980
// on exit, including exit by exception.
class Iau2000AScope {
public:
    Iau2000AScope() {
        // Register the switches through MeasTable first, so the ids below are
        // the ones MeasTable reads.
        (void)casacore::MeasTable::useIAU2000();
        (void)casacore::MeasTable::useIAU2000A();
        use_ = casacore::AipsrcValue<casacore::Bool>::registerRC("measures.iau2000.b_use",
                                                                 casacore::False);
        use_2000a_ = casacore::AipsrcValue<casacore::Bool>::registerRC(
            "measures.iau2000.b_use2000a", casacore::False);
        casacore::AipsrcValue<casacore::Bool>::set(use_, casacore::True);
        casacore::AipsrcValue<casacore::Bool>::set(use_2000a_, casacore::True);
    }

    ~Iau2000AScope() {
        casacore::AipsrcValue<casacore::Bool>::set(use_, casacore::False);
        casacore::AipsrcValue<casacore::Bool>::set(use_2000a_, casacore::False);
    }

    Iau2000AScope(const Iau2000AScope &) = delete;
    Iau2000AScope &operator=(const Iau2000AScope &) = delete;

    bool active() const {
        return casacore::MeasTable::useIAU2000() && casacore::MeasTable::useIAU2000A();
    }

private:
    casacore::uInt use_ = 0;
    casacore::uInt use_2000a_ = 0;
};

}  // namespace casacore_shim
