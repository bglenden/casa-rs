// Shared header for casacore C++ interop test shims.
// Provides common includes, the TerminateGuard, and make_error utility.
#pragma once

#include <casacore/casa/aips.h>
#include <casacore/casa/BasicSL/String.h>
#include <casacore/casa/BasicSL/Complex.h>
#include <casacore/tables/Tables/Table.h>
#include <casacore/tables/Tables/TableDesc.h>
#include <casacore/tables/Tables/SetupNewTab.h>
#include <casacore/tables/Tables/ScaColDesc.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/ScalarColumn.h>
#include <casacore/tables/Tables/ArrayColumn.h>
#include <casacore/tables/Tables/TableRecord.h>
#include <casacore/tables/Tables/TableLock.h>
#include <casacore/casa/Arrays/Array.h>
#include <casacore/casa/Arrays/IPosition.h>

#include <climits>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>

namespace casacore_shim {

// Install a custom terminate handler that prints a stack trace
// before aborting, to help diagnose exceptions in destructors.
struct TerminateGuard {
    std::terminate_handler prev;
    TerminateGuard() {
        prev = std::set_terminate([] {
            std::cerr << "[casacore_cpp_shim] std::terminate called";
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

inline char* make_error(const std::string& msg) {
    char* result = static_cast<char*>(std::malloc(msg.size() + 1));
    if (result) {
        std::memcpy(result, msg.c_str(), msg.size() + 1);
    }
    return result;
}

} // namespace casacore_shim
