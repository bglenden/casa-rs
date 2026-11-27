// SPDX-License-Identifier: LGPL-3.0-or-later
// Virtual column engine fixtures: ForwardColumnEngine, ScaledArrayEngine.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/ForwardCol.h>
#include <casacore/tables/DataMan/ScaledArrayEngine.h>
#include <casacore/tables/DataMan/StManAipsIO.h>

using casacore_shim::make_error;

namespace {

// ===== forward_column fixture =====
// Creates two tables:
//   base_table: col_value(Double) with 3 rows [1.5, 2.5, 3.5]
//   fwd_table:  col_value(Double) via ForwardColumnEngine -> base_table
// The test path is the fwd_table directory; base_table is a sibling.

void write_forward_column_fixture_impl(const std::string& path) {
    std::string base_path = path + "_base";

    // 1. Create base table with StManAipsIO.
    {
        casacore::TableDesc td("", casacore::TableDesc::Scratch);
        td.addColumn(casacore::ScalarColumnDesc<casacore::Double>("col_value"));
        casacore::SetupNewTable setup(base_path, td, casacore::Table::New);
        casacore::StManAipsIO stman;
        setup.bindAll(stman);
        casacore::Table base(setup, 3);
        casacore::ScalarColumn<casacore::Double> col(base, "col_value");
        col.put(0, 1.5);
        col.put(1, 2.5);
        col.put(2, 3.5);
    }

    // 2. Create forwarding table.
    {
        casacore::TableDesc td("", casacore::TableDesc::Scratch);
        td.addColumn(casacore::ScalarColumnDesc<casacore::Double>("col_value"));
        casacore::SetupNewTable setup(path, td, casacore::Table::New);
        casacore::ForwardColumnEngine fwdEngine(
            casacore::Table(base_path), "ForwardColumnEngine");
        setup.bindAll(fwdEngine);
        casacore::Table fwd(setup, 3);
    }
}

void verify_forward_column_fixture_impl(const std::string& path) {
    casacore::Table t(path);
    if (t.nrow() != 3)
        throw std::runtime_error("expected 3 rows, got " +
                                  std::to_string(t.nrow()));
    casacore::ScalarColumn<casacore::Double> col(t, "col_value");
    double expected[] = {1.5, 2.5, 3.5};
    for (casacore::uInt i = 0; i < 3; ++i) {
        double val = col(i);
        if (std::abs(val - expected[i]) > 1e-10)
            throw std::runtime_error(
                "row " + std::to_string(i) + ": expected " +
                std::to_string(expected[i]) + " got " + std::to_string(val));
    }
}

// ===== scaled_array fixture =====
// Creates a table with:
//   stored_col: ArrayColumnDesc<Int> (fixed shape [2]) with values [[1,2],[3,4],[5,6]]
//   virtual_col: ArrayColumnDesc<Double> via ScaledArrayEngine
//     with scale=2.5, offset=10.0
// Expected virtual values: [[12.5,15.0],[17.5,20.0],[22.5,25.0]]
// Note: ScaledArrayEngine requires array columns (inherits BaseMappedArrayEngine).

void write_scaled_array_fixture_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    // Use ColumnDesc::Direct so StManAipsIO stores as StManColumnArrayAipsIO
    // (direct arrays), not StManColumnIndArrayAipsIO (indirect arrays).
    td.addColumn(casacore::ArrayColumnDesc<casacore::Int>(
        "stored_col", casacore::IPosition(1, 2), casacore::ColumnDesc::Direct));
    td.addColumn(casacore::ArrayColumnDesc<casacore::Double>(
        "virtual_col", casacore::IPosition(1, 2), casacore::ColumnDesc::Direct));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindColumn("stored_col", stman);
    casacore::ScaledArrayEngine<casacore::Double, casacore::Int> scaleEngine(
        "virtual_col", "stored_col", 2.5, 10.0);
    setup.bindColumn("virtual_col", scaleEngine);
    casacore::Table t(setup, 3);
    casacore::ArrayColumn<casacore::Int> stored(t, "stored_col");
    stored.put(0, casacore::Array<casacore::Int>(casacore::IPosition(1, 2), 0));
    stored.put(1, casacore::Array<casacore::Int>(casacore::IPosition(1, 2), 0));
    stored.put(2, casacore::Array<casacore::Int>(casacore::IPosition(1, 2), 0));
    // Fill with values: row0=[1,2], row1=[3,4], row2=[5,6]
    {
        casacore::Array<casacore::Int> arr(casacore::IPosition(1, 2));
        arr(casacore::IPosition(1, 0)) = 1;
        arr(casacore::IPosition(1, 1)) = 2;
        stored.put(0, arr);
        arr(casacore::IPosition(1, 0)) = 3;
        arr(casacore::IPosition(1, 1)) = 4;
        stored.put(1, arr);
        arr(casacore::IPosition(1, 0)) = 5;
        arr(casacore::IPosition(1, 1)) = 6;
        stored.put(2, arr);
    }
}

void verify_scaled_array_fixture_impl(const std::string& path) {
    // ScaledArrayEngine<Double,Int> is not registered by default in casacore
    // (only <Complex,Short> and <Float,Short> are). Register explicitly.
    casacore::ScaledArrayEngine<casacore::Double, casacore::Int>::registerClass();
    casacore::Table t(path);
    if (t.nrow() != 3)
        throw std::runtime_error("expected 3 rows, got " +
                                  std::to_string(t.nrow()));
    casacore::ArrayColumn<casacore::Double> vcol(t, "virtual_col");
    // Expected: stored * 2.5 + 10.0
    // Row 0: [1*2.5+10, 2*2.5+10] = [12.5, 15.0]
    // Row 1: [3*2.5+10, 4*2.5+10] = [17.5, 20.0]
    // Row 2: [5*2.5+10, 6*2.5+10] = [22.5, 25.0]
    double expected[][2] = {{12.5, 15.0}, {17.5, 20.0}, {22.5, 25.0}};
    for (casacore::uInt i = 0; i < 3; ++i) {
        casacore::Array<casacore::Double> arr = vcol(i);
        for (int j = 0; j < 2; ++j) {
            double val = arr(casacore::IPosition(1, j));
            if (std::abs(val - expected[i][j]) > 1e-10)
                throw std::runtime_error(
                    "row " + std::to_string(i) + " elem " + std::to_string(j) +
                    ": expected " + std::to_string(expected[i][j]) +
                    " got " + std::to_string(val));
        }
    }
    // Also verify stored column is intact.
    casacore::ArrayColumn<casacore::Int> scol(t, "stored_col");
    int expected_stored[][2] = {{1, 2}, {3, 4}, {5, 6}};
    for (casacore::uInt i = 0; i < 3; ++i) {
        casacore::Array<casacore::Int> arr = scol(i);
        for (int j = 0; j < 2; ++j) {
            int val = arr(casacore::IPosition(1, j));
            if (val != expected_stored[i][j])
                throw std::runtime_error(
                    "stored row " + std::to_string(i) + " elem " + std::to_string(j) +
                    ": expected " + std::to_string(expected_stored[i][j]) +
                    " got " + std::to_string(val));
        }
    }
}

// NOTE: ScaledArrayEngine<Complex,Short> C++ fixture omitted because casacore
// 3.7.1's ScaledArrayEngine.tcc has a template bug: `if (offset == 0)` compares
// std::complex<float> with int, which Apple Clang rejects. The Rust RR test
// exercises Complex32 output independently.

} // anonymous namespace

extern "C" {

int32_t cpp_table_write_forward_column_fixture(const char* path, char** out_error) {
    try {
        write_forward_column_fixture_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in write_forward_column_fixture");
        return -1;
    }
}

int32_t cpp_table_verify_forward_column_fixture(const char* path, char** out_error) {
    try {
        verify_forward_column_fixture_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in verify_forward_column_fixture");
        return -1;
    }
}

int32_t cpp_table_write_scaled_array_fixture(const char* path, char** out_error) {
    try {
        write_scaled_array_fixture_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in write_scaled_array_fixture");
        return -1;
    }
}

int32_t cpp_table_verify_scaled_array_fixture(const char* path, char** out_error) {
    try {
        verify_scaled_array_fixture_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in verify_scaled_array_fixture");
        return -1;
    }
}

} // extern "C"
