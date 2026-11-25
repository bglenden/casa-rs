// Complex32 variable-shape array fixtures for both StManAipsIO and StandardStMan.
// Pattern matches MS DATA column: shape [num_corr, num_chan] with Complex values.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/StManAipsIO.h>
#include <casacore/tables/DataMan/StandardStMan.h>

using casacore_shim::make_error;
static casacore_shim::TerminateGuard g_terminate_guard_complex_vararray;

namespace {

// Common data pattern: 4 rows of Complex variable-shape arrays.
// Row 0: shape [2,4], values (1,0.5), (2,1.0), ..., (8,4.0)
// Row 1: shape [4,2], values (9,4.5), (10,5.0), ..., (16,8.0)
// Row 2: shape [4,2], values (17,8.5), (18,9.0), ..., (24,12.0)
// Row 3: shape [2,4], values (25,12.5), (26,13.0), ..., (32,16.0)

void fill_complex_vararray(casacore::ArrayColumn<casacore::Complex>& col) {
    float re = 1.0f;
    float im = 0.5f;
    // Row 0: [2,4]
    {
        casacore::IPosition shape(2, 2, 4);
        col.setShape(0, shape);
        casacore::Array<casacore::Complex> arr(shape);
        for (int j = 0; j < 4; j++) {
            for (int i = 0; i < 2; i++) {
                arr(casacore::IPosition(2, i, j)) = casacore::Complex(re, im);
                re += 1.0f;
                im += 0.5f;
            }
        }
        col.put(0, arr);
    }
    // Row 1: [4,2]
    {
        casacore::IPosition shape(2, 4, 2);
        col.setShape(1, shape);
        casacore::Array<casacore::Complex> arr(shape);
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 4; i++) {
                arr(casacore::IPosition(2, i, j)) = casacore::Complex(re, im);
                re += 1.0f;
                im += 0.5f;
            }
        }
        col.put(1, arr);
    }
    // Row 2: [4,2]
    {
        casacore::IPosition shape(2, 4, 2);
        col.setShape(2, shape);
        casacore::Array<casacore::Complex> arr(shape);
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 4; i++) {
                arr(casacore::IPosition(2, i, j)) = casacore::Complex(re, im);
                re += 1.0f;
                im += 0.5f;
            }
        }
        col.put(2, arr);
    }
    // Row 3: [2,4]
    {
        casacore::IPosition shape(2, 2, 4);
        col.setShape(3, shape);
        casacore::Array<casacore::Complex> arr(shape);
        for (int j = 0; j < 4; j++) {
            for (int i = 0; i < 2; i++) {
                arr(casacore::IPosition(2, i, j)) = casacore::Complex(re, im);
                re += 1.0f;
                im += 0.5f;
            }
        }
        col.put(3, arr);
    }
}

void verify_complex_vararray(casacore::ArrayColumn<casacore::Complex>& col) {
    if (col.nrow() != 4)
        throw std::runtime_error("expected 4 rows, got " + std::to_string(col.nrow()));

    float re = 1.0f;
    float im = 0.5f;

    auto check_cell = [&](casacore::uInt row, int dim0, int dim1) {
        casacore::IPosition expected_shape(2, dim0, dim1);
        casacore::Array<casacore::Complex> arr = col(row);
        if (!arr.shape().isEqual(expected_shape))
            throw std::runtime_error("row " + std::to_string(row) + " shape mismatch: expected ["
                + std::to_string(dim0) + "," + std::to_string(dim1) + "] got ["
                + std::to_string(arr.shape()[0]) + "," + std::to_string(arr.shape()[1]) + "]");
        for (int j = 0; j < dim1; j++) {
            for (int i = 0; i < dim0; i++) {
                casacore::Complex expected(re, im);
                casacore::Complex actual = arr(casacore::IPosition(2, i, j));
                if (std::abs(actual.real() - expected.real()) > 1e-5f ||
                    std::abs(actual.imag() - expected.imag()) > 1e-5f) {
                    throw std::runtime_error("row " + std::to_string(row)
                        + " [" + std::to_string(i) + "," + std::to_string(j)
                        + "] mismatch: expected (" + std::to_string(re) + "," + std::to_string(im)
                        + ") got (" + std::to_string(actual.real()) + "," + std::to_string(actual.imag()) + ")");
                }
                re += 1.0f;
                im += 0.5f;
            }
        }
    };

    check_cell(0, 2, 4);
    check_cell(1, 4, 2);
    check_cell(2, 4, 2);
    check_cell(3, 2, 4);
}

// --- AipsIO ---

void write_aipsio_complex_variable_array_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ArrayColumnDesc<casacore::Complex>("data", 2));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 4);
    casacore::ArrayColumn<casacore::Complex> col(table, "data");
    fill_complex_vararray(col);
    table.flush();
}

void verify_aipsio_complex_variable_array_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    casacore::ArrayColumn<casacore::Complex> col(table, "data");
    verify_complex_vararray(col);
}

// --- StandardStMan ---

void write_ssm_complex_variable_array_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ArrayColumnDesc<casacore::Complex>("data", 2));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StandardStMan stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 4);
    casacore::ArrayColumn<casacore::Complex> col(table, "data");
    fill_complex_vararray(col);
    table.flush();
}

void verify_ssm_complex_variable_array_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    casacore::ArrayColumn<casacore::Complex> col(table, "data");
    verify_complex_vararray(col);
}

} // anonymous namespace

extern "C" {

int32_t cpp_table_write_aipsio_complex_variable_array(const char* path, char** out_error) {
    try { write_aipsio_complex_variable_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

int32_t cpp_table_verify_aipsio_complex_variable_array(const char* path, char** out_error) {
    try { verify_aipsio_complex_variable_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

int32_t cpp_table_write_ssm_complex_variable_array(const char* path, char** out_error) {
    try { write_ssm_complex_variable_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

int32_t cpp_table_verify_ssm_complex_variable_array(const char* path, char** out_error) {
    try { verify_ssm_complex_variable_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception"); return -1; }
}

} // extern "C"
