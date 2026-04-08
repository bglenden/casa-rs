// AipsIO storage manager fixtures: variable-shape arrays.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/StManAipsIO.h>

using casacore_shim::make_error;
static casacore_shim::TerminateGuard g_terminate_guard_vararray;

namespace {

void write_aipsio_variable_array_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    // ndim=2, no shape arg => variable-shape column (no Direct | FixedShape)
    td.addColumn(casacore::ArrayColumnDesc<casacore::Float>("data", 2));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, 4);
    casacore::ArrayColumn<casacore::Float> colData(table, "data");

    // Row 0: shape [2,3], values 1.0..6.0 (column-major: [0,0]=1,[1,0]=2,[0,1]=3,[1,1]=4,[0,2]=5,[1,2]=6)
    {
        casacore::IPosition shape(2, 2, 3);
        colData.setShape(0, shape);
        casacore::Array<casacore::Float> arr(shape);
        arr(casacore::IPosition(2, 0, 0)) = 1.0f;
        arr(casacore::IPosition(2, 1, 0)) = 2.0f;
        arr(casacore::IPosition(2, 0, 1)) = 3.0f;
        arr(casacore::IPosition(2, 1, 1)) = 4.0f;
        arr(casacore::IPosition(2, 0, 2)) = 5.0f;
        arr(casacore::IPosition(2, 1, 2)) = 6.0f;
        colData.put(0, arr);
    }

    // Row 1: shape [3,2], values 7.0..12.0
    {
        casacore::IPosition shape(2, 3, 2);
        colData.setShape(1, shape);
        casacore::Array<casacore::Float> arr(shape);
        arr(casacore::IPosition(2, 0, 0)) = 7.0f;
        arr(casacore::IPosition(2, 1, 0)) = 8.0f;
        arr(casacore::IPosition(2, 2, 0)) = 9.0f;
        arr(casacore::IPosition(2, 0, 1)) = 10.0f;
        arr(casacore::IPosition(2, 1, 1)) = 11.0f;
        arr(casacore::IPosition(2, 2, 1)) = 12.0f;
        colData.put(1, arr);
    }

    // Row 2: shape [3,2], values 13.0..18.0
    {
        casacore::IPosition shape(2, 3, 2);
        colData.setShape(2, shape);
        casacore::Array<casacore::Float> arr(shape);
        arr(casacore::IPosition(2, 0, 0)) = 13.0f;
        arr(casacore::IPosition(2, 1, 0)) = 14.0f;
        arr(casacore::IPosition(2, 2, 0)) = 15.0f;
        arr(casacore::IPosition(2, 0, 1)) = 16.0f;
        arr(casacore::IPosition(2, 1, 1)) = 17.0f;
        arr(casacore::IPosition(2, 2, 1)) = 18.0f;
        colData.put(2, arr);
    }

    // Row 3: shape [2,3], values 19.0..24.0
    {
        casacore::IPosition shape(2, 2, 3);
        colData.setShape(3, shape);
        casacore::Array<casacore::Float> arr(shape);
        arr(casacore::IPosition(2, 0, 0)) = 19.0f;
        arr(casacore::IPosition(2, 1, 0)) = 20.0f;
        arr(casacore::IPosition(2, 0, 1)) = 21.0f;
        arr(casacore::IPosition(2, 1, 1)) = 22.0f;
        arr(casacore::IPosition(2, 0, 2)) = 23.0f;
        arr(casacore::IPosition(2, 1, 2)) = 24.0f;
        colData.put(3, arr);
    }

    table.flush();
}

void verify_aipsio_variable_array_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);

    if (table.nrow() != 4)
        throw std::runtime_error("expected 4 rows, got " + std::to_string(table.nrow()));

    casacore::ArrayColumn<casacore::Float> colData(table, "data");

    // Row 0: shape [2,3], values 1.0..6.0
    {
        casacore::Array<casacore::Float> arr = colData(0);
        casacore::IPosition expectedShape(2, 2, 3);
        if (!arr.shape().isEqual(expectedShape))
            throw std::runtime_error("row 0 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 1.0f)
            throw std::runtime_error("row 0 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 0)) != 2.0f)
            throw std::runtime_error("row 0 [1,0] mismatch");
        if (arr(casacore::IPosition(2, 0, 1)) != 3.0f)
            throw std::runtime_error("row 0 [0,1] mismatch");
        if (arr(casacore::IPosition(2, 1, 1)) != 4.0f)
            throw std::runtime_error("row 0 [1,1] mismatch");
        if (arr(casacore::IPosition(2, 0, 2)) != 5.0f)
            throw std::runtime_error("row 0 [0,2] mismatch");
        if (arr(casacore::IPosition(2, 1, 2)) != 6.0f)
            throw std::runtime_error("row 0 [1,2] mismatch");
    }

    // Row 1: shape [3,2], values 7.0..12.0
    {
        casacore::Array<casacore::Float> arr = colData(1);
        casacore::IPosition expectedShape(2, 3, 2);
        if (!arr.shape().isEqual(expectedShape))
            throw std::runtime_error("row 1 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 7.0f)
            throw std::runtime_error("row 1 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 0)) != 8.0f)
            throw std::runtime_error("row 1 [1,0] mismatch");
        if (arr(casacore::IPosition(2, 2, 0)) != 9.0f)
            throw std::runtime_error("row 1 [2,0] mismatch");
        if (arr(casacore::IPosition(2, 0, 1)) != 10.0f)
            throw std::runtime_error("row 1 [0,1] mismatch");
        if (arr(casacore::IPosition(2, 1, 1)) != 11.0f)
            throw std::runtime_error("row 1 [1,1] mismatch");
        if (arr(casacore::IPosition(2, 2, 1)) != 12.0f)
            throw std::runtime_error("row 1 [2,1] mismatch");
    }

    // Row 2: shape [3,2], values 13.0..18.0
    {
        casacore::Array<casacore::Float> arr = colData(2);
        casacore::IPosition expectedShape(2, 3, 2);
        if (!arr.shape().isEqual(expectedShape))
            throw std::runtime_error("row 2 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 13.0f)
            throw std::runtime_error("row 2 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 0)) != 14.0f)
            throw std::runtime_error("row 2 [1,0] mismatch");
        if (arr(casacore::IPosition(2, 2, 0)) != 15.0f)
            throw std::runtime_error("row 2 [2,0] mismatch");
        if (arr(casacore::IPosition(2, 0, 1)) != 16.0f)
            throw std::runtime_error("row 2 [0,1] mismatch");
        if (arr(casacore::IPosition(2, 1, 1)) != 17.0f)
            throw std::runtime_error("row 2 [1,1] mismatch");
        if (arr(casacore::IPosition(2, 2, 1)) != 18.0f)
            throw std::runtime_error("row 2 [2,1] mismatch");
    }

    // Row 3: shape [2,3], values 19.0..24.0
    {
        casacore::Array<casacore::Float> arr = colData(3);
        casacore::IPosition expectedShape(2, 2, 3);
        if (!arr.shape().isEqual(expectedShape))
            throw std::runtime_error("row 3 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 19.0f)
            throw std::runtime_error("row 3 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 0)) != 20.0f)
            throw std::runtime_error("row 3 [1,0] mismatch");
        if (arr(casacore::IPosition(2, 0, 1)) != 21.0f)
            throw std::runtime_error("row 3 [0,1] mismatch");
        if (arr(casacore::IPosition(2, 1, 1)) != 22.0f)
            throw std::runtime_error("row 3 [1,1] mismatch");
        if (arr(casacore::IPosition(2, 0, 2)) != 23.0f)
            throw std::runtime_error("row 3 [0,2] mismatch");
        if (arr(casacore::IPosition(2, 1, 2)) != 24.0f)
            throw std::runtime_error("row 3 [1,2] mismatch");
    }
}

} // anonymous namespace

extern "C" {

int32_t cpp_table_write_aipsio_variable_array(const char* path, char** out_error) {
    try { write_aipsio_variable_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_aipsio_variable_array"); return -1; }
}

int32_t cpp_table_verify_aipsio_variable_array(const char* path, char** out_error) {
    try { verify_aipsio_variable_array_impl(path); return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_aipsio_variable_array"); return -1; }
}

} // extern "C"
