// SPDX-License-Identifier: LGPL-3.0-or-later
// Tiled storage manager fixtures: TiledColumnStMan, TiledShapeStMan, TiledCellStMan.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/TiledColumnStMan.h>
#include <casacore/tables/DataMan/TiledShapeStMan.h>
#include <casacore/tables/DataMan/TiledCellStMan.h>

using casacore_shim::make_error;

namespace {

// ===== TiledColumnStMan fixture =====
// Schema: data Float32 [2,3], 3 rows, tile shape [2,3,2].
// Same cell values as ssm_fixed_array to allow easy comparison.

void write_tiled_column_stman_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    casacore::IPosition shape(2, 2, 3);
    td.addColumn(
        casacore::ArrayColumnDesc<casacore::Float>("data", shape,
                                                    casacore::ColumnDesc::Direct |
                                                    casacore::ColumnDesc::FixedShape));

    td.defineHypercolumn("TiledData", 3,
        casacore::Vector<casacore::String>(1, "data"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::IPosition tileShape(3, 2, 3, 2);
    casacore::TiledColumnStMan tsm("TiledData", tileShape);
    setup.bindAll(tsm);

    casacore::Table table(setup, 3);
    casacore::ArrayColumn<casacore::Float> colData(table, "data");

    // Row 0
    {
        casacore::Array<casacore::Float> arr(shape);
        arr(casacore::IPosition(2, 0, 0)) = 1.0f;
        arr(casacore::IPosition(2, 1, 0)) = 2.0f;
        arr(casacore::IPosition(2, 0, 1)) = 3.0f;
        arr(casacore::IPosition(2, 1, 1)) = 4.0f;
        arr(casacore::IPosition(2, 0, 2)) = 5.0f;
        arr(casacore::IPosition(2, 1, 2)) = 6.0f;
        colData.put(0, arr);
    }
    // Row 1
    {
        casacore::Array<casacore::Float> arr(shape);
        arr(casacore::IPosition(2, 0, 0)) = 7.0f;
        arr(casacore::IPosition(2, 1, 0)) = 8.0f;
        arr(casacore::IPosition(2, 0, 1)) = 9.0f;
        arr(casacore::IPosition(2, 1, 1)) = 10.0f;
        arr(casacore::IPosition(2, 0, 2)) = 11.0f;
        arr(casacore::IPosition(2, 1, 2)) = 12.0f;
        colData.put(1, arr);
    }
    // Row 2 (all zeros)
    {
        casacore::Array<casacore::Float> arr(shape, 0.0f);
        colData.put(2, arr);
    }

    table.flush();
}

void verify_tiled_column_stman_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);

    if (table.nrow() != 3)
        throw std::runtime_error("expected 3 rows");

    casacore::ArrayColumn<casacore::Float> colData(table, "data");
    casacore::IPosition expectedShape(2, 2, 3);

    // Row 0
    {
        casacore::Array<casacore::Float> arr = colData(0);
        if (!arr.shape().isEqual(expectedShape))
            throw std::runtime_error("row 0 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 1.0f)
            throw std::runtime_error("row 0 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 0)) != 2.0f)
            throw std::runtime_error("row 0 [1,0] mismatch");
        if (arr(casacore::IPosition(2, 0, 2)) != 5.0f)
            throw std::runtime_error("row 0 [0,2] mismatch");
        if (arr(casacore::IPosition(2, 1, 2)) != 6.0f)
            throw std::runtime_error("row 0 [1,2] mismatch");
    }
    // Row 1
    {
        casacore::Array<casacore::Float> arr = colData(1);
        if (arr(casacore::IPosition(2, 0, 0)) != 7.0f)
            throw std::runtime_error("row 1 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 1)) != 10.0f)
            throw std::runtime_error("row 1 [1,1] mismatch");
    }
    // Row 2 (all zeros)
    {
        casacore::Array<casacore::Float> arr = colData(2);
        if (arr(casacore::IPosition(2, 0, 0)) != 0.0f)
            throw std::runtime_error("row 2 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 2)) != 0.0f)
            throw std::runtime_error("row 2 [1,2] mismatch");
    }
}

// ===== TiledShapeStMan fixture =====
// Schema: data Float32 variable shape, 4 rows.
// Rows 0,3: shape [2,3], Rows 1,2: shape [3,2].
// Tests multi-cube row mapping.

void write_tiled_shape_stman_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ArrayColumnDesc<casacore::Float>("data", 2));

    td.defineHypercolumn("TiledData", 3,
        casacore::Vector<casacore::String>(1, "data"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::TiledShapeStMan tsm("TiledData", casacore::IPosition(3, 2, 3, 2));
    setup.bindAll(tsm);

    casacore::Table table(setup, 4);
    casacore::ArrayColumn<casacore::Float> colData(table, "data");

    // Row 0: shape [2,3], values 1..6
    {
        casacore::IPosition s(2, 2, 3);
        casacore::Array<casacore::Float> arr(s);
        for (int j = 0; j < 3; j++)
            for (int i = 0; i < 2; i++)
                arr(casacore::IPosition(2, i, j)) = static_cast<float>(i + j * 2 + 1);
        colData.put(0, arr);
    }
    // Row 1: shape [3,2], values 10..15
    {
        casacore::IPosition s(2, 3, 2);
        casacore::Array<casacore::Float> arr(s);
        for (int j = 0; j < 2; j++)
            for (int i = 0; i < 3; i++)
                arr(casacore::IPosition(2, i, j)) = static_cast<float>(i + j * 3 + 10);
        colData.put(1, arr);
    }
    // Row 2: shape [3,2], values 20..25
    {
        casacore::IPosition s(2, 3, 2);
        casacore::Array<casacore::Float> arr(s);
        for (int j = 0; j < 2; j++)
            for (int i = 0; i < 3; i++)
                arr(casacore::IPosition(2, i, j)) = static_cast<float>(i + j * 3 + 20);
        colData.put(2, arr);
    }
    // Row 3: shape [2,3], values 30..35
    {
        casacore::IPosition s(2, 2, 3);
        casacore::Array<casacore::Float> arr(s);
        for (int j = 0; j < 3; j++)
            for (int i = 0; i < 2; i++)
                arr(casacore::IPosition(2, i, j)) = static_cast<float>(i + j * 2 + 30);
        colData.put(3, arr);
    }

    table.flush();
}

void verify_tiled_shape_stman_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);

    if (table.nrow() != 4)
        throw std::runtime_error("expected 4 rows");

    casacore::ArrayColumn<casacore::Float> colData(table, "data");

    // Row 0: [2,3]
    {
        casacore::Array<casacore::Float> arr = colData(0);
        if (!arr.shape().isEqual(casacore::IPosition(2, 2, 3)))
            throw std::runtime_error("row 0 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 1.0f)
            throw std::runtime_error("row 0 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 2)) != 6.0f)
            throw std::runtime_error("row 0 [1,2] mismatch");
    }
    // Row 1: [3,2]
    {
        casacore::Array<casacore::Float> arr = colData(1);
        if (!arr.shape().isEqual(casacore::IPosition(2, 3, 2)))
            throw std::runtime_error("row 1 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 10.0f)
            throw std::runtime_error("row 1 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 2, 1)) != 15.0f)
            throw std::runtime_error("row 1 [2,1] mismatch");
    }
    // Row 2: [3,2]
    {
        casacore::Array<casacore::Float> arr = colData(2);
        if (!arr.shape().isEqual(casacore::IPosition(2, 3, 2)))
            throw std::runtime_error("row 2 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 20.0f)
            throw std::runtime_error("row 2 [0,0] mismatch");
    }
    // Row 3: [2,3]
    {
        casacore::Array<casacore::Float> arr = colData(3);
        if (!arr.shape().isEqual(casacore::IPosition(2, 2, 3)))
            throw std::runtime_error("row 3 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 30.0f)
            throw std::runtime_error("row 3 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 2)) != 35.0f)
            throw std::runtime_error("row 3 [1,2] mismatch");
    }
}

// ===== TiledCellStMan fixture =====
// Schema: data Float32 variable shape, 3 rows.
// Row 0: [2,3], Row 1: [4,2], Row 2: [3,3]. One cube per row.

void write_tiled_cell_stman_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ArrayColumnDesc<casacore::Float>("data", 2));

    td.defineHypercolumn("TiledData", 2,
        casacore::Vector<casacore::String>(1, "data"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::TiledCellStMan tsm("TiledData", casacore::IPosition(2, 4, 4));
    setup.bindAll(tsm);

    casacore::Table table(setup, 3);
    casacore::ArrayColumn<casacore::Float> colData(table, "data");

    // Row 0: shape [2,3], values 1..6
    {
        casacore::IPosition s(2, 2, 3);
        casacore::Array<casacore::Float> arr(s);
        for (int j = 0; j < 3; j++)
            for (int i = 0; i < 2; i++)
                arr(casacore::IPosition(2, i, j)) = static_cast<float>(i + j * 2 + 1);
        colData.setShape(0, s);
        colData.put(0, arr);
    }
    // Row 1: shape [4,2], values 10..17
    {
        casacore::IPosition s(2, 4, 2);
        casacore::Array<casacore::Float> arr(s);
        for (int j = 0; j < 2; j++)
            for (int i = 0; i < 4; i++)
                arr(casacore::IPosition(2, i, j)) = static_cast<float>(i + j * 4 + 10);
        colData.setShape(1, s);
        colData.put(1, arr);
    }
    // Row 2: shape [3,3], values 20..28
    {
        casacore::IPosition s(2, 3, 3);
        casacore::Array<casacore::Float> arr(s);
        for (int j = 0; j < 3; j++)
            for (int i = 0; i < 3; i++)
                arr(casacore::IPosition(2, i, j)) = static_cast<float>(i + j * 3 + 20);
        colData.setShape(2, s);
        colData.put(2, arr);
    }

    table.flush();
}

void verify_tiled_cell_stman_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);

    if (table.nrow() != 3)
        throw std::runtime_error("expected 3 rows");

    casacore::ArrayColumn<casacore::Float> colData(table, "data");

    // Row 0: [2,3]
    {
        casacore::Array<casacore::Float> arr = colData(0);
        if (!arr.shape().isEqual(casacore::IPosition(2, 2, 3)))
            throw std::runtime_error("row 0 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 1.0f)
            throw std::runtime_error("row 0 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 1, 2)) != 6.0f)
            throw std::runtime_error("row 0 [1,2] mismatch");
    }
    // Row 1: [4,2]
    {
        casacore::Array<casacore::Float> arr = colData(1);
        if (!arr.shape().isEqual(casacore::IPosition(2, 4, 2)))
            throw std::runtime_error("row 1 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 10.0f)
            throw std::runtime_error("row 1 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 3, 1)) != 17.0f)
            throw std::runtime_error("row 1 [3,1] mismatch");
    }
    // Row 2: [3,3]
    {
        casacore::Array<casacore::Float> arr = colData(2);
        if (!arr.shape().isEqual(casacore::IPosition(2, 3, 3)))
            throw std::runtime_error("row 2 shape mismatch");
        if (arr(casacore::IPosition(2, 0, 0)) != 20.0f)
            throw std::runtime_error("row 2 [0,0] mismatch");
        if (arr(casacore::IPosition(2, 2, 2)) != 28.0f)
            throw std::runtime_error("row 2 [2,2] mismatch");
    }
}

} // anonymous namespace

extern "C" {

int32_t cpp_table_write_tiled_column_stman(const char* path, char** out_error) {
    try {
        write_tiled_column_stman_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in write_tiled_column_stman");
        return -1;
    }
}

int32_t cpp_table_verify_tiled_column_stman(const char* path, char** out_error) {
    try {
        verify_tiled_column_stman_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in verify_tiled_column_stman");
        return -1;
    }
}

int32_t cpp_table_write_tiled_shape_stman(const char* path, char** out_error) {
    try {
        write_tiled_shape_stman_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in write_tiled_shape_stman");
        return -1;
    }
}

int32_t cpp_table_verify_tiled_shape_stman(const char* path, char** out_error) {
    try {
        verify_tiled_shape_stman_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in verify_tiled_shape_stman");
        return -1;
    }
}

int32_t cpp_table_write_tiled_cell_stman(const char* path, char** out_error) {
    try {
        write_tiled_cell_stman_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in write_tiled_cell_stman");
        return -1;
    }
}

int32_t cpp_table_verify_tiled_cell_stman(const char* path, char** out_error) {
    try {
        verify_tiled_cell_stman_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in verify_tiled_cell_stman");
        return -1;
    }
}

} // extern "C"
