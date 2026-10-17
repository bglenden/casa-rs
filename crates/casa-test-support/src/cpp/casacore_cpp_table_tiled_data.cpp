// SPDX-License-Identifier: LGPL-3.0-or-later
// TiledDataStMan fixture: user-assigned hypercubes with an id column.
//
// Schema: data Float32 2-D (shape set per hypercube), id Int32 (the
// hypercolumn's id column). Hypercolumn "TiledData", ndim 3.
//   hypercube 0: id 10, cube [2,3,2], tile [2,3,1] -> rows 0..1, cells [2,3]
//   hypercube 1: id 20, cube [3,2,3], tile [3,2,2] -> rows 2..4, cells [3,2]
// Cell values: data(row)[i] = 100*row + i over the cell's column-major index.
// The patched form negates rows 1 and 3, as casa-rs's in-place patch does.
// The table is written little-endian, or big-endian by the _big_endian writer.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/TiledDataStMan.h>
#include <casacore/tables/DataMan/TiledDataStManAccessor.h>

using casacore_shim::make_error;

namespace {

constexpr casacore::uInt kRows = 5;

casacore::IPosition cell_shape(casacore::uInt row) {
    return row < 2 ? casacore::IPosition(2, 2, 3) : casacore::IPosition(2, 3, 2);
}

casacore::Int expected_id(casacore::uInt row) { return row < 2 ? 10 : 20; }

bool is_patched(casacore::uInt row) { return row == 1 || row == 3; }

casacore::Float expected_value(casacore::uInt row, casacore::uInt index, bool patched) {
    casacore::Float value = 100.0f * row + index;
    return patched && is_patched(row) ? -value : value;
}

void write_tiled_data_stman_impl(const std::string& path, casacore::Table::EndianFormat endian) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ArrayColumnDesc<casacore::Float>("data", 2));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));
    td.defineHypercolumn("TiledData", 3, casacore::Vector<casacore::String>(1, "data"),
                         casacore::Vector<casacore::String>(),
                         casacore::Vector<casacore::String>(1, "id"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::TiledDataStMan tsm("TiledData");
    setup.bindAll(tsm);
    casacore::Table table(setup, casacore::Table::Plain, kRows, casacore::False, endian);

    casacore::TiledDataStManAccessor accessor(table, "TiledData");
    casacore::Record first;
    first.define("id", expected_id(0));
    accessor.addHypercube(casacore::IPosition(3, 2, 3, 2), casacore::IPosition(3, 2, 3, 1),
                          first);
    casacore::Record second;
    second.define("id", expected_id(2));
    accessor.addHypercube(casacore::IPosition(3, 3, 2, 3), casacore::IPosition(3, 3, 2, 2),
                          second);

    casacore::ArrayColumn<casacore::Float> data(table, "data");
    for (casacore::uInt row = 0; row < kRows; row++) {
        casacore::Array<casacore::Float> cell(cell_shape(row));
        casacore::Float* values = cell.data();
        for (casacore::uInt i = 0; i < cell.nelements(); i++) {
            values[i] = expected_value(row, i, false);
        }
        data.put(row, cell);
    }
    table.flush();
}

void verify_tiled_data_stman_impl(const std::string& path, bool patched) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != kRows) {
        throw std::runtime_error("expected 5 rows, found " + std::to_string(table.nrow()));
    }
    casacore::ArrayColumn<casacore::Float> data(table, "data");
    casacore::ScalarColumn<casacore::Int> id(table, "id");
    for (casacore::uInt row = 0; row < kRows; row++) {
        const std::string at = "row " + std::to_string(row);
        if (id(row) != expected_id(row)) {
            throw std::runtime_error(at + ": id " + std::to_string(id(row)));
        }
        casacore::Array<casacore::Float> cell = data(row);
        if (!cell.shape().isEqual(cell_shape(row))) {
            throw std::runtime_error(at + ": shape mismatch");
        }
        casacore::Bool deleteIt;
        const casacore::Float* values = cell.getStorage(deleteIt);
        for (casacore::uInt i = 0; i < cell.nelements(); i++) {
            if (values[i] != expected_value(row, i, patched)) {
                std::string message = at + "[" + std::to_string(i) + "]: " +
                                      std::to_string(values[i]);
                cell.freeStorage(values, deleteIt);
                throw std::runtime_error(message);
            }
        }
        cell.freeStorage(values, deleteIt);
    }
}

int32_t guarded(const char* operation, char** out_error, void (*body)(const std::string&),
                const char* path) {
    try {
        body(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error(std::string("unknown exception in ") + operation);
        return -1;
    }
}

}  // namespace

extern "C" {

int32_t cpp_table_write_tiled_data_stman(const char* path, char** out_error) {
    return guarded(
        "write_tiled_data_stman", out_error,
        [](const std::string& p) { write_tiled_data_stman_impl(p, casacore::Table::LittleEndian); },
        path);
}

int32_t cpp_table_write_tiled_data_stman_big_endian(const char* path, char** out_error) {
    return guarded(
        "write_tiled_data_stman_big_endian", out_error,
        [](const std::string& p) { write_tiled_data_stman_impl(p, casacore::Table::BigEndian); },
        path);
}

int32_t cpp_table_verify_tiled_data_stman(const char* path, char** out_error) {
    return guarded(
        "verify_tiled_data_stman", out_error,
        [](const std::string& p) { verify_tiled_data_stman_impl(p, false); }, path);
}

int32_t cpp_table_verify_tiled_data_stman_patched(const char* path, char** out_error) {
    return guarded(
        "verify_tiled_data_stman_patched", out_error,
        [](const std::string& p) { verify_tiled_data_stman_impl(p, true); }, path);
}

}  // extern "C"
