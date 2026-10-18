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

// ===== Multi: several data columns of equal pixel size (#756 review) =====
// Description order a (Float), b (Float), c (Complex), d (Int), all 2-D, and
// the id column id (Int). Hypercolumn "TiledMulti" defines data [b, a, d, c]
// and id [id]; casacore's tile is c, d, a, b. Hypercube id 10 is [2,3,2]
// (rows 0-1) and its values record also holds a stray field a = 77, which
// casacore keeps and ignores; hypercube id 20 is [3,2,2] (rows 2-3).
// At column-major index i of row r: a = 1000 + 100r + i, b = 2000 + 100r + i,
// c = (3000 + 100r + i, -(i + 1)), d = 4000 + 100r + i. The patched form
// negates a in rows 1 and 3 and changes nothing else.

constexpr casacore::uInt kMultiRows = 4;

casacore::IPosition multi_shape(casacore::uInt row) {
    return row < 2 ? casacore::IPosition(2, 2, 3) : casacore::IPosition(2, 3, 2);
}

casacore::Float multi_a(casacore::uInt row, casacore::uInt i, bool patched) {
    casacore::Float value = 1000.0f + 100.0f * row + i;
    return patched && (row == 1 || row == 3) ? -value : value;
}
casacore::Float multi_b(casacore::uInt row, casacore::uInt i) { return 2000.0f + 100.0f * row + i; }
casacore::Complex multi_c(casacore::uInt row, casacore::uInt i) {
    return casacore::Complex(3000.0f + 100.0f * row + i, -1.0f * (i + 1));
}
casacore::Int multi_d(casacore::uInt row, casacore::uInt i) {
    return casacore::Int(4000 + 100 * row + i);
}

template <typename T, typename F>
void put_multi(casacore::Table& table, const char* column, F value) {
    casacore::ArrayColumn<T> col(table, column);
    for (casacore::uInt row = 0; row < kMultiRows; row++) {
        casacore::Array<T> cell(multi_shape(row));
        T* values = cell.data();
        for (casacore::uInt i = 0; i < cell.nelements(); i++) {
            values[i] = value(row, i);
        }
        col.put(row, cell);
    }
}

template <typename T, typename F>
void check_multi(const casacore::Table& table, const char* column, F expected) {
    casacore::ArrayColumn<T> col(table, column);
    for (casacore::uInt row = 0; row < kMultiRows; row++) {
        casacore::Array<T> cell = col(row);
        const std::string at = std::string(column) + " row " + std::to_string(row);
        if (!cell.shape().isEqual(multi_shape(row))) {
            throw std::runtime_error(at + ": shape mismatch");
        }
        casacore::Bool deleteIt;
        const T* values = cell.getStorage(deleteIt);
        bool equal = true;
        for (casacore::uInt i = 0; i < cell.nelements(); i++) {
            equal = equal && values[i] == expected(row, i);
        }
        cell.freeStorage(values, deleteIt);
        if (!equal) {
            throw std::runtime_error(at + ": values differ");
        }
    }
}

void write_tiled_data_multi_impl(const std::string& path, casacore::Table::EndianFormat endian) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ArrayColumnDesc<casacore::Float>("a", 2));
    td.addColumn(casacore::ArrayColumnDesc<casacore::Float>("b", 2));
    td.addColumn(casacore::ArrayColumnDesc<casacore::Complex>("c", 2));
    td.addColumn(casacore::ArrayColumnDesc<casacore::Int>("d", 2));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));
    casacore::Vector<casacore::String> data(4);
    data(0) = "b";
    data(1) = "a";
    data(2) = "d";
    data(3) = "c";
    td.defineHypercolumn("TiledMulti", 3, data, casacore::Vector<casacore::String>(),
                         casacore::Vector<casacore::String>(1, "id"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::TiledDataStMan tsm("TiledMulti");
    setup.bindAll(tsm);
    casacore::Table table(setup, casacore::Table::Plain, kMultiRows, casacore::False, endian);

    casacore::TiledDataStManAccessor accessor(table, "TiledMulti");
    casacore::Record first;
    first.define("id", casacore::Int(10));
    first.define("a", casacore::Float(77));
    accessor.addHypercube(casacore::IPosition(3, 2, 3, 2), casacore::IPosition(3, 2, 3, 1),
                          first);
    casacore::Record second;
    second.define("id", casacore::Int(20));
    accessor.addHypercube(casacore::IPosition(3, 3, 2, 2), casacore::IPosition(3, 3, 2, 2),
                          second);

    put_multi<casacore::Float>(table, "a", [](casacore::uInt r, casacore::uInt i) {
        return multi_a(r, i, false);
    });
    put_multi<casacore::Float>(table, "b", multi_b);
    put_multi<casacore::Complex>(table, "c", multi_c);
    put_multi<casacore::Int>(table, "d", multi_d);
    table.flush();
}

void verify_tiled_data_multi_impl(const std::string& path, bool patched) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != kMultiRows) {
        throw std::runtime_error("expected 4 rows, found " + std::to_string(table.nrow()));
    }
    check_multi<casacore::Float>(table, "a", [patched](casacore::uInt r, casacore::uInt i) {
        return multi_a(r, i, patched);
    });
    check_multi<casacore::Float>(table, "b", multi_b);
    check_multi<casacore::Complex>(table, "c", multi_c);
    check_multi<casacore::Int>(table, "d", multi_d);
    casacore::ScalarColumn<casacore::Int> id(table, "id");
    for (casacore::uInt row = 0; row < kMultiRows; row++) {
        if (id(row) != (row < 2 ? 10 : 20)) {
            throw std::runtime_error("id row " + std::to_string(row));
        }
    }
}

// ===== Scalar: a scalar data column over two row axes (#756 review) =====
// Scalar Float s in hypercolumn "TiledScalar" of 2 axes: cube [2,2], tile
// [2,1], rows 0-3 holding 10, 11, 12, 13. casacore reads the four scalars;
// casa-rs refuses the column.

void write_tiled_data_scalar_impl(const std::string& path) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Float>("s"));
    td.defineHypercolumn("TiledScalar", 2, casacore::Vector<casacore::String>(1, "s"));
    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::TiledDataStMan tsm("TiledScalar");
    setup.bindAll(tsm);
    casacore::Table table(setup, 4);
    casacore::TiledDataStManAccessor accessor(table, "TiledScalar");
    accessor.addHypercube(casacore::IPosition(2, 2, 2), casacore::IPosition(2, 2, 1),
                          casacore::Record());
    casacore::ScalarColumn<casacore::Float> s(table, "s");
    for (casacore::uInt row = 0; row < 4; row++) {
        s.put(row, 10.0f + row);
    }
    table.flush();
}

void verify_tiled_data_scalar_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    casacore::ScalarColumn<casacore::Float> s(table, "s");
    for (casacore::uInt row = 0; row < 4; row++) {
        if (s(row) != 10.0f + row) {
            throw std::runtime_error("s row " + std::to_string(row));
        }
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

int32_t cpp_table_write_tiled_data_multi(const char* path, char** out_error) {
    return guarded(
        "write_tiled_data_multi", out_error,
        [](const std::string& p) { write_tiled_data_multi_impl(p, casacore::Table::LittleEndian); },
        path);
}

int32_t cpp_table_write_tiled_data_multi_big_endian(const char* path, char** out_error) {
    return guarded(
        "write_tiled_data_multi_big_endian", out_error,
        [](const std::string& p) { write_tiled_data_multi_impl(p, casacore::Table::BigEndian); },
        path);
}

int32_t cpp_table_verify_tiled_data_multi(const char* path, char** out_error) {
    return guarded(
        "verify_tiled_data_multi", out_error,
        [](const std::string& p) { verify_tiled_data_multi_impl(p, false); }, path);
}

int32_t cpp_table_verify_tiled_data_multi_patched(const char* path, char** out_error) {
    return guarded(
        "verify_tiled_data_multi_patched", out_error,
        [](const std::string& p) { verify_tiled_data_multi_impl(p, true); }, path);
}

int32_t cpp_table_write_tiled_data_scalar(const char* path, char** out_error) {
    return guarded("write_tiled_data_scalar", out_error, write_tiled_data_scalar_impl, path);
}

int32_t cpp_table_verify_tiled_data_scalar(const char* path, char** out_error) {
    return guarded("verify_tiled_data_scalar", out_error, verify_tiled_data_scalar_impl, path);
}

}  // extern "C"
