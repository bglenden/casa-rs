// SPDX-License-Identifier: LGPL-3.0-or-later
// Tile-order fixtures: hypercolumns whose data columns differ in pixel size.
//
// casacore lays a tile out over a hypercolumn's data columns by descending
// tile pixel size (Bool last), from the order of the hypercolumn definition
// or, without one, of the table description (TiledStMan::setup). Its sort
// is not stable: GenSortIndirect sorts ascending, ties by position, and
// reverses the result, so equal sizes come out in descending position.
//
// Shape mixed: TiledShapeStMan, hypercolumn "TiledMixed" defined with data
// columns [flag, sigma, data, weight]. Description order flag, data, weight,
// sigma (all 2-D, variable shape). Tile order: data, weight, sigma, flag;
// the two Float columns take descending definition position, not the
// description's order.
//   rows 0-1: [2,3] cells; rows 2-3: [3,2] cells (two hypercubes).
//   At column-major index i of row r:
//     data   = (10r + i, -i)       weight = 0.5(r + 1) + i
//     sigma  = 100 + 10r + i       flag   = (i + r) odd
//   The patched form negates data and inverts flag in rows 1 and 3, as
//   casa-rs's in-place patch does.
//
// Column no definition: TiledColumnStMan "TiledCol" with no hypercolumn
// definition. Description order count (Int), flag (Bool), value (Double),
// amp (Float), all fixed [2,3]. Tile order: value, amp, count, flag; Int and
// Float take descending description position.
//   3 rows. At index i of row r:
//     count = 1000r + i            flag  = i % 3 == r
//     value = 0.25 + r + 10i       amp   = -1.5(i + 1) - r
//
// Each table is written little-endian, or big-endian by the _big_endian
// writer.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/TiledColumnStMan.h>
#include <casacore/tables/DataMan/TiledShapeStMan.h>

#include <functional>

using casacore_shim::make_error;

namespace {

using casacore::Bool;
using casacore::Complex;
using casacore::Double;
using casacore::Float;
using casacore::Int;
using casacore::IPosition;
using casacore::uInt;

constexpr uInt kMixedRows = 4;
constexpr uInt kNoDefRows = 3;

IPosition mixed_shape(uInt row) { return row < 2 ? IPosition(2, 2, 3) : IPosition(2, 3, 2); }

bool mixed_patched(uInt row, bool patched) { return patched && (row == 1 || row == 3); }

Complex mixed_data(uInt row, uInt i, bool patched) {
    Complex value(10.0f * row + i, -1.0f * i);
    return mixed_patched(row, patched) ? -value : value;
}
Float mixed_weight(uInt row, uInt i) { return 0.5f * (row + 1) + i; }
Float mixed_sigma(uInt row, uInt i) { return 100.0f + 10.0f * row + i; }
Bool mixed_flag(uInt row, uInt i, bool patched) {
    bool flag = (i + row) % 2 == 1;
    return mixed_patched(row, patched) ? !flag : flag;
}

Int nodef_count(uInt row, uInt i) { return Int(1000 * row + i); }
Bool nodef_flag(uInt row, uInt i) { return i % 3 == row; }
Double nodef_value(uInt row, uInt i) { return 0.25 + row + 10.0 * i; }
Float nodef_amp(uInt row, uInt i) { return -1.5f * (i + 1) - row; }

template <typename T>
casacore::Array<T> make_cell(const IPosition& shape, const std::function<T(uInt)>& value) {
    casacore::Array<T> cell(shape);
    T* data = cell.data();
    for (uInt i = 0; i < cell.nelements(); i++) {
        data[i] = value(i);
    }
    return cell;
}

template <typename T>
void check_cell(const casacore::Table& table, const char* column, uInt row,
                const IPosition& shape, const std::function<T(uInt)>& expected) {
    casacore::ArrayColumn<T> col(table, column);
    casacore::Array<T> cell = col(row);
    const std::string at = std::string(column) + " row " + std::to_string(row);
    if (!cell.shape().isEqual(shape)) {
        throw std::runtime_error(at + ": shape mismatch");
    }
    casacore::Bool deleteIt;
    const T* values = cell.getStorage(deleteIt);
    for (uInt i = 0; i < cell.nelements(); i++) {
        if (!(values[i] == expected(i))) {
            cell.freeStorage(values, deleteIt);
            throw std::runtime_error(at + " [" + std::to_string(i) + "] differs");
        }
    }
    cell.freeStorage(values, deleteIt);
}

void write_shape_mixed_impl(const std::string& path, casacore::Table::EndianFormat endian) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ArrayColumnDesc<Bool>("flag", 2));
    td.addColumn(casacore::ArrayColumnDesc<Complex>("data", 2));
    td.addColumn(casacore::ArrayColumnDesc<Float>("weight", 2));
    td.addColumn(casacore::ArrayColumnDesc<Float>("sigma", 2));
    casacore::Vector<casacore::String> data(4);
    data(0) = "flag";
    data(1) = "sigma";
    data(2) = "data";
    data(3) = "weight";
    td.defineHypercolumn("TiledMixed", 3, data);

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::TiledShapeStMan tsm("TiledMixed", IPosition(3, 2, 3, 2));
    setup.bindAll(tsm);
    casacore::Table table(setup, casacore::Table::Plain, kMixedRows, casacore::False, endian);

    casacore::ArrayColumn<Bool> flag(table, "flag");
    casacore::ArrayColumn<Complex> cdata(table, "data");
    casacore::ArrayColumn<Float> weight(table, "weight");
    casacore::ArrayColumn<Float> sigma(table, "sigma");
    for (uInt row = 0; row < kMixedRows; row++) {
        const IPosition shape = mixed_shape(row);
        cdata.put(row, make_cell<Complex>(shape, [row](uInt i) { return mixed_data(row, i, false); }));
        weight.put(row, make_cell<Float>(shape, [row](uInt i) { return mixed_weight(row, i); }));
        sigma.put(row, make_cell<Float>(shape, [row](uInt i) { return mixed_sigma(row, i); }));
        flag.put(row, make_cell<Bool>(shape, [row](uInt i) { return mixed_flag(row, i, false); }));
    }
    table.flush();
}

void verify_shape_mixed_impl(const std::string& path, bool patched) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != kMixedRows) {
        throw std::runtime_error("expected 4 rows, found " + std::to_string(table.nrow()));
    }
    for (uInt row = 0; row < kMixedRows; row++) {
        const IPosition shape = mixed_shape(row);
        check_cell<Complex>(table, "data", row, shape,
                            [row, patched](uInt i) { return mixed_data(row, i, patched); });
        check_cell<Float>(table, "weight", row, shape, [row](uInt i) { return mixed_weight(row, i); });
        check_cell<Float>(table, "sigma", row, shape, [row](uInt i) { return mixed_sigma(row, i); });
        check_cell<Bool>(table, "flag", row, shape,
                         [row, patched](uInt i) { return mixed_flag(row, i, patched); });
    }
}

void write_column_nodef_impl(const std::string& path, casacore::Table::EndianFormat endian) {
    const IPosition shape(2, 2, 3);
    const int option = casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape;
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ArrayColumnDesc<Int>("count", shape, option));
    td.addColumn(casacore::ArrayColumnDesc<Bool>("flag", shape, option));
    td.addColumn(casacore::ArrayColumnDesc<Double>("value", shape, option));
    td.addColumn(casacore::ArrayColumnDesc<Float>("amp", shape, option));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::TiledColumnStMan tsm("TiledCol", IPosition(3, 2, 3, 2));
    setup.bindAll(tsm);
    casacore::Table table(setup, casacore::Table::Plain, kNoDefRows, casacore::False, endian);

    casacore::ArrayColumn<Int> count(table, "count");
    casacore::ArrayColumn<Bool> flag(table, "flag");
    casacore::ArrayColumn<Double> value(table, "value");
    casacore::ArrayColumn<Float> amp(table, "amp");
    for (uInt row = 0; row < kNoDefRows; row++) {
        count.put(row, make_cell<Int>(shape, [row](uInt i) { return nodef_count(row, i); }));
        flag.put(row, make_cell<Bool>(shape, [row](uInt i) { return nodef_flag(row, i); }));
        value.put(row, make_cell<Double>(shape, [row](uInt i) { return nodef_value(row, i); }));
        amp.put(row, make_cell<Float>(shape, [row](uInt i) { return nodef_amp(row, i); }));
    }
    table.flush();
}

void verify_column_nodef_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != kNoDefRows) {
        throw std::runtime_error("expected 3 rows, found " + std::to_string(table.nrow()));
    }
    const IPosition shape(2, 2, 3);
    for (uInt row = 0; row < kNoDefRows; row++) {
        check_cell<Int>(table, "count", row, shape, [row](uInt i) { return nodef_count(row, i); });
        check_cell<Bool>(table, "flag", row, shape, [row](uInt i) { return nodef_flag(row, i); });
        check_cell<Double>(table, "value", row, shape, [row](uInt i) { return nodef_value(row, i); });
        check_cell<Float>(table, "amp", row, shape, [row](uInt i) { return nodef_amp(row, i); });
    }
}

int32_t guarded(const char* operation, char** out_error,
                const std::function<void(const std::string&)>& body, const char* path) {
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

int32_t cpp_table_write_tiled_shape_mixed(const char* path, char** out_error) {
    return guarded("write_tiled_shape_mixed", out_error, [](const std::string& p) {
        write_shape_mixed_impl(p, casacore::Table::LittleEndian);
    }, path);
}

int32_t cpp_table_write_tiled_shape_mixed_big_endian(const char* path, char** out_error) {
    return guarded("write_tiled_shape_mixed_big_endian", out_error, [](const std::string& p) {
        write_shape_mixed_impl(p, casacore::Table::BigEndian);
    }, path);
}

int32_t cpp_table_verify_tiled_shape_mixed(const char* path, char** out_error) {
    return guarded("verify_tiled_shape_mixed", out_error,
                   [](const std::string& p) { verify_shape_mixed_impl(p, false); }, path);
}

int32_t cpp_table_verify_tiled_shape_mixed_patched(const char* path, char** out_error) {
    return guarded("verify_tiled_shape_mixed_patched", out_error,
                   [](const std::string& p) { verify_shape_mixed_impl(p, true); }, path);
}

int32_t cpp_table_write_tiled_column_nodef(const char* path, char** out_error) {
    return guarded("write_tiled_column_nodef", out_error, [](const std::string& p) {
        write_column_nodef_impl(p, casacore::Table::LittleEndian);
    }, path);
}

int32_t cpp_table_write_tiled_column_nodef_big_endian(const char* path, char** out_error) {
    return guarded("write_tiled_column_nodef_big_endian", out_error, [](const std::string& p) {
        write_column_nodef_impl(p, casacore::Table::BigEndian);
    }, path);
}

int32_t cpp_table_verify_tiled_column_nodef(const char* path, char** out_error) {
    return guarded("verify_tiled_column_nodef", out_error, verify_column_nodef_impl, path);
}

}  // extern "C"
