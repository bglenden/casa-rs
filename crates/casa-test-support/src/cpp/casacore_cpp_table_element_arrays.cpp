// Every-type array fixtures: ism_element_arrays, ssm_element_arrays and
// ism_indirect_arrays.
//
// Arrays of every element type, in 3 rows, in families of columns by how
// they are stored:
// - direct: fixed shapes [1], [3] and [2,3], with the Direct option, named
//   "<type>_<shape>" (Float64_2x3);
// - indirect: the same shapes without the Direct option, kept in the storage
//   manager's array file, named "<type>_<shape>_ind";
// - variable: variable-shape arrays of rank 1, kept in the array file, named
//   "<type>_var", whose cell in row r has r + 1 elements.
// Element i, in storage order, of row r is made from 7 + 10 r + i, so
// Float64_1 holds [7.0] in row 0. casa_test_support::table_element_arrays
// makes the same values.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/IncrementalStMan.h>
#include <casacore/tables/DataMan/StandardStMan.h>

#include <utility>
#include <vector>

using casacore_shim::make_error;

namespace {

const casacore::uInt kElementRows = 3;

template <class T> struct ElementTag { using type = T; };

// Call f(ElementTag<T>{}, name) for each element type, by the name its
// columns take.
template <class F> void forEachElementType(F&& f) {
    f(ElementTag<casacore::Bool>{}, "Bool");
    f(ElementTag<casacore::uChar>{}, "UInt8");
    f(ElementTag<casacore::Short>{}, "Int16");
    f(ElementTag<casacore::uShort>{}, "UInt16");
    f(ElementTag<casacore::Int>{}, "Int32");
    f(ElementTag<casacore::uInt>{}, "UInt32");
    f(ElementTag<casacore::Int64>{}, "Int64");
    f(ElementTag<casacore::Float>{}, "Float32");
    f(ElementTag<casacore::Double>{}, "Float64");
    f(ElementTag<casacore::Complex>{}, "Complex32");
    f(ElementTag<casacore::DComplex>{}, "Complex64");
    f(ElementTag<casacore::String>{}, "String");
}

std::vector<std::pair<std::string, casacore::IPosition>> elementShapes() {
    return {{"1", casacore::IPosition(1, 1)},
            {"3", casacore::IPosition(1, 3)},
            {"2x3", casacore::IPosition(2, 2, 3)}};
}

// A family of columns: the suffix of their names, their options, and
// whether they are variable-shape rather than of the fixed shapes.
struct ElementStorage {
    std::string suffix;
    int options;
    bool variable;
};

const ElementStorage kDirect{
    "", casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape, false};
const ElementStorage kIndirect{"_ind", casacore::ColumnDesc::FixedShape, false};
const ElementStorage kVariable{"var", 0, true};

// Call f(ElementTag<T>{}, name, shape, storage) for each column of each of
// `storages`; `shape` is empty for a variable-shape column.
template <class F>
void forEachElementColumn(const std::vector<ElementStorage>& storages, F&& f) {
    for (const auto& storage : storages) {
        forEachElementType([&](auto tag, const std::string& type) {
            if (storage.variable) {
                f(tag, type + "_" + storage.suffix, casacore::IPosition(), storage);
                return;
            }
            for (const auto& [name, shape] : elementShapes()) {
                f(tag, type + "_" + name + storage.suffix, shape, storage);
            }
        });
    }
}

// The shape of the cell in `row` of a column of `storage` and `shape`.
casacore::IPosition cellShape(const ElementStorage& storage, const casacore::IPosition& shape,
                              casacore::uInt row) {
    return storage.variable ? casacore::IPosition(1, row + 1) : shape;
}

casacore::Int64 elementBase(casacore::uInt row, size_t i) {
    return 7 + 10 * casacore::Int64(row) + casacore::Int64(i);
}

void elementValue(casacore::uInt row, size_t i, casacore::Bool& v) { v = (row + i) % 2 == 0; }
void elementValue(casacore::uInt row, size_t i, casacore::uChar& v) { v = casacore::uChar(elementBase(row, i)); }
void elementValue(casacore::uInt row, size_t i, casacore::Short& v) { v = casacore::Short(-elementBase(row, i)); }
void elementValue(casacore::uInt row, size_t i, casacore::uShort& v) { v = casacore::uShort(65535 - elementBase(row, i)); }
void elementValue(casacore::uInt row, size_t i, casacore::Int& v) { v = casacore::Int(-1000 * elementBase(row, i)); }
void elementValue(casacore::uInt row, size_t i, casacore::uInt& v) { v = casacore::uInt(4294967295LL - elementBase(row, i)); }
void elementValue(casacore::uInt row, size_t i, casacore::Int64& v) { v = -(elementBase(row, i) << 40); }
void elementValue(casacore::uInt row, size_t i, casacore::Float& v) { v = casacore::Float(elementBase(row, i)) + 0.25f; }
void elementValue(casacore::uInt row, size_t i, casacore::Double& v) { v = casacore::Double(elementBase(row, i)); }
void elementValue(casacore::uInt row, size_t i, casacore::Complex& v) {
    v = casacore::Complex(casacore::Float(elementBase(row, i)), -0.5f);
}
void elementValue(casacore::uInt row, size_t i, casacore::DComplex& v) {
    v = casacore::DComplex(casacore::Double(elementBase(row, i)), 0.125);
}
void elementValue(casacore::uInt row, size_t i, casacore::String& v) {
    v = "r" + std::to_string(row) + "e" + std::to_string(i);
}

template <class T>
casacore::Array<T> elementCell(casacore::uInt row, const casacore::IPosition& shape) {
    casacore::Array<T> cell(shape);
    T* data = cell.data();
    for (size_t i = 0; i < cell.nelements(); ++i) elementValue(row, i, data[i]);
    return cell;
}

void writeElementArrays(const char* path, casacore::DataManager& stman,
                        const std::vector<ElementStorage>& storages,
                        casacore::Table::EndianFormat endian = casacore::Table::AipsrcEndian) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    forEachElementColumn(storages, [&](auto tag, const std::string& name,
                                       const casacore::IPosition& shape,
                                       const ElementStorage& storage) {
        using T = typename decltype(tag)::type;
        if (storage.variable) {
            td.addColumn(casacore::ArrayColumnDesc<T>(name, 1, storage.options));
        } else {
            td.addColumn(casacore::ArrayColumnDesc<T>(name, shape, storage.options));
        }
    });
    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    setup.bindAll(stman);
    casacore::Table table(setup, kElementRows, casacore::False, endian);
    forEachElementColumn(storages, [&](auto tag, const std::string& name,
                                       const casacore::IPosition& shape,
                                       const ElementStorage& storage) {
        using T = typename decltype(tag)::type;
        casacore::ArrayColumn<T> column(table, name);
        for (casacore::uInt row = 0; row < kElementRows; ++row) {
            column.put(row, elementCell<T>(row, cellShape(storage, shape, row)));
        }
    });
    table.flush();
}

void verifyElementArrays(const char* path, const std::vector<ElementStorage>& storages) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != kElementRows)
        throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));
    forEachElementColumn(storages, [&](auto tag, const std::string& name,
                                       const casacore::IPosition& shape,
                                       const ElementStorage& storage) {
        using T = typename decltype(tag)::type;
        const int options = table.tableDesc().columnDesc(name).options();
        if ((options & casacore::ColumnDesc::Direct) !=
            (storage.options & casacore::ColumnDesc::Direct))
            throw std::runtime_error(name + ": Direct option differs");
        casacore::ArrayColumn<T> column(table, name);
        for (casacore::uInt row = 0; row < kElementRows; ++row) {
            const std::string cell = name + " row " + std::to_string(row);
            casacore::Array<T> found = column(row);
            const casacore::IPosition expectedShape = cellShape(storage, shape, row);
            if (!found.shape().isEqual(expectedShape))
                throw std::runtime_error(cell + ": shape " + found.shape().toString());
            casacore::Array<T> expected = elementCell<T>(row, expectedShape);
            casacore::Bool deleteFound;
            const T* values = found.getStorage(deleteFound);
            const T* expectedValues = expected.data();
            bool same = true;
            for (size_t i = 0; i < expected.nelements(); ++i) {
                if (!(values[i] == expectedValues[i])) same = false;
            }
            found.freeStorage(values, deleteFound);
            if (!same) throw std::runtime_error(cell + " mismatch");
        }
    });
}

// casa-rs writes IncrementalStMan arrays only directly, so the fixture it
// writes has only Direct columns; it reads the arrays casacore keeps in the
// array file (ISMIndColumn), which ism_indirect_arrays holds.
const std::vector<ElementStorage> kIsmStorages{kDirect};
const std::vector<ElementStorage> kIsmIndirectStorages{kIndirect, kVariable};
const std::vector<ElementStorage> kSsmStorages{kDirect, kIndirect, kVariable};

// Run `body`, reporting an exception as `out_error`.
template <class F> int32_t guarded(const char* what, char** out_error, F&& body) {
    try {
        body();
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error(std::string("unknown exception in ") + what);
        return -1;
    }
}

} // namespace

extern "C" {

int32_t cpp_table_write_ism_element_arrays(const char* path, char** out_error) {
    return guarded("write_ism_element_arrays", out_error, [&] {
        casacore::IncrementalStMan stman;
        writeElementArrays(path, stman, kIsmStorages);
    });
}

int32_t cpp_table_verify_ism_element_arrays(const char* path, char** out_error) {
    return guarded("verify_ism_element_arrays", out_error,
                   [&] { verifyElementArrays(path, kIsmStorages); });
}

int32_t cpp_table_write_ssm_element_arrays(const char* path, char** out_error) {
    return guarded("write_ssm_element_arrays", out_error, [&] {
        casacore::StandardStMan stman;
        writeElementArrays(path, stman, kSsmStorages);
    });
}

int32_t cpp_table_verify_ssm_element_arrays(const char* path, char** out_error) {
    return guarded("verify_ssm_element_arrays", out_error,
                   [&] { verifyElementArrays(path, kSsmStorages); });
}

// The ism_indirect_arrays tables are checked in to casa-tables'
// tests/data/ism_indirect_arrays, so their buckets are small: 4 KiB rather
// than the default 32 KiB, which holds the fixture's 3 rows but not the 10
// casacore checks for by default.
const casacore::uInt kIsmIndirectBucketBytes = 4096;

int32_t cpp_table_write_ism_indirect_arrays(const char* path, char** out_error) {
    return guarded("write_ism_indirect_arrays", out_error, [&] {
        casacore::IncrementalStMan stman("ISM", kIsmIndirectBucketBytes, casacore::False);
        writeElementArrays(path, stman, kIsmIndirectStorages, casacore::Table::LittleEndian);
    });
}

int32_t cpp_table_write_ism_indirect_arrays_big_endian(const char* path, char** out_error) {
    return guarded("write_ism_indirect_arrays_big_endian", out_error, [&] {
        casacore::IncrementalStMan stman("ISM", kIsmIndirectBucketBytes, casacore::False);
        writeElementArrays(path, stman, kIsmIndirectStorages, casacore::Table::BigEndian);
    });
}

int32_t cpp_table_verify_ism_indirect_arrays(const char* path, char** out_error) {
    return guarded("verify_ism_indirect_arrays", out_error,
                   [&] { verifyElementArrays(path, kIsmIndirectStorages); });
}

} // extern "C"
