// Every-type array fixtures: ism_element_arrays and ssm_element_arrays.
//
// Fixed-shape arrays of every element type, of shapes [1], [3] and [2,3], in
// 3 rows. A column is named "<type>_<shape>" (Float64_2x3) when it is stored
// directly (the Direct option) and "<type>_<shape>_ind" when it is stored
// indirectly, in the storage manager's array file. Element i, in storage
// order, of row r is made from 7 + 10 r + i, so Float64_1 holds [7.0] in
// row 0. casa_test_support::table_element_arrays makes the same values.
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

// The columns of one storage: the suffix of their names and their options.
struct ElementStorage {
    std::string suffix;
    int options;
};

const ElementStorage kDirect{"", casacore::ColumnDesc::Direct | casacore::ColumnDesc::FixedShape};
const ElementStorage kIndirect{"_ind", casacore::ColumnDesc::FixedShape};

// Call f(ElementTag<T>{}, name, shape, options) for each column of each of
// `storages`.
template <class F>
void forEachElementColumn(const std::vector<ElementStorage>& storages, F&& f) {
    for (const auto& storage : storages) {
        forEachElementType([&](auto tag, const std::string& type) {
            for (const auto& [name, shape] : elementShapes()) {
                f(tag, type + "_" + name + storage.suffix, shape, storage.options);
            }
        });
    }
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
                        const std::vector<ElementStorage>& storages) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    forEachElementColumn(storages, [&](auto tag, const std::string& name,
                                       const casacore::IPosition& shape, int options) {
        using T = typename decltype(tag)::type;
        td.addColumn(casacore::ArrayColumnDesc<T>(name, shape, options));
    });
    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    setup.bindAll(stman);
    casacore::Table table(setup, kElementRows);
    forEachElementColumn(storages, [&](auto tag, const std::string& name,
                                       const casacore::IPosition& shape, int) {
        using T = typename decltype(tag)::type;
        casacore::ArrayColumn<T> column(table, name);
        for (casacore::uInt row = 0; row < kElementRows; ++row) {
            column.put(row, elementCell<T>(row, shape));
        }
    });
    table.flush();
}

void verifyElementArrays(const char* path, const std::vector<ElementStorage>& storages) {
    casacore::Table table(path, casacore::Table::Old);
    if (table.nrow() != kElementRows)
        throw std::runtime_error("expected 3 rows, got " + std::to_string(table.nrow()));
    forEachElementColumn(storages, [&](auto tag, const std::string& name,
                                       const casacore::IPosition& shape, int options) {
        using T = typename decltype(tag)::type;
        if ((table.tableDesc().columnDesc(name).options() & casacore::ColumnDesc::Direct) !=
            (options & casacore::ColumnDesc::Direct))
            throw std::runtime_error(name + ": Direct option differs");
        casacore::ArrayColumn<T> column(table, name);
        for (casacore::uInt row = 0; row < kElementRows; ++row) {
            const std::string cell = name + " row " + std::to_string(row);
            casacore::Array<T> found = column(row);
            if (!found.shape().isEqual(shape))
                throw std::runtime_error(cell + ": shape " + found.shape().toString());
            casacore::Array<T> expected = elementCell<T>(row, shape);
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

// casa-rs writes IncrementalStMan arrays only directly, so its fixture has
// only Direct columns.
const std::vector<ElementStorage> kIsmStorages{kDirect};
const std::vector<ElementStorage> kSsmStorages{kDirect, kIndirect};

} // namespace

extern "C" {

int32_t cpp_table_write_ism_element_arrays(const char* path, char** out_error) {
    try {
        casacore::IncrementalStMan stman;
        writeElementArrays(path, stman, kIsmStorages);
        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_ism_element_arrays"); return -1; }
}

int32_t cpp_table_verify_ism_element_arrays(const char* path, char** out_error) {
    try {
        verifyElementArrays(path, kIsmStorages);
        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_ism_element_arrays"); return -1; }
}

int32_t cpp_table_write_ssm_element_arrays(const char* path, char** out_error) {
    try {
        casacore::StandardStMan stman;
        writeElementArrays(path, stman, kSsmStorages);
        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in write_ssm_element_arrays"); return -1; }
}

int32_t cpp_table_verify_ssm_element_arrays(const char* path, char** out_error) {
    try {
        verifyElementArrays(path, kSsmStorages);
        return 0;
    } catch (const std::exception& e) { *out_error = make_error(e.what()); return -1;
    } catch (...) { *out_error = make_error("unknown exception in verify_ssm_element_arrays"); return -1; }
}

} // extern "C"
