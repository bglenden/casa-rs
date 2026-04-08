// C++ TaQL interop shim: fixture writers and query executor.
// Provides extern "C" functions for Rust FFI to create test tables
// and execute TaQL queries via casacore's tableCommand().
#include "casacore_cpp_common.h"

#include <casacore/tables/TaQL/TableParse.h>
#include <casacore/tables/TaQL/TaQLResult.h>
#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/tables/Tables/TableRow.h>
#include <casacore/tables/DataMan/StManAipsIO.h>
#include <casacore/tables/DataMan/StandardStMan.h>
#include <casacore/casa/Arrays/ArrayMath.h>

#include <chrono>
#include <sstream>
#include <iomanip>
#include <vector>

static casacore_shim::TerminateGuard g_terminate_guard_taql;

namespace {

using namespace casacore;

// ── Simple fixture (50 rows) ──
// Schema: id(Int32), name(String), ra(Double), dec(Double), flux(Double),
//         category(String), flag(Bool), bigid(Int64), vis(DComplex)
// Must match Rust build_simple_fixture() exactly.
void write_simple_fixture_impl(const std::string& path) {
    TableDesc td("SimpleFixture", TableDesc::Scratch);
    td.addColumn(ScalarColumnDesc<Int>("id"));
    td.addColumn(ScalarColumnDesc<String>("name"));
    td.addColumn(ScalarColumnDesc<Double>("ra"));
    td.addColumn(ScalarColumnDesc<Double>("dec"));
    td.addColumn(ScalarColumnDesc<Double>("flux"));
    td.addColumn(ScalarColumnDesc<String>("category"));
    td.addColumn(ScalarColumnDesc<Bool>("flag"));
    td.addColumn(ScalarColumnDesc<Int64>("bigid"));
    td.addColumn(ScalarColumnDesc<DComplex>("vis"));

    SetupNewTable setup(path, td, Table::New);
    Table tab(setup, 50);

    ScalarColumn<Int> colId(tab, "id");
    ScalarColumn<String> colName(tab, "name");
    ScalarColumn<Double> colRa(tab, "ra");
    ScalarColumn<Double> colDec(tab, "dec");
    ScalarColumn<Double> colFlux(tab, "flux");
    ScalarColumn<String> colCat(tab, "category");
    ScalarColumn<Bool> colFlag(tab, "flag");
    ScalarColumn<Int64> colBigid(tab, "bigid");
    ScalarColumn<DComplex> colVis(tab, "vis");

    const char* categories[] = {"star", "galaxy", "pulsar", "quasar", "nebula"};

    for (uInt i = 0; i < 50; ++i) {
        colId.put(i, static_cast<Int>(i));
        std::ostringstream nm;
        nm << "SRC_" << std::setw(3) << std::setfill('0') << i;
        colName.put(i, String(nm.str()));
        colRa.put(i, static_cast<Double>(i) * 7.2);
        colDec.put(i, -45.0 + static_cast<Double>(i) * 1.8);
        colFlux.put(i, 0.1 + static_cast<Double>(i) * 0.5);
        colCat.put(i, String(categories[i % 5]));
        colFlag.put(i, (i % 3) != 0);  // false for multiples of 3
        colBigid.put(i, static_cast<Int64>(i) * 1000000LL);
        colVis.put(i, DComplex(static_cast<Double>(i) * 0.1,
                               static_cast<Double>(i) * -0.2));
    }
    tab.flush();
}

// ── Array fixture (10 rows) ──
// Schema: idata(Int32[2,3]), fdata(Double[3,2])
void write_array_fixture_impl(const std::string& path) {
    TableDesc td("ArrayFixture", TableDesc::Scratch);
    td.addColumn(ArrayColumnDesc<Int>("idata", IPosition(2, 2, 3),
                 ColumnDesc::Direct | ColumnDesc::FixedShape));
    td.addColumn(ArrayColumnDesc<Double>("fdata", IPosition(2, 3, 2),
                 ColumnDesc::Direct | ColumnDesc::FixedShape));

    SetupNewTable setup(path, td, Table::New);
    StandardStMan stman;
    setup.bindAll(stman);
    Table tab(setup, 10);

    ArrayColumn<Int> colI(tab, "idata");
    ArrayColumn<Double> colF(tab, "fdata");

    for (uInt row = 0; row < 10; ++row) {
        // idata: 2x3 array, values = row*6 + col_major_idx
        Array<Int> iarr(IPosition(2, 2, 3));
        for (Int j = 0; j < 6; ++j) {
            iarr.data()[j] = static_cast<Int>(row) * 6 + j;
        }
        colI.put(row, iarr);

        // fdata: 3x2 array, values = row*6.0 + col_major_idx + 0.5
        Array<Double> farr(IPosition(2, 3, 2));
        for (Int j = 0; j < 6; ++j) {
            farr.data()[j] = static_cast<Double>(row) * 6.0 + j + 0.5;
        }
        colF.put(row, farr);
    }
    tab.flush();
}

// ── Variable-shape array fixture (10 rows) ──
// Schema: vardata(Double[variable]), label(String)
// Row i gets shape [i+1] (1D array of increasing length).
void write_varshape_fixture_impl(const std::string& path) {
    TableDesc td("VarShapeFixture", TableDesc::Scratch);
    td.addColumn(ArrayColumnDesc<Double>("vardata"));
    td.addColumn(ScalarColumnDesc<String>("label"));

    SetupNewTable setup(path, td, Table::New);
    StManAipsIO stman;
    setup.bindAll(stman);
    Table tab(setup, 10);

    ArrayColumn<Double> colVar(tab, "vardata");
    ScalarColumn<String> colLabel(tab, "label");

    for (uInt row = 0; row < 10; ++row) {
        uInt len = row + 1;
        Array<Double> arr(IPosition(1, len));
        for (uInt j = 0; j < len; ++j) {
            arr.data()[j] = static_cast<Double>(row) * 10.0 + j + 0.5;
        }
        colVar.put(row, arr);
        std::ostringstream lbl;
        lbl << "R" << row;
        colLabel.put(row, String(lbl.str()));
    }
    tab.flush();
}

// ── Format a cell value to string matching Rust format_value() ──
std::string format_cell(const TableColumn& col, uInt row) {
    DataType dt = col.columnDesc().dataType();
    if (col.columnDesc().isArray()) {
        // Format arrays using Debug-like output
        std::ostringstream oss;
        switch (dt) {
            case TpInt: {
                Array<Int> arr;
                ArrayColumn<Int>(col).get(row, arr);
                oss << "[";
                Bool deleteIt;
                const Int* data = arr.getStorage(deleteIt);
                for (uInt i = 0; i < arr.nelements(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << data[i];
                }
                arr.freeStorage(data, deleteIt);
                oss << "]";
                break;
            }
            case TpDouble: {
                Array<Double> arr;
                ArrayColumn<Double>(col).get(row, arr);
                oss << "[";
                Bool deleteIt;
                const Double* data = arr.getStorage(deleteIt);
                for (uInt i = 0; i < arr.nelements(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << std::fixed << std::setprecision(6) << data[i];
                }
                arr.freeStorage(data, deleteIt);
                oss << "]";
                break;
            }
            case TpFloat: {
                Array<Float> arr;
                ArrayColumn<Float>(col).get(row, arr);
                oss << "[";
                Bool deleteIt;
                const Float* data = arr.getStorage(deleteIt);
                for (uInt i = 0; i < arr.nelements(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << std::fixed << std::setprecision(6) << data[i];
                }
                arr.freeStorage(data, deleteIt);
                oss << "]";
                break;
            }
            case TpInt64: {
                Array<Int64> arr;
                ArrayColumn<Int64>(col).get(row, arr);
                oss << "[";
                Bool deleteIt;
                const Int64* data = arr.getStorage(deleteIt);
                for (uInt i = 0; i < arr.nelements(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << data[i];
                }
                arr.freeStorage(data, deleteIt);
                oss << "]";
                break;
            }
            case TpBool: {
                Array<Bool> arr;
                ArrayColumn<Bool>(col).get(row, arr);
                oss << "[";
                Bool deleteIt;
                const Bool* data = arr.getStorage(deleteIt);
                for (uInt i = 0; i < arr.nelements(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << (data[i] ? "true" : "false");
                }
                arr.freeStorage(data, deleteIt);
                oss << "]";
                break;
            }
            case TpString: {
                Array<String> arr;
                ArrayColumn<String>(col).get(row, arr);
                oss << "[";
                Bool deleteIt;
                const String* data = arr.getStorage(deleteIt);
                for (uInt i = 0; i < arr.nelements(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << data[i];
                }
                arr.freeStorage(data, deleteIt);
                oss << "]";
                break;
            }
            case TpDComplex: {
                Array<DComplex> arr;
                ArrayColumn<DComplex>(col).get(row, arr);
                oss << "[";
                Bool deleteIt;
                const DComplex* data = arr.getStorage(deleteIt);
                for (uInt i = 0; i < arr.nelements(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << "(" << std::fixed << std::setprecision(6) << data[i].real()
                        << "," << std::fixed << std::setprecision(6) << data[i].imag() << ")";
                }
                arr.freeStorage(data, deleteIt);
                oss << "]";
                break;
            }
            default:
                oss << "<unsupported-array-type:" << dt << ">";
                break;
        }
        return oss.str();
    }

    // Scalar types
    switch (dt) {
        case TpBool: {
            Bool v;
            ScalarColumn<Bool>(col).get(row, v);
            return v ? "true" : "false";
        }
        case TpInt: {
            Int v;
            ScalarColumn<Int>(col).get(row, v);
            return std::to_string(v);
        }
        case TpInt64: {
            Int64 v;
            ScalarColumn<Int64>(col).get(row, v);
            return std::to_string(v);
        }
        case TpDouble: {
            Double v;
            ScalarColumn<Double>(col).get(row, v);
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(6) << v;
            return oss.str();
        }
        case TpFloat: {
            Float v;
            ScalarColumn<Float>(col).get(row, v);
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(6) << v;
            return oss.str();
        }
        case TpString: {
            String v;
            ScalarColumn<String>(col).get(row, v);
            return v;
        }
        case TpDComplex: {
            DComplex v;
            ScalarColumn<DComplex>(col).get(row, v);
            std::ostringstream oss;
            oss << "(" << std::fixed << std::setprecision(6) << v.real()
                << "," << std::fixed << std::setprecision(6) << v.imag() << ")";
            return oss.str();
        }
        default:
            return "<unsupported>";
    }
}

// ── Execute a TaQL query and return tab-separated result grid ──
void query_impl(const std::string& table_path,
                const std::string& query,
                std::string& out_result,
                uint64_t& out_nrow,
                uint64_t& out_ncol,
                uint64_t& out_elapsed_ns) {
    Table tab(table_path, Table::Old);

    // Replace $1 with the temp table
    std::vector<const Table*> tempTables = {&tab};

    auto t0 = std::chrono::steady_clock::now();

    // Execute via tableCommand
    Vector<String> colNames;
    String cmdType;
    TaQLResult taqlRes = tableCommand(query, tempTables, colNames, cmdType);
    Table result = taqlRes.table();

    auto t1 = std::chrono::steady_clock::now();
    out_elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

    out_nrow = result.nrow();
    // Get columns from result table
    Vector<String> resultCols;
    if (colNames.nelements() > 0) {
        resultCols = colNames;
    } else {
        // Use all columns from the result table
        resultCols.resize(result.tableDesc().ncolumn());
        for (uInt c = 0; c < result.tableDesc().ncolumn(); ++c) {
            resultCols[c] = result.tableDesc().columnNames()[c];
        }
    }
    out_ncol = resultCols.nelements();

    // Build tab-separated grid: first line is column names
    std::ostringstream oss;
    for (uInt c = 0; c < resultCols.nelements(); ++c) {
        if (c > 0) oss << '\t';
        oss << resultCols[c];
    }
    oss << '\n';

    // Data rows
    for (uInt r = 0; r < result.nrow(); ++r) {
        for (uInt c = 0; c < resultCols.nelements(); ++c) {
            if (c > 0) oss << '\t';
            TableColumn col(result, resultCols[c]);
            oss << format_cell(col, r);
        }
        oss << '\n';
    }

    out_result = oss.str();
}

} // anonymous namespace

extern "C" {

int32_t cpp_taql_write_simple_fixture(const char* path, char** out_error) {
    try {
        write_simple_fixture_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = casacore_shim::make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = casacore_shim::make_error("unknown exception");
        return -1;
    }
}

int32_t cpp_taql_write_array_fixture(const char* path, char** out_error) {
    try {
        write_array_fixture_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = casacore_shim::make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = casacore_shim::make_error("unknown exception");
        return -1;
    }
}

int32_t cpp_taql_write_varshape_fixture(const char* path, char** out_error) {
    try {
        write_varshape_fixture_impl(path);
        return 0;
    } catch (const std::exception& e) {
        *out_error = casacore_shim::make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = casacore_shim::make_error("unknown exception");
        return -1;
    }
}

int32_t cpp_taql_query(
    const char* table_path,
    const char* query,
    char** out_result,
    uint64_t* out_nrow,
    uint64_t* out_ncol,
    uint64_t* out_elapsed_ns,
    char** out_error)
{
    try {
        std::string result;
        uint64_t nrow = 0, ncol = 0, elapsed = 0;
        query_impl(table_path, query, result, nrow, ncol, elapsed);

        *out_nrow = nrow;
        *out_ncol = ncol;
        *out_elapsed_ns = elapsed;

        // Allocate result string for Rust
        *out_result = static_cast<char*>(std::malloc(result.size() + 1));
        if (*out_result) {
            std::memcpy(*out_result, result.c_str(), result.size() + 1);
        }
        return 0;
    } catch (const std::exception& e) {
        *out_error = casacore_shim::make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = casacore_shim::make_error("unknown exception");
        return -1;
    }
}

void cpp_taql_free_result(char* ptr) {
    std::free(ptr);
}

} // extern "C"
