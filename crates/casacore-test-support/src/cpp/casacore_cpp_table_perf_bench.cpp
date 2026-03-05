// Benchmark shims: set algebra, row copy, and cell slicing via C++ casacore.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/StManAipsIO.h>
#include <casacore/tables/Tables/TableCopy.h>
#include <casacore/casa/Arrays/Slicer.h>
#include <casacore/casa/Arrays/Vector.h>

#include <chrono>

using casacore_shim::make_error;

namespace {

// ===== Set algebra benchmark =====
//
// Creates a parent table with `nrows` rows (single Int column "id"),
// then creates two RefTables (rows [0..split_a) and [split_b..nrows))
// and times Table::operator|, operator&, and operator-.

void set_algebra_bench_impl(
    const std::string& path,
    uint64_t nrows,
    uint64_t split_a,
    uint64_t split_b,
    uint64_t* out_union_ns,
    uint64_t* out_intersection_ns,
    uint64_t* out_difference_ns,
    uint64_t* out_union_rows,
    uint64_t* out_intersection_rows,
    uint64_t* out_difference_rows)
{
    // Create the table
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("id"));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, nrows);
    casacore::ScalarColumn<casacore::Int> col(table, "id");
    for (uint64_t i = 0; i < nrows; ++i) {
        col.put(i, static_cast<casacore::Int>(i));
    }
    table.flush();

    // Build two row-selected sub-tables
    casacore::Vector<casacore::rownr_t> rowsA(split_a);
    for (uint64_t i = 0; i < split_a; ++i) rowsA(i) = i;
    casacore::Table selA = table(rowsA);

    uint64_t countB = nrows - split_b;
    casacore::Vector<casacore::rownr_t> rowsB(countB);
    for (uint64_t i = 0; i < countB; ++i) rowsB(i) = split_b + i;
    casacore::Table selB = table(rowsB);

    // Time union
    {
        auto t0 = std::chrono::steady_clock::now();
        casacore::Table result = selA | selB;
        auto t1 = std::chrono::steady_clock::now();
        *out_union_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
        *out_union_rows = result.nrow();
    }

    // Time intersection
    {
        auto t0 = std::chrono::steady_clock::now();
        casacore::Table result = selA & selB;
        auto t1 = std::chrono::steady_clock::now();
        *out_intersection_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
        *out_intersection_rows = result.nrow();
    }

    // Time difference
    {
        auto t0 = std::chrono::steady_clock::now();
        casacore::Table result = selA - selB;
        auto t1 = std::chrono::steady_clock::now();
        *out_difference_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
        *out_difference_rows = result.nrow();
    }
}

// ===== Row copy benchmark =====
//
// Creates a source table with `nrows` rows (Int32 "col_i32", Double "col_f64",
// String "col_str"), creates an empty dest, and times TableCopy::copyRows.

void copy_rows_bench_impl(
    const std::string& dir,
    uint64_t nrows,
    uint64_t* out_ns)
{
    std::string srcPath = dir + "/source.tbl";
    std::string dstPath = dir + "/dest.tbl";

    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ScalarColumnDesc<casacore::Int>("col_i32"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::Double>("col_f64"));
    td.addColumn(casacore::ScalarColumnDesc<casacore::String>("col_str"));

    // Write source
    {
        casacore::SetupNewTable setup(srcPath, td, casacore::Table::New);
        casacore::StManAipsIO stman;
        setup.bindAll(stman);
        casacore::Table src(setup, nrows);
        casacore::ScalarColumn<casacore::Int> c1(src, "col_i32");
        casacore::ScalarColumn<casacore::Double> c2(src, "col_f64");
        casacore::ScalarColumn<casacore::String> c3(src, "col_str");
        for (uint64_t i = 0; i < nrows; ++i) {
            c1.put(i, static_cast<casacore::Int>(i));
            c2.put(i, static_cast<casacore::Double>(i));
            c3.put(i, "row_" + std::to_string(i));
        }
        src.flush();
    }

    // Create empty dest
    {
        casacore::SetupNewTable setup(dstPath, td, casacore::Table::New);
        casacore::StManAipsIO stman;
        setup.bindAll(stman);
        casacore::Table dst(setup, 0);
        dst.flush();
    }

    // Open both and time copyRows
    casacore::Table src(srcPath, casacore::Table::Old);
    casacore::Table dst(dstPath, casacore::Table::Update);

    auto t0 = std::chrono::steady_clock::now();
    casacore::TableCopy::copyRows(dst, src);
    auto t1 = std::chrono::steady_clock::now();

    *out_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

    if (dst.nrow() != nrows) {
        throw std::runtime_error("copy_rows result row count mismatch: expected "
            + std::to_string(nrows) + ", got " + std::to_string(dst.nrow()));
    }
}

// ===== Cell slice benchmark =====
//
// Creates a table with `nrows` rows of fixed-shape Float64 arrays [dim0 x dim1],
// then times `nrows` getSlice calls with the given sub-region.

void cell_slice_bench_impl(
    const std::string& path,
    uint64_t nrows,
    int64_t dim0, int64_t dim1,
    int64_t slice_start0, int64_t slice_start1,
    int64_t slice_end0, int64_t slice_end1,
    uint64_t* out_write_ns,
    uint64_t* out_slice_ns)
{
    casacore::IPosition shape(2, dim0, dim1);

    // Write table
    {
        casacore::TableDesc td("", casacore::TableDesc::Scratch);
        td.addColumn(casacore::ArrayColumnDesc<casacore::Double>("data", shape,
            casacore::ColumnDesc::FixedShape));

        casacore::SetupNewTable setup(path, td, casacore::Table::New);
        casacore::StManAipsIO stman;
        setup.bindAll(stman);

        auto t0 = std::chrono::steady_clock::now();
        casacore::Table table(setup, nrows);
        casacore::ArrayColumn<casacore::Double> col(table, "data");
        casacore::Array<casacore::Double> arr(shape, 1.0);
        for (uint64_t r = 0; r < nrows; ++r) {
            col.put(r, arr);
        }
        table.flush();
        auto t1 = std::chrono::steady_clock::now();
        *out_write_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    }

    // Read slices
    casacore::Table table(path, casacore::Table::Old);
    casacore::ArrayColumn<casacore::Double> col(table, "data");

    // Slicer uses endIsLast (inclusive end index)
    casacore::Slicer slicer(
        casacore::IPosition(2, slice_start0, slice_start1),
        casacore::IPosition(2, slice_end0 - 1, slice_end1 - 1),
        casacore::Slicer::endIsLast);

    auto t0 = std::chrono::steady_clock::now();
    for (uint64_t r = 0; r < nrows; ++r) {
        casacore::Array<casacore::Double> slice = col.getSlice(r, slicer);
        // Prevent optimizer from eliding the call
        if (slice.nelements() == 0) {
            throw std::runtime_error("unexpected empty slice");
        }
    }
    auto t1 = std::chrono::steady_clock::now();

    *out_slice_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
}

} // anonymous namespace

extern "C" {

int32_t cpp_set_algebra_bench(
    const char* path,
    uint64_t nrows,
    uint64_t split_a,
    uint64_t split_b,
    uint64_t* out_union_ns,
    uint64_t* out_intersection_ns,
    uint64_t* out_difference_ns,
    uint64_t* out_union_rows,
    uint64_t* out_intersection_rows,
    uint64_t* out_difference_rows,
    char** out_error)
{
    try {
        set_algebra_bench_impl(path, nrows, split_a, split_b,
            out_union_ns, out_intersection_ns, out_difference_ns,
            out_union_rows, out_intersection_rows, out_difference_rows);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what()); return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in set_algebra_bench"); return -1;
    }
}

int32_t cpp_copy_rows_bench(
    const char* dir,
    uint64_t nrows,
    uint64_t* out_ns,
    char** out_error)
{
    try {
        copy_rows_bench_impl(dir, nrows, out_ns);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what()); return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in copy_rows_bench"); return -1;
    }
}

int32_t cpp_cell_slice_bench(
    const char* path,
    uint64_t nrows,
    int64_t dim0, int64_t dim1,
    int64_t slice_start0, int64_t slice_start1,
    int64_t slice_end0, int64_t slice_end1,
    uint64_t* out_write_ns,
    uint64_t* out_slice_ns,
    char** out_error)
{
    try {
        cell_slice_bench_impl(path, nrows, dim0, dim1,
            slice_start0, slice_start1, slice_end0, slice_end1,
            out_write_ns, out_slice_ns);
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what()); return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in cell_slice_bench"); return -1;
    }
}

} // extern "C"
