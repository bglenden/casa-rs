// Benchmark shim: write and read 10k variable-shape array cells via AipsIO.
#include "casacore_cpp_common.h"
#include <casacore/tables/DataMan/StManAipsIO.h>
#include <chrono>

using casacore_shim::make_error;

namespace {

// Write a table with `nrows` variable-shape Float32 rows,
// alternating shapes [2,3] and [3,2] with ascending values.
void bench_write_impl(const std::string& path, uint64_t nrows) {
    casacore::TableDesc td("", casacore::TableDesc::Scratch);
    td.addColumn(casacore::ArrayColumnDesc<casacore::Float>("data", 2));

    casacore::SetupNewTable setup(path, td, casacore::Table::New);
    casacore::StManAipsIO stman;
    setup.bindAll(stman);

    casacore::Table table(setup, nrows);
    casacore::ArrayColumn<casacore::Float> col(table, "data");

    float v = 1.0f;
    for (uint64_t r = 0; r < nrows; ++r) {
        casacore::IPosition shape = (r % 2 == 0)
            ? casacore::IPosition(2, 2, 3)
            : casacore::IPosition(2, 3, 2);
        col.setShape(r, shape);
        casacore::Array<casacore::Float> arr(shape);
        auto it = arr.begin();
        for (int64_t i = 0; i < shape.product(); ++i, ++it) {
            *it = v;
            v += 1.0f;
        }
        col.put(r, arr);
    }
    table.flush();
}

// Read back all rows, summing element counts for validation.
uint64_t bench_read_impl(const std::string& path) {
    casacore::Table table(path, casacore::Table::Old);
    casacore::ArrayColumn<casacore::Float> col(table, "data");
    uint64_t total_elems = 0;
    for (casacore::uInt r = 0; r < table.nrow(); ++r) {
        casacore::Array<casacore::Float> arr = col(r);
        total_elems += arr.nelements();
    }
    return total_elems;
}

} // anonymous namespace

extern "C" {

int32_t cpp_vararray_bench_write_read(
    const char* path,
    uint64_t nrows,
    uint64_t* out_write_ns,
    uint64_t* out_read_ns,
    uint64_t* out_total_elems,
    char** out_error)
{
    try {
        auto t0 = std::chrono::steady_clock::now();
        bench_write_impl(path, nrows);
        auto t1 = std::chrono::steady_clock::now();
        uint64_t elems = bench_read_impl(path);
        auto t2 = std::chrono::steady_clock::now();

        *out_write_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
        *out_read_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count());
        *out_total_elems = elems;
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what()); return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in vararray_bench"); return -1;
    }
}

} // extern "C"
