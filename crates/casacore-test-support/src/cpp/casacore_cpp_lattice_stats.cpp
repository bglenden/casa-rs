// Rust↔C++ benchmark shim for lattice statistics on forced-I/O paged lattices.
#include "casacore_cpp_common.h"

#include <casacore/lattices/Lattices/PagedArray.h>
#include <casacore/lattices/Lattices/SubLattice.h>
#include <casacore/lattices/Lattices/TiledShape.h>
#include <casacore/lattices/LatticeMath/LatticeStatistics.h>

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <string>

using casacore_shim::make_error;

namespace {

casacore::IPosition to_iposition(const int32_t* values, int32_t ndim) {
    casacore::IPosition out(ndim);
    for (int32_t i = 0; i < ndim; ++i) {
        out(i) = values[i];
    }
    return out;
}

void fill_ramp_cube(casacore::Array<casacore::Float>& data, const casacore::IPosition& shape) {
    if (shape.nelements() != 3) {
        throw std::runtime_error("lattice stats shim currently requires a 3-D shape");
    }

    const auto nx = shape(0);
    const auto ny = shape(1);
    const auto nz = shape(2);
    const auto plane = static_cast<double>(nx) * static_cast<double>(ny);

    for (int z = 0; z < nz; ++z) {
        for (int y = 0; y < ny; ++y) {
            for (int x = 0; x < nx; ++x) {
                const auto value = static_cast<float>(
                    static_cast<double>(x) + static_cast<double>(y) * nx + z * plane
                );
                data(casacore::IPosition(3, x, y, z)) = value;
            }
        }
    }
}

void copy_stat_array(
    const casacore::Array<double>& input,
    double* output,
    int64_t output_len,
    const char* label
) {
    const auto nelements = static_cast<int64_t>(input.nelements());
    if (nelements != output_len) {
        throw std::runtime_error(
            std::string(label) + " output length mismatch: expected "
            + std::to_string(output_len) + ", got " + std::to_string(nelements)
        );
    }

    bool delete_it = false;
    const auto* storage = input.getStorage(delete_it);
    std::copy(storage, storage + nelements, output);
    input.freeStorage(storage, delete_it);
}

void lattice_statistics_forced_io_impl(
    const char* path,
    const int32_t* shape,
    int32_t ndim,
    const int32_t* tile_shape,
    int32_t tile_ndim,
    uint64_t cache_tiles,
    double* mean_out,
    double* sigma_out,
    double* median_out,
    double* q1_out,
    double* q3_out,
    int64_t output_len,
    uint64_t* basic_ns_out,
    uint64_t* order_ns_out
) {
    if (!path) {
        throw std::runtime_error("null path");
    }
    if (!shape || !tile_shape) {
        throw std::runtime_error("null shape pointer");
    }
    if (!mean_out || !sigma_out || !median_out || !q1_out || !q3_out) {
        throw std::runtime_error("null output pointer");
    }
    if (!basic_ns_out || !order_ns_out) {
        throw std::runtime_error("null timing output pointer");
    }
    if (ndim != tile_ndim) {
        throw std::runtime_error("shape/tile ndim mismatch");
    }

    const auto lattice_shape = to_iposition(shape, ndim);
    const auto lattice_tile_shape = to_iposition(tile_shape, tile_ndim);
    const casacore::IPosition origin(ndim, 0);
    const casacore::IPosition unit_stride(ndim, 1);

    casacore::PagedArray<casacore::Float> lattice(
        casacore::TiledShape(lattice_shape, lattice_tile_shape),
        path
    );

    casacore::Array<casacore::Float> data(lattice_shape);
    fill_ramp_cube(data, lattice_shape);
    lattice.putSlice(data, origin, unit_stride);
    lattice.setCacheSizeInTiles(static_cast<casacore::uInt>(cache_tiles));
    lattice.tempClose();
    casacore::SubLattice<casacore::Float> sub_lattice(lattice);

    casacore::Vector<casacore::Int> axes(2);
    axes(0) = 0;
    axes(1) = 1;

    {
        casacore::LatticeStatistics<casacore::Float> stats(
            sub_lattice, false, false, false
        );
        if (!stats.setAxes(axes)) {
            throw std::runtime_error("C++ setAxes([0,1]) failed");
        }
        casacore::Array<double> npts;
        casacore::Array<double> mean;
        casacore::Array<double> sigma;

        const auto t0 = std::chrono::steady_clock::now();
        if (!stats.getStatistic(npts, casacore::LatticeStatsBase::NPTS)) {
            throw std::runtime_error("C++ getStatistic(NPTS) failed");
        }
        if (!stats.getStatistic(mean, casacore::LatticeStatsBase::MEAN)) {
            throw std::runtime_error("C++ getStatistic(MEAN) failed");
        }
        if (!stats.getStatistic(sigma, casacore::LatticeStatsBase::SIGMA)) {
            throw std::runtime_error("C++ getStatistic(SIGMA) failed");
        }
        const auto t1 = std::chrono::steady_clock::now();
        *basic_ns_out = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()
        );

        copy_stat_array(mean, mean_out, output_len, "mean");
        copy_stat_array(sigma, sigma_out, output_len, "sigma");
    }

    lattice.tempClose();

    {
        casacore::LatticeStatistics<casacore::Float> stats(
            sub_lattice, false, false, false
        );
        if (!stats.setAxes(axes)) {
            throw std::runtime_error("C++ setAxes([0,1]) failed");
        }
        casacore::Array<double> median;
        casacore::Array<double> q1;
        casacore::Array<double> q3;

        const auto t0 = std::chrono::steady_clock::now();
        if (!stats.getStatistic(median, casacore::LatticeStatsBase::MEDIAN)) {
            throw std::runtime_error("C++ getStatistic(MEDIAN) failed");
        }
        if (!stats.getStatistic(q1, casacore::LatticeStatsBase::Q1)) {
            throw std::runtime_error("C++ getStatistic(Q1) failed");
        }
        if (!stats.getStatistic(q3, casacore::LatticeStatsBase::Q3)) {
            throw std::runtime_error("C++ getStatistic(Q3) failed");
        }
        const auto t1 = std::chrono::steady_clock::now();
        *order_ns_out = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()
        );

        copy_stat_array(median, median_out, output_len, "median");
        copy_stat_array(q1, q1_out, output_len, "q1");
        copy_stat_array(q3, q3_out, output_len, "q3");
    }
}

void lattice_statistics_forced_io_repeated_basic_impl(
    const char* path,
    const int32_t* shape,
    int32_t ndim,
    const int32_t* tile_shape,
    int32_t tile_ndim,
    uint64_t cache_tiles,
    uint32_t iterations,
    uint64_t* total_ns_out,
    double* checksum_out
) {
    if (!path) {
        throw std::runtime_error("null path");
    }
    if (!shape || !tile_shape) {
        throw std::runtime_error("null shape pointer");
    }
    if (!total_ns_out || !checksum_out) {
        throw std::runtime_error("null output pointer");
    }
    if (ndim != tile_ndim) {
        throw std::runtime_error("shape/tile ndim mismatch");
    }

    const auto lattice_shape = to_iposition(shape, ndim);
    const auto lattice_tile_shape = to_iposition(tile_shape, tile_ndim);
    const casacore::IPosition origin(ndim, 0);
    const casacore::IPosition unit_stride(ndim, 1);

    casacore::PagedArray<casacore::Float> lattice(
        casacore::TiledShape(lattice_shape, lattice_tile_shape),
        path
    );

    casacore::Array<casacore::Float> data(lattice_shape);
    fill_ramp_cube(data, lattice_shape);
    lattice.putSlice(data, origin, unit_stride);
    lattice.setCacheSizeInTiles(static_cast<casacore::uInt>(cache_tiles));

    casacore::SubLattice<casacore::Float> sub_lattice(lattice);
    casacore::Vector<casacore::Int> axes(2);
    axes(0) = 0;
    axes(1) = 1;

    double checksum = 0.0;
    const auto t0 = std::chrono::steady_clock::now();
    for (uint32_t iter = 0; iter < iterations; ++iter) {
        lattice.tempClose();
        casacore::LatticeStatistics<casacore::Float> stats(
            sub_lattice, false, false, false
        );
        if (!stats.setAxes(axes)) {
            throw std::runtime_error("C++ setAxes([0,1]) failed");
        }

        casacore::Array<double> npts;
        casacore::Array<double> mean;
        casacore::Array<double> sigma;
        if (!stats.getStatistic(npts, casacore::LatticeStatsBase::NPTS)) {
            throw std::runtime_error("C++ getStatistic(NPTS) failed");
        }
        if (!stats.getStatistic(mean, casacore::LatticeStatsBase::MEAN)) {
            throw std::runtime_error("C++ getStatistic(MEAN) failed");
        }
        if (!stats.getStatistic(sigma, casacore::LatticeStatsBase::SIGMA)) {
            throw std::runtime_error("C++ getStatistic(SIGMA) failed");
        }

        bool delete_npts = false;
        bool delete_mean = false;
        bool delete_sigma = false;
        const auto* npts_storage = npts.getStorage(delete_npts);
        const auto* mean_storage = mean.getStorage(delete_mean);
        const auto* sigma_storage = sigma.getStorage(delete_sigma);
        checksum += npts_storage[0] + mean_storage[0] + sigma_storage[0];
        npts.freeStorage(npts_storage, delete_npts);
        mean.freeStorage(mean_storage, delete_mean);
        sigma.freeStorage(sigma_storage, delete_sigma);
    }
    const auto t1 = std::chrono::steady_clock::now();

    *total_ns_out = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()
    );
    *checksum_out = checksum;
}

}  // namespace

extern "C" {

int32_t cpp_lattice_stats_float_forced_io(
    const char* path,
    const int32_t* shape,
    int32_t ndim,
    const int32_t* tile_shape,
    int32_t tile_ndim,
    uint64_t cache_tiles,
    double* mean_out,
    double* sigma_out,
    double* median_out,
    double* q1_out,
    double* q3_out,
    int64_t output_len,
    uint64_t* basic_ns_out,
    uint64_t* order_ns_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        lattice_statistics_forced_io_impl(
            path,
            shape,
            ndim,
            tile_shape,
            tile_ndim,
            cache_tiles,
            mean_out,
            sigma_out,
            median_out,
            q1_out,
            q3_out,
            output_len,
            basic_ns_out,
            order_ns_out
        );
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error("unknown exception in cpp_lattice_stats_float_forced_io");
        return -1;
    }
}

int32_t cpp_lattice_stats_float_forced_io_repeated_basic(
    const char* path,
    const int32_t* shape,
    int32_t ndim,
    const int32_t* tile_shape,
    int32_t tile_ndim,
    uint64_t cache_tiles,
    uint32_t iterations,
    uint64_t* total_ns_out,
    double* checksum_out,
    char** out_error
) {
    try {
        casacore_shim::TerminateGuard tg;
        lattice_statistics_forced_io_repeated_basic_impl(
            path,
            shape,
            ndim,
            tile_shape,
            tile_ndim,
            cache_tiles,
            iterations,
            total_ns_out,
            checksum_out
        );
        return 0;
    } catch (const std::exception& e) {
        *out_error = make_error(e.what());
        return -1;
    } catch (...) {
        *out_error = make_error(
            "unknown exception in cpp_lattice_stats_float_forced_io_repeated_basic"
        );
        return -1;
    }
}

}  // extern "C"
