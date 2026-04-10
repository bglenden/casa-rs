// SPDX-License-Identifier: LGPL-3.0-or-later
#include "casacore_cpp_common.h"
#include <algorithm>
#include <cstdint>
#include <cmath>
#include <casacore/casa/Arrays/Array.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Arrays/Vector.h>
#include <casacore/casa/BasicSL/Complex.h>
#include <casacore/lattices/LatticeMath/LatticeFFT.h>
#include <casacore/lattices/Lattices/ArrayLattice.h>
#include <casacore/scimath/Mathematics/ConvolveGridder.h>

using casacore_shim::make_error;
static casacore_shim::TerminateGuard g_terminate_guard_gridder;

extern "C" int cpp_convolve_gridder_grid_unit_sample_2d(
    int nx,
    int ny,
    double scale_x,
    double scale_y,
    double offset_x,
    double offset_y,
    double u,
    double v,
    int* loc_out,
    double* gpos_out,
    int* support_out,
    int* sampling_out,
    int* x_out,
    int* y_out,
    float* value_re_out,
    float* value_im_out,
    int max_points,
    int* count_out,
    char** out_error
) {
    try {
        if (
            !loc_out || !gpos_out || !support_out || !sampling_out || !x_out || !y_out
            || !value_re_out || !value_im_out || !count_out || max_points <= 0
        ) {
            if (out_error) *out_error = make_error("null output buffer");
            return 1;
        }

        casacore::IPosition shape(2, nx, ny);
        casacore::Vector<double> scale(2);
        scale(0) = scale_x;
        scale(1) = scale_y;
        casacore::Vector<double> offset(2);
        offset(0) = offset_x;
        offset(1) = offset_y;
        casacore::ConvolveGridder<double, casacore::Complex> gridder(shape, scale, offset, "SF");
        casacore::Array<casacore::Complex> grid(shape);
        grid.set(casacore::Complex(0.0f, 0.0f));

        casacore::Vector<double> position(2);
        position(0) = u;
        position(1) = v;
        casacore::Vector<int> loc(2);
        casacore::Vector<double> gpos(2);
        gridder.location(loc, position);
        gridder.position(gpos, position);
        loc_out[0] = loc(0);
        loc_out[1] = loc(1);
        gpos_out[0] = gpos(0);
        gpos_out[1] = gpos(1);
        support_out[0] = gridder.cSupport()(0);
        sampling_out[0] = gridder.cSampling();

        if (!gridder.grid(grid, position, casacore::Complex(1.0f, 0.0f))) {
            if (out_error) *out_error = make_error("sample is off grid");
            return 2;
        }

        casacore::Bool delete_it = false;
        const casacore::Complex* storage = grid.getStorage(delete_it);
        int count = 0;
        for (int y_index = 0; y_index < ny; ++y_index) {
            for (int x_index = 0; x_index < nx; ++x_index) {
                const auto value = storage[x_index + nx * y_index];
                if (value != casacore::Complex(0.0f, 0.0f)) {
                    if (count >= max_points) {
                        grid.freeStorage(storage, delete_it);
                        if (out_error) *out_error = make_error("output buffer too small");
                        return 3;
                    }
                    x_out[count] = x_index;
                    y_out[count] = y_index;
                    value_re_out[count] = value.real();
                    value_im_out[count] = value.imag();
                    ++count;
                }
            }
        }
        grid.freeStorage(storage, delete_it);
        *count_out = count;
        return 0;
    } catch (const std::exception& err) {
        if (out_error) *out_error = make_error(err.what());
        return 1;
    } catch (...) {
        if (out_error) *out_error = make_error("unknown C++ exception");
        return 1;
    }
}

extern "C" int cpp_convolve_gridder_correction_row_2d(
    int nx,
    int ny,
    double scale_x,
    double scale_y,
    double offset_x,
    double offset_y,
    int locy,
    float* factor_out,
    int max_len,
    int* nread_out,
    char** out_error
) {
    try {
        if (!factor_out || !nread_out || max_len < nx) {
            if (out_error) *out_error = make_error("invalid correction-row output buffer");
            return 1;
        }
        casacore::IPosition shape(2, nx, ny);
        casacore::Vector<double> scale(2);
        scale(0) = scale_x;
        scale(1) = scale_y;
        casacore::Vector<double> offset(2);
        offset(0) = offset_x;
        offset(1) = offset_y;
        casacore::ConvolveGridder<double, casacore::Complex> gridder(shape, scale, offset, "SF");
        casacore::Vector<casacore::Complex> factor(nx);
        gridder.correctX1D(factor, locy);
        for (int index = 0; index < nx; ++index) {
            factor_out[index] = factor(index).real();
        }
        *nread_out = nx;
        return 0;
    } catch (const std::exception& err) {
        if (out_error) *out_error = make_error(err.what());
        return 1;
    } catch (...) {
        if (out_error) *out_error = make_error("unknown C++ exception");
        return 1;
    }
}

extern "C" int cpp_convolve_gridder_make_dirty_image_2d(
    int grid_nx,
    int grid_ny,
    int image_nx,
    int image_ny,
    double scale_x,
    double scale_y,
    double offset_x,
    double offset_y,
    const double* u_out,
    const double* v_out,
    const float* vis_re_out,
    const float* vis_im_out,
    const float* weight_out,
    const uint8_t* gridable_out,
    int nsamples,
    float* image_out,
    int max_image_len,
    char** out_error
) {
    try {
        if (
            !u_out || !v_out || !vis_re_out || !vis_im_out || !weight_out || !gridable_out
            || !image_out || nsamples < 0 || max_image_len < image_nx * image_ny
        ) {
            if (out_error) *out_error = make_error("invalid dirty-image buffers");
            return 1;
        }

        casacore::IPosition shape(2, grid_nx, grid_ny);
        casacore::Vector<double> scale(2);
        scale(0) = scale_x;
        scale(1) = scale_y;
        casacore::Vector<double> offset(2);
        offset(0) = offset_x;
        offset(1) = offset_y;
        casacore::ConvolveGridder<double, casacore::Complex> gridder(shape, scale, offset, "SF");
        casacore::Array<casacore::Complex> grid(shape);
        grid.set(casacore::Complex(0.0f, 0.0f));

        double normalization_sumwt = 0.0;
        for (int index = 0; index < nsamples; ++index) {
            if (!gridable_out[index]) {
                continue;
            }
            const float weight = weight_out[index];
            const float vis_re = vis_re_out[index];
            const float vis_im = vis_im_out[index];
            if (
                !std::isfinite(weight) || weight <= 0.0f || !std::isfinite(vis_re)
                || !std::isfinite(vis_im)
            ) {
                continue;
            }
            casacore::Vector<double> positive(2);
            positive(0) = u_out[index];
            positive(1) = -v_out[index];
            casacore::Vector<double> negative(2);
            negative(0) = -u_out[index];
            negative(1) = v_out[index];
            const casacore::Complex value(vis_re * weight, vis_im * weight);
            if (!gridder.grid(grid, positive, value)) {
                continue;
            }
            if (!gridder.grid(grid, negative, conj(value))) {
                continue;
            }
            normalization_sumwt += 2.0 * weight;
        }

        if (!(normalization_sumwt > 0.0) || !std::isfinite(normalization_sumwt)) {
            if (out_error) *out_error = make_error("no usable samples for dirty-image shim");
            return 2;
        }

        casacore::ArrayLattice<casacore::Complex> lattice(grid);
        casacore::LatticeFFT::cfft2d(lattice, false);

        casacore::Vector<casacore::Complex> correction(grid_nx);
        for (int y = 0; y < grid_ny; ++y) {
            gridder.correctX1D(correction, y);
            for (int x = 0; x < grid_nx; ++x) {
                grid(casacore::IPosition(2, x, y)) /= correction(x);
                grid(casacore::IPosition(2, x, y)) *= static_cast<float>(
                    (static_cast<double>(grid_nx) * static_cast<double>(grid_ny))
                    / normalization_sumwt
                );
            }
        }

        const int blc_x = (grid_nx - image_nx + ((grid_nx % 2) == 0 ? 1 : 0)) / 2;
        const int blc_y = (grid_ny - image_ny + ((grid_ny % 2) == 0 ? 1 : 0)) / 2;
        int out_index = 0;
        for (int x = 0; x < image_nx; ++x) {
            for (int y = 0; y < image_ny; ++y) {
                image_out[out_index++] =
                    real(grid(casacore::IPosition(2, blc_x + x, blc_y + y)));
            }
        }
        return 0;
    } catch (const std::exception& err) {
        if (out_error) *out_error = make_error(err.what());
        return 1;
    } catch (...) {
        if (out_error) *out_error = make_error("unknown C++ exception");
        return 1;
    }
}
