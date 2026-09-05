// SPDX-License-Identifier: LGPL-3.0-or-later
// CASA/casacore spectral interpolation oracle used by the focused T35/T36 gates.
#include "casacore_cpp_common.h"

#include <casacore/casa/Arrays/Array.h>
#include <casacore/casa/Arrays/Vector.h>
#include <casacore/scimath/Mathematics/InterpolateArray1D.h>

#include <algorithm>
#include <exception>

using casacore_shim::make_error;
static casacore_shim::TerminateGuard g_terminate_guard_spectral;

extern "C" int cpp_spectral_interpolation_coefficients(
    const double* input_coordinates,
    int input_count,
    double output_coordinate,
    int method,
    double* coefficients_out,
    unsigned char* valid_out,
    char** out_error)
{
    try {
        if (!input_coordinates || !coefficients_out || !valid_out || input_count <= 0) {
            throw std::runtime_error("invalid spectral interpolation buffers");
        }
        if (method < casacore::InterpolateArray1D<double, double>::nearestNeighbour
            || method > casacore::InterpolateArray1D<double, double>::cubic) {
            throw std::runtime_error("unsupported spectral interpolation method");
        }

        casacore::Vector<double> input_x(input_count);
        for (int index = 0; index < input_count; ++index) {
            input_x[index] = input_coordinates[index];
            coefficients_out[index] = 0.0;
        }
        casacore::Vector<double> output_x(1);
        output_x[0] = output_coordinate;

        const auto [minimum, maximum] = std::minmax_element(
            input_coordinates, input_coordinates + input_count);
        *valid_out = output_coordinate >= *minimum && output_coordinate <= *maximum ? 1 : 0;
        if (!*valid_out) {
            return 0;
        }

        for (int basis = 0; basis < input_count; ++basis) {
            casacore::Vector<double> input_y(input_count, 0.0);
            input_y[basis] = 1.0;
            casacore::Array<double> output_y;
            casacore::InterpolateArray1D<double, double>::interpolate(
                output_y, output_x, input_x, input_y, method);
            coefficients_out[basis] = output_y(casacore::IPosition(1, 0));
        }
        return 0;
    } catch (const std::exception& error) {
        *out_error = make_error(error.what());
        return 1;
    } catch (...) {
        *out_error = make_error("unknown casacore spectral interpolation failure");
        return 1;
    }
}
