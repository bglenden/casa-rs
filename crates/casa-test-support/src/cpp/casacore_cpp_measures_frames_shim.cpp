// SPDX-License-Identifier: LGPL-3.0-or-later
// C++ interop shim for the frame and sidereal-epoch laws (#850): casacore's
// raw direction vectors and its epochs, under either IAU model.

#include <casacore/casa/Quanta/MVDirection.h>
#include <casacore/casa/Quanta/MVEpoch.h>
#include <casacore/casa/Quanta/MVPosition.h>
#include <casacore/casa/Quanta/Quantum.h>
#include <casacore/measures/Measures/MCDirection.h>
#include <casacore/measures/Measures/MCEpoch.h>
#include <casacore/measures/Measures/MDirection.h>
#include <casacore/measures/Measures/MEpoch.h>
#include <casacore/measures/Measures/MPosition.h>
#include <casacore/measures/Measures/MeasConvert.h>
#include <casacore/measures/Measures/MeasFrame.h>

#include <memory>

#include "casacore_cpp_measures_iau_model.h"

using namespace casacore;

namespace {

std::unique_ptr<casacore_shim::Iau2000AScope> iau_model(int iau2000a) {
    return iau2000a ? std::make_unique<casacore_shim::Iau2000AScope>() : nullptr;
}

// A frame with an optional UTC epoch (0 means none) and an optional WGS84
// observatory (all-zero coordinates mean none).
MeasFrame make_frame(double epoch_mjd, double obs_lon, double obs_lat, double obs_h) {
    MeasFrame frame;
    if (epoch_mjd != 0.0) {
        frame.set(MEpoch(MVEpoch(epoch_mjd), MEpoch::UTC));
    }
    if (obs_lon != 0.0 || obs_lat != 0.0 || obs_h != 0.0) {
        frame.set(MPosition(MVPosition(Quantity(obs_h, "m"), Quantity(obs_lon, "rad"),
                                       Quantity(obs_lat, "rad")),
                            MPosition::WGS84));
    }
    return frame;
}

}  // namespace

extern "C" {

// Convert a direction and return casacore's direction vector as it leaves
// MCDirection, unnormalized: MVDirection::getLat() takes asin(z) of it.
int measures_shim_direction_convert_cosines(
    double lon_in, double lat_in, const char *ref_in, const char *ref_out, double epoch_mjd,
    int iau2000a, double *xyz_out) {
    try {
        auto model = iau_model(iau2000a);
        if (model && !model->active()) {
            return -2;
        }
        MDirection::Types type_in;
        MDirection::Types type_out;
        if (!MDirection::getType(type_in, String(ref_in)) ||
            !MDirection::getType(type_out, String(ref_out))) {
            return -3;
        }
        MDirection direction(MVDirection(Quantity(lon_in, "rad"), Quantity(lat_in, "rad")),
                             type_in);
        MeasFrame frame = make_frame(epoch_mjd, 0.0, 0.0, 0.0);
        MVDirection result =
            MDirection::Convert(direction, MDirection::Ref(type_out, frame))().getValue();
        for (int k = 0; k < 3; ++k) {
            xyz_out[k] = result(k);
        }
        return 0;
    } catch (...) {
        return -1;
    }
}

// Convert an epoch with an optional WGS84 observatory (all-zero coordinates
// mean none), under IAU 2006/2000A when `iau2000a` is set. Returns the full
// value, casacore's sidereal day count included.
int measures_shim_epoch_convert_model(
    double mjd_in, const char *ref_in, const char *ref_out, double obs_lon, double obs_lat,
    double obs_h, int iau2000a, double *mjd_out) {
    try {
        auto model = iau_model(iau2000a);
        if (model && !model->active()) {
            return -2;
        }
        MEpoch::Types type_in;
        MEpoch::Types type_out;
        if (!MEpoch::getType(type_in, String(ref_in)) ||
            !MEpoch::getType(type_out, String(ref_out))) {
            return -3;
        }
        MeasFrame frame = make_frame(0.0, obs_lon, obs_lat, obs_h);
        MEpoch epoch(MVEpoch(mjd_in), type_in);
        *mjd_out = MEpoch::Convert(epoch, MEpoch::Ref(type_out, frame))().getValue().get();
        return 0;
    } catch (...) {
        return -1;
    }
}

}  // extern "C"
