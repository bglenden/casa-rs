// SPDX-License-Identifier: LGPL-3.0-or-later
// C++ interop shim for measures module testing.

#include <cstring>
#include <cstdint>
#include <chrono>

#include <casacore/measures/Measures/MEpoch.h>
#include <casacore/measures/Measures/MPosition.h>
#include <casacore/measures/Measures/MeasConvert.h>
#include <casacore/measures/Measures/MeasFrame.h>
#include <casacore/measures/Measures/MCEpoch.h>
#include <casacore/measures/Measures/MCPosition.h>
#include <casacore/measures/Measures/MDirection.h>
#include <casacore/measures/Measures/MFrequency.h>
#include <casacore/measures/Measures/MDoppler.h>
#include <casacore/measures/Measures/MRadialVelocity.h>
#include <casacore/measures/Measures/MCDirection.h>
#include <casacore/measures/Measures/MCFrequency.h>
#include <casacore/measures/Measures/MCDoppler.h>
#include <casacore/measures/Measures/MCRadialVelocity.h>
#include <casacore/casa/Quanta/MVDirection.h>
#include <casacore/casa/Quanta/MVFrequency.h>
#include <casacore/casa/Quanta/MVDoppler.h>
#include <casacore/casa/Quanta/MVEpoch.h>
#include <casacore/casa/Quanta/MVPosition.h>
#include <casacore/casa/Quanta/Quantum.h>
#include <casacore/measures/Measures/MeasTable.h>
#include <casacore/casa/System/AipsrcValue.h>
#include <casacore/measures/Measures/Precession.h>
#include <casacore/measures/Measures/Aberration.h>
#include <casacore/measures/Measures/SolarPos.h>

using namespace casacore;

static MEpoch::Types parse_epoch_ref(const char* ref_str) {
    MEpoch::Types tp;
    if (!MEpoch::getType(tp, String(ref_str))) {
        throw std::runtime_error(String("Unknown epoch ref: ") + ref_str);
    }
    return tp;
}

static MPosition::Types parse_position_ref(const char* ref_str) {
    MPosition::Types tp;
    if (!MPosition::getType(tp, String(ref_str))) {
        throw std::runtime_error(String("Unknown position ref: ") + ref_str);
    }
    return tp;
}

static MDirection::Types parse_direction_ref(const char* ref_str) {
    MDirection::Types tp;
    if (!MDirection::getType(tp, String(ref_str))) {
        throw std::runtime_error(String("Unknown direction ref: ") + ref_str);
    }
    return tp;
}

static MFrequency::Types parse_frequency_ref(const char* ref_str) {
    MFrequency::Types tp;
    if (!MFrequency::getType(tp, String(ref_str))) {
        throw std::runtime_error(String("Unknown frequency ref: ") + ref_str);
    }
    return tp;
}

static MDoppler::Types parse_doppler_ref(const char* ref_str) {
    MDoppler::Types tp;
    if (!MDoppler::getType(tp, String(ref_str))) {
        throw std::runtime_error(String("Unknown doppler ref: ") + ref_str);
    }
    return tp;
}

extern "C" {

int measures_shim_epoch_convert(
    double mjd_in, const char* ref_in, const char* ref_out,
    double* mjd_out)
{
    try {
        auto tp_in = parse_epoch_ref(ref_in);
        auto tp_out = parse_epoch_ref(ref_out);

        MEpoch epoch(MVEpoch(mjd_in), tp_in);
        MEpoch result = MEpoch::Convert(epoch, tp_out)();

        *mjd_out = result.getValue().get();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_epoch_to_record(
    double mjd_in, const char* ref_in,
    double* value_out, char* unit_out, int unit_buf,
    char* refer_out, int refer_buf)
{
    try {
        auto tp_in = parse_epoch_ref(ref_in);
        MEpoch epoch(MVEpoch(mjd_in), tp_in);

        *value_out = epoch.getValue().get();

        strncpy(unit_out, "d", unit_buf - 1);
        unit_out[unit_buf - 1] = '\0';

        String ref_str = MEpoch::showType(tp_in);
        strncpy(refer_out, ref_str.c_str(), refer_buf - 1);
        refer_out[refer_buf - 1] = '\0';

        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_position_convert(
    double v0, double v1, double v2,
    const char* ref_in, const char* ref_out,
    double* out0, double* out1, double* out2)
{
    try {
        auto tp_in = parse_position_ref(ref_in);
        auto tp_out = parse_position_ref(ref_out);

        MVPosition mv;
        if (tp_in == MPosition::ITRF) {
            mv = MVPosition(v0, v1, v2);
        } else {
            // WGS84: construct from (height, lon, lat)
            mv = MVPosition(Quantity(v2, "m"), Quantity(v0, "rad"), Quantity(v1, "rad"));
        }

        MPosition pos(mv, tp_in);
        MPosition result = MPosition::Convert(pos, tp_out)();

        if (tp_out == MPosition::ITRF) {
            Vector<Double> xyz = result.getValue().getValue();
            *out0 = xyz(0);
            *out1 = xyz(1);
            *out2 = xyz(2);
        } else {
            *out0 = result.getValue().getLong();
            *out1 = result.getValue().getLat();
            *out2 = result.getValue().getLength().getValue();
        }
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_position_to_record(
    double x, double y, double z,
    double* lon_out, double* lat_out, double* radius_out)
{
    try {
        MVPosition mv(x, y, z);
        *lon_out = mv.getLong();
        *lat_out = mv.getLat();
        *radius_out = mv.getLength().getValue();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_bench_position_convert(
    double x_start, double y, double z,
    int count, const char* ref_in, const char* ref_out,
    int iterations, uint64_t* elapsed_ns)
{
    try {
        auto tp_in = parse_position_ref(ref_in);
        auto tp_out = parse_position_ref(ref_out);

        // Create a reusable converter to avoid per-call engine setup overhead
        MVPosition seed_mv(x_start, y, z);
        MPosition seed_pos(seed_mv, tp_in);
        MPosition::Convert converter(seed_pos, tp_out);

        for (int i = 0; i < 10; i++) {
            MVPosition mv(x_start + i, y, z);
            MPosition pos(mv, tp_in);
            converter(pos);
        }

        auto start = std::chrono::high_resolution_clock::now();
        for (int iter = 0; iter < iterations; iter++) {
            for (int i = 0; i < count; i++) {
                double x = x_start + i;
                MVPosition mv;
                if (tp_in == MPosition::ITRF) {
                    mv = MVPosition(x, y, z);
                } else {
                    mv = MVPosition(Quantity(z, "m"), Quantity(x, "rad"), Quantity(y, "rad"));
                }
                MPosition pos(mv, tp_in);
                MPosition result = converter(pos);
                if (result.getValue().getLength().getValue() < -1e30) {
                    *elapsed_ns = 0;
                    return -1;
                }
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        *elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_bench_epoch_convert(
    double mjd_start, int count, const char* ref_in, const char* ref_out,
    int iterations, uint64_t* elapsed_ns)
{
    try {
        auto tp_in = parse_epoch_ref(ref_in);
        auto tp_out = parse_epoch_ref(ref_out);

        // Create a reusable converter to avoid per-call engine setup overhead
        MEpoch seed(MVEpoch(mjd_start), tp_in);
        MEpoch::Convert converter(seed, tp_out);

        for (int i = 0; i < 10; i++) {
            MEpoch epoch(MVEpoch(mjd_start + i * 0.001), tp_in);
            converter(epoch);
        }

        auto start = std::chrono::high_resolution_clock::now();
        for (int iter = 0; iter < iterations; iter++) {
            for (int i = 0; i < count; i++) {
                double mjd = mjd_start + i * 0.001;
                MEpoch epoch(MVEpoch(mjd), tp_in);
                MEpoch result = converter(epoch);
                if (result.getValue().get() < -1e30) {
                    *elapsed_ns = 0;
                    return -1;
                }
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        *elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_doppler_convert(
    double value_in, const char* ref_in, const char* ref_out,
    double* value_out)
{
    try {
        auto tp_in = parse_doppler_ref(ref_in);
        auto tp_out = parse_doppler_ref(ref_out);
        MDoppler doppler(MVDoppler(value_in), tp_in);
        MDoppler result = MDoppler::Convert(doppler, tp_out)();
        *value_out = result.getValue().getValue();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_bench_doppler_convert(
    double value_start, int count,
    const char* ref_in, const char* ref_out,
    int iterations, uint64_t* elapsed_ns)
{
    try {
        auto tp_in = parse_doppler_ref(ref_in);
        auto tp_out = parse_doppler_ref(ref_out);

        // Create a reusable converter to avoid per-call engine setup overhead
        MDoppler seed(MVDoppler(value_start), tp_in);
        MDoppler::Convert converter(seed, tp_out);

        for (int i = 0; i < 10; i++) {
            MDoppler d(MVDoppler(value_start + i * 0.01), tp_in);
            converter(d);
        }
        auto start = std::chrono::high_resolution_clock::now();
        for (int iter = 0; iter < iterations; iter++) {
            for (int i = 0; i < count; i++) {
                double v = value_start + i * 0.001;
                MDoppler d(MVDoppler(v), tp_in);
                MDoppler result = converter(d);
                if (result.getValue().getValue() < -1e30) {
                    *elapsed_ns = 0; return -1;
                }
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        *elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_direction_convert(
    double lon_in, double lat_in,
    const char* ref_in, const char* ref_out,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    double* lon_out, double* lat_out)
{
    try {
        auto tp_in = parse_direction_ref(ref_in);
        auto tp_out = parse_direction_ref(ref_out);

        MDirection dir(MVDirection(Quantity(lon_in, "rad"), Quantity(lat_in, "rad")), tp_in);

        MeasFrame frame;
        if (epoch_mjd != 0.0) {
            MEpoch epoch(MVEpoch(epoch_mjd), MEpoch::UTC);
            frame.set(epoch);
        }
        if (obs_lon != 0.0 || obs_lat != 0.0 || obs_h != 0.0) {
            MPosition obs(MVPosition(Quantity(obs_h, "m"),
                                      Quantity(obs_lon, "rad"),
                                      Quantity(obs_lat, "rad")),
                          MPosition::WGS84);
            frame.set(obs);
        }

        MDirection::Ref ref_target(tp_out, frame);
        MDirection result = MDirection::Convert(dir, ref_target)();

        *lon_out = result.getValue().getLong();
        *lat_out = result.getValue().getLat();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_bench_direction_convert(
    double lon_start, double lat, int count,
    const char* ref_in, const char* ref_out,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    int iterations, uint64_t* elapsed_ns)
{
    try {
        auto tp_in = parse_direction_ref(ref_in);
        auto tp_out = parse_direction_ref(ref_out);

        MeasFrame frame;
        if (epoch_mjd != 0.0) {
            MEpoch epoch(MVEpoch(epoch_mjd), MEpoch::UTC);
            frame.set(epoch);
        }
        if (obs_lon != 0.0 || obs_lat != 0.0 || obs_h != 0.0) {
            MPosition obs(MVPosition(Quantity(obs_h, "m"),
                                      Quantity(obs_lon, "rad"),
                                      Quantity(obs_lat, "rad")),
                          MPosition::WGS84);
            frame.set(obs);
        }

        MDirection::Ref ref_target(tp_out, frame);

        // Create a reusable converter to avoid per-call engine setup overhead
        MDirection seed(MVDirection(Quantity(lon_start, "rad"),
                                     Quantity(lat, "rad")), tp_in);
        MDirection::Convert converter(seed, ref_target);

        for (int i = 0; i < 10; i++) {
            MDirection d(MVDirection(Quantity(lon_start + i * 0.01, "rad"),
                                     Quantity(lat, "rad")), tp_in);
            converter(d);
        }

        auto start = std::chrono::high_resolution_clock::now();
        for (int iter = 0; iter < iterations; iter++) {
            for (int i = 0; i < count; i++) {
                double lon = lon_start + i * 0.001;
                MDirection d(MVDirection(Quantity(lon, "rad"),
                                         Quantity(lat, "rad")), tp_in);
                MDirection result = converter(d);
                if (result.getValue().getLat() < -1e30) {
                    *elapsed_ns = 0; return -1;
                }
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        *elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_frequency_convert(
    double freq_hz, const char* ref_in, const char* ref_out,
    double dir_lon, double dir_lat, const char* dir_ref,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    double* freq_out)
{
    try {
        auto tp_in = parse_frequency_ref(ref_in);
        auto tp_out = parse_frequency_ref(ref_out);

        MFrequency freq(MVFrequency(Quantity(freq_hz, "Hz")), tp_in);

        MeasFrame frame;
        if (epoch_mjd != 0.0) {
            MEpoch epoch(MVEpoch(epoch_mjd), MEpoch::UTC);
            frame.set(epoch);
        }
        if (obs_lon != 0.0 || obs_lat != 0.0 || obs_h != 0.0) {
            MPosition obs(MVPosition(Quantity(obs_h, "m"),
                                      Quantity(obs_lon, "rad"),
                                      Quantity(obs_lat, "rad")),
                          MPosition::WGS84);
            frame.set(obs);
        }
        if (strlen(dir_ref) > 0) {
            auto tp_dir = parse_direction_ref(dir_ref);
            MDirection dir(MVDirection(Quantity(dir_lon, "rad"),
                                       Quantity(dir_lat, "rad")), tp_dir);
            frame.set(dir);
        }

        MFrequency::Ref ref_target(tp_out, frame);
        MFrequency result = MFrequency::Convert(freq, ref_target)();

        *freq_out = result.getValue().getValue();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_bench_frequency_convert(
    double freq_start, int count,
    const char* ref_in, const char* ref_out,
    double dir_lon, double dir_lat, const char* dir_ref,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    int iterations, uint64_t* elapsed_ns)
{
    try {
        auto tp_in = parse_frequency_ref(ref_in);
        auto tp_out = parse_frequency_ref(ref_out);

        MeasFrame frame;
        if (epoch_mjd != 0.0) {
            MEpoch epoch(MVEpoch(epoch_mjd), MEpoch::UTC);
            frame.set(epoch);
        }
        if (obs_lon != 0.0 || obs_lat != 0.0 || obs_h != 0.0) {
            MPosition obs(MVPosition(Quantity(obs_h, "m"),
                                      Quantity(obs_lon, "rad"),
                                      Quantity(obs_lat, "rad")),
                          MPosition::WGS84);
            frame.set(obs);
        }
        if (strlen(dir_ref) > 0) {
            auto tp_dir = parse_direction_ref(dir_ref);
            MDirection dir(MVDirection(Quantity(dir_lon, "rad"),
                                       Quantity(dir_lat, "rad")), tp_dir);
            frame.set(dir);
        }

        MFrequency::Ref ref_target(tp_out, frame);

        // Create a reusable converter to avoid per-call engine setup overhead
        MFrequency seed(MVFrequency(Quantity(freq_start, "Hz")), tp_in);
        MFrequency::Convert converter(seed, ref_target);

        for (int i = 0; i < 10; i++) {
            MFrequency f(MVFrequency(Quantity(freq_start + i * 1e6, "Hz")), tp_in);
            converter(f);
        }

        auto start = std::chrono::high_resolution_clock::now();
        for (int iter = 0; iter < iterations; iter++) {
            for (int i = 0; i < count; i++) {
                double hz = freq_start + i * 1e3;
                MFrequency f(MVFrequency(Quantity(hz, "Hz")), tp_in);
                MFrequency result = converter(f);
                if (result.getValue().getValue() < -1e30) {
                    *elapsed_ns = 0; return -1;
                }
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        *elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        return 0;
    } catch (...) {
        return -1;
    }
}

static MRadialVelocity::Types parse_radvel_ref(const char* ref_str) {
    MRadialVelocity::Types tp;
    if (!MRadialVelocity::getType(tp, String(ref_str))) {
        throw std::runtime_error(String("Unknown radvel ref: ") + ref_str);
    }
    return tp;
}

int measures_shim_radvel_convert(
    double ms_in, const char* ref_in, const char* ref_out,
    double dir_lon, double dir_lat, const char* dir_ref,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    double* ms_out)
{
    try {
        auto tp_in = parse_radvel_ref(ref_in);
        auto tp_out = parse_radvel_ref(ref_out);

        MRadialVelocity rv(MVRadialVelocity(Quantity(ms_in, "m/s")), tp_in);

        MeasFrame frame;
        if (epoch_mjd != 0.0) {
            MEpoch epoch(MVEpoch(epoch_mjd), MEpoch::UTC);
            frame.set(epoch);
        }
        if (obs_lon != 0.0 || obs_lat != 0.0 || obs_h != 0.0) {
            MPosition obs(MVPosition(Quantity(obs_h, "m"),
                                      Quantity(obs_lon, "rad"),
                                      Quantity(obs_lat, "rad")),
                          MPosition::WGS84);
            frame.set(obs);
        }
        if (strlen(dir_ref) > 0) {
            auto tp_dir = parse_direction_ref(dir_ref);
            MDirection dir(MVDirection(Quantity(dir_lon, "rad"),
                                       Quantity(dir_lat, "rad")), tp_dir);
            frame.set(dir);
        }

        MRadialVelocity::Ref ref_target(tp_out, frame);
        MRadialVelocity result = MRadialVelocity::Convert(rv, ref_target)();

        *ms_out = result.getValue().getValue();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_bench_radvel_convert(
    double ms_start, int count,
    const char* ref_in, const char* ref_out,
    double dir_lon, double dir_lat, const char* dir_ref,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    int iterations, uint64_t* elapsed_ns)
{
    try {
        auto tp_in = parse_radvel_ref(ref_in);
        auto tp_out = parse_radvel_ref(ref_out);

        MeasFrame frame;
        if (epoch_mjd != 0.0) {
            MEpoch epoch(MVEpoch(epoch_mjd), MEpoch::UTC);
            frame.set(epoch);
        }
        if (obs_lon != 0.0 || obs_lat != 0.0 || obs_h != 0.0) {
            MPosition obs(MVPosition(Quantity(obs_h, "m"),
                                      Quantity(obs_lon, "rad"),
                                      Quantity(obs_lat, "rad")),
                          MPosition::WGS84);
            frame.set(obs);
        }
        if (strlen(dir_ref) > 0) {
            auto tp_dir = parse_direction_ref(dir_ref);
            MDirection dir(MVDirection(Quantity(dir_lon, "rad"),
                                       Quantity(dir_lat, "rad")), tp_dir);
            frame.set(dir);
        }

        MRadialVelocity::Ref ref_target(tp_out, frame);

        MRadialVelocity seed(MVRadialVelocity(Quantity(ms_start, "m/s")), tp_in);
        MRadialVelocity::Convert converter(seed, ref_target);

        for (int i = 0; i < 10; i++) {
            MRadialVelocity r(MVRadialVelocity(Quantity(ms_start + i * 100.0, "m/s")), tp_in);
            converter(r);
        }

        auto start = std::chrono::high_resolution_clock::now();
        for (int iter = 0; iter < iterations; iter++) {
            for (int i = 0; i < count; i++) {
                double ms = ms_start + i * 10.0;
                MRadialVelocity r(MVRadialVelocity(Quantity(ms, "m/s")), tp_in);
                MRadialVelocity result = converter(r);
                if (result.getValue().getValue() < -1e30) {
                    *elapsed_ns = 0; return -1;
                }
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        *elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_frequency_convert_with_rv(
    double freq_hz, const char* ref_in, const char* ref_out,
    double dir_lon, double dir_lat, const char* dir_ref,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    double rv_ms, const char* rv_ref,
    double* freq_out)
{
    try {
        auto tp_in = parse_frequency_ref(ref_in);
        auto tp_out = parse_frequency_ref(ref_out);

        MFrequency freq(MVFrequency(Quantity(freq_hz, "Hz")), tp_in);

        MeasFrame frame;
        if (epoch_mjd != 0.0) {
            MEpoch epoch(MVEpoch(epoch_mjd), MEpoch::UTC);
            frame.set(epoch);
        }
        if (obs_lon != 0.0 || obs_lat != 0.0 || obs_h != 0.0) {
            MPosition obs(MVPosition(Quantity(obs_h, "m"),
                                      Quantity(obs_lon, "rad"),
                                      Quantity(obs_lat, "rad")),
                          MPosition::WGS84);
            frame.set(obs);
        }
        if (strlen(dir_ref) > 0) {
            auto tp_dir = parse_direction_ref(dir_ref);
            MDirection dir(MVDirection(Quantity(dir_lon, "rad"),
                                       Quantity(dir_lat, "rad")), tp_dir);
            frame.set(dir);
        }
        if (strlen(rv_ref) > 0) {
            auto tp_rv = parse_radvel_ref(rv_ref);
            MRadialVelocity rv(MVRadialVelocity(Quantity(rv_ms, "m/s")), tp_rv);
            frame.set(rv);
        }

        MFrequency::Ref ref_target(tp_out, frame);
        MFrequency result = MFrequency::Convert(freq, ref_target)();

        *freq_out = result.getValue().getValue();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_epoch_convert_with_frame(
    double mjd_in, const char* ref_in, const char* ref_out,
    double obs_lon, double obs_lat, double obs_h,
    double dut1,
    double* mjd_out)
{
    try {
        (void)dut1; // Rust passes dut1 for its own frame; C++ uses IERS tables internally
        auto tp_in = parse_epoch_ref(ref_in);
        auto tp_out = parse_epoch_ref(ref_out);

        MEpoch epoch(MVEpoch(mjd_in), tp_in);

        MeasFrame frame;
        if (obs_lon != 0.0 || obs_lat != 0.0 || obs_h != 0.0) {
            MPosition obs(MVPosition(Quantity(obs_h, "m"),
                                      Quantity(obs_lon, "rad"),
                                      Quantity(obs_lat, "rad")),
                          MPosition::WGS84);
            frame.set(obs);
        }

        MEpoch::Ref ref_target(tp_out, frame);
        MEpoch result = MEpoch::Convert(epoch, ref_target)();

        *mjd_out = result.getValue().get();
        return 0;
    } catch (...) {
        return -1;
    }
}

// EOP data query: get dUT1 and polar motion from C++ casacore's IERS tables.
// This allows comparing the Rust bundled EOP data against C++ casacore's data.
int measures_shim_eop_query(
    double mjd,
    double* dut1_out,
    double* xp_arcsec_out,
    double* yp_arcsec_out)
{
    try {
        *dut1_out = MeasTable::dUT1(mjd);

        Euler pm = MeasTable::polarMotion(mjd);
        // polarMotion returns (-x, -y, 0) Euler angles in radians; negate and convert to arcsec
        const double RAD_TO_ARCSEC = 180.0 * 3600.0 / M_PI;
        *xp_arcsec_out = -pm(0) * RAD_TO_ARCSEC;
        *yp_arcsec_out = -pm(1) * RAD_TO_ARCSEC;

        return 0;
    } catch (...) {
        return -1;
    }
}

// Diagnostic: apply frame bias + precession sequentially (matching MeasMath::applyPrecession)
// and return the resulting direction cosines.
int measures_shim_iau2000_precession_matrix(
    double epoch_mjd_tt,
    double* mat_out)  // 9 doubles: [0..2] = bias*v, [3..5] = prec*bias*v, [6..8] = bias matrix diag
{
    try {
        uInt reg_use = AipsrcValue<Bool>::registerRC(
            "measures.iau2000.b_use", False);
        uInt reg_2000a = AipsrcValue<Bool>::registerRC(
            "measures.iau2000.b_use2000a", False);
        AipsrcValue<Bool>::set(reg_use, True);
        AipsrcValue<Bool>::set(reg_2000a, True);

        // Test vector: (lon=1.0, lat=0.5) direction cosines
        MVDirection v(Quantity(1.0, "rad"), Quantity(0.5, "rad"));

        // Step 1: apply frame bias (same as in *= frameBias00())
        MVPosition v1(v);
        v1 *= MeasTable::frameBias00();
        mat_out[0] = v1.getLong();
        mat_out[1] = v1.getLat();
        mat_out[2] = 0;

        // Step 2: apply precession
        Precession prec(Precession::IAU2000);
        MVPosition v2(v1);
        v2 *= RotMatrix(prec(epoch_mjd_tt));
        mat_out[3] = v2.getLong();
        mat_out[4] = v2.getLat();
        mat_out[5] = 0;

        // Also output the precession Euler angles
        const Euler& e = prec(epoch_mjd_tt);
        mat_out[6] = e(0);  // zeta
        mat_out[7] = e(1);  // theta
        mat_out[8] = e(2);  // z

        AipsrcValue<Bool>::set(reg_use, False);
        AipsrcValue<Bool>::set(reg_2000a, False);
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_direction_convert_iau2000a(
    double lon_in, double lat_in,
    const char* ref_in, const char* ref_out,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    double* lon_out, double* lat_out)
{
    try {
        // Enable IAU 2000A mode via global Aipsrc switches.
        // Force static initialization of the registration IDs inside
        // MeasTable by calling the query functions first, then set True.
        // This ensures the static const uInt inside MeasTable::useIAU2000()
        // is initialized before we call set(), so registerRC won't re-register.
        (void)MeasTable::useIAU2000();
        (void)MeasTable::useIAU2000A();
        uInt reg_use = AipsrcValue<Bool>::registerRC(
            "measures.iau2000.b_use", False);
        uInt reg_2000a = AipsrcValue<Bool>::registerRC(
            "measures.iau2000.b_use2000a", False);
        AipsrcValue<Bool>::set(reg_use, True);
        AipsrcValue<Bool>::set(reg_2000a, True);

        // Verify the flags are actually set
        if (!MeasTable::useIAU2000() || !MeasTable::useIAU2000A()) {
            fprintf(stderr, "IAU2000A: FAILED to set flags! use=%d use2000a=%d\n",
                    (int)MeasTable::useIAU2000(), (int)MeasTable::useIAU2000A());
            return -2;
        }

        auto tp_in = parse_direction_ref(ref_in);
        auto tp_out = parse_direction_ref(ref_out);

        MDirection dir(MVDirection(Quantity(lon_in, "rad"),
                                    Quantity(lat_in, "rad")), tp_in);

        MeasFrame frame;
        if (epoch_mjd != 0.0) {
            MEpoch epoch(MVEpoch(epoch_mjd), MEpoch::UTC);
            frame.set(epoch);
        }
        if (obs_lon != 0.0 || obs_lat != 0.0 || obs_h != 0.0) {
            MPosition obs(MVPosition(Quantity(obs_h, "m"),
                                      Quantity(obs_lon, "rad"),
                                      Quantity(obs_lat, "rad")),
                          MPosition::WGS84);
            frame.set(obs);
        }

        MDirection::Ref ref_target(tp_out, frame);
        MDirection result = MDirection::Convert(dir, ref_target)();

        *lon_out = result.getValue().getLong();
        *lat_out = result.getValue().getLat();

        // Reset back to default IAU 1976/1980
        AipsrcValue<Bool>::set(reg_use, False);
        AipsrcValue<Bool>::set(reg_2000a, False);

        return 0;
    } catch (...) {
        return -1;
    }
}

// Diagnostic: get the Earth aberration velocity used by C++ casacore,
// and also the Sun position (for gravitational deflection).
// Returns velocity in units of c (same as epv00 divided by c_au_day).
int measures_shim_earth_velocity(
    double epoch_mjd_tdb,
    double* vx, double* vy, double* vz,
    double* sun_x, double* sun_y, double* sun_z)
{
    try {
        // Get aberration velocity from Stumpff series
        Aberration aber(Aberration::STANDARD);
        MVPosition vel = aber(epoch_mjd_tdb);
        *vx = vel(0);
        *vy = vel(1);
        *vz = vel(2);

        // Get Sun position
        SolarPos sp(SolarPos::STANDARD);
        MVPosition sun = sp(epoch_mjd_tdb);
        *sun_x = sun(0);
        *sun_y = sun(1);
        *sun_z = sun(2);

        return 0;
    } catch (...) {
        return -1;
    }
}

// ─── Table Measures shims ───────────────────────────────────────────────────

} // extern "C"

// These require table headers.
#include <casacore/tables/Tables/Table.h>
#include <casacore/tables/Tables/TableDesc.h>
#include <casacore/tables/Tables/SetupNewTab.h>
#include <casacore/tables/Tables/ScaColDesc.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/ScalarColumn.h>
#include <casacore/tables/Tables/ArrayColumn.h>
#include <casacore/measures/TableMeasures/TableMeasDesc.h>
#include <casacore/measures/TableMeasures/TableMeasRefDesc.h>
#include <casacore/measures/TableMeasures/TableMeasValueDesc.h>
#include <casacore/measures/TableMeasures/ScalarMeasColumn.h>
#include <casacore/measures/TableMeasures/ArrayMeasColumn.h>

extern "C" {

// Create a table with a fixed-ref MEpoch column "TIME" (3 rows).
int table_meas_create_epoch_fixed(const char* path_str)
{
    try {
        String path(path_str);
        TableDesc td("", "", TableDesc::Scratch);
        td.addColumn(ArrayColumnDesc<Double>("TIME", IPosition(1, 1), ColumnDesc::Direct));

        SetupNewTable snt(path, td, Table::New);
        Table tab(snt, 3);

        // Attach MEASINFO
        TableMeasRefDesc tmrd(MEpoch::UTC);
        TableMeasValueDesc tmvd(td, "TIME");
        TableMeasDesc<MEpoch> tmd(tmvd, tmrd);
        tmd.write(tab);

        ArrayColumn<Double> col(tab, "TIME");
        Vector<Double> v(1);
        v(0) = 51544.5; col.put(0, v);
        v(0) = 51545.0; col.put(1, v);
        v(0) = 51546.5; col.put(2, v);

        return 0;
    } catch (std::exception& e) {
        fprintf(stderr, "table_meas_create_epoch_fixed: %s\n", e.what());
        return -1;
    }
}

// Create a table with variable Int ref for MEpoch (3 rows).
int table_meas_create_epoch_var_int(const char* path_str)
{
    try {
        String path(path_str);
        TableDesc td("", "", TableDesc::Scratch);
        td.addColumn(ArrayColumnDesc<Double>("TIME", IPosition(1, 1), ColumnDesc::Direct));
        td.addColumn(ScalarColumnDesc<Int>("TimeRef"));

        SetupNewTable snt(path, td, Table::New);
        Table tab(snt, 3);

        TableMeasRefDesc tmrd(td, "TimeRef");
        TableMeasValueDesc tmvd(td, "TIME");
        TableMeasDesc<MEpoch> tmd(tmvd, tmrd);
        tmd.write(tab);

        ArrayColumn<Double> col(tab, "TIME");
        ScalarColumn<Int> rcol(tab, "TimeRef");

        Vector<Double> v(1);
        v(0) = 51544.5; col.put(0, v); rcol.put(0, (Int)MEpoch::UTC);
        v(0) = 51545.0; col.put(1, v); rcol.put(1, (Int)MEpoch::TAI);
        v(0) = 51546.5; col.put(2, v); rcol.put(2, (Int)MEpoch::TDT);  // TT

        return 0;
    } catch (std::exception& e) {
        fprintf(stderr, "table_meas_create_epoch_var_int: %s\n", e.what());
        return -1;
    }
}

// Create a table with variable String ref for MEpoch (3 rows).
int table_meas_create_epoch_var_str(const char* path_str)
{
    try {
        String path(path_str);
        TableDesc td("", "", TableDesc::Scratch);
        td.addColumn(ArrayColumnDesc<Double>("TIME", IPosition(1, 1), ColumnDesc::Direct));
        td.addColumn(ScalarColumnDesc<String>("TimeRefStr"));

        SetupNewTable snt(path, td, Table::New);
        Table tab(snt, 3);

        TableMeasRefDesc tmrd(td, "TimeRefStr");
        TableMeasValueDesc tmvd(td, "TIME");
        TableMeasDesc<MEpoch> tmd(tmvd, tmrd);
        tmd.write(tab);

        ArrayColumn<Double> col(tab, "TIME");
        ScalarColumn<String> rcol(tab, "TimeRefStr");

        Vector<Double> v(1);
        v(0) = 51544.5; col.put(0, v); rcol.put(0, "UTC");
        v(0) = 51545.0; col.put(1, v); rcol.put(1, "TAI");
        v(0) = 51546.5; col.put(2, v); rcol.put(2, "TDT");

        return 0;
    } catch (std::exception& e) {
        fprintf(stderr, "table_meas_create_epoch_var_str: %s\n", e.what());
        return -1;
    }
}

// Create a table with fixed-ref MDirection column "DIR" (3 rows, J2000).
int table_meas_create_direction_fixed(const char* path_str)
{
    try {
        String path(path_str);
        TableDesc td("", "", TableDesc::Scratch);
        td.addColumn(ArrayColumnDesc<Double>("DIR", IPosition(1, 2), ColumnDesc::Direct));

        SetupNewTable snt(path, td, Table::New);
        Table tab(snt, 3);

        TableMeasRefDesc tmrd(MDirection::J2000);
        TableMeasValueDesc tmvd(td, "DIR");
        TableMeasDesc<MDirection> tmd(tmvd, tmrd);
        tmd.write(tab);

        ArrayColumn<Double> col(tab, "DIR");
        Vector<Double> v(2);
        v(0) = 1.0; v(1) = 0.5; col.put(0, v);
        v(0) = 2.0; v(1) = -0.3; col.put(1, v);
        v(0) = 0.0; v(1) = 1.5; col.put(2, v);

        return 0;
    } catch (std::exception& e) {
        fprintf(stderr, "table_meas_create_direction_fixed: %s\n", e.what());
        return -1;
    }
}

// Read epochs from a table. Outputs: values[nrow], refs[nrow] (string buf).
// Uses ArrayMeasColumn since data columns are ArrayColumnDesc<Double>.
int table_meas_read_epochs(
    const char* path_str, const char* col_name,
    int nrow, double* values_out, char* refs_out, int ref_buf_len)
{
    try {
        String tpath(path_str);
        Table tab(tpath);
        String cname(col_name);
        ArrayMeasColumn<MEpoch> mcol(tab, cname);

        for (int i = 0; i < nrow; ++i) {
            Vector<MEpoch> mv = mcol(i);
            MEpoch m = mv(0);
            values_out[i] = m.getValue().get();  // MJD
            String ref_str = MEpoch::showType(m.getRef().getType());
            strncpy(refs_out + i * ref_buf_len, ref_str.c_str(), ref_buf_len - 1);
            refs_out[i * ref_buf_len + ref_buf_len - 1] = '\0';
        }

        return 0;
    } catch (std::exception& e) {
        fprintf(stderr, "table_meas_read_epochs: %s\n", e.what());
        return -1;
    }
}

// Read directions from a table. Outputs: values[nrow*2] (lon, lat rad).
// Uses ArrayMeasColumn since data columns are ArrayColumnDesc<Double>.
int table_meas_read_directions(
    const char* path_str, const char* col_name,
    int nrow, double* values_out, char* refs_out, int ref_buf_len)
{
    try {
        String tpath(path_str);
        Table tab(tpath);
        String cname(col_name);
        ArrayMeasColumn<MDirection> mcol(tab, cname);

        for (int i = 0; i < nrow; ++i) {
            Vector<MDirection> mv = mcol(i);
            MDirection m = mv(0);
            MVDirection mvd = m.getValue();
            // getLong/getLat return radians
            values_out[i * 2] = mvd.getLong();
            values_out[i * 2 + 1] = mvd.getLat();
            String ref_str = MDirection::showType(m.getRef().getType());
            strncpy(refs_out + i * ref_buf_len, ref_str.c_str(), ref_buf_len - 1);
            refs_out[i * ref_buf_len + ref_buf_len - 1] = '\0';
        }

        return 0;
    } catch (std::exception& e) {
        fprintf(stderr, "table_meas_read_directions: %s\n", e.what());
        return -1;
    }
}

// Verify a table written by Rust: read with C++ ArrayMeasColumn and check values.
// Returns 0 on success, -1 on failure.
int table_meas_verify_epochs(
    const char* path_str, const char* col_name,
    int nrow, const double* expected_mjds, const char* expected_refs, int ref_buf_len)
{
    try {
        String tpath(path_str);
        Table tab(tpath);
        String cname(col_name);
        ArrayMeasColumn<MEpoch> mcol(tab, cname);

        for (int i = 0; i < nrow; ++i) {
            Vector<MEpoch> mv = mcol(i);
            MEpoch m = mv(0);
            double mjd = m.getValue().get();
            if (std::abs(mjd - expected_mjds[i]) > 1e-9) {
                fprintf(stderr, "row %d: expected MJD %f, got %f\n", i, expected_mjds[i], mjd);
                return -1;
            }
            String ref_str = MEpoch::showType(m.getRef().getType());
            String expected(expected_refs + i * ref_buf_len);
            if (ref_str != expected) {
                fprintf(stderr, "row %d: expected ref '%s', got '%s'\n",
                    i, expected.c_str(), ref_str.c_str());
                return -1;
            }
        }
        return 0;
    } catch (std::exception& e) {
        fprintf(stderr, "table_meas_verify_epochs: %s\n", e.what());
        return -1;
    }
}

int table_meas_verify_directions(
    const char* path_str, const char* col_name,
    int nrow, const double* expected_vals, const char* expected_refs, int ref_buf_len)
{
    try {
        String tpath(path_str);
        Table tab(tpath);
        String cname(col_name);
        ArrayMeasColumn<MDirection> mcol(tab, cname);

        for (int i = 0; i < nrow; ++i) {
            Vector<MDirection> mvec = mcol(i);
            MDirection m = mvec(0);
            MVDirection mv = m.getValue();
            double lon = mv.getLong();
            double lat = mv.getLat();
            if (std::abs(lon - expected_vals[i*2]) > 1e-9 ||
                std::abs(lat - expected_vals[i*2+1]) > 1e-9) {
                fprintf(stderr, "row %d: expected (%f,%f), got (%f,%f)\n",
                    i, expected_vals[i*2], expected_vals[i*2+1], lon, lat);
                return -1;
            }
            String ref_str = MDirection::showType(m.getRef().getType());
            String expected(expected_refs + i * ref_buf_len);
            if (ref_str != expected) {
                fprintf(stderr, "row %d: expected ref '%s', got '%s'\n",
                    i, expected.c_str(), ref_str.c_str());
                return -1;
            }
        }
        return 0;
    } catch (std::exception& e) {
        fprintf(stderr, "table_meas_verify_directions: %s\n", e.what());
        return -1;
    }
}

// ─── Bench: read N rows of epoch measure column ────────────────────────────
int table_meas_bench_epoch_read(
    const char* path_str, const char* col_name,
    int iterations, uint64_t* elapsed_ns_out)
{
    try {
        String tpath(path_str);
        String cname(col_name);
        Table tab(tpath);
        ArrayMeasColumn<MEpoch> mcol(tab, cname);
        unsigned nrow = tab.nrow();

        auto start = std::chrono::high_resolution_clock::now();
        for (int iter = 0; iter < iterations; ++iter) {
            for (unsigned r = 0; r < nrow; ++r) {
                Vector<MEpoch> mv = mcol(r);
                (void)mv(0).getValue().get();
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        *elapsed_ns_out = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        return 0;
    } catch (std::exception& e) {
        fprintf(stderr, "table_meas_bench_epoch_read: %s\n", e.what());
        return -1;
    }
}

// ─── Bench: read N rows of direction measure column ────────────────────────
int table_meas_bench_direction_read(
    const char* path_str, const char* col_name,
    int iterations, uint64_t* elapsed_ns_out)
{
    try {
        String tpath(path_str);
        String cname(col_name);
        Table tab(tpath);
        ArrayMeasColumn<MDirection> mcol(tab, cname);
        unsigned nrow = tab.nrow();

        auto start = std::chrono::high_resolution_clock::now();
        for (int iter = 0; iter < iterations; ++iter) {
            for (unsigned r = 0; r < nrow; ++r) {
                Vector<MDirection> mv = mcol(r);
                (void)mv(0).getValue().getLong();
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        *elapsed_ns_out = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        return 0;
    } catch (std::exception& e) {
        fprintf(stderr, "table_meas_bench_direction_read: %s\n", e.what());
        return -1;
    }
}

} // extern "C"
