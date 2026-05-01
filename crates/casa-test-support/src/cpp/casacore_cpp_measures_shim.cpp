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
#include <casacore/measures/Measures/MEarthMagnetic.h>
#include <casacore/measures/Measures/EarthMagneticMachine.h>
#include <casacore/measures/Measures/MCEarthMagnetic.h>
#include <casacore/measures/Measures/MFrequency.h>
#include <casacore/measures/Measures/MDoppler.h>
#include <casacore/measures/Measures/MRadialVelocity.h>
#include <casacore/measures/Measures/MBaseline.h>
#include <casacore/measures/Measures/Muvw.h>
#include <casacore/measures/Measures/MCBaseline.h>
#include <casacore/measures/Measures/MCuvw.h>
#include <casacore/measures/Measures/MCDirection.h>
#include <casacore/measures/Measures/MCFrequency.h>
#include <casacore/measures/Measures/MCDoppler.h>
#include <casacore/measures/Measures/MCRadialVelocity.h>
#include <casacore/casa/Quanta/MVDirection.h>
#include <casacore/casa/Quanta/MVBaseline.h>
#include <casacore/casa/Quanta/MVuvw.h>
#include <casacore/casa/Quanta/MVAngle.h>
#include <casacore/casa/Quanta/MVFrequency.h>
#include <casacore/casa/Quanta/MVDoppler.h>
#include <casacore/casa/Quanta/MVEpoch.h>
#include <casacore/casa/Quanta/MVTime.h>
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

static MBaseline::Types parse_baseline_ref(const char* ref_str) {
    MBaseline::Types tp;
    if (!MBaseline::getType(tp, String(ref_str))) {
        throw std::runtime_error(String("Unknown baseline ref: ") + ref_str);
    }
    return tp;
}

static MEarthMagnetic::Types parse_earthmag_ref(const char* ref_str) {
    MEarthMagnetic::Types tp;
    if (!MEarthMagnetic::getType(tp, String(ref_str))) {
        throw std::runtime_error(String("Unknown EarthMagnetic ref: ") + ref_str);
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

static MeasFrame build_frame(
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h)
{
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
    return frame;
}

static std::pair<String, double> parse_named_source(const char* source_name) {
    String name(source_name);
    String upper(name);
    upper.upcase();
    double h = 0.0;

    if (upper.substr(0,3) == "SUN") {
        String ext(upper.substr(3));
        name = "SUN";
        h = -0.833;
        if (!ext.empty()) {
            if (ext == "-C") {
                h = 0.0;
            } else if (ext == "-U") {
                h = -0.25;
            } else if (ext == "-L") {
                h = 0.25;
            } else if (ext == "-CR") {
                h = -0.583;
            } else if (ext == "-UR") {
                h = -0.833;
            } else if (ext == "-LR") {
                h = -0.333;
            } else if (ext == "-CT") {
                h = -6.0;
            } else if (ext == "-NT") {
                h = -12.0;
            } else if (ext == "-AT") {
                h = -15.0;
            } else if (ext == "-ST") {
                h = -18.0;
            } else {
                throw std::runtime_error("invalid SUN type");
            }
        }
    } else if (upper.substr(0,4) == "MOON") {
        String ext(upper.substr(4));
        name = "MOON";
        h = -0.833;
        if (!ext.empty()) {
            if (ext == "-C") {
                h = 0.0;
            } else if (ext == "-U") {
                h = -0.25;
            } else if (ext == "-L") {
                h = 0.25;
            } else if (ext == "-CR") {
                h = -0.583;
            } else if (ext == "-UR") {
                h = -0.833;
            } else if (ext == "-LR") {
                h = -0.333;
            } else {
                throw std::runtime_error("invalid MOON type");
            }
        }
    }

    return {name, h * M_PI / 180.0};
}

static int fill_riseset(
    double epoch_mjd,
    const MDirection& dir,
    double lat,
    double h,
    const MEpoch& off,
    const MPosition& pos,
    double* rise,
    double* set)
{
    MeasFrame frame;
    frame.set(MEpoch(Quantity(epoch_mjd, "d"), MEpoch::UTC));
    frame.set(pos);

    MDirection::Ref hadec_ref(MDirection::HADEC, frame);
    MDirection hd = MDirection::Convert(MDirection::HADEC, hadec_ref)(dir);
    double dec = hd.getValue().get()[1];
    double ct = (sin(h) - sin(dec) * sin(lat)) / (cos(dec) * cos(lat));
    if (ct >= 1.0) {
        return 1;
    }
    if (ct <= -1.0) {
        return -1;
    }

    ct = acos(ct);
    MDirection::Ref app_ref(MDirection::APP, frame);
    MDirection app = MDirection::Convert(MDirection::APP, app_ref)(dir);
    double normra = MVAngle(app.getValue().get()[0])(0).radian();
    MEpoch::Ref last_ref(MEpoch::LAST, frame, off);

    if (rise) {
        Quantity tq = MVTime(Quantity(normra - ct, "rad")).get();
        MEpoch tr = MEpoch::Convert(MEpoch(tq, last_ref), MEpoch::UTC)();
        *rise = tr.getValue().get();
    }
    if (set) {
        Quantity tq = MVTime(Quantity(normra + ct, "rad")).get();
        MEpoch ts = MEpoch::Convert(MEpoch(tq, last_ref), MEpoch::UTC)();
        *set = ts.getValue().get();
    }
    return 0;
}

static void calc_riseset(
    const MDirection& dir,
    const MPosition& pos,
    const MEpoch& epoch,
    double h,
    double& rise,
    double& set)
{
    double lat = pos.getValue().get()[2];
    double start = floor(epoch.getValue().get() + 0.000001);
    MEpoch off = MEpoch(Quantity(start, "d"), MEpoch::Types(MEpoch::UTC | MEpoch::RAZE));
    int ab = fill_riseset(start + 0.5, dir, lat, h, off, pos, &rise, &set);
    if (ab > 0) {
        set = start;
        rise = set + 1.0;
    } else if (ab < 0) {
        rise = start;
        set = rise + 1.0;
    } else {
        if (rise < start) rise += 1.0 - 236.0 / 86400.0;
        if (set < start) set += 1.0 - 236.0 / 86400.0;
        if (set < rise) set += 1.0;
        for (int i = 0; i < 2; ++i) {
            fill_riseset(rise, dir, lat, h, off, pos, &rise, nullptr);
            if (rise < start) rise += 1.0 - 236.0 / 86400.0;
            fill_riseset(set, dir, lat, h, off, pos, nullptr, &set);
            if (set < start) set += 1.0 - 236.0 / 86400.0;
            if (set < rise) set += 1.0;
        }
    }
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

int measures_shim_position_to_wgs_xyz(
    double v0, double v1, double v2,
    const char* ref_in,
    double* out0, double* out1, double* out2)
{
    try {
        auto tp_in = parse_position_ref(ref_in);

        MVPosition mv;
        if (tp_in == MPosition::ITRF) {
            mv = MVPosition(v0, v1, v2);
        } else {
            mv = MVPosition(Quantity(v2, "m"), Quantity(v0, "rad"), Quantity(v1, "rad"));
        }

        MPosition pos(mv, tp_in);
        MPosition result = MPosition::Convert(pos, MPosition::WGS84)();
        Vector<Double> xyz = result.getValue().getValue();
        *out0 = xyz(0);
        *out1 = xyz(1);
        *out2 = xyz(2);
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
        MeasFrame frame = build_frame(epoch_mjd, obs_lon, obs_lat, obs_h);

        MDirection::Ref ref_target(tp_out, frame);
        MDirection result = MDirection::Convert(dir, ref_target)();

        *lon_out = result.getValue().getLong();
        *lat_out = result.getValue().getLat();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_named_direction_convert(
    const char* source_name, const char* ref_out,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    double* lon_out, double* lat_out)
{
    try {
        auto tp_out = parse_direction_ref(ref_out);
        auto [base_name, _h] = parse_named_source(source_name);
        MDirection dir = MDirection::makeMDirection(base_name);
        MeasFrame frame = build_frame(epoch_mjd, obs_lon, obs_lat, obs_h);
        MDirection::Ref ref_target(tp_out, frame);
        MDirection result = MDirection::Convert(dir, ref_target)();
        *lon_out = result.getValue().getLong();
        *lat_out = result.getValue().getLat();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_riseset(
    const char* source_name,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    double* rise_out, double* set_out)
{
    try {
        auto [base_name, h] = parse_named_source(source_name);
        MDirection dir = MDirection::makeMDirection(base_name);
        MPosition pos(MVPosition(Quantity(obs_h, "m"),
                                 Quantity(obs_lon, "rad"),
                                 Quantity(obs_lat, "rad")),
                      MPosition::WGS84);
        MEpoch epoch(MVEpoch(epoch_mjd), MEpoch::UTC);
        calc_riseset(dir, pos, epoch, h, *rise_out, *set_out);
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_line_frequency(
    const char* line_name,
    double* freq_out_hz)
{
    try {
        MFrequency line;
        if (!MeasTable::Line(line, String(line_name))) {
            return 1;
        }
        *freq_out_hz = line.getValue().getValue();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_earthmag_convert_xyz(
    double x_in, double y_in, double z_in,
    const char* ref_in, const char* ref_out,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    double* x_out, double* y_out, double* z_out)
{
    try {
        auto tp_in = parse_earthmag_ref(ref_in);
        auto tp_out = parse_earthmag_ref(ref_out);
        if (tp_in == MEarthMagnetic::IGRF || tp_out == MEarthMagnetic::IGRF) {
            throw std::runtime_error("IGRF cannot be used for explicit EarthMagnetic conversion");
        }

        MVEarthMagnetic mv(x_in, y_in, z_in);
        MEarthMagnetic field(mv, tp_in);
        MeasFrame frame = build_frame(epoch_mjd, obs_lon, obs_lat, obs_h);
        MEarthMagnetic::Ref ref_target(tp_out, frame);
        MEarthMagnetic result = MEarthMagnetic::Convert(field, ref_target)();
        Vector<Double> xyz = result.getValue().getValue();
        *x_out = xyz(0);
        *y_out = xyz(1);
        *z_out = xyz(2);
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_earthmag_convert_angles(
    double lon_in, double lat_in, double length_nt,
    const char* ref_in, const char* ref_out,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    double* lon_out, double* lat_out)
{
    try {
        auto tp_in = parse_earthmag_ref(ref_in);
        auto tp_out = parse_earthmag_ref(ref_out);

        MEarthMagnetic input(
            MVEarthMagnetic(
                Quantity(length_nt, "nT"),
                Quantity(lon_in, "rad"),
                Quantity(lat_in, "rad")),
            tp_in);
        MeasFrame frame = build_frame(epoch_mjd, obs_lon, obs_lat, obs_h);
        MEarthMagnetic::Ref ref_target(tp_out, frame);
        MEarthMagnetic result = MEarthMagnetic::Convert(input, ref_target)();
        Vector<double> vec = result.getValue().getAngle().getValue();
        *lon_out = vec[0];
        *lat_out = vec[1];
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_igrf_xyz(
    const char* ref_out,
    double height_m,
    double dir_lon, double dir_lat, const char* dir_ref,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    double* x_out, double* y_out, double* z_out)
{
    try {
        auto tp_out = parse_earthmag_ref(ref_out);
        if (tp_out == MEarthMagnetic::IGRF) {
            throw std::runtime_error("IGRF cannot be used as output reference");
        }
        auto dir_tp = parse_direction_ref(dir_ref);
        MeasFrame frame = build_frame(epoch_mjd, obs_lon, obs_lat, obs_h);
        MDirection::Ref dir_ref_in(dir_tp, frame);
        MDirection dir(MVDirection(Quantity(dir_lon, "rad"), Quantity(dir_lat, "rad")), dir_ref_in);
        EarthMagneticMachine machine(dir_ref_in, Quantum<Double>(height_m, "m"), frame);
        machine.calculate(dir.getValue());
        MEarthMagnetic field(machine.getField(), MEarthMagnetic::ITRF);
        MEarthMagnetic::Ref ref_target(tp_out, frame);
        MEarthMagnetic result = MEarthMagnetic::Convert(field, ref_target)();
        Vector<Double> xyz = result.getValue().getValue();
        *x_out = xyz(0);
        *y_out = xyz(1);
        *z_out = xyz(2);
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_igrf_los(
    double height_m,
    double dir_lon, double dir_lat, const char* dir_ref,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    double* value_out)
{
    try {
        auto dir_tp = parse_direction_ref(dir_ref);
        MeasFrame frame = build_frame(epoch_mjd, obs_lon, obs_lat, obs_h);
        MDirection::Ref dir_ref_in(dir_tp, frame);
        MDirection dir(MVDirection(Quantity(dir_lon, "rad"), Quantity(dir_lat, "rad")), dir_ref_in);
        EarthMagneticMachine machine(dir_ref_in, Quantum<Double>(height_m, "m"), frame);
        machine.calculate(dir.getValue());
        *value_out = machine.getLOSField();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_igrf_long(
    double height_m,
    double dir_lon, double dir_lat, const char* dir_ref,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    double* value_out)
{
    try {
        auto dir_tp = parse_direction_ref(dir_ref);
        MeasFrame frame = build_frame(epoch_mjd, obs_lon, obs_lat, obs_h);
        MDirection::Ref dir_ref_in(dir_tp, frame);
        MDirection dir(MVDirection(Quantity(dir_lon, "rad"), Quantity(dir_lat, "rad")), dir_ref_in);
        EarthMagneticMachine machine(dir_ref_in, Quantum<Double>(height_m, "m"), frame);
        machine.calculate(dir.getValue());
        *value_out = machine.getLong();
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

        MeasFrame frame = build_frame(epoch_mjd, obs_lon, obs_lat, obs_h);

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

int measures_shim_frequency_convert_via_model(
    double freq_hz, const char* ref_in, const char* ref_out,
    double dir_lon, double dir_lat, const char* dir_ref,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    double* freq_out)
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

        MFrequency::Convert converter;
        converter.setModel(MFrequency(MVFrequency(), MFrequency::Ref(tp_in, frame)));
        converter.setOut(tp_out);

        Vector<Double> freq_in_hz(1);
        freq_in_hz = freq_hz;
        MVFrequency freq_value;
        freq_value.putVector(freq_in_hz);
        MFrequency result = converter(freq_value);

        *freq_out = result.getValue().getValue();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_frequency_convert_via_mutated_model(
    double freq_hz, const char* ref_in, const char* ref_out,
    double dir_lon, double dir_lat, const char* dir_ref,
    double epoch_mjd, double obs_lon, double obs_lat, double obs_h,
    double* freq_out)
{
    try {
        auto tp_in = parse_frequency_ref(ref_in);
        auto tp_out = parse_frequency_ref(ref_out);
        auto tp_dir = parse_direction_ref(dir_ref);

        MeasFrame frame;
        frame.set(MDirection(MVDirection(), tp_dir));
        frame.set(MEpoch(MVEpoch(), MEpoch::UTC));
        if (obs_lon != 0.0 || obs_lat != 0.0 || obs_h != 0.0) {
            MPosition obs(MVPosition(Quantity(obs_h, "m"),
                                      Quantity(obs_lon, "rad"),
                                      Quantity(obs_lat, "rad")),
                          MPosition::WGS84);
            frame.set(obs);
        }

        MFrequency::Convert converter;
        converter.setModel(MFrequency(MVFrequency(), MFrequency::Ref(tp_in, frame)));

        if (epoch_mjd != 0.0) {
            MEpoch epoch(MVEpoch(epoch_mjd), MEpoch::UTC);
            frame.set(epoch);
        }
        if (strlen(dir_ref) > 0) {
            MDirection dir(MVDirection(Quantity(dir_lon, "rad"),
                                       Quantity(dir_lat, "rad")), tp_dir);
            frame.set(dir);
        }
        converter.setOut(tp_out);

        Vector<Double> freq_in_hz(1);
        freq_in_hz = freq_hz;
        MVFrequency freq_value;
        freq_value.putVector(freq_in_hz);
        MFrequency result = converter(freq_value);

        *freq_out = result.getValue().getValue();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_frequency_convert_between_frames(
    double freq_hz, const char* ref_in, const char* ref_out,
    double src_dir_lon, double src_dir_lat, const char* src_dir_ref,
    double src_epoch_mjd, double src_obs_lon, double src_obs_lat, double src_obs_h,
    double dst_dir_lon, double dst_dir_lat, const char* dst_dir_ref,
    double dst_epoch_mjd, double dst_obs_lon, double dst_obs_lat, double dst_obs_h,
    double* freq_out)
{
    try {
        auto tp_in = parse_frequency_ref(ref_in);
        auto tp_out = parse_frequency_ref(ref_out);

        MeasFrame source_frame;
        if (src_epoch_mjd != 0.0) {
            MEpoch epoch(MVEpoch(src_epoch_mjd), MEpoch::UTC);
            source_frame.set(epoch);
        }
        if (src_obs_lon != 0.0 || src_obs_lat != 0.0 || src_obs_h != 0.0) {
            MPosition obs(MVPosition(Quantity(src_obs_h, "m"),
                                     Quantity(src_obs_lon, "rad"),
                                     Quantity(src_obs_lat, "rad")),
                          MPosition::WGS84);
            source_frame.set(obs);
        }
        if (strlen(src_dir_ref) > 0) {
            auto tp_dir = parse_direction_ref(src_dir_ref);
            MDirection dir(MVDirection(Quantity(src_dir_lon, "rad"),
                                       Quantity(src_dir_lat, "rad")), tp_dir);
            source_frame.set(dir);
        }

        MeasFrame target_frame;
        if (dst_epoch_mjd != 0.0) {
            MEpoch epoch(MVEpoch(dst_epoch_mjd), MEpoch::UTC);
            target_frame.set(epoch);
        }
        if (dst_obs_lon != 0.0 || dst_obs_lat != 0.0 || dst_obs_h != 0.0) {
            MPosition obs(MVPosition(Quantity(dst_obs_h, "m"),
                                     Quantity(dst_obs_lon, "rad"),
                                     Quantity(dst_obs_lat, "rad")),
                          MPosition::WGS84);
            target_frame.set(obs);
        }
        if (strlen(dst_dir_ref) > 0) {
            auto tp_dir = parse_direction_ref(dst_dir_ref);
            MDirection dir(MVDirection(Quantity(dst_dir_lon, "rad"),
                                       Quantity(dst_dir_lat, "rad")), tp_dir);
            target_frame.set(dir);
        }

        MFrequency input(Quantity(freq_hz, "Hz"), MFrequency::Ref(tp_in, source_frame));
        MFrequency::Ref target_ref(tp_out, target_frame);
        MFrequency result = MFrequency::Convert(input, target_ref)();

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

int measures_shim_frequency_rest_with_doppler(
    double freq_hz, const char* ref_in,
    double doppler_value, const char* doppler_ref,
    double* freq_out)
{
    try {
        auto tp_in = parse_frequency_ref(ref_in);
        auto tp_doppler = parse_doppler_ref(doppler_ref);

        MFrequency freq(MVFrequency(Quantity(freq_hz, "Hz")), tp_in);
        MDoppler doppler(MVDoppler(doppler_value), tp_doppler);

        MFrequency result = freq.toRest(doppler);
        *freq_out = result.getValue().getValue();
        return 0;
    } catch (...) {
        return -1;
    }
}

int measures_shim_frequency_shift_with_doppler(
    double freq_hz, const char* ref_in,
    double doppler_value, const char* doppler_ref,
    double* freq_out)
{
    try {
        auto tp_in = parse_frequency_ref(ref_in);
        auto tp_doppler = parse_doppler_ref(doppler_ref);

        MFrequency freq(MVFrequency(Quantity(freq_hz, "Hz")), tp_in);
        MDoppler doppler(MVDoppler(doppler_value), tp_doppler);

        MFrequency result = MFrequency::fromDoppler(doppler, freq.getValue());
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

int measures_shim_simulator_baseline_uvw(
    double obs_x, double obs_y, double obs_z,
    double ant_x, double ant_y, double ant_z,
    double phase_ra, double phase_dec,
    double epoch_mjd,
    double* j2000_x_out, double* j2000_y_out, double* j2000_z_out,
    double* uvw_u_out, double* uvw_v_out, double* uvw_w_out)
{
    try {
        MPosition obsPos(MVPosition(obs_x, obs_y, obs_z), MPosition::ITRF);
        MEpoch epoch(MVEpoch(epoch_mjd), MEpoch::UT1);
        MDirection refdir(MVDirection(phase_ra, phase_dec), MDirection::J2000);
        MeasFrame measFrame(obsPos);
        measFrame.set(epoch);
        measFrame.set(refdir);

        MBaseline::Ref basref(MBaseline::ITRF, measFrame);
        MBaseline basMeas(MVBaseline(obsPos.getValue(), MVPosition(ant_x, ant_y, ant_z)), basref);
        basMeas.getRefPtr()->set(measFrame);
        MBaseline::Convert elconv(basMeas, MBaseline::Ref(MBaseline::J2000));
        MBaseline bas2000 = elconv(basMeas);
        const Vector<Double>& j2000 = bas2000.getValue().getValue();

        MVuvw uvw2000(bas2000.getValue(), refdir.getValue());
        const Vector<Double>& uvw = uvw2000.getValue();

        *j2000_x_out = j2000(0);
        *j2000_y_out = j2000(1);
        *j2000_z_out = j2000(2);
        *uvw_u_out = uvw(0);
        *uvw_v_out = uvw(1);
        *uvw_w_out = uvw(2);
        return 0;
    } catch (std::exception& e) {
        fprintf(stderr, "measures_shim_simulator_baseline_uvw: %s\n", e.what());
        return -1;
    }
}

int measures_shim_baseline_convert(
    double obs_x, double obs_y, double obs_z,
    double ant_x, double ant_y, double ant_z,
    double phase_ra, double phase_dec,
    double epoch_mjd,
    const char* ref_out,
    double* x_out, double* y_out, double* z_out)
{
    try {
        MPosition obsPos(MVPosition(obs_x, obs_y, obs_z), MPosition::ITRF);
        MEpoch epoch(MVEpoch(epoch_mjd), MEpoch::UT1);
        MDirection refdir(MVDirection(phase_ra, phase_dec), MDirection::J2000);
        MeasFrame measFrame(obsPos);
        measFrame.set(epoch);
        measFrame.set(refdir);

        MBaseline::Ref basref(MBaseline::ITRF, measFrame);
        MBaseline basMeas(MVBaseline(obsPos.getValue(), MVPosition(ant_x, ant_y, ant_z)), basref);
        basMeas.getRefPtr()->set(measFrame);

        MBaseline::Convert converter(basMeas, MBaseline::Ref(parse_baseline_ref(ref_out)));
        MBaseline converted = converter(basMeas);
        const Vector<Double>& xyz = converted.getValue().getValue();
        *x_out = xyz(0);
        *y_out = xyz(1);
        *z_out = xyz(2);
        return 0;
    } catch (std::exception& e) {
        fprintf(stderr, "measures_shim_baseline_convert: %s\n", e.what());
        return -1;
    }
}

int measures_shim_last_rad_for_itrf(
    double obs_x, double obs_y, double obs_z,
    double epoch_mjd,
    double* last_rad_out)
{
    try {
        MPosition obsPos(MVPosition(obs_x, obs_y, obs_z), MPosition::ITRF);
        MEpoch epoch(MVEpoch(epoch_mjd), MEpoch::UT1);
        MeasFrame measFrame(obsPos);
        measFrame.set(epoch);
        if (!measFrame.getLASTr(*last_rad_out)) {
            return -1;
        }
        return 0;
    } catch (std::exception& e) {
        fprintf(stderr, "measures_shim_last_rad_for_itrf: %s\n", e.what());
        return -1;
    }
}

} // extern "C"
