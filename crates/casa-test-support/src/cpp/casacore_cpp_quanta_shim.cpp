// SPDX-License-Identifier: LGPL-3.0-or-later
// C++ shim for cross-validating Rust quanta against C++ casacore.
//
// Exposes extern "C" functions that parse unit strings via the C++ UnitVal
// and return the resolved factor so the Rust test suite can compare.

#include <casacore/casa/Quanta/UnitVal.h>
#include <casacore/casa/Quanta/UnitMap.h>
#include <casacore/casa/Quanta/QC.h>
#include <casacore/casa/Quanta/MVAngle.h>
#include <casacore/casa/Quanta/MVTime.h>
#include <cstring>
#include <sstream>
#include <string>
#include <chrono>
#include <cstdint>

namespace {

// Dimension names in the order used by UnitDim operator<<.
// Index mapping: 0=m, 1=kg, 2=s, 3=A, 4=K, 5=cd, 6=mol, 7=rad, 8=sr, 9=_
const char* dim_names[] = {"m", "kg", "s", "A", "K", "cd", "mol", "rad", "sr", "_"};

// Extract the 10 dimension exponents from a UnitDim.
// UnitDim members are private, so we use the friend operator<< to serialize,
// then parse the output string.
//
// Format (from UnitDim.cc:147-160):
//   First non-zero dim prefixed by " ", subsequent by "."
//   Each token: dimName optionally followed by integer exponent (1 is omitted)
//   Example: " m.kg.s-2" = Length 1, Mass 1, Time -2
void extract_dims(const casacore::UnitDim& dim, int* out) {
    for (int i = 0; i < 10; ++i) out[i] = 0;
    std::ostringstream oss;
    oss << dim;
    std::string s = oss.str();
    if (s.empty()) return;

    // Skip leading space
    size_t pos = 0;
    if (pos < s.size() && s[pos] == ' ') ++pos;

    while (pos < s.size()) {
        // Find which dim name matches at current position
        int matched_idx = -1;
        size_t matched_len = 0;
        for (int i = 0; i < 10; ++i) {
            size_t dlen = std::strlen(dim_names[i]);
            if (s.compare(pos, dlen, dim_names[i]) == 0) {
                // Prefer longest match (e.g. "kg" over "k" if prefixes existed)
                if (dlen > matched_len) {
                    matched_idx = i;
                    matched_len = dlen;
                }
            }
        }
        if (matched_idx < 0) break;  // shouldn't happen with valid UnitDim output
        pos += matched_len;

        // Read optional exponent (possibly negative)
        int exp = 1;  // default exponent is 1 (omitted in output)
        if (pos < s.size() && (s[pos] == '-' || (s[pos] >= '0' && s[pos] <= '9'))) {
            bool neg = false;
            if (s[pos] == '-') { neg = true; ++pos; }
            int num = 0;
            while (pos < s.size() && s[pos] >= '0' && s[pos] <= '9') {
                num = num * 10 + (s[pos] - '0');
                ++pos;
            }
            exp = neg ? -num : num;
        }
        out[matched_idx] = exp;

        // Skip separator '.'
        if (pos < s.size() && s[pos] == '.') ++pos;
    }
}

} // anonymous namespace

extern "C" {

/// Initialise the C++ unit map (must be called once).
void quanta_shim_init() {
    casacore::UnitMap::putUser("__init__",
        casacore::UnitVal(1.0, casacore::UnitDim::Dnon), "init");
    casacore::UnitMap::removeUser("__init__");
}

/// Parse a unit string and return the factor.
/// Returns 0 on success, -1 on parse failure.
int quanta_shim_parse(const char* unit_str, double* factor_out) {
    casacore::UnitVal val;
    casacore::String name(unit_str);
    if (!casacore::UnitVal::check(name, val)) {
        return -1;
    }
    *factor_out = val.getFac();
    return 0;
}

/// Check if two unit strings are conformant in C++.
/// Uses UnitVal::operator== which compares dimensions only (factor-independent).
/// Returns 1 if conformant, 0 if not, -1 on parse error.
int quanta_shim_conformant(const char* unit_a, const char* unit_b) {
    casacore::UnitVal va, vb;
    casacore::String na(unit_a), nb(unit_b);
    if (!casacore::UnitVal::check(na, va) || !casacore::UnitVal::check(nb, vb)) {
        return -1;
    }
    // UnitVal::operator== compares dimensions (ignoring factor).
    return (va == vb) ? 1 : 0;
}

/// Return a QC constant's value and unit string.
int quanta_shim_qc_c(double* value_out, char* unit_buf, int buf_len) {
    auto q = casacore::QC::c();
    *value_out = q.getValue();
    std::strncpy(unit_buf, q.getUnit().c_str(), buf_len - 1);
    unit_buf[buf_len - 1] = '\0';
    return 0;
}

int quanta_shim_qc_h(double* value_out, char* unit_buf, int buf_len) {
    auto q = casacore::QC::h();
    *value_out = q.getValue();
    std::strncpy(unit_buf, q.getUnit().c_str(), buf_len - 1);
    unit_buf[buf_len - 1] = '\0';
    return 0;
}

int quanta_shim_qc_G(double* value_out, char* unit_buf, int buf_len) {
    auto q = casacore::QC::G();
    *value_out = q.getValue();
    std::strncpy(unit_buf, q.getUnit().c_str(), buf_len - 1);
    unit_buf[buf_len - 1] = '\0';
    return 0;
}

/// Parse a unit string, returning both the factor and 10 dimension exponents.
/// Returns 0 on success, -1 on parse failure.
int quanta_shim_parse_full(const char* unit_str, double* factor_out, int* dims_out) {
    casacore::UnitVal val;
    casacore::String name(unit_str);
    if (!casacore::UnitVal::check(name, val)) {
        return -1;
    }
    *factor_out = val.getFac();
    extract_dims(val.getDim(), dims_out);
    return 0;
}

/// Generic QC constant lookup by name.
/// Returns value, unit string, and dimension exponents.
/// Returns 0 on success, -1 if name is not recognized.
int quanta_shim_qc_constant(const char* name, double* value_out,
    char* unit_buf, int unit_buf_len, int* dims_out)
{
    // Map name to the appropriate QC function.
    casacore::Quantity q;
    std::string sname(name);

    if      (sname == "c")       q = casacore::QC::c();
    else if (sname == "G")       q = casacore::QC::G();
    else if (sname == "h")       q = casacore::QC::h();
    else if (sname == "HI")      q = casacore::QC::HI();
    else if (sname == "R")       q = casacore::QC::R();
    else if (sname == "NA")      q = casacore::QC::NA();
    else if (sname == "e")       q = casacore::QC::e();
    else if (sname == "mp")      q = casacore::QC::mp();
    else if (sname == "mp_me")   q = casacore::QC::mp_me();
    else if (sname == "mu0")     q = casacore::QC::mu0();
    else if (sname == "epsilon0") q = casacore::QC::epsilon0();
    else if (sname == "k")       q = casacore::QC::k();
    else if (sname == "F")       q = casacore::QC::F();
    else if (sname == "me")      q = casacore::QC::me();
    else if (sname == "re")      q = casacore::QC::re();
    else if (sname == "a0")      q = casacore::QC::a0();
    else if (sname == "R0")      q = casacore::QC::R0();
    else if (sname == "k2")      q = casacore::QC::k2();
    else if (sname == "qTurn")   q = casacore::QC::qTurn();
    else if (sname == "hTurn")   q = casacore::QC::hTurn();
    else if (sname == "fTurn")   q = casacore::QC::fTurn();
    else return -1;

    *value_out = q.getValue();
    std::strncpy(unit_buf, q.getUnit().c_str(), unit_buf_len - 1);
    unit_buf[unit_buf_len - 1] = '\0';

    // Parse the unit to get dimensions
    casacore::UnitVal uval;
    casacore::String uname(q.getUnit());
    if (casacore::UnitVal::check(uname, uval)) {
        extract_dims(uval.getDim(), dims_out);
    } else {
        for (int i = 0; i < 10; ++i) dims_out[i] = 0;
    }

    return 0;
}

/// Timed parse benchmark: parse count unit strings, iterations times each.
/// Returns total elapsed nanoseconds.
int quanta_shim_bench_parse(const char** unit_strs, int count,
    int iterations, uint64_t* elapsed_ns_out)
{
    // Warm up the unit map
    for (int i = 0; i < count; ++i) {
        casacore::UnitVal val;
        casacore::String name(unit_strs[i]);
        casacore::UnitVal::check(name, val);
    }

    auto start = std::chrono::high_resolution_clock::now();
    for (int iter = 0; iter < iterations; ++iter) {
        for (int i = 0; i < count; ++i) {
            casacore::UnitVal val;
            casacore::String name(unit_strs[i]);
            casacore::UnitVal::check(name, val);
            // Prevent optimization
            volatile double f = val.getFac();
            (void)f;
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    *elapsed_ns_out = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    return 0;
}

/// Timed conversion benchmark: convert value from->to, iterations times.
/// Returns total elapsed nanoseconds.
int quanta_shim_bench_convert(double value, const char* from_unit,
    const char* to_unit, int iterations, uint64_t* elapsed_ns_out)
{
    casacore::Quantity q(value, casacore::String(from_unit));
    casacore::Unit target(to_unit);

    // Warm up
    q.getValue(target);

    auto start = std::chrono::high_resolution_clock::now();
    for (int iter = 0; iter < iterations; ++iter) {
        volatile double v = q.getValue(target);
        (void)v;
    }
    auto end = std::chrono::high_resolution_clock::now();
    *elapsed_ns_out = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    return 0;
}

int quanta_shim_mvangle_format_angle(
    double radians, int second_decimals, char* out_buf, int out_buf_len)
{
    casacore::MVAngle angle(radians);
    auto rendered = angle.string(casacore::MVAngle::ANGLE, 6 + second_decimals);
    std::strncpy(out_buf, rendered.c_str(), out_buf_len - 1);
    out_buf[out_buf_len - 1] = '\0';
    return 0;
}

int quanta_shim_mvangle_format_angle_dig2(
    double radians, int second_decimals, char* out_buf, int out_buf_len)
{
    casacore::MVAngle angle(radians);
    auto rendered = angle.string(casacore::MVAngle::DIG2, 6 + second_decimals);
    std::strncpy(out_buf, rendered.c_str(), out_buf_len - 1);
    out_buf[out_buf_len - 1] = '\0';
    return 0;
}

int quanta_shim_mvangle_format_time(
    double radians, double lower_turns, int second_decimals, char* out_buf, int out_buf_len)
{
    casacore::MVAngle angle(radians);
    auto rendered = angle(lower_turns).string(casacore::MVAngle::TIME, 6 + second_decimals);
    std::strncpy(out_buf, rendered.c_str(), out_buf_len - 1);
    out_buf[out_buf_len - 1] = '\0';
    return 0;
}

int quanta_shim_mvtime_format_dmy(
    double mjd_days, int second_decimals, char* out_buf, int out_buf_len)
{
    casacore::MVTime time(mjd_days);
    auto rendered = time.string(casacore::MVTime::DMY, 6 + second_decimals);
    std::strncpy(out_buf, rendered.c_str(), out_buf_len - 1);
    out_buf[out_buf_len - 1] = '\0';
    return 0;
}

int quanta_shim_mvtime_format_time(
    double mjd_days, int second_decimals, char* out_buf, int out_buf_len)
{
    casacore::MVTime time(mjd_days);
    auto rendered = time.string(casacore::MVTime::TIME, 6 + second_decimals);
    std::strncpy(out_buf, rendered.c_str(), out_buf_len - 1);
    out_buf[out_buf_len - 1] = '\0';
    return 0;
}

int quanta_shim_mvtime_format_dmy_date(double mjd_days, char* out_buf, int out_buf_len) {
    casacore::MVTime time(mjd_days);
    auto rendered = time.string(
        casacore::MVTime::formatTypes(casacore::MVTime::DMY + casacore::MVTime::NO_TIME), 0);
    std::strncpy(out_buf, rendered.c_str(), out_buf_len - 1);
    out_buf[out_buf_len - 1] = '\0';
    return 0;
}

} // extern "C"
