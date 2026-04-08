// SPDX-License-Identifier: LGPL-3.0-or-later
//! Built-in unit definitions ported from C++ casacore `UnitMap2.cc`–`UnitMap7.cc`.
//!
//! Each table is a `const` slice of `(name, factor, description, dims)` tuples
//! where `dims` is the `[i8; 10]` SI dimension exponent array.
//!
//! The factor is always the SI-coherent conversion factor.  For derived units
//! whose C++ definition references other units (e.g. `"J" = 1 "N.m"`), the
//! factor stored here is the fully-resolved SI value.
//!
//! # Dimension index order
//!
//! `[m, kg, s, A, K, cd, mol, rad, sr, _]`

// ────────────────────────────────────────────────────────────────────────────
// Constants used in factor computations (matching C++ Constants.h)
// ────────────────────────────────────────────────────────────────────────────

/// Speed of light in vacuum (m/s), exact SI definition.
const C: f64 = 2.997_924_58e8;
/// π
const PI: f64 = std::f64::consts::PI;
/// Degree in radians.
const DEGREE: f64 = PI / 180.0;
/// Arcminute in radians.
const ARCMIN: f64 = DEGREE / 60.0;
/// Arcsecond in radians.
const ARCSEC: f64 = DEGREE / 3600.0;
/// Square degree in steradians.
const SQ_DEGREE: f64 = DEGREE * DEGREE;
/// Square arcminute in steradians.
const SQ_ARCMIN: f64 = ARCMIN * ARCMIN;
/// Square arcsecond in steradians.
const SQ_ARCSEC: f64 = ARCSEC * ARCSEC;
/// Julian day in seconds.
const DAY: f64 = 86400.0;
/// Hour in seconds.
const HOUR: f64 = 3600.0;
/// Minute in seconds.
const MINUTE: f64 = 60.0;
/// IAU light-travel time for 1 AU (seconds).
const IAU_TAU_A: f64 = 499.004_783_7;
/// IAU Gaussian gravitational constant.
const IAU_K: f64 = 0.017_202_098_95;
/// Inch in metres.
const INCH_M: f64 = 0.0254;

// ────────────────────────────────────────────────────────────────────────────
// Prefix table (from UnitMap2.cc)
// ────────────────────────────────────────────────────────────────────────────

/// `(name, factor, description)` for all 24 SI prefixes.
pub const PREFIXES: &[(&str, f64, &str)] = &[
    ("Q", 1e30, "quetta"),
    ("R", 1e27, "ronna"),
    ("Y", 1e24, "yotta"),
    ("Z", 1e21, "zetta"),
    ("E", 1e18, "exa"),
    ("P", 1e15, "peta"),
    ("T", 1e12, "tera"),
    ("G", 1e9, "giga"),
    ("M", 1e6, "mega"),
    ("k", 1e3, "kilo"),
    ("h", 1e2, "hecto"),
    ("da", 1e1, "deka"),
    ("d", 1e-1, "deci"),
    ("c", 1e-2, "centi"),
    ("m", 1e-3, "milli"),
    ("u", 1e-6, "micro"),
    ("n", 1e-9, "nano"),
    ("p", 1e-12, "pico"),
    ("f", 1e-15, "femto"),
    ("a", 1e-18, "atto"),
    ("z", 1e-21, "zepto"),
    ("y", 1e-24, "yocto"),
    ("r", 1e-27, "ronto"),
    ("q", 1e-30, "quecto"),
];

// ────────────────────────────────────────────────────────────────────────────
// Defining (base SI) units  (from UnitMap3.cc)
// ────────────────────────────────────────────────────────────────────────────

/// `(name, factor, description, [i8; 10])` for the 10 defining units.
pub const DEFINING: &[(&str, f64, &str, [i8; 10])] = &[
    ("m", 1.0, "metre", [1, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    ("kg", 1.0, "kilogram", [0, 1, 0, 0, 0, 0, 0, 0, 0, 0]),
    ("s", 1.0, "second", [0, 0, 1, 0, 0, 0, 0, 0, 0, 0]),
    ("A", 1.0, "ampere", [0, 0, 0, 1, 0, 0, 0, 0, 0, 0]),
    ("K", 1.0, "kelvin", [0, 0, 0, 0, 1, 0, 0, 0, 0, 0]),
    ("cd", 1.0, "candela", [0, 0, 0, 0, 0, 1, 0, 0, 0, 0]),
    ("mol", 1.0, "mole", [0, 0, 0, 0, 0, 0, 1, 0, 0, 0]),
    ("rad", 1.0, "radian", [0, 0, 0, 0, 0, 0, 0, 1, 0, 0]),
    ("sr", 1.0, "steradian", [0, 0, 0, 0, 0, 0, 0, 0, 1, 0]),
    ("_", 1.0, "undimensioned", [0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
];

// ────────────────────────────────────────────────────────────────────────────
// SI derived units (from UnitMap3.cc)
// ────────────────────────────────────────────────────────────────────────────

/// SI derived units with fully-resolved factors and dimensions.
///
/// For units defined in C++ as `UnitVal(1., "N.m", ...)`, the factor here
/// is the product of the constituents' factors (which is 1.0 for most
/// coherent SI derived units).
pub const SI_DERIVED: &[(&str, f64, &str, [i8; 10])] = &[
    // Currency / dimensionless specials
    (
        "$",
        1.0,
        "currency (dimensionless)",
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
    ),
    ("%", 0.01, "percent", [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    ("%%", 0.001, "permille", [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    // Gram (base unit is kg)
    ("g", 1e-3, "gram", [0, 1, 0, 0, 0, 0, 0, 0, 0, 0]),
    // Frequency / activity
    ("Bq", 1.0, "becquerel", [0, 0, -1, 0, 0, 0, 0, 0, 0, 0]),
    ("Hz", 1.0, "hertz", [0, 0, -1, 0, 0, 0, 0, 0, 0, 0]),
    // Electromagnetic
    ("C", 1.0, "coulomb", [0, 0, 1, 1, 0, 0, 0, 0, 0, 0]),
    // Photometry
    ("lm", 1.0, "lumen", [0, 0, 0, 0, 0, 1, 0, 0, 1, 0]),
    // Mechanics
    ("N", 1.0, "newton", [1, 1, -2, 0, 0, 0, 0, 0, 0, 0]),
    ("J", 1.0, "joule", [2, 1, -2, 0, 0, 0, 0, 0, 0, 0]),
    ("W", 1.0, "watt", [2, 1, -3, 0, 0, 0, 0, 0, 0, 0]),
    // Electrical
    ("V", 1.0, "volt", [2, 1, -3, -1, 0, 0, 0, 0, 0, 0]),
    ("F", 1.0, "farad", [-2, -1, 4, 2, 0, 0, 0, 0, 0, 0]),
    // Radiation dose
    ("Gy", 1.0, "gray", [2, 0, -2, 0, 0, 0, 0, 0, 0, 0]),
    // Photometry
    ("lx", 1.0, "lux", [-2, 0, 0, 0, 0, 1, 0, 0, 1, 0]),
    // Electrical
    ("Ohm", 1.0, "ohm", [2, 1, -3, -2, 0, 0, 0, 0, 0, 0]),
    ("Pa", 1.0, "pascal", [-1, 1, -2, 0, 0, 0, 0, 0, 0, 0]),
    ("S", 1.0, "siemens", [-2, -1, 3, 2, 0, 0, 0, 0, 0, 0]),
    // Radiation dose
    ("Sv", 1.0, "sievert", [2, 0, -2, 0, 0, 0, 0, 0, 0, 0]),
    // Magnetism
    ("Wb", 1.0, "weber", [2, 1, -2, -1, 0, 0, 0, 0, 0, 0]),
    ("H", 1.0, "henry", [2, 1, -2, -2, 0, 0, 0, 0, 0, 0]),
    ("T", 1.0, "tesla", [0, 1, -2, -1, 0, 0, 0, 0, 0, 0]),
];

// ────────────────────────────────────────────────────────────────────────────
// Additional SI / non-metric units (from UnitMap4.cc)
// ────────────────────────────────────────────────────────────────────────────

/// Additional SI and astronomical units with fully-resolved factors.
pub const SI_EXTRA: &[(&str, f64, &str, [i8; 10])] = &[
    // Angular
    ("deg", DEGREE, "degree", [0, 0, 0, 0, 0, 0, 0, 1, 0, 0]),
    (
        "arcmin",
        ARCMIN,
        "arcminute",
        [0, 0, 0, 0, 0, 0, 0, 1, 0, 0],
    ),
    (
        "arcsec",
        ARCSEC,
        "arcsecond",
        [0, 0, 0, 0, 0, 0, 0, 1, 0, 0],
    ),
    (
        "as",
        ARCSEC,
        "arcsecond (alias)",
        [0, 0, 0, 0, 0, 0, 0, 1, 0, 0],
    ),
    // Volume
    ("L", 1e-3, "litre", [3, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    ("l", 1e-3, "litre", [3, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    // Time
    ("d", DAY, "day", [0, 0, 1, 0, 0, 0, 0, 0, 0, 0]),
    ("h", HOUR, "hour", [0, 0, 1, 0, 0, 0, 0, 0, 0, 0]),
    ("min", MINUTE, "minute", [0, 0, 1, 0, 0, 0, 0, 0, 0, 0]),
    (
        "a",
        DAY * 365.25,
        "year (Julian)",
        [0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Mass
    ("t", 1000.0, "tonne", [0, 1, 0, 0, 0, 0, 0, 0, 0, 0]),
    // Astronomy
    //                    W/(m^2 Hz) = kg s^-2 * s^-1 ... but Jy has dims [0,1,-2,0,...] as W/m2/Hz
    //                    W = kg m^2 s^-3, W/m^2 = kg s^-3, W/m^2/Hz = kg s^-2
    ("Jy", 1e-26, "jansky", [0, 1, -2, 0, 0, 0, 0, 0, 0, 0]),
    // AU: c * 499.0047837 metres
    (
        "AU",
        C * IAU_TAU_A,
        "astronomical unit",
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    (
        "UA",
        C * IAU_TAU_A,
        "astronomical unit",
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    (
        "AE",
        C * IAU_TAU_A,
        "astronomical unit",
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Solar mass: IAU_k^2 / G in AU^3 / d^2 / (m^3/(kg s^2))
    // The C++ resolves this via UnitVal(IAU_k*IAU_k/6.67259e-11, "AU3/d2/(m3/kg/s2)")
    // which ultimately gives the mass in kg.
    // Factor = IAU_k^2 / 6.67259e-11 * AU^3 / day^2 / (1 m^3/(1 kg * 1 s^2))
    // = IAU_k^2 / 6.67259e-11 * (C * IAU_TAU_A)^3 / DAY^2
    (
        "S0",
        IAU_K * IAU_K / 6.672_59e-11 * (C * IAU_TAU_A) * (C * IAU_TAU_A) * (C * IAU_TAU_A)
            / (DAY * DAY),
        "solar mass",
        [0, 1, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    (
        "M0",
        IAU_K * IAU_K / 6.672_59e-11 * (C * IAU_TAU_A) * (C * IAU_TAU_A) * (C * IAU_TAU_A)
            / (DAY * DAY),
        "solar mass",
        [0, 1, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Parsec: 1/arcsec AU in metres
    (
        "pc",
        (C * IAU_TAU_A) / ARCSEC,
        "parsec",
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Century: 36525 days
    (
        "cy",
        DAY * 36525.0,
        "century",
        [0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
    ),
];

// ────────────────────────────────────────────────────────────────────────────
// Customary units (from UnitMap5.cc, UnitMap6.cc, UnitMap7.cc)
// ────────────────────────────────────────────────────────────────────────────

/// Customary (non-SI) units with fully-resolved SI factors.
pub const CUSTOMARY: &[(&str, f64, &str, [i8; 10])] = &[
    // ── Solid-angle specials (UnitMap5.cc) ──
    (
        "sq_deg",
        SQ_DEGREE,
        "square degree",
        [0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
    ),
    (
        "sq_arcmin",
        SQ_ARCMIN,
        "square arcminute",
        [0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
    ),
    (
        "sq_arcsec",
        SQ_ARCSEC,
        "square arcsecond",
        [0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
    ),
    (
        "deg_2",
        SQ_DEGREE,
        "square degree",
        [0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
    ),
    (
        "arcmin_2",
        SQ_ARCMIN,
        "square arcminute",
        [0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
    ),
    (
        "arcsec_2",
        SQ_ARCSEC,
        "square arcsecond",
        [0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
    ),
    // Symbolic angle/time aliases
    ("'", ARCMIN, "arcminute", [0, 0, 0, 0, 0, 0, 0, 1, 0, 0]),
    ("''", ARCSEC, "arcsecond", [0, 0, 0, 0, 0, 0, 0, 1, 0, 0]),
    ("\"", ARCSEC, "arcsecond", [0, 0, 0, 0, 0, 0, 0, 1, 0, 0]),
    (
        "'_2",
        SQ_ARCMIN,
        "square arcminute",
        [0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
    ),
    (
        "''_2",
        SQ_ARCSEC,
        "square arcsecond",
        [0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
    ),
    (
        "\"_2",
        SQ_ARCSEC,
        "square arcsecond",
        [0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
    ),
    // Time notation
    (
        ":",
        HOUR,
        "hour (time notation)",
        [0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
    ),
    (
        "::",
        MINUTE,
        "minute (time notation)",
        [0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
    ),
    (
        ":::",
        1.0,
        "second (time notation)",
        [0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Flux
    (
        "FU",
        1e-26,
        "flux unit (jansky)",
        [0, 1, -2, 0, 0, 0, 0, 0, 0, 0],
    ),
    (
        "fu",
        1e-26,
        "flux unit (jansky)",
        [0, 1, -2, 0, 0, 0, 0, 0, 0, 0],
    ),
    (
        "WU",
        5e-3 * 1e-26,
        "WSRT flux unit",
        [0, 1, -2, 0, 0, 0, 0, 0, 0, 0],
    ),
    // ── CGS electromagnetic (UnitMap6.cc) ──
    ("abA", 10.0, "abampere", [0, 0, 0, 1, 0, 0, 0, 0, 0, 0]),
    ("abC", 10.0, "abcoulomb", [0, 0, 1, 1, 0, 0, 0, 0, 0, 0]),
    ("abF", 1e9, "abfarad", [-2, -1, 4, 2, 0, 0, 0, 0, 0, 0]),
    ("abH", 1e-9, "abhenry", [2, 1, -2, -2, 0, 0, 0, 0, 0, 0]),
    ("abOhm", 1e-9, "abohm", [2, 1, -3, -2, 0, 0, 0, 0, 0, 0]),
    ("abV", 1e-8, "abvolt", [2, 1, -3, -1, 0, 0, 0, 0, 0, 0]),
    (
        "statA",
        0.1 / C,
        "statampere",
        [0, 0, 0, 1, 0, 0, 0, 0, 0, 0],
    ),
    (
        "statC",
        0.1 / C,
        "statcoulomb",
        [0, 0, 1, 1, 0, 0, 0, 0, 0, 0],
    ),
    (
        "statF",
        1.0 / (3e3 * C),
        "statfarad",
        [-2, -1, 4, 2, 0, 0, 0, 0, 0, 0],
    ),
    (
        "statH",
        3e3 * C,
        "stathenry",
        [2, 1, -2, -2, 0, 0, 0, 0, 0, 0],
    ),
    (
        "statOhm",
        3e3 * C,
        "statohm",
        [2, 1, -3, -2, 0, 0, 0, 0, 0, 0],
    ),
    (
        "statV",
        C * 1e-6,
        "statvolt",
        [2, 1, -3, -1, 0, 0, 0, 0, 0, 0],
    ),
    // Debye: 1e-17 statC·cm = 1e-17 * (0.1/C) C * 0.01 m
    (
        "debye",
        1e-17 * (0.1 / C) * 0.01,
        "debye",
        [1, 0, 1, 1, 0, 0, 0, 0, 0, 0],
    ),
    // ── Traditional / engineering (UnitMap6.cc) ──
    // Acre = 4 * 40 * 16.5 * 12 * 0.0254 * 16.5 * 12 * 0.0254 m²
    (
        "ac",
        4.0 * 40.0 * (16.5 * 12.0 * INCH_M) * (16.5 * 12.0 * INCH_M),
        "acre",
        [2, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    ("Ah", HOUR, "ampere hour", [0, 0, 1, 1, 0, 0, 0, 0, 0, 0]),
    (
        "Angstrom",
        1e-10,
        "angstrom",
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    (
        "atm",
        1.013_25e5,
        "atmosphere",
        [-1, 1, -2, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Technical atmosphere: 9.80665 N/cm² = 9.80665e4 Pa
    (
        "ata",
        9.80665e4,
        "technical atmosphere",
        [-1, 1, -2, 0, 0, 0, 0, 0, 0, 0],
    ),
    (
        "u",
        1.661e-27,
        "atomic mass unit",
        [0, 1, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    ("bar", 1e5, "bar", [-1, 1, -2, 0, 0, 0, 0, 0, 0, 0]),
    (
        "Btu",
        1055.056,
        "British thermal unit",
        [2, 1, -2, 0, 0, 0, 0, 0, 0, 0],
    ),
    (
        "cal",
        4.1868,
        "calorie (Int)",
        [2, 1, -2, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Cal = kcal = 1000 * 4.1868
    (
        "Cal",
        4186.8,
        "large calorie",
        [2, 1, -2, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Metric carat: 1e-3/5 kg = 2e-4 kg
    ("CM", 2e-4, "metric carat", [0, 1, 0, 0, 0, 0, 0, 0, 0, 0]),
    // Metre of mercury: 13.5951 * 9.80665 kPa = 133322.3... Pa
    (
        "mHg",
        13.5951 * 9.80665e3,
        "metre of mercury",
        [-1, 1, -2, 0, 0, 0, 0, 0, 0, 0],
    ),
    ("dyn", 1e-5, "dyne", [1, 1, -2, 0, 0, 0, 0, 0, 0, 0]),
    (
        "eV",
        1.602_177_33e-19,
        "electron volt",
        [2, 1, -2, 0, 0, 0, 0, 0, 0, 0],
    ),
    ("erg", 1e-7, "erg", [2, 1, -2, 0, 0, 0, 0, 0, 0, 0]),
    // ── Imperial / US (UnitMap7.cc) ──
    // Fluid ounce (Imp): 277.4193 * 2.54^3 / (5*4*2*4) cm³
    // = 277.4193 * 16.387064 / 160 cm³ = 28.41... cm³ = 28.41e-6 m³
    (
        "fl_oz",
        277.4193 * INCH_M * INCH_M * INCH_M * 1e6 / 160.0 * 1e-6,
        "fluid ounce (Imp)",
        [3, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    // US fluid ounce: 231 * 2.54^3 / (4*4*2*4) cm³ = 231 * 16.387064 / 128 cm³
    (
        "USfl_oz",
        231.0 * INCH_M * INCH_M * INCH_M * 1e6 / 128.0 * 1e-6,
        "fluid ounce (US)",
        [3, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Foot: 12 * 0.0254 = 0.3048 m
    ("ft", 0.3048, "foot", [1, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    // Furlong: 220 * 3 * 12 * 2.54 cm = 20116.8 cm = 201.168 m
    ("fur", 201.168, "furlong", [1, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    // Gal: 1 cm/s² = 0.01 m/s²
    ("Gal", 0.01, "gal", [1, 0, -2, 0, 0, 0, 0, 0, 0, 0]),
    // Imperial gallon: 277.4193 * 2.54^3 cm³
    (
        "gal",
        277.4193 * INCH_M * INCH_M * INCH_M * 1e6 * 1e-6,
        "gallon (Imp)",
        [3, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    // US gallon: 231 * 2.54^3 cm³
    (
        "USgal",
        231.0 * INCH_M * INCH_M * INCH_M * 1e6 * 1e-6,
        "gallon (US)",
        [3, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Gauss: 1e-4 T
    ("G", 1e-4, "gauss", [0, 1, -2, -1, 0, 0, 0, 0, 0, 0]),
    // Gilbert: 10/(4π) A
    (
        "Gb",
        10.0 / (4.0 * PI),
        "gilbert",
        [0, 0, 0, 1, 0, 0, 0, 0, 0, 0],
    ),
    // Hectare: 1 hm² = 10000 m²
    ("ha", 1e4, "hectare", [2, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    // Horsepower
    ("hp", 745.7, "horsepower", [2, 1, -3, 0, 0, 0, 0, 0, 0, 0]),
    // Hundredweight (UK): 4 * 2 * 14 * 0.45359237 kg
    (
        "cwt",
        4.0 * 2.0 * 14.0 * 0.453_592_37,
        "hundredweight",
        [0, 1, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Inch: 2.54 cm = 0.0254 m
    ("in", INCH_M, "inch", [1, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    // Knot (Imp): 6080 * 12 * 2.54 cm/h = 185318.4 cm/h = 1853.184 m / 3600 s
    (
        "kn",
        6080.0 * 12.0 * INCH_M / HOUR,
        "knot (Imp)",
        [1, 0, -1, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Light year
    (
        "ly",
        9.460_730_47e15,
        "light year",
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Maxwell: 1e-8 Wb
    ("Mx", 1e-8, "maxwell", [2, 1, -2, -1, 0, 0, 0, 0, 0, 0]),
    // Mile: 5280 * 12 * 0.0254 m
    (
        "mile",
        5280.0 * 12.0 * INCH_M,
        "mile",
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Nautical mile (Imp): 6080 * 12 * 2.54 cm = 1853.184 m
    (
        "n_mile",
        6080.0 * 12.0 * INCH_M,
        "nautical mile (Imp)",
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Oersted: 1000/(4π) A/m
    (
        "Oe",
        1000.0 / (4.0 * PI),
        "oersted",
        [-1, 0, 0, 1, 0, 0, 0, 0, 0, 0],
    ),
    // Ounce (avoirdupois): 1/16 * 0.45359237 kg
    (
        "oz",
        0.453_592_37 / 16.0,
        "ounce (avdp)",
        [0, 1, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Pound (avoirdupois)
    (
        "lb",
        0.453_592_37,
        "pound (avdp)",
        [0, 1, 0, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Roentgen: 2.58e-4 C/kg
    ("R", 2.58e-4, "roentgen", [0, -1, 1, 1, 0, 0, 0, 0, 0, 0]),
    // Stilb: 1e4 cd/m²
    ("sb", 1e4, "stilb", [-2, 0, 0, 0, 0, 1, 0, 0, 0, 0]),
    // Stokes: 1 cm²/s = 1e-4 m²/s
    ("St", 1e-4, "stokes", [2, 0, -1, 0, 0, 0, 0, 0, 0, 0]),
    // Torr: (1/760) * 101325 Pa
    (
        "Torr",
        1.013_25e5 / 760.0,
        "torr",
        [-1, 1, -2, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Yard: 3 * 12 * 2.54 cm = 91.44 cm = 0.9144 m
    ("yd", 0.9144, "yard", [1, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
    // Year (Julian) - same as "a"
    (
        "yr",
        DAY * 365.25,
        "year (Julian)",
        [0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
    ),
    // Dimensionless specials
    ("adu", 1.0, "ADC unit", [0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
    ("beam", 1.0, "beam area", [0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
    ("count", 1.0, "count", [0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
    ("pixel", 1.0, "pixel", [0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
    ("lambda", 1.0, "lambda", [0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
];

// ────────────────────────────────────────────────────────────────────────────
// Physical constants (from QC.h / QC.cc)
// ────────────────────────────────────────────────────────────────────────────

/// `(name, value, unit_string)` for the QC physical constants.
pub const PHYSICAL_CONSTANTS: &[(&str, f64, &str)] = &[
    ("c", C, "m/s"),
    ("G", 6.672_59e-11, "N.m2/kg2"),
    ("h", 6.626_075_5e-34, "J.s"),
    ("HI", 1_420.405_751_786e6, "Hz"),
    ("R", 8.314_510, "J/K/mol"),
    ("NA", 6.022_136_7e23, "/mol"),
    ("e", 1.602_177_33e-19, "C"),
    ("mp", 1.672_623_1e-27, "kg"),
    ("mp_me", 1_836.152_701, ""),
    ("mu0", 4e-7 * PI, "H/m"),
    ("epsilon0", 1.0 / (4e-7 * PI * C * C), "F/m"),
    ("k", 8.314_510 / 6.022_136_7e23, "J/K"),
    ("F", 6.022_136_7e23 * 1.602_177_33e-19, "C/mol"),
    ("me", 1.672_623_1e-27 / 1_836.152_701, "kg"),
    ("re", 2.8179e-15, "m"),
    ("a0", 5.2918e-11, "m"),
    ("R0", 6.9599e8, "m"),
    ("k2", IAU_K * IAU_K, "AU3/d2/S0"),
    ("qTurn", 90.0, "deg"),
    ("hTurn", 180.0, "deg"),
    ("fTurn", 360.0, "deg"),
];
