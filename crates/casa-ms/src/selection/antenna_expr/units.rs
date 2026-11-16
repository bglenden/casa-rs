// SPDX-License-Identifier: LGPL-3.0-or-later
//! Distance units of baseline-length antenna selection.
//!
//! `MSAntennaParse::getUnitFactor` (`ms/MSSel/MSAntennaParse.cc:336-347`)
//! builds a casacore `Unit` from the `UNIT` token (`[a-zA-Z]+`), refuses one
//! that does not conform to metres, and returns how many of that unit make
//! one metre. casacore resolves an alphabetic unit string as one field
//! (`casa/Quanta/UnitVal.cc:204-226`): the whole name in the user, customary
//! and SI maps, else a one-character prefix and a unit, else the
//! two-character prefix `da` and a unit. The tables below hold every unit an
//! alphabetic token can name (`casa/Quanta/UnitMap{3,4,5,6,7}.cc` and the SI
//! base names of `UnitDim.cc:115-130`), since a non-length name must also
//! shadow a prefixed reading (`min` is minutes, not milli-inches).

/// casacore's SI prefixes and their factors (`casa/Quanta/UnitMap2.cc`).
const PREFIXES: &[(&str, f64)] = &[
    ("Q", 1.0e30),
    ("R", 1.0e27),
    ("Y", 1.0e24),
    ("Z", 1.0e21),
    ("E", 1.0e18),
    ("P", 1.0e15),
    ("T", 1.0e12),
    ("G", 1.0e9),
    ("M", 1.0e6),
    ("k", 1.0e3),
    ("h", 1.0e2),
    ("da", 1.0e1),
    ("d", 1.0e-1),
    ("c", 1.0e-2),
    ("m", 1.0e-3),
    ("u", 1.0e-6),
    ("n", 1.0e-9),
    ("p", 1.0e-12),
    ("f", 1.0e-15),
    ("a", 1.0e-18),
    ("z", 1.0e-21),
    ("y", 1.0e-24),
    ("r", 1.0e-27),
    ("q", 1.0e-30),
];

/// IAU light time for one astronomical unit (`UnitMap4.cc:35`).
const IAU_TAU_A: f64 = 499.004_783_7;
/// Speed of light, m/s (`casa/BasicSL/Constants.h`, `C::c`).
const SPEED_OF_LIGHT: f64 = 2.997_924_58e8;
/// One arcsecond in radians (`Constants.h`, `C::arcsec`).
const ARCSEC: f64 = 0.000_004_848_136_811_095_36;

/// The length units an alphabetic token can name, in metres, written as
/// casacore composes them (`UnitVal(factor, "m" | "cm" | "AU")`).
fn length_unit(name: &str) -> Option<f64> {
    let centimetre = 1.0e-2;
    let astronomical_unit = SPEED_OF_LIGHT * IAU_TAU_A;
    Some(match name {
        "m" => 1.0,
        "AU" | "UA" | "AE" => astronomical_unit,
        "pc" => astronomical_unit * (1.0 / ARCSEC),
        "Angstrom" => 1.0e-10,
        "ft" => 12.0 * 2.54e-2,
        "fur" => centimetre * (7920.0 * 2.54),
        "in" => centimetre * 2.54,
        "ly" => 9.460_730_47e15,
        "mile" => 63360.0 * 2.54e-2,
        "yd" => centimetre * (36.0 * 2.54),
        _ => return None,
    })
}

/// Alphabetic casacore units that are not lengths.
const OTHER_UNITS: &[&str] = &[
    // SI base names other than m (kg is not in the SI map).
    "s", "A", "K", "cd", "mol", "rad", "sr", // UnitMap3.cc
    "g", "Bq", "Hz", "C", "lm", "N", "J", "W", "V", "F", "Gy", "lx", "Ohm", "Pa", "S", "Sv", "Wb",
    "H", "T", // UnitMap4.cc
    "deg", "arcmin", "arcsec", "as", "L", "l", "d", "h", "min", "a", "t", "Jy", "cy",
    // UnitMap5.cc
    "FU", "fu", "WU", // UnitMap6.cc
    "abA", "abC", "abF", "abH", "abOhm", "abV", "statA", "statC", "statF", "statH", "statOhm",
    "statV", "debye", "ac", "Ah", "atm", "ata", "u", "bar", "Btu", "cal", "Cal", "CM", "mHg",
    "dyn", "eV", "erg", // UnitMap7.cc
    "Gal", "gal", "USgal", "G", "Gb", "ha", "hp", "cwt", "kn", "Mx", "Oe", "oz", "lb", "R", "sb",
    "St", "Torr", "yr", "adu", "beam", "count", "pixel", "lambda",
];

/// What a whole unit name is in casacore's maps.
#[derive(Clone, Copy)]
enum Named {
    Length(f64),
    Other,
}

fn named_unit(name: &str) -> Option<Named> {
    length_unit(name)
        .map(Named::Length)
        .or_else(|| OTHER_UNITS.contains(&name).then_some(Named::Other))
}

fn prefix(name: &str) -> Option<f64> {
    PREFIXES
        .iter()
        .find(|(prefix, _)| *prefix == name)
        .map(|(_, factor)| *factor)
}

/// `UnitVal::field` for an alphabetic key (`UnitVal.cc:212-226`).
fn resolve(key: &str) -> Option<Named> {
    if let Some(named) = named_unit(key) {
        return Some(named);
    }
    // Prefixes are ASCII, so these byte splits are character boundaries.
    let one = key.get(..1).and_then(prefix).filter(|_| key.len() > 1)?;
    if let Some(named) = named_unit(&key[1..]) {
        return Some(scale(one, named));
    }
    let two = key.get(..2).and_then(prefix).filter(|_| key.len() > 2)?;
    named_unit(&key[2..]).map(|named| scale(two, named))
}

fn scale(prefix: f64, named: Named) -> Named {
    match named {
        Named::Length(metres) => Named::Length(prefix * metres),
        Named::Other => Named::Other,
    }
}

/// Why a unit cannot scale a baseline length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum UnitError {
    /// casacore knows no such unit: `Unit::check` throws an `AipsError`
    /// (`casa/Quanta/Unit.cc:218-222`).
    Unknown,
    /// The unit is not a length: `MSSelectionAntennaParseError`
    /// (`MSAntennaParse.cc:341-344`).
    NotDistance,
}

impl UnitError {
    /// casacore's message for `unit`.
    pub(super) fn message(self, unit: &str) -> String {
        match self {
            Self::Unknown => format!("Unit::check Illegal unit string '{unit}'"),
            Self::NotDistance => format!("Unit {unit} must be a distance unit (like m)"),
        }
    }
}

/// `MSAntennaParse::getUnitFactor`: how many `unit` make one metre.
///
/// A baseline length written `value unit` is `value / factor` metres
/// (`MSAntennaGram.yy:513-514`).
pub(super) fn unit_factor(unit: &str) -> Result<f64, UnitError> {
    match resolve(unit) {
        // Quantum<Double>(1, "m").getValue(unit) (Quantum.tcc:222-229).
        Some(Named::Length(metres)) => Ok(1.0 / (metres / 1.0)),
        Some(Named::Other) => Err(UnitError::NotDistance),
        None => Err(UnitError::Unknown),
    }
}
