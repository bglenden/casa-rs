// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore's `MeasureHolder`: an epoch, direction, position or frequency
//! with any of its kind's reference codes, read from and written to the
//! measure records CASA's tools exchange.
//!
//! A [`Measure`] keeps what casacore's measure value keeps: the epoch's MJD,
//! the direction's cosines, the position's Cartesian vector and the
//! frequency in Hz. It therefore holds codes casa-types does not convert
//! (`SUN`, `B1950_VLA`, `REST`): they define, show and compare, and only a
//! conversion that needs them fails.
//!
//! - **Reading** ([`Measure::from_record`]) follows
//!   `MeasureHolder::fromRecord` and the measure values' `putValue`: the
//!   record's `type` names the kind, `refer` names a code by unique
//!   case-insensitive prefix (an unknown one is the kind's default, with
//!   casacore's warning), and `m0`, `m1`, `m2` are quantity records in any
//!   unit their kind accepts.
//! - **Writing** ([`Measure::to_value`]) follows `MeasureHolder::toRecord`:
//!   `{'type', 'refer', 'm0', ...}` with the epoch in days, angles in
//!   radians, the position's length in metres and the frequency in Hz.
//! - **Positions** keep casacore's encoding of a WGS84 height as the length
//!   of the position vector (`MVPosition`): a zero height is a 1e-6 m
//!   vector, and heights between 0 and −7000 km, or near 743.568 m, are
//!   shifted, so `me.position('WGS84', '10deg', '20deg', '0m')` has a
//!   height of 1e-6 m, as in CASA.
//!
//! Divergence: measure offsets and multi-valued measures (quantity records
//! with more than one value) are not yet implemented in casa-rs; reading one
//! fails with a message that says so.

use std::collections::BTreeMap;
use std::str::FromStr;

use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::measures::{EpochRef, MEpoch, MPosition, MjdHighPrec, PositionRef};
use casa_types::quanta::{Dimension, Quantity, UnitDim, unique_prefix_index};

use super::value::CasaValue;
use super::{Variant, quantity_record};

/// The speed of light, m/s (casacore's `QC::c`).
const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;
/// Planck's constant, J s, as casacore's `QC::h` gives it (the 1986 CODATA
/// value).
const PLANCK_J_S: f64 = 6.626_075_5e-34;
/// The band of vector lengths in which casacore encodes WGS84 heights
/// (`MVPosition::loLimit` and `hiLimit`).
const LO_LIMIT_M: f64 = 743.568;
const HI_LIMIT_M: f64 = 743.569;

/// The kinds of measure casa-rs holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MeasureKind {
    Epoch,
    Direction,
    Position,
    Frequency,
}

/// A kind's reference codes, in casacore's `allMyTypes` order: the normal
/// codes, then the extra ones (planets), and the aliases with the code each
/// stands for.
pub(crate) struct Codes {
    pub(crate) all: &'static [&'static str],
    pub(crate) extra: usize,
    aliases: &'static [(&'static str, &'static str)],
    default: &'static str,
}

/// casacore's `MEpoch` reference codes.
pub(crate) const EPOCH_CODES: Codes = Codes {
    all: &[
        "LAST", "LMST", "GMST1", "GAST", "UT1", "UT2", "UTC", "TAI", "TDT", "TCG", "TDB", "TCB",
        "IAT", "GMST", "TT", "ET", "UT",
    ],
    extra: 0,
    aliases: &[
        ("IAT", "TAI"),
        ("GMST", "GMST1"),
        ("TT", "TDT"),
        ("ET", "TDT"),
        ("UT", "UT1"),
    ],
    default: "UTC",
};

/// casacore's `MDirection` reference codes.
pub(crate) const DIRECTION_CODES: Codes = Codes {
    all: &[
        "J2000",
        "JMEAN",
        "JTRUE",
        "APP",
        "B1950",
        "B1950_VLA",
        "BMEAN",
        "BTRUE",
        "GALACTIC",
        "HADEC",
        "AZEL",
        "AZELSW",
        "AZELNE",
        "AZELGEO",
        "AZELSWGEO",
        "AZELNEGEO",
        "JNAT",
        "ECLIPTIC",
        "MECLIPTIC",
        "TECLIPTIC",
        "SUPERGAL",
        "ITRF",
        "TOPO",
        "ICRS",
        "MERCURY",
        "VENUS",
        "MARS",
        "JUPITER",
        "SATURN",
        "URANUS",
        "NEPTUNE",
        "PLUTO",
        "SUN",
        "MOON",
        "COMET",
    ],
    extra: 11,
    aliases: &[("AZELNE", "AZEL"), ("AZELNEGEO", "AZELGEO")],
    default: "J2000",
};

/// casacore's `MPosition` reference codes.
pub(crate) const POSITION_CODES: Codes = Codes {
    all: &["ITRF", "WGS84"],
    extra: 0,
    aliases: &[],
    default: "ITRF",
};

/// casacore's `MFrequency` reference codes.
pub(crate) const FREQUENCY_CODES: Codes = Codes {
    all: &[
        "REST",
        "LSRK",
        "LSRD",
        "BARY",
        "GEO",
        "TOPO",
        "GALACTO",
        "LGROUP",
        "CMB",
        "Undefined",
    ],
    extra: 1,
    aliases: &[],
    default: "LSRK",
};

impl Codes {
    /// The code `input` names by unique case-insensitive prefix
    /// (`MUString::minimaxNC`), as the code it stands for; `None` if it names
    /// none.
    pub(crate) fn resolve(&self, input: &str) -> Option<&'static str> {
        let code = self.all[unique_prefix_index(input, self.all)?];
        Some(
            self.aliases
                .iter()
                .find(|(alias, _)| *alias == code)
                .map_or(code, |(_, canonical)| canonical),
        )
    }

    /// The code `input` names, else the kind's default (`setRefString`).
    pub(crate) fn resolve_or_default(&self, input: &str) -> &'static str {
        self.resolve(input).unwrap_or(self.default)
    }

    /// The kind's default code.
    pub(crate) fn default_code(&self) -> &'static str {
        self.default
    }
}

impl MeasureKind {
    /// The kind's reference codes.
    pub(crate) fn codes(self) -> &'static Codes {
        match self {
            Self::Epoch => &EPOCH_CODES,
            Self::Direction => &DIRECTION_CODES,
            Self::Position => &POSITION_CODES,
            Self::Frequency => &FREQUENCY_CODES,
        }
    }

    /// The record's `type`.
    pub(crate) fn record_type(self) -> &'static str {
        match self {
            Self::Epoch => "epoch",
            Self::Direction => "direction",
            Self::Position => "position",
            Self::Frequency => "frequency",
        }
    }

    /// casacore's `Measure::tellMe` (`me.gettype`).
    pub(crate) fn tell_me(self) -> &'static str {
        match self {
            Self::Epoch => "Epoch",
            Self::Direction => "Direction",
            Self::Position => "Position",
            Self::Frequency => "Frequency",
        }
    }
}

/// A measure's value as casacore's measure value keeps it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum MeasureValue {
    /// MJD days.
    Epoch(MjdHighPrec),
    /// Direction cosines.
    Direction([f64; 3]),
    /// The position vector, m: ITRF x, y, z, or a WGS84 direction whose
    /// length encodes the height.
    Position([f64; 3]),
    /// Hz.
    Frequency(f64),
}

/// A measure: a value and a canonical reference code of its kind.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Measure {
    pub(crate) refer: &'static str,
    pub(crate) value: MeasureValue,
}

/// A measure in casa-types' form, for conversions.
#[derive(Debug, Clone)]
pub(crate) enum CasaMeasure {
    Epoch(MEpoch),
    Direction(MDirection),
    Position(MPosition),
    Frequency(MFrequency),
}

/// A record [`Measure::from_record`] could not read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HolderError {
    /// Not a measure record (no string `type` and `refer`): casacore's
    /// "Illegal Measure record".
    NotAMeasure,
    /// casacore's message for a measure record it cannot read.
    Invalid(String),
}

impl HolderError {
    /// casacore's message.
    pub(crate) fn message(&self) -> String {
        match self {
            Self::NotAMeasure => {
                "Illegal Measure record in MeasureHolder::fromRecord\n".to_string()
            }
            Self::Invalid(message) => message.clone(),
        }
    }
}

/// A measure read from a record, with casacore's warning when the record's
/// reference code was unknown.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReadMeasure {
    pub(crate) measure: Measure,
    pub(crate) warning: Option<String>,
}

fn illegal_quantity() -> HolderError {
    HolderError::Invalid("Illegal quantity in MeasureHolder::fromRecord\n".to_string())
}

impl Measure {
    /// The measure's kind.
    pub(crate) fn kind(&self) -> MeasureKind {
        match self.value {
            MeasureValue::Epoch(_) => MeasureKind::Epoch,
            MeasureValue::Direction(_) => MeasureKind::Direction,
            MeasureValue::Position(_) => MeasureKind::Position,
            MeasureValue::Frequency(_) => MeasureKind::Frequency,
        }
    }

    /// `MeasureHolder::fromRecord`.
    pub(crate) fn from_record(
        record: &BTreeMap<String, Variant>,
    ) -> Result<ReadMeasure, HolderError> {
        let (Some(Variant::Text(kind)), Some(Variant::Text(refer))) =
            (record.get("type"), record.get("refer"))
        else {
            return Err(HolderError::NotAMeasure);
        };
        let kind = match kind.to_ascii_lowercase().as_str() {
            "epoch" => MeasureKind::Epoch,
            "direction" => MeasureKind::Direction,
            "position" => MeasureKind::Position,
            "frequency" => MeasureKind::Frequency,
            "doppler" | "radialvelocity" | "baseline" | "uvw" | "earthmagnetic" => {
                return Err(HolderError::Invalid(format!(
                    "{kind} measures are not yet implemented in casa-rs"
                )));
            }
            _ => {
                return Err(HolderError::Invalid(format!(
                    "{kind} is an unknown measure typeUnknown Measure record in \
                     MeasureHolder::fromRecord\n"
                )));
            }
        };
        let codes = kind.codes();
        let (code, warning) = match codes.resolve(refer) {
            Some(code) => (code, None),
            None => (
                codes.default,
                (!refer.is_empty()).then(|| {
                    format!(
                        "Illegal or unknown reference type '{refer}' for {} definition. \
                         DEFAULT ({}) assumed.",
                        kind.record_type(),
                        codes.default
                    )
                }),
            ),
        };
        if record.contains_key("offset") {
            return Err(HolderError::Invalid(
                "measure offsets are not yet implemented in casa-rs".to_string(),
            ));
        }
        let values = record_values(record)?;
        Ok(ReadMeasure {
            measure: Self::from_values(kind, code, &values)?,
            warning,
        })
    }

    /// The measure from quantities, as the kind's `putValue` reads them.
    pub(crate) fn from_values(
        kind: MeasureKind,
        refer: &'static str,
        values: &[Quantity],
    ) -> Result<Self, HolderError> {
        let value = match kind {
            MeasureKind::Epoch => epoch_value(values)?,
            MeasureKind::Direction => direction_value(values)?,
            MeasureKind::Position => position_value(values)?,
            MeasureKind::Frequency => frequency_value(values)?,
        };
        Ok(Self { refer, value })
    }

    /// `MeasureHolder::toRecord`.
    pub(crate) fn to_value(self) -> CasaValue {
        let mut fields = vec![
            (
                "type",
                CasaValue::Text(self.kind().record_type().to_string()),
            ),
            ("refer", CasaValue::Text(self.refer.to_string())),
        ];
        match self.value {
            MeasureValue::Epoch(mjd) => fields.push(("m0", CasaValue::quantity(mjd.as_mjd(), "d"))),
            MeasureValue::Direction(cosines) => {
                let (longitude, latitude) = direction_angles(cosines);
                fields.push(("m0", CasaValue::quantity(longitude, "rad")));
                fields.push(("m1", CasaValue::quantity(latitude, "rad")));
            }
            MeasureValue::Position(xyz) => {
                let (longitude, latitude, length) = position_spherical(xyz);
                fields.push(("m0", CasaValue::quantity(longitude, "rad")));
                fields.push(("m1", CasaValue::quantity(latitude, "rad")));
                fields.push(("m2", CasaValue::quantity(length, "m")));
            }
            MeasureValue::Frequency(hz) => fields.push(("m0", CasaValue::quantity(hz, "Hz"))),
        }
        CasaValue::record(fields)
    }

    /// The measure in casa-types' form, failing for a code casa-types does
    /// not convert.
    pub(crate) fn to_casa(self) -> Result<CasaMeasure, String> {
        let unsupported = |kind: &str| {
            format!(
                "the {} {kind} frame is not yet implemented in casa-rs",
                self.refer
            )
        };
        Ok(match self.value {
            MeasureValue::Epoch(mjd) => CasaMeasure::Epoch(MEpoch::new(
                mjd,
                EpochRef::from_str(self.refer).map_err(|_| unsupported("epoch"))?,
            )),
            MeasureValue::Direction(cosines) => CasaMeasure::Direction(MDirection::from_cosines(
                cosines,
                DirectionRef::from_str(self.refer).map_err(|_| unsupported("direction"))?,
            )),
            MeasureValue::Position(xyz) => CasaMeasure::Position(if self.refer == "WGS84" {
                let (longitude, latitude, height) = position_spherical(xyz);
                MPosition::new_wgs84(longitude, latitude, height)
            } else {
                MPosition::new_itrf(xyz[0], xyz[1], xyz[2])
            }),
            MeasureValue::Frequency(hz) => CasaMeasure::Frequency(MFrequency::new(
                hz,
                FrequencyRef::from_str(self.refer).map_err(|_| unsupported("frequency"))?,
            )),
        })
    }

    /// A casa-types measure in casacore's form.
    pub(crate) fn from_casa(measure: &CasaMeasure) -> Self {
        match measure {
            CasaMeasure::Epoch(epoch) => Self {
                refer: EPOCH_CODES.resolve_or_default(epoch.refer().casacore_name()),
                value: MeasureValue::Epoch(epoch.value()),
            },
            CasaMeasure::Direction(direction) => Self {
                refer: DIRECTION_CODES.resolve_or_default(direction.refer().as_str()),
                value: MeasureValue::Direction(direction.cosines()),
            },
            CasaMeasure::Position(position) => match position.refer() {
                PositionRef::ITRF => Self {
                    refer: "ITRF",
                    value: MeasureValue::Position(position.values()),
                },
                PositionRef::WGS84 => {
                    let [longitude, latitude, height] = position.values();
                    Self {
                        refer: "WGS84",
                        value: MeasureValue::Position(position_vector(longitude, latitude, height)),
                    }
                }
            },
            CasaMeasure::Frequency(frequency) => Self {
                refer: FREQUENCY_CODES.resolve_or_default(frequency.refer().as_str()),
                value: MeasureValue::Frequency(frequency.hz()),
            },
        }
    }
}

/// The `m0`, `m1`, `m2` quantities of a measure record, up to the first
/// missing one. A quantity record may hold one value, or an array of one.
fn record_values(record: &BTreeMap<String, Variant>) -> Result<Vec<Quantity>, HolderError> {
    let mut values = Vec::new();
    for key in ["m0", "m1", "m2"] {
        let Some(Variant::Record(value)) = record.get(key) else {
            break;
        };
        if let Some(Variant::Numbers(numbers)) = value.get("value")
            && numbers.len() > 1
        {
            return Err(HolderError::Invalid(
                "multi-valued measures are not yet implemented in casa-rs".to_string(),
            ));
        }
        values.push(quantity_record(value).map_err(HolderError::Invalid)?);
    }
    Ok(values)
}

fn has(quantity: &Quantity, dimension: Dimension) -> bool {
    quantity.unit().val().dim == UnitDim::basis(dimension)
}

/// `MVEpoch::putValue`: every value a time, summed.
fn epoch_value(values: &[Quantity]) -> Result<MeasureValue, HolderError> {
    if !values.iter().all(|value| has(value, Dimension::Time)) {
        return Err(illegal_quantity());
    }
    let (mut day, mut fraction) = (0.0, 0.0);
    for value in values {
        let days = days_of(value);
        let whole = days.floor();
        day += whole;
        fraction += days - whole;
    }
    Ok(MeasureValue::Epoch(MjdHighPrec::new(day, fraction)))
}

/// A time in days as casacore converts it (`Quantity::get("d")`).
pub(crate) fn days_of(time: &Quantity) -> f64 {
    time.value() * (time.unit().val().factor / 86_400.0)
}

/// `MVDirection::putValue`: three direction cosines, or up to two angles
/// (none is the pole, one a longitude on the equator); normalised.
fn direction_value(values: &[Quantity]) -> Result<MeasureValue, HolderError> {
    let mut xyz = if values.len() == 3
        && values
            .iter()
            .all(|value| value.unit().val().dim.is_dimensionless())
    {
        [values[0].value(), values[1].value(), values[2].value()]
    } else {
        if values.len() > 3 || !values.iter().all(|value| has(value, Dimension::Angle)) {
            return Err(illegal_quantity());
        }
        let angles = values
            .iter()
            .map(Quantity::get_si_value)
            .collect::<Vec<_>>();
        match angles.as_slice() {
            [] => [0.0, 0.0, 1.0],
            [longitude] => [longitude.cos(), longitude.sin(), 0.0],
            [longitude, latitude, ..] => direction_cosines(*longitude, *latitude),
        }
    };
    let length = norm(xyz);
    if length != 0.0 && length != 1.0 {
        xyz = xyz.map(|value| value / length);
    }
    Ok(MeasureValue::Direction(xyz))
}

/// `MVPosition::putValue`: three lengths (x, y, z), a length then two
/// angles, or two angles then a length.
fn position_value(values: &[Quantity]) -> Result<MeasureValue, HolderError> {
    let [first, second, third] = values else {
        return Err(illegal_quantity());
    };
    let (length, angle) = (Dimension::Length, Dimension::Angle);
    let xyz = if has(first, length) {
        if has(second, length) && has(third, length) {
            [
                first.get_si_value(),
                second.get_si_value(),
                third.get_si_value(),
            ]
        } else if has(second, angle) && has(third, angle) {
            position_vector(
                second.get_si_value(),
                third.get_si_value(),
                first.get_si_value(),
            )
        } else {
            return Err(illegal_quantity());
        }
    } else if has(third, length) && has(first, angle) && has(second, angle) {
        position_vector(
            first.get_si_value(),
            second.get_si_value(),
            third.get_si_value(),
        )
    } else {
        return Err(illegal_quantity());
    };
    Ok(MeasureValue::Position(xyz))
}

/// `MVFrequency::putValue`: none is 0 Hz; one value is a frequency, a
/// period, an angular rate, a wavelength, a wavenumber, an energy or a
/// momentum (`MVFrequency::makeF`).
fn frequency_value(values: &[Quantity]) -> Result<MeasureValue, HolderError> {
    match values {
        [] => Ok(MeasureValue::Frequency(0.0)),
        [value] => frequency_hz(value)
            .map(MeasureValue::Frequency)
            .ok_or_else(illegal_quantity),
        _ => Err(illegal_quantity()),
    }
}

/// `MVFrequency::makeF`: `quantity` in Hz, or `None` for a unit that is not
/// frequency-like.
pub(crate) fn frequency_hz(quantity: &Quantity) -> Option<f64> {
    let unit = quantity.unit().val();
    let (value, factor) = (quantity.value(), unit.factor);
    let basis = UnitDim::basis;
    let time = basis(Dimension::Time);
    let length = basis(Dimension::Length);
    let energy = basis(Dimension::Mass) * length * length / time / time;
    let momentum = basis(Dimension::Mass) * length;
    let dim = unit.dim;
    if dim == time {
        Some(1.0 / factor / value)
    } else if dim == UnitDim::NODIM / time {
        Some(value * factor)
    } else if dim == basis(Dimension::Angle) / time {
        Some(value * (factor / std::f64::consts::PI / 2.0))
    } else if dim == length {
        Some(SPEED_OF_LIGHT_M_S / factor / value)
    } else if dim == UnitDim::NODIM / length {
        Some(value * (SPEED_OF_LIGHT_M_S * factor / std::f64::consts::PI / 2.0))
    } else if dim == energy {
        Some(value * (factor / PLANCK_J_S))
    } else if dim == momentum {
        Some(value * (factor * SPEED_OF_LIGHT_M_S / PLANCK_J_S))
    } else {
        None
    }
}

fn norm(xyz: [f64; 3]) -> f64 {
    (xyz[0] * xyz[0] + xyz[1] * xyz[1] + xyz[2] * xyz[2]).sqrt()
}

/// The cosines of a direction (`MVDirection(lon, lat)`).
pub(crate) fn direction_cosines(longitude: f64, latitude: f64) -> [f64; 3] {
    let cos_latitude = latitude.cos();
    [
        longitude.cos() * cos_latitude,
        longitude.sin() * cos_latitude,
        latitude.sin(),
    ]
}

/// `MVDirection::get`: longitude in (−π, π] (0 at a pole) and latitude.
pub(crate) fn direction_angles(cosines: [f64; 3]) -> (f64, f64) {
    let longitude = if cosines[0] != 0.0 || cosines[1] != 0.0 {
        cosines[1].atan2(cosines[0])
    } else {
        0.0
    };
    (longitude, cosines[2].asin())
}

/// `MVPosition(length, lon, lat)`: the vector, its length encoding a WGS84
/// height as casacore encodes it.
pub(crate) fn position_vector(longitude: f64, latitude: f64, length_m: f64) -> [f64; 3] {
    let mut t = length_m;
    if t < 0.0 && t > -7.0e6 {
        t = t / 1.0e7 + HI_LIMIT_M;
    } else if t > LO_LIMIT_M && t < HI_LIMIT_M {
        t += 0.001;
    }
    let scale = if t == 0.0 { 1e-6 } else { t };
    direction_cosines(longitude, latitude).map(|value| value * scale)
}

/// `MVPosition::get`: longitude, latitude and the (decoded) length.
pub(crate) fn position_spherical(xyz: [f64; 3]) -> (f64, f64, f64) {
    let mut length = norm(xyz);
    let longitude = if xyz[0] != 0.0 || xyz[1] != 0.0 {
        xyz[1].atan2(xyz[0])
    } else {
        0.0
    };
    let latitude = (xyz[2] / if length == 0.0 { 1.0 } else { length }).asin();
    if length > LO_LIMIT_M && length < HI_LIMIT_M {
        length = (length - HI_LIMIT_M) * 1e7;
    }
    (longitude, latitude, length)
}

#[cfg(test)]
#[path = "holder_tests.rs"]
mod tests;
