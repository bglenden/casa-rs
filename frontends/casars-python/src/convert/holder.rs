// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore's scalar `MeasureHolder`: epoch, direction, position, frequency,
//! Doppler and radial-velocity values
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
//! The measures tool's record envelope handles vectors and reference offsets,
//! reusing [`Measure::record_definition`] to parse metadata and units once.
//! This scalar holder is also used by task parameter conversion.

use std::collections::BTreeMap;
use std::str::FromStr;

use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::doppler::{DopplerRef, MDoppler};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::measures::radial_velocity::MRadialVelocity;
use casa_types::measures::{EpochRef, MEpoch, MPosition, MjdHighPrec, PositionRef};
use casa_types::quanta::{Dimension, Quantity, UnitDim, unique_prefix_index};

use super::value::CasaValue;
use super::{Variant, quantity_record};

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
    Doppler,
    RadialVelocity,
    Baseline,
    Uvw,
    EarthMagnetic,
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

const RAZED_EPOCH_CODES: [&str; 12] = [
    "R_LAST", "R_LMST", "R_GMST1", "R_GAST", "R_UT1", "R_UT2", "R_UTC", "R_TAI", "R_TDT", "R_TCG",
    "R_TDB", "R_TCB",
];

/// Resolve the optional R_ epoch prefix without changing listcodes output.
pub(crate) fn resolve_epoch_code(input: &str) -> Option<&'static str> {
    if let Some(code) = input
        .strip_prefix("R_")
        .or_else(|| input.strip_prefix("r_"))
    {
        let canonical = EPOCH_CODES.resolve(code)?;
        let reference = EpochRef::from_str(canonical).ok()?;
        Some(RAZED_EPOCH_CODES[reference.casacore_code() as usize])
    } else {
        EPOCH_CODES.resolve(input)
    }
}

/// Interpret a canonical epoch code stored in a measure record.
pub(crate) fn epoch_ref(
    code: &str,
) -> Result<(EpochRef, bool), casa_types::measures::MeasureError> {
    let base = code.strip_prefix("R_");
    Ok((EpochRef::from_str(base.unwrap_or(code))?, base.is_some()))
}

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

/// CASA's canonical Doppler names and aliases (`MDoppler::allMyTypes`).
pub(crate) const DOPPLER_CODES: Codes = Codes {
    all: &[
        "RADIO",
        "Z",
        "RATIO",
        "BETA",
        "GAMMA",
        "OPTICAL",
        "TRUE",
        "RELATIVISTIC",
    ],
    extra: 0,
    aliases: &[("Z", "OPTICAL"), ("BETA", "TRUE"), ("RELATIVISTIC", "TRUE")],
    default: "RADIO",
};
/// CASA's radial-velocity reference codes.
pub(crate) const RADIAL_VELOCITY_CODES: Codes = Codes {
    all: &[
        "LSRK", "LSRD", "BARY", "GEO", "TOPO", "GALACTO", "LGROUP", "CMB",
    ],
    extra: 0,
    aliases: &[],
    default: "LSRK",
};
/// Exact speed of light, used by casacore's `MVDoppler` velocity quantities.
pub(crate) const C_MPS: f64 = 299_792_458.0;

pub(crate) fn doppler_ref(code: &str) -> Result<DopplerRef, String> {
    (if code == "TRUE" { "BETA" } else { code })
        .parse()
        .map_err(|e: casa_types::measures::MeasureError| e.to_string())
}

/// CASA geometry reference codes and aliases.
pub(crate) const BASELINE_CODES: Codes = Codes {
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
    ],
    extra: 0,
    aliases: &[("AZELNE", "AZEL"), ("AZELNEGEO", "AZELGEO")],
    default: "ITRF",
};

/// CASA geometry reference codes and aliases.
pub(crate) const EARTH_MAGNETIC_CODES: Codes = Codes {
    all: &[
        "J2000",
        "JMEAN",
        "JTRUE",
        "APP",
        "B1950",
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
        "IGRF",
    ],
    extra: 0,
    aliases: &[("AZELNE", "AZEL"), ("AZELNEGEO", "AZELGEO")],
    default: "IGRF",
};

impl MeasureKind {
    /// Resolve the canonical reference, including the epoch R_ flag.
    pub(crate) fn resolve_code(self, input: &str) -> Option<&'static str> {
        if self == Self::Epoch {
            resolve_epoch_code(input)
        } else {
            self.codes().resolve(input)
        }
    }

    /// The kind's reference codes.
    pub(crate) fn codes(self) -> &'static Codes {
        match self {
            Self::Epoch => &EPOCH_CODES,
            Self::Direction => &DIRECTION_CODES,
            Self::Position => &POSITION_CODES,
            Self::Frequency => &FREQUENCY_CODES,
            Self::Doppler => &DOPPLER_CODES,
            Self::RadialVelocity => &RADIAL_VELOCITY_CODES,
            Self::Baseline | Self::Uvw => &BASELINE_CODES,
            Self::EarthMagnetic => &EARTH_MAGNETIC_CODES,
        }
    }

    /// The record's `type`.
    pub(crate) fn record_type(self) -> &'static str {
        match self {
            Self::Epoch => "epoch",
            Self::Direction => "direction",
            Self::Position => "position",
            Self::Frequency => "frequency",
            Self::Doppler => "doppler",
            Self::RadialVelocity => "radialvelocity",
            Self::Baseline => "baseline",
            Self::Uvw => "uvw",
            Self::EarthMagnetic => "earthmagnetic",
        }
    }

    /// casacore's `Measure::tellMe` (`me.gettype`).
    pub(crate) fn tell_me(self) -> &'static str {
        match self {
            Self::Epoch => "Epoch",
            Self::Direction => "Direction",
            Self::Position => "Position",
            Self::Frequency => "Frequency",
            Self::Doppler => "Doppler",
            Self::RadialVelocity => "Radialvelocity",
            Self::Baseline => "Baseline",
            Self::Uvw => "uvw",
            Self::EarthMagnetic => "EarthMagnetic",
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
    /// Dimensionless Doppler convention value (velocity quantities are v/c).
    Doppler(f64),
    /// Radial velocity in m/s.
    RadialVelocity(f64),
    /// Cartesian baseline components, m.
    Baseline([f64; 3]),
    /// Cartesian uvw components, m.
    Uvw([f64; 3]),
    /// Cartesian magnetic-field components, nT.
    EarthMagnetic([f64; 3]),
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
    Doppler(MDoppler),
    RadialVelocity(MRadialVelocity),
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

/// Parsed reference and quantities, shared by scalar and vector records.
pub(crate) struct MeasureDefinition {
    pub(crate) kind: MeasureKind,
    pub(crate) refer: &'static str,
    pub(crate) quantities: Vec<Quantity>,
    pub(crate) warning: Option<String>,
}

impl Measure {
    /// The measure's kind.
    pub(crate) fn kind(&self) -> MeasureKind {
        match self.value {
            MeasureValue::Epoch(_) => MeasureKind::Epoch,
            MeasureValue::Direction(_) => MeasureKind::Direction,
            MeasureValue::Position(_) => MeasureKind::Position,
            MeasureValue::Frequency(_) => MeasureKind::Frequency,
            MeasureValue::Doppler(_) => MeasureKind::Doppler,
            MeasureValue::RadialVelocity(_) => MeasureKind::RadialVelocity,
            MeasureValue::Baseline(_) => MeasureKind::Baseline,
            MeasureValue::Uvw(_) => MeasureKind::Uvw,
            MeasureValue::EarthMagnetic(_) => MeasureKind::EarthMagnetic,
        }
    }

    /// `MeasureHolder::fromRecord`.
    pub(crate) fn from_record(
        record: &BTreeMap<String, Variant>,
    ) -> Result<ReadMeasure, HolderError> {
        let definition = Self::record_definition(record)?;
        Ok(ReadMeasure {
            measure: Self::from_values(definition.kind, definition.refer, &definition.quantities)?,
            warning: definition.warning,
        })
    }

    /// Parse the record's kind, reference and units once, before evaluating values.
    pub(crate) fn record_definition(
        record: &BTreeMap<String, Variant>,
    ) -> Result<MeasureDefinition, HolderError> {
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
            "doppler" => MeasureKind::Doppler,
            "radialvelocity" => MeasureKind::RadialVelocity,
            "baseline" => MeasureKind::Baseline,
            "uvw" => MeasureKind::Uvw,
            "earthmagnetic" => MeasureKind::EarthMagnetic,
            _ => {
                return Err(HolderError::Invalid(format!(
                    "{kind} is an unknown measure typeUnknown Measure record in \
                     MeasureHolder::fromRecord\n"
                )));
            }
        };
        let codes = kind.codes();
        let (code, warning) = match kind.resolve_code(refer) {
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
        Ok(MeasureDefinition {
            kind,
            refer: code,
            quantities: record_values(record)?,
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
            MeasureKind::Baseline => MeasureValue::Baseline(spatial_value(values, false)?),
            MeasureKind::Uvw => MeasureValue::Uvw(spatial_value(values, false)?),
            MeasureKind::EarthMagnetic => MeasureValue::EarthMagnetic(spatial_value(values, true)?),
            MeasureKind::Epoch => epoch_value(values)?,
            MeasureKind::Direction => direction_value(values)?,
            MeasureKind::Position => position_value(values)?,
            MeasureKind::Frequency => frequency_value(values)?,
            MeasureKind::Doppler => MeasureValue::Doppler(velocity_value(values, true)?),
            MeasureKind::RadialVelocity => {
                MeasureValue::RadialVelocity(velocity_value(values, false)?)
            }
        };
        Ok(Self { refer, value })
    }

    /// The ordered numeric components of `MeasureHolder::toRecord`.
    pub(crate) fn quantity_values(self) -> [f64; 3] {
        match self.value {
            MeasureValue::Epoch(mjd) => [mjd.as_mjd(), 0.0, 0.0],
            MeasureValue::Direction(cosines) => {
                let (lon, lat) = direction_angles(cosines);
                [lon, lat, 0.0]
            }
            MeasureValue::Position(xyz) => {
                let (lon, lat, length) = position_spherical(xyz);
                [lon, lat, length]
            }
            MeasureValue::Baseline(xyz) | MeasureValue::Uvw(xyz) => {
                let (lon, lat, length) = spatial_spherical(xyz);
                [lon, lat, length]
            }
            MeasureValue::Frequency(hz) => [hz, 0.0, 0.0],
            MeasureValue::Doppler(value) => [value * C_MPS, 0.0, 0.0],
            MeasureValue::RadialVelocity(value) => [value, 0.0, 0.0],
            MeasureValue::EarthMagnetic(xyz) => xyz,
        }
    }

    /// Units of the ordered record components.
    pub(crate) fn quantity_units(self) -> &'static [&'static str] {
        match self.kind() {
            MeasureKind::Epoch => &["d"],
            MeasureKind::Direction => &["rad", "rad"],
            MeasureKind::Position | MeasureKind::Baseline | MeasureKind::Uvw => {
                &["rad", "rad", "m"]
            }
            MeasureKind::EarthMagnetic => &["nT", "nT", "nT"],
            MeasureKind::Frequency => &["Hz"],
            MeasureKind::Doppler | MeasureKind::RadialVelocity => &["m/s"],
        }
    }

    /// `MeasureHolder::toRecord`.
    pub(crate) fn to_value(self) -> CasaValue {
        let values = self.quantity_values();
        let mut fields = vec![
            (
                "type".to_owned(),
                CasaValue::Text(self.kind().record_type().to_owned()),
            ),
            ("refer".to_owned(), CasaValue::Text(self.refer.to_owned())),
        ];
        for (i, &unit) in self.quantity_units().iter().enumerate() {
            fields.push((format!("m{i}"), CasaValue::quantity(values[i], unit)));
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
            MeasureValue::Baseline(_) | MeasureValue::Uvw(_) | MeasureValue::EarthMagnetic(_) => {
                return Err(
                    "geometry values use the native geometry conversion operator (#1252)".into(),
                );
            }
            MeasureValue::Epoch(mjd) => {
                let (refer, raze) = epoch_ref(self.refer).map_err(|_| unsupported("epoch"))?;
                CasaMeasure::Epoch(MEpoch::new(mjd, refer).with_razing(raze))
            }
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
            MeasureValue::Doppler(value) => {
                CasaMeasure::Doppler(MDoppler::new(value, doppler_ref(self.refer)?))
            }
            MeasureValue::RadialVelocity(value) => {
                CasaMeasure::RadialVelocity(MRadialVelocity::new(
                    value,
                    self.refer
                        .parse()
                        .map_err(|_| unsupported("radialvelocity"))?,
                ))
            }
        })
    }

    /// A casa-types measure in casacore's form.
    pub(crate) fn from_casa(measure: &CasaMeasure) -> Self {
        match measure {
            CasaMeasure::Epoch(epoch) => Self {
                refer: if epoch.is_razing() {
                    RAZED_EPOCH_CODES[epoch.refer().casacore_code() as usize]
                } else {
                    epoch.refer().casacore_name()
                },
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
            CasaMeasure::Doppler(value) => Self {
                refer: DOPPLER_CODES.resolve_or_default(value.refer().as_str()),
                value: MeasureValue::Doppler(value.value()),
            },
            CasaMeasure::RadialVelocity(value) => Self {
                refer: RADIAL_VELOCITY_CODES.resolve_or_default(value.refer().as_str()),
                value: MeasureValue::RadialVelocity(value.ms()),
            },
        }
    }
}

// MVBaseline/MVuvw putValue, and coherent magnetic-field record quantities.
fn spatial_value(values: &[Quantity], magnetic: bool) -> Result<[f64; 3], HolderError> {
    let [a, b, c] = values else {
        return Err(illegal_quantity());
    };
    let magnitude = if magnetic {
        casa_types::quanta::Unit::new("T").expect("tesla").val().dim
    } else {
        UnitDim::basis(Dimension::Length)
    };
    let angle = UnitDim::basis(Dimension::Angle);
    let is_magnitude = |q: &Quantity| q.unit().val().dim == magnitude;
    let is_angle = |q: &Quantity| q.unit().val().dim == angle;
    let scale = if magnetic { 1e9 } else { 1.0 };
    if values.iter().all(is_magnitude) {
        return Ok([
            a.get_si_value() * scale,
            b.get_si_value() * scale,
            c.get_si_value() * scale,
        ]);
    }
    let (length, lon, lat) = if is_magnitude(a) && is_angle(b) && is_angle(c) {
        (a, b, c)
    } else if is_angle(a) && is_angle(b) && is_magnitude(c) {
        (c, a, b)
    } else {
        return Err(illegal_quantity());
    };
    let length = length.get_si_value() * scale;
    let length = if length == 0.0 {
        if magnetic { 1e-6 } else { 1e-12 }
    } else {
        length
    };
    let unit = direction_cosines(lon.get_si_value(), lat.get_si_value());
    Ok(unit.map(|x| x * length))
}

fn velocity_value(values: &[Quantity], doppler: bool) -> Result<f64, HolderError> {
    let value = match values {
        [] => return Ok(0.0),
        [value] => value,
        _ => return Err(illegal_quantity()),
    };
    if doppler && value.unit().val().dim.is_dimensionless() {
        return Ok(value.get_si_value());
    }
    let speed = UnitDim::basis(Dimension::Length) / UnitDim::basis(Dimension::Time);
    if value.unit().val().dim != speed {
        return Err(illegal_quantity());
    }
    Ok(value.get_si_value() / if doppler { C_MPS } else { 1.0 })
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

pub(crate) use casa_types::quanta::casacore::frequency_hz;

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
    let (longitude, latitude, mut length) = spatial_spherical(xyz);
    if length > LO_LIMIT_M && length < HI_LIMIT_M {
        length = (length - HI_LIMIT_M) * 1e7;
    }
    (longitude, latitude, length)
}

/// Raw vector angles and magnitude without position height decoding.
pub(crate) fn spatial_spherical(xyz: [f64; 3]) -> (f64, f64, f64) {
    let length = norm(xyz);
    let longitude = if xyz[0] != 0.0 || xyz[1] != 0.0 {
        xyz[1].atan2(xyz[0])
    } else {
        0.0
    };
    let latitude = (xyz[2] / if length == 0.0 { 1.0 } else { length }).asin();
    (longitude, latitude, length)
}

#[cfg(test)]
#[path = "holder_tests.rs"]
mod tests;
