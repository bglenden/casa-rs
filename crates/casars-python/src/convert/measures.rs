// SPDX-License-Identifier: LGPL-3.0-or-later
//! Measures from CASA arguments: casatools' `casaMEpoch`, `casaMDirection`,
//! `casaMPosition` and `casaMFrequency` ([`frequency`]), and the reference
//! codes they accept.
//!
//! Each function returns `Ok(None)` where CASA's returns `false` (the
//! calling method then logs its own SEVERE message and continues with a
//! default measure) and an error where CASA raises.
//!
//! - **Strings** are split at commas, or else at blanks, into a frame and
//!   values: `"J2000 12h00m00 34d00m00"`, `"UTC 2020/01/01/00:00:00"`,
//!   `"ITRF 6378137m 10deg 20deg"` (a position string is a length, then a
//!   longitude and a latitude). A frame code may be any unique prefix
//!   (`j20`); an unknown one falls back to the default frame, as in CASA.
//!   A single token is a frame (its pole, for a direction), a catalogue
//!   source or an observatory.
//! - **Records** are `MeasureHolder` records, read by
//!   [`Measure::from_record`](super::holder::Measure::from_record).
//!
//! [`direction_measure`] keeps a direction in casacore's form, with any
//! reference code (`me.source('SUN')`); [`direction`] converts it to
//! casa-types' form, which `sm` needs.
//!
//! Divergence: where a string's value after the first does not parse, CASA
//! silently reuses the previous value (`"J2000 12h00m00 garbage"` is a
//! declination of 180 degrees); this raises instead.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use casa_types::measures::direction::MDirection;
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasuresProvider};
use casa_types::quanta::{Dimension, Quantity, UnitDim};

use super::holder::{
    CasaMeasure, DIRECTION_CODES, EPOCH_CODES, FREQUENCY_CODES, HolderError, Measure, MeasureKind,
    MeasureValue, POSITION_CODES, direction_cosines, frequency_hz,
};
use super::{ConvertError, ConvertResult, Variant, quantity_record, value_in};

/// The message casacore raises for a value it could not read at all.
const EMPTY_HOLDER: &str = "Empty QuantumHolder argument for asQuantumDouble";

/// CASA's `casaMEpoch`: a record, `"frame value"`, `"value"` (UTC), or a
/// number of MJD days (UTC, so `[]` is MJD 0). `""` is CASA's `false`.
pub(crate) fn epoch(value: &Variant) -> ConvertResult<Option<MEpoch>> {
    match value {
        Variant::Record(record) => epoch_record(record).map(Some),
        Variant::Text(_) | Variant::Texts(_) => {
            let tokens = tokens(value);
            match tokens.as_slice() {
                [only] if only.is_empty() => Ok(None),
                [only] => epoch_of(&read_value(only)?, EpochRef::UTC).map(Some),
                [frame, text] => {
                    let refer = EPOCH_CODES
                        .resolve(frame)
                        .map_or(Ok(EpochRef::UTC), epoch_reference)?;
                    epoch_of(&read_value(text)?, refer).map(Some)
                }
                _ => Err(ConvertError::runtime(format!(
                    "Could not understand Epoch parameter {}",
                    value.describe()
                ))),
            }
        }
        Variant::Empty => Ok(Some(MEpoch::from_mjd(0.0, EpochRef::UTC))),
        Variant::Number(days) => Ok(Some(MEpoch::from_mjd(*days, EpochRef::UTC))),
        Variant::Bool(_)
        | Variant::Numbers(_)
        | Variant::Complex(_)
        | Variant::Complexes(_)
        | Variant::Array { .. }
        | Variant::Other(_) => Err(ConvertError::Type(format!(
            "cannot convert {} to an epoch",
            value.describe()
        ))),
    }
}

/// CASA's `casaMDirection` in casa-types' form, failing for a reference code
/// casa-types does not convert; see [`direction_measure`].
pub(crate) fn direction(
    value: &Variant,
    measures: &Arc<dyn MeasuresProvider>,
) -> ConvertResult<Option<MDirection>> {
    direction_measure(value, measures)?
        .map(
            |measure| match measure.to_casa().map_err(ConvertError::runtime)? {
                CasaMeasure::Direction(direction) => Ok(direction),
                _ => unreachable!("a direction measure converts to a direction"),
            },
        )
        .transpose()
}

/// CASA's `casaMDirection`: a record, `"frame lon lat"`, `"lon lat"`
/// (J2000), or one name (a frame's pole, or a catalogue source). Other
/// values are CASA's `false`.
pub(crate) fn direction_measure(
    value: &Variant,
    measures: &Arc<dyn MeasuresProvider>,
) -> ConvertResult<Option<Measure>> {
    match value {
        Variant::Record(record) => direction_record(record).map(Some),
        Variant::Text(_) | Variant::Texts(_) => match tokens(value).as_slice() {
            [frame, longitude, latitude] => {
                let refer = DIRECTION_CODES.resolve_or_default(frame);
                direction_of(longitude, latitude, refer).map(Some)
            }
            [longitude, latitude] => direction_of(longitude, latitude, "J2000").map(Some),
            [name] => named_direction(name, measures),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

/// CASA's `casaMPosition`: a record, `"frame length lon lat"`,
/// `"length lon lat"` (ITRF), or an observatory name. Other values are CASA's
/// `false`.
pub(crate) fn position(
    value: &Variant,
    measures: &Arc<dyn MeasuresProvider>,
) -> ConvertResult<Option<MPosition>> {
    match value {
        Variant::Record(record) => position_record(record).map(Some),
        Variant::Text(_) | Variant::Texts(_) => match tokens(value).as_slice() {
            [frame, length, longitude, latitude] => {
                let wgs84 = POSITION_CODES.resolve(frame) == Some("WGS84");
                position_of(length, longitude, latitude, wgs84).map(Some)
            }
            [length, longitude, latitude] => {
                position_of(length, longitude, latitude, false).map(Some)
            }
            [name] => MPosition::from_observatory_name(&name.to_ascii_uppercase(), &**measures)
                .map_err(|error| ConvertError::runtime(error.to_string()))?
                .map(Some)
                .ok_or_else(|| ConvertError::runtime("Could not understand Position parameter")),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

/// `MFrequency::getType` on an upper-cased `refcode`, as `sm.setspwindow`
/// reads it; an empty code is TOPO.
pub(crate) fn frequency_reference(code: &str) -> ConvertResult<FrequencyRef> {
    if code.is_empty() {
        return Ok(FrequencyRef::TOPO);
    }
    let upper = code.to_ascii_uppercase();
    let invalid = || ConvertError::runtime(format!("Invalid frequency reference '{upper}'"));
    let name = FREQUENCY_CODES.resolve(&upper).ok_or_else(invalid)?;
    FrequencyRef::from_str(name).map_err(|_| {
        ConvertError::runtime(format!(
            "the {name} frequency frame is not yet implemented in casa-rs"
        ))
    })
}

/// CASA's `casaMFrequency`: a frequency record, or a quantity record
/// (LSRK); `"frame value"`; `"value"` (LSRK); or a number of Hz (LSRK). An
/// unknown frame is LSRK, as CASA defaults it (CASA prints a warning to
/// stderr; this one is returned), and the two tokens are read again as one,
/// so that a spaced quantity (`"1.4 GHz"`) keeps its value; when they do
/// not read as one, the second token's value stands (`"foo 1.4GHz"`). `""`
/// is CASA's `false`.
pub(crate) fn frequency(value: &Variant) -> ConvertResult<(Option<MFrequency>, Option<String>)> {
    match value {
        Variant::Record(record) => {
            frequency_record(record).map(|frequency| (Some(frequency), None))
        }
        Variant::Text(_) | Variant::Texts(_) => match tokens(value).as_slice() {
            [frame, text] => {
                let quantity = read_value(text)?;
                let (refer, quantity, warning) = match FREQUENCY_CODES.resolve(frame) {
                    Some(code) => (frequency_frame(code)?, quantity, None),
                    None => (
                        FrequencyRef::LSRK,
                        Quantity::read(&format!("{frame}{text}")).unwrap_or(quantity),
                        Some(
                            "Could not understand frequency frame..defaulting to LSRK".to_string(),
                        ),
                    ),
                };
                Ok((Some(frequency_of(&quantity, refer)?), warning))
            }
            [only] if only.is_empty() => Ok((None, None)),
            [only] => Ok((
                Some(frequency_of(&read_value(only)?, FrequencyRef::LSRK)?),
                None,
            )),
            _ => Ok((None, None)),
        },
        Variant::Number(hz) => Ok((Some(MFrequency::new(*hz, FrequencyRef::LSRK)), None)),
        Variant::Empty => Ok((Some(MFrequency::new(0.0, FrequencyRef::LSRK)), None)),
        Variant::Bool(_)
        | Variant::Numbers(_)
        | Variant::Complex(_)
        | Variant::Complexes(_)
        | Variant::Array { .. }
        | Variant::Other(_) => Err(ConvertError::Type(format!(
            "cannot convert {} to a frequency",
            value.describe()
        ))),
    }
}

/// A frequency record, or a quantity record taken as LSRK.
fn frequency_record(record: &BTreeMap<String, Variant>) -> ConvertResult<MFrequency> {
    let wrap = |error: String| {
        ConvertError::runtime(format!("Error {error}In converting Frequency parameter"))
    };
    match measure_of_kind(record, MeasureKind::Frequency, wrap)? {
        Some(measure) => match measure.to_casa().map_err(ConvertError::runtime)? {
            CasaMeasure::Frequency(frequency) => Ok(frequency),
            _ => unreachable!("a frequency measure converts to a frequency"),
        },
        None => {
            let quantity = quantity_record(record).map_err(wrap)?;
            frequency_of(&quantity, FrequencyRef::LSRK)
        }
    }
}

/// `MFrequency(quantity, refer)`: a frequency, period, wavelength, energy
/// or momentum (`MVFrequency::makeF`).
fn frequency_of(quantity: &Quantity, refer: FrequencyRef) -> ConvertResult<MFrequency> {
    let hz = frequency_hz(quantity).ok_or_else(|| {
        ConvertError::runtime(format!(
            "Illegal unit type '{}' for a frequency",
            quantity.unit().name()
        ))
    })?;
    Ok(MFrequency::new(hz, refer))
}

/// A frequency frame casa-types converts.
fn frequency_frame(code: &str) -> ConvertResult<FrequencyRef> {
    FrequencyRef::from_str(code).map_err(|_| {
        ConvertError::runtime(format!(
            "the {code} frequency frame is not yet implemented in casa-rs"
        ))
    })
}

fn epoch_reference(code: &str) -> ConvertResult<EpochRef> {
    EpochRef::from_str(code).map_err(|error| ConvertError::runtime(error.to_string()))
}

/// casatools' `sepCommaEmptyToVectorStrings` for a `str`, or the strings of
/// a string list.
fn tokens(value: &Variant) -> Vec<String> {
    match value {
        Variant::Text(text) => {
            let separator = if text.contains(',') { ',' } else { ' ' };
            if !text.contains(separator) {
                return vec![text.clone()];
            }
            text.split(separator)
                .filter(|token| !token.is_empty() && *token != " ")
                .map(str::to_string)
                .collect()
        }
        Variant::Texts(texts) => texts.clone(),
        _ => Vec::new(),
    }
}

/// One value of a measure string, raising casacore's message when it does not
/// read.
fn read_value(text: &str) -> ConvertResult<Quantity> {
    Quantity::read(text)
        .map_err(|_| ConvertError::runtime(format!("{EMPTY_HOLDER} (could not read {text:?})")))
}

/// `MEpoch(quantity, refer)`, which requires a time.
fn epoch_of(quantity: &Quantity, refer: EpochRef) -> ConvertResult<MEpoch> {
    require(quantity, Dimension::Time, "assure non-conforming unit type")?;
    Ok(MEpoch::from_mjd(value_in(quantity, "d")?, refer))
}

/// `MDirection(lon, lat, refer)`: casacore's `cos` and `sin` require angles
/// (the latitude is checked first), so `"J2000 10h 20d"` raises.
fn direction_of(longitude: &str, latitude: &str, refer: &'static str) -> ConvertResult<Measure> {
    let longitude = read_value(longitude)?;
    let latitude = read_value(latitude).map_err(|_| {
        ConvertError::runtime(format!(
            "Could not read the direction latitude {latitude:?}"
        ))
    })?;
    let latitude = angle_rad(&latitude)?;
    let longitude = angle_rad(&longitude)?;
    Ok(Measure {
        refer,
        value: MeasureValue::Direction(direction_cosines(longitude, latitude)),
    })
}

/// A name alone: a frame's pole, or a catalogue source; otherwise CASA's
/// `false`.
fn named_direction(
    name: &str,
    measures: &Arc<dyn MeasuresProvider>,
) -> ConvertResult<Option<Measure>> {
    if let Some(refer) = DIRECTION_CODES.resolve(name) {
        return Ok(Some(Measure {
            refer,
            value: MeasureValue::Direction([0.0, 0.0, 1.0]),
        }));
    }
    let source = measures
        .source(name)
        .map_err(|error| ConvertError::runtime(format!("source catalogue: {error}")))?;
    Ok(source.map(|source| Measure {
        refer: DIRECTION_CODES.resolve_or_default(&source.reference),
        value: MeasureValue::Direction(direction_cosines(
            source.longitude_rad,
            source.latitude_rad,
        )),
    }))
}

/// `MPosition(length, lon, lat, refer)`; casacore checks the latitude, then
/// the longitude, then the length.
fn position_of(
    length: &str,
    longitude: &str,
    latitude: &str,
    wgs84: bool,
) -> ConvertResult<MPosition> {
    let length = read_value(length)?;
    let longitude = read_value(longitude).map_err(|_| {
        ConvertError::runtime(format!(
            "Could not read the position longitude {longitude:?}"
        ))
    })?;
    let latitude = read_value(latitude).map_err(|_| {
        ConvertError::runtime(format!("Could not read the position latitude {latitude:?}"))
    })?;
    let latitude = angle_rad(&latitude)?;
    let longitude = angle_rad(&longitude)?;
    require(
        &length,
        Dimension::Length,
        "assure non-conforming unit type",
    )?;
    Ok(spherical_position(
        longitude,
        latitude,
        length.get_si_value(),
        wgs84,
    ))
}

/// A position from spherical coordinates: ITRF with a geocentric radius, or
/// WGS84 with a height.
fn spherical_position(longitude: f64, latitude: f64, length_m: f64, wgs84: bool) -> MPosition {
    if wgs84 {
        MPosition::new_wgs84(longitude, latitude, length_m)
    } else {
        let (sin_lon, cos_lon) = longitude.sin_cos();
        let (sin_lat, cos_lat) = latitude.sin_cos();
        MPosition::new_itrf(
            length_m * cos_lat * cos_lon,
            length_m * cos_lat * sin_lon,
            length_m * sin_lat,
        )
    }
}

/// An angle in radians, raising casacore's `cos` message otherwise.
fn angle_rad(quantity: &Quantity) -> ConvertResult<f64> {
    require(quantity, Dimension::Angle, "cos illegal unit type")?;
    Ok(quantity.get_si_value())
}

/// Raises `Quantum::<what> '<unit>'` unless `quantity` has dimension
/// `wanted`.
fn require(quantity: &Quantity, wanted: Dimension, what: &str) -> ConvertResult<()> {
    if quantity.unit().val().dim == UnitDim::basis(wanted) {
        Ok(())
    } else {
        Err(ConvertError::runtime(format!(
            "Quantum::{what} '{}'",
            quantity.unit().name()
        )))
    }
}

/// A measure record of `kind` (`MeasureHolder::fromRecord` and `as<Kind>`):
/// `Ok(None)` for a record that is not a measure record; errors are wrapped
/// by `wrap` as CASA wraps them.
fn measure_of_kind(
    record: &BTreeMap<String, Variant>,
    kind: MeasureKind,
    wrap: impl Fn(String) -> ConvertError,
) -> ConvertResult<Option<Measure>> {
    match Measure::from_record(record) {
        Ok(read) if read.measure.kind() == kind => Ok(Some(read.measure)),
        Ok(_) => Err(ConvertError::runtime(format!(
            "Empty or wrong MeasureHolder for asM{}",
            kind.tell_me()
        ))),
        Err(HolderError::NotAMeasure) => Ok(None),
        Err(error) => Err(wrap(error.message())),
    }
}

/// An epoch record, or a quantity record taken as UTC.
fn epoch_record(record: &BTreeMap<String, Variant>) -> ConvertResult<MEpoch> {
    let wrap = |error: String| {
        ConvertError::runtime(format!("Error {error}In converting Epoch parameter"))
    };
    match measure_of_kind(record, MeasureKind::Epoch, wrap)? {
        Some(measure) => match measure.to_casa().map_err(ConvertError::runtime)? {
            CasaMeasure::Epoch(epoch) => Ok(epoch),
            _ => unreachable!("an epoch measure converts to an epoch"),
        },
        None => {
            let quantity = quantity_record(record).map_err(wrap)?;
            epoch_of(&quantity, EpochRef::UTC)
        }
    }
}

/// A direction record: two angles, or three direction cosines.
fn direction_record(record: &BTreeMap<String, Variant>) -> ConvertResult<Measure> {
    let wrap = |error: String| {
        ConvertError::runtime(format!("Error {error} in converting Direction parameter"))
    };
    measure_of_kind(record, MeasureKind::Direction, wrap)?
        .ok_or_else(|| wrap(HolderError::NotAMeasure.message()))
}

/// A position record (`MVPosition::putValue`).
fn position_record(record: &BTreeMap<String, Variant>) -> ConvertResult<MPosition> {
    let wrap = |error: String| {
        ConvertError::runtime(format!("Error {error}In converting Position parameter"))
    };
    let measure = measure_of_kind(record, MeasureKind::Position, wrap)?
        .ok_or_else(|| wrap(HolderError::NotAMeasure.message()))?;
    match measure.to_casa().map_err(ConvertError::runtime)? {
        CasaMeasure::Position(position) => Ok(position),
        _ => unreachable!("a position measure converts to a position"),
    }
}

#[cfg(test)]
#[path = "measures_tests.rs"]
mod tests;
