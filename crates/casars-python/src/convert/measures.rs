// SPDX-License-Identifier: LGPL-3.0-or-later
//! Measures from CASA arguments: casatools' `casaMEpoch`, `casaMDirection`
//! and `casaMPosition`, and the reference codes they and `MeasureHolder`
//! accept.
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
//! - **Records** are `MeasureHolder` records,
//!   `{'type', 'refer', 'm0', 'm1', ...}`, with each `m` a quantity record in
//!   any conformant unit. A position record is longitude, latitude and a
//!   length (the geocentric radius for ITRF, as `me.observatory` writes it,
//!   and the height for WGS84), or three lengths x, y and z.
//!
//! Divergence: where a string's value after the first does not parse, CASA
//! silently reuses the previous value (`"J2000 12h00m00 garbage"` is a
//! declination of 180 degrees); this raises instead.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame, MeasuresProvider};
use casa_types::quanta::{Dimension, Quantity, UnitDim, unique_prefix_index};

use super::{ConvertError, ConvertResult, Variant, quantity_record, value_in};

/// casacore's `MEpoch` reference codes, in `MEpoch::allMyTypes` order.
const EPOCH_CODES: [&str; 17] = [
    "LAST", "LMST", "GMST1", "GAST", "UT1", "UT2", "UTC", "TAI", "TDT", "TCG", "TDB", "TCB", "IAT",
    "GMST", "TT", "ET", "UT",
];

/// casacore's `MDirection` reference codes, in `MDirection::allMyTypes`
/// order.
const DIRECTION_CODES: [&str; 35] = [
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
];

/// casacore's `MPosition` reference codes.
const POSITION_CODES: [&str; 2] = ["ITRF", "WGS84"];

/// casacore's `MFrequency` reference codes, in `MFrequency::allMyTypes`
/// order.
const FREQUENCY_CODES: [&str; 10] = [
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
];

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
                    let refer = reference_code(frame, &EPOCH_CODES)
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
        Variant::Bool(_) | Variant::Numbers(_) | Variant::Other(_) => Err(ConvertError::Type(
            format!("cannot convert {} to an epoch", value.describe()),
        )),
    }
}

/// CASA's `casaMDirection`: a record, `"frame lon lat"`, `"lon lat"`
/// (J2000), or one name (a frame's pole, or a catalogue source). Other
/// values are CASA's `false`.
pub(crate) fn direction(
    value: &Variant,
    measures: &Arc<dyn MeasuresProvider>,
) -> ConvertResult<Option<MDirection>> {
    match value {
        Variant::Record(record) => direction_record(record).map(Some),
        Variant::Text(_) | Variant::Texts(_) => match tokens(value).as_slice() {
            [frame, longitude, latitude] => {
                let refer = reference_code(frame, &DIRECTION_CODES)
                    .map_or(Ok(DirectionRef::J2000), direction_reference)?;
                direction_of(longitude, latitude, refer).map(Some)
            }
            [longitude, latitude] => {
                direction_of(longitude, latitude, DirectionRef::J2000).map(Some)
            }
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
                let wgs84 = reference_code(frame, &POSITION_CODES) == Some("WGS84");
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
    let name = reference_code(&upper, &FREQUENCY_CODES).ok_or_else(invalid)?;
    FrequencyRef::from_str(name).map_err(|_| {
        ConvertError::runtime(format!(
            "the {name} frequency frame is not yet implemented in casa-rs"
        ))
    })
}

/// The canonical code `input` names by unique prefix, if any.
fn reference_code(input: &str, codes: &[&'static str]) -> Option<&'static str> {
    unique_prefix_index(input, codes).map(|index| codes[index])
}

fn epoch_reference(code: &str) -> ConvertResult<EpochRef> {
    EpochRef::from_str(code).map_err(|error| ConvertError::runtime(error.to_string()))
}

fn direction_reference(code: &str) -> ConvertResult<DirectionRef> {
    let code = match code {
        "AZELNE" => "AZEL",
        "AZELNEGEO" => "AZELGEO",
        other => other,
    };
    DirectionRef::from_str(code).map_err(|_| {
        ConvertError::runtime(format!(
            "the {code} direction frame is not yet implemented in casa-rs"
        ))
    })
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
fn direction_of(longitude: &str, latitude: &str, refer: DirectionRef) -> ConvertResult<MDirection> {
    let longitude = read_value(longitude)?;
    let latitude = read_value(latitude).map_err(|_| {
        ConvertError::runtime(format!(
            "Could not read the direction latitude {latitude:?}"
        ))
    })?;
    let latitude = angle_rad(&latitude)?;
    let longitude = angle_rad(&longitude)?;
    Ok(MDirection::from_angles(longitude, latitude, refer))
}

/// A name alone: a frame's pole, or a catalogue source; otherwise CASA's
/// `false`.
fn named_direction(
    name: &str,
    measures: &Arc<dyn MeasuresProvider>,
) -> ConvertResult<Option<MDirection>> {
    if let Some(code) = reference_code(name, &DIRECTION_CODES) {
        let refer = direction_reference(code)?;
        return Ok(Some(MDirection::from_angles(
            0.0,
            std::f64::consts::FRAC_PI_2,
            refer,
        )));
    }
    let source = measures
        .source(name)
        .map_err(|error| ConvertError::runtime(format!("source catalogue: {error}")))?;
    if source.is_none() {
        return Ok(None);
    }
    let frame = MeasFrame::new().with_measures(Arc::clone(measures));
    MDirection::from_source_name(name, &frame)
        .map(Some)
        .map_err(|error| ConvertError::runtime(error.to_string()))
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

/// The parts of a `MeasureHolder` record: its reference code and values.
struct MeasureRecord {
    kind: String,
    refer: String,
    values: Vec<Quantity>,
}

/// Reads a `MeasureHolder` record (`MeasureHolder::fromRecord`).
fn measure_record(record: &BTreeMap<String, Variant>) -> Result<Option<MeasureRecord>, String> {
    let (Some(Variant::Text(kind)), Some(Variant::Text(refer))) =
        (record.get("type"), record.get("refer"))
    else {
        return Ok(None);
    };
    if record.contains_key("offset") {
        return Err("measure offsets are not yet implemented in casa-rs".to_string());
    }
    let mut values = Vec::new();
    for key in ["m0", "m1", "m2"] {
        match record.get(key) {
            Some(Variant::Record(value)) => values.push(quantity_record(value)?),
            _ => break,
        }
    }
    Ok(Some(MeasureRecord {
        kind: kind.to_ascii_lowercase(),
        refer: refer.clone(),
        values,
    }))
}

fn illegal_quantity() -> String {
    "Illegal quantity in MeasureHolder::fromRecord\n".to_string()
}

/// An epoch record, or a quantity record taken as UTC.
fn epoch_record(record: &BTreeMap<String, Variant>) -> ConvertResult<MEpoch> {
    let wrap = |error: String| {
        ConvertError::runtime(format!("Error {error}In converting Epoch parameter"))
    };
    match measure_record(record).map_err(wrap)? {
        Some(measure) => {
            if measure.kind != "epoch" {
                return Err(ConvertError::runtime(
                    "Empty or wrong MeasureHolder for asMEpoch",
                ));
            }
            let refer = reference_code(&measure.refer, &EPOCH_CODES)
                .map_or(Ok(EpochRef::UTC), epoch_reference)?;
            let time = UnitDim::basis(Dimension::Time);
            if measure.values.is_empty()
                || measure
                    .values
                    .iter()
                    .any(|value| value.unit().val().dim != time)
            {
                return Err(wrap(illegal_quantity()));
            }
            let mut days = 0.0;
            for value in &measure.values {
                days += value_in(value, "d")?;
            }
            Ok(MEpoch::from_mjd(days, refer))
        }
        None => {
            let quantity = quantity_record(record).map_err(wrap)?;
            epoch_of(&quantity, EpochRef::UTC)
        }
    }
}

/// A direction record: two angles, or three direction cosines.
fn direction_record(record: &BTreeMap<String, Variant>) -> ConvertResult<MDirection> {
    let wrap = |error: String| {
        ConvertError::runtime(format!("Error {error} in converting Direction parameter"))
    };
    let measure = measure_record(record)
        .map_err(wrap)?
        .ok_or_else(|| wrap("Illegal Measure record in MeasureHolder::fromRecord\n".to_string()))?;
    if measure.kind != "direction" {
        return Err(ConvertError::runtime(
            "Empty or wrong MeasureHolder for asMDirection",
        ));
    }
    let refer = reference_code(&measure.refer, &DIRECTION_CODES)
        .map_or(Ok(DirectionRef::J2000), direction_reference)?;
    let values = &measure.values;
    if values.len() == 3
        && values
            .iter()
            .all(|value| value.unit().val().dim.is_dimensionless())
    {
        let cosines = [values[0].value(), values[1].value(), values[2].value()];
        let norm = cosines
            .iter()
            .map(|value| value * value)
            .sum::<f64>()
            .sqrt();
        if norm == 0.0 {
            return Err(wrap(illegal_quantity()));
        }
        return Ok(MDirection::from_cosines(
            cosines.map(|value| value / norm),
            refer,
        ));
    }
    let angle = UnitDim::basis(Dimension::Angle);
    if values.iter().any(|value| value.unit().val().dim != angle) {
        return Err(wrap(illegal_quantity()));
    }
    Ok(match values.as_slice() {
        [] => MDirection::from_angles(0.0, std::f64::consts::FRAC_PI_2, refer),
        [longitude] => MDirection::from_angles(longitude.get_si_value(), 0.0, refer),
        [longitude, latitude, ..] => {
            MDirection::from_angles(longitude.get_si_value(), latitude.get_si_value(), refer)
        }
    })
}

/// A position record (`MVPosition::putValue`).
fn position_record(record: &BTreeMap<String, Variant>) -> ConvertResult<MPosition> {
    let wrap = |error: String| {
        ConvertError::runtime(format!("Error {error}In converting Position parameter"))
    };
    let measure = measure_record(record)
        .map_err(wrap)?
        .ok_or_else(|| wrap("Illegal Measure record in MeasureHolder::fromRecord\n".to_string()))?;
    if measure.kind != "position" {
        return Err(ConvertError::runtime(
            "Empty or wrong MeasureHolder for asMPosition",
        ));
    }
    let wgs84 = reference_code(&measure.refer, &POSITION_CODES) == Some("WGS84");
    let [first, second, third] = measure.values.as_slice() else {
        return Err(wrap(illegal_quantity()));
    };
    let is = |value: &Quantity, wanted: Dimension| value.unit().val().dim == UnitDim::basis(wanted);
    let (length, angle) = (Dimension::Length, Dimension::Angle);
    if is(first, length) && is(second, length) && is(third, length) {
        let xyz = [
            first.get_si_value(),
            second.get_si_value(),
            third.get_si_value(),
        ];
        if wgs84 {
            let radius = xyz.iter().map(|value| value * value).sum::<f64>().sqrt();
            let longitude = xyz[1].atan2(xyz[0]);
            let latitude = xyz[2].atan2(xyz[0].hypot(xyz[1]));
            return Ok(MPosition::new_wgs84(longitude, latitude, radius));
        }
        return Ok(MPosition::new_itrf(xyz[0], xyz[1], xyz[2]));
    }
    if is(first, length) && is(second, angle) && is(third, angle) {
        return Ok(spherical_position(
            second.get_si_value(),
            third.get_si_value(),
            first.get_si_value(),
            wgs84,
        ));
    }
    if is(third, length) && is(first, angle) && is(second, angle) {
        return Ok(spherical_position(
            first.get_si_value(),
            second.get_si_value(),
            third.get_si_value(),
            wgs84,
        ));
    }
    Err(wrap(illegal_quantity()))
}

#[cfg(test)]
#[path = "measures_tests.rs"]
mod tests;
