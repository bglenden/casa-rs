// SPDX-License-Identifier: LGPL-3.0-or-later
//! Record serialization for measures, compatible with C++ `MeasureHolder`.
//!
//! C++ casacore stores measures in `TableRecord` values with a specific field
//! layout. This module provides `to_record` / `from_record` functions that
//! produce and consume [`RecordValue`] in the same format, ensuring
//! interoperability with C++ tables.
//!
// TODO: The record functions depend on `RecordValue` / `Value` / `ScalarValue` from
// the tables layer. When `casacore-tables` is split into its own crate these types
// will need a shared dependency (e.g. a `casacore-record` crate) so that
// `casacore-types` does not pull in table-storage details.
//!
//! ## MEpoch record format
//!
//! ```text
//! type:  "epoch"
//! refer: "UTC"          (EpochRef::as_str())
//! m0:    { value: <f64>, unit: "d" }
//! ```
//!
//! ## MPosition record format
//!
//! ```text
//! type:  "position"
//! refer: "ITRF"
//! m0:    { value: <lon_rad>, unit: "rad" }
//! m1:    { value: <lat_rad>, unit: "rad" }
//! m2:    { value: <radius_m>, unit: "m" }
//! ```
//!
//! Note: C++ always serializes MPosition in spherical form (lon, lat, radius)
//! regardless of reference type.

use crate::{RecordField, RecordValue, ScalarValue, Value};

use super::direction::{DirectionRef, MDirection};
use super::doppler::{DopplerRef, MDoppler};
use super::epoch::{EpochRef, MEpoch, MjdHighPrec};
use super::error::MeasureError;
use super::frequency::{FrequencyRef, MFrequency};
use super::position::{MPosition, PositionRef};
use super::radial_velocity::{MRadialVelocity, RadialVelocityRef};

/// Creates a quantity-like sub-record `{ value: <v>, unit: <u> }`.
fn quantity_record(value: f64, unit: &str) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new("value", Value::Scalar(ScalarValue::Float64(value))),
        RecordField::new("unit", Value::Scalar(ScalarValue::String(unit.to_owned()))),
    ])
}

/// Extracts an f64 from a quantity sub-record.
fn extract_quantity_value(rec: &RecordValue) -> Result<f64, MeasureError> {
    match rec.get("value") {
        Some(Value::Scalar(ScalarValue::Float64(v))) => Ok(*v),
        _ => Err(MeasureError::InvalidRecord {
            reason: "quantity sub-record missing 'value' field".to_owned(),
        }),
    }
}

/// Extracts a string field from a record.
fn extract_string(rec: &RecordValue, field: &str) -> Result<String, MeasureError> {
    match rec.get(field) {
        Some(Value::Scalar(ScalarValue::String(s))) => Ok(s.clone()),
        _ => Err(MeasureError::InvalidRecord {
            reason: format!("missing or non-string field '{field}'"),
        }),
    }
}

/// Extracts a sub-record from a record field.
fn extract_sub_record<'a>(
    rec: &'a RecordValue,
    field: &str,
) -> Result<&'a RecordValue, MeasureError> {
    match rec.get(field) {
        Some(Value::Record(r)) => Ok(r),
        _ => Err(MeasureError::InvalidRecord {
            reason: format!("missing or non-record field '{field}'"),
        }),
    }
}

// ---------------------------------------------------------------------------
// MEpoch
// ---------------------------------------------------------------------------

/// Serializes an [`MEpoch`] to a [`RecordValue`] compatible with C++ `MeasureHolder`.
pub fn epoch_to_record(epoch: &MEpoch) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new(
            "type",
            Value::Scalar(ScalarValue::String("epoch".to_owned())),
        ),
        RecordField::new(
            "refer",
            Value::Scalar(ScalarValue::String(epoch.refer().as_str().to_owned())),
        ),
        RecordField::new(
            "m0",
            Value::Record(quantity_record(epoch.value().as_mjd(), "d")),
        ),
    ])
}

/// Deserializes an [`MEpoch`] from a [`RecordValue`] in C++ `MeasureHolder` format.
pub fn epoch_from_record(rec: &RecordValue) -> Result<MEpoch, MeasureError> {
    let type_str = extract_string(rec, "type")?;
    if type_str != "epoch" {
        return Err(MeasureError::InvalidRecord {
            reason: format!("expected type 'epoch', got '{type_str}'"),
        });
    }

    let refer_str = extract_string(rec, "refer")?;
    let refer: EpochRef = refer_str.parse()?;

    let m0 = extract_sub_record(rec, "m0")?;
    let mjd = extract_quantity_value(m0)?;

    Ok(MEpoch::new(MjdHighPrec::from_mjd(mjd), refer))
}

// ---------------------------------------------------------------------------
// MPosition
// ---------------------------------------------------------------------------

/// Serializes an [`MPosition`] to a [`RecordValue`] compatible with C++ `MeasureHolder`.
///
/// C++ `MVPosition::getRecordValue()` always stores positions in geocentric
/// spherical form (lon, lat, radius) for ITRF. For WGS84, we store the
/// geodetic coordinates (lon, lat, height) as-is.
pub fn position_to_record(pos: &MPosition) -> RecordValue {
    let (v0, v1, v2, u0, u1, u2) = match pos.refer() {
        PositionRef::ITRF => {
            let (lon, lat, r) = pos.as_spherical();
            (lon, lat, r, "rad", "rad", "m")
        }
        PositionRef::WGS84 => {
            let vals = pos.values();
            (vals[0], vals[1], vals[2], "rad", "rad", "m")
        }
    };

    RecordValue::new(vec![
        RecordField::new(
            "type",
            Value::Scalar(ScalarValue::String("position".to_owned())),
        ),
        RecordField::new(
            "refer",
            Value::Scalar(ScalarValue::String(pos.refer().as_str().to_owned())),
        ),
        RecordField::new("m0", Value::Record(quantity_record(v0, u0))),
        RecordField::new("m1", Value::Record(quantity_record(v1, u1))),
        RecordField::new("m2", Value::Record(quantity_record(v2, u2))),
    ])
}

/// Deserializes an [`MPosition`] from a [`RecordValue`] in C++ `MeasureHolder` format.
///
/// The record is expected to contain spherical coordinates (lon, lat, radius).
/// The result is returned in ITRF (converted from spherical to Cartesian) if
/// the refer is ITRF, or as WGS84 if refer is WGS84.
pub fn position_from_record(rec: &RecordValue) -> Result<MPosition, MeasureError> {
    let type_str = extract_string(rec, "type")?;
    if type_str != "position" {
        return Err(MeasureError::InvalidRecord {
            reason: format!("expected type 'position', got '{type_str}'"),
        });
    }

    let refer_str = extract_string(rec, "refer")?;
    let refer: PositionRef = refer_str.parse()?;

    let m0 = extract_sub_record(rec, "m0")?;
    let lon = extract_quantity_value(m0)?;
    let m1 = extract_sub_record(rec, "m1")?;
    let lat = extract_quantity_value(m1)?;
    let m2 = extract_sub_record(rec, "m2")?;
    let radius = extract_quantity_value(m2)?;

    match refer {
        PositionRef::ITRF => {
            // Convert spherical (lon, lat, radius) back to Cartesian.
            // The radius in the record is the geocentric radius, but C++
            // MeasureHolder actually stores geodetic height for ITRF.
            // For maximum compatibility, use gd2gc with height derived from radius.
            // However, the C++ record format stores (lon, lat, radius) where
            // radius is the geocentric distance. We reconstruct Cartesian directly.
            let x = radius * lat.cos() * lon.cos();
            let y = radius * lat.cos() * lon.sin();
            let z = radius * lat.sin();
            Ok(MPosition::new_itrf(x, y, z))
        }
        PositionRef::WGS84 => {
            // For WGS84, the record stores geodetic (lon, lat, height).
            // But the m2 field is labeled "m" (radius in m). In practice
            // C++ MVPosition stores the height for WGS84.
            Ok(MPosition::new_wgs84(lon, lat, radius))
        }
    }
}

// ---------------------------------------------------------------------------
// MDirection
// ---------------------------------------------------------------------------

/// Serializes an [`MDirection`] to a [`RecordValue`] compatible with C++ `MeasureHolder`.
///
/// Format: `{ type: "direction", refer: "J2000", m0: { value: lon_rad, unit: "rad" }, m1: { value: lat_rad, unit: "rad" } }`
pub fn direction_to_record(dir: &MDirection) -> RecordValue {
    let (lon, lat) = dir.as_angles();
    RecordValue::new(vec![
        RecordField::new(
            "type",
            Value::Scalar(ScalarValue::String("direction".to_owned())),
        ),
        RecordField::new(
            "refer",
            Value::Scalar(ScalarValue::String(dir.refer().as_str().to_owned())),
        ),
        RecordField::new("m0", Value::Record(quantity_record(lon, "rad"))),
        RecordField::new("m1", Value::Record(quantity_record(lat, "rad"))),
    ])
}

/// Deserializes an [`MDirection`] from a [`RecordValue`] in C++ `MeasureHolder` format.
pub fn direction_from_record(rec: &RecordValue) -> Result<MDirection, MeasureError> {
    let type_str = extract_string(rec, "type")?;
    if type_str != "direction" {
        return Err(MeasureError::InvalidRecord {
            reason: format!("expected type 'direction', got '{type_str}'"),
        });
    }

    let refer_str = extract_string(rec, "refer")?;
    let refer: DirectionRef = refer_str.parse()?;

    let m0 = extract_sub_record(rec, "m0")?;
    let lon = extract_quantity_value(m0)?;
    let m1 = extract_sub_record(rec, "m1")?;
    let lat = extract_quantity_value(m1)?;

    Ok(MDirection::from_angles(lon, lat, refer))
}

// ---------------------------------------------------------------------------
// MFrequency
// ---------------------------------------------------------------------------

/// Serializes an [`MFrequency`] to a [`RecordValue`] compatible with C++ `MeasureHolder`.
///
/// Format: `{ type: "frequency", refer: "LSRK", m0: { value: hz, unit: "Hz" } }`
pub fn frequency_to_record(freq: &MFrequency) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new(
            "type",
            Value::Scalar(ScalarValue::String("frequency".to_owned())),
        ),
        RecordField::new(
            "refer",
            Value::Scalar(ScalarValue::String(freq.refer().as_str().to_owned())),
        ),
        RecordField::new("m0", Value::Record(quantity_record(freq.hz(), "Hz"))),
    ])
}

/// Deserializes an [`MFrequency`] from a [`RecordValue`] in C++ `MeasureHolder` format.
pub fn frequency_from_record(rec: &RecordValue) -> Result<MFrequency, MeasureError> {
    let type_str = extract_string(rec, "type")?;
    if type_str != "frequency" {
        return Err(MeasureError::InvalidRecord {
            reason: format!("expected type 'frequency', got '{type_str}'"),
        });
    }

    let refer_str = extract_string(rec, "refer")?;
    let refer: FrequencyRef = refer_str.parse()?;

    let m0 = extract_sub_record(rec, "m0")?;
    let hz = extract_quantity_value(m0)?;

    Ok(MFrequency::new(hz, refer))
}

// ---------------------------------------------------------------------------
// MDoppler
// ---------------------------------------------------------------------------

/// Serializes an [`MDoppler`] to a [`RecordValue`] compatible with C++ `MeasureHolder`.
///
/// Format: `{ type: "doppler", refer: "RADIO", m0: { value: v, unit: "" } }`
pub fn doppler_to_record(doppler: &MDoppler) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new(
            "type",
            Value::Scalar(ScalarValue::String("doppler".to_owned())),
        ),
        RecordField::new(
            "refer",
            Value::Scalar(ScalarValue::String(doppler.refer().as_str().to_owned())),
        ),
        RecordField::new("m0", Value::Record(quantity_record(doppler.value(), ""))),
    ])
}

/// Deserializes an [`MDoppler`] from a [`RecordValue`] in C++ `MeasureHolder` format.
pub fn doppler_from_record(rec: &RecordValue) -> Result<MDoppler, MeasureError> {
    let type_str = extract_string(rec, "type")?;
    if type_str != "doppler" {
        return Err(MeasureError::InvalidRecord {
            reason: format!("expected type 'doppler', got '{type_str}'"),
        });
    }

    let refer_str = extract_string(rec, "refer")?;
    let refer: DopplerRef = refer_str.parse()?;

    let m0 = extract_sub_record(rec, "m0")?;
    let value = extract_quantity_value(m0)?;

    Ok(MDoppler::new(value, refer))
}

// ---------------------------------------------------------------------------
// MRadialVelocity
// ---------------------------------------------------------------------------

/// Serializes an [`MRadialVelocity`] to a [`RecordValue`] compatible with C++ `MeasureHolder`.
///
/// Format: `{ type: "radialvelocity", refer: "LSRK", m0: { value: m/s, unit: "m/s" } }`
pub fn radial_velocity_to_record(rv: &MRadialVelocity) -> RecordValue {
    RecordValue::new(vec![
        RecordField::new(
            "type",
            Value::Scalar(ScalarValue::String("radialvelocity".to_owned())),
        ),
        RecordField::new(
            "refer",
            Value::Scalar(ScalarValue::String(rv.refer().as_str().to_owned())),
        ),
        RecordField::new("m0", Value::Record(quantity_record(rv.ms(), "m/s"))),
    ])
}

/// Deserializes an [`MRadialVelocity`] from a [`RecordValue`] in C++ `MeasureHolder` format.
pub fn radial_velocity_from_record(rec: &RecordValue) -> Result<MRadialVelocity, MeasureError> {
    let type_str = extract_string(rec, "type")?;
    if type_str != "radialvelocity" {
        return Err(MeasureError::InvalidRecord {
            reason: format!("expected type 'radialvelocity', got '{type_str}'"),
        });
    }

    let refer_str = extract_string(rec, "refer")?;
    let refer: RadialVelocityRef = refer_str.parse()?;

    let m0 = extract_sub_record(rec, "m0")?;
    let ms = extract_quantity_value(m0)?;

    Ok(MRadialVelocity::new(ms, refer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_record_roundtrip() {
        let epoch = MEpoch::from_mjd(51544.5, EpochRef::UTC);
        let rec = epoch_to_record(&epoch);
        let decoded = epoch_from_record(&rec).unwrap();

        assert_eq!(decoded.refer(), EpochRef::UTC);
        assert!((decoded.value().as_mjd() - 51544.5).abs() < 1e-12);
    }

    #[test]
    fn epoch_record_field_names() {
        let epoch = MEpoch::from_mjd(51544.5, EpochRef::TAI);
        let rec = epoch_to_record(&epoch);

        assert_eq!(
            rec.get("type"),
            Some(&Value::Scalar(ScalarValue::String("epoch".to_owned())))
        );
        assert_eq!(
            rec.get("refer"),
            Some(&Value::Scalar(ScalarValue::String("TAI".to_owned())))
        );
        assert!(rec.get("m0").is_some());
    }

    #[test]
    fn position_record_roundtrip() {
        let pos = MPosition::new_itrf(-1601185.4, -5041977.5, 3554875.9);
        let rec = position_to_record(&pos);
        let decoded = position_from_record(&rec).unwrap();

        assert_eq!(decoded.refer(), PositionRef::ITRF);
        let orig = pos.values();
        let dec = decoded.values();
        for i in 0..3 {
            assert!(
                (orig[i] - dec[i]).abs() < 1.0,
                "coordinate {i}: {:.3} vs {:.3}",
                orig[i],
                dec[i]
            );
        }
    }

    #[test]
    fn position_record_field_names() {
        let pos = MPosition::new_itrf(1.0, 2.0, 3.0);
        let rec = position_to_record(&pos);

        assert_eq!(
            rec.get("type"),
            Some(&Value::Scalar(ScalarValue::String("position".to_owned())))
        );
        assert_eq!(
            rec.get("refer"),
            Some(&Value::Scalar(ScalarValue::String("ITRF".to_owned())))
        );
        assert!(rec.get("m0").is_some());
        assert!(rec.get("m1").is_some());
        assert!(rec.get("m2").is_some());
    }

    #[test]
    fn invalid_record_type() {
        let rec = RecordValue::new(vec![RecordField::new(
            "type",
            Value::Scalar(ScalarValue::String("direction".to_owned())),
        )]);
        assert!(epoch_from_record(&rec).is_err());
    }
}
