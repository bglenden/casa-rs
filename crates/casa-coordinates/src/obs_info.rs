// SPDX-License-Identifier: LGPL-3.0-or-later
//! Observation information container.
//!
//! [`ObsInfo`] stores metadata about the observation: telescope name, observer
//! name, observation date, and telescope position. It is serialised to/from
//! a casacore-compatible record for storage in measurement set metadata.
//!
//! Corresponds to C++ `ObsInfo`.

use casa_types::measures::epoch::{EpochRef, MEpoch};
use casa_types::measures::position::MPosition;
use casa_types::measures::{
    epoch_from_record, epoch_to_record, position_from_record, position_to_record,
};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};

use crate::error::CoordinateError;

/// Observation metadata: telescope, observer, date, and position.
///
/// This struct mirrors the C++ `ObsInfo` class and is typically stored as
/// part of a [`CoordinateSystem`](crate::CoordinateSystem) record.
///
/// All fields are optional except `telescope` and `observer`, which default
/// to empty strings.
#[derive(Debug, Clone)]
pub struct ObsInfo {
    /// The telescope name (e.g. "ALMA", "VLA").
    pub telescope: String,
    /// The observer name.
    pub observer: String,
    /// The observation date/time.
    pub date: Option<MEpoch>,
    /// The telescope position.
    pub telescope_position: Option<MPosition>,
    /// The pointing center as `[longitude_rad, latitude_rad]`.
    ///
    /// This mirrors C++ `ObsInfo`, which stores a raw `MVDirection` value
    /// record rather than a full `MDirection` measure record.
    pub pointing_center_rad: [f64; 2],
    /// Whether the pointing center still carries its initial placeholder value.
    pub pointing_center_initial: bool,
}

impl Default for ObsInfo {
    fn default() -> Self {
        Self {
            telescope: String::new(),
            observer: String::new(),
            date: None,
            telescope_position: None,
            pointing_center_rad: [0.0, 0.0],
            pointing_center_initial: true,
        }
    }
}

impl ObsInfo {
    /// Creates a new `ObsInfo` with the given telescope name.
    pub fn new(telescope: impl Into<String>) -> Self {
        Self {
            telescope: telescope.into(),
            ..Default::default()
        }
    }

    /// Sets the observer name. Returns `self` for chaining.
    pub fn with_observer(mut self, observer: impl Into<String>) -> Self {
        self.observer = observer.into();
        self
    }

    /// Sets the observation date. Returns `self` for chaining.
    pub fn with_date(mut self, date: MEpoch) -> Self {
        self.date = Some(date);
        self
    }

    /// Sets the telescope position. Returns `self` for chaining.
    pub fn with_telescope_position(mut self, position: MPosition) -> Self {
        self.telescope_position = Some(position);
        self
    }

    /// Sets the pointing center in radians. Returns `self` for chaining.
    pub fn with_pointing_center(mut self, longitude_rad: f64, latitude_rad: f64) -> Self {
        self.pointing_center_rad = [longitude_rad, latitude_rad];
        self.pointing_center_initial = false;
        self
    }

    /// Serializes this observation info to a casacore-compatible record.
    ///
    /// The record contains string fields `telescope` and `observer`, and
    /// optionally `date` (as MJD float) and `telescopeposition` (as a
    /// sub-record).
    pub fn to_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();

        rec.upsert(
            "telescope",
            Value::Scalar(ScalarValue::String(self.telescope.clone())),
        );
        rec.upsert(
            "observer",
            Value::Scalar(ScalarValue::String(self.observer.clone())),
        );
        if let Some(ref epoch) = self.date {
            rec.upsert("obsdate", Value::Record(epoch_to_record(epoch)));
        }
        if let Some(ref pos) = self.telescope_position {
            rec.upsert("telescopeposition", Value::Record(position_to_record(pos)));
        }
        let pointing_center_rec = RecordValue::new(vec![
            RecordField::new(
                "value",
                Value::Array(ArrayValue::from_f64_vec(self.pointing_center_rad.to_vec())),
            ),
            RecordField::new(
                "initial",
                Value::Scalar(ScalarValue::Bool(self.pointing_center_initial)),
            ),
        ]);
        rec.upsert("pointingcenter", Value::Record(pointing_center_rec));

        rec
    }

    /// Deserializes observation info from a casacore-compatible record.
    pub fn from_record(rec: &RecordValue) -> Result<Self, CoordinateError> {
        let telescope = match rec.get("telescope") {
            Some(Value::Scalar(ScalarValue::String(s))) => s.clone(),
            _ => String::new(),
        };
        let observer = match rec.get("observer") {
            Some(Value::Scalar(ScalarValue::String(s))) => s.clone(),
            _ => String::new(),
        };
        let date = match rec.get("date").or_else(|| rec.get("obsdate")) {
            Some(Value::Scalar(ScalarValue::Float64(mjd))) => {
                Some(MEpoch::from_mjd(*mjd, EpochRef::UTC))
            }
            Some(Value::Record(epoch_rec)) => {
                Some(epoch_from_record(epoch_rec).map_err(|err| {
                    CoordinateError::InvalidRecord(format!("invalid observation date: {err}"))
                })?)
            }
            _ => None,
        };
        let telescope_position = match rec.get("telescopeposition") {
            Some(Value::Record(pos_rec)) => {
                if pos_rec.get("refer").is_some() {
                    Some(position_from_record(pos_rec).map_err(|err| {
                        CoordinateError::InvalidRecord(format!(
                            "invalid telescopeposition measure: {err}"
                        ))
                    })?)
                } else {
                    let m0 = match pos_rec.get("m0") {
                        Some(Value::Scalar(ScalarValue::Float64(v))) => *v,
                        _ => {
                            return Err(CoordinateError::InvalidRecord(
                                "missing m0 in telescopeposition".into(),
                            ));
                        }
                    };
                    let m1 = match pos_rec.get("m1") {
                        Some(Value::Scalar(ScalarValue::Float64(v))) => *v,
                        _ => {
                            return Err(CoordinateError::InvalidRecord(
                                "missing m1 in telescopeposition".into(),
                            ));
                        }
                    };
                    let m2 = match pos_rec.get("m2") {
                        Some(Value::Scalar(ScalarValue::Float64(v))) => *v,
                        _ => {
                            return Err(CoordinateError::InvalidRecord(
                                "missing m2 in telescopeposition".into(),
                            ));
                        }
                    };
                    Some(MPosition::new_itrf(m0, m1, m2))
                }
            }
            _ => None,
        };
        let (pointing_center_rad, pointing_center_initial) = match rec.get("pointingcenter") {
            Some(Value::Record(pointing_center_rec)) => {
                let values = match pointing_center_rec.get("value") {
                    Some(Value::Array(ArrayValue::Float64(values))) => {
                        values.iter().copied().collect::<Vec<_>>()
                    }
                    Some(Value::Array(ArrayValue::Float32(values))) => {
                        values.iter().map(|value| f64::from(*value)).collect()
                    }
                    _ => {
                        return Err(CoordinateError::InvalidRecord(
                            "field pointingcenter does not contain subfield 'value'".into(),
                        ));
                    }
                };
                if values.len() != 2 {
                    return Err(CoordinateError::InvalidRecord(
                        "pointingcenter.value must contain exactly two angles".into(),
                    ));
                }
                let initial = match pointing_center_rec.get("initial") {
                    Some(Value::Scalar(ScalarValue::Bool(value))) => *value,
                    _ => {
                        return Err(CoordinateError::InvalidRecord(
                            "field pointingcenter does not contain subfield 'initial'".into(),
                        ));
                    }
                };
                ([values[0], values[1]], initial)
            }
            _ => ([0.0, 0.0], true),
        };

        Ok(Self {
            telescope,
            observer,
            date,
            telescope_position,
            pointing_center_rad,
            pointing_center_initial,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_types::measures::epoch::EpochRef;

    #[test]
    fn default_obs_info() {
        let info = ObsInfo::default();
        assert!(info.telescope.is_empty());
        assert!(info.observer.is_empty());
        assert!(info.date.is_none());
        assert!(info.telescope_position.is_none());
        assert_eq!(info.pointing_center_rad, [0.0, 0.0]);
        assert!(info.pointing_center_initial);
    }

    #[test]
    fn builder_pattern() {
        let info = ObsInfo::new("ALMA")
            .with_observer("John Doe")
            .with_date(MEpoch::from_mjd(59000.0, EpochRef::UTC))
            .with_pointing_center(1.0, 0.5);
        assert_eq!(info.telescope, "ALMA");
        assert_eq!(info.observer, "John Doe");
        assert!(info.date.is_some());
        assert_eq!(info.pointing_center_rad, [1.0, 0.5]);
        assert!(!info.pointing_center_initial);
    }

    #[test]
    fn record_roundtrip() {
        let info = ObsInfo::new("VLA").with_observer("Jane Smith");
        let rec = info.to_record();
        let restored = ObsInfo::from_record(&rec).unwrap();
        assert_eq!(restored.telescope, "VLA");
        assert_eq!(restored.observer, "Jane Smith");
    }

    #[test]
    fn record_with_date_roundtrip() {
        let info = ObsInfo::new("ALMA").with_date(MEpoch::from_mjd(59000.5, EpochRef::UTC));
        let rec = info.to_record();
        let restored = ObsInfo::from_record(&rec).unwrap();
        assert!(restored.date.is_some());
        let mjd = restored.date.unwrap().value().as_mjd();
        assert!((mjd - 59000.5).abs() < 1e-10);
    }

    #[test]
    fn record_with_position_roundtrip() {
        let pos = MPosition::new_itrf(2225142.18, -5440307.37, -2481029.85);
        let info = ObsInfo::new("ALMA").with_telescope_position(pos);
        let rec = info.to_record();
        let restored = ObsInfo::from_record(&rec).unwrap();
        assert!(restored.telescope_position.is_some());
        let itrf = restored.telescope_position.unwrap().as_itrf();
        assert!((itrf[0] - 2225142.18).abs() < 1e-6);
    }

    #[test]
    fn record_with_pointing_center_roundtrip() {
        let info = ObsInfo::new("ALMA").with_pointing_center(4.02298, 0.08843);
        let rec = info.to_record();
        let restored = ObsInfo::from_record(&rec).unwrap();
        assert_eq!(restored.pointing_center_rad, [4.02298, 0.08843]);
        assert!(!restored.pointing_center_initial);
    }

    #[test]
    fn from_record_parses_casa_style_pointing_center() {
        let mut pointing_center = RecordValue::default();
        pointing_center.upsert(
            "value",
            Value::Array(ArrayValue::from_f64_vec(vec![4.02298, 0.08843])),
        );
        pointing_center.upsert("initial", Value::Scalar(ScalarValue::Bool(false)));

        let rec = RecordValue::new(vec![RecordField::new(
            "pointingcenter",
            Value::Record(pointing_center),
        )]);
        let info = ObsInfo::from_record(&rec).unwrap();
        assert_eq!(info.pointing_center_rad, [4.02298, 0.08843]);
        assert!(!info.pointing_center_initial);
    }

    #[test]
    fn to_record_contains_fields() {
        let info = ObsInfo::new("MeerKAT");
        let rec = info.to_record();
        assert!(rec.get("telescope").is_some());
        assert!(rec.get("observer").is_some());
        assert!(rec.get("pointingcenter").is_some());
    }

    #[test]
    fn from_empty_record() {
        let rec = RecordValue::default();
        let info = ObsInfo::from_record(&rec).unwrap();
        assert!(info.telescope.is_empty());
        assert_eq!(info.pointing_center_rad, [0.0, 0.0]);
        assert!(info.pointing_center_initial);
    }
}
