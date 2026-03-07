// SPDX-License-Identifier: LGPL-3.0-or-later
//! Observation information container.
//!
//! [`ObsInfo`] stores metadata about the observation: telescope name, observer
//! name, observation date, and telescope position. It is serialised to/from
//! a casacore-compatible record for storage in measurement set metadata.
//!
//! Corresponds to C++ `ObsInfo`.

use casacore_types::measures::epoch::MEpoch;
use casacore_types::measures::position::MPosition;
use casacore_types::{RecordValue, ScalarValue, Value};

use crate::error::CoordinateError;

/// Observation metadata: telescope, observer, date, and position.
///
/// This struct mirrors the C++ `ObsInfo` class and is typically stored as
/// part of a [`CoordinateSystem`](crate::CoordinateSystem) record.
///
/// All fields are optional except `telescope` and `observer`, which default
/// to empty strings.
#[derive(Debug, Clone, Default)]
pub struct ObsInfo {
    /// The telescope name (e.g. "ALMA", "VLA").
    pub telescope: String,
    /// The observer name.
    pub observer: String,
    /// The observation date/time.
    pub date: Option<MEpoch>,
    /// The telescope position.
    pub telescope_position: Option<MPosition>,
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
            rec.upsert(
                "date",
                Value::Scalar(ScalarValue::Float64(epoch.value().as_mjd())),
            );
        }
        if let Some(ref pos) = self.telescope_position {
            let itrf = pos.as_itrf();
            let mut pos_rec = RecordValue::default();
            pos_rec.upsert(
                "type",
                Value::Scalar(ScalarValue::String("position".into())),
            );
            pos_rec.upsert("m0", Value::Scalar(ScalarValue::Float64(itrf[0])));
            pos_rec.upsert("m1", Value::Scalar(ScalarValue::Float64(itrf[1])));
            pos_rec.upsert("m2", Value::Scalar(ScalarValue::Float64(itrf[2])));
            rec.upsert("telescopeposition", Value::Record(pos_rec));
        }

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
        let date = match rec.get("date") {
            Some(Value::Scalar(ScalarValue::Float64(mjd))) => Some(MEpoch::from_mjd(
                *mjd,
                casacore_types::measures::epoch::EpochRef::UTC,
            )),
            _ => None,
        };
        // Telescope position: simplified deserialization
        let telescope_position = match rec.get("telescopeposition") {
            Some(Value::Record(pos_rec)) => {
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
            _ => None,
        };

        Ok(Self {
            telescope,
            observer,
            date,
            telescope_position,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use casacore_types::measures::epoch::EpochRef;

    #[test]
    fn default_obs_info() {
        let info = ObsInfo::default();
        assert!(info.telescope.is_empty());
        assert!(info.observer.is_empty());
        assert!(info.date.is_none());
        assert!(info.telescope_position.is_none());
    }

    #[test]
    fn builder_pattern() {
        let info = ObsInfo::new("ALMA")
            .with_observer("John Doe")
            .with_date(MEpoch::from_mjd(59000.0, EpochRef::UTC));
        assert_eq!(info.telescope, "ALMA");
        assert_eq!(info.observer, "John Doe");
        assert!(info.date.is_some());
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
    fn to_record_contains_fields() {
        let info = ObsInfo::new("MeerKAT");
        let rec = info.to_record();
        assert!(rec.get("telescope").is_some());
        assert!(rec.get("observer").is_some());
    }

    #[test]
    fn from_empty_record() {
        let rec = RecordValue::default();
        let info = ObsInfo::from_record(&rec).unwrap();
        assert!(info.telescope.is_empty());
    }
}
