// SPDX-License-Identifier: LGPL-3.0-or-later

//! The discrete data and error planes of a casacore Quality axis.

use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};

use crate::record_utils::get_optional_vec_string;
use crate::{Coordinate, CoordinateError, CoordinateModel, CoordinateSystem, CoordinateType};

/// A casacore quality-plane label and its numeric world-coordinate code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum QualityType {
    /// The unrecognized or undefined label, written as `??`.
    Undefined = 0,
    /// The measured data plane.
    Data = 1,
    /// The error plane associated with the data.
    Error = 2,
}

impl QualityType {
    /// Resolve a native label case-insensitively. Unknown labels are undefined.
    pub fn from_name(name: &str) -> Self {
        if name.eq_ignore_ascii_case("DATA") {
            Self::Data
        } else if name.eq_ignore_ascii_case("ERROR") {
            Self::Error
        } else {
            Self::Undefined
        }
    }

    /// Return the native label.
    pub fn name(self) -> &'static str {
        match self {
            Self::Undefined => "??",
            Self::Data => "DATA",
            Self::Error => "ERROR",
        }
    }

    /// Return the numeric code of the quality plane.
    pub fn code(self) -> i32 {
        self as i32
    }

    /// Interpret a world coordinate as casacore's display formatter does.
    pub fn from_world(value: f64) -> Self {
        match (value + 0.5) as i32 {
            1 => Self::Data,
            2 => Self::Error,
            _ => Self::Undefined,
        }
    }
}

/// A one-axis coordinate selecting data, error, or undefined quality planes.
#[derive(Debug, Clone)]
pub struct QualityCoordinate {
    values: Vec<QualityType>,
    name: String,
}

impl QualityCoordinate {
    /// The labels in pixel order.
    pub fn values(&self) -> &[QualityType] {
        &self.values
    }

    /// Restore the casacore record emitted for a quality coordinate.
    ///
    /// # Errors
    ///
    /// The name or label list is missing, or the label list is empty or
    /// includes duplicate quality types.
    pub fn from_record(record: &RecordValue) -> Result<Self, CoordinateError> {
        let names = get_optional_vec_string(record, "axes")
            .ok_or_else(|| CoordinateError::InvalidRecord("Missing Quality axis name".into()))?;
        let name = match names.as_slice() {
            [name] => name.clone(),
            // casacore retains the default name if the name vector has the wrong length.
            _ => "Quality".into(),
        };
        let labels = get_optional_vec_string(record, "quality")
            .filter(|labels| !labels.is_empty())
            .ok_or_else(|| {
                CoordinateError::InvalidRecord("Quality axis requires at least one plane".into())
            })?;
        let mut values = Vec::with_capacity(labels.len());
        for label in labels {
            let value = QualityType::from_name(&label);
            if values.contains(&value) {
                return Err(CoordinateError::InvalidRecord(
                    "You have specified the same Quality more than once".into(),
                ));
            }
            values.push(value);
        }
        Ok(Self { values, name })
    }

    pub(crate) fn to_record(&self) -> RecordValue {
        let mut record = RecordValue::default();
        record.upsert(
            "axes",
            Value::Array(ArrayValue::from_string_vec(self.axis_names())),
        );
        record.upsert(
            "quality",
            Value::Array(ArrayValue::from_string_vec(
                self.values
                    .iter()
                    .map(|item| item.name().to_owned())
                    .collect(),
            )),
        );
        record.upsert(
            "crval",
            Value::Array(ArrayValue::from_f64_vec(self.reference_value())),
        );
        record.upsert(
            "crpix",
            Value::Array(ArrayValue::from_f64_vec(self.reference_pixel())),
        );
        record.upsert(
            "cdelt",
            Value::Array(ArrayValue::from_f64_vec(self.increment())),
        );
        record.upsert(
            "pc",
            Value::Array(ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[1, 1]), 1.0))),
        );
        record
    }

    fn pixel_index(&self, pixel: f64) -> Result<usize, CoordinateError> {
        let nearest = (pixel + 0.5).trunc();
        let index = if (f64::from(i32::MIN)..=f64::from(i32::MAX)).contains(&nearest) {
            nearest as i32
        } else {
            i32::MIN
        };
        usize::try_from(index)
            .ok()
            .filter(|&index| index < self.values.len())
            .ok_or_else(|| {
                CoordinateError::ConversionFailed(format!(
                    "Pixel {index} is out of range [0..{}]",
                    self.values.len() - 1
                ))
            })
    }
}

impl Coordinate for QualityCoordinate {
    fn coordinate_type(&self) -> CoordinateType {
        CoordinateType::Quality
    }

    fn n_pixel_axes(&self) -> usize {
        1
    }

    fn n_world_axes(&self) -> usize {
        1
    }

    fn to_world(&self, pixel: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        let &[value] = pixel else {
            return Err(CoordinateError::DimensionMismatch {
                expected: 1,
                got: pixel.len(),
            });
        };
        let index = self.pixel_index(value)?;
        Ok(vec![f64::from(self.values[index].code())])
    }

    fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        let &[value] = world else {
            return Err(CoordinateError::DimensionMismatch {
                expected: 1,
                got: world.len(),
            });
        };
        let found = self.values.iter().position(|plane| {
            let code = f64::from(plane.code());
            if code == 0.0 {
                value.abs() <= (1.0 + 1.0e-13) * f64::MIN_POSITIVE
            } else {
                value.is_finite() && (value - code).abs() <= 1.0e-13 * value.abs().max(code)
            }
        });
        found.map(|index| vec![index as f64]).ok_or_else(|| {
            CoordinateError::ConversionFailed(format!(
                "Quality value {} is not contained in this QualityCoordinate",
                QualityType::from_world(value).name()
            ))
        })
    }

    fn reference_value(&self) -> Vec<f64> {
        vec![f64::from(self.values[0].code())]
    }

    fn reference_pixel(&self) -> Vec<f64> {
        vec![0.0]
    }

    fn increment(&self) -> Vec<f64> {
        vec![1.0]
    }

    fn axis_names(&self) -> Vec<String> {
        vec![self.name.clone()]
    }

    fn axis_units(&self) -> Vec<String> {
        vec![String::new()]
    }
}

impl CoordinateSystem {
    /// Write canonical Quality subrecords while retaining each coordinate's maps.
    ///
    /// # Errors
    /// The record's coordinate placement does not match this system.
    pub fn normalize_quality_records(
        &self,
        record: &mut RecordValue,
    ) -> Result<(), CoordinateError> {
        if self.find_coordinate(CoordinateType::Quality).is_none() {
            return Ok(());
        }
        for (index, entry) in self.coordinates_in_record(record)?.into_iter().enumerate() {
            let CoordinateModel::Quality(quality) = self.coordinate(index) else {
                continue;
            };
            let mut canonical = quality.to_record();
            if entry.field.starts_with("coordinate") {
                canonical.upsert(
                    "coordinate_type",
                    Value::Scalar(ScalarValue::String("Quality".into())),
                );
            }
            record.upsert(entry.field, Value::Record(canonical));
        }
        Ok(())
    }
}
