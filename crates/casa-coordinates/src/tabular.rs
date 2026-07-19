// SPDX-License-Identifier: LGPL-3.0-or-later
//! One-axis tabular coordinate with lookup table interpolation.
//!
//! [`TabularCoordinate`] maps between pixel and world values using a pair of
//! lookup tables. Intermediate values are found by linear interpolation.
//! This is useful for non-linear axes (e.g. irregularly-spaced frequency
//! channels or a velocity axis with non-uniform spacing).
//!
//! Corresponds to C++ `TabularCoordinate`.

use casa_types::{ArrayValue, RecordValue, Value};

use crate::coordinate::{Coordinate, CoordinateType};
use crate::error::CoordinateError;
use crate::record_utils::{get_optional_string, get_optional_vec_string, get_required_vec_f64};

/// A one-axis coordinate defined by a pair of lookup tables.
///
/// The `pixel_values` and `world_values` vectors must have the same length
/// and must be monotonically ordered (either both increasing or both
/// decreasing) to allow unique interpolation in both directions.
///
/// Corresponds to C++ `TabularCoordinate`.
#[derive(Debug, Clone)]
pub struct TabularCoordinate {
    pixel_values: Vec<f64>,
    world_values: Vec<f64>,
    name: String,
    unit: String,
}

impl TabularCoordinate {
    /// Creates a new tabular coordinate.
    ///
    /// Both vectors must have the same length (at least 2). The pixel values
    /// should be monotonically increasing or decreasing.
    ///
    /// # Panics
    ///
    /// Panics if the vectors have different lengths or fewer than 2 entries.
    pub fn new(
        pixel_values: Vec<f64>,
        world_values: Vec<f64>,
        name: impl Into<String>,
        unit: impl Into<String>,
    ) -> Self {
        assert_eq!(
            pixel_values.len(),
            world_values.len(),
            "pixel and world tables must have the same length"
        );
        assert!(
            pixel_values.len() >= 2,
            "tabular coordinate requires at least 2 entries"
        );
        Self {
            pixel_values,
            world_values,
            name: name.into(),
            unit: unit.into(),
        }
    }

    /// Returns the pixel lookup table.
    pub fn pixel_values(&self) -> &[f64] {
        &self.pixel_values
    }

    /// Returns the world lookup table.
    pub fn world_values(&self) -> &[f64] {
        &self.world_values
    }

    /// Reconstructs a tabular coordinate from a serialized record.
    pub fn from_record(rec: &RecordValue) -> Result<Self, CoordinateError> {
        let pixel_values = get_required_vec_f64(rec, "pixelvalues")?;
        let world_values = get_required_vec_f64(rec, "worldvalues")?;
        let name = get_optional_string(rec, "name")
            .or_else(|| get_optional_vec_string(rec, "axes").and_then(|v| v.into_iter().next()))
            .unwrap_or_else(|| "Tabular".into());
        let unit = get_optional_string(rec, "unit")
            .or_else(|| get_optional_vec_string(rec, "units").and_then(|v| v.into_iter().next()))
            .unwrap_or_default();
        if pixel_values.len() != world_values.len() || pixel_values.len() < 2 {
            return Err(CoordinateError::InvalidRecord(
                "tabular coordinate requires matching pixel/world tables with at least 2 entries"
                    .into(),
            ));
        }
        Ok(Self::new(pixel_values, world_values, name, unit))
    }
}

/// Linear interpolation: given tables `xs` and `ys`, find the `y` value
/// corresponding to `x` by interpolating (or extrapolating) between the
/// nearest bracketing entries.
fn interpolate(xs: &[f64], ys: &[f64], x: f64) -> Result<f64, CoordinateError> {
    let n = xs.len();
    if n < 2 {
        return Err(CoordinateError::ConversionFailed(
            "tabular interpolation requires at least 2 entries".into(),
        ));
    }

    // Determine if xs is increasing or decreasing
    let increasing = xs[n - 1] > xs[0];

    // Find the bracketing interval using binary search
    let idx = if increasing {
        match xs.binary_search_by(|probe| probe.partial_cmp(&x).unwrap()) {
            Ok(i) => return Ok(ys[i]),
            Err(i) => i,
        }
    } else {
        // For decreasing, search in reverse order
        let rev: Vec<f64> = xs.iter().rev().copied().collect();
        match rev.binary_search_by(|probe| probe.partial_cmp(&x).unwrap()) {
            Ok(i) => return Ok(ys[n - 1 - i]),
            Err(i) => n - i,
        }
    };

    // Clamp to valid interval for interpolation/extrapolation
    let lo = if idx == 0 { 0 } else { (idx - 1).min(n - 2) };
    let hi = lo + 1;

    let dx = xs[hi] - xs[lo];
    if dx.abs() < 1e-300 {
        return Err(CoordinateError::ConversionFailed(
            "duplicate pixel values in tabular coordinate".into(),
        ));
    }
    let t = (x - xs[lo]) / dx;
    Ok(ys[lo] + t * (ys[hi] - ys[lo]))
}

impl Coordinate for TabularCoordinate {
    fn coordinate_type(&self) -> CoordinateType {
        CoordinateType::Tabular
    }

    fn n_pixel_axes(&self) -> usize {
        1
    }

    fn n_world_axes(&self) -> usize {
        1
    }

    fn to_world(&self, pixel: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        if pixel.len() != 1 {
            return Err(CoordinateError::DimensionMismatch {
                expected: 1,
                got: pixel.len(),
            });
        }
        let world = interpolate(&self.pixel_values, &self.world_values, pixel[0])?;
        Ok(vec![world])
    }

    fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        if world.len() != 1 {
            return Err(CoordinateError::DimensionMismatch {
                expected: 1,
                got: world.len(),
            });
        }
        let pixel = interpolate(&self.world_values, &self.pixel_values, world[0])?;
        Ok(vec![pixel])
    }

    fn reference_value(&self) -> Vec<f64> {
        vec![self.world_values[0]]
    }

    fn reference_pixel(&self) -> Vec<f64> {
        vec![self.pixel_values[0]]
    }

    fn increment(&self) -> Vec<f64> {
        // Approximate increment from first two entries
        let dp = self.pixel_values[1] - self.pixel_values[0];
        let dw = self.world_values[1] - self.world_values[0];
        if dp.abs() < 1e-300 {
            vec![0.0]
        } else {
            vec![dw / dp]
        }
    }

    fn axis_names(&self) -> Vec<String> {
        vec![self.name.clone()]
    }

    fn axis_units(&self) -> Vec<String> {
        vec![self.unit.clone()]
    }
}

impl TabularCoordinate {
    pub(crate) fn to_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();
        rec.upsert(
            "crval",
            Value::Array(ArrayValue::from_f64_vec(self.reference_value())),
        );
        rec.upsert(
            "crpix",
            Value::Array(ArrayValue::from_f64_vec(self.reference_pixel())),
        );
        rec.upsert(
            "cdelt",
            Value::Array(ArrayValue::from_f64_vec(self.increment())),
        );
        rec.upsert(
            "pc",
            Value::Array(ArrayValue::Float64(
                ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[1, 1]), vec![1.0])
                    .expect("1x1 tabular pc matrix"),
            )),
        );
        rec.upsert(
            "axes",
            Value::Array(ArrayValue::from_string_vec(self.axis_names())),
        );
        rec.upsert(
            "units",
            Value::Array(ArrayValue::from_string_vec(self.axis_units())),
        );
        rec.upsert(
            "pixelvalues",
            Value::Array(ArrayValue::from_f64_vec(self.pixel_values.clone())),
        );
        rec.upsert(
            "worldvalues",
            Value::Array(ArrayValue::from_f64_vec(self.world_values.clone())),
        );
        rec
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_linear_tab() -> TabularCoordinate {
        // Linear table: pixel 0..4 -> world 100..500
        TabularCoordinate::new(
            vec![0.0, 1.0, 2.0, 3.0, 4.0],
            vec![100.0, 200.0, 300.0, 400.0, 500.0],
            "Velocity",
            "km/s",
        )
    }

    #[test]
    fn exact_table_values() {
        let coord = make_linear_tab();
        assert_eq!(coord.to_world(&[0.0]).unwrap(), vec![100.0]);
        assert_eq!(coord.to_world(&[2.0]).unwrap(), vec![300.0]);
        assert_eq!(coord.to_world(&[4.0]).unwrap(), vec![500.0]);
    }

    #[test]
    fn interpolation() {
        let coord = make_linear_tab();
        let world = coord.to_world(&[1.5]).unwrap();
        assert!((world[0] - 250.0).abs() < 1e-10);
    }

    #[test]
    fn reverse_interpolation() {
        let coord = make_linear_tab();
        let pixel = coord.to_pixel(&[250.0]).unwrap();
        assert!((pixel[0] - 1.5).abs() < 1e-10);
    }

    #[test]
    fn roundtrip() {
        let coord = make_linear_tab();
        let pixel = vec![2.7];
        let world = coord.to_world(&pixel).unwrap();
        let back = coord.to_pixel(&world).unwrap();
        assert!((back[0] - pixel[0]).abs() < 1e-10);
    }

    #[test]
    fn non_linear_table() {
        // Non-linear spacing
        let coord = TabularCoordinate::new(
            vec![0.0, 1.0, 2.0, 3.0],
            vec![0.0, 1.0, 4.0, 9.0], // quadratic-like
            "X",
            "",
        );
        // At pixel 1.5, world should be interpolated between 1.0 and 4.0 = 2.5
        let world = coord.to_world(&[1.5]).unwrap();
        assert!((world[0] - 2.5).abs() < 1e-10);
    }

    #[test]
    fn extrapolation() {
        let coord = make_linear_tab();
        // Extrapolate beyond table
        let world = coord.to_world(&[5.0]).unwrap();
        assert!((world[0] - 600.0).abs() < 1e-10);
    }

    #[test]
    fn dimension_mismatch() {
        let coord = make_linear_tab();
        assert!(coord.to_world(&[]).is_err());
        assert!(coord.to_pixel(&[1.0, 2.0]).is_err());
    }

    #[test]
    fn trait_methods() {
        let coord = make_linear_tab();
        assert_eq!(coord.coordinate_type(), CoordinateType::Tabular);
        assert_eq!(coord.n_pixel_axes(), 1);
        assert_eq!(coord.n_world_axes(), 1);
        assert_eq!(coord.axis_names(), vec!["Velocity"]);
        assert_eq!(coord.axis_units(), vec!["km/s"]);
        assert_eq!(coord.reference_value(), vec![100.0]);
        assert_eq!(coord.reference_pixel(), vec![0.0]);
    }

    #[test]
    fn to_record_has_fields() {
        let coord = make_linear_tab();
        let rec = coord.to_record();
        assert!(rec.get("pixelvalues").is_some());
        assert!(rec.get("worldvalues").is_some());
        assert!(rec.get("axes").is_some());
    }

    #[test]
    fn record_roundtrip() {
        let coord = make_linear_tab();
        let restored = TabularCoordinate::from_record(&coord.to_record()).unwrap();

        assert_eq!(restored.pixel_values(), coord.pixel_values());
        assert_eq!(restored.world_values(), coord.world_values());
        assert_eq!(restored.axis_names(), coord.axis_names());
        assert_eq!(restored.axis_units(), coord.axis_units());
    }
}
