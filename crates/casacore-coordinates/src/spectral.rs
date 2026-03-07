// SPDX-License-Identifier: LGPL-3.0-or-later
//! One-axis spectral (frequency/velocity) coordinate.
//!
//! [`SpectralCoordinate`] implements a linear mapping between pixel position
//! and frequency (or velocity) in a specified reference frame. This
//! corresponds to C++ `SpectralCoordinate` in its simplest (non-tabular) mode.
//!
//! The conversion is:
//!
//! ```text
//! world = crval + cdelt * (pixel - crpix)
//! ```

use casacore_types::measures::frequency::FrequencyRef;
use casacore_types::{RecordValue, ScalarValue, Value};

use crate::coordinate::{Coordinate, CoordinateType};
use crate::error::CoordinateError;

/// A one-axis spectral coordinate with linear pixel-to-frequency mapping.
///
/// Stores the frequency reference frame, a rest frequency for velocity
/// conversions, and the standard FITS WCS linear parameters (crval, cdelt,
/// crpix).
///
/// Corresponds to C++ `SpectralCoordinate`.
#[derive(Debug, Clone)]
pub struct SpectralCoordinate {
    /// The velocity reference frame (LSRK, BARY, TOPO, etc.).
    frequency_ref: FrequencyRef,
    /// The rest frequency in Hz (used for velocity conversions).
    rest_frequency: f64,
    /// Reference frequency in Hz.
    crval: f64,
    /// Frequency increment per pixel in Hz.
    cdelt: f64,
    /// Reference pixel position.
    crpix: f64,
    /// The world axis unit string.
    unit: String,
}

impl SpectralCoordinate {
    /// Creates a new spectral coordinate.
    ///
    /// - `frequency_ref`: the velocity reference frame.
    /// - `crval`: reference frequency in Hz.
    /// - `cdelt`: frequency increment per pixel in Hz.
    /// - `crpix`: reference pixel position.
    /// - `rest_frequency`: the rest frequency in Hz (for velocity conversions).
    pub fn new(
        frequency_ref: FrequencyRef,
        crval: f64,
        cdelt: f64,
        crpix: f64,
        rest_frequency: f64,
    ) -> Self {
        Self {
            frequency_ref,
            rest_frequency,
            crval,
            cdelt,
            crpix,
            unit: "Hz".into(),
        }
    }

    /// Sets the world axis unit string. Returns `self` for chaining.
    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = unit.into();
        self
    }

    /// Returns the velocity reference frame.
    pub fn frequency_ref(&self) -> FrequencyRef {
        self.frequency_ref
    }

    /// Returns the rest frequency in Hz.
    pub fn rest_frequency(&self) -> f64 {
        self.rest_frequency
    }
}

impl Coordinate for SpectralCoordinate {
    fn coordinate_type(&self) -> CoordinateType {
        CoordinateType::Spectral
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
        let world = self.crval + self.cdelt * (pixel[0] - self.crpix);
        Ok(vec![world])
    }

    fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        if world.len() != 1 {
            return Err(CoordinateError::DimensionMismatch {
                expected: 1,
                got: world.len(),
            });
        }
        if self.cdelt.abs() < 1e-300 {
            return Err(CoordinateError::ConversionFailed(
                "zero spectral increment".into(),
            ));
        }
        let pixel = self.crpix + (world[0] - self.crval) / self.cdelt;
        Ok(vec![pixel])
    }

    fn reference_value(&self) -> Vec<f64> {
        vec![self.crval]
    }

    fn reference_pixel(&self) -> Vec<f64> {
        vec![self.crpix]
    }

    fn increment(&self) -> Vec<f64> {
        vec![self.cdelt]
    }

    fn axis_names(&self) -> Vec<String> {
        vec!["Frequency".into()]
    }

    fn axis_units(&self) -> Vec<String> {
        vec![self.unit.clone()]
    }

    fn to_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();

        rec.upsert(
            "coordinate_type",
            Value::Scalar(ScalarValue::String("Spectral".into())),
        );
        rec.upsert(
            "frequency_ref",
            Value::Scalar(ScalarValue::String(self.frequency_ref.as_str().into())),
        );
        rec.upsert(
            "restfreq",
            Value::Scalar(ScalarValue::Float64(self.rest_frequency)),
        );
        rec.upsert(
            "crval",
            Value::Array(casacore_types::ArrayValue::from_f64_vec(vec![self.crval])),
        );
        rec.upsert(
            "cdelt",
            Value::Array(casacore_types::ArrayValue::from_f64_vec(vec![self.cdelt])),
        );
        rec.upsert(
            "crpix",
            Value::Array(casacore_types::ArrayValue::from_f64_vec(vec![self.crpix])),
        );
        rec.upsert(
            "unit",
            Value::Scalar(ScalarValue::String(self.unit.clone())),
        );

        rec
    }

    fn clone_box(&self) -> Box<dyn Coordinate> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_pixel_gives_crval() {
        let coord = SpectralCoordinate::new(FrequencyRef::LSRK, 1.4e9, 1e6, 0.0, 1.42040575e9);
        let world = coord.to_world(&[0.0]).unwrap();
        assert!((world[0] - 1.4e9).abs() < 1.0);
    }

    #[test]
    fn linear_mapping() {
        let coord = SpectralCoordinate::new(FrequencyRef::TOPO, 1.0e9, 1e6, 100.0, 0.0);
        // 10 channels above reference
        let world = coord.to_world(&[110.0]).unwrap();
        assert!((world[0] - 1.01e9).abs() < 1.0);
    }

    #[test]
    fn roundtrip() {
        let coord = SpectralCoordinate::new(FrequencyRef::BARY, 1.42e9, -500e3, 512.0, 1.42e9);
        let pixel = [600.0];
        let world = coord.to_world(&pixel).unwrap();
        let back = coord.to_pixel(&world).unwrap();
        assert!((back[0] - pixel[0]).abs() < 1e-10);
    }

    #[test]
    fn dimension_mismatch() {
        let coord = SpectralCoordinate::new(FrequencyRef::LSRK, 1e9, 1e6, 0.0, 0.0);
        assert!(coord.to_world(&[1.0, 2.0]).is_err());
        assert!(coord.to_pixel(&[]).is_err());
    }

    #[test]
    fn trait_methods() {
        let coord = SpectralCoordinate::new(FrequencyRef::LSRK, 1e9, 1e6, 0.0, 1.42e9);
        assert_eq!(coord.coordinate_type(), CoordinateType::Spectral);
        assert_eq!(coord.n_pixel_axes(), 1);
        assert_eq!(coord.n_world_axes(), 1);
        assert_eq!(coord.axis_names(), vec!["Frequency"]);
        assert_eq!(coord.axis_units(), vec!["Hz"]);
        assert_eq!(coord.frequency_ref(), FrequencyRef::LSRK);
        assert!((coord.rest_frequency() - 1.42e9).abs() < 1.0);
    }

    #[test]
    fn with_unit() {
        let coord =
            SpectralCoordinate::new(FrequencyRef::LSRK, 1e9, 1e6, 0.0, 0.0).with_unit("GHz");
        assert_eq!(coord.axis_units(), vec!["GHz"]);
    }

    #[test]
    fn to_record_has_fields() {
        let coord = SpectralCoordinate::new(FrequencyRef::LSRK, 1e9, 1e6, 0.0, 1.42e9);
        let rec = coord.to_record();
        assert!(rec.get("frequency_ref").is_some());
        assert!(rec.get("restfreq").is_some());
        assert!(rec.get("crval").is_some());
    }

    #[test]
    fn clone_box_works() {
        let coord = SpectralCoordinate::new(FrequencyRef::TOPO, 1e9, 1e6, 0.0, 0.0);
        let boxed: Box<dyn Coordinate> = Box::new(coord);
        let cloned = boxed.clone_box();
        assert_eq!(cloned.coordinate_type(), CoordinateType::Spectral);
    }
}
