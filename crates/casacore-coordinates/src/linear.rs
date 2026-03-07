// SPDX-License-Identifier: LGPL-3.0-or-later
//! N-axis linear coordinate.
//!
//! [`LinearCoordinate`] implements a general linear transformation between
//! pixel and world coordinates:
//!
//! ```text
//! world = crval + cdelt * PC * (pixel - crpix)
//! ```
//!
//! where `PC` is a rotation/coupling matrix (identity by default), `cdelt`
//! is the axis increment vector, `crval` is the reference world value, and
//! `crpix` is the reference pixel position.
//!
//! This corresponds to C++ `LinearCoordinate`.

use casacore_types::{RecordValue, ScalarValue, Value};
use ndarray::Array2;

use crate::coordinate::{Coordinate, CoordinateType};
use crate::error::CoordinateError;

/// An N-axis linear coordinate with an affine pixel-to-world mapping.
///
/// The transformation follows the FITS WCS linear convention:
///
/// ```text
/// world[i] = crval[i] + cdelt[i] * sum_j(PC[i][j] * (pixel[j] - crpix[j]))
/// ```
///
/// The PC matrix defaults to the identity, making the transformation a simple
/// scale-and-shift along each axis independently.
///
/// Corresponds to C++ `LinearCoordinate`.
#[derive(Debug, Clone)]
pub struct LinearCoordinate {
    crval: Vec<f64>,
    cdelt: Vec<f64>,
    crpix: Vec<f64>,
    pc: Array2<f64>,
    names: Vec<String>,
    units: Vec<String>,
}

impl LinearCoordinate {
    /// Creates a new linear coordinate with `naxes` axes and default values.
    ///
    /// The reference value, reference pixel, and all increments default to 0.0
    /// (increments default to 1.0). Axis names default to `"Linear1"`,
    /// `"Linear2"`, etc. Units default to empty strings. The PC matrix
    /// defaults to the identity.
    ///
    /// # Panics
    ///
    /// Panics if `naxes` is 0.
    pub fn new(naxes: usize, names: Vec<String>, units: Vec<String>) -> Self {
        assert!(naxes > 0, "LinearCoordinate requires at least 1 axis");
        let names = if names.len() == naxes {
            names
        } else {
            (1..=naxes).map(|i| format!("Linear{i}")).collect()
        };
        let units = if units.len() == naxes {
            units
        } else {
            vec![String::new(); naxes]
        };
        Self {
            crval: vec![0.0; naxes],
            cdelt: vec![1.0; naxes],
            crpix: vec![0.0; naxes],
            pc: Array2::eye(naxes),
            names,
            units,
        }
    }

    /// Sets the reference world values. Returns `self` for chaining.
    ///
    /// # Panics
    ///
    /// Panics if `crval.len()` does not match the number of axes.
    pub fn with_reference_value(mut self, crval: Vec<f64>) -> Self {
        assert_eq!(crval.len(), self.n_axes());
        self.crval = crval;
        self
    }

    /// Sets the reference pixel positions. Returns `self` for chaining.
    ///
    /// # Panics
    ///
    /// Panics if `crpix.len()` does not match the number of axes.
    pub fn with_reference_pixel(mut self, crpix: Vec<f64>) -> Self {
        assert_eq!(crpix.len(), self.n_axes());
        self.crpix = crpix;
        self
    }

    /// Sets the axis increments. Returns `self` for chaining.
    ///
    /// # Panics
    ///
    /// Panics if `cdelt.len()` does not match the number of axes.
    pub fn with_increment(mut self, cdelt: Vec<f64>) -> Self {
        assert_eq!(cdelt.len(), self.n_axes());
        self.cdelt = cdelt;
        self
    }

    /// Sets the PC (rotation/coupling) matrix. Returns `self` for chaining.
    ///
    /// # Panics
    ///
    /// Panics if the matrix shape is not `(naxes, naxes)`.
    pub fn with_pc_matrix(mut self, pc: Array2<f64>) -> Self {
        let n = self.n_axes();
        assert_eq!(pc.shape(), [n, n]);
        self.pc = pc;
        self
    }

    /// Returns the number of axes.
    fn n_axes(&self) -> usize {
        self.crval.len()
    }

    /// Returns a reference to the PC rotation/coupling matrix.
    pub fn pc_matrix(&self) -> &Array2<f64> {
        &self.pc
    }
}

impl Coordinate for LinearCoordinate {
    fn coordinate_type(&self) -> CoordinateType {
        CoordinateType::Linear
    }

    fn n_pixel_axes(&self) -> usize {
        self.n_axes()
    }

    fn n_world_axes(&self) -> usize {
        self.n_axes()
    }

    fn to_world(&self, pixel: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        let n = self.n_axes();
        if pixel.len() != n {
            return Err(CoordinateError::DimensionMismatch {
                expected: n,
                got: pixel.len(),
            });
        }

        // delta_pixel = pixel - crpix
        let delta: Vec<f64> = pixel.iter().zip(&self.crpix).map(|(p, r)| p - r).collect();

        // Apply PC matrix: rotated = PC * delta
        let mut rotated = vec![0.0; n];
        for (i, rotated_i) in rotated.iter_mut().enumerate() {
            for (j, delta_j) in delta.iter().enumerate() {
                *rotated_i += self.pc[[i, j]] * delta_j;
            }
        }

        // world = crval + cdelt * rotated
        let world: Vec<f64> = (0..n)
            .map(|i| self.crval[i] + self.cdelt[i] * rotated[i])
            .collect();

        Ok(world)
    }

    fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        let n = self.n_axes();
        if world.len() != n {
            return Err(CoordinateError::DimensionMismatch {
                expected: n,
                got: world.len(),
            });
        }

        // Compute (world - crval) / cdelt
        let mut scaled = vec![0.0; n];
        for i in 0..n {
            if self.cdelt[i].abs() < 1e-300 {
                return Err(CoordinateError::ConversionFailed(format!(
                    "zero increment on axis {i}"
                )));
            }
            scaled[i] = (world[i] - self.crval[i]) / self.cdelt[i];
        }

        // Invert PC matrix: pixel = crpix + PC^-1 * scaled
        // For small N, use direct inversion via cofactor / Gauss elimination.
        let inv_pc = invert_matrix(&self.pc)?;

        let mut pixel = vec![0.0; n];
        for i in 0..n {
            let mut sum = 0.0;
            for j in 0..n {
                sum += inv_pc[[i, j]] * scaled[j];
            }
            pixel[i] = sum + self.crpix[i];
        }

        Ok(pixel)
    }

    fn reference_value(&self) -> Vec<f64> {
        self.crval.clone()
    }

    fn reference_pixel(&self) -> Vec<f64> {
        self.crpix.clone()
    }

    fn increment(&self) -> Vec<f64> {
        self.cdelt.clone()
    }

    fn axis_names(&self) -> Vec<String> {
        self.names.clone()
    }

    fn axis_units(&self) -> Vec<String> {
        self.units.clone()
    }

    fn to_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();
        let n = self.n_axes();

        rec.upsert(
            "coordinate_type",
            Value::Scalar(ScalarValue::String("Linear".into())),
        );
        rec.upsert("naxes", Value::Scalar(ScalarValue::Int32(n as i32)));
        rec.upsert(
            "crval",
            Value::Array(casacore_types::ArrayValue::from_f64_vec(self.crval.clone())),
        );
        rec.upsert(
            "cdelt",
            Value::Array(casacore_types::ArrayValue::from_f64_vec(self.cdelt.clone())),
        );
        rec.upsert(
            "crpix",
            Value::Array(casacore_types::ArrayValue::from_f64_vec(self.crpix.clone())),
        );
        // PC matrix as flattened row-major array
        let pc_flat: Vec<f64> = self.pc.iter().copied().collect();
        rec.upsert(
            "pc",
            Value::Array(casacore_types::ArrayValue::from_f64_vec(pc_flat)),
        );
        rec.upsert(
            "axes",
            Value::Array(casacore_types::ArrayValue::from_string_vec(
                self.names.clone(),
            )),
        );
        rec.upsert(
            "units",
            Value::Array(casacore_types::ArrayValue::from_string_vec(
                self.units.clone(),
            )),
        );

        rec
    }

    fn clone_box(&self) -> Box<dyn Coordinate> {
        Box::new(self.clone())
    }
}

/// Inverts a square matrix using Gauss-Jordan elimination with partial pivoting.
///
/// Returns an error if the matrix is singular.
pub(crate) fn invert_matrix(m: &Array2<f64>) -> Result<Array2<f64>, CoordinateError> {
    let n = m.nrows();
    assert_eq!(n, m.ncols());

    // Augmented matrix [M | I]
    let mut aug = Array2::zeros((n, 2 * n));
    for i in 0..n {
        for j in 0..n {
            aug[[i, j]] = m[[i, j]];
        }
        aug[[i, n + i]] = 1.0;
    }

    // Forward elimination with partial pivoting
    for col in 0..n {
        // Find pivot
        let mut max_val = aug[[col, col]].abs();
        let mut max_row = col;
        for row in (col + 1)..n {
            let val = aug[[row, col]].abs();
            if val > max_val {
                max_val = val;
                max_row = row;
            }
        }

        if max_val < 1e-15 {
            return Err(CoordinateError::ConversionFailed(
                "singular PC matrix".into(),
            ));
        }

        // Swap rows
        if max_row != col {
            for j in 0..2 * n {
                let tmp = aug[[col, j]];
                aug[[col, j]] = aug[[max_row, j]];
                aug[[max_row, j]] = tmp;
            }
        }

        // Scale pivot row
        let pivot = aug[[col, col]];
        for j in 0..2 * n {
            aug[[col, j]] /= pivot;
        }

        // Eliminate column
        for row in 0..n {
            if row != col {
                let factor = aug[[row, col]];
                for j in 0..2 * n {
                    aug[[row, j]] -= factor * aug[[col, j]];
                }
            }
        }
    }

    // Extract inverse from right half
    let mut inv = Array2::zeros((n, n));
    for i in 0..n {
        for j in 0..n {
            inv[[i, j]] = aug[[i, n + j]];
        }
    }

    Ok(inv)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_linear_identity() {
        let coord = LinearCoordinate::new(2, vec![], vec![]);
        let pixel = vec![10.0, 20.0];
        let world = coord.to_world(&pixel).unwrap();
        // crval=0, cdelt=1, crpix=0, PC=I => world = pixel
        assert!((world[0] - 10.0).abs() < 1e-12);
        assert!((world[1] - 20.0).abs() < 1e-12);
    }

    #[test]
    fn linear_with_offset_and_scale() {
        let coord = LinearCoordinate::new(1, vec!["Frequency".into()], vec!["Hz".into()])
            .with_reference_value(vec![1.4e9])
            .with_reference_pixel(vec![100.0])
            .with_increment(vec![1e6]);

        // At crpix, world = crval
        let world = coord.to_world(&[100.0]).unwrap();
        assert!((world[0] - 1.4e9).abs() < 1.0);

        // 10 pixels above crpix
        let world = coord.to_world(&[110.0]).unwrap();
        assert!((world[0] - (1.4e9 + 10.0 * 1e6)).abs() < 1.0);
    }

    #[test]
    fn linear_roundtrip() {
        let coord = LinearCoordinate::new(2, vec![], vec![])
            .with_reference_value(vec![100.0, 200.0])
            .with_reference_pixel(vec![50.0, 50.0])
            .with_increment(vec![2.0, 3.0]);

        let pixel = vec![75.0, 30.0];
        let world = coord.to_world(&pixel).unwrap();
        let back = coord.to_pixel(&world).unwrap();

        assert!((back[0] - pixel[0]).abs() < 1e-10);
        assert!((back[1] - pixel[1]).abs() < 1e-10);
    }

    #[test]
    fn linear_with_rotation() {
        // 90-degree rotation
        let mut pc = Array2::zeros((2, 2));
        pc[[0, 1]] = -1.0;
        pc[[1, 0]] = 1.0;

        let coord = LinearCoordinate::new(2, vec![], vec![])
            .with_increment(vec![1.0, 1.0])
            .with_pc_matrix(pc);

        let pixel = vec![1.0, 0.0];
        let world = coord.to_world(&pixel).unwrap();
        // With 90-deg rotation: world = (0, 1)
        assert!((world[0]).abs() < 1e-12);
        assert!((world[1] - 1.0).abs() < 1e-12);

        // Roundtrip
        let back = coord.to_pixel(&world).unwrap();
        assert!((back[0] - pixel[0]).abs() < 1e-10);
        assert!((back[1] - pixel[1]).abs() < 1e-10);
    }

    #[test]
    fn dimension_mismatch_error() {
        let coord = LinearCoordinate::new(2, vec![], vec![]);
        assert!(coord.to_world(&[1.0]).is_err());
        assert!(coord.to_pixel(&[1.0, 2.0, 3.0]).is_err());
    }

    #[test]
    fn coordinate_trait_methods() {
        let coord = LinearCoordinate::new(
            2,
            vec!["X".into(), "Y".into()],
            vec!["m".into(), "m".into()],
        )
        .with_reference_value(vec![10.0, 20.0])
        .with_reference_pixel(vec![5.0, 5.0])
        .with_increment(vec![0.1, 0.2]);

        assert_eq!(coord.coordinate_type(), CoordinateType::Linear);
        assert_eq!(coord.n_pixel_axes(), 2);
        assert_eq!(coord.n_world_axes(), 2);
        assert_eq!(coord.reference_value(), vec![10.0, 20.0]);
        assert_eq!(coord.reference_pixel(), vec![5.0, 5.0]);
        assert_eq!(coord.increment(), vec![0.1, 0.2]);
        assert_eq!(coord.axis_names(), vec!["X", "Y"]);
        assert_eq!(coord.axis_units(), vec!["m", "m"]);
    }

    #[test]
    fn to_record_contains_fields() {
        let coord = LinearCoordinate::new(1, vec!["X".into()], vec!["m".into()]);
        let rec = coord.to_record();
        assert!(rec.get("coordinate_type").is_some());
        assert!(rec.get("crval").is_some());
        assert!(rec.get("cdelt").is_some());
        assert!(rec.get("crpix").is_some());
        assert!(rec.get("pc").is_some());
    }

    #[test]
    fn clone_box_works() {
        let coord = LinearCoordinate::new(1, vec![], vec![]);
        let boxed: Box<dyn Coordinate> = Box::new(coord);
        let cloned = boxed.clone_box();
        assert_eq!(cloned.coordinate_type(), CoordinateType::Linear);
    }

    #[test]
    fn invert_identity() {
        let m = Array2::eye(3);
        let inv = invert_matrix(&m).unwrap();
        for i in 0..3 {
            for j in 0..3 {
                let expected = if i == j { 1.0 } else { 0.0 };
                assert!((inv[[i, j]] - expected).abs() < 1e-12);
            }
        }
    }

    #[test]
    fn invert_2x2() {
        let m = Array2::from_shape_vec((2, 2), vec![1.0, 2.0, 3.0, 4.0]).unwrap();
        let inv = invert_matrix(&m).unwrap();
        // M * M^-1 should be identity
        let prod = m.dot(&inv);
        for i in 0..2 {
            for j in 0..2 {
                let expected = if i == j { 1.0 } else { 0.0 };
                assert!(
                    (prod[[i, j]] - expected).abs() < 1e-10,
                    "prod[{i},{j}] = {}",
                    prod[[i, j]]
                );
            }
        }
    }
}
