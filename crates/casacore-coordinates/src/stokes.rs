// SPDX-License-Identifier: LGPL-3.0-or-later
//! One-axis Stokes parameter coordinate.
//!
//! [`StokesCoordinate`] maps between pixel indices and Stokes parameter types.
//! Unlike other coordinates, it is a discrete lookup: each pixel maps to a
//! specific [`StokesType`] whose integer code becomes the world value.
//!
//! This corresponds to C++ `StokesCoordinate`.

use casacore_types::{RecordValue, ScalarValue, Value};

use crate::coordinate::{Coordinate, CoordinateType};
use crate::error::CoordinateError;

/// Stokes parameter types, matching C++ `Stokes::StokesTypes`.
///
/// The integer codes follow the FITS convention (AIPS Memo 114):
/// I=1, Q=2, U=3, V=4, RR=5, RL=6, LR=7, LL=8, XX=9, XY=10, YX=11, YY=12.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StokesType {
    /// Stokes I (total intensity).
    I = 1,
    /// Stokes Q (linear polarization).
    Q = 2,
    /// Stokes U (linear polarization).
    U = 3,
    /// Stokes V (circular polarization).
    V = 4,
    /// Right-Right circular correlation.
    RR = 5,
    /// Right-Left circular correlation.
    RL = 6,
    /// Left-Right circular correlation.
    LR = 7,
    /// Left-Left circular correlation.
    LL = 8,
    /// X-X linear correlation.
    XX = 9,
    /// X-Y linear correlation.
    XY = 10,
    /// Y-X linear correlation.
    YX = 11,
    /// Y-Y linear correlation.
    YY = 12,
}

impl StokesType {
    /// Returns the integer code for this Stokes type (FITS convention).
    pub fn code(self) -> i32 {
        self as i32
    }

    /// Converts an integer code to a [`StokesType`], if valid.
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            1 => Some(Self::I),
            2 => Some(Self::Q),
            3 => Some(Self::U),
            4 => Some(Self::V),
            5 => Some(Self::RR),
            6 => Some(Self::RL),
            7 => Some(Self::LR),
            8 => Some(Self::LL),
            9 => Some(Self::XX),
            10 => Some(Self::XY),
            11 => Some(Self::YX),
            12 => Some(Self::YY),
            _ => None,
        }
    }

    /// Returns the standard string name (e.g. "I", "RR", "XX").
    pub fn name(self) -> &'static str {
        match self {
            Self::I => "I",
            Self::Q => "Q",
            Self::U => "U",
            Self::V => "V",
            Self::RR => "RR",
            Self::RL => "RL",
            Self::LR => "LR",
            Self::LL => "LL",
            Self::XX => "XX",
            Self::XY => "XY",
            Self::YX => "YX",
            Self::YY => "YY",
        }
    }
}

impl std::fmt::Display for StokesType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A one-axis coordinate that maps pixel indices to Stokes parameter types.
///
/// The coordinate stores an ordered list of [`StokesType`] values. Pixel
/// index 0 maps to the first entry, pixel index 1 to the second, and so on.
/// The world value for each pixel is the integer Stokes code.
///
/// Corresponds to C++ `StokesCoordinate`.
#[derive(Debug, Clone)]
pub struct StokesCoordinate {
    stokes: Vec<StokesType>,
}

impl StokesCoordinate {
    /// Creates a new Stokes coordinate with the given parameter list.
    ///
    /// # Examples
    ///
    /// ```
    /// use casacore_coordinates::{StokesCoordinate, StokesType};
    ///
    /// let coord = StokesCoordinate::new(vec![
    ///     StokesType::I,
    ///     StokesType::Q,
    ///     StokesType::U,
    ///     StokesType::V,
    /// ]);
    /// assert_eq!(coord.n_stokes(), 4);
    /// ```
    pub fn new(stokes: Vec<StokesType>) -> Self {
        Self { stokes }
    }

    /// Returns the number of Stokes parameters.
    pub fn n_stokes(&self) -> usize {
        self.stokes.len()
    }

    /// Returns the Stokes types in order.
    pub fn stokes(&self) -> &[StokesType] {
        &self.stokes
    }
}

impl Coordinate for StokesCoordinate {
    fn coordinate_type(&self) -> CoordinateType {
        CoordinateType::Stokes
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
        let idx = pixel[0].round() as i64;
        if idx < 0 || idx as usize >= self.stokes.len() {
            return Err(CoordinateError::AxisOutOfRange {
                index: idx.max(0) as usize,
                naxes: self.stokes.len(),
            });
        }
        Ok(vec![self.stokes[idx as usize].code() as f64])
    }

    fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        if world.len() != 1 {
            return Err(CoordinateError::DimensionMismatch {
                expected: 1,
                got: world.len(),
            });
        }
        let code = world[0].round() as i32;
        let pos = self
            .stokes
            .iter()
            .position(|s| s.code() == code)
            .ok_or_else(|| {
                CoordinateError::ConversionFailed(format!("Stokes code {code} not in coordinate"))
            })?;
        Ok(vec![pos as f64])
    }

    fn reference_value(&self) -> Vec<f64> {
        if self.stokes.is_empty() {
            vec![0.0]
        } else {
            vec![self.stokes[0].code() as f64]
        }
    }

    fn reference_pixel(&self) -> Vec<f64> {
        vec![0.0]
    }

    fn increment(&self) -> Vec<f64> {
        vec![1.0]
    }

    fn axis_names(&self) -> Vec<String> {
        vec!["Stokes".into()]
    }

    fn axis_units(&self) -> Vec<String> {
        vec![String::new()]
    }

    fn to_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();

        rec.upsert(
            "coordinate_type",
            Value::Scalar(ScalarValue::String("Stokes".into())),
        );
        let codes: Vec<i32> = self.stokes.iter().map(|s| s.code()).collect();
        rec.upsert(
            "stokes",
            Value::Array(casacore_types::ArrayValue::from_i32_vec(codes)),
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

    fn make_iquv() -> StokesCoordinate {
        StokesCoordinate::new(vec![
            StokesType::I,
            StokesType::Q,
            StokesType::U,
            StokesType::V,
        ])
    }

    #[test]
    fn stokes_code_roundtrip() {
        for code in 1..=12 {
            let st = StokesType::from_code(code).unwrap();
            assert_eq!(st.code(), code);
        }
        assert!(StokesType::from_code(0).is_none());
        assert!(StokesType::from_code(13).is_none());
    }

    #[test]
    fn stokes_name() {
        assert_eq!(StokesType::I.name(), "I");
        assert_eq!(StokesType::RR.name(), "RR");
        assert_eq!(StokesType::YY.name(), "YY");
    }

    #[test]
    fn to_world_basic() {
        let coord = make_iquv();
        assert_eq!(coord.to_world(&[0.0]).unwrap(), vec![1.0]); // I
        assert_eq!(coord.to_world(&[1.0]).unwrap(), vec![2.0]); // Q
        assert_eq!(coord.to_world(&[2.0]).unwrap(), vec![3.0]); // U
        assert_eq!(coord.to_world(&[3.0]).unwrap(), vec![4.0]); // V
    }

    #[test]
    fn to_pixel_basic() {
        let coord = make_iquv();
        assert_eq!(coord.to_pixel(&[1.0]).unwrap(), vec![0.0]); // I
        assert_eq!(coord.to_pixel(&[2.0]).unwrap(), vec![1.0]); // Q
        assert_eq!(coord.to_pixel(&[3.0]).unwrap(), vec![2.0]); // U
        assert_eq!(coord.to_pixel(&[4.0]).unwrap(), vec![3.0]); // V
    }

    #[test]
    fn to_world_out_of_range() {
        let coord = make_iquv();
        assert!(coord.to_world(&[4.0]).is_err());
        assert!(coord.to_world(&[-1.0]).is_err());
    }

    #[test]
    fn to_pixel_missing_code() {
        let coord = make_iquv();
        // RR (code 5) is not in I/Q/U/V
        assert!(coord.to_pixel(&[5.0]).is_err());
    }

    #[test]
    fn roundtrip() {
        let coord = make_iquv();
        for i in 0..4 {
            let pixel = vec![i as f64];
            let world = coord.to_world(&pixel).unwrap();
            let back = coord.to_pixel(&world).unwrap();
            assert_eq!(back, pixel);
        }
    }

    #[test]
    fn circular_correlations() {
        let coord = StokesCoordinate::new(vec![
            StokesType::RR,
            StokesType::RL,
            StokesType::LR,
            StokesType::LL,
        ]);
        assert_eq!(coord.to_world(&[0.0]).unwrap(), vec![5.0]);
        assert_eq!(coord.to_world(&[3.0]).unwrap(), vec![8.0]);
    }

    #[test]
    fn trait_methods() {
        let coord = make_iquv();
        assert_eq!(coord.coordinate_type(), CoordinateType::Stokes);
        assert_eq!(coord.n_pixel_axes(), 1);
        assert_eq!(coord.n_world_axes(), 1);
        assert_eq!(coord.axis_names(), vec!["Stokes"]);
        assert_eq!(coord.axis_units(), vec![""]);
    }

    #[test]
    fn dimension_mismatch() {
        let coord = make_iquv();
        assert!(coord.to_world(&[1.0, 2.0]).is_err());
        assert!(coord.to_pixel(&[]).is_err());
    }

    #[test]
    fn to_record_has_stokes() {
        let coord = make_iquv();
        let rec = coord.to_record();
        assert!(rec.get("stokes").is_some());
        assert!(rec.get("coordinate_type").is_some());
    }

    #[test]
    fn clone_box_works() {
        let coord = make_iquv();
        let boxed: Box<dyn Coordinate> = Box::new(coord);
        let cloned = boxed.clone_box();
        assert_eq!(cloned.coordinate_type(), CoordinateType::Stokes);
    }

    #[test]
    fn stokes_display() {
        assert_eq!(format!("{}", StokesType::V), "V");
        assert_eq!(format!("{}", StokesType::XX), "XX");
    }
}
