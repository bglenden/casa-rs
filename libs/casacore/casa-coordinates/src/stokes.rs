// SPDX-License-Identifier: LGPL-3.0-or-later
//! One-axis Stokes parameter coordinate.
//!
//! [`StokesCoordinate`] maps between pixel indices and Stokes parameter types.
//! Unlike other coordinates, it is a discrete lookup: each pixel maps to a
//! specific [`StokesType`] whose integer code becomes the world value.
//!
//! This corresponds to C++ `StokesCoordinate`.

use casa_core::{RecordValue, Value};

use crate::coordinate::{Coordinate, CoordinateType};
use crate::error::CoordinateError;
use crate::record_utils::{get_optional_vec_string, get_required_vec_i32};

/// Stokes parameter types, matching C++ `Stokes::StokesTypes`.
///
/// The integer codes are the casacore Stokes enumeration used in native
/// records. FITS uses different numeric codes for correlation products.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StokesType {
    /// An unspecified polarization.
    Undefined = 0,
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
    /// Mixed RX correlation.
    RX = 13,
    /// Mixed RY correlation.
    RY = 14,
    /// Mixed LX correlation.
    LX = 15,
    /// Mixed LY correlation.
    LY = 16,
    /// Mixed XR correlation.
    XR = 17,
    /// Mixed XL correlation.
    XL = 18,
    /// Mixed YR correlation.
    YR = 19,
    /// Mixed YL correlation.
    YL = 20,
    /// Quasi-orthogonal PP correlation.
    PP = 21,
    /// Quasi-orthogonal PQ correlation.
    PQ = 22,
    /// Quasi-orthogonal QP correlation.
    QP = 23,
    /// Quasi-orthogonal QQ correlation.
    QQ = 24,
    /// Single-dish right-circular polarization.
    RCircular = 25,
    /// Single-dish left-circular polarization.
    LCircular = 26,
    /// Single-dish linear polarization.
    Linear = 27,
    /// Total polarized intensity: `sqrt(Q^2 + U^2 + V^2)`.
    Ptotal = 28,
    /// Linearly polarized intensity: `sqrt(Q^2 + U^2)`.
    Plinear = 29,
    /// Total fractional polarization: `Ptotal / I`.
    PFtotal = 30,
    /// Linear fractional polarization: `Plinear / I`.
    PFlinear = 31,
    /// Linear polarization angle: `0.5 * atan2(U, Q)`.
    Pangle = 32,
}

impl StokesType {
    /// Returns the integer code for this Stokes type in native records.
    pub fn code(self) -> i32 {
        self as i32
    }

    /// Converts an integer code to a [`StokesType`], if valid.
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(Self::Undefined),
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
            13 => Some(Self::RX),
            14 => Some(Self::RY),
            15 => Some(Self::LX),
            16 => Some(Self::LY),
            17 => Some(Self::XR),
            18 => Some(Self::XL),
            19 => Some(Self::YR),
            20 => Some(Self::YL),
            21 => Some(Self::PP),
            22 => Some(Self::PQ),
            23 => Some(Self::QP),
            24 => Some(Self::QQ),
            25 => Some(Self::RCircular),
            26 => Some(Self::LCircular),
            27 => Some(Self::Linear),
            28 => Some(Self::Ptotal),
            29 => Some(Self::Plinear),
            30 => Some(Self::PFtotal),
            31 => Some(Self::PFlinear),
            32 => Some(Self::Pangle),
            _ => None,
        }
    }

    /// Converts a standard Stokes string name to the corresponding type.
    pub fn from_name(name: &str) -> Option<Self> {
        if name == "??" {
            return Some(Self::Undefined);
        }
        casa_core::Correlation::ALL
            .into_iter()
            .find(|value| value.label().eq_ignore_ascii_case(name))
            .and_then(|value| Self::from_code(value.casacore_code()))
    }

    /// Returns the standard string name (e.g. "I", "RR", "XX", "Pangle").
    pub fn name(self) -> &'static str {
        casa_core::Correlation::from_casacore_code(self.code())
            .map_or("??", casa_core::Correlation::label)
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
    axis_name: String,
}

impl StokesCoordinate {
    /// Creates a new Stokes coordinate with the given parameter list.
    ///
    /// # Examples
    ///
    /// ```
    /// use casa_coordinates::{StokesCoordinate, StokesType};
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
        Self {
            stokes,
            axis_name: "Stokes".into(),
        }
    }

    /// Returns the number of Stokes parameters.
    pub fn n_stokes(&self) -> usize {
        self.stokes.len()
    }

    /// Returns the Stokes types in order.
    pub fn stokes(&self) -> &[StokesType] {
        &self.stokes
    }

    /// Reconstructs a Stokes coordinate from a serialized record.
    pub fn from_record(rec: &RecordValue) -> Result<Self, CoordinateError> {
        let name = get_optional_vec_string(rec, "axes")
            .and_then(|names| names.into_iter().next())
            .unwrap_or_else(|| "Stokes".into());
        if let Ok(codes) = get_required_vec_i32(rec, "stokes") {
            let stokes = codes
                .into_iter()
                .map(|code| {
                    StokesType::from_code(code).ok_or_else(|| {
                        CoordinateError::InvalidRecord(format!("invalid stokes code {code}"))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(Self {
                stokes,
                axis_name: name,
            });
        }

        let names = get_optional_vec_string(rec, "stokes")
            .ok_or_else(|| CoordinateError::InvalidRecord("missing or invalid stokes".into()))?;
        let stokes = names
            .into_iter()
            .map(|name| {
                StokesType::from_name(&name).ok_or_else(|| {
                    CoordinateError::InvalidRecord(format!("invalid stokes name {name}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            stokes,
            axis_name: name,
        })
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
        Ok(vec![self.world_at_pixel(pixel[0])?])
    }

    fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        if world.len() != 1 {
            return Err(CoordinateError::DimensionMismatch {
                expected: 1,
                got: world.len(),
            });
        }
        let world = world[0];
        let pos = self
            .stokes
            .iter()
            .position(|stokes| {
                // casacore::near uses a relative tolerance except at zero.
                let code = f64::from(stokes.code());
                if code == 0.0 {
                    world.abs() <= (1.0 + 1.0e-13) * f64::MIN_POSITIVE
                } else {
                    world.is_finite() && (world - code).abs() <= 1.0e-13 * world.abs().max(code)
                }
            })
            .ok_or_else(|| {
                CoordinateError::ConversionFailed(format!(
                    "Stokes value {world} is not contained in this StokesCoordinate"
                ))
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
        vec![self.axis_name.clone()]
    }

    fn axis_units(&self) -> Vec<String> {
        vec![String::new()]
    }
}

impl StokesCoordinate {
    pub(crate) fn world_at_pixel(&self, pixel: f64) -> Result<f64, CoordinateError> {
        let index = (pixel + 0.5).trunc();
        if !index.is_finite() || index < 0.0 || index >= self.stokes.len() as f64 {
            return Err(CoordinateError::AxisOutOfRange {
                index: index as usize,
                naxes: self.stokes.len(),
            });
        }
        Ok(f64::from(self.stokes[index as usize].code()))
    }

    pub(crate) fn to_record(&self) -> RecordValue {
        let mut rec = RecordValue::default();
        rec.upsert(
            "axes",
            Value::Array(casa_core::ArrayValue::from_string_vec(self.axis_names())),
        );
        rec.upsert(
            "stokes",
            Value::Array(casa_core::ArrayValue::from_string_vec(
                self.stokes
                    .iter()
                    .map(|stokes| stokes.name().to_string())
                    .collect(),
            )),
        );
        rec.upsert(
            "crval",
            Value::Array(casa_core::ArrayValue::from_f64_vec(self.reference_value())),
        );
        rec.upsert(
            "crpix",
            Value::Array(casa_core::ArrayValue::from_f64_vec(self.reference_pixel())),
        );
        rec.upsert(
            "cdelt",
            Value::Array(casa_core::ArrayValue::from_f64_vec(self.increment())),
        );
        rec.upsert(
            "pc",
            Value::Array(casa_core::ArrayValue::Float64(
                ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[1, 1]), vec![1.0])
                    .expect("1x1 stokes pc matrix"),
            )),
        );
        rec
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
        // Native CASA records support unspecified (0) and the entire
        // casacore enumeration, including mixed correlations.
        for code in 0..=32 {
            let st = StokesType::from_code(code).unwrap();
            assert_eq!(st.code(), code);
            assert_eq!(StokesType::from_name(st.name()), Some(st));
        }
        assert!(StokesType::from_code(-1).is_none());
        assert!(StokesType::from_code(33).is_none());
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
        assert!(coord.to_world(&[-1.5]).is_err());
        assert_eq!(coord.to_world(&[-1.0]).unwrap(), vec![1.0]);
        assert_eq!(coord.to_world(&[0.5]).unwrap(), vec![2.0]);
    }

    #[test]
    fn to_pixel_missing_code() {
        let coord = make_iquv();
        // RR (code 5) is not in I/Q/U/V
        assert!(coord.to_pixel(&[5.0]).is_err());
        assert!(coord.to_pixel(&[1.49]).is_err());
        assert_eq!(coord.to_pixel(&[1.0 + 1e-14]).unwrap(), vec![0.0]);
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
        assert!(rec.get("axes").is_some());
    }

    #[test]
    fn to_record_matches_canonical_layout() {
        let coord = make_iquv();
        let rec = coord.to_record();
        assert_eq!(
            rec.get("axes"),
            Some(&Value::Array(casa_core::ArrayValue::from_string_vec(vec![
                "Stokes".to_string()
            ])))
        );
        assert_eq!(
            rec.get("stokes"),
            Some(&Value::Array(casa_core::ArrayValue::from_string_vec(vec![
                "I".to_string(),
                "Q".to_string(),
                "U".to_string(),
                "V".to_string(),
            ])))
        );
        assert!(rec.get("crval").is_some());
        assert!(rec.get("crpix").is_some());
        assert!(rec.get("cdelt").is_some());
        assert!(rec.get("pc").is_some());
    }

    #[test]
    fn casa_record_serialization_uses_legacy_fields() {
        let coord = StokesCoordinate::new(vec![StokesType::I]);
        let rec = coord.to_record();

        assert_eq!(
            rec.get("axes"),
            Some(&Value::Array(casa_core::ArrayValue::from_string_vec(vec![
                "Stokes".into()
            ])))
        );
        assert_eq!(
            rec.get("stokes"),
            Some(&Value::Array(casa_core::ArrayValue::from_string_vec(vec![
                "I".into()
            ])))
        );
        assert_eq!(
            rec.get("crval"),
            Some(&Value::Array(casa_core::ArrayValue::from_f64_vec(vec![
                1.0
            ])))
        );
        assert_eq!(
            rec.get("crpix"),
            Some(&Value::Array(casa_core::ArrayValue::from_f64_vec(vec![
                0.0
            ])))
        );
        assert_eq!(
            rec.get("cdelt"),
            Some(&Value::Array(casa_core::ArrayValue::from_f64_vec(vec![
                1.0
            ])))
        );
    }

    #[test]
    fn stokes_display() {
        assert_eq!(format!("{}", StokesType::V), "V");
        assert_eq!(format!("{}", StokesType::XX), "XX");
    }

    #[test]
    fn record_roundtrip() {
        let coord = StokesCoordinate::new(vec![StokesType::RR, StokesType::RL, StokesType::LL]);
        let restored = StokesCoordinate::from_record(&coord.to_record()).unwrap();
        assert_eq!(restored.stokes(), coord.stokes());
    }
}
