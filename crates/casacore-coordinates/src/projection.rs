// SPDX-License-Identifier: LGPL-3.0-or-later
//! Spherical projection types and mathematics.
//!
//! This module implements the map projections used to relate the native
//! spherical coordinates (phi, theta) on the celestial sphere to the
//! intermediate (x, y) coordinates in the projection plane. The projection
//! math follows Calabretta & Greisen (2002), "Representations of celestial
//! coordinates in FITS" (A&A 395, 1077-1122).
//!
//! # Supported projections
//!
//! | Code | Name | FITS keyword |
//! |------|------|-------------|
//! | SIN  | Slant orthographic | `-SIN` |
//! | TAN  | Gnomonic (tangent plane) | `-TAN` |
//! | ARC  | Zenithal equidistant | `-ARC` |
//! | CAR  | Plate carree (cylindrical equidistant) | `-CAR` |
//! | SFL  | Sanson-Flamsteed (sinusoidal) | `-SFL` |
//! | MER  | Mercator | `-MER` |
//! | AIT  | Hammer-Aitoff | `-AIT` |
//! | ZEA  | Zenithal equal-area | `-ZEA` |
//! | STG  | Stereographic | `-STG` |
//! | NCP  | North Celestial Pole (SIN special case) | `-NCP` |

use std::f64::consts::{FRAC_PI_2, PI};
use std::fmt;

use crate::error::CoordinateError;

/// Degrees-to-radians conversion factor.
const DEG_TO_RAD: f64 = PI / 180.0;

/// Radians-to-degrees conversion factor.
const RAD_TO_DEG: f64 = 180.0 / PI;

/// The enumeration of supported projection types.
///
/// Each variant corresponds to a FITS WCS projection code. The mathematical
/// definitions follow Calabretta & Greisen (2002).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProjectionType {
    /// Slant orthographic projection.
    SIN,
    /// Gnomonic (tangent-plane) projection.
    TAN,
    /// Zenithal equidistant projection.
    ARC,
    /// Plate carree (cylindrical equidistant) projection.
    CAR,
    /// Sanson-Flamsteed (sinusoidal) projection.
    SFL,
    /// Mercator projection.
    MER,
    /// Hammer-Aitoff equal-area projection.
    AIT,
    /// Zenithal equal-area projection.
    ZEA,
    /// Stereographic projection.
    STG,
    /// North Celestial Pole (SIN with cot(dec_0) shift). Legacy AIPS convention.
    NCP,
}

impl ProjectionType {
    /// Parses a projection name (case-insensitive) to a [`ProjectionType`].
    ///
    /// Returns `None` if the name is not recognised.
    ///
    /// # Examples
    ///
    /// ```
    /// use casacore_coordinates::ProjectionType;
    ///
    /// assert_eq!(ProjectionType::from_name("SIN"), Some(ProjectionType::SIN));
    /// assert_eq!(ProjectionType::from_name("tan"), Some(ProjectionType::TAN));
    /// assert_eq!(ProjectionType::from_name("BON"), None);
    /// ```
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_uppercase().as_str() {
            "SIN" => Some(Self::SIN),
            "TAN" => Some(Self::TAN),
            "ARC" => Some(Self::ARC),
            "CAR" => Some(Self::CAR),
            "SFL" => Some(Self::SFL),
            "MER" => Some(Self::MER),
            "AIT" => Some(Self::AIT),
            "ZEA" => Some(Self::ZEA),
            "STG" => Some(Self::STG),
            "NCP" => Some(Self::NCP),
            _ => None,
        }
    }

    /// Returns the standard three-letter FITS WCS code for this projection.
    pub fn name(self) -> &'static str {
        match self {
            Self::SIN => "SIN",
            Self::TAN => "TAN",
            Self::ARC => "ARC",
            Self::CAR => "CAR",
            Self::SFL => "SFL",
            Self::MER => "MER",
            Self::AIT => "AIT",
            Self::ZEA => "ZEA",
            Self::STG => "STG",
            Self::NCP => "NCP",
        }
    }
}

impl fmt::Display for ProjectionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A projection with its type and optional parameters.
///
/// The `parameters` field carries projection-specific constants. For most
/// projections this is empty; for NCP and generalised SIN the parameters
/// encode the shift terms (xi, eta) as defined in Calabretta & Greisen (2002)
/// equations (38)-(39).
///
/// # Examples
///
/// ```
/// use casacore_coordinates::{Projection, ProjectionType};
///
/// let proj = Projection::new(ProjectionType::SIN);
/// assert_eq!(proj.projection_type(), ProjectionType::SIN);
/// assert!(proj.parameters().is_empty());
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Projection {
    projection_type: ProjectionType,
    parameters: Vec<f64>,
}

impl Projection {
    /// Creates a projection with no extra parameters.
    pub fn new(projection_type: ProjectionType) -> Self {
        Self {
            projection_type,
            parameters: Vec::new(),
        }
    }

    /// Creates a projection with the given parameters.
    pub fn with_parameters(projection_type: ProjectionType, parameters: Vec<f64>) -> Self {
        Self {
            projection_type,
            parameters,
        }
    }

    /// Returns the projection type.
    pub fn projection_type(&self) -> ProjectionType {
        self.projection_type
    }

    /// Returns the projection parameters (may be empty).
    pub fn parameters(&self) -> &[f64] {
        &self.parameters
    }

    /// Returns the standard name of this projection.
    pub fn name(&self) -> &str {
        self.projection_type.name()
    }

    /// Forward projection: native spherical (phi, theta) in radians to
    /// intermediate (x, y) in radians.
    ///
    /// Follows Calabretta & Greisen (2002) conventions:
    /// - phi is the native longitude (0 at the reference point)
    /// - theta is the native latitude (pi/2 at the pole)
    /// - x increases to the left (east), y increases upward (north)
    pub fn project(&self, phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
        match self.projection_type {
            ProjectionType::SIN => project_sin(phi, theta, &self.parameters),
            ProjectionType::TAN => project_tan(phi, theta),
            ProjectionType::ARC => project_arc(phi, theta),
            ProjectionType::CAR => project_car(phi, theta),
            ProjectionType::SFL => project_sfl(phi, theta),
            ProjectionType::MER => project_mer(phi, theta),
            ProjectionType::AIT => project_ait(phi, theta),
            ProjectionType::ZEA => project_zea(phi, theta),
            ProjectionType::STG => project_stg(phi, theta),
            ProjectionType::NCP => project_ncp(phi, theta),
        }
    }

    /// Reverse projection: intermediate (x, y) in radians to native
    /// spherical (phi, theta) in radians.
    pub fn deproject(&self, x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
        match self.projection_type {
            ProjectionType::SIN => deproject_sin(x, y, &self.parameters),
            ProjectionType::TAN => deproject_tan(x, y),
            ProjectionType::ARC => deproject_arc(x, y),
            ProjectionType::CAR => deproject_car(x, y),
            ProjectionType::SFL => deproject_sfl(x, y),
            ProjectionType::MER => deproject_mer(x, y),
            ProjectionType::AIT => deproject_ait(x, y),
            ProjectionType::ZEA => deproject_zea(x, y),
            ProjectionType::STG => deproject_stg(x, y),
            ProjectionType::NCP => deproject_ncp(x, y),
        }
    }
}

// ---------------------------------------------------------------------------
// SIN (slant orthographic)
// ---------------------------------------------------------------------------

/// SIN forward projection (Calabretta & Greisen 2002, eq. 36-37).
///
/// Standard zenithal form:
///   R = cos(theta)
///   x = R * sin(phi) = cos(theta) * sin(phi)
///   y = -R * cos(phi) = -cos(theta) * cos(phi)
///
/// At (phi=0, theta=pi/2), (x,y) = (0,0).
///
/// Generalised SIN adds shift parameters (xi, eta).
fn project_sin(phi: f64, theta: f64, params: &[f64]) -> Result<(f64, f64), CoordinateError> {
    let cos_theta = theta.cos();
    let sin_phi = phi.sin();
    let cos_phi = phi.cos();

    let xi = params.first().copied().unwrap_or(0.0);
    let eta = params.get(1).copied().unwrap_or(0.0);

    let sin_theta = theta.sin();
    let x = cos_theta * sin_phi + xi * (1.0 - sin_theta);
    let y = -cos_theta * cos_phi + eta * (1.0 - sin_theta);
    Ok((x, y))
}

fn deproject_sin(x: f64, y: f64, params: &[f64]) -> Result<(f64, f64), CoordinateError> {
    let xi = params.first().copied().unwrap_or(0.0);
    let eta = params.get(1).copied().unwrap_or(0.0);

    if xi == 0.0 && eta == 0.0 {
        // Standard SIN deproject:
        //   x = cos(theta) * sin(phi)
        //   y = -cos(theta) * cos(phi)
        //
        //   x^2 + y^2 = cos^2(theta) = R^2
        let r2 = x * x + y * y;
        if r2 > 1.0 + 1e-12 {
            return Err(CoordinateError::ConversionFailed(
                "SIN deproject: point outside unit circle".into(),
            ));
        }
        let r = r2.min(1.0).sqrt();
        // cos(theta) = r, so theta = atan2(sin_theta, cos_theta)
        let sin_theta = (1.0 - r2.min(1.0)).max(0.0).sqrt();
        let theta = sin_theta.atan2(r);
        let phi = x.atan2(-y);
        Ok((phi, theta))
    } else {
        // Generalised SIN:
        //   x = cos(theta)*sin(phi) + xi*(1 - sin(theta))
        //   y = -cos(theta)*cos(phi) + eta*(1 - sin(theta))
        //
        // Let s = sin_theta, c = cos_theta = sqrt(1-s^2).
        // x' = x - xi*(1-s) = c*sin(phi)
        // y' = y - eta*(1-s) = -c*cos(phi)
        // x'^2 + y'^2 = c^2 = 1 - s^2
        //
        // Expand: [x - xi + xi*s]^2 + [y - eta + eta*s]^2 = 1 - s^2
        // Let a = xi, b = eta, X = x - xi, Y = y - eta:
        // (X + a*s)^2 + (Y + b*s)^2 = 1 - s^2
        // X^2 + 2*X*a*s + a^2*s^2 + Y^2 + 2*Y*b*s + b^2*s^2 = 1 - s^2
        // s^2*(a^2 + b^2 + 1) + 2*s*(X*a + Y*b) + (X^2 + Y^2 - 1) = 0
        let big_x = x - xi;
        let big_y = y - eta;
        let a_coeff = xi * xi + eta * eta + 1.0;
        let b_coeff = 2.0 * (big_x * xi + big_y * eta);
        let c_coeff = big_x * big_x + big_y * big_y - 1.0;
        let disc = b_coeff * b_coeff - 4.0 * a_coeff * c_coeff;
        if disc < -1e-12 {
            return Err(CoordinateError::ConversionFailed(
                "SIN deproject: no solution (generalised)".into(),
            ));
        }
        let disc = disc.max(0.0);
        // Take the root closest to 1 (near the pole):
        let s = (-b_coeff + disc.sqrt()) / (2.0 * a_coeff);
        let s = s.clamp(-1.0, 1.0);
        let theta = s.asin();
        let cos_theta = theta.cos();
        let xp = x - xi * (1.0 - s);
        let yp = y - eta * (1.0 - s);
        let phi = if cos_theta.abs() < 1e-15 {
            0.0
        } else {
            xp.atan2(-yp)
        };
        Ok((phi, theta))
    }
}

// ---------------------------------------------------------------------------
// TAN (gnomonic)
// ---------------------------------------------------------------------------

fn project_tan(phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
    let cos_theta = theta.cos();
    let sin_theta = theta.sin();
    if sin_theta <= 0.0 {
        return Err(CoordinateError::ConversionFailed(
            "TAN project: theta <= 0 (behind tangent point)".into(),
        ));
    }
    let r = cos_theta / sin_theta;
    Ok((r * phi.sin(), -r * phi.cos()))
}

fn deproject_tan(x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
    let r = (x * x + y * y).sqrt();
    let phi = x.atan2(-y);
    let theta = (1.0_f64).atan2(r);
    Ok((phi, theta))
}

// ---------------------------------------------------------------------------
// ARC (zenithal equidistant)
// ---------------------------------------------------------------------------

fn project_arc(phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
    let r = FRAC_PI_2 - theta;
    Ok((r * phi.sin(), -r * phi.cos()))
}

fn deproject_arc(x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
    let r = (x * x + y * y).sqrt();
    let phi = x.atan2(-y);
    let theta = FRAC_PI_2 - r;
    Ok((phi, theta))
}

// ---------------------------------------------------------------------------
// CAR (plate carree)
// ---------------------------------------------------------------------------

fn project_car(phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
    Ok((phi, theta))
}

fn deproject_car(x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
    Ok((x, y))
}

// ---------------------------------------------------------------------------
// SFL (Sanson-Flamsteed / sinusoidal)
// ---------------------------------------------------------------------------

fn project_sfl(phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
    Ok((phi * theta.cos(), theta))
}

fn deproject_sfl(x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
    let theta = y;
    let cos_theta = theta.cos();
    let phi = if cos_theta.abs() < 1e-15 {
        0.0
    } else {
        x / cos_theta
    };
    Ok((phi, theta))
}

// ---------------------------------------------------------------------------
// MER (Mercator)
// ---------------------------------------------------------------------------

fn project_mer(phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
    if (FRAC_PI_2 - theta.abs()).abs() < 1e-12 {
        return Err(CoordinateError::ConversionFailed(
            "MER project: theta at pole".into(),
        ));
    }
    let y = ((FRAC_PI_2 + theta) / 2.0).tan().ln();
    Ok((phi, y))
}

fn deproject_mer(x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
    let theta = 2.0 * y.exp().atan() - FRAC_PI_2;
    Ok((x, theta))
}

// ---------------------------------------------------------------------------
// AIT (Hammer-Aitoff)
// ---------------------------------------------------------------------------

fn project_ait(phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
    let cos_theta = theta.cos();
    let half_phi = phi / 2.0;
    let gamma = (2.0 / (1.0 + cos_theta * half_phi.cos())).sqrt();
    let x = 2.0 * gamma * cos_theta * half_phi.sin();
    let y = gamma * theta.sin();
    Ok((x, y))
}

fn deproject_ait(x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
    let z2 = 1.0 - (x / 4.0).powi(2) - (y / 2.0).powi(2);
    if z2 < 0.0 {
        return Err(CoordinateError::ConversionFailed(
            "AIT deproject: point outside projection boundary".into(),
        ));
    }
    let z = z2.sqrt();
    let theta = (y * z).asin();
    let phi = 2.0 * (z * x / 2.0).atan2(2.0 * z2 - 1.0);
    Ok((phi, theta))
}

// ---------------------------------------------------------------------------
// ZEA (zenithal equal-area)
// ---------------------------------------------------------------------------

fn project_zea(phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
    let r = (2.0 * (1.0 - theta.sin())).sqrt();
    Ok((r * phi.sin(), -r * phi.cos()))
}

fn deproject_zea(x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
    let r = (x * x + y * y).sqrt();
    if r > 2.0 + 1e-12 {
        return Err(CoordinateError::ConversionFailed(
            "ZEA deproject: r > 2".into(),
        ));
    }
    let r_clamped = r.min(2.0);
    let theta = (1.0 - r_clamped * r_clamped / 2.0).asin();
    let phi = x.atan2(-y);
    Ok((phi, theta))
}

// ---------------------------------------------------------------------------
// STG (stereographic)
// ---------------------------------------------------------------------------

fn project_stg(phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
    let sin_theta = theta.sin();
    if (1.0 + sin_theta).abs() < 1e-12 {
        return Err(CoordinateError::ConversionFailed(
            "STG project: theta at south pole".into(),
        ));
    }
    let r = 2.0 * theta.cos() / (1.0 + sin_theta);
    Ok((r * phi.sin(), -r * phi.cos()))
}

fn deproject_stg(x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
    let r = (x * x + y * y).sqrt();
    let theta = FRAC_PI_2 - 2.0 * (r / 2.0).atan();
    let phi = x.atan2(-y);
    Ok((phi, theta))
}

// ---------------------------------------------------------------------------
// NCP (North Celestial Pole — legacy AIPS SIN variant)
// ---------------------------------------------------------------------------

/// NCP is equivalent to SIN with eta = 1/tan(theta_0), but for the
/// projection math alone we treat theta_0 = pi/2 (north pole). The NCP
/// forward/reverse are the standard SIN formulae used at the pole.
fn project_ncp(phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
    let sin_theta = theta.sin();
    if sin_theta.abs() < 1e-12 {
        return Err(CoordinateError::ConversionFailed(
            "NCP project: theta = 0".into(),
        ));
    }
    let cos_theta = theta.cos();
    let x = cos_theta * phi.sin();
    // NCP: y = (1 - cos(theta)*cos(phi)) / sin(theta)
    let y = (1.0 - cos_theta * phi.cos()) / sin_theta;
    Ok((x, y))
}

fn deproject_ncp(x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
    if y.abs() < 1e-15 && x.abs() < 1e-15 {
        return Ok((0.0, FRAC_PI_2));
    }
    // theta = atan2(1, sqrt(x^2 + (1-y)^2) ) ... iterative for general case
    // For NCP at pole: sin(theta) = 1/(y + cos(phi)*sec(theta)) ...
    // Standard formula: theta = acos(sqrt(x^2 + (1-y)^2))  ... hmm
    // Actually NCP: x = cos(theta)*sin(phi), y = (1 - cos(theta)*cos(phi))/sin(theta)
    // => sin(theta)*y = 1 - cos(theta)*cos(phi)
    // => cos(theta)*cos(phi) = 1 - sin(theta)*y
    // Also x^2 + cos^2(theta)*cos^2(phi) = cos^2(theta)
    // => x^2 + (1 - sin(theta)*y)^2 = cos^2(theta) = 1 - sin^2(theta)
    // => x^2 + 1 - 2*sin(theta)*y + sin^2(theta)*y^2 = 1 - sin^2(theta)
    // => sin^2(theta)*(1 + y^2) - 2*sin(theta)*y + x^2 = 0
    // Quadratic in sin(theta):
    let a = 1.0 + y * y;
    let b = -2.0 * y;
    let c = x * x;
    let disc = b * b - 4.0 * a * c;
    if disc < 0.0 {
        return Err(CoordinateError::ConversionFailed(
            "NCP deproject: no solution".into(),
        ));
    }
    // Take the solution closest to sin(theta)=1 (north pole)
    let sin_theta = (-b + disc.sqrt()) / (2.0 * a);
    if !(-1.0 - 1e-12..=1.0 + 1e-12).contains(&sin_theta) {
        return Err(CoordinateError::ConversionFailed(
            "NCP deproject: sin(theta) out of range".into(),
        ));
    }
    let sin_theta = sin_theta.clamp(-1.0, 1.0);
    let theta = sin_theta.asin();
    let cos_theta = theta.cos();
    // phi = atan2(x / cos_theta, (1 - sin_theta*y) / cos_theta)
    let phi = if cos_theta.abs() < 1e-15 {
        0.0
    } else {
        (x / cos_theta).atan2((1.0 - sin_theta * y) / cos_theta)
    };
    Ok((phi, theta))
}

// ---------------------------------------------------------------------------
// Unit conversion helpers (used by DirectionCoordinate)
// ---------------------------------------------------------------------------

/// Converts degrees to radians.
#[inline]
pub fn deg_to_rad(deg: f64) -> f64 {
    deg * DEG_TO_RAD
}

/// Converts radians to degrees.
#[inline]
pub fn rad_to_deg(rad: f64) -> f64 {
    rad * RAD_TO_DEG
}

#[cfg(test)]
mod tests {
    use super::*;
    const TOL: f64 = 1e-12;

    #[test]
    fn from_name_all() {
        assert_eq!(ProjectionType::from_name("SIN"), Some(ProjectionType::SIN));
        assert_eq!(ProjectionType::from_name("tan"), Some(ProjectionType::TAN));
        assert_eq!(ProjectionType::from_name("Arc"), Some(ProjectionType::ARC));
        assert_eq!(ProjectionType::from_name("car"), Some(ProjectionType::CAR));
        assert_eq!(ProjectionType::from_name("SFL"), Some(ProjectionType::SFL));
        assert_eq!(ProjectionType::from_name("MER"), Some(ProjectionType::MER));
        assert_eq!(ProjectionType::from_name("AIT"), Some(ProjectionType::AIT));
        assert_eq!(ProjectionType::from_name("ZEA"), Some(ProjectionType::ZEA));
        assert_eq!(ProjectionType::from_name("STG"), Some(ProjectionType::STG));
        assert_eq!(ProjectionType::from_name("NCP"), Some(ProjectionType::NCP));
        assert_eq!(ProjectionType::from_name("BON"), None);
    }

    #[test]
    fn projection_name() {
        let proj = Projection::new(ProjectionType::TAN);
        assert_eq!(proj.name(), "TAN");
    }

    fn roundtrip(proj: &Projection, phi: f64, theta: f64) {
        let (x, y) = proj.project(phi, theta).unwrap();
        let (phi2, theta2) = proj.deproject(x, y).unwrap();
        assert!(
            (phi2 - phi).abs() < TOL,
            "{}: phi roundtrip failed: {phi} -> {phi2} (diff {})",
            proj.name(),
            (phi2 - phi).abs()
        );
        assert!(
            (theta2 - theta).abs() < TOL,
            "{}: theta roundtrip failed: {theta} -> {theta2} (diff {})",
            proj.name(),
            (theta2 - theta).abs()
        );
    }

    #[test]
    fn sin_roundtrip() {
        let proj = Projection::new(ProjectionType::SIN);
        roundtrip(&proj, 0.1, 1.2);
        roundtrip(&proj, 0.0, FRAC_PI_2);
        roundtrip(&proj, -0.3, 1.0);
    }

    #[test]
    fn tan_roundtrip() {
        let proj = Projection::new(ProjectionType::TAN);
        roundtrip(&proj, 0.1, 1.2);
        roundtrip(&proj, 0.0, FRAC_PI_2);
        roundtrip(&proj, -0.3, 1.0);
    }

    #[test]
    fn arc_roundtrip() {
        let proj = Projection::new(ProjectionType::ARC);
        roundtrip(&proj, 0.1, 1.2);
        roundtrip(&proj, 0.0, FRAC_PI_2);
        roundtrip(&proj, 0.5, 0.3);
    }

    #[test]
    fn car_roundtrip() {
        let proj = Projection::new(ProjectionType::CAR);
        roundtrip(&proj, 0.5, 0.8);
        roundtrip(&proj, -1.0, -0.5);
    }

    #[test]
    fn sfl_roundtrip() {
        let proj = Projection::new(ProjectionType::SFL);
        roundtrip(&proj, 0.5, 0.8);
        roundtrip(&proj, -0.3, 1.0);
    }

    #[test]
    fn mer_roundtrip() {
        let proj = Projection::new(ProjectionType::MER);
        roundtrip(&proj, 0.5, 0.8);
        roundtrip(&proj, -1.0, -0.5);
    }

    #[test]
    fn ait_roundtrip() {
        let proj = Projection::new(ProjectionType::AIT);
        roundtrip(&proj, 0.3, 0.5);
        roundtrip(&proj, -0.5, 0.8);
    }

    #[test]
    fn zea_roundtrip() {
        let proj = Projection::new(ProjectionType::ZEA);
        roundtrip(&proj, 0.1, 1.2);
        roundtrip(&proj, 0.0, FRAC_PI_2);
    }

    #[test]
    fn stg_roundtrip() {
        let proj = Projection::new(ProjectionType::STG);
        roundtrip(&proj, 0.1, 1.2);
        roundtrip(&proj, 0.0, FRAC_PI_2);
    }

    #[test]
    fn car_is_identity() {
        let proj = Projection::new(ProjectionType::CAR);
        let phi = 0.3;
        let theta = 0.7;
        let (x, y) = proj.project(phi, theta).unwrap();
        assert!((x - phi).abs() < TOL);
        assert!((y - theta).abs() < TOL);
    }

    #[test]
    fn sin_at_pole() {
        let proj = Projection::new(ProjectionType::SIN);
        let (x, y) = proj.project(0.0, FRAC_PI_2).unwrap();
        // At the native pole (phi=0, theta=pi/2): x=0, y=0
        assert!(x.abs() < TOL);
        assert!(y.abs() < TOL);
    }

    #[test]
    fn tan_behind_tangent_fails() {
        let proj = Projection::new(ProjectionType::TAN);
        let result = proj.project(0.0, -0.1);
        assert!(result.is_err());
    }

    #[test]
    fn ncp_roundtrip() {
        let proj = Projection::new(ProjectionType::NCP);
        // NCP works near the pole
        let phi = 0.05;
        let theta = 1.4; // close to pi/2
        let (x, y) = proj.project(phi, theta).unwrap();
        let (phi2, theta2) = proj.deproject(x, y).unwrap();
        assert!(
            (phi2 - phi).abs() < 1e-10,
            "NCP phi roundtrip: {phi} -> {phi2}"
        );
        assert!(
            (theta2 - theta).abs() < 1e-10,
            "NCP theta roundtrip: {theta} -> {theta2}"
        );
    }

    #[test]
    fn generalised_sin_roundtrip() {
        let proj = Projection::with_parameters(ProjectionType::SIN, vec![0.1, 0.2]);
        let phi = 0.1;
        let theta = 1.2;
        let (x, y) = proj.project(phi, theta).unwrap();
        let (phi2, theta2) = proj.deproject(x, y).unwrap();
        assert!(
            (phi2 - phi).abs() < 1e-10,
            "gen-SIN phi roundtrip: {phi} -> {phi2}"
        );
        assert!(
            (theta2 - theta).abs() < 1e-10,
            "gen-SIN theta roundtrip: {theta} -> {theta2}"
        );
    }

    #[test]
    fn projection_type_display() {
        assert_eq!(format!("{}", ProjectionType::SIN), "SIN");
        assert_eq!(format!("{}", ProjectionType::MER), "MER");
    }

    #[test]
    fn deg_rad_conversion() {
        assert!((deg_to_rad(180.0) - PI).abs() < TOL);
        assert!((rad_to_deg(PI) - 180.0).abs() < TOL);
        assert!((deg_to_rad(90.0) - FRAC_PI_2).abs() < TOL);
    }
}
