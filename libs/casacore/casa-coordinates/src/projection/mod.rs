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
//! | AZP  | Zenithal perspective with a tilted plane | `-AZP` |
//! | ZPN  | Zenithal polynomial | `-ZPN` |
//! | SZP  | Slant zenithal perspective | `-SZP` |
//! | AIR  | Airy's zenithal projection | `-AIR` |
//! | TAN  | Gnomonic (tangent plane) | `-TAN` |
//! | ARC  | Zenithal equidistant | `-ARC` |
//! | CAR  | Plate carree (cylindrical equidistant) | `-CAR` |
//! | CYP  | Cylindrical perspective | `-CYP` |
//! | CEA  | Cylindrical equal area | `-CEA` |
//! | COP  | Conic perspective | `-COP` |
//! | COD  | Conic equidistant | `-COD` |
//! | COE  | Conic equal area | `-COE` |
//! | COO  | Conic orthomorphic | `-COO` |
//! | BON  | Bonne's equal-area projection | `-BON` |
//! | SFL  | Sanson-Flamsteed (sinusoidal) | `-SFL` |
//! | PAR  | Parabolic | `-PAR` |
//! | MOL  | Mollweide equal-area | `-MOL` |
//! | PCO  | Polyconic | `-PCO` |
//! | MER  | Mercator | `-MER` |
//! | AIT  | Hammer-Aitoff | `-AIT` |
//! | ZEA  | Zenithal equal-area | `-ZEA` |
//! | STG  | Stereographic | `-STG` |
//! | TSC  | Tangential spherical cube | `-TSC` |
//! | CSC  | COBE quadrilateralized spherical cube | `-CSC` |
//! | QSC  | Quadrilateralized spherical cube | `-QSC` |
//! | HPX  | HEALPix | `-HPX` |
//! | NCP  | North Celestial Pole (SIN special case) | `-NCP` |

use std::f64::consts::{FRAC_PI_2, LN_2, PI, SQRT_2, TAU};
use std::fmt;

use crate::error::CoordinateError;

mod azp;
mod bonne;
mod conic;
mod cube;
mod cylindrical;
mod healpix;
mod szp;
mod zpn;

use azp::ZenithalPerspective;
use szp::SlantZenithalPerspective;

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
    /// Zenithal perspective with a tilted projection plane.
    AZP,
    /// Zenithal polynomial projection.
    ZPN,
    /// Slant zenithal perspective with a displaced projection point.
    SZP,
    /// Airy's zenithal projection.
    AIR,
    /// Gnomonic (tangent-plane) projection.
    TAN,
    /// Zenithal equidistant projection.
    ARC,
    /// Plate carree (cylindrical equidistant) projection.
    CAR,
    /// Cylindrical perspective projection.
    CYP,
    /// Cylindrical equal-area projection.
    CEA,
    /// Conic perspective projection.
    COP,
    /// Conic equidistant projection.
    COD,
    /// Conic equal-area projection.
    COE,
    /// Conic orthomorphic projection.
    COO,
    /// Sanson-Flamsteed (sinusoidal) projection.
    SFL,
    /// Parabolic projection.
    PAR,
    /// Mollweide equal-area projection.
    MOL,
    /// Polyconic projection.
    PCO,
    /// Bonne's equal-area projection.
    BON,
    /// Tangential spherical cube.
    TSC,
    /// COBE quadrilateralized spherical cube.
    CSC,
    /// Quadrilateralized spherical cube.
    QSC,
    /// HEALPix projection.
    HPX,
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
    /// Defaults for the latitude-axis `PV` parameters supported by the
    /// FITS reader and writer, starting at [`Self::fits_parameter_start`].
    #[must_use]
    pub fn fits_parameter_defaults(self) -> &'static [f64] {
        match self {
            Self::AZP => &[0.0, 0.0],
            Self::ZPN => &[0.0; 30],
            Self::SZP => &[0.0, 0.0, 90.0],
            Self::AIR => &[90.0],
            Self::CYP => &[1.0, 1.0],
            Self::CEA => &[1.0],
            Self::COP | Self::COD | Self::COE | Self::COO => &[0.0, 0.0],
            Self::BON => &[0.0],
            Self::HPX => &[4.0, 3.0],
            _ => &[],
        }
    }

    /// First latitude-axis FITS `PV` index for this projection.
    #[must_use]
    pub fn fits_parameter_start(self) -> usize {
        usize::from(self != Self::ZPN)
    }

    /// Parses a projection name (case-insensitive) to a [`ProjectionType`].
    ///
    /// Returns `None` if the name is not recognised.
    ///
    /// # Examples
    ///
    /// ```
    /// use casa_coordinates::ProjectionType;
    ///
    /// assert_eq!(ProjectionType::from_name("SIN"), Some(ProjectionType::SIN));
    /// assert_eq!(ProjectionType::from_name("tan"), Some(ProjectionType::TAN));
    /// assert_eq!(ProjectionType::from_name("XPH"), None);
    /// ```
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_uppercase().as_str() {
            "SIN" => Some(Self::SIN),
            "AZP" => Some(Self::AZP),
            "ZPN" => Some(Self::ZPN),
            "SZP" => Some(Self::SZP),
            "AIR" => Some(Self::AIR),
            "TAN" => Some(Self::TAN),
            "ARC" => Some(Self::ARC),
            "CAR" => Some(Self::CAR),
            "CYP" => Some(Self::CYP),
            "CEA" => Some(Self::CEA),
            "COP" => Some(Self::COP),
            "COD" => Some(Self::COD),
            "COE" => Some(Self::COE),
            "COO" => Some(Self::COO),
            "SFL" => Some(Self::SFL),
            "PAR" => Some(Self::PAR),
            "MOL" => Some(Self::MOL),
            "PCO" => Some(Self::PCO),
            "BON" => Some(Self::BON),
            "TSC" => Some(Self::TSC),
            "CSC" => Some(Self::CSC),
            "QSC" => Some(Self::QSC),
            "HPX" => Some(Self::HPX),
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
            Self::AZP => "AZP",
            Self::ZPN => "ZPN",
            Self::SZP => "SZP",
            Self::AIR => "AIR",
            Self::TAN => "TAN",
            Self::ARC => "ARC",
            Self::CAR => "CAR",
            Self::CYP => "CYP",
            Self::CEA => "CEA",
            Self::COP => "COP",
            Self::COD => "COD",
            Self::COE => "COE",
            Self::COO => "COO",
            Self::SFL => "SFL",
            Self::PAR => "PAR",
            Self::MOL => "MOL",
            Self::PCO => "PCO",
            Self::BON => "BON",
            Self::TSC => "TSC",
            Self::CSC => "CSC",
            Self::QSC => "QSC",
            Self::HPX => "HPX",
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
/// use casa_coordinates::{Projection, ProjectionType};
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
            ProjectionType::AZP => ZenithalPerspective::new(&self.parameters)?.project(phi, theta),
            ProjectionType::ZPN => {
                zpn::ZenithalPolynomial::new(&self.parameters)?.project(phi, theta)
            }
            ProjectionType::SZP => {
                SlantZenithalPerspective::new(&self.parameters)?.project(phi, theta)
            }
            ProjectionType::AIR => project_air(phi, theta, self.airy_coefficient()?),
            ProjectionType::TAN => project_tan(phi, theta),
            ProjectionType::ARC => project_arc(phi, theta),
            ProjectionType::CAR => project_car(phi, theta),
            ProjectionType::CYP => cylindrical::project_cyp(&self.parameters, phi, theta),
            ProjectionType::CEA => cylindrical::project_cea(&self.parameters, phi, theta),
            kind @ (ProjectionType::COP
            | ProjectionType::COD
            | ProjectionType::COE
            | ProjectionType::COO) => {
                conic::Conic::new(kind, &self.parameters)?.project(phi, theta)
            }
            ProjectionType::HPX => healpix::Healpix::new(&self.parameters)?.project(phi, theta),
            ProjectionType::SFL => project_sfl(phi, theta),
            ProjectionType::PAR => project_par(phi, theta),
            ProjectionType::MOL => project_mol(phi, theta),
            ProjectionType::PCO => project_pco(phi, theta),
            ProjectionType::BON => bonne::project(phi, theta, bonne::latitude(&self.parameters)?),
            ProjectionType::TSC | ProjectionType::CSC | ProjectionType::QSC => {
                cube::project(self.projection_type, phi, theta)
            }
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
            ProjectionType::AZP => ZenithalPerspective::new(&self.parameters)?.deproject(x, y),
            ProjectionType::ZPN => zpn::ZenithalPolynomial::new(&self.parameters)?.deproject(x, y),
            ProjectionType::SZP => SlantZenithalPerspective::new(&self.parameters)?.deproject(x, y),
            ProjectionType::AIR => deproject_air(x, y, self.airy_coefficient()?),
            ProjectionType::TAN => deproject_tan(x, y),
            ProjectionType::ARC => deproject_arc(x, y),
            ProjectionType::CAR => deproject_car(x, y),
            ProjectionType::CYP => cylindrical::deproject_cyp(&self.parameters, x, y),
            ProjectionType::CEA => cylindrical::deproject_cea(&self.parameters, x, y),
            kind @ (ProjectionType::COP
            | ProjectionType::COD
            | ProjectionType::COE
            | ProjectionType::COO) => conic::Conic::new(kind, &self.parameters)?.deproject(x, y),
            ProjectionType::HPX => healpix::Healpix::new(&self.parameters)?.deproject(x, y),
            ProjectionType::SFL => deproject_sfl(x, y),
            ProjectionType::PAR => deproject_par(x, y),
            ProjectionType::MOL => deproject_mol(x, y),
            ProjectionType::PCO => deproject_pco(x, y),
            ProjectionType::BON => bonne::deproject(x, y, bonne::latitude(&self.parameters)?),
            ProjectionType::TSC | ProjectionType::CSC | ProjectionType::QSC => {
                cube::deproject(self.projection_type, x, y)
            }
            ProjectionType::MER => deproject_mer(x, y),
            ProjectionType::AIT => deproject_ait(x, y),
            ProjectionType::ZEA => deproject_zea(x, y),
            ProjectionType::STG => deproject_stg(x, y),
            ProjectionType::NCP => deproject_ncp(x, y),
        }
    }

    /// Confirm that the supported transform can use the stated parameters.
    ///
    /// AZP reads the perspective distance and the plane tilt in degrees.
    /// Casacore pads missing SZP parameters with `[0, 0, 90]`. AIR uses a
    /// reference latitude in degrees, defaulting to 90°.
    /// Surplus parameters are stored but have no effect on these projections.
    pub fn validate_conversion_parameters(&self) -> Result<(), CoordinateError> {
        match self.projection_type {
            ProjectionType::AZP => ZenithalPerspective::new(&self.parameters).map(|_| ()),
            ProjectionType::ZPN => zpn::ZenithalPolynomial::new(&self.parameters).map(|_| ()),
            ProjectionType::SZP => SlantZenithalPerspective::new(&self.parameters).map(|_| ()),
            ProjectionType::AIR => self.airy_coefficient().map(|_| ()),
            ProjectionType::CYP => cylindrical::cyp_parameters(&self.parameters).map(|_| ()),
            ProjectionType::CEA => cylindrical::cea_scale(&self.parameters).map(|_| ()),
            kind @ (ProjectionType::COP
            | ProjectionType::COD
            | ProjectionType::COE
            | ProjectionType::COO) => conic::Conic::new(kind, &self.parameters).map(|_| ()),
            ProjectionType::HPX => healpix::Healpix::new(&self.parameters).map(|_| ()),
            ProjectionType::BON => bonne::latitude(&self.parameters).map(|_| ()),
            _ => Ok(()),
        }
    }

    fn airy_coefficient(&self) -> Result<f64, CoordinateError> {
        let latitude = self.parameters.first().copied().unwrap_or(90.0);
        let half_angle = (((90.0 - latitude) / 2.0) % 360.0).abs();
        let half_angle = half_angle.min(360.0 - half_angle);
        if !latitude.is_finite() || latitude <= -90.0 || half_angle > 90.0 {
            return Err(CoordinateError::ConversionFailed(
                "AIR requires a finite reference latitude above -90 degrees and a real coefficient"
                    .into(),
            ));
        }
        match half_angle {
            0.0 => Ok(1.0),
            45.0 => Ok(LN_2),
            90.0 => Ok(0.0),
            _ => {
                let tangent = (half_angle * DEG_TO_RAD).tan();
                Ok(airy_logarithmic_factor(tangent * tangent))
            }
        }
    }
}

/// The intersection closer to the native pole on a tilted projection.
fn planeward_latitude(midpoint: f64, offset: f64) -> f64 {
    let wrap = |theta| {
        if theta > FRAC_PI_2 {
            theta - TAU
        } else {
            theta
        }
    };
    wrap(midpoint - offset).max(wrap(midpoint + offset + PI))
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

    // WCSLIB accepts only the near side of the orthographic or slant projection.
    let horizon = if xi == 0.0 && eta == 0.0 {
        0.0
    } else {
        -(xi * sin_phi - eta * cos_phi).atan()
    };
    if theta < horizon {
        return Err(CoordinateError::OutsideProjectionDomain(
            "SIN project: point outside visible hemisphere",
        ));
    }

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
        let phi = if r2 == 0.0 { 0.0 } else { x.atan2(-y) };
        // Match WCSLIB's numerically stable orthographic branches. CASA image
        // coordinates are backed by WCSLIB, which uses acos near the native
        // pole and asin near the projection boundary rather than atan2.
        let theta = if r2 < 0.5 {
            r2.sqrt().acos()
        } else {
            (1.0 - r2.min(1.0)).max(0.0).sqrt().asin()
        };
        Ok((phi, theta))
    } else {
        // Generalised (slant) SIN, as WCSLIB's `sinx2s` solves it:
        //   x = cos(theta)*sin(phi) + xi*(1 - sin(theta))
        //   y = -cos(theta)*cos(phi) + eta*(1 - sin(theta))
        // so that, with s = sin(theta), w1 = xi^2 + eta^2 and
        // xy = x*xi + y*eta,
        //   (1 + w1)*s^2 + 2*(xy - w1)*s + (r2 - 2*xy + w1 - 1) = 0.
        // Near the native pole the root is s ~ 1, where asin would turn
        // the root's rounding into an error of sqrt(eps) in theta; WCSLIB
        // takes the small-angle form there instead.
        let r2 = x * x + y * y;
        let xy = x * xi + y * eta;
        let w1 = xi * xi + eta * eta;
        let (theta, z) = if r2 < 1.0e-10 {
            (FRAC_PI_2 - (r2 / (1.0 + xy)).sqrt(), r2 / 2.0)
        } else {
            let a = w1 + 1.0;
            let b = xy - w1;
            let c = r2 - xy - xy + w1 - 1.0;
            let d = b * b - a * c;
            if d < 0.0 {
                return Err(CoordinateError::ConversionFailed(
                    "SIN deproject: no solution (generalised)".into(),
                ));
            }
            let d = d.sqrt();
            // The solution nearest the pole.
            let (s1, s2) = ((-b + d) / a, (-b - d) / a);
            let mut s = s1.max(s2);
            if s > 1.0 {
                s = if s - 1.0 < 1.0e-13 { 1.0 } else { s1.min(s2) };
            }
            if s < -1.0 && s + 1.0 > -1.0e-13 {
                s = -1.0;
            }
            if !(-1.0..=1.0).contains(&s) {
                return Err(CoordinateError::ConversionFailed(
                    "SIN deproject: no solution (generalised)".into(),
                ));
            }
            (s.asin(), 1.0 - s)
        };
        let x1 = -y + eta * z;
        let y1 = x - xi * z;
        let phi = if x1 == 0.0 && y1 == 0.0 {
            0.0
        } else {
            y1.atan2(x1)
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
// AIR (Airy with θ_b = 0 or 90 degrees)
// ---------------------------------------------------------------------------

// WCSLIB uses the small-angle limit below this half-zenith angle.
const AIR_LINEAR_LIMIT: f64 = 1e-4;

/// Calabretta & Greisen (2002), Eq. 71, in radians. With t = tan(ξ), the
/// radius is ln(1 + t²)/t + kt. The latitude parameter sets k.
fn project_air(phi: f64, theta: f64, coefficient: f64) -> Result<(f64, f64), CoordinateError> {
    if theta <= -FRAC_PI_2 {
        return Err(CoordinateError::ConversionFailed(
            "AIR project: the native south pole is outside the projection".into(),
        ));
    }
    let half_zenith = (FRAC_PI_2 - theta) / 2.0;
    let radius = if half_zenith < AIR_LINEAR_LIMIT {
        (1.0 + coefficient) * half_zenith
    } else {
        let tangent = half_zenith.tan();
        (tangent * tangent).ln_1p() / tangent + coefficient * tangent
    };
    Ok((radius * phi.sin(), -radius * phi.cos()))
}

fn airy_logarithmic_factor(squared: f64) -> f64 {
    squared.ln_1p() / squared
}

/// Invert the radius in t. A small coefficient can give several
/// intersections, where the solution follows WCSLIB's branch selection.
fn deproject_air(x: f64, y: f64, coefficient: f64) -> Result<(f64, f64), CoordinateError> {
    let radius = x.hypot(y);
    if radius == 0.0 {
        return Ok((0.0, FRAC_PI_2));
    }
    let phi = x.atan2(-y);
    let linear = radius / (1.0 + coefficient);
    if linear < AIR_LINEAR_LIMIT {
        return Ok((phi, FRAC_PI_2 - 2.0 * linear));
    }
    if coefficient < LN_2 {
        return air_inverse_with_wcslib_branch(radius, coefficient)
            .map(|half_zenith| (phi, FRAC_PI_2 - 2.0 * half_zenith));
    }

    // In this range the radius is monotone, with ln(1+t²)/t² in [0, 1].
    let (mut low, mut high) = (linear, radius / coefficient);
    let mut tangent = low;
    for _ in 0..32 {
        let squared = tangent * tangent;
        let logarithmic_factor = airy_logarithmic_factor(squared);
        let error = tangent * (coefficient + logarithmic_factor) - radius;
        if error.abs() <= 4.0 * f64::EPSILON * radius {
            return Ok((phi, FRAC_PI_2 - 2.0 * tangent.atan()));
        }
        if error < 0.0 {
            low = tangent;
        } else {
            high = tangent;
        }
        let derivative = coefficient + 2.0 / (1.0 + squared) - logarithmic_factor;
        let next = tangent - error / derivative;
        tangent = if low < next && next < high {
            next
        } else {
            low.midpoint(high)
        };
    }
    Err(CoordinateError::ConversionFailed(
        "AIR deproject: no finite inverse radius".into(),
    ))
}

fn air_inverse_with_wcslib_branch(radius: f64, coefficient: f64) -> Result<f64, CoordinateError> {
    let radius_at = |cosine: f64| {
        let tangent = (1.0 - cosine * cosine).sqrt() / cosine;
        -2.0 * cosine.ln() / tangent + coefficient * tangent
    };
    // Several roots can share the first bracket. Follow WCSLIB's weighted
    // division of the cosine interval to select the same intersection.
    let (mut near_cosine, mut near_radius) = (1.0, 0.0);
    let (mut far_cosine, mut far_radius) = (1.0, 0.0);
    for _ in 0..30 {
        far_cosine = near_cosine * 0.5;
        far_radius = radius_at(far_cosine);
        if far_radius >= radius {
            break;
        }
        (near_cosine, near_radius) = (far_cosine, far_radius);
    }
    if far_radius >= radius {
        for _ in 0..100 {
            let fraction = ((far_radius - radius) / (far_radius - near_radius)).clamp(0.1, 0.9);
            let cosine = far_cosine + fraction * (near_cosine - far_cosine);
            let trial_radius = radius_at(cosine);
            // WCSLIB tests against 1e-12 in units of half the radius.
            if (trial_radius - radius).abs() < 2e-12 {
                return Ok(cosine.acos());
            }
            if trial_radius < radius {
                (near_cosine, near_radius) = (cosine, trial_radius);
            } else {
                (far_cosine, far_radius) = (cosine, trial_radius);
            }
        }
    }
    Err(CoordinateError::ConversionFailed(
        "AIR deproject: pixel outside the projected region".into(),
    ))
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
// PAR (parabolic)
// ---------------------------------------------------------------------------

/// Calabretta & Greisen (2002), Eqs. 94–95, in radians.
fn project_par(phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
    if theta.abs() == FRAC_PI_2 {
        return Ok((0.0, FRAC_PI_2.copysign(theta)));
    }
    let sine = (theta / 3.0).sin();
    Ok((phi * (1.0 - 4.0 * sine * sine), PI * sine))
}

fn deproject_par(x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
    let sine = y / PI;
    if sine.abs() <= 0.5 {
        if sine.abs() == 0.5 {
            if x.abs() < 1e-13 * DEG_TO_RAD {
                return Ok((0.0, FRAC_PI_2.copysign(y)));
            }
        } else {
            let phi = x / (1.0 - 4.0 * sine * sine);
            if phi.abs() <= PI + 1e-12 * DEG_TO_RAD {
                return Ok((phi.clamp(-PI, PI), 3.0 * sine.asin()));
            }
        }
    }
    Err(CoordinateError::ConversionFailed(
        "PAR deproject: point outside projection boundary".into(),
    ))
}

// ---------------------------------------------------------------------------
// MOL (Mollweide equal-area)
// ---------------------------------------------------------------------------

/// Calabretta & Greisen (2002), Eqs. 97–100, in radians. Solve the
/// monotone auxiliary-angle equation with bracketed Newton steps.
fn project_mol(phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
    if theta.abs() == FRAC_PI_2 {
        return Ok((0.0, SQRT_2.copysign(theta)));
    }
    let target = PI * theta.sin().abs();
    let (mut low, mut high) = (0.0, PI);
    let mut angle = target / 2.0;
    for _ in 0..64 {
        let residual = (angle - target) + angle.sin();
        if residual.abs() <= 4.0 * f64::EPSILON {
            let (sine, cosine) = (angle.copysign(theta) / 2.0).sin_cos();
            return Ok((2.0 * SQRT_2 / PI * phi * cosine, SQRT_2 * sine));
        }
        if residual < 0.0 {
            low = angle;
        } else {
            high = angle;
        }
        let next = angle - residual / (1.0 + angle.cos());
        angle = if low < next && next < high {
            next
        } else {
            low.midpoint(high)
        };
    }
    Err(CoordinateError::ConversionFailed(
        "MOL project: auxiliary angle did not converge".into(),
    ))
}

fn deproject_mol(x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
    let width_squared = 2.0 - y * y;
    // WCSLIB's pole tolerance is applied in the intermediate plane.
    if width_squared >= -1e-12 {
        let sine = (y / SQRT_2).clamp(-1.0, 1.0);
        if width_squared <= 1e-12 {
            if x.abs() < 1e-12 * DEG_TO_RAD {
                return Ok((0.0, (2.0 * sine.asin() / PI).asin()));
            }
        } else {
            let width = width_squared.sqrt();
            let phi = (PI / 2.0) * x / width;
            if phi.abs() <= PI + 1e-11 * DEG_TO_RAD {
                let theta = ((2.0 * sine.asin() + y * width) / PI)
                    .clamp(-1.0, 1.0)
                    .asin();
                return Ok((phi.clamp(-PI, PI), theta));
            }
        }
    }
    Err(CoordinateError::ConversionFailed(
        "MOL deproject: point outside projection boundary".into(),
    ))
}

// ---------------------------------------------------------------------------
// PCO (polyconic)
// ---------------------------------------------------------------------------

// WCSLIB's small-angle switch and equator/pole tolerance, in radians.
const PCO_EQUATOR_LIMIT: f64 = 1e-4 * DEG_TO_RAD;
const PCO_ANGLE_TOLERANCE: f64 = 1e-12 * DEG_TO_RAD;

/// Calabretta & Greisen (2002), §5.5.2. The half-angle expression
/// for 1 - cos(a) preserves small offsets from the central meridian.
fn project_pco(phi: f64, theta: f64) -> Result<(f64, f64), CoordinateError> {
    if theta.abs() == FRAC_PI_2 {
        return Ok((0.0, theta));
    }
    let (sine, cosine) = theta.sin_cos();
    if theta.abs() < PCO_EQUATOR_LIMIT {
        let x = phi * cosine;
        return Ok((x, theta * (1.0 + x * x / 2.0)));
    }
    let a = phi * sine;
    let cotangent = cosine / sine;
    Ok((
        cotangent * a.sin(),
        theta + 2.0 * cotangent * (a / 2.0).sin().powi(2),
    ))
}

fn deproject_pco(x: f64, y: f64) -> Result<(f64, f64), CoordinateError> {
    let height = y.abs();
    let (phi, theta) = if height < PCO_ANGLE_TOLERANCE {
        (x, 0.0)
    } else if (height - FRAC_PI_2).abs() < PCO_ANGLE_TOLERANCE {
        (0.0, FRAC_PI_2)
    } else {
        let theta = pco_inverse_latitude(x, height)?;
        let (sine, cosine) = theta.sin_cos();
        let phi = (x * sine).atan2(cosine - (height - theta) * sine) / sine;
        (phi, theta)
    };
    if phi.abs() <= PI + PCO_ANGLE_TOLERANCE {
        Ok((phi.clamp(-PI, PI), theta.copysign(y)))
    } else {
        Err(CoordinateError::ConversionFailed(
            "PCO deproject: point outside projection boundary".into(),
        ))
    }
}

/// Invert WCSLIB's circle equation after multiplying by sin(theta).
/// The residual's derivative is (x² + (height - theta)² + 2) cos(theta),
/// so the physical root is bracketed between the equator and the pole.
/// The exact central meridian has `theta = height`, avoiding WCSLIB's
/// zero divided by zero in the first weighted division when `x == 0`.
fn pco_inverse_latitude(x: f64, height: f64) -> Result<f64, CoordinateError> {
    let x_squared = x * x;
    let estimate = height / (1.0 + x_squared / 2.0);
    if height < PCO_EQUATOR_LIMIT {
        return Ok(estimate);
    }
    let (mut low, mut high) = (0.0, height.min(FRAC_PI_2));
    let mut theta = estimate.min(high);
    for _ in 0..64 {
        let (sine, cosine) = theta.sin_cos();
        let offset = height - theta;
        let radius_squared = x_squared + offset * offset;
        let residual = radius_squared * sine - 2.0 * offset * cosine;
        let slope = (radius_squared + 2.0) * cosine;
        if residual.abs() <= PCO_ANGLE_TOLERANCE * slope || high - low <= PCO_ANGLE_TOLERANCE {
            return Ok(theta);
        }
        if residual < 0.0 {
            low = theta;
        } else {
            high = theta;
        }
        let next = theta - residual / slope;
        theta = if low < next && next < high {
            next
        } else {
            low.midpoint(high)
        };
    }
    Err(CoordinateError::ConversionFailed(
        "PCO deproject: latitude did not converge".into(),
    ))
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
        assert_eq!(ProjectionType::from_name("BON"), Some(ProjectionType::BON));
        assert_eq!(ProjectionType::from_name("XPH"), None);
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
        assert!(proj.project(0.0, 0.0).is_ok());
        assert!(proj.project(0.0, -0.1).is_err());
    }

    #[test]
    fn zenithal_transforms_refuse_unsupported_parameters() {
        for (kind, parameters) in [
            (ProjectionType::AZP, vec![-1.0, 0.0]),
            (ProjectionType::AZP, vec![0.0, 90.0]),
            (ProjectionType::SZP, vec![-1.0, 0.0, 90.0]),
            (ProjectionType::SZP, vec![0.0, f64::NAN, 45.0]),
            (ProjectionType::AIR, vec![-90.0]),
            (ProjectionType::AIR, vec![300.0]),
        ] {
            let projection = Projection::with_parameters(kind, parameters);
            assert!(projection.project(0.4, 1.2).is_err(), "{kind:?}");
            assert!(projection.deproject(0.04, 0.03).is_err(), "{kind:?}");
        }
    }

    #[test]
    fn azp_matches_wcslib_for_tilted_planes() {
        // WCSLIB through Astropy Sky2Pix_ZenithalPerspective, in degrees.
        for (distance, tilt, phi, theta, x, y) in [
            (
                2.0,
                30.0,
                35.0,
                55.0,
                18.29823807694109,
                -30.175318324704378,
            ),
            (
                -2.0,
                -35.0,
                -52.0,
                50.0,
                -19.206219600866895,
                -18.318385967910768,
            ),
            (0.7, 55.0, 115.0, 40.0, 76.80775157299566, 62.44336498899339),
            (
                0.0,
                25.0,
                25.0,
                30.0,
                24.215004766569404,
                -57.29758261470182,
            ),
        ] {
            let projection = Projection::with_parameters(ProjectionType::AZP, vec![distance, tilt]);
            let (phi, theta) = (phi * DEG_TO_RAD, theta * DEG_TO_RAD);
            let expected = (x * DEG_TO_RAD, y * DEG_TO_RAD);
            let projected = projection.project(phi, theta).unwrap();
            assert!((projected.0 - expected.0).abs() < TOL);
            assert!((projected.1 - expected.1).abs() < TOL);
            let actual = projection.deproject(expected.0, expected.1).unwrap();
            assert!((actual.0 - phi).abs() < TOL);
            assert!((actual.1 - theta).abs() < TOL);
        }
    }

    #[test]
    fn azp_rejects_points_beyond_the_visible_limb() {
        let projection = Projection::with_parameters(ProjectionType::AZP, vec![2.0, 0.0]);
        assert!(projection.project(0.0, -45.0 * DEG_TO_RAD).is_err());
        assert!(projection.deproject(120.0 * DEG_TO_RAD, 0.0).is_err());
    }

    #[test]
    fn szp_matches_wcslib_for_displaced_projection_points() {
        // WCSLIB via Astropy's slant perspective transform, in degrees.
        for (parameters, native, plane) in [
            (
                [2.0, 30.0, 45.0],
                [35.0, 55.0],
                [23.656763006284216, -34.782361134931605],
            ),
            (
                [0.7, 55.0, 40.0],
                [115.0, 40.0],
                [61.0095691277204, 18.85213736129163],
            ),
            (
                [3.0, 25.0, 90.0],
                [25.0, 30.0],
                [23.96588497000227, -51.395006174305884],
            ),
            (
                [2.0, 30.0, 45.0],
                [30.0, 89.9998],
                [0.00010000010224184309, -0.00017320525784495465],
            ),
        ] {
            let projection = Projection::with_parameters(ProjectionType::SZP, parameters.to_vec());
            let [phi, theta] = native.map(|value| value * DEG_TO_RAD);
            let [x, y] = plane.map(|value| value * DEG_TO_RAD);
            let actual = projection.project(phi, theta).unwrap();
            assert!((actual.0 - x).abs() < TOL);
            assert!((actual.1 - y).abs() < TOL);
            let actual = projection.deproject(x, y).unwrap();
            assert!((actual.0 - phi).abs() < TOL);
            assert!((actual.1 - theta).abs() < TOL);
        }
        let projection = Projection::with_parameters(ProjectionType::SZP, vec![-2.0, -35.0, 70.0]);
        assert!(
            projection
                .project(-52.0 * DEG_TO_RAD, 50.0 * DEG_TO_RAD)
                .is_err()
        );
    }

    #[test]
    fn airy_default_matches_its_analytic_equatorial_radius() {
        for parameters in [vec![], vec![90.0]] {
            let projection = Projection::with_parameters(ProjectionType::AIR, parameters);
            let (x, y) = projection.project(FRAC_PI_2, 0.0).unwrap();
            // At the native equator tan(xi) is 1, with k = 1 for theta_b = 90°.
            assert!((x - (LN_2 + 1.0)).abs() < TOL);
            assert!(y.abs() < TOL);
            roundtrip(&projection, FRAC_PI_2, 0.0);
        }
    }

    #[test]
    fn airy_reference_latitudes_match_wcslib() {
        // WCSLIB via Astropy's Airy transform, with all angles in degrees.
        for (latitude, phi, theta, x, y) in [
            (45.0, 29.0, 36.0, 25.645147655021365, -46.26507106064551),
            (-82.0, -68.0, 45.0, -20.881351402380602, -8.436613597441491),
            (-82.0, 37.0, -87.0, 40.86830631740053, -54.23407426690307),
            (-45.0, -110.0, -28.0, -72.46483019384043, 26.37504122171267),
        ] {
            let projection = Projection::with_parameters(ProjectionType::AIR, vec![latitude]);
            let expected = [x * DEG_TO_RAD, y * DEG_TO_RAD];
            let actual = projection
                .project(phi * DEG_TO_RAD, theta * DEG_TO_RAD)
                .unwrap();
            assert!((actual.0 - expected[0]).abs() < TOL);
            assert!((actual.1 - expected[1]).abs() < TOL);
            let (actual_phi, actual_theta) =
                projection.deproject(expected[0], expected[1]).unwrap();
            assert!((actual_phi - phi * DEG_TO_RAD).abs() < TOL);
            assert!((actual_theta - theta * DEG_TO_RAD).abs() < TOL);
        }
    }

    #[test]
    fn airy_uses_wcslib_branch_when_the_radius_has_several_solutions() {
        for (reference, plane, expected) in [
            (-85.0, -35.91553840292542, 12.844743211493224),
            (-76.48329678302335, 54.352917860275376, -60.7625099804296),
        ] {
            let projection = Projection::with_parameters(ProjectionType::AIR, vec![reference]);
            let (_, latitude) = projection.deproject(0.0, plane * DEG_TO_RAD).unwrap();
            assert!(
                (latitude - expected * DEG_TO_RAD).abs() < 1e-9,
                "{latitude}"
            );
        }
    }

    /// WCSLIB's AIR forward transform has no finite projection at the antipode.
    #[test]
    fn airy_rejects_the_native_south_pole() {
        let projection = Projection::with_parameters(ProjectionType::AIR, vec![0.0]);
        assert!(projection.project(0.0, -FRAC_PI_2).is_err());
    }

    /// As in WCSLIB, the pseudocylindrical boundaries collapse to one
    /// representable point at either pole and have a finite equator.
    #[test]
    fn pseudocylindrical_boundaries_refuse_outside_pixels() {
        for (kind, pole, equator_edge) in [
            (ProjectionType::PAR, FRAC_PI_2, PI),
            (ProjectionType::MOL, SQRT_2, 2.0 * SQRT_2),
        ] {
            let projection = Projection::new(kind);
            for sign in [-1.0, 1.0] {
                let pole = pole * sign;
                assert_eq!(
                    projection.project(0.7, FRAC_PI_2 * sign).unwrap(),
                    (0.0, pole)
                );
                let (phi, theta) = projection.deproject(0.0, pole).unwrap();
                assert_eq!(phi, 0.0);
                assert!((theta - FRAC_PI_2 * sign).abs() < 1e-12);
                assert!(projection.deproject(0.001, pole).is_err(), "{kind:?}");
                assert!(
                    projection.deproject(0.0, pole + 0.001 * sign).is_err(),
                    "{kind:?}"
                );
            }
            assert!(projection.deproject(equator_edge, 0.0).is_ok(), "{kind:?}");
            assert!(
                projection.deproject(equator_edge + 0.01, 0.0).is_err(),
                "{kind:?}"
            );
        }
    }

    /// WCSLIB maps the equator directly and rejects longitude beyond ±π.
    /// The native inverse also gives finite worlds on the central meridian.
    #[test]
    fn polyconic_equator_and_central_meridian_are_finite() {
        let projection = Projection::new(ProjectionType::PCO);
        assert_eq!(projection.project(1.1, 0.0).unwrap(), (1.1, 0.0));
        assert_eq!(projection.deproject(PI, 0.0).unwrap(), (PI, 0.0));
        assert!(projection.deproject(PI + 1e-3, 0.0).is_err());
        let offset = 24.0 * DEG_TO_RAD / 3600.0;
        for latitude in [-offset, offset] {
            assert_eq!(
                projection.deproject(0.0, latitude).unwrap(),
                (0.0, latitude)
            );
        }
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
        assert!(proj.project(FRAC_PI_2, -0.05).is_ok());
        assert!(proj.project(FRAC_PI_2, -0.2).is_err());
        assert!(proj.project(0.0, 0.1).is_err());
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
