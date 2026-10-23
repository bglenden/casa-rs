// SPDX-License-Identifier: LGPL-3.0-or-later
//! A component's shape on the sky, CASA's `ComponentShape` family.
//!
//! CASA's shapes are a class hierarchy: `ComponentShape` holds the reference
//! direction and its errors, `TwoSidedShape` adds the axes shared by
//! `GaussianShape`, `DiskShape` and `LimbDarkenedDiskShape`, and `PointShape`
//! adds nothing. Here one [`ComponentShape`] holds the direction and its
//! errors, and a [`ShapeKind`] enum carries what each shape adds.
//!
//! # Parameters
//!
//! A component list stores a shape as its name (`Shape`), its reference
//! direction (`Reference_Direction`, `Direction_Frame`), the direction's errors
//! (`Direction_Error`, `Direction_Error_Units`), and two vectors laid out as
//! CASA's `parameters()` and `errors()` lay them out
//! ([`ComponentShape::parameters`], [`ComponentShape::errors`]):
//!
//! | shape | parameters | errors |
//! |---|---|---|
//! | point | none | none |
//! | Gaussian, disk, limb-darkened disk | major axis, minor axis, position angle (rad) | the same three (rad) |
//!
//! The axes are full widths: the full width at half maximum for a Gaussian and
//! the diameters for the disks. The position angle is the major axis's,
//! measured from north through east.
//!
//! CASA also keeps the units it was given each axis in, for display; tables do
//! not store them, and CASA reports a shape read from a table in arcmin
//! (axes), degrees (position angle) and radians (errors). casa-rs keeps
//! radians only.

use std::f64::consts::PI;

use casa_types::measures::direction::MDirection;
use casa_types::quanta::{Quantity, Unit};

use crate::component_type::ShapeType;
use crate::error::ComponentError;

/// The errors of a reference direction, as two angles.
///
/// CASA keeps each as a quantity in the unit it was given and a component
/// list stores the unit too: `Direction_Error` holds the values as
/// `[latitude, longitude]` (the reverse of `Reference_Direction`'s order) and
/// `Direction_Error_Units` their units.
#[derive(Debug, Clone, PartialEq)]
pub struct DirectionError {
    latitude: Quantity,
    longitude: Quantity,
}

impl DirectionError {
    /// Errors of zero radians, CASA's default.
    pub fn zero() -> Self {
        Self {
            latitude: radians(0.0),
            longitude: radians(0.0),
        }
    }

    /// Errors in latitude and longitude
    /// (`ComponentShape::setRefDirectionError`).
    ///
    /// # Errors
    ///
    /// [`ComponentError::BadError`] when either is negative or not an angle,
    /// as CASA refuses them.
    pub fn new(latitude: Quantity, longitude: Quantity) -> Result<Self, ComponentError> {
        check_angle_error("direction latitude error", &latitude)?;
        check_angle_error("direction longitude error", &longitude)?;
        Ok(Self {
            latitude,
            longitude,
        })
    }

    /// The latitude error (`refDirectionErrorLat`).
    pub fn latitude(&self) -> &Quantity {
        &self.latitude
    }

    /// The longitude error (`refDirectionErrorLong`).
    pub fn longitude(&self) -> &Quantity {
        &self.longitude
    }
}

/// The axes of a Gaussian, disk or limb-darkened disk, in radians.
///
/// CASA requires both axes to be positive and the major axis to be at least
/// the minor axis (`GaussianShape`, `DiskShape` and `LimbDarkenedDiskShape`'s
/// `setWidthInRad`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Ellipse {
    major_axis: f64,
    minor_axis: f64,
    position_angle: f64,
}

impl Ellipse {
    /// Axes from full widths and a position angle, all in radians.
    ///
    /// # Errors
    ///
    /// [`ComponentError::Axes`] when an axis is not positive or the minor
    /// axis is longer than the major axis.
    pub fn new(
        major_axis: f64,
        minor_axis: f64,
        position_angle: f64,
    ) -> Result<Self, ComponentError> {
        let invalid = |reason: String| ComponentError::Axes {
            shape: "elliptical",
            reason,
        };
        // Written so that NaN axes fail, as casacore's `width <= 0` and
        // `AlwaysAssert(major > 0 && minor > 0 && major >= minor)` fail them.
        let positive = major_axis > 0.0 && minor_axis > 0.0;
        if !positive {
            return Err(invalid(format!(
                "the axes must be positive, got {major_axis} and {minor_axis} rad"
            )));
        }
        let ordered = major_axis >= minor_axis;
        if !ordered {
            return Err(invalid(format!(
                "the major axis {major_axis} rad is shorter than the minor axis {minor_axis} rad"
            )));
        }
        Ok(Self {
            major_axis,
            minor_axis,
            position_angle,
        })
    }

    /// The major axis's full width, rad.
    pub fn major_axis(&self) -> f64 {
        self.major_axis
    }

    /// The minor axis's full width, rad.
    pub fn minor_axis(&self) -> f64 {
        self.minor_axis
    }

    /// The major axis's position angle, rad, north through east.
    pub fn position_angle(&self) -> f64 {
        self.position_angle
    }

    /// The minor-to-major axial ratio (`TwoSidedShape::axialRatio`).
    pub fn axial_ratio(&self) -> f64 {
        self.minor_axis / self.major_axis
    }
}

/// The errors of an [`Ellipse`]'s axes and position angle, in radians.
///
/// CASA's `TwoSidedShape::setErrors(Vector)`, which `readTable` uses, does not
/// check them in a release build, so neither does casa-rs.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct EllipseErrors {
    /// The major axis's error, rad.
    pub major_axis: f64,
    /// The minor axis's error, rad.
    pub minor_axis: f64,
    /// The position angle's error, rad.
    pub position_angle: f64,
}

/// What each shape adds to the reference direction.
#[derive(Debug, Clone, PartialEq)]
pub enum ShapeKind {
    /// A point source.
    Point,
    /// An elliptical Gaussian whose axes are full widths at half maximum.
    ///
    /// CASA normalises its position angle into [0, π) (casacore's
    /// `Gaussian2DParam::PA`); [`ComponentShape::new`] does the same.
    Gaussian {
        /// The axes.
        axes: Ellipse,
        /// Their errors.
        errors: EllipseErrors,
    },
    /// A uniform elliptical disk whose axes are diameters.
    Disk {
        /// The axes.
        axes: Ellipse,
        /// Their errors.
        errors: EllipseErrors,
    },
    /// A limb-darkened elliptical disk whose axes are diameters.
    ///
    /// Its brightness falls as I₀(1 − ρ²)^(n/2). CASA keeps the exponent n
    /// with the component, not the shape: it is the first of the component's
    /// optional parameters, stored in a component list's
    /// `Optional_Parameters` column (see
    /// [`SkyComponent::limb_darkening_exponent`](crate::SkyComponent::limb_darkening_exponent)).
    LimbDarkenedDisk {
        /// The axes.
        axes: Ellipse,
        /// Their errors.
        errors: EllipseErrors,
    },
}

impl ShapeKind {
    /// The shape's type.
    pub fn shape_type(&self) -> ShapeType {
        match self {
            Self::Point => ShapeType::Point,
            Self::Gaussian { .. } => ShapeType::Gaussian,
            Self::Disk { .. } => ShapeType::Disk,
            Self::LimbDarkenedDisk { .. } => ShapeType::LimbDarkenedDisk,
        }
    }

    /// The axes and their errors, for the shapes that have them.
    pub fn axes(&self) -> Option<(&Ellipse, &EllipseErrors)> {
        match self {
            Self::Point => None,
            Self::Gaussian { axes, errors }
            | Self::Disk { axes, errors }
            | Self::LimbDarkenedDisk { axes, errors } => Some((axes, errors)),
        }
    }

    /// A shape of `shape_type` with the given axes (ignored for a point).
    fn with_axes(shape_type: ShapeType, axes: Ellipse, errors: EllipseErrors) -> Self {
        match shape_type {
            ShapeType::Point => Self::Point,
            ShapeType::Gaussian => Self::Gaussian { axes, errors },
            ShapeType::Disk => Self::Disk { axes, errors },
            ShapeType::LimbDarkenedDisk => Self::LimbDarkenedDisk { axes, errors },
        }
    }

    /// Apply CASA's setters' normalisation: a Gaussian's position angle into
    /// [0, π), and a disk's minor axis onto its major axis when they differ by
    /// less than 1e-17 rad (`DiskShape::setWidthInRad`).
    fn normalised(self) -> Result<Self, ComponentError> {
        Ok(match self {
            Self::Gaussian { axes, errors } => Self::Gaussian {
                axes: Ellipse {
                    position_angle: gaussian_position_angle(axes.position_angle)?,
                    ..axes
                },
                errors,
            },
            Self::Disk { axes, errors } => {
                let minor_axis = if (axes.major_axis - axes.minor_axis).abs() < 1.0e-17 {
                    axes.major_axis
                } else {
                    axes.minor_axis
                };
                Self::Disk {
                    axes: Ellipse { minor_axis, ..axes },
                    errors,
                }
            }
            other => other,
        })
    }
}

/// A component's shape: its reference direction, that direction's errors,
/// and what the shape adds ([`ShapeKind`]).
///
/// This is CASA's `ComponentShape` with its subclasses folded into
/// [`ShapeKind`]. The reference direction is a casa-types [`MDirection`]
/// carrying its frame; a component list stores it in that frame
/// (`Direction_Frame`), unconverted.
#[derive(Debug, Clone)]
pub struct ComponentShape {
    reference_direction: MDirection,
    direction_error: DirectionError,
    kind: ShapeKind,
}

impl ComponentShape {
    /// A shape at `reference_direction`, with zero direction errors.
    ///
    /// A Gaussian's position angle is normalised into [0, π), and a disk
    /// whose axes differ by less than 1e-17 rad becomes circular, as CASA's
    /// setters do.
    ///
    /// # Errors
    ///
    /// [`ComponentError::Axes`] when a Gaussian's position angle lies outside
    /// ±2π, which casacore's `Gaussian2DParam::setPA` refuses.
    pub fn new(reference_direction: MDirection, kind: ShapeKind) -> Result<Self, ComponentError> {
        Ok(Self {
            reference_direction,
            direction_error: DirectionError::zero(),
            kind: kind.normalised()?,
        })
    }

    /// A point source at `reference_direction`.
    pub fn point(reference_direction: MDirection) -> Self {
        Self {
            reference_direction,
            direction_error: DirectionError::zero(),
            kind: ShapeKind::Point,
        }
    }

    /// This shape with its direction errors replaced.
    pub fn with_direction_error(mut self, direction_error: DirectionError) -> Self {
        self.direction_error = direction_error;
        self
    }

    /// The reference direction, in its own frame (`refDirection`).
    pub fn reference_direction(&self) -> &MDirection {
        &self.reference_direction
    }

    /// Replace the reference direction (`setRefDirection`).
    pub fn set_reference_direction(&mut self, reference_direction: MDirection) {
        self.reference_direction = reference_direction;
    }

    /// The reference direction's errors.
    pub fn direction_error(&self) -> &DirectionError {
        &self.direction_error
    }

    /// Replace the reference direction's errors (`setRefDirectionError`).
    pub fn set_direction_error(&mut self, direction_error: DirectionError) {
        self.direction_error = direction_error;
    }

    /// What the shape adds to the direction.
    pub fn kind(&self) -> &ShapeKind {
        &self.kind
    }

    /// The shape's type.
    pub fn shape_type(&self) -> ShapeType {
        self.kind.shape_type()
    }

    /// The shape's parameters, laid out as CASA's `parameters()`: none for a
    /// point, and the major axis, minor axis and position angle (rad) for the
    /// others.
    pub fn parameters(&self) -> Vec<f64> {
        self.kind.axes().map_or_else(Vec::new, |(axes, _)| {
            vec![axes.major_axis, axes.minor_axis, axes.position_angle]
        })
    }

    /// The parameters' errors, laid out as CASA's `errors()`.
    pub fn errors(&self) -> Vec<f64> {
        self.kind.axes().map_or_else(Vec::new, |(_, errors)| {
            vec![errors.major_axis, errors.minor_axis, errors.position_angle]
        })
    }

    /// A shape of `shape_type` from parameter and error vectors laid out as
    /// [`Self::parameters`] and [`Self::errors`], as `readTable` builds it:
    /// `setParameters`, then `setErrors` when the table has errors.
    ///
    /// A point ignores both vectors, as CASA's `PointShape` does. The other
    /// shapes read the first three values of each and ignore any more.
    ///
    /// # Errors
    ///
    /// [`ComponentError::ParameterCount`] when a vector is too short (CASA
    /// would read past its end), and [`ComponentError::Axes`] for axes CASA
    /// refuses.
    pub fn from_parameters(
        shape_type: ShapeType,
        reference_direction: MDirection,
        parameters: &[f64],
        errors: Option<&[f64]>,
    ) -> Result<Self, ComponentError> {
        if shape_type == ShapeType::Point {
            return Ok(Self::point(reference_direction));
        }
        let what = match shape_type {
            ShapeType::Gaussian => "Gaussian",
            ShapeType::Disk => "disk",
            _ => "limb-darkened disk",
        };
        let [major_axis, minor_axis, position_angle] = first_three(what, parameters)?;
        let axes =
            Ellipse::new(major_axis, minor_axis, position_angle).map_err(|error| match error {
                ComponentError::Axes { reason, .. } => ComponentError::Axes {
                    shape: what,
                    reason,
                },
                other => other,
            })?;
        let errors = match errors {
            Some(errors) => {
                let [major_axis, minor_axis, position_angle] = first_three(what, errors)?;
                EllipseErrors {
                    major_axis,
                    minor_axis,
                    position_angle,
                }
            }
            None => EllipseErrors::default(),
        };
        Self::new(
            reference_direction,
            ShapeKind::with_axes(shape_type, axes, errors),
        )
    }
}

impl PartialEq for ComponentShape {
    fn eq(&self, other: &Self) -> bool {
        same_direction(&self.reference_direction, &other.reference_direction)
            && self.direction_error == other.direction_error
            && self.kind == other.kind
    }
}

/// Two directions with the same frame and direction cosines.
pub(crate) fn same_direction(left: &MDirection, right: &MDirection) -> bool {
    left.refer() == right.refer() && left.cosines() == right.cosines()
}

/// casacore's `Gaussian2DParam`: refuse |pa| > 2π, and report the angle in
/// [0, π) (`fmod(pa, π)`, plus π when negative).
fn gaussian_position_angle(position_angle: f64) -> Result<f64, ComponentError> {
    if position_angle.abs() > 2.0 * PI {
        return Err(ComponentError::Axes {
            shape: "Gaussian",
            reason: format!(
                "the position angle {position_angle} rad lies outside ±2π (casacore's Gaussian2D refuses it)"
            ),
        });
    }
    let reduced = position_angle % PI;
    Ok(if reduced < 0.0 { reduced + PI } else { reduced })
}

fn first_three(what: &'static str, values: &[f64]) -> Result<[f64; 3], ComponentError> {
    match values {
        [a, b, c, ..] => Ok([*a, *b, *c]),
        _ => Err(ComponentError::ParameterCount {
            what,
            expected: 3,
            actual: values.len(),
        }),
    }
}

fn radians(value: f64) -> Quantity {
    Quantity::with_unit(value, Unit::new("rad").expect("rad is a unit"))
}

/// `ComponentShape::badError`: an error must be an angle and not negative.
fn check_angle_error(what: &'static str, error: &Quantity) -> Result<(), ComponentError> {
    let angle = Unit::new("rad").expect("rad is a unit");
    if !error.unit().conformant(&angle) || error.value() < 0.0 {
        return Err(ComponentError::BadError {
            what,
            kind: "angle",
            value: error.value(),
            unit: error.unit().name().to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use casa_types::measures::direction::DirectionRef;

    use super::*;

    fn north() -> MDirection {
        MDirection::from_angles(0.0, 0.5, DirectionRef::J2000)
    }

    #[test]
    fn gaussian_position_angles_are_reduced_into_zero_to_pi() {
        let axes = Ellipse::new(2.0e-4, 1.0e-4, 200.0_f64.to_radians()).expect("axes");
        let shape = ComponentShape::new(
            north(),
            ShapeKind::Gaussian {
                axes,
                errors: EllipseErrors::default(),
            },
        )
        .expect("gaussian");
        let pa = shape.parameters()[2];
        assert!((pa - 20.0_f64.to_radians()).abs() < 1e-15, "{pa}");
        let negative = ComponentShape::new(
            north(),
            ShapeKind::Gaussian {
                axes: Ellipse::new(2.0e-4, 1.0e-4, -30.0_f64.to_radians()).expect("axes"),
                errors: EllipseErrors::default(),
            },
        )
        .expect("gaussian");
        assert!((negative.parameters()[2] - 150.0_f64.to_radians()).abs() < 1e-15);
        assert!(
            ComponentShape::new(
                north(),
                ShapeKind::Gaussian {
                    axes: Ellipse::new(2.0e-4, 1.0e-4, 7.0).expect("axes"),
                    errors: EllipseErrors::default(),
                },
            )
            .is_err()
        );
    }

    #[test]
    fn disks_keep_their_position_angle_and_snap_nearly_circular_axes() {
        let shape = ComponentShape::new(
            north(),
            ShapeKind::Disk {
                axes: Ellipse::new(1.0e-4, 1.0e-4 - 5.0e-18, -0.5).expect("axes"),
                errors: EllipseErrors::default(),
            },
        )
        .expect("disk");
        assert_eq!(shape.parameters(), vec![1.0e-4, 1.0e-4, -0.5]);
    }

    #[test]
    fn axes_must_be_positive_and_ordered() {
        assert!(Ellipse::new(0.0, 0.0, 0.0).is_err());
        assert!(Ellipse::new(1.0, 2.0, 0.0).is_err());
        assert!(Ellipse::new(f64::NAN, 1.0, 0.0).is_err());
        assert!(Ellipse::new(1.0, 1.0, 0.0).is_ok());
    }

    #[test]
    fn parameter_vectors_follow_casa_layout() {
        let shape = ComponentShape::from_parameters(
            ShapeType::LimbDarkenedDisk,
            north(),
            &[3.0e-4, 2.0e-4, 0.25],
            Some(&[1.0e-6, 2.0e-6, 0.01]),
        )
        .expect("ldisk");
        assert_eq!(shape.parameters(), vec![3.0e-4, 2.0e-4, 0.25]);
        assert_eq!(shape.errors(), vec![1.0e-6, 2.0e-6, 0.01]);
        let point =
            ComponentShape::from_parameters(ShapeType::Point, north(), &[], None).expect("point");
        assert!(point.parameters().is_empty() && point.errors().is_empty());
        assert!(matches!(
            ComponentShape::from_parameters(ShapeType::Disk, north(), &[1.0], None),
            Err(ComponentError::ParameterCount {
                expected: 3,
                actual: 1,
                ..
            })
        ));
    }

    #[test]
    fn direction_errors_must_be_non_negative_angles() {
        let arcsec = Quantity::new(0.5, "arcsec").expect("arcsec");
        assert!(DirectionError::new(arcsec.clone(), arcsec.clone()).is_ok());
        assert!(
            DirectionError::new(Quantity::new(1.0, "Hz").expect("Hz"), arcsec.clone()).is_err()
        );
        assert!(DirectionError::new(arcsec, Quantity::new(-1.0, "rad").expect("rad")).is_err());
    }
}
