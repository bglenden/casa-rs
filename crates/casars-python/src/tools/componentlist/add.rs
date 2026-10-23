// SPDX-License-Identifier: LGPL-3.0-or-later
//! `cl.addcomponent`: a component built as CASA's binding builds it, from
//! its own setters on a new component (`simulate(1)`, then `setlabel`,
//! `setflux`, the direction, `setshape`, `setspectrum`, `setfreq` and
//! `setfreqframe`, in that order).
//!
//! What a setter raises is raised and the component is not added, as CASA's
//! `addcomponent` removes the component it began. `setshape` raises what
//! reading its axes or constructing the shape raises (an axis that does not
//! read, axes that are not positive or not ordered, a Gaussian's position
//! angle outside ±2π). It refuses, logging and leaving the component a
//! point, a shape name it does not know, a limb-darkened disk and an axis
//! that is not an angle.
//!
//! As CASA's does, `setflux` stores the flux's values as Stokes whatever
//! `polarization` says (a CASA bug casa-rs follows, recorded in
//! `docs/CASA (C++) bugs.md`), and takes the real parts of complex values.
//!
//! Divergence, recorded in `docs/CASA (C++) bugs.md`: **the flux keeps
//! double precision.** CASA's `setflux` reads the values into a
//! single-precision `Vector<Complex>` (0.1 is stored as
//! 0.10000000149011612); casa-rs keeps the values given.

use std::f64::consts::PI;
use std::sync::Arc;

use casa_components::{
    ComponentShape, Ellipse, EllipseErrors, Flux, Polarisation, PowerLogPolynomial, ShapeKind,
    ShapeType, SkyComponent, SpectralIndex, SpectralKind, SpectralModel,
};
use casa_types::measures::MeasuresProvider;
use casa_types::measures::direction::MDirection;
use casa_types::quanta::{Dimension, UnitDim};
use num_complex::Complex64;

use crate::convert::{ConvertError, ConvertResult, Variant, complex_values, measures, quantity};
use crate::tools::LogLine;

/// The direction `addcomponent` takes for `dir=[]`.
const DEFAULT_DIRECTION: &str = "J2000 00h00m00.0 90d00m00";

/// The frequency `addcomponent` takes for `freq=[]`.
const DEFAULT_FREQUENCY: &str = "LSRK 1.420GHz";

/// The axes of `GaussianShape`'s default Gaussian, one arcminute, rad.
const DEFAULT_GAUSSIAN_AXIS: f64 = PI / 10_800.0;

/// `addcomponent`'s arguments.
#[derive(Debug, Clone)]
pub(crate) struct AddComponent {
    pub(crate) flux: Variant,
    pub(crate) fluxunit: String,
    pub(crate) polarization: String,
    pub(crate) dir: Variant,
    pub(crate) shape: String,
    pub(crate) majoraxis: Variant,
    pub(crate) minoraxis: Variant,
    pub(crate) positionangle: Variant,
    pub(crate) freq: Variant,
    pub(crate) spectrumtype: String,
    pub(crate) index: Variant,
    pub(crate) label: String,
}

impl AddComponent {
    /// The component, as CASA's setters leave it; `lines` gets what
    /// `setshape` and the frequency reader log.
    pub(crate) fn component(
        &self,
        measures: &Arc<dyn MeasuresProvider>,
        lines: &mut Vec<LogLine>,
    ) -> ConvertResult<SkyComponent> {
        let flux = flux(&self.flux, &self.fluxunit, &self.polarization)?;
        let dir = match &self.dir {
            Variant::Empty => Variant::Text(DEFAULT_DIRECTION.to_string()),
            other => other.clone(),
        };
        let direction = measures::direction(&dir, measures)?
            .ok_or_else(|| ConvertError::runtime("Could not interpret direction parameter"))?;
        let shape = match self.shape(direction.clone())? {
            Ok(shape) => shape,
            Err(message) => {
                lines.push(LogLine::error(message));
                ComponentShape::point(direction)
            }
        };
        let kind = spectral_kind(&self.spectrumtype, &self.index)?;
        let freq = match &self.freq {
            Variant::Empty => Variant::Text(DEFAULT_FREQUENCY.to_string()),
            other => other.clone(),
        };
        let (frequency, warning) = measures::frequency(&freq)?;
        lines.extend(warning.map(LogLine::warning));
        let frequency = frequency
            .ok_or_else(|| ConvertError::runtime("Could not interpret frequency parameter"))?;
        let spectrum = SpectralModel::new(frequency, kind);
        Ok(SkyComponent::new(flux, shape, spectrum).with_label(self.label.clone()))
    }

    /// `setshape`'s shape at `direction`: `Ok(Err(message))` when it refuses
    /// the shape, logging `message` and leaving the shape unchanged, and an
    /// error for what it raises.
    fn shape(&self, direction: MDirection) -> ConvertResult<Result<ComponentShape, String>> {
        let unchanged = "Shape not changed.";
        let Ok(shape) = ShapeType::parse_casa(&self.shape) else {
            return Ok(Err(format!(
                "Could not translate the shape type to a known value. Known types are: Point, \
                 Gaussian, Disk. {unchanged}"
            )));
        };
        match shape {
            ShapeType::Point => return Ok(Ok(ComponentShape::point(direction))),
            ShapeType::LimbDarkenedDisk => {
                return Ok(Err(format!(
                    "Limbdarkeneddisk is not implemented yet. {unchanged}"
                )));
            }
            ShapeType::Gaussian | ShapeType::Disk => {}
        }
        // `casaQuantity` reads all three, raising for one it cannot read,
        // before the shape's `fromRecord` refuses one that is not an angle.
        let quantities = [
            ("majoraxis", quantity(&self.majoraxis)?),
            ("minoraxis", quantity(&self.minoraxis)?),
            ("positionangle", quantity(&self.positionangle)?),
        ];
        let mut radians = [0.0; 3];
        for ((name, quantity), radian) in quantities.iter().zip(&mut radians) {
            if quantity.unit().val().dim != UnitDim::basis(Dimension::Angle) {
                return Ok(Err(format!(
                    "Could not parse the shape parameters. The error was: The {name} field does \
                     not have angular units {unchanged}"
                )));
            }
            *radian = quantity.get_si_value();
        }
        let [major, minor, position_angle] = radians;
        if let Some(message) = constructor_error(shape, major, minor, position_angle) {
            return Err(ConvertError::runtime(message));
        }
        let raised =
            |error: casa_components::ComponentError| ConvertError::runtime(error.to_string());
        let axes = Ellipse::new(major, minor, position_angle).map_err(raised)?;
        let errors = EllipseErrors::default();
        let kind = if shape == ShapeType::Gaussian {
            ShapeKind::Gaussian { axes, errors }
        } else {
            ShapeKind::Disk { axes, errors }
        };
        ComponentShape::new(direction, kind).map(Ok).map_err(raised)
    }
}

/// What casacore raises making a Gaussian or a disk with these axes and
/// position angle (rad), or `None` when it makes it.
///
/// `GaussianShape` sets the axes through `Gaussian2DParam::setWidth` on its
/// default one-arcminute circle, which sets the major axis first only when
/// it is longer than an arcminute, so that order decides which axis is
/// reported; then `setPA` refuses an angle outside ±2π. `DiskShape` asserts
/// its axes, taking two within 1e-17 rad as equal, and takes any angle.
fn constructor_error(
    shape: ShapeType,
    major: f64,
    minor: f64,
    position_angle: f64,
) -> Option<&'static str> {
    if shape == ShapeType::Disk {
        let minor = if (major - minor).abs() < 1e-17 {
            major
        } else {
            minor
        };
        let asserted = major > 0.0 && minor > 0.0 && major >= minor;
        return (!asserted).then_some(
            "Failed AlwaysAssert _majorAxis > 0 && _minorAxis > 0 && _majorAxis >=_minorAxis",
        );
    }
    let set_major = |width: f64, minor_axis: f64| {
        if width <= 0.0 {
            Some("Gaussian2DParam<T>::setMajorAxis(const T &width) - width must be positive")
        } else if width < minor_axis {
            Some(
                "Gaussian2DParam<T>::setMajorAxis(const T &width) - major axis is smaller than \
                 minor axis",
            )
        } else {
            None
        }
    };
    let set_minor = |width: f64, major_axis: f64| {
        if width <= 0.0 {
            Some("Gaussian2DParam<T>::setMinorAxis(const T &width) - width must be positive")
        } else if width > major_axis {
            Some(
                "Gaussian2DParam<T>::setMinorAxis(const T &width) - minor axis is greater than \
                 major axis",
            )
        } else {
            None
        }
    };
    let widths = if major.abs() > DEFAULT_GAUSSIAN_AXIS {
        set_major(major, DEFAULT_GAUSSIAN_AXIS).or_else(|| set_minor(minor, major))
    } else {
        set_minor(minor, DEFAULT_GAUSSIAN_AXIS).or_else(|| set_major(major, minor))
    };
    widths.or_else(|| {
        (position_angle.abs() > 2.0 * PI).then_some(
            "Gaussian2DParam<T>::setPA(const T &pa) - PA must be in radians and between -2pi \
             and 2pi",
        )
    })
}

/// `setflux`'s flux: one value (Stokes I) or four, in `unit` (a flux
/// density), checked in CASA's order (the representation, the unit, then
/// the values) with its messages.
///
/// `polarization` must name a representation, and one value needs Stokes,
/// but the values are stored as Stokes whatever it names, as CASA's
/// `FluxRep::setValue` stores them. Complex values (a `complex`, or a
/// sequence with one) give their real parts, as CASA's setter takes them.
fn flux(value: &Variant, unit: &str, polarization: &str) -> ConvertResult<Flux> {
    let polarisation = Polarisation::parse_casa(polarization).map_err(|_| {
        ConvertError::runtime(
            "Unknown polarization. Possible values are:\n 'Stokes'  'Linear'  'Circular' ",
        )
    })?;
    let unchanged = "Flux not changed on any components";
    let mut flux =
        Flux::new([Complex64::new(0.0, 0.0); 4], unit, Polarisation::Stokes).map_err(|_| {
            ConvertError::runtime(format!(
                "The flux units must have the same dimensions as the Jansky\n{unchanged}"
            ))
        })?;
    let values: Vec<f64> = match value {
        Variant::Empty => vec![0.0; 4],
        other => complex_values(other)
            .ok_or_else(|| {
                ConvertError::runtime("Could not understand the type of flux value variable")
            })?
            .iter()
            .map(|value| value.re)
            .collect(),
    };
    let values = match (values.as_slice(), polarisation) {
        ([i], Polarisation::Stokes) => [*i, 0.0, 0.0, 0.0],
        ([a, b, c, d], _) => [*a, *b, *c, *d],
        _ => {
            return Err(ConvertError::runtime(format!(
                "The flux must have one or four elements,\none element can only be used if the \
                 polarization is 'Stokes'.\n{unchanged}"
            )));
        }
    };
    flux.set_values(values.map(|value| Complex64::new(value, 0.0)));
    Ok(flux)
}

/// `setspectrum`'s model for `spectrumtype` (its first letter, upper-cased)
/// and `index`, before `setfreq` gives it its reference frequency.
fn spectral_kind(spectrumtype: &str, index: &Variant) -> ConvertResult<SpectralKind> {
    match spectrumtype.to_ascii_uppercase().chars().next() {
        // `addcomponent` passes `setspectrum`'s one-point default tabulation.
        Some('T') => Err(ConvertError::runtime(
            "There need to be at least 2 points in a tabular spectrum",
        )),
        Some('S') => {
            let index = match index {
                Variant::Number(index) => *index,
                Variant::Numbers(indices) => indices[0],
                _ => {
                    return Err(ConvertError::runtime(
                        "Unrecognized type for index, must be double or doubleArray",
                    ));
                }
            };
            Ok(SpectralKind::SpectralIndex(SpectralIndex::new(index)))
        }
        Some('P') => match index {
            Variant::Numbers(coefficients) => Ok(SpectralKind::PowerLogPolynomial(
                PowerLogPolynomial::new(coefficients.clone()),
            )),
            _ => Err(ConvertError::runtime(
                "for a spectrum='plp', index must be a list of numbers",
            )),
        },
        Some('C') => Ok(SpectralKind::Constant),
        _ => Err(ConvertError::runtime(format!(
            "Unkinown spectral type {spectrumtype}"
        ))),
    }
}
