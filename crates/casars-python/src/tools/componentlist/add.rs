// SPDX-License-Identifier: LGPL-3.0-or-later
//! `cl.addcomponent`: a component built as CASA's binding builds it, from
//! its own setters on a new component (`simulate(1)`, then `setlabel`,
//! `setflux`, the direction, `setshape`, `setspectrum`, `setfreq` and
//! `setfreqframe`, in that order).
//!
//! What a setter raises is raised (and the component is not added); what
//! `setshape` refuses it logs, leaving the component a point, as CASA's
//! does.
//!
//! Divergences, recorded in `docs/CASA (C++) bugs.md`:
//! - **The flux keeps its polarisation.** CASA's `setflux` stores four values
//!   through `FluxRep::setValue(Vector<Double>)`, which resets the
//!   representation to Stokes, so `polarization='circular'` with
//!   `[RR, RL, LR, LL]` is stored as I, Q, U, V. casa-rs stores the values in
//!   the representation given.
//! - **The flux keeps double precision.** CASA's `setflux` reads the values
//!   into a single-precision `Vector<Complex>` (0.1 is stored as
//!   0.10000000149011612); casa-rs keeps the values given.

use std::sync::Arc;

use casa_components::{
    ComponentShape, Ellipse, EllipseErrors, Flux, Polarisation, PowerLogPolynomial, ShapeKind,
    ShapeType, SkyComponent, SpectralIndex, SpectralKind, SpectralModel,
};
use casa_types::measures::MeasuresProvider;
use casa_types::measures::direction::MDirection;
use casa_types::quanta::{Dimension, UnitDim};
use num_complex::Complex64;

use crate::convert::{ConvertError, ConvertResult, Variant, measures, quantity};
use crate::tools::LogLine;

/// The direction `addcomponent` takes for `dir=[]`.
const DEFAULT_DIRECTION: &str = "J2000 00h00m00.0 90d00m00";

/// The frequency `addcomponent` takes for `freq=[]`.
const DEFAULT_FREQUENCY: &str = "LSRK 1.420GHz";

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
        let shape = match self.shape(direction.clone()) {
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

    /// `setshape`'s shape at `direction`, or the message it logs when it
    /// leaves the shape unchanged.
    fn shape(&self, direction: MDirection) -> Result<ComponentShape, String> {
        let unchanged = "Shape not changed.";
        let shape = ShapeType::parse_casa(&self.shape).map_err(|_| {
            format!(
                "Could not translate the shape type to a known value. Known types are: Point, \
                 Gaussian, Disk. {unchanged}"
            )
        })?;
        let axes = || {
            let angle = |value: &Variant, name: &str| {
                let quantity = quantity(value).map_err(|error| error.to_string())?;
                if quantity.unit().val().dim == UnitDim::basis(Dimension::Angle) {
                    Ok(quantity.get_si_value())
                } else {
                    Err(format!("The {name} field does not have angular units"))
                }
            };
            let axes = Ellipse::new(
                angle(&self.majoraxis, "majoraxis")?,
                angle(&self.minoraxis, "minoraxis")?,
                angle(&self.positionangle, "positionangle")?,
            )
            .map_err(|error| error.to_string())?;
            Ok::<_, String>(axes)
        };
        let parsed = |reason: String| {
            format!("Could not parse the shape parameters. The error was: {reason} {unchanged}")
        };
        let kind = match shape {
            ShapeType::Point => return Ok(ComponentShape::point(direction)),
            ShapeType::Gaussian => ShapeKind::Gaussian {
                axes: axes().map_err(parsed)?,
                errors: EllipseErrors::default(),
            },
            ShapeType::Disk => ShapeKind::Disk {
                axes: axes().map_err(parsed)?,
                errors: EllipseErrors::default(),
            },
            ShapeType::LimbDarkenedDisk => {
                return Err(format!(
                    "Limbdarkeneddisk is not implemented yet. {unchanged}"
                ));
            }
        };
        ComponentShape::new(direction, kind).map_err(|error| parsed(error.to_string()))
    }
}

/// `setflux`'s flux: one value (Stokes I) or four, in `unit` (a flux
/// density) and `polarization`'s representation, checked in CASA's order
/// (the representation, the unit, then the values) with its messages.
fn flux(value: &Variant, unit: &str, polarization: &str) -> ConvertResult<Flux> {
    let polarisation = Polarisation::parse_casa(polarization).map_err(|_| {
        ConvertError::runtime(
            "Unknown polarization. Possible values are:\n 'Stokes'  'Linear'  'Circular' ",
        )
    })?;
    let unchanged = "Flux not changed on any components";
    let mut flux = Flux::new([Complex64::new(0.0, 0.0); 4], unit, polarisation).map_err(|_| {
        ConvertError::runtime(format!(
            "The flux units must have the same dimensions as the Jansky\n{unchanged}"
        ))
    })?;
    let values: Vec<f64> = match value {
        Variant::Empty => vec![0.0; 4],
        Variant::Number(number) => vec![*number],
        Variant::Numbers(numbers) => numbers.clone(),
        _ => {
            return Err(ConvertError::runtime(
                "Could not understand the type of flux value variable",
            ));
        }
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
