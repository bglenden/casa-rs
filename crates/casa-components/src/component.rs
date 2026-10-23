// SPDX-License-Identifier: LGPL-3.0-or-later
//! One sky component, CASA's `SkyComponent`.

use crate::component_type::ShapeType;
use crate::flux::Flux;
use crate::shape::ComponentShape;
use crate::spectrum::SpectralModel;

/// The limb-darkening exponent CASA's `LimbDarkenedDiskShape` starts with.
pub const DEFAULT_LIMB_DARKENING_EXPONENT: f64 = 0.05;

/// One component of a sky model: a flux, a shape, a spectral model, a label
/// and optional parameters.
///
/// This is CASA's `SkyComponent` (`SkyCompRep`). The flux is the integrated
/// flux at the spectral model's reference frequency; the shape places it on
/// the sky and spreads it; the spectrum scales it with frequency.
///
/// # Optional parameters
///
/// CASA keeps a vector of optional parameters with the component, used today
/// only by the limb-darkened disk, whose first optional parameter is its
/// limb-darkening exponent n. A component list stores the vector in its
/// `Optional_Parameters` column, which CASA adds only to lists holding a
/// limb-darkened disk and fills only for those rows. CASA also copies the
/// exponent into the shape when it reads a table; casa-rs keeps the one copy
/// the table persists, here, and derives the exponent from it
/// ([`Self::limb_darkening_exponent`]).
#[derive(Debug, Clone, PartialEq)]
pub struct SkyComponent {
    flux: Flux,
    shape: ComponentShape,
    spectrum: SpectralModel,
    label: String,
    optional_parameters: Vec<f64>,
}

impl SkyComponent {
    /// A component with an empty label and no optional parameters
    /// (`SkyComponent(flux, shape, spectrum)`).
    pub fn new(flux: Flux, shape: ComponentShape, spectrum: SpectralModel) -> Self {
        Self {
            flux,
            shape,
            spectrum,
            label: String::new(),
            optional_parameters: Vec::new(),
        }
    }

    /// This component with its label replaced.
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self
    }

    /// This component with its optional parameters replaced
    /// (`optionalParameters()`).
    pub fn with_optional_parameters(mut self, optional_parameters: Vec<f64>) -> Self {
        self.optional_parameters = optional_parameters;
        self
    }

    /// The flux.
    pub fn flux(&self) -> &Flux {
        &self.flux
    }

    /// Mutable access to the flux.
    pub fn flux_mut(&mut self) -> &mut Flux {
        &mut self.flux
    }

    /// The shape.
    pub fn shape(&self) -> &ComponentShape {
        &self.shape
    }

    /// Mutable access to the shape.
    pub fn shape_mut(&mut self) -> &mut ComponentShape {
        &mut self.shape
    }

    /// The spectral model.
    pub fn spectrum(&self) -> &SpectralModel {
        &self.spectrum
    }

    /// Mutable access to the spectral model.
    pub fn spectrum_mut(&mut self) -> &mut SpectralModel {
        &mut self.spectrum
    }

    /// The user's label (`label()`).
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Replace the label.
    pub fn set_label(&mut self, label: impl Into<String>) {
        self.label = label.into();
    }

    /// The optional parameters (`optionalParameters()`).
    pub fn optional_parameters(&self) -> &[f64] {
        &self.optional_parameters
    }

    /// Replace the optional parameters.
    pub fn set_optional_parameters(&mut self, optional_parameters: Vec<f64>) {
        self.optional_parameters = optional_parameters;
    }

    /// A limb-darkened disk's exponent n: the first optional parameter, or
    /// [`DEFAULT_LIMB_DARKENING_EXPONENT`] when there is none. `None` for
    /// other shapes.
    ///
    /// CASA's shape holds n as a single-precision float; prediction should
    /// round it the same way.
    pub fn limb_darkening_exponent(&self) -> Option<f64> {
        (self.shape.shape_type() == ShapeType::LimbDarkenedDisk).then(|| {
            self.optional_parameters
                .first()
                .copied()
                .unwrap_or(DEFAULT_LIMB_DARKENING_EXPONENT)
        })
    }
}
