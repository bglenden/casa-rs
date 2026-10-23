// SPDX-License-Identifier: LGPL-3.0-or-later
//! A component's spectral model, CASA's `SpectralModel` family.
//!
//! CASA's spectral models are a class hierarchy under `SpectralModel`, which
//! holds the reference frequency and its error. Here one [`SpectralModel`]
//! holds those, and a [`SpectralKind`] enum carries what each model adds.
//!
//! # Parameters
//!
//! A component list stores a spectrum as its name (`Spectrum_Shape`), its
//! reference frequency (`Reference_Frequency`, `Frequency_Frame`), that
//! frequency's error (`Frequency_Error`, `Frequency_Error_Units`), two vectors
//! laid out as CASA's `parameters()` and `errors()`
//! ([`SpectralModel::parameters`], [`SpectralModel::errors`]), and the record
//! CASA's `toRecord` writes (`Spectral_Record`), which carries what the vectors
//! cannot:
//!
//! | model | parameters | errors | only in `Spectral_Record` |
//! |---|---|---|---|
//! | constant | none | `[0]` | nothing |
//! | spectral index | α | α's error | the four Stokes indices |
//! | tabular | none | none | the frame, frequencies and Stokes fluxes of the table |
//! | power-log-polynomial | the coefficients | their errors | nothing |
//!
//! (CASA's constant spectrum reports one zero error although it has no
//! parameters; casa-rs writes the same.)

use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::quanta::{Quantity, Unit};

use crate::component_type::SpectralType;
use crate::error::ComponentError;

/// A spectral index: the flux scales as (ν/ν₀)^α.
///
/// CASA's `SpectralIndex` keeps α and, optionally, indices for all four
/// Stokes parameters, `[α_I, α_pol, RM, α_V]`: I scales with α_I, Q and U with
/// α_I + α_pol (so the fractional linear polarisation scales with α_pol) and
/// are rotated by the rotation measure RM (rad m⁻²), and V scales with
/// α_I + α_V.
///
/// Without the four, CASA keeps a one-element Stokes index, which it reports
/// (`stokesIndex()`, and a list's `Spectral_Record`) but never uses: α alone
/// scales the flux. Setting α (`setIndex`) sets it to α, but setting the
/// parameters (`setParameters`, as `readTable` does) leaves it, so a list
/// read from a table without `Spectral_Record` reports 0. casa-rs keeps it as
/// CASA does, so that what it writes and reports matches CASA.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralIndex {
    index: f64,
    error: f64,
    stokes_index: StokesIndex,
}

/// CASA's `itsStokesIndex`: one value (reported, not used) or the four.
#[derive(Debug, Clone, Copy, PartialEq)]
enum StokesIndex {
    Single(f64),
    Full([f64; 4]),
}

impl SpectralIndex {
    /// A spectral index α with zero error and no Stokes indices
    /// (`SpectralIndex(refFreq, exponent)`).
    pub fn new(index: f64) -> Self {
        Self {
            index,
            error: 0.0,
            stokes_index: StokesIndex::Single(index),
        }
    }

    /// Replace CASA's Stokes index as `SpectralIndex::fromRecord` does from a
    /// record's `stokesindex`: one value or four, leaving α alone. Other
    /// lengths are ignored, as `fromRecord` refuses them.
    pub(crate) fn with_casa_stokes_index(mut self, values: &[f64]) -> Self {
        match *values {
            [single] => self.stokes_index = StokesIndex::Single(single),
            [i, pol, rm, v] => self.stokes_index = StokesIndex::Full([i, pol, rm, v]),
            _ => {}
        }
        self
    }

    /// This index with α's error set (`setErrors`).
    ///
    /// # Errors
    ///
    /// [`ComponentError::Spectrum`] when the error is negative, which CASA
    /// refuses.
    pub fn with_error(mut self, error: f64) -> Result<Self, ComponentError> {
        self.error = non_negative_error("spectral index", error)?;
        Ok(self)
    }

    /// This index with the four Stokes indices `[α_I, α_pol, RM, α_V]` set
    /// (`setStokesIndex`). As in CASA, α becomes α_I.
    pub fn with_stokes_indices(mut self, stokes_indices: [f64; 4]) -> Self {
        self.index = stokes_indices[0];
        self.stokes_index = StokesIndex::Full(stokes_indices);
        self
    }

    /// α (`index`).
    pub fn index(&self) -> f64 {
        self.index
    }

    /// α's error.
    pub fn error(&self) -> f64 {
        self.error
    }

    /// The four Stokes indices, when they were set (`stokesIndex` of length
    /// four).
    pub fn stokes_indices(&self) -> Option<[f64; 4]> {
        match self.stokes_index {
            StokesIndex::Full(indices) => Some(indices),
            StokesIndex::Single(_) => None,
        }
    }

    /// CASA's `stokesIndex()`: the four indices, or the one-element vector
    /// CASA reports without them.
    pub fn casa_stokes_index(&self) -> Vec<f64> {
        match self.stokes_index {
            StokesIndex::Full(indices) => indices.to_vec(),
            StokesIndex::Single(single) => vec![single],
        }
    }
}

/// Flux tabulated against frequency, CASA's `TabularSpectrum`.
///
/// The frequencies are in Hz in the tabulation frame ([`Self::frame`]), and
/// each flux is Stokes I, Q, U and V. CASA normalises the tabulated flux to its
/// value at the reference frequency expressed in the tabulation frame.
#[derive(Debug, Clone, PartialEq)]
pub struct TabularSpectrum {
    frame: FrequencyRef,
    frequencies_hz: Vec<f64>,
    stokes_flux: Vec<[f64; 4]>,
}

impl TabularSpectrum {
    /// A tabulation in `frame`.
    ///
    /// # Errors
    ///
    /// [`ComponentError::Spectrum`] when the frequencies and fluxes differ in
    /// length (`TabularSpectrum::setValues`).
    pub fn new(
        frame: FrequencyRef,
        frequencies_hz: Vec<f64>,
        stokes_flux: Vec<[f64; 4]>,
    ) -> Result<Self, ComponentError> {
        if frequencies_hz.len() != stokes_flux.len() {
            return Err(ComponentError::Spectrum {
                spectrum: "tabular",
                reason: format!(
                    "{} frequencies but {} fluxes",
                    frequencies_hz.len(),
                    stokes_flux.len()
                ),
            });
        }
        Ok(Self {
            frame,
            frequencies_hz,
            stokes_flux,
        })
    }

    /// The empty tabulation in LSRK of CASA's default `TabularSpectrum`, which
    /// a table row without a `Spectral_Record` reads as.
    pub fn empty() -> Self {
        Self {
            frame: FrequencyRef::LSRK,
            frequencies_hz: Vec::new(),
            stokes_flux: Vec::new(),
        }
    }

    /// The frame the frequencies are in (`freqRef`).
    pub fn frame(&self) -> FrequencyRef {
        self.frame
    }

    /// The tabulated frequencies, Hz (`tabFreqVal`).
    pub fn frequencies_hz(&self) -> &[f64] {
        &self.frequencies_hz
    }

    /// The tabulated Stokes I, Q, U and V fluxes (`ival`, `qval`, `uval`,
    /// `vval`).
    pub fn stokes_flux(&self) -> &[[f64; 4]] {
        &self.stokes_flux
    }

    /// The highest and lowest tabulated frequencies, Hz (`maxFreq`,
    /// `minFreq`), or zeros for an empty tabulation as CASA's default.
    pub fn frequency_range_hz(&self) -> (f64, f64) {
        if self.frequencies_hz.is_empty() {
            return (0.0, 0.0);
        }
        let max = self
            .frequencies_hz
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let min = self
            .frequencies_hz
            .iter()
            .copied()
            .fold(f64::INFINITY, f64::min);
        (max, min)
    }

    /// The reference frequency expressed in the tabulation frame, Hz
    /// (CASA's `referenceFreq`, from `SpectralModel::refFreqInFrame`).
    ///
    /// CASA takes the value unconverted when the frames agree, or when either
    /// is REST, and otherwise converts it with an empty measures frame, which
    /// fails for every pair of moving frames.
    ///
    /// # Errors
    ///
    /// [`ComponentError::Spectrum`] when the frames differ and neither is
    /// REST.
    pub fn reference_frequency_in_frame(
        &self,
        reference: &MFrequency,
    ) -> Result<f64, ComponentError> {
        let unconverted = reference.refer() == self.frame
            || reference.refer() == FrequencyRef::REST
            || self.frame == FrequencyRef::REST;
        if unconverted {
            Ok(reference.hz())
        } else {
            Err(ComponentError::Spectrum {
                spectrum: "tabular",
                reason: format!(
                    "the reference frequency is in {} but the tabulation is in {}; \
                     CASA cannot convert between them without a measures frame",
                    reference.refer().as_str(),
                    self.frame.as_str()
                ),
            })
        }
    }
}

/// A power law whose exponent is a polynomial in the logarithm of the
/// frequency ratio, CASA's `PowerLogPoly`: with x = ν/ν₀ the flux scales as
/// x^(c₀ + c₁ ln x + c₂ (ln x)² + …).
#[derive(Debug, Clone, PartialEq)]
pub struct PowerLogPolynomial {
    coefficients: Vec<f64>,
    errors: Vec<f64>,
}

impl PowerLogPolynomial {
    /// Coefficients c₀, c₁, … with zero errors (`PowerLogPoly(refFreq, coeffs)`).
    pub fn new(coefficients: Vec<f64>) -> Self {
        let errors = vec![0.0; coefficients.len()];
        Self {
            coefficients,
            errors,
        }
    }

    /// This polynomial with its coefficients' errors set (`setErrors`).
    ///
    /// # Errors
    ///
    /// [`ComponentError::Spectrum`] when an error is negative (CASA refuses
    /// it) or the lengths differ.
    pub fn with_errors(mut self, errors: Vec<f64>) -> Result<Self, ComponentError> {
        if errors.len() != self.coefficients.len() {
            return Err(ComponentError::Spectrum {
                spectrum: "power-log-polynomial",
                reason: format!(
                    "{} coefficients but {} errors",
                    self.coefficients.len(),
                    errors.len()
                ),
            });
        }
        for error in &errors {
            non_negative_error("power-log-polynomial", *error)?;
        }
        self.errors = errors;
        Ok(self)
    }

    /// The coefficients c₀, c₁, … (`coeffs`).
    pub fn coefficients(&self) -> &[f64] {
        &self.coefficients
    }

    /// The coefficients' errors.
    pub fn errors(&self) -> &[f64] {
        &self.errors
    }
}

/// What each spectral model adds to the reference frequency.
#[derive(Debug, Clone, PartialEq)]
pub enum SpectralKind {
    /// A flat spectrum.
    Constant,
    /// A power law (spectral index).
    SpectralIndex(SpectralIndex),
    /// A tabulated spectrum.
    Tabular(TabularSpectrum),
    /// A power law with a log-polynomial exponent.
    PowerLogPolynomial(PowerLogPolynomial),
}

impl SpectralKind {
    /// The spectral model's type.
    pub fn spectral_type(&self) -> SpectralType {
        match self {
            Self::Constant => SpectralType::Constant,
            Self::SpectralIndex(_) => SpectralType::SpectralIndex,
            Self::Tabular(_) => SpectralType::Tabular,
            Self::PowerLogPolynomial(_) => SpectralType::PowerLogPolynomial,
        }
    }

    /// The model CASA's default constructor of `spectral_type` makes, before
    /// a table row's values are applied.
    pub(crate) fn casa_default(spectral_type: SpectralType) -> Self {
        match spectral_type {
            SpectralType::Constant => Self::Constant,
            SpectralType::SpectralIndex => Self::SpectralIndex(SpectralIndex::new(0.0)),
            SpectralType::Tabular => Self::Tabular(TabularSpectrum::empty()),
            SpectralType::PowerLogPolynomial => {
                Self::PowerLogPolynomial(PowerLogPolynomial::new(Vec::new()))
            }
        }
    }
}

/// A component's spectral model: its reference frequency, that frequency's
/// error, and what the model adds ([`SpectralKind`]).
///
/// This is CASA's `SpectralModel` with its subclasses folded into
/// [`SpectralKind`]. The reference frequency is a casa-types [`MFrequency`]
/// carrying its frame; a component list stores it in that frame
/// (`Frequency_Frame`), unconverted. The error is a quantity whose unit the
/// table stores too (`Frequency_Error_Units`); CASA's default is 0 GHz.
#[derive(Debug, Clone)]
pub struct SpectralModel {
    reference_frequency: MFrequency,
    reference_frequency_error: Quantity,
    kind: SpectralKind,
}

impl SpectralModel {
    /// A model at `reference_frequency` with a zero error (0 GHz, CASA's
    /// default).
    pub fn new(reference_frequency: MFrequency, kind: SpectralKind) -> Self {
        Self {
            reference_frequency,
            reference_frequency_error: zero_frequency_error(),
            kind,
        }
    }

    /// A flat spectrum at `reference_frequency`.
    pub fn constant(reference_frequency: MFrequency) -> Self {
        Self::new(reference_frequency, SpectralKind::Constant)
    }

    /// This model with its reference frequency's error replaced
    /// (`setRefFrequencyError`).
    ///
    /// # Errors
    ///
    /// [`ComponentError::BadError`] when the error is negative or not a
    /// frequency, which CASA refuses.
    pub fn with_reference_frequency_error(
        mut self,
        error: Quantity,
    ) -> Result<Self, ComponentError> {
        self.set_reference_frequency_error(error)?;
        Ok(self)
    }

    /// The reference frequency, in its own frame (`refFrequency`).
    pub fn reference_frequency(&self) -> &MFrequency {
        &self.reference_frequency
    }

    /// Replace the reference frequency (`setRefFrequency`).
    pub fn set_reference_frequency(&mut self, reference_frequency: MFrequency) {
        self.reference_frequency = reference_frequency;
    }

    /// The reference frequency's error (`refFrequencyError`).
    pub fn reference_frequency_error(&self) -> &Quantity {
        &self.reference_frequency_error
    }

    /// Replace the reference frequency's error (`setRefFrequencyError`).
    ///
    /// # Errors
    ///
    /// [`ComponentError::BadError`] when the error is negative or not a
    /// frequency (`SpectralModel::badError`).
    pub fn set_reference_frequency_error(&mut self, error: Quantity) -> Result<(), ComponentError> {
        let hz = Unit::new("Hz").expect("Hz is a unit");
        if !error.unit().conformant(&hz) || error.value() < 0.0 {
            return Err(ComponentError::BadError {
                what: "reference frequency error",
                kind: "frequency",
                value: error.value(),
                unit: error.unit().name().to_string(),
            });
        }
        self.reference_frequency_error = error;
        Ok(())
    }

    /// What the model adds to the reference frequency.
    pub fn kind(&self) -> &SpectralKind {
        &self.kind
    }

    /// Mutable access to what the model adds.
    pub fn kind_mut(&mut self) -> &mut SpectralKind {
        &mut self.kind
    }

    /// The model's type.
    pub fn spectral_type(&self) -> SpectralType {
        self.kind.spectral_type()
    }

    /// The parameters, laid out as CASA's `parameters()`.
    pub fn parameters(&self) -> Vec<f64> {
        match &self.kind {
            SpectralKind::Constant | SpectralKind::Tabular(_) => Vec::new(),
            SpectralKind::SpectralIndex(index) => vec![index.index],
            SpectralKind::PowerLogPolynomial(polynomial) => polynomial.coefficients.clone(),
        }
    }

    /// The parameters' errors, laid out as CASA's `errors()`; a constant
    /// spectrum reports one zero.
    pub fn errors(&self) -> Vec<f64> {
        match &self.kind {
            SpectralKind::Constant => vec![0.0],
            SpectralKind::Tabular(_) => Vec::new(),
            SpectralKind::SpectralIndex(index) => vec![index.error],
            SpectralKind::PowerLogPolynomial(polynomial) => polynomial.errors.clone(),
        }
    }

    /// Apply a parameter vector as CASA's `setParameters` does.
    ///
    /// A constant spectrum ignores it; a spectral index takes α from its first
    /// value; a power-log polynomial takes all of them as coefficients,
    /// keeping its errors for the coefficients it still has and zero for new
    /// ones.
    ///
    /// # Errors
    ///
    /// [`ComponentError::ParameterCount`] for a spectral index without a
    /// value or a tabular spectrum with any (CASA asserts it has none).
    pub fn set_parameters(&mut self, parameters: &[f64]) -> Result<(), ComponentError> {
        match &mut self.kind {
            SpectralKind::Constant => {}
            SpectralKind::SpectralIndex(index) => {
                index.index = *parameters.first().ok_or(ComponentError::ParameterCount {
                    what: "spectral index parameters",
                    expected: 1,
                    actual: 0,
                })?;
            }
            SpectralKind::Tabular(_) => {
                if !parameters.is_empty() {
                    return Err(ComponentError::ParameterCount {
                        what: "tabular spectrum parameters",
                        expected: 0,
                        actual: parameters.len(),
                    });
                }
            }
            SpectralKind::PowerLogPolynomial(polynomial) => {
                polynomial.coefficients = parameters.to_vec();
                polynomial.errors.resize(parameters.len(), 0.0);
            }
        }
        Ok(())
    }

    /// Apply an error vector as CASA's `setErrors` does.
    ///
    /// # Errors
    ///
    /// [`ComponentError::Spectrum`] for a negative spectral-index or
    /// power-log-polynomial error, and [`ComponentError::ParameterCount`] for
    /// a vector of the wrong length.
    pub fn set_errors(&mut self, errors: &[f64]) -> Result<(), ComponentError> {
        match &mut self.kind {
            SpectralKind::Constant => {}
            SpectralKind::SpectralIndex(index) => {
                let error = *errors.first().ok_or(ComponentError::ParameterCount {
                    what: "spectral index errors",
                    expected: 1,
                    actual: 0,
                })?;
                index.error = non_negative_error("spectral index", error)?;
            }
            SpectralKind::Tabular(_) => {
                if !errors.is_empty() {
                    return Err(ComponentError::ParameterCount {
                        what: "tabular spectrum errors",
                        expected: 0,
                        actual: errors.len(),
                    });
                }
            }
            SpectralKind::PowerLogPolynomial(polynomial) => {
                if errors.len() != polynomial.coefficients.len() {
                    return Err(ComponentError::ParameterCount {
                        what: "power-log-polynomial errors",
                        expected: polynomial.coefficients.len(),
                        actual: errors.len(),
                    });
                }
                for error in errors {
                    non_negative_error("power-log-polynomial", *error)?;
                }
                polynomial.errors = errors.to_vec();
            }
        }
        Ok(())
    }
}

impl PartialEq for SpectralModel {
    fn eq(&self, other: &Self) -> bool {
        self.reference_frequency.refer() == other.reference_frequency.refer()
            && self.reference_frequency.hz() == other.reference_frequency.hz()
            && self.reference_frequency_error == other.reference_frequency_error
            && self.kind == other.kind
    }
}

/// CASA's default reference-frequency error, 0 GHz.
pub(crate) fn zero_frequency_error() -> Quantity {
    Quantity::with_unit(0.0, Unit::new("GHz").expect("GHz is a unit"))
}

fn non_negative_error(spectrum: &'static str, error: f64) -> Result<f64, ComponentError> {
    if error < 0.0 {
        Err(ComponentError::Spectrum {
            spectrum,
            reason: format!("the errors must be non-negative, got {error}"),
        })
    } else {
        Ok(error)
    }
}
