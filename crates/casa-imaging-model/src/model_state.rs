// SPDX-License-Identifier: LGPL-3.0-or-later

//! Closed commitments and value schemas for authoritative model state.

use thiserror::Error;

use crate::{
    CompiledGeometry, DirectionCoordinateSpec, ImageDomainRole, ImageShape, ModelCoefficientSpace,
    NumericPrecision, NumericsContract, ReconstructionBasis, SpectralCoordinateSpec,
};

/// One finite, canonical semantic `f64` model coefficient or increment.
///
/// The representation is precision-independent. Arithmetic that creates a new
/// value remains governed by the lifecycle precision selected from the
/// Compiled Problem's Numerics Contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelValue(u64);

impl ModelValue {
    /// Construct a finite value, canonicalizing negative zero.
    pub fn new(value: f64) -> Result<Self, ModelContractError> {
        if !value.is_finite() {
            return Err(ModelContractError::NonFiniteValue);
        }
        Ok(Self(canonical_f64_bits(value)))
    }

    /// Return the represented value.
    #[must_use]
    pub fn value(self) -> f64 {
        f64::from_bits(self.0)
    }
}

/// Whether one model coefficient belongs to the declared valid support.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ModelSupport {
    /// The coefficient is scientifically defined.
    Valid,
    /// The coefficient is outside valid support; its numeric payload is not a value.
    Invalid,
}

/// One semantic model coefficient and its independent support state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelSample {
    value: ModelValue,
    support: ModelSupport,
}

impl ModelSample {
    /// Construct one valid model coefficient.
    #[must_use]
    pub const fn valid(value: ModelValue) -> Self {
        Self {
            value,
            support: ModelSupport::Valid,
        }
    }

    /// Construct an invalid-support sample with a canonical non-value payload.
    pub fn invalid() -> Self {
        Self {
            value: ModelValue(0),
            support: ModelSupport::Invalid,
        }
    }

    /// Return the semantic numeric payload.
    ///
    /// Callers must inspect [`Self::support`] before treating it as a value.
    #[must_use]
    pub const fn value(self) -> ModelValue {
        self.value
    }

    /// Return the independent validity state.
    #[must_use]
    pub const fn support(self) -> ModelSupport {
        self.support
    }
}

/// Explicit logical cardinality and numeric ceilings for one model lifecycle.
/// Physical residency is independently bounded by the execution storage plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelBounds {
    max_model_samples: usize,
    max_delta_terms: usize,
    max_absolute_model_value: ModelValue,
    max_absolute_delta_value: ModelValue,
}

impl ModelBounds {
    /// Construct positive finite numeric bounds and exact count ceilings.
    pub fn new(
        max_model_samples: usize,
        max_delta_terms: usize,
        max_absolute_model_value: f64,
        max_absolute_delta_value: f64,
    ) -> Result<Self, ModelContractError> {
        if max_model_samples == 0
            || max_delta_terms == 0
            || !max_absolute_model_value.is_finite()
            || max_absolute_model_value <= 0.0
            || !max_absolute_delta_value.is_finite()
            || max_absolute_delta_value <= 0.0
        {
            return Err(ModelContractError::InvalidBounds);
        }
        Ok(Self {
            max_model_samples,
            max_delta_terms,
            max_absolute_model_value: ModelValue::new(max_absolute_model_value)?,
            max_absolute_delta_value: ModelValue::new(max_absolute_delta_value)?,
        })
    }

    /// Return the maximum logical target-model sample count.
    #[must_use]
    pub const fn max_model_samples(self) -> usize {
        self.max_model_samples
    }

    /// Return the maximum number of terms in one Model Delta.
    #[must_use]
    pub const fn max_delta_terms(self) -> usize {
        self.max_delta_terms
    }

    /// Return the absolute model-value ceiling.
    #[must_use]
    pub fn max_absolute_model_value(self) -> f64 {
        self.max_absolute_model_value.value()
    }

    /// Return the absolute per-term delta ceiling.
    #[must_use]
    pub fn max_absolute_delta_value(self) -> f64 {
        self.max_absolute_delta_value.value()
    }
}

/// One coefficient cell flattened in domain, coefficient, polarization, y, x order.
///
/// The two-element pixel coordinate itself is represented as `[x, y]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelCell {
    domain: usize,
    coefficient: usize,
    polarization: usize,
    pixel: [usize; 2],
}

impl ModelCell {
    /// Construct one typed cell coordinate.
    #[must_use]
    pub const fn new(
        domain: usize,
        coefficient: usize,
        polarization: usize,
        pixel: [usize; 2],
    ) -> Self {
        Self {
            domain,
            coefficient,
            polarization,
            pixel,
        }
    }

    /// Return the image-domain ordinal.
    #[must_use]
    pub const fn domain(self) -> usize {
        self.domain
    }

    /// Return the spectral-basis coefficient ordinal.
    #[must_use]
    pub const fn coefficient(self) -> usize {
        self.coefficient
    }

    /// Return the polarization-coordinate ordinal.
    #[must_use]
    pub const fn polarization(self) -> usize {
        self.polarization
    }

    /// Return `[x, y]` pixel coordinates.
    #[must_use]
    pub const fn pixel(self) -> [usize; 2] {
        self.pixel
    }
}

/// Rectangular model-array shape with typed WCS and coefficient-space provenance.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelSourceShape {
    coefficient_space: ModelCoefficientSpace,
    domains: Box<[ImageShape]>,
    domain_roles: Box<[ImageDomainRole]>,
    directions: Box<[DirectionCoordinateSpec]>,
    spectral: SpectralCoordinateSpec,
    coefficients: usize,
    polarizations: usize,
    samples: usize,
}

// `ModelSourceShape` is constructed only from canonical compiled geometry, so
// its finite floating-point coordinate laws have reflexive equality.
impl Eq for ModelSourceShape {}

impl ModelSourceShape {
    /// Derive one checked shape from compiler-owned geometry and the coefficient
    /// space compiled with it.
    pub fn from_compiled(
        geometry: &CompiledGeometry,
        coefficient_space: &ModelCoefficientSpace,
    ) -> Result<Self, ModelContractError> {
        let domains = geometry
            .domains()
            .iter()
            .map(|domain| domain.shape())
            .collect::<Vec<_>>();
        let directions = geometry
            .domains()
            .iter()
            .map(|domain| domain.direction())
            .collect::<Vec<_>>();
        let domain_roles = geometry
            .domains()
            .iter()
            .map(|domain| domain.role().clone())
            .collect::<Vec<_>>();
        let coefficients = coefficient_count(coefficient_space.basis());
        let polarizations = coefficient_space.polarization().coordinates().len();
        if domains.is_empty() || coefficients == 0 || polarizations == 0 {
            return Err(ModelContractError::InvalidShape);
        }
        let mut samples = 0usize;
        for domain in &domains {
            let [width, height] = domain.pixels();
            if width == 0 || height == 0 {
                return Err(ModelContractError::InvalidShape);
            }
            let domain_samples = width
                .checked_mul(height)
                .and_then(|pixels| pixels.checked_mul(coefficients))
                .and_then(|values| values.checked_mul(polarizations))
                .ok_or(ModelContractError::ShapeTooLarge)?;
            samples = samples
                .checked_add(domain_samples)
                .ok_or(ModelContractError::ShapeTooLarge)?;
        }
        Ok(Self {
            coefficient_space: coefficient_space.clone(),
            domains: domains.into_boxed_slice(),
            domain_roles: domain_roles.into_boxed_slice(),
            directions: directions.into_boxed_slice(),
            spectral: geometry.spectral().clone(),
            coefficients,
            polarizations,
            samples,
        })
    }

    /// Return the exact compiler-owned coordinate and coefficient space.
    #[must_use]
    pub const fn coefficient_space(&self) -> &ModelCoefficientSpace {
        &self.coefficient_space
    }

    /// Return domain shapes in canonical domain order.
    #[must_use]
    pub const fn domains(&self) -> &[ImageShape] {
        &self.domains
    }

    /// Return domain roles in the same canonical order as [`Self::domains`].
    #[must_use]
    pub const fn domain_roles(&self) -> &[ImageDomainRole] {
        &self.domain_roles
    }

    /// Return the exact direction-coordinate law for one canonical domain.
    #[must_use]
    pub fn direction(&self, domain: usize) -> Option<DirectionCoordinateSpec> {
        self.directions.get(domain).copied()
    }

    /// Return the exact compiler-owned spectral coordinate law.
    #[must_use]
    pub const fn spectral(&self) -> &SpectralCoordinateSpec {
        &self.spectral
    }

    /// Return the number of spectral-basis coefficient planes.
    #[must_use]
    pub const fn coefficients(&self) -> usize {
        self.coefficients
    }

    /// Return the number of polarization planes.
    #[must_use]
    pub const fn polarizations(&self) -> usize {
        self.polarizations
    }

    /// Return the exact flattened sample count.
    #[must_use]
    pub const fn sample_count(&self) -> usize {
        self.samples
    }

    /// Return a cell's canonical flattened ordinal, when it belongs to this shape.
    #[must_use]
    pub fn flat_index(&self, cell: ModelCell) -> Option<usize> {
        if cell.coefficient >= self.coefficients || cell.polarization >= self.polarizations {
            return None;
        }
        let mut offset = 0usize;
        for (domain_index, shape) in self.domains.iter().enumerate() {
            let [width, height] = shape.pixels();
            if domain_index == cell.domain {
                let [x, y] = cell.pixel;
                if x >= width || y >= height {
                    return None;
                }
                let plane = cell
                    .coefficient
                    .checked_mul(self.polarizations)?
                    .checked_add(cell.polarization)?;
                return offset
                    .checked_add(plane.checked_mul(width.checked_mul(height)?)?)?
                    .checked_add(y.checked_mul(width)?)?
                    .checked_add(x);
            }
            offset = offset.checked_add(
                width
                    .checked_mul(height)?
                    .checked_mul(self.coefficients)?
                    .checked_mul(self.polarizations)?,
            )?;
        }
        None
    }

    /// Return the typed cell at one canonical flattened ordinal.
    #[must_use]
    pub fn cell_at(&self, mut index: usize) -> Option<ModelCell> {
        if index >= self.samples {
            return None;
        }
        for (domain, shape) in self.domains.iter().enumerate() {
            let [width, height] = shape.pixels();
            let pixels = width.checked_mul(height)?;
            let domain_samples = pixels
                .checked_mul(self.coefficients)?
                .checked_mul(self.polarizations)?;
            if index < domain_samples {
                let plane = index / pixels;
                index %= pixels;
                return Some(ModelCell::new(
                    domain,
                    plane / self.polarizations,
                    plane % self.polarizations,
                    [index % width, index / width],
                ));
            }
            index -= domain_samples;
        }
        None
    }
}

/// Uncompiled lifecycle requirements carried by the sole imaging request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelLifecycleRequirements {
    bounds: ModelBounds,
    arithmetic_precision: NumericPrecision,
}

impl ModelLifecycleRequirements {
    /// Bind explicit resource bounds and arithmetic precision.
    #[must_use]
    pub const fn new(bounds: ModelBounds, arithmetic_precision: NumericPrecision) -> Self {
        Self {
            bounds,
            arithmetic_precision,
        }
    }
}

/// Compiler-owned closed commitment consumed by the reconstruction owner.
///
/// Every lifecycle begins from a zero-valued model with valid support
/// everywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelLifecycleContract {
    target: ModelSourceShape,
    bounds: ModelBounds,
    arithmetic_precision: NumericPrecision,
}

impl ModelLifecycleContract {
    /// Return the sole compiler-derived target model space.
    #[must_use]
    pub const fn target(&self) -> &ModelSourceShape {
        &self.target
    }

    /// Return all explicit owner bounds.
    #[must_use]
    pub const fn bounds(&self) -> ModelBounds {
        self.bounds
    }

    /// Return the arithmetic precision selected from the Numerics Contract.
    #[must_use]
    pub const fn arithmetic_precision(&self) -> NumericPrecision {
        self.arithmetic_precision
    }
}

/// One sparse solver update term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelDeltaTerm {
    cell: ModelCell,
    increment: ModelValue,
}

impl ModelDeltaTerm {
    /// Construct one typed cell increment.
    #[must_use]
    pub const fn new(cell: ModelCell, increment: ModelValue) -> Self {
        Self { cell, increment }
    }

    /// Return the changed cell.
    #[must_use]
    pub const fn cell(self) -> ModelCell {
        self.cell
    }

    /// Return the additive increment.
    #[must_use]
    pub const fn increment(self) -> ModelValue {
        self.increment
    }
}

/// Exact reason a model schema or compiler commitment was rejected.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ModelContractError {
    /// A model coefficient or increment was non-finite.
    #[error("model values must be finite")]
    NonFiniteValue,
    /// Numeric or count bounds were empty, non-finite, or non-positive.
    #[error("model lifecycle bounds must be explicit, finite, and positive")]
    InvalidBounds,
    /// A source or target shape was empty.
    #[error("model shapes require domains, coefficient planes, and polarization planes")]
    InvalidShape,
    /// A shape sample count overflowed `usize`.
    #[error("model shape is too large to represent")]
    ShapeTooLarge,
    /// The target model exceeded its compiled residency ceiling.
    #[error("target model has {samples} samples, exceeding bound {bound}")]
    ModelSampleBoundExceeded {
        /// Exact target sample count.
        samples: usize,
        /// Declared target ceiling.
        bound: usize,
    },
    /// The lifecycle selected arithmetic not permitted by the Numerics Contract.
    #[error("model lifecycle arithmetic precision is not permitted by the Numerics Contract")]
    PrecisionNotPermitted,
}

pub(crate) fn compile_model_lifecycle_contract(
    geometry: &CompiledGeometry,
    coefficient_space: &ModelCoefficientSpace,
    numerics: &NumericsContract,
    requirements: ModelLifecycleRequirements,
) -> Result<ModelLifecycleContract, ModelContractError> {
    let target = ModelSourceShape::from_compiled(geometry, coefficient_space)?;
    let ModelLifecycleRequirements {
        bounds,
        arithmetic_precision,
    } = requirements;
    if target.sample_count() > bounds.max_model_samples {
        return Err(ModelContractError::ModelSampleBoundExceeded {
            samples: target.sample_count(),
            bound: bounds.max_model_samples,
        });
    }
    if !numerics
        .permitted_precisions()
        .contains(&arithmetic_precision)
    {
        return Err(ModelContractError::PrecisionNotPermitted);
    }
    Ok(ModelLifecycleContract {
        target,
        bounds,
        arithmetic_precision,
    })
}

const fn coefficient_count(basis: ReconstructionBasis) -> usize {
    match basis {
        ReconstructionBasis::Constant => 1,
        ReconstructionBasis::Taylor { terms } => terms,
        ReconstructionBasis::ChannelLocal { channels } => channels,
    }
}

fn canonical_f64_bits(value: f64) -> u64 {
    if value == 0.0 { 0 } else { value.to_bits() }
}
