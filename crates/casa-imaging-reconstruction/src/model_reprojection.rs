// SPDX-License-Identifier: LGPL-3.0-or-later

//! Reconstruction-owned reprojection of a source model onto a compiled target.

use std::{error::Error, fmt, ops::Deref};

use casa_imaging_model::{
    CompiledProblem, DirectionCoordinateSpec, LogicalIdentity, ModelBasisConversionRegistry,
    ModelBounds, ModelCell, ModelContractError, ModelDirectionConversionRegistry,
    ModelInputCommitment, ModelInvalidContributorPolicy, ModelLifecycleRequirements,
    ModelPolarizationConversionRegistry, ModelReprojectedSeedProjection, ModelReprojectionPolicy,
    ModelSample, ModelSourceShape, ModelStateIdentity, ModelSupport, ModelUncoveredTargetPolicy,
    ModelValue, NumericPrecision, PolarizationCoordinate, ReconstructionBasis,
    model_reprojected_seed_mapping_identity,
};

use crate::{ModelLifecycleError, ModelReprojectionId, validate_model_value};

/// Fallible random-access source used by reconstruction-owned reprojection.
///
/// The reader supplies only source identity, typed geometry, and samples as
/// data. It cannot supply interpolation rules, support evidence, or a
/// reprojection identity; the reconstruction owner derives those values.
pub trait ModelSourceReader {
    /// Storage/provider error preserved by the preparation pass.
    type Error;

    /// Return the immutable source artifact identity.
    fn source_identity(&self) -> LogicalIdentity;

    /// Return the source artifact's exact typed model space.
    fn source_shape(&self) -> &ModelSourceShape;

    /// Read one source sample named by its typed cell.
    fn read_sample(&mut self, cell: ModelCell) -> Result<ModelSample, Self::Error>;
}

/// Failure while deriving one reprojected model seed from a source reader.
#[derive(Debug, PartialEq, Eq)]
pub enum ModelReprojectionError<E> {
    /// The source reader failed at the requested cell.
    Source(E),
    /// Reconstruction rejected geometry, support, numerics, or bounds.
    Lifecycle(ModelLifecycleError),
}

impl<E: fmt::Display> fmt::Display for ModelReprojectionError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source(error) => write!(formatter, "model source read failed: {error}"),
            Self::Lifecycle(error) => error.fmt(formatter),
        }
    }
}

impl<E: Error + 'static> Error for ModelReprojectionError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::Lifecycle(error) => Some(error),
        }
    }
}

impl<E> From<ModelLifecycleError> for ModelReprojectionError<E> {
    fn from(error: ModelLifecycleError) -> Self {
        Self::Lifecycle(error)
    }
}

/// Opaque owner-derived reprojection ready to bind into a Compiled Problem.
///
/// Preparation is target-ordered and retains only the target generation plus
/// the current interpolation stencil. The value is deliberately not `Clone`;
/// lifecycle ingestion consumes it after checking every compiled claim.
#[derive(Debug)]
pub struct PreparedReprojectedSeed {
    pub(crate) projection: ModelReprojectedSeedProjection,
    pub(crate) target_shape: ModelSourceShape,
    pub(crate) bounds: ModelBounds,
    pub(crate) precision: NumericPrecision,
    pub(crate) samples: Box<[ModelSample]>,
}

impl PreparedReprojectedSeed {
    /// Return requirements containing the exact owner-derived evidence.
    #[must_use]
    pub fn lifecycle_requirements(&self) -> ModelLifecycleRequirements {
        ModelLifecycleRequirements::new(
            self.bounds,
            self.precision,
            ModelInputCommitment::ReprojectedSeed(self.projection.clone()),
        )
    }

    /// Return the canonical owner-derived reprojection identity.
    #[must_use]
    pub fn reprojection_id(&self) -> ModelReprojectionId {
        ModelReprojectionId(self.projection.reprojection())
    }

    /// Bind this owner preparation to its final compiler projection.
    ///
    /// The compiler projection alone is descriptive. Consuming this opaque
    /// preparation is the only way to construct an executable reprojected
    /// problem, and every projected identity is checked before branding it.
    pub fn bind_compiled_problem(
        self,
        problem: CompiledProblem,
    ) -> Result<ExecutableModelProblem, ModelLifecycleError> {
        let ModelInputCommitment::ReprojectedSeed(projection) = problem.model_lifecycle().input()
        else {
            return Err(ModelLifecycleError::InitialModelKindMismatch);
        };
        if projection.source() != self.projection.source()
            || projection.source_shape() != self.projection.source_shape()
            || projection.preparation_contract() != self.projection.preparation_contract()
            || self.target_shape != *problem.model_lifecycle().target()
            || self.bounds != problem.model_lifecycle().bounds()
            || self.precision != problem.model_lifecycle().arithmetic_precision()
            || problem.inputs().model() != ModelStateIdentity::Seed(self.projection.source())
        {
            return Err(ModelLifecycleError::SourceProvenanceMismatch);
        }
        if projection.reprojection() != self.projection.reprojection() {
            return Err(ModelLifecycleError::ReprojectionIdentityMismatch);
        }
        Ok(ExecutableModelProblem {
            problem,
            prepared: Some(self),
        })
    }
}

/// Reconstruction-branded Compiled Problem accepted by execution and receipts.
///
/// Direct inputs are admitted only when no reprojected preparation claim is
/// present. Reprojected inputs can be constructed only by consuming
/// [`PreparedReprojectedSeed::bind_compiled_problem`].
///
/// The brand cannot be minted by downstream callers:
///
/// ```compile_fail
/// use casa_imaging_model::CompiledProblem;
/// use casa_imaging_reconstruction::ExecutableModelProblem;
///
/// fn forge(problem: CompiledProblem) -> ExecutableModelProblem {
///     ExecutableModelProblem {
///         problem,
///         prepared: None,
///     }
/// }
/// ```
#[derive(Debug)]
pub struct ExecutableModelProblem {
    pub(crate) problem: CompiledProblem,
    pub(crate) prepared: Option<PreparedReprojectedSeed>,
}

impl ExecutableModelProblem {
    /// Admit a Compiled Problem whose initial model needs no prior reprojection.
    pub fn from_compiled(problem: CompiledProblem) -> Result<Self, ModelLifecycleError> {
        if matches!(
            problem.model_lifecycle().input(),
            ModelInputCommitment::ReprojectedSeed(_)
        ) {
            return Err(ModelLifecycleError::OwnerPreparationRequired);
        }
        Ok(Self {
            problem,
            prepared: None,
        })
    }

    /// Borrow the immutable dependency-free compiler projection.
    #[must_use]
    pub const fn compiled_problem(&self) -> &CompiledProblem {
        &self.problem
    }
}

impl Deref for ExecutableModelProblem {
    type Target = CompiledProblem;

    fn deref(&self) -> &Self::Target {
        &self.problem
    }
}

#[derive(Debug, Clone, Copy)]
struct WeightedSourceCell {
    cell: ModelCell,
    weight: f64,
}

#[derive(Debug, Clone, Copy)]
struct WeightedCoefficient {
    coefficient: usize,
    weight: f64,
}

#[derive(Debug, Clone, Copy)]
struct WeightedPolarization {
    polarization: usize,
    weight: f64,
}

/// Derive one exact target-ordered reprojection and its compiled input evidence.
///
/// The caller provides only a fallible source reader and a compiler-owned
/// empty target problem. Reconstruction derives the target shape, bounds,
/// precision, coefficient and polarization correspondence, affine
/// direction-coordinate stencils, target support, and the reprojection
/// identity in the same bounded pass.
pub fn prepare_reprojected_seed<R: ModelSourceReader>(
    reader: &mut R,
    target_problem: &CompiledProblem,
) -> Result<PreparedReprojectedSeed, ModelReprojectionError<R::Error>> {
    let target_contract = target_problem.model_lifecycle();
    if !matches!(target_contract.input(), ModelInputCommitment::Empty) {
        return Err(ModelLifecycleError::InitialModelKindMismatch.into());
    }
    let target_shape = target_contract.target();
    let bounds = target_contract.bounds();
    let precision = target_contract.arithmetic_precision();
    let reprojection_policy = target_contract.reprojection_policy();
    let source = reader.source_identity();
    let source_shape = reader.source_shape().clone();
    if source.as_bytes() == [0; 32] {
        return Err(
            ModelLifecycleError::Contract(ModelContractError::UnidentifiedInputEvidence).into(),
        );
    }
    if source_shape == *target_shape {
        return Err(ModelLifecycleError::SourceProvenanceMismatch.into());
    }
    if source_shape.sample_count() > bounds.max_source_samples() {
        return Err(
            ModelLifecycleError::Contract(ModelContractError::SourceSampleBoundExceeded {
                samples: source_shape.sample_count(),
                bound: bounds.max_source_samples(),
            })
            .into(),
        );
    }
    if target_shape.sample_count() > bounds.max_model_samples() {
        return Err(
            ModelLifecycleError::Contract(ModelContractError::ModelSampleBoundExceeded {
                samples: target_shape.sample_count(),
                bound: bounds.max_model_samples(),
            })
            .into(),
        );
    }
    if source_shape.domain_roles() != target_shape.domain_roles() {
        return Err(ModelLifecycleError::UnsupportedDomainMapping.into());
    }

    let preparation_contract =
        LogicalIdentity::from_sha256(target_contract.contract_id().as_bytes());
    let mapping = model_reprojected_seed_mapping_identity(
        preparation_contract,
        source_shape.identity(),
        target_shape.identity(),
    );
    let mut samples = Vec::with_capacity(target_shape.sample_count());
    let mut term_count = 0usize;
    for target_index in 0..target_shape.sample_count() {
        let target = target_shape
            .cell_at(target_index)
            .expect("target index is derived from the target shape");
        let stencil = derive_reprojection_stencil(
            &source_shape,
            target_shape,
            target,
            precision,
            reprojection_policy,
            term_count,
            bounds.max_reprojection_terms(),
        )?;
        match stencil {
            None => match reprojection_policy.uncovered_target() {
                ModelUncoveredTargetPolicy::Invalid => {
                    samples.push(ModelSample::invalid());
                }
            },
            Some(stencil) => {
                term_count = term_count.checked_add(stencil.len()).ok_or(
                    ModelLifecycleError::ReprojectionTermBoundExceeded {
                        terms: usize::MAX,
                        bound: bounds.max_reprojection_terms(),
                    },
                )?;
                if term_count > bounds.max_reprojection_terms() {
                    return Err(ModelLifecycleError::ReprojectionTermBoundExceeded {
                        terms: term_count,
                        bound: bounds.max_reprojection_terms(),
                    }
                    .into());
                }
                let mut valid = true;
                let mut value = 0.0_f64;
                for weighted in stencil {
                    let sample = reader
                        .read_sample(weighted.cell)
                        .map_err(ModelReprojectionError::Source)?;
                    if sample.support() == ModelSupport::Invalid {
                        match reprojection_policy.invalid_contributor() {
                            ModelInvalidContributorPolicy::InvalidateTarget => valid = false,
                        }
                    } else {
                        validate_model_value(sample.value(), bounds.max_absolute_model_value())?;
                        let product = multiply_with_precision(
                            precision,
                            sample.value().value(),
                            weighted.weight,
                        );
                        value = add_with_precision(precision, value, product);
                    }
                }
                if valid {
                    let value = ModelValue::new(value).map_err(ModelLifecycleError::from)?;
                    validate_model_value(value, bounds.max_absolute_model_value())?;
                    samples.push(ModelSample::valid(value));
                } else {
                    samples.push(ModelSample::invalid());
                }
            }
        }
    }
    let projection = ModelReprojectedSeedProjection::from_identities(
        source,
        source_shape.clone(),
        preparation_contract,
        mapping,
    )
    .map_err(ModelLifecycleError::from)?;
    Ok(PreparedReprojectedSeed {
        projection,
        target_shape: target_shape.clone(),
        bounds,
        precision,
        samples: samples.into_boxed_slice(),
    })
}

fn derive_reprojection_stencil(
    source: &ModelSourceShape,
    target_shape: &ModelSourceShape,
    target: ModelCell,
    precision: NumericPrecision,
    reprojection_policy: ModelReprojectionPolicy,
    prior_terms: usize,
    term_bound: usize,
) -> Result<Option<Vec<WeightedSourceCell>>, ModelLifecycleError> {
    if source.coefficient_space().inner_product()
        != target_shape.coefficient_space().inner_product()
    {
        return Err(ModelLifecycleError::UnsupportedBasisConversion);
    }
    let coefficient_terms = match reprojection_policy.basis_registry() {
        ModelBasisConversionRegistry::ExactSpectralV1 => {
            basis_conversion_terms(source, target_shape, target.coefficient(), precision)
        }
    }?;
    let Some(coefficient_terms) = coefficient_terms else {
        return Ok(None);
    };
    let polarization_terms = match reprojection_policy.polarization_registry() {
        ModelPolarizationConversionRegistry::RealParallelHandsV1 => {
            polarization_conversion_terms(source, target_shape, target.polarization())
        }
    }?;

    let source_direction = source
        .direction(target.domain())
        .ok_or(ModelLifecycleError::UnsupportedDomainMapping)?;
    let target_direction = target_shape
        .direction(target.domain())
        .ok_or(ModelLifecycleError::UnsupportedDomainMapping)?;
    let source_pixel = match reprojection_policy.direction_registry() {
        ModelDirectionConversionRegistry::SameTangentPlaneAffineBilinearV1 => {
            affine_source_pixel(source_direction, target_direction, target.pixel())
        }
    }?;
    let [width, height] = source
        .domains()
        .get(target.domain())
        .ok_or(ModelLifecycleError::UnsupportedDomainMapping)?
        .pixels();
    let tolerance = coordinate_tolerance(precision);
    let Some(x_terms) = axis_stencil(source_pixel[0], width, tolerance) else {
        return Ok(None);
    };
    let Some(y_terms) = axis_stencil(source_pixel[1], height, tolerance) else {
        return Ok(None);
    };
    if !normalized_weight_sum(
        precision,
        x_terms.iter().fold(0.0, |sum, (_, weight)| {
            add_with_precision(precision, sum, *weight)
        }),
    ) || !normalized_weight_sum(
        precision,
        y_terms.iter().fold(0.0, |sum, (_, weight)| {
            add_with_precision(precision, sum, *weight)
        }),
    ) {
        return Err(ModelLifecycleError::UnnormalizedReprojectionStencil);
    }
    let mut stencil = Vec::new();
    for (y, y_weight) in y_terms {
        for &(x, x_weight) in &x_terms {
            for coefficient in &coefficient_terms {
                for polarization in &polarization_terms {
                    let spatial_weight = multiply_with_precision(precision, x_weight, y_weight);
                    let basis_weight =
                        multiply_with_precision(precision, coefficient.weight, polarization.weight);
                    let weight = canonical_f64(multiply_with_precision(
                        precision,
                        spatial_weight,
                        basis_weight,
                    ));
                    if !weight.is_finite() {
                        return Err(ModelContractError::NonFiniteReprojectionWeight.into());
                    }
                    if weight != 0.0 {
                        if prior_terms
                            .checked_add(stencil.len())
                            .and_then(|terms| terms.checked_add(1))
                            .is_none_or(|terms| terms > term_bound)
                        {
                            return Err(ModelLifecycleError::ReprojectionTermBoundExceeded {
                                terms: prior_terms.saturating_add(stencil.len()).saturating_add(1),
                                bound: term_bound,
                            });
                        }
                        stencil.push(WeightedSourceCell {
                            cell: ModelCell::new(
                                target.domain(),
                                coefficient.coefficient,
                                polarization.polarization,
                                [x, y],
                            ),
                            weight,
                        });
                    }
                }
            }
        }
    }
    stencil.sort_unstable_by_key(|weighted| {
        source
            .flat_index(weighted.cell)
            .expect("owner-derived source stencil remains in range")
    });
    Ok(Some(stencil))
}

fn basis_conversion_terms(
    source: &ModelSourceShape,
    target: &ModelSourceShape,
    target_coefficient: usize,
    precision: NumericPrecision,
) -> Result<Option<Vec<WeightedCoefficient>>, ModelLifecycleError> {
    let source_basis = source.coefficient_space().basis();
    let target_basis = target.coefficient_space().basis();
    if target_coefficient >= target.coefficients() {
        return Err(ModelLifecycleError::CellOutsideShape);
    }
    if source.spectral().output_frame() != target.spectral().output_frame() {
        return Err(ModelLifecycleError::UnsupportedBasisConversion);
    }

    match (
        polynomial_terms(source_basis),
        polynomial_terms(target_basis),
    ) {
        (Some(source_terms), Some(target_terms)) => {
            if target_terms < source_terms {
                return Err(ModelLifecycleError::UnsupportedBasisConversion);
            }
            if target_coefficient >= source_terms {
                return Ok(Some(Vec::new()));
            }
            let source_reference = spectral_reference_frequency(source)?;
            let target_reference = spectral_reference_frequency(target)?;
            let offset = (target_reference - source_reference) / source_reference;
            let scale = target_reference / source_reference;
            let mut terms = Vec::with_capacity(source_terms - target_coefficient);
            for source_coefficient in target_coefficient..source_terms {
                let weight = binomial(source_coefficient, target_coefficient)
                    * offset.powi((source_coefficient - target_coefficient) as i32)
                    * scale.powi(target_coefficient as i32);
                let weight = round_to_precision(precision, weight);
                if weight != 0.0 {
                    terms.push(WeightedCoefficient {
                        coefficient: source_coefficient,
                        weight,
                    });
                }
            }
            Ok(Some(terms))
        }
        (Some(source_terms), None) => {
            let frequency = target
                .spectral()
                .channel_centre_hz(target_coefficient)
                .ok_or(ModelLifecycleError::CellOutsideShape)?;
            let reference = spectral_reference_frequency(source)?;
            let x = round_to_precision(precision, (frequency - reference) / reference);
            Ok(Some(
                (0..source_terms)
                    .filter_map(|coefficient| {
                        let weight = round_to_precision(precision, x.powi(coefficient as i32));
                        (weight != 0.0).then_some(WeightedCoefficient {
                            coefficient,
                            weight,
                        })
                    })
                    .collect(),
            ))
        }
        (None, Some(target_terms)) => {
            if target_terms < source.coefficients() {
                return Err(ModelLifecycleError::UnsupportedBasisConversion);
            }
            if target_coefficient >= source.coefficients() {
                return Ok(Some(Vec::new()));
            }
            channel_to_polynomial_terms(source, target, target_coefficient, precision).map(Some)
        }
        (None, None) => {
            let frequency = target
                .spectral()
                .channel_centre_hz(target_coefficient)
                .ok_or(ModelLifecycleError::CellOutsideShape)?;
            channel_interpolation_terms(source, frequency, precision)
        }
    }
}

const fn polynomial_terms(basis: ReconstructionBasis) -> Option<usize> {
    match basis {
        ReconstructionBasis::Constant => Some(1),
        ReconstructionBasis::Taylor { terms }
        | ReconstructionBasis::TaylorViaChannelMajor { terms, .. } => Some(terms),
        ReconstructionBasis::ChannelLocal { .. } => None,
    }
}

fn spectral_reference_frequency(shape: &ModelSourceShape) -> Result<f64, ModelLifecycleError> {
    let first = shape
        .spectral()
        .channel_centre_hz(0)
        .ok_or(ModelLifecycleError::UnsupportedBasisConversion)?;
    let last = shape
        .spectral()
        .channel_centre_hz(shape.spectral().output_channels() - 1)
        .ok_or(ModelLifecycleError::UnsupportedBasisConversion)?;
    let reference = 0.5 * (first + last);
    if reference.is_finite() && reference > 0.0 {
        Ok(reference)
    } else {
        Err(ModelLifecycleError::UnsupportedBasisConversion)
    }
}

fn channel_to_polynomial_terms(
    source: &ModelSourceShape,
    target: &ModelSourceShape,
    target_coefficient: usize,
    precision: NumericPrecision,
) -> Result<Vec<WeightedCoefficient>, ModelLifecycleError> {
    let reference = spectral_reference_frequency(target)?;
    let mut abscissae = Vec::with_capacity(source.coefficients());
    for channel in 0..source.coefficients() {
        let frequency = source
            .spectral()
            .channel_centre_hz(channel)
            .ok_or(ModelLifecycleError::UnsupportedBasisConversion)?;
        abscissae.push((frequency - reference) / reference);
    }
    let mut result = Vec::with_capacity(source.coefficients());
    for source_coefficient in 0..source.coefficients() {
        let mut polynomial = vec![1.0];
        let mut denominator = 1.0;
        for (other, x) in abscissae.iter().copied().enumerate() {
            if other == source_coefficient {
                continue;
            }
            let mut next = vec![0.0; polynomial.len() + 1];
            for (degree, coefficient) in polynomial.iter().copied().enumerate() {
                next[degree] -= coefficient * x;
                next[degree + 1] += coefficient;
            }
            polynomial = next;
            denominator *= abscissae[source_coefficient] - x;
        }
        if !denominator.is_finite() || denominator == 0.0 {
            return Err(ModelLifecycleError::UnsupportedBasisConversion);
        }
        let weight = round_to_precision(precision, polynomial[target_coefficient] / denominator);
        if weight != 0.0 {
            result.push(WeightedCoefficient {
                coefficient: source_coefficient,
                weight,
            });
        }
    }
    Ok(result)
}

fn channel_interpolation_terms(
    source: &ModelSourceShape,
    frequency: f64,
    precision: NumericPrecision,
) -> Result<Option<Vec<WeightedCoefficient>>, ModelLifecycleError> {
    let frequencies = (0..source.coefficients())
        .map(|channel| {
            source
                .spectral()
                .channel_centre_hz(channel)
                .ok_or(ModelLifecycleError::UnsupportedBasisConversion)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let tolerance = frequency_tolerance(precision, frequency);
    if let Some(channel) = frequencies
        .iter()
        .position(|candidate| (frequency - candidate).abs() <= tolerance)
    {
        return Ok(Some(vec![WeightedCoefficient {
            coefficient: channel,
            weight: 1.0,
        }]));
    }
    for channel in 0..frequencies.len().saturating_sub(1) {
        let lower = frequencies[channel];
        let upper = frequencies[channel + 1];
        if (frequency - lower) * (frequency - upper) < 0.0 {
            let upper_weight = round_to_precision(precision, (frequency - lower) / (upper - lower));
            return Ok(Some(vec![
                WeightedCoefficient {
                    coefficient: channel,
                    weight: round_to_precision(precision, 1.0 - upper_weight),
                },
                WeightedCoefficient {
                    coefficient: channel + 1,
                    weight: upper_weight,
                },
            ]));
        }
    }
    Ok(None)
}

fn polarization_conversion_terms(
    source: &ModelSourceShape,
    target: &ModelSourceShape,
    target_polarization: usize,
) -> Result<Vec<WeightedPolarization>, ModelLifecycleError> {
    let source_coordinates = source.coefficient_space().polarization().coordinates();
    let target_coordinate = *target
        .coefficient_space()
        .polarization()
        .coordinates()
        .get(target_polarization)
        .ok_or(ModelLifecycleError::CellOutsideShape)?;
    if let Some(polarization) = source_coordinates
        .iter()
        .position(|coordinate| *coordinate == target_coordinate)
    {
        return Ok(vec![WeightedPolarization {
            polarization,
            weight: 1.0,
        }]);
    }
    let pair = |first, second, first_weight, second_weight| {
        let first = source_coordinates
            .iter()
            .position(|value| *value == first)?;
        let second = source_coordinates
            .iter()
            .position(|value| *value == second)?;
        Some(vec![
            WeightedPolarization {
                polarization: first,
                weight: first_weight,
            },
            WeightedPolarization {
                polarization: second,
                weight: second_weight,
            },
        ])
    };
    let terms = match target_coordinate {
        PolarizationCoordinate::StokesI => pair(
            PolarizationCoordinate::LinearXx,
            PolarizationCoordinate::LinearYy,
            0.5,
            0.5,
        )
        .or_else(|| {
            pair(
                PolarizationCoordinate::CircularRr,
                PolarizationCoordinate::CircularLl,
                0.5,
                0.5,
            )
        }),
        PolarizationCoordinate::StokesQ => pair(
            PolarizationCoordinate::LinearXx,
            PolarizationCoordinate::LinearYy,
            0.5,
            -0.5,
        ),
        PolarizationCoordinate::StokesV => pair(
            PolarizationCoordinate::CircularRr,
            PolarizationCoordinate::CircularLl,
            0.5,
            -0.5,
        ),
        PolarizationCoordinate::LinearXx => pair(
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesQ,
            1.0,
            1.0,
        ),
        PolarizationCoordinate::LinearYy => pair(
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesQ,
            1.0,
            -1.0,
        ),
        PolarizationCoordinate::CircularRr => pair(
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesV,
            1.0,
            1.0,
        ),
        PolarizationCoordinate::CircularLl => pair(
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesV,
            1.0,
            -1.0,
        ),
        PolarizationCoordinate::StokesU
        | PolarizationCoordinate::LinearXy
        | PolarizationCoordinate::LinearYx
        | PolarizationCoordinate::CircularRl
        | PolarizationCoordinate::CircularLr => None,
    };
    terms.ok_or(ModelLifecycleError::UnsupportedPolarizationConversion)
}

fn binomial(n: usize, k: usize) -> f64 {
    let k = k.min(n - k);
    (0..k).fold(1.0, |value, index| {
        value * (n - index) as f64 / (index + 1) as f64
    })
}

fn frequency_tolerance(precision: NumericPrecision, frequency: f64) -> f64 {
    coordinate_tolerance(precision) * frequency.abs().max(1.0)
}

fn round_to_precision(precision: NumericPrecision, value: f64) -> f64 {
    match precision {
        NumericPrecision::F32 => f64::from(value as f32),
        NumericPrecision::F64 => value,
    }
}

fn affine_source_pixel(
    source: DirectionCoordinateSpec,
    target: DirectionCoordinateSpec,
    target_pixel: [usize; 2],
) -> Result<[f64; 2], ModelLifecycleError> {
    if source.projection() != target.projection()
        || source.reference_direction() != target.reference_direction()
        || source.pole_deg() != target.pole_deg()
    {
        return Err(ModelLifecycleError::UnsupportedDirectionConversion);
    }
    let target_offset = [
        target_pixel[0] as f64 - target.reference_pixel()[0],
        target_pixel[1] as f64 - target.reference_pixel()[1],
    ];
    let target_pc = target.pc();
    let target_increment = target.increment_rad();
    let intermediate = [
        target_increment[0]
            * (target_pc[0][0] * target_offset[0] + target_pc[0][1] * target_offset[1]),
        target_increment[1]
            * (target_pc[1][0] * target_offset[0] + target_pc[1][1] * target_offset[1]),
    ];
    let source_increment = source.increment_rad();
    let source_intermediate = [
        intermediate[0] / source_increment[0],
        intermediate[1] / source_increment[1],
    ];
    let source_pc = source.pc();
    let determinant = source_pc[0][0] * source_pc[1][1] - source_pc[0][1] * source_pc[1][0];
    let source_offset = [
        (source_pc[1][1] * source_intermediate[0] - source_pc[0][1] * source_intermediate[1])
            / determinant,
        (-source_pc[1][0] * source_intermediate[0] + source_pc[0][0] * source_intermediate[1])
            / determinant,
    ];
    let reference = source.reference_pixel();
    let pixel = [
        canonical_f64(reference[0] + source_offset[0]),
        canonical_f64(reference[1] + source_offset[1]),
    ];
    if pixel.iter().all(|coordinate| coordinate.is_finite()) {
        Ok(pixel)
    } else {
        Err(ModelLifecycleError::UnsupportedDirectionConversion)
    }
}

fn axis_stencil(coordinate: f64, length: usize, tolerance: f64) -> Option<Vec<(usize, f64)>> {
    let maximum = (length - 1) as f64;
    if coordinate < -tolerance || coordinate > maximum + tolerance {
        return None;
    }
    let coordinate = coordinate.clamp(0.0, maximum);
    let nearest = coordinate.round();
    if (coordinate - nearest).abs() <= tolerance {
        return Some(vec![(nearest as usize, 1.0)]);
    }
    let lower = coordinate.floor() as usize;
    let upper = lower + 1;
    if upper >= length {
        return None;
    }
    let upper_weight = coordinate - lower as f64;
    Some(vec![(lower, 1.0 - upper_weight), (upper, upper_weight)])
}

const fn coordinate_tolerance(precision: NumericPrecision) -> f64 {
    match precision {
        NumericPrecision::F32 => 64.0 * f32::EPSILON as f64,
        NumericPrecision::F64 => 64.0 * f64::EPSILON,
    }
}

fn normalized_weight_sum(precision: NumericPrecision, sum: f64) -> bool {
    let tolerance = match precision {
        NumericPrecision::F32 => 64.0 * f64::from(f32::EPSILON),
        NumericPrecision::F64 => 64.0 * f64::EPSILON,
    };
    sum.is_finite() && (sum - 1.0).abs() <= tolerance
}

pub(crate) fn add_with_precision(precision: NumericPrecision, left: f64, right: f64) -> f64 {
    match precision {
        NumericPrecision::F32 => f64::from((left as f32) + (right as f32)),
        NumericPrecision::F64 => left + right,
    }
}

fn multiply_with_precision(precision: NumericPrecision, left: f64, right: f64) -> f64 {
    match precision {
        NumericPrecision::F32 => f64::from((left as f32) * (right as f32)),
        NumericPrecision::F64 => left * right,
    }
}

fn canonical_f64(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}
