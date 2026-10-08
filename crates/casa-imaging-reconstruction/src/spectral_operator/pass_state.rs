// SPDX-License-Identifier: LGPL-3.0-or-later
//! Normal states assembled from major-cycle pass images.
//!
//! A major-cycle pass yields unnormalised dirty or residual images, PSF
//! moments and `sumwt` per output channel. [`PassNormalState`] writes them
//! into the normal-state representation the minor cycle and products read:
//! resident coefficient families for the constant and Taylor bases, paged
//! channel planes for a channel-local basis. A refresh keeps the previous
//! state's PSF, `sumwt` and validity and replaces only the residual.

use std::ops::Range;
use std::sync::Mutex;

use casa_imaging_model::{
    CompiledGeometryId, CompiledProblem, CompiledProblemId, NumericsContractId,
    ReconstructionBasis, SpectralWcs, WeightingCommitmentId,
};
use num_complex::Complex64;

use super::normal_storage::{
    CompleteDataNormalState, NormalStatePrimitives, NormalStoragePlan, StoredChannelNormalDomain,
};
use super::{
    CompleteDataOwnerCompletion, CubeRealFields, SpectralBasisPlan, SpectralChannelValidity,
    SpectralDomainPrimitives, SpectralOperatorError, SpectralOperatorPrimitives,
    SpectralPrimitiveCatalog, SpectralPrimitiveDomains, SpectralSlabPlan, checked_cells,
};
use crate::block_normal::BlockNormalPlan;
use crate::{FinalNormalState, ModelGenerationId, WeightingGenerationId, WeightingReplayId};

/// Unnormalised images of one image domain from one pass over a contiguous
/// range of output channels.
///
/// Planes are x-major (`index = x · height + y`), the layout every
/// normal-state reader indexes, and ordered `[channel][term][pol]`: a
/// channel-local basis has one term per channel, a constant or Taylor basis
/// one channel.
#[derive(Clone, Debug, PartialEq)]
pub struct PassImages {
    /// Canonical image-domain ordinal.
    pub domain: usize,
    /// `[width, height]` of every plane.
    pub shape: [usize; 2],
    /// Output channels held.
    pub channels: Range<usize>,
    /// Requested polarizations per term.
    pub polarizations: usize,
    /// Dirty or residual planes, one per data term.
    pub residual: Vec<f32>,
    /// PSF moment planes on an initial pass; `None` on a residual pass.
    pub psf: Option<Vec<f32>>,
    /// `sumwt` per PSF-moment plane on an initial pass; empty otherwise.
    pub sum_weights: Vec<f64>,
}

#[expect(
    clippy::large_enum_variant,
    reason = "one state per image domain, replaced in place; boxing saves nothing"
)]
enum DomainState {
    Empty,
    Coupled(SpectralDomainPrimitives),
    Channels {
        domain: StoredChannelNormalDomain,
        refresh: bool,
    },
}

/// The problem, numerics and weighting a normal state is formed for; a
/// finished state carries them as its completion.
#[derive(Clone, Copy, Debug)]
struct Formed {
    problem: CompiledProblemId,
    geometry: CompiledGeometryId,
    numerics: NumericsContractId,
    weighting_commitment: WeightingCommitmentId,
    weighting_generation: WeightingGenerationId,
}

/// A normal state being assembled domain by domain and, for a
/// channel-local basis, channel range by channel range in order.
pub struct PassNormalState {
    formed: Formed,
    basis: SpectralBasisPlan,
    total_channels: usize,
    polarizations: usize,
    model: ModelGenerationId,
    storage: NormalStoragePlan,
    domains: Vec<DomainState>,
    roles: Vec<casa_imaging_model::ImageDomainRole>,
    shapes: Vec<[usize; 2]>,
    /// The refreshed channel-local state, retired once the refresh finishes.
    previous: Option<FinalNormalState>,
    /// Samples the refreshed state placed; a refresh must place as many.
    refreshed_samples: Option<u64>,
}

impl PassNormalState {
    /// An empty state for the initial pass of `problem` with imaging
    /// weights `weighting`, whose images were formed with model generation
    /// `model`.
    pub fn initial(
        problem: &CompiledProblem,
        weighting: WeightingGenerationId,
        model: ModelGenerationId,
        storage: NormalStoragePlan,
    ) -> Result<Self, SpectralOperatorError> {
        let domains = problem.geometry().domains();
        Ok(Self {
            formed: Formed {
                problem: problem.problem_id(),
                geometry: problem.geometry().geometry_id(),
                numerics: problem.numerics_id(),
                weighting_commitment: problem.weighting().commitment_id(),
                weighting_generation: weighting,
            },
            basis: basis_plan(problem)?,
            total_channels: problem.geometry().spectral().output_channels(),
            polarizations: problem.reconstruction().polarization().coordinates().len(),
            model,
            storage,
            domains: domains.iter().map(|_| DomainState::Empty).collect(),
            roles: domains.iter().map(|domain| domain.role().clone()).collect(),
            shapes: domains
                .iter()
                .map(|domain| domain.shape().pixels())
                .collect(),
            previous: None,
            refreshed_samples: None,
        })
    }

    /// A residual refresh of `previous`, formed with model generation
    /// `model` and `previous`'s imaging weights: the PSF, `sumwt` and
    /// validity carry over; every residual plane must be appended again,
    /// and [`Self::finish`] requires the refresh to have placed the samples
    /// `previous` placed.
    ///
    /// `previous` must be a state of the same problem and weighting
    /// commitment.
    pub fn refresh(
        problem: &CompiledProblem,
        previous: FinalNormalState,
        model: ModelGenerationId,
        storage: NormalStoragePlan,
    ) -> Result<Self, SpectralOperatorError> {
        if previous.problem_id() != problem.problem_id()
            || previous.weighting_commitment_id() != problem.weighting().commitment_id()
        {
            return Err(SpectralOperatorError::ReusableNormalStateMismatch);
        }
        let mut state = Self::initial(problem, previous.weighting_generation(), model, storage)?;
        state.refreshed_samples = Some(previous.sample_count());
        match previous.primitives() {
            NormalStatePrimitives::ChannelLocal(domains) => {
                for (target, domain) in state.domains.iter_mut().zip(domains.iter()) {
                    *target = DomainState::Channels {
                        domain: domain.refresh(model, &state.storage)?,
                        refresh: true,
                    };
                }
                state.previous = Some(previous);
            }
            NormalStatePrimitives::Coupled(_) => {
                for (target, domain) in state
                    .domains
                    .iter_mut()
                    .zip(previous.into_primitives().into_coupled()?)
                {
                    *target = DomainState::Coupled(domain);
                }
            }
        }
        Ok(state)
    }

    /// Add one pass's images of one domain.
    ///
    /// A non-finite image value or `sumwt` is a generated non-finite value,
    /// rejected under every [`casa_imaging_model::FiniteValuePolicy`]: the
    /// policies differ only on non-finite inputs, which the source flags or
    /// rejects before gridding. Nothing is changed when an append fails
    /// validation.
    pub fn append(&mut self, images: PassImages) -> Result<(), SpectralOperatorError> {
        let index = images.domain;
        if self.shapes.get(index) != Some(&images.shape)
            || images.polarizations != self.polarizations
        {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        if !all_finite(&images) {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        let model = self.model;
        if let DomainState::Channels {
            domain,
            refresh: true,
        } = &mut self.domains[index]
        {
            if images.psf.is_some() {
                return Err(SpectralOperatorError::ProblemMismatch);
            }
            return domain.append_residual_planes(
                images.channels.clone(),
                images.shape,
                model,
                &images.residual,
            );
        }
        if let DomainState::Coupled(domain) = &mut self.domains[index] {
            let primitives = &mut domain.primitives;
            if images.psf.is_some() || images.residual.len() != primitives.dirty.len() {
                return Err(SpectralOperatorError::ProblemMismatch);
            }
            primitives.dirty = widen(&images.residual);
            primitives.residual_model = Some(model);
            return Ok(());
        }
        if self.basis != SpectralBasisPlan::ChannelLocal {
            self.domains[index] = DomainState::Coupled(self.coupled_primitives(images)?);
            return Ok(());
        }
        let primitives = self.channel_primitives(images)?;
        match &mut self.domains[index] {
            DomainState::Channels { domain, .. } => domain.append(primitives),
            slot => {
                *slot = DomainState::Channels {
                    domain: StoredChannelNormalDomain::begin(primitives, &self.storage)?,
                    refresh: false,
                };
                Ok(())
            }
        }
    }

    /// The completed state, tagged with the problem and weighting it was
    /// formed for and the traversal counts it came from. A refresh must have
    /// placed the samples the refreshed state placed.
    pub fn finish(
        self,
        samples: u64,
        blocks: u64,
    ) -> Result<CompleteDataNormalState, SpectralOperatorError> {
        if self
            .refreshed_samples
            .is_some_and(|previous| previous != samples)
        {
            return Err(SpectralOperatorError::ReusableNormalStateMismatch);
        }
        let catalog = match self.basis {
            SpectralBasisPlan::ChannelLocal => SpectralPrimitiveCatalog::UnnormalizedChannelSlabV1,
            SpectralBasisPlan::Polynomial(plan) if plan.coefficient_term_count() > 1 => {
                SpectralPrimitiveCatalog::UnnormalizedTaylorBlockV1
            }
            SpectralBasisPlan::Polynomial(_) => SpectralPrimitiveCatalog::UnnormalizedPlaneV1,
        };
        let primitives = if self.basis == SpectralBasisPlan::ChannelLocal {
            NormalStatePrimitives::ChannelLocal(
                self.domains
                    .into_iter()
                    .map(|state| match state {
                        DomainState::Channels { domain, .. } if domain.is_complete() => Ok(domain),
                        _ => Err(SpectralOperatorError::IncompleteCoverage),
                    })
                    .collect::<Result<Box<[_]>, _>>()?,
            )
        } else {
            NormalStatePrimitives::Coupled(SpectralPrimitiveDomains {
                domains: self
                    .domains
                    .into_iter()
                    .map(|state| match state {
                        DomainState::Coupled(domain)
                            if domain.primitives.residual_model == Some(self.model) =>
                        {
                            Ok(domain)
                        }
                        _ => Err(SpectralOperatorError::IncompleteCoverage),
                    })
                    .collect::<Result<Box<[_]>, _>>()?,
            })
        };
        if let Some(previous) = self.previous {
            previous.retire_obsolete()?;
        }
        Ok(CompleteDataNormalState {
            primitives,
            completion: CompleteDataOwnerCompletion {
                problem: self.formed.problem,
                geometry: self.formed.geometry,
                numerics: self.formed.numerics,
                weighting_commitment: self.formed.weighting_commitment,
                weighting_generation: self.formed.weighting_generation,
                replay: WeightingReplayId::next(),
                primitives: catalog,
                sample_count: samples,
                block_count: blocks,
            },
        })
    }

    fn channel_primitives(
        &self,
        images: PassImages,
    ) -> Result<SpectralDomainPrimitives, SpectralOperatorError> {
        let PassImages {
            domain,
            shape,
            channels,
            polarizations,
            residual,
            psf,
            sum_weights,
        } = images;
        let planes = channels.len() * polarizations;
        let values = planes * checked_cells(shape)?;
        let psf = psf.ok_or(SpectralOperatorError::ProblemMismatch)?;
        if channels.is_empty()
            || channels.end > self.total_channels
            || residual.len() != values
            || psf.len() != values
            || sum_weights.len() != planes
        {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        let validity = sum_weights.iter().map(|weight| validity(*weight)).collect();
        Ok(SpectralDomainPrimitives::new(
            domain,
            self.roles[domain].clone(),
            SpectralOperatorPrimitives {
                shape,
                slab: SpectralSlabPlan {
                    total_channels: self.total_channels,
                    core_start: channels.start,
                    core_end: channels.end,
                },
                basis: SpectralBasisPlan::ChannelLocal,
                polarizations,
                dirty: Box::new([]),
                cube_real: Some(CubeRealFields {
                    dirty: residual.into_boxed_slice(),
                    psf: psf.into_boxed_slice(),
                }),
                psf: Box::new([]),
                clark_workspace: Mutex::new(None),
                sensitivity: Box::new([]),
                published_sum_weights: sum_weights.clone().into_boxed_slice(),
                sum_weights: sum_weights.into_boxed_slice(),
                validity,
                residual_model: Some(self.model),
            },
        ))
    }

    fn coupled_primitives(
        &self,
        images: PassImages,
    ) -> Result<SpectralDomainPrimitives, SpectralOperatorError> {
        let PassImages {
            domain,
            shape,
            channels,
            polarizations,
            residual,
            psf,
            sum_weights,
        } = images;
        let slab = SpectralSlabPlan {
            total_channels: self.total_channels,
            core_start: 0,
            core_end: self.total_channels,
        };
        let cells = checked_cells(shape)?;
        let terms = self.basis.coefficient_terms(slab);
        let moments = self.basis.normal_moments(slab);
        let psf = psf.ok_or(SpectralOperatorError::ProblemMismatch)?;
        if channels != (0..self.total_channels)
            || residual.len() != terms * polarizations * cells
            || psf.len() != moments * polarizations * cells
            || sum_weights.len() != moments * polarizations
        {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        let sensitivity = sum_weights
            .iter()
            .flat_map(|weight| std::iter::repeat_n(*weight, cells))
            .collect();
        let validity = sum_weights[..polarizations]
            .iter()
            .map(|weight| validity(*weight))
            .collect();
        Ok(SpectralDomainPrimitives::new(
            domain,
            self.roles[domain].clone(),
            SpectralOperatorPrimitives {
                shape,
                slab,
                basis: self.basis,
                polarizations,
                dirty: widen(&residual),
                cube_real: None,
                psf: widen(&psf),
                clark_workspace: Mutex::new(None),
                sensitivity,
                published_sum_weights: sum_weights.clone().into_boxed_slice(),
                sum_weights: sum_weights.into_boxed_slice(),
                validity,
                residual_model: Some(self.model),
            },
        ))
    }
}

/// Whether every image value and `sumwt` of `images` is finite.
fn all_finite(images: &PassImages) -> bool {
    images.residual.iter().all(|value| value.is_finite())
        && images
            .psf
            .as_ref()
            .is_none_or(|psf| psf.iter().all(|value| value.is_finite()))
        && images.sum_weights.iter().all(|weight| weight.is_finite())
}

/// A plane whose accumulated weight is zero is unmapped whether or not
/// samples reached it: nothing downstream distinguishes the two cases.
fn validity(sum_weight: f64) -> SpectralChannelValidity {
    if sum_weight > 0.0 {
        SpectralChannelValidity::Valid
    } else {
        SpectralChannelValidity::Unmapped
    }
}

fn widen(values: &[f32]) -> Box<[Complex64]> {
    values
        .iter()
        .map(|value| Complex64::new(f64::from(*value), 0.0))
        .collect()
}

/// The coefficient basis of `problem`; a Taylor basis expands about the
/// image's reference frequency. Taylor terms via channel cubes (CASA `mvc`)
/// keep `Σ W² xᵗ` as their published weights, which no pass forms until the
/// primary-beam operators are installed (IF-3), so they are unsupported.
fn basis_plan(problem: &CompiledProblem) -> Result<SpectralBasisPlan, SpectralOperatorError> {
    let reference_hz = || match problem.geometry().spectral().wcs() {
        SpectralWcs::Linear {
            reference_frequency_hz,
            ..
        } => Ok(*reference_frequency_hz),
        SpectralWcs::Tabular { .. } => Err(SpectralOperatorError::UnsupportedProblem),
    };
    Ok(match problem.reconstruction().basis() {
        ReconstructionBasis::ChannelLocal { .. } => SpectralBasisPlan::ChannelLocal,
        ReconstructionBasis::Constant => {
            SpectralBasisPlan::Polynomial(BlockNormalPlan::constant(reference_hz()?))
        }
        ReconstructionBasis::Taylor { terms } => SpectralBasisPlan::Polynomial(
            BlockNormalPlan::taylor(reference_hz()?, terms)
                .ok_or(SpectralOperatorError::ResidencyOverflow)?,
        ),
        ReconstructionBasis::TaylorViaChannelMajor { .. } => {
            return Err(SpectralOperatorError::UnsupportedProblem);
        }
    })
}

#[cfg(test)]
impl SpectralOperatorPrimitives {
    /// Coupled one-domain Stokes-I primitives over externally captured
    /// planes, for lib tests whose sensitivity or published weights no pass
    /// forms. `response` is `(sensitivity plane, normal weight, published
    /// weight)`; without it both weights and the sensitivity are one.
    pub(crate) fn native_taylor_fixture(
        problem: &CompiledProblem,
        residual_model: ModelGenerationId,
        dirty: Box<[Complex64]>,
        psf: Box<[Complex64]>,
        response: Option<(Vec<f64>, f64, f64)>,
    ) -> Self {
        let basis = basis_plan(problem).expect("fixture coefficient basis");
        let total_channels = problem.geometry().spectral().output_channels();
        let slab = SpectralSlabPlan {
            total_channels,
            core_start: 0,
            core_end: total_channels,
        };
        let shape = problem.geometry().domains()[0].shape().pixels();
        let cells = shape[0] * shape[1];
        let moments = basis.normal_moments(slab);
        assert_eq!(dirty.len(), basis.coefficient_terms(slab) * cells);
        assert_eq!(psf.len(), moments * cells);
        let (sensitivity, normal_weight, published_weight) =
            response.unwrap_or_else(|| (vec![1.0; cells], 1.0, 1.0));
        assert_eq!(sensitivity.len(), cells);
        Self {
            shape,
            slab,
            basis,
            polarizations: 1,
            dirty,
            cube_real: None,
            psf,
            clark_workspace: Mutex::new(None),
            sensitivity: sensitivity.repeat(moments).into_boxed_slice(),
            sum_weights: vec![normal_weight; moments].into_boxed_slice(),
            published_sum_weights: vec![published_weight; moments].into_boxed_slice(),
            validity: vec![SpectralChannelValidity::Valid].into_boxed_slice(),
            residual_model: Some(residual_model),
        }
    }
}
