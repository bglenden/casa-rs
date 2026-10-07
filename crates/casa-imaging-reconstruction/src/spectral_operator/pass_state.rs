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

use casa_imaging_model::{CompiledProblem, ReconstructionBasis, SpectralWcs};
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

enum DomainState {
    Empty,
    Coupled(SpectralDomainPrimitives),
    Channels {
        domain: StoredChannelNormalDomain,
        refresh: bool,
    },
}

/// A normal state being assembled domain by domain and, for a
/// channel-local basis, channel range by channel range in order.
pub struct PassNormalState {
    basis: SpectralBasisPlan,
    total_channels: usize,
    model: ModelGenerationId,
    storage: NormalStoragePlan,
    domains: Vec<DomainState>,
    roles: Vec<casa_imaging_model::ImageDomainRole>,
    previous: Option<FinalNormalState>,
}

impl PassNormalState {
    /// An empty state for the initial pass of `problem`, whose images were
    /// formed with model generation `model`.
    pub fn initial(
        problem: &CompiledProblem,
        model: ModelGenerationId,
        storage: NormalStoragePlan,
    ) -> Result<Self, SpectralOperatorError> {
        let roles = problem
            .geometry()
            .domains()
            .iter()
            .map(|domain| domain.role().clone())
            .collect::<Vec<_>>();
        Ok(Self {
            basis: basis_plan(problem)?,
            total_channels: problem.geometry().spectral().output_channels(),
            model,
            storage,
            domains: roles.iter().map(|_| DomainState::Empty).collect(),
            roles,
            previous: None,
        })
    }

    /// A residual refresh of `previous`, formed with model generation
    /// `model`: the PSF, `sumwt` and validity carry over; every residual
    /// plane must be appended again.
    pub fn refresh(
        problem: &CompiledProblem,
        previous: FinalNormalState,
        model: ModelGenerationId,
        storage: NormalStoragePlan,
    ) -> Result<Self, SpectralOperatorError> {
        let mut state = Self::initial(problem, model, storage)?;
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
    pub fn append(&mut self, images: PassImages) -> Result<(), SpectralOperatorError> {
        let index = images.domain;
        let state = std::mem::replace(
            self.domains
                .get_mut(index)
                .ok_or(SpectralOperatorError::ProblemMismatch)?,
            DomainState::Empty,
        );
        self.domains[index] = match (state, self.basis) {
            (DomainState::Empty, SpectralBasisPlan::ChannelLocal) => {
                let primitives = self.channel_primitives(images)?;
                DomainState::Channels {
                    domain: StoredChannelNormalDomain::begin(primitives, &self.storage)?,
                    refresh: false,
                }
            }
            (DomainState::Empty, _) => DomainState::Coupled(self.coupled_primitives(images)?),
            (
                DomainState::Channels {
                    mut domain,
                    refresh: false,
                },
                _,
            ) => {
                domain.append(self.channel_primitives(images)?)?;
                DomainState::Channels {
                    domain,
                    refresh: false,
                }
            }
            (
                DomainState::Channels {
                    mut domain,
                    refresh: true,
                },
                _,
            ) => {
                if images.psf.is_some() {
                    return Err(SpectralOperatorError::ProblemMismatch);
                }
                domain.append_residual_planes(
                    images.channels.clone(),
                    images.shape,
                    self.model,
                    &images.residual,
                )?;
                DomainState::Channels {
                    domain,
                    refresh: true,
                }
            }
            (DomainState::Coupled(mut domain), _) => {
                let primitives = &mut domain.primitives;
                if images.psf.is_some()
                    || images.shape != primitives.shape
                    || images.residual.len() != primitives.dirty.len()
                {
                    return Err(SpectralOperatorError::ProblemMismatch);
                }
                primitives.dirty = widen(&images.residual);
                primitives.residual_model = Some(self.model);
                DomainState::Coupled(domain)
            }
        };
        Ok(())
    }

    /// The completed state, tagged with the weighting generation and the
    /// traversal counts it came from.
    pub fn finish(
        self,
        problem: &CompiledProblem,
        weighting: WeightingGenerationId,
        samples: u64,
        blocks: u64,
    ) -> Result<CompleteDataNormalState, SpectralOperatorError> {
        let catalog = match self.basis {
            SpectralBasisPlan::ChannelLocal => SpectralPrimitiveCatalog::UnnormalizedChannelSlabV1,
            SpectralBasisPlan::Polynomial(plan)
            | SpectralBasisPlan::TaylorViaChannelMajor(plan)
                if plan.coefficient_term_count() > 1 =>
            {
                SpectralPrimitiveCatalog::UnnormalizedTaylorBlockV1
            }
            SpectralBasisPlan::Polynomial(_) | SpectralBasisPlan::TaylorViaChannelMajor(_) => {
                SpectralPrimitiveCatalog::UnnormalizedPlaneV1
            }
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
                problem: problem.problem_id(),
                geometry: problem.geometry().geometry_id(),
                numerics: problem.numerics_id(),
                weighting_commitment: problem.weighting().commitment_id(),
                weighting_generation: weighting,
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
                    resident_start: channels.start,
                    resident_end: channels.end,
                },
                basis: SpectralBasisPlan::ChannelLocal,
                polarizations,
                dirty: Box::new([]),
                cube_real: Some(CubeRealFields {
                    dirty: residual.into_boxed_slice(),
                    invariant_dirty: None,
                    psf: psf.into_boxed_slice(),
                }),
                invariant_dirty: None,
                psf: Box::new([]),
                clark_workspace: Mutex::new(None),
                sensitivity: Box::new([]),
                primary_beam_weighted_sum: None,
                published_sum_weights: sum_weights.clone().into_boxed_slice(),
                sum_weights: sum_weights.into_boxed_slice(),
                validity,
                major_cycle_residual: None,
                major_cycle_residual_promoted: true,
                residual_model: Some(self.model),
                #[cfg(test)]
                measurements: super::SpectralOperatorMeasurements::default(),
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
            resident_start: 0,
            resident_end: self.total_channels,
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
                invariant_dirty: None,
                psf: widen(&psf),
                clark_workspace: Mutex::new(None),
                sensitivity,
                primary_beam_weighted_sum: None,
                published_sum_weights: sum_weights.clone().into_boxed_slice(),
                sum_weights: sum_weights.into_boxed_slice(),
                validity,
                major_cycle_residual: None,
                major_cycle_residual_promoted: true,
                residual_model: Some(self.model),
                #[cfg(test)]
                measurements: super::SpectralOperatorMeasurements::default(),
            },
        ))
    }
}

/// CASA marks a plane whose accumulated weight is zero as blank; nothing
/// downstream distinguishes a blank plane from one no sample reached.
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
/// image's reference frequency.
fn basis_plan(problem: &CompiledProblem) -> Result<SpectralBasisPlan, SpectralOperatorError> {
    let reference_hz = || match problem.geometry().spectral().wcs() {
        SpectralWcs::Linear {
            reference_frequency_hz,
            ..
        } => Ok(*reference_frequency_hz),
        SpectralWcs::Tabular { .. } => Err(SpectralOperatorError::UnsupportedProblem),
    };
    let invalid = |_| SpectralOperatorError::UnsupportedProblem;
    Ok(match problem.reconstruction().basis() {
        ReconstructionBasis::ChannelLocal { .. } => SpectralBasisPlan::ChannelLocal,
        ReconstructionBasis::Constant => SpectralBasisPlan::Polynomial(
            BlockNormalPlan::constant(reference_hz()?).map_err(invalid)?,
        ),
        ReconstructionBasis::Taylor { terms } => SpectralBasisPlan::Polynomial(
            BlockNormalPlan::taylor(reference_hz()?, terms).map_err(invalid)?,
        ),
        ReconstructionBasis::TaylorViaChannelMajor { terms, .. } => {
            SpectralBasisPlan::TaylorViaChannelMajor(
                BlockNormalPlan::taylor(reference_hz()?, terms).map_err(invalid)?,
            )
        }
    })
}
