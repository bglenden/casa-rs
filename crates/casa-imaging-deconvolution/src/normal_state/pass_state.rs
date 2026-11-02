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

use casa_imaging_model::{CompiledProblem, ReconstructionBasis, SpectralWcs};
use num_complex::Complex64;

use super::storage::{NormalStatePrimitives, NormalStoragePlan, StoredChannelNormalDomain};
use super::{
    CubeRealFields, NormalPrimitives, NormalStateError, SpectralBasisPlan, SpectralChannelValidity,
    SpectralDomainPrimitives, SpectralPrimitiveDomains, SpectralSlabPlan, checked_cells,
};
use crate::FinalNormalState;

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
    /// `sumwt` of the PSF gridding per PSF-moment plane on an initial pass;
    /// empty otherwise. It normalises the PSF and weight planes.
    pub sum_weights: Vec<f64>,
    /// The `.sumwt` CASA publishes per PSF-moment plane on an initial pass;
    /// empty otherwise. It normalises the residual
    /// (`SIImageStore::divideResidualByWeight`). The pass that assembles
    /// the images chooses it as CASA does: the PSF gridding's sum, kept
    /// through the data passes (`FTMachine::finalizeToSkyNew`), or for a
    /// Taylor basis the data gridding's per data term
    /// (`MultiTermFTNew::finalizeToSkyNew`); it equals `sum_weights`
    /// otherwise.
    pub published_sum_weights: Vec<f64>,
    /// The gridded sensitivity (weight) planes of an initial pass with a
    /// kernel set that has weight taps (mosaic, AW), `[channel][pol]`;
    /// `None` when the sensitivity is the scalar `sumwt`.
    pub weight: Option<Vec<f32>>,
}

// `allow`, not `expect`: whether the variants differ enough to trip the
// lint depends on the target's lock and allocator layouts.
#[allow(
    clippy::large_enum_variant,
    reason = "one state per image domain, replaced in place; boxing saves nothing"
)]
enum DomainState {
    Empty,
    /// A constant or Taylor domain. A refresh carries the previous
    /// primitives with `refreshed` false until the pass appends the domain's
    /// residual again.
    Coupled {
        domain: SpectralDomainPrimitives,
        refreshed: bool,
    },
    Channels {
        domain: StoredChannelNormalDomain,
        refresh: bool,
    },
}

/// A normal state being assembled domain by domain and, for a
/// channel-local basis, channel range by channel range in order.
pub(crate) struct PassNormalState {
    basis: SpectralBasisPlan,
    total_channels: usize,
    polarizations: usize,
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
    /// An empty state for the initial pass of `problem`.
    pub(crate) fn initial(
        problem: &CompiledProblem,
        storage: NormalStoragePlan,
    ) -> Result<Self, NormalStateError> {
        let domains = problem.geometry().domains();
        Ok(Self {
            basis: basis_plan(problem)?,
            total_channels: problem.geometry().spectral().output_channels(),
            polarizations: problem.reconstruction().polarization().coordinates().len(),
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

    /// A residual refresh of `previous`, with `previous`'s imaging weights:
    /// the PSF, `sumwt` and validity carry over; every residual plane must be
    /// appended again, and [`Self::finish`] requires the refresh to have
    /// placed the samples `previous` placed.
    pub(crate) fn refresh(
        problem: &CompiledProblem,
        previous: FinalNormalState,
        storage: NormalStoragePlan,
    ) -> Result<Self, NormalStateError> {
        let mut state = Self::initial(problem, storage)?;
        state.refreshed_samples = Some(previous.sample_count());
        match previous.primitives() {
            NormalStatePrimitives::ChannelLocal(domains) => {
                for (target, domain) in state.domains.iter_mut().zip(domains.iter()) {
                    *target = DomainState::Channels {
                        domain: domain.refresh(&state.storage)?,
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
                    *target = DomainState::Coupled {
                        domain,
                        refreshed: false,
                    };
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
    pub(crate) fn append(&mut self, images: PassImages) -> Result<(), NormalStateError> {
        let index = images.domain;
        if self.shapes.get(index) != Some(&images.shape)
            || images.polarizations != self.polarizations
        {
            return Err(NormalStateError::ProblemMismatch);
        }
        if !all_finite(&images) {
            return Err(NormalStateError::GeneratedNonfinite);
        }
        if let DomainState::Channels {
            domain,
            refresh: true,
        } = &mut self.domains[index]
        {
            if images.psf.is_some() {
                return Err(NormalStateError::ProblemMismatch);
            }
            return domain.append_residual_planes(
                images.channels.clone(),
                images.shape,
                &images.residual,
            );
        }
        if let DomainState::Coupled { domain, refreshed } = &mut self.domains[index] {
            let primitives = &mut domain.primitives;
            if images.psf.is_some() || images.residual.len() != primitives.dirty.len() {
                return Err(NormalStateError::ProblemMismatch);
            }
            primitives.dirty = widen(&images.residual);
            *refreshed = true;
            return Ok(());
        }
        if self.basis != SpectralBasisPlan::ChannelLocal {
            self.domains[index] = DomainState::Coupled {
                domain: self.coupled_primitives(images)?,
                refreshed: true,
            };
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

    /// The completed state of a pass that placed `samples`. Every domain's
    /// images must be complete, and a refresh must have formed every
    /// residual again and placed the samples the refreshed state placed.
    pub(crate) fn finish(self, samples: u64) -> Result<NormalStatePrimitives, NormalStateError> {
        if self
            .refreshed_samples
            .is_some_and(|previous| previous != samples)
        {
            return Err(NormalStateError::ReusableNormalStateMismatch);
        }
        let primitives = if self.basis == SpectralBasisPlan::ChannelLocal {
            NormalStatePrimitives::ChannelLocal(
                self.domains
                    .into_iter()
                    .map(|state| match state {
                        DomainState::Channels { domain, .. } if domain.is_complete() => Ok(domain),
                        _ => Err(NormalStateError::IncompleteCoverage),
                    })
                    .collect::<Result<Box<[_]>, _>>()?,
            )
        } else {
            NormalStatePrimitives::Coupled(SpectralPrimitiveDomains {
                domains: self
                    .domains
                    .into_iter()
                    .map(|state| match state {
                        DomainState::Coupled {
                            domain,
                            refreshed: true,
                        } => Ok(domain),
                        _ => Err(NormalStateError::IncompleteCoverage),
                    })
                    .collect::<Result<Box<[_]>, _>>()?,
            })
        };
        if let Some(previous) = self.previous {
            previous.retire_obsolete()?;
        }
        Ok(primitives)
    }

    fn channel_primitives(
        &self,
        images: PassImages,
    ) -> Result<SpectralDomainPrimitives, NormalStateError> {
        let PassImages {
            domain,
            shape,
            channels,
            polarizations,
            residual,
            psf,
            sum_weights,
            published_sum_weights,
            weight,
        } = images;
        let planes = channels.len() * polarizations;
        let values = planes * checked_cells(shape)?;
        let psf = psf.ok_or(NormalStateError::ProblemMismatch)?;
        if channels.is_empty()
            || channels.end > self.total_channels
            || residual.len() != values
            || psf.len() != values
            || sum_weights.len() != planes
            || published_sum_weights.len() != planes
            || weight.as_ref().is_some_and(|weight| weight.len() != values)
        {
            return Err(NormalStateError::InvalidSlab);
        }
        let validity = sum_weights
            .iter()
            .zip(&published_sum_weights)
            .map(|(weight, published)| validity(*weight, *published))
            .collect();
        Ok(SpectralDomainPrimitives::new(
            domain,
            self.roles[domain].clone(),
            NormalPrimitives {
                shape,
                slab: SpectralSlabPlan {
                    total_channels: self.total_channels,
                    core_start: channels.start,
                    core_end: channels.end,
                },
                basis: SpectralBasisPlan::ChannelLocal,
                polarizations,
                dirty: Box::new([]),
                // A set with weight taps (mosaic) keeps each plane's gridded
                // weight image as it is, in f32; otherwise the sensitivity is
                // the plane's `sumwt`.
                cube_real: Some(CubeRealFields {
                    dirty: residual.into_boxed_slice(),
                    psf: psf.into_boxed_slice(),
                    sensitivity: weight.map(Vec::into_boxed_slice),
                }),
                psf: Box::new([]),
                sensitivity: Box::new([]),
                published_sum_weights: published_sum_weights.into_boxed_slice(),
                sum_weights: sum_weights.into_boxed_slice(),
                validity,
            },
        ))
    }

    fn coupled_primitives(
        &self,
        images: PassImages,
    ) -> Result<SpectralDomainPrimitives, NormalStateError> {
        let PassImages {
            domain,
            shape,
            channels,
            polarizations,
            residual,
            psf,
            sum_weights,
            published_sum_weights,
            weight,
        } = images;
        let slab = SpectralSlabPlan {
            total_channels: self.total_channels,
            core_start: 0,
            core_end: self.total_channels,
        };
        let cells = checked_cells(shape)?;
        let terms = self.basis.coefficient_terms(slab);
        let moments = self.basis.normal_moments(slab);
        let psf = psf.ok_or(NormalStateError::ProblemMismatch)?;
        if channels != (0..self.total_channels)
            || residual.len() != terms * polarizations * cells
            || psf.len() != moments * polarizations * cells
            || sum_weights.len() != moments * polarizations
            || published_sum_weights.len() != moments * polarizations
            || weight
                .as_ref()
                .is_some_and(|weight| weight.len() != moments * polarizations * cells)
        {
            return Err(NormalStateError::InvalidSlab);
        }
        // The sensitivity per (moment, pol) plane: the gridded weight image
        // of the moment and pol for a set with weight taps (`MosaicFT`'s sky
        // coverage, a Taylor family's moment by moment), else the scalar
        // `sumwt`.
        let sensitivity = match &weight {
            Some(weight) => weight.iter().map(|value| f64::from(*value)).collect(),
            None => sum_weights
                .iter()
                .flat_map(|weight| std::iter::repeat_n(*weight, cells))
                .collect(),
        };
        let validity = sum_weights[..polarizations]
            .iter()
            .zip(&published_sum_weights[..polarizations])
            .map(|(weight, published)| validity(*weight, *published))
            .collect();
        Ok(SpectralDomainPrimitives::new(
            domain,
            self.roles[domain].clone(),
            NormalPrimitives {
                shape,
                slab,
                basis: self.basis,
                polarizations,
                dirty: widen(&residual),
                cube_real: None,
                psf: widen(&psf),
                sensitivity,
                published_sum_weights: published_sum_weights.into_boxed_slice(),
                sum_weights: sum_weights.into_boxed_slice(),
                validity,
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
        && images
            .published_sum_weights
            .iter()
            .all(|weight| weight.is_finite())
        && images
            .weight
            .as_ref()
            .is_none_or(|weight| weight.iter().all(|value| value.is_finite()))
}

/// A plane whose accumulated weight is zero is unmapped whether or not
/// samples reached it: nothing downstream distinguishes the two cases. So
/// is a plane whose published `.sumwt` is at most CASA's empty-plane bound,
/// which CASA zeroes rather than divides ([`super::casa_divides_plane`]).
fn validity(sum_weight: f64, published_sum_weight: f64) -> SpectralChannelValidity {
    if sum_weight > 0.0 && super::casa_divides_plane(published_sum_weight) {
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

/// The coefficient basis of `problem`. A constant or Taylor basis needs a
/// linear spectral axis, whose reference frequency it expands about.
fn basis_plan(problem: &CompiledProblem) -> Result<SpectralBasisPlan, NormalStateError> {
    let polynomial = |terms: usize| {
        if matches!(
            problem.geometry().spectral().wcs(),
            SpectralWcs::Tabular { .. }
        ) {
            return Err(NormalStateError::UnsupportedProblem);
        }
        terms
            .checked_mul(2)
            .map(|_| SpectralBasisPlan::Polynomial { terms })
            .ok_or(NormalStateError::ResidencyOverflow)
    };
    match problem.reconstruction().basis() {
        ReconstructionBasis::ChannelLocal { .. } => Ok(SpectralBasisPlan::ChannelLocal),
        ReconstructionBasis::Constant => polynomial(1),
        ReconstructionBasis::Taylor { terms } => polynomial(terms),
    }
}
