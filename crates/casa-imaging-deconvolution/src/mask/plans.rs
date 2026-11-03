// SPDX-License-Identifier: LGPL-3.0-or-later

//! Each image domain's mask plan, and the masks a major-cycle boundary
//! materializes from them: a static mask (every pixel, pixel boxes or a
//! reprojected mask image, its planes as CASA expands it), or CASA's
//! automatic mask of every channel and polarization, which evolves from
//! one boundary to the next.

use casa_imaging_model::{DirectionCoordinateSpec, ModelSample};

use super::auto_multithresh::{
    AutoMultithreshControls, AutoMultithreshEvidence, auto_multithresh_bytes,
};
use super::auto_planes::{
    AutoMaskEvidence, AutoMaskSource, AutoMaskState, auto_mask_domain, channel_blocks,
};
use super::{MaskBox, MaskError, MaskPlanes, ReconstructionMask};
use crate::{FinalNormalState, ModelGeneration, NormalBasis};

/// Deferred mask construction evaluated at an exact major-cycle boundary.
#[derive(Debug, Clone)]
pub enum ReconstructionMaskPlan {
    /// Admit every model-support pixel.
    FullPlane {
        /// Target model direction coordinate.
        coordinate: DirectionCoordinateSpec,
    },
    /// Admit the union of explicit target-grid boxes.
    Boxes {
        /// Target model direction coordinate.
        coordinate: DirectionCoordinateSpec,
        /// Inclusive target-grid regions.
        boxes: Vec<MaskBox>,
    },
    /// Admit exact target-grid support reprojected from a source mask's WCS.
    Reprojected {
        /// Target model direction coordinate.
        coordinate: DirectionCoordinateSpec,
        /// The planes the support holds, shared along an axis of one.
        planes: MaskPlanes,
        /// Canonical target-grid support produced by exact WCS reprojection,
        /// plane after plane (channel-major), each x-major.
        support: Box<[bool]>,
    },
    /// Generate CASA auto-multithreshold support of every plane from the
    /// current Normal State.
    AutoMultithresh {
        /// Target model direction coordinate.
        coordinate: DirectionCoordinateSpec,
        /// Thresholding, pruning, smoothing, and growth controls.
        controls: AutoMultithreshControls,
        /// Number of completed major cycles before this boundary.
        completed_major_cycles: usize,
        /// Whether the previous cycle reached its stopping threshold.
        cycle_threshold_reached: bool,
        /// Prior immutable mask generation, when another cycle already ran.
        previous: Option<Box<ReconstructionMask>>,
        /// The channels CASA's channel-stop rule froze
        /// (`SDMaskHandler::skipChannels`); empty before the first boundary.
        stopped_channels: Box<[bool]>,
    },
}

impl ReconstructionMaskPlan {
    /// Advance live automasking to the next major-cycle boundary from the
    /// mask `current` and its automatic evidence.
    #[must_use]
    pub fn next_cycle(
        &self,
        current: &ReconstructionMask,
        evidence: Option<&AutoMaskEvidence>,
        completed_major_cycles: usize,
        cycle_threshold_reached: bool,
    ) -> Self {
        match self {
            Self::AutoMultithresh {
                coordinate,
                controls,
                ..
            } => Self::AutoMultithresh {
                coordinate: *coordinate,
                controls: *controls,
                completed_major_cycles,
                cycle_threshold_reached,
                previous: Some(Box::new(current.clone())),
                stopped_channels: evidence
                    .map(|evidence| evidence.stopped_channels().collect())
                    .unwrap_or_default(),
            },
            other => other.clone(),
        }
    }

    /// The planes this plan's masks hold for an image of `channels` by
    /// `polarizations` planes: an automatic mask has one per plane.
    #[must_use]
    pub fn mask_planes(&self, channels: usize, polarizations: usize) -> MaskPlanes {
        match self {
            Self::FullPlane { .. } | Self::Boxes { .. } => MaskPlanes::SHARED,
            Self::Reprojected { planes, .. } => *planes,
            Self::AutoMultithresh { .. } => {
                MaskPlanes::new(channels, polarizations).unwrap_or(MaskPlanes::SHARED)
            }
        }
    }

    /// Materialize the mask of domain `domain`, `shape` pixels with
    /// `planes` planes, at this boundary.
    fn materialize<E>(
        &self,
        domain: usize,
        (shape, planes): ([usize; 2], MaskPlanes),
        auto: Option<&AutoMaskSource<'_, E>>,
    ) -> Result<(ReconstructionMask, Option<AutoMaskEvidence>), E>
    where
        E: From<MaskError> + Send,
    {
        let mask = match self {
            Self::FullPlane { coordinate } => ReconstructionMask::full_plane(*coordinate, shape)?,
            Self::Boxes { coordinate, boxes } => {
                ReconstructionMask::from_boxes(*coordinate, shape, boxes.iter().copied())?
            }
            Self::Reprojected {
                coordinate,
                planes: held,
                support,
            } => {
                if !held.covers(planes.channels(), planes.polarizations()) {
                    return Err(MaskError::ShapeMismatch.into());
                }
                ReconstructionMask::from_plane_support(*coordinate, shape, *held, support.to_vec())?
            }
            Self::AutoMultithresh {
                coordinate,
                controls,
                completed_major_cycles,
                cycle_threshold_reached,
                previous,
                stopped_channels,
            } => {
                let source = auto.ok_or(MaskError::InvalidBeamArea)?;
                let state = AutoMaskState {
                    coordinate: *coordinate,
                    controls,
                    completed_major_cycles: *completed_major_cycles,
                    threshold_reached: *cycle_threshold_reached,
                    previous: previous.as_deref(),
                    stopped_channels,
                };
                let (mask, evidence) = auto_mask_domain(&state, domain, shape, planes, source)?;
                return Ok((mask, Some(evidence)));
            }
        };
        Ok((mask, None))
    }
}

/// Canonical per-image-domain mask plans for one shared reconstruction cycle.
///
/// Plan ordinal is the compiled image-domain ordinal. Construction rejects an
/// empty collection; materialization checks the domain count and shapes
/// against the Normal State.
#[derive(Debug, Clone)]
pub struct ImageDomainReconstructionMaskPlans {
    plans: Box<[ReconstructionMaskPlan]>,
    /// Major cycles completed before the boundary these plans are for.
    completed_major_cycles: usize,
}

/// The clean mask tclean's robust noise estimate (`fastnoise=False`)
/// leaves out of one plane's statistics at a boundary
/// (`SDMaskHandler::calcRobustImageStatistics`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoiseMaskExclusion<'a> {
    /// No mask yet: Chauvenet statistics of the valid pixels. tclean's
    /// first noise estimate reads the mask image before it is filled.
    Unmasked,
    /// The plane's clean mask, in canonical x-major order: the classical
    /// statistics outside it, or Chauvenet's when it is empty or `full`.
    Mask {
        /// The plane's support.
        support: &'a [bool],
        /// Whether the mask covers the whole first plane CASA's statistics
        /// read it from: the automatic mask's channel block, or else the
        /// plane itself.
        full: bool,
    },
    /// A plane whose automatic mask stopped evolving: tclean records its
    /// statistics as zero.
    Stopped,
}

/// Immutable per-image-domain masks of one shared cycle.
#[derive(Debug, Clone)]
pub struct ImageDomainReconstructionMasks {
    masks: Box<[ReconstructionMask]>,
}

/// Materialized domain masks and their optional auto-mask evidence.
#[derive(Debug, Clone)]
pub struct ImageDomainMaskMaterialization {
    masks: ImageDomainReconstructionMasks,
    auto_mask_evidence: Box<[Option<AutoMaskEvidence>]>,
}

/// Exact spatial-support generation consumed by one reconstruction cycle.
#[derive(Debug, Clone)]
pub enum ReconstructionMaskSet {
    /// One support used by a scalar, cube, or Taylor solve.
    Shared(Box<ReconstructionMask>),
    /// One canonical support for every compiled image domain.
    Domains(ImageDomainReconstructionMasks),
}

impl ReconstructionMaskSet {
    /// Return the primary support.
    #[must_use]
    pub const fn primary(&self) -> &ReconstructionMask {
        match self {
            Self::Shared(mask) => mask,
            Self::Domains(masks) => masks.primary(),
        }
    }

    /// Return canonical image-domain supports when this is a multi-chart cycle.
    #[must_use]
    pub const fn domains(&self) -> Option<&ImageDomainReconstructionMasks> {
        match self {
            Self::Domains(masks) => Some(masks),
            Self::Shared(_) => None,
        }
    }
}

/// The channel and polarization planes of `normal`: a Taylor family's
/// terms share one plane per polarization.
fn normal_planes(normal: &FinalNormalState) -> MaskPlanes {
    let channels = if normal.basis() == NormalBasis::Taylor {
        1
    } else {
        normal.channel_count()
    };
    MaskPlanes::new(channels, normal.polarization_count()).unwrap_or(MaskPlanes::SHARED)
}

impl ImageDomainReconstructionMaskPlans {
    /// Adopt one non-empty plan for each image domain in compiled order.
    pub fn new(plans: impl IntoIterator<Item = ReconstructionMaskPlan>) -> Result<Self, MaskError> {
        let plans = plans.into_iter().collect::<Vec<_>>().into_boxed_slice();
        if plans.is_empty() {
            return Err(MaskError::DomainCardinalityMismatch);
        }
        Ok(Self {
            plans,
            completed_major_cycles: 0,
        })
    }

    /// Return the number of canonical domain plans.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.plans.len()
    }

    /// Return whether no domain plans are present (always false for valid values).
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.plans.is_empty()
    }

    /// Borrow the primary-domain plan.
    #[must_use]
    pub const fn primary(&self) -> &ReconstructionMaskPlan {
        &self.plans[0]
    }

    /// Whether some domain's mask is CASA's automatic mask.
    #[must_use]
    pub fn automatic(&self) -> bool {
        self.plans
            .iter()
            .any(|plan| matches!(plan, ReconstructionMaskPlan::AutoMultithresh { .. }))
    }

    /// Advance every domain plan to the next shared major-cycle boundary
    /// from the masks `current` and their automatic `evidence`, both in
    /// domain order.
    pub fn next_cycle(
        &self,
        current: &ImageDomainReconstructionMasks,
        evidence: &[Option<AutoMaskEvidence>],
        completed_major_cycles: usize,
        cycle_threshold_reached: bool,
    ) -> Result<Self, MaskError> {
        if self.plans.len() != current.len() || self.plans.len() != evidence.len() {
            return Err(MaskError::DomainCardinalityMismatch);
        }
        let plans =
            self.plans
                .iter()
                .zip(current.iter())
                .zip(evidence)
                .map(|((plan, mask), evidence)| {
                    plan.next_cycle(
                        mask,
                        evidence.as_ref(),
                        completed_major_cycles,
                        cycle_threshold_reached,
                    )
                });
        let mut next = Self::new(plans)?;
        next.completed_major_cycles = completed_major_cycles;
        Ok(next)
    }

    /// What tclean's robust noise estimate leaves out of the statistics of
    /// domain `ordinal`'s plane `channel` and `polarization` at this
    /// boundary, `current` being the masks materialized for it: an
    /// automatic mask's inherited support, or a static mask once the first
    /// boundary has passed.
    #[must_use]
    pub fn noise_exclusion<'a>(
        &'a self,
        ordinal: usize,
        (channel, polarization): (usize, usize),
        current: &'a ImageDomainReconstructionMasks,
    ) -> NoiseMaskExclusion<'a> {
        let full = |plane: &[bool]| plane.iter().all(|set| *set);
        match self.plans.get(ordinal) {
            Some(ReconstructionMaskPlan::AutoMultithresh {
                stopped_channels, ..
            }) if stopped_channels.get(channel).copied().unwrap_or(false) => {
                NoiseMaskExclusion::Stopped
            }
            Some(ReconstructionMaskPlan::AutoMultithresh { previous, .. }) => previous
                .as_deref()
                .and_then(|mask| {
                    let support = mask.plane(channel, polarization)?;
                    // The statistics of a channel block read the mask's
                    // first plane in it (`calcRobustImageStatistics`).
                    let first = channel_blocks(mask.planes().channels(), mask.shape())
                        .into_iter()
                        .find(|block| block.contains(&channel))
                        .map_or(0, |block| block.start);
                    Some(NoiseMaskExclusion::Mask {
                        support,
                        full: mask.plane(first, 0).is_some_and(full),
                    })
                })
                .unwrap_or(NoiseMaskExclusion::Unmasked),
            _ if self.completed_major_cycles == 0 => NoiseMaskExclusion::Unmasked,
            _ => current
                .get(ordinal)
                .and_then(|mask| mask.plane(channel, polarization))
                .map_or(NoiseMaskExclusion::Unmasked, |support| {
                    NoiseMaskExclusion::Mask {
                        support,
                        full: full(support),
                    }
                }),
        }
    }

    /// Heap bytes the masks materialized from these plans hold against
    /// domains of the shapes `domain_shape` gives by ordinal, with
    /// `channels` by `polarizations` planes: each domain's support, one
    /// per plane its plan holds ([`ReconstructionMaskPlan::mask_planes`]),
    /// and an automatic mask's summary row per plane.
    #[must_use]
    pub fn mask_bytes_for(
        &self,
        domain_shape: impl Fn(usize) -> Option<[usize; 2]>,
        (channels, polarizations): (usize, usize),
    ) -> u64 {
        self.plans
            .iter()
            .enumerate()
            .map(|(ordinal, plan)| {
                let cells =
                    domain_shape(ordinal).map_or(0, |[width, height]| (width * height) as u64);
                let planes = plan.mask_planes(channels, polarizations).count() as u64;
                let rows = match plan {
                    ReconstructionMaskPlan::AutoMultithresh { .. } => {
                        planes * size_of::<AutoMultithreshEvidence>() as u64
                    }
                    _ => 0,
                };
                cells * planes * size_of::<bool>() as u64 + rows
            })
            .sum()
    }

    /// Heap bytes [`Self::materialize`] holds at most against `normal`
    /// with `workers` channels masked at once.
    #[must_use]
    pub fn materialize_bytes(&self, normal: &FinalNormalState, workers: usize) -> u64 {
        let planes = normal_planes(normal);
        self.materialize_bytes_for(
            |ordinal| normal.domain_shape(ordinal),
            (planes.channels(), planes.polarizations()),
            workers,
        )
    }

    /// [`Self::materialize_bytes`] against image domains of the shapes
    /// `domain_shape` gives by ordinal, with `channels` by `polarizations`
    /// planes: what a run plans before its first major cycle forms the
    /// Normal State (#737). Each domain's new masks
    /// ([`Self::mask_bytes_for`]) and, for an automatic mask, on each of
    /// the `workers` channels masked at once, what one plane's mask holds
    /// ([`auto_multithresh_bytes`]), the plane's read (as `Complex64`) and
    /// the model support it reads, and the channel's new supports.
    #[must_use]
    pub fn materialize_bytes_for(
        &self,
        domain_shape: impl Fn(usize) -> Option<[usize; 2]>,
        (channels, polarizations): (usize, usize),
        workers: usize,
    ) -> u64 {
        let working = self
            .plans
            .iter()
            .enumerate()
            .filter(|(_, plan)| matches!(plan, ReconstructionMaskPlan::AutoMultithresh { .. }))
            .map(|(ordinal, _)| {
                let Some(shape) = domain_shape(ordinal) else {
                    return 0;
                };
                let cells = (shape[0] * shape[1]) as u64;
                let read = size_of::<num_complex::Complex64>() as u64
                    + size_of::<ModelSample>() as u64
                    + polarizations as u64 * size_of::<bool>() as u64;
                workers.min(channels).max(1) as u64 * (auto_multithresh_bytes(shape) + cells * read)
            })
            .max()
            .unwrap_or(0);
        self.mask_bytes_for(&domain_shape, (channels, polarizations)) + working
    }

    /// Materialize every domain's mask against one shared Normal State;
    /// `auto` reads the planes an automatic mask is made from, and is
    /// required by one. Channels of an automatic mask are masked side by
    /// side on the current rayon pool.
    ///
    /// # Errors
    ///
    /// A plan does not fit the state, a static mask is invalid, or the
    /// automatic mask fails or cannot read a plane.
    pub fn materialize<E>(
        &self,
        base: &ModelGeneration,
        normal: &FinalNormalState,
        auto: Option<&AutoMaskSource<'_, E>>,
    ) -> Result<ImageDomainMaskMaterialization, E>
    where
        E: From<MaskError> + Send,
    {
        if self.plans.len() != normal.domain_count()
            || self.plans.len() != base.shape().domains().len()
        {
            return Err(MaskError::DomainCardinalityMismatch.into());
        }
        let planes = normal_planes(normal);
        let mut masks = Vec::with_capacity(self.plans.len());
        let mut evidence = Vec::with_capacity(self.plans.len());
        for (ordinal, plan) in self.plans.iter().enumerate() {
            let shape = normal
                .domain_shape(ordinal)
                .ok_or(MaskError::DomainCardinalityMismatch)?;
            let (mask, auto_evidence) = plan.materialize(ordinal, (shape, planes), auto)?;
            masks.push(mask);
            evidence.push(auto_evidence);
        }
        Ok(ImageDomainMaskMaterialization {
            masks: ImageDomainReconstructionMasks::new(masks)?,
            auto_mask_evidence: evidence.into_boxed_slice(),
        })
    }
}

impl ImageDomainMaskMaterialization {
    /// Release the canonical masks and matching evidence in domain order.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ImageDomainReconstructionMasks,
        Box<[Option<AutoMaskEvidence>]>,
    ) {
        (self.masks, self.auto_mask_evidence)
    }
}

impl ImageDomainReconstructionMasks {
    fn new(masks: impl IntoIterator<Item = ReconstructionMask>) -> Result<Self, MaskError> {
        let masks = masks.into_iter().collect::<Vec<_>>().into_boxed_slice();
        if masks.is_empty() {
            return Err(MaskError::DomainCardinalityMismatch);
        }
        Ok(Self { masks })
    }

    /// Return the canonical domain count.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.masks.len()
    }

    /// Return whether no domain supports are present (always false for valid values).
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.masks.is_empty()
    }

    /// Borrow the primary-domain support.
    #[must_use]
    pub const fn primary(&self) -> &ReconstructionMask {
        &self.masks[0]
    }

    /// Borrow one support by canonical domain ordinal.
    #[must_use]
    pub fn get(&self, domain_ordinal: usize) -> Option<&ReconstructionMask> {
        self.masks.get(domain_ordinal)
    }

    /// Iterate supports in canonical domain order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &ReconstructionMask> {
        self.masks.iter()
    }
}
