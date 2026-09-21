// SPDX-License-Identifier: LGPL-3.0-or-later

//! Temporary normal-state handoff, compiled in the existing primitive owner so
//! the new band implementation does not expose or depend on its field inventory.

use super::*;
use crate::streaming_cube::band::{BandImages, BandPhase};
use std::mem::size_of_val;

impl SpectralOperatorSpecification {
    pub(crate) fn cube_geometry(&self) -> Result<SpectralOperatorGeometry, SpectralOperatorError> {
        if self.basis != SpectralBasisPlan::ChannelLocal
            || self.domains.len() != 1
            || self.charts.len() != 1
            || self.charts[0].window.origin() != [0, 0]
            || self.charts[0].geometry.image_shape != self.image_shape
            || self.polarization_coordinates.as_ref() != [PolarizationCoordinate::StokesI]
            || self.w_projection.is_some()
            || self.aw_projection.is_some()
            || self.instrument_model.is_some()
            || self.mosaic
            || self.spectral_kernel != SpectralKernel::Linear
        {
            return Err(SpectralOperatorError::UnsupportedProblem);
        }
        Ok(self.charts[0].geometry)
    }
}

impl SpectralOperatorPrimitives {
    pub(crate) fn cube_owned_bytes(
        &self,
        include_residual: bool,
    ) -> Result<usize, SpectralOperatorError> {
        let fields = [
            if include_residual {
                size_of_val(self.dirty.as_ref())
            } else {
                0
            },
            self.invariant_dirty.as_deref().map_or(0, size_of_val),
            size_of_val(self.psf.as_ref()),
            size_of_val(self.sensitivity.as_ref()),
            size_of_val(self.sum_weights.as_ref()),
            size_of_val(self.published_sum_weights.as_ref()),
            size_of_val(self.validity.as_ref()),
            size_of_val(self.joint_line_term_by_channel.as_ref()),
        ];
        fields
            .into_iter()
            .try_fold(size_of::<Self>(), |sum, bytes| {
                sum.checked_add(bytes)
                    .ok_or(SpectralOperatorError::ResidencyOverflow)
            })
    }

    /// Runtime has already matched the run/weighting/source identity. This
    /// consuming handoff validates the local layout and releases the obsolete
    /// residual before allocating its replacement, without scanning content.
    pub(crate) fn prepare_cube_reuse(
        &mut self,
        shape: [usize; 2],
        core: std::ops::Range<usize>,
        total_channels: usize,
    ) -> Result<(), SpectralOperatorError> {
        self.validate_cube_layout(shape, core, total_channels)?;
        self.dirty = Box::new([]);
        Ok(())
    }

    pub(crate) fn validate_cube_layout(
        &self,
        shape: [usize; 2],
        core: std::ops::Range<usize>,
        total_channels: usize,
    ) -> Result<(), SpectralOperatorError> {
        let values = checked_cells(shape)?
            .checked_mul(core.len())
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        if self.shape != shape
            || core.is_empty()
            || core.end > total_channels
            || self.slab.core_range() != core
            || self.slab.total_channels() != total_channels
            || self.basis != SpectralBasisPlan::ChannelLocal
            || self.polarizations != 1
            || !self.major_cycle_residual_promoted
            || self.residual_model.is_none()
            || self.dirty.len() != values
            || self.psf.len() != values
            || self.sensitivity.len() != values
            || self.sum_weights.len() != core.len()
            || self.published_sum_weights.len() != core.len()
            || self.validity.len() != core.len()
            || self
                .invariant_dirty
                .as_ref()
                .is_some_and(|v| v.len() != values)
            || self.major_cycle_residual.is_some()
            || self.primary_beam_weighted_sum.is_some()
            || self.common_residual.is_some()
            || self.invariant_common_dirty.is_some()
            || !self.channel_sum_weights.is_empty()
            || self.joint_line_term_by_channel.len() != total_channels
            || self.joint_line_term_by_channel.iter().any(Option::is_some)
        {
            return Err(SpectralOperatorError::ReusableNormalStateMismatch);
        }
        Ok(())
    }

    pub(crate) fn from_cube_band(
        images: BandImages,
        total_channels: usize,
        model: ModelGenerationId,
        prior: Option<Self>,
    ) -> Result<Self, SpectralOperatorError> {
        let BandImages {
            phase,
            shape,
            core,
            dirty,
            residual,
            psf,
            sum_weight,
            mapped,
        } = images;
        let cells = checked_cells(shape)?;
        let values = cells
            .checked_mul(core.len())
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        if phase == BandPhase::Residual {
            let mut prior = prior.ok_or(SpectralOperatorError::ReusableNormalStateMismatch)?;
            if residual.len() != values
                || !dirty.is_empty()
                || !psf.is_empty()
                || !sum_weight.is_empty()
                || !mapped.is_empty()
                || prior.shape != shape
                || prior.slab.core_range() != core
                || prior.slab.total_channels() != total_channels
                || !prior.dirty.is_empty()
            {
                return Err(SpectralOperatorError::ReusableNormalStateMismatch);
            }
            prior.dirty = residual.into_boxed_slice();
            prior.residual_model = Some(model);
            return Ok(prior);
        }
        if prior.is_some()
            || core.is_empty()
            || core.end > total_channels
            || dirty.len() != values
            || residual.len() != if phase == BandPhase::Full { values } else { 0 }
            || psf.len() != values
            || sum_weight.len() != core.len()
            || mapped.len() != core.len()
        {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        if sum_weight
            .iter()
            .any(|weight| !weight.is_finite() || *weight < 0.0)
        {
            return Err(SpectralOperatorError::GeneratedNonfinite);
        }
        let mut sensitivity = Vec::with_capacity(values);
        for &weight in &sum_weight {
            sensitivity.extend(std::iter::repeat_n(weight, cells));
        }
        let sensitivity = sensitivity.into_boxed_slice();
        let validity = mapped
            .iter()
            .zip(&sum_weight)
            .map(|(&mapped, &weight)| validity_from_support(mapped, weight))
            .collect();
        let (dirty, invariant_dirty) = if phase == BandPhase::InitialZero {
            // A direct-native refresh does not consume the old data-side dirty
            // image. The initial residual is already dirty, so move it once.
            (dirty.into_boxed_slice(), None)
        } else {
            (residual.into_boxed_slice(), Some(dirty.into_boxed_slice()))
        };
        Ok(Self {
            shape,
            slab: SpectralSlabPlan {
                total_channels,
                core_start: core.start,
                core_end: core.end,
                resident_start: core.start,
                resident_end: core.end,
            },
            basis: SpectralBasisPlan::ChannelLocal,
            polarizations: 1,
            joint_line_term_by_channel: vec![None; total_channels].into(),
            // The controller consumes the exact residual. Retain the data-side
            // dirty image by moving it, avoiding the old clone-then-promotion copy.
            dirty,
            invariant_dirty,
            common_residual: None,
            invariant_common_dirty: None,
            psf: psf.into_boxed_slice(),
            sensitivity,
            primary_beam_weighted_sum: None,
            published_sum_weights: sum_weight.clone().into_boxed_slice(),
            sum_weights: sum_weight.into_boxed_slice(),
            channel_sum_weights: Box::new([]),
            validity,
            major_cycle_residual: None,
            major_cycle_residual_promoted: true,
            residual_model: Some(model),
            #[cfg(test)]
            measurements: SpectralOperatorMeasurements::default(),
        })
    }
}

impl CompleteDataOwnerResult {
    /// Transfer a completed native band into the existing normal-state fold.
    ///
    /// The runtime must first finish its native-input writer and authoritative
    /// weighted traversal, then match the problem/attempt/node/lease and frozen
    /// source association. This binds that terminal replay to an owned numerical
    /// result; it does not manufacture source completion or inspect image values.
    #[doc(hidden)]
    pub fn from_streaming_cube(
        specification: &SpectralOperatorSpecification,
        primitives: SpectralOperatorPrimitives,
        replay: &WeightingReplaySummary,
        selected_generation: SelectedObservationGenerationId,
        continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    ) -> Result<Self, SpectralOperatorError> {
        let geometry = specification.cube_geometry()?;
        primitives.validate_cube_layout(
            geometry.image_shape,
            specification.slab.core_range(),
            specification.slab.total_channels(),
        )?;
        if replay.sample_count() == 0 || replay.block_count() == 0 {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        let domains =
            combine_initial_chart_primitives(specification, std::iter::once(Ok(primitives)))?;
        Ok(Self {
            domains,
            completion: CompleteDataOwnerCompletion {
                problem: specification.problem,
                geometry: specification.geometry,
                numerics: specification.numerics,
                weighting_commitment: specification.weighting_commitment,
                weighting_generation: replay.weighting_generation(),
                replay: replay.replay_id(),
                coverage: replay.coverage(),
                // Reuse terminal source coverage; no second encoding pass.
                coverage_proof_bytes: 0,
                coverage_proof_hash_calls: 0,
                primitives: SpectralPrimitiveCatalog::UnnormalizedChannelSlabV1,
                selected_generation,
                continuum_transform_generation,
                sample_count: replay.sample_count(),
                block_count: replay.block_count(),
            },
        })
    }
}

#[test]
fn cube_phase_handoff_moves_initial_dirty_and_preserves_explicit_invariants() {
    let model = ModelGenerationId(LogicalIdentity::from_sha256([47; 32]));
    let make = |phase| BandImages {
        phase,
        shape: [2, 1],
        core: 0..1,
        dirty: vec![Complex64::new(3.0, 0.0); 2],
        residual: if phase == BandPhase::Full {
            vec![Complex64::new(1.0, 0.0); 2]
        } else {
            Vec::new()
        },
        psf: vec![Complex64::new(2.0, 0.0); 2],
        sum_weight: vec![2.0],
        mapped: vec![3],
    };
    let images = make(BandPhase::InitialZero);
    let pointer = images.dirty.as_ptr();
    let initial = SpectralOperatorPrimitives::from_cube_band(images, 1, model, None).unwrap();
    assert_eq!(initial.dirty.as_ptr(), pointer);
    assert!(initial.invariant_dirty.is_none());
    assert!(initial.major_cycle_residual.is_none());
    let images = make(BandPhase::Full);
    let dirty_pointer = images.dirty.as_ptr();
    let residual_pointer = images.residual.as_ptr();
    let mut full = SpectralOperatorPrimitives::from_cube_band(images, 1, model, None).unwrap();
    assert_eq!(full.dirty.as_ptr(), residual_pointer);
    assert_eq!(
        full.invariant_dirty.as_ref().unwrap().as_ptr(),
        dirty_pointer
    );
    assert_eq!(
        full.invariant_dirty.as_deref().unwrap(),
        &[Complex64::new(3.0, 0.0); 2]
    );
    assert!(full.prepare_cube_reuse([2, 1], 0..1, 2).is_err());
    assert_eq!(
        full.dirty.as_ptr(),
        residual_pointer,
        "failed layout check must not mutate"
    );
    full.prepare_cube_reuse([2, 1], 0..1, 1).unwrap();
    assert!(full.dirty.is_empty());
    assert_eq!(
        full.invariant_dirty.as_ref().unwrap().as_ptr(),
        dirty_pointer
    );
}
