// SPDX-License-Identifier: LGPL-3.0-or-later

//! Temporary normal-state handoff, compiled in the existing primitive owner so
//! the new band implementation does not expose or depend on its field inventory.

use super::*;
use crate::streaming_cube::band::{BandImages, BandPhase};
#[cfg(test)]
use std::mem::size_of_val;

impl SpectralOperatorSpecification {
    pub(crate) fn cube_single_channel(
        &self,
    ) -> Option<crate::spectral_sampling::CasaSingleChannel> {
        self.single_output_channel
    }

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
            || (self.output_channel_frequencies_hz.len() == 1
                && self.single_output_channel.is_none())
        {
            return Err(SpectralOperatorError::UnsupportedProblem);
        }
        Ok(self.charts[0].geometry)
    }
}

impl SpectralOperatorPrimitives {
    #[cfg(test)]
    pub(crate) fn cube_owned_bytes(
        &self,
        include_residual: bool,
    ) -> Result<usize, SpectralOperatorError> {
        let fields = [
            if include_residual {
                self.cube_real
                    .as_ref()
                    .map_or(size_of_val(self.dirty.as_ref()), |real| {
                        size_of_val(real.dirty.as_ref())
                    })
            } else {
                0
            },
            self.cube_real.as_ref().map_or_else(
                || self.invariant_dirty.as_deref().map_or(0, size_of_val),
                |real| real.invariant_dirty.as_deref().map_or(0, size_of_val),
            ),
            self.cube_real
                .as_ref()
                .map_or(size_of_val(self.psf.as_ref()), |real| {
                    size_of_val(real.psf.as_ref())
                }),
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
            || self.cube_real.as_ref().is_none_or(|real| {
                real.dirty.len() != values
                    || real.psf.len() != values
                    || real
                        .invariant_dirty
                        .as_ref()
                        .is_some_and(|v| v.len() != values)
            })
            || !self.dirty.is_empty()
            || !self.psf.is_empty()
            || !self.sensitivity.is_empty()
            || self.sum_weights.len() != core.len()
            || self.published_sum_weights.len() != core.len()
            || self.validity.len() != core.len()
            || self.invariant_dirty.is_some()
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
        if phase == BandPhase::Residual
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
            clark_workspace: std::sync::Mutex::new(None),
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
            dirty: Box::new([]),
            cube_real: Some(CubeRealFields {
                dirty,
                invariant_dirty,
                psf: psf.into_boxed_slice(),
            }),
            invariant_dirty: None,
            common_residual: None,
            invariant_common_dirty: None,
            psf: Box::new([]),
            sensitivity: Box::new([]),
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
    ) -> Result<Self, SpectralOperatorError> {
        let geometry = specification.cube_geometry()?;
        primitives.validate_cube_layout(
            geometry.image_shape,
            specification.slab.core_range(),
            specification.slab.total_channels(),
        )?;
        let domains =
            combine_initial_chart_primitives(specification, std::iter::once(Ok(primitives)))?;
        Ok(Self {
            domains,
            completion: CompleteDataOwnerCompletion::from_streaming_cube(specification, replay)?,
        })
    }
}

impl CompleteDataOwnerCompletion {
    pub(crate) fn from_streaming_cube(
        specification: &SpectralOperatorSpecification,
        replay: &WeightingReplaySummary,
    ) -> Result<Self, SpectralOperatorError> {
        specification.cube_geometry()?;
        if replay.sample_count() == 0 || replay.block_count() == 0 {
            return Err(SpectralOperatorError::IncompleteCoverage);
        }
        Ok(Self {
            problem: specification.problem,
            geometry: specification.geometry,
            numerics: specification.numerics,
            weighting_commitment: specification.weighting_commitment,
            weighting_generation: replay.weighting_generation(),
            replay: replay.replay_id(),

            // Reuse terminal source coverage; no second encoding pass.
            primitives: SpectralPrimitiveCatalog::UnnormalizedChannelSlabV1,

            sample_count: replay.sample_count(),
            block_count: replay.block_count(),
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
        dirty: vec![3.0; 2],
        residual: if phase == BandPhase::Full {
            vec![1.0; 2]
        } else {
            Vec::new()
        },
        psf: vec![2.0; 2],
        sum_weight: vec![2.0],
        mapped: vec![3],
    };
    let images = make(BandPhase::InitialZero);
    let pointer = images.dirty.as_ptr();
    let initial = SpectralOperatorPrimitives::from_cube_band(images, 1, model).unwrap();
    assert_eq!(initial.cube_real.as_ref().unwrap().dirty.as_ptr(), pointer);
    assert_eq!(initial.dirty().real(), Some(&[3.0; 2][..]));
    assert_eq!(initial.psf().real(), Some(&[2.0; 2][..]));
    assert_eq!(initial.sensitivity().iter().collect::<Vec<_>>(), [2.0; 2]);
    assert!(initial.invariant_dirty.is_none());
    assert!(initial.major_cycle_residual.is_none());
    let images = make(BandPhase::Full);
    let dirty_pointer = images.dirty.as_ptr();
    let residual_pointer = images.residual.as_ptr();
    let full = SpectralOperatorPrimitives::from_cube_band(images, 1, model).unwrap();
    assert_eq!(
        full.cube_real.as_ref().unwrap().dirty.as_ptr(),
        residual_pointer
    );
    assert_eq!(
        full.cube_real
            .as_ref()
            .unwrap()
            .invariant_dirty
            .as_ref()
            .unwrap()
            .as_ptr(),
        dirty_pointer
    );
    assert_eq!(
        full.cube_real
            .as_ref()
            .unwrap()
            .invariant_dirty
            .as_deref()
            .unwrap(),
        &[3.0; 2]
    );
    assert!(full.validate_cube_layout([2, 1], 0..1, 2).is_err());
}

#[test]
fn compact_reads_and_diagnostic_cover_pixels_without_retaining_widened_buffers() {
    let model = ModelGenerationId(LogicalIdentity::from_sha256([47; 32]));
    let make = |dirty, psf| {
        SpectralOperatorPrimitives::from_cube_band(
            BandImages {
                phase: BandPhase::InitialZero,
                shape: [2, 1],
                core: 0..1,
                dirty,
                residual: Vec::new(),
                psf,
                sum_weight: vec![2.0],
                mapped: vec![3],
            },
            1,
            model,
        )
        .unwrap()
    };
    let original = make(vec![3.0, 4.0], vec![2.0, 1.0]);
    let changed_dirty = make(vec![3.0, 5.0], vec![2.0, 1.0]);
    let changed_psf = make(vec![3.0, 4.0], vec![2.0, 1.5]);
    let fingerprint = original.normal_state_content_identity();
    assert_ne!(fingerprint, changed_dirty.normal_state_content_identity());
    assert_ne!(fingerprint, changed_psf.normal_state_content_identity());
    // The existing diagnostic format denotes numerical values, not storage width.
    let mut wide = make(vec![3.0, 4.0], vec![2.0, 1.0]);
    let real = wide.cube_real.take().unwrap();
    wide.dirty = real
        .dirty
        .iter()
        .map(|&v| Complex64::new(f64::from(v), 0.0))
        .collect();
    wide.psf = real
        .psf
        .iter()
        .map(|&v| Complex64::new(f64::from(v), 0.0))
        .collect();
    wide.sensitivity = vec![2.0; 2].into();
    assert_eq!(fingerprint, wide.normal_state_content_identity());

    let dirty_pointer = original.dirty().real().unwrap().as_ptr();
    let psf_pointer = original.psf().real().unwrap().as_ptr();
    let compact_bytes = original.cube_owned_bytes(true).unwrap();
    let domains = SpectralPrimitiveDomains::new(
        vec![SpectralDomainPrimitives::new(
            0,
            ImageDomainRole::Main,
            original,
        )]
        .into(),
    )
    .unwrap();
    let expected = size_of::<SpectralDomainPrimitives>()
        + 4 * size_of::<f32>()
        + 2 * size_of::<f64>()
        + size_of::<SpectralChannelValidity>()
        + size_of::<Option<usize>>();
    for _ in 0..3 {
        assert_eq!(domains.owned_bytes(), expected);
        let p = domains.primary();
        assert_eq!(p.dirty().real().unwrap().as_ptr(), dirty_pointer);
        assert_eq!(p.psf().real().unwrap().as_ptr(), psf_pointer);
        assert_eq!(
            p.dirty().iter().collect::<Vec<_>>(),
            [Complex64::new(3.0, 0.0), Complex64::new(4.0, 0.0)]
        );
        assert_eq!(p.sensitivity().iter().collect::<Vec<_>>(), [2.0; 2]);
        assert!(p.dirty().complex().is_none());
        assert!(p.psf().complex().is_none());
        assert!(p.sensitivity().dense().is_none());
        assert_eq!(p.normal_state_content_identity(), fingerprint);
        assert_eq!(p.cube_owned_bytes(true).unwrap(), compact_bytes);
        assert_eq!(domains.owned_bytes(), expected);
    }
}
