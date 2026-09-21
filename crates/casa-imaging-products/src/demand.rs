// SPDX-License-Identifier: LGPL-3.0-or-later

//! Checked array and metadata residency owned by continuum product generation.

use std::mem::size_of;

use casa_imaging_model::{
    ImageDomainRole, ProductBeamRule, ProductRole, ProductValidityRule, RestoringBeamPolicy,
    SpectralWcs,
};
use casa_imaging_reconstruction::NormalStateCatalog;
use num_complex::Complex64;
use rustfft::FftPlanner;

use crate::{
    ContinuumProductInputs, PlannedContinuumGeneration, ProductStoragePlan, ProductsError,
    PublishedContinuumGeneration, PublishedMember, RestoringBeam,
};

/// Checked owned-array and metadata demand for one planned continuum generation.
///
/// This is deliberately non-persistent. It describes the current algorithm
/// catalog's owned payload, validity, and working arrays so the runtime can
/// acquire one bounded lease before production starts. The generated summary's
/// owned metadata and temporary beam arrays are included; allocator bookkeeping,
/// the borrowed plan and borrowed reconstruction inputs are not charged here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinuumGenerationDemand {
    storage_plan: ProductStoragePlan,
    algorithm_scratch_bytes: u64,
    retained_metadata_bytes: u64,
    beam_scratch_bytes: u64,
    maximum_member_payload_bytes: u64,
    maximum_member_validity_bytes: u64,
    maximum_window_payload_bytes: u64,
    maximum_window_validity_bytes: u64,
    peak_residency_bytes: u64,
}

impl ContinuumGenerationDemand {
    /// Explicit selected window bound used by this generation.
    #[must_use]
    pub const fn storage_plan(self) -> ProductStoragePlan {
        self.storage_plan
    }

    /// Largest resident numeric output window used by generation.
    #[must_use]
    pub const fn maximum_window_payload_bytes(self) -> u64 {
        self.maximum_window_payload_bytes
    }

    /// Largest resident stored-mask window.
    #[must_use]
    pub const fn maximum_window_validity_bytes(self) -> u64 {
        self.maximum_window_validity_bytes
    }

    /// Maximum product-algorithm working-array bytes retained with production.
    #[must_use]
    pub const fn algorithm_scratch_bytes(self) -> u64 {
        self.algorithm_scratch_bytes
    }

    /// Owned generation summary, member contracts, WCS and beam arrays.
    #[must_use]
    pub const fn retained_metadata_bytes(self) -> u64 {
        self.retained_metadata_bytes
    }

    /// Peak beam phase, including parallel input conversion, fitting and slots.
    #[must_use]
    pub const fn beam_scratch_bytes(self) -> u64 {
        self.beam_scratch_bytes
    }

    /// Largest single member's binary32 payload.
    #[must_use]
    pub const fn maximum_member_payload_bytes(self) -> u64 {
        self.maximum_member_payload_bytes
    }

    /// Largest single member's byte-addressed validity array.
    #[must_use]
    pub const fn maximum_member_validity_bytes(self) -> u64 {
        self.maximum_member_validity_bytes
    }

    /// Bounded owned arrays plus retained metadata and temporary beam storage.
    #[must_use]
    pub const fn peak_residency_bytes(self) -> u64 {
        self.peak_residency_bytes
    }
}

impl PlannedContinuumGeneration {
    /// Derive current-catalog array and metadata demand for these inputs.
    ///
    /// # Errors
    ///
    /// Fails closed when the inputs do not name this plan's problem and source
    /// generations, or when any byte calculation overflows.
    pub fn demand(
        &self,
        inputs: &ContinuumProductInputs<'_>,
        storage_plan: ProductStoragePlan,
    ) -> Result<ContinuumGenerationDemand, ProductsError> {
        if inputs.problem().problem_id() != self.problem_id()
            || inputs.problem().product_graph().graph_id() != self.graph_id()
            || inputs.major_cycle_completion() != self.major_cycle_completion()
            || inputs.normal_state_completion() != self.normal_state_completion()
            || inputs.final_model().generation_id() != self.final_model_generation()
            || inputs.reconstruction_mask_generation() != self.reconstruction_mask_generation()
            || inputs
                .coupled_reconstruction_masks()
                .map(|masks| masks.line().generation_id())
                != self.line_reconstruction_mask_generation()
        {
            return Err(ProductsError::SourceLineageMismatch);
        }

        let mut maximum_member_payload_bytes = 0_u64;
        let mut maximum_member_validity_bytes = 0_u64;
        let mut maximum_window_payload_bytes = 0_u64;
        let mut maximum_window_validity_bytes = 0_u64;
        let mut maximum_windows = 1;
        for member in self.members() {
            let values = checked_shape_values(member.shape())?;
            if values != member.payload_values() {
                return Err(ProductsError::PayloadLengthMismatch {
                    expected: values,
                    actual: member.payload_values(),
                });
            }
            let payload = bytes_for::<f32>(values, "member payload")?;
            let validity = bytes_for::<bool>(values, "member validity")?;
            maximum_member_payload_bytes = maximum_member_payload_bytes.max(payload);
            maximum_member_validity_bytes = maximum_member_validity_bytes.max(validity);
            let layout = storage_plan.layout(member.axes())?;
            maximum_windows = maximum_windows
                .max(layout.shape()[layout.spectral_axis()].div_ceil(layout.maximum_channels()));
            let window_values = layout.maximum_values();
            maximum_window_payload_bytes = maximum_window_payload_bytes
                .max(bytes_for::<f32>(window_values, "product window payload")?);
            maximum_window_validity_bytes = maximum_window_validity_bytes
                .max(bytes_for::<bool>(window_values, "product window validity")?);
        }

        let requires_beam = self
            .members()
            .iter()
            .any(|member| member.beam_rule() != ProductBeamRule::None);
        let beam_jobs = if requires_beam {
            inputs
                .normal_state()
                .domain_count()
                .checked_mul(inputs.normal_state().channel_count())
                .and_then(|count| count.checked_mul(inputs.normal_state().polarization_count()))
                .ok_or(ProductsError::ResourceDemandOverflow(
                    "generation beam count",
                ))?
        } else {
            0
        };
        let workers = match inputs.normal_state().catalog() {
            NormalStateCatalog::UnnormalizedTaylorBlockV1
            | NormalStateCatalog::UnnormalizedJointBlockV1 => 1,
            _ => storage_plan
                .maximum_workers()
                .min(maximum_windows.max(beam_jobs)),
        };
        let storage_plan = ProductStoragePlan::new(storage_plan.maximum_channels(), workers)?;

        let mut algorithm_scratch_bytes = match inputs.normal_state().catalog() {
            NormalStateCatalog::UnnormalizedTaylorBlockV1 => taylor_scratch_bytes(inputs)?,
            NormalStateCatalog::UnnormalizedJointBlockV1 => generic_scratch_bytes(self, inputs)?,
            NormalStateCatalog::UnnormalizedPlaneV1
            | NormalStateCatalog::UnnormalizedChannelSlabV1 => generic_scratch_bytes(self, inputs)?,
        };
        if matches!(
            inputs.normal_state().catalog(),
            NormalStateCatalog::UnnormalizedTaylorBlockV1
                | NormalStateCatalog::UnnormalizedJointBlockV1
        ) {
            algorithm_scratch_bytes = checked_add(
                algorithm_scratch_bytes,
                checked_add(
                    maximum_member_payload_bytes,
                    maximum_member_validity_bytes,
                    "coupled member arrays",
                )?,
                "coupled member production scratch",
            )?;
        }
        algorithm_scratch_bytes = checked_add(
            algorithm_scratch_bytes,
            checked_add(
                maximum_window_payload_bytes,
                maximum_window_validity_bytes,
                "active product window",
            )?,
            "windowed production scratch",
        )?;
        // Each lane can retain a completed window while another lane still owns
        // its plane workspace. Beam fitting joins before these windows begin.
        algorithm_scratch_bytes = algorithm_scratch_bytes.checked_mul(workers as u64).ok_or(
            ProductsError::ResourceDemandOverflow("parallel product windows"),
        )?;
        if !matches!(
            inputs.normal_state().catalog(),
            NormalStateCatalog::UnnormalizedTaylorBlockV1
                | NormalStateCatalog::UnnormalizedJointBlockV1
        ) {
            algorithm_scratch_bytes = checked_add(
                algorithm_scratch_bytes,
                bytes_for::<Option<crate::ProductWindow>>(workers, "product window slots")?,
                "parallel product slots",
            )?;
        }
        let (retained_metadata_bytes, member_beam_bytes, beam_scratch_bytes) =
            self.metadata_demand(inputs, workers)?;
        let transient_bytes = if matches!(
            inputs.normal_state().catalog(),
            NormalStateCatalog::UnnormalizedTaylorBlockV1
                | NormalStateCatalog::UnnormalizedJointBlockV1
        ) {
            checked_add(
                algorithm_scratch_bytes,
                beam_scratch_bytes,
                "coupled beam workspace",
            )?
        } else {
            checked_add(
                algorithm_scratch_bytes,
                member_beam_bytes,
                "active member preparation",
            )?
            .max(beam_scratch_bytes)
        };
        let peak_residency_bytes = checked_add(
            retained_metadata_bytes,
            transient_bytes,
            "product generation residency",
        )?;
        Ok(ContinuumGenerationDemand {
            storage_plan,
            algorithm_scratch_bytes,
            retained_metadata_bytes,
            beam_scratch_bytes,
            maximum_member_payload_bytes,
            maximum_member_validity_bytes,
            maximum_window_payload_bytes,
            maximum_window_validity_bytes,
            peak_residency_bytes,
        })
    }

    fn metadata_demand(
        &self,
        inputs: &ContinuumProductInputs<'_>,
        workers: usize,
    ) -> Result<(u64, u64, u64), ProductsError> {
        let state = inputs.normal_state();
        let domains = state.domain_count();
        if domains == 0 {
            return Err(ProductsError::SourceLineageMismatch);
        }
        let requires_beam = self
            .members()
            .iter()
            .any(|member| member.beam_rule() != ProductBeamRule::None);
        let fitted = if !requires_beam {
            0
        } else {
            match state.catalog() {
                NormalStateCatalog::UnnormalizedTaylorBlockV1 => 1,
                NormalStateCatalog::UnnormalizedJointBlockV1 => state.channel_count(),
                _ => domains
                    .checked_mul(state.channel_count())
                    .and_then(|count| count.checked_mul(state.polarization_count()))
                    .ok_or(ProductsError::ResourceDemandOverflow(
                        "generation beam count",
                    ))?,
            }
        };
        let restoring_policy = inputs.problem().products().restoring_beam();
        let restoring = match (restoring_policy, state.catalog()) {
            (RestoringBeamPolicy::None, _) => 0,
            (_, NormalStateCatalog::UnnormalizedTaylorBlockV1) => 1,
            _ => fitted,
        };
        let mut retained = checked_add(
            size_of::<PublishedContinuumGeneration>() as u64,
            bytes_for::<PublishedMember>(self.members().len(), "published members")?,
            "generation summary",
        )?;
        for count in [fitted, restoring] {
            retained = checked_add(
                retained,
                bytes_for::<Option<RestoringBeam>>(count, "generation beams")?,
                "generation metadata",
            )?;
        }
        let mut active_member_beams = 0;
        for member in self.members() {
            let domain_name = match member.axes().domain() {
                ImageDomainRole::Main => 0,
                ImageDomainRole::Outlier(name) => name.len(),
            };
            let spectral_values = match member.axes().spectral().wcs() {
                SpectralWcs::Linear { .. } => 0,
                SpectralWcs::Tabular {
                    channel_centres_hz,
                    channel_boundaries_hz,
                } => channel_centres_hz
                    .len()
                    .checked_add(channel_boundaries_hz.len())
                    .ok_or(ProductsError::ResourceDemandOverflow("member WCS"))?,
            };
            let beams = match member.beam_rule() {
                ProductBeamRule::None
                | ProductBeamRule::Restoring(RestoringBeamPolicy::None)
                | ProductBeamRule::Metadata(RestoringBeamPolicy::None) => 0,
                ProductBeamRule::Fitted => fitted / domains,
                ProductBeamRule::Restoring(RestoringBeamPolicy::Common)
                | ProductBeamRule::Metadata(RestoringBeamPolicy::Common) => 1,
                _ => restoring / domains,
            };
            let beam_bytes = bytes_for::<Option<RestoringBeam>>(beams, "member beams")?;
            active_member_beams = active_member_beams.max(beam_bytes);
            for bytes in [
                bytes_for::<u8>(member.name().len(), "member name")?,
                bytes_for::<u8>(domain_name, "member domain")?,
                bytes_for::<f64>(spectral_values, "member WCS")?,
                std::mem::size_of_val(member.axes().polarization()) as u64,
                std::mem::size_of_val(member.dependencies()) as u64,
                beam_bytes,
            ] {
                retained = checked_add(retained, bytes, "member metadata")?;
            }
        }
        let common_scratch = if restoring_policy == RestoringBeamPolicy::Common
            && matches!(
                state.catalog(),
                NormalStateCatalog::UnnormalizedPlaneV1
                    | NormalStateCatalog::UnnormalizedChannelSlabV1
            ) {
            checked_add(
                bytes_for::<RestoringBeam>(fitted / domains, "common valid beams")?,
                checked_mul(
                    bytes_for::<casa_numerics::EllipticalGaussian>(
                        fitted / domains,
                        "common numeric beams",
                    )?,
                    2,
                    "common converted and mutable numeric beams",
                )?,
                "common beam scratch",
            )?
        } else {
            0
        };
        let mut fitting_scratch = 0;
        if requires_beam {
            for ordinal in 0..domains {
                let shape = state
                    .domain_shape(ordinal)
                    .ok_or(ProductsError::SourceLineageMismatch)?;
                let cells = checked_shape_values([shape[0], shape[1], 1, 1])?;
                let input = bytes_for::<f32>(cells, "beam real PSF")?;
                // Paged PSFs own the decoded complex plane while converting to
                // f32; fitting then retains only f32 and the fit workspace.
                let conversion = bytes_for::<Complex64>(cells, "beam complex PSF")?;
                fitting_scratch = fitting_scratch.max(
                    checked_add(conversion, input, "beam input conversion")?.max(checked_add(
                        input,
                        casa_imaging_reconstruction::psf_fit_workspace_bytes(shape),
                        "beam fitting workspace",
                    )?),
                );
            }
            fitting_scratch = checked_mul(fitting_scratch, workers as u64, "parallel beam fits")?;
            fitting_scratch = checked_add(
                fitting_scratch,
                bytes_for::<Option<Option<RestoringBeam>>>(workers, "beam summary slots")?,
                "parallel beam summaries",
            )?;
        }
        Ok((
            retained,
            active_member_beams,
            active_member_beams.max(common_scratch).max(fitting_scratch),
        ))
    }
}

fn generic_scratch_bytes(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
) -> Result<u64, ProductsError> {
    let requires_validity = planned
        .members()
        .iter()
        .any(|member| member.validity() != ProductValidityRule::All);
    let requires_restoration = planned
        .members()
        .iter()
        .any(|member| matches!(member.role(), ProductRole::RestoredImage(_)));
    let mut maximum = 0;
    let mut normal_window = 0;
    for ordinal in 0..inputs.normal_state().domain_count() {
        let shape = inputs
            .normal_state()
            .domain_shape(ordinal)
            .ok_or(ProductsError::SourceLineageMismatch)?;
        let cells = checked_shape_values([shape[0], shape[1], 1, 1])?;
        let plane = bytes_for::<f32>(cells, "generic plane")?;
        let validity = bytes_for::<bool>(cells, "generic validity plane")?;
        // The full-window allowance conservatively bounds field-selective reads,
        // including the scalar-to-complex conversion overlap.
        normal_window = checked_add(
            normal_window,
            casa_imaging_reconstruction::normal_state_window_residency_bytes(
                shape,
                inputs.normal_state().polarization_count(),
                inputs.normal_state().channel_count(),
                1,
            )?,
            "generic normal-state input window",
        )?;
        let mut scratch = if requires_validity { validity } else { 0 };

        // Normalization overlaps one converted source plane with its result.
        scratch = scratch.max(checked_mul(plane, 2, "generic converted plane pair")?);
        if requires_restoration {
            // Restoration retains its restored result and normalized residual while
            // a Gaussian kernel and one exact rustfft convolution workspace live.
            scratch = scratch.max(checked_add(
                checked_mul(plane, 3, "generic restoration planes")?,
                fft_convolution_workspace_bytes(shape)?,
                "generic restoration scratch",
            )?);
        }
        maximum = maximum.max(checked_add(
            scratch,
            bytes_for::<casa_imaging_model::ModelSample>(cells, "generic model input plane")?,
            "generic loaded model and algorithm scratch",
        )?);
    }
    checked_add(
        maximum,
        normal_window,
        "generic loaded normal state and algorithm scratch",
    )
}

fn taylor_scratch_bytes(inputs: &ContinuumProductInputs<'_>) -> Result<u64, ProductsError> {
    let state = inputs.normal_state();
    let shape = state.shape();
    let cells = checked_shape_values([shape[0], shape[1], 1, 1])?;
    let terms = u64::try_from(state.coefficient_term_count())
        .map_err(|_| ProductsError::ResourceDemandOverflow("Taylor term count"))?;
    let moments = u64::try_from(state.normal_moment_count())
        .map_err(|_| ProductsError::ResourceDemandOverflow("Taylor moment count"))?;
    if terms < 2 || moments != terms.saturating_mul(2).saturating_sub(1) {
        return Err(ProductsError::SourceLineageMismatch);
    }
    let cells = u64::try_from(cells)
        .map_err(|_| ProductsError::ResourceDemandOverflow("Taylor cell count"))?;

    // TaylorProducts retains PSF and weight moments; residual, model,
    // restored, PB and PB-corrected term families; alpha, PB-corrected alpha,
    // alpha-error, sensitivity and mask;
    // two validity planes; and scalar sum weights until every planned member
    // has been copied into the produced generation.
    let retained_f32_planes = checked_add(
        checked_mul(moments, 2, "Taylor PSF and weight planes")?,
        checked_add(
            checked_mul(terms, 5, "Taylor term families")?,
            5,
            "Taylor scalar product planes",
        )?,
        "Taylor retained plane count",
    )?;
    let retained = checked_add(
        checked_mul(
            checked_mul(retained_f32_planes, cells, "Taylor retained values")?,
            size_of::<f32>() as u64,
            "Taylor retained payload bytes",
        )?,
        checked_add(
            checked_mul(
                checked_mul(2, cells, "Taylor validity values")?,
                size_of::<bool>() as u64,
                "Taylor validity bytes",
            )?,
            checked_mul(moments, size_of::<f32>() as u64, "Taylor sum weights")?,
            "Taylor validity and sum-weight bytes",
        )?,
        "Taylor retained scratch",
    )?;

    // Construction additionally retains the principal residual family and PB0
    // source plane. The normal solve owns three dense term matrices plus two
    // term vectors at its peak. This intentionally follows the product owner's
    // concrete current data structures, rather than a runtime approximation.
    let transient_planes = checked_mul(
        checked_add(terms, 1, "Taylor transient plane count")?,
        checked_mul(
            cells,
            size_of::<f32>() as u64,
            "Taylor transient plane bytes",
        )?,
        "Taylor transient planes",
    )?;
    let term_square = checked_mul(terms, terms, "Taylor normal matrix values")?;
    let solve = checked_mul(
        checked_add(
            checked_mul(term_square, 3, "Taylor normal matrices")?,
            checked_mul(terms, 2, "Taylor solve vectors")?,
            "Taylor solve values",
        )?,
        size_of::<f64>() as u64,
        "Taylor solve bytes",
    )?;
    let mut total = checked_add(
        retained,
        checked_add(transient_planes, solve, "Taylor transient scratch")?,
        "Taylor algorithm scratch",
    )?;
    if inputs.problem().products().restoring_beam() != RestoringBeamPolicy::None {
        total = checked_add(
            total,
            checked_add(
                checked_mul(cells, size_of::<f32>() as u64, "Taylor restoring kernel")?,
                fft_convolution_workspace_bytes(shape)?,
                "Taylor convolution scratch",
            )?,
            "Taylor restoration scratch",
        )?;
    }
    Ok(total)
}

fn fft_convolution_workspace_bytes(shape: [usize; 2]) -> Result<u64, ProductsError> {
    let cells = checked_shape_values([shape[0], shape[1], 1, 1])?;
    let complex_planes = bytes_for::<Complex64>(
        cells
            .checked_mul(2)
            .ok_or(ProductsError::ResourceDemandOverflow("FFT complex planes"))?,
        "FFT complex planes",
    )?;
    let output = bytes_for::<f32>(cells, "FFT output plane")?;
    let mut planner = FftPlanner::<f64>::new();
    let mut lane_values = 0usize;
    for length in shape {
        for plan in [
            planner.plan_fft_forward(length),
            planner.plan_fft_inverse(length),
        ] {
            lane_values = lane_values.max(
                length
                    .checked_add(plan.get_inplace_scratch_len())
                    .ok_or(ProductsError::ResourceDemandOverflow("FFT lane scratch"))?,
            );
        }
    }
    checked_add(
        checked_add(complex_planes, output, "FFT planes and output")?,
        bytes_for::<Complex64>(lane_values, "FFT lane and scratch")?,
        "FFT convolution workspace",
    )
}

fn checked_shape_values(shape: [usize; 4]) -> Result<usize, ProductsError> {
    shape.into_iter().try_fold(1usize, |values, extent| {
        values
            .checked_mul(extent)
            .ok_or(ProductsError::ResourceDemandOverflow("member shape"))
    })
}

fn bytes_for<T>(count: usize, what: &'static str) -> Result<u64, ProductsError> {
    let count = u64::try_from(count).map_err(|_| ProductsError::ResourceDemandOverflow(what))?;
    checked_mul(count, size_of::<T>() as u64, what)
}

fn checked_add(left: u64, right: u64, what: &'static str) -> Result<u64, ProductsError> {
    left.checked_add(right)
        .ok_or(ProductsError::ResourceDemandOverflow(what))
}

fn checked_mul(left: u64, right: u64, what: &'static str) -> Result<u64, ProductsError> {
    left.checked_mul(right)
        .ok_or(ProductsError::ResourceDemandOverflow(what))
}

#[cfg(test)]
mod tests {
    #[test]
    fn demand_arithmetic_fails_closed() {
        assert!(matches!(
            super::checked_mul(u64::MAX, 2, "test"),
            Err(crate::ProductsError::ResourceDemandOverflow("test"))
        ));
        assert!(matches!(
            super::checked_shape_values([usize::MAX, 2, 1, 1]),
            Err(crate::ProductsError::ResourceDemandOverflow("member shape"))
        ));
    }
}
