// SPDX-License-Identifier: LGPL-3.0-or-later

//! Exact numeric-array residency owned by continuum product generation.

use std::mem::size_of;

use casa_imaging_model::{ProductRole, ProductValidityRule, RestoringBeamPolicy};
use casa_imaging_reconstruction::NormalStateCatalog;
use num_complex::Complex64;
use rustfft::FftPlanner;

use crate::{ContinuumProductInputs, PlannedContinuumGeneration, ProductsError};

/// Checked numeric-array demand for one planned continuum generation.
///
/// This is deliberately non-persistent. It describes the current algorithm
/// catalog's owned payload, validity, and working arrays so the runtime can
/// acquire one bounded lease before production starts. Metadata containers,
/// allocator bookkeeping, and borrowed reconstruction inputs are not charged
/// here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinuumGenerationDemand {
    produced_residency_bytes: u64,
    sealed_residency_bytes: u64,
    algorithm_scratch_bytes: u64,
    maximum_member_payload_bytes: u64,
    maximum_member_validity_bytes: u64,
    peak_residency_bytes: u64,
}

impl ContinuumGenerationDemand {
    /// Numeric payload and validity bytes retained by produced members.
    #[must_use]
    pub const fn produced_residency_bytes(self) -> u64 {
        self.produced_residency_bytes
    }

    /// Numeric payload and validity bytes retained by sealed members.
    #[must_use]
    pub const fn sealed_residency_bytes(self) -> u64 {
        self.sealed_residency_bytes
    }

    /// Maximum product-algorithm working-array bytes retained with production.
    #[must_use]
    pub const fn algorithm_scratch_bytes(self) -> u64 {
        self.algorithm_scratch_bytes
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

    /// Peak owned numeric-array residency across production and authorization.
    ///
    /// Production overlaps the produced generation with algorithm scratch;
    /// authorization overlaps produced and sealed generations after scratch is
    /// released.
    #[must_use]
    pub const fn peak_residency_bytes(self) -> u64 {
        self.peak_residency_bytes
    }
}

impl PlannedContinuumGeneration {
    /// Derive the exact current-catalog numeric-array demand for these inputs.
    ///
    /// # Errors
    ///
    /// Fails closed when the inputs do not name this plan's problem and source
    /// generations, or when any byte calculation overflows.
    pub fn demand(
        &self,
        inputs: &ContinuumProductInputs<'_>,
    ) -> Result<ContinuumGenerationDemand, ProductsError> {
        if inputs.problem().problem_id() != self.problem_id()
            || inputs.final_model().generation_id() != self.final_model_generation()
            || inputs.reconstruction_mask_generation() != self.reconstruction_mask_generation()
            || inputs
                .coupled_reconstruction_masks()
                .map(|masks| masks.line().generation_id())
                != self.line_reconstruction_mask_generation()
        {
            return Err(ProductsError::CommitmentMismatch);
        }

        let mut produced_residency_bytes = 0_u64;
        let mut maximum_member_payload_bytes = 0_u64;
        let mut maximum_member_validity_bytes = 0_u64;
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
            produced_residency_bytes = checked_add(
                produced_residency_bytes,
                checked_add(payload, validity, "member payload plus validity")?,
                "produced generation residency",
            )?;
            maximum_member_payload_bytes = maximum_member_payload_bytes.max(payload);
            maximum_member_validity_bytes = maximum_member_validity_bytes.max(validity);
        }

        let algorithm_scratch_bytes = match inputs.normal_state().catalog() {
            NormalStateCatalog::UnnormalizedTaylorBlockV1
            | NormalStateCatalog::UnnormalizedJointBlockV1 => taylor_scratch_bytes(inputs)?,
            NormalStateCatalog::UnnormalizedPlaneV1
            | NormalStateCatalog::UnnormalizedChannelSlabV1 => generic_scratch_bytes(self, inputs)?,
        };
        let production_peak = checked_add(
            produced_residency_bytes,
            algorithm_scratch_bytes,
            "product production peak residency",
        )?;
        let authorization_peak = checked_add(
            produced_residency_bytes,
            produced_residency_bytes,
            "product authorization peak residency",
        )?;
        Ok(ContinuumGenerationDemand {
            produced_residency_bytes,
            sealed_residency_bytes: produced_residency_bytes,
            algorithm_scratch_bytes,
            maximum_member_payload_bytes,
            maximum_member_validity_bytes,
            peak_residency_bytes: production_peak.max(authorization_peak),
        })
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
    for domain in inputs.normal_state().domains() {
        let shape = domain.shape();
        let cells = checked_shape_values([shape[0], shape[1], 1, 1])?;
        let plane = bytes_for::<f32>(cells, "generic plane")?;
        let validity = bytes_for::<bool>(cells, "generic validity plane")?;
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
        maximum = maximum.max(scratch);
    }
    Ok(maximum)
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
