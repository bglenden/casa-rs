// SPDX-License-Identifier: LGPL-3.0-or-later

//! A bounded, publication-equivalent cube plane for MVC conversion.

use casa_imaging_deconvolution::FinalNormalPlaneReader;
use casa_imaging_model::{ImageDomainRole, ProductRole, ProductTerm, ProductValidityRule};

use crate::generation::adjust_cube_residual;
use crate::plane::{
    PlaneMemberRequest, produce_plane_member, product_plane_validity, zero_invalid_plane_values,
};
use crate::source::supports_cube_taylor_beam;
use crate::{ContinuumProductInputs, PlannedContinuumGeneration, PlannedMember, ProductsError};

/// One published cube channel in x-major image order.
///
/// The caller owns and admits these three Float planes, and discards them
/// after adding the channel to the Taylor sum; no cube is retained.
pub struct MvcCubePlane {
    /// Normalized cube PSF.
    pub psf: Vec<f32>,
    /// Cube residual, with the common PB adjustment when supplied to inputs.
    pub residual: Vec<f32>,
    /// Published cutoff-applied cube primary beam.
    pub primary_beam: Vec<f32>,
    /// Published Float sum of weights.
    pub sum_weight: f32,
    /// Output-channel frequency, in Hz.
    pub frequency_hz: f64,
}

/// A published cube primary-beam channel and its Float sum weight.
///
/// The caller owns and admits `pixels`; reading this first-pass input does not
/// read or retain the channel's PSF or residual.
pub struct MvcCubePrimaryBeam {
    /// Cutoff-applied primary-beam pixels, in x-major order.
    pub pixels: Vec<f32>,
    /// Published Float sum of weights.
    pub sum_weight: f32,
}

/// Read only one cube PB and its sum weight for MVC's first reduction pass.
///
/// # Errors
///
/// The inputs are not a supported cube, required PB or sumwt products or
/// the beam model are missing, the channel is absent, or reading fails.
pub fn read_mvc_cube_primary_beam(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
    channel: usize,
) -> Result<MvcCubePrimaryBeam, ProductsError> {
    let state = inputs.normal_state();
    if !supports_cube_taylor_beam(
        state.basis(),
        state.domain_count(),
        state.polarization_count(),
    ) {
        return Err(ProductsError::UnsupportedProblem);
    }
    planned
        .primary_beam_model()
        .ok_or(ProductsError::MissingPrimaryBeamModel)?;
    let pb = planned
        .members()
        .iter()
        .find(|member| member.role() == ProductRole::PrimaryBeam(ProductTerm::Single))
        .ok_or(ProductsError::UnsupportedProblem)?;
    if !planned
        .members()
        .iter()
        .any(|member| member.role() == ProductRole::SumWeights(ProductTerm::Single))
    {
        return Err(ProductsError::UnsupportedProblem);
    }
    let plane = state.read_plane(0, channel, 0)?;
    let mut pixels = raw_plane(pb, &plane, planned, inputs)?;
    apply_validity(&mut pixels, pb, &plane, planned, inputs)?;
    let sum_weight = plane.sum_weight() as f32;
    if !sum_weight.is_finite() {
        return Err(ProductsError::GeneratedNonfinite);
    }
    Ok(MvcCubePrimaryBeam { pixels, sum_weight })
}

/// Read one channel through the same normal-state and product rules used to
/// publish the cube. The caller admits the returned planes and transient
/// product workspace before reading, and retains the inputs' common PB.
///
/// # Errors
///
/// The inputs are not a single-domain, single-polarization cube with PSF,
/// residual, PB and sumwt products, the channel is absent, or reading or
/// forming its products fails.
pub fn read_mvc_cube_plane(
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
    channel: usize,
) -> Result<MvcCubePlane, ProductsError> {
    let state = inputs.normal_state();
    if !supports_cube_taylor_beam(
        state.basis(),
        state.domain_count(),
        state.polarization_count(),
    ) {
        return Err(ProductsError::UnsupportedProblem);
    }
    planned
        .primary_beam_model()
        .ok_or(ProductsError::MissingPrimaryBeamModel)?;
    let member = |role| {
        planned
            .members()
            .iter()
            .find(|member| member.role() == role)
            .ok_or(ProductsError::UnsupportedProblem)
    };
    let psf_member = member(ProductRole::Psf(ProductTerm::Single))?;
    let residual_member = member(ProductRole::Residual(ProductTerm::Single))?;
    let pb_member = member(ProductRole::PrimaryBeam(ProductTerm::Single))?;
    member(ProductRole::SumWeights(ProductTerm::Single))?;
    let plane = state.read_plane(0, channel, 0)?;
    let psf = make_plane(psf_member, &plane, planned, inputs)?;
    let mut residual = raw_plane(residual_member, &plane, planned, inputs)?;
    // The raw PB is needed for the residual adjustment; publication blanks
    // its unsupported values only after that adjustment has been evaluated.
    let mut primary_beam = raw_plane(pb_member, &plane, planned, inputs)?;
    if let Some(common) = inputs.cube_taylor_primary_beam() {
        adjust_cube_residual(
            &mut residual,
            common,
            &primary_beam,
            inputs
                .problem()
                .products()
                .validity()
                .primary_beam()
                .cutoff(),
        )?;
    }
    apply_validity(&mut residual, residual_member, &plane, planned, inputs)?;
    apply_validity(&mut primary_beam, pb_member, &plane, planned, inputs)?;
    let sum_weight = plane.sum_weight() as f32;
    if !sum_weight.is_finite() {
        return Err(ProductsError::GeneratedNonfinite);
    }
    let frequency_hz = inputs
        .problem()
        .geometry()
        .spectral()
        .channel_centre_hz(channel)
        .ok_or(ProductsError::UnsupportedProblem)?;
    Ok(MvcCubePlane {
        psf,
        residual,
        primary_beam,
        sum_weight,
        frequency_hz,
    })
}

fn raw_plane(
    member: &PlannedMember,
    plane: &FinalNormalPlaneReader<'_>,
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
) -> Result<Vec<f32>, ProductsError> {
    produce_plane_member(&PlaneMemberRequest {
        member,
        inputs,
        plane,
        domain_ordinal: 0,
        polarization: 0,
        fitted_beam: None,
        restoring_beam: None,
        primary_beam_model: planned.primary_beam_model(),
        fft_threads: 1,
    })
}

fn make_plane(
    member: &PlannedMember,
    plane: &FinalNormalPlaneReader<'_>,
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
) -> Result<Vec<f32>, ProductsError> {
    let mut values = raw_plane(member, plane, planned, inputs)?;
    apply_validity(&mut values, member, plane, planned, inputs)?;
    Ok(values)
}

fn apply_validity(
    values: &mut [f32],
    member: &PlannedMember,
    plane: &FinalNormalPlaneReader<'_>,
    planned: &PlannedContinuumGeneration,
    inputs: &ContinuumProductInputs<'_>,
) -> Result<(), ProductsError> {
    match member.validity() {
        ProductValidityRule::All => {}
        ProductValidityRule::PrimaryBeam(policy)
            if matches!(member.role(), ProductRole::PrimaryBeam(_)) =>
        {
            for value in values {
                if *value <= policy.cutoff() {
                    *value = 0.0;
                }
            }
        }
        rule => {
            let valid = product_plane_validity(
                rule,
                plane,
                planned.primary_beam_model(),
                inputs,
                (&ImageDomainRole::Main, 0),
            )?;
            zero_invalid_plane_values(values, &valid);
        }
    }
    Ok(())
}
