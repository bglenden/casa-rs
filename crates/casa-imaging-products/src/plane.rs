// SPDX-License-Identifier: LGPL-3.0-or-later

//! One member's values on one normal-state plane, and where a plane's
//! values go in a member's storage order.

use casa_imaging_deconvolution::{
    FinalNormalPlaneReader, ModelGeneration, ReconstructionMask, SpectralChannelValidity,
};
use casa_imaging_model::{
    AxisOrder, ImageAxis, ImageDomainRole, ProductAxes, ProductNormalization, ProductRole,
    ProductSupportComparison, ProductValidityRule,
};

use crate::beam::RestoringBeam;
use crate::error::ProductsError;
use crate::normalization::{MosaicSensitivity, normalize_plane, normalized_psf_value, psf_peak};
use crate::plan::{AnalyticPrimaryBeamModel, PlannedMember};
use crate::restore::{rescale_residual_to_beam, restore_model_plane};
use crate::source::ContinuumProductInputs;
use crate::taylor::{
    analytic_alma_airy_primary_beam, analytic_evla_primary_beam, analytic_vla_primary_beam,
};

pub(crate) struct PlaneMemberRequest<'request, 'inputs, 'plane> {
    pub(crate) member: &'request PlannedMember,
    pub(crate) inputs: &'request ContinuumProductInputs<'inputs>,
    pub(crate) plane: &'request FinalNormalPlaneReader<'plane>,
    pub(crate) domain_ordinal: usize,
    pub(crate) polarization: usize,
    pub(crate) fitted_beam: Option<RestoringBeam>,
    pub(crate) restoring_beam: Option<RestoringBeam>,
    pub(crate) primary_beam_model: Option<AnalyticPrimaryBeamModel>,
    pub(crate) fft_threads: usize,
}

pub(crate) fn produce_plane_member(
    request: &PlaneMemberRequest<'_, '_, '_>,
) -> Result<Vec<f32>, ProductsError> {
    let PlaneMemberRequest {
        member,
        inputs,
        plane,
        primary_beam_model,
        ..
    } = *request;
    let scalar_sensitivity = plane.sum_weight();
    let valid = plane.validity() == SpectralChannelValidity::Valid
        && scalar_sensitivity.is_finite()
        && scalar_sensitivity > 0.0;
    let shape = plane.shape();
    let cells = shape[0] * shape[1];
    let invalid_residual = || vec![0.0; cells];
    match member.role {
        ProductRole::Psf(casa_imaging_model::ProductTerm::Single)
        | ProductRole::Psf(casa_imaging_model::ProductTerm::Taylor(0)) => {
            if valid {
                let mut values = psf_real_plane(plane)?;
                let peak = psf_peak(values.iter().copied())?;
                for value in &mut values {
                    *value = normalized_psf_value(*value, peak);
                }
                Ok(values)
            } else {
                Ok(vec![0.0; cells])
            }
        }
        ProductRole::Residual(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        ) => {
            if valid {
                normalize_domain_plane(
                    &residual_real_plane(plane)?,
                    required_normalization(member),
                    plane,
                    inputs,
                )
            } else {
                Ok(invalid_residual())
            }
        }
        ProductRole::Weight(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        ) => {
            let scale = if primary_beam_model == Some(AnalyticPrimaryBeamModel::MosaicSensitivity) {
                // CASA publishes an empty plane's weight as zeros
                // (`SIImageStore::divideImageByWeightVal`).
                if !valid {
                    return Ok(vec![0.0; cells]);
                }
                plane.sum_weight()
            } else {
                1.0
            };
            Ok(plane
                .read_sensitivity()?
                .iter()
                .map(|value| (*value / scale) as f32)
                .collect())
        }
        ProductRole::Sensitivity => Ok(plane
            .read_sensitivity()?
            .iter()
            .map(|value| *value as f32)
            .collect()),
        ProductRole::RestoredImage(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        ) => {
            if !valid {
                return Ok(invalid_residual());
            }
            restored_plane(request)
        }
        ProductRole::PrimaryBeam(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        ) => primary_beam_plane(primary_beam_model, inputs, member.axes().domain(), plane),
        ProductRole::PbCorrectedImage(
            casa_imaging_model::ProductTerm::Single | casa_imaging_model::ProductTerm::Taylor(0),
        ) => {
            if !valid {
                return Ok(invalid_residual());
            }
            let restored = restored_plane(request)?;
            let primary_beam =
                primary_beam_plane(primary_beam_model, inputs, member.axes().domain(), plane)?;
            correct_primary_beam(
                &restored,
                &primary_beam,
                inputs.problem().products().validity().primary_beam(),
            )
        }
        role => Err(ProductsError::UnsupportedProductRole { role }),
    }
}

fn normalize_domain_plane(
    values: &[f32],
    normalization: ProductNormalization,
    plane: &FinalNormalPlaneReader<'_>,
    inputs: &ContinuumProductInputs<'_>,
) -> Result<Vec<f32>, ProductsError> {
    match normalization {
        ProductNormalization::UnitResponse => Ok(normalize_plane(values, plane.sum_weight())),
        ProductNormalization::FlatNoise | ProductNormalization::FlatSky => {
            MosaicSensitivity::new(&plane.read_sensitivity()?)?.normalize(
                values,
                normalization,
                inputs.problem().products().validity().primary_beam(),
            )
        }
    }
}

/// The model in the units CASA restores and publishes: under flat-noise
/// with a direction-dependent sensitivity the apparent model, the physical
/// model the lifecycle holds times the unit-peak beam and zero outside its
/// support (`SynthesisNormalizer::multiplyModelByWeight` after every major
/// cycle); the physical model otherwise (flat-sky, or a scalar
/// sensitivity).
pub(crate) fn apparent_model(
    model: Vec<f32>,
    plane: &FinalNormalPlaneReader<'_>,
    normalization: ProductNormalization,
    policy: casa_imaging_model::PrimaryBeamValidityPolicy,
    primary_beam_model: Option<AnalyticPrimaryBeamModel>,
) -> Result<Vec<f32>, ProductsError> {
    if primary_beam_model != Some(AnalyticPrimaryBeamModel::MosaicSensitivity)
        || !matches!(
            normalization,
            ProductNormalization::FlatNoise | ProductNormalization::FlatSky
        )
    {
        return Ok(model);
    }
    let sensitivity = plane.read_sensitivity()?;
    // A plane no sample reached holds no model; CASA leaves it as it is.
    let Some(response) = MosaicSensitivity::try_new(&sensitivity)? else {
        return Ok(model);
    };
    let response = response.with_normal_sum_weight(plane.sum_weight())?;
    model
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            Ok(
                response.physical_to_apparent(f64::from(value), index, normalization, policy)?
                    as f32,
            )
        })
        .collect()
}

fn restored_plane(request: &PlaneMemberRequest<'_, '_, '_>) -> Result<Vec<f32>, ProductsError> {
    let PlaneMemberRequest {
        member,
        inputs,
        plane,
        domain_ordinal,
        polarization,
        fitted_beam,
        restoring_beam,
        primary_beam_model,
        fft_threads,
    } = *request;
    let unfitted = || {
        ProductsError::BeamFitFailed(
            "restoration requires a fitted beam for every valid plane".to_string(),
        )
    };
    let beam = restoring_beam.ok_or_else(unfitted)?;
    let fitted_beam = fitted_beam.ok_or_else(unfitted)?;
    let model = apparent_model(
        model_real_plane(
            inputs.final_model(),
            domain_ordinal,
            plane.output_channel(),
            polarization,
            plane.shape(),
        )?,
        plane,
        required_normalization(member),
        inputs.problem().products().validity().primary_beam(),
        primary_beam_model,
    )?;
    let cell_size = inputs.cell_size_rad_for_domain(member.axes().domain());
    let residual = normalize_domain_plane(
        &residual_real_plane(plane)?,
        required_normalization(member),
        plane,
        inputs,
    )?;
    let residual =
        rescale_residual_to_beam(&residual, plane.shape(), cell_size, fitted_beam, beam)?
            .into_values();
    Ok(restore_model_plane(
        &model,
        residual,
        plane.shape(),
        &beam,
        cell_size,
        inputs.normal_state().slab().total_channels(),
        fft_threads,
    ))
}

/// The CLEAN mask's support on one domain: 1 where the minor cycle could
/// clean, else 0; everywhere 1 without a mask.
pub(crate) fn reconstruction_support_plane(
    inputs: &ContinuumProductInputs<'_>,
    role: &ImageDomainRole,
    cells: usize,
) -> Vec<f32> {
    let mask = reconstruction_mask_for_domain(inputs, role);
    (0..cells)
        .map(|index| {
            if mask.is_none_or(|mask| mask.support()[index]) {
                1.0
            } else {
                0.0
            }
        })
        .collect()
}

fn reconstruction_mask_for_domain<'a>(
    inputs: &'a ContinuumProductInputs<'_>,
    role: &ImageDomainRole,
) -> Option<&'a ReconstructionMask> {
    if let Some(mask) = inputs.reconstruction_mask() {
        assert_eq!(
            role,
            &ImageDomainRole::Main,
            "a shared reconstruction mask is a single-domain run's"
        );
        return Some(mask);
    }
    inputs.domain_reconstruction_masks().map(|masks| {
        masks
            .get(inputs.model_domain_ordinal(role))
            .expect("the run's reconstruction masks are one per domain")
    })
}

/// The normalization the product list gives the residual, image and
/// PB-corrected members.
fn required_normalization(member: &PlannedMember) -> ProductNormalization {
    member
        .normalization
        .expect("the product list normalizes residual, image and pbcor members")
}

pub(crate) fn psf_real_plane(
    plane: &FinalNormalPlaneReader<'_>,
) -> Result<Vec<f32>, ProductsError> {
    Ok(plane
        .read_psf()?
        .iter()
        .map(|value| value.re as f32)
        .collect())
}

fn residual_real_plane(plane: &FinalNormalPlaneReader<'_>) -> Result<Vec<f32>, ProductsError> {
    Ok(plane
        .read_residual()?
        .iter()
        .map(|value| value.re as f32)
        .collect())
}

pub(crate) fn product_plane_validity(
    rule: ProductValidityRule,
    plane: &FinalNormalPlaneReader<'_>,
    primary_beam_model: Option<AnalyticPrimaryBeamModel>,
    inputs: &ContinuumProductInputs<'_>,
    domain_role: &ImageDomainRole,
) -> Result<Vec<bool>, ProductsError> {
    let shape = plane.shape();
    match rule {
        ProductValidityRule::All => Ok(vec![true; shape[0] * shape[1]]),
        ProductValidityRule::FinalNormalState => {
            let valid = plane.validity() == SpectralChannelValidity::Valid
                && plane.sum_weight().is_finite()
                && plane.sum_weight() > 0.0;
            Ok(vec![valid; shape[0] * shape[1]])
        }
        ProductValidityRule::PrimaryBeam(policy) => {
            Ok(
                primary_beam_plane(primary_beam_model, inputs, domain_role, plane)?
                    .into_iter()
                    .map(|value| match policy.comparison() {
                        ProductSupportComparison::StrictlyGreater => value > policy.cutoff(),
                    })
                    .collect(),
            )
        }
        ProductValidityRule::Taylor(_) | ProductValidityRule::TaylorAndPrimaryBeam { .. } => {
            unreachable!(
                "Taylor validity rules belong to Taylor members, which the Taylor path makes"
            )
        }
    }
}

fn primary_beam_plane(
    model: Option<AnalyticPrimaryBeamModel>,
    inputs: &ContinuumProductInputs<'_>,
    domain_role: &ImageDomainRole,
    plane: &FinalNormalPlaneReader<'_>,
) -> Result<Vec<f32>, ProductsError> {
    match model {
        Some(AnalyticPrimaryBeamModel::CasaEvlaCommon) => {
            analytic_evla_primary_beam(inputs, domain_role, plane.shape(), plane.output_channel())
        }
        Some(AnalyticPrimaryBeamModel::CasaVlaBand) => {
            analytic_vla_primary_beam(inputs, domain_role, plane.shape(), plane.output_channel())
        }
        Some(AnalyticPrimaryBeamModel::CasaAlma12mAiry) => analytic_alma_airy_primary_beam(
            inputs,
            domain_role,
            plane.shape(),
            plane.output_channel(),
            10.7,
        ),
        Some(AnalyticPrimaryBeamModel::CasaAca7mAiry) => analytic_alma_airy_primary_beam(
            inputs,
            domain_role,
            plane.shape(),
            plane.output_channel(),
            6.25,
        ),
        Some(AnalyticPrimaryBeamModel::MosaicSensitivity) => {
            let shape = plane.shape();
            Ok(
                MosaicSensitivity::try_new(&plane.read_sensitivity()?)?.map_or_else(
                    || vec![0.0; shape[0] * shape[1]],
                    |sensitivity| sensitivity.primary_beam(),
                ),
            )
        }
        None => unreachable!("planning refuses a PB product or mask without a primary-beam model"),
    }
}

fn correct_primary_beam(
    values: &[f32],
    primary_beam: &[f32],
    policy: casa_imaging_model::PrimaryBeamValidityPolicy,
) -> Result<Vec<f32>, ProductsError> {
    values
        .iter()
        .zip(primary_beam)
        .map(|(value, beam)| {
            let valid = match policy.comparison() {
                ProductSupportComparison::StrictlyGreater => *beam > policy.cutoff(),
            };
            if !valid {
                return Ok(0.0);
            }
            let corrected = *value / *beam;
            corrected
                .is_finite()
                .then_some(corrected)
                .ok_or(ProductsError::GeneratedNonfinite)
        })
        .collect()
}

pub(crate) fn zero_invalid_plane_values(payload: &mut [f32], validity: &[bool]) {
    for (value, valid) in payload.iter_mut().zip(validity) {
        if !*valid {
            *value = 0.0;
        }
    }
}

pub(crate) fn model_real_plane(
    model: &ModelGeneration,
    domain_ordinal: usize,
    output_channel: usize,
    polarization: usize,
    plane_shape: [usize; 2],
) -> Result<Vec<f32>, ProductsError> {
    let [width, height] = plane_shape;
    // Canonical model order is y-major (`flat = y * W + x`); product planes
    // are stored x-major like every normal-state primitive.
    let mut plane = vec![0.0_f32; width * height];
    let samples = model.read_plane(domain_ordinal, output_channel, polarization)?;
    for y in 0..height {
        for x in 0..width {
            plane[x * height + y] = samples[y * width + x].value().value() as f32;
        }
    }
    Ok(plane)
}

/// Write one x-major plane into a window in the member's storage order.
pub(crate) fn scatter_image_polarization_plane<T: Copy>(
    payload: &mut [T],
    order: &AxisOrder,
    storage_shape: [usize; 4],
    polarization: usize,
    output_channel: usize,
    plane_shape: [usize; 2],
    plane: &[T],
) {
    let [width, height] = plane_shape;
    debug_assert_eq!(width * height, plane.len());
    let base = product_offset(order, storage_shape, 0, 0, polarization, output_channel);
    let (mut longitude_stride, mut latitude_stride) = (0, 0);
    let mut stride = 1usize;
    for (axis, extent) in order.positions().iter().zip(storage_shape).rev() {
        match axis {
            ImageAxis::DirectionLongitude => longitude_stride = stride,
            ImageAxis::DirectionLatitude => latitude_stride = stride,
            _ => {}
        }
        stride *= extent;
    }
    for (x, column) in plane.chunks_exact(height).enumerate() {
        let start = base + x * longitude_stride;
        for (y, value) in column.iter().enumerate() {
            payload[start + y * latitude_stride] = *value;
        }
    }
}

/// Write one plane's scalar state, such as its sum of weights, at every
/// direction pixel: one per facet of a faceted domain, `value(facet)` at
/// facet `row·N + column`'s pixel `(column, row)` as CASA's facet stores
/// address `.sumwt` (`SIImageStore::getSubImageStore`), else the single
/// pixel's `value(0)`.
pub(crate) fn scatter_polarization_plane_state(
    payload: &mut [f32],
    axes: &ProductAxes,
    storage_shape: [usize; 4],
    polarization: usize,
    output_channel: usize,
    value: impl Fn(usize) -> f32,
) {
    let extent = |axis| {
        axes.order()
            .positions()
            .iter()
            .position(|candidate| *candidate == axis)
            .map_or(1, |position| storage_shape[position])
    };
    let columns = extent(ImageAxis::DirectionLongitude);
    for longitude in 0..columns {
        for latitude in 0..extent(ImageAxis::DirectionLatitude) {
            let offset = product_offset(
                axes.order(),
                storage_shape,
                longitude,
                latitude,
                polarization,
                output_channel,
            );
            payload[offset] = value(latitude * columns + longitude);
        }
    }
}

fn product_offset(
    order: &AxisOrder,
    storage_shape: [usize; 4],
    longitude: usize,
    latitude: usize,
    polarization: usize,
    spectral: usize,
) -> usize {
    order
        .positions()
        .iter()
        .zip(storage_shape)
        .fold(0, |offset, (axis, extent)| {
            let coordinate = match axis {
                ImageAxis::DirectionLongitude => longitude,
                ImageAxis::DirectionLatitude => latitude,
                ImageAxis::Polarization => polarization,
                ImageAxis::Spectral => spectral,
            };
            debug_assert!(coordinate < extent, "a plane lies inside its window");
            offset * extent + coordinate
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scatter_matches_scalar_offsets_for_all_axis_orders() {
        let axes = [
            ImageAxis::DirectionLongitude,
            ImageAxis::DirectionLatitude,
            ImageAxis::Polarization,
            ImageAxis::Spectral,
        ];
        for a in 0..4 {
            for b in (0..4).filter(|b| *b != a) {
                for c in (0..4).filter(|c| *c != a && *c != b) {
                    let d = (0..4).find(|d| *d != a && *d != b && *d != c).unwrap();
                    let order = AxisOrder::new([axes[a], axes[b], axes[c], axes[d]]);
                    let shape = [a, b, c, d].map(|axis| [3, 5, 2, 4][axis]);
                    let mut actual = vec![usize::MAX; 120];
                    let mut expected = actual.clone();
                    for polarization in 0..2 {
                        for channel in 0..4 {
                            let plane = (0..15)
                                .map(|value| value + 15 * (channel + 4 * polarization))
                                .collect::<Vec<_>>();
                            scatter_image_polarization_plane(
                                &mut actual,
                                &order,
                                shape,
                                polarization,
                                channel,
                                [3, 5],
                                &plane,
                            );
                            for x in 0..3 {
                                for y in 0..5 {
                                    let offset =
                                        product_offset(&order, shape, x, y, polarization, channel);
                                    expected[offset] = plane[x * 5 + y];
                                }
                            }
                        }
                    }
                    assert_eq!(actual, expected, "{:?}", order.positions());
                }
            }
        }
    }
}
