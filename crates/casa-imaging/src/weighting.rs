// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-style imaging-weight preparation for the pure imaging core.

use ndarray::Array2;

use crate::{
    GaussianUvTaper, ImagingRequest, UvTaperSize, VisibilityBatch, WeightDensityMode,
    WeightingMode,
    gridder::{DensityCellConvention, StandardGridder},
};

pub(crate) fn apply_weighting(
    request: &ImagingRequest,
    gridder: &StandardGridder,
) -> Result<Vec<VisibilityBatch>, crate::ImagingError> {
    apply_weighting_with_density_source(
        request.weighting,
        WeightDensityMode::Combined,
        None,
        &request.visibility_batches,
        &request.visibility_batches,
        gridder,
    )
}

pub(crate) fn apply_weighting_with_density_source(
    weighting: WeightingMode,
    weight_density_mode: WeightDensityMode,
    uv_taper: Option<GaussianUvTaper>,
    target_batches: &[VisibilityBatch],
    density_batches: &[VisibilityBatch],
    gridder: &StandardGridder,
) -> Result<Vec<VisibilityBatch>, crate::ImagingError> {
    let density_convention = density_cell_convention(weighting, weight_density_mode);
    match weighting {
        WeightingMode::Natural => Ok(apply_optional_uv_taper(target_batches.to_vec(), uv_taper)),
        WeightingMode::Uniform => {
            let density = build_density_grid(density_batches, gridder, true, density_convention);
            Ok(apply_optional_uv_taper(
                target_batches
                    .iter()
                    .map(|batch| {
                        reweight_batch(
                            batch,
                            gridder,
                            &density,
                            density_convention,
                            |weight, density| weight / density,
                        )
                    })
                    .collect(),
                uv_taper,
            ))
        }
        WeightingMode::Briggs { robust } => {
            let density = build_density_grid(density_batches, gridder, true, density_convention);
            let total_density_weight = density.iter().map(|value| f64::from(*value)).sum::<f64>();
            let sumlocwt = density
                .iter()
                .filter(|value| **value > 0.0)
                .map(|value| f64::from(*value) * f64::from(*value))
                .sum::<f64>();
            let f2 = if sumlocwt > 0.0 && total_density_weight > 0.0 {
                (5.0f64 * 10f64.powf(-(robust as f64))).powi(2) / (sumlocwt / total_density_weight)
            } else {
                0.0
            } as f32;
            Ok(apply_optional_uv_taper(
                target_batches
                    .iter()
                    .map(|batch| {
                        reweight_batch(
                            batch,
                            gridder,
                            &density,
                            density_convention,
                            |weight, density| weight / (f2 * density + 1.0),
                        )
                    })
                    .collect(),
                uv_taper,
            ))
        }
    }
}

fn density_cell_convention(
    weighting: WeightingMode,
    weight_density_mode: WeightDensityMode,
) -> DensityCellConvention {
    match (weighting, weight_density_mode) {
        (WeightingMode::Uniform | WeightingMode::Briggs { .. }, WeightDensityMode::PerPlane) => {
            DensityCellConvention::CubeBriggsWeightor
        }
        _ => DensityCellConvention::VisImagingWeight,
    }
}

fn apply_optional_uv_taper(
    mut batches: Vec<VisibilityBatch>,
    taper: Option<GaussianUvTaper>,
) -> Vec<VisibilityBatch> {
    let Some(taper) = taper else {
        return batches;
    };
    let (major_coeff, minor_coeff) = taper_coefficients(taper);
    let cos_pa = taper.position_angle_rad.sin();
    let sin_pa = taper.position_angle_rad.cos();
    for batch in &mut batches {
        for index in 0..batch.len() {
            let weight = batch.weight[index];
            if !(weight.is_finite() && weight > 0.0) {
                batch.weight[index] = 0.0;
                continue;
            }
            let u = batch.u_lambda[index] as f64;
            let v = batch.v_lambda[index] as f64;
            let ru = cos_pa * u + sin_pa * v;
            let rv = -sin_pa * u + cos_pa * v;
            let filter = (-major_coeff * ru * ru - minor_coeff * rv * rv).exp() as f32;
            batch.weight[index] *= filter;
        }
    }
    batches
}

fn taper_coefficients(taper: GaussianUvTaper) -> (f64, f64) {
    let image_factor = std::f64::consts::PI * std::f64::consts::PI / (4.0 * std::f64::consts::LN_2);
    let major = match taper.major {
        UvTaperSize::ImageFwhmRad(value) => image_factor * value * value,
        UvTaperSize::BaselineHwhmLambda(value) => std::f64::consts::LN_2 / (value * value),
    };
    let minor = match taper.minor {
        UvTaperSize::ImageFwhmRad(value) => image_factor * value * value,
        UvTaperSize::BaselineHwhmLambda(value) => std::f64::consts::LN_2 / (value * value),
    };
    (major, minor)
}

fn build_density_grid(
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    mirror_hermitian: bool,
    convention: DensityCellConvention,
) -> Array2<f32> {
    let [nx, ny] = gridder.density_grid_shape();
    let mut density_grid = Array2::<f32>::zeros((nx, ny));
    for batch in batches {
        for index in 0..batch.len() {
            let weight = batch.weight[index];
            if !(weight.is_finite() && weight > 0.0) {
                continue;
            }
            let primary = (batch.u_lambda[index], batch.v_lambda[index]);
            let conjugate = (-batch.u_lambda[index], -batch.v_lambda[index]);
            let positions = if mirror_hermitian {
                [Some(primary), Some(conjugate)]
            } else {
                [Some(primary), None]
            };
            for (u_lambda, v_lambda) in positions.into_iter().flatten() {
                let Some((x, y)) =
                    gridder.density_cell_index_with_convention(u_lambda, v_lambda, convention)
                else {
                    continue;
                };
                density_grid[(x, y)] += weight;
            }
        }
    }
    density_grid
}

fn reweight_batch(
    batch: &VisibilityBatch,
    gridder: &StandardGridder,
    density: &Array2<f32>,
    convention: DensityCellConvention,
    transform: impl Fn(f32, f32) -> f32,
) -> VisibilityBatch {
    let mut reweighted = batch.clone();
    for index in 0..batch.len() {
        let weight = batch.weight[index];
        let Some(cell_density) = gridder.density_at_with_convention(
            density,
            batch.u_lambda[index],
            batch.v_lambda[index],
            convention,
        ) else {
            reweighted.weight[index] = 0.0;
            continue;
        };
        if !(weight.is_finite() && weight > 0.0 && cell_density.is_finite() && cell_density > 0.0) {
            reweighted.weight[index] = 0.0;
            continue;
        }
        reweighted.weight[index] = transform(weight, cell_density);
    }
    reweighted
}

#[cfg(test)]
mod tests {
    use num_complex::Complex32;

    use super::*;
    use crate::{
        CleanConfig, CompatibilityMode, Deconvolver, ImageGeometry, ImagingRequest, PlaneStokes,
    };

    fn request_for(mode: WeightingMode) -> ImagingRequest {
        ImagingRequest {
            geometry: ImageGeometry {
                image_shape: [128, 128],
                cell_size_rad: [1.0e-4, 1.0e-4],
            },
            visibility_batches: vec![VisibilityBatch {
                u_lambda: vec![0.0, 0.0, 0.0, 0.0, 320.0],
                v_lambda: vec![0.0, 0.0, 0.0, 0.0, 280.0],
                w_lambda: vec![0.0; 5],
                weight: vec![1.0; 5],
                sumwt_factor: vec![1.0; 5],
                gridable: vec![true; 5],
                visibility: vec![Complex32::new(1.0, 0.0); 5],
            }],
            plane_stokes: PlaneStokes::I,
            weighting: mode,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            w_term_mode: crate::WTermMode::None,
            compatibility: CompatibilityMode::CasaStandardMfs,
        }
    }

    #[test]
    fn uniform_weighting_downweights_dense_uv_regions() {
        let request = request_for(WeightingMode::Uniform);
        let gridder = StandardGridder::new(request.geometry).unwrap();
        let weighted = apply_weighting(&request, &gridder).unwrap();
        let dense_weight = weighted[0].weight[0];
        let sparse_weight = weighted[0].weight[4];
        assert!(dense_weight < sparse_weight);
    }

    #[test]
    fn briggs_extremes_interpolate_between_natural_and_uniform() {
        let geometry = request_for(WeightingMode::Natural).geometry;
        let gridder = StandardGridder::new(geometry).unwrap();
        let natural = apply_weighting(&request_for(WeightingMode::Natural), &gridder).unwrap();
        let uniform = apply_weighting(&request_for(WeightingMode::Uniform), &gridder).unwrap();
        let briggs_naturalish = apply_weighting(
            &request_for(WeightingMode::Briggs { robust: 2.0 }),
            &gridder,
        )
        .unwrap();
        let briggs_uniformish = apply_weighting(
            &request_for(WeightingMode::Briggs { robust: -2.0 }),
            &gridder,
        )
        .unwrap();

        let dense_index = 0usize;
        let sparse_index = 4usize;
        let natural_ratio = natural[0].weight[dense_index] / natural[0].weight[sparse_index];
        let uniform_ratio = uniform[0].weight[dense_index] / uniform[0].weight[sparse_index];
        let briggs_naturalish_ratio =
            briggs_naturalish[0].weight[dense_index] / briggs_naturalish[0].weight[sparse_index];
        let briggs_uniformish_ratio =
            briggs_uniformish[0].weight[dense_index] / briggs_uniformish[0].weight[sparse_index];

        assert!(
            (briggs_naturalish_ratio - natural_ratio).abs()
                < (briggs_uniformish_ratio - natural_ratio).abs()
        );
        assert!(
            (briggs_uniformish_ratio - uniform_ratio).abs()
                < (briggs_naturalish_ratio - uniform_ratio).abs()
        );
    }

    #[test]
    fn density_grid_accumulates_conjugate_samples_for_uniform_weighting() {
        let geometry = ImageGeometry {
            image_shape: [32, 32],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let gridder = StandardGridder::new(geometry).unwrap();
        let du = 1.0 / (geometry.image_shape[0] as f64 * geometry.cell_size_rad[0]);
        let batch = VisibilityBatch {
            u_lambda: vec![1.2 * du],
            v_lambda: vec![-1.2 * du],
            w_lambda: vec![0.0],
            weight: vec![2.0],
            sumwt_factor: vec![1.0],
            gridable: vec![true],
            visibility: vec![Complex32::new(1.0, 0.0)],
        };
        let density = build_density_grid(
            &[batch],
            &gridder,
            true,
            DensityCellConvention::VisImagingWeight,
        );
        assert_eq!(density[(17, 17)], 2.0);
        assert_eq!(density[(14, 14)], 2.0);
    }
}
