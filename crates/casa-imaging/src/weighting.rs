// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-style imaging-weight preparation for the pure imaging core.

use std::{env, thread};

use ndarray::{Array2, Zip};

use crate::{
    GaussianUvTaper, ImagingRequest, UvTaperSize, VisibilityBatch, WeightDensityMode,
    WeightingMode,
    gridder::{DensityCellConvention, StandardGridder},
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WeightingSampleTraceInternal {
    pub batch_index: usize,
    pub sample_index: usize,
    pub u_lambda: f64,
    pub v_lambda: f64,
    pub w_lambda: f64,
    pub input_weight: f32,
    pub density_weight: Option<f32>,
    pub output_weight: f32,
    pub sumwt_factor: f32,
    pub gridable: bool,
    pub normalization_contribution: f32,
    pub reported_contribution: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WeightingTraceInternal {
    pub weighted_batches: Vec<VisibilityBatch>,
    pub samples: Vec<WeightingSampleTraceInternal>,
    pub gridded_samples: usize,
    pub skipped_samples: usize,
    pub normalization_sumwt: f32,
    pub reported_sumwt: f32,
}

#[derive(Debug, Clone, Copy)]
enum DensityReweightMode {
    Uniform,
    Briggs {
        f2: f32,
        use_bandwidth_taper: bool,
        fractional_bandwidth: f64,
    },
}

fn standard_mfs_worker_threads() -> usize {
    env::var("CASA_RS_STANDARD_MFS_GRID_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1)
}

pub(crate) fn apply_weighting(
    request: &ImagingRequest,
    gridder: &StandardGridder,
) -> Result<Vec<VisibilityBatch>, crate::ImagingError> {
    apply_weighting_with_density_source(
        request.weighting,
        WeightDensityMode::Combined,
        None,
        fractional_bandwidth_from_frequency_range(request.selected_frequency_range_hz),
        &request.visibility_batches,
        &request.visibility_batches,
        gridder,
    )
}

pub(crate) fn apply_weighting_to_owned_batches(
    request: &ImagingRequest,
    gridder: &StandardGridder,
    batches: Vec<VisibilityBatch>,
) -> Result<Vec<VisibilityBatch>, crate::ImagingError> {
    apply_weighting_to_owned_batches_with_options(
        request.weighting,
        None,
        fractional_bandwidth_from_frequency_range(request.selected_frequency_range_hz),
        batches,
        gridder,
    )
}

fn apply_weighting_to_owned_batches_with_options(
    weighting: WeightingMode,
    uv_taper: Option<GaussianUvTaper>,
    fractional_bandwidth: f64,
    batches: Vec<VisibilityBatch>,
    gridder: &StandardGridder,
) -> Result<Vec<VisibilityBatch>, crate::ImagingError> {
    let density_convention = density_cell_convention(weighting, WeightDensityMode::Combined);
    let density_build_convention =
        density_build_cell_convention(weighting, WeightDensityMode::Combined);
    let trace_weighting = trace_weighting_enabled();
    match weighting {
        WeightingMode::Natural => Ok(apply_optional_uv_taper(batches, uv_taper)),
        WeightingMode::Uniform => {
            let density = build_density_grid(
                &batches,
                gridder,
                density_includes_conjugates(density_build_convention),
                density_build_convention,
            );
            Ok(apply_optional_uv_taper(
                reweight_owned_batches(
                    batches,
                    gridder,
                    &density,
                    density_convention,
                    trace_weighting,
                    DensityReweightMode::Uniform,
                ),
                uv_taper,
            ))
        }
        WeightingMode::Briggs { robust } | WeightingMode::BriggsBwTaper { robust } => {
            let density = build_density_grid(
                &batches,
                gridder,
                density_includes_conjugates(density_build_convention),
                density_build_convention,
            );
            let density_weight_sum = density.iter().map(|value| f64::from(*value)).sum::<f64>();
            let total_density_weight = density_weight_sum;
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
            if trace_weighting {
                let density_nonzero = density.iter().filter(|value| **value > 0.0).count();
                let density_max = density
                    .iter()
                    .copied()
                    .fold(0.0f32, |acc, value| acc.max(value));
                eprintln!(
                    "CASA_RS_TRACE_RUST_WEIGHTING briggs_density_summary total_density_weight={total_density_weight:.12e} density_sum_sq={sumlocwt:.12e} density_max={density_max:.12e} density_nonzero={density_nonzero} f2={f2:.12e}"
                );
            }
            Ok(apply_optional_uv_taper(
                reweight_owned_batches(
                    batches,
                    gridder,
                    &density,
                    density_convention,
                    trace_weighting,
                    DensityReweightMode::Briggs {
                        f2,
                        use_bandwidth_taper: matches!(
                            weighting,
                            WeightingMode::BriggsBwTaper { .. }
                        ),
                        fractional_bandwidth,
                    },
                ),
                uv_taper,
            ))
        }
    }
}

pub(crate) fn apply_weighting_with_density_source(
    weighting: WeightingMode,
    weight_density_mode: WeightDensityMode,
    uv_taper: Option<GaussianUvTaper>,
    fractional_bandwidth: f64,
    target_batches: &[VisibilityBatch],
    density_batches: &[VisibilityBatch],
    gridder: &StandardGridder,
) -> Result<Vec<VisibilityBatch>, crate::ImagingError> {
    let density_convention = density_cell_convention(weighting, weight_density_mode);
    let density_build_convention = density_build_cell_convention(weighting, weight_density_mode);
    let trace_weighting = trace_weighting_enabled();
    let aligned_lookup =
        aligned_density_lookup_batches(weight_density_mode, target_batches, density_batches);
    let density_build_batches = if aligned_lookup.is_some() {
        target_batches
    } else {
        density_batches
    };
    match weighting {
        WeightingMode::Natural => Ok(apply_optional_uv_taper(target_batches.to_vec(), uv_taper)),
        WeightingMode::Uniform => {
            let density = build_density_grid(
                density_build_batches,
                gridder,
                density_includes_conjugates(density_build_convention),
                density_build_convention,
            );
            Ok(apply_optional_uv_taper(
                target_batches
                    .iter()
                    .enumerate()
                    .map(|(batch_index, batch)| {
                        let lookup_batch = aligned_lookup
                            .as_ref()
                            .and_then(|lookup_batches| lookup_batches.get(batch_index))
                            .copied()
                            .unwrap_or(batch);
                        reweight_batch(
                            batch,
                            lookup_batch,
                            gridder,
                            &density,
                            density_convention,
                            trace_weighting,
                            |weight, density, _, _| weight / density,
                        )
                    })
                    .collect(),
                uv_taper,
            ))
        }
        WeightingMode::Briggs { robust } | WeightingMode::BriggsBwTaper { robust } => {
            let density = build_density_grid(
                density_build_batches,
                gridder,
                density_includes_conjugates(density_build_convention),
                density_build_convention,
            );
            let density_weight_sum = density.iter().map(|value| f64::from(*value)).sum::<f64>();
            let total_density_weight = density_weight_sum
                * match density_convention {
                    DensityCellConvention::VisImagingWeight => 1.0,
                    DensityCellConvention::CubeBriggsWeightorDensity
                    | DensityCellConvention::CubeBriggsWeightorLookup => 1.0,
                };
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
            if std::env::var_os("CASA_RS_TRACE_RUST_WEIGHTING").is_some() {
                let density_nonzero = density.iter().filter(|value| **value > 0.0).count();
                let density_max = density
                    .iter()
                    .copied()
                    .fold(0.0f32, |acc, value| acc.max(value));
                eprintln!(
                    "CASA_RS_TRACE_RUST_WEIGHTING briggs_density_summary total_density_weight={total_density_weight:.12e} density_sum_sq={sumlocwt:.12e} density_max={density_max:.12e} density_nonzero={density_nonzero} f2={f2:.12e}"
                );
            }
            Ok(apply_optional_uv_taper(
                target_batches
                    .iter()
                    .enumerate()
                    .map(|(batch_index, batch)| {
                        let lookup_batch = aligned_lookup
                            .as_ref()
                            .and_then(|lookup_batches| lookup_batches.get(batch_index))
                            .copied()
                            .unwrap_or(batch);
                        reweight_batch(
                            batch,
                            lookup_batch,
                            gridder,
                            &density,
                            density_convention,
                            trace_weighting,
                            |weight, density, u_lambda, v_lambda| {
                                let taper_factor = match weighting {
                                    WeightingMode::BriggsBwTaper { .. } => {
                                        briggs_bw_taper_uv_distance_factor(
                                            fractional_bandwidth,
                                            gridder,
                                            u_lambda,
                                            v_lambda,
                                        ) as f32
                                    }
                                    _ => 1.0,
                                };
                                weight / ((f2 * density) / taper_factor + 1.0)
                            },
                        )
                    })
                    .collect(),
                uv_taper,
            ))
        }
    }
}

pub(crate) fn trace_weighting_with_density_source(
    weighting: WeightingMode,
    weight_density_mode: WeightDensityMode,
    uv_taper: Option<GaussianUvTaper>,
    fractional_bandwidth: f64,
    target_batches: &[VisibilityBatch],
    density_batches: &[VisibilityBatch],
    gridder: &StandardGridder,
) -> Result<WeightingTraceInternal, crate::ImagingError> {
    let density_convention = density_cell_convention(weighting, weight_density_mode);
    let density_build_convention = density_build_cell_convention(weighting, weight_density_mode);
    let aligned_lookup =
        aligned_density_lookup_batches(weight_density_mode, target_batches, density_batches);
    let density_build_batches = if aligned_lookup.is_some() {
        target_batches
    } else {
        density_batches
    };
    let density = match weighting {
        WeightingMode::Natural => None,
        WeightingMode::Uniform
        | WeightingMode::Briggs { .. }
        | WeightingMode::BriggsBwTaper { .. } => Some(build_density_grid(
            density_build_batches,
            gridder,
            density_includes_conjugates(density_build_convention),
            density_build_convention,
        )),
    };
    let weighted_batches = apply_weighting_with_density_source(
        weighting,
        weight_density_mode,
        uv_taper,
        fractional_bandwidth,
        target_batches,
        density_batches,
        gridder,
    )?;
    let mut samples = Vec::new();
    let mut gridded_samples = 0usize;
    let mut skipped_samples = 0usize;
    let mut normalization_sumwt = 0.0f32;
    let mut reported_sumwt = 0.0f32;

    for (batch_index, (input_batch, weighted_batch)) in target_batches
        .iter()
        .zip(weighted_batches.iter())
        .enumerate()
    {
        let lookup_batch = aligned_lookup
            .as_ref()
            .and_then(|lookup_batches| lookup_batches.get(batch_index))
            .copied()
            .unwrap_or(input_batch);
        for sample_index in 0..input_batch.len() {
            let output_weight = weighted_batch.weight[sample_index];
            let sumwt_factor = weighted_batch.sumwt_factor[sample_index];
            let gridable = weighted_batch.gridable[sample_index];
            let contributes = gridable
                && output_weight.is_finite()
                && output_weight > 0.0
                && sumwt_factor.is_finite()
                && sumwt_factor > 0.0;
            let reported_contribution = if contributes {
                output_weight * sumwt_factor
            } else {
                0.0
            };
            let normalization_contribution = reported_contribution;
            if contributes {
                gridded_samples += 1;
                normalization_sumwt += normalization_contribution;
                reported_sumwt += reported_contribution;
            } else {
                skipped_samples += 1;
            }
            samples.push(WeightingSampleTraceInternal {
                batch_index,
                sample_index,
                u_lambda: input_batch.u_lambda[sample_index],
                v_lambda: input_batch.v_lambda[sample_index],
                w_lambda: input_batch.w_lambda[sample_index],
                input_weight: input_batch.weight[sample_index],
                density_weight: density.as_ref().and_then(|grid| {
                    gridder.density_at_with_convention(
                        grid,
                        lookup_batch.u_lambda[sample_index],
                        lookup_batch.v_lambda[sample_index],
                        density_convention,
                    )
                }),
                output_weight,
                sumwt_factor,
                gridable,
                normalization_contribution,
                reported_contribution,
            });
        }
    }

    Ok(WeightingTraceInternal {
        weighted_batches,
        samples,
        gridded_samples,
        skipped_samples,
        normalization_sumwt,
        reported_sumwt,
    })
}

fn density_cell_convention(
    weighting: WeightingMode,
    weight_density_mode: WeightDensityMode,
) -> DensityCellConvention {
    match (weighting, weight_density_mode) {
        (
            WeightingMode::Uniform
            | WeightingMode::Briggs { .. }
            | WeightingMode::BriggsBwTaper { .. },
            WeightDensityMode::PerPlane,
        ) => DensityCellConvention::CubeBriggsWeightorLookup,
        _ => DensityCellConvention::VisImagingWeight,
    }
}

fn density_build_cell_convention(
    weighting: WeightingMode,
    weight_density_mode: WeightDensityMode,
) -> DensityCellConvention {
    match (weighting, weight_density_mode) {
        (
            WeightingMode::Uniform
            | WeightingMode::Briggs { .. }
            | WeightingMode::BriggsBwTaper { .. },
            WeightDensityMode::PerPlane,
        ) => DensityCellConvention::CubeBriggsWeightorDensity,
        _ => DensityCellConvention::VisImagingWeight,
    }
}

fn density_includes_conjugates(convention: DensityCellConvention) -> bool {
    match convention {
        DensityCellConvention::VisImagingWeight => true,
        DensityCellConvention::CubeBriggsWeightorDensity
        | DensityCellConvention::CubeBriggsWeightorLookup => false,
    }
}

fn aligned_density_lookup_batches<'a>(
    weight_density_mode: WeightDensityMode,
    target_batches: &'a [VisibilityBatch],
    density_batches: &'a [VisibilityBatch],
) -> Option<Vec<&'a VisibilityBatch>> {
    if weight_density_mode != WeightDensityMode::PerPlane
        || target_batches.len() != density_batches.len()
    {
        return None;
    }
    let mut aligned = Vec::with_capacity(target_batches.len());
    for (target, density) in target_batches.iter().zip(density_batches) {
        if target.len() != density.len() {
            return None;
        }
        aligned.push(density);
    }
    Some(aligned)
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
            let u = batch.u_lambda[index];
            let v = batch.v_lambda[index];
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
    let sample_count = batches.iter().map(VisibilityBatch::len).sum::<usize>();
    let requested_threads = standard_mfs_worker_threads();
    let thread_count = requested_threads
        .min(batches.len())
        .min(thread::available_parallelism().map_or(1, |value| value.get()))
        .max(1);
    if thread_count > 1 && sample_count >= 100_000 {
        return build_density_grid_parallel(
            batches,
            gridder,
            mirror_hermitian,
            convention,
            thread_count,
        );
    }
    build_density_grid_serial(batches, gridder, mirror_hermitian, convention)
}

fn build_density_grid_serial(
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

fn build_density_grid_parallel(
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    mirror_hermitian: bool,
    convention: DensityCellConvention,
    thread_count: usize,
) -> Array2<f32> {
    let chunk_len = batches.len().div_ceil(thread_count);
    let mut local_grids = Vec::with_capacity(thread_count);
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(thread_count);
        for chunk in batches.chunks(chunk_len) {
            handles.push(scope.spawn(move || {
                build_density_grid_serial(chunk, gridder, mirror_hermitian, convention)
            }));
        }
        for handle in handles {
            local_grids.push(handle.join().expect("standard MFS density worker panicked"));
        }
    });

    let [nx, ny] = gridder.density_grid_shape();
    let mut density_grid = Array2::<f32>::zeros((nx, ny));
    for local_grid in &local_grids {
        add_f32_grid(&mut density_grid, local_grid);
    }
    density_grid
}

fn add_f32_grid(target: &mut Array2<f32>, source: &Array2<f32>) {
    if let (Some(target), Some(source)) = (
        target.as_slice_memory_order_mut(),
        source.as_slice_memory_order(),
    ) {
        for (target, source) in target.iter_mut().zip(source.iter()) {
            *target += *source;
        }
        return;
    }
    Zip::from(target).and(source).for_each(|target, source| {
        *target += *source;
    });
}

fn reweight_batch(
    batch: &VisibilityBatch,
    lookup_batch: &VisibilityBatch,
    gridder: &StandardGridder,
    density: &Array2<f32>,
    convention: DensityCellConvention,
    trace_weighting: bool,
    transform: impl Fn(f32, f32, f64, f64) -> f32,
) -> VisibilityBatch {
    let mut reweighted = batch.clone();
    for index in 0..batch.len() {
        let weight = batch.weight[index];
        let Some(cell_density) = gridder.density_at_with_convention(
            density,
            lookup_batch.u_lambda[index],
            lookup_batch.v_lambda[index],
            convention,
        ) else {
            reweighted.weight[index] = 0.0;
            continue;
        };
        if !(weight.is_finite() && weight > 0.0 && cell_density.is_finite() && cell_density > 0.0) {
            reweighted.weight[index] = 0.0;
            if trace_weighting {
                trace_weighting_sample(
                    index,
                    batch.u_lambda[index],
                    batch.v_lambda[index],
                    weight,
                    cell_density,
                    0.0,
                    gridder.density_cell_index_with_convention(
                        lookup_batch.u_lambda[index],
                        lookup_batch.v_lambda[index],
                        convention,
                    ),
                );
            }
            continue;
        }
        let output_weight = transform(
            weight,
            cell_density,
            lookup_batch.u_lambda[index],
            lookup_batch.v_lambda[index],
        );
        if trace_weighting {
            trace_weighting_sample(
                index,
                batch.u_lambda[index],
                batch.v_lambda[index],
                weight,
                cell_density,
                output_weight,
                gridder.density_cell_index_with_convention(
                    lookup_batch.u_lambda[index],
                    lookup_batch.v_lambda[index],
                    convention,
                ),
            );
        }
        reweighted.weight[index] = output_weight;
    }
    reweighted
}

fn reweight_owned_batches(
    mut batches: Vec<VisibilityBatch>,
    gridder: &StandardGridder,
    density: &Array2<f32>,
    convention: DensityCellConvention,
    trace_weighting: bool,
    mode: DensityReweightMode,
) -> Vec<VisibilityBatch> {
    let requested_threads = standard_mfs_worker_threads();
    let thread_count = requested_threads
        .min(batches.len())
        .min(thread::available_parallelism().map_or(1, |value| value.get()))
        .max(1);
    if trace_weighting || thread_count <= 1 || batches.len() < 2 {
        for batch in &mut batches {
            reweight_owned_batch_in_place(
                batch,
                gridder,
                density,
                convention,
                trace_weighting,
                mode,
            );
        }
        return batches;
    }

    let chunk_len = batches.len().div_ceil(thread_count);
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(thread_count);
        for chunk in batches.chunks_mut(chunk_len) {
            handles.push(scope.spawn(move || {
                for batch in chunk {
                    reweight_owned_batch_in_place(batch, gridder, density, convention, false, mode);
                }
            }));
        }
        for handle in handles {
            handle
                .join()
                .expect("standard MFS reweight worker panicked");
        }
    });
    batches
}

fn reweight_density_sample(
    weight: f32,
    cell_density: f32,
    u_lambda: f64,
    v_lambda: f64,
    gridder: &StandardGridder,
    mode: DensityReweightMode,
) -> f32 {
    match mode {
        DensityReweightMode::Uniform => weight / cell_density,
        DensityReweightMode::Briggs {
            f2,
            use_bandwidth_taper,
            fractional_bandwidth,
        } => {
            let taper_factor = if use_bandwidth_taper {
                briggs_bw_taper_uv_distance_factor(
                    fractional_bandwidth,
                    gridder,
                    u_lambda,
                    v_lambda,
                ) as f32
            } else {
                1.0
            };
            weight / ((f2 * cell_density) / taper_factor + 1.0)
        }
    }
}

fn reweight_owned_batch_in_place(
    batch: &mut VisibilityBatch,
    gridder: &StandardGridder,
    density: &Array2<f32>,
    convention: DensityCellConvention,
    trace_weighting: bool,
    mode: DensityReweightMode,
) {
    reweight_owned_batch_with_transform(
        batch,
        gridder,
        density,
        convention,
        trace_weighting,
        |weight, density, u_lambda, v_lambda| {
            reweight_density_sample(weight, density, u_lambda, v_lambda, gridder, mode)
        },
    );
}

fn reweight_owned_batch_with_transform(
    batch: &mut VisibilityBatch,
    gridder: &StandardGridder,
    density: &Array2<f32>,
    convention: DensityCellConvention,
    trace_weighting: bool,
    transform: impl Fn(f32, f32, f64, f64) -> f32,
) {
    for index in 0..batch.len() {
        let weight = batch.weight[index];
        let u_lambda = batch.u_lambda[index];
        let v_lambda = batch.v_lambda[index];
        let Some(cell_density) =
            gridder.density_at_with_convention(density, u_lambda, v_lambda, convention)
        else {
            batch.weight[index] = 0.0;
            continue;
        };
        if !(weight.is_finite() && weight > 0.0 && cell_density.is_finite() && cell_density > 0.0) {
            batch.weight[index] = 0.0;
            if trace_weighting {
                trace_weighting_sample(
                    index,
                    u_lambda,
                    v_lambda,
                    weight,
                    cell_density,
                    0.0,
                    gridder.density_cell_index_with_convention(u_lambda, v_lambda, convention),
                );
            }
            continue;
        }
        let output_weight = transform(weight, cell_density, u_lambda, v_lambda);
        if trace_weighting {
            trace_weighting_sample(
                index,
                u_lambda,
                v_lambda,
                weight,
                cell_density,
                output_weight,
                gridder.density_cell_index_with_convention(u_lambda, v_lambda, convention),
            );
        }
        batch.weight[index] = output_weight;
    }
}

fn trace_weighting_enabled() -> bool {
    std::env::var_os("CASA_RS_TRACE_RUST_WEIGHTING").is_some()
}

fn trace_weighting_sample(
    index: usize,
    u_lambda: f64,
    v_lambda: f64,
    input_weight: f32,
    density: f32,
    output_weight: f32,
    cell: Option<(usize, usize)>,
) {
    let should_trace = index < 16 || (90..=240).contains(&index);
    if !should_trace {
        return;
    }
    let (cell_x, cell_y) = cell
        .map(|(x, y)| (x.to_string(), y.to_string()))
        .unwrap_or_else(|| ("null".to_string(), "null".to_string()));
    eprintln!(
        "CASA_RS_TRACE_RUST_WEIGHTING sample index={index} u_lambda={u_lambda:.17e} v_lambda={v_lambda:.17e} cell=({cell_x},{cell_y}) input_weight={input_weight:.17e} density={density:.17e} output_weight={output_weight:.17e}"
    );
}

pub(crate) fn fractional_bandwidth_from_frequency_range(frequency_range_hz: [f64; 2]) -> f64 {
    let min_freq = frequency_range_hz[0].abs().min(frequency_range_hz[1].abs());
    let max_freq = frequency_range_hz[0].abs().max(frequency_range_hz[1].abs());
    if min_freq > 0.0 && max_freq.is_finite() {
        2.0 * (max_freq - min_freq) / (max_freq + min_freq)
    } else {
        0.0
    }
}

fn briggs_bw_taper_uv_distance_factor(
    fractional_bandwidth: f64,
    gridder: &StandardGridder,
    u_lambda: f64,
    v_lambda: f64,
) -> f64 {
    let n_cells_bw = fractional_bandwidth * gridder.cube_briggs_uv_cell_radius(u_lambda, v_lambda);
    let mut factor = n_cells_bw + 0.5;
    if factor < 1.5 {
        factor = (4.0 - n_cells_bw) / (4.0 - 2.0 * n_cells_bw);
    }
    factor.max(f64::MIN_POSITIVE)
}

#[cfg(test)]
mod tests {
    use num_complex::Complex32;

    use super::*;
    use crate::{
        CleanConfig, CompatibilityMode, Deconvolver, GridderMode, ImageGeometry, ImagingRequest,
        PlaneStokes,
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
            gridder_mode: GridderMode::Standard,
            plane_stokes: PlaneStokes::I,
            weighting: mode,
            reffreq_hz: 1.4e9,
            selected_frequency_range_hz: [1.399e9, 1.401e9],
            deconvolver: Deconvolver::Hogbom,
            multiscale_scales: Vec::new(),
            small_scale_bias: 0.0,
            clean: CleanConfig::default(),
            clean_mask: None,
            initial_model: None,
            w_term_mode: crate::WTermMode::None,
            w_project_planes: None,
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
    fn briggs_bandwidth_taper_relaxes_robust_downweighting_at_large_uv_radius() {
        let geometry = request_for(WeightingMode::Natural).geometry;
        let gridder = StandardGridder::new(geometry).unwrap();
        let mut briggs_request = request_for(WeightingMode::Briggs { robust: 0.0 });
        briggs_request.selected_frequency_range_hz = [1.0e9, 3.0e9];
        let mut tapered_request = request_for(WeightingMode::BriggsBwTaper { robust: 0.0 });
        tapered_request.selected_frequency_range_hz = briggs_request.selected_frequency_range_hz;

        let briggs = apply_weighting(&briggs_request, &gridder).unwrap();
        let tapered = apply_weighting(&tapered_request, &gridder).unwrap();

        let center_index = 0usize;
        let outer_index = 4usize;
        assert!((tapered[0].weight[center_index] - briggs[0].weight[center_index]).abs() < 1e-6);
        assert!(tapered[0].weight[outer_index] > briggs[0].weight[outer_index]);
    }

    #[test]
    fn owned_briggs_weighting_matches_borrowed_weighting() {
        let request = request_for(WeightingMode::Briggs { robust: 0.5 });
        let gridder = StandardGridder::new(request.geometry).unwrap();

        let borrowed = apply_weighting(&request, &gridder).unwrap();
        let owned = apply_weighting_to_owned_batches(
            &request,
            &gridder,
            request.visibility_batches.clone(),
        )
        .unwrap();

        assert_eq!(owned, borrowed);
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
