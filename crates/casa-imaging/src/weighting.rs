// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-style imaging-weight preparation for the pure imaging core.

use std::{sync::LazyLock, thread};

use ndarray::{Array2, Zip};

use crate::{
    GaussianUvTaper, ImageGeometry, ImagingRequest, StandardMfsVisibilityPolarization, UvTaperSize,
    VisibilityBatch, VisibilityBlockView, VisibilityFloatSamplesRef, VisibilitySampleRange,
    WeightDensityMode, WeightingMode,
    gridder::{DensityCellConvention, StandardGridder},
    profile,
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

#[derive(Debug, Clone, Copy)]
#[cfg_attr(any(not(target_os = "macos"), coverage), allow(dead_code))]
pub(crate) enum StandardMfsStreamingReweightPlan<'a> {
    Natural,
    Uniform {
        density: &'a Array2<f32>,
        convention: DensityCellConvention,
    },
    Briggs {
        density: &'a Array2<f32>,
        convention: DensityCellConvention,
        f2: f32,
        use_bandwidth_taper: bool,
        fractional_bandwidth: f64,
    },
}

/// Streaming standard-MFS weighting state for bounded row-block execution.
///
/// Natural weighting is a single pass. Uniform and Briggs variants use an
/// explicit density pass followed by bounded row-block reweighting with the
/// same density conventions as the retained-batch weighting path.
pub struct StandardMfsStreamingWeightingPlan {
    gridder: StandardGridder,
    weighting: WeightingMode,
    density_convention: DensityCellConvention,
    density_build_convention: DensityCellConvention,
    fractional_bandwidth: f64,
    density: Option<Array2<f32>>,
    mode: Option<DensityReweightMode>,
}

impl StandardMfsStreamingWeightingPlan {
    /// Create an empty streaming weighting plan for one standard-MFS image.
    pub fn new(
        geometry: ImageGeometry,
        weighting: WeightingMode,
        selected_frequency_range_hz: [f64; 2],
    ) -> Result<Self, crate::ImagingError> {
        Self::new_with_density_mode(
            geometry,
            weighting,
            selected_frequency_range_hz,
            WeightDensityMode::Combined,
        )
    }

    /// Create an empty streaming weighting plan with an explicit density-sharing mode.
    pub fn new_with_density_mode(
        geometry: ImageGeometry,
        weighting: WeightingMode,
        selected_frequency_range_hz: [f64; 2],
        weight_density_mode: WeightDensityMode,
    ) -> Result<Self, crate::ImagingError> {
        let density_convention = density_cell_convention(weighting, weight_density_mode);
        let density_build_convention =
            density_build_cell_convention(weighting, weight_density_mode);
        Self::new_with_density_conventions(
            geometry,
            weighting,
            selected_frequency_range_hz,
            density_convention,
            density_build_convention,
        )
    }

    pub(crate) fn new_with_density_conventions(
        geometry: ImageGeometry,
        weighting: WeightingMode,
        selected_frequency_range_hz: [f64; 2],
        density_convention: DensityCellConvention,
        density_build_convention: DensityCellConvention,
    ) -> Result<Self, crate::ImagingError> {
        let gridder = StandardGridder::new(geometry)?;
        let density = match weighting {
            WeightingMode::Natural => None,
            WeightingMode::Uniform
            | WeightingMode::Briggs { .. }
            | WeightingMode::BriggsBwTaper { .. } => {
                let [nx, ny] = gridder.density_grid_shape();
                Some(Array2::<f32>::zeros((nx, ny)))
            }
        };
        Ok(Self {
            gridder,
            weighting,
            density_convention,
            density_build_convention,
            fractional_bandwidth: fractional_bandwidth_from_frequency_range(
                selected_frequency_range_hz,
            ),
            density,
            mode: None,
        })
    }

    /// Return true when the selected weighting requires a first density pass.
    pub fn needs_density_pass(&self) -> bool {
        self.density.is_some()
    }

    /// Accumulate density from one bounded row block.
    pub fn accumulate_density_batches(&mut self, batches: &[VisibilityBatch]) {
        let Some(density) = self.density.as_mut() else {
            return;
        };
        accumulate_density_grid_serial(
            batches,
            &self.gridder,
            density_includes_conjugates(self.density_build_convention),
            self.density_build_convention,
            density,
            profile::standard_mfs_profile_detail_enabled(),
        );
    }

    /// Accumulate one already-filtered standard-MFS density sample.
    #[inline]
    pub fn accumulate_density_sample(&mut self, u_lambda: f64, v_lambda: f64, weight: f32) {
        let Some(density) = self.density.as_mut() else {
            return;
        };
        accumulate_density_sample_serial(
            &self.gridder,
            density_includes_conjugates(self.density_build_convention),
            self.density_build_convention,
            density,
            u_lambda,
            v_lambda,
            weight,
        );
    }

    /// Create an empty density accumulator with the same weighting geometry.
    ///
    /// Frontends use this to accumulate bounded row blocks on worker-local
    /// density grids, then merge those grids into the main plan before robust
    /// statistics are finalized.
    pub fn fork_density_accumulator(&self) -> Result<Self, crate::ImagingError> {
        let density = self.density.as_ref().map(|density| {
            let (nx, ny) = density.dim();
            Array2::<f32>::zeros((nx, ny))
        });
        Ok(Self {
            gridder: StandardGridder::new(self.gridder.geometry())?,
            weighting: self.weighting,
            density_convention: self.density_convention,
            density_build_convention: self.density_build_convention,
            fractional_bandwidth: self.fractional_bandwidth,
            density,
            mode: None,
        })
    }

    /// Merge a worker-local density accumulator into this plan.
    pub fn merge_density_accumulator(&mut self, other: Self) -> Result<(), crate::ImagingError> {
        match (self.density.as_mut(), other.density) {
            (Some(target), Some(source)) => {
                add_f32_grid(target, &source);
                Ok(())
            }
            (None, None) => Ok(()),
            _ => Err(crate::ImagingError::InvalidRequest(
                "cannot merge mismatched standard MFS density accumulators".to_string(),
            )),
        }
    }

    /// Finalize robust statistics after the density pass.
    pub fn finish_density_pass(&mut self) {
        self.mode = match self.weighting {
            WeightingMode::Natural => None,
            WeightingMode::Uniform => Some(DensityReweightMode::Uniform),
            WeightingMode::Briggs { robust } | WeightingMode::BriggsBwTaper { robust } => {
                let Some(density) = self.density.as_ref() else {
                    return;
                };
                let density_weight_sum = density.iter().map(|value| f64::from(*value)).sum::<f64>();
                let total_density_weight = robust_density_weight_sum_for_f2(
                    density_weight_sum,
                    self.density_build_convention,
                );
                let sumlocwt = density
                    .iter()
                    .filter(|value| **value > 0.0)
                    .map(|value| f64::from(*value) * f64::from(*value))
                    .sum::<f64>();
                let f2 = if sumlocwt > 0.0 && total_density_weight > 0.0 {
                    (5.0f64 * 10f64.powf(-(robust as f64))).powi(2)
                        / (sumlocwt / total_density_weight)
                } else {
                    0.0
                } as f32;
                if trace_weighting_enabled() {
                    let density_nonzero = density.iter().filter(|value| **value > 0.0).count();
                    let density_max = density
                        .iter()
                        .copied()
                        .fold(0.0f32, |acc, value| acc.max(value));
                    eprintln!(
                        "CASA_RS_TRACE_RUST_WEIGHTING streaming_briggs_density_summary total_density_weight={total_density_weight:.12e} density_sum={density_weight_sum:.12e} density_sum_sq={sumlocwt:.12e} density_max={density_max:.12e} density_nonzero={density_nonzero} f2={f2:.12e}"
                    );
                }
                Some(DensityReweightMode::Briggs {
                    f2,
                    use_bandwidth_taper: matches!(
                        self.weighting,
                        WeightingMode::BriggsBwTaper { .. }
                    ),
                    fractional_bandwidth: self.fractional_bandwidth,
                })
            }
        };
    }

    /// Apply final imaging weights to one owned row block and return it.
    pub fn weight_owned_batches(
        &self,
        batches: Vec<VisibilityBatch>,
    ) -> Result<Vec<VisibilityBatch>, crate::ImagingError> {
        match self.weighting {
            WeightingMode::Natural => Ok(batches),
            WeightingMode::Uniform
            | WeightingMode::Briggs { .. }
            | WeightingMode::BriggsBwTaper { .. } => {
                let density = self.density.as_ref().ok_or_else(|| {
                    crate::ImagingError::InvalidRequest(
                        "streaming standard MFS weighting density pass was not initialized"
                            .to_string(),
                    )
                })?;
                let mode = self.mode.ok_or_else(|| {
                    crate::ImagingError::InvalidRequest(
                        "streaming standard MFS weighting density pass was not finalized"
                            .to_string(),
                    )
                })?;
                Ok(reweight_owned_batches(
                    batches,
                    &self.gridder,
                    density,
                    self.density_convention,
                    trace_weighting_enabled(),
                    mode,
                ))
            }
        }
    }

    /// Return the final standard-MFS imaging weight for one sample.
    ///
    /// This mirrors [`Self::weight_owned_batches`] for frontends that stream
    /// accepted row-block samples directly to the standard-MFS gridders instead
    /// of materializing an owned [`VisibilityBatch`] for each replay.
    #[inline]
    pub fn weight_sample(
        &self,
        u_lambda: f64,
        v_lambda: f64,
        input_weight: f32,
    ) -> Result<f32, crate::ImagingError> {
        match self.weighting {
            WeightingMode::Natural => Ok(input_weight),
            WeightingMode::Uniform
            | WeightingMode::Briggs { .. }
            | WeightingMode::BriggsBwTaper { .. } => {
                let density = self.density.as_ref().ok_or_else(|| {
                    crate::ImagingError::InvalidRequest(
                        "streaming standard MFS weighting density pass was not initialized"
                            .to_string(),
                    )
                })?;
                let mode = self.mode.ok_or_else(|| {
                    crate::ImagingError::InvalidRequest(
                        "streaming standard MFS weighting density pass was not finalized"
                            .to_string(),
                    )
                })?;
                let Some(cell_density) = self.gridder.density_at_with_convention(
                    density,
                    u_lambda,
                    v_lambda,
                    self.density_convention,
                ) else {
                    return Ok(0.0);
                };
                if !(input_weight.is_finite()
                    && input_weight > 0.0
                    && cell_density.is_finite()
                    && cell_density > 0.0)
                {
                    return Ok(0.0);
                }
                Ok(reweight_density_sample(
                    input_weight,
                    cell_density,
                    u_lambda,
                    v_lambda,
                    &self.gridder,
                    mode,
                ))
            }
        }
    }

    #[cfg_attr(any(not(target_os = "macos"), coverage), allow(dead_code))]
    pub(crate) fn reweight_plan(
        &self,
    ) -> Result<StandardMfsStreamingReweightPlan<'_>, crate::ImagingError> {
        match self.weighting {
            WeightingMode::Natural => Ok(StandardMfsStreamingReweightPlan::Natural),
            WeightingMode::Uniform => {
                let density = self.density.as_ref().ok_or_else(|| {
                    crate::ImagingError::InvalidRequest(
                        "streaming standard MFS weighting density pass was not initialized"
                            .to_string(),
                    )
                })?;
                let Some(DensityReweightMode::Uniform) = self.mode else {
                    return Err(crate::ImagingError::InvalidRequest(
                        "streaming standard MFS uniform density pass was not finalized".to_string(),
                    ));
                };
                Ok(StandardMfsStreamingReweightPlan::Uniform {
                    density,
                    convention: self.density_convention,
                })
            }
            WeightingMode::Briggs { .. } | WeightingMode::BriggsBwTaper { .. } => {
                let density = self.density.as_ref().ok_or_else(|| {
                    crate::ImagingError::InvalidRequest(
                        "streaming standard MFS weighting density pass was not initialized"
                            .to_string(),
                    )
                })?;
                let Some(DensityReweightMode::Briggs {
                    f2,
                    use_bandwidth_taper,
                    fractional_bandwidth,
                }) = self.mode
                else {
                    return Err(crate::ImagingError::InvalidRequest(
                        "streaming standard MFS Briggs density pass was not finalized".to_string(),
                    ));
                };
                Ok(StandardMfsStreamingReweightPlan::Briggs {
                    density,
                    convention: self.density_convention,
                    f2,
                    use_bandwidth_taper,
                    fractional_bandwidth,
                })
            }
        }
    }
}

/// Accumulate standard-MFS density samples from one row in a borrowed visibility block.
///
/// The block must use the crate's neutral columnar layout:
/// `[channel][row][correlation]` for `FLAG` and `WEIGHT_SPECTRUM`, and
/// `[row][correlation]` for `WEIGHT`. `uvw_m` is supplied separately so
/// frontends can pass either raw or already-reprojected coordinates without
/// changing the source block contract.
#[allow(clippy::too_many_arguments)]
pub fn accumulate_standard_mfs_density_row_from_visibility_block(
    weighting_plan: &mut StandardMfsStreamingWeightingPlan,
    source: VisibilityBlockView<'_>,
    row_slot: usize,
    uvw_m: [f64; 3],
    mfs_frequency_scale: f64,
    source_channel_indices: &[usize],
    source_channel_frequencies_hz: &[f64],
    polarization: StandardMfsVisibilityPolarization,
) -> Result<usize, crate::ImagingError> {
    if row_slot >= source.row_count() {
        return Err(crate::ImagingError::InvalidRequest(format!(
            "standard MFS density row slot {row_slot} is out of bounds for {} rows",
            source.row_count()
        )));
    }
    let flags = source.flags.ok_or_else(|| {
        crate::ImagingError::InvalidRequest(
            "standard MFS density source requires FLAG samples".to_string(),
        )
    })?;
    let weights = source.weights.ok_or_else(|| {
        crate::ImagingError::InvalidRequest(
            "standard MFS density source requires WEIGHT samples".to_string(),
        )
    })?;
    let corr_count = source.partition.shape.correlation_count;
    let channel_count = source.channel_count;
    accumulate_standard_mfs_density_row_with_accessors(
        weighting_plan,
        uvw_m,
        mfs_frequency_scale,
        source_channel_indices,
        source_channel_frequencies_hz,
        polarization,
        |source_channel| {
            standard_mfs_density_local_channel(source_channel, source.channel_start, channel_count)
        },
        |corr, _source_channel, local_channel| {
            standard_mfs_density_columnar_flag(source, flags, row_slot, corr, local_channel)
        },
        |corr, _source_channel, local_channel| {
            standard_mfs_density_columnar_weight(
                source,
                weights,
                source.weight_spectrum,
                row_slot,
                corr,
                local_channel,
            )
        },
        |corr| {
            if source.weight_spectrum.is_some() {
                Ok(None)
            } else {
                standard_mfs_density_columnar_row_weight(source, weights, row_slot, corr).map(Some)
            }
        },
        |first_corr, second_corr| {
            if source.weight_spectrum.is_some() {
                Ok(None)
            } else if first_corr >= corr_count || second_corr >= corr_count {
                Err(crate::ImagingError::InvalidRequest(format!(
                    "standard MFS density WEIGHT pair [{first_corr}, {second_corr}] is out of bounds"
                )))
            } else {
                Ok(Some((
                    standard_mfs_density_columnar_row_weight(
                        source, weights, row_slot, first_corr,
                    )?,
                    standard_mfs_density_columnar_row_weight(
                        source,
                        weights,
                        row_slot,
                        second_corr,
                    )?,
                )))
            }
        },
    )
}

/// Accumulate standard-MFS density samples from one row-major visibility row.
///
/// `flags` and `weight_spectrum` use `[correlation][local_channel]` layout,
/// while `weights` is one value per correlation. `channel_origin` maps absolute
/// source-channel indices onto the local channel axis.
#[allow(clippy::too_many_arguments)]
pub fn accumulate_standard_mfs_density_row_from_arrays(
    weighting_plan: &mut StandardMfsStreamingWeightingPlan,
    uvw_m: [f64; 3],
    mfs_frequency_scale: f64,
    channel_origin: usize,
    flags: &Array2<bool>,
    weights: &[f32],
    weight_spectrum: Option<&Array2<f32>>,
    source_channel_indices: &[usize],
    source_channel_frequencies_hz: &[f64],
    polarization: StandardMfsVisibilityPolarization,
) -> Result<usize, crate::ImagingError> {
    let (corr_count, channel_count) = flags.dim();
    accumulate_standard_mfs_density_row_with_accessors(
        weighting_plan,
        uvw_m,
        mfs_frequency_scale,
        source_channel_indices,
        source_channel_frequencies_hz,
        polarization,
        |source_channel| {
            standard_mfs_density_local_channel(source_channel, channel_origin, channel_count)
        },
        |corr, source_channel, local_channel| {
            flags.get((corr, local_channel)).copied().ok_or_else(|| {
                crate::ImagingError::InvalidRequest(format!(
                    "standard MFS density FLAG index [{corr}, {source_channel}] is out of bounds"
                ))
            })
        },
        |corr, source_channel, local_channel| {
            if let Some(weight_spectrum) = weight_spectrum
                && let Some(weight) = weight_spectrum.get((corr, local_channel)).copied()
            {
                return Ok(weight);
            }
            weights.get(corr).copied().ok_or_else(|| {
                crate::ImagingError::InvalidRequest(format!(
                    "standard MFS density WEIGHT correlation {corr} is out of bounds for source channel {source_channel}"
                ))
            })
        },
        |corr| {
            if weight_spectrum.is_some() {
                Ok(None)
            } else {
                weights.get(corr).copied().map(Some).ok_or_else(|| {
                    crate::ImagingError::InvalidRequest(format!(
                        "standard MFS density WEIGHT correlation {corr} is out of bounds"
                    ))
                })
            }
        },
        |first_corr, second_corr| {
            if weight_spectrum.is_some() {
                Ok(None)
            } else if first_corr >= corr_count || second_corr >= corr_count {
                Err(crate::ImagingError::InvalidRequest(format!(
                    "standard MFS density WEIGHT pair [{first_corr}, {second_corr}] is out of bounds"
                )))
            } else {
                Ok(Some((
                    *weights.get(first_corr).ok_or_else(|| {
                        crate::ImagingError::InvalidRequest(format!(
                            "standard MFS density WEIGHT correlation {first_corr} is out of bounds"
                        ))
                    })?,
                    *weights.get(second_corr).ok_or_else(|| {
                        crate::ImagingError::InvalidRequest(format!(
                            "standard MFS density WEIGHT correlation {second_corr} is out of bounds"
                        ))
                    })?,
                )))
            }
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn accumulate_standard_mfs_density_row_with_accessors<
    Local,
    Flag,
    Weight,
    Invariant,
    PairInvariant,
>(
    weighting_plan: &mut StandardMfsStreamingWeightingPlan,
    uvw_m: [f64; 3],
    mfs_frequency_scale: f64,
    source_channel_indices: &[usize],
    source_channel_frequencies_hz: &[f64],
    polarization: StandardMfsVisibilityPolarization,
    mut local_channel_for_source: Local,
    mut flag_at: Flag,
    mut weight_at: Weight,
    mut channel_invariant_weight: Invariant,
    mut channel_invariant_pair_weights: PairInvariant,
) -> Result<usize, crate::ImagingError>
where
    Local: FnMut(usize) -> Result<usize, crate::ImagingError>,
    Flag: FnMut(usize, usize, usize) -> Result<bool, crate::ImagingError>,
    Weight: FnMut(usize, usize, usize) -> Result<f32, crate::ImagingError>,
    Invariant: FnMut(usize) -> Result<Option<f32>, crate::ImagingError>,
    PairInvariant: FnMut(usize, usize) -> Result<Option<(f32, f32)>, crate::ImagingError>,
{
    if source_channel_indices.len() != source_channel_frequencies_hz.len() {
        return Err(crate::ImagingError::InvalidRequest(format!(
            "standard MFS density source-channel count {} differs from frequency count {}",
            source_channel_indices.len(),
            source_channel_frequencies_hz.len()
        )));
    }
    let mfs_lambda_scale = mfs_frequency_scale / crate::SPEED_OF_LIGHT_M_PER_S;
    let mut accepted_samples = 0usize;
    match polarization {
        StandardMfsVisibilityPolarization::Explicit { corr_index, .. } => {
            let invariant_weight = channel_invariant_weight(corr_index)?;
            for (&source_channel, &frequency_hz) in source_channel_indices
                .iter()
                .zip(source_channel_frequencies_hz.iter())
            {
                let local_channel = local_channel_for_source(source_channel)?;
                if flag_at(corr_index, source_channel, local_channel)? {
                    continue;
                }
                let weight = match invariant_weight {
                    Some(weight) => weight,
                    None => weight_at(corr_index, source_channel, local_channel)?,
                };
                if !(weight.is_finite() && weight > 0.0) {
                    continue;
                }
                let lambda_scale = frequency_hz * mfs_lambda_scale;
                weighting_plan.accumulate_density_sample(
                    uvw_m[0] * lambda_scale,
                    uvw_m[1] * lambda_scale,
                    weight,
                );
                accepted_samples += 1;
            }
        }
        StandardMfsVisibilityPolarization::CollapsedPair {
            first_corr_index,
            second_corr_index,
            ..
        } => {
            let invariant_pair =
                channel_invariant_pair_weights(first_corr_index, second_corr_index)?;
            for (&source_channel, &frequency_hz) in source_channel_indices
                .iter()
                .zip(source_channel_frequencies_hz.iter())
            {
                let local_channel = local_channel_for_source(source_channel)?;
                if flag_at(first_corr_index, source_channel, local_channel)?
                    || flag_at(second_corr_index, source_channel, local_channel)?
                {
                    continue;
                }
                let (first_weight, second_weight) = match invariant_pair {
                    Some(weights) => weights,
                    None => (
                        weight_at(first_corr_index, source_channel, local_channel)?,
                        weight_at(second_corr_index, source_channel, local_channel)?,
                    ),
                };
                if !(first_weight.is_finite()
                    && first_weight > 0.0
                    && second_weight.is_finite()
                    && second_weight > 0.0)
                {
                    continue;
                }
                let combined_weight = 0.5 * (first_weight + second_weight);
                if !(combined_weight.is_finite() && combined_weight > 0.0) {
                    continue;
                }
                let lambda_scale = frequency_hz * mfs_lambda_scale;
                weighting_plan.accumulate_density_sample(
                    uvw_m[0] * lambda_scale,
                    uvw_m[1] * lambda_scale,
                    combined_weight,
                );
                accepted_samples += 1;
            }
        }
    }
    Ok(accepted_samples)
}

fn standard_mfs_density_local_channel(
    source_channel: usize,
    channel_origin: usize,
    channel_count: usize,
) -> Result<usize, crate::ImagingError> {
    let local_channel = source_channel.checked_sub(channel_origin).ok_or_else(|| {
        crate::ImagingError::InvalidRequest(format!(
            "standard MFS density source channel {source_channel} precedes loaded channel origin {channel_origin}"
        ))
    })?;
    if local_channel >= channel_count {
        return Err(crate::ImagingError::InvalidRequest(format!(
            "standard MFS density source channel {source_channel} is outside loaded channel range {}..{}",
            channel_origin,
            channel_origin.saturating_add(channel_count)
        )));
    }
    Ok(local_channel)
}

fn standard_mfs_density_columnar_flag(
    source: VisibilityBlockView<'_>,
    flags: &[bool],
    row_slot: usize,
    corr: usize,
    local_channel: usize,
) -> Result<bool, crate::ImagingError> {
    let index = source.channel_row_corr_index(local_channel, row_slot, corr);
    flags.get(index).copied().ok_or_else(|| {
        crate::ImagingError::InvalidRequest(format!(
            "standard MFS density FLAG sample index {index} is out of bounds"
        ))
    })
}

fn standard_mfs_density_columnar_weight(
    source: VisibilityBlockView<'_>,
    weights: VisibilityFloatSamplesRef<'_>,
    weight_spectrum: Option<VisibilityFloatSamplesRef<'_>>,
    row_slot: usize,
    corr: usize,
    local_channel: usize,
) -> Result<f32, crate::ImagingError> {
    if let Some(weight_spectrum) = weight_spectrum {
        let index = source.channel_row_corr_index(local_channel, row_slot, corr);
        return standard_mfs_density_float_sample(weight_spectrum, index, "WEIGHT_SPECTRUM");
    }
    standard_mfs_density_columnar_row_weight(source, weights, row_slot, corr)
}

fn standard_mfs_density_columnar_row_weight(
    source: VisibilityBlockView<'_>,
    weights: VisibilityFloatSamplesRef<'_>,
    row_slot: usize,
    corr: usize,
) -> Result<f32, crate::ImagingError> {
    let corr_count = source.partition.shape.correlation_count;
    if corr >= corr_count {
        return Err(crate::ImagingError::InvalidRequest(format!(
            "standard MFS density WEIGHT correlation {corr} is out of bounds"
        )));
    }
    let index = row_slot
        .checked_mul(corr_count)
        .and_then(|value| value.checked_add(corr))
        .ok_or_else(|| {
            crate::ImagingError::InvalidRequest(
                "standard MFS density WEIGHT sample index overflowed".to_string(),
            )
        })?;
    standard_mfs_density_float_sample(weights, index, "WEIGHT")
}

fn standard_mfs_density_float_sample(
    values: VisibilityFloatSamplesRef<'_>,
    index: usize,
    label: &str,
) -> Result<f32, crate::ImagingError> {
    match values {
        VisibilityFloatSamplesRef::Float32(values) => values.get(index).copied().ok_or_else(|| {
            crate::ImagingError::InvalidRequest(format!(
                "standard MFS density {label} sample index {index} is out of bounds"
            ))
        }),
        VisibilityFloatSamplesRef::Float64(values) => {
            values.get(index).map(|value| *value as f32).ok_or_else(|| {
                crate::ImagingError::InvalidRequest(format!(
                    "standard MFS density {label} sample index {index} is out of bounds"
                ))
            })
        }
    }
}

fn standard_mfs_worker_threads() -> usize {
    crate::standard_mfs_thread_count_from_env()
}

fn visibility_sample_count(batches: &[VisibilityBatch]) -> usize {
    batches.iter().map(VisibilityBatch::len).sum()
}

#[derive(Debug, Clone, Copy, Default)]
struct WeightingWorkStats {
    accepted_samples: usize,
    skipped_invalid_weight: usize,
    skipped_invalid_density: usize,
    skipped_out_of_grid: usize,
    density_cell_hits: usize,
}

impl WeightingWorkStats {
    fn add(&mut self, other: Self) {
        self.accepted_samples += other.accepted_samples;
        self.skipped_invalid_weight += other.skipped_invalid_weight;
        self.skipped_invalid_density += other.skipped_invalid_density;
        self.skipped_out_of_grid += other.skipped_out_of_grid;
        self.density_cell_hits += other.density_cell_hits;
    }
}

fn log_weighting_stage(
    stage: &str,
    samples_total: usize,
    density: Option<&Array2<f32>>,
    density_build: std::time::Duration,
    robust_scaling: std::time::Duration,
    reweight: std::time::Duration,
    total: std::time::Duration,
) {
    if !profile::standard_mfs_profile_detail_enabled() {
        return;
    }
    let density_cells = density.map_or(0, |density| density.len());
    let density_nonzero = density.map_or(0, |density| {
        density.iter().filter(|value| **value > 0.0).count()
    });
    eprintln!(
        "standard_mfs_weighting_stage stage={} samples_total={} density_cells={} density_nonzero={} density_build_ms={:.3} robust_scaling_ms={:.3} reweight_ms={:.3} stage_total_ms={:.3}",
        stage,
        samples_total,
        density_cells,
        density_nonzero,
        profile::millis(density_build),
        profile::millis(robust_scaling),
        profile::millis(reweight),
        profile::millis(total),
    );
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

pub(crate) fn apply_weighting_to_owned_batches_with_options(
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
            let stage_started = profile::maybe_profile_now();
            let density_started = profile::maybe_profile_now();
            let density = build_density_grid(
                &batches,
                gridder,
                density_includes_conjugates(density_build_convention),
                density_build_convention,
            );
            let density_elapsed = profile::elapsed_since(density_started);
            let reweight_started = profile::maybe_profile_now();
            let weighted = reweight_owned_batches(
                batches,
                gridder,
                &density,
                density_convention,
                trace_weighting,
                DensityReweightMode::Uniform,
            );
            let reweight_elapsed = profile::elapsed_since(reweight_started);
            let samples_total = visibility_sample_count(&weighted);
            let weighted = apply_optional_uv_taper(weighted, uv_taper);
            log_weighting_stage(
                "owned_uniform",
                samples_total,
                Some(&density),
                density_elapsed,
                profile::elapsed_since(None),
                reweight_elapsed,
                profile::elapsed_since(stage_started),
            );
            Ok(weighted)
        }
        WeightingMode::Briggs { robust } | WeightingMode::BriggsBwTaper { robust } => {
            let stage_started = profile::maybe_profile_now();
            let density_started = profile::maybe_profile_now();
            let density = build_density_grid(
                &batches,
                gridder,
                density_includes_conjugates(density_build_convention),
                density_build_convention,
            );
            let density_elapsed = profile::elapsed_since(density_started);
            let robust_started = profile::maybe_profile_now();
            let density_weight_sum = density.iter().map(|value| f64::from(*value)).sum::<f64>();
            let total_density_weight =
                robust_density_weight_sum_for_f2(density_weight_sum, density_build_convention);
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
            let robust_elapsed = profile::elapsed_since(robust_started);
            let reweight_started = profile::maybe_profile_now();
            let weighted = reweight_owned_batches(
                batches,
                gridder,
                &density,
                density_convention,
                trace_weighting,
                DensityReweightMode::Briggs {
                    f2,
                    use_bandwidth_taper: matches!(weighting, WeightingMode::BriggsBwTaper { .. }),
                    fractional_bandwidth,
                },
            );
            let reweight_elapsed = profile::elapsed_since(reweight_started);
            let samples_total = visibility_sample_count(&weighted);
            let weighted = apply_optional_uv_taper(weighted, uv_taper);
            log_weighting_stage(
                "owned_briggs",
                samples_total,
                Some(&density),
                density_elapsed,
                robust_elapsed,
                reweight_elapsed,
                profile::elapsed_since(stage_started),
            );
            Ok(weighted)
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
            let total_density_weight =
                robust_density_weight_sum_for_f2(density_weight_sum, density_build_convention);
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
            if trace_weighting_enabled() {
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

pub(crate) fn apply_weighting_to_owned_batches_by_sample_groups(
    weighting: WeightingMode,
    weight_density_mode: WeightDensityMode,
    uv_taper: Option<GaussianUvTaper>,
    fractional_bandwidth: f64,
    mut target_batches: Vec<VisibilityBatch>,
    sample_groups: &[Vec<(usize, usize)>],
    gridder: &StandardGridder,
) -> Result<Vec<VisibilityBatch>, crate::ImagingError> {
    match weighting {
        WeightingMode::Natural => return Ok(apply_optional_uv_taper(target_batches, uv_taper)),
        WeightingMode::Uniform
        | WeightingMode::Briggs { .. }
        | WeightingMode::BriggsBwTaper { .. } => {}
    }

    let density_convention = density_cell_convention(weighting, weight_density_mode);
    let density_build_convention = density_build_cell_convention(weighting, weight_density_mode);
    let mirror_hermitian = density_includes_conjugates(density_build_convention);
    let trace_weighting = trace_weighting_enabled();

    for positions in sample_groups {
        let density = build_density_grid_for_positions(
            &target_batches,
            positions,
            gridder,
            mirror_hermitian,
            density_build_convention,
        )?;
        let mode = match weighting {
            WeightingMode::Uniform => DensityReweightMode::Uniform,
            WeightingMode::Briggs { robust } | WeightingMode::BriggsBwTaper { robust } => {
                let density_weight_sum = density.iter().map(|value| f64::from(*value)).sum::<f64>();
                let sumlocwt = density
                    .iter()
                    .filter(|value| **value > 0.0)
                    .map(|value| f64::from(*value) * f64::from(*value))
                    .sum::<f64>();
                let total_density_weight =
                    robust_density_weight_sum_for_f2(density_weight_sum, density_build_convention);
                let f2 = if sumlocwt > 0.0 && total_density_weight > 0.0 {
                    (5.0f64 * 10f64.powf(-(robust as f64))).powi(2)
                        / (sumlocwt / total_density_weight)
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
                DensityReweightMode::Briggs {
                    f2,
                    use_bandwidth_taper: matches!(weighting, WeightingMode::BriggsBwTaper { .. }),
                    fractional_bandwidth,
                }
            }
            WeightingMode::Natural => unreachable!("natural weighting returned above"),
        };
        reweight_positions_in_place(
            &mut target_batches,
            positions,
            gridder,
            &density,
            density_convention,
            trace_weighting,
            mode,
        )?;
    }

    Ok(apply_optional_uv_taper(target_batches, uv_taper))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VisibilitySampleRangeRef {
    pub batch_index: usize,
    pub range: VisibilitySampleRange,
}

pub(crate) fn apply_weighting_to_owned_batches_by_sample_range_groups(
    weighting: WeightingMode,
    weight_density_mode: WeightDensityMode,
    uv_taper: Option<GaussianUvTaper>,
    fractional_bandwidth: f64,
    mut target_batches: Vec<VisibilityBatch>,
    sample_groups: &[Vec<VisibilitySampleRangeRef>],
    gridder: &StandardGridder,
) -> Result<Vec<VisibilityBatch>, crate::ImagingError> {
    match weighting {
        WeightingMode::Natural => return Ok(apply_optional_uv_taper(target_batches, uv_taper)),
        WeightingMode::Uniform
        | WeightingMode::Briggs { .. }
        | WeightingMode::BriggsBwTaper { .. } => {}
    }

    let density_convention = density_cell_convention(weighting, weight_density_mode);
    let density_build_convention = density_build_cell_convention(weighting, weight_density_mode);
    let mirror_hermitian = density_includes_conjugates(density_build_convention);
    let trace_weighting = trace_weighting_enabled();

    for ranges in sample_groups {
        let density = build_density_grid_for_sample_ranges(
            &target_batches,
            ranges,
            gridder,
            mirror_hermitian,
            density_build_convention,
        )?;
        let mode = match weighting {
            WeightingMode::Uniform => DensityReweightMode::Uniform,
            WeightingMode::Briggs { robust } | WeightingMode::BriggsBwTaper { robust } => {
                let density_weight_sum = density.iter().map(|value| f64::from(*value)).sum::<f64>();
                let sumlocwt = density
                    .iter()
                    .filter(|value| **value > 0.0)
                    .map(|value| f64::from(*value) * f64::from(*value))
                    .sum::<f64>();
                let total_density_weight =
                    robust_density_weight_sum_for_f2(density_weight_sum, density_build_convention);
                let f2 = if sumlocwt > 0.0 && total_density_weight > 0.0 {
                    (5.0f64 * 10f64.powf(-(robust as f64))).powi(2)
                        / (sumlocwt / total_density_weight)
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
                DensityReweightMode::Briggs {
                    f2,
                    use_bandwidth_taper: matches!(weighting, WeightingMode::BriggsBwTaper { .. }),
                    fractional_bandwidth,
                }
            }
            WeightingMode::Natural => unreachable!("natural weighting returned above"),
        };
        reweight_sample_ranges_in_place(
            &mut target_batches,
            ranges,
            gridder,
            &density,
            density_convention,
            trace_weighting,
            mode,
        )?;
    }

    Ok(apply_optional_uv_taper(target_batches, uv_taper))
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
        DensityCellConvention::VisImagingWeight
        | DensityCellConvention::MosaicVisImagingWeight
        | DensityCellConvention::CubeBriggsWeightorDensity => true,
        DensityCellConvention::CubeBriggsWeightorLookup => false,
    }
}

fn robust_density_weight_sum_for_f2(
    density_weight_sum: f64,
    build_convention: DensityCellConvention,
) -> f64 {
    if density_includes_conjugates(build_convention) {
        density_weight_sum
    } else {
        2.0 * density_weight_sum
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
    accumulate_density_grid_serial(
        batches,
        gridder,
        mirror_hermitian,
        convention,
        &mut density_grid,
        false,
    );
    density_grid
}

fn build_density_grid_for_positions(
    batches: &[VisibilityBatch],
    positions: &[(usize, usize)],
    gridder: &StandardGridder,
    mirror_hermitian: bool,
    convention: DensityCellConvention,
) -> Result<Array2<f32>, crate::ImagingError> {
    let [nx, ny] = gridder.density_grid_shape();
    let mut density_grid = Array2::<f32>::zeros((nx, ny));
    for &(batch_index, sample_index) in positions {
        let batch = batches.get(batch_index).ok_or_else(|| {
            crate::ImagingError::InvalidRequest(
                "mosaic weighting sample group references an unknown batch".to_string(),
            )
        })?;
        if sample_index >= batch.len() {
            return Err(crate::ImagingError::InvalidRequest(
                "mosaic weighting sample group references an unknown sample".to_string(),
            ));
        }
        let weight = batch.weight[sample_index];
        if !(weight.is_finite() && weight > 0.0) {
            continue;
        }
        accumulate_density_sample_serial(
            gridder,
            mirror_hermitian,
            convention,
            &mut density_grid,
            batch.u_lambda[sample_index],
            batch.v_lambda[sample_index],
            weight,
        );
    }
    Ok(density_grid)
}

fn build_density_grid_for_sample_ranges(
    batches: &[VisibilityBatch],
    ranges: &[VisibilitySampleRangeRef],
    gridder: &StandardGridder,
    mirror_hermitian: bool,
    convention: DensityCellConvention,
) -> Result<Array2<f32>, crate::ImagingError> {
    let [nx, ny] = gridder.density_grid_shape();
    let mut density_grid = Array2::<f32>::zeros((nx, ny));
    for range_ref in ranges {
        let batch = batches.get(range_ref.batch_index).ok_or_else(|| {
            crate::ImagingError::InvalidRequest(
                "mosaic weighting sample range group references an unknown batch".to_string(),
            )
        })?;
        if range_ref.range.start > range_ref.range.end || range_ref.range.end > batch.len() {
            return Err(crate::ImagingError::InvalidRequest(
                "mosaic weighting sample range group references an unknown sample range"
                    .to_string(),
            ));
        }
        for sample_index in range_ref.range.start..range_ref.range.end {
            let weight = batch.weight[sample_index];
            if !(weight.is_finite() && weight > 0.0) {
                continue;
            }
            accumulate_density_sample_serial(
                gridder,
                mirror_hermitian,
                convention,
                &mut density_grid,
                batch.u_lambda[sample_index],
                batch.v_lambda[sample_index],
                weight,
            );
        }
    }
    Ok(density_grid)
}

fn accumulate_density_grid_serial(
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    mirror_hermitian: bool,
    convention: DensityCellConvention,
    density_grid: &mut Array2<f32>,
    collect_stats: bool,
) -> WeightingWorkStats {
    let mut stats = WeightingWorkStats::default();
    for batch in batches {
        for index in 0..batch.len() {
            let weight = batch.weight[index];
            if !(weight.is_finite() && weight > 0.0) {
                if collect_stats {
                    stats.skipped_invalid_weight += 1;
                }
                continue;
            }
            let u_lambda = batch.u_lambda[index];
            let v_lambda = batch.v_lambda[index];
            let hits = accumulate_density_sample_serial(
                gridder,
                mirror_hermitian,
                convention,
                density_grid,
                u_lambda,
                v_lambda,
                weight,
            );
            if !collect_stats {
                continue;
            }
            if hits > 0 {
                stats.accepted_samples += 1;
                stats.density_cell_hits += hits;
            } else {
                stats.skipped_out_of_grid += 1;
            }
        }
    }
    stats
}

fn accumulate_density_grid_sample_range_serial(
    batch: &VisibilityBatch,
    sample_range: std::ops::Range<usize>,
    gridder: &StandardGridder,
    mirror_hermitian: bool,
    convention: DensityCellConvention,
    density_grid: &mut Array2<f32>,
    collect_stats: bool,
) -> WeightingWorkStats {
    let mut stats = WeightingWorkStats::default();
    for index in sample_range {
        let weight = batch.weight[index];
        if !(weight.is_finite() && weight > 0.0) {
            if collect_stats {
                stats.skipped_invalid_weight += 1;
            }
            continue;
        }
        let hits = accumulate_density_sample_serial(
            gridder,
            mirror_hermitian,
            convention,
            density_grid,
            batch.u_lambda[index],
            batch.v_lambda[index],
            weight,
        );
        if !collect_stats {
            continue;
        }
        if hits > 0 {
            stats.accepted_samples += 1;
            stats.density_cell_hits += hits;
        } else {
            stats.skipped_out_of_grid += 1;
        }
    }
    stats
}

#[inline]
fn accumulate_density_sample_serial(
    gridder: &StandardGridder,
    mirror_hermitian: bool,
    convention: DensityCellConvention,
    density_grid: &mut Array2<f32>,
    u_lambda: f64,
    v_lambda: f64,
    weight: f32,
) -> usize {
    let mut hits = 0usize;
    if let Some((x, y)) = gridder.density_cell_index_with_convention(u_lambda, v_lambda, convention)
    {
        density_grid[(x, y)] += weight;
        hits += 1;
    }
    if mirror_hermitian
        && let Some((x, y)) =
            gridder.density_cell_index_with_convention(-u_lambda, -v_lambda, convention)
    {
        density_grid[(x, y)] += weight;
        hits += 1;
    }
    hits
}

fn build_density_grid_parallel(
    batches: &[VisibilityBatch],
    gridder: &StandardGridder,
    mirror_hermitian: bool,
    convention: DensityCellConvention,
    thread_count: usize,
) -> Array2<f32> {
    let requested_threads = standard_mfs_worker_threads();
    let [nx, ny] = gridder.density_grid_shape();
    if batches.len() == 1 {
        return build_density_grid_parallel_single_batch(
            &batches[0],
            gridder,
            mirror_hermitian,
            convention,
            thread_count,
            requested_threads,
        );
    }
    let actual_threads = thread_count.min(batches.len()).max(1);
    let chunk_len = batches.len().div_ceil(actual_threads);
    let stage_started = profile::maybe_profile_now();
    let collect_stats = profile::standard_mfs_profile_detail_enabled();
    let mut local_grids = Vec::with_capacity(actual_threads);
    let join_started = profile::maybe_profile_now();
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(actual_threads);
        for chunk in batches.chunks(chunk_len) {
            let worker_samples = visibility_sample_count(chunk);
            handles.push(scope.spawn(move || {
                let alloc_started = profile::maybe_profile_now();
                let mut density_grid = Array2::<f32>::zeros((nx, ny));
                let alloc_elapsed = profile::elapsed_since(alloc_started);
                let compute_started = profile::maybe_profile_now();
                let stats = accumulate_density_grid_serial(
                    chunk,
                    gridder,
                    mirror_hermitian,
                    convention,
                    &mut density_grid,
                    collect_stats,
                );
                let compute_elapsed = profile::elapsed_since(compute_started);
                (
                    worker_samples,
                    stats,
                    density_grid,
                    alloc_elapsed,
                    compute_elapsed,
                )
            }));
        }
        for handle in handles {
            local_grids.push(handle.join().expect("standard MFS density worker panicked"));
        }
    });
    let join_elapsed = profile::elapsed_since(join_started);

    let mut density_grid = Array2::<f32>::zeros((nx, ny));
    let merge_started = profile::maybe_profile_now();
    for (_, _, local_grid, _, _) in &local_grids {
        add_f32_grid(&mut density_grid, local_grid);
    }
    let merge_elapsed = profile::elapsed_since(merge_started);
    profile::log_parallel_stage(profile::ParallelStageProfile {
        stage: "weighting_density",
        requested_threads,
        actual_threads: local_grids.len(),
        chunking: "batch",
        chunk_len,
        samples_total: visibility_sample_count(batches),
        samples_per_worker: local_grids
            .iter()
            .map(|(worker_samples, _, _, _, _)| *worker_samples)
            .collect(),
        local_grid_bytes_per_worker: nx
            .saturating_mul(ny)
            .saturating_mul(std::mem::size_of::<f32>()),
        local_grid_count: 1,
        local_alloc_zero_by_worker: local_grids
            .iter()
            .map(|(_, _, _, alloc_elapsed, _)| *alloc_elapsed)
            .collect(),
        worker_compute_by_worker: local_grids
            .iter()
            .map(|(_, _, _, _, compute_elapsed)| *compute_elapsed)
            .collect(),
        join_duration: join_elapsed,
        merge_duration: merge_elapsed,
        stage_duration: profile::elapsed_since(stage_started),
    });
    for (worker_index, (worker_samples, stats, _, alloc_elapsed, compute_elapsed)) in
        local_grids.iter().enumerate()
    {
        profile::log_parallel_worker(profile::ParallelWorkerProfile {
            stage: "weighting_density",
            worker_index,
            samples: *worker_samples,
            accepted_samples: stats.accepted_samples,
            finite_visibility_samples: 0,
            nonfinite_visibility_samples: 0,
            skipped_not_gridable: 0,
            skipped_invalid_weight: stats.skipped_invalid_weight,
            skipped_invalid_sumwt: 0,
            skipped_invalid_density: stats.skipped_invalid_density,
            skipped_out_of_grid: stats.skipped_out_of_grid,
            degrid_tap_visits: 0,
            grid_tap_visits: 0,
            density_cell_hits: stats.density_cell_hits,
            local_alloc_zero: *alloc_elapsed,
            worker_compute: *compute_elapsed,
        });
    }
    density_grid
}

fn build_density_grid_parallel_single_batch(
    batch: &VisibilityBatch,
    gridder: &StandardGridder,
    mirror_hermitian: bool,
    convention: DensityCellConvention,
    thread_count: usize,
    requested_threads: usize,
) -> Array2<f32> {
    let [nx, ny] = gridder.density_grid_shape();
    let actual_threads = thread_count.min(batch.len()).max(1);
    let chunk_len = batch.len().div_ceil(actual_threads);
    let stage_started = profile::maybe_profile_now();
    let collect_stats = profile::standard_mfs_profile_detail_enabled();
    let mut local_grids = Vec::with_capacity(actual_threads);
    let join_started = profile::maybe_profile_now();
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(actual_threads);
        for start in (0..batch.len()).step_by(chunk_len) {
            let end = (start + chunk_len).min(batch.len());
            handles.push(scope.spawn(move || {
                let alloc_started = profile::maybe_profile_now();
                let mut density_grid = Array2::<f32>::zeros((nx, ny));
                let alloc_elapsed = profile::elapsed_since(alloc_started);
                let compute_started = profile::maybe_profile_now();
                let stats = accumulate_density_grid_sample_range_serial(
                    batch,
                    start..end,
                    gridder,
                    mirror_hermitian,
                    convention,
                    &mut density_grid,
                    collect_stats,
                );
                let compute_elapsed = profile::elapsed_since(compute_started);
                (
                    end - start,
                    stats,
                    density_grid,
                    alloc_elapsed,
                    compute_elapsed,
                )
            }));
        }
        for handle in handles {
            local_grids.push(handle.join().expect("density worker panicked"));
        }
    });
    let join_elapsed = profile::elapsed_since(join_started);

    let mut density_grid = Array2::<f32>::zeros((nx, ny));
    let merge_started = profile::maybe_profile_now();
    for (_, _, local_grid, _, _) in &local_grids {
        add_f32_grid(&mut density_grid, local_grid);
    }
    let merge_elapsed = profile::elapsed_since(merge_started);
    profile::log_parallel_stage(profile::ParallelStageProfile {
        stage: "weighting_density",
        requested_threads,
        actual_threads: local_grids.len(),
        chunking: "sample-range",
        chunk_len,
        samples_total: batch.len(),
        samples_per_worker: local_grids
            .iter()
            .map(|(worker_samples, _, _, _, _)| *worker_samples)
            .collect(),
        local_grid_bytes_per_worker: nx
            .saturating_mul(ny)
            .saturating_mul(std::mem::size_of::<f32>()),
        local_grid_count: 1,
        local_alloc_zero_by_worker: local_grids
            .iter()
            .map(|(_, _, _, alloc_elapsed, _)| *alloc_elapsed)
            .collect(),
        worker_compute_by_worker: local_grids
            .iter()
            .map(|(_, _, _, _, compute_elapsed)| *compute_elapsed)
            .collect(),
        join_duration: join_elapsed,
        merge_duration: merge_elapsed,
        stage_duration: profile::elapsed_since(stage_started),
    });
    for (worker_index, (worker_samples, stats, _, alloc_elapsed, compute_elapsed)) in
        local_grids.iter().enumerate()
    {
        profile::log_parallel_worker(profile::ParallelWorkerProfile {
            stage: "weighting_density",
            worker_index,
            samples: *worker_samples,
            accepted_samples: stats.accepted_samples,
            finite_visibility_samples: 0,
            nonfinite_visibility_samples: 0,
            skipped_not_gridable: 0,
            skipped_invalid_weight: stats.skipped_invalid_weight,
            skipped_invalid_sumwt: 0,
            skipped_invalid_density: stats.skipped_invalid_density,
            skipped_out_of_grid: stats.skipped_out_of_grid,
            degrid_tap_visits: 0,
            grid_tap_visits: 0,
            density_cell_hits: stats.density_cell_hits,
            local_alloc_zero: *alloc_elapsed,
            worker_compute: *compute_elapsed,
        });
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
            let _ = reweight_owned_batch_in_place(
                batch,
                gridder,
                density,
                convention,
                trace_weighting,
                mode,
                false,
            );
        }
        return batches;
    }

    let chunk_len = batches.len().div_ceil(thread_count);
    let stage_started = profile::maybe_profile_now();
    let collect_stats = profile::standard_mfs_profile_detail_enabled();
    let mut worker_profiles = Vec::with_capacity(thread_count);
    let join_started = profile::maybe_profile_now();
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(thread_count);
        for chunk in batches.chunks_mut(chunk_len) {
            let worker_samples = visibility_sample_count(chunk);
            handles.push(scope.spawn(move || {
                let compute_started = profile::maybe_profile_now();
                let mut stats = WeightingWorkStats::default();
                for batch in chunk {
                    stats.add(reweight_owned_batch_in_place(
                        batch,
                        gridder,
                        density,
                        convention,
                        false,
                        mode,
                        collect_stats,
                    ));
                }
                let compute_elapsed = profile::elapsed_since(compute_started);
                (worker_samples, stats, compute_elapsed)
            }));
        }
        for handle in handles {
            worker_profiles.push(
                handle
                    .join()
                    .expect("standard MFS reweight worker panicked"),
            );
        }
    });
    let join_elapsed = profile::elapsed_since(join_started);
    profile::log_parallel_stage(profile::ParallelStageProfile {
        stage: "weighting_reweight",
        requested_threads,
        actual_threads: worker_profiles.len(),
        chunking: "batch",
        chunk_len,
        samples_total: visibility_sample_count(&batches),
        samples_per_worker: worker_profiles
            .iter()
            .map(|(worker_samples, _, _)| *worker_samples)
            .collect(),
        local_grid_bytes_per_worker: 0,
        local_grid_count: 0,
        local_alloc_zero_by_worker: vec![std::time::Duration::ZERO; worker_profiles.len()],
        worker_compute_by_worker: worker_profiles
            .iter()
            .map(|(_, _, compute_elapsed)| *compute_elapsed)
            .collect(),
        join_duration: join_elapsed,
        merge_duration: std::time::Duration::ZERO,
        stage_duration: profile::elapsed_since(stage_started),
    });
    for (worker_index, (worker_samples, stats, compute_elapsed)) in
        worker_profiles.iter().enumerate()
    {
        profile::log_parallel_worker(profile::ParallelWorkerProfile {
            stage: "weighting_reweight",
            worker_index,
            samples: *worker_samples,
            accepted_samples: stats.accepted_samples,
            finite_visibility_samples: 0,
            nonfinite_visibility_samples: 0,
            skipped_not_gridable: 0,
            skipped_invalid_weight: stats.skipped_invalid_weight,
            skipped_invalid_sumwt: 0,
            skipped_invalid_density: stats.skipped_invalid_density,
            skipped_out_of_grid: stats.skipped_out_of_grid,
            degrid_tap_visits: 0,
            grid_tap_visits: 0,
            density_cell_hits: stats.density_cell_hits,
            local_alloc_zero: std::time::Duration::ZERO,
            worker_compute: *compute_elapsed,
        });
    }
    batches
}

fn reweight_positions_in_place(
    batches: &mut [VisibilityBatch],
    positions: &[(usize, usize)],
    gridder: &StandardGridder,
    density: &Array2<f32>,
    convention: DensityCellConvention,
    trace_weighting: bool,
    mode: DensityReweightMode,
) -> Result<(), crate::ImagingError> {
    for &(batch_index, sample_index) in positions {
        let batch = batches.get_mut(batch_index).ok_or_else(|| {
            crate::ImagingError::InvalidRequest(
                "mosaic weighting sample group references an unknown batch".to_string(),
            )
        })?;
        if sample_index >= batch.len() {
            return Err(crate::ImagingError::InvalidRequest(
                "mosaic weighting sample group references an unknown sample".to_string(),
            ));
        }
        let weight = batch.weight[sample_index];
        let u_lambda = batch.u_lambda[sample_index];
        let v_lambda = batch.v_lambda[sample_index];
        let Some(cell_density) =
            gridder.density_at_with_convention(density, u_lambda, v_lambda, convention)
        else {
            batch.weight[sample_index] = 0.0;
            continue;
        };
        if !(weight.is_finite() && weight > 0.0 && cell_density.is_finite() && cell_density > 0.0) {
            batch.weight[sample_index] = 0.0;
            if trace_weighting {
                trace_weighting_sample(
                    sample_index,
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
        let output_weight =
            reweight_density_sample(weight, cell_density, u_lambda, v_lambda, gridder, mode);
        if trace_weighting {
            trace_weighting_sample(
                sample_index,
                u_lambda,
                v_lambda,
                weight,
                cell_density,
                output_weight,
                gridder.density_cell_index_with_convention(u_lambda, v_lambda, convention),
            );
        }
        batch.weight[sample_index] = output_weight;
    }
    Ok(())
}

fn reweight_sample_ranges_in_place(
    batches: &mut [VisibilityBatch],
    ranges: &[VisibilitySampleRangeRef],
    gridder: &StandardGridder,
    density: &Array2<f32>,
    convention: DensityCellConvention,
    trace_weighting: bool,
    mode: DensityReweightMode,
) -> Result<(), crate::ImagingError> {
    for range_ref in ranges {
        let batch = batches.get_mut(range_ref.batch_index).ok_or_else(|| {
            crate::ImagingError::InvalidRequest(
                "mosaic weighting sample range group references an unknown batch".to_string(),
            )
        })?;
        if range_ref.range.start > range_ref.range.end || range_ref.range.end > batch.len() {
            return Err(crate::ImagingError::InvalidRequest(
                "mosaic weighting sample range group references an unknown sample range"
                    .to_string(),
            ));
        }
        for sample_index in range_ref.range.start..range_ref.range.end {
            let weight = batch.weight[sample_index];
            let u_lambda = batch.u_lambda[sample_index];
            let v_lambda = batch.v_lambda[sample_index];
            let Some(cell_density) =
                gridder.density_at_with_convention(density, u_lambda, v_lambda, convention)
            else {
                batch.weight[sample_index] = 0.0;
                continue;
            };
            if !(weight.is_finite()
                && weight > 0.0
                && cell_density.is_finite()
                && cell_density > 0.0)
            {
                batch.weight[sample_index] = 0.0;
                if trace_weighting {
                    trace_weighting_sample(
                        sample_index,
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
            let output_weight =
                reweight_density_sample(weight, cell_density, u_lambda, v_lambda, gridder, mode);
            if trace_weighting {
                trace_weighting_sample(
                    sample_index,
                    u_lambda,
                    v_lambda,
                    weight,
                    cell_density,
                    output_weight,
                    gridder.density_cell_index_with_convention(u_lambda, v_lambda, convention),
                );
            }
            batch.weight[sample_index] = output_weight;
        }
    }
    Ok(())
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
    collect_stats: bool,
) -> WeightingWorkStats {
    if !trace_weighting {
        return match mode {
            DensityReweightMode::Uniform => reweight_owned_batch_uniform_in_place(
                batch,
                gridder,
                density,
                convention,
                collect_stats,
            ),
            DensityReweightMode::Briggs {
                f2,
                use_bandwidth_taper: false,
                ..
            } => reweight_owned_batch_briggs_in_place(
                batch,
                gridder,
                density,
                convention,
                f2,
                collect_stats,
            ),
            DensityReweightMode::Briggs {
                f2,
                use_bandwidth_taper: true,
                fractional_bandwidth,
            } => reweight_owned_batch_briggs_taper_in_place(
                batch,
                gridder,
                density,
                convention,
                f2,
                fractional_bandwidth,
                collect_stats,
            ),
        };
    }
    reweight_owned_batch_with_transform(
        batch,
        gridder,
        density,
        convention,
        trace_weighting,
        collect_stats,
        |weight, density, u_lambda, v_lambda| {
            reweight_density_sample(weight, density, u_lambda, v_lambda, gridder, mode)
        },
    )
}

fn reweight_owned_batch_uniform_in_place(
    batch: &mut VisibilityBatch,
    gridder: &StandardGridder,
    density: &Array2<f32>,
    convention: DensityCellConvention,
    collect_stats: bool,
) -> WeightingWorkStats {
    let mut stats = WeightingWorkStats::default();
    for index in 0..batch.len() {
        let weight = batch.weight[index];
        let Some(cell_density) = gridder.density_at_with_convention(
            density,
            batch.u_lambda[index],
            batch.v_lambda[index],
            convention,
        ) else {
            batch.weight[index] = 0.0;
            if collect_stats {
                stats.skipped_out_of_grid += 1;
            }
            continue;
        };
        if collect_stats {
            stats.density_cell_hits += 1;
        }
        batch.weight[index] =
            if weight.is_finite() && weight > 0.0 && cell_density.is_finite() && cell_density > 0.0
            {
                if collect_stats {
                    stats.accepted_samples += 1;
                }
                weight / cell_density
            } else {
                if collect_stats {
                    if !(weight.is_finite() && weight > 0.0) {
                        stats.skipped_invalid_weight += 1;
                    } else {
                        stats.skipped_invalid_density += 1;
                    }
                }
                0.0
            };
    }
    stats
}

fn reweight_owned_batch_briggs_in_place(
    batch: &mut VisibilityBatch,
    gridder: &StandardGridder,
    density: &Array2<f32>,
    convention: DensityCellConvention,
    f2: f32,
    collect_stats: bool,
) -> WeightingWorkStats {
    let mut stats = WeightingWorkStats::default();
    for index in 0..batch.len() {
        let weight = batch.weight[index];
        let Some(cell_density) = gridder.density_at_with_convention(
            density,
            batch.u_lambda[index],
            batch.v_lambda[index],
            convention,
        ) else {
            batch.weight[index] = 0.0;
            if collect_stats {
                stats.skipped_out_of_grid += 1;
            }
            continue;
        };
        if collect_stats {
            stats.density_cell_hits += 1;
        }
        batch.weight[index] =
            if weight.is_finite() && weight > 0.0 && cell_density.is_finite() && cell_density > 0.0
            {
                if collect_stats {
                    stats.accepted_samples += 1;
                }
                weight / (f2 * cell_density + 1.0)
            } else {
                if collect_stats {
                    if !(weight.is_finite() && weight > 0.0) {
                        stats.skipped_invalid_weight += 1;
                    } else {
                        stats.skipped_invalid_density += 1;
                    }
                }
                0.0
            };
    }
    stats
}

fn reweight_owned_batch_briggs_taper_in_place(
    batch: &mut VisibilityBatch,
    gridder: &StandardGridder,
    density: &Array2<f32>,
    convention: DensityCellConvention,
    f2: f32,
    fractional_bandwidth: f64,
    collect_stats: bool,
) -> WeightingWorkStats {
    let mut stats = WeightingWorkStats::default();
    for index in 0..batch.len() {
        let weight = batch.weight[index];
        let u_lambda = batch.u_lambda[index];
        let v_lambda = batch.v_lambda[index];
        let Some(cell_density) =
            gridder.density_at_with_convention(density, u_lambda, v_lambda, convention)
        else {
            batch.weight[index] = 0.0;
            if collect_stats {
                stats.skipped_out_of_grid += 1;
            }
            continue;
        };
        if collect_stats {
            stats.density_cell_hits += 1;
        }
        batch.weight[index] =
            if weight.is_finite() && weight > 0.0 && cell_density.is_finite() && cell_density > 0.0
            {
                if collect_stats {
                    stats.accepted_samples += 1;
                }
                let taper_factor = briggs_bw_taper_uv_distance_factor(
                    fractional_bandwidth,
                    gridder,
                    u_lambda,
                    v_lambda,
                ) as f32;
                weight / ((f2 * cell_density) / taper_factor + 1.0)
            } else {
                if collect_stats {
                    if !(weight.is_finite() && weight > 0.0) {
                        stats.skipped_invalid_weight += 1;
                    } else {
                        stats.skipped_invalid_density += 1;
                    }
                }
                0.0
            };
    }
    stats
}

fn reweight_owned_batch_with_transform(
    batch: &mut VisibilityBatch,
    gridder: &StandardGridder,
    density: &Array2<f32>,
    convention: DensityCellConvention,
    trace_weighting: bool,
    collect_stats: bool,
    transform: impl Fn(f32, f32, f64, f64) -> f32,
) -> WeightingWorkStats {
    let mut stats = WeightingWorkStats::default();
    for index in 0..batch.len() {
        let weight = batch.weight[index];
        let u_lambda = batch.u_lambda[index];
        let v_lambda = batch.v_lambda[index];
        let Some(cell_density) =
            gridder.density_at_with_convention(density, u_lambda, v_lambda, convention)
        else {
            batch.weight[index] = 0.0;
            if collect_stats {
                stats.skipped_out_of_grid += 1;
            }
            continue;
        };
        if collect_stats {
            stats.density_cell_hits += 1;
        }
        if !(weight.is_finite() && weight > 0.0 && cell_density.is_finite() && cell_density > 0.0) {
            batch.weight[index] = 0.0;
            if collect_stats {
                if !(weight.is_finite() && weight > 0.0) {
                    stats.skipped_invalid_weight += 1;
                } else {
                    stats.skipped_invalid_density += 1;
                }
            }
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
        if collect_stats {
            stats.accepted_samples += 1;
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
    stats
}

fn trace_weighting_enabled() -> bool {
    static ENABLED: LazyLock<bool> =
        LazyLock::new(|| std::env::var_os("CASA_RS_TRACE_RUST_WEIGHTING").is_some());
    *ENABLED
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

/// Compute the CASA cube Briggs robust scale factor for one density plane.
///
/// Uniform weighting returns `1.0`. Natural or unsupported weighting modes
/// return `0.0` so callers can reject the sample without special casing.
pub fn casa_cube_briggs_f2(weighting: WeightingMode, density: &Array2<f32>) -> f32 {
    if weighting == WeightingMode::Uniform {
        return 1.0;
    }
    let robust = match weighting {
        WeightingMode::Briggs { robust } | WeightingMode::BriggsBwTaper { robust } => robust,
        _ => return 0.0,
    };
    let density_weight_sum = density.iter().map(|value| f64::from(*value)).sum::<f64>();
    let sumlocwt = density
        .iter()
        .filter(|value| **value > 0.0)
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>();
    if sumlocwt > 0.0 && density_weight_sum > 0.0 {
        ((5.0f64 * 10f64.powf(-(robust as f64))).powi(2) / (sumlocwt / density_weight_sum)) as f32
    } else {
        0.0
    }
}

/// Map a cube Briggs `GridFT` density sample in wavelengths to a density cell.
///
/// This follows CASA's one-based intermediate rounding path before converting
/// back to zero-based Rust indices.
pub fn casa_cube_briggs_gridft_density_cell_from_lambda(
    shape: (usize, usize),
    u_lambda: f64,
    v_lambda: f64,
    cell_size_rad: [f64; 2],
) -> Option<(usize, usize)> {
    let nx = shape.0 as f64;
    let ny = shape.1 as f64;
    let x_loc = (u_lambda * nx * cell_size_rad[0] + nx / 2.0 + 1.0).round() as isize;
    let y_loc = (-v_lambda * ny * cell_size_rad[1] + ny / 2.0 + 1.0).round() as isize;
    let x = x_loc - 1;
    let y = y_loc - 1;
    if x <= 0 || y <= 0 || x >= shape.0 as isize || y >= shape.1 as isize {
        return None;
    }
    Some((x as usize, y as usize))
}

/// Map a CASA cube Briggs density lookup in wavelengths to a density cell.
///
/// The calculation intentionally uses CASA-like `f32` rounding before the final
/// integer cell conversion.
pub fn casa_cube_briggs_density_cell_from_lambda(
    shape: (usize, usize),
    u_lambda: f64,
    v_lambda: f64,
    cell_size_rad: [f64; 2],
) -> Option<(usize, usize)> {
    let nx_f32 = shape.0 as f32;
    let ny_f32 = shape.1 as f32;
    let x =
        ((u_lambda as f32) * nx_f32 * (cell_size_rad[0] as f32) + nx_f32 / 2.0).round() as isize;
    let y =
        (-(v_lambda as f32) * ny_f32 * (cell_size_rad[1] as f32) + ny_f32 / 2.0).round() as isize;
    if x <= 0 || y <= 0 || x >= shape.0 as isize || y >= shape.1 as isize {
        return None;
    }
    Some((x as usize, y as usize))
}

/// Compute the CASA cube Briggs preweighting denominator for a density cell.
pub fn casa_cube_briggs_weight_denominator(
    weighting: WeightingMode,
    geometry: ImageGeometry,
    fractional_bandwidth: f64,
    u_lambda: f64,
    v_lambda: f64,
    density: f32,
    f2: f32,
) -> f32 {
    match weighting {
        WeightingMode::Uniform => density,
        WeightingMode::BriggsBwTaper { .. } => {
            let taper = casa_cube_briggs_bw_taper_uv_distance_factor(
                geometry,
                fractional_bandwidth,
                u_lambda,
                v_lambda,
            ) as f32;
            (f2 * density) / taper + 1.0
        }
        WeightingMode::Briggs { .. } => f2 * density + 1.0,
        WeightingMode::Natural => 0.0,
    }
}

fn casa_cube_briggs_bw_taper_uv_distance_factor(
    geometry: ImageGeometry,
    fractional_bandwidth: f64,
    u_lambda: f64,
    v_lambda: f64,
) -> f64 {
    let nx = geometry.image_shape[0] as f64;
    let ny = geometry.image_shape[1] as f64;
    let u_cells = u_lambda * nx * geometry.cell_size_rad[0];
    let v_cells = v_lambda * ny * geometry.cell_size_rad[1];
    let n_cells_bw = fractional_bandwidth * (u_cells * u_cells + v_cells * v_cells).sqrt();
    let mut factor = n_cells_bw + 0.5;
    if factor < 1.5 {
        factor = (4.0 - n_cells_bw) / (4.0 - 2.0 * n_cells_bw);
    }
    factor.max(f64::MIN_POSITIVE)
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
        PlaneStokes, StandardMfsPairCollapseTransform, StandardMfsVisibilityPolarization,
        VisibilityBlockView, VisibilityFloatSamplesRef, VisibilitySourcePartition,
        VisibilitySourcePartitionId, VisibilitySourceShape,
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
    fn grouped_owned_weighting_matches_manual_group_batches() {
        let request = request_for(WeightingMode::Briggs { robust: 0.5 });
        let gridder = StandardGridder::new(request.geometry).unwrap();
        let sample_groups = vec![vec![(0, 0), (0, 1), (0, 2), (0, 3)], vec![(0, 4)]];

        let grouped = apply_weighting_to_owned_batches_by_sample_groups(
            request.weighting,
            WeightDensityMode::Combined,
            None,
            fractional_bandwidth_from_frequency_range(request.selected_frequency_range_hz),
            request.visibility_batches.clone(),
            &sample_groups,
            &gridder,
        )
        .unwrap();

        let mut manual = request.visibility_batches.clone();
        for positions in sample_groups {
            let source = &request.visibility_batches[0];
            let group_batch = VisibilityBatch {
                u_lambda: positions
                    .iter()
                    .map(|&(_, index)| source.u_lambda[index])
                    .collect(),
                v_lambda: positions
                    .iter()
                    .map(|&(_, index)| source.v_lambda[index])
                    .collect(),
                w_lambda: positions
                    .iter()
                    .map(|&(_, index)| source.w_lambda[index])
                    .collect(),
                weight: positions
                    .iter()
                    .map(|&(_, index)| source.weight[index])
                    .collect(),
                sumwt_factor: positions
                    .iter()
                    .map(|&(_, index)| source.sumwt_factor[index])
                    .collect(),
                gridable: positions
                    .iter()
                    .map(|&(_, index)| source.gridable[index])
                    .collect(),
                visibility: positions
                    .iter()
                    .map(|&(_, index)| source.visibility[index])
                    .collect(),
            };
            let weighted_group = apply_weighting_with_density_source(
                request.weighting,
                WeightDensityMode::Combined,
                None,
                fractional_bandwidth_from_frequency_range(request.selected_frequency_range_hz),
                std::slice::from_ref(&group_batch),
                std::slice::from_ref(&group_batch),
                &gridder,
            )
            .unwrap();
            for (group_index, &(_, sample_index)) in positions.iter().enumerate() {
                manual[0].weight[sample_index] = weighted_group[0].weight[group_index];
            }
        }

        assert_eq!(grouped, manual);
    }

    #[test]
    fn streaming_density_samples_match_batch_density_weighting() {
        for mode in [
            WeightingMode::Uniform,
            WeightingMode::Briggs { robust: 0.5 },
            WeightingMode::BriggsBwTaper { robust: 0.5 },
        ] {
            let request = request_for(mode);
            let mut batch_plan = StandardMfsStreamingWeightingPlan::new(
                request.geometry,
                mode,
                request.selected_frequency_range_hz,
            )
            .unwrap();
            batch_plan.accumulate_density_batches(&request.visibility_batches);
            batch_plan.finish_density_pass();
            let batch_weighted = batch_plan
                .weight_owned_batches(request.visibility_batches.clone())
                .unwrap();

            let mut sample_plan = StandardMfsStreamingWeightingPlan::new(
                request.geometry,
                mode,
                request.selected_frequency_range_hz,
            )
            .unwrap();
            for batch in &request.visibility_batches {
                for index in 0..batch.len() {
                    sample_plan.accumulate_density_sample(
                        batch.u_lambda[index],
                        batch.v_lambda[index],
                        batch.weight[index],
                    );
                }
            }
            sample_plan.finish_density_pass();
            let sample_weighted = sample_plan
                .weight_owned_batches(request.visibility_batches.clone())
                .unwrap();

            for (batch, sample) in batch_weighted.iter().zip(&sample_weighted) {
                assert_eq!(batch.len(), sample.len());
                for index in 0..batch.len() {
                    assert!((batch.weight[index] - sample.weight[index]).abs() < 1.0e-6);
                }
            }
        }
    }

    #[test]
    fn briggs_per_plane_density_source_preserves_aligned_sample_order() {
        let request = request_for(WeightingMode::Briggs { robust: 0.5 });
        let gridder = StandardGridder::new(request.geometry).unwrap();
        let target_batch = VisibilityBatch {
            u_lambda: vec![40.0, 140.0, 260.0],
            v_lambda: vec![10.0, -20.0, 35.0],
            w_lambda: vec![0.0; 3],
            weight: vec![1.0; 3],
            sumwt_factor: vec![1.0; 3],
            gridable: vec![true; 3],
            visibility: vec![Complex32::new(1.0, 0.0); 3],
        };
        let density_batch = VisibilityBatch {
            u_lambda: vec![0.0, 0.0, 260.0],
            v_lambda: vec![0.0, 0.0, 35.0],
            w_lambda: target_batch.w_lambda.clone(),
            weight: target_batch.weight.clone(),
            sumwt_factor: target_batch.sumwt_factor.clone(),
            gridable: target_batch.gridable.clone(),
            visibility: target_batch.visibility.clone(),
        };

        let sidecar_trace = trace_weighting_with_density_source(
            WeightingMode::Briggs { robust: 0.5 },
            WeightDensityMode::PerPlane,
            None,
            fractional_bandwidth_from_frequency_range(request.selected_frequency_range_hz),
            std::slice::from_ref(&target_batch),
            std::slice::from_ref(&density_batch),
            &gridder,
        )
        .unwrap();
        let target_trace = trace_weighting_with_density_source(
            WeightingMode::Briggs { robust: 0.5 },
            WeightDensityMode::PerPlane,
            None,
            fractional_bandwidth_from_frequency_range(request.selected_frequency_range_hz),
            std::slice::from_ref(&target_batch),
            std::slice::from_ref(&target_batch),
            &gridder,
        )
        .unwrap();

        assert_eq!(sidecar_trace.samples.len(), 3);
        for (index, sample) in sidecar_trace.samples.iter().enumerate() {
            assert_eq!(sample.batch_index, 0);
            assert_eq!(sample.sample_index, index);
            assert_eq!(sample.u_lambda, target_batch.u_lambda[index]);
            assert_eq!(sample.v_lambda, target_batch.v_lambda[index]);
        }
        assert_eq!(
            sidecar_trace.samples[0].density_weight,
            sidecar_trace.samples[1].density_weight
        );
        assert_ne!(
            sidecar_trace.samples[1].density_weight,
            target_trace.samples[1].density_weight
        );
        assert_ne!(
            sidecar_trace.weighted_batches[0].weight[1],
            target_trace.weighted_batches[0].weight[1]
        );
    }

    #[test]
    fn standard_mfs_density_array_row_applies_flags_and_weight_spectrum_precedence() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let mut explicit_plan = StandardMfsStreamingWeightingPlan::new(
            geometry,
            WeightingMode::Uniform,
            [1.0e9, 1.4e9],
        )
        .unwrap();
        let flags = Array2::from_shape_vec(
            (2, 4),
            vec![false, false, false, false, false, false, true, false],
        )
        .unwrap();
        let weight_spectrum =
            Array2::from_shape_vec((2, 4), vec![0.0, 2.0, f32::NAN, 3.0, 4.0, 5.0, 6.0, 0.0])
                .unwrap();
        let source_channels = [10, 11, 12, 13];
        let source_frequencies_hz = [1.0e9, 1.1e9, 1.2e9, 1.3e9];

        let accepted = accumulate_standard_mfs_density_row_from_arrays(
            &mut explicit_plan,
            [12.0, -7.0, 0.0],
            1.0,
            10,
            &flags,
            &[10.0, 20.0],
            Some(&weight_spectrum),
            &source_channels,
            &source_frequencies_hz,
            StandardMfsVisibilityPolarization::Explicit {
                corr_index: 0,
                sumwt_factor: 1.0,
            },
        )
        .unwrap();

        assert_eq!(accepted, 2);

        let mut paired_plan = StandardMfsStreamingWeightingPlan::new(
            geometry,
            WeightingMode::Uniform,
            [1.0e9, 1.4e9],
        )
        .unwrap();
        let accepted = accumulate_standard_mfs_density_row_from_arrays(
            &mut paired_plan,
            [12.0, -7.0, 0.0],
            1.0,
            10,
            &flags,
            &[10.0, 20.0],
            Some(&weight_spectrum),
            &source_channels,
            &source_frequencies_hz,
            StandardMfsVisibilityPolarization::CollapsedPair {
                first_corr_index: 0,
                second_corr_index: 1,
                transform: StandardMfsPairCollapseTransform::HalfSum,
                sumwt_factor: 2.0,
            },
        )
        .unwrap();

        assert_eq!(accepted, 1);
    }

    #[test]
    fn standard_mfs_density_visibility_block_uses_channel_invariant_weights() {
        let geometry = ImageGeometry {
            image_shape: [64, 64],
            cell_size_rad: [1.0e-4, 1.0e-4],
        };
        let mut plan = StandardMfsStreamingWeightingPlan::new(
            geometry,
            WeightingMode::Uniform,
            [1.0e9, 1.3e9],
        )
        .unwrap();
        let row_indices = [100usize, 101usize];
        let mut flags = vec![false; 3 * 2 * 2];
        flags[(2 * 2 + 1) * 2] = true;
        let weights = [99.0f32, 88.0, 4.0, 6.0];
        let source = VisibilityBlockView {
            partition: VisibilitySourcePartition {
                id: VisibilitySourcePartitionId(3),
                ms_id: 0,
                data_desc_id: 0,
                spectral_window_id: 0,
                polarization_id: 0,
                shape: VisibilitySourceShape {
                    channel_count: 8,
                    correlation_count: 2,
                },
            },
            row_indices: &row_indices,
            channel_start: 5,
            channel_count: 3,
            data: None,
            flags: Some(&flags),
            weights: Some(VisibilityFloatSamplesRef::Float32(&weights)),
            weight_spectrum: None,
            uvw_m: None,
            flag_row: None,
            antenna1: None,
            antenna2: None,
            field_ids: None,
            time: None,
        };

        let accepted = accumulate_standard_mfs_density_row_from_visibility_block(
            &mut plan,
            source,
            1,
            [9.0, 4.0, 0.0],
            1.0,
            &[5, 6, 7],
            &[1.0e9, 1.1e9, 1.2e9],
            StandardMfsVisibilityPolarization::CollapsedPair {
                first_corr_index: 0,
                second_corr_index: 1,
                transform: StandardMfsPairCollapseTransform::HalfSum,
                sumwt_factor: 2.0,
            },
        )
        .unwrap();

        assert_eq!(accepted, 2);
    }

    #[test]
    fn cube_briggs_density_helpers_match_casa_rounding() {
        let geometry = ImageGeometry {
            image_shape: [800, 800],
            cell_size_rad: [0.5 * std::f64::consts::PI / (180.0 * 3600.0); 2],
        };

        assert_eq!(
            casa_cube_briggs_density_cell_from_lambda(
                (geometry.image_shape[0], geometry.image_shape[1]),
                36242.06640625,
                7992.76708984375,
                geometry.cell_size_rad,
            ),
            Some((470, 385))
        );
        assert_eq!(
            casa_cube_briggs_gridft_density_cell_from_lambda(
                (geometry.image_shape[0], geometry.image_shape[1]),
                36242.06640625,
                7992.76708984375,
                geometry.cell_size_rad,
            ),
            Some((470, 384))
        );
    }

    #[test]
    fn cube_briggs_f2_and_denominator_follow_casa_formula() {
        let density = Array2::from_shape_vec((2, 2), vec![0.0, 2.0, 3.0, 0.0]).unwrap();
        let f2 = casa_cube_briggs_f2(WeightingMode::Briggs { robust: 0.0 }, &density);
        assert!((f2 - 9.615385).abs() < 1.0e-5);
        assert_eq!(casa_cube_briggs_f2(WeightingMode::Uniform, &density), 1.0);

        let geometry = ImageGeometry {
            image_shape: [800, 800],
            cell_size_rad: [0.5 * std::f64::consts::PI / (180.0 * 3600.0); 2],
        };
        assert_eq!(
            casa_cube_briggs_weight_denominator(
                WeightingMode::Briggs { robust: 0.0 },
                geometry,
                0.0,
                100.0,
                200.0,
                2.0,
                f2,
            ),
            f2 * 2.0 + 1.0
        );
        assert_eq!(
            casa_cube_briggs_weight_denominator(
                WeightingMode::Uniform,
                geometry,
                0.0,
                100.0,
                200.0,
                2.0,
                f2,
            ),
            2.0
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
