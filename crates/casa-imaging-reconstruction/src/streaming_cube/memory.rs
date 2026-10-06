// SPDX-License-Identifier: LGPL-3.0-or-later

//! Checked byte projection of the actual band allocations. Runtime sums these
//! peaks across concurrently live jobs and adds its input, queue and stack owners.

use super::*;
use crate::spectral_operator::fft_planning_words_for_shape;
use std::mem::size_of;

/// Conservative per-job phase peaks, including its generated fields and FFT.
/// Shape-derived library bounds are shared with the existing spectral owner.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct BandMemory {
    /// FFT construction, grid allocation and bounded model-plane loading.
    pub preparation_bytes: usize,
    /// Grids, compensation, mapping, FFT and reused normal during row work.
    pub accumulation_bytes: usize,
    /// Simultaneously live grids, images and normal-field construction.
    pub completion_bytes: usize,
    /// Returned normal fields and recyclable FFT after completion.
    pub retained_bytes: usize,
}

impl BandMemory {
    /// Safe even when jobs in one wave are at different phases.
    pub fn peak_bytes(self) -> usize {
        self.preparation_bytes
            .max(self.accumulation_bytes)
            .max(self.completion_bytes)
            .max(self.retained_bytes)
    }
}

fn add(values: &[usize]) -> Result<usize, SpectralOperatorError> {
    values.iter().try_fold(0_usize, |sum, &value| {
        sum.checked_add(value)
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    })
}

fn mul(left: usize, right: usize) -> Result<usize, SpectralOperatorError> {
    left.checked_mul(right)
        .filter(|&bytes| bytes <= isize::MAX as usize)
        .ok_or(SpectralOperatorError::ResidencyOverflow)
}

impl BandPlan {
    /// No allocation or payload scan. The runtime must also count input/source,
    /// shared model storage, job collection headers, output sinks and stacks.
    pub fn memory(&self) -> Result<BandMemory, SpectralOperatorError> {
        if self.core.is_empty() || self.core.end > self.total_channels {
            return Err(SpectralOperatorError::InvalidSlab);
        }
        let depth = self.core.len();
        let grid_cells = mul(self.geometry.grid_shape[0], self.geometry.grid_shape[1])?;
        let image_cells = mul(self.geometry.image_shape[0], self.geometry.image_shape[1])?;
        let grid = mul(mul(depth, grid_cells)?, size_of::<Complex64>())?;
        let image = mul(mul(depth, image_cells)?, size_of::<Complex64>())?;
        let fft = mul(
            fft_resident_complex_values_for_shape(self.geometry.grid_shape)?,
            size_of::<Complex64>(),
        )?;
        let planning = mul(
            fft_planning_words_for_shape(self.geometry.grid_shape)?,
            size_of::<usize>(),
        )?;
        let convolution = StandardConvolution::dynamic_bytes(self.geometry.grid_shape)?;
        let support = mul(self.support.model.capacity(), size_of::<usize>())?;
        let normal = self.phase != BandPhase::Residual;
        let predicts = self.phase != BandPhase::InitialZero;
        let grid_count = match self.phase {
            BandPhase::InitialZero => 4,
            BandPhase::Full => 6,
            BandPhase::Residual => 2,
        };
        let image_count = grid_count / 2;
        let forward = if predicts {
            mul(
                mul(self.support.model.len(), grid_cells)?,
                size_of::<Complex64>(),
            )?
        } else {
            0
        };
        let model_window = if predicts && !self.support.model.is_empty() {
            mul(image_cells, size_of::<casa_imaging_model::ModelSample>())?
        } else {
            0
        };
        let stats = if normal {
            mul(depth, 2 * size_of::<f64>() + size_of::<u64>())?
        } else {
            0
        };
        // Headers for by-value plan/job transfers are charged in addition to the
        // runtime's collection slots. No assumption about optimizer elision.
        let headers = add(&[size_of::<BandPlan>(), size_of::<EpochBand<'_>>()])?;
        let workspace = add(&[
            headers,
            support,
            fft,
            convolution,
            mul(grid_count, grid)?,
            forward,
            if predicts {
                self.support.model.len()
            } else {
                0
            },
            stats,
        ])?;
        let accumulation_bytes = workspace;
        let preparation_bytes = workspace
            .max(add(&[accumulation_bytes, model_window])?)
            .max(add(&[headers, support, fft, planning])?);
        // Compensation/forward/support arrays are explicitly dropped first.
        // Each image allocation overlaps all not-yet-consumed grids and the
        // already completed images; a grid is dropped after its conversion.
        let completion_base = add(&[
            headers,
            fft,
            convolution,
            if normal { mul(depth, 16)? } else { 0 },
        ])?;
        let mut completion_bytes = 0;
        for formed in 1..=image_count {
            completion_bytes = completion_bytes.max(add(&[
                completion_base,
                mul(image_count - formed + 1, grid)?,
                mul(formed, image)?,
            ])?);
        }
        let normal_metadata = if normal {
            add(&[
                mul(mul(depth, image_cells)?, size_of::<f64>())?,
                mul(
                    depth,
                    2 * size_of::<f64>() + size_of::<crate::SpectralChannelValidity>(),
                )?,
                mul(self.total_channels, size_of::<Option<usize>>())?,
            ])?
        } else {
            0
        };
        let result_headers = size_of::<(BandResult, PreparedFft)>();
        let retained_bytes = add(&[
            result_headers,
            fft,
            mul(image_count, image)?,
            normal_metadata,
        ])?;
        // Mapped counts and the unboxed sum-weight vector can still coexist with
        // the final metadata while from_cube_band forms the primitive owner.
        completion_bytes = completion_bytes.max(add(&[
            retained_bytes,
            headers,
            if normal { mul(depth, 16)? } else { 0 },
        ])?);
        let result = BandMemory {
            preparation_bytes,
            accumulation_bytes,
            completion_bytes,
            retained_bytes,
        };
        if result.peak_bytes() > isize::MAX as usize {
            return Err(SpectralOperatorError::ResidencyOverflow);
        }
        Ok(result)
    }
}
