// SPDX-License-Identifier: LGPL-3.0-or-later

//! Shared-science descriptors for a connected device residual operator.

use super::*;

/// One unique row/coarse-model-plane gather, with host-evaluated conjugate phase.
#[doc(hidden)]
#[repr(C)]
#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub struct ResidualPrediction {
    pub tap: SpatialTap,
    pub plane: u32,
    pub padding: u32,
}

/// Original ordered spectral terms for a native endpoint. Missing entries use MAX.
#[doc(hidden)]
#[repr(C)]
#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub struct NativePrediction {
    pub indices: [u32; 2],
    pub factors: [f32; 2],
}

/// One fine-frequency residual contribution; endpoint ordinals address the refill.
#[doc(hidden)]
#[repr(C)]
#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub struct ResidualSample {
    pub tap: SpatialTap,
    pub left: u32,
    pub right: u32,
    /// High two bits encode which endpoint flags CASA consults; low bits are ordinal.
    pub nearest_flags: u32,
    pub plane: u32,
    pub factors: [f32; 2],
}

/// Polarization projection shared by every sample in this selected source block.
#[doc(hidden)]
#[repr(C)]
#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub struct DeviceCorrelations {
    pub coefficients: [[f32; 2]; 4],
    pub correlations: u32,
    pub direct: i32,
    pub padding: [u32; 2],
}

/// Borrowed, bounded descriptor destinations, including mapped device storage.
/// Counts identify initialized prediction, native-endpoint and fine-sample prefixes.
#[doc(hidden)]
pub struct ResidualRefill<'a> {
    pub predictions: &'a mut [ResidualPrediction],
    pub native: &'a mut [NativePrediction],
    pub samples: &'a mut [ResidualSample],
    pub counts: [usize; 3],
    pub requested_predictions: u64,
}

impl DeviceCorrelations {
    pub fn new(polarization: &PolarizationOperator) -> Result<Self, SpectralOperatorError> {
        let reduction = StokesIReducer::new(polarization)?;
        let mut coefficients = [[0.0; 2]; 4];
        for (target, value) in coefficients.iter_mut().zip(reduction.coefficients) {
            *target = [value.re as f32, value.im as f32];
        }
        Ok(Self {
            coefficients,
            correlations: reduction.correlations as u32,
            direct: reduction.direct.map_or(-1, |v| v as i32),
            padding: [0; 2],
        })
    }
}

impl BandPlan {
    pub fn is_residual(&self) -> bool {
        self.phase == BandPhase::Residual
    }

    /// Join contiguous bands from one compiled problem and immutable model epoch.
    /// Halo identities become global-plane identities before any model FFT is loaded.
    pub fn residual_wave(bands: &[Self]) -> Result<Self, SpectralOperatorError> {
        let mut wave = bands
            .first()
            .ok_or(SpectralOperatorError::InvalidSlab)?
            .clone();
        if !wave.is_residual() || wave.single_channel.is_some() {
            return Err(SpectralOperatorError::UnsupportedProblem);
        }
        for band in &bands[1..] {
            if !band.is_residual()
                || band.single_channel.is_some()
                || band.core.start != wave.core.end
                || band.total_channels != wave.total_channels
                || band.geometry != wave.geometry
            {
                return Err(SpectralOperatorError::ProblemMismatch);
            }
            wave.core.end = band.core.end;
            if !band.support.native.is_empty() {
                wave.support.native = if wave.support.native.is_empty() {
                    band.support.native.clone()
                } else {
                    wave.support.native.start.min(band.support.native.start)
                        ..wave.support.native.end.max(band.support.native.end)
                };
            }
            wave.support.model.extend_from_slice(&band.support.model);
            wave.fine_per_output = wave.fine_per_output.max(band.fine_per_output);
        }
        wave.support.model.sort_unstable();
        wave.support.model.dedup();
        Ok(wave)
    }

    /// Exact shape bounds learned during source dependency discovery.
    pub fn residual_capacities(&self, rows: usize) -> Result<[usize; 3], SpectralOperatorError> {
        let multiply = |n: usize| {
            rows.checked_mul(n)
                .ok_or(SpectralOperatorError::ResidencyOverflow)
        };
        Ok([
            multiply(self.support.model.len())?,
            multiply(self.support.native.len())?,
            multiply(
                self.core
                    .len()
                    .checked_mul(self.fine_per_output)
                    .ok_or(SpectralOperatorError::ResidencyOverflow)?,
            )?,
        ])
    }
}

impl EpochBand<'_> {
    /// Compile CPU geometry and mappings, leaving interpolation/reduction on device.
    /// All discrete decisions and phases use the same primitives as CPU imaging.
    pub fn prepare_residual_refill(
        &self,
        block: NativeBlockView<'_>,
        layout: &NativeLayout,
        selected: Range<usize>,
        output_hz: &[f64],
        refill: &mut ResidualRefill<'_>,
    ) -> Result<(), SpectralOperatorError> {
        let w = &self.workspace;
        if w.phase != BandPhase::Residual
            || w.single_channel.is_some()
            || output_hz.len() != self.generation.shape().coefficients()
            || selected.len() != layout.channels.len()
        {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        refill.counts = [0; 3];
        refill.requested_predictions = 0;
        if self.native_range.is_empty() {
            return Ok(());
        }
        if self.native_range.start < selected.start || self.native_range.end > selected.end {
            return Err(SpectralOperatorError::IncompleteSpectralHalo);
        }
        let cells = block
            .metadata
            .len()
            .checked_mul(block.channels)
            .filter(|&n| n < (1 << 30))
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        refill
            .native
            .get_mut(..cells)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?
            .fill(NativePrediction {
                indices: [u32::MAX; 2],
                factors: [0.0; 2],
            });
        refill.counts[1] = cells;
        let local =
            self.native_range.start - selected.start..self.native_range.end - selected.start;
        let rows = block.rows(layout, 0..block.channels, local.clone())?;
        let mut previous: Option<(&[f64], [f64; 2], Range<usize>, Option<RowStencil>)> = None;
        let mut unique = vec![u32::MAX; w.model_channels.len()];
        for row_index in 0..block.metadata.len() {
            let mut row = rows.row(row_index);
            let frequencies = row.frequencies_hz;
            let reuse = previous.as_ref().is_some_and(|(hz, pair, _, _)| {
                *hz == frequencies && *pair == row.original_pair_hz
            });
            let support = if reuse {
                previous.as_ref().unwrap().2.clone()
            } else {
                BandSupport::native_window(
                    output_hz,
                    w.core.clone(),
                    frequencies,
                    row.original_pair_hz,
                )?
            };
            if support.is_empty() {
                previous = Some((frequencies, row.original_pair_hz, support, None));
                continue;
            }
            row.restrict(support.clone())?;
            if !reuse {
                previous = Some((
                    frequencies,
                    row.original_pair_hz,
                    support.clone(),
                    Some(RowStencil::compile(
                        &row,
                        output_hz,
                        w.core.clone(),
                        &w.model_channels,
                        w.phase,
                    )?),
                ));
            }
            let stencil = previous.as_ref().unwrap().3.as_ref().unwrap();
            unique.fill(u32::MAX);
            let first = row_index * block.channels + local.start + support.start;
            for channel in 0..row.channels.len() {
                let terms = &stencil.prediction_terms[channel];
                if terms.len() > 2 {
                    return Err(SpectralOperatorError::UnsupportedProblem);
                }
                for (index, term) in terms.iter().enumerate() {
                    if !w.forward_nonzero[term.plane] {
                        continue;
                    }
                    refill.requested_predictions += 1;
                    if unique[term.plane] == u32::MAX {
                        if let Some(taps) = w.convolution.taps([
                            row.uvw_m[0] * term.wavelength_scale,
                            row.uvw_m[1] * term.wavelength_scale,
                        ]) {
                            let rotation = phase(row.phase_shift_m, term.frequency_hz).conj();
                            unique[term.plane] = refill.counts[0]
                                .try_into()
                                .map_err(|_| SpectralOperatorError::ResidencyOverflow)?;
                            *refill
                                .predictions
                                .get_mut(refill.counts[0])
                                .ok_or_else(|| SpectralOperatorError::ResidencyOverflow)? =
                                ResidualPrediction {
                                    tap: SpatialTap::new(
                                        taps,
                                        Complex32::new(rotation.re as f32, rotation.im as f32),
                                    )?,
                                    plane: term.plane as u32,
                                    padding: 0,
                                };
                            refill.counts[0] += 1;
                        }
                    }
                    refill.native[first + channel].indices[index] = unique[term.plane];
                    refill.native[first + channel].factors[index] = term.factor as f32;
                }
                for &fine in stencil.samples(channel) {
                    let right = first + channel;
                    let left = right - 1;
                    let nearest = if fine.nearest_is_right() { right } else { left };
                    let flag_mask = u32::from(fine.linear_flag(true, false))
                        | (u32::from(fine.linear_flag(false, true)) << 1);
                    let rotation = phase(row.phase_shift_m, fine.frequency_hz());
                    let scale = fine.frequency_hz() / SPEED_OF_LIGHT_M_PER_S;
                    let tap = match w
                        .convolution
                        .taps([row.uvw_m[0] * scale, row.uvw_m[1] * scale])
                    {
                        Some(taps) => SpatialTap::new(
                            taps,
                            Complex32::new(rotation.re as f32, rotation.im as f32),
                        )?,
                        None => SpatialTap {
                            x: u32::MAX,
                            ..SpatialTap::default()
                        },
                    };
                    *refill
                        .samples
                        .get_mut(refill.counts[2])
                        .ok_or_else(|| SpectralOperatorError::ResidencyOverflow)? =
                        ResidualSample {
                            tap,
                            left: left as u32,
                            right: right as u32,
                            nearest_flags: nearest as u32 | (flag_mask << 30),
                            plane: (fine.output_channel() - w.core.start) as u32,
                            factors: fine.factors().map(|v| v as f32),
                        };
                    refill.counts[2] += 1;
                }
            }
        }
        Ok(())
    }
}
