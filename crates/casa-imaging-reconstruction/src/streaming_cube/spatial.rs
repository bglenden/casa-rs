// SPDX-License-Identifier: LGPL-3.0-or-later

//! Batched spatial execution beneath the shared cube spectral operator.

use super::*;

/// Plain seven-tap geometry and value introduced by the shared science kernel.
#[doc(hidden)]
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, bytemuck::Pod, bytemuck::Zeroable)]
pub struct SpatialTap {
    /// First grid x cell.
    pub x: u32,
    /// First grid y cell.
    pub y: u32,
    /// Seven-element x convolution-table row.
    pub x_weights: u32,
    /// Seven-element y convolution-table row.
    pub y_weights: u32,
    /// Complex Float contribution.
    pub value: [f32; 2],
}

impl SpatialTap {
    pub(super) fn new(
        taps: crate::spectral_operator::SampleTaps,
        value: Complex32,
    ) -> Result<Self, SpectralOperatorError> {
        Ok(Self {
            x: taps
                .x
                .start
                .try_into()
                .map_err(|_| SpectralOperatorError::ResidencyOverflow)?,
            y: taps
                .y
                .start
                .try_into()
                .map_err(|_| SpectralOperatorError::ResidencyOverflow)?,
            x_weights: taps
                .x
                .weight_index
                .try_into()
                .map_err(|_| SpectralOperatorError::ResidencyOverflow)?,
            y_weights: taps
                .y
                .weight_index
                .try_into()
                .map_err(|_| SpectralOperatorError::ResidencyOverflow)?,
            value: [value.re, value.im],
        })
    }
}

/// Grid role; model planes retain their shared spectral-support ordinal.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpatialField {
    Dirty,
    Residual,
    Psf,
    Model,
}

/// One prepared model plane in a bounded prediction command batch.
#[doc(hidden)]
pub struct SpatialPredictionBatch {
    /// Ordinal in the band's model-support planes.
    pub plane: usize,
    /// Prepared source-refill requests for this plane.
    pub taps: Vec<SpatialTap>,
    /// Exact-sized prediction destination in matching request order.
    pub values: Vec<Complex32>,
}

/// One output plane in a bounded accumulation command batch.
#[doc(hidden)]
pub struct SpatialGridBatch {
    /// Output grid role.
    pub field: SpatialField,
    /// Ordinal in the band's core output planes.
    pub plane: usize,
    /// Prepared contributions for the current source refill.
    pub taps: Vec<SpatialTap>,
}

/// Runtime-owned resident grids and bounded synchronous dispatches. Only spatial
/// convolution moves across this seam; row interpolation, flags, polarization,
/// phase rotation, weights and image conversion remain in this module.
#[doc(hidden)]
pub trait CubeSpatialBackend {
    /// Initialize the active band's grids and convolution table once per wave.
    fn initialize(
        &mut self,
        shape: [usize; 2],
        weights: &[[f32; 7]],
        fields: [usize; 4],
        model: &[Complex32],
    ) -> Result<(), SpectralOperatorError>;
    /// Evaluate the active model planes together in one substantial batch.
    fn degrid(
        &mut self,
        batches: &mut [SpatialPredictionBatch],
    ) -> Result<(), SpectralOperatorError>;
    /// Accumulate the active output fields together in one substantial batch.
    fn grid(&mut self, batches: &[SpatialGridBatch]) -> Result<(), SpectralOperatorError>;
    /// Return one completed grid for the shared CPU FFT/image conversion.
    fn download(
        &mut self,
        field: SpatialField,
        plane: usize,
        values: &mut [Complex32],
    ) -> Result<(), SpectralOperatorError>;
}

impl BandPlan {
    /// Whether this initial band has no model prediction dependency.
    pub fn is_initial_zero(&self) -> bool {
        self.phase == BandPhase::InitialZero
    }

    /// Float seven-tap table shared by every grid in the wave.
    pub fn spatial_weight_bytes() -> usize {
        StandardConvolution::weight_row_count() * 7 * size_of::<f32>()
    }
    /// Largest simultaneous list of model or output dispatches for this band.
    pub fn spatial_command_capacity(&self) -> usize {
        let outputs = match self.phase {
            BandPhase::InitialZero => 2,
            BandPhase::Full => 3,
            BandPhase::Residual => 1,
        };
        self.support.model.len().max(outputs * self.core.len())
    }
    /// Additional shared-device grid bytes; host FFT grids are charged by memory().
    pub fn spatial_grid_bytes(&self) -> Result<usize, SpectralOperatorError> {
        let outputs = match self.phase {
            BandPhase::InitialZero => 2,
            BandPhase::Full => 3,
            BandPhase::Residual => 1,
        };
        let models = if self.phase == BandPhase::InitialZero {
            0
        } else {
            self.support.model.len()
        };
        self.geometry.grid_shape[0]
            .checked_mul(self.geometry.grid_shape[1])
            .and_then(|n| n.checked_mul(outputs * self.core.len() + models))
            .and_then(|n| n.checked_mul(size_of::<Complex32>()))
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    }

    /// Maximum packed requests across a band's command batch for an input refill.
    pub fn spatial_request_capacity(&self, rows: usize) -> Result<usize, SpectralOperatorError> {
        let native = if self.phase == BandPhase::InitialZero {
            0
        } else {
            self.support.native.len()
        };
        let outputs = match self.phase {
            BandPhase::InitialZero => 2,
            BandPhase::Full => 3,
            BandPhase::Residual => 1,
        };
        let predictions = native.checked_mul(2);
        let contributions = self.core.len().checked_mul(outputs);
        predictions
            .zip(contributions)
            .and_then(|(a, b)| rows.checked_mul(a.max(b).max(1)))
            .ok_or(SpectralOperatorError::ResidencyOverflow)
    }

    /// Conservative host preparation storage for one active accelerated band.
    pub fn spatial_host_bytes(&self, rows: usize) -> Result<usize, SpectralOperatorError> {
        let native = if self.phase == BandPhase::InitialZero {
            0
        } else {
            self.support.native.len()
        };
        let outputs = match self.phase {
            BandPhase::InitialZero => 2,
            BandPhase::Full => 3,
            BandPhase::Residual => 1,
        };
        // Two spectral terms per native channel. Growing request/destination
        // vectors may have up to twice their live length; results are exact-sized.
        let samples = native
            .checked_mul(
                4 * (size_of::<SpatialTap>() + size_of::<PredictionDestination>())
                    + 2 * size_of::<Complex32>(),
            )
            .and_then(|n| n.checked_add(native * size_of::<Complex64>()))
            .and_then(|n| n.checked_add(outputs * self.core.len() * size_of::<SpatialTap>()))
            .and_then(|n| n.checked_mul(rows))
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let headers = self
            .support
            .model
            .len()
            .checked_mul(
                size_of::<SpatialPredictionBatch>()
                    + size_of::<Vec<PredictionDestination>>()
                    + 8 * (size_of::<SpatialTap>() + size_of::<PredictionDestination>()),
            )
            .and_then(|n| n.checked_add(outputs * self.core.len() * size_of::<SpatialGridBatch>()))
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        let preparation = samples
            .checked_add(headers)
            .ok_or(SpectralOperatorError::ResidencyOverflow)?;
        // Initialization converts the convolution table before packing begins.
        // Its transient storage still applies to an empty source refill.
        Ok(preparation.max(Self::spatial_weight_bytes()))
    }
}

struct PredictionDestination {
    index: usize,
    factor: Complex64,
}

impl EpochBand<'_> {
    /// Load resident spatial buffers from the already prepared shared model FFT.
    pub fn initialize_spatial(
        &self,
        backend: &mut impl CubeSpatialBackend,
    ) -> Result<(), SpectralOperatorError> {
        let w = &self.workspace;
        let weights = w.convolution.float_weights();
        backend.initialize(
            w.geometry.grid_shape,
            &weights,
            [
                w.dirty.len_of(Axis(0)),
                w.residual.len_of(Axis(0)),
                w.psf.len_of(Axis(0)),
                w.forward.len_of(Axis(0)),
            ],
            w.forward.as_slice().expect("contiguous cube model"),
        )
    }

    /// Prepare predictions and contributions in source-refill sized batches.
    pub fn consume_source_window_spatial(
        &mut self,
        block: NativeBlockView<'_>,
        layout: &NativeLayout,
        selected: Range<usize>,
        output_hz: &[f64],
        polarization: &PolarizationOperator,
        backend: &mut impl CubeSpatialBackend,
    ) -> Result<(), SpectralOperatorError> {
        if output_hz.len() != self.generation.shape().coefficients()
            || selected.len() != layout.channels.len()
        {
            return Err(SpectralOperatorError::ProblemMismatch);
        }
        if self.native_range.is_empty() {
            return Ok(());
        }
        if self.native_range.start < selected.start || self.native_range.end > selected.end {
            return Err(SpectralOperatorError::IncompleteSpectralHalo);
        }
        if self.workspace.single_channel.is_some() {
            return Err(SpectralOperatorError::UnsupportedProblem);
        }
        let native =
            self.native_range.start - selected.start..self.native_range.end - selected.start;
        self.workspace
            .consume_spatial(block, layout, native, output_hz, polarization, backend)
    }

    /// Download only at the wave boundary, then use the ordinary completion path.
    pub fn download_spatial(
        &mut self,
        backend: &mut impl CubeSpatialBackend,
    ) -> Result<(), SpectralOperatorError> {
        for (field, grids) in [
            (SpatialField::Dirty, &mut self.workspace.dirty),
            (SpatialField::Residual, &mut self.workspace.residual),
            (SpatialField::Psf, &mut self.workspace.psf),
        ] {
            for (plane, mut grid) in grids.axis_iter_mut(Axis(0)).enumerate() {
                backend.download(
                    field,
                    plane,
                    grid.as_slice_mut().expect("contiguous cube grid"),
                )?;
            }
        }
        Ok(())
    }
}

impl BandWorkspace {
    pub(in crate::streaming_cube) fn consume_spatial(
        &mut self,
        block: NativeBlockView<'_>,
        layout: &NativeLayout,
        native: Range<usize>,
        output_hz: &[f64],
        polarization: &PolarizationOperator,
        backend: &mut impl CubeSpatialBackend,
    ) -> Result<(), SpectralOperatorError> {
        let rows = block.metadata.len();
        let native_rows = block.rows(layout, 0..block.channels, native.clone())?;
        let stride = native.len();
        let predicts = self.phase != BandPhase::InitialZero;
        let mut predictions = if predicts {
            vec![Complex64::default(); rows * stride]
        } else {
            Vec::new()
        };
        if predicts {
            let mut batches: Vec<_> = (0..self.model_channels.len())
                .map(|plane| SpatialPredictionBatch {
                    plane,
                    taps: Vec::new(),
                    values: Vec::new(),
                })
                .collect();
            let mut destinations: Vec<Vec<PredictionDestination>> =
                (0..self.model_channels.len()).map(|_| Vec::new()).collect();
            let mut previous: Option<CachedRowSupport<'_>> = None;
            for row_index in 0..rows {
                let mut row = native_rows.row(row_index);
                let frequencies = row.frequencies_hz;
                let reuse = previous.as_ref().is_some_and(|(hz, pair, _, _)| {
                    *hz == frequencies && *pair == row.original_pair_hz
                });
                let support = if reuse {
                    previous.as_ref().unwrap().2.clone()
                } else {
                    BandSupport::native_window(
                        output_hz,
                        self.core.clone(),
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
                    let stencil = RowStencil::compile(
                        &row,
                        output_hz,
                        self.core.clone(),
                        &self.model_channels,
                        self.phase,
                    )?;
                    previous = Some((
                        frequencies,
                        row.original_pair_hz,
                        support.clone(),
                        Some(stencil),
                    ));
                }
                let stencil = previous.as_ref().unwrap().3.as_ref().unwrap();
                for channel in 0..row.channels.len() {
                    let terms = &stencil.prediction_terms[channel];
                    for term in terms {
                        if !self.forward_nonzero[term.plane] {
                            continue;
                        }
                        if let Some(taps) = self.convolution.taps([
                            row.uvw_m[0] * term.wavelength_scale,
                            row.uvw_m[1] * term.wavelength_scale,
                        ]) {
                            batches[term.plane]
                                .taps
                                .push(SpatialTap::new(taps, Complex32::default())?);
                            destinations[term.plane].push(PredictionDestination {
                                index: row_index * stride + support.start + channel,
                                factor: phase(row.phase_shift_m, term.frequency_hz).conj()
                                    * term.factor,
                            });
                        }
                    }
                }
            }
            for batch in &mut batches {
                batch.values = vec![Complex32::default(); batch.taps.len()];
            }
            backend.degrid(&mut batches)?;
            for (batch, destinations) in batches.into_iter().zip(destinations) {
                for (value, destination) in batch.values.into_iter().zip(destinations) {
                    predictions[destination.index] += widen(value) * destination.factor;
                }
            }
        }
        let fields = if self.phase == BandPhase::Residual {
            &[SpatialField::Residual][..]
        } else if self.phase == BandPhase::InitialZero {
            &[SpatialField::Dirty, SpatialField::Psf][..]
        } else {
            &[
                SpatialField::Dirty,
                SpatialField::Residual,
                SpatialField::Psf,
            ][..]
        };
        let plane_count = self.core.len();
        let mut batches: Vec<_> = fields
            .iter()
            .flat_map(|&field| {
                (0..plane_count).map(move |plane| SpatialGridBatch {
                    field,
                    plane,
                    taps: Vec::with_capacity(rows),
                })
            })
            .collect();
        let mut previous: Option<CachedRowSupport<'_>> = None;
        for row_index in 0..rows {
            let mut row = native_rows.row(row_index);
            let frequencies = row.frequencies_hz;
            let reuse = previous.as_ref().is_some_and(|(hz, pair, _, _)| {
                *hz == frequencies && *pair == row.original_pair_hz
            });
            let support = if reuse {
                previous.as_ref().unwrap().2.clone()
            } else {
                BandSupport::native_window(
                    output_hz,
                    self.core.clone(),
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
                let stencil = RowStencil::compile(
                    &row,
                    output_hz,
                    self.core.clone(),
                    &self.model_channels,
                    self.phase,
                )?;
                previous = Some((
                    frequencies,
                    row.original_pair_hz,
                    support.clone(),
                    Some(stencil),
                ));
            }
            let stencil = previous.as_ref().unwrap().3.as_ref().unwrap();
            let count = row.channels.len();
            let mut accumulator = self.begin_row_cached(&row, polarization, stencil)?;
            accumulator.push_with(
                0..count,
                |_, _, _, polarization, channel| {
                    polarization
                        .predict(&[if predicts {
                            predictions[row_index * stride + support.start + channel]
                        } else {
                            Complex64::default()
                        }])
                        .map_err(|_| SpectralOperatorError::GeneratedNonfinite)
                },
                |band, channel, frequency, row, observed, predicted, weight| {
                    band.grid_sample_with(
                        channel,
                        frequency,
                        row,
                        observed,
                        predicted,
                        weight,
                        |field, _, _, plane, taps, value| {
                            let lane = fields
                                .iter()
                                .position(|f| *f == field)
                                .expect("active output field");
                            batches[lane * plane_count + plane]
                                .taps
                                .push(SpatialTap::new(taps, value)?);
                            Ok(())
                        },
                    )
                },
            )?;
            accumulator.finish()?;
        }
        backend.grid(&batches)?;
        Ok(())
    }
}
