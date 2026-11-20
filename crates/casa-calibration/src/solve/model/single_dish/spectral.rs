// SPDX-License-Identifier: LGPL-3.0-or-later
//! Frequency interpolation of a sampled SDGrid image spectrum.

use casa_coordinates::SpectralCoordinate;
use casa_imaging_operator::{
    Basis, NativeRow, PredictionScratch, RowContext, SpectralAxis, SpectralResampler,
};
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_ms::MeasurementSet;
use casa_types::{Complex32, RecordValue};
use ndarray::ArrayD;

use super::super::fourier::{FourierRow, scratch_channel_masked, spectral_layout};
use super::super::{GainSolveError, model_error};

pub(super) struct ImageSpectrum {
    resampler: SpectralResampler,
    scratch: PredictionScratch,
    frequencies: Vec<f64>,
    flags: Vec<bool>,
    prediction: Vec<Complex32>,
    _memory: Reservation,
}

impl ImageSpectrum {
    pub(super) fn load(
        record: &RecordValue,
        spectral: &SpectralCoordinate,
        image_channels: usize,
        ms: &MeasurementSet,
        host: &HostResources,
    ) -> Result<Option<Self>, GainSolveError> {
        let Some(input) = record.get("casars_live_image_frequency") else {
            return Ok(None);
        };
        let Some(method) = record.get("freqinterpmethod") else {
            return Err(model_error(
                "a sampled SDGrid spectrum needs a frequency interpolation method",
            ));
        };
        let centres = (0..image_channels)
            .map(|plane| {
                spectral
                    .world_frequency_hz(plane as f64)
                    .map_err(model_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let [first, second, ..] = centres.as_slice() else {
            return Ok(None);
        };
        let axis = SpectralAxis::tabular(&centres, second - first).map_err(model_error)?;
        let mut layout = RecordValue::default();
        layout.upsert("casars_live_image_frequency", input.clone());
        layout.upsert("freqinterpmethod", method.clone());
        let (_, resampler) = spectral_layout(&layout, &axis, Basis::Constant)?;

        let windows = ms.spectral_window().map_err(model_error)?;
        let mut channels = 1;
        for id in 0..windows.row_count() {
            channels = channels.max(windows.num_chan(id).map_err(model_error)? as usize);
        }
        let polarization = ms.polarization().map_err(model_error)?;
        let mut correlations = 1;
        for id in 0..polarization.row_count() {
            correlations = correlations.max(polarization.corr_type(id).map_err(model_error)?.len());
        }
        let cells = channels.saturating_mul(correlations);
        let scratch =
            SpectralResampler::prediction_scratch_bytes([&resampler], correlations, channels, 1);
        let series = channels
            .saturating_mul(size_of::<f64>())
            .saturating_add(cells.saturating_mul(size_of::<bool>() + size_of::<Complex32>()));
        let memory = admit(
            host,
            &ResourcePolicy::Balanced,
            &Demand {
                phase: "sample legacy single-dish spectrum",
                memory: scratch
                    .saturating_add(series as u64)
                    .saturating_add(4 * image_channels as u64 * size_of::<f64>() as u64),
            },
        )
        .map_err(model_error)?;
        let scratch =
            SpectralResampler::prediction_scratch([&resampler], correlations, channels, 1);
        Ok(Some(Self {
            resampler,
            scratch,
            frequencies: Vec::with_capacity(channels),
            flags: Vec::with_capacity(cells),
            prediction: Vec::with_capacity(cells),
            _memory: memory,
        }))
    }

    pub(super) fn predict(
        &mut self,
        row: &FourierRow<'_>,
        (ratio, mask): (f64, Option<&ArrayD<i32>>),
        output: &mut ArrayD<Complex32>,
        sample: impl FnMut(u32, &mut [Complex32]),
    ) -> Result<bool, GainSolveError> {
        self.frequencies.clear();
        self.frequencies
            .extend(row.frequencies.iter().map(|frequency| frequency * ratio));
        self.flags.clear();
        let correlations = row.correlations.len();
        for channel in 0..self.frequencies.len() {
            let masked = scratch_channel_masked(mask, row.scratch_channels, channel, false);
            self.flags.extend((0..correlations).map(|corr| {
                masked || row.raw_flags.get([corr, channel].as_slice()) == Some(&true)
            }));
        }
        self.resampler.set_legacy_prediction_selection(
            row.scratch_channels
                .map(|[_, start, count, step]| [start as usize, count as usize, step as usize]),
        );
        let native = NativeRow {
            uvw_m: row.uvw,
            frequencies_hz: &self.frequencies,
            values: &[],
            weights: &[],
            flags: &self.flags,
            row_flag: row.row.flag_row,
            context: RowContext {
                native_frequency_ratio: ratio.recip(),
                original_uvw_m: None,
                time_s: row.row.time_seconds,
                antennas: [row.row.antenna1 as u32; 2],
                antenna_types: [0; 2],
                parallactic_angle_rad: [0.; 2],
                field: row.row.field_id as u32,
                spectral_window: row.row.data_spw_id as u32,
            },
        };
        self.prediction
            .resize(self.flags.len(), Complex32::default());
        if !self
            .resampler
            .predict_sampled_spectrum(
                &native,
                correlations,
                &mut self.scratch,
                &mut self.prediction,
                sample,
            )
            .map_err(model_error)?
        {
            return Ok(false);
        }
        let output = output
            .as_slice_memory_order_mut()
            .expect("contiguous model visibilities");
        for (output, &sample) in output.iter_mut().zip(&self.prediction) {
            *output += sample;
        }
        Ok(true)
    }
}
