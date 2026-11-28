// SPDX-License-Identifier: LGPL-3.0-or-later
//! Frequency interpolation of a sampled SDGrid image spectrum.

use casa_coordinates::SpectralCoordinate;
use casa_core::{ArrayValue, Complex32, RecordValue, ScalarValue, Value};
use casa_imaging_operator::{
    Basis, NativeRow, PredictionScratch, RowContext, SpectralAxis, SpectralResampler,
};
use casa_imaging_runtime::{Demand, HostResources, Reservation, ResourcePolicy, admit};
use casa_ms::MeasurementSet;
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
        match record.get("casars_live_image_frequency") {
            None => return Ok(None),
            Some(Value::Array(ArrayValue::Float64(frequencies)))
                if frequencies.ndim() == 1
                    && frequencies.len() == image_channels
                    && image_channels > 1 => {}
            _ => return Err(model_error("invalid saved SDGrid frequency axis")),
        }
        if !matches!(
            record.get("freqinterpmethod"),
            Some(Value::Scalar(ScalarValue::Int32(1..=3)))
        ) {
            return Err(model_error(
                "invalid saved SDGrid frequency interpolation method",
            ));
        }
        // Two shared axes remain live. Their construction has one temporary
        // array of centres, reused in sequence for the two axes.
        let axis_bytes = (image_channels as u64).saturating_mul(size_of::<f64>() as u64);
        let axis_headers = 4 * size_of::<usize>() as u64;
        let mut memory = admit(
            host,
            &ResourcePolicy::Balanced,
            &Demand {
                phase: "open legacy single-dish spectral axes",
                memory: axis_bytes.saturating_mul(3).saturating_add(axis_headers),
            },
        )
        .map_err(model_error)?;
        let mut centres = Vec::with_capacity(image_channels);
        for plane in 0..image_channels {
            centres.push(
                spectral
                    .world_frequency_hz(plane as f64)
                    .map_err(model_error)?,
            );
        }
        let axis = SpectralAxis::tabular(&centres, centres[1] - centres[0]).map_err(model_error)?;
        drop(centres);
        let (_, resampler) = spectral_layout(record, &axis, Basis::Constant, true)?;
        memory.retain(axis_bytes.saturating_mul(2).saturating_add(axis_headers));

        let windows = ms.spectral_window().map_err(model_error)?;
        let mut channels = 1;
        for id in 0..windows.row_count() {
            let count =
                usize::try_from(windows.num_chan(id).map_err(model_error)?).map_err(model_error)?;
            channels = channels.max(count);
        }
        let polarization = ms.polarization().map_err(model_error)?;
        let mut correlations = 1;
        for id in 0..polarization.row_count() {
            let count = usize::try_from(polarization.num_corr(id).map_err(model_error)?)
                .map_err(model_error)?;
            correlations = correlations.max(count);
        }
        let cells = channels.saturating_mul(correlations);
        let scratch =
            SpectralResampler::prediction_scratch_bytes([&resampler], correlations, channels, 1);
        let series = channels
            .saturating_mul(size_of::<f64>())
            .saturating_add(cells.saturating_mul(size_of::<bool>() + size_of::<Complex32>()));
        memory.join(
            admit(
                host,
                &ResourcePolicy::Balanced,
                &Demand {
                    phase: "sample legacy single-dish spectrum",
                    memory: scratch.saturating_add(series as u64),
                },
            )
            .map_err(model_error)?,
        );
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
            selection_spacing_hz: None,
            uvw_m: row.uvw,
            phase_shift_m: 0.,
            pointing_offset_rad: [0.; 2],
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imaging_memory_test::{isolated_test, live_since, peak_since, reset_peak};
    use casa_core::measures::frequency::FrequencyRef;
    use casa_imaging_runtime::ReservationWindow;
    use casa_ms::MeasurementSetBuilder;

    #[test]
    fn sampled_spline_stays_within_its_charge() {
        if !isolated_test(module_path!(), "sampled_spline_stays_within_its_charge") {
            return;
        }
        const PLANES: usize = 65;
        const CHANNELS: usize = 129;
        const CORRELATIONS: usize = 4;
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        let mut spw = ms.spectral_window_mut().unwrap();
        let table = spw.table_mut();
        let mut window = table.schema().unwrap().default_row();
        window.upsert(
            "NUM_CHAN",
            Value::Scalar(ScalarValue::Int32(CHANNELS as i32)),
        );
        table.add_row(window).unwrap();
        let mut pol = ms.polarization_mut().unwrap();
        let table = pol.table_mut();
        let mut polarization = table.schema().unwrap().default_row();
        polarization.upsert(
            "NUM_CORR",
            Value::Scalar(ScalarValue::Int32(CORRELATIONS as i32)),
        );
        table.add_row(polarization).unwrap();
        let coordinate = SpectralCoordinate::new(FrequencyRef::TOPO, 1e9, 1e6, 0., 1e9);
        let mut record = RecordValue::default();
        record.upsert(
            "casars_live_image_frequency",
            Value::Array(ArrayValue::from_f64_vec(
                (0..PLANES).map(|plane| 1e9 + plane as f64 * 1e6).collect(),
            )),
        );
        record.upsert("freqinterpmethod", Value::Scalar(ScalarValue::Int32(3)));
        let host = HostResources::detect().unwrap();
        let reservation_window = ReservationWindow::open();
        let baseline = reset_peak();
        let mut model = ImageSpectrum::load(&record, &coordinate, PLANES, &ms, &host)
            .unwrap()
            .unwrap();
        model
            .frequencies
            .extend((0..CHANNELS).map(|channel| 1e9 + channel as f64 * 0.5e6));
        model.flags.resize(CHANNELS * CORRELATIONS, false);
        model
            .prediction
            .resize(CHANNELS * CORRELATIONS, Complex32::default());
        let native = NativeRow {
            selection_spacing_hz: None,
            uvw_m: [0.; 3],
            phase_shift_m: 0.,
            pointing_offset_rad: [0.; 2],
            frequencies_hz: &model.frequencies,
            values: &[],
            weights: &[],
            flags: &model.flags,
            row_flag: false,
            context: RowContext {
                native_frequency_ratio: 1.,
                original_uvw_m: None,
                time_s: 0.,
                antennas: [0; 2],
                antenna_types: [0; 2],
                parallactic_angle_rad: [0.; 2],
                field: 0,
                spectral_window: 0,
            },
        };
        assert!(
            model
                .resampler
                .predict_sampled_spectrum(
                    &native,
                    CORRELATIONS,
                    &mut model.scratch,
                    &mut model.prediction,
                    |plane, values| values.fill(Complex32::new(plane as f32, 1.)),
                )
                .unwrap()
        );
        let measured = peak_since(baseline);
        let admitted = reservation_window.close();
        assert!(
            measured <= admitted,
            "{measured} bytes used over {admitted}"
        );
        assert!(live_since(baseline) <= model._memory.memory());
        assert_eq!(
            model.prediction[63 * CORRELATIONS],
            Complex32::new(31.5, 1.)
        );
        let flagged = NativeRow {
            selection_spacing_hz: None,
            row_flag: true,
            ..native
        };
        assert!(
            model
                .resampler
                .predict_sampled_spectrum(
                    &flagged,
                    CORRELATIONS,
                    &mut model.scratch,
                    &mut model.prediction,
                    |_, _| panic!("a flagged row must not sample the sky"),
                )
                .unwrap()
        );
        assert!(
            model
                .prediction
                .iter()
                .all(|&cell| cell == Complex32::default())
        );
    }
}
