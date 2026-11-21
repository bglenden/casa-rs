// SPDX-License-Identifier: LGPL-3.0-or-later
//! Group polarized samples and solve known-source leakage.

mod fit;
mod output;

use super::*;
use crate::execute::coherency_order;
use crate::session::prepare_calibration_solve;
use std::path::PathBuf;

use fit::LeakageFit;

#[derive(Clone, Copy)]
pub(crate) enum LeakageSolveKind {
    Dlls,
    Dflls,
}

impl LeakageSolveKind {
    pub(crate) fn subtype(self) -> &'static str {
        match self {
            Self::Dlls => "DLLS Jones",
            Self::Dflls => "DfLLS Jones",
        }
    }

    fn channelized(self) -> bool {
        matches!(self, Self::Dflls)
    }
}

pub(crate) struct LeakageSolveRequest {
    pub(crate) output: PathBuf,
    pub(crate) selection: MsSelection,
    pub(crate) kind: LeakageSolveKind,
    pub(crate) model_source: GainSolveModelSource,
    pub(crate) stokes: [f32; 4],
    pub(crate) priors: Vec<ApplyCalibrationTableSpec>,
    pub(crate) interval: GainSolveInterval,
    pub(crate) frequency_interval: Option<crate::BandpassFrequencyInterval>,
    pub(crate) combine: GainSolveCombine,
    pub(crate) preaverage_seconds: f64,
    pub(crate) refant: RefAntSelector,
    pub(crate) append: bool,
}

pub(crate) struct LeakageSolution {
    pub(crate) spectral_window: i32,
    pub(crate) field: i32,
    pub(crate) observation: i32,
    pub(crate) scan: i32,
    pub(crate) time: f64,
    pub(crate) reference: i32,
    pub(crate) channels: Vec<usize>,
    /// Channel-major solutions with one pair of leakage terms per antenna.
    pub(crate) values: Vec<Vec<Option<[Complex32; 2]>>>,
}

pub(crate) fn solve_leakage(
    ms: &MeasurementSet,
    request: &LeakageSolveRequest,
) -> Result<usize, String> {
    let error = |error: GainSolveError| error.to_string();
    let context = prepare_calibration_solve(
        ms,
        &request.selection,
        &request.refant,
        &request.priors,
        true,
        true,
        PriorCalibrationMode::Leakage,
    )
    .map_err(error)?;
    let ordered = bucketed_rows(
        &context.rows,
        request.interval,
        request.combine,
        false,
        false,
    );
    let mut models = super::super::model::ModelInputs::new(ms, &context.rows, request.model_source)
        .map_err(error)?;
    models
        .bind_buffers(ms, &ordered, request.combine)
        .map_err(error)?;
    models.phase_times =
        weights::chunk_phase_times(&ordered, request.combine, models.dynamic_fields());
    let spectral_weights = SpectralWeights::new(ms, &ordered, request.combine).map_err(error)?;
    let channel_bin_widths = crate::bandpass::solution_frequency_bin_widths(
        ms,
        &context.rows,
        request
            .frequency_interval
            .filter(|_| request.kind.channelized()),
    )
    .map_err(|error| error.to_string())?;
    let preapplied = context
        .preapplied_rows
        .as_ref()
        .expect("polcal always applies P Jones");
    let setup = LeakageInputs {
        ms,
        request,
        models: &models,
        preapplied,
        spectral_weights,
        channel_bin_widths: &channel_bin_widths,
    };
    let mut solutions = Vec::new();
    for rows in ordered.chunk_by(|a, b| a.1 == b.1 && a.2 == b.2) {
        if let Some(solution) = setup
            .solve_interval(rows, context.available_antennas.len(), context.refant_id)
            .map_err(error)?
        {
            solutions.push(solution);
        }
    }
    output::write(ms, request, &context.rows, &solutions, channel_bin_widths)
}

struct LeakageInputs<'a> {
    ms: &'a MeasurementSet,
    request: &'a LeakageSolveRequest,
    models: &'a super::super::model::ModelInputs,
    preapplied: &'a PreappliedRows,
    spectral_weights: SpectralWeights,
    channel_bin_widths: &'a BTreeMap<i32, usize>,
}

type BaselineAverages = BTreeMap<(i32, i32, i32), Vec<CoherencyMean>>;

impl LeakageInputs<'_> {
    fn solve_interval(
        &self,
        rows: &[BucketedRow<'_>],
        antennas: usize,
        reference: i32,
    ) -> Result<Option<LeakageSolution>, GainSolveError> {
        let first = rows[0].0;
        let data = match self.preapplied.data.get(&first.row_index) {
            Some(preapplied) => &preapplied.corrected_data,
            None => main_cell(self.ms, first.row_index, "DATA")?,
        };
        if data.shape().len() != 2 {
            return Err(unsupported_shape("DATA", data));
        }
        let channels = selected_channels(first.channel_selection.as_ref(), data.shape()[1])?;
        let bin_width = if self.request.kind.channelized() {
            self.channel_bin_widths
                .get(&first.data_spw_id)
                .copied()
                .unwrap_or(1)
        } else {
            channels.len().max(1)
        };
        let count = channels.len().div_ceil(bin_width);
        let mut fits = (0..count)
            .map(|_| LeakageFit::new(antennas))
            .collect::<Vec<_>>();
        let mut averages = BaselineAverages::new();
        let mut current_bin = None;
        let mut time = WeightedTime::default();
        for batch in rows.chunks(256) {
            let indices = batch
                .iter()
                .map(|(row, _, _)| row.row_index)
                .collect::<Vec<_>>();
            let input = read_input_batch(
                self.ms,
                &indices,
                (
                    self.preapplied.data.is_empty(),
                    batch
                        .iter()
                        .any(|(row, _, _)| self.models.uses_fourier(row.field_id)),
                ),
                batch
                    .iter()
                    .any(|(row, _, _)| self.models.reads_column(row.field_id)),
                batch
                    .iter()
                    .map(|row| self.spectral_weights.contains(row))
                    .collect(),
                false,
            )?;
            for (index, &(row, _, _)) in batch.iter().enumerate() {
                let bin = self.averaging_bin(first.time_seconds, row);
                if current_bin != Some(bin) {
                    accumulate_averages(std::mem::take(&mut averages), &mut fits, antennas);
                    current_bin = Some(bin);
                }
                let key = (row.data_spw_id, row.antenna1, row.antenna2);
                let bins = averages
                    .entry(key)
                    .or_insert_with(|| vec![CoherencyMean::default(); count]);
                let weight = self.average_row(row, (&input, index), &channels, bin_width, bins)?;
                time.observe(row.time_centroid_seconds, weight);
            }
        }
        accumulate_averages(averages, &mut fits, antennas);
        let mut nonempty = false;
        let mut values = Vec::with_capacity(count);
        for fit in fits {
            let channel = fit.solve(reference).unwrap_or_else(|| vec![None; antennas]);
            nonempty |= channel.iter().any(Option::is_some);
            values.push(channel);
        }
        Ok(nonempty.then(|| LeakageSolution {
            spectral_window: first.data_spw_id,
            field: first.field_id,
            observation: first.observation_id,
            scan: first.scan_number,
            time: time.mean().unwrap_or(first.time_seconds),
            reference,
            channels,
            values,
        }))
    }

    fn averaging_bin(&self, begin: f64, row: &SelectedSolveRow) -> u64 {
        let requested = if self.request.preaverage_seconds < 0.0 {
            300.0
        } else {
            self.request.preaverage_seconds
        };
        match self.request.interval {
            GainSolveInterval::Integration => row.time_seconds.to_bits(),
            _ => {
                // genericGatherAndSolve averages up to solint when preavg is
                // zero or exceeds it. Each bucket here already lies in solint.
                if requested > f64::from(f32::EPSILON) {
                    ((row.time_seconds - begin) / requested) as u64
                } else {
                    0
                }
            }
        }
    }

    fn average_row(
        &self,
        row: &SelectedSolveRow,
        batch: (&SolveInputBatch, usize),
        channels: &[usize],
        bin_width: usize,
        bins: &mut [CoherencyMean],
    ) -> Result<f64, GainSolveError> {
        let prior = self.preapplied.data.get(&row.row_index);
        let input = read_row_inputs(
            self.ms,
            row,
            prior,
            self.models.reads_column(row.field_id),
            batch,
        )?;
        let correlations = correlation_types_for_ddid(self.ms, row.data_desc_id)?;
        let order = coherency_order(row.data_desc_id, &correlations).map_err(prior_error)?;
        let raw_flags = if self.models.uses_fourier(row.field_id) {
            match input_cell(self.ms, row.row_index, batch, "FLAG")? {
                ArrayValue::Bool(flags) => flags,
                other => return Err(unsupported_shape("FLAG", other)),
            }
        } else {
            input.flags.as_ref()
        };
        let prediction = self
            .models
            .predict(self.ms, row, &correlations, raw_flags)?;
        let point_model = || {
            let first = correlations[order[0]];
            ArrayD::from_shape_fn(input.data.raw_dim(), |index| {
                samples::point_visibility(self.request.stokes, correlations[index[0]], first)
            })
        };
        let model = prediction
            .or_else(|| input.model.cloned())
            .unwrap_or_else(point_model);
        let forward = self
            .preapplied
            .model
            .as_ref()
            .expect("P Jones is on the model side of D");
        let (model, flags) = forward
            .corrupt(row.row_index, &correlations, model, &input.flags)
            .map_err(prior_error)?;
        let flags = flags.as_ref().unwrap_or(input.flags.as_ref());
        let samples = input.samples();
        let mut total = 0.0;
        for (position, &channel) in channels.iter().enumerate() {
            if channel >= samples.data.shape()[1]
                || order.iter().any(|&corr| flags[[corr, channel]])
            {
                continue;
            }
            let slot = position / bin_width;
            let weights = order.map(|corr| samples.weights[[corr, channel]]);
            bins[slot].observe(
                order.map(|corr| samples.data[[corr, channel]]),
                order.map(|corr| model[[corr, channel]]),
                weights,
            );
            total += weights
                .into_iter()
                .filter(|weight| weight.is_finite() && *weight > 0.0)
                .map(f64::from)
                .sum::<f64>();
        }
        Ok(total)
    }
}

fn prior_error(source: crate::ApplyExecutionError) -> GainSolveError {
    GainSolveError::PriorCalibrationApply {
        source: Box::new(source),
    }
}

/// Ordered solve rows release the averaged coherencies at each time boundary.
fn accumulate_averages(averages: BaselineAverages, fits: &mut [LeakageFit], antennas: usize) {
    for ((_, left, right), bins) in averages {
        let (Ok(left), Ok(right)) = (usize::try_from(left), usize::try_from(right)) else {
            continue;
        };
        if left >= antennas || right >= antennas {
            continue;
        }
        for (fit, mean) in fits.iter_mut().zip(bins) {
            if let Some((data, model, weights)) = mean.finish() {
                fit.add_baseline([left, right], data, model, weights);
            }
        }
    }
}

#[derive(Default, Clone)]
struct CoherencyMean {
    data: [Complex32; 4],
    model: [Complex32; 4],
    weights: [f32; 4],
}

impl CoherencyMean {
    fn observe(&mut self, data: [Complex32; 4], model: [Complex32; 4], weights: [f32; 4]) {
        for (corr, weight) in weights.into_iter().enumerate() {
            if weight > 0.0
                && weight.is_finite()
                && data[corr].is_finite()
                && model[corr].is_finite()
            {
                self.data[corr] += weight * data[corr];
                self.model[corr] += weight * model[corr];
                self.weights[corr] += weight;
            }
        }
    }

    fn finish(mut self) -> Option<([Complex32; 4], [Complex32; 4], [f32; 4])> {
        for (corr, weight) in self.weights.into_iter().enumerate() {
            if weight <= 0.0 {
                return None;
            }
            self.data[corr] /= weight;
            self.model[corr] /= weight;
        }
        Some((self.data, self.model, self.weights))
    }
}
