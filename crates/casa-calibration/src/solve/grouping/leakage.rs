// SPDX-License-Identifier: LGPL-3.0-or-later
//! Group polarized samples and solve known-source leakage.

mod channel_samples;
mod fit;
mod interval;
mod output;
mod standard;

use super::*;
use crate::execute::coherency_order;
use crate::session::prepare_calibration_solve;
use std::path::PathBuf;

use interval::IntervalFits;

#[derive(Clone, Copy)]
pub(crate) enum LeakageSolveKind {
    D,
    Df,
    Dlls,
    Dflls,
}

impl LeakageSolveKind {
    pub(crate) fn subtype(self) -> &'static str {
        match self {
            Self::D => "D Jones",
            Self::Df => "Df Jones",
            Self::Dlls => "DLLS Jones",
            Self::Dflls => "DfLLS Jones",
        }
    }

    fn channelized(self) -> bool {
        matches!(self, Self::Df | Self::Dflls)
    }

    fn iterative(self) -> bool {
        matches!(self, Self::D | Self::Df)
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
    pub(crate) min_baselines: usize,
    pub(crate) min_snr: f32,
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
    /// Channel-major solutions with one pair of leakage terms per antenna.
    pub(crate) values: Vec<Vec<LeakageEstimate>>,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct LeakageEstimate {
    gain: [Complex32; 2],
    valid: [bool; 2],
    error: [f32; 2],
    snr: [f32; 2],
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
    let mut windows = BTreeMap::new();
    for row in &context.rows {
        if let std::collections::btree_map::Entry::Vacant(entry) = windows.entry(row.data_spw_id) {
            let data = main_cell(ms, row.row_index, "DATA").map_err(error)?;
            if data.shape().len() != 2 {
                return Err(unsupported_shape("DATA", data).to_string());
            }
            let channels = selected_channels(row.channel_selection.as_ref(), data.shape()[1])
                .map_err(error)?;
            let bin_width = if request.kind.channelized() {
                channel_bin_widths
                    .get(&row.data_spw_id)
                    .copied()
                    .unwrap_or(1)
            } else {
                channels.len().max(1)
            };
            entry.insert(WindowChannels {
                channels,
                bin_width,
            });
        }
    }
    if request.combine.spectral_windows
        && let Some(first) = windows.values().next()
        && windows
            .values()
            .any(|window| window.count() != first.count())
    {
        return Err("Cannot combine spectral windows with different channel counts".into());
    }
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
        windows: &windows,
    };
    let mut intervals = ordered
        .chunk_by(|a, b| a.1 == b.1 && a.2 == b.2)
        .collect::<Vec<_>>();
    intervals.sort_by(|a, b| {
        interval_window(a)
            .cmp(&interval_window(b))
            .then_with(|| a[0].0.time_seconds.total_cmp(&b[0].0.time_seconds))
    });
    let mut references = Vec::new();
    if context.refant_id >= 0 {
        references.extend([context.refant_id, context.refant_id]);
        references.extend(context.refant_alternates.iter().copied());
        references.extend(
            super::super::refant_distance_order(ms, *references.last().expect("primary antenna"))
                .map_err(error)?,
        );
    }
    let mut previous: Option<usize> = None;
    let mut solutions: Vec<LeakageSolution> = Vec::new();
    for rows in intervals {
        let previous_interval = previous
            .and_then(|index| solutions.get(index))
            .filter(|solution| solution.spectral_window == interval_window(rows));
        if let Some(solution) = setup
            .solve_interval(
                rows,
                context.available_antennas.len(),
                &references,
                previous_interval,
            )
            .map_err(error)?
        {
            if solution.reference >= 0 {
                references[1] = solution.reference;
                previous = Some(solutions.len());
            }
            solutions.push(solution);
        }
    }
    if request.kind.iterative() {
        interval::reference_iterative_solutions(&mut solutions, &mut references);
    }
    let bin_widths = windows
        .iter()
        .map(|(&id, window)| (id, window.bin_width))
        .collect();
    output::write(ms, request, &context.rows, &solutions, bin_widths)
}

struct LeakageInputs<'a> {
    ms: &'a MeasurementSet,
    request: &'a LeakageSolveRequest,
    models: &'a super::super::model::ModelInputs,
    preapplied: &'a PreappliedRows,
    spectral_weights: SpectralWeights,
    windows: &'a BTreeMap<i32, WindowChannels>,
}

struct WindowChannels {
    channels: Vec<usize>,
    bin_width: usize,
}

impl WindowChannels {
    fn count(&self) -> usize {
        self.channels.len().div_ceil(self.bin_width)
    }
}

struct BaselineAverages {
    start: f64,
    bins: Vec<CoherencyMean>,
}

fn interval_window(rows: &[BucketedRow<'_>]) -> i32 {
    rows.iter()
        .map(|(row, _, _)| row.data_spw_id)
        .min()
        .expect("nonempty solve interval")
}

impl LeakageInputs<'_> {
    fn solve_interval(
        &self,
        rows: &[BucketedRow<'_>],
        antennas: usize,
        references: &[i32],
        previous: Option<&LeakageSolution>,
    ) -> Result<Option<LeakageSolution>, GainSolveError> {
        let first = rows[0].0;
        let spectral_window = interval_window(rows);
        let count = self.windows[&spectral_window].count();
        let requested = if self.request.preaverage_seconds < 0.0 {
            300.0
        } else {
            self.request.preaverage_seconds
        };
        // For preavg=0 CASA averages throughout the enclosing solint.
        let span = (requested > f64::from(f32::EPSILON)).then_some(0.999 * requested);
        let mut fits = IntervalFits::new(count, antennas, self.request.kind);
        let mut averages = BTreeMap::new();
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
                let pair = [row.antenna1, row.antenna2];
                let active = averages
                    .entry((row.field_id, row.data_desc_id, pair))
                    .or_insert_with(|| BaselineAverages {
                        start: row.time_seconds,
                        bins: vec![CoherencyMean::default(); count],
                    });
                if span.is_some_and(|span| row.time_seconds - active.start > span) {
                    fits.accumulate(pair, &mut active.bins, antennas)?;
                    active.start = row.time_seconds;
                }
                let weight = self.average_row(
                    row,
                    (&input, index),
                    &self.windows[&row.data_spw_id],
                    &mut active.bins,
                )?;
                time.observe(row.time_centroid_seconds, weight);
            }
        }
        for ((_, _, pair), mut active) in averages {
            fits.accumulate(pair, &mut active.bins, antennas)?;
        }
        Ok(fits
            .finish(antennas, references, previous, self.request)?
            .map(|(reference, values)| LeakageSolution {
                spectral_window,
                field: first.field_id,
                observation: first.observation_id,
                scan: first.scan_number,
                time: time.mean().unwrap_or(first.time_seconds),
                reference,
                values,
            }))
    }

    fn average_row(
        &self,
        row: &SelectedSolveRow,
        batch: (&SolveInputBatch, usize),
        window: &WindowChannels,
        bins: &mut [CoherencyMean],
    ) -> Result<f64, GainSolveError> {
        let correlations = correlation_types_for_ddid(self.ms, row.data_desc_id)?;
        let order = coherency_order(row.data_desc_id, &correlations).map_err(prior_error)?;
        let prior = self
            .preapplied
            .data_prior
            .as_ref()
            .map(|prior| {
                prior
                    .correct(
                        row.row_index,
                        &correlations,
                        input_cell(self.ms, row.row_index, batch, "DATA")?,
                        input_cell(self.ms, row.row_index, batch, "FLAG")?,
                    )
                    .map_err(prior_error)
            })
            .transpose()?
            .flatten();
        let input = read_row_inputs(
            self.ms,
            row,
            prior.as_ref(),
            self.models.reads_column(row.field_id),
            batch,
        )?;
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
        let retain_unweighted_model = self.request.kind.iterative()
            && matches!(self.request.interval, GainSolveInterval::Integration)
            && window.bin_width == 1;
        for (position, &channel) in window.channels.iter().enumerate() {
            if channel >= samples.data.shape()[1]
                || order.iter().any(|&corr| flags[[corr, channel]])
            {
                continue;
            }
            let slot = position / window.bin_width;
            let weights = order.map(|corr| samples.weights[[corr, channel]]);
            bins[slot].observe(
                order.map(|corr| samples.data[[corr, channel]]),
                order.map(|corr| model[[corr, channel]]),
                weights,
                retain_unweighted_model,
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

#[derive(Default, Clone)]
struct CoherencyMean {
    data: [Complex32; 4],
    model: [Complex32; 4],
    weights: [f32; 4],
}

impl CoherencyMean {
    fn observe(
        &mut self,
        data: [Complex32; 4],
        model: [Complex32; 4],
        weights: [f32; 4],
        retain_unweighted_model: bool,
    ) {
        for (corr, weight) in weights.into_iter().enumerate() {
            if weight > 0.0
                && weight.is_finite()
                && data[corr].is_finite()
                && model[corr].is_finite()
            {
                if self.weights[corr] == 0.0 {
                    self.model[corr] = Complex32::ZERO;
                }
                self.data[corr] += weight * data[corr];
                self.model[corr] += weight * model[corr];
                self.weights[corr] += weight;
            } else if retain_unweighted_model
                && self.weights[corr] == 0.0
                && model[corr].is_finite()
            {
                // The unaveraged model keeps its parallel hands even when
                // the corresponding observations have zero weight.
                self.model[corr] = model[corr];
            }
        }
    }

    fn finish(mut self, require_all: bool) -> Option<([Complex32; 4], [Complex32; 4], [f32; 4])> {
        if self.weights.iter().all(|&weight| weight <= 0.0) {
            return None;
        }
        for (corr, weight) in self.weights.into_iter().enumerate() {
            if weight > 0.0 {
                self.data[corr] /= weight;
                self.model[corr] /= weight;
            } else if require_all {
                return None;
            }
        }
        Some((self.data, self.model, self.weights))
    }
}
