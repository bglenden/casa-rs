// SPDX-License-Identifier: LGPL-3.0-or-later
//! Share selected visibility cells and channel-bin samples between plot axes.

use std::collections::HashSet;

use casa_types::{Complex32, Complex64};
use ndarray::ArrayView2;

use super::MsAxis;
use super::{
    ChannelBin, ComplexGrid, FloatGrid, MsAverageSpec, MsDataColumn, MsPlotSpec, MsScatterSeries,
    PlotMsInput, PreparedSelectedDataSource, VisibilityBinSamples, VisibilityColumnProjection,
    check_grid_shapes, collect_bin_samples_into, compute_visibility_math,
    compute_weighted_visibility_math, plotms_figure, scatter_series_identity,
    weighted_visibility_mean,
};
use crate::MeasurementSet;
use crate::selection::ChannelRange;
use crate::subtables::data_description::MsDataDescription;
use crate::transform::{WeightKind, sigma_from_weight, weight_from_sigma};

pub(super) struct VisibilityColumns<'a> {
    sources: Vec<(MsDataColumn, PreparedSelectedDataSource<'a>)>,
    axes: Vec<Option<usize>>,
    project_after_average: bool,
}

impl<'a> VisibilityColumns<'a> {
    pub(super) fn prepare(
        ms: &MeasurementSet,
        spec: &MsPlotSpec,
        rows: &[usize],
        input: Option<&PlotMsInput<'a>>,
    ) -> Result<Self, String> {
        let primary = spec.data_column;
        let x = input
            .and_then(|input| input.x_data_column)
            .unwrap_or(primary);
        let secondary = input
            .and_then(|input| input.secondary_y_data_column)
            .unwrap_or(primary);
        let additional = input.map_or(&[][..], |input| input.additional_ordinates);
        let kinds = std::iter::once(spec.x_axis).chain(spec.y_axes.iter().copied());
        let columns = [x, primary, secondary]
            .into_iter()
            .chain(additional.iter().map(|&(_, column)| column));
        let mut sources = Vec::new();
        let mut axes = Vec::with_capacity(spec.y_axes.len() + 1);
        for (axis, column) in kinds.zip(columns) {
            if !axis.is_visibility_math() {
                axes.push(None);
                continue;
            }
            let index = match sources.iter().position(|(existing, _)| *existing == column) {
                Some(index) => index,
                None => {
                    let index = sources.len();
                    sources.push((
                        column,
                        PreparedSelectedDataSource::new(ms, column, rows, input)?,
                    ));
                    index
                }
            };
            axes.push(Some(index));
        }
        Ok(Self {
            sources,
            axes,
            project_after_average: spec.averaging.avgchannel.is_some_and(|width| width > 1)
                || input.is_some_and(|input| input.extended_derived),
        })
    }

    pub(super) fn column(&self, ordinate: usize) -> Option<MsDataColumn> {
        self.axes[ordinate + 1].map(|index| self.sources[index].0)
    }

    pub(super) fn grid<'b>(
        &self,
        axis: usize,
        grids: &'b [ComplexGrid],
    ) -> Option<&'b ComplexGrid> {
        self.axes[axis].map(|index| &grids[index])
    }

    pub(super) fn series_identity(
        &self,
        spec: &MsPlotSpec,
        ordinate: usize,
        group_key: &str,
        group_label: &str,
    ) -> (String, String, String) {
        let axis = spec.y_axes[ordinate];
        let (key, label, color) =
            scatter_series_identity(axis, spec.y_axes.len() > 1, group_key, group_label);
        if spec.y_axes.len() <= 2 && spec.secondary_y_axis() != Some(spec.y_axis()) {
            return (key, label, color);
        }
        let column = self.column(ordinate).unwrap_or(spec.data_column);
        let label = plotms_figure::column_axis_name(axis, column);
        let label = if group_key == "all" {
            label
        } else {
            format!("{label} · {group_label}")
        };
        (
            format!("{ordinate}::{key}"),
            label,
            format!("{ordinate}::{color}"),
        )
    }

    pub(super) fn read_row(
        &self,
        row: usize,
        project: bool,
        grids: &mut Vec<ComplexGrid>,
    ) -> Result<(), String> {
        grids.clear();
        for (_, source) in &self.sources {
            let grid = source.row(row, project, self.project_after_average)?;
            if let Some(first) = grids.first() {
                check_grid_shapes(first, &grid)?;
            }
            grids.push(grid);
        }
        Ok(())
    }

    pub(super) fn buffers(&self) -> Vec<BinBuffer> {
        (0..self.sources.len())
            .map(|_| BinBuffer::default())
            .collect()
    }

    pub(super) fn column_weights<'b>(
        &self,
        axis_weights: impl IntoIterator<Item = Option<&'b FloatGrid>>,
    ) -> Vec<Option<&'b FloatGrid>> {
        let mut weights = Vec::new();
        for (index, spectrum) in self.axes.iter().copied().zip(axis_weights) {
            if let (Some(index), Some(spectrum)) = (index, spectrum) {
                if weights.is_empty() {
                    weights.resize(self.sources.len(), None);
                }
                weights[index] = Some(spectrum);
            }
        }
        weights
    }

    pub(super) fn samples<'b>(
        &self,
        axis: usize,
        grids: &[ComplexGrid],
        buffers: &'b [BinBuffer],
        zero_invalid_weight: bool,
    ) -> VisibilityBinSamples<'b> {
        self.axes[axis].map_or_else(VisibilityBinSamples::default, |index| {
            let buffer = &buffers[index];
            VisibilityBinSamples {
                values: &buffer.values,
                weights: &buffer.weights,
                projection: buffer.projection,
                signed_amplitude: grids[index].signed_amplitude,
                zero_invalid_weight,
            }
        })
    }
}

#[derive(Default)]
pub(super) struct BinBuffer {
    values: Vec<Complex64>,
    weights: Vec<f64>,
    projection: Option<(VisibilityColumnProjection, [Complex64; 2])>,
}

impl BinBuffer {
    pub(super) fn collect(
        &mut self,
        grid: &ComplexGrid,
        flags: &ArrayView2<'_, bool>,
        weight: Option<&FloatGrid>,
        corr: usize,
        bin: &ChannelBin,
        show_flagged: bool,
    ) {
        self.weights.clear();
        self.projection = None;
        collect_bin_samples_into(
            grid,
            flags,
            corr,
            bin,
            show_flagged,
            weight.map(|spectrum| (spectrum, &mut self.weights)),
            &mut self.values,
        );
        let Some((projection, model)) = grid.model.as_ref() else {
            return;
        };
        if let Some(&observed) = self.values.first() {
            debug_assert_eq!(bin.channels().count(), 1);
            self.projection = Some((
                *projection,
                [observed, model[corr * grid.chan_count + bin.start]],
            ));
        }
    }

    pub(super) fn collect_channel_average(
        &mut self,
        grid: &ComplexGrid,
        flags: &ArrayView2<'_, bool>,
        spectra: [Option<&FloatGrid>; 2],
        corr: usize,
        bin: &ChannelBin,
    ) {
        self.values.clear();
        self.weights.clear();
        self.projection = None;
        if let Some((projection, operands)) = channel_averaged_grid(grid, flags, spectra, corr, bin)
        {
            if let Some(projection) = projection {
                self.projection = Some((projection, operands));
            } else {
                self.values.push(operands[0]);
            }
        }
    }

    pub(super) fn normalize_vector_average(&mut self) {
        let Some(average) = vector_mean(&self.values, &self.weights) else {
            return;
        };
        self.values[0] = average;
        self.values.truncate(1);
        self.weights.clear();
    }
}

fn channel_averaged_grid(
    grid: &ComplexGrid,
    flags: &ArrayView2<'_, bool>,
    spectra: [Option<&FloatGrid>; 2],
    corr: usize,
    bin: &ChannelBin,
) -> Option<(Option<VisibilityColumnProjection>, [Complex64; 2])> {
    if let Some((projection, model)) = grid.model.as_ref() {
        let operands = channel_averaged_operands(
            [grid.values.as_slice(), model.as_slice()],
            grid.chan_count,
            flags,
            spectra,
            corr,
            bin,
        )?;
        Some((Some(*projection), operands))
    } else {
        let [value] = channel_averaged_operands(
            [grid.values.as_slice()],
            grid.chan_count,
            flags,
            [spectra[0]],
            corr,
            bin,
        )?;
        Some((None, [value, Complex64::default()]))
    }
}

fn channel_averaged_operands<const N: usize>(
    inputs: [&[Complex64]; N],
    channel_count: usize,
    flags: &ArrayView2<'_, bool>,
    spectra: [Option<&FloatGrid>; N],
    corr: usize,
    bin: &ChannelBin,
) -> Option<[Complex64; N]> {
    let mut sums = [Complex32::new(0.0, 0.0); N];
    let mut total_weights = [0.0_f32; N];
    let mut sample_count = 0_usize;
    for channel in bin.channels() {
        if channel >= channel_count || flags[(corr, channel)] {
            continue;
        }
        let index = corr * channel_count + channel;
        // The channel iterator accumulates clear visibility values in Complex32.
        // In particular it retains any nonfinite value on a clear channel.
        for operand in 0..N {
            let value = inputs[operand][index];
            let weight = spectra[operand].map_or(1.0, |weights| weights.values[index] as f32);
            sums[operand] += Complex32::new(value.re as f32, value.im as f32) * weight;
            total_weights[operand] += weight;
        }
        sample_count += 1;
    }
    if sample_count == 0 {
        return None;
    }
    Some(std::array::from_fn(|operand| {
        let normalization = if spectra[operand].is_some() {
            total_weights[operand]
        } else {
            sample_count as f32
        };
        channel_visibility_mean(sums[operand], normalization, spectra[operand].is_some())
    }))
}

fn channel_visibility_mean(sum: Complex32, normalization: f32, weighted: bool) -> Complex64 {
    let valid_weight = has_channel_weight(normalization);
    let mut mean = if valid_weight {
        sum / normalization
    } else {
        Complex32::default()
    };
    // CASA divides a weighted complex sum by a complex normalization.
    // A nonfinite component affects its finite partner even for a real weight.
    if weighted && valid_weight && normalization.is_finite() {
        if sum.re.is_finite() && !sum.im.is_finite() {
            mean.re = f32::NAN;
        } else if sum.im.is_finite() && !sum.re.is_finite() {
            mean.im = f32::NAN;
        }
    }
    Complex64::new(f64::from(mean.re), f64::from(mean.im))
}

fn has_channel_weight(weight: f32) -> bool {
    // Compare the signed real sum directly. Squaring it can erase a finite
    // nonzero normalization in the CASA complex-weight kernel.
    weight != 0.0 && !weight.is_nan()
}

fn vector_mean(values: &[Complex64], weights: &[f64]) -> Option<Complex64> {
    if values.is_empty() {
        return None;
    }
    if weights.is_empty() {
        return Some(values.iter().copied().sum::<Complex64>() / values.len() as f64);
    }
    let total = weights.iter().sum::<f64>();
    Some(if total > 0.0 {
        weighted_visibility_mean(values, weights, total)
    } else {
        Complex64::new(0.0, 0.0)
    })
}

#[derive(Debug, Default)]
pub(super) struct AveragedVisibility(VisibilitySamples);

#[derive(Debug)]
enum VisibilitySamples {
    Weighted {
        values: Vec<Complex64>,
        weights: Vec<f64>,
    },
    ChannelMeans(Vec<ChannelVisibilityMean>),
    TimeProjection(TimeProjection),
    TimeChannelProjection(TimeChannelProjection),
}

impl Default for VisibilitySamples {
    fn default() -> Self {
        Self::Weighted {
            values: Vec::new(),
            weights: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct ChannelVisibilityMean {
    sum: Complex64,
    samples: usize,
}

#[derive(Clone, Debug)]
struct TimeProjection {
    projection: Option<VisibilityColumnProjection>,
    weighted_sums: [Complex32; 2],
    total_weights: [f32; 2],
}

#[derive(Clone, Copy)]
pub(super) struct TransformedVectorSample {
    visibility: Complex32,
    model: Option<(VisibilityColumnProjection, Complex32)>,
    pub(super) channel_weight: Option<f32>,
}

impl TimeProjection {
    fn channel_sample(
        grid: &ComplexGrid,
        spectra: [Option<&FloatGrid>; 2],
        corr: usize,
        channel: usize,
    ) -> Self {
        let index = corr * grid.chan_count + channel;
        let (projection, model_value) = grid
            .model
            .as_ref()
            .map_or((None, Complex64::default()), |(projection, model)| {
                (Some(*projection), model[index])
            });
        // DATA uses inverse variance. MODEL_DATA uses calibrated weights if the
        // iterator reads CORRECTED_DATA, and observed weights otherwise.
        let total_weights =
            spectra.map(|spectrum| spectrum.map_or(1.0, |grid| grid.values[index] as f32));
        let values = [grid.values[index], model_value];
        let weighted_sums = std::array::from_fn(|operand| {
            let value = values[operand];
            Complex32::new(value.re as f32, value.im as f32) * total_weights[operand]
        });
        Self {
            projection,
            weighted_sums,
            total_weights,
        }
    }

    fn accumulate(&mut self, sample: Self) {
        for (operand, value) in sample.weighted_sums.into_iter().enumerate() {
            self.weighted_sums[operand] += value;
            self.total_weights[operand] += sample.total_weights[operand];
        }
    }

    fn time_mean(&self, operand: usize) -> Complex32 {
        let sum = self.weighted_sums[operand];
        let weight = self.total_weights[operand];
        // AveragingTvi2 keeps the accumulated value for a nonpositive weight.
        if weight > 0.0 { sum / weight } else { sum }
    }

    fn project(&self, axis: MsAxis) -> Option<f64> {
        let means = std::array::from_fn(|operand| {
            let mean = self.time_mean(operand);
            Complex64::new(f64::from(mean.re), f64::from(mean.im))
        });
        self.project_operands(axis, means)
    }

    fn project_scalar(&self, axis: MsAxis) -> Option<f64> {
        use VisibilityColumnProjection::{
            ScalarRatio, ScalarResidual, VectorRatio, VectorResidual,
        };

        let visibility = self.time_mean(0);
        let model = self.time_mean(1);
        let projected = match self.projection {
            None => visibility,
            Some(VectorResidual | ScalarResidual) => visibility - model,
            Some(VectorRatio) => {
                let widen =
                    |value: Complex32| Complex64::new(f64::from(value.re), f64::from(value.im));
                let ratio =
                    VisibilityColumnProjection::complex_quotient(widen(visibility), widen(model));
                Complex32::new(ratio.re as f32, ratio.im as f32)
            }
            // CASA divides the packed amplitude and phase components for these
            // axes. Its scalar ratio uses their differences for Real and Imaginary.
            Some(ScalarRatio) if matches!(axis, MsAxis::Amplitude | MsAxis::Phase) => {
                Complex32::new(visibility.re / model.re, visibility.im / model.im)
            }
            Some(ScalarRatio) => visibility - model,
        };
        Some(f64::from(match axis {
            MsAxis::Amplitude | MsAxis::Real => projected.re,
            MsAxis::Phase => VisibilityColumnProjection::float_phase_degrees(projected.im),
            MsAxis::Imaginary => projected.im,
            _ => return None,
        }))
    }

    fn project_operands(&self, axis: MsAxis, means: [Complex64; 2]) -> Option<f64> {
        match self.projection {
            Some(projection) => projection.axis_value(axis, means, true),
            None => Some(VisibilityColumnProjection::vector_axis_value(
                axis, means[0],
            )),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct VectorTimeWeights<'a> {
    pub(super) spectra: [Option<&'a FloatGrid>; 2],
    pub(super) observed_operands: [bool; 2],
    pub(super) primary: Option<(&'a FloatGrid, WeightKind)>,
    pub(super) equal_channel_means: bool,
}

pub(super) struct ScalarTimeSample<'a> {
    pub(super) row_weight: f64,
    pub(super) channel_spectra: Option<[Option<&'a FloatGrid>; 2]>,
    pub(super) conjugate: bool,
}

#[derive(Clone, Debug)]
struct TimeChannelSample {
    operands: TimeProjection,
    primary_weight: f32,
}

#[derive(Debug)]
struct TimeChannelProjection {
    channels: Vec<Option<TimeChannelSample>>,
    observed_operands: [bool; 2],
    primary_is_observed: bool,
    equal_channel_means: bool,
}

impl TimeChannelProjection {
    fn project(&self, axis: MsAxis) -> Option<f64> {
        let (operands, means, _) = self.means_and_weight()?;
        operands.project_operands(axis, means)
    }

    fn means_and_weight(&self) -> Option<(&TimeProjection, [Complex64; 2], f32)> {
        let mut usable = self.channels.iter().flatten();
        let first = usable.next()?;
        let mut sums = [Complex32::default(); 2];
        let mut totals = [0.0_f32; 2];
        let mut visibility_weight = 0.0_f32;
        let mut spectrum_weight = 0.0_f32;
        // The channel transform receives normalized Float means and the time
        // stage's spectra, including the observed weight-to-sigma round trip.
        for channel in std::iter::once(first).chain(usable) {
            visibility_weight +=
                channel_weight_after_time(channel.primary_weight, self.primary_is_observed);
            // The output WEIGHT_SPECTRUM sums the original time weights.
            spectrum_weight += channel.primary_weight;
            for (operand, sum) in sums.iter_mut().enumerate() {
                let weight = if self.equal_channel_means {
                    1.0
                } else {
                    channel_weight_after_time(
                        channel.operands.total_weights[operand],
                        self.observed_operands[operand],
                    )
                };
                *sum += channel.operands.time_mean(operand) * weight;
                totals[operand] += weight;
            }
        }
        if !has_channel_weight(visibility_weight) {
            return None;
        }
        let means = std::array::from_fn(|operand| {
            channel_visibility_mean(sums[operand], totals[operand], true)
        });
        Some((&first.operands, means, spectrum_weight))
    }
}

fn channel_weight_after_time(weight: f32, observed: bool) -> f32 {
    if observed {
        weight_from_sigma(sigma_from_weight(weight))
    } else {
        weight
    }
}

impl AveragedVisibility {
    fn weighted_mut(&mut self) -> (&mut Vec<Complex64>, &mut Vec<f64>) {
        let VisibilitySamples::Weighted { values, weights } = &mut self.0 else {
            unreachable!("time channel means are combined before weighted accumulation");
        };
        (values, weights)
    }

    pub(super) fn add_weighted_value(&mut self, value: Complex64, weight: f64) {
        let (values, weights) = self.weighted_mut();
        values.push(value);
        weights.push(weight);
    }

    pub(super) fn add_vector_time_samples(
        &mut self,
        grid: &ComplexGrid,
        flags: &ArrayView2<'_, bool>,
        weights: VectorTimeWeights<'_>,
        corr: usize,
        bin: &ChannelBin,
    ) -> bool {
        if bin.averaged {
            return self.add_time_channel_samples(grid, flags, weights, corr, bin);
        }
        if flags[(corr, bin.start)] {
            return false;
        }
        self.add_time_projection(TimeProjection::channel_sample(
            grid,
            weights.spectra,
            corr,
            bin.start,
        ));
        true
    }

    fn add_time_projection(&mut self, sample: TimeProjection) {
        if let VisibilitySamples::TimeProjection(average) = &mut self.0 {
            average.accumulate(sample);
        } else {
            self.0 = VisibilitySamples::TimeProjection(sample);
        }
    }

    fn add_time_channel_samples(
        &mut self,
        grid: &ComplexGrid,
        flags: &ArrayView2<'_, bool>,
        weights: VectorTimeWeights<'_>,
        corr: usize,
        bin: &ChannelBin,
    ) -> bool {
        if !matches!(self.0, VisibilitySamples::TimeChannelProjection(_)) {
            self.0 = VisibilitySamples::TimeChannelProjection(TimeChannelProjection {
                channels: vec![None; bin.channels().count()],
                observed_operands: weights.observed_operands,
                primary_is_observed: weights
                    .primary
                    .is_some_and(|(_, kind)| kind == WeightKind::Observed),
                equal_channel_means: weights.equal_channel_means,
            });
        }
        let VisibilitySamples::TimeChannelProjection(projection) = &mut self.0 else {
            unreachable!("time and channel projections initialize one accumulator per channel");
        };
        let mut used = false;
        for (channel, average) in bin.channels().zip(&mut projection.channels) {
            if flags[(corr, channel)] {
                continue;
            }
            let operands = TimeProjection::channel_sample(grid, weights.spectra, corr, channel);
            let primary_weight = weights.primary.map_or(1.0, |(spectrum, _)| {
                spectrum.values[corr * spectrum.chan_count + channel] as f32
            });
            if let Some(average) = average {
                average.operands.accumulate(operands);
                average.primary_weight += primary_weight;
            } else {
                *average = Some(TimeChannelSample {
                    operands,
                    primary_weight,
                });
            }
            used = true;
        }
        used
    }

    /// The CASA scalar reducer stores amplitude and phase as separate components.
    pub(super) fn add_scalar(
        &mut self,
        samples: VisibilityBinSamples<'_>,
        weight: f64,
        conjugate: bool,
    ) {
        let (values, weights) = self.weighted_mut();
        let sample_weights = samples
            .weights
            .iter()
            .copied()
            .chain(std::iter::repeat(weight));
        for (value, weight) in samples.values.iter().zip(sample_weights) {
            let packed = Complex64::new(value.norm(), value.arg());
            values.push(if conjugate { packed.conj() } else { packed });
            weights.push(super::plotms_columns::baseline_weight(weight));
        }
    }

    pub(super) fn add_scalar_sample(
        &mut self,
        grid: &ComplexGrid,
        flags: &ArrayView2<'_, bool>,
        options: ScalarTimeSample<'_>,
        corr: usize,
        bin: &ChannelBin,
    ) -> bool {
        let mut sample = if let Some(spectra) = options.channel_spectra {
            let Some((projection, operands)) =
                channel_averaged_grid(grid, flags, spectra, corr, bin)
            else {
                return false;
            };
            TimeProjection {
                projection,
                weighted_sums: operands
                    .map(|value| Complex32::new(value.re as f32, value.im as f32)),
                total_weights: [1.0; 2],
            }
        } else {
            if flags[(corr, bin.start)] {
                return false;
            }
            TimeProjection::channel_sample(grid, [None; 2], corr, bin.start)
        };
        let weight = options.row_weight as f32;
        // The scalar reducer packs the channel means' amplitude and phase
        // into the two Float components before accumulating in time.
        sample.weighted_sums = sample.weighted_sums.map(|value| {
            let value = if options.conjugate {
                value.conj()
            } else {
                value
            };
            Complex32::new(value.norm(), value.arg()) * weight
        });
        sample.total_weights = [weight; 2];
        self.add_time_projection(sample);
        true
    }

    pub(super) fn project_scalar(&self, axis: MsAxis) -> Option<f64> {
        if let VisibilitySamples::TimeProjection(average) = &self.0 {
            return average.project_scalar(axis);
        }
        let mean = self.vector_mean()?;
        match axis {
            MsAxis::Amplitude | MsAxis::Real => Some(mean.re),
            MsAxis::Phase => Some(mean.im.to_degrees()),
            MsAxis::Imaginary => Some(mean.im),
            _ => None,
        }
    }

    pub(super) fn add_channel_time_row(
        &mut self,
        grid: &ComplexGrid,
        flags: &ArrayView2<'_, bool>,
        correlation: usize,
        bin: &ChannelBin,
    ) {
        if matches!(self.0, VisibilitySamples::Weighted { .. }) {
            self.0 = VisibilitySamples::ChannelMeans(vec![
                ChannelVisibilityMean::default();
                bin.channels().count()
            ]);
        }
        let VisibilitySamples::ChannelMeans(means) = &mut self.0 else {
            unreachable!("model-only channel averaging prepares per-channel time means");
        };
        for (channel, mean) in bin.channels().zip(means) {
            let value = grid.values[correlation * grid.chan_count + channel];
            if !flags[(correlation, channel)] && value.re.is_finite() && value.im.is_finite() {
                mean.sum += value;
                mean.samples += 1;
            }
        }
    }

    pub(super) fn vector_mean(&self) -> Option<Complex64> {
        match &self.0 {
            VisibilitySamples::Weighted { values, weights } => vector_mean(values, weights),
            VisibilitySamples::ChannelMeans(channels) => {
                let mut sum = Complex64::default();
                let mut usable = 0;
                for channel in channels.iter().filter(|channel| channel.samples > 0) {
                    sum += channel.sum / channel.samples as f64;
                    usable += 1;
                }
                (usable > 0).then(|| sum / usable as f64)
            }
            VisibilitySamples::TimeProjection(_) | VisibilitySamples::TimeChannelProjection(_) => {
                unreachable!("time operands project together")
            }
        }
    }

    pub(super) fn transformed_vector_sample(&self) -> Option<TransformedVectorSample> {
        match &self.0 {
            VisibilitySamples::TimeProjection(average) => Some(TransformedVectorSample {
                visibility: average.time_mean(0),
                model: average
                    .projection
                    .map(|projection| (projection, average.time_mean(1))),
                channel_weight: None,
            }),
            VisibilitySamples::TimeChannelProjection(average) => {
                let (sample, [visibility, model], weight) = average.means_and_weight()?;
                let narrow = |value: Complex64| Complex32::new(value.re as f32, value.im as f32);
                Some(TransformedVectorSample {
                    visibility: narrow(visibility),
                    model: sample
                        .projection
                        .map(|projection| (projection, narrow(model))),
                    channel_weight: Some(weight),
                })
            }
            _ => None,
        }
    }

    pub(super) fn add_normalized_vector_sample(
        &mut self,
        sample: TransformedVectorSample,
        weight: f32,
        conjugate: bool,
    ) {
        let weighted = |value: Complex32| {
            let value = if conjugate { value.conj() } else { value };
            // Even the first endpoint adds its contribution to positive zero.
            Complex32::default() + value * weight
        };
        let (projection, model, model_weight) = sample
            .model
            .map_or((None, Complex32::default(), 0.0), |(projection, model)| {
                (Some(projection), weighted(model), weight)
            });
        self.add_time_projection(TimeProjection {
            projection,
            weighted_sums: [weighted(sample.visibility), model],
            total_weights: [weight, model_weight],
        });
    }

    pub(super) fn add(
        &mut self,
        samples: VisibilityBinSamples<'_>,
        scalar_weight: f64,
        conjugate: bool,
    ) {
        let (values, weights) = self.weighted_mut();
        if conjugate {
            values.extend(samples.values.iter().map(|v| v.conj()));
        } else {
            values.extend_from_slice(samples.values);
        }
        if samples.weights.is_empty() {
            weights.resize(values.len(), scalar_weight);
        } else {
            weights.extend_from_slice(samples.weights);
        }
    }

    pub(super) fn project(
        &self,
        axis: MsAxis,
        scalar: bool,
        zero_invalid_weight: bool,
    ) -> Option<f64> {
        match &self.0 {
            VisibilitySamples::Weighted { values, weights } => {
                compute_weighted_visibility_math(axis, values, weights, scalar, zero_invalid_weight)
            }
            VisibilitySamples::ChannelMeans(_) => {
                let mean = self.vector_mean()?;
                compute_visibility_math(axis, &[mean], scalar)
            }
            VisibilitySamples::TimeProjection(average) => average.project(axis),
            VisibilitySamples::TimeChannelProjection(projection) => projection.project(axis),
        }
    }
}

pub(super) struct VisibilityTimeBounds {
    first: f64,
    last: f64,
    last_unflagged: Option<f64>,
}

#[derive(Default)]
pub(super) struct BaselineIntegrationGroups {
    anchor: Option<([i32; 4], f64)>,
    ordinal: usize,
}

impl BaselineIntegrationGroups {
    pub(super) fn observe(&mut self, ids: [i32; 4], time: f64, interval: f64) -> usize {
        match self.anchor {
            Some((anchor_ids, start))
                if anchor_ids == ids && time >= start && time - start <= interval => {}
            anchor => {
                self.ordinal += usize::from(anchor.is_some());
                self.anchor = Some((ids, time));
            }
        }
        self.ordinal
    }
}

pub(super) struct ScalarSpanColumns<'a> {
    pub(super) observations: &'a [i32],
    pub(super) scans: &'a [i32],
    pub(super) fields: &'a [i32],
    pub(super) descriptions: &'a [i32],
    pub(super) times: &'a [f64],
}

pub(super) struct ScalarSpanMetadata {
    pub(super) scan: i32,
    pub(super) field: i32,
}

pub(super) struct ScalarSpanPlan {
    row_groups: Vec<Option<usize>>,
    metadata: Vec<ScalarSpanMetadata>,
}

impl ScalarSpanPlan {
    pub(super) fn new(
        columns: ScalarSpanColumns<'_>,
        descriptions: &MsDataDescription<'_>,
        averaging: &MsAverageSpec,
    ) -> Result<Self, String> {
        let interval = averaging.avgtime.unwrap_or(0.0);
        let mut row_groups = Vec::with_capacity(columns.times.len());
        let mut metadata = Vec::<ScalarSpanMetadata>::new();
        let mut groups = BaselineIntegrationGroups::default();
        let mut windows = vec![None; descriptions.row_count()];
        for (slot, &time) in columns.times.iter().enumerate() {
            let Some(window) = windows.get_mut(columns.descriptions[slot] as usize) else {
                row_groups.push(None);
                continue;
            };
            let spw = match *window {
                Some(spw) => spw,
                None => {
                    let spw = descriptions
                        .spectral_window_id(columns.descriptions[slot] as usize)
                        .map_err(|error| error.to_string())?;
                    *window = Some(spw);
                    spw
                }
            };
            let scan = columns.scans[slot];
            let field = columns.fields[slot];
            let group = groups.observe(
                [
                    columns.observations[slot],
                    if averaging.avgscan { 0 } else { scan },
                    if averaging.avgfield { 0 } else { field },
                    if averaging.avgspw { 0 } else { spw },
                ],
                time,
                interval,
            );
            if group == metadata.len() {
                metadata.push(ScalarSpanMetadata { scan, field });
            } else if metadata[group].scan != scan {
                // PlotMS marks a scalar buffer spanning scans with scan -1.
                metadata[group].scan = -1;
            }
            row_groups.push(Some(group));
        }
        Ok(Self {
            row_groups,
            metadata,
        })
    }

    pub(super) fn group(&self, row_slot: usize) -> Option<usize> {
        self.row_groups[row_slot]
    }

    pub(super) fn metadata(&self, group: usize) -> &ScalarSpanMetadata {
        &self.metadata[group]
    }
}

#[derive(Default)]
pub(super) struct VisibilityAverageShapes {
    previous: Option<(usize, usize, usize)>,
}

impl VisibilityAverageShapes {
    pub(super) fn check_shape(
        &mut self,
        average: &str,
        group: usize,
        correlations: usize,
        channels: usize,
    ) -> Result<(), String> {
        // Samples in one reduction group are consecutive in iterator order.
        if let Some((previous_group, expected_correlations, expected_channels)) = self.previous
            && previous_group == group
            && (expected_correlations, expected_channels) != (correlations, channels)
        {
            return Err(format!(
                "PlotMS {average} requires equal correlation and channel counts within a bin; expected {expected_correlations} correlations and {expected_channels} channels, found {correlations} correlations and {channels} channels",
            ));
        }
        self.previous = Some((group, correlations, channels));
        Ok(())
    }
}

pub(super) fn has_unflagged_input(
    flags: &ArrayView2<'_, bool>,
    correlations: &[(usize, String)],
    selection: Option<&[ChannelRange]>,
) -> bool {
    correlations.iter().any(|(corr, _)| {
        let unflagged = |channel| !flags[(*corr, channel)];
        match selection {
            Some(ranges) => ranges
                .iter()
                .filter_map(ChannelRange::channels)
                .flatten()
                .filter(|&channel| channel < flags.ncols())
                .any(unflagged),
            None => (0..flags.ncols()).any(unflagged),
        }
    })
}

impl VisibilityTimeBounds {
    pub(super) fn new(time: f64) -> Self {
        Self {
            first: time,
            last: time,
            last_unflagged: None,
        }
    }

    pub(super) fn observe(&mut self, time: f64, unflagged: bool) {
        self.first = self.first.min(time);
        self.last = self.last.max(time);
        if unflagged {
            self.last_unflagged = Some(self.last_unflagged.map_or(time, |last| last.max(time)));
        }
    }

    pub(super) fn midpoint(&self) -> Option<f64> {
        // CASA anchors on the first integration and ends at the last usable one.
        self.last_unflagged
            .map(|last| self.first + (last - self.first) / 2.0)
    }

    pub(super) fn buffer_midpoint(&self) -> f64 {
        // A fully flagged first baseline still supplies the cache's buffer time.
        let last = self.last_unflagged.unwrap_or(self.last);
        self.first + (last - self.first) / 2.0
    }
}

#[derive(Clone, Copy)]
pub(super) struct PointDimensions {
    pub(super) channel: bool,
    pub(super) baseline: bool,
}

pub(super) struct PointIndex {
    pub(super) row: usize,
    pub(super) channel: usize,
    pub(super) correlation: usize,
}

/// Keep one clear sample per point in the pair's collapsed visibility shape.
#[derive(Default)]
pub(super) struct CollapsedPointMask {
    row: HashSet<(usize, usize)>,
    buffer: HashSet<(usize, usize)>,
    first_buffer_row: usize,
}

impl CollapsedPointMask {
    pub(super) fn begin_row(&mut self, row: usize, starts_buffer: bool) {
        self.row.clear();
        if starts_buffer {
            self.buffer.clear();
            self.first_buffer_row = row;
        }
    }

    pub(super) fn project_clear_sample(
        &mut self,
        ordinate: usize,
        row: usize,
        channel: usize,
        dimensions: PointDimensions,
        [channels, correlations]: [usize; 2],
    ) -> Option<PointIndex> {
        let channel_slot = if dimensions.channel { channel } else { 0 };
        let (seen, row) = if dimensions.baseline {
            (&mut self.row, row)
        } else {
            (&mut self.buffer, self.first_buffer_row)
        };
        if !seen.insert((ordinate, channel_slot)) {
            return None;
        }
        // PlotMS uses the collapsed point's linear index for the reported
        // identifiers, including projected dimensions of the full cube.
        let channels_per_point = if dimensions.channel { channels } else { 1 };
        let index = (row - self.first_buffer_row) * channels_per_point + channel_slot;
        Some(PointIndex {
            row,
            channel: index % channels,
            correlation: index % correlations,
        })
    }
}

pub(super) fn order_series(series: &mut MsScatterSeries) {
    let point_order = |index: usize| {
        let iterator_slot = series
            .plotms_metadata
            .get(index)
            .and_then(|metadata| metadata.averaging_group);
        series.provenance[index].plotms_order(iterator_slot)
    };
    let indices = 0..series.provenance.len();
    if indices.clone().is_sorted_by_key(point_order) {
        return;
    }
    let mut order = indices.collect::<Vec<_>>();
    order.sort_unstable_by_key(|&index| point_order(index));
    series.points = order.iter().map(|&index| series.points[index]).collect();
    series.provenance = order
        .iter()
        .map(|&index| series.provenance[index].clone())
        .collect();
    if !series.plotms_metadata.is_empty() {
        series.plotms_metadata = order
            .iter()
            .map(|&index| series.plotms_metadata[index])
            .collect();
    }
}
