// SPDX-License-Identifier: LGPL-3.0-or-later
//! Share selected visibility cells and channel-bin samples between plot axes.

use casa_types::Complex64;
use ndarray::ArrayView2;

use super::MsAxis;
use super::{
    ChannelBin, ComplexGrid, FloatGrid, MsDataColumn, MsPlotSpec, MsScatterSeries, PlotMsInput,
    PreparedSelectedDataSource, VisibilityBinSamples, check_grid_shapes, collect_bin_samples_into,
    compute_visibility_math, compute_weighted_visibility_math, plotms_figure,
    scatter_series_identity, weighted_visibility_mean,
};
use crate::MeasurementSet;
use crate::selection::ChannelRange;

pub(super) struct VisibilityColumns<'a> {
    sources: Vec<(MsDataColumn, PreparedSelectedDataSource<'a>)>,
    axes: Vec<Option<usize>>,
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
        Ok(Self { sources, axes })
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
            let grid = source.row(row, project)?;
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
        axis_weights: [Option<&'b FloatGrid>; 3],
    ) -> Vec<Option<&'b FloatGrid>> {
        if axis_weights.iter().all(Option::is_none) {
            return Vec::new();
        }
        let mut weights = vec![None; self.sources.len()];
        for (index, spectrum) in self.axes.iter().copied().zip(axis_weights) {
            if let (Some(index), Some(spectrum)) = (index, spectrum) {
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
        corr: usize,
        bin: &ChannelBin,
        zero_invalid_weight: bool,
    ) -> VisibilityBinSamples<'b> {
        self.axes[axis].map_or_else(VisibilityBinSamples::default, |index| {
            let buffer = &buffers[index];
            VisibilityBinSamples::new(
                Some(&grids[index]),
                &buffer.values,
                &buffer.weights,
                corr,
                bin,
                zero_invalid_weight,
            )
        })
    }
}

#[derive(Default)]
pub(super) struct BinBuffer {
    values: Vec<Complex64>,
    weights: Vec<f64>,
}

impl BinBuffer {
    pub(super) fn collect(
        &mut self,
        grid: &ComplexGrid,
        flags: &ArrayView2<'_, bool>,
        weight: Option<&FloatGrid>,
        corr: usize,
        bin: &ChannelBin,
    ) {
        self.weights.clear();
        collect_bin_samples_into(
            grid,
            flags,
            corr,
            bin,
            weight.map(|spectrum| (spectrum, &mut self.weights)),
            &mut self.values,
        );
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

    /// The CASA scalar reducer stores amplitude and phase as separate components.
    pub(super) fn add_scalar_time(&mut self, samples: VisibilityBinSamples<'_>, weight: f64) {
        let (values, weights) = self.weighted_mut();
        values.extend(
            samples
                .values
                .iter()
                .map(|value| Complex64::new(value.norm(), value.arg())),
        );
        weights.resize(values.len(), weight);
    }

    pub(super) fn project_scalar_time(&self, axis: MsAxis) -> Option<f64> {
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
        }
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
        }
    }
}

pub(super) struct VisibilityTimeBounds {
    first: f64,
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
            last_unflagged: None,
        }
    }

    pub(super) fn observe(&mut self, time: f64, unflagged: bool) {
        self.first = self.first.min(time);
        if unflagged {
            self.last_unflagged = Some(self.last_unflagged.map_or(time, |last| last.max(time)));
        }
    }

    pub(super) fn midpoint(&self) -> Option<f64> {
        // CASA anchors on the first integration and ends at the last usable one.
        self.last_unflagged
            .map(|last| self.first + (last - self.first) / 2.0)
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
