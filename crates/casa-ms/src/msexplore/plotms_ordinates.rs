// SPDX-License-Identifier: LGPL-3.0-or-later
//! Share selected visibility cells and channel-bin samples between plot axes.

use casa_types::Complex64;
use ndarray::ArrayView2;

use super::MsAxis;
use super::{
    ChannelBin, ComplexGrid, FloatGrid, MsDataColumn, MsPlotSpec, MsScatterPointRef,
    MsScatterSeries, PlotMsInput, PreparedSelectedDataSource, VisibilityBinSamples,
    check_grid_shapes, collect_bin_samples_into, compute_weighted_visibility_math, plotms_figure,
    scatter_series_identity,
};
use crate::MeasurementSet;

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
}

#[derive(Debug, Default)]
pub(super) struct AveragedVisibility {
    values: Vec<Complex64>,
    weights: Vec<f64>,
}

impl AveragedVisibility {
    pub(super) fn add(
        &mut self,
        samples: VisibilityBinSamples<'_>,
        scalar_weight: f64,
        conjugate: bool,
    ) {
        if conjugate {
            self.values.extend(samples.values.iter().map(|v| v.conj()));
        } else {
            self.values.extend_from_slice(samples.values);
        }
        if samples.weights.is_empty() {
            self.weights.resize(self.values.len(), scalar_weight);
        } else {
            self.weights.extend_from_slice(samples.weights);
        }
    }

    pub(super) fn project(
        &self,
        axis: MsAxis,
        scalar: bool,
        zero_invalid_weight: bool,
    ) -> Option<f64> {
        compute_weighted_visibility_math(
            axis,
            &self.values,
            &self.weights,
            scalar,
            zero_invalid_weight,
        )
    }
}

pub(super) struct VisibilityTimeBounds {
    first: f64,
    last_unflagged: Option<f64>,
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
    if series
        .provenance
        .iter()
        .is_sorted_by_key(MsScatterPointRef::plotms_order)
    {
        return;
    }
    let mut order = (0..series.provenance.len()).collect::<Vec<_>>();
    order.sort_unstable_by_key(|&index| series.provenance[index].plotms_order());
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
