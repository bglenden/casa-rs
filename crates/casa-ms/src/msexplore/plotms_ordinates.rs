// SPDX-License-Identifier: LGPL-3.0-or-later
//! Share selected visibility cells and channel-bin samples between plot axes.

use casa_types::Complex64;
use ndarray::ArrayView2;

use super::{
    ChannelBin, ComplexGrid, FloatGrid, MsDataColumn, MsPlotSpec, PlotMsInput,
    PreparedSelectedDataSource, VisibilityBinSamples, check_grid_shapes, collect_bin_samples_into,
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
