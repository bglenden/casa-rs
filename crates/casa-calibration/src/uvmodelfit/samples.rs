// SPDX-License-Identifier: LGPL-3.0-or-later
//! Accumulate the fit's small normal equations in bounded MS row batches.

use std::collections::BTreeMap;
use std::ops::Range;

use casa_components::ShapeTransform;
use casa_core::{ArrayValue, Complex32, Complex64};
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::write_session::maximum_visibility_cell_elements;
use casa_ms::{MeasurementSet, MsResult, ResolvedMsSelection, ResolvedMsSelectionRow};
use casa_tables::{SelectedArray1DCells, Table};
use ndarray::{ArrayD, Axis};

use super::{Equations, FitModel, PARAMETERS, invalid};
use crate::model_columns::calibration_channel_span;
const CORRECTED: VisibilityDataColumn = VisibilityDataColumn::CorrectedData;
const WORKSPACE_BYTES: usize = 32 * 1024 * 1024;

pub(super) struct FitSamples<'a> {
    ms: &'a MeasurementSet,
    selected: &'a ResolvedMsSelection,
    frequencies: BTreeMap<i32, Vec<f64>>,
    channels: BTreeMap<i32, Range<usize>>,
    parallel: BTreeMap<i32, Vec<usize>>,
    batch_rows: usize,
}

impl<'a> FitSamples<'a> {
    pub(super) fn new(ms: &'a MeasurementSet, selected: &'a ResolvedMsSelection) -> MsResult<Self> {
        let spectral_windows = ms.spectral_window()?;
        let mut frequencies = BTreeMap::new();
        let mut channels = BTreeMap::new();
        let polarization = ms.polarization()?;
        let mut parallel = BTreeMap::new();
        for row in &selected.selected_rows {
            let window = row.spectral_window_id;
            if let std::collections::btree_map::Entry::Vacant(entry) = frequencies.entry(window) {
                let values = spectral_windows.chan_freq(window as usize)?;
                let indices = selected
                    .channel_selections
                    .get(&window)
                    .and_then(calibration_channel_span)
                    .unwrap_or(0..values.len());
                if indices.end > values.len() {
                    return Err(invalid("channel selection exceeds the spectral window"));
                }
                channels.insert(window, indices);
                entry.insert(values);
            }
            let id = row.polarization_id;
            if let std::collections::btree_map::Entry::Vacant(entry) = parallel.entry(id) {
                let correlations = polarization.corr_type(id as usize)?;
                entry.insert(
                    correlations
                        .iter()
                        .enumerate()
                        .filter_map(|(index, &code)| {
                            matches!(code, 5 | 8 | 9 | 12).then_some(index)
                        })
                        .collect(),
                );
            }
        }
        let cell_elements = maximum_visibility_cell_elements(ms).map_err(invalid)?;
        let bytes_per_row = cell_elements.saturating_mul(16).max(1);
        let batch_rows = (WORKSPACE_BYTES / bytes_per_row).clamp(1, 1024);
        Ok(Self {
            ms,
            selected,
            frequencies,
            channels,
            parallel,
            batch_rows,
        })
    }

    pub(super) fn equations(
        &self,
        model: FitModel,
        vary: &[bool; PARAMETERS],
    ) -> MsResult<Equations> {
        let transform = model.transform()?;
        let mut equations = Equations::default();
        for rows in self.selected.selected_rows.chunks(self.batch_rows) {
            let rows: Vec<_> = rows
                .iter()
                .filter(|row| !row.flag_row && row.antenna1_id != row.antenna2_id)
                .collect();
            if rows.is_empty() {
                continue;
            }
            let batch = self.batch(&rows)?;
            for (index, row) in rows.into_iter().enumerate() {
                let sample = batch.sample(index)?;
                self.accumulate(&mut equations, row, sample, &transform, model, vary)?;
            }
        }
        if equations.sum_weight.is_nan() || equations.sum_weight <= 0.0 {
            return Err(invalid(
                "found no unflagged weighted visibility data to fit",
            ));
        }
        Ok(equations)
    }

    fn batch(&self, rows: &[&ResolvedMsSelectionRow]) -> MsResult<Batch> {
        let indices: Vec<_> = rows.iter().map(|row| row.row_index).collect();
        let main = self.ms.main_table();
        let read = |column| {
            main.column_accessor(column)?
                .array_cells_owned_uncached(&indices)
        };
        Ok(Batch {
            data: read(CORRECTED.name())?,
            flags: read("FLAG")?,
            weights: read("WEIGHT")?,
            uvw: read_uvw(main, &indices)?,
        })
    }

    fn accumulate(
        &self,
        equations: &mut Equations,
        row: &ResolvedMsSelectionRow,
        sample: Sample<'_>,
        transform: &ShapeTransform,
        model: FitModel,
        vary: &[bool; PARAMETERS],
    ) -> MsResult<()> {
        let window = &self.frequencies[&row.spectral_window_id];
        let parallel = &self.parallel[&row.polarization_id];
        if sample.flags.shape() != sample.data.shape() || sample.data.shape()[1] != window.len() {
            return Err(invalid(
                "visibility arrays do not match the MeasurementSet layout",
            ));
        }
        let baseline = transform.at_baseline(sample.uvw);
        for channel in self.channels[&row.spectral_window_id].clone() {
            if sample
                .flags
                .index_axis(Axis(1), channel)
                .iter()
                .any(|&flag| flag)
            {
                continue;
            }
            let frequency = window[channel];
            let shape = baseline.at_frequency(frequency).map_err(invalid)?;
            let (predicted, slopes) = model.derivatives(sample.uvw, frequency, shape);
            for &polarization in parallel {
                let value = sample.data[[polarization, channel]];
                let measured = Complex64::new(f64::from(value.re), f64::from(value.im));
                equations.add(measured - predicted, &slopes, sample.weight, vary);
            }
        }
        Ok(())
    }
}

fn read_uvw(table: &Table, rows: &[usize]) -> MsResult<Vec<[f64; 3]>> {
    let column = table.column_accessor("UVW")?;
    // The typed selected-row reader avoids a whole-column read for IncrementalStMan.
    if let Ok(SelectedArray1DCells::Float64(values)) = column.array_cells_1d_typed_uncached(rows) {
        if values.axis0_count() != 3 {
            return Err(invalid("a visibility fit row has an invalid cell shape"));
        }
        return Ok(values.values().as_chunks::<3>().0.to_vec());
    }

    column
        .array_cells_owned_uncached(rows)?
        .into_iter()
        .map(|value| {
            let Some(ArrayValue::Float64(uvw)) = value else {
                return Err(invalid("UVW is not a Double array"));
            };
            if uvw.ndim() != 1 || uvw.len() != 3 {
                return Err(invalid("a visibility fit row has an invalid cell shape"));
            }
            Ok(std::array::from_fn(|axis| uvw[axis]))
        })
        .collect()
}

struct Batch {
    data: Vec<Option<ArrayValue>>,
    flags: Vec<Option<ArrayValue>>,
    weights: Vec<Option<ArrayValue>>,
    uvw: Vec<[f64; 3]>,
}

impl Batch {
    fn sample(&self, index: usize) -> MsResult<Sample<'_>> {
        let Some(ArrayValue::Complex32(data)) = &self.data[index] else {
            return Err(invalid("there are no corrected visibility samples"));
        };
        let Some(ArrayValue::Bool(flags)) = &self.flags[index] else {
            return Err(invalid("FLAG is not a Bool array"));
        };
        let Some(ArrayValue::Float32(weights)) = &self.weights[index] else {
            return Err(invalid("WEIGHT is not a Float array"));
        };
        if weights.is_empty() || weights.ndim() != 1 || data.ndim() != 2 || flags.ndim() != 2 {
            return Err(invalid("a visibility fit row has an invalid cell shape"));
        }
        let first = weights[[0]];
        let last = weights[[weights.len() - 1]];
        let weight = f64::from((first + last) * 0.5);
        Ok(Sample {
            data,
            flags,
            weight,
            uvw: self.uvw[index],
        })
    }
}

struct Sample<'a> {
    data: &'a ArrayD<Complex32>,
    flags: &'a ArrayD<bool>,
    weight: f64,
    uvw: [f64; 3],
}
