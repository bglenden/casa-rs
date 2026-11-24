// SPDX-License-Identifier: LGPL-3.0-or-later
//! Baseline and visibility accumulation for MeasurementSet time averaging.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use casa_tables::TableError;
use casa_types::ArrayValue;

use crate::MeasurementSet;
use crate::selection::ResolvedMsSelectionRow;

use super::selection_metadata::MainRowOrder;
use super::{MsTransformError, TimeAverageOptions, scalar_i32};

/// The columns used by the visibility iterator to establish a chunk.
/// A dimension selected by `timespan` is zeroed before building this key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct TimeChunk([i32; 8]);

/// Row metadata needed to decide the averaging group before reading visibility arrays.
#[derive(Clone, Copy, Debug)]
pub(super) struct TimeSample {
    pub(super) row: usize,
    pub(super) description: i32,
    pub(super) chunk: TimeChunk,
    pub(super) time: f64,
    pub(super) antenna_pair: [i32; 2],
    pub(super) uvw: [f64; 3],
}

#[derive(Clone, Copy, Debug)]
pub(super) struct TimeInputRow {
    pub(super) physical: usize,
    pub(super) buffer_first: usize,
}

/// A baseline accumulation, anchored on the first selected physical row.
#[derive(Debug)]
pub(super) struct TimeGroup {
    pub(super) first: TimeSample,
    pub(super) rows: Vec<TimeInputRow>,
    pub(super) row_in_chunk: usize,
}

impl TimeGroup {
    fn new(first: TimeSample, row: TimeInputRow) -> Self {
        Self {
            first,
            rows: vec![row],
            row_in_chunk: 0,
        }
    }

    fn exceeds(&self, sample: &TimeSample, max_time: f64, max_uvw_squared: f64) -> bool {
        if sample.time - self.first.time > max_time {
            return true;
        }
        if max_uvw_squared.is_infinite() {
            return false;
        }
        let displacement = sample
            .uvw
            .iter()
            .zip(self.first.uvw)
            .map(|(&current, first)| (current - first).powi(2))
            .sum::<f64>();
        displacement > max_uvw_squared
    }
}

/// Project the scalar grouping keys and optionally the input baseline motion.
pub(super) fn plan(
    input: &MeasurementSet,
    selected: &[ResolvedMsSelectionRow],
    options: TimeAverageOptions,
    order: MainRowOrder,
) -> Result<Vec<TimeGroup>, MsTransformError> {
    let input_path = input.path().unwrap_or_else(|| Path::new("<memory>"));
    let table_error = |source| MsTransformError::MutateMeasurementSet {
        path: input_path.display().to_string(),
        source: Box::new(source),
    };
    let span_states = options.span_states
        || (options.interval_seconds > 30.0
            && input
                .observation()
                .ok()
                .and_then(|obs| obs.string(0, "TELESCOPE_NAME").ok())
                .is_some_and(|name| name.contains("ALMA")));
    let rows = selected.iter().map(|row| row.row_index).collect::<Vec<_>>();
    let baseline = matches!(order, MainRowOrder::Baseline);
    let beam_order = matches!(order, MainRowOrder::SingleDish | MainRowOrder::Atmosphere);
    let mut samples = selected
        .iter()
        .map(|row| TimeSample {
            row: row.row_index,
            description: row.data_desc_id,
            chunk: TimeChunk([
                0,
                0,
                0,
                0,
                0,
                0,
                if options.span_fields { 0 } else { row.field_id },
                row.data_desc_id,
            ]),
            time: row.time_mjd_seconds,
            antenna_pair: [row.antenna1_id, row.antenna2_id],
            uvw: [0.0; 3],
        })
        .collect::<Vec<_>>();
    let table = input.main_table();
    let keys = [
        ("OBSERVATION_ID", !baseline),
        ("ARRAY_ID", !baseline),
        ("ANTENNA1", matches!(order, MainRowOrder::SingleDish)),
        ("FEED1", beam_order),
        ("SCAN_NUMBER", !options.span_scans),
        ("STATE_ID", !span_states),
    ];
    for (slot, (name, barrier)) in keys.into_iter().enumerate() {
        if !barrier {
            continue;
        }
        let cells = table
            .column_accessor(name)
            .and_then(|column| column.scalar_cells_owned_for_rows(&rows))
            .map_err(table_error)?;
        for (sample, cell) in samples.iter_mut().zip(cells) {
            sample.chunk.0[slot] = scalar_i32(cell.as_ref(), name, sample.row)?;
        }
    }
    if baseline {
        // Single-dish line fitting puts DATA_DESC_ID before scan, state and field.
        for sample in &mut samples {
            sample.chunk.0.rotate_right(1);
        }
    }
    if options.max_uvw_distance > 0.0 {
        let column = table.column_accessor("UVW").map_err(table_error)?;
        for (rows, samples) in rows.chunks(4096).zip(samples.chunks_mut(4096)) {
            let uvw = column
                .array_cells_owned_uncached(rows)
                .map_err(table_error)?;
            for (sample, uvw) in samples.iter_mut().zip(uvw) {
                sample.uvw = uvw_coordinates(uvw.as_ref()).ok_or_else(|| {
                    table_error(TableError::Schema(format!(
                        "UVW row {} is not a Float64 vector of length three",
                        sample.row
                    )))
                })?;
            }
        }
    }
    Ok(group_baselines(
        samples,
        options.interval_seconds,
        options.max_uvw_distance,
    ))
}

pub(super) fn uvw_coordinates(uvw: Option<&ArrayValue>) -> Option<[f64; 3]> {
    let ArrayValue::Float64(uvw) = uvw? else {
        return None;
    };
    if uvw.shape() != [3] {
        return None;
    }
    Some([uvw[0], uvw[1], uvw[2]])
}

/// The selected physical rows and chronological anchors of PlotMS time groups.
pub(crate) struct PlotTimeGroups {
    group_by_row_slot: Vec<usize>,
    first_row_slots: Vec<usize>,
}

impl PlotTimeGroups {
    /// Return the time group for a row's position in the original selection.
    pub(crate) fn group(&self, row_slot: usize) -> usize {
        self.group_by_row_slot[row_slot]
    }

    /// Return the first chronological integration supplying the group's metadata.
    pub(crate) fn first_row_slot(&self, group: usize) -> usize {
        self.first_row_slots[group]
    }
}

/// Plan vector time groups for the selected PlotMS rows and scan/field barriers.
pub(crate) fn selected_plot_time_groups(
    ms: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
    options: TimeAverageOptions,
) -> Result<PlotTimeGroups, MsTransformError> {
    let groups = plan(ms, rows, options, MainRowOrder::Transform)?;
    let mut group_by_row = HashMap::with_capacity(rows.len());
    let mut first_rows = Vec::with_capacity(groups.len());
    for (index, group) in groups.into_iter().enumerate() {
        first_rows.push(group.first.row);
        group_by_row.extend(group.rows.into_iter().map(|row| (row.physical, index)));
    }
    let mut first_row_slots = vec![0; first_rows.len()];
    let group_by_row_slot = rows
        .iter()
        .enumerate()
        .map(|(row_slot, row)| {
            let group = group_by_row[&row.row_index];
            if first_rows[group] == row.row_index {
                first_row_slots[group] = row_slot;
            }
            group
        })
        .collect();
    Ok(PlotTimeGroups {
        group_by_row_slot,
        first_row_slots,
    })
}

pub(super) fn group_baselines(
    mut samples: Vec<TimeSample>,
    timebin: f64,
    max_uvw_distance: f64,
) -> Vec<TimeGroup> {
    samples.sort_unstable_by(|left, right| {
        left.chunk
            .cmp(&right.chunk)
            .then_with(|| left.time.total_cmp(&right.time))
            .then_with(|| left.row.cmp(&right.row))
    });
    let max_time = 0.999 * timebin;
    let max_uvw_squared = if max_uvw_distance > 0.0 {
        max_uvw_distance.powi(2)
    } else {
        f64::INFINITY
    };
    let mut finished = Vec::new();
    let mut baselines = BTreeMap::<[i32; 2], usize>::new();
    let mut active: Vec<TimeGroup> = Vec::new();
    let mut chunk = None;
    let mut buffer_start: Option<TimeSample> = None;
    for sample in samples {
        if chunk != Some(sample.chunk) {
            finished.append(&mut active);
            baselines.clear();
            chunk = Some(sample.chunk);
        }
        let first = buffer_start
            .filter(|start| start.chunk == sample.chunk && start.time == sample.time)
            .unwrap_or(sample);
        buffer_start = Some(first);
        let row = TimeInputRow {
            physical: sample.row,
            buffer_first: first.row,
        };
        let slot = *baselines.entry(sample.antenna_pair).or_insert(active.len());
        if slot == active.len() {
            active.push(TimeGroup::new(sample, row));
        } else if active[slot].exceeds(&sample, max_time, max_uvw_squared) {
            finished.push(std::mem::replace(
                &mut active[slot],
                TimeGroup::new(sample, row),
            ));
        } else {
            active[slot].rows.push(row);
        }
    }
    finished.append(&mut active);
    let mut previous = None;
    let mut row = 0;
    for group in &mut finished {
        if previous != Some(group.first.chunk) {
            previous = Some(group.first.chunk);
            row = 0;
        }
        group.row_in_chunk = row;
        row += 1;
    }
    finished
}

/// Row-level values consumed and emitted independently of the visibility cube.
#[derive(Clone, Copy, Debug)]
pub(super) struct TimeCoordinates {
    pub(super) time: f64,
    pub(super) centroid: f64,
    pub(super) interval: f64,
    pub(super) exposure: f64,
    pub(super) uvw: [f64; 3],
    pub(super) flag_row: bool,
}

/// CASA measures the group from the first selected baseline time. Row-level
/// sums prefer unflagged rows and are reset when the first such row arrives.
pub(super) struct TimeCoordinatesAverage {
    first: TimeCoordinates,
    last: TimeCoordinates,
    count: usize,
    flagged: bool,
    weight: f64,
    exposure: f64,
    centroid_offset: f64,
    uvw: [f64; 3],
}

impl TimeCoordinatesAverage {
    pub(super) fn new(first: TimeCoordinates) -> Self {
        Self {
            first,
            last: first,
            count: 0,
            flagged: true,
            weight: 0.0,
            exposure: 0.0,
            centroid_offset: 0.0,
            uvw: [0.0; 3],
        }
    }

    pub(super) fn add(&mut self, sample: TimeCoordinates, scalar_weight: f64) {
        if !self.flagged && sample.flag_row {
            return;
        }
        if self.flagged && !sample.flag_row {
            *self = Self::new(self.first);
            self.flagged = false;
        }
        let weight = if self.flagged { 1.0 } else { scalar_weight };
        self.count += 1;
        self.weight += weight;
        self.exposure += sample.exposure;
        self.centroid_offset += (sample.centroid - self.first.time) * weight;
        for (accumulator, coordinate) in self.uvw.iter_mut().zip(sample.uvw) {
            *accumulator += coordinate * weight;
        }
        self.last = sample;
    }

    pub(super) fn finish(self) -> TimeCoordinates {
        let elapsed = self.last.time - self.first.time;
        let interval = if elapsed == 0.0 {
            self.first.interval
        } else {
            elapsed + (self.first.interval + self.last.interval) * 0.5
        };
        let normalization = if self.weight == 0.0 {
            self.count.max(1) as f64
        } else {
            self.weight
        };
        TimeCoordinates {
            time: self.first.time + elapsed * 0.5,
            centroid: self.first.time + self.centroid_offset / normalization,
            interval,
            exposure: self.exposure,
            uvw: self.uvw.map(|coordinate| coordinate / normalization),
            // The time iterator uses its row flag during accumulation and
            // clears it on output. The selected cell flags carry through.
            flag_row: false,
        }
    }
}

use ndarray::{Array1, Array2, ArrayD, ArrayView2, Ix2};
use num_complex::Complex32;
use std::ops::{AddAssign, Div, Mul};

use crate::schema::main_table::OptionalMainColumn;

use super::channel_average::{InputStatistic, SpectrumCells, sigma_from_weight, weight_from_sigma};
use super::visibility_columns::VisibilityColumns;

#[derive(Clone, Copy)]
enum TimeWeight {
    Observed,
    Calibrated,
    Unit,
}

struct TimeColumn {
    physical: OptionalMainColumn,
    weight: TimeWeight,
    sum: Option<ArrayValue>,
}

#[derive(Clone, Copy)]
struct SampleUpdate {
    include: bool,
    reset: bool,
    weights: [f32; 3],
}

/// Streaming accumulator for one baseline and a selected correlation/channel grid.
pub(super) struct TimeVisibilityAverage {
    shape: (usize, usize),
    columns: Vec<TimeColumn>,
    flags: Array2<bool>,
    sum_weights: Array2<[f32; 3]>,
    observed: bool,
    corrected: bool,
    floating: bool,
    lag_only: bool,
    lags: Option<LagVisibilityAverage>,
}

/// LAG_DATA has no channel correspondence. A whole correlation is usable
/// when any of its selected frequency flags are clear.
struct LagVisibilityAverage {
    sum: Array2<Complex32>,
    weights: Vec<f32>,
    flagged: Vec<bool>,
}

impl LagVisibilityAverage {
    fn new(shape: (usize, usize)) -> Self {
        Self {
            sum: Array2::zeros(shape),
            weights: vec![0.0; shape.0],
            flagged: vec![true; shape.0],
        }
    }

    fn add(
        &mut self,
        values: ArrayView2<'_, Complex32>,
        flags: &ArrayView2<'_, bool>,
        weight: &InputStatistic<'_>,
    ) -> Result<(), TableError> {
        if self.sum.dim() != values.dim() || values.nrows() != flags.nrows() {
            return Err(TableError::Schema(
                "LAG_DATA shapes differ during time averaging".to_string(),
            ));
        }
        for correlation in 0..values.nrows() {
            let flagged = flags.row(correlation).iter().all(|&flag| flag);
            if !self.flagged[correlation] && flagged {
                continue;
            }
            let mut sum = self.sum.row_mut(correlation);
            if self.flagged[correlation] && !flagged {
                self.weights[correlation] = 0.0;
                self.flagged[correlation] = false;
                sum.fill(Complex32::default());
            }
            let weight = weight.value(correlation, 0);
            self.weights[correlation] += weight;
            for (sum, &value) in sum.iter_mut().zip(values.row(correlation)) {
                *sum += value * weight;
            }
        }
        Ok(())
    }

    fn finish(mut self) -> ArrayValue {
        for (mut row, weight) in self.sum.rows_mut().into_iter().zip(self.weights) {
            if weight > 0.0 {
                row.mapv_inplace(|value| value / weight);
            }
        }
        ArrayValue::Complex32(self.sum.into_dyn())
    }
}

pub(super) struct TimeVisibilityRow<'a> {
    pub(super) values: &'a [Option<ArrayValue>],
    pub(super) flags: &'a ArrayValue,
    pub(super) weight: &'a ArrayValue,
    pub(super) sigma: &'a ArrayValue,
    pub(super) spectra: SpectrumCells<'a>,
}

pub(super) struct AveragedTimeVisibilities {
    pub(super) values: Vec<Option<ArrayValue>>,
    pub(super) flags: ArrayValue,
    pub(super) weight: ArrayValue,
    pub(super) sigma: ArrayValue,
    pub(super) weight_spectrum: ArrayValue,
    pub(super) sigma_spectrum: ArrayValue,
}

impl TimeVisibilityAverage {
    pub(super) fn new(columns: &VisibilityColumns, shape: (usize, usize)) -> Self {
        use OptionalMainColumn::{CorrectedData, Data, FloatData, LagData, ModelData};
        let has = |name| columns.copies().iter().any(|copy| copy.source == name);
        let observed = has(Data);
        let corrected = has(CorrectedData);
        let floating = has(FloatData);
        let lag_only = columns.copies().len() == 1 && has(LagData);
        let columns = columns
            .copies()
            .iter()
            .map(|copy| {
                let weight = match copy.source {
                    Data | FloatData => TimeWeight::Observed,
                    CorrectedData => TimeWeight::Calibrated,
                    ModelData if corrected => TimeWeight::Calibrated,
                    ModelData if observed => TimeWeight::Observed,
                    _ => TimeWeight::Unit,
                };
                let sum = match copy.source {
                    LagData => None,
                    FloatData => Some(ArrayValue::Float32(Array2::zeros(shape).into_dyn())),
                    _ => Some(ArrayValue::Complex32(Array2::zeros(shape).into_dyn())),
                };
                TimeColumn {
                    physical: copy.source,
                    weight,
                    sum,
                }
            })
            .collect();
        Self {
            shape,
            columns,
            flags: Array2::from_elem(shape, true),
            sum_weights: Array2::from_elem(shape, [0.; 3]),
            observed,
            corrected,
            floating,
            lag_only,
            lags: None,
        }
    }

    /// Return the row-level flag and weight used to average the time centroid and UVW.
    pub(super) fn add(&mut self, row: TimeVisibilityRow<'_>) -> Result<(bool, f64), TableError> {
        let ArrayValue::Bool(flags) = row.flags else {
            return Err(TableError::Schema("FLAG must hold Bool".to_string()));
        };
        let flags = flags
            .view()
            .into_dimensionality::<Ix2>()
            .ok()
            .filter(|flags| flags.dim() == self.shape)
            .ok_or_else(|| {
                TableError::Schema("flags do not match the time averaging grid".to_string())
            })?;
        let shape = [self.shape.0, self.shape.1];
        let weight = InputStatistic::from_cells(row.weight, row.spectra.weight, shape, "WEIGHT")?;
        let sigma = InputStatistic::from_cells(row.sigma, row.spectra.sigma, shape, "SIGMA")?;
        let changes = Array2::from_shape_fn(self.shape, |(corr, chan)| {
            let i = [corr, chan];
            let flagged = flags[i];
            let was_flagged = self.flags[i];
            let include = was_flagged || !flagged;
            let reset = was_flagged && !flagged;
            let weights = [
                weight_from_sigma(sigma.value(corr, chan)),
                weight.value(corr, chan),
                1.0,
            ];
            if include {
                self.flags[i] = flagged;
                if reset {
                    self.sum_weights[i] = weights;
                } else {
                    for (sum, value) in self.sum_weights[i].iter_mut().zip(weights) {
                        *sum += value;
                    }
                }
            }
            SampleUpdate {
                include,
                reset,
                weights,
            }
        });
        for (column, value) in self.columns.iter_mut().zip(row.values) {
            if column.physical == OptionalMainColumn::LagData {
                if let Some(value) = value.as_ref().filter(|value| !value.is_empty()) {
                    let ArrayValue::Complex32(values) = value else {
                        return Err(TableError::Schema(
                            "LAG_DATA must hold Complex32".to_string(),
                        ));
                    };
                    let lags = values.view().into_dimensionality::<Ix2>().map_err(|_| {
                        TableError::Schema(
                            "LAG_DATA must have correlation and lag axes".to_string(),
                        )
                    })?;
                    let row_weight_values =
                        InputStatistic::from_cells(row.weight, None, shape, "WEIGHT")?;
                    self.lags
                        .get_or_insert_with(|| LagVisibilityAverage::new(lags.dim()))
                        .add(lags, &flags, &row_weight_values)?;
                }
                continue;
            }
            match (column.sum.as_mut(), value.as_ref()) {
                (Some(ArrayValue::Complex32(sum)), Some(ArrayValue::Complex32(value))) => {
                    add_visibility(sum, value, &changes, column.weight)?;
                }
                (Some(ArrayValue::Float32(sum)), Some(ArrayValue::Float32(value))) => {
                    add_visibility(sum, value, &changes, column.weight)?;
                }
                _ => {
                    return Err(TableError::Schema(format!(
                        "missing or invalid {} time sample",
                        column.physical.name()
                    )));
                }
            }
        }
        let adjusted_weight = if self.corrected {
            row_weight(row.weight, |weight| weight)
        } else if self.observed || self.floating {
            row_weight(row.sigma, weight_from_sigma)
        } else {
            self.shape.0 as f64
        };
        Ok((flags.iter().all(|&flag| flag), adjusted_weight))
    }

    pub(super) fn finish(mut self) -> AveragedTimeVisibilities {
        let output_weight = if self.corrected || self.lag_only {
            TimeWeight::Calibrated
        } else if self.observed || self.floating {
            TimeWeight::Observed
        } else {
            TimeWeight::Unit
        };
        let output_variance = if (self.observed || self.floating) && self.corrected {
            TimeWeight::Observed
        } else {
            output_weight
        };
        let weight = self.sum_weights.map(|sum| sum[output_weight as usize]);
        let sigma = self
            .sum_weights
            .map(|sum| sigma_from_weight(sum[output_variance as usize]));
        let scalar_weight = self.mean_weight(output_weight);
        let scalar_sigma = self.mean_weight(output_variance).mapv(sigma_from_weight);
        let values = self
            .columns
            .into_iter()
            .map(|column| {
                if column.physical == OptionalMainColumn::LagData {
                    return self.lags.take().map(LagVisibilityAverage::finish);
                }
                column.sum.map(|mut value| {
                    match &mut value {
                        ArrayValue::Complex32(array) => {
                            finish_visibility(array, &self.sum_weights, column.weight, false);
                        }
                        ArrayValue::Float32(array) => {
                            finish_visibility(array, &self.sum_weights, column.weight, true)
                        }
                        _ => {}
                    }
                    value
                })
            })
            .collect();
        AveragedTimeVisibilities {
            values,
            flags: ArrayValue::Bool(self.flags.into_dyn()),
            weight: ArrayValue::Float32(scalar_weight.into_dyn()),
            sigma: ArrayValue::Float32(scalar_sigma.into_dyn()),
            weight_spectrum: ArrayValue::Float32(weight.into_dyn()),
            sigma_spectrum: ArrayValue::Float32(sigma.into_dyn()),
        }
    }

    fn mean_weight(&self, role: TimeWeight) -> Array1<f32> {
        Array1::from_shape_fn(self.shape.0, |corr| {
            let flags = self.flags.row(corr);
            let all_flagged = flags.iter().all(|&flag| flag);
            let mut sum = 0.0;
            let mut count = 0;
            for (&flag, weights) in flags.iter().zip(self.sum_weights.row(corr)) {
                if flag == all_flagged {
                    sum += weights[role as usize];
                    count += 1;
                }
            }
            sum / count.max(1) as f32
        })
    }
}

fn row_weight(value: &ArrayValue, transform: impl Fn(f32) -> f32) -> f64 {
    match value {
        ArrayValue::Float32(values) => values
            .iter()
            .map(|&value| f64::from(transform(value)))
            .sum(),
        _ => 0.0,
    }
}

fn add_visibility<T>(
    sum: &mut ArrayD<T>,
    input: &ArrayD<T>,
    sample: &Array2<SampleUpdate>,
    weight: TimeWeight,
) -> Result<(), TableError>
where
    T: Copy + AddAssign + Mul<f32, Output = T>,
{
    if sum.shape() != input.shape() || sum.shape() != sample.shape() {
        return Err(TableError::Schema(
            "visibility and FLAG time-averaging grids differ".to_string(),
        ));
    }
    for ((output, &input), sample) in sum.iter_mut().zip(input).zip(sample) {
        if sample.include {
            let contribution = input * sample.weights[weight as usize];
            if sample.reset {
                *output = contribution;
            } else {
                *output += contribution;
            }
        }
    }
    Ok(())
}

fn finish_visibility<T>(
    sum: &mut ArrayD<T>,
    weights: &Array2<[f32; 3]>,
    role: TimeWeight,
    divide_by_zero: bool,
) where
    T: Copy + Div<f32, Output = T>,
{
    for (value, weights) in sum.iter_mut().zip(weights) {
        let normalization = weights[role as usize];
        if normalization > 0.0 || divide_by_zero {
            *value = *value / normalization;
        }
    }
}
