// SPDX-License-Identifier: LGPL-3.0-or-later
//! Plan and reduce synchronized visibility rows onto a combined spectral window.

use std::collections::BTreeMap;
use std::ops::{AddAssign, Div, Mul};

use casa_core::{ArrayValue, ScalarValue, Value};
use casa_tables::{Table, TableError, TableOptions};
use ndarray::{Array1, Array2, Array3, ArrayView2, Ix2, Ix3};

use casa_ms::MeasurementSet;
use casa_ms::{
    CombinedChannel, RegridQuantity, RegridRequest, casa_data_channel_frequencies, combine_windows,
};

use super::channel_average::{
    AveragedStatistics, ChannelAverages, InputStatistic, SpectrumCells, sigma_from_weight,
    weight_from_sigma,
};
use super::correlation_metadata::{CorrelationSelection, select_visibility_axes};
use super::main_arrays::MainArrayBlock;
use super::time_average::{TimeCoordinates, TimeGroup};
use super::time_projection::{TimeGroupRead, TimeProjection};
use super::visibility_columns::VisibilityColumns;
use super::{
    CompactSpectralMetadata, MsTransformError, MsTransformRequest, PreparedTransformInputView,
    STANDARD_SPECTRA, TransformChannelSelection, scalar_bool, scalar_f64, scalar_i32,
    select_channels, transform_flag_category_channels,
};

pub(super) struct Grid {
    pub(super) windows: BTreeMap<i32, usize>,
    pub(super) channels: Vec<CombinedChannel>,
    pub(super) bins: Vec<Vec<usize>>,
    pub(super) frequencies: Vec<f64>,
    pub(super) width: f64,
    interpolation: Vec<LinearSample>,
    legacy_cvel: bool,
}

impl Grid {
    pub(super) fn new(
        input: &MeasurementSet,
        selection: &BTreeMap<i32, TransformChannelSelection>,
        grid_selection: &BTreeMap<i32, TransformChannelSelection>,
        bin_width: usize,
        grid_budget: usize,
        legacy_cvel: bool,
    ) -> Result<Option<Self>, MsTransformError> {
        if grid_selection.len() < 2 {
            return Ok(None);
        }
        let error = MsTransformError::Spectrum;
        let spw = input
            .spectral_window()
            .map_err(|source| error(source.to_string()))?;
        let mut axes = Vec::with_capacity(grid_selection.len());
        let mut first_frame = None;
        let expected = selection
            .values()
            .next()
            .map_or(0, |spw| spw.channels.len());
        for selected in selection.values() {
            let count = selected.channels.len();
            if !legacy_cvel && count != expected {
                return Err(error(format!(
                    "Currently the option 'combinespws' is only supported when the number of channels is the same for all the spw's selected. One of the SPWs selected has {expected} channels, but another selected SPW has {count} channels."
                )));
            }
        }
        for (&id, selected) in grid_selection {
            let row = id as usize;
            let frame = spw
                .reference_frequency_reference(row)
                .map_err(|source| error(source.to_string()))?;
            if frame != *first_frame.get_or_insert(frame) {
                return Err(error(format!(
                    "SPW {id} has a non-matching frequency reference frame."
                )));
            }
            let frequencies = spw
                .chan_freq(row)
                .map_err(|source| error(source.to_string()))?;
            let widths = spw
                .chan_width(row)
                .map_err(|source| error(source.to_string()))?;
            let (centres, spans) = selected
                .channels
                .iter()
                .zip(&selected.metadata_ends)
                .map(|(&first, &last)| {
                    let invalid = || error(format!("SPW {id} has inconsistent channel metadata."));
                    let a = frequencies.get(first).ok_or_else(invalid)?;
                    let b = frequencies.get(last).ok_or_else(invalid)?;
                    let w1 = widths.get(first).ok_or_else(invalid)?;
                    let w2 = widths.get(last).ok_or_else(invalid)?;
                    Ok(((a + b) / 2., (a - b).abs() + (w1.abs() + w2.abs()) / 2.))
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .unzip::<_, _, Vec<_>, Vec<_>>();
            axes.push((centres, spans));
        }
        let pairs = axes
            .iter()
            .map(|(centres, spans)| (centres.as_slice(), spans.as_slice()))
            .collect::<Vec<_>>();
        let channels = combine_windows(&pairs, true).map_err(|source| error(source.to_string()))?;
        let mut bins = Vec::new();
        let mut centres = Vec::new();
        let mut widths = Vec::new();
        for (index, part) in channels.chunks(bin_width).enumerate() {
            let from = index * bin_width;
            bins.push((from..from + part.len()).collect());
            centres.push(part.iter().map(|c| c.frequency).sum::<f64>() / part.len() as f64);
            widths.push(part.iter().map(|c| c.width).sum::<f64>());
        }
        let width = widths[0];
        let start = centres[0];
        let span = centres[centres.len() - 1] + widths[widths.len() - 1] / 2. - start + width / 2.;
        let count = (span / width + 0.01).floor();
        if !(1. ..=f64::from(i32::MAX)).contains(&count) || width <= 0. {
            return Err(error("Combined SPW has invalid channel bounds.".into()));
        }
        admit_grid(count as usize, grid_budget)?;
        let frequencies = casa_data_channel_frequencies(
            &RegridRequest {
                quantity: RegridQuantity::Frequency,
                start: Some(start),
                width: Some(width),
                nchan: None,
                rest_frequency_hz: 0.,
            },
            &centres,
            &widths,
            grid_budget / (std::mem::size_of::<LinearSample>() + 5 * std::mem::size_of::<f64>()),
        )
        .map_err(|source| error(source.to_string()))?
        .centres_hz;
        let interpolation = linear_samples(&centres, &frequencies);
        Ok(Some(Self {
            windows: grid_selection
                .keys()
                .enumerate()
                .map(|(pos, &id)| (id, pos))
                .collect(),
            channels,
            bins,
            frequencies,
            width,
            interpolation,
            legacy_cvel,
        }))
    }

    pub(super) fn interpolate(&self, value: ArrayValue) -> Result<ArrayValue, TableError> {
        match value {
            ArrayValue::Complex32(data) => Ok(ArrayValue::Complex32(
                apply_linear(array_view(&data)?, &self.interpolation).into_dyn(),
            )),
            ArrayValue::Float32(data) => Ok(ArrayValue::Float32(
                apply_linear(array_view(&data)?, &self.interpolation).into_dyn(),
            )),
            ArrayValue::Bool(flags) => {
                let flags = array_view(&flags)?;
                let output = Array2::from_shape_fn(
                    (flags.nrows(), self.interpolation.len()),
                    |(corr, channel)| {
                        let sample = self.interpolation[channel];
                        sample.outside || flags[[corr, sample.left]] || flags[[corr, sample.right]]
                    },
                );
                Ok(ArrayValue::Bool(output.into_dyn()))
            }
            _ => Err(TableError::Schema(
                "Invalid combined spectral cell type.".into(),
            )),
        }
    }

    pub(super) fn interpolate_categories(
        &self,
        value: ArrayValue,
    ) -> Result<ArrayValue, TableError> {
        let values = category_view(&value)?;
        let (correlations, _, categories) = values.dim();
        let output = Array3::from_shape_fn(
            (correlations, self.interpolation.len(), categories),
            |(corr, channel, category)| {
                let sample = self.interpolation[channel];
                values[[corr, sample.left, category]] || values[[corr, sample.right, category]]
            },
        );
        Ok(ArrayValue::Bool(output.into_dyn()))
    }
}

fn admit_grid(channels: usize, available_bytes: usize) -> Result<(), MsTransformError> {
    // Include interpolation, frequency centres and the temporary SPW
    // metadata arrays written for each output channel.
    let bytes = channels
        .saturating_mul(std::mem::size_of::<LinearSample>() + 5 * std::mem::size_of::<f64>());
    if bytes > available_bytes {
        return Err(MsTransformError::Spectrum(format!(
            "Combined SPW of {channels} channels needs {bytes} bytes for the grid, \
             exceeding the {available_bytes} byte grid budget."
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::admit_grid;

    #[test]
    fn combined_grid_checks_its_footprint_before_allocation() {
        assert!(admit_grid(10, 1024).is_ok());
        assert!(
            admit_grid(100, 1024)
                .unwrap_err()
                .to_string()
                .contains("exceeding")
        );
    }
}

#[derive(Clone, Copy)]
struct LinearSample {
    left: usize,
    right: usize,
    fraction: f64,
    outside: bool,
}

fn linear_samples(input: &[f64], output: &[f64]) -> Vec<LinearSample> {
    let last = input.len() - 1;
    let identity = input == output;
    output
        .iter()
        .map(|&frequency| {
            let at = input.partition_point(|value| *value < frequency);
            if last == 0 {
                return LinearSample {
                    left: 0,
                    right: 0,
                    fraction: 0.,
                    outside: true,
                };
            }
            if identity {
                return LinearSample {
                    left: at,
                    right: at,
                    fraction: 0.,
                    outside: false,
                };
            }
            let mut right = at.clamp(1, last);
            let mut left = right - 1;
            let fraction = (frequency - input[left]) / (input[right] - input[left]);
            if fraction <= f64::EPSILON {
                right = left;
            } else if fraction >= 1.0 - f64::EPSILON {
                left = right;
            }
            LinearSample {
                left,
                right,
                fraction,
                outside: frequency < input[0] || frequency > input[last],
            }
        })
        .collect()
}

fn apply_linear<T: Copy + LinearSampleValue>(
    input: ArrayView2<'_, T>,
    samples: &[LinearSample],
) -> Array2<T> {
    Array2::from_shape_fn((input.nrows(), samples.len()), |(corr, channel)| {
        let LinearSample {
            left,
            right,
            fraction,
            ..
        } = samples[channel];
        let a = input[[corr, left]];
        if left == right {
            a
        } else {
            a.interpolate(input[[corr, right]], fraction)
        }
    })
}

trait LinearSampleValue {
    fn interpolate(self, right: Self, fraction: f64) -> Self;
}

impl LinearSampleValue for f32 {
    fn interpolate(self, right: Self, fraction: f64) -> Self {
        // Preserve a small positive interpolated weight near a zero endpoint.
        (f64::from(self) + f64::from(right - self) * fraction) as f32
    }
}

impl LinearSampleValue for num_complex::Complex32 {
    fn interpolate(self, right: Self, fraction: f64) -> Self {
        self + (right - self) * fraction as f32
    }
}

fn array_view<T>(array: &ndarray::ArrayD<T>) -> Result<ArrayView2<'_, T>, TableError> {
    array
        .view()
        .into_dimensionality::<Ix2>()
        .map_err(|_| TableError::Schema("Visibility arrays must be rank-2 [corr, chan].".into()))
}

pub(super) struct Group {
    pub(super) representative: usize,
    pub(super) members: Vec<(usize, usize)>,
}

pub(super) struct Batch {
    pub(super) arrays: MainArrayBlock,
    pub(super) coordinates: Vec<TimeCoordinates>,
    pub(super) exposure_flags: Vec<(f64, bool)>,
}

#[derive(Clone, Copy)]
struct IntegrationTime(f64);

impl PartialEq for IntegrationTime {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for IntegrationTime {}

impl PartialOrd for IntegrationTime {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IntegrationTime {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

pub(super) fn group_rows(
    input: &MeasurementSet,
    rows: &[usize],
    descriptions: &[i32],
    ddid_to_spw: &BTreeMap<i32, i32>,
    windows: &BTreeMap<i32, usize>,
    projection: Option<(&[f64], &[TimeGroup])>,
) -> Result<Vec<Group>, MsTransformError> {
    let error = |source: TableError| MsTransformError::MutateMeasurementSet {
        path: input
            .path()
            .unwrap_or_else(|| std::path::Path::new("<memory>"))
            .display()
            .to_string(),
        source: Box::new(source),
    };
    let table = input.main_table();
    let read = |column| {
        table
            .column_accessor(column)?
            .scalar_cells_owned_for_rows(rows)
    };
    let columns = [
        "OBSERVATION_ID",
        "ARRAY_ID",
        "SCAN_NUMBER",
        "STATE_ID",
        "FIELD_ID",
        "ANTENNA1",
        "ANTENNA2",
        "FEED1",
        "FEED2",
    ];
    let mut keys = vec![[0; 9]; rows.len()];
    for (axis, name) in columns.into_iter().enumerate() {
        let values = read(name).map_err(error)?;
        for ((key, value), &row) in keys.iter_mut().zip(values).zip(rows) {
            key[axis] = scalar_i32(value.as_ref(), name, row)?;
        }
    }
    let times = match projection {
        Some((times, _)) => times.to_vec(),
        None => read("TIME")
            .map_err(error)?
            .iter()
            .zip(rows)
            .map(|(value, &row)| scalar_f64(value.as_ref(), "TIME", row))
            .collect::<Result<Vec<_>, _>>()?,
    };
    let mut grouped = BTreeMap::new();
    let ddi = input
        .data_description()
        .map_err(|source| MsTransformError::Spectrum(source.to_string()))?;
    let mut pol_for_ddi = BTreeMap::new();
    for (pos, (&description, time)) in descriptions.iter().zip(times).enumerate() {
        let pol = match pol_for_ddi.get(&description) {
            Some(&value) => value,
            None => {
                let value = ddi
                    .polarization_id(description as usize)
                    .map_err(|source| MsTransformError::Spectrum(source.to_string()))?;
                pol_for_ddi.insert(description, value);
                value
            }
        };
        let time = IntegrationTime(time);
        let [obs, array, scan, state, field, a1, a2, f1, f2] = keys[pos];
        let group = grouped
            .entry((
                [obs, array, scan, state, field],
                time,
                [a1, a2, f1, f2, pol],
            ))
            .or_insert_with(|| Group {
                representative: pos,
                members: Vec::new(),
            });
        group
            .members
            .push((windows[&ddid_to_spw[&description]], pos));
    }
    if let Some((_, time_groups)) = projection {
        let mut ordered = grouped.into_iter().collect::<Vec<_>>();
        // CASA emits baseline order within each time-averaging buffer, even
        // when the effective averaged epochs in that buffer differ.
        ordered.sort_by_key(|((chunk, time, baseline), group)| {
            (
                *chunk,
                IntegrationTime(time_groups[group.representative].first.time),
                *baseline,
                *time,
            )
        });
        return Ok(ordered.into_iter().map(|(_, group)| group).collect());
    }
    Ok(grouped.into_values().collect())
}

pub(super) fn projected_times(
    input: &MeasurementSet,
    groups: &[TimeGroup],
    channels: &BTreeMap<i32, TransformChannelSelection>,
    ddid_to_spw: &BTreeMap<i32, i32>,
    correlations: Option<&CorrelationSelection>,
    input_view: Option<&dyn PreparedTransformInputView>,
) -> Result<Vec<f64>, MsTransformError> {
    let table_error = |source: TableError| MsTransformError::MutateMeasurementSet {
        path: input
            .path()
            .unwrap_or_else(|| std::path::Path::new("<memory>"))
            .display()
            .to_string(),
        source: Box::new(source),
    };
    let table = input.main_table();
    let flags = table.column_accessor("FLAG").map_err(table_error)?;
    let epoch = |row| {
        casa_ms::subtables::get_f64(table, row, "TIME")
            .map_err(|source| MsTransformError::Spectrum(source.to_string()))
    };
    groups
        .iter()
        .map(|group| {
            let first = group.first.time;
            let mut last = group.rows.last().expect("time groups have rows").physical;
            if group.rows.len() > 1 {
                let ddid = group.first.description;
                let selected = &channels[&ddid_to_spw[&ddid]].channels;
                let axes = correlations.and_then(|plan| plan.axes(ddid));
                // The time accumulator's last sample is the last row with any
                // selected sample unflagged, or the last input when all are flagged.
                // Walk backwards so an ordinary group reads only one FLAG cell.
                for member in group.rows.iter().rev() {
                    let indices = [member.physical];
                    let mut computed = input_view
                        .map(|view| view.read(table, &indices, &["FLAG"]))
                        .transpose()
                        .map_err(table_error)?;
                    let mut values =
                        match computed.as_mut().and_then(|values| values.remove("FLAG")) {
                            Some(values) => values,
                            None => flags
                                .array_cells_owned_uncached(&indices)
                                .map_err(table_error)?,
                        };
                    let Some(ArrayValue::Bool(values)) = values.pop().flatten() else {
                        return Err(table_error(TableError::Schema(
                            "FLAG must hold Bool.".into(),
                        )));
                    };
                    let view = array_view(&values).map_err(table_error)?;
                    let is_usable = |corr| {
                        selected
                            .iter()
                            .any(|&chan| view.get((corr, chan)) == Some(&false))
                    };
                    let usable = match axes {
                        Some(axes) => axes.iter().copied().any(is_usable),
                        None => (0..view.nrows()).any(is_usable),
                    };
                    if usable {
                        last = member.physical;
                        break;
                    }
                }
            }
            Ok(first + (epoch(last)? - first) * 0.5)
        })
        .collect()
}

pub(super) fn read_batch(
    grid: &Grid,
    groups: &[Group],
    input: &MeasurementSet,
    (rows, descriptions): (&[usize], &[i32]),
    (channels, ddid_to_spw, correlations): (
        &BTreeMap<i32, TransformChannelSelection>,
        &BTreeMap<i32, i32>,
        Option<&CorrelationSelection>,
    ),
    (columns, spectra, input_view, continuum): (
        &VisibilityColumns,
        &[&str],
        Option<&dyn PreparedTransformInputView>,
        Option<&super::ContinuumFit>,
    ),
    time: Option<(&mut TimeProjection<'_>, &[TimeGroup])>,
) -> Result<Batch, MsTransformError> {
    let error = |source: TableError| MsTransformError::MutateMeasurementSet {
        path: input
            .path()
            .unwrap_or_else(|| std::path::Path::new("<memory>"))
            .display()
            .to_string(),
        source: Box::new(source),
    };
    let sources = groups
        .iter()
        .flat_map(|group| group.members.iter().map(|&(_, pos)| pos))
        .collect::<Vec<_>>();
    let averaging = time.is_some();
    let (mut raw, times, exposures, mut row_flags) = if let Some((projection, time_groups)) = time {
        let plans = sources
            .iter()
            .map(|&pos| {
                let description = descriptions[pos];
                TimeGroupRead {
                    group: &time_groups[pos],
                    channels: &channels[&ddid_to_spw[&description]].channels,
                    axes: correlations.and_then(|selection| selection.axes(description)),
                    legacy_bins: None,
                }
            })
            .collect::<Vec<_>>();
        let (block, times) = projection.average_groups(&plans)?;
        let exposure = times.iter().map(|time| time.exposure).collect::<Vec<_>>();
        let flags = times.iter().map(|time| time.flag_row).collect::<Vec<_>>();
        (block, times, exposure, flags)
    } else {
        let indices = sources.iter().map(|&index| rows[index]).collect::<Vec<_>>();
        let table = input.main_table();
        let mut block =
            MainArrayBlock::read(table, &indices, columns, spectra, input_view).map_err(error)?;
        let exposure = table
            .column_accessor("EXPOSURE")
            .and_then(|column| column.scalar_cells_owned_for_rows(&indices))
            .map_err(error)?;
        let exposure = exposure
            .iter()
            .zip(&indices)
            .map(|(value, &row)| scalar_f64(value.as_ref(), "EXPOSURE", row))
            .collect::<Result<Vec<_>, _>>()?;
        let flags = table
            .column_accessor("FLAG_ROW")
            .and_then(|column| column.scalar_cells_owned_for_rows(&indices))
            .map_err(error)?
            .iter()
            .zip(&indices)
            .map(|(value, &row)| scalar_bool(value.as_ref(), "FLAG_ROW", row))
            .collect::<Result<Vec<_>, _>>()?;
        let mut spectra_cells = std::mem::take(&mut block.spectra);
        block.spectra = STANDARD_SPECTRA
            .iter()
            .map(|name| {
                spectra
                    .iter()
                    .position(|source| source == name)
                    .map_or_else(
                        || vec![None; indices.len()],
                        |index| std::mem::take(&mut spectra_cells[index]),
                    )
            })
            .collect();
        for (row, &pos) in sources.iter().enumerate() {
            let ddid = descriptions[pos];
            let axes = correlations.and_then(|plan| plan.axes(ddid));
            let channels = &channels[&ddid_to_spw[&ddid]].channels;
            select_row(&mut block, row, columns, axes, channels).map_err(error)?;
        }
        (block, Vec::new(), exposure, flags)
    };
    if let Some(fit) = continuum {
        fit.apply(
            &mut raw,
            columns,
            sources
                .iter()
                .map(|&position| ddid_to_spw[&descriptions[position]]),
            &STANDARD_SPECTRA,
            true,
        )?;
        for (row, flags) in row_flags.iter_mut().zip(&raw.flags) {
            *row = super::continuum::row_flag(flags.as_ref(), None);
        }
    }
    let mut output = MainArrayBlock::for_capacity(groups.len(), columns.copies().len(), 2);
    let mut coordinates = Vec::with_capacity(if averaging { groups.len() } else { 0 });
    let mut exposure_flags = Vec::with_capacity(if averaging { 0 } else { groups.len() });
    let mut start = 0;
    for group in groups {
        let end = start + group.members.len();
        let members = group
            .members
            .iter()
            .zip(start..end)
            .map(|(&(window, _), index)| (window, index))
            .collect::<Vec<_>>();
        let first_exposure = exposures[start];
        let group_exposures = &exposures[start..end];
        let different_exposure = group_exposures
            .iter()
            .any(|exposure| (exposure - first_exposure).abs() > f64::from(f32::EPSILON));
        let exposure = group_exposures.iter().sum::<f64>() / group_exposures.len() as f64;
        let flag_row = row_flags[start..end].iter().all(|&flag| flag);
        reduce_group(
            grid,
            &raw,
            &members,
            columns,
            averaging,
            different_exposure,
            &mut output,
        )
        .map_err(error)?;
        if averaging {
            coordinates.push(TimeCoordinates {
                exposure,
                flag_row,
                ..times[start]
            });
        } else {
            exposure_flags.push((exposure, flag_row));
        }
        start = end;
    }
    Ok(Batch {
        arrays: output,
        coordinates,
        exposure_flags,
    })
}

fn select_row(
    block: &mut MainArrayBlock,
    row: usize,
    columns: &VisibilityColumns,
    axes: Option<&[usize]>,
    channels: &[usize],
) -> Result<(), TableError> {
    let select = |cell: &mut Option<ArrayValue>, name, frequency_axis| -> Result<(), TableError> {
        if let Some(value) = cell.take() {
            let value = select_visibility_axes(value, axes, name)?;
            *cell = Some(if frequency_axis {
                select_channels(value, channels)?
            } else {
                value
            });
        }
        Ok(())
    };
    for (column, cells) in columns.copies().iter().zip(&mut block.visibility) {
        select(
            &mut cells[row],
            column.source.name(),
            column.source != casa_ms::schema::main_table::OptionalMainColumn::LagData,
        )?;
    }
    select(&mut block.flags[row], "FLAG", true)?;
    if let Some(categories) = block.categories[row]
        .take()
        .filter(|value| !value.is_empty())
    {
        let categories = select_visibility_axes(categories, axes, "FLAG_CATEGORY")?;
        let bins = channels
            .iter()
            .map(|&channel| vec![channel])
            .collect::<Vec<_>>();
        block.categories[row] = Some(transform_flag_category_channels(categories, &bins)?);
    }
    select(&mut block.weights[row], "WEIGHT", false)?;
    select(&mut block.sigmas[row], "SIGMA", false)?;
    for (name, cells) in STANDARD_SPECTRA.into_iter().zip(&mut block.spectra) {
        if cells[row].as_ref().is_some_and(ArrayValue::is_empty) {
            cells[row] = None;
        }
        select(&mut cells[row], name, true)?;
    }
    Ok(())
}

fn required_cell<'a>(
    cells: &'a [Option<ArrayValue>],
    row: usize,
    name: &str,
) -> Result<&'a ArrayValue, TableError> {
    cells[row]
        .as_ref()
        .ok_or_else(|| TableError::Schema(format!("Missing {name} while combining SPWs.")))
}

fn reduce_group(
    grid: &Grid,
    input: &MainArrayBlock,
    members: &[(usize, usize)],
    columns: &VisibilityColumns,
    time_averaged: bool,
    different_exposure: bool,
    output: &mut MainArrayBlock,
) -> Result<(), TableError> {
    use casa_ms::schema::main_table::OptionalMainColumn::{
        CorrectedData, Data, FloatData, LagData, ModelData,
    };
    let mut rows_by_window = Vec::with_capacity(grid.windows.len());
    rows_by_window.resize_with(grid.windows.len(), || None);
    let has = |source| {
        columns
            .copies()
            .iter()
            .any(|column| column.source == source)
    };
    let mut common_shape = None;
    for &(window, row) in members {
        let ArrayValue::Bool(flags) = required_cell(&input.flags, row, "FLAG")? else {
            return Err(TableError::Schema("FLAG must hold Bool".into()));
        };
        let flags = array_view(flags)?;
        let shape = [flags.nrows(), flags.ncols()];
        let common = common_shape.get_or_insert(shape);
        if shape[0] != common[0] || (!grid.legacy_cvel && shape[1] != common[1]) {
            return Err(TableError::Schema(
                "SPW combination requires compatible correlation and selected channel axes.".into(),
            ));
        }
        let weight = InputStatistic::from_cells(
            required_cell(&input.weights, row, "WEIGHT")?,
            input.spectra[0][row].as_ref(),
            shape,
            "WEIGHT",
        )?;
        let sigma = InputStatistic::from_cells(
            required_cell(&input.sigmas, row, "SIGMA")?,
            input.spectra[1][row].as_ref(),
            shape,
            "SIGMA",
        )?;
        rows_by_window[window] = Some(Window {
            index: window,
            row,
            flags,
            weight,
            sigma,
        });
    }
    let first = members[0].1;
    let correlations = rows_by_window[members[0].0].as_ref().unwrap().flags.nrows();
    let shape = (correlations, grid.channels.len());
    let calibrated = has(CorrectedData) || !(has(Data) || has(ModelData));
    let observed = has(Data) || (time_averaged && has(FloatData));
    let weight_mode = if (time_averaged && !(observed && has(CorrectedData))) || calibrated {
        Noise::Calibrated
    } else if observed {
        Noise::Observed
    } else {
        Noise::Unit
    };
    let sigma_mode = if observed && has(CorrectedData) {
        Noise::Observed
    } else {
        weight_mode
    };
    let contributions = Contributors::new(grid, &rows_by_window, shape);
    let different_exposure = different_exposure && !grid.legacy_cvel;
    for (i, copy) in columns.copies().iter().enumerate() {
        let cells = &input.visibility[i];
        let result = match copy.source {
            LagData => {
                let original = &cells[first];
                if members.iter().any(|&(_, row)| &cells[row] != original) {
                    return Err(TableError::Schema(
                        "Cannot combine different LAG_DATA cells: their lag axes have no frequency mapping."
                            .into(),
                    ));
                }
                original.clone()
            }
            FloatData => Some(ArrayValue::Float32(
                contributions
                    .data(cells, weight_mode, different_exposure)?
                    .into_dyn(),
            )),
            _ => {
                let mode = match copy.source {
                    Data => Noise::Observed,
                    CorrectedData => Noise::Calibrated,
                    _ => weight_mode,
                };
                Some(ArrayValue::Complex32(
                    contributions
                        .data(cells, mode, different_exposure)?
                        .into_dyn(),
                ))
            }
        };
        output.visibility[i].push(result);
    }
    let masks = contributions.flags();
    let (weight, sigma) = if grid.legacy_cvel {
        // SubMS combines persisted spectra with the same overlap fractions as DATA.
        let spectrum = |slot: usize, noise| {
            input.spectra[slot][first]
                .as_ref()
                .map(|_| ArrayValue::Float32(contributions.noise(noise).into_dyn()))
        };
        (spectrum(0, Noise::Calibrated), spectrum(1, Noise::Sigma))
    } else {
        let weight = contributions.noise(weight_mode);
        let sigma = if weight_mode == sigma_mode {
            weight.mapv(sigma_from_weight)
        } else {
            contributions.noise(sigma_mode).mapv(sigma_from_weight)
        };
        (
            Some(ArrayValue::Float32(weight.into_dyn())),
            Some(ArrayValue::Float32(sigma.into_dyn())),
        )
    };
    output.flags.push(Some(ArrayValue::Bool(masks.into_dyn())));
    output.categories.push(combine_categories(
        grid,
        input,
        &rows_by_window,
        correlations,
    )?);
    output.uvw.push(input.uvw[first].clone());
    output.weights.push(input.weights[first].clone());
    output.sigmas.push(input.sigmas[first].clone());
    output.spectra[0].push(weight);
    output.spectra[1].push(sigma);
    Ok(())
}

fn category_view(value: &ArrayValue) -> Result<ndarray::ArrayView3<'_, bool>, TableError> {
    if let ArrayValue::Bool(values) = value
        && let Ok(view) = values.view().into_dimensionality::<Ix3>()
    {
        return Ok(view);
    }
    Err(TableError::Schema(
        "FLAG_CATEGORY must hold rank-3 Bool cells.".into(),
    ))
}

fn combine_categories(
    grid: &Grid,
    input: &MainArrayBlock,
    windows: &[Option<Window<'_>>],
    correlations: usize,
) -> Result<Option<ArrayValue>, TableError> {
    let views = windows
        .iter()
        .map(|window| {
            window
                .as_ref()
                .and_then(|window| input.categories[window.row].as_ref())
                .filter(|cell| !cell.is_empty())
                .map(category_view)
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let Some(categories) = views.iter().flatten().next().map(|view| view.dim().2) else {
        return Ok(None);
    };
    for (view, window) in views.iter().zip(windows) {
        if let (Some(view), Some(window)) = (view, window) {
            let (corr, channels, count) = view.dim();
            if corr != correlations
                || count != categories
                || (channels != 1 && channels != window.flags.dim().1)
            {
                return Err(TableError::Schema(
                    "FLAG_CATEGORY shapes disagree when combining spectral windows.".into(),
                ));
            }
        }
    }
    let output = Array3::from_shape_fn(
        (correlations, grid.channels.len(), categories),
        |(corr, channel, category)| {
            grid.channels[channel].sources.iter().any(|source| {
                views[source.window].is_some_and(|view| {
                    view[[corr, source.channel.min(view.dim().1 - 1), category]]
                })
            })
        },
    );
    Ok(Some(ArrayValue::Bool(output.into_dyn())))
}

struct Window<'a> {
    index: usize,
    row: usize,
    flags: ArrayView2<'a, bool>,
    weight: InputStatistic<'a>,
    sigma: InputStatistic<'a>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Noise {
    Calibrated,
    Observed,
    Sigma,
    Unit,
}

impl Window<'_> {
    fn noise(&self, kind: Noise, corr: usize, channel: usize) -> f32 {
        match kind {
            Noise::Calibrated => self.weight.value(corr, channel),
            Noise::Observed => weight_from_sigma(self.sigma.value(corr, channel)),
            Noise::Sigma => self.sigma.value(corr, channel),
            Noise::Unit => 1.0,
        }
    }
}

struct Contributors<'a> {
    grid: &'a Grid,
    windows: &'a [Option<Window<'a>>],
    shape: (usize, usize),
}

impl<'a> Contributors<'a> {
    fn new(grid: &'a Grid, windows: &'a [Option<Window<'a>>], shape: (usize, usize)) -> Self {
        Self {
            grid,
            windows,
            shape,
        }
    }

    fn for_sample(
        &self,
        corr: usize,
        channel: usize,
        mut add: impl FnMut(&Window<'a>, usize, f64),
    ) -> f64 {
        let sources = &self.grid.channels[channel].sources;
        let has_full = sources
            .iter()
            .any(|source| source.fraction >= 1. && self.windows[source.window].is_some());
        let mut total = 0.;
        for source in sources {
            let Some(window) = self.windows[source.window].as_ref() else {
                continue;
            };
            if window.flags[[corr, source.channel]] {
                continue;
            }
            if source.fraction < 1. && has_full {
                let valid_partials = sources
                    .iter()
                    .filter(|other| {
                        other.window == source.window
                            && other.fraction < 1.
                            && !window.flags[[corr, other.channel]]
                    })
                    .count();
                if valid_partials % 2 != 0 {
                    continue;
                }
            }
            total += source.fraction;
            add(window, source.channel, source.fraction);
        }
        total
    }

    fn flags(&self) -> Array2<bool> {
        Array2::from_shape_fn(self.shape, |(corr, channel)| {
            self.for_sample(corr, channel, |_, _, _| {}) < 0.999999
        })
    }

    fn noise(&self, kind: Noise) -> Array2<f32> {
        Array2::from_shape_fn(self.shape, |(corr, channel)| {
            let mut sum = 0.;
            let total = self.for_sample(corr, channel, |window, input, fraction| {
                sum += fraction * f64::from(window.noise(kind, corr, input));
            });
            if total >= 0.999999 || (self.grid.legacy_cvel && total > 0.) {
                (sum / total) as f32
            } else {
                sum as f32
            }
        })
    }

    fn data<T>(
        &self,
        cells: &'a [Option<ArrayValue>],
        kind: Noise,
        different_exposure: bool,
    ) -> Result<Array2<T>, TableError>
    where
        T: VisibilityNumber,
    {
        let views = self
            .windows
            .iter()
            .map(|window| {
                window
                    .as_ref()
                    .map(|w| T::view(cells[w.row].as_ref()))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Array2::from_shape_fn(self.shape, |(corr, channel)| {
            let mut sum = T::default();
            let mut normalization = 0.;
            self.for_sample(corr, channel, |window, input, fraction| {
                let weight = if different_exposure {
                    fraction * f64::from(window.noise(kind, corr, input))
                } else {
                    fraction
                };
                sum += views[window.index].unwrap()[[corr, input]] * weight as f32;
                normalization += weight;
            });
            if normalization > 0. {
                sum / normalization as f32
            } else {
                sum
            }
        }))
    }
}

trait VisibilityNumber:
    Copy + Default + AddAssign + Mul<f32, Output = Self> + Div<f32, Output = Self>
{
    fn view(value: Option<&ArrayValue>) -> Result<ArrayView2<'_, Self>, TableError>;
}

impl VisibilityNumber for f32 {
    fn view(value: Option<&ArrayValue>) -> Result<ArrayView2<'_, Self>, TableError> {
        match value {
            Some(ArrayValue::Float32(cell)) => array_view(cell),
            _ => Err(TableError::Schema(
                "FLOAT_DATA must be defined for SPW combination.".into(),
            )),
        }
    }
}

impl VisibilityNumber for num_complex::Complex32 {
    fn view(value: Option<&ArrayValue>) -> Result<ArrayView2<'_, Self>, TableError> {
        match value {
            Some(ArrayValue::Complex32(cell)) => array_view(cell),
            _ => Err(TableError::Schema(
                "Complex data must be defined for SPW combination.".into(),
            )),
        }
    }
}

pub(super) fn finish_noise(
    grid: &Grid,
    statistics: Option<AveragedStatistics>,
    averages: Option<&ChannelAverages<'_>>,
    spectra: SpectrumCells<'_>,
    flags: &ArrayValue,
) -> Result<AveragedStatistics, TableError> {
    let statistics = statistics.or_else(|| averages.map(ChannelAverages::statistics));
    let (weight, sigma) = match statistics {
        Some(values) => (values.weight_spectrum, values.sigma_spectrum),
        None => {
            let missing = || TableError::Schema("Missing combined SPW noise spectrum.".into());
            (
                spectra.weight.ok_or_else(missing)?.clone(),
                spectra.sigma.ok_or_else(missing)?.clone(),
            )
        }
    };
    let ArrayValue::Float32(sigma) = sigma else {
        return Err(TableError::Schema(
            "SIGMA_SPECTRUM must have Float32 cells.".into(),
        ));
    };
    let sigma_weights = ArrayValue::Float32(sigma.mapv(weight_from_sigma));
    let transformed_sigma = match grid.interpolate(sigma_weights)? {
        ArrayValue::Float32(values) => ArrayValue::Float32(values.mapv(sigma_from_weight)),
        _ => unreachable!("a float spectrum stays float"),
    };
    let weights = grid.interpolate(weight)?;
    let flags = grid.interpolate(flags.clone())?;
    let ArrayValue::Bool(flags) = flags else {
        return Err(TableError::Schema("FLAG must have Bool cells.".into()));
    };
    let flags = array_view(&flags)?;
    Ok(AveragedStatistics {
        weight: mean_noise(&weights, flags)?,
        sigma: mean_noise(&transformed_sigma, flags)?,
        weight_spectrum: weights,
        sigma_spectrum: transformed_sigma,
    })
}

pub(super) fn mean_noise(
    data: &ArrayValue,
    masks: ArrayView2<'_, bool>,
) -> Result<ArrayValue, TableError> {
    let ArrayValue::Float32(data) = data else {
        return Err(TableError::Schema(
            "Noise spectra must have Float32 cells.".into(),
        ));
    };
    let data = array_view(data)?;
    Ok(ArrayValue::Float32(
        Array1::from_shape_fn(data.nrows(), |corr| {
            let row = data.row(corr);
            let flags = masks.row(corr);
            let all_flagged = flags.iter().all(|&flag| flag);
            let mut count = 0;
            let mut sum = 0.;
            for (&value, &flag) in row.iter().zip(flags) {
                if flag == all_flagged {
                    sum += value;
                    count += 1;
                }
            }
            sum / count.max(1) as f32
        })
        .into_dyn(),
    ))
}

pub(super) fn write_metadata(
    request: &MsTransformRequest,
    grid: &Grid,
    metadata: &mut CompactSpectralMetadata,
) -> Result<(), MsTransformError> {
    let error = |source: TableError| MsTransformError::MutateMeasurementSet {
        path: request.output_ms.display().to_string(),
        source: Box::new(source),
    };
    let mut spw =
        Table::open(TableOptions::new(request.output_ms.join("SPECTRAL_WINDOW"))).map_err(error)?;
    spw.remove_rows(&(1..spw.row_count()).collect::<Vec<_>>())
        .map_err(error)?;
    let channels = grid.frequencies.len();
    for (name, values) in [
        ("CHAN_FREQ", grid.frequencies.clone()),
        ("CHAN_WIDTH", vec![grid.width; channels]),
        ("EFFECTIVE_BW", vec![grid.width; channels]),
        ("RESOLUTION", vec![grid.width; channels]),
    ] {
        spw.column_accessor_mut(name)
            .and_then(|mut column| {
                column.set(
                    0,
                    Value::Array(ArrayValue::Float64(Array1::from_vec(values).into_dyn())),
                )
            })
            .map_err(error)?;
    }
    for (name, scalar) in [
        ("NUM_CHAN", ScalarValue::Int32(channels as i32)),
        ("REF_FREQUENCY", ScalarValue::Float64(grid.frequencies[0])),
        (
            "TOTAL_BANDWIDTH",
            ScalarValue::Float64(grid.width * channels as f64),
        ),
    ] {
        spw.column_accessor_mut(name)
            .and_then(|mut column| column.set(0, Value::Scalar(scalar)))
            .map_err(error)?;
    }
    for (name, empty) in [
        ("ASSOC_SPW_ID", ArrayValue::from_i32_vec(Vec::new())),
        ("ASSOC_NATURE", ArrayValue::from_string_vec(Vec::new())),
    ] {
        if spw
            .schema()
            .is_some_and(|schema| schema.contains_column(name))
        {
            spw.column_accessor_mut(name)
                .and_then(|mut column| column.set(0, Value::Array(empty)))
                .map_err(error)?;
        }
    }
    spw.prepare_write()
        .save(TableOptions::new(request.output_ms.join("SPECTRAL_WINDOW")))
        .map_err(error)?;
    for id in metadata.spw_map.values_mut() {
        *id = 0;
    }

    let path = request.output_ms.join("DATA_DESCRIPTION");
    let mut descriptions = Table::open(TableOptions::new(&path)).map_err(error)?;
    let mut pol_rows = BTreeMap::new();
    let mut mapping = BTreeMap::new();
    let mut remove = Vec::new();
    for row in 0..descriptions.row_count() {
        let pol = casa_ms::subtables::get_i32(&descriptions, row, "POLARIZATION_ID")
            .map_err(|source| MsTransformError::Spectrum(source.to_string()))?;
        let next = pol_rows.len() as i32;
        let output = *pol_rows.entry(pol).or_insert(next);
        mapping.insert(row as i32, output);
        if output < next {
            remove.push(row);
        } else {
            descriptions
                .column_accessor_mut("SPECTRAL_WINDOW_ID")
                .and_then(|mut column| column.set(row, Value::Scalar(ScalarValue::Int32(0))))
                .map_err(error)?;
        }
    }
    descriptions.remove_rows(&remove).map_err(error)?;
    descriptions
        .prepare_write()
        .save(TableOptions::new(path))
        .map_err(error)?;
    for id in metadata.ddid_map.values_mut() {
        *id = mapping[id];
    }
    Ok(())
}

pub(super) fn remap_subtable_references(
    request: &MsTransformRequest,
) -> Result<(), MsTransformError> {
    use casa_ms::subtables::{get_f64, get_i32};
    for (table, keys) in [
        ("SOURCE", &["SOURCE_ID"][..]),
        ("FEED", &["ANTENNA_ID", "FEED_ID", "TIME"]),
        ("SYSCAL", &["ANTENNA_ID", "FEED_ID", "TIME"]),
        ("CALDEVICE", &["ANTENNA_ID", "FEED_ID", "TIME"]),
        ("SYSPOWER", &["ANTENNA_ID", "FEED_ID", "TIME"]),
        ("FREQ_OFFSET", &["ANTENNA1", "ANTENNA2", "FEED_ID", "TIME"]),
    ] {
        let path = request.output_ms.join(table);
        if !path.exists() {
            continue;
        }
        let error = |source: TableError| MsTransformError::MutateMeasurementSet {
            path: path.display().to_string(),
            source: Box::new(source),
        };
        let subtable = Table::open(TableOptions::new(&path)).map_err(error)?;
        let mut records = std::collections::BTreeSet::new();
        let mut keep = Vec::new();
        for row in 0..subtable.row_count() {
            let key = keys
                .iter()
                .map(|&column| {
                    let value = if column == "TIME" {
                        get_f64(&subtable, row, column).map(f64::to_bits)
                    } else {
                        get_i32(&subtable, row, column).map(|value| value as u64)
                    };
                    value.map_err(|source| MsTransformError::Spectrum(source.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if records.insert(key) {
                keep.push(row);
            }
        }
        if keep.is_empty() {
            continue;
        }
        drop(subtable);
        let original = request.output_ms.join(format!(".{table}.combine-original"));
        let io_error = |source: std::io::Error| MsTransformError::PrepareOutput {
            path: path.display().to_string(),
            reason: source.to_string(),
        };
        std::fs::rename(&path, &original).map_err(io_error)?;
        super::rewrite_selected_table_rows(&original, &path, &keep, |_, mut row| {
            row.upsert("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0)));
            Ok(row)
        })?;
        std::fs::remove_dir_all(original).map_err(io_error)?;
    }
    Ok(())
}
