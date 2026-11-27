// SPDX-License-Identifier: LGPL-3.0-or-later
//! Select and group only the calibration rows required by an analysis query.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use casa_ms::selection::{
    AntennaRow, DataDescriptionRow, FieldSelectorRow, SpectralWindowRow, resolve_antenna_lists,
    resolve_field_selector_rows, resolve_spw_selector,
};

use super::*;
use crate::constants::COL_SPECTRAL_WINDOW_ID;

mod fit;
mod series;

pub use fit::CalAnalysisFit;

/// CASA's common selection and conversion arguments for `get` and `fit`.
#[derive(Debug, Clone, Copy)]
pub struct CalAnalysisQuery<'a> {
    /// Field expression.
    pub field: &'a str,
    /// Antenna expression.
    pub antenna: &'a str,
    /// Inclusive time limits in seconds, if supplied.
    pub time_range: Option<[f64; 2]>,
    /// Spectral-window and channel expression.
    pub spw: &'a str,
    /// Comma-separated feed names.
    pub feed: &'a str,
    /// Which axis identifies each returned series.
    pub axis: &'a str,
    /// Complex component to expose.
    pub amplitude_or_phase: &'a str,
    /// Normalize each amplitude series.
    pub normalize: bool,
    /// Unwrap each phase series.
    pub unwrap: bool,
    /// Maximum phase jump in radians.
    pub jump_max: f64,
}

struct WindowMetadata {
    name: String,
    frequencies: Vec<f64>,
    widths: Vec<f64>,
    reference: f64,
    bandwidth: f64,
    flagged: bool,
}

#[derive(Clone, Copy)]
struct SelectedRow {
    index: usize,
    window: usize,
    time: f64,
}

struct SelectedGroup {
    baseline: (i32, i32, i32),
    rows: Vec<SelectedRow>,
}

struct SelectedQuery {
    table: Table,
    groups: Vec<SelectedGroup>,
    windows: Vec<WindowMetadata>,
    channels: BTreeMap<usize, BTreeSet<usize>>,
    feeds: Vec<usize>,
}

impl CalAnalysis {
    fn select(&self, request: CalAnalysisQuery<'_>) -> Result<SelectedQuery, String> {
        let path = Path::new(&self.name);
        let fields = field_ids(path, expression(request.field))?;
        let (first, mut second) = antenna_ids(path, expression(request.antenna))?;
        if self.second_antennas.first() == Some(&-1) {
            second = vec![-1];
        }
        if first.is_empty() || second.is_empty() {
            return Err("Invalid antenna selection".into());
        }
        let windows = read_windows(path)?;
        let channels = channels(&windows, expression(request.spw))?;
        let feeds = self.feed_indices(request.feed)?;
        let times = request.time_range.unwrap_or([
            self.times.first().copied().unwrap_or_default(),
            self.times.last().copied().unwrap_or_default(),
        ]);
        let table = open_table(path)?;
        let groups = select_rows(&table, &fields, &first, &second, times, &channels)?;
        Ok(SelectedQuery {
            table,
            groups,
            windows,
            channels,
            feeds,
        })
    }

    fn feed_indices(&self, expression: &str) -> Result<Vec<usize>, String> {
        let expression = expression.trim();
        if expression.is_empty() {
            return Ok((0..self.feeds.len()).collect());
        }
        expression
            .split(',')
            .map(|name| {
                let name = name.trim().to_ascii_uppercase();
                self.feeds
                    .iter()
                    .position(|feed| *feed == name)
                    .ok_or_else(|| format!("Invalid feed {name}"))
            })
            .collect::<Result<BTreeSet<_>, _>>()
            .map(|indices| indices.into_iter().collect())
    }
}

fn expression(value: &str) -> &str {
    let value = value.trim();
    if value.is_empty() { "*" } else { value }
}

fn field_ids(path: &Path, expression: &str) -> Result<Vec<i32>, String> {
    let table = open_table(&path.join("FIELD"))?;
    let names = text_column(&table, "NAME")?;
    let codes = text_column(&table, "CODE")?;
    let flags = flag_column(&table)?;
    let fields = names
        .iter()
        .zip(&codes)
        .zip(flags)
        .map(|((name, code), flagged)| FieldSelectorRow {
            name,
            code: Some(code),
            flagged,
        })
        .collect::<Vec<_>>();
    resolve_field_selector_rows(&fields, expression).map_err(|error| error.to_string())
}

fn antenna_ids(path: &Path, expression: &str) -> Result<(Vec<i32>, Vec<i32>), String> {
    let table = open_table(&path.join("ANTENNA"))?;
    let names = text_column(&table, "NAME")?;
    let stations = text_column(&table, "STATION")?;
    let positions = real_array_column(&table, "POSITION")?;
    let antennas = names
        .iter()
        .zip(&stations)
        .zip(positions)
        .map(|((name, station), position)| {
            Ok(AntennaRow {
                name,
                station,
                position_m: position
                    .try_into()
                    .map_err(|_| String::from("ANTENNA POSITION requires three coordinates"))?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    resolve_antenna_lists(&antennas, expression).map_err(|error| error.to_string())
}

fn read_windows(path: &Path) -> Result<Vec<WindowMetadata>, String> {
    let table = open_table(&path.join("SPECTRAL_WINDOW"))?;
    let names = text_column(&table, "NAME")?;
    let freqs = read_frequencies(&table)?;
    let widths = real_array_column(&table, "CHAN_WIDTH")?;
    let pivots = float_column(&table, "REF_FREQUENCY")?;
    let bands = float_column(&table, "TOTAL_BANDWIDTH")?;
    let flags = flag_column(&table)?;
    Ok(names
        .into_iter()
        .zip(freqs)
        .zip(widths)
        .zip(pivots)
        .zip(bands)
        .zip(flags)
        .map(
            |(((((name, frequencies), widths), reference), bandwidth), flagged)| WindowMetadata {
                name,
                frequencies,
                widths,
                reference,
                bandwidth,
                flagged,
            },
        )
        .collect())
}

fn channels(
    windows: &[WindowMetadata],
    expression: &str,
) -> Result<BTreeMap<usize, BTreeSet<usize>>, String> {
    let desc = windows
        .iter()
        .enumerate()
        .map(|(index, _)| DataDescriptionRow {
            spectral_window_id: index as i32,
            flag_row: false,
        })
        .collect::<Vec<_>>();
    let rows = windows
        .iter()
        .map(|window| SpectralWindowRow {
            name: &window.name,
            chan_freq_hz: &window.frequencies,
            chan_width_hz: &window.widths,
            ref_frequency_hz: window.reference,
            total_bandwidth_hz: window.bandwidth,
            flag_row: window.flagged,
        })
        .collect::<Vec<_>>();
    let selection =
        resolve_spw_selector(&rows, &desc, expression).map_err(|error| error.to_string())?;
    let mut channels = BTreeMap::<usize, BTreeSet<usize>>::new();
    for range in selection.channel_list() {
        if let Some(indices) = range.channels() {
            let count = windows[range.spw_id].frequencies.len();
            channels
                .entry(range.spw_id)
                .or_default()
                .extend(indices.filter(|&channel| channel < count));
        }
    }
    channels.retain(|_, indices| !indices.is_empty());
    Ok(channels)
}

fn select_rows(
    table: &Table,
    fields: &[i32],
    first: &[i32],
    second: &[i32],
    time: [f64; 2],
    channels: &BTreeMap<usize, BTreeSet<usize>>,
) -> Result<Vec<SelectedGroup>, String> {
    let field_ids = int_column(table, COL_FIELD_ID)?;
    let antenna1 = int_column(table, COL_ANTENNA1)?;
    let antenna2 = int_column(table, COL_ANTENNA2)?;
    let spw_ids = int_column(table, COL_SPECTRAL_WINDOW_ID)?;
    let times = float_column(table, COL_TIME)?;
    let mut positions = HashMap::new();
    let mut groups = Vec::<SelectedGroup>::new();
    for index in 0..table.row_count() {
        let (field, p, q) = (field_ids[index], antenna1[index], antenna2[index]);
        let window = spw_ids[index] as usize;
        let instant = times[index];
        if !fields.contains(&field)
            || !first.contains(&p)
            || !second.contains(&q)
            || !(time[0]..=time[1]).contains(&instant)
            || !channels.contains_key(&window)
        {
            continue;
        }
        let baseline = (field, p, q);
        let group = *positions.entry(baseline).or_insert_with(|| {
            groups.push(SelectedGroup {
                baseline,
                rows: Vec::new(),
            });
            groups.len() - 1
        });
        groups[group].rows.push(SelectedRow {
            index,
            window,
            time: instant,
        });
    }
    Ok(groups)
}

fn text_column(table: &Table, column: &str) -> Result<Vec<String>, String> {
    read_scalars(table, column, |value| match value {
        ScalarValue::String(value) => Some(value),
        _ => None,
    })
}

fn flag_column(table: &Table) -> Result<Vec<bool>, String> {
    read_scalars(table, "FLAG_ROW", |value| match value {
        ScalarValue::Bool(value) => Some(value),
        _ => None,
    })
}

fn int_column(table: &Table, column: &str) -> Result<Vec<i32>, String> {
    read_scalars(table, column, |value| match value {
        ScalarValue::Int32(value) => Some(value),
        _ => None,
    })
}

fn float_column(table: &Table, column: &str) -> Result<Vec<f64>, String> {
    read_scalars(table, column, |value| match value {
        ScalarValue::Float64(value) => Some(value),
        _ => None,
    })
}

fn real_array_column(table: &Table, column: &str) -> Result<Vec<Vec<f64>>, String> {
    let rows = (0..table.row_count()).collect::<Vec<_>>();
    table
        .column_accessor(column)
        .and_then(|reader| reader.array_cells_owned(&rows))
        .map_err(|error| error.to_string())?
        .into_iter()
        .enumerate()
        .map(|(row, cell)| match cell {
            Some(ArrayValue::Float64(values)) if values.ndim() == 1 => {
                Ok(values.into_iter().collect())
            }
            _ => Err(format!("missing or invalid {column} at row {row}")),
        })
        .collect()
}
