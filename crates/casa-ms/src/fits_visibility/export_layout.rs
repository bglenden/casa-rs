// SPDX-License-Identifier: LGPL-3.0-or-later
//! Select and order the rows and frequency axes of a random-groups export.

use std::collections::{BTreeMap, BTreeSet};

use casa_core::{ArrayValue, ScalarValue};
use casa_tables::Table;

use super::invalid;
use super::uvfits_export::UvFitsExportOptions;
use crate::selection::MsSelectionIndices;
use crate::{MeasurementSet, MsError, MsResult, SubtableId};

pub(super) struct OutputLayout {
    pub(super) rows: Vec<VisibilityRow>,
    pub(super) groups: Vec<OutputGroup>,
    pub(super) windows: Vec<OutputWindow>,
    pub(super) fields: BTreeMap<i32, i32>,
    pub(super) field: i32,
    pub(super) correlations: Vec<(i32, usize)>,
    pub(super) input_channels: usize,
    pub(super) channels: Vec<usize>,
    pub(super) reference: f64,
    pub(super) pixel: usize,
    pub(super) step: f64,
    pub(super) frame: i32,
    pub(super) combine: bool,
    pub(super) multi: bool,
}

pub(super) struct VisibilityRow {
    pub(super) index: usize,
    pub(super) time: f64,
    pub(super) antenna: [i32; 2],
    pub(super) field: i32,
    pub(super) window: usize,
    description: i32,
}

pub(super) struct OutputGroup {
    pub(super) first: usize,
    pub(super) slots: Vec<Option<usize>>,
}

pub(super) struct OutputWindow {
    pub(super) id: i32,
    pub(super) frequency: f64,
    pub(super) increment: f64,
    pub(super) bandwidth: f64,
    pub(super) sideband: i32,
}

impl OutputLayout {
    pub(super) fn resolve(
        ms: &MeasurementSet,
        options: &UvFitsExportOptions<'_>,
    ) -> MsResult<Self> {
        let mut indices = MsSelectionIndices::default();
        let mut selector = options.selection.clone();
        selector.spw.get_or_insert_with(|| "*".to_owned());
        let predicate = ms.parse_ms_tool_selection(&selector, &mut indices)?;
        if !options.multisource && indices.field.len() > 1 {
            return Err(invalid(
                "If multisource is false, no more than one field should be specified",
            ));
        }
        let field = indices.field.first().copied().unwrap_or(0);
        let selected = predicate.select_rows(ms)?;
        let selected = if selected.is_empty() {
            (0..ms.row_count()).collect::<Vec<_>>()
        } else {
            selected
        };
        if selected.is_empty() {
            return Err(invalid("Cannot write UVFITS from an empty MeasurementSet"));
        }
        let descriptions = subtable(ms, SubtableId::DataDescription)?;
        let (rows, used) = load_rows(ms.main_table(), descriptions, &selected)?;
        let fields = rows
            .iter()
            .map(|row| row.field)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .zip(1..)
            .collect::<BTreeMap<_, _>>();
        let multi = options.multisource || fields.len() > 1;
        let spectral = SpectralLayout::resolve(ms, &used, indices.channel.first().copied())?;
        let windows_by_id = spectral
            .windows
            .iter()
            .enumerate()
            .map(|(index, window)| (window.id, index))
            .collect::<BTreeMap<_, _>>();
        let mut rows = rows;
        for row in &mut rows {
            row.window = windows_by_id
                [&i32_cell(descriptions, row.description as usize, "SPECTRAL_WINDOW_ID")?];
        }
        rows.sort_by(|a, b| {
            a.time
                .total_cmp(&b.time)
                .then(a.antenna.cmp(&b.antenna))
                .then(a.field.cmp(&b.field))
                .then(a.description.cmp(&b.description))
        });
        let combine = options.combinespw;
        let groups = group_rows(
            &rows,
            ms.main_table(),
            spectral.windows.len(),
            combine,
            multi,
            options.padwithflags,
        )?;
        Ok(Self {
            rows,
            groups,
            windows: spectral.windows,
            fields,
            field,
            correlations: spectral.correlations,
            input_channels: spectral.input_channels,
            channels: spectral.channels,
            reference: spectral.reference,
            pixel: spectral.pixel,
            step: spectral.step,
            frame: spectral.frame,
            combine,
            multi,
        })
    }
}

fn load_rows(
    table: &Table,
    descriptions: &Table,
    selected: &[usize],
) -> MsResult<(Vec<VisibilityRow>, BTreeSet<i32>)> {
    let mut used = BTreeSet::new();
    let mut rows = Vec::with_capacity(selected.len());
    for &index in selected {
        let description = i32_cell(table, index, "DATA_DESC_ID")?;
        if description < 0 || description as usize >= descriptions.row_count() {
            return Err(invalid(format!("Invalid DATA_DESC_ID {description}")));
        }
        used.insert(description);
        rows.push(VisibilityRow {
            index,
            time: f64_cell(table, index, "TIME_CENTROID")?,
            antenna: [
                i32_cell(table, index, "ANTENNA1")?,
                i32_cell(table, index, "ANTENNA2")?,
            ],
            field: i32_cell(table, index, "FIELD_ID")?,
            window: 0,
            description,
        });
    }
    Ok((rows, used))
}

fn group_rows(
    rows: &[VisibilityRow],
    table: &Table,
    windows: usize,
    combine: bool,
    multi: bool,
    pad: bool,
) -> MsResult<Vec<OutputGroup>> {
    if !combine || windows == 1 {
        return Ok((0..rows.len())
            .map(|index| OutputGroup {
                first: index,
                slots: vec![Some(index)],
            })
            .collect());
    }
    let mut groups = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        let first = &rows[start];
        let limit = first.time + 0.2 * f64_cell(table, first.index, "INTERVAL")?;
        let mut slots = vec![None; windows];
        let length = rows[start..]
            .iter()
            .take(windows)
            .take_while(|row| {
                row.time <= limit
                    && row.antenna == first.antenna
                    && (!multi || row.field == first.field)
            })
            .count()
            .max(1);
        for (index, row) in rows.iter().enumerate().skip(start).take(length) {
            if slots[row.window].replace(index).is_some() {
                return Err(invalid("Duplicate spectral window in a UVFITS integration"));
            }
        }
        if !pad && slots.iter().any(Option::is_none) {
            return Err(invalid(
                "Spectral windows cannot be combined without padwithflags",
            ));
        }
        groups.push(OutputGroup {
            first: start,
            slots,
        });
        start += length;
    }
    Ok(groups)
}

struct SpectralLayout {
    windows: Vec<OutputWindow>,
    correlations: Vec<(i32, usize)>,
    input_channels: usize,
    channels: Vec<usize>,
    reference: f64,
    pixel: usize,
    step: f64,
    frame: i32,
}

impl SpectralLayout {
    fn resolve(
        ms: &MeasurementSet,
        used: &BTreeSet<i32>,
        channel: Option<[i32; 4]>,
    ) -> MsResult<Self> {
        let descriptions = subtable(ms, SubtableId::DataDescription)?;
        let spectral = subtable(ms, SubtableId::SpectralWindow)?;
        let polarization = subtable(ms, SubtableId::Polarization)?;
        let mut windows = BTreeMap::new();
        let mut shape = None;
        for &id in used {
            let window_id = i32_cell(descriptions, id as usize, "SPECTRAL_WINDOW_ID")?;
            let pol_id = i32_cell(descriptions, id as usize, "POLARIZATION_ID")?;
            let ArrayValue::Int32(stokes) = polarization
                .cell_accessor(pol_id as usize, "CORR_TYPE")?
                .array()?
            else {
                return Err(invalid("Invalid MeasurementSet correlation types"));
            };
            let stokes = stokes.iter().copied().collect::<Vec<_>>();
            let ArrayValue::Float64(frequencies) = spectral
                .cell_accessor(window_id as usize, "CHAN_FREQ")?
                .array()?
            else {
                return Err(invalid("Invalid MeasurementSet channel frequencies"));
            };
            let bandwidth = f64_cell(spectral, window_id as usize, "TOTAL_BANDWIDTH")?;
            let frequencies = frequencies.iter().copied().collect::<Vec<_>>();
            let first = *frequencies
                .first()
                .ok_or_else(|| invalid("No spectral channels"))?;
            let delta = frequencies.get(1).map_or(bandwidth, |next| next - first);
            let frame = i32_cell(spectral, window_id as usize, "MEAS_FREQ_REF")?;
            let initial = shape.get_or_insert_with(|| AxisShape {
                channels: select_channels(frequencies.len(), channel),
                count: frequencies.len(),
                frame,
                stokes: stokes.clone(),
                width: delta,
            });
            if initial.count != frequencies.len()
                || initial.stokes != stokes
                || initial.frame != frame
                || !near(initial.width.abs(), delta.abs())
            {
                return Err(invalid(
                    "Spectral windows have incompatible channel, correlation or frequency axes",
                ));
            }
            let channels = &initial.channels;
            let step = channels.get(1).map_or(delta, |index| {
                frequencies[*index] - frequencies[channels[0]]
            });
            if channels
                .windows(2)
                .any(|pair| !near(frequencies[pair[1]] - frequencies[pair[0]], step))
            {
                return Err(invalid("Channel width varies across the band"));
            }
            let middle = channels.len() / 2;
            windows.insert(
                window_id,
                OutputWindow {
                    id: window_id,
                    frequency: frequencies[channels[middle]],
                    increment: step,
                    bandwidth,
                    sideband: i32_cell(spectral, window_id as usize, "NET_SIDEBAND")?,
                },
            );
        }
        let shape = shape.ok_or_else(|| invalid("No selected spectral windows"))?;
        let windows = windows.into_values().collect::<Vec<_>>();
        let reference = windows[0].frequency;
        let step = windows[0].increment;
        let pixel = shape.channels.len() / 2 + 1;
        Ok(Self {
            windows,
            correlations: fits_correlations(&shape.stokes)?,
            input_channels: shape.count,
            channels: shape.channels,
            reference,
            pixel,
            step,
            frame: shape.frame,
        })
    }
}

struct AxisShape {
    channels: Vec<usize>,
    count: usize,
    frame: i32,
    stokes: Vec<i32>,
    width: f64,
}

fn near(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1.0e-5 * left.abs().max(right.abs())
}

fn select_channels(count: usize, selection: Option<[i32; 4]>) -> Vec<usize> {
    if let Some([_, first, last, stride]) = selection {
        let mut stride = stride.max(1);
        let mut length = (last - first + 1) / stride;
        if length < 1 {
            length = 1;
            stride = 1;
        }
        if first >= 0 && i64::from(first) + i64::from(length) * i64::from(stride) <= count as i64 {
            return (0..length)
                .map(|index| (first + index * stride) as usize)
                .collect();
        }
    }
    (0..count).collect()
}

fn fits_correlations(stokes: &[i32]) -> MsResult<Vec<(i32, usize)>> {
    let mut correlations = stokes
        .iter()
        .enumerate()
        .map(|(index, &value)| {
            let fits = match value {
                1..=4 => value,
                5 => -1,
                6 => -3,
                7 => -4,
                8 => -2,
                9 => -5,
                10 => -7,
                11 => -8,
                12 => -6,
                _ => {
                    return Err(invalid(format!(
                        "Stokes code {value} cannot be written to UVFITS"
                    )));
                }
            };
            Ok((fits, index))
        })
        .collect::<MsResult<Vec<_>>>()?;
    let first = correlations
        .first()
        .ok_or_else(|| invalid("No spectral correlations"))?
        .0;
    correlations.sort_unstable_by_key(|(code, _)| if first >= 0 { *code } else { -*code });
    if correlations
        .windows(3)
        .any(|values| values[2].0 - values[1].0 != values[1].0 - values[0].0)
    {
        return Err(invalid(
            "The selected Stokes types cannot be represented on a FITS axis",
        ));
    }
    Ok(correlations)
}

pub(super) fn i32_cell(table: &Table, row: usize, column: &str) -> MsResult<i32> {
    match table.cell_accessor(row, column)?.scalar()? {
        ScalarValue::Int32(value) => Ok(*value),
        _ => Err(invalid(format!("Invalid integer column {column}"))),
    }
}

pub(super) fn f64_cell(table: &Table, row: usize, column: &str) -> MsResult<f64> {
    match table.cell_accessor(row, column)?.scalar()? {
        ScalarValue::Float64(value) => Ok(*value),
        _ => Err(invalid(format!("Invalid floating-point column {column}"))),
    }
}

pub(super) fn subtable(ms: &MeasurementSet, id: SubtableId) -> MsResult<&Table> {
    ms.subtable(id)
        .ok_or_else(|| MsError::MissingSubtable(id.name().to_owned()))
}

pub(super) fn has_column(table: &Table, name: &str) -> bool {
    table
        .schema()
        .is_some_and(|schema| schema.contains_column(name))
}
