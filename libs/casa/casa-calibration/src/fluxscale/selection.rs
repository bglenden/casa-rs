// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration-row selections using the shared CASA selector grammars.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;

use casa_core::ArrayValue;
use casa_ms::selection::syntax::parse_numeric_row_id_ranges;
use casa_ms::selection::{
    AntennaRow, TimeDefaults, TimeSelection, TimeSelector, TimeSelectorDefaults,
    resolve_antenna_selector,
};
use casa_tables::{Table, TableOptions};

use super::{FluxScaleError, FluxScaleOptions, GainSampleRow};

pub(super) struct SampleMasks {
    /// Rows visited by CTIter before partial per-antenna suppression.
    pub(super) logical: Vec<bool>,
    pub(super) data: Vec<bool>,
    pub(super) threshold: Vec<bool>,
}

impl SampleMasks {
    pub(super) fn all(count: usize) -> Self {
        Self {
            logical: vec![true; count],
            data: vec![true; count],
            threshold: vec![true; count],
        }
    }
}

fn invalid(error: impl std::fmt::Display) -> FluxScaleError {
    FluxScaleError::UnsupportedInput {
        path: "fluxscale selection".into(),
        reason: error.to_string(),
    }
}

pub(super) fn sample_masks(
    root: &Path,
    rows: &[GainSampleRow],
    options: &FluxScaleOptions,
    fields: &BTreeSet<i32>,
) -> Result<SampleMasks, FluxScaleError> {
    if options.antenna.is_empty() && options.timerange.is_empty() && options.scan.is_empty() {
        let mut masks = SampleMasks::all(rows.len());
        for (index, row) in rows.iter().enumerate() {
            masks.data[index] = fields.contains(&row.field_id);
            masks.threshold[index] = masks.data[index];
            masks.logical[index] = masks.data[index];
        }
        return Ok(masks);
    }
    let partial = options.antenna.starts_with('!')
        && (!options.timerange.is_empty() || !options.scan.is_empty());
    let antenna = if partial {
        options.antenna.trim_start_matches('!')
    } else {
        &options.antenna
    };
    let antenna_mask = antenna_mask(root, rows, antenna)?;
    let scan_ranges = if options.scan.is_empty() {
        None
    } else {
        Some(parse_numeric_row_id_ranges(&options.scan, "scan").map_err(invalid)?)
    };
    let logical = rows
        .iter()
        .zip(&antenna_mask)
        .map(|(row, antenna)| {
            *antenna
                && (partial || fields.contains(&row.field_id))
                && scan_ranges.as_ref().is_none_or(|ranges| {
                    ranges
                        .iter()
                        .any(|(a, b)| (*a..=*b).contains(&row.scan_number))
                })
        })
        .collect::<Vec<_>>();
    let selector = TimeSelector::parse(&options.timerange).map_err(invalid)?;
    let time = time_selection(rows, logical.iter().position(|value| *value), &selector)?;
    let selected = rows
        .iter()
        .zip(&logical)
        .map(|(row, logical)| *logical && time.holds(row.time_seconds))
        .collect::<Vec<_>>();
    if partial {
        // CASA's !antenna + time/scan mode restricts only ANTENNA1 IDs and
        // exact times actually present in the positive baseline selection.
        let mut antennas = HashSet::new();
        let mut times = HashSet::new();
        for (row, selected) in rows.iter().zip(&selected) {
            if *selected {
                antennas.insert(row.antenna_id);
                times.insert(row.time_seconds.to_bits());
            }
        }
        let data = rows
            .iter()
            .map(|row| {
                fields.contains(&row.field_id)
                    && (times.is_empty()
                        || !antennas.contains(&row.antenna_id)
                        || times.contains(&row.time_seconds.to_bits()))
            })
            .collect::<Vec<_>>();
        return Ok(SampleMasks {
            logical: rows
                .iter()
                .map(|row| fields.contains(&row.field_id))
                .collect(),
            threshold: data.clone(),
            data,
        });
    }
    // The upstream median pass ignores time/scan when antenna is empty.
    let threshold = if options.antenna.is_empty() {
        rows.iter()
            .map(|row| fields.contains(&row.field_id))
            .collect()
    } else {
        let mut first = BTreeMap::new();
        for (index, (row, logical)) in rows.iter().zip(&logical).enumerate() {
            if *logical {
                first.entry((row.field_id, row.spw_id)).or_insert(index);
            }
        }
        let times = first
            .into_iter()
            .map(|(key, index)| Ok((key, time_selection(rows, Some(index), &selector)?)))
            .collect::<Result<BTreeMap<_, _>, FluxScaleError>>()?;
        rows.iter()
            .zip(&logical)
            .map(|(row, logical)| {
                *logical
                    && times
                        .get(&(row.field_id, row.spw_id))
                        .is_some_and(|time| time.holds(row.time_seconds))
            })
            .collect()
    };
    Ok(SampleMasks {
        logical: selected.clone(),
        data: selected,
        threshold,
    })
}

fn time_selection(
    rows: &[GainSampleRow],
    first: Option<usize>,
    selector: &TimeSelector,
) -> Result<TimeSelection, FluxScaleError> {
    if selector.selects_all() {
        return Ok(TimeSelection::all());
    }
    let first_row = rows.first().ok_or_else(|| invalid("no calibration rows"))?;
    let logical_row = &rows[first.unwrap_or(0)];
    let defaults = |row: &GainSampleRow| TimeDefaults {
        time_mjd_s: row.time_seconds,
        exposure_s: row.interval_seconds,
    };
    selector
        .resolve(TimeSelectorDefaults {
            first_term: defaults(logical_row),
            later_terms: defaults(first_row),
        })
        .map_err(invalid)
}

fn antenna_mask(
    root: &Path,
    rows: &[GainSampleRow],
    expression: &str,
) -> Result<Vec<bool>, FluxScaleError> {
    if expression.is_empty() {
        return Ok(vec![true; rows.len()]);
    }
    let table = Table::open(TableOptions::new(root.join("ANTENNA"))).map_err(invalid)?;
    let mut metadata = Vec::new();
    for index in 0..table.row_count() {
        let position = table
            .cell_accessor(index, "POSITION")
            .and_then(|cell| cell.array())
            .ok()
            .and_then(|array| match array {
                ArrayValue::Float64(values) if values.len() == 3 => Some([
                    values[ndarray::IxDyn(&[0])],
                    values[ndarray::IxDyn(&[1])],
                    values[ndarray::IxDyn(&[2])],
                ]),
                _ => None,
            })
            .unwrap_or([0.; 3]);
        metadata.push((
            super::get_string(&table, index, "NAME")?,
            super::get_string(&table, index, "STATION").unwrap_or_default(),
            position,
        ));
    }
    let metadata = metadata
        .iter()
        .map(|(name, station, position)| AntennaRow {
            name,
            station,
            position_m: *position,
        })
        .collect::<Vec<_>>();
    let selection = resolve_antenna_selector(&metadata, expression).map_err(invalid)?;
    Ok(rows
        .iter()
        .map(|row| selection.holds(row.antenna_id as usize, row.antenna2_id as usize))
        .collect())
}
