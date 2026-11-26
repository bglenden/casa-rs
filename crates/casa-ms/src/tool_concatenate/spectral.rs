// SPDX-License-Identifier: LGPL-3.0-or-later
//! Spectral-window, polarization and data-description reconciliation.

use casa_core::{ArrayValue, RecordValue, ScalarValue, Value, measures::frequency::FrequencyRef};
use casa_tables::{Table, table_quantum::ScalarQuantColumn};

use crate::columns::frequency_columns::ChanFreqColumn;
use crate::subtables::{get_array, get_bool, get_i32};
use crate::{MeasurementSet, MsError, MsResult, SubtableId};

use super::required_table;

pub(super) struct SpectralRows {
    pub(super) windows: Vec<RecordValue>,
    pub(super) polarizations: Vec<RecordValue>,
    pub(super) descriptions: Vec<RecordValue>,
    pub(super) window_ids: Vec<usize>,
    pub(super) donor_descriptions: Vec<DonorDescription>,
}

pub(super) struct DonorDescription {
    pub(super) id: i32,
    pub(super) reverse: bool,
    pub(super) polarization: usize,
}

pub(super) fn reconcile(
    target: &MeasurementSet,
    donor: &MeasurementSet,
    tolerance_hz: f64,
) -> MsResult<SpectralRows> {
    let target_spw = required_table(target, SubtableId::SpectralWindow)?;
    let incoming_spw = required_table(donor, SubtableId::SpectralWindow)?;
    let target_pol = required_table(target, SubtableId::Polarization)?;
    let incoming_pol = required_table(donor, SubtableId::Polarization)?;
    let target_dd = required_table(target, SubtableId::DataDescription)?;
    let incoming_dd = required_table(donor, SubtableId::DataDescription)?;
    let mut known = windows(target_spw)?;
    let incoming = windows(incoming_spw)?;
    let mut rows = SpectralRows {
        windows: Vec::new(),
        polarizations: Vec::new(),
        descriptions: Vec::new(),
        window_ids: vec![usize::MAX; incoming.len()],
        donor_descriptions: Vec::new(),
    };
    let mut polarizations = target_pol.rows()?.iter().collect::<Vec<_>>();
    let incoming_polarizations = incoming_pol.rows()?;
    let mut known_descriptions = descriptions(target_dd)?;
    for (window, polarization) in descriptions(incoming_dd)? {
        let incoming_window = incoming
            .get(window)
            .ok_or_else(|| invalid_index("SPECTRAL_WINDOW", window, incoming.len()))?;
        let (window_id, reverse) = match_window(
            &mut known,
            &mut rows.windows,
            incoming_spw,
            window,
            incoming_window,
            tolerance_hz,
        )?;
        rows.window_ids[window] = window_id;
        let source = incoming_polarizations.get(polarization).ok_or_else(|| {
            invalid_index("POLARIZATION", polarization, incoming_polarizations.len())
        })?;
        let (polarization_id, description) = match match_polarization(&polarizations, source) {
            Some(id) => (
                id,
                known_descriptions
                    .iter()
                    .position(|pair| *pair == (window_id, id)),
            ),
            None => {
                let index = polarizations.len();
                rows.polarizations.push(source.clone());
                polarizations.push(source);
                (index, None)
            }
        };
        rows.donor_descriptions.push(DonorDescription {
            id: row_id(description.unwrap_or(known_descriptions.len()))?,
            reverse,
            polarization,
        });
        if description.is_none() {
            rows.descriptions.push(RecordValue::new(vec![
                casa_core::RecordField::new(
                    "SPECTRAL_WINDOW_ID",
                    Value::Scalar(ScalarValue::Int32(row_id(window_id)?)),
                ),
                casa_core::RecordField::new(
                    "POLARIZATION_ID",
                    Value::Scalar(ScalarValue::Int32(row_id(polarization_id)?)),
                ),
                casa_core::RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ]));
            known_descriptions.push((window_id, polarization_id));
        }
    }
    for (index, window) in incoming.iter().enumerate() {
        if rows.window_ids[index] == usize::MAX {
            rows.window_ids[index] = match_window(
                &mut known,
                &mut rows.windows,
                incoming_spw,
                index,
                window,
                tolerance_hz,
            )?
            .0;
        }
    }
    Ok(rows)
}

#[derive(Clone)]
struct Window {
    reference_hz: f64,
    frame: FrequencyRef,
    bandwidth_hz: f64,
    chain: i32,
    flagged: bool,
    channels: Vec<f64>,
}

fn windows(table: &Table) -> MsResult<Vec<Window>> {
    if table.row_count() == 0 {
        return Ok(Vec::new());
    }
    let reference = ScalarQuantColumn::with_unit(table, "REF_FREQUENCY", "Hz")?;
    let bandwidth = ScalarQuantColumn::with_unit(table, "TOTAL_BANDWIDTH", "Hz")?;
    let frequencies = ChanFreqColumn::new(table);
    (0..table.row_count())
        .map(|row| {
            let ArrayValue::Float64(channels) = get_array(table, row, "CHAN_FREQ")? else {
                return Err(MsError::InvalidInput("CHAN_FREQ must hold doubles".into()));
            };
            let count = get_i32(table, row, "NUM_CHAN")?;
            if channels.ndim() != 1 || channels.len() != count as usize || count <= 0 {
                return Err(MsError::InvalidInput(format!(
                    "SPECTRAL_WINDOW {row} has invalid channels"
                )));
            }
            Ok(Window {
                reference_hz: reference.get(row)?.value(),
                frame: frequencies.reference_frequency_reference(row)?,
                bandwidth_hz: bandwidth.get(row)?.value(),
                chain: get_i32(table, row, "IF_CONV_CHAIN")?,
                flagged: get_bool(table, row, "FLAG_ROW")?,
                channels: channels.iter().copied().collect(),
            })
        })
        .collect()
}

fn match_window(
    known: &mut Vec<Window>,
    additions: &mut Vec<RecordValue>,
    table: &Table,
    row: usize,
    incoming: &Window,
    tolerance: f64,
) -> MsResult<(usize, bool)> {
    let near = |a: f64, b: f64| (a - b).abs() <= tolerance;
    for (index, existing) in known.iter().enumerate() {
        if existing.flagged
            || existing.channels.len() != incoming.channels.len()
            || existing.chain != incoming.chain
            || existing.frame != incoming.frame
            || (existing.bandwidth_hz - incoming.bandwidth_hz).abs()
                > incoming.bandwidth_hz.abs() / 4.0
            || !near(existing.reference_hz, incoming.reference_hz)
        {
            continue;
        }
        let matches = |channels: &[f64]| {
            existing
                .channels
                .iter()
                .zip(channels)
                .all(|(&a, &b)| near(a, b))
        };
        if incoming.channels.len() == 1 || matches(&incoming.channels) {
            return Ok((index, false));
        }
        if existing
            .channels
            .iter()
            .zip(incoming.channels.iter().rev())
            .all(|(&a, &b)| near(a, b))
        {
            return Ok((index, true));
        }
    }
    let index = known.len();
    known.push(incoming.clone());
    additions.push(table.rows()?[row].clone());
    Ok((index, false))
}

fn match_polarization(rows: &[&RecordValue], incoming: &RecordValue) -> Option<usize> {
    rows.iter().rposition(|row| {
        row.get("FLAG_ROW") == Some(&Value::Scalar(ScalarValue::Bool(false)))
            && row.get("NUM_CORR") == incoming.get("NUM_CORR")
            && row.get("CORR_TYPE") == incoming.get("CORR_TYPE")
    })
}

fn descriptions(table: &Table) -> MsResult<Vec<(usize, usize)>> {
    (0..table.row_count())
        .map(|row| {
            let id = |column| {
                usize::try_from(get_i32(table, row, column)?).map_err(|_| {
                    MsError::InvalidInput(format!(
                        "negative {column} in DATA_DESCRIPTION row {row}"
                    ))
                })
            };
            Ok((id("SPECTRAL_WINDOW_ID")?, id("POLARIZATION_ID")?))
        })
        .collect()
}

pub(super) fn row_id(index: usize) -> MsResult<i32> {
    i32::try_from(index).map_err(|_| MsError::InvalidInput("subtable row ID exceeds int32".into()))
}

fn invalid_index(table: &str, index: usize, count: usize) -> MsError {
    MsError::InvalidIndex {
        context: table.into(),
        index,
        max: count.saturating_sub(1),
    }
}
