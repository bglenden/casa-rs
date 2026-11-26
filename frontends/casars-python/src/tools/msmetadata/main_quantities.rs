// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use casa_core::{ArrayValue, ScalarValue, Value};
use casa_ms::{MeasurementSet, MsError, MsResult};
use casa_tables::{Table, TableColumn};
use ndarray::{ArrayView2, Ix2};
use pyo3::prelude::*;

use super::main_scalar_batches::main_scalar_batches;
use super::names::checked_row;
use super::scan_index::Time;
use super::{PyMsMetadata, Source};
use crate::convert::value::CasaValue;
use crate::tools::{LogLine, emit};

const VISIBILITY_BATCH_ROWS: usize = 512;
const SCALAR_BATCH_ROWS: usize = 1 << 16;

#[derive(Clone)]
struct Window {
    bandwidth: f64,
    channels: Arc<[f64]>,
}

fn windows_per_description(ms: &MeasurementSet) -> MsResult<Vec<(i32, i32)>> {
    let descriptions = ms.data_description()?;
    (0..descriptions.row_count())
        .map(|row| {
            Ok((
                descriptions.spectral_window_id(row)?,
                descriptions.polarization_id(row)?,
            ))
        })
        .collect()
}

fn window_bandwidth(ms: &MeasurementSet) -> MsResult<Vec<Window>> {
    let windows = ms.spectral_window()?;
    let definitions = windows_per_description(ms)?;
    let mut bandwidths = BTreeMap::<i32, Window>::new();
    let mut result = Vec::with_capacity(definitions.len());
    for (id, _) in definitions {
        let row = checked_row(i64::from(id), windows.row_count(), "Spectral window ID")?;
        if let std::collections::btree_map::Entry::Vacant(entry) = bandwidths.entry(id) {
            entry.insert(Window {
                bandwidth: windows.total_bandwidth(row)?,
                // Channel order can give signed widths. A usable fraction
                // or exposure is weighted by physical bandwidth.
                channels: windows.chan_width(row)?.into_iter().map(f64::abs).collect(),
            });
        }
        result.push(bandwidths[&id].clone());
    }
    Ok(result)
}

fn bandwidth_definition(windows: &[Window], ddid: i32) -> MsResult<&Window> {
    windows
        .get(ddid as usize)
        .ok_or_else(|| MsError::InvalidInput(format!("Unknown data description ID {ddid}")))
}

fn flags(value: &Option<ArrayValue>) -> MsResult<ArrayView2<'_, bool>> {
    let Some(ArrayValue::Bool(flags)) = value else {
        return Err(MsError::InvalidInput(
            "MAIN.FLAG has a missing or wrongly typed cell".into(),
        ));
    };
    flags
        .view()
        .into_dimensionality::<Ix2>()
        .map_err(|_| MsError::InvalidInput("MAIN.FLAG must have two axes".into()))
}

fn selected_bandwidth(
    flags: ArrayView2<'_, bool>,
    window: &Window,
    complete_correlation_uses_total: bool,
) -> MsResult<f64> {
    if flags.ncols() != window.channels.len() {
        return Err(MsError::InvalidInput(
            "MAIN.FLAG channel count differs from SPECTRAL_WINDOW".into(),
        ));
    }
    let selected = flags
        .rows()
        .into_iter()
        .map(|channels| {
            if complete_correlation_uses_total && channels.iter().all(|&flagged| !flagged) {
                window.bandwidth
            } else {
                channels
                    .iter()
                    .zip(window.channels.iter())
                    .filter_map(|(&flagged, &width)| (!flagged).then_some(width))
                    .sum()
            }
        })
        .sum();
    Ok(selected)
}

fn visit_flag_cells(
    column: &TableColumn<'_>,
    rows: &[usize],
    antennas: [&[i32]; 2],
    row_flags: &[bool],
    autoc: bool,
    mut visit: impl FnMut(usize, ArrayView2<'_, bool>) -> MsResult<()>,
) -> MsResult<()> {
    let [first, second] = antennas;
    let selected = (0..rows.len())
        .filter(|&index| !row_flags[index] && (autoc || first[index] != second[index]))
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return Ok(());
    }
    let physical = selected
        .iter()
        .map(|&index| rows[index])
        .collect::<Vec<_>>();
    let flags_batch = column.array_cells_owned_uncached(&physical)?;
    for (index, value) in selected.into_iter().zip(&flags_batch) {
        visit(index, flags(value)?)?;
    }
    Ok(())
}

fn unflagged_rows(ms: &MeasurementSet, autoc: bool) -> MsResult<f64> {
    let windows = window_bandwidth(ms)?;
    let flag_column = ms.main_table().column_accessor("FLAG")?;
    let mut total = 0.0;
    main_scalar_batches(
        ms,
        ["ANTENNA1", "ANTENNA2", "DATA_DESC_ID"],
        [],
        true,
        VISIBILITY_BATCH_ROWS,
        |rows, [first, second, descriptions], [], row_flags| {
            visit_flag_cells(
                &flag_column,
                rows,
                [first, second],
                row_flags,
                autoc,
                |index, flag| {
                    if flag.iter().all(|&flagged| !flagged) {
                        total += 1.0;
                    } else if flag.iter().any(|&flagged| !flagged) {
                        let window = bandwidth_definition(&windows, descriptions[index])?;
                        total += selected_bandwidth(flag, window, true)?
                            / (window.bandwidth * flag.nrows() as f64);
                    }
                    Ok(())
                },
            )
        },
    )?;
    Ok(total)
}

fn cross_rows(ms: &MeasurementSet) -> MsResult<usize> {
    let mut count = 0;
    main_scalar_batches(
        ms,
        ["ANTENNA1", "ANTENNA2"],
        [],
        false,
        SCALAR_BATCH_ROWS,
        |_, [first, second], [], _| {
            count += first.iter().zip(second).filter(|(a, b)| a != b).count();
            Ok(())
        },
    )?;
    Ok(count)
}

fn exposure_unit(main: &Table) -> &str {
    main.column_keywords("EXPOSURE")
        .and_then(|record| record.get("QuantumUnits"))
        .and_then(|unit| match unit {
            Value::Array(ArrayValue::String(values)) => values.first().map(String::as_str),
            Value::Scalar(ScalarValue::String(unit)) => Some(unit.as_str()),
            _ => None,
        })
        .unwrap_or("s")
}

fn exposure(
    ms: &MeasurementSet,
    scan: i64,
    spw: i64,
    pol: i64,
    observation: i64,
    array: i64,
) -> MsResult<(CasaValue, Vec<LogLine>)> {
    let windows = ms.spectral_window()?;
    let observations = ms.observation()?;
    checked_row(spw, windows.row_count(), "Spectral window ID")?;
    checked_row(observation, observations.row_count(), "Observation ID")?;
    if pol >= 0 {
        checked_row(pol, ms.polarization()?.row_count(), "Polarization ID")?;
    }
    let description_ids = windows_per_description(ms)?;
    let mut first_by_description = BTreeMap::<usize, (f64, i32, f64)>::new();
    main_scalar_batches(
        ms,
        [
            "OBSERVATION_ID",
            "ARRAY_ID",
            "SCAN_NUMBER",
            "DATA_DESC_ID",
            "FIELD_ID",
        ],
        ["TIME", "EXPOSURE"],
        false,
        SCALAR_BATCH_ROWS,
        |rows, [obs, arr, scan_number, desc, fields], [times, exposures], _| {
            for index in 0..rows.len() {
                if i64::from(obs[index]) != observation
                    || i64::from(arr[index]) != array
                    || i64::from(scan_number[index]) != scan
                {
                    continue;
                }
                let row = checked_row(
                    i64::from(desc[index]),
                    description_ids.len(),
                    "Data description ID",
                )?;
                let (window, polarization) = description_ids[row];
                if i64::from(window) == spw && (pol < 0 || i64::from(polarization) == pol) {
                    let time = times[index];
                    let field = fields[index];
                    let sample = (time, field, exposures[index]);
                    let first = first_by_description.entry(row).or_insert(sample);
                    // CASA visits tied subscans in field-ID order and keeps
                    // the first physical row for an exact tie within a field.
                    if time < first.0 || (time == first.0 && field < first.1) {
                        *first = sample;
                    }
                }
            }
            Ok(())
        },
    )?;
    let pol = if pol >= 0 {
        pol as i32
    } else {
        let pols = first_by_description
            .keys()
            .map(|&id| description_ids[id].1)
            .collect::<BTreeSet<_>>();
        let &first = pols.first().ok_or_else(|| {
            MsError::InvalidInput(
                "This dataset has no records for the specified scan and spwid".into(),
            )
        })?;
        if pols.len() > 1 {
            return Ok((
                CasaValue::empty_record(),
                vec![LogLine::warning(format!(
                    "This scan and spectral window have multiple polarization IDs {pols:?}. Specify one of those IDs."
                ))],
            ));
        }
        first
    };
    let id = description_ids
        .iter()
        .rposition(|&(window, polarization)| i64::from(window) == spw && polarization == pol)
        .ok_or_else(|| {
            MsError::InvalidInput(
                "MS has no data description ID for the specified spectral window and polarization"
                    .into(),
            )
        })?;
    let (_, _, first) = first_by_description.get(&id).ok_or_else(|| {
        MsError::InvalidInput(
            "MS has no records for the specified scan, spectral window, and polarization".into(),
        )
    })?;
    Ok((
        CasaValue::quantity(*first, exposure_unit(ms.main_table())),
        Vec::new(),
    ))
}

fn exposure_bandwidth(
    ms: &MeasurementSet,
    descriptions: &[Window],
) -> MsResult<BTreeMap<Time, f64>> {
    let mut time_windows = BTreeMap::<Time, BTreeSet<i32>>::new();
    main_scalar_batches(
        ms,
        ["ANTENNA1", "ANTENNA2", "DATA_DESC_ID"],
        ["TIME"],
        false,
        SCALAR_BATCH_ROWS,
        |rows, [first, second, ddids], [times], _| {
            for i in 0..rows.len() {
                if first[i] != second[i] {
                    time_windows
                        .entry(Time(times[i]))
                        .or_default()
                        .insert(ddids[i]);
                }
            }
            Ok(())
        },
    )?;
    time_windows
        .into_iter()
        .map(|(time, windows)| {
            let mut total = 0.0;
            for window in windows {
                total += bandwidth_definition(descriptions, window)?.bandwidth;
            }
            Ok((time, total))
        })
        .collect()
}

fn effective_exposure(ms: &MeasurementSet) -> MsResult<CasaValue> {
    let descriptions = window_bandwidth(ms)?;
    let time_bandwidth = exposure_bandwidth(ms, &descriptions)?;
    let count = ms.antenna()?.row_count();
    let baselines = (count.saturating_mul(count.saturating_sub(1)) / 2) as f64;
    let flag_column = ms.main_table().column_accessor("FLAG")?;
    let mut total = 0.0;
    main_scalar_batches(
        ms,
        ["ANTENNA1", "ANTENNA2", "DATA_DESC_ID"],
        ["TIME", "EXPOSURE"],
        true,
        VISIBILITY_BATCH_ROWS,
        |rows, [first, second, ddids], [times, exposure], row_flags| {
            visit_flag_cells(
                &flag_column,
                rows,
                [first, second],
                row_flags,
                false,
                |index, flags| {
                    if flags.iter().any(|&flagged| !flagged) {
                        let window = bandwidth_definition(&descriptions, ddids[index])?;
                        let bandwidth = selected_bandwidth(flags, window, false)?;
                        let time = Time(times[index]);
                        total += exposure[index] * bandwidth
                            / (time_bandwidth[&time] * baselines * flags.nrows() as f64);
                    }
                    Ok(())
                },
            )
        },
    )?;
    Ok(CasaValue::quantity(total, exposure_unit(ms.main_table())))
}

#[pymethods]
impl PyMsMetadata {
    #[pyo3(signature = (autoc = true, flagged = true), text_signature = "($self, autoc=True, flagged=True)")]
    fn nrows(&self, py: Python<'_>, autoc: bool, flagged: bool) -> PyResult<f64> {
        if flagged && autoc {
            return self.with_attachment(py, |attached| {
                let count = match &attached.source {
                    Source::Open(snapshot) => snapshot.ms.row_count(),
                    Source::Linked(link) => link.row_count()?,
                };
                Ok(count as f64)
            });
        }
        self.with_ms(py, |ms| {
            if flagged {
                cross_rows(ms).map(|count| count as f64)
            } else {
                unflagged_rows(ms, autoc)
            }
        })
    }

    #[pyo3(signature = (scan = 0, spwid = 0, polid = -1, obsid = 0, arrayid = 0), text_signature = "($self, scan=0, spwid=0, polid=-1, obsid=0, arrayid=0)")]
    fn exposuretime(
        &self,
        py: Python<'_>,
        scan: i64,
        spwid: i64,
        polid: i64,
        obsid: i64,
        arrayid: i64,
    ) -> PyResult<Py<PyAny>> {
        let (exposure, logs) =
            self.with_ms(py, |ms| exposure(ms, scan, spwid, polid, obsid, arrayid))?;
        emit(py, "casars.tools.msmetadata", &logs)?;
        exposure.into_python(py)
    }

    #[pyo3(signature = (), text_signature = "($self)")]
    fn effexposuretime(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.with_ms(py, effective_exposure)?.into_python(py)
    }
}
