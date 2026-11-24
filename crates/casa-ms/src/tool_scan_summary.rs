// SPDX-License-Identifier: LGPL-3.0-or-later
//! Integration runs returned by CASA's MeasurementSet scan-summary tool.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use casa_tables::RequiredScalarColumnValues;

use crate::{MeasurementSet, MsError, MsReadPlan, MsResult, MsSelectionIoBudget};

/// One subscan in the public `ms.getscansummary()` report.
#[derive(Debug, PartialEq)]
pub struct ToolScanSummary {
    /// The first integration center, as MJD days.
    pub begin_day: f64,
    /// The last integration center, as MJD days.
    pub end_day: f64,
    /// Field ID of the first visibility row at the start of the run.
    pub field_id: i32,
    /// State ID of the first visibility row at the start of the run.
    pub state_id: i32,
    /// Count of visibility rows in the run, including repeated view rows.
    pub row_count: usize,
    /// Mean MAIN.EXPOSURE, in seconds.
    pub mean_exposure: f64,
    /// DATA_DESC_IDs in order of first occurrence within an integration.
    pub data_description_ids: Vec<i32>,
    /// One SPECTRAL_WINDOW_ID per data description, in the same order.
    pub spectral_window_ids: Vec<i32>,
}

#[derive(Clone, Copy, PartialEq)]
struct IntegrationTime(f64);

// Only finite TIME values are admitted. Use ordinary equality so +0 and -0
// describe the same integration, as they do in the C++ table iterator.
impl Eq for IntegrationTime {}

impl PartialOrd for IntegrationTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IntegrationTime {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .partial_cmp(&other.0)
            .expect("finite integration times")
    }
}

#[derive(Default, PartialEq, Eq)]
struct IntegrationIds {
    fields: Vec<i32>,
    data_descriptions: Vec<i32>,
    states: Vec<i32>,
}

#[derive(Default)]
struct Integration {
    ids: IntegrationIds,
    row_count: usize,
    exposure_sum: f64,
}

impl Integration {
    fn add_row(&mut self, field: i32, description: i32, state: i32, exposure: f64) {
        push_unique(&mut self.ids.fields, field);
        push_unique(&mut self.ids.data_descriptions, description);
        push_unique(&mut self.ids.states, state);
        self.row_count += 1;
        self.exposure_sum += exposure;
    }
}

fn push_unique(ids: &mut Vec<i32>, id: i32) {
    if !ids.contains(&id) {
        ids.push(id);
    }
}

type ScanKey = (i32, i32, i32);
type Integrations = BTreeMap<IntegrationTime, Integration>;

/// Report the consecutive integration runs within each observation, array
/// and scan of the supplied MeasurementSet view. A change to an integration's
/// ordered field, data description or state IDs starts a new run.
/// A supplied selection is an ordered list of row indices into the view;
/// otherwise all of MAIN is used.
///
/// Keys are scan numbers. The vector is indexed by the subscan number in
/// CASA's record. When a scan number recurs in another observation or array,
/// the later group's subscan records replace any with the same index.
///
/// # Errors
/// Returns an error for unreadable MAIN cells, nonfinite integration times
/// or an invalid DATA_DESCRIPTION reference.
pub fn tool_scan_summary(
    ms: &MeasurementSet,
    selected_rows: Option<&[usize]>,
) -> MsResult<BTreeMap<i32, Vec<ToolScanSummary>>> {
    let row_count = selected_rows.map_or_else(|| ms.row_count(), <[usize]>::len);
    if row_count == 0 {
        return Ok(BTreeMap::new());
    }
    let scans = read_integrations(ms, selected_rows, row_count)?;
    let description = ms.data_description()?;
    let spectral_windows = (0..description.row_count())
        .map(|row| description.spectral_window_id(row))
        .collect::<MsResult<Vec<_>>>()?;
    let mut by_scan = BTreeMap::<_, Vec<_>>::new();
    for ((_, _, number), integrations) in scans {
        let summaries = by_scan.entry(number).or_default();
        let mut integrations = integrations.into_iter().peekable();
        let mut index = 0;
        while let Some((begin, first)) = integrations.next() {
            let mut end = begin;
            let mut row_count = first.row_count;
            let mut exposure_sum = first.exposure_sum;
            while let Some((time, same)) = integrations.next_if(|(_, next)| next.ids == first.ids) {
                end = time;
                row_count += same.row_count;
                exposure_sum += same.exposure_sum;
            }
            let data_description_ids = first.ids.data_descriptions;
            let spectral_window_ids = data_description_ids
                .iter()
                .map(|&id| {
                    spectral_windows.get(id as usize).copied().ok_or_else(|| {
                        MsError::InvalidInput(format!("invalid MAIN.DATA_DESC_ID {id}"))
                    })
                })
                .collect::<MsResult<Vec<_>>>()?;
            let summary = ToolScanSummary {
                begin_day: begin.0 / 86_400.0,
                end_day: end.0 / 86_400.0,
                field_id: first.ids.fields[0],
                state_id: first.ids.states[0],
                row_count,
                mean_exposure: exposure_sum / row_count as f64,
                data_description_ids,
                spectral_window_ids,
            };
            if let Some(previous) = summaries.get_mut(index) {
                *previous = summary;
            } else {
                summaries.push(summary);
            }
            index += 1;
        }
    }
    Ok(by_scan)
}

fn read_integrations(
    ms: &MeasurementSet,
    selected_rows: Option<&[usize]>,
    row_count: usize,
) -> MsResult<BTreeMap<ScanKey, Integrations>> {
    const COLUMNS: [&str; 8] = [
        "OBSERVATION_ID",
        "ARRAY_ID",
        "SCAN_NUMBER",
        "FIELD_ID",
        "DATA_DESC_ID",
        "STATE_ID",
        "TIME",
        "EXPOSURE",
    ];
    let bytes_per_row = 6 * std::mem::size_of::<i32>()
        + 2 * std::mem::size_of::<f64>()
        + std::mem::size_of::<usize>();
    let plan = MsSelectionIoBudget::from_system_memory(1, bytes_per_row, None)
        .and_then(|budget| MsReadPlan::new(row_count, budget))
        .map_err(|error| MsError::InvalidInput(error.to_string()))?;
    let table = ms.main_table();
    let mut scans = BTreeMap::<ScanKey, Integrations>::new();
    let mut row_buffer = Vec::new();
    for start in (0..row_count).step_by(plan.rows_per_block) {
        let end = start.saturating_add(plan.rows_per_block).min(row_count);
        let rows = if let Some(selected) = selected_rows {
            &selected[start..end]
        } else {
            row_buffer.clear();
            row_buffer.extend(start..end);
            &row_buffer
        };
        let mut columns = table.required_scalar_columns_owned_for_rows(&COLUMNS, rows)?;
        let [
            observation,
            array,
            scan,
            field,
            description,
            state,
            time,
            exposure,
        ] = COLUMNS;
        let observation = take_integer(&mut columns, observation)?;
        let array = take_integer(&mut columns, array)?;
        let scan = take_integer(&mut columns, scan)?;
        let field = take_integer(&mut columns, field)?;
        let description = take_integer(&mut columns, description)?;
        let state = take_integer(&mut columns, state)?;
        let times = take_double(&mut columns, time)?;
        let exposures = take_double(&mut columns, exposure)?;
        for (row, time) in times.into_iter().enumerate() {
            if !time.is_finite() {
                return Err(MsError::InvalidInput("MAIN.TIME must be finite".into()));
            }
            scans
                .entry((observation[row], array[row], scan[row]))
                .or_default()
                .entry(IntegrationTime(time))
                .or_default()
                .add_row(field[row], description[row], state[row], exposures[row]);
        }
    }
    Ok(scans)
}

pub(crate) fn take_integer(
    columns: &mut HashMap<String, RequiredScalarColumnValues>,
    name: &str,
) -> MsResult<Vec<i32>> {
    match columns.remove(name) {
        Some(RequiredScalarColumnValues::Int32(values)) => Ok(values),
        _ => Err(MsError::InvalidInput(format!(
            "MAIN.{name} must contain Int32 scalars"
        ))),
    }
}

pub(crate) fn take_double(
    columns: &mut HashMap<String, RequiredScalarColumnValues>,
    name: &str,
) -> MsResult<Vec<f64>> {
    match columns.remove(name) {
        Some(RequiredScalarColumnValues::Float64(values)) => Ok(values),
        _ => Err(MsError::InvalidInput(format!(
            "MAIN.{name} must contain Float64 scalars"
        ))),
    }
}
