// SPDX-License-Identifier: LGPL-3.0-or-later
//! WVR-specific name and ID resolution shared by the task and tool.

use std::collections::BTreeMap;

use casa_core::ScalarValue;
use casa_ms::{MeasurementSet, MsError, schema::SubtableId};

use super::{WvrError, WvrRequest};

/// Selectors accepted as names or numbers at the public WVR boundaries.
#[derive(Debug, Clone, Default)]
pub struct WvrSelectors {
    /// Antenna names or IDs whose radiometers must be replaced.
    pub flagged_antennas: Vec<String>,
    /// Ordered antenna names or IDs available for reference fitting.
    pub reference_antennas: Vec<String>,
    /// Source names to flag in segmented mode.
    pub flagged_sources: Vec<String>,
    /// Comma-separated source names or IDs sharing a fit.
    pub tied_sources: Vec<String>,
    /// Source name for diagnostics. CASA uses the first entry.
    pub statistic_sources: Vec<String>,
    /// Field name or ID for diagnostics. CASA uses the first entry.
    pub statistic_fields: Vec<String>,
    /// Spectral-window selector accepted by the task's `reversespw`.
    pub reversed_windows: String,
}

/// Resolve the named selectors into the indices used by the WVR engine.
pub fn resolve_wvr_selectors(
    request: &mut WvrRequest,
    selectors: &WvrSelectors,
) -> Result<(), WvrError> {
    let ms = MeasurementSet::open(&request.measurement_set)?;
    let antenna_table = ms.antenna()?;
    let names = (0..antenna_table.row_count())
        .map(|index| antenna_table.name(index))
        .collect::<Result<Vec<_>, _>>()?;
    let resolve_antennas = |values: &[String]| -> Result<Vec<_>, WvrError> {
        values
            .iter()
            .flat_map(|value| value.split(','))
            .filter(|value| !value.is_empty())
            .map(|value| {
                names
                    .iter()
                    .rposition(|name| name == value)
                    .or_else(|| value.parse::<usize>().ok().filter(|&id| id < names.len()))
                    .ok_or_else(|| WvrError::Invalid(format!("Unknown WVR antenna {value:?}")))
            })
            .collect()
    };
    request.flagged_antennas = resolve_antennas(&selectors.flagged_antennas)?;
    request.reference_antennas = resolve_antennas(&selectors.reference_antennas)?;
    if !selectors.reversed_windows.is_empty() {
        request.reversed_windows = ms
            .resolve_spw_selector(&selectors.reversed_windows)?
            .spw_ids()
            .iter()
            .map(|&window| window as i32)
            .collect();
    }
    if !selectors.flagged_sources.is_empty()
        || !selectors.tied_sources.is_empty()
        || !selectors.statistic_sources.is_empty()
    {
        resolve_sources(&ms, request, selectors)?;
    }
    if selectors.statistic_sources.is_empty()
        && let Some(text) = selectors.statistic_fields.first()
    {
        let fields = ms.field()?;
        let names = (0..fields.row_count())
            .map(|id| fields.name(id))
            .collect::<Result<Vec<_>, _>>()?;
        let index = names
            .iter()
            .rposition(|name| name == text)
            .map(|id| id as i32)
            .or_else(|| text.parse().ok())
            .unwrap_or(-1);
        request.statistic_fields = vec![index];
    }
    Ok(())
}

fn resolve_sources(
    ms: &MeasurementSet,
    request: &mut WvrRequest,
    selectors: &WvrSelectors,
) -> Result<(), WvrError> {
    let mut sources = BTreeMap::new();
    if let Some(table) = ms.subtable(SubtableId::Source) {
        for row in 0..table.row_count() {
            let id = table
                .cell_accessor(row, "SOURCE_ID")
                .and_then(|cell| cell.scalar())
                .map_err(MsError::from)?;
            let name = table
                .cell_accessor(row, "NAME")
                .and_then(|cell| cell.scalar())
                .map_err(MsError::from)?;
            if let (ScalarValue::Int32(id), ScalarValue::String(name)) = (id, name) {
                sources.entry(*id).or_insert_with(|| name.clone());
            }
        }
    }
    let by_name = |text: &str| {
        sources
            .iter()
            .rev()
            .find(|(_, name)| name.as_str() == text)
            .map(|(&id, _)| id)
    };
    request.flagged_sources = selectors
        .flagged_sources
        .iter()
        .filter_map(|text| by_name(text))
        .collect();
    if let Some(text) = selectors.statistic_sources.first() {
        request.statistic_sources = sources
            .iter()
            .filter(|(_, name)| *name == text)
            .map(|(&id, _)| id)
            .collect();
        if request.statistic_sources.is_empty() {
            request.statistic_sources.push(i32::MIN);
        }
    }
    for group in &selectors.tied_sources {
        request.tied_sources.push(
            group
                .split(',')
                .filter(|text| !text.is_empty())
                .map(|text| {
                    text.parse::<i32>()
                        .ok()
                        .filter(|id| sources.contains_key(id))
                        .or_else(|| by_name(text))
                        .ok_or_else(|| {
                            WvrError::Invalid(format!("Unknown tied WVR source {text:?}"))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    Ok(())
}
