// SPDX-License-Identifier: LGPL-3.0-or-later
//! Page header values for calibration-table figures.

use std::path::Path;

use casa_task_runtime::{TaskFailure, TaskInvocation};
use casa_types::quanta::{MvAngle, MvTime};
use casa_types::{ArrayValue, ScalarValue, Value};

use super::iteration::PlotPanels;
use super::table_data::{CalibrationTable, array, subtable, text};
use crate::msexplore::{MsPageHeaderItem, wrap_header_segments};

pub(super) fn items(task: &TaskInvocation<'_>) -> Result<Vec<MsPageHeaderItem>, TaskFailure> {
    let requested = task.text("headeritems")?.unwrap_or_default();
    let mut items = Vec::new();
    for token in requested
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        if let Some(item) = MsPageHeaderItem::parse(token) {
            if !items.contains(&item) {
                items.push(item);
            }
        } else {
            tracing::warn!("Ignoring invalid page header item: {token}");
        }
    }
    Ok(items)
}

pub(super) fn lines(
    items: &[MsPageHeaderItem],
    table: &CalibrationTable,
    path: &Path,
    panels: &PlotPanels,
) -> Vec<String> {
    if items.is_empty() {
        return Vec::new();
    }
    let Some(panel) = panels.panels.first() else {
        return Vec::new();
    };
    let coordinates = panel
        .series
        .iter()
        .find_map(|series| series.points.first().map(|point| point.coordinates));
    let (obs, field) = coordinates.map_or((0, 0), |point| {
        (
            usize::try_from(point.observation).unwrap_or_default(),
            usize::try_from(point.field).unwrap_or_default(),
        )
    });
    let observation = subtable(&table.table, path, "OBSERVATION").ok();
    let read_obs = |name| {
        observation
            .as_ref()
            .and_then(|metadata| text(metadata, obs, name))
            .unwrap_or_default()
    };
    let start = observation
        .as_ref()
        .and_then(|metadata| match array(metadata, obs, "TIME_RANGE").ok()? {
            ArrayValue::Float64(values) => values
                .iter()
                .copied()
                .next()
                .filter(|time| time.is_finite()),
            _ => None,
        })
        .map(MvTime::from_mjd_seconds);
    let date_time = start.as_ref().map(|time| time.format_dmy(1));
    let date = date_time.as_deref().map_or("", |value| {
        value.split_once('/').map_or(value, |(date, _)| date)
    });
    let segments = items
        .iter()
        .map(|item| match item {
            MsPageHeaderItem::Filename => format!(
                "File: {}",
                path.file_name()
                    .unwrap_or(path.as_os_str())
                    .to_string_lossy()
            ),
            MsPageHeaderItem::YColumn => {
                let names = panel
                    .series
                    .iter()
                    .map(|series| series.y.label())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("Y Column(s): {names}")
            }
            MsPageHeaderItem::ObsDate => format!("Observation date: {date}"),
            MsPageHeaderItem::ObsTime => format!(
                "Observation time: {}",
                start
                    .as_ref()
                    .map(|time| time.format_time(1))
                    .unwrap_or_default()
            ),
            MsPageHeaderItem::Observer => format!("Observer: {}", read_obs("OBSERVER")),
            MsPageHeaderItem::ProjId => format!("Project ID: {}", read_obs("PROJECT")),
            MsPageHeaderItem::Telescope => format!("Telescope: {}", read_obs("TELESCOPE_NAME")),
            MsPageHeaderItem::TargName => format!(
                "Target name: {}",
                table.fields.get(field).map_or("", |row| row.name.as_str())
            ),
            MsPageHeaderItem::TargDir => format!(
                "Target direction: {}",
                direction(table, path, field).unwrap_or_default()
            ),
        })
        .collect::<Vec<_>>();
    wrap_header_segments(&segments)
}

fn direction(table: &CalibrationTable, path: &Path, row: usize) -> Option<String> {
    let field = subtable(&table.table, path, "FIELD").ok()?;
    let ArrayValue::Float64(values) = array(&field, row, "PHASE_DIR").ok()? else {
        return None;
    };
    let mut angles = values.iter().copied().map(MvAngle::from_radians);
    let ra = angles.next()?.format_time(6);
    let dec = angles.next()?.format_angle_dig2(5);
    let reference = field
        .column_keywords("PHASE_DIR")
        .and_then(|keywords| {
            let Value::Record(measure) = keywords.get("MEASINFO")? else {
                return None;
            };
            let Value::Scalar(ScalarValue::String(reference)) = measure.get("Ref")? else {
                return None;
            };
            Some(reference.as_str())
        })
        .unwrap_or_default();
    Some(format!("{ra} {dec} {reference}").trim_end().into())
}
