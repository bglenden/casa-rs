// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scan and field colors for final per-antenna time averages.

use std::collections::{BTreeMap, BTreeSet};

use super::{MsColorAxis, MsPlotSpec, MsScatterPlotMsAverageIds, MsScatterSeries};
use casa_ms::subtables::field::MsField;

// PMS::COLORS_LIST, shared by the plotted points and legend bins.
const COLORS: [&str; 10] = [
    "#202020", "#e00066", "#e07600", "#66d000", "#ac6600", "#0091a0", "#10e050", "#6600e0",
    "#0066f0", "#a868d8",
];

pub fn axis(spec: &MsPlotSpec) -> Option<MsColorAxis> {
    (spec.averaging.avgantenna
        && spec.averaging.avgtime.is_some()
        && !spec.averaging.scalar
        && (spec.averaging.avgscan || spec.averaging.avgfield)
        && matches!(spec.color_by, MsColorAxis::Scan | MsColorAxis::Field))
    .then_some(spec.color_by)
}

pub(super) fn category(axis: MsColorAxis, &(scan, field, _): &(i32, i32, i32)) -> i32 {
    if axis == MsColorAxis::Scan {
        scan
    } else {
        field
    }
}

fn key(axis: MsColorAxis, id: i32) -> String {
    format!("{}-{id}", axis.as_str())
}

fn category_ids(axis: MsColorAxis, rows: &[(i32, i32, i32)]) -> Vec<i32> {
    let mut seen = BTreeSet::new();
    rows.iter()
        .map(|row| category(axis, row))
        .filter(|&id| seen.insert(id))
        .collect()
}

pub(super) fn groups(
    axis: MsColorAxis,
    rows: &[(i32, i32, i32)],
    field: &MsField<'_>,
) -> Vec<(i32, String, String)> {
    let ids = category_ids(axis, rows);
    let Some(maximum) = ids.iter().max() else {
        return Vec::new();
    };
    let width = maximum.to_string().len() + 1;
    ids.into_iter()
        .map(|id| {
            let label = if axis == MsColorAxis::Scan {
                format!("Scan{id:>width$}")
            } else {
                let name = usize::try_from(id)
                    .ok()
                    .and_then(|row| field.name(row).ok())
                    .unwrap_or_default();
                format!("Field{id:>width$}: {name}")
            };
            (id, key(axis, id), label)
        })
        .collect()
}

pub fn palette(axis: MsColorAxis, series: &[MsScatterSeries]) -> BTreeMap<String, &'static str> {
    let Some(rows) = series
        .iter()
        .find_map(|item| match &item.plotms_averaged_ids {
            Some(MsScatterPlotMsAverageIds::ScanFieldAndAntenna(rows)) => Some(rows),
            _ => None,
        })
    else {
        return BTreeMap::new();
    };
    let ids = category_ids(axis, rows);
    // PlotMS uses the unsigned palette size in this modulo, including for Scan -1.
    let bin = |id: i32| id as usize % COLORS.len();
    // Equal numeric bins across every category switch to cache encounter order.
    let use_order = ids.len() > 1 && ids.iter().all(|&id| bin(id) == bin(ids[0]));
    ids.into_iter()
        .enumerate()
        .map(|(index, id)| {
            let index = if use_order {
                index % COLORS.len()
            } else {
                bin(id)
            };
            (key(axis, id), COLORS[index])
        })
        .collect()
}
