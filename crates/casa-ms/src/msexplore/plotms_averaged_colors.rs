// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scan and field colors for final per-antenna time averages.

use std::collections::BTreeMap;

use super::{MsColorAxis, MsScatterPlotMsAverageIds, MsScatterSeries};
use crate::subtables::field::MsField;

// PMS::COLORS_LIST, shared by the plotted points and legend bins.
const COLORS: [&str; 10] = [
    "#202020", "#e00066", "#e07600", "#66d000", "#ac6600", "#0091a0", "#10e050", "#6600e0",
    "#0066f0", "#a868d8",
];

pub(super) fn group(axis: MsColorAxis, id: i32, field: &MsField<'_>) -> (String, String) {
    match axis {
        MsColorAxis::Scan => (format!("scan-{id}"), format!("Scan {id}")),
        MsColorAxis::Field => {
            let name = usize::try_from(id)
                .ok()
                .and_then(|row| field.name(row).ok())
                .filter(|name| !name.is_empty());
            let label = match name {
                Some(name) => format!("Field {id}: {name}"),
                None => format!("Field {id}"),
            };
            (format!("field-{id}"), label)
        }
        _ => unreachable!("antenna averages use scan or field colors"),
    }
}

pub(super) fn palette(series: &[MsScatterSeries]) -> BTreeMap<&str, &'static str> {
    let mut colors = series
        .iter()
        .filter_map(|item| {
            if !matches!(
                item.plotms_averaged_ids,
                Some(MsScatterPlotMsAverageIds::ScanFieldAndAntenna(_))
            ) {
                return None;
            }
            let group = item.color_group.as_str();
            let id = group
                .strip_prefix("scan-")
                .or_else(|| group.strip_prefix("field-"))?
                .parse::<i32>()
                .ok()?;
            // PlotMS uses the unsigned palette size in this modulo, including for Scan -1.
            Some((group, COLORS[id as usize % COLORS.len()]))
        })
        .collect::<BTreeMap<_, _>>();
    if colors.len() > 1
        && colors
            .values()
            .all(|color| color == colors.values().next().unwrap())
    {
        // If every numeric ID would have the same color, use the order of the
        // final averaged rows, which is also the cache order used by PlotMS.
        let mut first = BTreeMap::new();
        for item in series {
            let group = item.color_group.as_str();
            if colors.contains_key(group)
                && let Some(order) = item
                    .plotms_metadata
                    .iter()
                    .filter_map(|point| point.averaging_group)
                    .min()
            {
                let first_row = first.entry(group).or_insert(order);
                *first_row = (*first_row).min(order);
            }
        }
        let mut ordered = first.into_iter().collect::<Vec<_>>();
        ordered.sort_unstable_by_key(|&(_, first_row)| first_row);
        for (index, (group, _)) in ordered.into_iter().enumerate() {
            colors.insert(group, COLORS[index % COLORS.len()]);
        }
    }
    colors
}
