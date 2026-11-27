// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn apply_page_header_lines(payload: &mut MsPlotPayload, header_lines: Vec<String>) {
    if header_lines.is_empty() {
        return;
    }
    match payload {
        MsPlotPayload::Scatter(payload) => payload.header_lines = header_lines,
        MsPlotPayload::ScatterGrid(payload) => payload.header_lines = header_lines,
        MsPlotPayload::ScatterPage(payload) => payload.header_lines = header_lines,
        MsPlotPayload::ListObs(_) => {}
    }
}

pub(super) fn resolve_page_header_lines(
    ms: &MeasurementSet,
    spec: &MsExploreSpec,
) -> Result<Vec<String>, String> {
    if spec.header_items.is_empty() {
        return Ok(Vec::new());
    }
    let summary = ListObsSummary::from_ms_with_options(ms, &spec.selection.to_summary_options())
        .map_err(|error| error.to_string())?;
    let segments = spec
        .header_items
        .iter()
        .filter_map(|item| resolve_page_header_segment(*item, spec, &summary))
        .collect::<Vec<_>>();
    Ok(wrap_header_segments(&segments))
}

pub(super) fn resolve_page_header_segment(
    item: MsPageHeaderItem,
    spec: &MsExploreSpec,
    summary: &ListObsSummary,
) -> Option<String> {
    match item {
        MsPageHeaderItem::Filename => spec
            .ms_path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| format!("Filename: {name}")),
        MsPageHeaderItem::YColumn => {
            let y_columns = collect_page_y_column_labels(spec);
            (!y_columns.is_empty()).then(|| format!("Y Column: {}", y_columns.join(", ")))
        }
        MsPageHeaderItem::ObsDate => measurement_set_start_time(summary)
            .map(|time| format!("Obs Date: {}", MvTime::from_mjd_seconds(time).format_dmy(1))),
        MsPageHeaderItem::ObsTime => measurement_set_start_time(summary).map(|time| {
            format!(
                "Obs Time: {}",
                MvTime::from_mjd_seconds(time).format_time(1)
            )
        }),
        MsPageHeaderItem::Observer => first_non_empty(
            summary
                .observations
                .iter()
                .map(|observation| observation.observer.as_str()),
        )
        .map(|value| format!("Observer: {value}")),
        MsPageHeaderItem::ProjId => first_non_empty(
            summary
                .observations
                .iter()
                .map(|observation| observation.project.as_str()),
        )
        .map(|value| format!("Project: {value}")),
        MsPageHeaderItem::Telescope => first_non_empty(
            summary
                .observations
                .iter()
                .map(|observation| observation.telescope_name.as_str()),
        )
        .map(|value| format!("Telescope: {value}")),
        MsPageHeaderItem::TargName => summary
            .fields
            .first()
            .map(|field| field.name.clone())
            .or_else(|| summary.sources.first().map(|source| source.name.clone()))
            .filter(|value| !value.trim().is_empty())
            .map(|value| format!("Target: {value}")),
        MsPageHeaderItem::TargDir => summary.fields.first().map(|field| {
            let ra = MvAngle::from_radians(field.phase_direction_radians[0]).format_time(6);
            let dec = MvAngle::from_radians(field.phase_direction_radians[1]).format_angle_dig2(5);
            match field.direction_reference.as_deref() {
                Some(reference) if !reference.trim().is_empty() => {
                    format!("Target Dir: {ra} {dec} {reference}")
                }
                _ => format!("Target Dir: {ra} {dec}"),
            }
        }),
    }
}

pub(super) fn collect_page_y_column_labels(spec: &MsExploreSpec) -> Vec<String> {
    let mut labels = Vec::<String>::new();
    for plot in &spec.plots {
        let plot_labels =
            if let Some(label) = plot.style.ylabel.as_ref().filter(|label| !label.is_empty()) {
                vec![label.clone()]
            } else {
                plot.y_axes
                    .iter()
                    .map(|axis| axis.display_name().to_string())
                    .collect::<Vec<_>>()
            };
        for label in plot_labels {
            if !labels.iter().any(|existing| existing == &label) {
                labels.push(label);
            }
        }
    }
    labels
}

/// Wraps header fragments into the lines displayed by PlotMS.
pub fn wrap_header_segments(segments: &[String]) -> Vec<String> {
    const MAX_HEADER_LINE_CHARS: usize = 96;
    let mut lines = Vec::<String>::new();
    let mut current = String::new();
    for segment in segments {
        if current.is_empty() {
            current.push_str(segment);
            continue;
        }
        let candidate_len = current.len() + 3 + segment.len();
        if candidate_len > MAX_HEADER_LINE_CHARS {
            lines.push(current);
            current = segment.clone();
        } else {
            current.push_str("   ");
            current.push_str(segment);
        }
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines
}

pub(super) fn measurement_set_start_time(summary: &ListObsSummary) -> Option<f64> {
    summary.measurement_set.start_mjd_seconds.or_else(|| {
        summary
            .observations
            .iter()
            .find_map(|observation| observation.start_mjd_seconds)
    })
}

pub(super) fn first_non_empty<'a>(values: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
    values
        .into_iter()
        .map(str::trim)
        .find(|value| !value.is_empty())
}
