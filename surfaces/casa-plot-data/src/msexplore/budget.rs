// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[derive(Debug, Clone)]
pub(super) struct PointBudget {
    pub(super) max_plot_points: Option<usize>,
    pub(super) rendered_points: usize,
    pub(super) allow_predecimation: bool,
}

impl PointBudget {
    pub(super) fn unlimited() -> Self {
        Self {
            max_plot_points: None,
            rendered_points: 0,
            allow_predecimation: false,
        }
    }

    pub(super) fn limited(max_plot_points: usize, allow_predecimation: bool) -> Self {
        Self {
            max_plot_points: Some(max_plot_points),
            rendered_points: 0,
            allow_predecimation,
        }
    }

    pub(super) fn record_points(
        &mut self,
        additional_points: usize,
        context: &str,
    ) -> Result<(), String> {
        self.rendered_points = self
            .rendered_points
            .checked_add(additional_points)
            .ok_or_else(|| format!("{context} exceeded the supported plotted-point range"))?;
        Ok(())
    }
}

pub(super) fn allocate_series_point_quotas(lengths: &[usize], budget: usize) -> Vec<usize> {
    let total_points = lengths.iter().sum::<usize>();
    if total_points <= budget {
        return lengths.to_vec();
    }
    if budget == 0 {
        return vec![0; lengths.len()];
    }

    let mut quotas = vec![0usize; lengths.len()];
    let mut remainders = Vec::new();
    let mut assigned = 0usize;
    for (index, &length) in lengths.iter().enumerate() {
        if length == 0 {
            continue;
        }
        let scaled = length.saturating_mul(budget);
        let base = scaled / total_points;
        let remainder = scaled % total_points;
        quotas[index] = base;
        assigned += base;
        remainders.push((index, remainder, length));
    }

    remainders.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.2.cmp(&left.2)));
    let mut remaining = budget.saturating_sub(assigned);
    for (index, _, _) in remainders {
        if remaining == 0 {
            break;
        }
        quotas[index] += 1;
        remaining -= 1;
    }

    quotas
}

pub(super) fn sampled_indices(length: usize, take: usize) -> Vec<usize> {
    if take >= length {
        return (0..length).collect();
    }
    if take == 0 {
        return Vec::new();
    }
    if take == 1 {
        return vec![length / 2];
    }

    (0..take)
        .map(|slot| slot.saturating_mul(length.saturating_sub(1)) / (take - 1))
        .collect()
}

pub(super) fn compact_scatter_series(
    series: Vec<MsScatterSeries>,
    max_plot_points: usize,
) -> (Vec<MsScatterSeries>, usize, usize) {
    let lengths = series
        .iter()
        .map(|entry| entry.points.len())
        .collect::<Vec<_>>();
    let total_points = lengths.iter().sum::<usize>();
    if total_points <= max_plot_points {
        return (series, total_points, total_points);
    }

    let quotas = allocate_series_point_quotas(&lengths, max_plot_points);
    let mut kept_points = 0usize;
    let compacted = series
        .into_iter()
        .zip(quotas)
        .filter_map(|(entry, quota)| {
            if quota == 0 || entry.points.is_empty() {
                return None;
            }
            let indices = sampled_indices(entry.points.len(), quota);
            kept_points += indices.len();
            let points = indices.iter().map(|&index| entry.points[index]).collect();
            let provenance = indices
                .iter()
                .map(|&index| entry.provenance[index].clone())
                .collect();
            let plotms_metadata = if entry.plotms_metadata.is_empty() {
                Vec::new()
            } else {
                indices
                    .iter()
                    .map(|&index| entry.plotms_metadata[index])
                    .collect()
            };
            Some(MsScatterSeries {
                label: entry.label,
                color_group: entry.color_group,
                y_axis: entry.y_axis,
                y_index: entry.y_index,
                points,
                provenance,
                plotms_metadata,
                plotms_reporting_frequencies_hz: if entry.plotms_reporting_frequencies_hz.is_empty()
                {
                    Vec::new()
                } else {
                    indices
                        .iter()
                        .map(|&index| entry.plotms_reporting_frequencies_hz[index])
                        .collect()
                },
                plotms_averaged_ids: entry.plotms_averaged_ids,
            })
        })
        .collect();

    (compacted, total_points, kept_points)
}

pub(super) fn compact_payload_to_budget(
    payload: MsPlotPayload,
    point_budget: &PointBudget,
) -> Result<MsPlotPayload, String> {
    let Some(max_plot_points) = point_budget.max_plot_points else {
        return Ok(payload);
    };
    if point_budget.rendered_points <= max_plot_points {
        return Ok(payload);
    }

    let warning = format!(
        "Downsampled plot from {} to at most {} points using evenly spaced per-series decimation.",
        point_budget.rendered_points, max_plot_points
    );

    let payload = match payload {
        MsPlotPayload::ListObs(payload) => MsPlotPayload::ListObs(payload),
        MsPlotPayload::Scatter(mut payload) => {
            let (series, original_points, kept_points) =
                compact_scatter_series(payload.series, max_plot_points);
            if kept_points == 0 {
                return Err(format!(
                    "{} produced no drawable points after point-budget decimation",
                    payload.title
                ));
            }
            payload.series = series;
            payload.header_lines.push(warning.clone());
            payload.summary = format!(
                "{} Decimated points from {} to {}.",
                payload.summary, original_points, kept_points
            );
            MsPlotPayload::Scatter(payload)
        }
        MsPlotPayload::ScatterGrid(mut payload) => {
            let total_points = payload
                .panels
                .iter()
                .map(|panel| {
                    panel
                        .series
                        .iter()
                        .map(|series| series.points.len())
                        .sum::<usize>()
                })
                .sum::<usize>();
            let lengths = payload
                .panels
                .iter()
                .flat_map(|panel| panel.series.iter().map(|series| series.points.len()))
                .collect::<Vec<_>>();
            let quotas = allocate_series_point_quotas(&lengths, max_plot_points);
            let mut quota_iter = quotas.into_iter();
            let mut kept_points = 0usize;
            let mut kept_panels = Vec::new();
            for panel in std::mem::take(&mut payload.panels) {
                let mut kept_series = Vec::new();
                for series in panel.series {
                    let quota = quota_iter.next().unwrap_or(0);
                    let (mut compacted, _, kept) = compact_scatter_series(vec![series], quota);
                    kept_points += kept;
                    kept_series.append(&mut compacted);
                }
                if !kept_series.is_empty() {
                    let panel_point_count = kept_series
                        .iter()
                        .map(|series| series.points.len())
                        .sum::<usize>();
                    kept_panels.push(MsScatterPanelPayload {
                        key: panel.key,
                        label: panel.label,
                        summary: format!(
                            "{} Decimated points to {}.",
                            panel.summary, panel_point_count
                        ),
                        series: kept_series,
                    });
                }
            }
            if kept_points == 0 {
                return Err(format!(
                    "{} produced no drawable panels after point-budget decimation",
                    payload.title
                ));
            }
            payload.panels = kept_panels;
            payload.header_lines.push(warning.clone());
            payload.summary = format!(
                "{} Decimated points from {} to {}.",
                payload.summary, total_points, kept_points
            );
            MsPlotPayload::ScatterGrid(payload)
        }
        MsPlotPayload::ScatterPage(mut payload) => {
            let total_points = payload
                .items
                .iter()
                .map(|item| {
                    item.plot
                        .series
                        .iter()
                        .map(|series| series.points.len())
                        .sum::<usize>()
                })
                .sum::<usize>();
            let lengths = payload
                .items
                .iter()
                .flat_map(|item| item.plot.series.iter().map(|series| series.points.len()))
                .collect::<Vec<_>>();
            let quotas = allocate_series_point_quotas(&lengths, max_plot_points);
            let mut quota_iter = quotas.into_iter();
            let mut kept_points = 0usize;
            let mut kept_items = Vec::new();
            for mut item in std::mem::take(&mut payload.items) {
                let mut kept_series = Vec::new();
                for series in item.plot.series {
                    let quota = quota_iter.next().unwrap_or(0);
                    let (mut compacted, _, kept) = compact_scatter_series(vec![series], quota);
                    kept_points += kept;
                    kept_series.append(&mut compacted);
                }
                if !kept_series.is_empty() {
                    item.plot.series = kept_series;
                    item.plot.summary = format!(
                        "{} Decimated points to {}.",
                        item.plot.summary,
                        item.plot
                            .series
                            .iter()
                            .map(|series| series.points.len())
                            .sum::<usize>()
                    );
                    kept_items.push(item);
                }
            }
            if kept_points == 0 {
                return Err(format!(
                    "{} produced no drawable plots after point-budget decimation",
                    payload.title
                ));
            }
            payload.items = kept_items;
            payload.header_lines.push(warning);
            payload.summary = format!(
                "{} Decimated points from {} to {}.",
                payload.summary, total_points, kept_points
            );
            MsPlotPayload::ScatterPage(payload)
        }
    };

    Ok(payload)
}
