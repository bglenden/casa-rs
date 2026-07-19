// SPDX-License-Identifier: LGPL-3.0-or-later

//! Renderer-neutral MeasurementSet plot data shared by GUI and Python surfaces.

use serde::{Deserialize, Serialize};

use crate::{
    MsPlotPayload, MsPlotSpec, MsScatterPagePayload, MsScatterPlotPayload, MsScatterPointRef,
    MsScatterSeries, MsSelection, build_msexplore_plot_payload_from_path,
};

/// Version of the renderer-neutral plot-data contract.
pub const MS_PLOT_DATA_SCHEMA_VERSION: u32 = 1;

/// One numeric axis in a plot-data panel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MsPlotDataAxis {
    /// Stable axis identifier referenced by series.
    pub id: String,
    /// Human-readable axis label.
    pub label: String,
    /// Unit parsed from the axis label when available.
    pub unit: String,
    /// Lower numeric display bound.
    pub lower: f64,
    /// Upper numeric display bound.
    pub upper: f64,
}

/// Source location for one plotted visibility sample.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MsPlotDataPointProvenance {
    /// MAIN-table row index.
    pub row: usize,
    /// Correlation index within the row.
    pub corr: usize,
    /// Inclusive channel start.
    pub chan_start: usize,
    /// Exclusive channel end.
    pub chan_end: usize,
}

/// One editable numeric series in a plot-data panel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MsPlotDataSeries {
    /// Stable human-readable series label.
    pub label: String,
    /// Stable palette grouping key.
    pub color_group: String,
    /// Identifier of the Y axis used by this series.
    pub y_axis_id: String,
    /// Numeric X coordinates.
    pub x: Vec<f64>,
    /// Numeric Y coordinates paired with `x`.
    pub y: Vec<f64>,
    /// Optional source provenance paired with the numeric points.
    pub provenance: Vec<MsPlotDataPointProvenance>,
}

/// One panel in a renderer-neutral plot-data document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MsPlotDataPanel {
    /// Stable panel identifier.
    pub id: String,
    /// Human-readable panel title.
    pub title: String,
    /// Numeric axes used by this panel.
    pub axes: Vec<MsPlotDataAxis>,
    /// Default scatter-symbol diameter in renderer pixels.
    pub symbol_size: f64,
    /// Editable numeric series in this panel.
    pub series: Vec<MsPlotDataSeries>,
}

/// Canonical numeric plot data produced by `casa-ms`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MsPlotData {
    /// Plot-data contract version.
    pub schema_version: u32,
    /// Human-readable document title.
    pub title: String,
    /// Human-readable data preparation summary.
    pub summary: String,
    /// Resolved page header lines.
    pub header_lines: Vec<String>,
    /// Whether renderers should show a legend by default.
    pub show_legend: bool,
    /// Ordered plot panels.
    pub panels: Vec<MsPlotDataPanel>,
}

impl MsPlotData {
    /// Project an existing msexplore payload into the shared numeric contract.
    pub fn from_payload(payload: &MsPlotPayload) -> Result<Self, String> {
        match payload {
            MsPlotPayload::Scatter(scatter) => Ok(from_scatter(scatter)),
            MsPlotPayload::ScatterGrid(grid) => Ok(Self {
                schema_version: MS_PLOT_DATA_SCHEMA_VERSION,
                title: grid.title.clone(),
                summary: grid.summary.clone(),
                header_lines: grid.header_lines.clone(),
                show_legend: grid.showlegend,
                panels: grid
                    .panels
                    .iter()
                    .enumerate()
                    .map(|(index, panel)| {
                        panel_from_series(PanelProjectionSpec {
                            id: format!("panel-{index}"),
                            title: panel.label.clone(),
                            x_label: &grid.x_label,
                            y_label: &grid.y_label,
                            fixed_x_bounds: grid.fixed_x_bounds,
                            fixed_y_bounds: grid.fixed_y_bounds,
                            series: &panel.series,
                            symbol_size_px: grid.symbol_size_px,
                        })
                    })
                    .collect(),
            }),
            MsPlotPayload::ScatterPage(page) => Ok(from_page(page)),
            MsPlotPayload::ListObs(_) => Err(
                "the shared Python plot-data contract currently accepts visibility scatter payloads"
                    .to_owned(),
            ),
        }
    }
}

/// Build shared numeric data directly from a MeasurementSet path.
pub fn build_msexplore_plot_data_from_path(
    path: &std::path::Path,
    selection: &MsSelection,
    spec: &MsPlotSpec,
) -> Result<MsPlotData, String> {
    let payload = build_msexplore_plot_payload_from_path(path, selection, spec)?;
    MsPlotData::from_payload(&payload)
}

fn from_scatter(scatter: &MsScatterPlotPayload) -> MsPlotData {
    MsPlotData {
        schema_version: MS_PLOT_DATA_SCHEMA_VERSION,
        title: scatter.title.clone(),
        summary: scatter.summary.clone(),
        header_lines: scatter.header_lines.clone(),
        show_legend: scatter.showlegend,
        panels: vec![panel_from_series(PanelProjectionSpec {
            id: "main".to_owned(),
            title: scatter.title.clone(),
            x_label: &scatter.x_label,
            y_label: &scatter.y_label,
            fixed_x_bounds: scatter.fixed_x_bounds,
            fixed_y_bounds: scatter.fixed_y_bounds,
            series: &scatter.series,
            symbol_size_px: scatter.symbol_size_px,
        })],
    }
}

fn from_page(page: &MsScatterPagePayload) -> MsPlotData {
    MsPlotData {
        schema_version: MS_PLOT_DATA_SCHEMA_VERSION,
        title: page.title.clone(),
        summary: page.summary.clone(),
        header_lines: page.header_lines.clone(),
        show_legend: true,
        panels: page
            .items
            .iter()
            .map(|item| {
                panel_from_series(PanelProjectionSpec {
                    id: format!("plot-{}", item.plotindex),
                    title: item.plot.title.clone(),
                    x_label: &item.plot.x_label,
                    y_label: &item.plot.y_label,
                    fixed_x_bounds: item.plot.fixed_x_bounds,
                    fixed_y_bounds: item.plot.fixed_y_bounds,
                    series: &item.plot.series,
                    symbol_size_px: item.plot.symbol_size_px,
                })
            })
            .collect(),
    }
}

struct PanelProjectionSpec<'a> {
    id: String,
    title: String,
    x_label: &'a str,
    y_label: &'a str,
    fixed_x_bounds: Option<(f64, f64)>,
    fixed_y_bounds: Option<(f64, f64)>,
    series: &'a [MsScatterSeries],
    symbol_size_px: Option<u32>,
}

fn panel_from_series(spec: PanelProjectionSpec<'_>) -> MsPlotDataPanel {
    let (x_bounds, y_bounds) = spec
        .series
        .iter()
        .flat_map(|series| series.points.iter().copied())
        .fold((None, None), |(x_bounds, y_bounds), (x, y)| {
            (accumulate(x_bounds, x), accumulate(y_bounds, y))
        });
    let x_bounds = expand(spec.fixed_x_bounds.or(x_bounds).unwrap_or((0.0, 1.0)));
    let y_bounds = expand(spec.fixed_y_bounds.or(y_bounds).unwrap_or((0.0, 1.0)));
    MsPlotDataPanel {
        id: spec.id,
        title: spec.title,
        axes: vec![
            axis("x", spec.x_label, x_bounds),
            axis("y", spec.y_label, y_bounds),
        ],
        symbol_size: f64::from(spec.symbol_size_px.unwrap_or(3)),
        series: spec.series.iter().map(project_series).collect(),
    }
}

fn project_series(series: &MsScatterSeries) -> MsPlotDataSeries {
    let (x, y) = series.points.iter().copied().unzip();
    MsPlotDataSeries {
        label: series.label.clone(),
        color_group: series.color_group.clone(),
        y_axis_id: "y".to_owned(),
        x,
        y,
        provenance: series.provenance.iter().map(project_provenance).collect(),
    }
}

fn project_provenance(value: &MsScatterPointRef) -> MsPlotDataPointProvenance {
    MsPlotDataPointProvenance {
        row: value.row,
        corr: value.corr,
        chan_start: value.chan_start,
        chan_end: value.chan_end,
    }
}

fn axis(id: &str, label: &str, bounds: (f64, f64)) -> MsPlotDataAxis {
    MsPlotDataAxis {
        id: id.to_owned(),
        label: label.to_owned(),
        unit: label
            .rsplit_once('(')
            .and_then(|(_, suffix)| suffix.strip_suffix(')'))
            .unwrap_or_default()
            .to_owned(),
        lower: bounds.0,
        upper: bounds.1,
    }
}

fn accumulate(bounds: Option<(f64, f64)>, value: f64) -> Option<(f64, f64)> {
    if !value.is_finite() {
        return bounds;
    }
    Some(match bounds {
        Some((lower, upper)) => (lower.min(value), upper.max(value)),
        None => (value, value),
    })
}

fn expand((lower, upper): (f64, f64)) -> (f64, f64) {
    if lower < upper {
        return (lower, upper);
    }
    let padding = lower.abs().max(1.0) * 0.05;
    (lower - padding, upper + padding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MsAxis, MsLegendPosition};

    #[test]
    fn shared_scatter_contract_preserves_numeric_data_and_provenance() {
        let payload = MsPlotPayload::Scatter(MsScatterPlotPayload {
            title: "Amplitude vs time".into(),
            x_axis: MsAxis::Time,
            y_axis: MsAxis::Amplitude,
            secondary_y_axis: None,
            x_label: "Time (s)".into(),
            y_label: "Amplitude (Jy)".into(),
            secondary_y_label: None,
            fixed_x_bounds: None,
            fixed_y_bounds: None,
            secondary_fixed_y_bounds: None,
            showlegend: true,
            legend_position: MsLegendPosition::UpperRight,
            showmajorgrid: false,
            showminorgrid: false,
            symbol_size_px: Some(3),
            series: vec![MsScatterSeries {
                label: "field 0".into(),
                color_group: "field-0".into(),
                y_axis: MsAxis::Amplitude,
                points: vec![(1.0, 3.0), (2.0, 4.0)],
                provenance: vec![
                    MsScatterPointRef {
                        row: 4,
                        corr: 0,
                        chan_start: 0,
                        chan_end: 1,
                    },
                    MsScatterPointRef {
                        row: 5,
                        corr: 0,
                        chan_start: 0,
                        chan_end: 1,
                    },
                ],
            }],
            header_lines: Vec::new(),
            summary: "2 samples".into(),
        });

        let data = MsPlotData::from_payload(&payload).expect("project shared plot data");
        assert_eq!(data.schema_version, MS_PLOT_DATA_SCHEMA_VERSION);
        assert_eq!(data.panels[0].axes[0].unit, "s");
        assert_eq!(data.panels[0].series[0].x, vec![1.0, 2.0]);
        assert_eq!(data.panels[0].series[0].y, vec![3.0, 4.0]);
        assert_eq!(data.panels[0].series[0].provenance[1].row, 5);
    }
}
