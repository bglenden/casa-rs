// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Typed plot payload prepared for one render/export step.
#[derive(Debug, Clone, PartialEq)]
pub enum MsPlotPayload {
    /// Reused `listobs` payload for metadata-oriented presets.
    ListObs(ListObsPlotPayload),
    /// Generic scatter payload for raw visibility plots.
    Scatter(MsScatterPlotPayload),
    /// Iterated multi-panel scatter payload for raw visibility plots.
    ScatterGrid(MsScatterGridPayload),
    /// Multi-plot scatter page payload for stacked/common paired views.
    ScatterPage(MsScatterPagePayload),
}

impl MsPlotPayload {}

/// Scatter payload for one raw MeasurementSet plot.
#[derive(Debug, Clone, PartialEq)]
pub struct MsScatterPlotPayload {
    /// Plot title used for export naming.
    pub title: String,
    /// X axis kind.
    pub x_axis: MsAxis,
    /// Y axis kind.
    pub y_axis: MsAxis,
    /// Optional right-side Y axis kind.
    pub secondary_y_axis: Option<MsAxis>,
    /// X axis label.
    pub x_label: String,
    /// Y axis label.
    pub y_label: String,
    /// Optional right-side Y axis label.
    pub secondary_y_label: Option<String>,
    /// Optional fixed X axis bounds.
    pub fixed_x_bounds: Option<(f64, f64)>,
    /// Optional fixed Y axis bounds.
    pub fixed_y_bounds: Option<(f64, f64)>,
    /// Optional fixed right-side Y axis bounds.
    pub secondary_fixed_y_bounds: Option<(f64, f64)>,
    /// Show a legend when multiple series are present.
    pub showlegend: bool,
    /// Legend placement.
    pub legend_position: MsLegendPosition,
    /// Show major grid lines.
    pub showmajorgrid: bool,
    /// Show minor grid lines.
    pub showminorgrid: bool,
    /// Optional scatter marker radius in rendered pixels.
    pub symbol_size_px: Option<u32>,
    /// Grouped scatter series.
    pub series: Vec<MsScatterSeries>,
    /// Resolved page header lines.
    pub header_lines: Vec<String>,
    /// Human-readable summary.
    pub summary: String,
}

/// One grouped scatter series.
#[derive(Debug, Clone, PartialEq)]
pub struct MsScatterSeries {
    /// Stable series label.
    pub label: String,
    /// Stable group key used for palette selection.
    pub color_group: String,
    /// Y axis rendered by this series.
    pub y_axis: MsAxis,
    /// Position of this ordinate in the plot request.
    pub y_index: usize,
    /// Plot points as `(x, y)`.
    pub points: Vec<(f64, f64)>,
    /// Provenance for each plotted point in `points`.
    pub provenance: Vec<MsScatterPointRef>,
    /// Display metadata for PlotMS points, when available.
    pub plotms_metadata: Vec<MsScatterPlotMsMetadata>,
    /// Frequencies in the PlotMS reporting frame, aligned with `points` when present.
    pub plotms_reporting_frequencies_hz: Vec<f64>,
    /// Shared display IDs indexed by PlotMS averaging order.
    pub plotms_averaged_ids: Option<MsScatterPlotMsAverageIds>,
}

/// MeasurementSet identifiers shared by the series of an averaged PlotMS plot.
#[derive(Debug, Clone, PartialEq)]
pub enum MsScatterPlotMsAverageIds {
    /// Scan and field IDs for scalar bins spanning original scans or fields.
    ScanAndField(Arc<[(i32, i32)]>),
    /// Output antenna IDs for per-antenna averages.
    Antenna(Arc<[i32]>),
    /// Shared `(scan, field, antenna)` IDs when antenna averaging spans scans or fields.
    ScanFieldAndAntenna(Arc<[(i32, i32, i32)]>),
    /// Shared spectral coordinates and identifiers for each averaged output row.
    SpectralWindow {
        /// Spectral coordinates for each averaged visibility buffer.
        groups: Arc<[Option<MsScatterPlotMsAverageSpw>]>,
        /// Buffer index for each output row in PlotMS export order.
        row_groups: Arc<[usize]>,
        /// Output antenna for per-antenna rows in the same order, when requested.
        antenna_ids: Option<Arc<[i32]>>,
    },
}

/// Display metadata for one PlotMS export coordinate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MsScatterPlotMsMetadata {
    /// Channel identifier after any channel averaging.
    pub channel: usize,
    /// Timestamp after time averaging, when enabled.
    pub average_time: Option<f64>,
    /// Position in the selected visibility iterator, when export needs its order.
    pub averaging_group: Option<usize>,
}

/// One plotted point's originating visibility sample range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MsScatterPointRef {
    /// MAIN row index.
    pub row: usize,
    /// Correlation slot within the row FLAG/DATA cell.
    pub corr: usize,
    /// Inclusive channel start for this plotted point.
    pub chan_start: usize,
    /// Exclusive channel end for this plotted point.
    pub chan_end: usize,
}

impl MsScatterPointRef {
    pub fn plotms_order(&self, iterator_slot: Option<usize>) -> (usize, usize, usize) {
        (
            iterator_slot.unwrap_or(self.row),
            self.chan_start,
            self.corr,
        )
    }
}

/// One iterated scatter panel within a multi-panel page.
#[derive(Debug, Clone, PartialEq)]
pub struct MsScatterPanelPayload {
    /// Stable panel key derived from the iteraxis value.
    pub key: String,
    /// Human-readable panel label.
    pub label: String,
    /// Grouped scatter series within the panel.
    pub series: Vec<MsScatterSeries>,
    /// Human-readable summary for the panel.
    pub summary: String,
}

/// Multi-panel scatter payload produced by iterated `msexplore` plots.
#[derive(Debug, Clone, PartialEq)]
pub struct MsScatterGridPayload {
    /// Page title used for export naming.
    pub title: String,
    /// X axis kind.
    pub x_axis: MsAxis,
    /// Y axis kind.
    pub y_axis: MsAxis,
    /// X axis label.
    pub x_label: String,
    /// Y axis label.
    pub y_label: String,
    /// Optional fixed X axis bounds.
    pub fixed_x_bounds: Option<(f64, f64)>,
    /// Optional fixed Y axis bounds.
    pub fixed_y_bounds: Option<(f64, f64)>,
    /// Show a legend when multiple series are present.
    pub showlegend: bool,
    /// Legend placement.
    pub legend_position: MsLegendPosition,
    /// Show major grid lines.
    pub showmajorgrid: bool,
    /// Show minor grid lines.
    pub showminorgrid: bool,
    /// Optional scatter marker radius in rendered pixels.
    pub symbol_size_px: Option<u32>,
    /// Iteration axis used to build the page.
    pub iteraxis: MsIterationAxis,
    /// Resolved page grid row count.
    pub gridrows: usize,
    /// Resolved page grid column count.
    pub gridcols: usize,
    /// Share X bounds across panels.
    pub share_x_bounds: bool,
    /// Share Y bounds across panels.
    pub share_y_bounds: bool,
    /// Ordered panel payloads.
    pub panels: Vec<MsScatterPanelPayload>,
    /// Resolved page header lines.
    pub header_lines: Vec<String>,
    /// Human-readable page summary.
    pub summary: String,
}

/// One scatter subplot placed on a multi-plot page.
#[derive(Debug, Clone, PartialEq)]
pub struct MsScatterPageItemPayload {
    /// Zero-based plot index on the page.
    pub plotindex: usize,
    /// Zero-based row placement.
    pub rowindex: usize,
    /// Zero-based column placement.
    pub colindex: usize,
    /// Child scatter plot payload.
    pub plot: MsScatterPlotPayload,
}

/// Multi-plot page payload composed from non-iterated scatter plots.
#[derive(Debug, Clone, PartialEq)]
pub struct MsScatterPagePayload {
    /// Page title used for export naming.
    pub title: String,
    /// Page export range behavior.
    pub exprange: MsPageExportRange,
    /// Resolved page grid row count.
    pub gridrows: usize,
    /// Resolved page grid column count.
    pub gridcols: usize,
    /// Resolved page header lines.
    pub header_lines: Vec<String>,
    /// Ordered child plots placed on the page.
    pub items: Vec<MsScatterPageItemPayload>,
    /// Human-readable page summary.
    pub summary: String,
}

pub(super) fn render_scatter_manifest(payload: &MsPlotPayload) -> Result<String, String> {
    match payload {
        MsPlotPayload::Scatter(payload) => {
            let mut out = String::new();
            out.push_str("# msexplore-manifest-v1\n");
            out.push_str(&format!("# title={}\n", payload.title));
            out.push_str(&format!("# x_axis={}\n", payload.x_axis.as_str()));
            out.push_str(&format!("# y_axis={}\n", payload.y_axis.as_str()));
            out.push_str(&format!("# legendposition={}\n", payload.legend_position));
            for line in &payload.header_lines {
                out.push_str(&format!("# header_line={line}\n"));
            }
            if let Some(secondary_y_axis) = payload.secondary_y_axis {
                out.push_str(&format!(
                    "# secondary_y_axis={}\n",
                    secondary_y_axis.as_str()
                ));
                out.push_str("series_key\tseries_label\ty_axis\tx\ty\n");
            } else {
                out.push_str("series_key\tseries_label\tx\ty\n");
            }
            for series in &payload.series {
                for (x, y) in &series.points {
                    if payload.secondary_y_axis.is_some() {
                        out.push_str(&format!(
                            "{}\t{}\t{}\t{:.12}\t{:.12}\n",
                            series.color_group,
                            series.label,
                            series.y_axis.as_str(),
                            x,
                            y
                        ));
                    } else {
                        out.push_str(&format!(
                            "{}\t{}\t{:.12}\t{:.12}\n",
                            series.color_group, series.label, x, y
                        ));
                    }
                }
            }
            Ok(out)
        }
        MsPlotPayload::ScatterGrid(payload) => {
            let mut out = String::new();
            out.push_str("# msexplore-manifest-v1\n");
            out.push_str(&format!("# title={}\n", payload.title));
            out.push_str(&format!("# x_axis={}\n", payload.x_axis.as_str()));
            out.push_str(&format!("# y_axis={}\n", payload.y_axis.as_str()));
            out.push_str(&format!("# iteraxis={}\n", payload.iteraxis.as_str()));
            out.push_str(&format!("# gridrows={}\n", payload.gridrows));
            out.push_str(&format!("# gridcols={}\n", payload.gridcols));
            out.push_str(&format!("# legendposition={}\n", payload.legend_position));
            out.push_str(&format!("# share_x_bounds={}\n", payload.share_x_bounds));
            out.push_str(&format!("# share_y_bounds={}\n", payload.share_y_bounds));
            for line in &payload.header_lines {
                out.push_str(&format!("# header_line={line}\n"));
            }
            out.push_str("panel_key\tpanel_label\tseries_key\tseries_label\tx\ty\n");
            for panel in &payload.panels {
                for series in &panel.series {
                    for (x, y) in &series.points {
                        out.push_str(&format!(
                            "{}\t{}\t{}\t{}\t{:.12}\t{:.12}\n",
                            panel.key, panel.label, series.color_group, series.label, x, y
                        ));
                    }
                }
            }
            Ok(out)
        }
        MsPlotPayload::ScatterPage(payload) => {
            let mut out = String::new();
            out.push_str("# msexplore-manifest-v1\n");
            out.push_str(&format!("# title={}\n", payload.title));
            out.push_str(&format!("# exprange={}\n", payload.exprange));
            out.push_str(&format!("# gridrows={}\n", payload.gridrows));
            out.push_str(&format!("# gridcols={}\n", payload.gridcols));
            for line in &payload.header_lines {
                out.push_str(&format!("# header_line={line}\n"));
            }
            out.push_str(
                "plotindex\trowindex\tcolindex\tplot_title\tx_axis\ty_axis\tseries_key\tseries_label\tx\ty\n",
            );
            for item in &payload.items {
                for series in &item.plot.series {
                    for (x, y) in &series.points {
                        out.push_str(&format!(
                            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.12}\t{:.12}\n",
                            item.plotindex,
                            item.rowindex,
                            item.colindex,
                            item.plot.title,
                            item.plot.x_axis.as_str(),
                            series.y_axis.as_str(),
                            series.color_group,
                            series.label,
                            x,
                            y
                        ));
                    }
                }
            }
            Ok(out)
        }
        MsPlotPayload::ListObs(_) => {
            Err("text manifest export requires a scatter payload".to_string())
        }
    }
}

pub(super) fn render_listobs_manifest(payload: &ListObsPlotPayload) -> Result<String, String> {
    match payload {
        ListObsPlotPayload::UvCoverage(payload) => {
            let mut out = String::new();
            out.push_str("# msexplore-manifest-v1\n");
            out.push_str("# payload=listobs_uv_coverage\n");
            out.push_str("# title=UV Coverage\n");
            out.push_str("# x_axis=u_lambda\n");
            out.push_str("# y_axis=v_lambda\n");
            out.push_str(&format!("# mirror={}\n", payload.mirror));
            out.push_str(&format!(
                "# axis_extent_lambda={:.12}\n",
                payload.axis_extent_lambda
            ));
            out.push_str(&format!("# tracks={}\n", payload.tracks.len()));
            out.push_str(&format!("# summary={}\n", payload.summary));
            out.push_str("series_key\tseries_label\tmirrored\tx\ty\n");
            for (track_index, track) in payload.tracks.iter().enumerate() {
                let series_key = format!("track-{track_index}");
                for (x, y) in &track.points {
                    out.push_str(&format!(
                        "{}\t{}\tfalse\t{:.12}\t{:.12}\n",
                        series_key, track.label, x, y
                    ));
                    if payload.mirror {
                        out.push_str(&format!(
                            "{}\t{}\ttrue\t{:.12}\t{:.12}\n",
                            series_key, track.label, -x, -y
                        ));
                    }
                }
            }
            Ok(out)
        }
        _ => Err("text export is currently available for UV coverage and raw msexplore scatter plots only".to_string()),
    }
}
