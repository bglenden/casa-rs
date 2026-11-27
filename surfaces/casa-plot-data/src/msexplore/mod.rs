// SPDX-License-Identifier: LGPL-3.0-or-later
//! Generic MeasurementSet plotting specifications and export helpers.
//!
//! `msexplore` is the next plotting layer above the curated MeasurementSet
//! metadata presets:
//!
//! - metadata summaries keep their fixed, compatibility-oriented preset catalog.
//! - `msexplore` exposes reusable plot specifications with explicit axes,
//!   selections, averaging, transforms, and export settings.
//! - Common curated raw-visibility plots are lowered into the generic
//!   `msexplore` scatter builder so new feature work lands in one place.
//!
//! The first delivery focuses on the most common MeasurementSet `plotms`
//! views backed by complex visibility samples:
//!
//! - amplitude/phase vs time
//! - amplitude/phase vs UV distance
//! - amplitude/phase vs channel/frequency
//! - real vs imaginary
//! - weight/sigma/flag vs time
//! - elevation/azimuth/hour angle/parallactic angle diagnostics
//!
//! Additional transform, multi-plot page composition, and staged flag-edit
//! fields are already modeled in the public specification types so future
//! waves can extend the implementation without redesigning the interface.

pub mod atmosphere;
pub mod plotms_averaged_colors;
pub mod plotms_columns;
mod plotms_iteration;
pub mod plotms_model;
pub mod plotms_ordinates;
mod plotms_page_headers;
pub mod plotms_phasecenter;
pub mod plotms_pointing;
pub mod plotms_row_axes;
mod plotms_spw;
mod plotms_time_baselines;

mod average_keys;
mod averaged_output;
mod averaged_projection;
mod averaged_scatter;
mod averaged_values;
mod axes;
mod budget;
mod flag_edit;
mod headers;
mod payload;
mod plot_requests;
mod raw_scatter;
mod selected_columns;
mod specification;
mod spectral;
mod visibility;

use average_keys::*;
use averaged_output::*;
use averaged_projection::*;
use averaged_scatter::*;
use averaged_values::*;
use budget::*;
use headers::*;
use payload::*;
use plot_requests::*;
use raw_scatter::*;
use selected_columns::*;
use spectral::*;
use visibility::*;

pub use axes::MsAxis;
pub use axes::MsColorAxis;
pub use axes::MsDataColumn;
pub use axes::MsExportFormat;
pub use axes::MsPlotPreset;
pub use axes::axis_name;
pub use axes::column_axis_name;
pub use flag_edit::MsFlagAction;
pub use flag_edit::MsFlagEditPreview;
pub use flag_edit::MsFlagRegion;
pub use flag_edit::MsFlagRowEdit;
pub use flag_edit::MsFlagSampleEdit;
pub use flag_edit::apply_msexplore_flag_edit;
pub use flag_edit::apply_msexplore_flag_edit_for_request;
pub use flag_edit::build_msexplore_payload_from_spec;
pub use flag_edit::clone_flag_matrix;
pub use flag_edit::preview_msexplore_flag_edit;
pub use flag_edit::preview_msexplore_flag_edit_for_request;
pub use headers::wrap_header_segments;
pub use payload::MsPlotPayload;
pub use payload::MsScatterGridPayload;
pub use payload::MsScatterPageItemPayload;
pub use payload::MsScatterPagePayload;
pub use payload::MsScatterPanelPayload;
pub use payload::MsScatterPlotMsAverageIds;
pub use payload::MsScatterPlotMsMetadata;
pub use payload::MsScatterPlotPayload;
pub use payload::MsScatterPointRef;
pub use payload::MsScatterSeries;
pub use plot_requests::build_calibrated_plotms_payload;
pub use plot_requests::build_msexplore_payload;
pub use plot_requests::build_msexplore_plot_payload;
pub use plot_requests::build_msexplore_plot_payload_from_path;
pub use plot_requests::build_uncalibrated_plotms_payload;
pub use plot_requests::export_msexplore_plot;
pub use selected_columns::selected_f64_values;
pub use selected_columns::selected_i32_values;
pub use specification::DEFAULT_MAX_PLOT_POINTS;
pub use specification::MsAverageSpec;
pub use specification::MsExploreSpec;
pub use specification::MsFlagEditSpec;
pub use specification::MsIterationAxis;
pub use specification::MsIterationSpec;
pub use specification::MsLayoutSpec;
pub use specification::MsLegendPosition;
pub use specification::MsPageExportRange;
pub use specification::MsPageHeaderItem;
pub use specification::MsPlotSpec;
pub use specification::MsPlotStyleSpec;
pub use specification::MsTransformSpec;
pub use spectral::ChannelBin;
pub use spectral::plot_selected_channel_bins;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use casa_core::measures::direction::MDirection;
use casa_core::measures::doppler::DopplerRef;
use casa_core::measures::frequency::{FrequencyRef, MFrequency};
use casa_core::quanta::{MvAngle, MvTime};
use casa_core::{ArrayValue, Complex64, ScalarValue, Value};
use casa_ms::columns::{
    frequency_columns::ChanFreqColumn,
    weight_columns::{SigmaSpectrumColumn, WeightSpectrumColumn},
};
use casa_ms::derived::engine::MsCalEngine;
use casa_ms::selection::parser::parse_correlation_selector;
use casa_ms::selection::resolve_row_indices_with_system_budget;
use casa_ms::selection::spw_expr::ChannelRange;
use casa_ms::subtables::SubTable;
use casa_ms::subtables::data_description::MsDataDescription;
use casa_ms::subtables::polarization::MsPolarization;
use casa_ms::{ListObsOptions, ListObsSummary, ListObsUvCoverage};
use casa_ms::{MeasurementSet, MeasurementSetSummaryOutputFormat, MsSelection};
use casa_ms::{convert_frequency_to_frame, parse_rest_frequency_hz, velocity_ms_from_frequency_hz};
use casa_mstransform::{
    PlotMsModelReferences, PlotMsModelSpwSelection, PlotTimeGroups, PlotTimePhaseReferences,
    TimeAverageOptions, TimeUvwAverage, WeightKind,
};
use casa_tables::Table;
use ndarray::{Ix1, Ix2};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::plot::{
    ListObsPlotKind, ListObsPlotPayload, ListObsPlotSpec, build_listobs_plot_payload_from_summary,
    build_listobs_uv_plot_payload,
};

pub use plotms_spw::MsScatterPlotMsAverageSpw;

/// Compute MODEL_DATA on the exact selected MAIN rows. Return None when the
/// physical or default model supplies all selected cells.
pub type PlotMsModelLoader = fn(
    &MeasurementSet,
    &[casa_ms::selection::ResolvedMsSelectionRow],
    Option<PlotMsModelReferences<'_>>,
) -> Result<Option<HashMap<usize, ArrayValue>>, String>;

/// UTC MJD midnight (in seconds) of the earliest finite timestamp.
pub fn reference_midnight(times: impl Iterator<Item = f64>) -> Option<i64> {
    times
        .filter(|time| time.is_finite())
        .reduce(f64::min)
        .map(|time| ((time / 86_400.0).floor() * 86_400.0) as i64)
}

const AVG_TIME_BUCKET_EPSILON_SECONDS: f64 = 0.002;

/// The `tracing` target of the visibility-scatter timing (`debug`).
const MSEXPLORE_TIMING: &str = "casa_ms::msexplore_timing";

#[derive(Default)]
struct VisibilityScatterTiming {
    row_grid: Duration,
    row_arrays: Duration,
    grouping: Duration,
    bin_collect: Duration,
    axis_compute: Duration,
    point_push: Duration,
}

impl VisibilityScatterTiming {
    /// Whether the logging configuration takes the timing events (`debug`,
    /// target [`MSEXPLORE_TIMING`]).
    fn enabled() -> bool {
        tracing::enabled!(target: MSEXPLORE_TIMING, tracing::Level::DEBUG)
    }

    fn checkpoint(label: &str, started: &mut Instant) {
        if Self::enabled() {
            let elapsed = started.elapsed();
            tracing::debug!(
                target: MSEXPLORE_TIMING,
                "msexplore timing: {label}={} ms",
                elapsed.as_millis()
            );
        }
        *started = Instant::now();
    }

    fn report(&self, label: &str, rows: usize, points: usize, started: Instant) {
        if !Self::enabled() {
            return;
        }
        tracing::debug!(
            target: MSEXPLORE_TIMING,
            "msexplore timing: {label} total={} ms rows={} points={} row_grid={} ms row_arrays={} ms grouping={} ms bin_collect={} ms axis_compute={} ms point_push={} ms",
            started.elapsed().as_millis(),
            rows,
            points,
            self.row_grid.as_millis(),
            self.row_arrays.as_millis(),
            self.grouping.as_millis(),
            self.bin_collect.as_millis(),
            self.axis_compute.as_millis(),
            self.point_push.as_millis()
        );
    }
}

/// One read-only PlotMS visibility row after calibration.
pub struct PlotMsCalibratedRow {
    /// Transient corrected visibility cell.
    pub corrected_data: ArrayValue,
    /// Data flags combined with the calibration flags.
    pub flags: ArrayValue,
    /// Calibrated per-correlation weights.
    pub weight: ArrayValue,
    /// Calibrated per-channel weights when the input defines a spectrum.
    pub weight_spectrum: Option<ArrayValue>,
}

/// Selected MAIN rows and their read-only calibration overlay.
pub struct PlotMsCalibration {
    /// Selected original MAIN rows in traversal order.
    pub row_order: Vec<usize>,
    /// Corrected visibility and flag cells keyed by their original MAIN row.
    pub rows: HashMap<usize, PlotMsCalibratedRow>,
    /// Channel ranges preserving the requested bounds and strides.
    pub channels: BTreeMap<i32, Vec<ChannelRange>>,
    /// The independent abscissa column when PlotMS has two column selectors.
    pub x_data_column: Option<MsDataColumn>,
}

#[derive(Clone, Copy)]
/// Visibility of valid and flagged symbols in the raw PlotMS scatter output.
pub struct RawSymbolVisibility {
    /// Whether to retain unflagged points in the plot.
    pub unflagged: bool,
    /// Whether to retain flagged points in the plot.
    pub flagged: bool,
}

/// CASA row and channel selection, optionally with transient calibrated arrays.
struct PlotMsInput<'a> {
    row_order: &'a [usize],
    header_row: Option<usize>,
    starts_visibility_buffer: &'a [bool],
    time_bins: Option<&'a PlotTimeGroups>,
    extended_derived: bool,
    channels: &'a BTreeMap<i32, Vec<ChannelRange>>,
    rows: Option<&'a HashMap<usize, PlotMsCalibratedRow>>,
    models: Option<&'a HashMap<usize, ArrayValue>>,
    x_data_column: Option<MsDataColumn>,
    secondary_y_data_column: Option<MsDataColumn>,
    additional_ordinates: &'a [(MsAxis, MsDataColumn)],
    weight_axis_columns: [Option<MsDataColumn>; 3],
    prefer_float_data: bool,
    raw_symbols: Option<RawSymbolVisibility>,
    pointing: Option<&'a plotms_pointing::PointingAxes>,
    phasecenter: Option<MDirection>,
}

/// Per-axis data-column choices and optional geometry for PlotMS.
pub struct PlotMsColumnSelection<'a> {
    /// X-axis data column override.
    pub x: Option<MsDataColumn>,
    /// Data column for the first secondary ordinate.
    pub secondary_y: Option<MsDataColumn>,
    /// Additional ordinate and column pairs in display order.
    pub additional_y: &'a [(MsAxis, MsDataColumn)],
    /// Per-axis choices governing visibility weights for the primary, secondary, and horizontal
    /// projections.
    pub weights: [Option<MsDataColumn>; 3],
    /// Optional filter controlling the visibility of flagged and valid raw symbols.
    pub raw_symbols: Option<RawSymbolVisibility>,
    /// Optional pointing frame and interpolation settings for plotted axes.
    pub pointing: Option<&'a plotms_pointing::PointingAxes>,
    /// Optional phase tracking center including its celestial reference frame.
    pub phasecenter: Option<MDirection>,
    /// Whether to permit the extended combinations of derived ordinates.
    pub extended_derived: bool,
    /// Whether a derived visibility expression can use an implicit model.
    pub implicit_derived_model: bool,
}

#[derive(Debug, Default)]
struct ScatterPanelAccumulator {
    label: String,
    iteration_rank: i32,
    series: std::collections::BTreeMap<String, MsScatterSeries>,
    contributing_rows: usize,
    contributing_points: usize,
}

struct VisibilityPredecimationContext<'a> {
    data_description: &'a MsDataDescription<'a>,
    spectral_window: &'a casa_ms::subtables::spectral_window::MsSpectralWindow<'a>,
    polarization: &'a MsPolarization<'a>,
    requested_corr_codes: Option<&'a [i32]>,
}

fn predecimate_visibility_rows(
    ms: &MeasurementSet,
    row_numbers: Vec<usize>,
    spec: &MsPlotSpec,
    point_budget: &PointBudget,
    context: VisibilityPredecimationContext<'_>,
) -> Result<Vec<usize>, String> {
    if !point_budget.allow_predecimation || row_numbers.len() <= 1 {
        return Ok(row_numbers);
    }
    let Some(max_plot_points) = point_budget.max_plot_points else {
        return Ok(row_numbers);
    };
    let Some(first_row) = row_numbers.first().copied() else {
        return Ok(row_numbers);
    };

    let ddid = selected_i32_values(ms.main_table(), "DATA_DESC_ID", &[first_row])?
        .into_iter()
        .next()
        .ok_or_else(|| format!("msexplore requires DATA_DESC_ID data for row {first_row}"))?;
    if ddid < 0 || (ddid as usize) >= context.data_description.row_count() {
        return Ok(row_numbers);
    }
    let ddid = ddid as usize;
    let spw_id = context
        .data_description
        .spectral_window_id(ddid)
        .map_err(|error| error.to_string())?;
    let chan_count = if spw_id >= 0 && (spw_id as usize) < context.spectral_window.row_count() {
        let num_chan = context
            .spectral_window
            .num_chan(spw_id as usize)
            .map_err(|error| error.to_string())?;
        usize::try_from(num_chan).unwrap_or(0)
    } else {
        0
    };
    if chan_count == 0 {
        return Ok(row_numbers);
    }
    let pol_id = context
        .data_description
        .polarization_id(ddid)
        .map_err(|error| error.to_string())?;
    let (corr_count, corr_types) =
        if pol_id >= 0 && (pol_id as usize) < context.polarization.row_count() {
            let pol_id = pol_id as usize;
            let corr_types = context
                .polarization
                .corr_type(pol_id)
                .map_err(|error| error.to_string())?;
            let num_corr = context
                .polarization
                .num_corr(pol_id)
                .map_err(|error| error.to_string())?;
            let corr_count = usize::try_from(num_corr)
                .ok()
                .filter(|count| *count > 0)
                .unwrap_or(corr_types.len());
            (corr_count, corr_types)
        } else {
            (0, Vec::new())
        };
    if corr_count == 0 {
        return Ok(row_numbers);
    }
    let selected_correlations =
        select_correlation_slots(corr_count, &corr_types, context.requested_corr_codes);
    if selected_correlations.is_empty() {
        return Ok(row_numbers);
    }
    let correlation_required = spec.x_axis.uses_correlation_slots()
        || spec
            .y_axes
            .iter()
            .copied()
            .any(MsAxis::uses_correlation_slots)
        || matches!(spec.color_by, MsColorAxis::Correlation)
        || matches!(spec.iteration.iteraxis, Some(MsIterationAxis::Correlation));
    let corr_points_per_row = if correlation_required {
        selected_correlations.len()
    } else {
        1
    };
    let channel_points_per_corr = plot_channel_bins(chan_count, spec)?.len();
    let y_points_per_bin = spec.y_axes.len().max(1);
    let estimated_points_per_row = corr_points_per_row
        .saturating_mul(channel_points_per_corr)
        .saturating_mul(y_points_per_bin);
    if estimated_points_per_row == 0 {
        return Ok(row_numbers);
    }

    let estimated_points = row_numbers.len().saturating_mul(estimated_points_per_row);
    if estimated_points <= max_plot_points {
        return Ok(row_numbers);
    }
    let target_rows = (max_plot_points / estimated_points_per_row).clamp(1, row_numbers.len());
    let sampled = sampled_indices(row_numbers.len(), target_rows)
        .into_iter()
        .map(|index| row_numbers[index])
        .collect();
    Ok(sampled)
}

fn build_stacked_amplitude_phase_time_page(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
    point_budget: &mut PointBudget,
) -> Result<MsPlotPayload, String> {
    if spec.iteration.iteraxis.is_some() {
        return Err(
            "msexplore stacked paired presets do not yet support nested iteraxis pages".to_string(),
        );
    }
    if spec.secondary_y_axis().is_some() {
        return Err(
            "msexplore stacked paired presets already define their subplot pairing".to_string(),
        );
    }
    if spec.layout.gridrows != 1
        || spec.layout.gridcols != 1
        || spec.layout.rowindex != 0
        || spec.layout.colindex != 0
        || spec.layout.plotindex != 0
    {
        return Err(
            "msexplore stacked paired presets currently manage their own fixed two-row page layout"
                .to_string(),
        );
    }

    let page_title = spec.style.title.clone().unwrap_or_else(|| {
        MsPlotPreset::AmplitudePhaseVsTimeStacked
            .display_name()
            .to_string()
    });
    let amplitude = build_stacked_page_child(
        ms,
        selection,
        spec,
        MsPlotPreset::AmplitudeVsTime,
        MsAxis::Amplitude,
        "Amplitude vs Time",
        point_budget,
    )?;
    let phase = build_stacked_page_child(
        ms,
        selection,
        spec,
        MsPlotPreset::PhaseVsTime,
        MsAxis::Phase,
        "Phase vs Time",
        point_budget,
    )?;

    Ok(MsPlotPayload::ScatterPage(MsScatterPagePayload {
        title: page_title,
        exprange: MsPageExportRange::Current,
        gridrows: 2,
        gridcols: 1,
        header_lines: Vec::new(),
        summary: format!(
            "{}. Plots=2 Data column={}",
            MsPlotPreset::AmplitudePhaseVsTimeStacked.display_name(),
            spec.data_column
        ),
        items: vec![
            MsScatterPageItemPayload {
                plotindex: 0,
                rowindex: 0,
                colindex: 0,
                plot: amplitude,
            },
            MsScatterPageItemPayload {
                plotindex: 1,
                rowindex: 1,
                colindex: 0,
                plot: phase,
            },
        ],
    }))
}

fn build_stacked_page_child(
    ms: &MeasurementSet,
    selection: &MsSelection,
    spec: &MsPlotSpec,
    preset: MsPlotPreset,
    y_axis: MsAxis,
    title: &str,
    point_budget: &mut PointBudget,
) -> Result<MsScatterPlotPayload, String> {
    let mut child = MsPlotSpec::from_preset(preset);
    child.data_column = spec.data_column;
    child.color_by = spec.color_by;
    child.averaging = spec.averaging.clone();
    child.transforms = spec.transforms.clone();
    child.style = spec.style.clone();
    child.style.title = Some(title.to_string());
    child.style.ylabel = None;
    child.y_axes = vec![y_axis];
    match build_generic_visibility_scatter(ms, selection, &child, point_budget, None)? {
        MsPlotPayload::Scatter(payload) => Ok(payload),
        _ => Err("internal error: stacked subplot lowered to a non-scatter payload".to_string()),
    }
}

/// Resolves native MeasurementSet row and channel selectors for PlotMS, propagating invalid
/// selection and table errors.
pub fn resolve_selected_rows_with_msselect(
    ms: &MeasurementSet,
    selection: &MsSelection,
    _listobs_options: &ListObsOptions,
) -> Result<Vec<usize>, String> {
    if !selection.selectdata {
        return Ok((0..ms.row_count()).collect());
    }
    resolve_row_indices_with_system_budget(ms, selection).map_err(|error| error.to_string())
}

fn fixed_bounds(axis: MsAxis) -> Option<(f64, f64)> {
    matches!(axis, MsAxis::Phase).then_some((-180.0, 180.0))
}

/// Returns the PlotMS display label for an axis, including its unit where applicable.
pub fn axis_label(axis: MsAxis) -> String {
    match axis {
        MsAxis::Time => "Time (MJD seconds)".to_string(),
        MsAxis::Interval => "Interval (s)".to_string(),
        MsAxis::UvDistance => "UV Distance (m)".to_string(),
        MsAxis::U => "U (m)".to_string(),
        MsAxis::V => "V (m)".to_string(),
        MsAxis::W => "W (m)".to_string(),
        axis if axis.uses_wavelengths() => format!("{} (λ)", axis.display_name()),
        MsAxis::Channel => "Channel".to_string(),
        MsAxis::Frequency => "Frequency (Hz)".to_string(),
        MsAxis::Velocity => "Velocity (km/s)".to_string(),
        MsAxis::RadialVelocity => "Radial Velocity (km/s)".to_string(),
        MsAxis::EphemerisDistance => "Distance (rho) (km)".to_string(),
        MsAxis::Azimuth => "Azimuth (deg)".to_string(),
        MsAxis::Elevation => "Elevation (deg)".to_string(),
        MsAxis::HourAngle => "Hour Angle (hours)".to_string(),
        MsAxis::ParallacticAngle => "Parallactic Angle (deg)".to_string(),
        axis if axis.is_antenna_horizontal() => format!("{} (deg)", axis.display_name()),
        MsAxis::Amplitude => "Amplitude".to_string(),
        MsAxis::Phase => "Phase (deg)".to_string(),
        MsAxis::Real => "Real".to_string(),
        MsAxis::Imaginary => "Imaginary".to_string(),
        MsAxis::Weight => "Weight".to_string(),
        MsAxis::Sigma => "Sigma".to_string(),
        MsAxis::WeightSpectrum => "Weight Spectrum".to_string(),
        MsAxis::SigmaSpectrum => "Sigma Spectrum".to_string(),
        MsAxis::Flag => "Flag".to_string(),
        MsAxis::FlagRow => "Flag Row".to_string(),
        _ => axis.display_name().to_string(),
    }
}

#[allow(clippy::too_many_arguments)]
fn visibility_group(
    color_by: MsColorAxis,
    field_id: i32,
    field: &casa_ms::subtables::MsField<'_>,
    spw_id: i32,
    spectral_window: &casa_ms::subtables::MsSpectralWindow<'_>,
    scan_number: i32,
    antenna1: i32,
    antenna2: i32,
    correlation_label: Option<&str>,
) -> (String, String) {
    match color_by {
        MsColorAxis::None => ("all".to_string(), "All data".to_string()),
        MsColorAxis::Field => {
            let field_name = if field_id >= 0 && (field_id as usize) < field.row_count() {
                field
                    .name(field_id as usize)
                    .unwrap_or_else(|_| format!("FIELD {field_id}"))
            } else {
                format!("FIELD {field_id}")
            };
            (format!("field-{field_id}"), field_name)
        }
        MsColorAxis::Scan => (format!("scan-{scan_number}"), format!("Scan {scan_number}")),
        MsColorAxis::SpectralWindow => {
            let spw_name = if spw_id >= 0 && (spw_id as usize) < spectral_window.row_count() {
                spectral_window
                    .name(spw_id as usize)
                    .unwrap_or_else(|_| format!("SPW {spw_id}"))
            } else {
                format!("SPW {spw_id}")
            };
            (format!("spw-{spw_id}"), spw_name)
        }
        MsColorAxis::Baseline => {
            let label = format!("a{antenna1}-a{antenna2}");
            (format!("baseline-{label}"), label)
        }
        MsColorAxis::Correlation => {
            let label = correlation_label.unwrap_or("corr").to_string();
            (format!("corr-{label}"), label)
        }
    }
}

fn iterates_plotms_ordinates(spec: &MsPlotSpec, input: Option<&PlotMsInput<'_>>) -> bool {
    input.is_some_and(|input| input.rows.is_none() || input.secondary_y_data_column.is_some())
        && spec.iteration.iteraxis.is_some()
        && spec.secondary_y_axis().is_some()
}

fn iteration_group(
    iteraxis: Option<MsIterationAxis>,
    field_id: i32,
    field: &casa_ms::subtables::MsField<'_>,
    spw_id: i32,
    spectral_window: &casa_ms::subtables::MsSpectralWindow<'_>,
    scan_number: i32,
    correlation: Option<(&str, i32)>,
) -> (String, String, i32) {
    match iteraxis {
        None
        | Some(MsIterationAxis::Antenna | MsIterationAxis::Baseline | MsIterationAxis::Time) => {
            ("all".to_string(), "All data".to_string(), 0)
        }
        Some(MsIterationAxis::Field) => {
            let field_name = if field_id >= 0 && (field_id as usize) < field.row_count() {
                field
                    .name(field_id as usize)
                    .unwrap_or_else(|_| format!("Field {field_id}"))
            } else {
                format!("Field {field_id}")
            };
            (format!("field-{field_id}"), field_name, field_id)
        }
        Some(MsIterationAxis::Scan) => (
            format!("scan-{scan_number}"),
            format!("Scan {scan_number}"),
            scan_number,
        ),
        Some(MsIterationAxis::SpectralWindow) => {
            let spw_name = if spw_id >= 0 && (spw_id as usize) < spectral_window.row_count() {
                spectral_window
                    .name(spw_id as usize)
                    .unwrap_or_else(|_| format!("SPW {spw_id}"))
            } else {
                format!("SPW {spw_id}")
            };
            (format!("spw-{spw_id}"), spw_name, spw_id)
        }
        Some(MsIterationAxis::Correlation) => {
            let (label, code) = correlation.unwrap_or(("corr", 0));
            (format!("corr-{label}"), label.to_string(), code)
        }
    }
}

fn scatter_series_identity(
    y_axis: MsAxis,
    multi_y: bool,
    base_key: &str,
    base_label: &str,
) -> (String, String, String) {
    if !multi_y {
        return (
            base_key.to_string(),
            base_label.to_string(),
            base_key.to_string(),
        );
    }
    let series_key = format!("{}::{base_key}", y_axis.as_str());
    let series_label = if base_key == "all" {
        y_axis.display_name().to_string()
    } else {
        format!("{} · {base_label}", y_axis.display_name())
    };
    (series_key.clone(), series_label, series_key)
}

fn resolve_iterated_grid(
    panel_count: usize,
    requested_rows: usize,
    requested_cols: usize,
) -> Result<(usize, usize), String> {
    if panel_count == 0 {
        return Err("msexplore iteration produced no populated panels".to_string());
    }
    let rows = requested_rows.max(1);
    let cols = requested_cols.max(1);
    if rows == 1 && cols == 1 {
        let auto_cols = (panel_count as f64).sqrt().ceil().max(1.0) as usize;
        let auto_rows = panel_count.div_ceil(auto_cols);
        return Ok((auto_rows, auto_cols));
    }
    if rows == 1 {
        return Ok((panel_count.div_ceil(cols), cols));
    }
    if cols == 1 {
        return Ok((rows, panel_count.div_ceil(rows)));
    }
    if rows * cols < panel_count {
        return Err(format!(
            "msexplore gridrows={rows} and gridcols={cols} cannot hold {panel_count} iterated panels"
        ));
    }
    Ok((rows, cols))
}

fn share_axis_bounds(self_scale: bool, shared_axis: bool) -> bool {
    shared_axis || !self_scale
}

#[cfg(test)]
mod tests;
