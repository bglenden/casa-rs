// SPDX-License-Identifier: LGPL-3.0-or-later
//! `listobs`-style MeasurementSet summaries.
//!
//! CASA's `listobs` task produces a rich human-readable summary, but its
//! returned Python dictionary contains only a subset of that information. This
//! module keeps a richer canonical Rust summary model and renders both
//! human-oriented text and machine-oriented JSON from the same data.
//!
//! The intent is:
//!
//! - CLI tools default to CASA-like text output.
//! - Automation and future bindings consume the structured summary directly or
//!   render it as JSON.
//! - The summary builder stays reusable so Python bindings do not need to parse
//!   terminal output later.
//!
//! The first implementation focuses on the common read-only `listobs` subset:
//! observation metadata, scan summaries, fields, spectral windows, sources,
//! antennas, polarization setups, and data-description mappings.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use casa_core::measures::MeasuresProvider;
use casa_core::measures::direction::DirectionRef;
use casa_core::measures::position::{MPosition, PositionRef};
use casa_core::quanta::{MvAngle, MvTime};
use casa_core::{ArrayValue, ScalarValue};
use casa_measures::table_measures::{MeasRefDesc, TableMeasDesc};
use casa_tables::Table;
use ndarray::IxDyn;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(test)]
use crate::selection::parser::{parse_correlation_selector, selection_from_options};
use crate::selection::{MsSelection, resolve_row_indices_with_system_budget};
use crate::subtables::measured_columns::selected_antenna_positions;
use crate::subtables::{
    FieldDirectionColumn, MetadataDirection, SubTable, antenna_observatory_offsets, get_i32,
    has_column,
};
use crate::tool_scan_summary::{take_double, take_integer};
use crate::{MeasurementSet, MsError, MsReadPlan, MsResult, MsSelectionIoBudget};

mod summary;

mod format;
mod scan_analysis;
mod uv_coverage;

pub use format::format_float_compact;
pub use format::stokes_name;
use format::*;
use scan_analysis::*;

const LISTOBS_SCHEMA_VERSION: u32 = 1;
const LISTOBS_UV_SCHEMA_VERSION: u32 = 1;
const SPEED_OF_LIGHT_M_S: f64 = 299_792_458.0;

/// Output formats supported by the `listobs` renderers and CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ListObsOutputFormat {
    /// CASA-like human-readable text.
    Text,
    /// Stable machine-readable JSON.
    Json,
}

/// Library-level `listobs` options shared by the CLI and future bindings.
///
/// This keeps the application layer thin: callers construct options once and
/// then reuse the same summary builder and renderers regardless of whether the
/// entrypoint is a standalone executable or a future Python binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ListObsOptions {
    /// Render the verbose CASA-style text layout.
    pub verbose: bool,
    /// Enable row filtering from the selection fields below.
    pub selectdata: bool,
    /// FIELD selector in CASA task syntax.
    pub field: Option<String>,
    /// SPECTRAL_WINDOW selector in CASA task syntax.
    pub spw: Option<String>,
    /// ANTENNA selector in CASA task syntax.
    pub antenna: Option<String>,
    /// SCAN selector in CASA task syntax.
    pub scan: Option<String>,
    /// OBSERVATION selector in CASA task syntax.
    pub observation: Option<String>,
    /// ARRAY selector in CASA task syntax.
    pub array: Option<String>,
    /// TIME selector in CASA task syntax.
    pub timerange: Option<String>,
    /// UV-distance selector in CASA task syntax.
    pub uvrange: Option<String>,
    /// Correlation selector in CASA task syntax.
    pub correlation: Option<String>,
    /// Intent selector in CASA task syntax.
    pub intent: Option<String>,
    /// Raw TaQL/MSSelection expression applied after the structured selectors.
    pub msselect: Option<String>,
    ///
    /// CASA's `listobs` task documents this as not yet implemented, so the
    /// current Rust implementation rejects it explicitly rather than silently
    /// accepting a no-op selector.
    ///
    /// Cf. CASA `casatasks/xml/listobs.xml`.
    /// FEED selector in CASA task syntax.
    pub feed: Option<String>,
    /// Include fractional unflagged row counts.
    pub listunfl: bool,
    ///
    /// CASA forwards this to the `MSMetaData` cache-size knob used by
    /// `MSSummary`. `casa-rs` does not yet have a matching reusable metadata
    /// cache layer for `listobs`, so this option currently errors instead of
    /// pretending to affect performance.
    ///
    /// Cf. C++ `casacore::MSMetaData(maxCacheSizeMB)` and `MSSummary`.
    /// CASA-style metadata cache size in MiB.
    pub cachesize_mb: Option<f32>,
}

impl Default for ListObsOptions {
    fn default() -> Self {
        Self {
            verbose: true,
            selectdata: true,
            field: None,
            spw: None,
            antenna: None,
            scan: None,
            observation: None,
            array: None,
            timerange: None,
            uvrange: None,
            correlation: None,
            intent: None,
            msselect: None,
            feed: None,
            listunfl: false,
            cachesize_mb: None,
        }
    }
}

impl ListObsOptions {
    /// Return `true` if any row-selection option was provided.
    pub fn has_selection(&self) -> bool {
        self.field.is_some()
            || self.spw.is_some()
            || self.antenna.is_some()
            || self.scan.is_some()
            || self.observation.is_some()
            || self.array.is_some()
            || self.timerange.is_some()
            || self.uvrange.is_some()
            || self.correlation.is_some()
            || self.intent.is_some()
            || self.msselect.is_some()
            || self.feed.is_some()
    }

    fn validate_supported(&self) -> MsResult<()> {
        if !self.selectdata && self.has_selection() {
            return Err(MsError::VersionError(
                "--selectdata=false cannot be combined with selection options".to_string(),
            ));
        }
        if self.feed.is_some() {
            return Err(MsError::VersionError(
                "feed selection is not implemented by CASA listobs and is not supported here"
                    .to_string(),
            ));
        }
        if self.cachesize_mb.is_some() {
            return Err(MsError::VersionError(
                "cachesize maps to CASA's experimental MSMetaData cache control and is not implemented yet for listobs"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

/// Structured `listobs` summary for a MeasurementSet.
///
/// `schema_version` is included so JSON consumers can pin their deserializers
/// to a known shape as this summary grows. New optional fields may be added in
/// future schema versions without changing the default text output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ListObsSummary {
    /// Version of the JSON schema emitted by [`render_json_pretty`](Self::render_json_pretty).
    pub schema_version: u32,
    /// Options applied while generating this summary.
    pub options: ListObsOptions,
    /// General information about the MeasurementSet being summarized.
    pub measurement_set: MeasurementSetInfo,
    /// OBSERVATION subtable rows.
    pub observations: Vec<ObservationSummary>,
    /// Aggregated MAIN-table scan summaries, grouped by observation, array, and scan number.
    pub scans: Vec<ScanSummary>,
    /// FIELD subtable rows.
    pub fields: Vec<FieldSummary>,
    /// POLARIZATION subtable rows.
    pub polarization_setups: Vec<PolarizationSummary>,
    /// DATA_DESCRIPTION subtable rows.
    pub data_descriptions: Vec<DataDescriptionSummary>,
    /// SPECTRAL_WINDOW subtable rows with derived polarization information.
    pub spectral_windows: Vec<SpectralWindowSummary>,
    /// SOURCE subtable rows.
    pub sources: Vec<SourceSummary>,
    /// ANTENNA subtable rows.
    pub antennas: Vec<AntennaSummary>,
}

/// Lazy-load UV coverage extracted for a `listobs` selection.
///
/// The payload is intentionally one-sided: it carries the observed baseline
/// samples only. Consumers can mirror `(-u, -v)` at render time when they want
/// the usual interferometric coverage display.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ListObsUvCoverage {
    /// Version of the JSON schema emitted by [`render_json_pretty`](Self::render_json_pretty).
    pub schema_version: u32,
    /// Options applied while generating this coverage payload.
    pub options: ListObsOptions,
    /// Filesystem path of the MeasurementSet root, if the MS is disk-backed.
    pub measurement_set_path: Option<String>,
    /// Axis label used by the rendered TUI plot.
    pub axis_unit: String,
    /// Whether the UI should mirror `(-u, -v)` for display.
    pub mirrored_display: bool,
    /// Total number of raw, one-sided samples in `tracks`.
    pub sample_count: usize,
    /// Maximum absolute extent across the `u` and `v` axes, in wavelengths.
    pub max_abs_uv_lambda: f64,
    /// Track groups sorted by `(antenna1, antenna2, field_id, spectral_window_id)`.
    pub tracks: Vec<ListObsUvTrack>,
}

/// One baseline/field/SPW UV track.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ListObsUvTrack {
    /// Baseline antenna 1 id.
    pub antenna1: i32,
    /// Baseline antenna 2 id.
    pub antenna2: i32,
    /// MAIN.FIELD_ID for the grouped rows.
    pub field_id: i32,
    /// SPECTRAL_WINDOW row used to scale these samples.
    pub spectral_window_id: i32,
    /// Representative center frequency used for the lambda conversion.
    pub center_frequency_hz: f64,
    /// Samples sorted by time.
    pub samples: Vec<ListObsUvPoint>,
}

/// One UVW sample converted to wavelengths.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ListObsUvPoint {
    /// MAIN-table row index.
    pub row: usize,
    /// MAIN.TIME in MJD seconds.
    pub time_mjd_seconds: f64,
    /// `u / lambda`.
    pub u_lambda: f64,
    /// `v / lambda`.
    pub v_lambda: f64,
    /// `w / lambda`.
    pub w_lambda: f64,
}

/// General metadata about the summarized MeasurementSet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MeasurementSetInfo {
    /// Filesystem path of the MeasurementSet root, if the MS is disk-backed.
    pub path: Option<String>,
    /// MS_VERSION keyword from the main table.
    pub ms_version: Option<f64>,
    /// Number of MAIN-table rows.
    pub row_count: usize,
    /// Number of OBSERVATION rows.
    pub observation_count: usize,
    /// Number of FIELD rows.
    pub field_count: usize,
    /// Number of SPECTRAL_WINDOW rows.
    pub spectral_window_count: usize,
    /// Number of POLARIZATION rows.
    pub polarization_count: usize,
    /// Number of DATA_DESCRIPTION rows.
    pub data_description_count: usize,
    /// Number of SOURCE rows.
    pub source_count: usize,
    /// Number of ANTENNA rows.
    pub antenna_count: usize,
    /// Total number of rows in the ANTENNA subtable before listobs filtering.
    pub antenna_table_count: usize,
    /// Reference frame for MAIN.TIME when it can be determined from `MEASINFO`.
    pub time_reference: Option<String>,
    /// Earliest observed time in MJD seconds.
    pub start_mjd_seconds: Option<f64>,
    /// Latest observed time in MJD seconds.
    pub end_mjd_seconds: Option<f64>,
    /// `end_mjd_seconds - start_mjd_seconds`, if both are known.
    pub total_elapsed_seconds: Option<f64>,
}

/// Summary of one OBSERVATION subtable row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ObservationSummary {
    /// Row index in the OBSERVATION subtable.
    pub observation_id: usize,
    /// Telescope or array name.
    pub telescope_name: String,
    /// Observer name.
    pub observer: String,
    /// Project string.
    pub project: String,
    /// Release date in MJD seconds.
    pub release_date_mjd_seconds: f64,
    /// Start of the observation's `TIME_RANGE`, if present.
    pub start_mjd_seconds: Option<f64>,
    /// End of the observation's `TIME_RANGE`, if present.
    pub end_mjd_seconds: Option<f64>,
}

/// Summary of one MAIN-table scan group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ScanSummary {
    /// OBSERVATION_ID for the grouped rows.
    pub observation_id: i32,
    /// ARRAY_ID for the grouped rows.
    pub array_id: i32,
    /// SCAN_NUMBER for the grouped rows.
    pub scan_number: i32,
    /// Total number of MAIN-table rows in this scan group.
    pub row_count: usize,
    /// Field ID for the grouped rows.
    pub field_id: i32,
    /// Field name for the grouped rows.
    pub field_name: String,
    /// Distinct field IDs present in the scan group.
    pub field_ids: Vec<i32>,
    /// Distinct field names present in the scan group.
    pub field_names: Vec<String>,
    /// Distinct DATA_DESC_ID values present in the scan group.
    pub data_description_ids: Vec<i32>,
    /// Distinct SPECTRAL_WINDOW_ID values derived from `data_description_ids`.
    pub spectral_window_ids: Vec<i32>,
    /// Distinct STATE_ID values present in the scan group.
    pub state_ids: Vec<i32>,
    /// Scan intents derived from the STATE subtable.
    pub scan_intents: Vec<String>,
    /// Earliest MAIN.TIME in the group, in MJD seconds.
    pub start_mjd_seconds: f64,
    /// Latest MAIN.TIME in the group, in MJD seconds.
    pub end_mjd_seconds: f64,
    /// Mean MAIN.EXPOSURE for the scan record's integration time, in seconds.
    pub mean_interval_seconds: f64,
    /// Mean MAIN.INTERVAL grouped by spectral window, aligned with `spectral_window_ids`.
    pub mean_interval_seconds_by_spw: Vec<f64>,
    /// Fractional count of unflagged rows in this scan grouping.
    pub unflagged_row_count: Option<f64>,
}

/// Summary of one FIELD subtable row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FieldSummary {
    /// Row index in the FIELD subtable.
    pub field_id: usize,
    /// Field name.
    pub name: String,
    /// Field code such as `"C"` for calibrator.
    pub code: String,
    /// SOURCE_ID reference from the FIELD row.
    pub source_id: i32,
    /// Number of MAIN-table rows that reference this field.
    pub row_count: usize,
    /// Fractional count of unflagged rows that reference this field.
    pub unflagged_row_count: Option<f64>,
    /// Time origin for the field direction polynomial, in MJD seconds.
    pub time_mjd_seconds: f64,
    /// Direction reference frame for `PHASE_DIR` when it can be reconstructed from `MEASINFO`.
    pub direction_reference: Option<String>,
    /// Phase-center direction as `[longitude, latitude]` in radians.
    pub phase_direction_radians: [f64; 2],
}

/// Summary of one POLARIZATION subtable row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PolarizationSummary {
    /// Row index in the POLARIZATION subtable.
    pub polarization_id: usize,
    /// Number of correlation products.
    pub num_correlations: i32,
    /// Correlation product names derived from the Stokes codes.
    pub correlation_types: Vec<String>,
}

/// Summary of one DATA_DESCRIPTION subtable row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct DataDescriptionSummary {
    /// Row index in the DATA_DESCRIPTION subtable.
    pub data_description_id: usize,
    /// Referenced SPECTRAL_WINDOW row index.
    pub spectral_window_id: i32,
    /// Referenced POLARIZATION row index.
    pub polarization_id: i32,
    /// Row flag.
    pub flagged: bool,
}

/// Summary of one SPECTRAL_WINDOW subtable row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SpectralWindowSummary {
    /// Row index in the SPECTRAL_WINDOW subtable.
    pub spectral_window_id: usize,
    /// Raw NAME column value.
    pub name: String,
    /// Number of channels.
    pub num_channels: i32,
    /// Frequency reference frame name.
    pub frame: Option<String>,
    /// First channel center frequency in Hz.
    pub first_channel_frequency_hz: f64,
    /// First channel width in Hz.
    pub channel_width_hz: f64,
    /// Reference frequency in Hz.
    pub reference_frequency_hz: f64,
    /// Center frequency in Hz.
    pub center_frequency_hz: f64,
    /// Lower spectral edge in Hz, including half-channel padding.
    pub min_frequency_hz: f64,
    /// Upper spectral edge in Hz, including half-channel padding.
    pub max_frequency_hz: f64,
    /// Total bandwidth in Hz.
    pub total_bandwidth_hz: f64,
    /// DATA_DESCRIPTION rows that point to this spectral window.
    pub data_description_ids: Vec<usize>,
    /// POLARIZATION rows reachable through the linked DATA_DESCRIPTION rows.
    pub polarization_ids: Vec<usize>,
    /// Distinct correlation-product names reachable through the linked polarization rows.
    pub correlation_types: Vec<String>,
}

/// Summary of one SOURCE subtable row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SourceSummary {
    /// Display ID for the SOURCE subtable row.
    pub source_id: i32,
    /// Source name.
    pub name: String,
    /// Source code.
    pub code: String,
    /// SPECTRAL_WINDOW_ID reference for the source row.
    pub spectral_window_id: i32,
    /// Calibration group.
    pub calibration_group: i32,
    /// Number of spectral lines.
    pub num_lines: i32,
    /// First rest frequency entry in Hz, when present.
    pub rest_frequency_hz: Option<f64>,
    /// First systemic velocity entry in m/s, when present.
    pub system_velocity_m_s: Option<f64>,
    /// Midpoint time for the source parameters, in MJD seconds.
    pub time_mjd_seconds: f64,
    /// Source direction as `[longitude, latitude]` in radians.
    pub direction_radians: [f64; 2],
}

/// Summary of one ANTENNA subtable row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AntennaSummary {
    /// Row index in the ANTENNA subtable.
    pub antenna_id: usize,
    /// Antenna name.
    pub name: String,
    /// Station name.
    pub station: String,
    /// Antenna type.
    pub antenna_type: String,
    /// Mount type.
    pub mount: String,
    /// Dish diameter in meters.
    pub dish_diameter_m: f64,
    /// Antenna longitude in radians.
    pub longitude_radians: f64,
    /// Antenna latitude in radians.
    pub latitude_radians: f64,
    /// Offset from the observatory position as `[east, north, elevation]` in meters.
    pub offset_from_observatory_m: [f64; 3],
    /// ITRF position in meters.
    pub position_m: [f64; 3],
}

#[derive(Debug, Default, Clone)]
struct MainUsage {
    field_row_counts: HashMap<i32, usize>,
    field_unflagged_row_counts: HashMap<i32, f64>,
    unflagged_row_fractions: Option<Vec<f64>>,
    used_field_ids: BTreeSet<usize>,
    used_observation_ids: BTreeSet<usize>,
    used_array_ids: BTreeSet<i32>,
    used_antenna_ids: BTreeSet<usize>,
    used_data_description_ids: BTreeSet<usize>,
    time_range: Option<(f64, f64)>,
    selected_row_count: usize,
    selection_applied: bool,
}

#[derive(Debug, Clone)]
struct UnflaggedRowContext {
    use_row_flag: bool,
    ddid_to_bandwidth_hz: HashMap<i32, f64>,
    ddid_to_channel_widths_hz: HashMap<i32, Vec<f64>>,
    flag_bytes_per_row: usize,
}

fn build_observations(
    ms: &MeasurementSet,
    selected_rows: Option<&[usize]>,
) -> MsResult<Vec<ObservationSummary>> {
    if matches!(selected_rows, Some(rows) if rows.is_empty()) {
        return Ok(Vec::new());
    }
    let used_observation_ids = selected_rows
        .map(|rows| collect_used_ids(ms.main_table(), rows, "OBSERVATION_ID"))
        .transpose()?
        .unwrap_or_default();
    let observation = ms.observation()?;
    let mut summaries = Vec::with_capacity(observation.row_count());
    for row in 0..observation.row_count() {
        if !used_observation_ids.is_empty() && !used_observation_ids.contains(&(row as i32)) {
            continue;
        }
        let (start_mjd_seconds, end_mjd_seconds) =
            extract_f64_pair(observation.array(row, "TIME_RANGE")?)?;
        summaries.push(ObservationSummary {
            observation_id: row,
            telescope_name: observation.string(row, "TELESCOPE_NAME")?,
            observer: observation.string(row, "OBSERVER")?,
            project: observation.string(row, "PROJECT")?,
            release_date_mjd_seconds: observation.f64(row, "RELEASE_DATE")?,
            start_mjd_seconds,
            end_mjd_seconds,
        });
    }
    Ok(summaries)
}

fn build_fields(
    ms: &MeasurementSet,
    main_usage: &MainUsage,
    tool_view: bool,
) -> MsResult<(Vec<FieldSummary>, Vec<MetadataDirection>)> {
    let field = ms.field()?;
    let mut summaries = Vec::with_capacity(field.row_count());
    let mut directions = Vec::new();
    for row in 0..field.row_count() {
        if (main_usage.selection_applied || !main_usage.used_field_ids.is_empty())
            && !main_usage.used_field_ids.contains(&row)
        {
            continue;
        }
        let (direction_reference, phase_direction_radians) = if tool_view {
            let measured =
                field.metadata_direction_at_seconds(row, FieldDirectionColumn::Phase, 0.0)?;
            let direction = &measured.direction;
            let reference = Some(direction.refer().as_str().to_string());
            let angles = [direction.longitude_rad(), direction.latitude_rad()];
            directions.push(measured);
            (reference, angles)
        } else {
            (
                column_measure_reference_for_row(field.table(), "PHASE_DIR", row)?,
                extract_direction_pair(field.phase_dir(row)?)?,
            )
        };
        summaries.push(FieldSummary {
            field_id: row,
            name: field.name(row)?,
            code: field.code(row)?,
            source_id: field.source_id(row)?,
            row_count: *main_usage.field_row_counts.get(&(row as i32)).unwrap_or(&0),
            unflagged_row_count: main_usage
                .field_unflagged_row_counts
                .get(&(row as i32))
                .copied(),
            time_mjd_seconds: field.time(row)?,
            direction_reference,
            phase_direction_radians,
        });
    }
    Ok((summaries, directions))
}

fn build_polarizations(ms: &MeasurementSet) -> MsResult<Vec<PolarizationSummary>> {
    let pol = ms.polarization()?;
    let mut summaries = Vec::with_capacity(pol.row_count());
    for row in 0..pol.row_count() {
        summaries.push(PolarizationSummary {
            polarization_id: row,
            num_correlations: pol.num_corr(row)?,
            correlation_types: pol
                .corr_type(row)?
                .into_iter()
                .map(stokes_name)
                .map(str::to_string)
                .collect(),
        });
    }
    Ok(summaries)
}

fn build_data_descriptions(
    ms: &MeasurementSet,
    main_usage: &MainUsage,
) -> MsResult<Vec<DataDescriptionSummary>> {
    let dd = ms.data_description()?;
    let mut summaries = Vec::with_capacity(dd.row_count());
    for row in 0..dd.row_count() {
        if (main_usage.selection_applied || !main_usage.used_data_description_ids.is_empty())
            && !main_usage.used_data_description_ids.contains(&row)
        {
            continue;
        }
        summaries.push(DataDescriptionSummary {
            data_description_id: row,
            spectral_window_id: dd.spectral_window_id(row)?,
            polarization_id: dd.polarization_id(row)?,
            flagged: dd.flag_row(row)?,
        });
    }
    Ok(summaries)
}

fn build_spectral_windows(
    ms: &MeasurementSet,
    data_descriptions: &[DataDescriptionSummary],
    polarization_setups: &[PolarizationSummary],
) -> MsResult<Vec<SpectralWindowSummary>> {
    let pol_lookup: HashMap<usize, &PolarizationSummary> = polarization_setups
        .iter()
        .map(|summary| (summary.polarization_id, summary))
        .collect();
    let mut dd_by_spw = BTreeMap::<i32, Vec<&DataDescriptionSummary>>::new();
    for dd in data_descriptions {
        dd_by_spw.entry(dd.spectral_window_id).or_default().push(dd);
    }

    let spw = ms.spectral_window()?;
    let mut summaries = Vec::with_capacity(spw.row_count());
    let frequency_refs = crate::columns::frequency_columns::ChanFreqColumn::new(spw.table());
    for row in 0..spw.row_count() {
        let linked_dds = dd_by_spw.get(&(row as i32)).cloned().unwrap_or_default();
        if linked_dds.is_empty() {
            continue;
        }
        let mut polarization_ids = BTreeSet::new();
        let mut correlation_types = Vec::new();
        let mut data_description_ids = Vec::with_capacity(linked_dds.len());
        for dd in linked_dds {
            data_description_ids.push(dd.data_description_id);
            if dd.polarization_id >= 0 {
                let pol_id = dd.polarization_id as usize;
                polarization_ids.insert(pol_id);
                if let Some(pol) = pol_lookup.get(&pol_id) {
                    for corr in &pol.correlation_types {
                        push_unique_string(&mut correlation_types, corr);
                    }
                }
            }
        }
        let chan_freq = spw.chan_freq(row)?;
        let chan_width = spw.chan_width(row)?;

        let (min_frequency_hz, max_frequency_hz) =
            spectral_window_frequency_bounds(&chan_freq, &chan_width);

        summaries.push(SpectralWindowSummary {
            spectral_window_id: row,
            name: spw.name(row)?,
            num_channels: spw.num_chan(row)?,
            frame: frequency_refs
                .frequency_reference(row)
                .ok()
                .map(|reference| reference.as_str().to_string()),
            first_channel_frequency_hz: chan_freq.first().copied().unwrap_or(0.0),
            channel_width_hz: chan_width.first().copied().unwrap_or(0.0),
            reference_frequency_hz: spw.ref_frequency(row)?,
            center_frequency_hz: mean_or_zero(&chan_freq),
            min_frequency_hz,
            max_frequency_hz,
            total_bandwidth_hz: spw.total_bandwidth(row)?,
            data_description_ids,
            polarization_ids: polarization_ids.into_iter().collect(),
            correlation_types,
        });
    }
    Ok(summaries)
}

fn build_sources(
    ms: &MeasurementSet,
    used_spw_ids: &BTreeSet<i32>,
) -> MsResult<Vec<SourceSummary>> {
    let source = match ms.source() {
        Ok(source) => source,
        Err(MsError::MissingSubtable(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut summaries = Vec::with_capacity(source.row_count());
    for row in 0..source.row_count() {
        let source_id = row as i32;
        let spw_id = source.i32(row, "SPECTRAL_WINDOW_ID")?;
        let matches_spw = spw_id < 0 || used_spw_ids.is_empty() || used_spw_ids.contains(&spw_id);
        if !matches_spw {
            continue;
        }
        let rest_frequency_hz =
            extract_first_f64_opt(source.optional_array(row, "REST_FREQUENCY")?)?
                .filter(|value| *value > 0.0);
        summaries.push(SourceSummary {
            source_id,
            name: source.string(row, "NAME")?,
            code: source.string(row, "CODE")?,
            spectral_window_id: spw_id,
            calibration_group: source.i32(row, "CALIBRATION_GROUP")?,
            num_lines: source.i32(row, "NUM_LINES")?,
            rest_frequency_hz,
            system_velocity_m_s: if rest_frequency_hz.is_some() {
                extract_first_f64_opt(source.optional_array(row, "SYSVEL")?)?
            } else {
                None
            },
            time_mjd_seconds: source.f64(row, "TIME")?,
            direction_radians: extract_direction_pair(source.array(row, "DIRECTION")?)?,
        });
    }
    Ok(summaries)
}

fn build_antennas(
    ms: &MeasurementSet,
    observations: &[ObservationSummary],
    main_usage: &MainUsage,
    tool_view: bool,
    verbose: bool,
    measures: Option<&OnceLock<Arc<dyn MeasuresProvider>>>,
) -> MsResult<Vec<AntennaSummary>> {
    let antenna = ms.antenna()?;
    let observatory = if !tool_view {
        observatory_position(observations, main_usage, &antenna)?
    } else if verbose && antenna.row_count() > 0 {
        let provider = measures_provider(measures)?;
        Some(
            ms.observation()?
                .observatory_position(0, provider.as_ref())?,
        )
    } else {
        None
    };
    let rows: Vec<_> = if main_usage.selection_applied || !main_usage.used_antenna_ids.is_empty() {
        main_usage
            .used_antenna_ids
            .range(..antenna.row_count())
            .copied()
            .collect()
    } else {
        (0..antenna.row_count()).collect()
    };
    let positions = if tool_view {
        selected_antenna_positions(antenna.table(), &rows)?
    } else {
        antenna
            .positions(&rows)?
            .into_iter()
            .map(|[x, y, z]| MPosition::new_itrf(x, y, z))
            .collect()
    };
    let mut summaries = Vec::with_capacity(rows.len());
    for (row, position) in rows.into_iter().zip(positions) {
        let position_m = position.as_itrf();
        let (longitude_radians, latitude_radians, offset_position) =
            report_position(&position, position_m);
        let offsets = match observatory.as_ref() {
            Some(center) if tool_view => {
                antenna_observatory_offsets(offset_position, center.as_itrf())
            }
            center => observatory_offset(position_m, center),
        };
        summaries.push(AntennaSummary {
            antenna_id: row,
            name: antenna.name(row)?,
            station: antenna.station(row)?,
            antenna_type: antenna.antenna_type(row)?,
            mount: antenna.mount(row)?,
            dish_diameter_m: antenna.dish_diameter(row)?,
            longitude_radians,
            latitude_radians,
            offset_from_observatory_m: offsets,
            position_m,
        });
    }
    Ok(summaries)
}

fn report_position(position: &MPosition, itrf: [f64; 3]) -> (f64, f64, [f64; 3]) {
    if position.refer() == PositionRef::WGS84 {
        // CASA displays converted ITRF coordinates, but its angle and offset
        // formulas use the vector represented in the original reference.
        let [longitude, latitude, radius] = position.values();
        let (sin_latitude, cos_latitude) = latitude.sin_cos();
        let (sin_longitude, cos_longitude) = longitude.sin_cos();
        return (
            longitude,
            latitude,
            [
                radius * cos_latitude * cos_longitude,
                radius * cos_latitude * sin_longitude,
                radius * sin_latitude,
            ],
        );
    }
    let (longitude, latitude, _) = position.as_spherical();
    (longitude, latitude, itrf)
}

fn observatory_position(
    observations: &[ObservationSummary],
    main_usage: &MainUsage,
    antenna: &crate::MsAntenna<'_>,
) -> MsResult<Option<MPosition>> {
    let telescope_name = observations
        .iter()
        .map(|obs| obs.telescope_name.as_str())
        .find(|name| !name.is_empty());
    if let Some(name) = telescope_name {
        let provider = crate::open_measures_runtime()?;
        if let Some(position) = MPosition::from_observatory_name(name, provider.as_ref())? {
            return Ok(Some(position));
        }
    }

    for row in 0..antenna.row_count() {
        if main_usage.used_antenna_ids.contains(&row) {
            continue;
        }
        let station = antenna.station(row)?;
        if station.ends_with(":OUT") {
            let position = antenna.position(row)?;
            return Ok(Some(MPosition::new_itrf(
                position[0],
                position[1],
                position[2],
            )));
        }
    }
    Ok(None)
}

fn measures_provider(
    cache: Option<&OnceLock<Arc<dyn MeasuresProvider>>>,
) -> MsResult<Arc<dyn MeasuresProvider>> {
    if let Some(provider) = cache.and_then(OnceLock::get) {
        return Ok(Arc::clone(provider));
    }
    let provider = crate::open_measures_runtime()?;
    Ok(match cache {
        Some(cache) => Arc::clone(cache.get_or_init(|| provider)),
        None => provider,
    })
}

fn observatory_offset(position_m: [f64; 3], observatory: Option<&MPosition>) -> [f64; 3] {
    let Some(observatory) = observatory else {
        return [0.0, 0.0, 0.0];
    };
    let [obs_x, obs_y, obs_z] = observatory.as_itrf();
    let delta = [
        position_m[0] - obs_x,
        position_m[1] - obs_y,
        position_m[2] - obs_z,
    ];
    let lon = observatory.longitude_rad();
    let lat = observatory.latitude_rad();
    let (sin_lon, cos_lon) = lon.sin_cos();
    let (sin_lat, cos_lat) = lat.sin_cos();
    let east = -sin_lon * delta[0] + cos_lon * delta[1];
    let north = -sin_lat * cos_lon * delta[0] - sin_lat * sin_lon * delta[1] + cos_lat * delta[2];
    let up = cos_lat * cos_lon * delta[0] + cos_lat * sin_lon * delta[1] + sin_lat * delta[2];
    [east, north, up]
}

fn extract_direction_pair(value: &ArrayValue) -> MsResult<[f64; 2]> {
    match value {
        ArrayValue::Float64(values) => {
            if values.ndim() == 1 && values.len() >= 2 {
                return Ok([
                    values
                        .get(IxDyn(&[0]))
                        .copied()
                        .ok_or_else(direction_shape_error)?,
                    values
                        .get(IxDyn(&[1]))
                        .copied()
                        .ok_or_else(direction_shape_error)?,
                ]);
            }
            if values.ndim() == 2 && values.shape().first() == Some(&2) && values.shape()[1] >= 1 {
                return Ok([
                    values
                        .get(IxDyn(&[0, 0]))
                        .copied()
                        .ok_or_else(direction_shape_error)?,
                    values
                        .get(IxDyn(&[1, 0]))
                        .copied()
                        .ok_or_else(direction_shape_error)?,
                ]);
            }
            let mut iter = values.iter().copied();
            let first = iter.next().ok_or_else(direction_shape_error)?;
            let second = iter.next().ok_or_else(direction_shape_error)?;
            Ok([first, second])
        }
        other => Err(MsError::ColumnTypeMismatch {
            column: "direction".to_string(),
            table: "listobs".to_string(),
            expected: "Float64 array".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

fn extract_f64_pair(value: &ArrayValue) -> MsResult<(Option<f64>, Option<f64>)> {
    match value {
        ArrayValue::Float64(values) => {
            if values.len() < 2 {
                return Ok((None, None));
            }
            let first = values.iter().next().copied();
            let second = values.iter().nth(1).copied();
            Ok((first, second))
        }
        other => Err(MsError::ColumnTypeMismatch {
            column: "array".to_string(),
            table: "listobs".to_string(),
            expected: "Float64 array".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

fn direction_shape_error() -> MsError {
    MsError::VersionError("direction array shape is too small to summarize".to_string())
}

fn first_non_empty<'a>(mut values: impl Iterator<Item = &'a str>) -> &'a str {
    values.find(|value| !value.is_empty()).unwrap_or("")
}

fn unique_count<T>(values: impl IntoIterator<Item = T>) -> usize
where
    T: Ord,
{
    values.into_iter().collect::<BTreeSet<_>>().len()
}

fn push_unique_string(values: &mut Vec<String>, value: &str) {
    if values.iter().any(|existing| existing == value) {
        return;
    }
    values.push(value.to_string());
}

fn spw_display_name(spw: &SpectralWindowSummary) -> String {
    if spw.name.is_empty() {
        "none".to_string()
    } else {
        spw.name.clone()
    }
}

#[cfg(test)]
mod tests;
