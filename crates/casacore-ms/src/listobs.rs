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

use casacore_tables::Table;
use casacore_tables::table_measures::{MeasRefDesc, TableMeasDesc};
use casacore_types::ArrayValue;
use casacore_types::measures::frequency::FrequencyRef;
use casacore_types::measures::position::MPosition;
use casacore_types::quanta::{MvAngle, MvTime};
use ndarray::IxDyn;
use serde::{Deserialize, Serialize};

use crate::selection::MsSelection;
use crate::subtables::{SubTable, get_f64, get_i32, has_column};
use crate::{MeasurementSet, MsError, MsResult};

/// Output formats supported by the `listobs` renderers and CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListObsOutputFormat {
    /// CASA-like human-readable text.
    Text,
    /// Stable machine-readable JSON.
    Json,
}

impl ListObsOutputFormat {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            other => Err(format!(
                "unsupported format {other:?}; expected one of: text, json"
            )),
        }
    }
}

/// Library-level `listobs` options shared by the CLI and future bindings.
///
/// This keeps the application layer thin: callers construct options once and
/// then reuse the same summary builder and renderers regardless of whether the
/// entrypoint is a standalone executable or a future Python binding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// General metadata about the summarized MeasurementSet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Mean MAIN.INTERVAL across the grouped rows, in seconds.
    pub mean_interval_seconds: f64,
    /// Mean MAIN.INTERVAL grouped by spectral window, aligned with `spectral_window_ids`.
    pub mean_interval_seconds_by_spw: Vec<f64>,
    /// Fractional count of unflagged rows in this scan grouping.
    pub unflagged_row_count: Option<f64>,
}

/// Summary of one FIELD subtable row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// First phase-center direction coefficient as `[longitude, latitude]` in radians.
    pub phase_direction_radians: [f64; 2],
}

/// Summary of one POLARIZATION subtable row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolarizationSummary {
    /// Row index in the POLARIZATION subtable.
    pub polarization_id: usize,
    /// Number of correlation products.
    pub num_correlations: i32,
    /// Correlation product names derived from the Stokes codes.
    pub correlation_types: Vec<String>,
}

/// Summary of one DATA_DESCRIPTION subtable row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceSummary {
    /// SOURCE_ID column value.
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    ddid_to_bandwidth_hz: HashMap<i32, f64>,
    ddid_to_channel_widths_hz: HashMap<i32, Vec<f64>>,
}

impl ListObsSummary {
    /// Build a structured `listobs` summary from an open [`MeasurementSet`].
    pub fn from_ms(ms: &MeasurementSet) -> MsResult<Self> {
        Self::from_ms_with_options(ms, &ListObsOptions::default())
    }

    /// Build a structured `listobs` summary from an open [`MeasurementSet`]
    /// using explicit task-style options.
    pub fn from_ms_with_options(ms: &MeasurementSet, options: &ListObsOptions) -> MsResult<Self> {
        options.validate_supported()?;
        let selected_rows = resolve_selected_rows(ms, options)?;
        let unflagged_context = if options.listunfl {
            Some(UnflaggedRowContext::new(ms)?)
        } else {
            None
        };
        let observations = build_observations(ms, selected_rows.as_deref())?;
        let main_usage =
            analyze_main_table(ms, selected_rows.as_deref(), unflagged_context.as_ref())?;
        let fields = build_fields(ms, &main_usage)?;
        let polarization_setups = build_polarizations(ms)?;
        let data_descriptions = build_data_descriptions(ms, &main_usage)?;
        let spectral_windows =
            build_spectral_windows(ms, &data_descriptions, &polarization_setups)?;
        let used_source_ids = fields
            .iter()
            .filter_map(|field| (field.source_id >= 0).then_some(field.source_id))
            .collect::<BTreeSet<_>>();
        let used_spw_ids = spectral_windows
            .iter()
            .map(|summary| summary.spectral_window_id as i32)
            .collect::<BTreeSet<_>>();
        let sources = build_sources(ms, &used_source_ids, &used_spw_ids)?;
        let antennas = build_antennas(ms, &observations, &main_usage)?;

        let field_name_lookup: HashMap<i32, String> = fields
            .iter()
            .map(|field| (field.field_id as i32, field.name.clone()))
            .collect();
        let state_intent_lookup = build_state_intent_lookup(ms)?;
        let dd_to_spw: HashMap<i32, i32> = data_descriptions
            .iter()
            .map(|dd| (dd.data_description_id as i32, dd.spectral_window_id))
            .collect();
        let scans = build_scans(
            ms,
            selected_rows.as_deref(),
            &field_name_lookup,
            &dd_to_spw,
            &state_intent_lookup,
            unflagged_context.as_ref(),
        )?;

        let displayed_polarizations = unique_count(
            spectral_windows
                .iter()
                .flat_map(|summary| summary.polarization_ids.iter().copied()),
        );
        let time_range = main_usage.time_range;
        let (start_mjd_seconds, end_mjd_seconds, total_elapsed_seconds) = time_range
            .map(|(start, end)| (Some(start), Some(end), Some(end - start)))
            .unwrap_or((None, None, None));

        Ok(Self {
            schema_version: 1,
            options: options.clone(),
            measurement_set: MeasurementSetInfo {
                path: ms.path().map(|path| path.display().to_string()),
                ms_version: ms.ms_version(),
                row_count: main_usage.selected_row_count,
                observation_count: observations.len(),
                field_count: fields.len(),
                spectral_window_count: spectral_windows.len(),
                polarization_count: displayed_polarizations,
                data_description_count: data_descriptions.len(),
                source_count: sources.len(),
                antenna_count: antennas.len(),
                antenna_table_count: ms.antenna().map(|antenna| antenna.row_count()).unwrap_or(0),
                time_reference: main_time_reference(ms.main_table()),
                start_mjd_seconds,
                end_mjd_seconds,
                total_elapsed_seconds,
            },
            observations,
            scans,
            fields,
            polarization_setups,
            data_descriptions,
            spectral_windows,
            sources,
            antennas,
        })
    }

    /// Open an on-disk MeasurementSet and summarize it.
    pub fn from_path(path: impl AsRef<Path>) -> MsResult<Self> {
        Self::from_path_with_options(path, &ListObsOptions::default())
    }

    /// Open an on-disk MeasurementSet and summarize it using explicit
    /// task-style options.
    pub fn from_path_with_options(
        path: impl AsRef<Path>,
        options: &ListObsOptions,
    ) -> MsResult<Self> {
        let ms = MeasurementSet::open(path)?;
        Self::from_ms_with_options(&ms, options)
    }

    /// Render the summary as pretty-printed JSON.
    pub fn render_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Render the summary as CASA-style text.
    pub fn render_text(&self) -> String {
        self.render_text_with_options(&self.options)
    }

    /// Render the summary as CASA-style text using explicit render options.
    pub fn render_text_with_options(&self, options: &ListObsOptions) -> String {
        let mut out = String::new();
        let name = self
            .measurement_set
            .path
            .as_deref()
            .unwrap_or("<memory MeasurementSet>");
        let ms_version = self
            .measurement_set
            .ms_version
            .map(|value| format!("{value:.0}"))
            .unwrap_or_else(|| "?".to_string());
        let observer = first_non_empty(self.observations.iter().map(|obs| obs.observer.as_str()));
        let project = first_non_empty(self.observations.iter().map(|obs| obs.project.as_str()));
        let telescope = first_non_empty(
            self.observations
                .iter()
                .map(|obs| obs.telescope_name.as_str()),
        );
        let time_ref = self
            .measurement_set
            .time_reference
            .as_deref()
            .unwrap_or("unknown");

        let _ = writeln!(
            out,
            "================================================================================"
        );
        let _ = writeln!(
            out,
            "MeasurementSet Name:  {name}      MS Version {ms_version}"
        );
        let _ = writeln!(
            out,
            "================================================================================"
        );
        let _ = writeln!(out, "Observer: {observer}     Project: {project}");
        if options.verbose {
            let _ = writeln!(out, "Observation: {telescope}");
        } else {
            let _ = writeln!(
                out,
                "Observation: {telescope}({} antennas)",
                self.measurement_set.antenna_table_count
            );
        }
        let _ = writeln!(
            out,
            "Data records: {}       Total elapsed time = {}",
            self.measurement_set.row_count,
            format_elapsed(self.measurement_set.total_elapsed_seconds)
        );
        if let Some((start, end)) = self
            .measurement_set
            .start_mjd_seconds
            .zip(self.measurement_set.end_mjd_seconds)
        {
            let start = MvTime::from_mjd_seconds(start);
            let end = MvTime::from_mjd_seconds(end);
            let _ = writeln!(
                out,
                "Observed from   {}   to   {} ({time_ref})",
                start.format_dmy(1),
                end.format_dmy(1),
            );
        }
        let _ = writeln!(out);

        if options.verbose && !self.scans.is_empty() {
            let mut grouped = BTreeMap::<(i32, i32), Vec<&ScanSummary>>::new();
            for scan in &self.scans {
                grouped
                    .entry((scan.observation_id, scan.array_id))
                    .or_default()
                    .push(scan);
            }

            for ((observation_id, array_id), scans) in grouped {
                let _ = writeln!(
                    out,
                    "ObservationID = {observation_id}         ArrayID = {array_id}"
                );
                let _ = writeln!(
                    out,
                    "{}",
                    if options.listunfl {
                        format!(
                            "Date        Timerange ({time_ref})          Scan  FldId FieldName             nRows     nUnflRows   SpwIds   Average Interval(s)    ScanIntent"
                        )
                    } else {
                        format!(
                            "Date        Timerange ({time_ref})          Scan  FldId FieldName             nRows     SpwIds   Average Interval(s)    ScanIntent"
                        )
                    }
                );
                let mut previous_day = None;
                for scan in scans {
                    let scan_start = MvTime::from_mjd_seconds(scan.start_mjd_seconds);
                    let scan_end = MvTime::from_mjd_seconds(scan.end_mjd_seconds);
                    let day = scan_start.format_dmy_date();
                    let time_start = if previous_day == Some(day.clone()) {
                        scan_start.format_time(1)
                    } else {
                        scan_start.format_dmy(1)
                    };
                    let spw_ids = format_i32_bracket_list(&scan.spectral_window_ids);
                    let mean_intervals =
                        format_f64_bracket_list(&scan.mean_interval_seconds_by_spw);
                    let scan_intents = join_or_blank(&scan.scan_intents);
                    if options.listunfl {
                        let _ = writeln!(
                            out,
                            "  {:<22} - {:<10} {:>4} {:>6} {:<20} {:>10} {:>13}  {:<8} {:<23} {}",
                            time_start,
                            scan_end.format_time(1),
                            scan.scan_number,
                            scan.field_id,
                            truncate_for_column(&scan.field_name, 20),
                            scan.row_count,
                            format_float_compact(scan.unflagged_row_count.unwrap_or(0.0), 2),
                            spw_ids,
                            mean_intervals,
                            scan_intents,
                        );
                    } else {
                        let _ = writeln!(
                            out,
                            "  {:<22} - {:<10} {:>4} {:>6} {:<20} {:>10}  {:<8} {:<23} {}",
                            time_start,
                            scan_end.format_time(1),
                            scan.scan_number,
                            scan.field_id,
                            truncate_for_column(&scan.field_name, 20),
                            scan.row_count,
                            spw_ids,
                            mean_intervals,
                            scan_intents,
                        );
                    }
                    previous_day = Some(day);
                }
                let _ = writeln!(out, "           (nRows = Total number of rows per scan) ");
                let _ = writeln!(out);
            }
        }

        let _ = writeln!(out, "Fields: {}", self.fields.len());
        let _ = writeln!(
            out,
            "{}",
            if options.listunfl {
                "  ID   Code Name                RA               Decl           Epoch   SrcId      nRows    nUnflRows"
            } else {
                "  ID   Code Name                RA               Decl           Epoch   SrcId      nRows"
            }
        );
        for field in &self.fields {
            if options.listunfl {
                let _ = writeln!(
                    out,
                    "  {:<5}{:<5}{:<20}{:<16}{:<16}{:<8}{:<6}{:>10}{:>13}",
                    field.field_id,
                    truncate_for_column(&field.code, 4),
                    truncate_for_column(&field.name, 20),
                    MvAngle::from_radians(field.phase_direction_radians[0])
                        .normalized(0.0)
                        .format_time(6),
                    MvAngle::from_radians(field.phase_direction_radians[1]).format_angle_dig2(5),
                    field.direction_reference.as_deref().unwrap_or("?"),
                    field.source_id,
                    field.row_count,
                    format_float_compact(field.unflagged_row_count.unwrap_or(0.0), 2),
                );
            } else {
                let _ = writeln!(
                    out,
                    "  {:<5}{:<5}{:<20}{:<16}{:<16}{:<8}{:<6}{:>10}",
                    field.field_id,
                    truncate_for_column(&field.code, 4),
                    truncate_for_column(&field.name, 20),
                    MvAngle::from_radians(field.phase_direction_radians[0])
                        .normalized(0.0)
                        .format_time(6),
                    MvAngle::from_radians(field.phase_direction_radians[1]).format_angle_dig2(5),
                    field.direction_reference.as_deref().unwrap_or("?"),
                    field.source_id,
                    field.row_count,
                );
            }
        }
        let _ = writeln!(out);

        let _ = writeln!(
            out,
            "Spectral Windows: ({} unique spectral windows and {} unique polarization setups)",
            self.spectral_windows.len(),
            self.polarization_setups.len(),
        );
        let _ = writeln!(
            out,
            "  SpwID  Name   #Chans   Frame   Ch0(MHz)  ChanWid(kHz)  TotBW(kHz) CtrFreq(MHz)  Corrs"
        );
        for spw in &self.spectral_windows {
            let _ = writeln!(
                out,
                "  {:<7}{:<6}{:>8}   {:<6}{:>10.3}{:>14.3}{:>12.1}{:>13.4}   {}",
                spw.spectral_window_id,
                truncate_for_column(&spw_display_name(spw), 6),
                spw.num_channels,
                spw.frame.as_deref().unwrap_or("?"),
                spw.first_channel_frequency_hz / 1.0e6,
                spw.channel_width_hz / 1.0e3,
                spw.total_bandwidth_hz / 1.0e3,
                spw.center_frequency_hz / 1.0e6,
                join_with_padding(&spw.correlation_types),
            );
        }
        let _ = writeln!(out);

        if options.verbose {
            let _ = writeln!(out, "Sources: {}", self.sources.len());
            let _ = writeln!(
                out,
                "  ID   Name                SpwId RestFreq(MHz)  SysVel(km/s) "
            );
            for source in &self.sources {
                let _ = writeln!(
                    out,
                    "  {:<5}{:<20}{:<6}{:<15}{:<13}",
                    source.source_id,
                    truncate_for_column(&source.name, 20),
                    if source.spectral_window_id < 0 {
                        "any".to_string()
                    } else {
                        source.spectral_window_id.to_string()
                    },
                    source
                        .rest_frequency_hz
                        .map(|value| format!("{:.6}", value / 1.0e6))
                        .unwrap_or_else(|| "-".to_string()),
                    source
                        .system_velocity_m_s
                        .map(|value| format_float_compact(value / 1.0e3, 3))
                        .unwrap_or_else(|| "-".to_string()),
                );
            }
            let _ = writeln!(out);
        }

        if options.verbose {
            let _ = writeln!(out, "Antennas: {}:", self.antennas.len());
            let _ = writeln!(
                out,
                "  ID   Name  Station   Diam.    Long.         Lat.                Offset from array center (m)                ITRF Geocentric coordinates (m)        "
            );
            let _ = writeln!(
                out,
                "                                                                     East         North     Elevation               x               y               z"
            );
            for antenna in &self.antennas {
                let _ = writeln!(
                    out,
                    "  {:<5}{:<6}{:<10}{:>5.1} m   {:<14}{:<13}{:>14.4}{:>14.4}{:>14.4}{:>16.6}{:>16.6}{:>16.6}",
                    antenna.antenna_id,
                    truncate_for_column(&antenna.name, 6),
                    truncate_for_column(&antenna.station, 10),
                    antenna.dish_diameter_m,
                    MvAngle::from_radians(antenna.longitude_radians).format_angle(1),
                    MvAngle::from_radians(antenna.latitude_radians).format_angle_dig2(1),
                    antenna.offset_from_observatory_m[0],
                    antenna.offset_from_observatory_m[1],
                    antenna.offset_from_observatory_m[2],
                    antenna.position_m[0],
                    antenna.position_m[1],
                    antenna.position_m[2],
                );
            }
        } else {
            let _ = writeln!(out, "Antennas: {} 'name'='station' ", self.antennas.len());
            for line in format_compact_antenna_lines(&self.antennas) {
                let _ = writeln!(out, "{line}");
            }
        }

        out
    }

    /// Render the summary using the requested output format.
    pub fn render(&self, format: ListObsOutputFormat) -> Result<String, serde_json::Error> {
        match format {
            ListObsOutputFormat::Text => Ok(self.render_text()),
            ListObsOutputFormat::Json => self.render_json_pretty(),
        }
    }
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

fn build_fields(ms: &MeasurementSet, main_usage: &MainUsage) -> MsResult<Vec<FieldSummary>> {
    let field = ms.field()?;
    let direction_reference = column_measure_reference(field.table(), "PHASE_DIR");
    let mut summaries = Vec::with_capacity(field.row_count());
    for row in 0..field.row_count() {
        if (main_usage.selection_applied || !main_usage.used_field_ids.is_empty())
            && !main_usage.used_field_ids.contains(&row)
        {
            continue;
        }
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
            direction_reference: direction_reference.clone(),
            phase_direction_radians: extract_direction_pair(field.phase_dir(row)?)?,
        });
    }
    Ok(summaries)
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

        summaries.push(SpectralWindowSummary {
            spectral_window_id: row,
            name: spw.name(row)?,
            num_channels: spw.num_chan(row)?,
            frame: frequency_reference_name(spw.meas_freq_ref(row)?),
            first_channel_frequency_hz: chan_freq.first().copied().unwrap_or(0.0),
            channel_width_hz: chan_width.first().copied().unwrap_or(0.0),
            reference_frequency_hz: spw.ref_frequency(row)?,
            center_frequency_hz: mean_or_zero(&chan_freq),
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
    used_source_ids: &BTreeSet<i32>,
    used_spw_ids: &BTreeSet<i32>,
) -> MsResult<Vec<SourceSummary>> {
    let source = match ms.source() {
        Ok(source) => source,
        Err(MsError::MissingSubtable(_)) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut summaries = Vec::with_capacity(source.row_count());
    for row in 0..source.row_count() {
        let source_id = source.i32(row, "SOURCE_ID")?;
        let spw_id = source.i32(row, "SPECTRAL_WINDOW_ID")?;
        let matches_source = used_source_ids.is_empty() || used_source_ids.contains(&source_id);
        let matches_spw = spw_id < 0 || used_spw_ids.is_empty() || used_spw_ids.contains(&spw_id);
        if !matches_source || !matches_spw {
            continue;
        }
        summaries.push(SourceSummary {
            source_id,
            name: source.string(row, "NAME")?,
            code: source.string(row, "CODE")?,
            spectral_window_id: spw_id,
            calibration_group: source.i32(row, "CALIBRATION_GROUP")?,
            num_lines: source.i32(row, "NUM_LINES")?,
            rest_frequency_hz: extract_first_f64_opt(
                source.optional_array(row, "REST_FREQUENCY")?,
            )?,
            system_velocity_m_s: extract_first_f64_opt(source.optional_array(row, "SYSVEL")?)?,
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
) -> MsResult<Vec<AntennaSummary>> {
    let antenna = ms.antenna()?;
    let observatory = observatory_position(observations, main_usage, &antenna)?;
    let mut summaries = Vec::with_capacity(antenna.row_count());
    for row in 0..antenna.row_count() {
        if (main_usage.selection_applied || !main_usage.used_antenna_ids.is_empty())
            && !main_usage.used_antenna_ids.contains(&row)
        {
            continue;
        }
        let position_m = antenna.position(row)?;
        let position = MPosition::new_itrf(position_m[0], position_m[1], position_m[2]);
        let (longitude_radians, latitude_radians, _) = position.as_spherical();
        summaries.push(AntennaSummary {
            antenna_id: row,
            name: antenna.name(row)?,
            station: antenna.station(row)?,
            antenna_type: antenna.antenna_type(row)?,
            mount: antenna.mount(row)?,
            dish_diameter_m: antenna.dish_diameter(row)?,
            longitude_radians,
            latitude_radians,
            offset_from_observatory_m: observatory_offset(position_m, observatory.as_ref()),
            position_m,
        });
    }
    Ok(summaries)
}

fn build_scans(
    ms: &MeasurementSet,
    selected_rows: Option<&[usize]>,
    field_name_lookup: &HashMap<i32, String>,
    dd_to_spw: &HashMap<i32, i32>,
    state_intent_lookup: &HashMap<i32, String>,
    unflagged_context: Option<&UnflaggedRowContext>,
) -> MsResult<Vec<ScanSummary>> {
    #[derive(Debug, Clone)]
    struct MainRowInfo {
        observation_id: i32,
        array_id: i32,
        scan_number: i32,
        field_id: i32,
        data_desc_id: i32,
        state_id: i32,
        time: f64,
        exposure_seconds: f64,
        unflagged_row_fraction: Option<f64>,
    }

    #[derive(Debug, Default, Clone)]
    struct TimeGroup {
        observation_id: i32,
        array_id: i32,
        scan_number: i32,
        time: f64,
        row_count: usize,
        field_ids: BTreeSet<i32>,
        field_names: BTreeSet<String>,
        data_description_ids: BTreeSet<i32>,
        spectral_window_ids: BTreeSet<i32>,
        state_ids: BTreeSet<i32>,
        exposure_by_spw: BTreeMap<i32, (f64, usize)>,
        exposure_sum_seconds: f64,
        unflagged_row_sum: f64,
    }

    #[derive(Debug, Default)]
    struct SubScanAccumulator {
        observation_id: i32,
        array_id: i32,
        scan_number: i32,
        row_count: usize,
        field_ids: BTreeSet<i32>,
        field_names: BTreeSet<String>,
        data_description_ids: BTreeSet<i32>,
        spectral_window_ids: BTreeSet<i32>,
        state_ids: BTreeSet<i32>,
        exposure_by_spw: BTreeMap<i32, (f64, usize)>,
        exposure_sum_seconds: f64,
        unflagged_row_sum: f64,
    }

    impl TimeGroup {
        fn from_row(
            row: &MainRowInfo,
            field_name_lookup: &HashMap<i32, String>,
            dd_to_spw: &HashMap<i32, i32>,
        ) -> Self {
            let mut group = Self {
                observation_id: row.observation_id,
                array_id: row.array_id,
                scan_number: row.scan_number,
                time: row.time,
                ..Self::default()
            };
            group.add_row(row, field_name_lookup, dd_to_spw);
            group
        }

        fn add_row(
            &mut self,
            row: &MainRowInfo,
            field_name_lookup: &HashMap<i32, String>,
            dd_to_spw: &HashMap<i32, i32>,
        ) {
            self.row_count += 1;
            self.field_ids.insert(row.field_id);
            self.field_names.insert(
                field_name_lookup
                    .get(&row.field_id)
                    .cloned()
                    .unwrap_or_else(|| format!("FIELD_{}", row.field_id)),
            );
            self.data_description_ids.insert(row.data_desc_id);
            if let Some(spw_id) = dd_to_spw.get(&row.data_desc_id).copied() {
                self.spectral_window_ids.insert(spw_id);
                let entry = self.exposure_by_spw.entry(spw_id).or_insert((0.0, 0));
                entry.0 += row.exposure_seconds;
                entry.1 += 1;
            }
            self.state_ids.insert(row.state_id);
            self.exposure_sum_seconds += row.exposure_seconds;
            self.unflagged_row_sum += row.unflagged_row_fraction.unwrap_or(0.0);
        }
    }

    impl SubScanAccumulator {
        fn new(group: &TimeGroup) -> Self {
            Self {
                observation_id: group.observation_id,
                array_id: group.array_id,
                scan_number: group.scan_number,
                row_count: group.row_count,
                field_ids: group.field_ids.clone(),
                field_names: group.field_names.clone(),
                data_description_ids: group.data_description_ids.clone(),
                spectral_window_ids: group.spectral_window_ids.clone(),
                state_ids: group.state_ids.clone(),
                exposure_by_spw: group.exposure_by_spw.clone(),
                exposure_sum_seconds: group.exposure_sum_seconds,
                unflagged_row_sum: group.unflagged_row_sum,
            }
        }

        fn absorb(&mut self, group: &TimeGroup) {
            self.row_count += group.row_count;
            self.exposure_sum_seconds += group.exposure_sum_seconds;
            self.field_ids.extend(group.field_ids.iter().copied());
            self.field_names.extend(group.field_names.iter().cloned());
            self.data_description_ids
                .extend(group.data_description_ids.iter().copied());
            self.spectral_window_ids
                .extend(group.spectral_window_ids.iter().copied());
            self.state_ids.extend(group.state_ids.iter().copied());
            for (spw_id, (sum, count)) in &group.exposure_by_spw {
                let entry = self.exposure_by_spw.entry(*spw_id).or_insert((0.0, 0));
                entry.0 += *sum;
                entry.1 += *count;
            }
            self.unflagged_row_sum += group.unflagged_row_sum;
        }

        fn matches_group(&self, group: &TimeGroup) -> bool {
            self.observation_id == group.observation_id
                && self.array_id == group.array_id
                && self.scan_number == group.scan_number
                && self.field_ids == group.field_ids
                && self.data_description_ids == group.data_description_ids
                && self.state_ids == group.state_ids
        }
    }

    let table = ms.main_table();
    let has_exposure = has_column(table, "EXPOSURE");
    let mut rows = Vec::with_capacity(table.row_count());
    let mut scan_time_ranges = BTreeMap::<(i32, i32, i32), (f64, f64)>::new();
    let row_numbers = selected_rows
        .map(|rows| rows.to_vec())
        .unwrap_or_else(|| (0..table.row_count()).collect());
    for row in row_numbers {
        let observation_id = get_i32(table, row, "OBSERVATION_ID")?;
        let array_id = get_i32(table, row, "ARRAY_ID")?;
        let scan_number = get_i32(table, row, "SCAN_NUMBER")?;
        let field_id = get_i32(table, row, "FIELD_ID")?;
        let data_desc_id = get_i32(table, row, "DATA_DESC_ID")?;
        let state_id = get_i32(table, row, "STATE_ID")?;
        let time = get_f64(table, row, "TIME")?;
        let interval = get_f64(table, row, "INTERVAL")?;
        let exposure = if has_exposure {
            get_f64(table, row, "EXPOSURE")?
        } else {
            interval
        };
        let unflagged_row_fraction = unflagged_context
            .map(|context| compute_unflagged_row_fraction(ms, row, context))
            .transpose()?;
        rows.push(MainRowInfo {
            observation_id,
            array_id,
            scan_number,
            field_id,
            data_desc_id,
            state_id,
            time,
            exposure_seconds: exposure,
            unflagged_row_fraction,
        });
        let begin = time - interval / 2.0;
        let end = time + interval / 2.0;
        let entry = scan_time_ranges
            .entry((observation_id, array_id, scan_number))
            .or_insert((begin, end));
        entry.0 = entry.0.min(begin);
        entry.1 = entry.1.max(end);
    }

    rows.sort_by(|left, right| {
        left.observation_id
            .cmp(&right.observation_id)
            .then_with(|| left.array_id.cmp(&right.array_id))
            .then_with(|| left.scan_number.cmp(&right.scan_number))
            .then_with(|| left.time.total_cmp(&right.time))
            .then_with(|| left.field_id.cmp(&right.field_id))
            .then_with(|| left.data_desc_id.cmp(&right.data_desc_id))
            .then_with(|| left.state_id.cmp(&right.state_id))
    });

    let mut time_groups = Vec::<TimeGroup>::new();
    for row in &rows {
        let should_extend = time_groups.last().is_some_and(|group| {
            group.observation_id == row.observation_id
                && group.array_id == row.array_id
                && group.scan_number == row.scan_number
                && group.time == row.time
        });
        if should_extend {
            if let Some(group) = time_groups.last_mut() {
                group.add_row(row, field_name_lookup, dd_to_spw);
            }
            continue;
        }

        time_groups.push(TimeGroup::from_row(row, field_name_lookup, dd_to_spw));
    }

    let finish_scan_summary = |scan: SubScanAccumulator| {
        let field_id = scan.field_ids.iter().copied().next().unwrap_or(-1);
        let field_name = field_name_lookup
            .get(&field_id)
            .cloned()
            .or_else(|| scan.field_names.iter().next().cloned())
            .unwrap_or_else(|| format!("FIELD_{field_id}"));
        let spectral_window_ids: Vec<i32> = scan.spectral_window_ids.iter().copied().collect();
        let mut scan_intents = Vec::new();
        for state_id in &scan.state_ids {
            if let Some(intent) = state_intent_lookup.get(state_id) {
                push_unique_string(&mut scan_intents, intent);
            }
        }
        let (start_mjd_seconds, end_mjd_seconds) = scan_time_ranges
            .get(&(scan.observation_id, scan.array_id, scan.scan_number))
            .copied()
            .unwrap_or((0.0, 0.0));

        ScanSummary {
            observation_id: scan.observation_id,
            array_id: scan.array_id,
            scan_number: scan.scan_number,
            row_count: scan.row_count,
            field_id,
            field_name,
            field_ids: scan.field_ids.into_iter().collect(),
            field_names: scan.field_names.into_iter().collect(),
            data_description_ids: scan.data_description_ids.into_iter().collect(),
            spectral_window_ids: spectral_window_ids.clone(),
            state_ids: scan.state_ids.into_iter().collect(),
            scan_intents,
            start_mjd_seconds,
            end_mjd_seconds,
            mean_interval_seconds: if scan.row_count == 0 {
                0.0
            } else {
                scan.exposure_sum_seconds / scan.row_count as f64
            },
            mean_interval_seconds_by_spw: spectral_window_ids
                .iter()
                .map(|spw_id| {
                    scan.exposure_by_spw
                        .get(spw_id)
                        .map(|(sum, count)| {
                            if *count == 0 {
                                0.0
                            } else {
                                *sum / *count as f64
                            }
                        })
                        .unwrap_or(0.0)
                })
                .collect(),
            unflagged_row_count: unflagged_context.map(|_| scan.unflagged_row_sum),
        }
    };

    let mut summaries = Vec::with_capacity(time_groups.len());
    let mut current = None::<SubScanAccumulator>;
    for group in &time_groups {
        let same_subscan = current
            .as_ref()
            .is_some_and(|scan| scan.matches_group(group));

        if !same_subscan {
            if let Some(scan) = current.take() {
                summaries.push(finish_scan_summary(scan));
            }
            current = Some(SubScanAccumulator::new(group));
            continue;
        }

        if let Some(scan) = current.as_mut() {
            scan.absorb(group);
        }
    }

    if let Some(scan) = current {
        summaries.push(finish_scan_summary(scan));
    }
    Ok(summaries)
}

impl UnflaggedRowContext {
    fn new(ms: &MeasurementSet) -> MsResult<Self> {
        let dd = ms.data_description()?;
        let spw = ms.spectral_window()?;
        let mut ddid_to_bandwidth_hz = HashMap::new();
        let mut ddid_to_channel_widths_hz = HashMap::new();
        for row in 0..dd.row_count() {
            let spw_id = dd.spectral_window_id(row)?;
            if spw_id < 0 {
                continue;
            }
            let mut channel_widths = spw.chan_width(spw_id as usize)?;
            if channel_widths.is_empty() {
                channel_widths = spw.effective_bw(spw_id as usize)?;
            }
            channel_widths
                .iter_mut()
                .for_each(|width| *width = width.abs());
            let bandwidth_hz = spw
                .total_bandwidth(spw_id as usize)
                .map(f64::abs)
                .unwrap_or_else(|_| channel_widths.iter().sum());
            ddid_to_bandwidth_hz.insert(row as i32, bandwidth_hz);
            ddid_to_channel_widths_hz.insert(row as i32, channel_widths);
        }
        Ok(Self {
            ddid_to_bandwidth_hz,
            ddid_to_channel_widths_hz,
        })
    }
}

fn compute_unflagged_row_fraction(
    ms: &MeasurementSet,
    row: usize,
    context: &UnflaggedRowContext,
) -> MsResult<f64> {
    let flag_column = ms.flag_column();
    let flags = flag_column.get(row)?;
    let data_desc_id = get_i32(ms.main_table(), row, "DATA_DESC_ID")?;
    let bandwidth_hz = context
        .ddid_to_bandwidth_hz
        .get(&data_desc_id)
        .copied()
        .unwrap_or(0.0);
    let channel_widths = context
        .ddid_to_channel_widths_hz
        .get(&data_desc_id)
        .cloned()
        .unwrap_or_default();
    if bandwidth_hz <= 0.0 {
        return Ok(0.0);
    }
    let row_flagged = ms.flag_row_column().get(row).unwrap_or(false);
    if row_flagged {
        return Ok(0.0);
    }
    match flags {
        ArrayValue::Bool(values) => {
            let shape = values.shape();
            let slice = values.iter().copied().collect::<Vec<_>>();
            let (n_corr, n_chan) = match shape {
                [] => (1, 1),
                [only] => (1, *only),
                [corr, chan, ..] => (*corr, *chan),
            };
            if slice.iter().all(|flag| !*flag) {
                return Ok(1.0);
            }
            if slice.iter().all(|flag| *flag) {
                return Ok(0.0);
            }
            let denom = bandwidth_hz * n_corr as f64;
            if denom == 0.0 {
                return Ok(0.0);
            }
            let mut bandwidth_sum_hz = 0.0;
            for corr in 0..n_corr {
                let start = corr * n_chan;
                let end = start + n_chan;
                let corr_slice = &slice[start..end];
                if corr_slice.iter().all(|flag| !*flag) {
                    bandwidth_sum_hz += bandwidth_hz;
                    continue;
                }
                if corr_slice.iter().all(|flag| *flag) {
                    continue;
                }
                for (channel_index, flagged) in corr_slice.iter().enumerate() {
                    if !*flagged {
                        bandwidth_sum_hz += channel_widths
                            .get(channel_index)
                            .copied()
                            .unwrap_or_else(|| bandwidth_hz / n_chan as f64);
                    }
                }
            }
            Ok(bandwidth_sum_hz / denom)
        }
        other => Err(MsError::ColumnTypeMismatch {
            column: "FLAG".to_string(),
            table: "MAIN".to_string(),
            expected: "Bool array".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

fn main_time_reference(table: &Table) -> Option<String> {
    let desc = TableMeasDesc::reconstruct(table, "TIME")?;
    match desc.ref_desc() {
        MeasRefDesc::Fixed { refer } => Some(refer.clone()),
        MeasRefDesc::VariableInt { ref_column, .. } => Some(format!("variable:{ref_column}")),
        MeasRefDesc::VariableString { ref_column } => Some(format!("variable:{ref_column}")),
    }
}

fn column_measure_reference(table: &Table, column: &str) -> Option<String> {
    let desc = TableMeasDesc::reconstruct(table, column)?;
    match desc.ref_desc() {
        MeasRefDesc::Fixed { refer } => Some(refer.clone()),
        MeasRefDesc::VariableInt { ref_column, .. } => Some(format!("variable:{ref_column}")),
        MeasRefDesc::VariableString { ref_column } => Some(format!("variable:{ref_column}")),
    }
}

fn analyze_main_table(
    ms: &MeasurementSet,
    selected_rows: Option<&[usize]>,
    unflagged_context: Option<&UnflaggedRowContext>,
) -> MsResult<MainUsage> {
    let table = ms.main_table();
    let mut usage = MainUsage {
        selection_applied: selected_rows.is_some(),
        ..MainUsage::default()
    };
    let mut start = f64::INFINITY;
    let mut end = f64::NEG_INFINITY;
    let row_numbers = selected_rows
        .map(|rows| rows.to_vec())
        .unwrap_or_else(|| (0..table.row_count()).collect());
    for row in row_numbers {
        let field_id = get_i32(table, row, "FIELD_ID")?;
        if field_id >= 0 {
            usage.used_field_ids.insert(field_id as usize);
            *usage.field_row_counts.entry(field_id).or_insert(0) += 1;
            if let Some(context) = unflagged_context {
                *usage
                    .field_unflagged_row_counts
                    .entry(field_id)
                    .or_insert(0.0) += compute_unflagged_row_fraction(ms, row, context)?;
            }
        }
        let observation_id = get_i32(table, row, "OBSERVATION_ID")?;
        if observation_id >= 0 {
            usage.used_observation_ids.insert(observation_id as usize);
        }
        let array_id = get_i32(table, row, "ARRAY_ID")?;
        usage.used_array_ids.insert(array_id);

        for column in ["ANTENNA1", "ANTENNA2"] {
            let antenna_id = get_i32(table, row, column)?;
            if antenna_id >= 0 {
                usage.used_antenna_ids.insert(antenna_id as usize);
            }
        }

        let data_desc_id = get_i32(table, row, "DATA_DESC_ID")?;
        if data_desc_id >= 0 {
            usage
                .used_data_description_ids
                .insert(data_desc_id as usize);
        }

        let time = get_f64(table, row, "TIME")?;
        let interval = get_f64(table, row, "INTERVAL")?;
        start = start.min(time - interval / 2.0);
        end = end.max(time + interval / 2.0);
        usage.selected_row_count += 1;
    }

    if usage.selected_row_count > 0 {
        usage.time_range = Some((start, end));
    }
    Ok(usage)
}

fn build_state_intent_lookup(ms: &MeasurementSet) -> MsResult<HashMap<i32, String>> {
    let state = ms.state()?;
    let mut lookup = HashMap::with_capacity(state.row_count());
    for row in 0..state.row_count() {
        let intent = state.string(row, "OBS_MODE")?;
        if !intent.is_empty() {
            lookup.insert(row as i32, intent);
        }
    }
    Ok(lookup)
}

fn resolve_selected_rows(
    ms: &MeasurementSet,
    options: &ListObsOptions,
) -> MsResult<Option<Vec<usize>>> {
    if !options.selectdata || !options.has_selection() {
        return Ok(None);
    }

    let selection = selection_from_options(ms, options)?;
    Ok(Some(selection.apply(ms)?))
}

fn selection_from_options(ms: &MeasurementSet, options: &ListObsOptions) -> MsResult<MsSelection> {
    let mut selection = MsSelection::new();

    if let Some(field) = options.field.as_deref() {
        let (field_ids, field_names) = parse_field_selector(ms, field)?;
        if !field_ids.is_empty() {
            selection = selection.field(&field_ids);
        }
        if !field_names.is_empty() {
            let names = field_names.iter().map(String::as_str).collect::<Vec<_>>();
            let resolved_ids = resolve_field_names(ms, &names)?;
            selection = selection.field(&resolved_ids);
        }
    }

    if let Some(spw) = options.spw.as_deref() {
        selection = selection.spw(&parse_numeric_selector(spw, "spw")?);
    }
    if let Some(scan) = options.scan.as_deref() {
        selection = selection.scan(&parse_numeric_selector(scan, "scan")?);
    }
    if let Some(observation) = options.observation.as_deref() {
        selection = selection.observation(&parse_numeric_selector(observation, "observation")?);
    }
    if let Some(array) = options.array.as_deref() {
        selection = selection.array(&parse_numeric_selector(array, "array")?);
    }
    if let Some(antenna) = options.antenna.as_deref() {
        selection = apply_antenna_selector(ms, selection, antenna)?;
    }
    if let Some(intent) = options.intent.as_deref() {
        selection = apply_intent_selector(ms, selection, intent)?;
    }
    if let Some(correlation) = options.correlation.as_deref() {
        selection = apply_correlation_selector(ms, selection, correlation)?;
    }
    if let Some(uvrange) = options.uvrange.as_deref() {
        selection = apply_uvrange_selector(ms, selection, uvrange)?;
    }
    if let Some(timerange) = options.timerange.as_deref() {
        selection = apply_timerange_selector(ms, selection, timerange)?;
    }

    Ok(selection)
}

fn parse_field_selector(ms: &MeasurementSet, value: &str) -> MsResult<(Vec<i32>, Vec<String>)> {
    let mut ids = Vec::new();
    let mut names = Vec::new();
    for raw_part in value.split(',') {
        let part = raw_part.trim();
        if part.is_empty() {
            continue;
        }
        if part.contains('*') {
            let matched = collect_matching_field_names(ms, part)?;
            if matched.is_empty() {
                return Err(MsError::VersionError(format!(
                    "field selector {part:?} did not match any field names"
                )));
            }
            names.extend(matched);
            continue;
        }
        if let Some((start, end)) = parse_numeric_range(part) {
            ids.extend(start..=end);
            continue;
        }
        if let Ok(id) = part.parse::<i32>() {
            ids.push(id);
            continue;
        }
        names.push(part.to_string());
    }
    Ok((dedup_i32(ids), dedup_strings(names)))
}

fn apply_antenna_selector(
    ms: &MeasurementSet,
    mut selection: MsSelection,
    value: &str,
) -> MsResult<MsSelection> {
    let mut antenna_ids = Vec::new();
    let mut antenna_names = Vec::new();
    let mut baselines = Vec::new();

    for raw_part in value.split(',') {
        let part = raw_part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((left, right)) = part.split_once("&&") {
            baselines.push((
                resolve_antenna_ref(ms, left.trim())?,
                resolve_antenna_ref(ms, right.trim())?,
            ));
            continue;
        }
        if let Ok(id) = part.parse::<i32>() {
            antenna_ids.push(id);
            continue;
        }
        antenna_names.push(part.to_string());
    }

    if !antenna_ids.is_empty() {
        selection = selection.antenna(&dedup_i32(antenna_ids));
    }
    if !antenna_names.is_empty() {
        let names = antenna_names.iter().map(String::as_str).collect::<Vec<_>>();
        selection = selection.antenna_name(&names);
    }
    if !baselines.is_empty() {
        selection = selection.baseline(&baselines);
    }

    Ok(selection)
}

fn apply_intent_selector(
    ms: &MeasurementSet,
    mut selection: MsSelection,
    value: &str,
) -> MsResult<MsSelection> {
    let state_ids = parse_state_selector(ms, value)?;
    if state_ids.is_empty() {
        return Err(MsError::VersionError(format!(
            "intent selector {value:?} did not match any STATE rows"
        )));
    }
    selection = selection.state(&state_ids);
    Ok(selection)
}

fn apply_correlation_selector(
    ms: &MeasurementSet,
    mut selection: MsSelection,
    value: &str,
) -> MsResult<MsSelection> {
    let requested_codes = parse_correlation_selector(value)?;
    let polarization = ms.polarization()?;
    let data_description = ms.data_description()?;
    let mut matching_ddids = Vec::new();
    for row in 0..data_description.row_count() {
        let pol_id = data_description.polarization_id(row)?;
        if pol_id < 0 {
            continue;
        }
        let corr_types = polarization.corr_type(pol_id as usize)?;
        if requested_codes
            .iter()
            .all(|code| corr_types.iter().any(|candidate| candidate == code))
        {
            matching_ddids.push(row as i32);
        }
    }
    if matching_ddids.is_empty() {
        return Err(MsError::VersionError(format!(
            "correlation selector {value:?} did not match any DATA_DESCRIPTION rows"
        )));
    }
    selection = selection.data_description(&dedup_i32(matching_ddids));
    Ok(selection)
}

fn apply_uvrange_selector(
    ms: &MeasurementSet,
    mut selection: MsSelection,
    value: &str,
) -> MsResult<MsSelection> {
    let taql = build_uvrange_taql(ms, value)?;
    selection = selection.taql(&taql);
    Ok(selection)
}

fn apply_timerange_selector(
    ms: &MeasurementSet,
    mut selection: MsSelection,
    value: &str,
) -> MsResult<MsSelection> {
    let taql = build_timerange_taql(ms, &selection, value)?;
    selection = selection.taql(&taql);
    Ok(selection)
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct TimeSelectionDefaults {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: f64,
    exposure_seconds: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct PartialTimeSpec {
    year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
    hour: Option<u32>,
    minute: Option<u32>,
    second: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ResolvedTimeSpec {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: f64,
}

fn build_timerange_taql(
    ms: &MeasurementSet,
    selection: &MsSelection,
    value: &str,
) -> MsResult<String> {
    let defaults = timerange_defaults(ms, selection)?;
    let clauses = value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| build_timerange_clause(part, defaults))
        .collect::<MsResult<Vec<_>>>()?;
    if clauses.is_empty() {
        return Err(MsError::VersionError(
            "timerange selector was empty".to_string(),
        ));
    }
    Ok(format!("({})", clauses.join(" OR ")))
}

fn timerange_defaults(
    ms: &MeasurementSet,
    selection: &MsSelection,
) -> MsResult<TimeSelectionDefaults> {
    let rows = selection.apply(ms)?;
    let row = rows.first().copied().ok_or_else(|| {
        MsError::VersionError("timerange selection produced an empty row set".to_string())
    })?;
    let table = ms.main_table();
    let time = get_f64(table, row, "TIME")?;
    let exposure_seconds = if has_column(table, "EXPOSURE") {
        get_f64(table, row, "EXPOSURE")?
    } else {
        0.1
    };
    let calendar = MvTime::from_mjd_seconds(time);
    let time_of_day = time.rem_euclid(86_400.0);
    let hour = (time_of_day / 3_600.0).floor() as u32;
    let minute = ((time_of_day / 60.0).floor() as u32) % 60;
    let second = time_of_day - f64::from(hour) * 3_600.0 - f64::from(minute) * 60.0;

    Ok(TimeSelectionDefaults {
        year: calendar.year(),
        month: calendar.month(),
        day: calendar.month_day(),
        hour,
        minute,
        second,
        exposure_seconds,
    })
}

fn build_timerange_clause(value: &str, defaults: TimeSelectionDefaults) -> MsResult<String> {
    if let Some(rest) = value.strip_prefix(">=") {
        let time = resolve_time_spec(parse_partial_time_spec(rest)?, defaults, None)?;
        return Ok(format!(
            "(TIME >= {})",
            format_float_compact(time.as_mjd_seconds(), 6)
        ));
    }
    if let Some(rest) = value.strip_prefix("<=") {
        let time = resolve_time_spec(parse_partial_time_spec(rest)?, defaults, None)?;
        return Ok(format!(
            "(TIME <= {})",
            format_float_compact(time.as_mjd_seconds(), 6)
        ));
    }
    if let Some(rest) = value.strip_prefix('>') {
        let time = resolve_time_spec(parse_partial_time_spec(rest)?, defaults, None)?;
        return Ok(format!(
            "(TIME > {})",
            format_float_compact(time.as_mjd_seconds(), 6)
        ));
    }
    if let Some(rest) = value.strip_prefix('<') {
        let time = resolve_time_spec(parse_partial_time_spec(rest)?, defaults, None)?;
        return Ok(format!(
            "(TIME < {})",
            format_float_compact(time.as_mjd_seconds(), 6)
        ));
    }
    if let Some((left, right)) = value.split_once('~') {
        let start = resolve_time_spec(parse_partial_time_spec(left)?, defaults, None)?;
        let end = resolve_time_spec(parse_partial_time_spec(right)?, defaults, Some(start))?;
        if end.as_mjd_seconds() < start.as_mjd_seconds() {
            return Err(MsError::VersionError(format!(
                "timerange selector {value:?} has an end before its start"
            )));
        }
        return Ok(format!(
            "(TIME >= {} AND TIME <= {})",
            format_float_compact(start.as_mjd_seconds(), 6),
            format_float_compact(end.as_mjd_seconds(), 6)
        ));
    }
    if let Some((left, right)) = value.split_once('+') {
        let start = resolve_time_spec(parse_partial_time_spec(left)?, defaults, None)?;
        let duration_seconds = parse_duration_seconds(right)?;
        let end = start.as_mjd_seconds() + duration_seconds;
        return Ok(format!(
            "(TIME >= {} AND TIME <= {})",
            format_float_compact(start.as_mjd_seconds(), 6),
            format_float_compact(end, 6)
        ));
    }

    let center = resolve_time_spec(parse_partial_time_spec(value)?, defaults, None)?;
    let tolerance = defaults.exposure_seconds / 2.0;
    Ok(format!(
        "(ABS(TIME - {}) <= {})",
        format_float_compact(center.as_mjd_seconds(), 6),
        format_float_compact(tolerance, 6)
    ))
}

fn parse_partial_time_spec(value: &str) -> MsResult<PartialTimeSpec> {
    let trimmed = value.trim().trim_matches('\'').trim_matches('"');
    if trimmed.is_empty() {
        return Err(MsError::VersionError(
            "timerange selector contains an empty time expression".to_string(),
        ));
    }

    let mut spec = PartialTimeSpec::default();
    if let Some((date_part, time_part)) = split_date_time_parts(trimmed) {
        parse_date_part(date_part, &mut spec)?;
        if let Some(time_part) = time_part {
            parse_time_part(time_part, &mut spec)?;
        }
        return Ok(spec);
    }

    parse_time_part(trimmed, &mut spec)?;
    Ok(spec)
}

fn split_date_time_parts(value: &str) -> Option<(&str, Option<&str>)> {
    if let Some((date, time)) = value.split_once('T') {
        return Some((date.trim(), Some(time.trim())));
    }
    if let Some((date, time)) = value.split_once(' ') {
        if looks_like_date(date.trim()) {
            return Some((date.trim(), Some(time.trim())));
        }
    }
    let slash_count = value.matches('/').count();
    if slash_count >= 2 && !value.contains(':') {
        return Some((value.trim(), None));
    }
    if slash_count >= 2 {
        let (date, time) = value.rsplit_once('/')?;
        if looks_like_date(date.trim()) {
            return Some((date.trim(), Some(time.trim())));
        }
    }
    if looks_like_date(value) {
        return Some((value.trim(), None));
    }
    None
}

fn looks_like_date(value: &str) -> bool {
    let trimmed = value.trim();
    (trimmed.contains('/') && trimmed.split('/').count() >= 3)
        || (trimmed.contains('-')
            && trimmed.split('-').filter(|part| !part.is_empty()).count() >= 3
            && trimmed
                .chars()
                .any(|ch| ch.is_ascii_alphabetic() || ch.is_ascii_digit()))
}

fn parse_date_part(value: &str, spec: &mut PartialTimeSpec) -> MsResult<()> {
    let trimmed = value.trim();
    if trimmed.contains('/') {
        let parts = trimmed.split('/').map(str::trim).collect::<Vec<_>>();
        if parts.len() != 3 {
            return Err(MsError::VersionError(format!(
                "timerange date {value:?} is not a supported yyyy/mm/dd value"
            )));
        }
        spec.year = Some(parse_i32_component(parts[0], "year", value)?);
        spec.month = Some(parse_u32_component(parts[1], "month", value)?);
        spec.day = Some(parse_u32_component(parts[2], "day", value)?);
        return Ok(());
    }

    let normalized = trimmed.replace(' ', "-");
    let parts = normalized.split('-').map(str::trim).collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(MsError::VersionError(format!(
            "timerange date {value:?} is not a supported date expression"
        )));
    }
    if parts[1].chars().all(|ch| ch.is_ascii_digit()) {
        spec.year = Some(parse_i32_component(parts[0], "year", value)?);
        spec.month = Some(parse_u32_component(parts[1], "month", value)?);
        spec.day = Some(parse_u32_component(parts[2], "day", value)?);
    } else {
        spec.day = Some(parse_u32_component(parts[0], "day", value)?);
        spec.month = Some(parse_month_component(parts[1], value)?);
        spec.year = Some(parse_two_or_four_digit_year(parts[2], value)?);
    }
    Ok(())
}

fn parse_time_part(value: &str, spec: &mut PartialTimeSpec) -> MsResult<()> {
    let trimmed = value.trim();
    let parts = trimmed.split(':').map(str::trim).collect::<Vec<_>>();
    match parts.len() {
        2 => {
            spec.hour = Some(parse_u32_component(parts[0], "hour", value)?);
            spec.minute = Some(parse_u32_component(parts[1], "minute", value)?);
        }
        3 => {
            spec.hour = Some(parse_u32_component(parts[0], "hour", value)?);
            spec.minute = Some(parse_u32_component(parts[1], "minute", value)?);
            spec.second = Some(parse_f64_component(parts[2], "second", value)?);
        }
        _ => {
            return Err(MsError::VersionError(format!(
                "timerange time {value:?} is not a supported hh:mm[:ss] value"
            )));
        }
    }
    Ok(())
}

fn parse_duration_seconds(value: &str) -> MsResult<f64> {
    let trimmed = value.trim().trim_matches('\'').trim_matches('"');
    if trimmed.is_empty() {
        return Err(MsError::VersionError(
            "timerange interval is empty".to_string(),
        ));
    }
    if trimmed.contains(':') {
        let parts = trimmed.split(':').map(str::trim).collect::<Vec<_>>();
        let seconds = match parts.len() {
            2 => {
                let hours = parse_u32_component(parts[0], "hour", value)?;
                let minutes = parse_u32_component(parts[1], "minute", value)?;
                f64::from(hours) * 3_600.0 + f64::from(minutes) * 60.0
            }
            3 => {
                let hours = parse_u32_component(parts[0], "hour", value)?;
                let minutes = parse_u32_component(parts[1], "minute", value)?;
                let seconds = parse_f64_component(parts[2], "second", value)?;
                f64::from(hours) * 3_600.0 + f64::from(minutes) * 60.0 + seconds
            }
            _ => {
                return Err(MsError::VersionError(format!(
                    "timerange interval {value:?} is not a supported hh:mm[:ss] value"
                )));
            }
        };
        return Ok(seconds);
    }

    trimmed.parse::<f64>().map_err(|_| {
        MsError::VersionError(format!(
            "timerange interval {value:?} is not a supported numeric duration"
        ))
    })
}

fn resolve_time_spec(
    spec: PartialTimeSpec,
    defaults: TimeSelectionDefaults,
    reference: Option<ResolvedTimeSpec>,
) -> MsResult<ResolvedTimeSpec> {
    let reference = reference.unwrap_or(ResolvedTimeSpec {
        year: defaults.year,
        month: defaults.month,
        day: defaults.day,
        hour: defaults.hour,
        minute: defaults.minute,
        second: defaults.second,
    });
    let resolved = ResolvedTimeSpec {
        year: spec.year.unwrap_or(reference.year),
        month: spec.month.unwrap_or(reference.month),
        day: spec.day.unwrap_or(reference.day),
        hour: spec.hour.unwrap_or(reference.hour),
        minute: spec.minute.unwrap_or(reference.minute),
        second: spec.second.unwrap_or(reference.second),
    };
    if MvTime::from_ymd_hms_utc(
        resolved.year,
        resolved.month,
        resolved.day,
        resolved.hour,
        resolved.minute,
        resolved.second,
    )
    .is_none()
    {
        return Err(MsError::VersionError(format!(
            "timerange resolved to an invalid UTC timestamp: {:04}-{:02}-{:02} {:02}:{:02}:{:06.3}",
            resolved.year,
            resolved.month,
            resolved.day,
            resolved.hour,
            resolved.minute,
            resolved.second
        )));
    }
    Ok(resolved)
}

impl ResolvedTimeSpec {
    fn as_mjd_seconds(self) -> f64 {
        MvTime::from_ymd_hms_utc(
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute,
            self.second,
        )
        .expect("validated time")
        .as_mjd_seconds()
    }
}

fn parse_i32_component(value: &str, label: &str, original: &str) -> MsResult<i32> {
    value.parse::<i32>().map_err(|_| {
        MsError::VersionError(format!(
            "timerange {label} in {original:?} is not a valid integer"
        ))
    })
}

fn parse_u32_component(value: &str, label: &str, original: &str) -> MsResult<u32> {
    value.parse::<u32>().map_err(|_| {
        MsError::VersionError(format!(
            "timerange {label} in {original:?} is not a valid integer"
        ))
    })
}

fn parse_f64_component(value: &str, label: &str, original: &str) -> MsResult<f64> {
    value.parse::<f64>().map_err(|_| {
        MsError::VersionError(format!(
            "timerange {label} in {original:?} is not a valid number"
        ))
    })
}

fn parse_month_component(value: &str, original: &str) -> MsResult<u32> {
    match value.to_ascii_lowercase().as_str() {
        "jan" | "january" => Ok(1),
        "feb" | "february" => Ok(2),
        "mar" | "march" => Ok(3),
        "apr" | "april" => Ok(4),
        "may" => Ok(5),
        "jun" | "june" => Ok(6),
        "jul" | "july" => Ok(7),
        "aug" | "august" => Ok(8),
        "sep" | "sept" | "september" => Ok(9),
        "oct" | "october" => Ok(10),
        "nov" | "november" => Ok(11),
        "dec" | "december" => Ok(12),
        _ => Err(MsError::VersionError(format!(
            "timerange month in {original:?} is not a supported month name"
        ))),
    }
}

fn parse_two_or_four_digit_year(value: &str, original: &str) -> MsResult<i32> {
    let year = parse_i32_component(value, "year", original)?;
    Ok(if value.len() < 3 { 2000 + year } else { year })
}

fn resolve_field_names(ms: &MeasurementSet, names: &[&str]) -> MsResult<Vec<i32>> {
    let field = ms.field()?;
    let mut ids = Vec::new();
    for row in 0..field.row_count() {
        let name = field.name(row)?;
        if names.iter().any(|candidate| candidate == &name) {
            ids.push(row as i32);
        }
    }
    if ids.is_empty() {
        return Err(MsError::VersionError(format!(
            "field selector names {names:?} did not match any FIELD rows"
        )));
    }
    Ok(dedup_i32(ids))
}

fn collect_matching_field_names(ms: &MeasurementSet, pattern: &str) -> MsResult<Vec<String>> {
    let field = ms.field()?;
    let mut matches = Vec::new();
    for row in 0..field.row_count() {
        let name = field.name(row)?;
        if matches_simple_glob(pattern, &name) {
            matches.push(name);
        }
    }
    Ok(dedup_strings(matches))
}

fn parse_state_selector(ms: &MeasurementSet, value: &str) -> MsResult<Vec<i32>> {
    let state = ms.state()?;
    let mut ids = Vec::new();
    for raw_part in value.split(',') {
        let part = raw_part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((start, end)) = parse_numeric_range(part) {
            ids.extend(start..=end);
            continue;
        }
        if let Ok(id) = part.parse::<i32>() {
            ids.push(id);
            continue;
        }
        let mut matched = false;
        for row in 0..state.row_count() {
            let obs_mode = state.string(row, "OBS_MODE")?;
            let is_match = if part.contains('*') {
                matches_simple_glob(part, &obs_mode)
            } else {
                obs_mode == part
            };
            if is_match {
                ids.push(row as i32);
                matched = true;
            }
        }
        if !matched {
            return Err(MsError::VersionError(format!(
                "intent selector {part:?} did not match any STATE rows"
            )));
        }
    }
    Ok(dedup_i32(ids))
}

fn parse_correlation_selector(value: &str) -> MsResult<Vec<i32>> {
    let mut codes = Vec::new();
    for raw_part in value.split(',') {
        let part = raw_part.trim();
        if part.is_empty() {
            continue;
        }
        let normalized = part
            .trim_matches('\'')
            .trim_matches('"')
            .to_ascii_uppercase();
        let Some(code) = stokes_code(&normalized) else {
            return Err(MsError::VersionError(format!(
                "correlation selector {part:?} is not a supported Stokes/correlation name"
            )));
        };
        codes.push(code);
    }
    if codes.is_empty() {
        return Err(MsError::VersionError(
            "correlation selector was empty".to_string(),
        ));
    }
    Ok(dedup_i32(codes))
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum BoundOp {
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct UvBound {
    value: f64,
    op: BoundOp,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum UvUnit {
    Meters,
    Lambda(f64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct UvSelectionRange {
    lower: Option<UvBound>,
    upper: Option<UvBound>,
    unit: UvUnit,
}

fn build_uvrange_taql(ms: &MeasurementSet, value: &str) -> MsResult<String> {
    let ranges = parse_uvrange_selector(value)?;
    let ddid_lambda_map = ddid_to_lambda_map(ms)?;
    let clauses = ranges
        .iter()
        .map(|range| uvrange_clause(range, &ddid_lambda_map))
        .collect::<MsResult<Vec<_>>>()?;
    if clauses.is_empty() {
        return Err(MsError::VersionError(
            "uvrange selector was empty".to_string(),
        ));
    }
    Ok(format!("({})", clauses.join(" OR ")))
}

fn parse_uvrange_selector(value: &str) -> MsResult<Vec<UvSelectionRange>> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(parse_uvrange_part)
        .collect()
}

fn parse_uvrange_part(value: &str) -> MsResult<UvSelectionRange> {
    if let Some(rest) = value.strip_prefix(">=") {
        let (parsed, unit) = parse_uv_bound_value(rest)?;
        return Ok(UvSelectionRange {
            lower: Some(UvBound {
                value: parsed,
                op: BoundOp::GreaterEqual,
            }),
            upper: None,
            unit: unit.unwrap_or(UvUnit::Meters),
        });
    }
    if let Some(rest) = value.strip_prefix("<=") {
        let (parsed, unit) = parse_uv_bound_value(rest)?;
        return Ok(UvSelectionRange {
            lower: None,
            upper: Some(UvBound {
                value: parsed,
                op: BoundOp::LessEqual,
            }),
            unit: unit.unwrap_or(UvUnit::Meters),
        });
    }
    if let Some(rest) = value.strip_prefix('>') {
        let (parsed, unit) = parse_uv_bound_value(rest)?;
        return Ok(UvSelectionRange {
            lower: Some(UvBound {
                value: parsed,
                op: BoundOp::Greater,
            }),
            upper: None,
            unit: unit.unwrap_or(UvUnit::Meters),
        });
    }
    if let Some(rest) = value.strip_prefix('<') {
        let (parsed, unit) = parse_uv_bound_value(rest)?;
        return Ok(UvSelectionRange {
            lower: None,
            upper: Some(UvBound {
                value: parsed,
                op: BoundOp::Less,
            }),
            unit: unit.unwrap_or(UvUnit::Meters),
        });
    }
    if let Some((start_raw, end_raw)) = value.split_once('~') {
        let (start, start_unit) = parse_uv_bound_value(start_raw)?;
        let (end, end_unit) = parse_uv_bound_value(end_raw)?;
        let unit = merge_uv_units(start_unit, end_unit)?;
        let (start, end) = if start <= end {
            (start, end)
        } else {
            (end, start)
        };
        return Ok(UvSelectionRange {
            lower: Some(UvBound {
                value: start,
                op: BoundOp::GreaterEqual,
            }),
            upper: Some(UvBound {
                value: end,
                op: BoundOp::LessEqual,
            }),
            unit,
        });
    }

    let (parsed, unit) = parse_uv_bound_value(value)?;
    Ok(UvSelectionRange {
        lower: Some(UvBound {
            value: parsed,
            op: BoundOp::GreaterEqual,
        }),
        upper: Some(UvBound {
            value: parsed,
            op: BoundOp::LessEqual,
        }),
        unit: unit.unwrap_or(UvUnit::Meters),
    })
}

fn parse_uv_bound_value(value: &str) -> MsResult<(f64, Option<UvUnit>)> {
    let trimmed = value.trim();
    let split_at = trimmed
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.' || ch == '+' || ch == '-'))
        .unwrap_or(trimmed.len());
    let (number, suffix) = trimmed.split_at(split_at);
    if number.is_empty() {
        return Err(MsError::VersionError(format!(
            "uvrange selector {value:?} is missing a numeric bound"
        )));
    }
    let parsed = number.parse::<f64>().map_err(|_| {
        MsError::VersionError(format!(
            "uvrange selector {value:?} has an invalid numeric bound"
        ))
    })?;
    let unit = if suffix.trim().is_empty() {
        None
    } else {
        Some(parse_uv_unit(suffix.trim())?)
    };
    Ok((parsed, unit))
}

fn parse_uv_unit(value: &str) -> MsResult<UvUnit> {
    match value.to_ascii_lowercase().as_str() {
        "m" => Ok(UvUnit::Meters),
        "lambda" => Ok(UvUnit::Lambda(1.0)),
        "klambda" => Ok(UvUnit::Lambda(1_000.0)),
        "mlambda" => Ok(UvUnit::Lambda(1_000_000.0)),
        "glambda" => Ok(UvUnit::Lambda(1_000_000_000.0)),
        other => Err(MsError::VersionError(format!(
            "uvrange unit {other:?} is not supported; use m, lambda, klambda, mlambda, or glambda"
        ))),
    }
}

fn merge_uv_units(left: Option<UvUnit>, right: Option<UvUnit>) -> MsResult<UvUnit> {
    match (left, right) {
        (Some(left), Some(right)) if left != right => Err(MsError::VersionError(
            "uvrange bounds must use the same unit".to_string(),
        )),
        (Some(unit), _) | (_, Some(unit)) => Ok(unit),
        (None, None) => Ok(UvUnit::Meters),
    }
}

fn ddid_to_lambda_map(ms: &MeasurementSet) -> MsResult<Vec<(i32, f64)>> {
    let dd = ms.data_description()?;
    let spw = ms.spectral_window()?;
    let mut mapping = Vec::new();
    for row in 0..dd.row_count() {
        let spw_id = dd.spectral_window_id(row)?;
        if spw_id < 0 {
            continue;
        }
        let ref_frequency_hz = spw.ref_frequency(spw_id as usize)?;
        if ref_frequency_hz <= 0.0 {
            continue;
        }
        mapping.push((row as i32, 299_792_458.0 / ref_frequency_hz));
    }
    Ok(mapping)
}

fn uvrange_clause(range: &UvSelectionRange, ddid_lambda_map: &[(i32, f64)]) -> MsResult<String> {
    match range.unit {
        UvUnit::Meters => Ok(build_uvdist_condition(range, None)),
        UvUnit::Lambda(scale) => {
            let mut clauses = Vec::new();
            for &(ddid, lambda_m) in ddid_lambda_map {
                let scaled = lambda_m * scale;
                clauses.push(format!(
                    "(DATA_DESC_ID=={ddid} AND {})",
                    build_uvdist_condition(range, Some(scaled))
                ));
            }
            if clauses.is_empty() {
                return Err(MsError::VersionError(
                    "uvrange lambda selection could not resolve any DATA_DESCRIPTION rows"
                        .to_string(),
                ));
            }
            Ok(format!("({})", clauses.join(" OR ")))
        }
    }
}

fn build_uvdist_condition(range: &UvSelectionRange, lambda_m: Option<f64>) -> String {
    let distance_expr = "SQUARE(UVW[1]) + SQUARE(UVW[2])";
    let mut terms = Vec::new();
    if let Some(lower) = range.lower {
        let bound = scale_uv_bound(lower.value, lambda_m);
        let op = match lower.op {
            BoundOp::Greater => ">",
            BoundOp::GreaterEqual => ">=",
            BoundOp::Less => "<",
            BoundOp::LessEqual => "<=",
        };
        terms.push(format!(
            "{distance_expr} {op} {}",
            format_float_compact(bound, 6)
        ));
    }
    if let Some(upper) = range.upper {
        let bound = scale_uv_bound(upper.value, lambda_m);
        let op = match upper.op {
            BoundOp::Greater => ">",
            BoundOp::GreaterEqual => ">=",
            BoundOp::Less => "<",
            BoundOp::LessEqual => "<=",
        };
        terms.push(format!(
            "{distance_expr} {op} {}",
            format_float_compact(bound, 6)
        ));
    }
    format!("({})", terms.join(" AND "))
}

fn scale_uv_bound(value: f64, lambda_m: Option<f64>) -> f64 {
    let meters = lambda_m.map(|scale| value * scale).unwrap_or(value);
    meters * meters
}

fn resolve_antenna_ref(ms: &MeasurementSet, value: &str) -> MsResult<i32> {
    if let Ok(id) = value.parse::<i32>() {
        return Ok(id);
    }
    let antenna = ms.antenna()?;
    for row in 0..antenna.row_count() {
        if antenna.name(row)? == value {
            return Ok(row as i32);
        }
    }
    Err(MsError::VersionError(format!(
        "antenna selector {value:?} did not match any ANTENNA rows"
    )))
}

fn collect_used_ids(table: &Table, rows: &[usize], column: &str) -> MsResult<BTreeSet<i32>> {
    let mut ids = BTreeSet::new();
    for &row in rows {
        let id = get_i32(table, row, column)?;
        if id >= 0 {
            ids.insert(id);
        }
    }
    Ok(ids)
}

fn parse_numeric_selector(value: &str, label: &str) -> MsResult<Vec<i32>> {
    let mut values = Vec::new();
    for raw_part in value.split(',') {
        let part = raw_part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((start, end)) = parse_numeric_range(part) {
            values.extend(start..=end);
            continue;
        }
        let parsed = part.parse::<i32>().map_err(|_| {
            MsError::VersionError(format!(
                "{label} selector {part:?} is not a supported numeric id or range"
            ))
        })?;
        values.push(parsed);
    }
    Ok(dedup_i32(values))
}

fn parse_numeric_range(value: &str) -> Option<(i32, i32)> {
    let (start, end) = value.split_once('~')?;
    let start = start.trim().parse::<i32>().ok()?;
    let end = end.trim().parse::<i32>().ok()?;
    let (start, end) = if start <= end {
        (start, end)
    } else {
        (end, start)
    };
    Some((start, end))
}

fn dedup_i32(values: Vec<i32>) -> Vec<i32> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn dedup_strings(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn matches_simple_glob(pattern: &str, candidate: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == candidate;
    }
    let mut rest = candidate;
    let parts = pattern.split('*').collect::<Vec<_>>();
    let anchored_start = !pattern.starts_with('*');
    let anchored_end = !pattern.ends_with('*');

    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if index == 0 && anchored_start {
            if !rest.starts_with(part) {
                return false;
            }
            rest = &rest[part.len()..];
            continue;
        }
        if index == parts.len() - 1 && anchored_end {
            return rest.ends_with(part);
        }
        let Some(position) = rest.find(part) else {
            return false;
        };
        rest = &rest[position + part.len()..];
    }

    true
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
    if let Some(position) = telescope_name.and_then(known_observatory_position) {
        return Ok(Some(position));
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

fn known_observatory_position(name: &str) -> Option<MPosition> {
    match name {
        // CASA MeasTable::Observatory("VLA"), expressed in ITRF meters.
        "VLA" | "EVLA" => Some(MPosition::new_itrf(
            -1_601_185.365,
            -5_041_977.547,
            3_554_875.870,
        )),
        _ => None,
    }
}

fn observatory_offset(position_m: [f64; 3], observatory: Option<&MPosition>) -> [f64; 3] {
    let Some(observatory) = observatory else {
        return [0.0, 0.0, 0.0];
    };
    let position = MPosition::new_itrf(position_m[0], position_m[1], position_m[2]);
    let (long_obs, lat_obs, r_obs) = observatory.as_spherical();
    if r_obs == 0.0 {
        return [0.0, 0.0, 0.0];
    }
    let (long_ant, lat_ant, r_ant) = position.as_spherical();
    [
        (long_ant - long_obs) * r_obs * lat_obs.cos(),
        (lat_ant - lat_obs) * r_obs,
        r_ant - r_obs,
    ]
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

fn truncate_for_column(value: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }

    let mut chars = value.chars().collect::<Vec<_>>();
    if chars.len() > width {
        chars.truncate(width);
        if let Some(last) = chars.last_mut() {
            *last = '*';
        }
    }
    let mut truncated = chars.into_iter().collect::<String>();
    while truncated.chars().count() < width {
        truncated.push(' ');
    }
    truncated
}

fn format_elapsed(value: Option<f64>) -> String {
    match value {
        Some(seconds) => format!("{seconds:.0} seconds"),
        None => "unknown".to_string(),
    }
}

fn frequency_reference_name(code: i32) -> Option<String> {
    FrequencyRef::from_casacore_code(code).map(|reference| reference.as_str().to_string())
}

fn mean_or_zero(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn extract_first_f64_opt(value: Option<&ArrayValue>) -> MsResult<Option<f64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        ArrayValue::Float64(values) => Ok(values.iter().next().copied()),
        other => Err(MsError::ColumnTypeMismatch {
            column: "array".to_string(),
            table: "listobs".to_string(),
            expected: "Float64 array".to_string(),
            found: format!("{:?}", other.primitive_type()),
        }),
    }
}

fn join_or_blank(values: &[String]) -> String {
    values.join(", ")
}

fn format_compact_antenna_lines(antennas: &[AntennaSummary]) -> Vec<String> {
    if antennas.is_empty() {
        return Vec::new();
    }

    let mut lines = Vec::new();
    let mut line = String::new();
    let mut first_id = antennas[0].antenna_id;
    let mut last_id = antennas[0].antenna_id;
    let max_id = antennas
        .iter()
        .map(|antenna| antenna.antenna_id)
        .max()
        .unwrap_or(first_id);

    for antenna in antennas {
        let fragment = format!("'{}'='{}'", antenna.name, antenna.station);
        let mut candidate = line.clone();
        if !candidate.is_empty() {
            candidate.push_str(", ");
        }
        candidate.push_str(&fragment);
        if candidate.len() > 55 && !line.is_empty() {
            lines.push(format!(
                "   ID={:>8}: {}",
                format!("{first_id}-{last_id}"),
                line
            ));
            line = fragment;
            first_id = antenna.antenna_id;
        } else {
            line = candidate;
        }
        last_id = antenna.antenna_id;
        if antenna.antenna_id == max_id {
            lines.push(format!(
                "   ID={:>8}: {}",
                format!("{first_id}-{last_id}"),
                line
            ));
        }
    }

    lines
}

fn join_with_padding(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("{value:<4}"))
        .collect::<String>()
        .trim_end()
        .to_string()
}

fn format_i32_bracket_list(values: &[i32]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn format_f64_bracket_list(values: &[f64]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| format_float_compact(*value, 3))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn format_float_compact(value: f64, decimals: usize) -> String {
    let epsilon = 10f64.powi(-(decimals as i32)) / 2.0;
    let normalized = if value.abs() < epsilon { 0.0 } else { value };
    let mut formatted = format!("{normalized:.decimals$}");
    while formatted.contains('.') && formatted.ends_with('0') {
        formatted.pop();
    }
    if formatted.ends_with('.') {
        formatted.pop();
    }
    formatted
}

fn stokes_name(code: i32) -> &'static str {
    match code {
        1 => "I",
        2 => "Q",
        3 => "U",
        4 => "V",
        5 => "RR",
        6 => "RL",
        7 => "LR",
        8 => "LL",
        9 => "XX",
        10 => "XY",
        11 => "YX",
        12 => "YY",
        _ => "??",
    }
}

fn stokes_code(name: &str) -> Option<i32> {
    match name {
        "I" => Some(1),
        "Q" => Some(2),
        "U" => Some(3),
        "V" => Some(4),
        "RR" => Some(5),
        "RL" => Some(6),
        "LR" => Some(7),
        "LL" => Some(8),
        "XX" => Some(9),
        "XY" => Some(10),
        "YX" => Some(11),
        "YY" => Some(12),
        _ => None,
    }
}

/// Hidden CLI support shared by the `listobs` and `msinfo` binaries.
#[doc(hidden)]
pub mod cli;

#[cfg(test)]
mod tests {
    use super::*;

    use casacore_types::{RecordField, RecordValue, ScalarValue, Value};
    use ndarray::ArrayD;

    use crate::MeasurementSetBuilder;
    use crate::test_helpers::default_value;

    #[test]
    fn summary_collects_core_sections() {
        let ms = make_summary_fixture();

        let summary = ListObsSummary::from_ms(&ms).expect("build summary");
        assert_eq!(summary.schema_version, 1);
        assert_eq!(summary.measurement_set.row_count, 2);
        assert_eq!(summary.observations.len(), 1);
        assert_eq!(summary.fields.len(), 1);
        assert_eq!(summary.spectral_windows.len(), 1);
        assert_eq!(summary.sources.len(), 1);
        assert_eq!(summary.antennas.len(), 2);
        assert_eq!(summary.scans.len(), 1);
        assert_eq!(summary.scans[0].spectral_window_ids, vec![0]);
        assert_eq!(summary.scans[0].field_names, vec!["3C286".to_string()]);
        assert_eq!(
            summary.measurement_set.time_reference.as_deref(),
            Some("UTC")
        );
    }

    #[test]
    fn text_renderer_mentions_casa_sections() {
        let summary = ListObsSummary::from_ms(&make_summary_fixture()).expect("build summary");
        let text = summary.render_text();

        assert!(text.contains("MeasurementSet Name:"));
        assert!(text.contains("ObservationID = 0"));
        assert!(text.contains("Date        Timerange"));
        assert!(text.contains("Fields: 1"));
        assert!(text.contains(
            "Spectral Windows: (1 unique spectral windows and 1 unique polarization setups)"
        ));
        assert!(text.contains("Sources: 1"));
        assert!(text.contains("Antennas: 2:"));
        assert!(text.contains("3C286"));
    }

    #[test]
    fn terse_renderer_omits_verbose_only_sections() {
        let summary = ListObsSummary::from_ms(&make_summary_fixture()).expect("build summary");
        let text = summary.render_text_with_options(&ListObsOptions {
            verbose: false,
            ..ListObsOptions::default()
        });

        assert!(text.contains("Observation: VLA(2 antennas)"));
        assert!(text.contains("Antennas: 2 'name'='station'"));
        assert!(!text.contains("Sources:"));
        assert!(!text.contains("ObservationID ="));
    }

    #[test]
    fn correlation_selection_filters_matching_data_descriptions() {
        let ms = make_selection_fixture();
        let summary = ListObsSummary::from_ms_with_options(
            &ms,
            &ListObsOptions {
                correlation: Some("XX".to_string()),
                ..ListObsOptions::default()
            },
        )
        .expect("build selected summary");

        assert_eq!(summary.measurement_set.row_count, 2);
        assert_eq!(summary.data_descriptions.len(), 1);
        assert_eq!(summary.data_descriptions[0].data_description_id, 0);
        assert_eq!(summary.scans.len(), 2);
    }

    #[test]
    fn intent_selection_filters_matching_state_rows() {
        let ms = make_selection_fixture();
        let summary = ListObsSummary::from_ms_with_options(
            &ms,
            &ListObsOptions {
                intent: Some("CALIBRATE*".to_string()),
                ..ListObsOptions::default()
            },
        )
        .expect("build selected summary");

        assert_eq!(summary.measurement_set.row_count, 1);
        assert_eq!(summary.scans.len(), 1);
        assert_eq!(
            summary.scans[0].scan_intents,
            vec!["CALIBRATE_PHASE.ON_SOURCE".to_string()]
        );
    }

    #[test]
    fn uvrange_selection_filters_rows_in_meters() {
        let ms = make_selection_fixture();
        let summary = ListObsSummary::from_ms_with_options(
            &ms,
            &ListObsOptions {
                uvrange: Some("0~100m".to_string()),
                ..ListObsOptions::default()
            },
        )
        .expect("build selected summary");

        assert_eq!(summary.measurement_set.row_count, 2);
        assert_eq!(summary.scans.len(), 2);
    }

    #[test]
    fn uvrange_selection_filters_rows_in_lambda_units() {
        let ms = make_selection_fixture();
        let summary = ListObsSummary::from_ms_with_options(
            &ms,
            &ListObsOptions {
                uvrange: Some("0~200lambda".to_string()),
                ..ListObsOptions::default()
            },
        )
        .expect("build selected summary");

        assert_eq!(summary.measurement_set.row_count, 1);
        assert_eq!(summary.scans.len(), 1);
        assert_eq!(summary.scans[0].scan_number, 3);
    }

    #[test]
    fn timerange_selection_filters_rows_by_time_of_day() {
        let ms = make_selection_fixture();
        let summary = ListObsSummary::from_ms_with_options(
            &ms,
            &ListObsOptions {
                timerange: Some("11:06:35~11:06:55".to_string()),
                ..ListObsOptions::default()
            },
        )
        .expect("build selected summary");

        assert_eq!(summary.measurement_set.row_count, 2);
        assert_eq!(summary.scans.len(), 2);
        assert_eq!(summary.scans[0].scan_number, 1);
        assert_eq!(summary.scans[1].scan_number, 2);
    }

    #[test]
    fn timerange_interval_selection_filters_rows() {
        let ms = make_selection_fixture();
        let summary = ListObsSummary::from_ms_with_options(
            &ms,
            &ListObsOptions {
                timerange: Some("2016-09-19T11:06:40+00:00:15".to_string()),
                ..ListObsOptions::default()
            },
        )
        .expect("build selected summary");

        assert_eq!(summary.measurement_set.row_count, 2);
        assert_eq!(summary.scans.len(), 2);
        assert_eq!(summary.scans[0].scan_number, 1);
        assert_eq!(summary.scans[1].scan_number, 2);
    }

    #[test]
    fn listunfl_reports_fractional_unflagged_counts() {
        let ms = make_selection_fixture();
        let summary = ListObsSummary::from_ms_with_options(
            &ms,
            &ListObsOptions {
                listunfl: true,
                ..ListObsOptions::default()
            },
        )
        .expect("build listunfl summary");

        assert_eq!(summary.scans[0].unflagged_row_count, Some(0.75));
        assert_eq!(summary.scans[1].unflagged_row_count, Some(0.0));
        assert_eq!(summary.scans[2].unflagged_row_count, Some(1.0));
        let text = summary.render_text();
        assert!(text.contains("nUnflRows"));
        assert!(text.contains("0.75"));
    }

    fn make_summary_fixture() -> MeasurementSet {
        let mut ms = MeasurementSet::create_memory(
            MeasurementSetBuilder::new().with_optional_subtable(crate::SubtableId::Source),
        )
        .expect("create MS");
        add_observation_row(&mut ms, 4_981_000_000.0, 4_981_000_030.0);
        add_field_row(&mut ms);
        add_source_row(&mut ms);
        add_spectral_window_row(&mut ms);
        add_polarization_row(&mut ms);
        add_data_description_row(&mut ms);
        add_antenna_rows(&mut ms);
        add_main_row(&mut ms, 4_981_000_000.0, 0, 1);
        add_main_row(&mut ms, 4_981_000_015.0, 1, 1);
        ms
    }

    fn make_selection_fixture() -> MeasurementSet {
        let mut ms = MeasurementSet::create_memory(
            MeasurementSetBuilder::new().with_optional_subtable(crate::SubtableId::Source),
        )
        .expect("create MS");
        add_observation_row(&mut ms, 4_981_000_000.0, 4_981_000_030.0);
        add_field_row(&mut ms);
        add_source_row(&mut ms);
        add_state_row(&mut ms, "CALIBRATE_PHASE.ON_SOURCE");
        add_state_row(&mut ms, "TARGET.ON_SOURCE");
        add_spectral_window_row_with_ref(&mut ms, "SPW0", 1.4e9);
        add_spectral_window_row_with_ref(&mut ms, "SPW1", 2.8e9);
        add_polarization_row_with_codes(&mut ms, &[9, 12]);
        add_polarization_row_with_codes(&mut ms, &[5, 8]);
        add_data_description_mapping_row(&mut ms, 0, 0);
        add_data_description_mapping_row(&mut ms, 1, 1);
        add_antenna_rows(&mut ms);
        add_main_row_with_selection(&mut ms, 4_981_000_000.0, 1, 1, 0, 0, [30.0, 40.0, 0.0]);
        add_main_row_with_selection(&mut ms, 4_981_000_010.0, 1, 2, 1, 1, [300.0, 400.0, 0.0]);
        add_main_row_with_selection(&mut ms, 4_981_000_020.0, 1, 3, 0, 1, [5.0, 12.0, 0.0]);
        set_main_row_flag_matrix(
            &mut ms,
            0,
            ArrayD::from_shape_vec(vec![2, 2], vec![false, false, false, true]).unwrap(),
        );
        set_main_row_flag_matrix(
            &mut ms,
            1,
            ArrayD::from_shape_vec(vec![2, 2], vec![true, true, true, true]).unwrap(),
        );
        set_main_row_flag_matrix(
            &mut ms,
            2,
            ArrayD::from_shape_vec(vec![2, 2], vec![false, false, false, false]).unwrap(),
        );
        ms
    }

    fn add_observation_row(ms: &mut MeasurementSet, start: f64, end: f64) {
        let table = ms
            .subtable_mut(crate::SubtableId::Observation)
            .expect("OBSERVATION table");
        let row = RecordValue::new(vec![
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "LOG",
                Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(vec![1], vec!["log".to_string()]).unwrap(),
                )),
            ),
            RecordField::new(
                "OBSERVER",
                Value::Scalar(ScalarValue::String("TESTER".to_string())),
            ),
            RecordField::new(
                "PROJECT",
                Value::Scalar(ScalarValue::String("CASA-RS".to_string())),
            ),
            RecordField::new("RELEASE_DATE", Value::Scalar(ScalarValue::Float64(end))),
            RecordField::new(
                "SCHEDULE",
                Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(vec![1], vec!["default".to_string()]).unwrap(),
                )),
            ),
            RecordField::new(
                "SCHEDULE_TYPE",
                Value::Scalar(ScalarValue::String("standard".to_string())),
            ),
            RecordField::new(
                "TELESCOPE_NAME",
                Value::Scalar(ScalarValue::String("VLA".to_string())),
            ),
            RecordField::new(
                "TIME_RANGE",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![2], vec![start, end]).unwrap(),
                )),
            ),
        ]);
        table.add_row(row).unwrap();
    }

    fn add_field_row(ms: &mut MeasurementSet) {
        let table = ms
            .subtable_mut(crate::SubtableId::Field)
            .expect("FIELD table");
        let direction =
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], vec![1.234, 0.456]).unwrap());
        let row = RecordValue::new(vec![
            RecordField::new("CODE", Value::Scalar(ScalarValue::String("C".to_string()))),
            RecordField::new("DELAY_DIR", Value::Array(direction.clone())),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "NAME",
                Value::Scalar(ScalarValue::String("3C286".to_string())),
            ),
            RecordField::new("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("PHASE_DIR", Value::Array(direction.clone())),
            RecordField::new("REFERENCE_DIR", Value::Array(direction)),
            RecordField::new("SOURCE_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(4_981_000_000.0))),
        ]);
        table.add_row(row).unwrap();
    }

    fn add_source_row(ms: &mut MeasurementSet) {
        let table = ms
            .subtable_mut(crate::SubtableId::Source)
            .expect("SOURCE table");
        let row = RecordValue::new(vec![
            RecordField::new("CALIBRATION_GROUP", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("CODE", Value::Scalar(ScalarValue::String("C".to_string()))),
            RecordField::new(
                "DIRECTION",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![2], vec![1.234, 0.456]).unwrap(),
                )),
            ),
            RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(30.0))),
            RecordField::new(
                "NAME",
                Value::Scalar(ScalarValue::String("3C286".to_string())),
            ),
            RecordField::new("NUM_LINES", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new(
                "PROPER_MOTION",
                Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(vec![2], vec![0.0, 0.0]).unwrap(),
                )),
            ),
            RecordField::new("SOURCE_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(4_981_000_000.0))),
        ]);
        table.add_row(row).unwrap();
    }

    fn add_spectral_window_row(ms: &mut MeasurementSet) {
        add_spectral_window_row_with_ref(ms, "SPW0", 1.4e9);
    }

    fn add_spectral_window_row_with_ref(
        ms: &mut MeasurementSet,
        name: &str,
        ref_frequency_hz: f64,
    ) {
        let table = ms
            .subtable_mut(crate::SubtableId::SpectralWindow)
            .expect("SPECTRAL_WINDOW table");
        let freq =
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![2], vec![1.4e9, 1.401e9]).unwrap());
        let width =
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![2], vec![1.0e6, 1.0e6]).unwrap());
        let row = RecordValue::new(vec![
            RecordField::new("CHAN_FREQ", Value::Array(freq)),
            RecordField::new("CHAN_WIDTH", Value::Array(width.clone())),
            RecordField::new("EFFECTIVE_BW", Value::Array(width.clone())),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new(
                "FREQ_GROUP_NAME",
                Value::Scalar(ScalarValue::String("GROUP0".to_string())),
            ),
            RecordField::new("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
            RecordField::new("NAME", Value::Scalar(ScalarValue::String(name.to_string()))),
            RecordField::new("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("NUM_CHAN", Value::Scalar(ScalarValue::Int32(2))),
            RecordField::new(
                "REF_FREQUENCY",
                Value::Scalar(ScalarValue::Float64(ref_frequency_hz)),
            ),
            RecordField::new("RESOLUTION", Value::Array(width.clone())),
            RecordField::new(
                "TOTAL_BANDWIDTH",
                Value::Scalar(ScalarValue::Float64(2.0e6)),
            ),
        ]);
        table.add_row(row).unwrap();
    }

    fn add_polarization_row(ms: &mut MeasurementSet) {
        add_polarization_row_with_codes(ms, &[9, 12]);
    }

    fn add_polarization_row_with_codes(ms: &mut MeasurementSet, corr_types: &[i32]) {
        let table = ms
            .subtable_mut(crate::SubtableId::Polarization)
            .expect("POLARIZATION table");
        let corr_product = match corr_types.len() {
            2 => vec![0, 1, 0, 1],
            4 => vec![0, 0, 1, 1, 0, 1, 0, 1],
            len => vec![0; len * 2],
        };
        let row = RecordValue::new(vec![
            RecordField::new(
                "CORR_PRODUCT",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(vec![2, corr_types.len()], corr_product).unwrap(),
                )),
            ),
            RecordField::new(
                "CORR_TYPE",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(vec![corr_types.len()], corr_types.to_vec()).unwrap(),
                )),
            ),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "NUM_CORR",
                Value::Scalar(ScalarValue::Int32(corr_types.len() as i32)),
            ),
        ]);
        table.add_row(row).unwrap();
    }

    fn add_data_description_row(ms: &mut MeasurementSet) {
        add_data_description_mapping_row(ms, 0, 0);
    }

    fn add_data_description_mapping_row(
        ms: &mut MeasurementSet,
        polarization_id: i32,
        spw_id: i32,
    ) {
        let table = ms
            .subtable_mut(crate::SubtableId::DataDescription)
            .expect("DATA_DESCRIPTION table");
        let row = RecordValue::new(vec![
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            RecordField::new(
                "POLARIZATION_ID",
                Value::Scalar(ScalarValue::Int32(polarization_id)),
            ),
            RecordField::new(
                "SPECTRAL_WINDOW_ID",
                Value::Scalar(ScalarValue::Int32(spw_id)),
            ),
        ]);
        table.add_row(row).unwrap();
    }

    fn add_state_row(ms: &mut MeasurementSet, obs_mode: &str) {
        let table = ms
            .subtable_mut(crate::SubtableId::State)
            .expect("STATE table");
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("CAL", Value::Scalar(ScalarValue::Float64(0.0))),
                RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                RecordField::new("LOAD", Value::Scalar(ScalarValue::Float64(0.0))),
                RecordField::new(
                    "OBS_MODE",
                    Value::Scalar(ScalarValue::String(obs_mode.to_string())),
                ),
                RecordField::new("REF", Value::Scalar(ScalarValue::Bool(false))),
                RecordField::new("SIG", Value::Scalar(ScalarValue::Bool(true))),
                RecordField::new("SUB_SCAN", Value::Scalar(ScalarValue::Int32(0))),
            ]))
            .unwrap();
    }

    fn add_antenna_rows(ms: &mut MeasurementSet) {
        let mut antenna = ms.antenna_mut().expect("ANTENNA accessor");
        antenna
            .add_antenna(
                "VLA01",
                "N01",
                "GROUND-BASED",
                "ALT-AZ",
                [0.0, 10.0, 20.0],
                [0.0, 0.0, 0.0],
                25.0,
            )
            .unwrap();
        antenna
            .add_antenna(
                "VLA02",
                "N02",
                "GROUND-BASED",
                "ALT-AZ",
                [1.0, 11.0, 21.0],
                [0.0, 0.0, 0.0],
                25.0,
            )
            .unwrap();
    }

    fn add_main_row(ms: &mut MeasurementSet, time: f64, antenna2: i32, scan_number: i32) {
        add_main_row_with_selection(ms, time, antenna2, scan_number, 0, -1, [0.0, 0.0, 0.0]);
    }

    fn add_main_row_with_selection(
        ms: &mut MeasurementSet,
        time: f64,
        antenna2: i32,
        scan_number: i32,
        data_desc_id: i32,
        state_id: i32,
        uvw: [f64; 3],
    ) {
        let schema = ms.main_table().schema().unwrap().clone();
        let fields = schema
            .columns()
            .iter()
            .map(|column| match column.name() {
                "ANTENNA1" => RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(0))),
                "ANTENNA2" => {
                    RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(antenna2)))
                }
                "ARRAY_ID" => RecordField::new("ARRAY_ID", Value::Scalar(ScalarValue::Int32(0))),
                "DATA_DESC_ID" => RecordField::new(
                    "DATA_DESC_ID",
                    Value::Scalar(ScalarValue::Int32(data_desc_id)),
                ),
                "FIELD_ID" => RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(0))),
                "INTERVAL" => {
                    RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(15.0)))
                }
                "EXPOSURE" => {
                    RecordField::new("EXPOSURE", Value::Scalar(ScalarValue::Float64(15.0)))
                }
                "OBSERVATION_ID" => {
                    RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0)))
                }
                "SCAN_NUMBER" => RecordField::new(
                    "SCAN_NUMBER",
                    Value::Scalar(ScalarValue::Int32(scan_number)),
                ),
                "STATE_ID" => {
                    RecordField::new("STATE_ID", Value::Scalar(ScalarValue::Int32(state_id)))
                }
                "TIME" => RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(time))),
                "TIME_CENTROID" => {
                    RecordField::new("TIME_CENTROID", Value::Scalar(ScalarValue::Float64(time)))
                }
                "UVW" => RecordField::new(
                    "UVW",
                    Value::Array(ArrayValue::Float64(
                        ArrayD::from_shape_vec(vec![3], uvw.to_vec()).unwrap(),
                    )),
                ),
                name => RecordField::new(name, default_value(name)),
            })
            .collect();
        ms.main_table_mut()
            .add_row(RecordValue::new(fields))
            .unwrap();
    }

    fn set_main_row_flag_matrix(ms: &mut MeasurementSet, row: usize, flags: ArrayD<bool>) {
        ms.main_table_mut()
            .set_cell(row, "FLAG", Value::Array(ArrayValue::Bool(flags)))
            .unwrap();
    }
}
