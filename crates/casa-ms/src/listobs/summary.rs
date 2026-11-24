// SPDX-License-Identifier: LGPL-3.0-or-later
//! Build and render the metadata summary shared by listobs and the MS tool.

use super::*;

impl ListObsSummary {
    /// Build a structured `listobs` summary from an open [`MeasurementSet`].
    pub fn from_ms(ms: &MeasurementSet) -> MsResult<Self> {
        Self::from_ms_with_options(ms, &ListObsOptions::default())
    }

    /// Build a structured `listobs` summary from an open [`MeasurementSet`]
    /// using explicit task-style options.
    pub fn from_ms_with_options(ms: &MeasurementSet, options: &ListObsOptions) -> MsResult<Self> {
        options.validate_supported()?;
        let selected_rows = if !options.selectdata || !options.has_selection() {
            None
        } else {
            Some(resolve_row_indices_with_system_budget(
                ms,
                &MsSelection::from_summary_options(options),
            )?)
        };
        Self::from_rows(ms, options, selected_rows.as_deref(), false, None)
            .map(|(summary, _)| summary)
    }

    /// Summarize the current MS tool view, grouping each scan by field.
    /// Returns the summary and one evaluated phase direction per displayed field.
    /// Row indices are in view order and may contain repeated rows.
    /// The attachment's Measures provider is initialized only when needed.
    pub fn from_tool_view(
        ms: &MeasurementSet,
        selected_rows: Option<&[usize]>,
        verbose: bool,
        listunfl: bool,
        cachesize: f64,
        measures: &OnceLock<Arc<dyn MeasuresProvider>>,
    ) -> MsResult<(Self, Vec<MetadataDirection>)> {
        let options = ListObsOptions {
            verbose,
            listunfl,
            cachesize_mb: Some(cachesize as f32),
            ..ListObsOptions::default()
        };
        Self::from_rows(ms, &options, selected_rows, true, Some(measures))
    }

    fn from_rows(
        ms: &MeasurementSet,
        options: &ListObsOptions,
        selected_rows: Option<&[usize]>,
        tool_view: bool,
        measures: Option<&OnceLock<Arc<dyn MeasuresProvider>>>,
    ) -> MsResult<(Self, Vec<MetadataDirection>)> {
        let unflagged_context = if options.listunfl {
            Some(UnflaggedRowContext::new(ms, !tool_view)?)
        } else {
            None
        };
        let observation_rows = if tool_view { None } else { selected_rows };
        let observations = build_observations(ms, observation_rows)?;
        let main_usage = analyze_main_table(ms, selected_rows, unflagged_context.as_ref())?;
        let (fields, directions) = build_fields(ms, &main_usage, tool_view)?;
        let polarization_setups = build_polarizations(ms)?;
        let data_descriptions = build_data_descriptions(ms, &main_usage)?;
        let spectral_windows =
            build_spectral_windows(ms, &data_descriptions, &polarization_setups)?;
        let used_spw_ids = spectral_windows
            .iter()
            .map(|summary| summary.spectral_window_id as i32)
            .collect::<BTreeSet<_>>();
        let sources = build_sources(ms, &used_spw_ids)?;
        let antennas = build_antennas(
            ms,
            &observations,
            &main_usage,
            tool_view,
            options.verbose,
            measures,
        )?;

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
            selected_rows,
            &field_name_lookup,
            &dd_to_spw,
            &state_intent_lookup,
            main_usage.unflagged_row_fractions.as_deref(),
            tool_view,
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

        let summary = Self {
            schema_version: LISTOBS_SCHEMA_VERSION,
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
        };
        Ok((summary, directions))
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
        self.render_text_for_view(options, false)
    }

    /// Render the text reported by the public MS tool for its current view.
    pub fn render_tool_text(&self) -> String {
        self.render_text_for_view(&self.options, true)
    }

    fn render_observations(&self, out: &mut String, verbose: bool, tool_view: bool) {
        let head = |field: fn(&ObservationSummary) -> &str| {
            if tool_view {
                self.observations.first().map_or("", field)
            } else {
                first_non_empty(self.observations.iter().map(field))
            }
        };
        if tool_view && self.observations.is_empty() {
            let _ = writeln!(out, "The OBSERVATION table is empty");
            return;
        }
        let observer = head(|obs| &obs.observer);
        let project = head(|obs| &obs.project);
        let telescope = head(|obs| &obs.telescope_name);
        let _ = writeln!(out, "Observer: {observer}     Project: {project}");
        if verbose {
            let _ = writeln!(out, "Observation: {telescope}");
        } else {
            let _ = writeln!(
                out,
                "Observation: {telescope}({} antennas)",
                self.measurement_set.antenna_table_count
            );
        }
        if tool_view && self.observations.len() > 1 {
            let _ = writeln!(
                out,
                "\n  {:<10}{:<20}{:<15}Project",
                "Telescope", "Observation Date", "Observer"
            );
            for observation in &self.observations {
                let date = match observation
                    .start_mjd_seconds
                    .zip(observation.end_mjd_seconds)
                {
                    Some((start, end)) => format!("[{start}, {end}]"),
                    None => "[]".to_string(),
                };
                let _ = writeln!(
                    out,
                    "  {:<10}{:<20}{:<15}{}",
                    observation.telescope_name, date, observation.observer, observation.project
                );
            }
            let _ = writeln!(out);
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "predates the function-length limit; #705"
    )]
    fn render_text_for_view(&self, options: &ListObsOptions, tool_view: bool) -> String {
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
        self.render_observations(&mut out, options.verbose, tool_view);
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
            if tool_view {
                self.measurement_set.polarization_count
            } else {
                self.polarization_setups.len()
            },
        );
        let _ = writeln!(
            out,
            "  SpwID  Name                           #Chans   Frame   Ch0(MHz)  ChanWid(kHz)  TotBW(kHz) CtrFreq(MHz)  Corrs"
        );
        for spw in &self.spectral_windows {
            let _ = writeln!(
                out,
                "  {:<7}{:<30}{:>8}   {:<6}{:>10.3}{:>14.3}{:>12.1}{:>13.4}   {}",
                spw.spectral_window_id,
                truncate_for_column(&spw_display_name(spw), 30),
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
