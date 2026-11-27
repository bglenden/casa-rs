// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) struct ArchiveObservation {
    pub(super) id: i32,
    pub(super) projects: String,
    pub(super) start: f64,
    pub(super) end: f64,
}

pub(super) struct VlaImportState {
    pub(super) options: ImportVlaOptions,
    pub(super) ms: MeasurementSet,
    pub(super) measures: Arc<casa_measures::MeasuresRuntime>,
    pub(super) logical_records_seen: usize,
    pub(super) logical_records_imported: usize,
    pub(super) logical_records_skipped: usize,
    pub(super) main_rows_written: usize,
    pub(super) perf_summary: ImportPerfSummary,
    pub(super) antenna_rows: HashMap<String, Vec<(i32, [f64; 3])>>,
    pub(super) last_antenna_rows: Vec<i32>,
    pub(super) field_rows: HashMap<FieldKey, i32>,
    pub(super) source_rows: HashMap<FieldKey, i32>,
    pub(super) archive_observation: Option<ArchiveObservation>,
    pub(super) ms_direction_epoch: Option<DirectionEpoch>,
    pub(super) spectral_windows: Vec<SpectralWindowEntry>,
    pub(super) last_spectral_windows: [Option<usize>; 4],
    pub(super) current_freq_group: i32,
    pub(super) pending_record_freq_group: Option<i32>,
    pub(super) polarization_rows: HashMap<PolarizationKey, i32>,
    pub(super) data_description_rows: HashMap<DataDescriptionKey, i32>,
    pub(super) doppler_rows: Vec<((i32, SpectralDescriptor), i32)>,
    pub(super) last_source_row: Option<i32>,
    pub(super) next_scan_number: i32,
    pub(super) new_scan_pending: bool,
    pub(super) current_scan_by_array: HashMap<i32, i32>,
    pub(super) last_field_by_array: HashMap<i32, i32>,
    pub(super) last_data_desc_ids_by_array: HashMap<i32, Vec<i32>>,
    pub(super) main_write_session: Option<MeasurementSetWriteSession>,
}

impl VlaImportState {
    #[cfg(test)]
    pub(super) fn new(
        options: ImportVlaOptions,
        ms: MeasurementSet,
        row_count: usize,
        channel_count: usize,
    ) -> Result<Self, VlaError> {
        let output = ms
            .path()
            .ok_or_else(|| VlaError::import("test importer requires a disk-backed MS"))?;
        let write_plan = test_import_write_plan(row_count, 4, channel_count)?;
        let main_write_session = MeasurementSetWriteSession::start(output, write_plan)
            .map_err(|error| VlaError::import(error.to_string()))?;
        Self::new_inner(options, ms, main_write_session)
    }

    pub(super) fn new_with_write_session(
        options: ImportVlaOptions,
        ms: MeasurementSet,
        main_write_session: MeasurementSetWriteSession,
    ) -> Result<Self, VlaError> {
        Self::new_inner(options, ms, main_write_session)
    }

    fn new_inner(
        options: ImportVlaOptions,
        ms: MeasurementSet,
        main_write_session: MeasurementSetWriteSession,
    ) -> Result<Self, VlaError> {
        let measures = casa_measures::MeasuresRuntime::open_discovered(Default::default())
            .map(Arc::new)
            .map_err(|error| VlaError::import(error.to_string()))?;
        let mut writer = Self {
            options,
            ms,
            measures,
            logical_records_seen: 0,
            logical_records_imported: 0,
            logical_records_skipped: 0,
            main_rows_written: 0,
            perf_summary: ImportPerfSummary::default(),
            antenna_rows: HashMap::new(),
            last_antenna_rows: Vec::new(),
            field_rows: HashMap::new(),
            source_rows: HashMap::new(),
            archive_observation: None,
            ms_direction_epoch: None,
            spectral_windows: Vec::new(),
            last_spectral_windows: [None; 4],
            current_freq_group: 0,
            pending_record_freq_group: None,
            polarization_rows: HashMap::new(),
            data_description_rows: HashMap::new(),
            doppler_rows: Vec::new(),
            last_source_row: None,
            next_scan_number: 1,
            new_scan_pending: true,
            current_scan_by_array: HashMap::new(),
            last_field_by_array: HashMap::new(),
            last_data_desc_ids_by_array: HashMap::new(),
            main_write_session: Some(main_write_session),
        };
        writer.initialize_epoch_measure_references()?;
        writer.initialize_vla_spectral_window_columns()?;
        Ok(writer)
    }

    pub(super) fn note_skipped_record(&mut self) {
        self.new_scan_pending = true;
    }

    fn initialize_epoch_measure_references(&mut self) -> Result<(), VlaError> {
        for column in ["TIME", "TIME_CENTROID"] {
            set_epoch_measure_reference(self.ms.main_table_mut(), column, EpochRef::TAI)?;
        }
        {
            let mut field = self.ms.field_mut().map_err(|error| {
                VlaError::import(format!("open FIELD for epoch metadata update: {error}"))
            })?;
            set_epoch_measure_reference(field.table_mut(), "TIME", EpochRef::TAI)?;
        }
        {
            let mut feed = self.ms.feed_mut().map_err(|error| {
                VlaError::import(format!("open FEED for epoch metadata update: {error}"))
            })?;
            set_epoch_measure_reference(feed.table_mut(), "TIME", EpochRef::TAI)?;
        }
        {
            let mut source = self.ms.source_mut().map_err(|error| {
                VlaError::import(format!("open SOURCE for epoch metadata update: {error}"))
            })?;
            set_epoch_measure_reference(source.table_mut(), "TIME", EpochRef::TAI)?;
        }
        {
            let mut observation = self.ms.observation_mut().map_err(|error| {
                VlaError::import(format!(
                    "open OBSERVATION for epoch metadata update: {error}"
                ))
            })?;
            for column in ["TIME_RANGE", "RELEASE_DATE"] {
                set_epoch_measure_reference(observation.table_mut(), column, EpochRef::TAI)?;
            }
        }
        Ok(())
    }

    fn initialize_vla_spectral_window_columns(&mut self) -> Result<(), VlaError> {
        let mut spw = self.ms.spectral_window_mut().map_err(|error| {
            VlaError::import(format!(
                "open SPECTRAL_WINDOW for VLA schema initialization: {error}"
            ))
        })?;
        if spw
            .as_ref()
            .table()
            .schema()
            .is_none_or(|schema| !schema.contains_column("DOPPLER_ID"))
        {
            spw.table_mut()
                .add_column(
                    ColumnSchema::scalar("DOPPLER_ID", PrimitiveType::Int32),
                    Some(Value::Scalar(ScalarValue::Int32(-1))),
                )
                .map_err(|error| {
                    VlaError::import(format!("add SPECTRAL_WINDOW.DOPPLER_ID column: {error}"))
                })?;
        }
        Ok(())
    }

    pub(super) fn push_record(&mut self, record: NormalizedRecord) -> Result<(), VlaError> {
        let mut new_scan = self.new_scan_pending;
        self.new_scan_pending = false;
        self.pending_record_freq_group = None;
        let ms_direction_epoch = self.ensure_direction_epoch(record.direction_epoch)?;
        let source_direction = align_direction_for_epoch(
            record.source_direction,
            record.direction_epoch,
            ms_direction_epoch,
        )?;
        let observation_id = self.observe_record(&record)?;
        let field_key = FieldKey {
            source_name: record.source_name.clone(),
            calibration_code: record.calibration_code.clone(),
            ra_bits: source_direction[0].to_bits(),
            dec_bits: source_direction[1].to_bits(),
        };
        let source_id = self.ensure_source(&record, &field_key, source_direction)?;
        let field_id = self.ensure_field(&record, &field_key, source_direction, source_id)?;
        if self.last_field_by_array.get(&record.array_id).copied() != Some(field_id) {
            new_scan = true;
            self.last_field_by_array.insert(record.array_id, field_id);
        }

        let array_id = record.array_id;
        let time_seconds = record.time_seconds;
        let integration_seconds = record.integration_seconds;
        let antennas = record.antennas;
        let mut sorted_antennas = antennas.iter().enumerate().collect::<Vec<_>>();
        sorted_antennas.sort_by_key(|(_, antenna)| antenna.antenna_id);
        if self.last_antenna_rows.len() != antennas.len() {
            self.last_antenna_rows.clear();
        }
        let mut antenna_ids = vec![0; antennas.len()];
        let mut antenna_added = false;
        for (archive_index, antenna) in sorted_antennas {
            let preferred = self.last_antenna_rows.get(archive_index).copied();
            let (id, inserted) = self.ensure_antenna(antenna, preferred)?;
            antenna_ids[archive_index] = id;
            antenna_added |= inserted;
        }
        new_scan |= antenna_added;

        let mut group_data_desc_ids = Vec::with_capacity(record.groups.len());
        let mut group_rows = Vec::with_capacity(record.groups.len());
        let mut current_spectral_windows = [None; 4];

        for group in record.groups {
            let polarization_id = self.ensure_polarization(&group.baselines[0].corr_types)?;
            let preferred = group
                .cda_ids
                .first()
                .and_then(|id| self.last_spectral_windows[id.index()]);
            let spectral_window_id = self.ensure_spectral_window(
                &group.descriptor,
                source_id,
                time_seconds,
                source_direction,
                ms_direction_epoch,
                preferred,
            )?;
            for id in group.cda_ids {
                current_spectral_windows[id.index()] = Some(spectral_window_id as usize);
            }
            let data_desc_id = self.ensure_data_description(spectral_window_id, polarization_id)?;
            group_data_desc_ids.push(data_desc_id);
            group_rows.push((group.baselines, data_desc_id));
        }
        self.last_spectral_windows = current_spectral_windows;

        if self
            .last_data_desc_ids_by_array
            .get(&array_id)
            .is_none_or(|existing| existing != &group_data_desc_ids)
        {
            new_scan = true;
            self.last_data_desc_ids_by_array
                .insert(array_id, group_data_desc_ids);
        }

        let scan_number = if new_scan {
            let scan_number = self.next_scan_number;
            self.next_scan_number += 1;
            self.current_scan_by_array.insert(array_id, scan_number);
            scan_number
        } else {
            *self.current_scan_by_array.get(&array_id).ok_or_else(|| {
                VlaError::import(format!(
                    "missing current scan number for subarray {}",
                    array_id
                ))
            })?
        };

        for (baselines, data_desc_id) in group_rows {
            for baseline in baselines {
                let antenna1_id = antenna_ids[baseline.antenna1_archive];
                let antenna2_id = antenna_ids[baseline.antenna2_archive];
                self.add_main_row(
                    antenna1_id,
                    antenna2_id,
                    field_id,
                    observation_id,
                    data_desc_id,
                    scan_number,
                    array_id,
                    time_seconds,
                    integration_seconds,
                    baseline,
                )?;
            }
        }
        self.last_antenna_rows = antenna_ids;

        Ok(())
    }

    pub(super) fn finish(&mut self) -> Result<(), VlaError> {
        let finish_started = Instant::now();
        self.finish_archive()?;
        let main = self.ms.main_table_mut();
        let mut scan_keywords = main
            .column_keywords("SCAN_NUMBER")
            .cloned()
            .unwrap_or_default();
        scan_keywords.upsert(
            "LAST_SCAN",
            Value::Array(ArrayValue::from_i32_vec(
                (0..VLA_SUBARRAY_COUNT)
                    .map(|array| self.current_scan_by_array.get(&array).copied().unwrap_or(0))
                    .collect(),
            )),
        );
        main.set_column_keywords("SCAN_NUMBER", scan_keywords);
        self.perf_summary.finish_metadata_ns = finish_started.elapsed().as_nanos() as u64;
        let save_started = Instant::now();
        let main_write_session = self
            .main_write_session
            .take()
            .ok_or_else(|| VlaError::import("MAIN write session was already finalized"))?;
        let mut overrides = casa_tables::ColumnOverrides::for_row_count(self.main_rows_written);
        for column in [
            "DATA",
            "CORRECTED_DATA",
            "MODEL_DATA",
            "FLAG",
            "FLAG_CATEGORY",
            "WEIGHT",
            "SIGMA",
        ] {
            overrides.insert_deferred(column);
        }
        overrides.insert_deferred("UVW");
        let write_telemetry = main_write_session
            .save_and_finish(&mut self.ms, &overrides)
            .map_err(|error| VlaError::import(error.to_string()))?;
        self.perf_summary.write_producer_ns = seconds_to_nanos(write_telemetry.producer_seconds);
        self.perf_summary.write_queue_wait_ns =
            seconds_to_nanos(write_telemetry.queue_wait_seconds);
        self.perf_summary.write_assemble_ns = seconds_to_nanos(write_telemetry.assemble_seconds);
        self.perf_summary.write_physical_ns = seconds_to_nanos(write_telemetry.write_seconds);
        self.perf_summary.write_finalize_ns = seconds_to_nanos(write_telemetry.finalize_seconds);
        self.perf_summary.write_rows = write_telemetry.rows_written;
        self.perf_summary.write_bytes = write_telemetry.bytes_written;
        self.perf_summary.write_peak_planned_resident_bytes =
            write_telemetry.maximum_resident_bytes;
        let result = Ok(());
        self.perf_summary.save_ns = save_started.elapsed().as_nanos() as u64;
        result
    }

    fn ensure_direction_epoch(
        &mut self,
        epoch: DirectionEpoch,
    ) -> Result<DirectionEpoch, VlaError> {
        if let Some(existing) = self.ms_direction_epoch {
            return Ok(existing);
        }
        set_direction_measure_reference(self.ms.main_table_mut(), "UVW", epoch)?;
        {
            let mut field = self.ms.field_mut().map_err(|error| {
                VlaError::import(format!("open FIELD for metadata update: {error}"))
            })?;
            for column in ["DELAY_DIR", "PHASE_DIR", "REFERENCE_DIR"] {
                set_direction_measure_reference(field.table_mut(), column, epoch)?;
            }
        }
        {
            let mut source = self.ms.source_mut().map_err(|error| {
                VlaError::import(format!("open SOURCE for metadata update: {error}"))
            })?;
            set_direction_measure_reference(source.table_mut(), "DIRECTION", epoch)?;
        }
        self.ms_direction_epoch = Some(epoch);
        Ok(epoch)
    }

    fn ensure_antenna(
        &mut self,
        antenna: &NormalizedAntenna,
        preferred: Option<i32>,
    ) -> Result<(i32, bool), VlaError> {
        if let Some(entries) = self.antenna_rows.get(&antenna.name) {
            let matches_position = |position: &[f64; 3]| {
                position
                    .iter()
                    .zip(antenna.position_itrf_m)
                    .all(|(stored, incoming)| (stored - incoming).abs() <= 1.0)
            };
            let previous = preferred.and_then(|id| entries.iter().find(|(row, _)| *row == id));
            if let Some(&(row, _)) = previous
                .into_iter()
                .chain(entries.iter().rev())
                .find(|(_, position)| matches_position(position))
            {
                return Ok((row, false));
            }
        }
        let row = {
            let mut ant = self
                .ms
                .antenna_mut()
                .map_err(|error| VlaError::import(format!("open ANTENNA for write: {error}")))?;
            i32::try_from(
                ant.add_antenna(
                    &antenna.name,
                    &antenna.station,
                    "GROUND-BASED",
                    "ALT-AZ",
                    antenna.position_itrf_m,
                    [0.0, 0.0, 0.0],
                    25.0,
                )
                .map_err(|error| VlaError::import(format!("add ANTENNA row: {error}")))?,
            )
            .map_err(|_| VlaError::import("ANTENNA row index does not fit in i32"))?
        };
        self.add_feed_row(row)?;
        self.antenna_rows
            .entry(antenna.name.clone())
            .or_default()
            .push((row, antenna.position_itrf_m));
        Ok((row, true))
    }

    fn add_feed_row(&mut self, antenna_id: i32) -> Result<(), VlaError> {
        let feed_table = self
            .ms
            .subtable_mut(SubtableId::Feed)
            .ok_or_else(|| VlaError::import("missing FEED subtable"))?;
        let pol_response = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 2]).f(),
                vec![
                    Complex32::new(1.0, 0.0),
                    Complex32::new(0.0, 0.0),
                    Complex32::new(0.0, 0.0),
                    Complex32::new(1.0, 0.0),
                ],
            )
            .map_err(|error| VlaError::import(format!("shape FEED.POL_RESPONSE: {error}")))?,
        );
        let polarization_type = ArrayValue::String(
            ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec!["R".to_string(), "L".to_string()])
                .map_err(|error| {
                    VlaError::import(format!("shape FEED.POLARIZATION_TYPE: {error}"))
                })?,
        );
        let receptor_angle = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![0.0, 0.0])
                .map_err(|error| VlaError::import(format!("shape FEED.RECEPTOR_ANGLE: {error}")))?,
        );
        let beam_offset = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![0.0; 4])
                .map_err(|error| VlaError::import(format!("shape FEED.BEAM_OFFSET: {error}")))?,
        );
        let position = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[3]).f(), vec![0.0, 0.0, 0.0])
                .map_err(|error| VlaError::import(format!("shape FEED.POSITION: {error}")))?,
        );
        let row = make_row_from_columns(
            casa_ms::schema::feed::REQUIRED_COLUMNS,
            &[
                ("ANTENNA_ID", Value::Scalar(ScalarValue::Int32(antenna_id))),
                ("BEAM_ID", Value::Scalar(ScalarValue::Int32(-1))),
                ("BEAM_OFFSET", Value::Array(beam_offset)),
                ("FEED_ID", Value::Scalar(ScalarValue::Int32(0))),
                ("INTERVAL", Value::Scalar(ScalarValue::Float64(0.0))),
                ("NUM_RECEPTORS", Value::Scalar(ScalarValue::Int32(2))),
                ("POL_RESPONSE", Value::Array(pol_response)),
                ("POLARIZATION_TYPE", Value::Array(polarization_type)),
                ("POSITION", Value::Array(position)),
                ("RECEPTOR_ANGLE", Value::Array(receptor_angle)),
                ("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(-1))),
                ("TIME", Value::Scalar(ScalarValue::Float64(0.0))),
            ],
        )?;
        feed_table
            .prepare_row_appender()
            .and_then(|mut appender| appender.append(row))
            .map_err(|error| VlaError::import(format!("add FEED row: {error}")))
    }

    fn observe_record(&mut self, record: &NormalizedRecord) -> Result<i32, VlaError> {
        if self.archive_observation.is_none() {
            let table = self
                .ms
                .subtable(SubtableId::Observation)
                .ok_or_else(|| VlaError::import("missing OBSERVATION subtable"))?;
            let id = i32::try_from(table.row_count())
                .map_err(|_| VlaError::import("OBSERVATION row count too large"))?;
            self.archive_observation = Some(ArchiveObservation {
                id,
                projects: String::new(),
                start: record.time_seconds,
                end: record.time_seconds,
            });
        }
        let observation = self
            .archive_observation
            .as_mut()
            .expect("initialized above");
        if !observation.projects.contains(&record.project) {
            if !observation.projects.is_empty() {
                observation.projects.push(' ');
            }
            observation.projects.push_str(&record.project);
        }
        observation.end = record.time_seconds;
        Ok(observation.id)
    }

    pub(super) fn finish_archive(&mut self) -> Result<(), VlaError> {
        self.new_scan_pending = true;
        self.last_field_by_array.clear();
        self.last_data_desc_ids_by_array.clear();
        self.last_spectral_windows = [None; 4];
        self.last_antenna_rows.clear();
        let Some(observation) = self.archive_observation.take() else {
            return Ok(());
        };
        let log = ArrayValue::String(
            ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec!["unavailable".to_string()])
                .map_err(|error| VlaError::import(format!("shape OBSERVATION.LOG: {error}")))?,
        );
        let schedule = ArrayValue::String(
            ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec!["unavailable".to_string()]).map_err(
                |error| VlaError::import(format!("shape OBSERVATION.SCHEDULE: {error}")),
            )?,
        );
        let time_range = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![observation.start, observation.end])
                .map_err(|error| {
                    VlaError::import(format!("shape OBSERVATION.TIME_RANGE: {error}"))
                })?,
        );
        let table = self
            .ms
            .subtable_mut(SubtableId::Observation)
            .ok_or_else(|| VlaError::import("missing OBSERVATION subtable"))?;
        let row = make_row_from_columns(
            casa_ms::schema::observation::REQUIRED_COLUMNS,
            &[
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ("LOG", Value::Array(log)),
                (
                    "OBSERVER",
                    Value::Scalar(ScalarValue::String("unavailable".to_string())),
                ),
                (
                    "PROJECT",
                    Value::Scalar(ScalarValue::String(observation.projects)),
                ),
                (
                    "RELEASE_DATE",
                    Value::Scalar(ScalarValue::Float64(
                        observation.end + 1.5 * SECONDS_PER_JULIAN_YEAR,
                    )),
                ),
                ("SCHEDULE", Value::Array(schedule)),
                (
                    "SCHEDULE_TYPE",
                    Value::Scalar(ScalarValue::String("unknown".to_string())),
                ),
                (
                    "TELESCOPE_NAME",
                    Value::Scalar(ScalarValue::String(DEFAULT_TELESCOPE_NAME.to_string())),
                ),
                ("TIME_RANGE", Value::Array(time_range)),
            ],
        )?;
        table
            .prepare_row_appender()
            .and_then(|mut appender| appender.append(row))
            .map_err(|error| VlaError::import(format!("add OBSERVATION row: {error}")))?;
        Ok(())
    }

    fn ensure_source(
        &mut self,
        record: &NormalizedRecord,
        field_key: &FieldKey,
        source_direction: [f64; 2],
    ) -> Result<i32, VlaError> {
        if let Some(&row) = self.source_rows.get(field_key) {
            return Ok(row);
        }
        let direction = ArrayValue::Float64(
            ArrayD::from_shape_vec(
                IxDyn(&[2]).f(),
                vec![source_direction[0], source_direction[1]],
            )
            .map_err(|error| VlaError::import(format!("shape SOURCE.DIRECTION: {error}")))?,
        );
        let proper_motion = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![0.0, 0.0]).map_err(|error| {
                VlaError::import(format!("shape SOURCE.PROPER_MOTION: {error}"))
            })?,
        );
        let row = {
            let table = self
                .ms
                .subtable_mut(SubtableId::Source)
                .ok_or_else(|| VlaError::import("missing SOURCE subtable"))?;
            let row_index = i32::try_from(table.row_count())
                .map_err(|_| VlaError::import("SOURCE row count too large"))?;
            let row = make_row_from_columns(
                casa_ms::schema::source::REQUIRED_COLUMNS,
                &[
                    ("CALIBRATION_GROUP", Value::Scalar(ScalarValue::Int32(0))),
                    (
                        "CODE",
                        Value::Scalar(ScalarValue::String(record.calibration_code.clone())),
                    ),
                    ("DIRECTION", Value::Array(direction)),
                    ("INTERVAL", Value::Scalar(ScalarValue::Float64(0.0))),
                    (
                        "NAME",
                        Value::Scalar(ScalarValue::String(record.source_name.clone())),
                    ),
                    (
                        "NUM_LINES",
                        Value::Scalar(ScalarValue::Int32(record.source_num_lines)),
                    ),
                    ("PROPER_MOTION", Value::Array(proper_motion)),
                    ("SOURCE_ID", Value::Scalar(ScalarValue::Int32(row_index))),
                    ("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(-1))),
                    (
                        "TIME",
                        Value::Scalar(ScalarValue::Float64(record.time_seconds)),
                    ),
                ],
            )?;
            table
                .prepare_row_appender()
                .and_then(|mut appender| appender.append(row))
                .map_err(|error| VlaError::import(format!("add SOURCE row: {error}")))?;
            row_index
        };
        self.source_rows.insert(field_key.clone(), row);
        self.last_source_row = Some(row);
        Ok(row)
    }

    fn ensure_field(
        &mut self,
        record: &NormalizedRecord,
        field_key: &FieldKey,
        source_direction: [f64; 2],
        source_id: i32,
    ) -> Result<i32, VlaError> {
        if let Some(&row) = self.field_rows.get(field_key) {
            return Ok(row);
        }
        let dir = ArrayValue::Float64(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 1]).f(),
                vec![source_direction[0], source_direction[1]],
            )
            .map_err(|error| VlaError::import(format!("shape FIELD direction array: {error}")))?,
        );
        let row = {
            let table = self
                .ms
                .subtable_mut(SubtableId::Field)
                .ok_or_else(|| VlaError::import("missing FIELD subtable"))?;
            let row_index = i32::try_from(table.row_count())
                .map_err(|_| VlaError::import("FIELD row count too large"))?;
            let row = make_row_from_columns(
                casa_ms::schema::field::REQUIRED_COLUMNS,
                &[
                    (
                        "CODE",
                        Value::Scalar(ScalarValue::String(record.calibration_code.clone())),
                    ),
                    ("DELAY_DIR", Value::Array(dir.clone())),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                    (
                        "NAME",
                        Value::Scalar(ScalarValue::String(record.source_name.clone())),
                    ),
                    ("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
                    ("PHASE_DIR", Value::Array(dir.clone())),
                    ("REFERENCE_DIR", Value::Array(dir)),
                    ("SOURCE_ID", Value::Scalar(ScalarValue::Int32(source_id))),
                    (
                        "TIME",
                        Value::Scalar(ScalarValue::Float64(record.time_seconds)),
                    ),
                ],
            )?;
            table
                .prepare_row_appender()
                .and_then(|mut appender| appender.append(row))
                .map_err(|error| VlaError::import(format!("add FIELD row: {error}")))?;
            row_index
        };
        self.field_rows.insert(field_key.clone(), row);
        Ok(row)
    }

    fn ensure_polarization(&mut self, corr_types: &[i32]) -> Result<i32, VlaError> {
        let key = PolarizationKey {
            corr_types: corr_types.to_vec(),
        };
        if let Some(&row) = self.polarization_rows.get(&key) {
            return Ok(row);
        }
        let corr_product = corr_product_array(corr_types)?;
        let corr_type = ArrayValue::Int32(
            ArrayD::from_shape_vec(IxDyn(&[corr_types.len()]).f(), corr_types.to_vec()).map_err(
                |error| VlaError::import(format!("shape POLARIZATION.CORR_TYPE: {error}")),
            )?,
        );
        let row = {
            let table = self
                .ms
                .subtable_mut(SubtableId::Polarization)
                .ok_or_else(|| VlaError::import("missing POLARIZATION subtable"))?;
            let row_index = i32::try_from(table.row_count())
                .map_err(|_| VlaError::import("POLARIZATION row count too large"))?;
            let row = make_row_from_columns(
                casa_ms::schema::polarization::REQUIRED_COLUMNS,
                &[
                    ("CORR_PRODUCT", Value::Array(corr_product)),
                    ("CORR_TYPE", Value::Array(corr_type)),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                    (
                        "NUM_CORR",
                        Value::Scalar(ScalarValue::Int32(
                            i32::try_from(corr_types.len())
                                .map_err(|_| VlaError::import("NUM_CORR does not fit in i32"))?,
                        )),
                    ),
                ],
            )?;
            table
                .prepare_row_appender()
                .and_then(|mut appender| appender.append(row))
                .map_err(|error| VlaError::import(format!("add POLARIZATION row: {error}")))?;
            row_index
        };
        self.polarization_rows.insert(key, row);
        Ok(row)
    }

    fn ensure_doppler(
        &mut self,
        source_id: i32,
        descriptor: &SpectralDescriptor,
    ) -> Result<i32, VlaError> {
        let source_id = self.last_source_row.unwrap_or(source_id);
        if let Some((_, row)) = self
            .doppler_rows
            .iter()
            .find(|((existing_source, existing), _)| {
                *existing_source == source_id
                    && same_spectral_descriptor(existing, descriptor, self.options.frequencytol_hz)
            })
        {
            return Ok(*row);
        }
        let row = {
            let table = self
                .ms
                .subtable_mut(SubtableId::Doppler)
                .ok_or_else(|| VlaError::import("missing DOPPLER subtable"))?;
            let row_index = i32::try_from(table.row_count())
                .map_err(|_| VlaError::import("DOPPLER row count too large"))?;
            let row = make_row_from_columns(
                casa_ms::schema::doppler::REQUIRED_COLUMNS,
                &[
                    ("DOPPLER_ID", Value::Scalar(ScalarValue::Int32(row_index))),
                    ("SOURCE_ID", Value::Scalar(ScalarValue::Int32(source_id))),
                    ("TRANSITION_ID", Value::Scalar(ScalarValue::Int32(0))),
                    (
                        "VELDEF",
                        Value::Scalar(ScalarValue::Float64(if descriptor.doppler_tracking {
                            descriptor.doppler_velocity_mps
                        } else {
                            0.0
                        })),
                    ),
                ],
            )?;
            table
                .prepare_row_appender()
                .and_then(|mut appender| appender.append(row))
                .map_err(|error| VlaError::import(format!("add DOPPLER row: {error}")))?;
            row_index
        };
        self.doppler_rows
            .push(((source_id, descriptor.clone()), row));
        Ok(row)
    }

    fn ensure_spectral_window(
        &mut self,
        descriptor: &SpectralDescriptor,
        source_id: i32,
        time_seconds: f64,
        source_direction: [f64; 2],
        direction_epoch: DirectionEpoch,
        preferred: Option<usize>,
    ) -> Result<i32, VlaError> {
        let measures: Arc<dyn MeasuresProvider> = self.measures.clone();
        let previous = preferred.and_then(|index| self.spectral_windows.get(index));
        let newest = self
            .spectral_windows
            .iter()
            .rev()
            .filter(|entry| Some(entry.row as usize) != preferred);
        for entry in previous.into_iter().chain(newest) {
            if spectral_window_matches(
                entry,
                descriptor,
                time_seconds,
                source_direction,
                direction_epoch,
                self.options.frequencytol_hz,
                measures.clone(),
            )? {
                return Ok(entry.row);
            }
        }
        let freq_group = if let Some(group) = self.pending_record_freq_group {
            group
        } else {
            self.current_freq_group += 1;
            self.pending_record_freq_group = Some(self.current_freq_group);
            self.current_freq_group
        };

        let frame =
            spectral_conversion_frame(time_seconds, source_direction, direction_epoch, measures)?;
        let ref_freq = spectral_reference_frequency(descriptor);
        let meas_freq_ref = frequency_reference_for_descriptor(descriptor);
        let chan_freq: Vec<f64> = (0..descriptor.num_chan)
            .map(|index| {
                descriptor.edge_frequency_hz + (index as f64 + 0.5) * descriptor.channel_width_hz
            })
            .map(|hz| convert_topocentric_frequency(hz, meas_freq_ref, &frame))
            .collect::<Result<Vec<_>, _>>()?;
        let converted_width = if meas_freq_ref == FrequencyRef::TOPO {
            descriptor.channel_width_hz
        } else {
            convert_topocentric_frequency(descriptor.channel_width_hz, meas_freq_ref, &frame)?
        };
        let widths = vec![converted_width; descriptor.num_chan];
        let stored_bandwidth_hz = widths.iter().sum();
        let resolution = widths.clone();
        let chan_freq_array = ArrayValue::Float64(
            ArrayD::from_shape_vec(IxDyn(&[descriptor.num_chan]).f(), chan_freq).map_err(
                |error| VlaError::import(format!("shape SPECTRAL_WINDOW.CHAN_FREQ: {error}")),
            )?,
        );
        let width_array = |values: Vec<f64>, column: &str| -> Result<ArrayValue, VlaError> {
            Ok(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[descriptor.num_chan]).f(), values).map_err(
                    |error| VlaError::import(format!("shape SPECTRAL_WINDOW.{column}: {error}")),
                )?,
            ))
        };
        let doppler_id = self.ensure_doppler(source_id, descriptor)?;
        let row = {
            let table = self
                .ms
                .subtable_mut(SubtableId::SpectralWindow)
                .ok_or_else(|| VlaError::import("missing SPECTRAL_WINDOW subtable"))?;
            let row_index = i32::try_from(table.row_count())
                .map_err(|_| VlaError::import("SPECTRAL_WINDOW row count too large"))?;
            let mut row = make_row_from_columns(
                casa_ms::schema::spectral_window::REQUIRED_COLUMNS,
                &[
                    (
                        "NUM_CHAN",
                        Value::Scalar(ScalarValue::Int32(
                            i32::try_from(descriptor.num_chan)
                                .map_err(|_| VlaError::import("NUM_CHAN does not fit in i32"))?,
                        )),
                    ),
                    (
                        "NAME",
                        Value::Scalar(ScalarValue::String(spectral_window_name(
                            descriptor,
                            ref_freq.hz(),
                            meas_freq_ref,
                        ))),
                    ),
                    (
                        "REF_FREQUENCY",
                        Value::Scalar(ScalarValue::Float64(ref_freq.hz())),
                    ),
                    (
                        "TOTAL_BANDWIDTH",
                        Value::Scalar(ScalarValue::Float64(stored_bandwidth_hz)),
                    ),
                    ("CHAN_FREQ", Value::Array(chan_freq_array)),
                    (
                        "CHAN_WIDTH",
                        Value::Array(width_array(widths.clone(), "CHAN_WIDTH")?),
                    ),
                    (
                        "EFFECTIVE_BW",
                        Value::Array(width_array(widths, "EFFECTIVE_BW")?),
                    ),
                    (
                        "RESOLUTION",
                        Value::Array(width_array(resolution, "RESOLUTION")?),
                    ),
                    (
                        "MEAS_FREQ_REF",
                        Value::Scalar(ScalarValue::Int32(meas_freq_ref.casacore_code())),
                    ),
                    ("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
                    ("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(freq_group))),
                    (
                        "FREQ_GROUP_NAME",
                        Value::Scalar(ScalarValue::String(format!("Group {freq_group}"))),
                    ),
                    (
                        "IF_CONV_CHAIN",
                        Value::Scalar(ScalarValue::Int32(descriptor.if_conv_chain)),
                    ),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ],
            )?;
            row.push(RecordField::new(
                "DOPPLER_ID",
                Value::Scalar(ScalarValue::Int32(doppler_id)),
            ));
            table
                .prepare_row_appender()
                .and_then(|mut appender| appender.append(row))
                .map_err(|error| VlaError::import(format!("add SPECTRAL_WINDOW row: {error}")))?;
            row_index
        };
        self.spectral_windows.push(SpectralWindowEntry {
            descriptor: descriptor.clone(),
            stored_bandwidth_hz,
            row,
            time_seconds,
            source_direction,
            direction_epoch,
        });
        Ok(row)
    }

    fn ensure_data_description(
        &mut self,
        spectral_window_id: i32,
        polarization_id: i32,
    ) -> Result<i32, VlaError> {
        let key = DataDescriptionKey {
            spectral_window_id,
            polarization_id,
        };
        if let Some(&row) = self.data_description_rows.get(&key) {
            return Ok(row);
        }
        let row = {
            let table = self
                .ms
                .subtable_mut(SubtableId::DataDescription)
                .ok_or_else(|| VlaError::import("missing DATA_DESCRIPTION subtable"))?;
            let row_index = i32::try_from(table.row_count())
                .map_err(|_| VlaError::import("DATA_DESCRIPTION row count too large"))?;
            let row = make_row_from_columns(
                casa_ms::schema::data_description::REQUIRED_COLUMNS,
                &[
                    (
                        "SPECTRAL_WINDOW_ID",
                        Value::Scalar(ScalarValue::Int32(spectral_window_id)),
                    ),
                    (
                        "POLARIZATION_ID",
                        Value::Scalar(ScalarValue::Int32(polarization_id)),
                    ),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ],
            )?;
            table
                .prepare_row_appender()
                .and_then(|mut appender| appender.append(row))
                .map_err(|error| VlaError::import(format!("add DATA_DESCRIPTION row: {error}")))?;
            row_index
        };
        self.data_description_rows.insert(key, row);
        Ok(row)
    }

    #[allow(clippy::too_many_arguments)]
    fn add_main_row(
        &mut self,
        antenna1_id: i32,
        antenna2_id: i32,
        field_id: i32,
        observation_id: i32,
        data_desc_id: i32,
        scan_number: i32,
        array_id: i32,
        time_seconds: f64,
        integration_seconds: f64,
        baseline: NormalizedBaselineRow,
    ) -> Result<(), VlaError> {
        let NormalizedBaselineRow {
            corr_types,
            uvw_m,
            data,
            corrected_data,
            model_data,
            flag,
            flag_category,
            weight,
            sigma,
            ..
        } = baseline;
        let (
            row_ant1,
            row_ant2,
            row_data,
            row_corrected,
            row_model,
            row_flag,
            row_flag_category,
            row_weight,
            row_sigma,
            _row_corr_types,
        ) = {
            let reorder_started = Instant::now();
            let result = reorder_baseline_for_ms(
                antenna1_id,
                antenna2_id,
                data,
                corrected_data,
                model_data,
                flag,
                flag_category,
                weight,
                sigma,
                &corr_types,
            )?;
            self.perf_summary.reorder_baseline_ns += reorder_started.elapsed().as_nanos() as u64;
            result
        };
        let row_uvw = if row_ant1 == antenna1_id && row_ant2 == antenna2_id {
            uvw_m
        } else {
            [-uvw_m[0], -uvw_m[1], -uvw_m[2]]
        };
        let row_flag_row = {
            let flag_started = Instant::now();
            let value = flag_row_value(&row_flag)?;
            self.perf_summary.flag_row_ns += flag_started.elapsed().as_nanos() as u64;
            value
        };
        {
            let writer = self
                .main_write_session
                .as_mut()
                .ok_or_else(|| VlaError::import("MAIN write session is unavailable"))?;
            let append_started = Instant::now();
            push_import_complex32_row(writer, "DATA", &row_data)?;
            push_import_complex32_row(writer, "CORRECTED_DATA", &row_corrected)?;
            push_import_complex32_row(writer, "MODEL_DATA", &row_model)?;
            push_import_bool_row(writer, "FLAG", &row_flag)?;
            push_import_bool_row(writer, "FLAG_CATEGORY", &row_flag_category)?;
            push_import_f32_row(writer, "WEIGHT", &row_weight)?;
            push_import_f32_row(writer, "SIGMA", &row_sigma)?;
            writer
                .push_f64_row("UVW", &[3], &row_uvw)
                .map_err(|error| VlaError::import(error.to_string()))?;
            push_import_scalar_row(
                writer,
                row_ant1,
                row_ant2,
                array_id,
                data_desc_id,
                integration_seconds,
                field_id,
                row_flag_row,
                observation_id,
                scan_number,
                time_seconds,
            )?;
            self.perf_summary.append_main_row_ns += append_started.elapsed().as_nanos() as u64;
        }
        self.main_rows_written += 1;
        Ok(())
    }
}
