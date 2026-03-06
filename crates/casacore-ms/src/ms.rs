// SPDX-License-Identifier: LGPL-3.0-or-later
//! High-level MeasurementSet type that manages main table + subtables as a unit.
//!
//! A [`MeasurementSet`] bundles the MS main table with its required (and
//! optional) subtables. It can be created from scratch via
//! [`MeasurementSet::create`], opened from disk via [`MeasurementSet::open`],
//! or assembled in memory for testing.
//!
//! Cf. C++ `MeasurementSet` class.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use casacore_tables::{Table, TableInfo, TableOptions};
use casacore_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};

use crate::builder::{MeasurementSetBuilder, MsSchemas};
use crate::column_def::ColumnDef;
use crate::error::{MsError, MsResult};
use crate::metadata::{measinfo_for, quantum_units_for};
use crate::schema::SubtableId;
use crate::subtables::{
    MsAntenna, MsAntennaMut, MsDataDescription, MsDataDescriptionMut, MsDoppler, MsDopplerMut,
    MsFeed, MsFeedMut, MsField, MsFieldMut, MsFlagCmd, MsFlagCmdMut, MsFreqOffset, MsFreqOffsetMut,
    MsHistory, MsHistoryMut, MsObservation, MsObservationMut, MsPointing, MsPointingMut,
    MsPolarization, MsPolarizationMut, MsProcessor, MsProcessorMut, MsSource, MsSourceMut,
    MsSpectralWindow, MsSpectralWindowMut, MsState, MsStateMut, MsSysCal, MsSysCalMut, MsWeather,
    MsWeatherMut,
};
use crate::validate::{self, ValidationIssue};

/// The MS version number written by this crate.
pub const MS_VERSION: f32 = 2.0;
const CASACORE_MS_TABLE_TYPE: &str = "Measurement Set";

/// A MeasurementSet: main table + subtables.
///
/// This is the top-level type for working with MS data. It owns the main
/// table and all attached subtables (12 required + 5 optional).
///
/// When saved to disk, subtable links are emitted as true casacore `TpTable`
/// keywords, using the same relative-path encoding that C++ `MeasurementSet`
/// writes for sibling subtables.
///
/// # Example
///
/// ```rust
/// use casacore_ms::ms::MeasurementSet;
/// use casacore_ms::builder::MeasurementSetBuilder;
///
/// // Create an in-memory MS
/// let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new())
///     .expect("create MS");
/// assert_eq!(ms.row_count(), 0);
/// assert_eq!(ms.subtable_ids().len(), 12);
/// ```
pub struct MeasurementSet {
    main: Table,
    subtables: HashMap<SubtableId, Table>,
    subtable_paths: HashMap<SubtableId, PathBuf>,
    path: Option<PathBuf>,
}

impl MeasurementSet {
    /// Create an in-memory MeasurementSet from a builder.
    ///
    /// The resulting MS has empty tables with the correct schemas and
    /// the MS_VERSION keyword set.
    pub fn create_memory(builder: MeasurementSetBuilder) -> MsResult<Self> {
        let schemas = builder.build_schemas()?;
        Self::from_schemas(schemas, None)
    }

    /// Create an MS on disk at the given path.
    ///
    /// Creates the main table directory and subtable subdirectories.
    pub fn create(path: impl AsRef<Path>, builder: MeasurementSetBuilder) -> MsResult<Self> {
        let path = path.as_ref().to_path_buf();
        let schemas = builder.build_schemas()?;
        let mut ms = Self::from_schemas(schemas, Some(path.clone()))?;
        ms.save()?;
        Ok(ms)
    }

    /// Open an existing MS from disk.
    ///
    /// Opens the main table and follows subtable keywords to open subtables.
    /// casacore-style `TpTable` keywords are preferred, but legacy string
    /// keywords written by older versions of this crate are also accepted.
    pub fn open(path: impl AsRef<Path>) -> MsResult<Self> {
        let path = path.as_ref().to_path_buf();
        let main = Table::open(TableOptions::new(&path))?;

        let mut subtables = HashMap::new();
        let mut subtable_paths = HashMap::new();

        // Try to open each known subtable
        let all_ids: Vec<SubtableId> = SubtableId::ALL_REQUIRED
            .iter()
            .chain(SubtableId::ALL_OPTIONAL.iter())
            .copied()
            .collect();

        for id in all_ids {
            if let Some(subtable_path) = subtable_path_from_main(&main, &path, id) {
                let table = Table::open(TableOptions::new(&subtable_path))?;
                subtables.insert(id, table);
                subtable_paths.insert(id, subtable_path);
            }
        }

        Ok(Self {
            main,
            subtables,
            subtable_paths,
            path: Some(path),
        })
    }

    /// Save the MS to its path (must have been created with a path or opened from disk).
    ///
    /// The save step refreshes the main-table metadata and rewrites subtable
    /// keyword payloads using casacore's relative `././SUBTABLE` form.
    pub fn save(&mut self) -> MsResult<()> {
        let path = self
            .path
            .as_ref()
            .ok_or_else(|| MsError::VersionError("MS has no path; use save_as()".to_string()))?
            .clone();

        self.refresh_subtable_paths(&path);
        self.sync_main_metadata(&path);
        self.main.save(TableOptions::new(&path))?;

        for (id, table) in &self.subtables {
            let subtable_path = self
                .subtable_paths
                .get(id)
                .cloned()
                .unwrap_or_else(|| path.join(id.name()));
            table.save(TableOptions::new(&subtable_path))?;
        }

        Ok(())
    }

    /// Save the MS to a new path.
    ///
    /// All persisted subtable references are rebased so the copied MS remains
    /// self-consistent when reopened by Rust or C++ casacore tools.
    pub fn save_as(&mut self, path: impl AsRef<Path>) -> MsResult<()> {
        let path = path.as_ref().to_path_buf();
        self.path = Some(path.clone());
        self.rebase_subtable_paths(&path);
        self.save()
    }

    /// Validate the MS structure and required column metadata.
    ///
    /// This mirrors the casacore `MSTableImpl::validate` checks for required
    /// columns, `QuantumUnits`, and `MEASINFO.type` on the standard MS schema.
    pub fn validate(&self) -> MsResult<Vec<ValidationIssue>> {
        validate::validate_ms(&self.main, &self.subtables)
    }

    /// Number of rows in the main table.
    pub fn row_count(&self) -> usize {
        self.main.row_count()
    }

    /// MS version from the MS_VERSION keyword, or `None` if missing.
    pub fn ms_version(&self) -> Option<f64> {
        match self.main.keywords().get("MS_VERSION") {
            Some(Value::Scalar(ScalarValue::Float32(v))) => Some(*v as f64),
            Some(Value::Scalar(ScalarValue::Float64(v))) => Some(*v),
            _ => None,
        }
    }

    /// Reference to the main table.
    pub fn main_table(&self) -> &Table {
        &self.main
    }

    /// Mutable reference to the main table.
    pub fn main_table_mut(&mut self) -> &mut Table {
        &mut self.main
    }

    /// Reference to a subtable, or `None` if not present.
    pub fn subtable(&self, id: SubtableId) -> Option<&Table> {
        self.subtables.get(&id)
    }

    /// Mutable reference to a subtable, or `None` if not present.
    pub fn subtable_mut(&mut self, id: SubtableId) -> Option<&mut Table> {
        self.subtables.get_mut(&id)
    }

    /// All subtable IDs present in this MS.
    pub fn subtable_ids(&self) -> Vec<SubtableId> {
        self.subtables.keys().copied().collect()
    }

    /// The filesystem path, if any.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    // ---- Typed subtable accessors ----

    /// Typed read-only accessor for the ANTENNA subtable.
    pub fn antenna(&self) -> MsResult<MsAntenna<'_>> {
        let table = self
            .subtable(SubtableId::Antenna)
            .ok_or_else(|| MsError::MissingSubtable("ANTENNA".to_string()))?;
        Ok(MsAntenna::new(table))
    }

    /// Typed mutable accessor for the ANTENNA subtable.
    pub fn antenna_mut(&mut self) -> MsResult<MsAntennaMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Antenna)
            .ok_or_else(|| MsError::MissingSubtable("ANTENNA".to_string()))?;
        Ok(MsAntennaMut::new(table))
    }

    /// Typed read-only accessor for the FIELD subtable.
    pub fn field(&self) -> MsResult<MsField<'_>> {
        let table = self
            .subtable(SubtableId::Field)
            .ok_or_else(|| MsError::MissingSubtable("FIELD".to_string()))?;
        Ok(MsField::new(table))
    }

    /// Typed read-only accessor for the SPECTRAL_WINDOW subtable.
    pub fn spectral_window(&self) -> MsResult<MsSpectralWindow<'_>> {
        let table = self
            .subtable(SubtableId::SpectralWindow)
            .ok_or_else(|| MsError::MissingSubtable("SPECTRAL_WINDOW".to_string()))?;
        Ok(MsSpectralWindow::new(table))
    }

    /// Typed read-only accessor for the POLARIZATION subtable.
    pub fn polarization(&self) -> MsResult<MsPolarization<'_>> {
        let table = self
            .subtable(SubtableId::Polarization)
            .ok_or_else(|| MsError::MissingSubtable("POLARIZATION".to_string()))?;
        Ok(MsPolarization::new(table))
    }

    /// Typed read-only accessor for the DATA_DESCRIPTION subtable.
    pub fn data_description(&self) -> MsResult<MsDataDescription<'_>> {
        let table = self
            .subtable(SubtableId::DataDescription)
            .ok_or_else(|| MsError::MissingSubtable("DATA_DESCRIPTION".to_string()))?;
        Ok(MsDataDescription::new(table))
    }

    /// Typed mutable accessor for the DATA_DESCRIPTION subtable.
    pub fn data_description_mut(&mut self) -> MsResult<MsDataDescriptionMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::DataDescription)
            .ok_or_else(|| MsError::MissingSubtable("DATA_DESCRIPTION".to_string()))?;
        Ok(MsDataDescriptionMut::new(table))
    }

    /// Typed read-only accessor for the FEED subtable.
    pub fn feed(&self) -> MsResult<MsFeed<'_>> {
        let table = self
            .subtable(SubtableId::Feed)
            .ok_or_else(|| MsError::MissingSubtable("FEED".to_string()))?;
        Ok(MsFeed::new(table))
    }

    /// Typed mutable accessor for the FEED subtable.
    pub fn feed_mut(&mut self) -> MsResult<MsFeedMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Feed)
            .ok_or_else(|| MsError::MissingSubtable("FEED".to_string()))?;
        Ok(MsFeedMut::new(table))
    }

    /// Typed read-only accessor for the FLAG_CMD subtable.
    pub fn flag_cmd(&self) -> MsResult<MsFlagCmd<'_>> {
        let table = self
            .subtable(SubtableId::FlagCmd)
            .ok_or_else(|| MsError::MissingSubtable("FLAG_CMD".to_string()))?;
        Ok(MsFlagCmd::new(table))
    }

    /// Typed mutable accessor for the FLAG_CMD subtable.
    pub fn flag_cmd_mut(&mut self) -> MsResult<MsFlagCmdMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::FlagCmd)
            .ok_or_else(|| MsError::MissingSubtable("FLAG_CMD".to_string()))?;
        Ok(MsFlagCmdMut::new(table))
    }

    /// Typed mutable accessor for the FIELD subtable.
    pub fn field_mut(&mut self) -> MsResult<MsFieldMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Field)
            .ok_or_else(|| MsError::MissingSubtable("FIELD".to_string()))?;
        Ok(MsFieldMut::new(table))
    }

    /// Typed read-only accessor for the HISTORY subtable.
    pub fn history(&self) -> MsResult<MsHistory<'_>> {
        let table = self
            .subtable(SubtableId::History)
            .ok_or_else(|| MsError::MissingSubtable("HISTORY".to_string()))?;
        Ok(MsHistory::new(table))
    }

    /// Typed mutable accessor for the HISTORY subtable.
    pub fn history_mut(&mut self) -> MsResult<MsHistoryMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::History)
            .ok_or_else(|| MsError::MissingSubtable("HISTORY".to_string()))?;
        Ok(MsHistoryMut::new(table))
    }

    /// Typed read-only accessor for the OBSERVATION subtable.
    pub fn observation(&self) -> MsResult<MsObservation<'_>> {
        let table = self
            .subtable(SubtableId::Observation)
            .ok_or_else(|| MsError::MissingSubtable("OBSERVATION".to_string()))?;
        Ok(MsObservation::new(table))
    }

    /// Typed mutable accessor for the OBSERVATION subtable.
    pub fn observation_mut(&mut self) -> MsResult<MsObservationMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Observation)
            .ok_or_else(|| MsError::MissingSubtable("OBSERVATION".to_string()))?;
        Ok(MsObservationMut::new(table))
    }

    /// Typed read-only accessor for the POINTING subtable.
    pub fn pointing(&self) -> MsResult<MsPointing<'_>> {
        let table = self
            .subtable(SubtableId::Pointing)
            .ok_or_else(|| MsError::MissingSubtable("POINTING".to_string()))?;
        Ok(MsPointing::new(table))
    }

    /// Typed mutable accessor for the POINTING subtable.
    pub fn pointing_mut(&mut self) -> MsResult<MsPointingMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Pointing)
            .ok_or_else(|| MsError::MissingSubtable("POINTING".to_string()))?;
        Ok(MsPointingMut::new(table))
    }

    /// Typed mutable accessor for the POLARIZATION subtable.
    pub fn polarization_mut(&mut self) -> MsResult<MsPolarizationMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Polarization)
            .ok_or_else(|| MsError::MissingSubtable("POLARIZATION".to_string()))?;
        Ok(MsPolarizationMut::new(table))
    }

    /// Typed read-only accessor for the PROCESSOR subtable.
    pub fn processor(&self) -> MsResult<MsProcessor<'_>> {
        let table = self
            .subtable(SubtableId::Processor)
            .ok_or_else(|| MsError::MissingSubtable("PROCESSOR".to_string()))?;
        Ok(MsProcessor::new(table))
    }

    /// Typed mutable accessor for the PROCESSOR subtable.
    pub fn processor_mut(&mut self) -> MsResult<MsProcessorMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Processor)
            .ok_or_else(|| MsError::MissingSubtable("PROCESSOR".to_string()))?;
        Ok(MsProcessorMut::new(table))
    }

    /// Typed mutable accessor for the SPECTRAL_WINDOW subtable.
    pub fn spectral_window_mut(&mut self) -> MsResult<MsSpectralWindowMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::SpectralWindow)
            .ok_or_else(|| MsError::MissingSubtable("SPECTRAL_WINDOW".to_string()))?;
        Ok(MsSpectralWindowMut::new(table))
    }

    /// Typed read-only accessor for the STATE subtable.
    pub fn state(&self) -> MsResult<MsState<'_>> {
        let table = self
            .subtable(SubtableId::State)
            .ok_or_else(|| MsError::MissingSubtable("STATE".to_string()))?;
        Ok(MsState::new(table))
    }

    /// Typed mutable accessor for the STATE subtable.
    pub fn state_mut(&mut self) -> MsResult<MsStateMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::State)
            .ok_or_else(|| MsError::MissingSubtable("STATE".to_string()))?;
        Ok(MsStateMut::new(table))
    }

    /// Typed read-only accessor for the DOPPLER subtable.
    pub fn doppler(&self) -> MsResult<MsDoppler<'_>> {
        let table = self
            .subtable(SubtableId::Doppler)
            .ok_or_else(|| MsError::MissingSubtable("DOPPLER".to_string()))?;
        Ok(MsDoppler::new(table))
    }

    /// Typed mutable accessor for the DOPPLER subtable.
    pub fn doppler_mut(&mut self) -> MsResult<MsDopplerMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Doppler)
            .ok_or_else(|| MsError::MissingSubtable("DOPPLER".to_string()))?;
        Ok(MsDopplerMut::new(table))
    }

    /// Typed read-only accessor for the FREQ_OFFSET subtable.
    pub fn freq_offset(&self) -> MsResult<MsFreqOffset<'_>> {
        let table = self
            .subtable(SubtableId::FreqOffset)
            .ok_or_else(|| MsError::MissingSubtable("FREQ_OFFSET".to_string()))?;
        Ok(MsFreqOffset::new(table))
    }

    /// Typed mutable accessor for the FREQ_OFFSET subtable.
    pub fn freq_offset_mut(&mut self) -> MsResult<MsFreqOffsetMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::FreqOffset)
            .ok_or_else(|| MsError::MissingSubtable("FREQ_OFFSET".to_string()))?;
        Ok(MsFreqOffsetMut::new(table))
    }

    /// Typed read-only accessor for the SOURCE subtable.
    pub fn source(&self) -> MsResult<MsSource<'_>> {
        let table = self
            .subtable(SubtableId::Source)
            .ok_or_else(|| MsError::MissingSubtable("SOURCE".to_string()))?;
        Ok(MsSource::new(table))
    }

    /// Typed mutable accessor for the SOURCE subtable.
    pub fn source_mut(&mut self) -> MsResult<MsSourceMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Source)
            .ok_or_else(|| MsError::MissingSubtable("SOURCE".to_string()))?;
        Ok(MsSourceMut::new(table))
    }

    /// Typed read-only accessor for the SYSCAL subtable.
    pub fn syscal(&self) -> MsResult<MsSysCal<'_>> {
        let table = self
            .subtable(SubtableId::SysCal)
            .ok_or_else(|| MsError::MissingSubtable("SYSCAL".to_string()))?;
        Ok(MsSysCal::new(table))
    }

    /// Typed mutable accessor for the SYSCAL subtable.
    pub fn syscal_mut(&mut self) -> MsResult<MsSysCalMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::SysCal)
            .ok_or_else(|| MsError::MissingSubtable("SYSCAL".to_string()))?;
        Ok(MsSysCalMut::new(table))
    }

    /// Typed read-only accessor for the WEATHER subtable.
    pub fn weather(&self) -> MsResult<MsWeather<'_>> {
        let table = self
            .subtable(SubtableId::Weather)
            .ok_or_else(|| MsError::MissingSubtable("WEATHER".to_string()))?;
        Ok(MsWeather::new(table))
    }

    /// Typed mutable accessor for the WEATHER subtable.
    pub fn weather_mut(&mut self) -> MsResult<MsWeatherMut<'_>> {
        let table = self
            .subtable_mut(SubtableId::Weather)
            .ok_or_else(|| MsError::MissingSubtable("WEATHER".to_string()))?;
        Ok(MsWeatherMut::new(table))
    }

    // ---- Main-table column accessors ----

    /// Typed read-only accessor for a visibility DATA column (DATA, CORRECTED_DATA, or MODEL_DATA).
    ///
    /// Returns `MsError::ColumnNotPresent` if the column is absent.
    pub fn data_column(
        &self,
        name: &'static str,
    ) -> MsResult<crate::columns::data_columns::DataColumn<'_>> {
        match name {
            "DATA" => crate::columns::data_columns::DataColumn::data(&self.main),
            "CORRECTED_DATA" => {
                crate::columns::data_columns::DataColumn::corrected_data(&self.main)
            }
            "MODEL_DATA" => crate::columns::data_columns::DataColumn::model_data(&self.main),
            _ => Err(MsError::ColumnNotPresent(name.to_string())),
        }
    }

    /// Typed mutable accessor for a visibility DATA column (DATA, CORRECTED_DATA, or MODEL_DATA).
    ///
    /// Returns `MsError::ColumnNotPresent` if the column is absent.
    pub fn data_column_mut(
        &mut self,
        name: &'static str,
    ) -> MsResult<crate::columns::data_columns::DataColumnMut<'_>> {
        match name {
            "DATA" => crate::columns::data_columns::DataColumnMut::data(&mut self.main),
            "CORRECTED_DATA" => {
                crate::columns::data_columns::DataColumnMut::corrected_data(&mut self.main)
            }
            "MODEL_DATA" => crate::columns::data_columns::DataColumnMut::model_data(&mut self.main),
            _ => Err(MsError::ColumnNotPresent(name.to_string())),
        }
    }

    /// Typed accessor for the FLAG column.
    pub fn flag_column(&self) -> crate::columns::flag_columns::FlagColumn<'_> {
        crate::columns::flag_columns::FlagColumn::new(&self.main)
    }

    /// Typed accessor for the FLAG_ROW column.
    pub fn flag_row_column(&self) -> crate::columns::flag_columns::FlagRowColumn<'_> {
        crate::columns::flag_columns::FlagRowColumn::new(&self.main)
    }

    /// Typed accessor for the WEIGHT column.
    pub fn weight_column(&self) -> crate::columns::weight_columns::WeightColumn<'_> {
        crate::columns::weight_columns::WeightColumn::new(&self.main)
    }

    /// Typed accessor for the SIGMA column.
    pub fn sigma_column(&self) -> crate::columns::weight_columns::SigmaColumn<'_> {
        crate::columns::weight_columns::SigmaColumn::new(&self.main)
    }

    // ---- Selection and iteration convenience methods ----

    /// Apply a selection to this MS, returning matching row indices.
    ///
    /// Convenience wrapper around [`MsSelection::apply`](crate::selection::MsSelection::apply).
    pub fn select(&mut self, sel: &crate::selection::MsSelection) -> MsResult<Vec<usize>> {
        sel.apply(self)
    }

    /// Iterate over the main table with canonical sort order
    /// (ARRAY_ID, FIELD_ID, DATA_DESC_ID, TIME).
    ///
    /// Convenience wrapper around [`grouping::iter_ms`](crate::grouping::iter_ms).
    pub fn iter(&self) -> MsResult<crate::grouping::MsIterator<'_>> {
        crate::grouping::iter_ms(self)
    }

    /// Iterate over the main table grouped by custom columns.
    ///
    /// Convenience wrapper around [`grouping::iter_ms_by`](crate::grouping::iter_ms_by).
    pub fn iter_by<'a>(&'a self, columns: &[&str]) -> MsResult<crate::grouping::MsIterator<'a>> {
        crate::grouping::iter_ms_by(self, columns)
    }

    // ---- Internal helpers ----

    fn from_schemas(schemas: MsSchemas, path: Option<PathBuf>) -> MsResult<Self> {
        let mut main = Table::with_schema(schemas.main);
        main.set_info(TableInfo {
            table_type: CASACORE_MS_TABLE_TYPE.to_string(),
            sub_type: String::new(),
        });
        apply_column_metadata(&mut main, crate::schema::main_table::REQUIRED_COLUMNS);
        apply_column_metadata(&mut main, crate::schema::main_table::OPTIONAL_COLUMNS);
        ensure_main_column_keywords(&mut main);

        // Set MS_VERSION keyword
        main.keywords_mut().push(RecordField::new(
            "MS_VERSION",
            Value::Scalar(ScalarValue::Float32(MS_VERSION)),
        ));

        // Set subtable reference keywords (as string paths)
        let mut subtables = HashMap::new();
        let mut subtable_paths = HashMap::new();
        for (id, schema) in schemas.subtables {
            let subtable_ref = id.name().to_string();
            main.keywords_mut()
                .push(RecordField::new(id.name(), Value::table_ref(subtable_ref)));

            let table = Table::with_schema(schema);
            let mut table = table;
            apply_column_metadata(&mut table, crate::schema::required_columns(id));
            apply_column_metadata(&mut table, crate::schema::optional_columns(id));
            if let Some(ref p) = path {
                subtable_paths.insert(id, p.join(id.name()));
            }
            subtables.insert(id, table);
        }

        Ok(Self {
            main,
            subtables,
            subtable_paths,
            path,
        })
    }

    fn refresh_subtable_paths(&mut self, base_path: &Path) {
        for id in self.subtables.keys().copied() {
            self.subtable_paths
                .entry(id)
                .or_insert_with(|| base_path.join(id.name()));
        }
    }

    fn rebase_subtable_paths(&mut self, base_path: &Path) {
        self.subtable_paths = self
            .subtables
            .keys()
            .copied()
            .map(|id| (id, base_path.join(id.name())))
            .collect();
    }

    fn sync_main_metadata(&mut self, base_path: &Path) {
        self.main.set_info(TableInfo {
            table_type: CASACORE_MS_TABLE_TYPE.to_string(),
            sub_type: String::new(),
        });
        apply_column_metadata(&mut self.main, crate::schema::main_table::REQUIRED_COLUMNS);
        apply_column_metadata(&mut self.main, crate::schema::main_table::OPTIONAL_COLUMNS);
        ensure_main_column_keywords(&mut self.main);
        self.main.keywords_mut().upsert(
            "MS_VERSION",
            Value::Scalar(ScalarValue::Float32(MS_VERSION)),
        );

        for id in SubtableId::ALL_REQUIRED
            .iter()
            .chain(SubtableId::ALL_OPTIONAL.iter())
        {
            if let Some(subtable_path) = self.subtable_paths.get(id) {
                let keyword_path = subtable_keyword_value(base_path, subtable_path);
                self.main
                    .keywords_mut()
                    .upsert(id.name(), Value::table_ref(keyword_path));
            } else {
                self.main.keywords_mut().remove(id.name());
            }
        }
    }
}

fn apply_column_metadata(table: &mut Table, defs: &[ColumnDef]) {
    for def in defs {
        let has_column = table
            .schema()
            .and_then(|schema| schema.column(def.name))
            .is_some();
        if !has_column {
            continue;
        }

        let mut keywords = table.column_keywords(def.name).cloned().unwrap_or_default();
        if let Some(units) = quantum_units_for(def) {
            keywords.upsert(
                "QuantumUnits",
                Value::Array(ArrayValue::from_string_vec(units)),
            );
        }
        if let Some(measinfo) = measinfo_for(def) {
            keywords.upsert("MEASINFO", Value::Record(measinfo));
        }
        table.set_column_keywords(def.name, keywords);
    }
}

fn ensure_main_column_keywords(main: &mut Table) {
    main.set_column_keywords("FLAG_CATEGORY", flag_category_keywords());
}

fn flag_category_keywords() -> RecordValue {
    RecordValue::new(vec![RecordField::new(
        "CATEGORY",
        Value::Array(ArrayValue::String(
            ndarray::ArrayD::from_shape_vec(vec![0], Vec::<String>::new()).unwrap(),
        )),
    )])
}

fn subtable_path_from_main(main: &Table, base_path: &Path, id: SubtableId) -> Option<PathBuf> {
    if let Some(value) = main.keywords().get(id.name()) {
        let keyword_path = match value {
            Value::TableRef(path) => Some(path.as_str()),
            // Accept older Rust-written MS trees that stored string keywords.
            Value::Scalar(ScalarValue::String(path)) => Some(path.as_str()),
            _ => None,
        };
        if let Some(keyword_path) = keyword_path {
            let candidate = resolve_subtable_path(base_path, keyword_path);
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }

    let fallback = base_path.join(id.name());
    fallback.exists().then_some(fallback)
}

fn resolve_subtable_path(base_path: &Path, keyword_path: &str) -> PathBuf {
    let mut trimmed = keyword_path;
    let mut removed_prefixes = 0_usize;
    while let Some(rest) = trimmed.strip_prefix("./") {
        trimmed = rest;
        removed_prefixes += 1;
    }

    if removed_prefixes > 0 {
        let anchor = if removed_prefixes == 1 {
            base_path.parent().unwrap_or(base_path)
        } else {
            base_path
        };
        return anchor.join(trimmed);
    }

    let candidate = PathBuf::from(trimmed);
    if candidate.is_absolute() {
        candidate
    } else {
        base_path.join(candidate)
    }
}

fn subtable_keyword_value(base_path: &Path, subtable_path: &Path) -> String {
    if let Ok(relative) = subtable_path.strip_prefix(base_path) {
        let rel = relative.to_string_lossy();
        return format!("././{rel}");
    }
    if let Some(parent) = base_path.parent()
        && let Ok(relative) = subtable_path.strip_prefix(parent)
    {
        let rel = relative.to_string_lossy();
        return format!("./{rel}");
    }
    if subtable_path.is_relative() {
        let rel = subtable_path.to_string_lossy();
        return format!("././{}", rel.trim_start_matches("./"));
    }
    subtable_path.to_string_lossy().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::MeasurementSetBuilder;
    use casacore_tables::TableOptions;
    use std::fs;

    #[test]
    fn create_memory_ms() {
        let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        assert_eq!(ms.row_count(), 0);
        assert_eq!(ms.subtable_ids().len(), 12);
        assert!(ms.ms_version().is_some());
        assert!((ms.ms_version().unwrap() - 2.0).abs() < 0.01);
    }

    #[test]
    fn validate_empty_ms() {
        let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        let issues = ms.validate().unwrap();
        assert!(issues.is_empty(), "unexpected issues: {issues:?}");
    }

    #[test]
    fn validate_detects_missing_column_metadata() {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        ms.main_table_mut()
            .set_column_keywords("TIME", RecordValue::default());

        let issues = ms.validate().unwrap();
        assert!(issues.iter().any(|issue| matches!(
            issue,
            ValidationIssue::MissingQuantumUnits {
                table_name,
                column_name
            } if table_name == "MAIN" && column_name == "TIME"
        )));
        assert!(issues.iter().any(|issue| matches!(
            issue,
            ValidationIssue::MissingMeasureInfo {
                table_name,
                column_name
            } if table_name == "MAIN" && column_name == "TIME"
        )));
    }

    #[test]
    fn validate_missing_subtable() {
        let schemas = MeasurementSetBuilder::new().build_schemas().unwrap();
        let main = Table::with_schema(schemas.main);
        let subtables = HashMap::new(); // empty - no subtables
        let issues = validate::validate_subtable_keywords(&main, &subtables);
        assert_eq!(
            issues.len(),
            13 // 12 missing subtables + 1 missing MS_VERSION
        );
    }

    #[test]
    fn typed_subtable_accessors() {
        let ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        assert_eq!(ms.antenna().unwrap().row_count(), 0);
        assert_eq!(ms.data_description().unwrap().row_count(), 0);
        assert_eq!(ms.feed().unwrap().row_count(), 0);
        assert_eq!(ms.field().unwrap().row_count(), 0);
        assert_eq!(ms.flag_cmd().unwrap().row_count(), 0);
        assert_eq!(ms.history().unwrap().row_count(), 0);
        assert_eq!(ms.observation().unwrap().row_count(), 0);
        assert_eq!(ms.pointing().unwrap().row_count(), 0);
        assert_eq!(ms.spectral_window().unwrap().row_count(), 0);
        assert_eq!(ms.polarization().unwrap().row_count(), 0);
        assert_eq!(ms.processor().unwrap().row_count(), 0);
        assert_eq!(ms.state().unwrap().row_count(), 0);
    }

    #[test]
    fn optional_subtable_accessors() {
        let mut builder = MeasurementSetBuilder::new();
        for id in SubtableId::ALL_OPTIONAL {
            builder = builder.with_optional_subtable(*id);
        }
        let ms = MeasurementSet::create_memory(builder).unwrap();
        assert_eq!(ms.doppler().unwrap().row_count(), 0);
        assert_eq!(ms.freq_offset().unwrap().row_count(), 0);
        assert_eq!(ms.source().unwrap().row_count(), 0);
        assert_eq!(ms.syscal().unwrap().row_count(), 0);
        assert_eq!(ms.weather().unwrap().row_count(), 0);
    }

    #[test]
    fn add_antenna_via_ms() {
        let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).unwrap();
        {
            let mut ant = ms.antenna_mut().unwrap();
            ant.add_antenna(
                "VLA01",
                "N01",
                "GROUND-BASED",
                "ALT-AZ",
                [1.0; 3],
                [0.0; 3],
                25.0,
            )
            .unwrap();
        }
        assert_eq!(ms.antenna().unwrap().row_count(), 1);
        assert_eq!(ms.antenna().unwrap().name(0).unwrap(), "VLA01");
    }

    #[test]
    fn create_and_reopen_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let ms_path = dir.path().join("test.ms");

        // Create
        {
            let mut ms = MeasurementSet::create(&ms_path, MeasurementSetBuilder::new()).unwrap();
            {
                let mut ant = ms.antenna_mut().unwrap();
                ant.add_antenna(
                    "ALMA01",
                    "A001",
                    "GROUND-BASED",
                    "ALT-AZ",
                    [2.0; 3],
                    [0.0; 3],
                    12.0,
                )
                .unwrap();
            }
            ms.save().unwrap();
        }

        // Reopen
        {
            let ms = MeasurementSet::open(&ms_path).unwrap();
            assert_eq!(ms.antenna().unwrap().row_count(), 1);
            assert_eq!(ms.antenna().unwrap().name(0).unwrap(), "ALMA01");
        }
    }

    #[test]
    fn open_uses_subtable_keyword_paths() {
        let dir = tempfile::tempdir().unwrap();
        let ms_path = dir.path().join("keyword_paths.ms");

        {
            let mut ms = MeasurementSet::create(&ms_path, MeasurementSetBuilder::new()).unwrap();
            ms.antenna_mut()
                .unwrap()
                .add_antenna(
                    "ALMA01",
                    "A001",
                    "GROUND-BASED",
                    "ALT-AZ",
                    [1.0; 3],
                    [0.0; 3],
                    12.0,
                )
                .unwrap();
            ms.save().unwrap();
        }

        let relocated = ms_path.join("ANTENNA_RELOCATED");
        fs::rename(ms_path.join("ANTENNA"), &relocated).unwrap();

        {
            let mut main = Table::open(TableOptions::new(&ms_path)).unwrap();
            main.keywords_mut()
                .upsert("ANTENNA", Value::table_ref("././ANTENNA_RELOCATED"));
            main.save(TableOptions::new(&ms_path)).unwrap();
        }

        let reopened = MeasurementSet::open(&ms_path).unwrap();
        assert_eq!(reopened.antenna().unwrap().row_count(), 1);
        assert_eq!(reopened.antenna().unwrap().name(0).unwrap(), "ALMA01");
    }

    #[test]
    fn open_uses_legacy_string_keyword_paths() {
        let dir = tempfile::tempdir().unwrap();
        let ms_path = dir.path().join("legacy_keyword_paths.ms");

        {
            let mut ms = MeasurementSet::create(&ms_path, MeasurementSetBuilder::new()).unwrap();
            ms.antenna_mut()
                .unwrap()
                .add_antenna(
                    "ALMA01",
                    "A001",
                    "GROUND-BASED",
                    "ALT-AZ",
                    [1.0; 3],
                    [0.0; 3],
                    12.0,
                )
                .unwrap();
            ms.save().unwrap();
        }

        let relocated = ms_path.join("ANTENNA_RELOCATED");
        fs::rename(ms_path.join("ANTENNA"), &relocated).unwrap();

        {
            let mut main = Table::open(TableOptions::new(&ms_path)).unwrap();
            main.keywords_mut().upsert(
                "ANTENNA",
                Value::Scalar(ScalarValue::String("ANTENNA_RELOCATED".to_string())),
            );
            main.save(TableOptions::new(&ms_path)).unwrap();
        }

        let reopened = MeasurementSet::open(&ms_path).unwrap();
        assert_eq!(reopened.antenna().unwrap().row_count(), 1);
        assert_eq!(reopened.antenna().unwrap().name(0).unwrap(), "ALMA01");
    }

    #[test]
    fn save_as_rewrites_subtable_keywords_relative_to_new_path() {
        let dir = tempfile::tempdir().unwrap();
        let first_path = dir.path().join("first.ms");
        let second_path = dir.path().join("second.ms");

        let mut ms = MeasurementSet::create(&first_path, MeasurementSetBuilder::new()).unwrap();
        ms.save_as(&second_path).unwrap();

        let reopened = Table::open(TableOptions::new(&second_path)).unwrap();
        assert_eq!(
            reopened.keywords().get("ANTENNA"),
            Some(&Value::TableRef("././ANTENNA".to_string()))
        );
        assert_eq!(reopened.info().table_type, "Measurement Set");
    }

    #[test]
    fn validation_fails_on_wrong_column_type() {
        use casacore_tables::{ColumnSchema, TableSchema};
        use casacore_types::PrimitiveType;

        // Create a table with TIME as Int32 instead of Float64
        let bad_schema = TableSchema::new(vec![
            ColumnSchema::scalar("TIME", PrimitiveType::Int32), // wrong type!
        ])
        .unwrap();
        let table = Table::with_schema(bad_schema);

        let issues = validate::validate_columns(
            &table,
            "MAIN",
            &[crate::column_def::ColumnDef {
                name: "TIME",
                data_type: PrimitiveType::Float64,
                column_kind: crate::column_def::ColumnKind::Scalar,
                unit: "s",
                measure_type: None,
                measure_ref: "",
                comment: "",
            }],
        );
        assert_eq!(issues.len(), 1);
        assert!(matches!(issues[0], ValidationIssue::WrongColumnType { .. }));
    }
}
