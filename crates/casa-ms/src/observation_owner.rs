// SPDX-License-Identifier: LGPL-3.0-or-later

//! Storage-owner identity and generation authority for native imaging.
//!
//! The owner manifest is one versioned string keyword in the MeasurementSet
//! MAIN table. It does not alter any standard row, column, subtable, or image
//! semantic, and C++ casacore preserves the unknown keyword. Logical identity
//! is owner-minted from operating-system entropy; filesystem paths, inode and
//! timestamp metadata, and content hashes never participate.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};

use casa_imaging_model::{
    ColumnGeneration, ConsistencyToken, CorrectedDataColumnState, FlagPolicy, LogicalIdentity,
    MeasurementSetIdentity, MetadataGeneration, MetadataTableKind, ModelColumnState,
    ModelStateIdentity, MsColumnKind, ObservationSelection, ObservationSnapshotInput,
    ObservationSourceInput, ObservationSourceProvenance, ObservationSourceState, ReferenceDataKind,
    SelectedColumns, SelectedMainRow, SelectedRowsBuilder, SourceGenerations,
    SpectralWindowCoordinateCatalog, SpectralWindowSelection, VisibilityColumn, WeightColumn,
};
use casa_tables::{ColumnSchema, LockType, Table};
use casa_types::{
    ArrayValue, Complex32, PrimitiveType, RecordValue, ScalarValue, Value,
    measures::MeasuresProvider,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::selected_observation::validate_selected_coordinates;
use crate::subtables::SubTable;
use crate::{
    BoundObservationSourceError, BoundSelectedObservation, BoundSelectedObservationError,
    MeasurementSet, MsError, MsSelectionIoBudget, ObservationSourceBinding,
    SelectedObservationContentBudget, SelectedObservationEphemeris, SelectedObservationMeasures,
    SelectedObservationMeasuresError, SelectedObservationResidencyCertificate,
    SelectedObservationRow, SelectedObservationRowSelection, SubtableId,
};

const OWNER_MANIFEST_KEYWORD: &str = "CASA_RS_IMAGING_OWNER_MANIFEST";
const OWNER_MANIFEST_SCHEMA_VERSION: u32 = 1;
const OWNER_IDENTITY_DOMAIN: &[u8] = b"casa-rs-ms-owner-manifest-v1";
const VISIBILITY_WRITE_BATCH_ROWS: u64 = 10_000;
static OWNER_INITIALIZATION_MUTEX: Mutex<()> = Mutex::new(());

const TRACKED_COLUMNS: &[(MsColumnKind, &str)] = &[
    (MsColumnKind::Data, "DATA"),
    (MsColumnKind::CorrectedData, "CORRECTED_DATA"),
    (MsColumnKind::FloatData, "FLOAT_DATA"),
    (MsColumnKind::Flag, "FLAG"),
    (MsColumnKind::FlagRow, "FLAG_ROW"),
    (MsColumnKind::Weight, "WEIGHT"),
    (MsColumnKind::WeightSpectrum, "WEIGHT_SPECTRUM"),
    (MsColumnKind::Uvw, "UVW"),
    (MsColumnKind::Time, "TIME"),
    (MsColumnKind::TimeCentroid, "TIME_CENTROID"),
    (MsColumnKind::Interval, "INTERVAL"),
    (MsColumnKind::Exposure, "EXPOSURE"),
    (MsColumnKind::FieldId, "FIELD_ID"),
    (MsColumnKind::DataDescriptionId, "DATA_DESC_ID"),
    (MsColumnKind::Antenna1, "ANTENNA1"),
    (MsColumnKind::Antenna2, "ANTENNA2"),
    (MsColumnKind::Feed1, "FEED1"),
    (MsColumnKind::Feed2, "FEED2"),
    (MsColumnKind::ScanNumber, "SCAN_NUMBER"),
    (MsColumnKind::StateId, "STATE_ID"),
    (MsColumnKind::ObservationId, "OBSERVATION_ID"),
    (MsColumnKind::ArrayId, "ARRAY_ID"),
    (MsColumnKind::ModelData, "MODEL_DATA"),
];

const REQUIRED_COORDINATE_COLUMNS: [MsColumnKind; 15] = [
    MsColumnKind::Uvw,
    MsColumnKind::Time,
    MsColumnKind::TimeCentroid,
    MsColumnKind::Interval,
    MsColumnKind::Exposure,
    MsColumnKind::FieldId,
    MsColumnKind::DataDescriptionId,
    MsColumnKind::Antenna1,
    MsColumnKind::Antenna2,
    MsColumnKind::Feed1,
    MsColumnKind::Feed2,
    MsColumnKind::ScanNumber,
    MsColumnKind::StateId,
    MsColumnKind::ObservationId,
    MsColumnKind::ArrayId,
];

const TRACKED_METADATA: &[(MetadataTableKind, SubtableId)] = &[
    (MetadataTableKind::Antenna, SubtableId::Antenna),
    (
        MetadataTableKind::DataDescription,
        SubtableId::DataDescription,
    ),
    (MetadataTableKind::Doppler, SubtableId::Doppler),
    (MetadataTableKind::Feed, SubtableId::Feed),
    (MetadataTableKind::Field, SubtableId::Field),
    (MetadataTableKind::FrequencyOffset, SubtableId::FreqOffset),
    (MetadataTableKind::Observation, SubtableId::Observation),
    (MetadataTableKind::Pointing, SubtableId::Pointing),
    (MetadataTableKind::Polarization, SubtableId::Polarization),
    (MetadataTableKind::Source, SubtableId::Source),
    (
        MetadataTableKind::SpectralWindow,
        SubtableId::SpectralWindow,
    ),
    (MetadataTableKind::State, SubtableId::State),
    (MetadataTableKind::SysCal, SubtableId::SysCal),
    (MetadataTableKind::Weather, SubtableId::Weather),
];

const REQUIRED_METADATA: [MetadataTableKind; 9] = [
    MetadataTableKind::Antenna,
    MetadataTableKind::DataDescription,
    MetadataTableKind::Feed,
    MetadataTableKind::Field,
    MetadataTableKind::Observation,
    MetadataTableKind::Pointing,
    MetadataTableKind::Polarization,
    MetadataTableKind::SpectralWindow,
    MetadataTableKind::State,
];

/// Explicit production inputs for resolving one selected MeasurementSet owner.
///
/// Cloning duplicates only this immutable resolution description. Every call to
/// [`resolve_selected_observation`] performs a fresh owner-state probe and
/// returns a new affine access capability; live table authority is never cloned.
#[derive(Clone)]
pub struct SelectedObservationResolutionRequest {
    locator: String,
    selection_request: LogicalIdentity,
    selection: Arc<ObservationSelection>,
    visibility: VisibilityColumn,
    weights: WeightColumn,
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
    model: ModelStateIdentity,
    content_budget: SelectedObservationContentBudget,
    measures_provider: Arc<dyn MeasuresProvider>,
    ephemeris: Option<SelectedObservationEphemeris>,
}

impl SelectedObservationResolutionRequest {
    /// Construct one single-source production observation resolution.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        locator: impl Into<String>,
        selection_request: LogicalIdentity,
        selection: ObservationSelection,
        visibility: VisibilityColumn,
        weights: WeightColumn,
        reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
        model: ModelStateIdentity,
        content_budget: SelectedObservationContentBudget,
        measures_provider: Arc<dyn MeasuresProvider>,
    ) -> Self {
        Self {
            locator: locator.into(),
            selection_request,
            selection: Arc::new(selection),
            visibility,
            weights,
            reference_data,
            model,
            content_budget,
            measures_provider,
            ephemeris: None,
        }
    }

    /// Bind immutable moving-source reference data to this owner resolution.
    #[must_use]
    pub fn with_ephemeris(mut self, ephemeris: Option<SelectedObservationEphemeris>) -> Self {
        self.ephemeris = ephemeris;
        self
    }

    /// Return the storage-owner locator used for each fresh resolution.
    #[must_use]
    pub fn locator(&self) -> &str {
        &self.locator
    }

    /// Return shared exact row, channel, and correlation selection authority.
    #[must_use]
    pub fn selection(&self) -> Arc<ObservationSelection> {
        Arc::clone(&self.selection)
    }
}

/// Selected visibility columns written by one bounded owner transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedVisibilityWriteTargets {
    model_data: bool,
    corrected_data: bool,
}

impl SelectedVisibilityWriteTargets {
    /// Construct the exact destination set. At least one destination is required.
    #[must_use]
    pub const fn new(model_data: bool, corrected_data: bool) -> Self {
        Self {
            model_data,
            corrected_data,
        }
    }

    /// Return whether `MODEL_DATA` is written.
    #[must_use]
    pub const fn model_data(self) -> bool {
        self.model_data
    }

    /// Return whether existing `CORRECTED_DATA` is written.
    #[must_use]
    pub const fn corrected_data(self) -> bool {
        self.corrected_data
    }
}

/// Owner generations published by one selected visibility write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedVisibilityWriteGenerations {
    /// Final prediction generation, when `MODEL_DATA` was selected.
    pub model_data: Option<LogicalIdentity>,
    /// Transformed-observation generation, when `CORRECTED_DATA` was selected.
    pub corrected_data: Option<LogicalIdentity>,
}

/// Bounded in-place selected-visibility writer following ordinary casacore semantics.
///
/// The writer retains the MAIN write lock and the repository's standard
/// incomplete-write marker. Successful completion flushes every selected
/// destination, updates its owner generation, and removes the marker. Failure may leave partially
/// written derived values, but the retained marker makes that state fail closed
/// until explicitly recovered or recomputed. No backup, staging column,
/// rollback, snapshot, or content digest is created.
#[cfg(unix)]
pub struct SelectedVisibilityWrite {
    measurement_set: Option<MeasurementSet>,
    manifest: OwnerManifest,
    incomplete_marker: Option<std::path::PathBuf>,
    targets: SelectedVisibilityWriteTargets,
    pending_cell: Option<PendingVisibilityCells>,
    pending_rows: Vec<usize>,
    completed: bool,
}

struct PendingVisibilityCells {
    row: usize,
    model_data: Option<ndarray::ArrayD<Complex32>>,
    corrected_data: Option<ndarray::ArrayD<Complex32>>,
}

/// Owner-derived storage plan for one bounded selected-visibility write.
///
/// The plan is derived from MAIN row/DDID coordinates and the standard
/// DATA_DESCRIPTION, SPECTRAL_WINDOW, and POLARIZATION metadata. It never reads
/// a visibility payload merely to discover a cell shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedVisibilityStoragePlan {
    additional_persistent_bytes: u64,
    write_bytes: u64,
    maximum_cell_bytes: u64,
    write_buffer_bytes: u64,
}

impl SelectedVisibilityStoragePlan {
    /// New persistent capacity required for this write.
    ///
    /// This includes complete logical `MODEL_DATA` capacity when creation and
    /// zero-initialization are required. Existing destinations add no capacity.
    #[must_use]
    pub const fn additional_persistent_bytes(self) -> u64 {
        self.additional_persistent_bytes
    }

    /// Bytes written by the initial column creation and selected-cell update.
    #[must_use]
    pub const fn write_bytes(self) -> u64 {
        self.write_bytes
    }

    /// Largest single-row cell copied or updated by the bounded writer.
    #[must_use]
    pub const fn maximum_cell_bytes(self) -> u64 {
        self.maximum_cell_bytes
    }

    /// Maximum payload bytes retained by the bounded row-batch writer.
    #[must_use]
    pub const fn write_buffer_bytes(self) -> u64 {
        self.write_buffer_bytes
    }
}

#[cfg(unix)]
impl SelectedVisibilityWrite {
    /// Acquire owner write authority and start a detectable in-place mutation.
    pub fn begin(
        path: impl AsRef<Path>,
        expected: &ObservationSourceState,
        selection: &ObservationSelection,
        targets: SelectedVisibilityWriteTargets,
    ) -> Result<Self, ObservationOwnerError> {
        if !targets.model_data && !targets.corrected_data {
            return Err(ObservationOwnerError::EmptyWriteTargets);
        }
        let path = path.as_ref();
        let mut measurement_set = MeasurementSet::open_retained_read(path)?;
        if !measurement_set.main_table_mut().lock(LockType::Write, 1)? {
            return Err(ObservationOwnerError::WriteLockUnavailable);
        }
        let manifest = OwnerManifest::read(measurement_set.main_table().keywords())?;
        manifest.validate_physical_state(&measurement_set)?;
        validate_transaction_precondition(&manifest, &measurement_set, expected)?;
        if selection.rows() != expected.selected_rows() {
            return Err(ObservationOwnerError::TransactionPrecondition);
        }
        let has_model = {
            let schema = measurement_set.main_table().schema().ok_or_else(|| {
                MsError::InvalidInput("MeasurementSet MAIN has no schema".to_string())
            })?;
            schema.contains_column("MODEL_DATA")
        };
        let has_corrected = measurement_set
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("CORRECTED_DATA"));
        if targets.corrected_data
            && (!has_corrected
                || expected.generations().corrected_data_column()
                    == CorrectedDataColumnState::Absent)
        {
            return Err(ObservationOwnerError::MissingCorrectedDataDestination);
        }
        let incomplete_marker = crate::write_session::begin_in_place_write(path)?;
        if targets.model_data && !has_model {
            measurement_set.main_table_mut().add_column(
                ColumnSchema::array_variable("MODEL_DATA", PrimitiveType::Complex32, Some(2)),
                None,
            )?;
            measurement_set
                .main_table_mut()
                .prepare_write()
                .add_tiled_column_clone("DATA", "MODEL_DATA", "TiledModelData")?;
            let row_count = measurement_set.main_table().row_count();
            let mut pending_rows = Vec::with_capacity(VISIBILITY_WRITE_BATCH_ROWS as usize);
            for row in 0..row_count {
                let data_description_id = main_data_description_id(&measurement_set, row)?;
                let shape = model_cell_shape(&measurement_set, data_description_id)?;
                queue_visibility_cell(
                    measurement_set.main_table_mut(),
                    "MODEL_DATA",
                    row,
                    ArrayValue::Complex32(ndarray::ArrayD::from_elem(
                        ndarray::IxDyn(&shape),
                        Complex32::new(0.0, 0.0),
                    )),
                )?;
                pending_rows.push(row);
                if pending_rows.len() == VISIBILITY_WRITE_BATCH_ROWS as usize {
                    persist_visibility_rows(
                        measurement_set.main_table_mut(),
                        &["MODEL_DATA"],
                        &pending_rows,
                    )?;
                    pending_rows.clear();
                }
            }
            if !pending_rows.is_empty() {
                persist_visibility_rows(
                    measurement_set.main_table_mut(),
                    &["MODEL_DATA"],
                    &pending_rows,
                )?;
            }
        }
        Ok(Self {
            measurement_set: Some(measurement_set),
            manifest,
            incomplete_marker,
            targets,
            pending_cell: None,
            pending_rows: Vec::with_capacity(VISIBILITY_WRITE_BATCH_ROWS as usize),
            completed: false,
        })
    }

    /// Write one selected prediction at its physical row/channel/correlation.
    pub fn write(
        &mut self,
        column: MsColumnKind,
        row: u64,
        channel: u32,
        correlation: u32,
        value: Complex32,
    ) -> Result<(), ObservationOwnerError> {
        let row = usize::try_from(row).map_err(|_| ObservationOwnerError::PredictionAddress)?;
        if self.pending_cell.as_ref().map(|cell| cell.row) != Some(row) {
            self.flush_pending_cell()?;
            let measurement_set = self
                .measurement_set
                .as_ref()
                .ok_or(ObservationOwnerError::TransactionClosed)?;
            let load = |name: &str| -> Result<ndarray::ArrayD<Complex32>, ObservationOwnerError> {
                let current = measurement_set
                    .main_table()
                    .column_accessor(name)?
                    .get(row)?
                    .cloned()
                    .ok_or(ObservationOwnerError::PredictionAddress)?;
                let Value::Array(ArrayValue::Complex32(values)) = current else {
                    return Err(ObservationOwnerError::PredictionAddress);
                };
                Ok(values)
            };
            self.pending_cell = Some(PendingVisibilityCells {
                row,
                model_data: self
                    .targets
                    .model_data
                    .then(|| load("MODEL_DATA"))
                    .transpose()?,
                corrected_data: self
                    .targets
                    .corrected_data
                    .then(|| load("CORRECTED_DATA"))
                    .transpose()?,
            });
        }
        let pending = self
            .pending_cell
            .as_mut()
            .ok_or(ObservationOwnerError::TransactionClosed)?;
        let values = match column {
            MsColumnKind::ModelData if self.targets.model_data => pending.model_data.as_mut(),
            MsColumnKind::CorrectedData if self.targets.corrected_data => {
                pending.corrected_data.as_mut()
            }
            _ => None,
        }
        .ok_or(ObservationOwnerError::UnselectedWriteTarget)?;
        let index = [correlation as usize, channel as usize];
        let Some(cell) = values.get_mut(index) else {
            return Err(ObservationOwnerError::PredictionAddress);
        };
        *cell = value;
        Ok(())
    }

    /// Flush the in-place write, publish its generation, and clear the marker.
    pub fn complete(
        mut self,
        generations: SelectedVisibilityWriteGenerations,
    ) -> Result<(), ObservationOwnerError> {
        if self.targets.model_data != generations.model_data.is_some()
            || self.targets.corrected_data != generations.corrected_data.is_some()
        {
            return Err(ObservationOwnerError::WriteGenerationMismatch);
        }
        self.flush_pending_cell()?;
        self.persist_pending_rows()?;
        let measurement_set = self
            .measurement_set
            .as_mut()
            .ok_or(ObservationOwnerError::TransactionClosed)?;
        if let Some(generation) = generations.model_data {
            self.manifest.model_data = PersistedModelColumn::Present {
                generation: encode_identity(generation),
            };
        }
        if let Some(generation) = generations.corrected_data {
            self.manifest
                .columns
                .insert("CORRECTED_DATA".to_string(), encode_identity(generation));
        }
        let mut hasher = Sha256::new();
        hasher.update(b"casa-rs-selected-visibility-consistency-v1");
        hasher.update(
            parse_identity(&self.manifest.consistency_token, "consistency token")?.as_bytes(),
        );
        if let Some(generation) = generations.model_data {
            hasher.update(b"MODEL_DATA");
            hasher.update(generation.as_bytes());
        }
        if let Some(generation) = generations.corrected_data {
            hasher.update(b"CORRECTED_DATA");
            hasher.update(generation.as_bytes());
        }
        self.manifest.consistency_token =
            encode_identity(LogicalIdentity::from_sha256(hasher.finalize().into()));
        self.manifest.main_modify_counter = measurement_set
            .main_table()
            .locked_modify_counter()?
            .wrapping_add(1);
        write_owner_manifest(measurement_set.main_table_mut(), &self.manifest)?;
        measurement_set.main_table_mut().unlock_metadata_only()?;
        self.measurement_set = None;
        crate::write_session::complete_in_place_write(self.incomplete_marker.take())?;
        self.completed = true;
        Ok(())
    }

    fn flush_pending_cell(&mut self) -> Result<(), ObservationOwnerError> {
        let Some(cell) = self.pending_cell.take() else {
            return Ok(());
        };
        let measurement_set = self
            .measurement_set
            .as_mut()
            .ok_or(ObservationOwnerError::TransactionClosed)?;
        if let Some(values) = cell.model_data {
            queue_visibility_cell(
                measurement_set.main_table_mut(),
                "MODEL_DATA",
                cell.row,
                ArrayValue::Complex32(values),
            )?;
        }
        if let Some(values) = cell.corrected_data {
            queue_visibility_cell(
                measurement_set.main_table_mut(),
                "CORRECTED_DATA",
                cell.row,
                ArrayValue::Complex32(values),
            )?;
        }
        self.pending_rows.push(cell.row);
        if self.pending_rows.len() == VISIBILITY_WRITE_BATCH_ROWS as usize {
            self.persist_pending_rows()?;
        }
        Ok(())
    }

    fn persist_pending_rows(&mut self) -> Result<(), ObservationOwnerError> {
        if self.pending_rows.is_empty() {
            return Ok(());
        }
        let columns = match (self.targets.model_data, self.targets.corrected_data) {
            (true, true) => &["MODEL_DATA", "CORRECTED_DATA"][..],
            (true, false) => &["MODEL_DATA"][..],
            (false, true) => &["CORRECTED_DATA"][..],
            (false, false) => return Err(ObservationOwnerError::EmptyWriteTargets),
        };
        let measurement_set = self
            .measurement_set
            .as_mut()
            .ok_or(ObservationOwnerError::TransactionClosed)?;
        persist_visibility_rows(
            measurement_set.main_table_mut(),
            columns,
            &self.pending_rows,
        )?;
        self.pending_rows.clear();
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for SelectedVisibilityWrite {
    fn drop(&mut self) {
        if !self.completed {
            if let Some(measurement_set) = self.measurement_set.as_mut() {
                let _ = measurement_set.main_table_mut().unlock();
            }
            self.measurement_set = None;
        }
    }
}

/// Compiler input paired inseparably with the owner access used to probe it.
pub struct ResolvedSelectedObservation {
    snapshot_input: ObservationSnapshotInput,
    access: ResolvedSelectedObservationAccess,
}

impl ResolvedSelectedObservation {
    /// Split the compiler input from the access capability retained for execution.
    #[must_use]
    pub fn into_parts(self) -> (ObservationSnapshotInput, ResolvedSelectedObservationAccess) {
        (self.snapshot_input, self.access)
    }
}

/// Owner-minted state, Measures capability, and resource binding for one source.
pub struct ResolvedSelectedObservationAccess {
    binding: ObservationSourceBinding,
    measures: SelectedObservationMeasures,
    visibility_storage: SelectedVisibilityStoragePlanner,
}

struct SelectedVisibilityStoragePlanner {
    locator: String,
    selection: Arc<ObservationSelection>,
    content_budget: SelectedObservationContentBudget,
}

impl ResolvedSelectedObservationAccess {
    /// Return the exact current source state captured with the compiler input.
    #[must_use]
    pub const fn source_state(&self) -> &ObservationSourceState {
        self.binding.current_state()
    }

    /// Return the exact resource binding captured with the compiler input.
    #[must_use]
    pub const fn source_binding(&self) -> &ObservationSourceBinding {
        &self.binding
    }

    /// Return the payload-free storage plan captured by this owner resolution.
    pub fn selected_visibility_storage_plan(
        &self,
        targets: SelectedVisibilityWriteTargets,
    ) -> Result<SelectedVisibilityStoragePlan, ObservationOwnerError> {
        if !targets.model_data && !targets.corrected_data {
            return Err(ObservationOwnerError::EmptyWriteTargets);
        }
        let measurement_set = MeasurementSet::open_retained_read(&self.visibility_storage.locator)?;
        let manifest = OwnerManifest::read(measurement_set.main_table().keywords())?;
        validate_transaction_precondition(&manifest, &measurement_set, self.source_state())?;
        let empty = SelectedVisibilityStoragePlan {
            additional_persistent_bytes: 0,
            write_bytes: 0,
            maximum_cell_bytes: 0,
            write_buffer_bytes: 0,
        };
        let corrected = if targets.corrected_data {
            if !measurement_set
                .main_table()
                .schema()
                .is_some_and(|schema| schema.contains_column("CORRECTED_DATA"))
            {
                return Err(ObservationOwnerError::MissingCorrectedDataDestination);
            }
            derive_column_storage_plan(
                &measurement_set,
                &self.visibility_storage.selection,
                "CORRECTED_DATA",
                false,
                self.visibility_storage.content_budget,
            )?
        } else {
            empty
        };
        let model = if targets.model_data {
            derive_column_storage_plan(
                &measurement_set,
                &self.visibility_storage.selection,
                "MODEL_DATA",
                true,
                self.visibility_storage.content_budget,
            )?
        } else {
            empty
        };
        let maximum_cell_bytes = model
            .maximum_cell_bytes
            .checked_add(corrected.maximum_cell_bytes)
            .ok_or(ObservationOwnerError::PredictionAddress)?;
        Ok(SelectedVisibilityStoragePlan {
            additional_persistent_bytes: model.additional_persistent_bytes,
            write_bytes: model
                .write_bytes
                .checked_add(corrected.write_bytes)
                .ok_or(ObservationOwnerError::PredictionAddress)?,
            maximum_cell_bytes,
            write_buffer_bytes: model
                .write_buffer_bytes
                .checked_add(corrected.write_buffer_bytes)
                .ok_or(ObservationOwnerError::PredictionAddress)?,
        })
    }

    /// Mint the scheduler-visible residency certificate for the compiled problem.
    pub fn certify_residency(
        &self,
        problem: &casa_imaging_model::CompiledProblem,
    ) -> Result<SelectedObservationResidencyCertificate, BoundSelectedObservationError> {
        BoundSelectedObservation::certify_residency(problem, std::slice::from_ref(&self.binding))
    }

    /// Return the exact shared replay-proof heap that must remain reserved
    /// between major plans if this access completes exhaustively.
    pub fn replay_proof_retained_heap_bytes(
        &self,
        problem: &casa_imaging_model::CompiledProblem,
    ) -> Result<usize, BoundSelectedObservationError> {
        BoundSelectedObservation::replay_proof_retained_heap_bytes(
            problem,
            std::slice::from_ref(&self.binding),
        )
    }

    /// Consume this exact owner probe and open bounded retained observation access.
    #[cfg(unix)]
    pub fn open(
        self,
        problem: &casa_imaging_model::CompiledProblem,
    ) -> Result<BoundSelectedObservation, BoundSelectedObservationError> {
        BoundSelectedObservation::open_owner_validated(problem, self.measures, vec![self.binding])
    }

    /// Consume this fresh owner probe and authorize a prior exhaustive proof
    /// under newly acquired retained locks.
    #[cfg(unix)]
    pub fn rebind(
        self,
        problem: &casa_imaging_model::CompiledProblem,
        proof: &crate::SelectedObservationReplayProof,
    ) -> Result<BoundSelectedObservation, BoundSelectedObservationError> {
        BoundSelectedObservation::rebind(problem, self.measures, vec![self.binding], proof)
    }
}

/// Initialize or migrate an unmarked MeasurementSet into owner-authoritative imaging access.
///
/// The operation acquires the MAIN-table write lock, fails if any owner
/// manifest is already present, and publishes the complete v1 manifest in one
/// locked table-keyword update. Existing rows and standard MeasurementSet
/// metadata are not rewritten.
#[cfg(unix)]
pub fn initialize_measurement_set_owner_manifest(
    path: impl AsRef<Path>,
) -> Result<MeasurementSetIdentity, ObservationOwnerError> {
    let _process_guard = OWNER_INITIALIZATION_MUTEX
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut measurement_set = MeasurementSet::open_retained_read(path)?;
    if measurement_set
        .main_table()
        .keywords()
        .get(OWNER_MANIFEST_KEYWORD)
        .is_some()
    {
        return Err(ObservationOwnerError::AlreadyInitialized);
    }
    if !measurement_set.main_table_mut().lock(LockType::Write, 1)? {
        return Err(ObservationOwnerError::WriteLockUnavailable);
    }
    // Lock acquisition reloads table state after another writer commits. The
    // pre-lock check is only a fast path; this check is the atomic once-only
    // migration decision.
    if measurement_set
        .main_table()
        .keywords()
        .get(OWNER_MANIFEST_KEYWORD)
        .is_some()
    {
        measurement_set.main_table_mut().unlock_metadata_only()?;
        return Err(ObservationOwnerError::AlreadyInitialized);
    }
    let manifest = OwnerManifest::mint(&measurement_set)?;
    let encoded = serde_json::to_string(&manifest)?;
    measurement_set.main_table_mut().keywords_mut().upsert(
        OWNER_MANIFEST_KEYWORD,
        Value::Scalar(ScalarValue::String(encoded)),
    );
    measurement_set.main_table_mut().unlock_metadata_only()?;
    Ok(MeasurementSetIdentity::new(parse_identity(
        &manifest.measurement_set_identity,
        "MeasurementSet identity",
    )?))
}

/// Resolve a marked MeasurementSet into compiler input and matching owner access.
///
/// Unmarked datasets fail closed. The source manifest, physical schema, selected
/// rows, state probe, Measures provider, and resource binding are all evaluated
/// while one retained MeasurementSet read capability is alive.
#[cfg(unix)]
pub fn resolve_selected_observation(
    request: SelectedObservationResolutionRequest,
) -> Result<ResolvedSelectedObservation, ObservationOwnerError> {
    if request.reference_data.iter().any(|(kind, _)| {
        matches!(
            kind,
            ReferenceDataKind::Measures | ReferenceDataKind::Ephemeris
        )
    }) {
        return Err(ObservationOwnerError::MeasuresReferenceIsOwnerSupplied);
    }
    let measurement_set = MeasurementSet::open_retained_read(&request.locator)?;
    let manifest = OwnerManifest::read(measurement_set.main_table().keywords())?;
    manifest.validate_physical_state(&measurement_set)?;
    let selection = Arc::new(bind_physical_spectral_coordinates(
        &measurement_set,
        &request.selection,
        request.content_budget,
    )?);
    validate_physical_selection(&measurement_set, &selection, request.content_budget)?;
    let visibility_storage = SelectedVisibilityStoragePlanner {
        locator: request.locator.clone(),
        selection: Arc::clone(&selection),
        content_budget: request.content_budget,
    };
    let generations =
        manifest.source_generations(&measurement_set, request.visibility, request.weights)?;
    let identity = MeasurementSetIdentity::new(parse_identity(
        &manifest.measurement_set_identity,
        "MeasurementSet identity",
    )?);
    let state =
        ObservationSourceState::new(identity, selection.rows().clone(), generations.clone());
    let source = ObservationSourceInput::new(
        identity,
        ObservationSourceProvenance::new(request.locator, request.selection_request),
        Arc::unwrap_or_clone(selection),
        generations,
    );
    let measures = SelectedObservationMeasures::new(request.measures_provider)?;
    let mut reference_data = request.reference_data;
    reference_data.push((ReferenceDataKind::Measures, measures.identity()));
    if let Some(ephemeris) = request.ephemeris.as_ref() {
        reference_data.push((ReferenceDataKind::Ephemeris, ephemeris.identity()));
    }
    let snapshot_input = ObservationSnapshotInput::new(vec![source], reference_data, request.model);
    let binding = ObservationSourceBinding::new(state, request.content_budget)
        .with_ephemeris(request.ephemeris);
    Ok(ResolvedSelectedObservation {
        snapshot_input,
        access: ResolvedSelectedObservationAccess {
            binding,
            measures,
            visibility_storage,
        },
    })
}

fn bind_physical_spectral_coordinates(
    measurement_set: &MeasurementSet,
    selection: &ObservationSelection,
    content_budget: SelectedObservationContentBudget,
) -> Result<ObservationSelection, ObservationOwnerError> {
    let spectral_window = measurement_set.spectral_window()?;
    let mut retained_catalog_bytes = 0usize;
    let mut spectral_windows = Vec::with_capacity(selection.spectral_windows().len());
    for selected in selection.spectral_windows() {
        let row = usize::try_from(selected.spectral_window_id())
            .map_err(|_| ObservationOwnerError::PhysicalSelectionMismatch)?;
        let frequency_shape = spectral_window
            .table()
            .array_shape(row, "CHAN_FREQ")?
            .filter(|shape| shape.len() == 1 && shape[0] > 0)
            .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)?;
        let width_shape = spectral_window
            .table()
            .array_shape(row, "CHAN_WIDTH")?
            .filter(|shape| shape.len() == 1 && shape[0] > 0)
            .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)?;
        if frequency_shape != width_shape {
            return Err(ObservationOwnerError::PhysicalSelectionMismatch);
        }
        let values = frequency_shape[0];
        let simultaneous_array_bytes = values
            .checked_mul(2)
            .and_then(|values| values.checked_mul(std::mem::size_of::<f64>()))
            .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)?;
        let catalog_bytes = values
            .checked_mul(std::mem::size_of::<f64>())
            .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<usize>()))
            .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)?;
        // Arc<[f64]> construction briefly overlaps the source Vec with the
        // destination allocation. CHAN_WIDTH is dropped first, so both the
        // two-array read peak and the Arc conversion peak are exactly covered.
        let construction_peak_bytes = retained_catalog_bytes
            .checked_add(simultaneous_array_bytes)
            .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<usize>()))
            .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)?;
        retained_catalog_bytes = retained_catalog_bytes
            .checked_add(catalog_bytes)
            .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)?;
        let required_bytes = construction_peak_bytes.max(retained_catalog_bytes);
        if required_bytes > content_budget.available_bytes() {
            return Err(ObservationOwnerError::SpectralCoordinateCatalogBudget {
                required_bytes,
                available_bytes: content_budget.available_bytes(),
            });
        }
        let frequencies_hz = spectral_window.chan_freq(row)?;
        let widths_hz = spectral_window.chan_width(row)?;
        if frequencies_hz.len() != values || widths_hz.len() != values {
            return Err(ObservationOwnerError::PhysicalSelectionMismatch);
        }
        let first_channel_width_hz = widths_hz[0];
        drop(widths_hz);
        let coordinate_catalog =
            SpectralWindowCoordinateCatalog::new(frequencies_hz, first_channel_width_hz)
                .ok_or(ObservationOwnerError::PhysicalSelectionMismatch)?;
        spectral_windows.push(
            SpectralWindowSelection::new(
                selected.spectral_window_id(),
                selected.channel_indices().to_vec(),
            )
            .with_coordinate_catalog(coordinate_catalog),
        );
    }
    Ok(ObservationSelection::new(
        selection.rows().clone(),
        selection.rows_filter().clone(),
        selection.data_descriptions().to_vec(),
        spectral_windows,
        selection.correlations().to_vec(),
    ))
}

/// Rederive one compiled source's selected read state while its newly opened
/// retained read locks are held.
///
/// This is deliberately crate-private: cross-plan replay must pass through the
/// selected-observation owner, which combines this physical validation with an
/// opaque prior completion proof before exposing any blocks.
#[cfg(unix)]
pub(crate) fn validate_reopened_selected_observation_source(
    measurement_set: &MeasurementSet,
    source: &casa_imaging_model::ObservationSource,
    content_budget: SelectedObservationContentBudget,
) -> Result<ObservationSourceState, ObservationOwnerError> {
    let manifest = OwnerManifest::read(measurement_set.main_table().keywords())?;
    manifest.validate_physical_state(measurement_set)?;
    validate_physical_selection(measurement_set, source.selection(), content_budget)?;
    let selected_columns = source.generations().columns();
    let generations = manifest.source_generations(
        measurement_set,
        selected_columns.visibility(),
        selected_columns.weights(),
    )?;
    let identity = MeasurementSetIdentity::new(parse_identity(
        &manifest.measurement_set_identity,
        "MeasurementSet identity",
    )?);
    Ok(ObservationSourceState::new(
        identity,
        source.selection().rows().clone(),
        generations,
    ))
}

/// Failure to initialize, read, or bind a MeasurementSet owner manifest.
#[derive(Debug, Error)]
pub enum ObservationOwnerError {
    /// MeasurementSet or subtable access failed.
    #[error(transparent)]
    MeasurementSet(#[from] MsError),
    /// MAIN-table locking or keyword persistence failed.
    #[error(transparent)]
    Table(#[from] casa_tables::TableError),
    /// Operating-system entropy was unavailable for owner identity minting.
    #[error(transparent)]
    Entropy(#[from] getrandom::Error),
    /// The persisted manifest could not be encoded or decoded.
    #[error(transparent)]
    Encoding(#[from] serde_json::Error),
    /// Measures acquisition or bounded-state preparation failed.
    #[error(transparent)]
    Measures(#[from] SelectedObservationMeasuresError),
    /// The compiled physical coordinates no longer match the retained MS.
    #[error(transparent)]
    SelectedCoordinates(#[from] BoundObservationSourceError),
    /// The MeasurementSet has no owner manifest and must be migrated explicitly.
    #[error(
        "MeasurementSet has no {OWNER_MANIFEST_KEYWORD} keyword; initialize or migrate it explicitly before imaging"
    )]
    Uninitialized,
    /// Initialization never overwrites a prior or malformed owner manifest.
    #[error("MeasurementSet already contains an owner manifest")]
    AlreadyInitialized,
    /// The owner manifest schema is not understood by this reader.
    #[error("unsupported MeasurementSet owner-manifest schema version {0}")]
    UnsupportedSchema(u32),
    /// A persisted identity was malformed or the all-zero sentinel.
    #[error("invalid {field} in MeasurementSet owner manifest")]
    InvalidIdentity {
        /// Manifest field that failed validation.
        field: &'static str,
    },
    /// The manifest keyword has an incompatible casacore value type.
    #[error("MeasurementSet owner manifest keyword is not a scalar string")]
    InvalidKeywordType,
    /// One selected column is missing physically or from the owner manifest.
    #[error("selected MeasurementSet column {column} has no matching physical owner generation")]
    MissingColumnGeneration {
        /// Standard MAIN column name.
        column: &'static str,
    },
    /// One required metadata table is missing physically or from the owner manifest.
    #[error(
        "required MeasurementSet metadata table {table} has no matching physical owner generation"
    )]
    MissingMetadataGeneration {
        /// Standard subtable name.
        table: &'static str,
    },
    /// Physical `MODEL_DATA` existence no longer matches the owner manifest.
    #[error("physical MODEL_DATA state no longer matches the MeasurementSet owner manifest")]
    ModelColumnStateMismatch,
    /// A retained table lock observes a different durable modification counter.
    #[error(
        "MeasurementSet table {table} modification counter mismatch: manifest={expected:?}, retained={actual:?}"
    )]
    ModificationCounterMismatch {
        /// MAIN or standard subtable name.
        table: String,
        /// Counter persisted by the owner manifest, if any.
        expected: Option<u32>,
        /// Counter observed under the retained lock, if the table exists.
        actual: Option<u32>,
    },
    /// The selected physical MAIN row sequence differs from the compiled selection.
    #[error("selected physical MAIN rows no longer match the compiled observation selection")]
    PhysicalSelectionMismatch,
    /// The complete physical SPW coordinate catalog cannot fit its explicit source budget.
    #[error(
        "spectral coordinate catalog requires {required_bytes} bytes but the content budget has {available_bytes} bytes"
    )]
    SpectralCoordinateCatalogBudget {
        /// Peak catalog construction or retained bytes.
        required_bytes: usize,
        /// Total source content budget.
        available_bytes: usize,
    },
    /// Measures identity is always injected by the acquired provider.
    #[error(
        "reference_data must not include Measures; the storage owner injects it from the acquired provider"
    )]
    MeasuresReferenceIsOwnerSupplied,
    /// The MAIN write lock could not be acquired.
    #[error("could not acquire the MeasurementSet MAIN write lock")]
    WriteLockUnavailable,
    /// A selected visibility addressed a cell outside its destination.
    #[error("selected visibility address is outside its destination column")]
    PredictionAddress,
    /// The bounded selected-visibility writer was already completed or released.
    #[error("selected visibility write is closed")]
    TransactionClosed,
    /// The retained owner generation no longer matches the planned write.
    #[error("selected visibility write precondition changed")]
    TransactionPrecondition,
    /// No destination was selected for a write transaction.
    #[error("selected visibility write requires at least one destination")]
    EmptyWriteTargets,
    /// CORRECTED_DATA persistence requires an existing owner-tracked column.
    #[error("selected visibility write requires existing owner-tracked CORRECTED_DATA")]
    MissingCorrectedDataDestination,
    /// A cell write named a column outside the bound destination set.
    #[error("selected visibility write named an unselected destination")]
    UnselectedWriteTarget,
    /// Completion generations must exactly match the bound destination set.
    #[error("selected visibility write completion generations do not match its destinations")]
    WriteGenerationMismatch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OwnerManifest {
    schema_version: u32,
    measurement_set_identity: String,
    consistency_token: String,
    columns: BTreeMap<String, String>,
    metadata_tables: BTreeMap<String, String>,
    main_modify_counter: u32,
    metadata_modify_counters: BTreeMap<String, u32>,
    model_data: PersistedModelColumn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum PersistedModelColumn {
    Absent,
    Present { generation: String },
}

impl OwnerManifest {
    fn mint(measurement_set: &MeasurementSet) -> Result<Self, ObservationOwnerError> {
        let main = measurement_set.main_table();
        let schema = main.schema().ok_or_else(|| {
            MsError::InvalidInput("MeasurementSet MAIN table has no schema".to_string())
        })?;
        let mut entropy = [0_u8; 32];
        getrandom::fill(&mut entropy)?;
        let mut ordinal = 0_u64;
        let mut mint = |label: &[u8]| {
            ordinal += 1;
            encode_identity(mint_owner_identity(&entropy, ordinal, label))
        };
        let measurement_set_identity = mint(b"measurement-set");
        let consistency_token = mint(b"consistency-token");
        let mut columns = BTreeMap::new();
        for (kind, name) in TRACKED_COLUMNS {
            if *kind != MsColumnKind::ModelData && schema.contains_column(name) {
                columns.insert((*name).to_string(), mint(name.as_bytes()));
            }
        }
        let mut metadata_tables = BTreeMap::new();
        let mut metadata_modify_counters = BTreeMap::new();
        for (_, subtable) in TRACKED_METADATA {
            if let Some(table) = measurement_set.subtable(*subtable) {
                metadata_tables.insert(
                    subtable.name().to_string(),
                    mint(subtable.name().as_bytes()),
                );
                metadata_modify_counters
                    .insert(subtable.name().to_string(), table.locked_modify_counter()?);
            }
        }
        let model_data = if schema.contains_column("MODEL_DATA") {
            PersistedModelColumn::Present {
                generation: mint(b"MODEL_DATA"),
            }
        } else {
            PersistedModelColumn::Absent
        };
        Ok(Self {
            schema_version: OWNER_MANIFEST_SCHEMA_VERSION,
            measurement_set_identity,
            consistency_token,
            columns,
            metadata_tables,
            main_modify_counter: main.locked_modify_counter()?.wrapping_add(1),
            metadata_modify_counters,
            model_data,
        })
    }

    fn read(keywords: &RecordValue) -> Result<Self, ObservationOwnerError> {
        let value = keywords
            .get(OWNER_MANIFEST_KEYWORD)
            .ok_or(ObservationOwnerError::Uninitialized)?;
        let Value::Scalar(ScalarValue::String(encoded)) = value else {
            return Err(ObservationOwnerError::InvalidKeywordType);
        };
        let manifest: Self = serde_json::from_str(encoded)?;
        if manifest.schema_version != OWNER_MANIFEST_SCHEMA_VERSION {
            return Err(ObservationOwnerError::UnsupportedSchema(
                manifest.schema_version,
            ));
        }
        parse_identity(
            &manifest.measurement_set_identity,
            "MeasurementSet identity",
        )?;
        parse_identity(&manifest.consistency_token, "consistency token")?;
        for generation in manifest.columns.values() {
            parse_identity(generation, "column generation")?;
        }
        for generation in manifest.metadata_tables.values() {
            parse_identity(generation, "metadata generation")?;
        }
        if manifest
            .metadata_tables
            .keys()
            .ne(manifest.metadata_modify_counters.keys())
        {
            return Err(ObservationOwnerError::ModificationCounterMismatch {
                table: "metadata manifest key set".to_string(),
                expected: None,
                actual: None,
            });
        }
        if let PersistedModelColumn::Present { generation } = &manifest.model_data {
            parse_identity(generation, "MODEL_DATA generation")?;
        }
        Ok(manifest)
    }

    fn validate_physical_state(
        &self,
        measurement_set: &MeasurementSet,
    ) -> Result<(), ObservationOwnerError> {
        let actual_main = measurement_set.main_table().locked_modify_counter()?;
        if self.main_modify_counter != actual_main {
            return Err(ObservationOwnerError::ModificationCounterMismatch {
                table: "MAIN".to_string(),
                expected: Some(self.main_modify_counter),
                actual: Some(actual_main),
            });
        }
        for (_, subtable) in TRACKED_METADATA {
            let expected = self.metadata_modify_counters.get(subtable.name()).copied();
            let actual = measurement_set
                .subtable(*subtable)
                .map(Table::locked_modify_counter)
                .transpose()?;
            if expected != actual {
                return Err(ObservationOwnerError::ModificationCounterMismatch {
                    table: subtable.name().to_string(),
                    expected,
                    actual,
                });
            }
        }
        let physical_model = measurement_set
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("MODEL_DATA"));
        match (&self.model_data, physical_model) {
            (PersistedModelColumn::Absent, false)
            | (PersistedModelColumn::Present { .. }, true) => Ok(()),
            _ => Err(ObservationOwnerError::ModelColumnStateMismatch),
        }
    }

    fn source_generations(
        &self,
        measurement_set: &MeasurementSet,
        visibility: VisibilityColumn,
        weights: WeightColumn,
    ) -> Result<SourceGenerations, ObservationOwnerError> {
        let visibility_kind = match visibility {
            VisibilityColumn::Data => MsColumnKind::Data,
            VisibilityColumn::CorrectedData => MsColumnKind::CorrectedData,
            VisibilityColumn::FloatData => MsColumnKind::FloatData,
        };
        let weight_kind = match weights {
            WeightColumn::Weight => MsColumnKind::Weight,
            WeightColumn::WeightSpectrum => MsColumnKind::WeightSpectrum,
        };
        let selected_kinds = [
            visibility_kind,
            MsColumnKind::Flag,
            MsColumnKind::FlagRow,
            weight_kind,
        ]
        .into_iter()
        .chain(REQUIRED_COORDINATE_COLUMNS);
        let mut columns = Vec::new();
        for kind in selected_kinds {
            if columns
                .iter()
                .any(|generation: &ColumnGeneration| generation.kind() == kind)
            {
                continue;
            }
            let name = column_name(kind);
            let physical = measurement_set
                .main_table()
                .schema()
                .is_some_and(|schema| schema.contains_column(name));
            let Some(generation) = physical
                .then(|| self.columns.get(name))
                .flatten()
                .map(|generation| parse_identity(generation, "column generation"))
                .transpose()?
            else {
                return Err(ObservationOwnerError::MissingColumnGeneration { column: name });
            };
            columns.push(ColumnGeneration::new(kind, generation));
        }
        let mut metadata = Vec::with_capacity(REQUIRED_METADATA.len());
        for kind in REQUIRED_METADATA {
            let subtable = metadata_subtable(kind);
            let Some(generation) = measurement_set
                .subtable(subtable)
                .and_then(|_| self.metadata_tables.get(subtable.name()))
                .map(|generation| parse_identity(generation, "metadata generation"))
                .transpose()?
            else {
                return Err(ObservationOwnerError::MissingMetadataGeneration {
                    table: subtable.name(),
                });
            };
            metadata.push(MetadataGeneration::new(kind, generation));
        }
        let model_column = match &self.model_data {
            PersistedModelColumn::Absent => ModelColumnState::Absent,
            PersistedModelColumn::Present { generation } => {
                ModelColumnState::Present(parse_identity(generation, "MODEL_DATA generation")?)
            }
        };
        let corrected_data_column = self
            .columns
            .get("CORRECTED_DATA")
            .map(|generation| parse_identity(generation, "CORRECTED_DATA generation"))
            .transpose()?
            .map_or(
                CorrectedDataColumnState::Absent,
                CorrectedDataColumnState::Present,
            );
        Ok(SourceGenerations::new(
            ConsistencyToken::new(parse_identity(
                &self.consistency_token,
                "consistency token",
            )?),
            SelectedColumns::new(visibility, FlagPolicy::FlagOrFlagRow, weights, columns),
            metadata,
            model_column,
        )
        .with_corrected_data_column(corrected_data_column))
    }
}

fn validate_physical_selection(
    measurement_set: &MeasurementSet,
    selection: &ObservationSelection,
    content_budget: SelectedObservationContentBudget,
) -> Result<(), ObservationOwnerError> {
    validate_selected_coordinates(measurement_set, selection)?;
    let row_selection = SelectedObservationRowSelection::from_compiled(selection);
    let mut actual = SelectedRowsBuilder::with_data_description_capacity(
        u64::try_from(measurement_set.row_count())
            .map_err(|_| ObservationOwnerError::PhysicalSelectionMismatch)?,
        selection.data_descriptions().len(),
    );
    let mut invalid = false;
    measurement_set.visit_selected_observation_rows(
        &row_selection,
        physical_selection_io_budget(content_budget),
        |row| {
            if !invalid {
                invalid = actual
                    .push(SelectedMainRow::new(
                        row.physical_row() as u64,
                        u32::try_from(row.data_description_id()).unwrap_or(u32::MAX),
                    ))
                    .is_err();
            }
        },
    )?;
    if invalid || &actual.finish() != selection.rows() {
        return Err(ObservationOwnerError::PhysicalSelectionMismatch);
    }
    Ok(())
}

fn physical_selection_io_budget(
    content_budget: SelectedObservationContentBudget,
) -> MsSelectionIoBudget {
    MsSelectionIoBudget {
        available_bytes: content_budget.available_bytes(),
        maximum_live_blocks: content_budget.maximum_live_blocks(),
        requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
        storage_alignment_rows: None,
    }
}

fn mint_owner_identity(entropy: &[u8; 32], ordinal: u64, label: &[u8]) -> LogicalIdentity {
    let mut hasher = Sha256::new();
    hasher.update(OWNER_IDENTITY_DOMAIN);
    hasher.update(entropy);
    hasher.update(ordinal.to_le_bytes());
    hasher.update((label.len() as u64).to_le_bytes());
    hasher.update(label);
    LogicalIdentity::from_sha256(hasher.finalize().into())
}

fn encode_identity(identity: LogicalIdentity) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in identity.as_bytes() {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing into String is infallible");
    }
    encoded
}

fn parse_identity(
    encoded: &str,
    field: &'static str,
) -> Result<LogicalIdentity, ObservationOwnerError> {
    if encoded.len() != 64 {
        return Err(ObservationOwnerError::InvalidIdentity { field });
    }
    let mut bytes = [0_u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16)
            .map_err(|_| ObservationOwnerError::InvalidIdentity { field })?;
    }
    if bytes == [0; 32] {
        return Err(ObservationOwnerError::InvalidIdentity { field });
    }
    Ok(LogicalIdentity::from_sha256(bytes))
}

fn validate_transaction_precondition(
    manifest: &OwnerManifest,
    measurement_set: &MeasurementSet,
    expected: &ObservationSourceState,
) -> Result<(), ObservationOwnerError> {
    let identity = parse_identity(
        &manifest.measurement_set_identity,
        "MeasurementSet identity",
    )?;
    let token = parse_identity(&manifest.consistency_token, "consistency token")?;
    let physical_model = match &manifest.model_data {
        PersistedModelColumn::Absent => ModelColumnState::Absent,
        PersistedModelColumn::Present { generation } => {
            ModelColumnState::Present(parse_identity(generation, "MODEL_DATA generation")?)
        }
    };
    let expected_columns = expected.generations().columns();
    let actual_generations = manifest.source_generations(
        measurement_set,
        expected_columns.visibility(),
        expected_columns.weights(),
    )?;
    if expected.identity().identity() != identity
        || expected.generations().consistency_token().identity() != token
        || expected.generations().model_column() != physical_model
        || expected.generations() != &actual_generations
    {
        return Err(ObservationOwnerError::TransactionPrecondition);
    }
    manifest.validate_physical_state(measurement_set)
}

fn main_data_description_id(
    measurement_set: &MeasurementSet,
    row: usize,
) -> Result<usize, ObservationOwnerError> {
    let value = measurement_set
        .main_table()
        .column_accessor("DATA_DESC_ID")?
        .get(row)?
        .ok_or(ObservationOwnerError::PredictionAddress)?;
    let Value::Scalar(ScalarValue::Int32(data_description_id)) = value else {
        return Err(ObservationOwnerError::PredictionAddress);
    };
    usize::try_from(*data_description_id).map_err(|_| ObservationOwnerError::PredictionAddress)
}

fn model_cell_shape(
    measurement_set: &MeasurementSet,
    data_description_id: usize,
) -> Result<[usize; 2], ObservationOwnerError> {
    let data_description = measurement_set.data_description()?;
    let spectral_window_id =
        usize::try_from(data_description.spectral_window_id(data_description_id)?)
            .map_err(|_| ObservationOwnerError::PredictionAddress)?;
    let polarization_id = usize::try_from(data_description.polarization_id(data_description_id)?)
        .map_err(|_| ObservationOwnerError::PredictionAddress)?;
    let channels = usize::try_from(
        measurement_set
            .spectral_window()?
            .num_chan(spectral_window_id)?,
    )
    .map_err(|_| ObservationOwnerError::PredictionAddress)?;
    let correlations = usize::try_from(measurement_set.polarization()?.num_corr(polarization_id)?)
        .map_err(|_| ObservationOwnerError::PredictionAddress)?;
    Ok([correlations, channels])
}

fn model_cell_bytes(
    measurement_set: &MeasurementSet,
    data_description_id: usize,
) -> Result<u64, ObservationOwnerError> {
    let [correlations, channels] = model_cell_shape(measurement_set, data_description_id)?;
    u64::try_from(correlations)
        .ok()
        .and_then(|correlations| {
            u64::try_from(channels)
                .ok()
                .and_then(|channels| correlations.checked_mul(channels))
        })
        .and_then(|samples| samples.checked_mul(std::mem::size_of::<Complex32>() as u64))
        .ok_or(ObservationOwnerError::PredictionAddress)
}

fn derive_column_storage_plan(
    measurement_set: &MeasurementSet,
    selection: &ObservationSelection,
    column: &str,
    create_if_absent: bool,
    content_budget: SelectedObservationContentBudget,
) -> Result<SelectedVisibilityStoragePlan, ObservationOwnerError> {
    let has_column = measurement_set
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(column));
    let data_description_count = measurement_set.data_description()?.row_count();
    let mut bytes_by_data_description = Vec::with_capacity(data_description_count);
    for data_description_id in 0..data_description_count {
        bytes_by_data_description.push(model_cell_bytes(measurement_set, data_description_id)?);
    }
    let mut selected_write_bytes = 0_u64;
    let mut maximum_cell_bytes = 0_u64;
    let mut selected_error = false;
    measurement_set.visit_selected_observation_rows(
        &SelectedObservationRowSelection::from_compiled(selection),
        physical_selection_io_budget(content_budget),
        |row| {
            let Some(bytes) = usize::try_from(row.data_description_id())
                .ok()
                .and_then(|id| bytes_by_data_description.get(id))
                .copied()
            else {
                selected_error = true;
                return;
            };
            selected_write_bytes = selected_write_bytes.saturating_add(bytes);
            maximum_cell_bytes = maximum_cell_bytes.max(bytes);
        },
    )?;
    if selected_error || selected_write_bytes == u64::MAX {
        return Err(ObservationOwnerError::PredictionAddress);
    }
    let mut additional_persistent_bytes = 0_u64;
    if create_if_absent && !has_column {
        let mut invalid_data_description = false;
        let plan = crate::MsReadPlan::new(
            measurement_set.row_count(),
            physical_selection_io_budget(content_budget),
        )
        .map_err(|_| ObservationOwnerError::PredictionAddress)?;
        measurement_set.visit_main_row_selection_blocks(plan, |block| {
            for offset in 0..block.len() {
                let fact = block
                    .row(offset)
                    .expect("offset is bounded by MAIN selection block length");
                let Some(bytes) = usize::try_from(fact.data_description_id())
                    .ok()
                    .and_then(|id| bytes_by_data_description.get(id))
                    .copied()
                else {
                    invalid_data_description = true;
                    continue;
                };
                additional_persistent_bytes = additional_persistent_bytes.saturating_add(bytes);
                maximum_cell_bytes = maximum_cell_bytes.max(bytes);
            }
        })?;
        if invalid_data_description || additional_persistent_bytes == u64::MAX {
            return Err(ObservationOwnerError::PredictionAddress);
        }
    }
    let write_bytes = additional_persistent_bytes
        .checked_add(selected_write_bytes)
        .ok_or(ObservationOwnerError::PredictionAddress)?;
    let possible_buffer_rows = if create_if_absent && !has_column {
        u64::try_from(measurement_set.row_count())
            .map_err(|_| ObservationOwnerError::PredictionAddress)?
    } else {
        selection.rows().selected_row_count()
    }
    .min(VISIBILITY_WRITE_BATCH_ROWS);
    Ok(SelectedVisibilityStoragePlan {
        additional_persistent_bytes,
        write_bytes,
        maximum_cell_bytes,
        write_buffer_bytes: maximum_cell_bytes
            .checked_mul(possible_buffer_rows)
            .ok_or(ObservationOwnerError::PredictionAddress)?,
    })
}

#[cfg(unix)]
fn queue_visibility_cell(
    table: &mut Table,
    column: &str,
    row: usize,
    value: ArrayValue,
) -> Result<(), ObservationOwnerError> {
    {
        let mut prepared = table.row_accessor_mut().prepare(&[column])?;
        prepared.seek(row)?;
        prepared.set_value_at(0, Value::Array(value))?;
    }
    Ok(())
}

#[cfg(unix)]
fn persist_visibility_rows(
    table: &mut Table,
    columns: &[&str],
    rows: &[usize],
) -> Result<(), ObservationOwnerError> {
    table.prepare_write().save_selected_rows(columns, rows)?;
    table.discard_persisted_cell_updates(columns, rows);
    Ok(())
}

fn write_owner_manifest(
    table: &mut Table,
    manifest: &OwnerManifest,
) -> Result<(), ObservationOwnerError> {
    table.keywords_mut().upsert(
        OWNER_MANIFEST_KEYWORD,
        Value::Scalar(ScalarValue::String(serde_json::to_string(manifest)?)),
    );
    Ok(())
}

fn column_name(kind: MsColumnKind) -> &'static str {
    TRACKED_COLUMNS
        .iter()
        .find_map(|(candidate, name)| (*candidate == kind).then_some(*name))
        .expect("every tracked column kind has one owner-manifest name")
}

fn metadata_subtable(kind: MetadataTableKind) -> SubtableId {
    TRACKED_METADATA
        .iter()
        .find_map(|(candidate, subtable)| (*candidate == kind).then_some(*subtable))
        .expect("every model metadata kind has one owner-manifest subtable")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MeasurementSetBuilder, OptionalMainColumn, column_def::ColumnDef, schema,
        test_helpers::default_value_for_def,
    };
    use casa_imaging_model::{
        AntennaSelection, CorrelationProduct, CorrelationSelection, CorrelationType,
        DataDescriptionSelection, IdSelection, IntentSelection, RowSelection, SelectedMainRow,
        SelectedRows, SpectralWindowSelection, TimeSelection, UvSelection, compile_observation,
    };
    use casa_tables::{LockMode, LockOptions, TableOptions};
    use casa_types::{ArrayValue, Complex32, RecordField};
    use ndarray::ArrayD;

    fn identity(byte: u8) -> LogicalIdentity {
        LogicalIdentity::from_sha256([byte; 32])
    }

    fn one_row_selection() -> ObservationSelection {
        one_row_selection_with_channels(vec![0])
    }

    fn one_row_selection_with_channels(channel_indices: Vec<u32>) -> ObservationSelection {
        one_row_selection_with_total_rows(channel_indices, 1)
    }

    fn one_row_selection_with_total_rows(
        channel_indices: Vec<u32>,
        total_rows: usize,
    ) -> ObservationSelection {
        let rows = (0..total_rows)
            .map(|row| SelectedMainRow::new(row as u64, 0))
            .collect::<Vec<_>>();
        ObservationSelection::new(
            SelectedRows::from_ordered_main_rows(total_rows as u64, rows)
                .expect("ordered selection manifest"),
            RowSelection::new(
                IdSelection::All,
                TimeSelection::All,
                UvSelection::All,
                AntennaSelection::All,
                IdSelection::All,
                IdSelection::All,
                IntentSelection::All,
                IdSelection::All,
            ),
            vec![DataDescriptionSelection::new(0, 0, 0)],
            vec![SpectralWindowSelection::new(0, channel_indices)],
            vec![CorrelationSelection::new(
                0,
                vec![CorrelationProduct::new(0, CorrelationType::CircularRr)],
            )],
        )
    }

    #[test]
    #[cfg(unix)]
    fn resolution_binds_exact_full_nonuniform_spw_for_a_subselection() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("nonuniform-subselection.ms");
        let frequencies_hz = [1.0e9, 1.001e9, 1.004e9, 1.010e9];
        let widths_hz = [-1.0e6, -1.1e6, -2.5e6, -4.0e6];
        create_ms_columns_with_spectral_coordinates(
            &path,
            false,
            false,
            &frequencies_hz,
            &widths_hz,
        );
        initialize_measurement_set_owner_manifest(&path).expect("initialize owner manifest");
        let request = SelectedObservationResolutionRequest::new(
            path.display().to_string(),
            identity(2),
            one_row_selection_with_channels(vec![1, 3]),
            VisibilityColumn::Data,
            WeightColumn::Weight,
            Vec::new(),
            ModelStateIdentity::Empty,
            SelectedObservationContentBudget::new(1 << 20, 1, 4),
            casa_test_support::deterministic_measures_provider_for_identity([90; 32]),
        );

        let (snapshot_input, _) = resolve_selected_observation(request)
            .expect("resolve exact physical catalog")
            .into_parts();
        let snapshot = compile_observation(snapshot_input).expect("compile bound catalog");
        let spectral_window = &snapshot.sources()[0].selection().spectral_windows()[0];
        assert_eq!(spectral_window.channel_indices(), &[1, 3]);
        let catalog = spectral_window
            .coordinate_catalog()
            .expect("owner-certified full SPW catalog");
        assert_eq!(catalog.channel_frequencies_hz(), frequencies_hz);
        assert_eq!(catalog.first_channel_width_hz(), widths_hz[0]);
        let measurement_set = MeasurementSet::open(&path).expect("reopen physical catalog");
        validate_selected_coordinates(&measurement_set, snapshot.sources()[0].selection())
            .expect("runtime access accepts the exact owner-certified catalog");
    }

    #[test]
    #[cfg(unix)]
    fn full_spw_catalog_construction_obeys_the_source_content_budget() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("catalog-budget.ms");
        create_ms_columns_with_spectral_coordinates(
            &path,
            false,
            false,
            &[1.0e9, 1.001e9, 1.004e9, 1.010e9],
            &[1.0e6; 4],
        );
        let measurement_set = MeasurementSet::open(&path).expect("open test MS");

        assert!(matches!(
            bind_physical_spectral_coordinates(
                &measurement_set,
                &one_row_selection_with_channels(vec![1, 3]),
                SelectedObservationContentBudget::new(79, 1, 4),
            ),
            Err(ObservationOwnerError::SpectralCoordinateCatalogBudget {
                required_bytes: 80,
                available_bytes: 79,
            })
        ));
    }

    fn request(path: &Path) -> SelectedObservationResolutionRequest {
        SelectedObservationResolutionRequest::new(
            path.display().to_string(),
            identity(2),
            one_row_selection(),
            VisibilityColumn::Data,
            WeightColumn::Weight,
            Vec::new(),
            ModelStateIdentity::Empty,
            SelectedObservationContentBudget::new(1 << 20, 1, 4),
            casa_test_support::deterministic_measures_provider_for_identity([90; 32]),
        )
    }

    #[test]
    fn physical_selection_batches_rows_within_the_owner_content_budget() {
        let content_budget = SelectedObservationContentBudget::new(64 << 20, 2, 4);
        let io_budget = physical_selection_io_budget(content_budget);

        assert_eq!(
            io_budget,
            MsSelectionIoBudget {
                available_bytes: 64 << 20,
                maximum_live_blocks: 2,
                requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                storage_alignment_rows: None,
            }
        );
        let plan = crate::MsReadPlan::new(42_320, io_budget).expect("budget admits the scan");
        assert_eq!(plan.rows_per_block, 42_320);
        assert_eq!(plan.row_count, 42_320);
    }

    fn create_ms(path: &Path, model_data: bool) {
        create_ms_columns(path, model_data, false);
    }

    fn create_ms_columns(path: &Path, model_data: bool, corrected_data: bool) {
        create_ms_columns_with_spectral_coordinates(
            path,
            model_data,
            corrected_data,
            &[1.0e9],
            &[1.0e6],
        );
    }

    fn create_ms_columns_with_spectral_coordinates(
        path: &Path,
        model_data: bool,
        corrected_data: bool,
        channel_frequencies_hz: &[f64],
        channel_widths_hz: &[f64],
    ) {
        create_ms_columns_with_spectral_coordinates_and_rows(
            path,
            model_data,
            corrected_data,
            channel_frequencies_hz,
            channel_widths_hz,
            1,
        );
    }

    fn create_ms_columns_with_spectral_coordinates_and_rows(
        path: &Path,
        model_data: bool,
        corrected_data: bool,
        channel_frequencies_hz: &[f64],
        channel_widths_hz: &[f64],
        row_count: usize,
    ) {
        assert_eq!(channel_frequencies_hz.len(), channel_widths_hz.len());
        assert!(!channel_frequencies_hz.is_empty());
        assert!(row_count > 0);
        let channel_count = channel_frequencies_hz.len();
        let mut builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
        if model_data {
            builder = builder.with_main_column(OptionalMainColumn::ModelData);
        }
        if corrected_data {
            builder = builder.with_main_column(OptionalMainColumn::CorrectedData);
        }
        let mut measurement_set =
            MeasurementSet::create(path, builder).expect("create test MeasurementSet");

        measurement_set
            .subtable_mut(SubtableId::Polarization)
            .expect("POLARIZATION subtable")
            .add_row(row(
                schema::polarization::REQUIRED_COLUMNS,
                &[
                    ("NUM_CORR", Value::Scalar(ScalarValue::Int32(1))),
                    (
                        "CORR_TYPE",
                        Value::Array(ArrayValue::Int32(
                            ArrayD::from_shape_vec(vec![1], vec![5]).expect("one RR code"),
                        )),
                    ),
                    (
                        "CORR_PRODUCT",
                        Value::Array(ArrayValue::Int32(
                            ArrayD::from_shape_vec(vec![2, 1], vec![0, 0])
                                .expect("one receptor pair"),
                        )),
                    ),
                ],
            ))
            .expect("add POLARIZATION row");
        measurement_set
            .subtable_mut(SubtableId::SpectralWindow)
            .expect("SPECTRAL_WINDOW subtable")
            .add_row(row(
                schema::spectral_window::REQUIRED_COLUMNS,
                &[
                    (
                        "NUM_CHAN",
                        Value::Scalar(ScalarValue::Int32(
                            i32::try_from(channel_count).expect("test channel count"),
                        )),
                    ),
                    (
                        "CHAN_FREQ",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(
                                vec![channel_count],
                                channel_frequencies_hz.to_vec(),
                            )
                            .expect("channel frequencies"),
                        )),
                    ),
                    (
                        "CHAN_WIDTH",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(vec![channel_count], channel_widths_hz.to_vec())
                                .expect("channel widths"),
                        )),
                    ),
                    (
                        "EFFECTIVE_BW",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(
                                vec![channel_count],
                                channel_widths_hz.iter().map(|width| width.abs()).collect(),
                            )
                            .expect("effective bandwidths"),
                        )),
                    ),
                    (
                        "RESOLUTION",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(
                                vec![channel_count],
                                channel_widths_hz.iter().map(|width| width.abs()).collect(),
                            )
                            .expect("channel resolutions"),
                        )),
                    ),
                    (
                        "REF_FREQUENCY",
                        Value::Scalar(ScalarValue::Float64(channel_frequencies_hz[0])),
                    ),
                    (
                        "TOTAL_BANDWIDTH",
                        Value::Scalar(ScalarValue::Float64(
                            channel_widths_hz.iter().map(|width| width.abs()).sum(),
                        )),
                    ),
                    ("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))),
                ],
            ))
            .expect("add SPECTRAL_WINDOW row");
        measurement_set
            .subtable_mut(SubtableId::DataDescription)
            .expect("DATA_DESCRIPTION subtable")
            .add_row(row(
                schema::data_description::REQUIRED_COLUMNS,
                &[
                    ("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
                    ("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                ],
            ))
            .expect("add DATA_DESCRIPTION row");

        let main_schema = measurement_set
            .main_table()
            .schema()
            .expect("MAIN schema")
            .clone();
        let main = main_schema
            .columns()
            .iter()
            .map(|column| {
                let value = match column.name() {
                    "DATA_DESC_ID" => Value::Scalar(ScalarValue::Int32(0)),
                    "DATA" | "MODEL_DATA" | "CORRECTED_DATA" => {
                        Value::Array(ArrayValue::Complex32(
                            ArrayD::from_shape_vec(
                                vec![1, channel_count],
                                vec![Complex32::new(1.0, 0.0); channel_count],
                            )
                            .expect("channel visibilities"),
                        ))
                    }
                    "FLAG" => Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(vec![1, channel_count], vec![false; channel_count])
                            .expect("channel flags"),
                    )),
                    "WEIGHT" => Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![1], vec![1.0]).expect("one weight"),
                    )),
                    _ => crate::test_helpers::default_value(column.name()),
                };
                RecordField::new(column.name(), value)
            })
            .collect();
        let main = RecordValue::new(main);
        for _ in 0..row_count {
            measurement_set
                .main_table_mut()
                .add_row(main.clone())
                .expect("add MAIN row");
        }
        measurement_set.save().expect("save test MeasurementSet");
    }

    fn row(definitions: &[ColumnDef], overrides: &[(&str, Value)]) -> RecordValue {
        RecordValue::new(
            definitions
                .iter()
                .map(|definition| {
                    let value = overrides
                        .iter()
                        .find_map(|(name, value)| (*name == definition.name).then(|| value.clone()))
                        .unwrap_or_else(|| default_value_for_def(definition));
                    RecordField::new(definition.name, value)
                })
                .collect(),
        )
    }

    fn external_locked_keyword_mutation(path: &Path) {
        let mut table = Table::open_with_lock(
            TableOptions::new(path),
            LockOptions::new(LockMode::UserLocking),
        )
        .expect("open external writer");
        assert!(
            table
                .lock(LockType::Write, 0)
                .expect("acquire external write lock")
        );
        table.keywords_mut().upsert(
            "EXTERNAL_LOCKED_MUTATION",
            Value::Scalar(ScalarValue::Bool(true)),
        );
        table.unlock().expect("commit external locked mutation");
    }

    #[test]
    #[cfg(unix)]
    fn unmarked_measurement_set_fails_closed_until_explicit_initialization() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("unmarked.ms");
        create_ms(&path, false);
        let data_managers_before = MeasurementSet::open(&path)
            .expect("open before initialization")
            .main_table()
            .data_manager_info()
            .iter()
            .filter(|manager| manager.columns.iter().any(|column| column == "DATA"))
            .map(|manager| manager.dm_type.clone())
            .collect::<Vec<_>>();

        assert!(matches!(
            resolve_selected_observation(request(&path)),
            Err(ObservationOwnerError::Uninitialized)
        ));
        initialize_measurement_set_owner_manifest(&path).expect("explicit migration");
        let data_managers_after = MeasurementSet::open(&path)
            .expect("open after initialization")
            .main_table()
            .data_manager_info()
            .iter()
            .filter(|manager| manager.columns.iter().any(|column| column == "DATA"))
            .map(|manager| manager.dm_type.clone())
            .collect::<Vec<_>>();
        assert_eq!(data_managers_after, data_managers_before);
        assert_eq!(data_managers_after, vec!["TiledShapeStMan"]);
        resolve_selected_observation(request(&path)).expect("marked owner resolves");
    }

    #[test]
    #[cfg(unix)]
    fn concurrent_initialization_mints_exactly_one_owner_identity() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("concurrent.ms");
        create_ms(&path, false);
        let barrier = Arc::new(std::sync::Barrier::new(2));

        let workers = (0..2)
            .map(|_| {
                let path = path.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    initialize_measurement_set_owner_manifest(path)
                })
            })
            .collect::<Vec<_>>();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().expect("initializer thread"))
            .collect::<Vec<_>>();

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(ObservationOwnerError::AlreadyInitialized)))
                .count(),
            1
        );
        let persisted = OwnerManifest::read(
            MeasurementSet::open(&path)
                .expect("reopen initialized MS")
                .main_table()
                .keywords(),
        )
        .expect("one complete persisted manifest");
        let successful = results
            .into_iter()
            .find_map(Result::ok)
            .expect("one successful identity");
        assert_eq!(
            parse_identity(
                &persisted.measurement_set_identity,
                "MeasurementSet identity"
            )
            .expect("persisted identity"),
            successful.identity()
        );
    }

    #[test]
    #[cfg(unix)]
    fn resolver_returns_one_canonical_snapshot_and_matching_owner_state() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("owned.ms");
        create_ms(&path, false);
        let initialized =
            initialize_measurement_set_owner_manifest(&path).expect("initialize owner manifest");

        let resolved = resolve_selected_observation(request(&path)).expect("resolve owner");
        let (snapshot_input, access) = resolved.into_parts();
        assert_eq!(access.source_state().identity(), initialized);
        assert_eq!(
            access.source_state().generations().model_column(),
            ModelColumnState::Absent
        );
        assert_eq!(
            access.source_binding().measurement_set(),
            access.source_state().identity()
        );
        let snapshot = compile_observation(snapshot_input).expect("compile owner snapshot");
        assert_eq!(snapshot.sources()[0].identity(), initialized);
        assert_eq!(
            snapshot.sources()[0].generations(),
            access.source_state().generations()
        );
        assert_eq!(
            snapshot.sources()[0].selection().rows(),
            access.source_state().selected_rows()
        );
        assert_eq!(
            snapshot.reference_data(),
            &[(ReferenceDataKind::Measures, identity(90))]
        );
    }

    #[test]
    #[cfg(unix)]
    fn initialization_is_once_only_and_location_independent() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let original = directory.path().join("original.ms");
        let moved = directory.path().join("moved.ms");
        create_ms(&original, false);
        let initialized = initialize_measurement_set_owner_manifest(&original)
            .expect("initialize owner manifest");
        assert!(matches!(
            initialize_measurement_set_owner_manifest(&original),
            Err(ObservationOwnerError::AlreadyInitialized)
        ));

        std::fs::rename(&original, &moved).expect("relocate MeasurementSet");
        let resolved = resolve_selected_observation(request(&moved)).expect("resolve after move");
        assert_eq!(resolved.access.source_state().identity(), initialized);
    }

    #[test]
    #[cfg(unix)]
    fn model_data_existence_and_generation_are_owner_bound() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("model.ms");
        create_ms(&path, true);
        initialize_measurement_set_owner_manifest(&path).expect("initialize owner manifest");

        let resolved = resolve_selected_observation(request(&path)).expect("resolve owner");
        let ModelColumnState::Present(model_generation) =
            resolved.access.source_state().generations().model_column()
        else {
            panic!("MODEL_DATA must be marked present")
        };
        assert_ne!(model_generation.as_bytes(), [0; 32]);
    }

    #[test]
    #[cfg(unix)]
    fn interrupted_model_column_write_fails_closed_without_rollback() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("model-abort.ms");
        create_ms(&path, true);
        initialize_measurement_set_owner_manifest(&path).expect("initialize owner manifest");
        let before = resolve_selected_observation(request(&path)).expect("resolve owner");
        let expected = before.access.source_state().clone();
        let generation = expected.generations().model_column();

        {
            let mut writer = SelectedVisibilityWrite::begin(
                &path,
                &expected,
                &one_row_selection(),
                SelectedVisibilityWriteTargets::new(true, false),
            )
            .expect("begin write");
            writer
                .write(MsColumnKind::ModelData, 0, 0, 0, Complex32::new(9.0, -2.0))
                .expect("write prediction");
        }

        let raw = Table::open(TableOptions::new(&path)).expect("inspect marked MAIN directly");
        let Value::Array(ArrayValue::Complex32(values)) = raw
            .column_accessor("MODEL_DATA")
            .expect("MODEL_DATA accessor")
            .get(0)
            .expect("read existing MODEL_DATA")
            .expect("defined MODEL_DATA cell")
        else {
            panic!("MODEL_DATA is complex")
        };
        assert_eq!(
            values[[0, 0]],
            Complex32::new(1.0, 0.0),
            "begin and an unflushed prediction must not persist a destructive zero pass"
        );

        let error = match MeasurementSet::open(&path) {
            Ok(_) => panic!("incomplete write must fail closed"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("incomplete write marker"));
        assert!(path.join(".casa-rs-write-incomplete").exists());
        assert!(matches!(generation, ModelColumnState::Present(_)));
    }

    #[test]
    #[cfg(unix)]
    fn model_column_storage_plan_reserves_capacity_only_for_creation() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("model-plan-create.ms");
        create_ms(&path, false);
        initialize_measurement_set_owner_manifest(&path).expect("initialize owner manifest");
        let resolved = resolve_selected_observation(request(&path)).expect("resolve owner");
        let plan = resolved
            .access
            .selected_visibility_storage_plan(SelectedVisibilityWriteTargets::new(true, false))
            .expect("MODEL_DATA storage plan");

        assert_eq!(plan.additional_persistent_bytes(), 8);
        assert_eq!(plan.write_bytes(), 16);
        assert_eq!(plan.maximum_cell_bytes(), 8);
        assert_eq!(plan.write_buffer_bytes(), 8);

        let existing_path = directory.path().join("model-plan-overwrite.ms");
        create_ms(&existing_path, true);
        initialize_measurement_set_owner_manifest(&existing_path)
            .expect("initialize existing MODEL_DATA owner manifest");
        let existing = resolve_selected_observation(request(&existing_path))
            .expect("resolve existing MODEL_DATA owner");
        let existing_plan = existing
            .access
            .selected_visibility_storage_plan(SelectedVisibilityWriteTargets::new(true, false))
            .expect("MODEL_DATA storage plan");

        assert_eq!(existing_plan.additional_persistent_bytes(), 0);
        assert_eq!(existing_plan.write_bytes(), 8);
        assert_eq!(existing_plan.maximum_cell_bytes(), 8);
        assert_eq!(existing_plan.write_buffer_bytes(), 8);
    }

    #[test]
    #[cfg(unix)]
    fn read_only_resolution_does_not_traverse_unselected_rows_for_a_write_plan() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("read-only-plan.ms");
        create_ms(&path, false);

        let mut measurement_set = MeasurementSet::open(&path).expect("reopen test MS");
        let schema = measurement_set
            .main_table()
            .schema()
            .expect("MAIN schema")
            .clone();
        let second_row = schema
            .columns()
            .iter()
            .map(|column| {
                let value = match column.name() {
                    "DATA_DESC_ID" => Value::Scalar(ScalarValue::Int32(99)),
                    "FIELD_ID" => Value::Scalar(ScalarValue::Int32(1)),
                    "DATA" => Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(vec![1, 1], vec![Complex32::new(1.0, 0.0)])
                            .expect("one visibility"),
                    )),
                    "FLAG" => Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(vec![1, 1], vec![false]).expect("one flag"),
                    )),
                    "WEIGHT" => Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(vec![1], vec![1.0]).expect("one weight"),
                    )),
                    _ => crate::test_helpers::default_value(column.name()),
                };
                RecordField::new(column.name(), value)
            })
            .collect();
        measurement_set
            .main_table_mut()
            .add_row(RecordValue::new(second_row))
            .expect("add unselected row");
        measurement_set.save().expect("save two-row MS");
        initialize_measurement_set_owner_manifest(&path).expect("initialize owner manifest");

        let selection = ObservationSelection::new(
            SelectedRows::from_ordered_main_rows(2, [SelectedMainRow::new(0, 0)])
                .expect("selected row manifest"),
            RowSelection::new(
                IdSelection::Only(vec![0]),
                TimeSelection::All,
                UvSelection::All,
                AntennaSelection::All,
                IdSelection::All,
                IdSelection::All,
                IntentSelection::All,
                IdSelection::All,
            ),
            vec![DataDescriptionSelection::new(0, 0, 0)],
            vec![SpectralWindowSelection::new(0, vec![0])],
            vec![CorrelationSelection::new(
                0,
                vec![CorrelationProduct::new(0, CorrelationType::CircularRr)],
            )],
        );
        let request = SelectedObservationResolutionRequest::new(
            path.display().to_string(),
            identity(2),
            selection,
            VisibilityColumn::Data,
            WeightColumn::Weight,
            Vec::new(),
            ModelStateIdentity::Empty,
            SelectedObservationContentBudget::new(1 << 20, 1, 4),
            casa_test_support::deterministic_measures_provider_for_identity([90; 32]),
        );

        resolve_selected_observation(request)
            .expect("read-only resolution must not plan an unrequested MODEL_DATA write");
    }

    #[test]
    #[cfg(unix)]
    fn completed_model_column_write_publishes_exact_prediction_and_generation() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("model-commit.ms");
        create_ms(&path, false);
        initialize_measurement_set_owner_manifest(&path).expect("initialize owner manifest");
        let before = resolve_selected_observation(request(&path)).expect("resolve owner");
        let expected = before.access.source_state().clone();
        let generation = identity(73);

        let mut writer = SelectedVisibilityWrite::begin(
            &path,
            &expected,
            &one_row_selection(),
            SelectedVisibilityWriteTargets::new(true, false),
        )
        .expect("begin write");
        writer
            .write(MsColumnKind::ModelData, 0, 0, 0, Complex32::new(4.5, -1.25))
            .expect("write prediction");
        writer
            .complete(SelectedVisibilityWriteGenerations {
                model_data: Some(generation),
                corrected_data: None,
            })
            .expect("complete write");

        let reopened = MeasurementSet::open(&path).expect("reopen committed MS");
        let schema = reopened.main_table().schema().expect("MAIN schema");
        assert!(schema.contains_column("MODEL_DATA"));
        assert!(!path.join(".casa-rs-write-incomplete").exists());
        let model_column = reopened
            .data_column(crate::VisibilityDataColumn::ModelData)
            .expect("MODEL_DATA column");
        let ArrayValue::Complex32(values) = model_column.get(0).expect("MODEL_DATA cell") else {
            panic!("MODEL_DATA is complex")
        };
        assert_eq!(values[[0, 0]], Complex32::new(4.5, -1.25));
        drop(reopened);
        let after = resolve_selected_observation(request(&path)).expect("resolve after commit");
        assert_eq!(
            after.access.source_state().generations().model_column(),
            ModelColumnState::Present(generation)
        );
    }

    #[test]
    #[cfg(unix)]
    fn model_column_creation_is_not_persisted_one_row_at_a_time() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("model-create-throughput.ms");
        create_ms_columns_with_spectral_coordinates_and_rows(
            &path,
            false,
            false,
            &[1.0e9],
            &[1.0e6],
            4_096,
        );
        initialize_measurement_set_owner_manifest(&path).expect("initialize owner manifest");
        let selection = one_row_selection_with_total_rows(vec![0], 4_096);
        let before = resolve_selected_observation(SelectedObservationResolutionRequest::new(
            path.display().to_string(),
            identity(2),
            selection.clone(),
            VisibilityColumn::Data,
            WeightColumn::Weight,
            Vec::new(),
            ModelStateIdentity::Empty,
            SelectedObservationContentBudget::new(1 << 20, 1, 4),
            casa_test_support::deterministic_measures_provider_for_identity([90; 32]),
        ))
        .expect("resolve owner");
        let expected = before.access.source_state().clone();

        let started = std::time::Instant::now();
        let writer = SelectedVisibilityWrite::begin(
            &path,
            &expected,
            &selection,
            SelectedVisibilityWriteTargets::new(true, false),
        )
        .expect("begin MODEL_DATA creation");
        let elapsed = started.elapsed();
        drop(writer);

        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "creating a 4096-row MODEL_DATA column took {elapsed:?}; the creation path must not persist one row per transaction"
        );
    }

    #[test]
    #[cfg(unix)]
    fn model_column_updates_are_persisted_in_bounded_row_batches() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("model-update-throughput.ms");
        create_ms_columns_with_spectral_coordinates_and_rows(
            &path,
            true,
            false,
            &[1.0e9],
            &[1.0e6],
            4_096,
        );
        initialize_measurement_set_owner_manifest(&path).expect("initialize owner manifest");
        let selection = one_row_selection_with_total_rows(vec![0], 4_096);
        let before = resolve_selected_observation(SelectedObservationResolutionRequest::new(
            path.display().to_string(),
            identity(2),
            selection.clone(),
            VisibilityColumn::Data,
            WeightColumn::Weight,
            Vec::new(),
            ModelStateIdentity::Empty,
            SelectedObservationContentBudget::new(1 << 20, 1, 4),
            casa_test_support::deterministic_measures_provider_for_identity([90; 32]),
        ))
        .expect("resolve owner");
        let expected = before.access.source_state().clone();

        let started = std::time::Instant::now();
        let mut writer = SelectedVisibilityWrite::begin(
            &path,
            &expected,
            &selection,
            SelectedVisibilityWriteTargets::new(true, false),
        )
        .expect("begin MODEL_DATA update");
        for row in 0..4_096 {
            writer
                .write(
                    MsColumnKind::ModelData,
                    row,
                    0,
                    0,
                    Complex32::new(row as f32, -1.0),
                )
                .expect("write predicted row");
        }
        writer
            .complete(SelectedVisibilityWriteGenerations {
                model_data: Some(identity(76)),
                corrected_data: None,
            })
            .expect("complete MODEL_DATA update");
        let elapsed = started.elapsed();

        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "updating a 4096-row MODEL_DATA column took {elapsed:?}; row updates must use bounded batches"
        );
        let reopened = MeasurementSet::open(&path).expect("reopen updated MODEL_DATA");
        let model = reopened
            .data_column(crate::VisibilityDataColumn::ModelData)
            .expect("MODEL_DATA column");
        let ArrayValue::Complex32(last) = model.get(4_095).expect("last MODEL_DATA row") else {
            panic!("MODEL_DATA is complex")
        };
        assert_eq!(last[[0, 0]], Complex32::new(4_095.0, -1.0));
    }

    #[test]
    #[cfg(unix)]
    fn combined_visibility_write_uses_one_owner_transaction_and_publishes_both_generations() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("combined-write.ms");
        create_ms_columns(&path, true, true);
        initialize_measurement_set_owner_manifest(&path).expect("initialize owner manifest");
        let before = resolve_selected_observation(request(&path)).expect("resolve owner");
        let expected = before.access.source_state().clone();
        let before_token = expected.generations().consistency_token();
        let model_generation = identity(74);
        let corrected_generation = identity(75);

        let mut writer = SelectedVisibilityWrite::begin(
            &path,
            &expected,
            &one_row_selection(),
            SelectedVisibilityWriteTargets::new(true, true),
        )
        .expect("begin combined write");
        writer
            .write(MsColumnKind::ModelData, 0, 0, 0, Complex32::new(4.0, -1.0))
            .expect("write model");
        writer
            .write(
                MsColumnKind::CorrectedData,
                0,
                0,
                0,
                Complex32::new(2.5, 0.5),
            )
            .expect("write corrected");
        writer
            .complete(SelectedVisibilityWriteGenerations {
                model_data: Some(model_generation),
                corrected_data: Some(corrected_generation),
            })
            .expect("complete combined write");

        assert!(!path.join(".casa-rs-write-incomplete").exists());
        let reopened = MeasurementSet::open(&path).expect("reopen committed MS");
        let model_column = reopened
            .data_column(crate::VisibilityDataColumn::ModelData)
            .expect("MODEL_DATA");
        let ArrayValue::Complex32(model) = model_column.get(0).expect("model cell") else {
            panic!("MODEL_DATA is complex")
        };
        let corrected_column = reopened
            .data_column(crate::VisibilityDataColumn::CorrectedData)
            .expect("CORRECTED_DATA");
        let ArrayValue::Complex32(corrected) = corrected_column.get(0).expect("corrected cell")
        else {
            panic!("CORRECTED_DATA is complex")
        };
        assert_eq!(model[[0, 0]], Complex32::new(4.0, -1.0));
        assert_eq!(corrected[[0, 0]], Complex32::new(2.5, 0.5));
        drop(reopened);

        let after = resolve_selected_observation(request(&path)).expect("resolve after commit");
        let generations = after.access.source_state().generations();
        assert_eq!(
            generations.model_column(),
            ModelColumnState::Present(model_generation)
        );
        assert_eq!(
            generations.corrected_data_column(),
            CorrectedDataColumnState::Present(corrected_generation)
        );
        assert_ne!(generations.consistency_token(), before_token);
    }

    #[test]
    #[cfg(unix)]
    fn external_locked_main_mutation_invalidates_owner_resolution() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("main-mutated.ms");
        create_ms(&path, false);
        initialize_measurement_set_owner_manifest(&path).expect("initialize owner manifest");

        external_locked_keyword_mutation(&path);

        assert!(matches!(
            resolve_selected_observation(request(&path)),
            Err(ObservationOwnerError::ModificationCounterMismatch { table, .. })
                if table == "MAIN"
        ));
    }

    #[test]
    #[cfg(unix)]
    fn external_locked_metadata_mutation_invalidates_owner_resolution() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("metadata-mutated.ms");
        create_ms(&path, false);
        initialize_measurement_set_owner_manifest(&path).expect("initialize owner manifest");

        external_locked_keyword_mutation(&path.join("FIELD"));

        assert!(matches!(
            resolve_selected_observation(request(&path)),
            Err(ObservationOwnerError::ModificationCounterMismatch { table, .. })
                if table == "FIELD"
        ));
    }
}
