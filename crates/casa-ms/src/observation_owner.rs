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
    sync::{Arc, Condvar, Mutex},
};

use casa_imaging_model::{
    ColumnGeneration, ConsistencyToken, FlagPolicy, LogicalIdentity, MeasurementSetIdentity,
    MetadataGeneration, MetadataTableKind, ModelColumnState, ModelStateIdentity, MsColumnKind,
    ObservationSelection, ObservationSnapshotInput, ObservationSourceInput,
    ObservationSourceProvenance, ObservationSourceState, ReferenceDataKind, SelectedColumns,
    SourceGenerations, VisibilityColumn, WeightColumn,
};
use casa_tables::{ColumnSchema, LockType, Table};
use casa_types::{
    ArrayValue, Complex32, PrimitiveType, RecordValue, ScalarValue, Value,
    measures::MeasuresProvider,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::selected_observation::{BoundObservationSource, validate_selected_coordinates};
use crate::{
    BoundObservationSourceError, BoundSelectedObservation, BoundSelectedObservationError,
    MeasurementSet, MsError, MsSelectionIoBudget, ObservationSourceBinding,
    SelectedObservationContentBudget, SelectedObservationMeasures,
    SelectedObservationMeasuresError, SelectedObservationResidencyCertificate,
    SelectedObservationRow, SelectedObservationRowSelection, SubtableId,
};

const OWNER_MANIFEST_KEYWORD: &str = "CASA_RS_IMAGING_OWNER_MANIFEST";
const OWNER_MANIFEST_SCHEMA_VERSION: u32 = 1;
const OWNER_IDENTITY_DOMAIN: &[u8] = b"casa-rs-ms-owner-manifest-v1";
static OWNER_INITIALIZATION_MUTEX: Mutex<()> = Mutex::new(());
static MODEL_COLUMN_WRITE_ACTIVE: (Mutex<bool>, Condvar) = (Mutex::new(false), Condvar::new());

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
        }
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

/// Bounded in-place `MODEL_DATA` writer following ordinary casacore semantics.
///
/// The writer retains the MAIN write lock and the repository's standard
/// incomplete-write marker. Successful completion flushes `MODEL_DATA`, updates
/// its owner generation, and removes the marker. Failure may leave partially
/// written derived values, but the retained marker makes that state fail closed
/// until explicitly recovered or recomputed. No backup, staging column,
/// rollback, snapshot, or content digest is created.
#[cfg(unix)]
pub struct ModelDataWrite {
    measurement_set: Option<MeasurementSet>,
    manifest: OwnerManifest,
    incomplete_marker: Option<std::path::PathBuf>,
    completed: bool,
    _process_lease: ModelColumnProcessLease,
}

/// Process-wide exclusion is represented by an owned lease rather than a
/// thread-bound mutex guard so one bounded writer can move with its worker.
struct ModelColumnProcessLease;

impl ModelColumnProcessLease {
    fn acquire() -> Result<Self, ObservationOwnerError> {
        let (active, available) = &MODEL_COLUMN_WRITE_ACTIVE;
        let mut active = active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while *active {
            active = available
                .wait(active)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        *active = true;
        Ok(Self)
    }
}

impl Drop for ModelColumnProcessLease {
    fn drop(&mut self) {
        let (active, available) = &MODEL_COLUMN_WRITE_ACTIVE;
        let mut active = active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active = false;
        available.notify_one();
    }
}

/// Exact physical bounds of one bounded `MODEL_DATA` write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelColumnStorageBounds {
    column_bytes: u64,
    maximum_cell_bytes: u64,
}

impl ModelColumnStorageBounds {
    /// Bytes occupied by all complex samples in the complete destination column.
    #[must_use]
    pub const fn column_bytes(self) -> u64 {
        self.column_bytes
    }

    /// Largest single-row cell copied or updated by the bounded writer.
    #[must_use]
    pub const fn maximum_cell_bytes(self) -> u64 {
        self.maximum_cell_bytes
    }
}

/// Probe physical write bounds while validating the owner precondition.
#[cfg(unix)]
pub fn model_column_storage_bounds(
    path: impl AsRef<Path>,
    expected: &ObservationSourceState,
) -> Result<ModelColumnStorageBounds, ObservationOwnerError> {
    let measurement_set = MeasurementSet::open_retained_read(path)?;
    let manifest = OwnerManifest::read(measurement_set.main_table().keywords())?;
    manifest.validate_physical_state(&measurement_set)?;
    validate_transaction_precondition(&manifest, &measurement_set, expected)?;
    let data = measurement_set.data_column(crate::VisibilityDataColumn::Data)?;
    let mut column_bytes = 0_u64;
    let mut maximum_cell_bytes = 0_u64;
    for row in 0..measurement_set.main_table().row_count() {
        let samples = u64::try_from(data.get(row)?.len())
            .map_err(|_| ObservationOwnerError::PredictionAddress)?;
        let bytes = samples
            .checked_mul(std::mem::size_of::<Complex32>() as u64)
            .ok_or(ObservationOwnerError::PredictionAddress)?;
        column_bytes = column_bytes
            .checked_add(bytes)
            .ok_or(ObservationOwnerError::PredictionAddress)?;
        maximum_cell_bytes = maximum_cell_bytes.max(bytes);
    }
    Ok(ModelColumnStorageBounds {
        column_bytes,
        maximum_cell_bytes,
    })
}

#[cfg(unix)]
impl ModelDataWrite {
    /// Acquire owner write authority and start a detectable in-place mutation.
    pub fn begin(
        path: impl AsRef<Path>,
        expected: &ObservationSourceState,
        selection: &ObservationSelection,
    ) -> Result<Self, ObservationOwnerError> {
        let path = path.as_ref();
        let process_lease = ModelColumnProcessLease::acquire()?;
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
        let incomplete_marker = crate::write_session::begin_in_place_write(path)?;
        if !has_model {
            measurement_set.main_table_mut().add_column(
                ColumnSchema::array_variable("MODEL_DATA", PrimitiveType::Complex32, Some(2)),
                None,
            )?;
            measurement_set
                .main_table_mut()
                .prepare_write()
                .add_tiled_column_clone("DATA", "MODEL_DATA", "TiledModelData")?;
            let row_count = measurement_set.main_table().row_count();
            for row in 0..row_count {
                let source = zero_model_cell(
                    measurement_set
                        .data_column(crate::VisibilityDataColumn::Data)?
                        .get(row)?,
                )?;
                measurement_set
                    .main_table_mut()
                    .row_accessor_mut()
                    .set_cell(row, "MODEL_DATA", Value::Array(source))?;
            }
        }
        zero_selected_model_cells(&mut measurement_set, selection)?;
        Ok(Self {
            measurement_set: Some(measurement_set),
            manifest,
            incomplete_marker,
            completed: false,
            _process_lease: process_lease,
        })
    }

    /// Write one selected prediction at its physical row/channel/correlation.
    pub fn write(
        &mut self,
        row: u64,
        channel: u32,
        correlation: u32,
        value: Complex32,
    ) -> Result<(), ObservationOwnerError> {
        let row = usize::try_from(row).map_err(|_| ObservationOwnerError::PredictionAddress)?;
        let measurement_set = self
            .measurement_set
            .as_mut()
            .ok_or(ObservationOwnerError::TransactionClosed)?;
        let current = measurement_set
            .main_table()
            .column_accessor("MODEL_DATA")?
            .get(row)?
            .cloned()
            .ok_or(ObservationOwnerError::PredictionAddress)?;
        let Value::Array(ArrayValue::Complex32(mut values)) = current else {
            return Err(ObservationOwnerError::PredictionAddress);
        };
        let index = [correlation as usize, channel as usize];
        let Some(cell) = values.get_mut(index) else {
            return Err(ObservationOwnerError::PredictionAddress);
        };
        *cell = value;
        measurement_set
            .main_table_mut()
            .row_accessor_mut()
            .set_cell(
                row,
                "MODEL_DATA",
                Value::Array(ArrayValue::Complex32(values)),
            )?;
        Ok(())
    }

    /// Flush the in-place write, publish its generation, and clear the marker.
    pub fn complete(mut self, generation: LogicalIdentity) -> Result<(), ObservationOwnerError> {
        let measurement_set = self
            .measurement_set
            .as_mut()
            .ok_or(ObservationOwnerError::TransactionClosed)?;
        measurement_set
            .main_table_mut()
            .prepare_write()
            .save_selected_columns(&["MODEL_DATA"])?;
        self.manifest.model_data = PersistedModelColumn::Present {
            generation: encode_identity(generation),
        };
        let mut hasher = Sha256::new();
        hasher.update(b"casa-rs-model-column-consistency-v1");
        hasher.update(
            parse_identity(&self.manifest.consistency_token, "consistency token")?.as_bytes(),
        );
        hasher.update(generation.as_bytes());
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
}

#[cfg(unix)]
impl Drop for ModelDataWrite {
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

    /// Replace the caller's upper-bound content budget with the smallest budget
    /// that the MeasurementSet owner can prove admits one complete selected row.
    ///
    /// The owner derives this value from the same retained metadata, Measures
    /// state, shared allocation charges, and content planner used by [`Self::open`].
    /// The caller's live-block and POINTING polynomial ceilings are preserved;
    /// an upper bound too small to admit one row fails closed.
    #[cfg(unix)]
    pub fn with_minimum_content_budget(
        mut self,
        problem: &casa_imaging_model::CompiledProblem,
    ) -> Result<Self, BoundSelectedObservationError> {
        BoundSelectedObservation::certify_residency(problem, std::slice::from_ref(&self.binding))?;
        let source = problem
            .inputs()
            .observation_snapshot()
            .sources()
            .first()
            .ok_or(BoundSelectedObservationError::BindingSetMismatch)?;
        let shared_bytes = BoundSelectedObservation::single_source_shared_bytes(
            problem,
            &self.measures,
            &self.binding,
        )?;
        let measurement_set = self.binding.measurement_set();
        let content_budget = BoundObservationSource::minimum_content_budget_with_measures(
            problem,
            source,
            self.binding.current_state(),
            &self.measures,
            shared_bytes,
            self.binding.content_budget(),
        )
        .map_err(|error| BoundSelectedObservationError::Source {
            measurement_set,
            error: Box::new(error),
        })?;
        self.binding =
            ObservationSourceBinding::new(self.binding.current_state().clone(), content_budget);
        Ok(self)
    }

    /// Mint the scheduler-visible residency certificate for the compiled problem.
    pub fn certify_residency(
        &self,
        problem: &casa_imaging_model::CompiledProblem,
    ) -> Result<SelectedObservationResidencyCertificate, BoundSelectedObservationError> {
        BoundSelectedObservation::certify_residency(problem, std::slice::from_ref(&self.binding))
    }

    /// Consume this exact owner probe and open bounded retained observation access.
    #[cfg(unix)]
    pub fn open(
        self,
        problem: &casa_imaging_model::CompiledProblem,
    ) -> Result<BoundSelectedObservation, BoundSelectedObservationError> {
        BoundSelectedObservation::open(problem, self.measures, vec![self.binding])
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
    if request
        .reference_data
        .iter()
        .any(|(kind, _)| *kind == ReferenceDataKind::Measures)
    {
        return Err(ObservationOwnerError::MeasuresReferenceIsOwnerSupplied);
    }
    let measurement_set = MeasurementSet::open_retained_read(&request.locator)?;
    let manifest = OwnerManifest::read(measurement_set.main_table().keywords())?;
    manifest.validate_physical_state(&measurement_set)?;
    validate_physical_selection(&measurement_set, &request.selection)?;
    let generations =
        manifest.source_generations(&measurement_set, request.visibility, request.weights)?;
    let identity = MeasurementSetIdentity::new(parse_identity(
        &manifest.measurement_set_identity,
        "MeasurementSet identity",
    )?);
    let state = ObservationSourceState::new(
        identity,
        request.selection.rows().clone(),
        generations.clone(),
    );
    let source = ObservationSourceInput::new(
        identity,
        ObservationSourceProvenance::new(request.locator, request.selection_request),
        Arc::unwrap_or_clone(request.selection),
        generations,
    );
    let measures = SelectedObservationMeasures::new(request.measures_provider)?;
    let mut reference_data = request.reference_data;
    reference_data.push((ReferenceDataKind::Measures, measures.identity()));
    let snapshot_input = ObservationSnapshotInput::new(vec![source], reference_data, request.model);
    let binding = ObservationSourceBinding::new(state, request.content_budget);
    Ok(ResolvedSelectedObservation {
        snapshot_input,
        access: ResolvedSelectedObservationAccess { binding, measures },
    })
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
    /// Measures identity is always injected by the acquired provider.
    #[error(
        "reference_data must not include Measures; the storage owner injects it from the acquired provider"
    )]
    MeasuresReferenceIsOwnerSupplied,
    /// The MAIN write lock could not be acquired.
    #[error("could not acquire the MeasurementSet MAIN write lock")]
    WriteLockUnavailable,
    /// Final prediction addressed a row/channel/correlation outside MODEL_DATA.
    #[error("final prediction address is outside MODEL_DATA")]
    PredictionAddress,
    /// The bounded MODEL_DATA writer was already completed or released.
    #[error("MODEL_DATA write is closed")]
    TransactionClosed,
    /// The retained owner generation no longer matches the planned write.
    #[error("MODEL_DATA write precondition changed")]
    TransactionPrecondition,
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
        Ok(SourceGenerations::new(
            ConsistencyToken::new(parse_identity(
                &self.consistency_token,
                "consistency token",
            )?),
            SelectedColumns::new(visibility, FlagPolicy::FlagOrFlagRow, weights, columns),
            metadata,
            model_column,
        ))
    }
}

fn validate_physical_selection(
    measurement_set: &MeasurementSet,
    selection: &ObservationSelection,
) -> Result<(), ObservationOwnerError> {
    validate_selected_coordinates(measurement_set, selection)?;
    let expected = selection.rows().ordered_main_rows();
    let row_selection = SelectedObservationRowSelection::from_compiled(selection);
    let mut visited = 0_usize;
    let mut mismatch = false;
    measurement_set.visit_selected_observation_rows(
        &row_selection,
        MsSelectionIoBudget {
            available_bytes: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            maximum_live_blocks: 1,
            requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: None,
        },
        |row| {
            let matches = expected.get(visited).is_some_and(|planned| {
                planned.physical_row() == row.physical_row() as u64
                    && i32::try_from(planned.data_description_id()).ok()
                        == Some(row.data_description_id())
            });
            mismatch |= !matches;
            visited += 1;
        },
    )?;
    if mismatch || visited != expected.len() {
        return Err(ObservationOwnerError::PhysicalSelectionMismatch);
    }
    Ok(())
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

fn zero_model_cell(source: &ArrayValue) -> Result<ArrayValue, ObservationOwnerError> {
    let ArrayValue::Complex32(values) = source else {
        return Err(ObservationOwnerError::PredictionAddress);
    };
    Ok(ArrayValue::Complex32(ndarray::ArrayD::from_elem(
        values.raw_dim(),
        Complex32::new(0.0, 0.0),
    )))
}

#[cfg(unix)]
fn zero_selected_model_cells(
    measurement_set: &mut MeasurementSet,
    selection: &ObservationSelection,
) -> Result<(), ObservationOwnerError> {
    for selected_row in selection.rows().ordered_main_rows() {
        let description = selection
            .data_descriptions()
            .iter()
            .find(|description| {
                description.data_description_id() == selected_row.data_description_id()
            })
            .ok_or(ObservationOwnerError::PredictionAddress)?;
        let channels = selection
            .spectral_windows()
            .iter()
            .find(|spectral| spectral.spectral_window_id() == description.spectral_window_id())
            .ok_or(ObservationOwnerError::PredictionAddress)?;
        let correlations = selection
            .correlations()
            .iter()
            .find(|correlation| correlation.polarization_id() == description.polarization_id())
            .ok_or(ObservationOwnerError::PredictionAddress)?;
        let row = usize::try_from(selected_row.physical_row())
            .map_err(|_| ObservationOwnerError::PredictionAddress)?;
        let row_flag = measurement_set
            .main_table()
            .column_accessor("FLAG_ROW")?
            .get(row)?
            .ok_or(ObservationOwnerError::PredictionAddress)?;
        match row_flag {
            Value::Scalar(ScalarValue::Bool(true)) => continue,
            Value::Scalar(ScalarValue::Bool(false)) => {}
            _ => return Err(ObservationOwnerError::PredictionAddress),
        }
        let current = measurement_set
            .main_table()
            .column_accessor("MODEL_DATA")?
            .get(row)?
            .cloned()
            .ok_or(ObservationOwnerError::PredictionAddress)?;
        let Value::Array(ArrayValue::Complex32(mut values)) = current else {
            return Err(ObservationOwnerError::PredictionAddress);
        };
        for channel in channels.channel_indices() {
            for product in correlations.products() {
                let index = [product.correlation_index() as usize, *channel as usize];
                let Some(value) = values.get_mut(index) else {
                    return Err(ObservationOwnerError::PredictionAddress);
                };
                *value = Complex32::new(0.0, 0.0);
            }
        }
        measurement_set
            .main_table_mut()
            .row_accessor_mut()
            .set_cell(
                row,
                "MODEL_DATA",
                Value::Array(ArrayValue::Complex32(values)),
            )?;
    }
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
        .expect("every model column kind has one owner-manifest name")
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
        ObservationSelection::new(
            SelectedRows::from_ordered_main_rows(1, [SelectedMainRow::new(0, 0)])
                .expect("one-row selection manifest"),
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
            vec![SpectralWindowSelection::new(0, vec![0])],
            vec![CorrelationSelection::new(
                0,
                vec![CorrelationProduct::new(0, CorrelationType::CircularRr)],
            )],
        )
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

    fn create_ms(path: &Path, model_data: bool) {
        let mut builder = MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data);
        if model_data {
            builder = builder.with_main_column(OptionalMainColumn::ModelData);
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
                    ("NUM_CHAN", Value::Scalar(ScalarValue::Int32(1))),
                    (
                        "CHAN_FREQ",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(vec![1], vec![1.0e9])
                                .expect("one channel frequency"),
                        )),
                    ),
                    (
                        "CHAN_WIDTH",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(vec![1], vec![1.0e6])
                                .expect("one channel width"),
                        )),
                    ),
                    (
                        "EFFECTIVE_BW",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(vec![1], vec![1.0e6])
                                .expect("one effective bandwidth"),
                        )),
                    ),
                    (
                        "RESOLUTION",
                        Value::Array(ArrayValue::Float64(
                            ArrayD::from_shape_vec(vec![1], vec![1.0e6])
                                .expect("one channel resolution"),
                        )),
                    ),
                    ("REF_FREQUENCY", Value::Scalar(ScalarValue::Float64(1.0e9))),
                    (
                        "TOTAL_BANDWIDTH",
                        Value::Scalar(ScalarValue::Float64(1.0e6)),
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
                    "DATA" | "MODEL_DATA" => Value::Array(ArrayValue::Complex32(
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
            .add_row(RecordValue::new(main))
            .expect("add MAIN row");
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
            let mut writer =
                ModelDataWrite::begin(&path, &expected, &one_row_selection()).expect("begin write");
            writer
                .write(0, 0, 0, Complex32::new(9.0, -2.0))
                .expect("write prediction");
        }

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
    fn model_column_storage_bounds_cover_the_complete_destination_column() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("model-bounds.ms");
        create_ms(&path, false);
        initialize_measurement_set_owner_manifest(&path).expect("initialize owner manifest");
        let resolved = resolve_selected_observation(request(&path)).expect("resolve owner");
        let bounds = model_column_storage_bounds(&path, resolved.access.source_state())
            .expect("probe complete column bounds");

        assert_eq!(bounds.column_bytes(), 8);
        assert_eq!(bounds.maximum_cell_bytes(), 8);
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

        let mut writer =
            ModelDataWrite::begin(&path, &expected, &one_row_selection()).expect("begin write");
        writer
            .write(0, 0, 0, Complex32::new(4.5, -1.25))
            .expect("write prediction");
        writer.complete(generation).expect("complete write");

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
