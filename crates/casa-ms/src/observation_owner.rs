// SPDX-License-Identifier: LGPL-3.0-or-later

//! Storage-owner identity and generation authority for native imaging.
//!
//! The owner manifest is one versioned string keyword in the MeasurementSet
//! MAIN table. It does not alter any standard row, column, subtable, or image
//! semantic, and C++ casacore preserves the unknown keyword. Logical identity
//! is owner-minted from operating-system entropy; filesystem paths, inode and
//! timestamp metadata, and content hashes never participate.

use std::{collections::BTreeMap, path::Path, sync::Arc};

use casa_imaging_model::{
    ColumnGeneration, ConsistencyToken, FlagPolicy, LogicalIdentity, MeasurementSetIdentity,
    MetadataGeneration, MetadataTableKind, ModelColumnState, ModelStateIdentity, MsColumnKind,
    ObservationSelection, ObservationSnapshotInput, ObservationSourceInput,
    ObservationSourceProvenance, ObservationSourceState, ReferenceDataKind, SelectedColumns,
    SourceGenerations, VisibilityColumn, WeightColumn,
};
use casa_tables::{LockType, Table};
use casa_types::{RecordValue, ScalarValue, Value, measures::MeasuresProvider};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::selected_observation::validate_selected_coordinates;
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
#[derive(Clone)]
pub struct SelectedObservationResolutionRequest {
    locator: String,
    selection_request: LogicalIdentity,
    selection: ObservationSelection,
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
            selection,
            visibility,
            weights,
            reference_data,
            model,
            content_budget,
            measures_provider,
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
    let manifest = OwnerManifest::mint(&measurement_set)?;
    let encoded = serde_json::to_string(&manifest)?;
    measurement_set.main_table_mut().keywords_mut().upsert(
        OWNER_MANIFEST_KEYWORD,
        Value::Scalar(ScalarValue::String(encoded)),
    );
    measurement_set.main_table_mut().unlock()?;
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
        request.selection,
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
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
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

        assert!(matches!(
            resolve_selected_observation(request(&path)),
            Err(ObservationOwnerError::Uninitialized)
        ));
        initialize_measurement_set_owner_manifest(&path).expect("explicit migration");
        resolve_selected_observation(request(&path)).expect("marked owner resolves");
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
