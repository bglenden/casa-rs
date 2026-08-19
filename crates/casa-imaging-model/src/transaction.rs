// SPDX-License-Identifier: LGPL-3.0-or-later

//! MeasurementSet access and atomic-side-effect contracts for imaging.

use std::fmt;

use crate::compiled_problem::CanonicalEncoder;
use crate::{
    ColumnGeneration, ConsistencyToken, LogicalIdentity, MeasurementSetIdentity,
    MetadataGeneration, ModelColumnState, MsColumnKind, ObservationSelection, ObservationSnapshot,
    ObservationSnapshotId,
};

const OBSERVATION_TRANSACTION_IDENTITY_DOMAIN: &[u8] = b"casa-rs-observation-transaction";
const OBSERVATION_TRANSACTION_IDENTITY_VERSION: u32 = 2;

/// Stable compiler-derived identity of one snapshot-bound access contract.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObservationTransactionId(LogicalIdentity);

impl ObservationTransactionId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = OBSERVATION_TRANSACTION_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for ObservationTransactionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ObservationTransactionId({})", self.0)
    }
}

impl fmt::Display for ObservationTransactionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Whether a compiled imaging run writes predicted visibilities to `MODEL_DATA`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelColumnWrite {
    /// Do not mutate any MeasurementSet.
    Disabled,
    /// Replace the selected `MODEL_DATA` cells for every snapshot source.
    SelectedRows,
}

/// Snapshot-captured `MODEL_DATA` state that an atomic replacement must match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelColumnPrecondition {
    /// The column did not exist when the observation snapshot was captured.
    Absent,
    /// The column existed with this exact storage-owner generation.
    Generation(LogicalIdentity),
}

/// Physical coverage required to implement one logical `MODEL_DATA` write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelColumnWriteDisposition {
    /// Replace only the selected cells in an existing column generation.
    ReplaceSelectedCells,
    /// Create the column and initialize every source row before selected replacement.
    CreateAndInitializeAllRows {
        /// Exact MeasurementSet row count whose cells require initialization.
        row_count: u64,
        /// Canonical initialization value for cells outside the selected replacement.
        initialization: ModelColumnInitialization,
    },
}

/// Canonical initialization for a newly created `MODEL_DATA` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelColumnInitialization {
    /// Initialize every complex visibility cell to zero.
    Zero,
}

/// User-visible side-effect requirements compiled against one observation snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationTransactionRequirements {
    model_column_write: ModelColumnWrite,
}

impl ObservationTransactionRequirements {
    /// Construct the complete MeasurementSet side-effect requirement.
    #[must_use]
    pub const fn new(model_column_write: ModelColumnWrite) -> Self {
        Self { model_column_write }
    }

    /// Return the requested `MODEL_DATA` behavior.
    #[must_use]
    pub const fn model_column_write(self) -> ModelColumnWrite {
        self.model_column_write
    }
}

/// Exact immutable MeasurementSet data consumed from one snapshot source.
#[derive(Debug, Clone, PartialEq)]
pub struct MeasurementSetReadAccess {
    measurement_set: MeasurementSetIdentity,
    selection: ObservationSelection,
    column_generations: Vec<ColumnGeneration>,
    metadata: Vec<MetadataGeneration>,
    consistency_token: ConsistencyToken,
}

impl MeasurementSetReadAccess {
    /// Return the location-independent source identity.
    #[must_use]
    pub const fn measurement_set(&self) -> MeasurementSetIdentity {
        self.measurement_set
    }

    /// Return the exact selected rows, channels, and correlations.
    #[must_use]
    pub const fn selection(&self) -> &ObservationSelection {
        &self.selection
    }

    /// Return every MAIN column read and its exact generation, in canonical order.
    #[must_use]
    pub fn column_generations(&self) -> &[ColumnGeneration] {
        &self.column_generations
    }

    /// Return every metadata-table generation read, in canonical order.
    #[must_use]
    pub fn metadata(&self) -> &[MetadataGeneration] {
        &self.metadata
    }

    /// Return the storage owner's atomic consistency token.
    #[must_use]
    pub const fn consistency_token(&self) -> ConsistencyToken {
        self.consistency_token
    }
}

/// Canonical read set for one imaging run.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationReadSet {
    sources: Vec<MeasurementSetReadAccess>,
}

impl ObservationReadSet {
    /// Return source read sets in canonical MeasurementSet identity order.
    #[must_use]
    pub fn sources(&self) -> &[MeasurementSetReadAccess] {
        &self.sources
    }
}

/// Exact selected `MODEL_DATA` cells staged for one MeasurementSet.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelColumnWriteAccess {
    measurement_set: MeasurementSetIdentity,
    selection: ObservationSelection,
    expected_consistency_token: ConsistencyToken,
    precondition: ModelColumnPrecondition,
    disposition: ModelColumnWriteDisposition,
}

impl ModelColumnWriteAccess {
    /// Return the location-independent source identity.
    #[must_use]
    pub const fn measurement_set(&self) -> MeasurementSetIdentity {
        self.measurement_set
    }

    /// Return the exact selected cells replaced by this write.
    #[must_use]
    pub const fn selection(&self) -> &ObservationSelection {
        &self.selection
    }

    /// Return the only MAIN column this access may replace.
    #[must_use]
    pub const fn column(&self) -> MsColumnKind {
        MsColumnKind::ModelData
    }

    /// Return the source consistency token that must still hold at commit.
    #[must_use]
    pub const fn expected_consistency_token(&self) -> ConsistencyToken {
        self.expected_consistency_token
    }

    /// Return the exact prior `MODEL_DATA` state required at commit.
    #[must_use]
    pub const fn precondition(&self) -> ModelColumnPrecondition {
        self.precondition
    }

    /// Return whether this write updates an existing column or creates it fully initialized.
    #[must_use]
    pub const fn disposition(&self) -> ModelColumnWriteDisposition {
        self.disposition
    }
}

/// Canonical MeasurementSet write set for one imaging run.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationWriteSet {
    model_columns: Vec<ModelColumnWriteAccess>,
}

impl ObservationWriteSet {
    /// Return staged `MODEL_DATA` writes in canonical MeasurementSet order.
    #[must_use]
    pub fn model_columns(&self) -> &[ModelColumnWriteAccess] {
        &self.model_columns
    }
}

/// Snapshot-bound read/write contract consumed by planning and execution.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationTransactionContract {
    transaction_id: ObservationTransactionId,
    observation_snapshot_id: ObservationSnapshotId,
    read_set: ObservationReadSet,
    write_set: ObservationWriteSet,
}

impl ObservationTransactionContract {
    /// Return the canonical identity of this exact read/write contract.
    #[must_use]
    pub const fn transaction_id(&self) -> ObservationTransactionId {
        self.transaction_id
    }

    /// Return the immutable observation snapshot this contract accesses.
    #[must_use]
    pub const fn observation_snapshot_id(&self) -> ObservationSnapshotId {
        self.observation_snapshot_id
    }

    /// Return the complete MeasurementSet read set.
    #[must_use]
    pub const fn read_set(&self) -> &ObservationReadSet {
        &self.read_set
    }

    /// Return the complete MeasurementSet write set.
    #[must_use]
    pub const fn write_set(&self) -> &ObservationWriteSet {
        &self.write_set
    }
}

/// Compile exact MeasurementSet read/write sets from one immutable snapshot.
///
/// The read set is derived rather than caller-supplied, so execution cannot
/// omit a generation that participated in snapshot identity.
#[must_use]
pub fn compile_observation_transaction(
    snapshot: &ObservationSnapshot,
    requirements: ObservationTransactionRequirements,
) -> ObservationTransactionContract {
    let read_set = ObservationReadSet {
        sources: snapshot
            .sources()
            .iter()
            .map(|source| MeasurementSetReadAccess {
                measurement_set: source.identity(),
                selection: source.selection().clone(),
                column_generations: source.generations().columns().generations().to_vec(),
                metadata: source.generations().metadata_generations().to_vec(),
                consistency_token: source.generations().consistency_token(),
            })
            .collect(),
    };
    let model_columns = match requirements.model_column_write {
        ModelColumnWrite::Disabled => Vec::new(),
        ModelColumnWrite::SelectedRows => snapshot
            .sources()
            .iter()
            .map(|source| {
                let precondition = source.generations().model_column().into();
                let disposition = match precondition {
                    ModelColumnPrecondition::Absent => {
                        ModelColumnWriteDisposition::CreateAndInitializeAllRows {
                            row_count: source.selection().rows().source_row_count(),
                            initialization: ModelColumnInitialization::Zero,
                        }
                    }
                    ModelColumnPrecondition::Generation(_) => {
                        ModelColumnWriteDisposition::ReplaceSelectedCells
                    }
                };
                ModelColumnWriteAccess {
                    measurement_set: source.identity(),
                    selection: source.selection().clone(),
                    expected_consistency_token: source.generations().consistency_token(),
                    precondition,
                    disposition,
                }
            })
            .collect(),
    };
    ObservationTransactionContract {
        transaction_id: canonical_transaction_id(snapshot.snapshot_id(), requirements),
        observation_snapshot_id: snapshot.snapshot_id(),
        read_set,
        write_set: ObservationWriteSet { model_columns },
    }
}

impl From<ModelColumnState> for ModelColumnPrecondition {
    fn from(state: ModelColumnState) -> Self {
        match state {
            ModelColumnState::Absent => Self::Absent,
            ModelColumnState::Present(generation) => Self::Generation(generation),
        }
    }
}

fn canonical_transaction_id(
    snapshot_id: ObservationSnapshotId,
    requirements: ObservationTransactionRequirements,
) -> ObservationTransactionId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(OBSERVATION_TRANSACTION_IDENTITY_DOMAIN);
    encoder.u32(OBSERVATION_TRANSACTION_IDENTITY_VERSION);
    encoder.identity(snapshot_id.identity());
    encoder.u8(match requirements.model_column_write {
        ModelColumnWrite::Disabled => 0,
        ModelColumnWrite::SelectedRows => 1,
    });
    ObservationTransactionId(LogicalIdentity::from_sha256(encoder.finish()))
}
