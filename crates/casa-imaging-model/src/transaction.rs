// SPDX-License-Identifier: LGPL-3.0-or-later

//! MeasurementSet access and bounded-side-effect contracts for imaging.

use std::{fmt, sync::Arc};

use crate::compiled_problem::CanonicalEncoder;
use crate::{
    LogicalIdentity, MsColumnKind, ObservationSelection, ObservationSnapshot,
    ObservationSnapshotId, SelectedColumns, SequentialContinuumTransform, SpectralWindowSelection,
};
use thiserror::Error;

const OBSERVATION_TRANSACTION_IDENTITY_DOMAIN: &[u8] = b"casa-rs-observation-transaction";
const OBSERVATION_TRANSACTION_IDENTITY_VERSION: u32 = 3;

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

/// Whether a compiled imaging run persists transformed observations to `CORRECTED_DATA`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrectedDataWrite {
    /// Leave `CORRECTED_DATA` unchanged.
    Disabled,
    /// Replace only output-role selected cells in an existing owner-tracked column.
    SelectedOutputRows,
}

/// User-visible side-effect requirements compiled against one observation snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationTransactionRequirements {
    model_column_write: ModelColumnWrite,
    corrected_data_write: CorrectedDataWrite,
}

impl ObservationTransactionRequirements {
    /// Construct the complete MeasurementSet side-effect requirement.
    #[must_use]
    pub const fn new(model_column_write: ModelColumnWrite) -> Self {
        Self {
            model_column_write,
            corrected_data_write: CorrectedDataWrite::Disabled,
        }
    }

    /// Return the requested `MODEL_DATA` behavior.
    #[must_use]
    pub const fn model_column_write(self) -> ModelColumnWrite {
        self.model_column_write
    }

    /// Add optional in-place persistence of transformed output-role observations.
    #[must_use]
    pub const fn with_corrected_data_write(mut self, write: CorrectedDataWrite) -> Self {
        self.corrected_data_write = write;
        self
    }

    /// Return the requested `CORRECTED_DATA` behavior.
    #[must_use]
    pub const fn corrected_data_write(self) -> CorrectedDataWrite {
        self.corrected_data_write
    }
}

/// Exact immutable MeasurementSet data consumed from one snapshot source.
#[derive(Debug, Clone, PartialEq)]
pub struct MeasurementSetReadAccess {
    measurement_set: usize,
    selection: Arc<ObservationSelection>,
    columns: SelectedColumns,
}

impl MeasurementSetReadAccess {
    /// Return the MeasurementSet's position in the snapshot (its request order).
    #[must_use]
    pub const fn measurement_set(&self) -> usize {
        self.measurement_set
    }

    /// Return the exact selected rows, channels, and correlations.
    #[must_use]
    pub fn selection(&self) -> &ObservationSelection {
        &self.selection
    }

    /// Return the visibility, flag, and weight columns read.
    #[must_use]
    pub const fn selected_columns(&self) -> SelectedColumns {
        self.columns
    }
}

/// Canonical read set for one imaging run.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationReadSet {
    sources: Vec<MeasurementSetReadAccess>,
}

impl ObservationReadSet {
    /// Return source read sets in snapshot order.
    #[must_use]
    pub fn sources(&self) -> &[MeasurementSetReadAccess] {
        &self.sources
    }
}

/// Exact selected visibility-column cells written in place for one MeasurementSet.
///
/// The writer creates `MODEL_DATA` when MAIN lacks it, as CASA does;
/// `CORRECTED_DATA` must already exist.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedVisibilityWriteAccess {
    measurement_set: usize,
    column: MsColumnKind,
    selection: Arc<ObservationSelection>,
}

impl SelectedVisibilityWriteAccess {
    /// Return the MeasurementSet's position in the snapshot (its request order).
    #[must_use]
    pub const fn measurement_set(&self) -> usize {
        self.measurement_set
    }

    /// Return the exact selected cells replaced by this write.
    #[must_use]
    pub fn selection(&self) -> &ObservationSelection {
        &self.selection
    }

    /// Return the exact MAIN visibility column this access may replace.
    #[must_use]
    pub const fn column(&self) -> MsColumnKind {
        self.column
    }
}

/// Canonical MeasurementSet write set for one imaging run.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationWriteSet {
    visibility_columns: Vec<SelectedVisibilityWriteAccess>,
}

impl ObservationWriteSet {
    /// Return in-place selected visibility writes in canonical source/column order.
    #[must_use]
    pub fn visibility_columns(&self) -> &[SelectedVisibilityWriteAccess] {
        &self.visibility_columns
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
/// The read set is derived rather than caller-supplied. A `CORRECTED_DATA`
/// write is refused here, before any work, when MAIN has no such column.
pub fn compile_observation_transaction(
    snapshot: &ObservationSnapshot,
    requirements: ObservationTransactionRequirements,
    visibility_transform: Option<&SequentialContinuumTransform>,
) -> Result<ObservationTransactionContract, ObservationTransactionCompileError> {
    let read_set = ObservationReadSet {
        sources: snapshot
            .sources()
            .iter()
            .map(|source| MeasurementSetReadAccess {
                measurement_set: source.input_ordinal(),
                selection: source.selection_arc(),
                columns: source.columns(),
            })
            .collect(),
    };
    let transform =
        match requirements.corrected_data_write {
            CorrectedDataWrite::Disabled => None,
            CorrectedDataWrite::SelectedOutputRows => Some(visibility_transform.ok_or(
                ObservationTransactionCompileError::CorrectedDataRequiresContinuumTransform,
            )?),
        };
    let mut visibility_columns = Vec::new();
    for source in snapshot.sources() {
        if requirements.model_column_write == ModelColumnWrite::SelectedRows {
            visibility_columns.push(SelectedVisibilityWriteAccess {
                measurement_set: source.input_ordinal(),
                column: MsColumnKind::ModelData,
                selection: source.selection_arc(),
            });
        }
        if let Some(transform) = transform {
            if !source.corrected_data_present() {
                return Err(ObservationTransactionCompileError::MissingCorrectedDataDestination);
            }
            visibility_columns.push(SelectedVisibilityWriteAccess {
                measurement_set: source.input_ordinal(),
                column: MsColumnKind::CorrectedData,
                selection: Arc::new(output_role_selection(source.selection(), transform)?),
            });
        }
    }
    Ok(ObservationTransactionContract {
        transaction_id: canonical_transaction_id(snapshot.snapshot_id(), requirements),
        observation_snapshot_id: snapshot.snapshot_id(),
        read_set,
        write_set: ObservationWriteSet { visibility_columns },
    })
}

fn output_role_selection(
    selected: &ObservationSelection,
    transform: &SequentialContinuumTransform,
) -> Result<ObservationSelection, ObservationTransactionCompileError> {
    let spectral_windows = selected
        .spectral_windows()
        .iter()
        .map(|selection| {
            let output_channels = selection
                .channel_indices()
                .iter()
                .copied()
                .filter(|channel| {
                    transform.rules().iter().any(|rule| {
                        rule.spectral_window_id() == selection.spectral_window_id()
                            && rule
                                .channel_use(*channel)
                                .is_some_and(|role| role.contributes_to_output())
                    })
                })
                .collect::<Vec<_>>();
            let mut output = (!output_channels.is_empty())
                .then(|| {
                    SpectralWindowSelection::new(selection.spectral_window_id(), output_channels)
                })
                .ok_or(ObservationTransactionCompileError::EmptyOutputRoleSelection)?;
            if let Some(catalog) = selection.coordinate_catalog() {
                output = output.with_coordinate_catalog(catalog.clone());
            }
            Ok(output)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ObservationSelection::new(
        selected.rows().clone(),
        selected.rows_filter().clone(),
        selected.data_descriptions().to_vec(),
        spectral_windows,
        selected.correlations().to_vec(),
    ))
}

/// Failure to compile a requested MeasurementSet side effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ObservationTransactionCompileError {
    /// Residual persistence is meaningful only for a compiled continuum transform.
    #[error("CORRECTED_DATA residual persistence requires continuum subtraction")]
    CorrectedDataRequiresContinuumTransform,
    /// The MeasurementSet has no `CORRECTED_DATA` column to write.
    #[error("CORRECTED_DATA residual persistence requires an existing CORRECTED_DATA column")]
    MissingCorrectedDataDestination,
    /// The transform must expose at least one output-role channel for each selected SPW.
    #[error("continuum transform selected no CORRECTED_DATA output-role channels")]
    EmptyOutputRoleSelection,
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
    encoder.u8(match requirements.corrected_data_write {
        CorrectedDataWrite::Disabled => 0,
        CorrectedDataWrite::SelectedOutputRows => 1,
    });
    ObservationTransactionId(LogicalIdentity::from_bytes(encoder.finish()))
}
