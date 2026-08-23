// SPDX-License-Identifier: LGPL-3.0-or-later

//! Immutable observation manifests and exact resolved selection semantics.

use std::{cmp::Ordering, collections::BTreeSet, fmt, sync::Arc};

use thiserror::Error;

use crate::compiled_problem::{
    CanonicalEncoder, LogicalIdentity, ModelStateIdentity, ReferenceDataKind,
};

const OBSERVATION_SNAPSHOT_IDENTITY_DOMAIN: &[u8] = b"casa-rs-observation-snapshot";
const OBSERVATION_SNAPSHOT_IDENTITY_VERSION: u32 = 4;
const OBSERVATION_PROVENANCE_IDENTITY_DOMAIN: &[u8] = b"casa-rs-observation-provenance";
const OBSERVATION_PROVENANCE_IDENTITY_VERSION: u32 = 1;
const SELECTED_ROW_SEQUENCE_IDENTITY_DOMAIN: &[u8] = b"casa-rs-selected-row-sequence";
const SELECTED_ROW_SEQUENCE_IDENTITY_VERSION: u32 = 2;

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

const REQUIRED_METADATA_TABLES: [MetadataTableKind; 9] = [
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

/// Stable compiler-derived identity of one immutable observation snapshot.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObservationSnapshotId(LogicalIdentity);

impl ObservationSnapshotId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = OBSERVATION_SNAPSHOT_IDENTITY_VERSION;

    /// Return the wrapped logical identity.
    #[must_use]
    pub const fn identity(self) -> LogicalIdentity {
        self.0
    }

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for ObservationSnapshotId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ObservationSnapshotId({})", self.0)
    }
}

impl fmt::Display for ObservationSnapshotId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable compiler-derived identity of snapshot origin and request provenance.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObservationProvenanceId(LogicalIdentity);

impl ObservationProvenanceId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = OBSERVATION_PROVENANCE_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for ObservationProvenanceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ObservationProvenanceId({})", self.0)
    }
}

impl fmt::Display for ObservationProvenanceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Logical identity of one MeasurementSet independent of its filesystem location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MeasurementSetIdentity(LogicalIdentity);

impl MeasurementSetIdentity {
    /// Wrap an identity supplied by the observation storage owner.
    #[must_use]
    pub const fn new(identity: LogicalIdentity) -> Self {
        Self(identity)
    }

    /// Return the wrapped logical identity.
    #[must_use]
    pub const fn identity(self) -> LogicalIdentity {
        self.0
    }
}

impl fmt::Display for MeasurementSetIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Storage-owner token proving that a source is still one consistency generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConsistencyToken(LogicalIdentity);

impl ConsistencyToken {
    /// Wrap a consistency token supplied by the observation storage owner.
    #[must_use]
    pub const fn new(identity: LogicalIdentity) -> Self {
        Self(identity)
    }

    /// Return the wrapped logical identity.
    #[must_use]
    pub const fn identity(self) -> LogicalIdentity {
        self.0
    }
}

/// A resolved non-negative MeasurementSet identifier selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdSelection {
    /// Select every identifier present in the source generation.
    All,
    /// Select exactly these identifiers.
    Only(Vec<u32>),
}

impl IdSelection {
    /// Return selected identifiers, or `None` when every identifier is selected.
    #[must_use]
    pub fn ids(&self) -> Option<&[u32]> {
        match self {
            Self::All => None,
            Self::Only(ids) => Some(ids),
        }
    }

    fn canonicalize(&mut self, selector: &'static str) -> Result<(), CompileObservationError> {
        if let Self::Only(ids) = self {
            ids.sort_unstable();
            ids.dedup();
            if ids.is_empty() {
                return Err(CompileObservationError::EmptyIdSelection { selector });
            }
        }
        Ok(())
    }
}

/// One finite scalar selection boundary.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectionBound {
    value: f64,
    inclusive: bool,
}

impl SelectionBound {
    /// Construct an inclusive boundary.
    #[must_use]
    pub const fn inclusive(value: f64) -> Self {
        Self {
            value,
            inclusive: true,
        }
    }

    /// Construct an exclusive boundary.
    #[must_use]
    pub const fn exclusive(value: f64) -> Self {
        Self {
            value,
            inclusive: false,
        }
    }

    /// Return the scalar value.
    #[must_use]
    pub const fn value(self) -> f64 {
        self.value
    }

    /// Return whether the boundary includes its exact value.
    #[must_use]
    pub const fn is_inclusive(self) -> bool {
        self.inclusive
    }

    fn canonicalize(&mut self) -> Result<(), CompileObservationError> {
        if !self.value.is_finite() {
            return Err(CompileObservationError::InvalidScalarRange);
        }
        self.value = canonical_zero(self.value);
        Ok(())
    }
}

/// One resolved interval in MeasurementSet `TIME` MJD seconds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimeRange {
    lower: Option<SelectionBound>,
    upper: Option<SelectionBound>,
}

impl TimeRange {
    /// Construct a possibly one-sided time interval.
    #[must_use]
    pub const fn new(lower: Option<SelectionBound>, upper: Option<SelectionBound>) -> Self {
        Self { lower, upper }
    }

    /// Return the lower boundary, if bounded.
    #[must_use]
    pub const fn lower(self) -> Option<SelectionBound> {
        self.lower
    }

    /// Return the upper boundary, if bounded.
    #[must_use]
    pub const fn upper(self) -> Option<SelectionBound> {
        self.upper
    }
}

/// Exact resolved time-selection union.
#[derive(Debug, Clone, PartialEq)]
pub enum TimeSelection {
    /// Select every time.
    All,
    /// Select the union of these intervals.
    Ranges(Vec<TimeRange>),
}

/// Unit in which a UV-distance predicate is evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UvDistanceUnit {
    /// Projected baseline length in metres.
    Meters,
    /// Projected baseline length divided by the row's DDID-linked SPW reference wavelength.
    Wavelengths,
}

/// One resolved UV-distance interval.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UvDistanceRange {
    lower: Option<SelectionBound>,
    upper: Option<SelectionBound>,
    unit: UvDistanceUnit,
}

impl UvDistanceRange {
    /// Construct a possibly one-sided UV-distance interval.
    #[must_use]
    pub const fn new(
        lower: Option<SelectionBound>,
        upper: Option<SelectionBound>,
        unit: UvDistanceUnit,
    ) -> Self {
        Self { lower, upper, unit }
    }

    /// Return the lower boundary, if bounded.
    #[must_use]
    pub const fn lower(self) -> Option<SelectionBound> {
        self.lower
    }

    /// Return the upper boundary, if bounded.
    #[must_use]
    pub const fn upper(self) -> Option<SelectionBound> {
        self.upper
    }

    /// Return the distance unit.
    #[must_use]
    pub const fn unit(self) -> UvDistanceUnit {
        self.unit
    }
}

/// Exact resolved UV-distance selection union.
#[derive(Debug, Clone, PartialEq)]
pub enum UvSelection {
    /// Select every projected baseline length.
    All,
    /// Select the union of these intervals.
    Ranges(Vec<UvDistanceRange>),
}

/// One unordered antenna pair admitted by a resolved baseline selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AntennaBaseline {
    first: u32,
    second: u32,
}

impl AntennaBaseline {
    /// Construct an unordered antenna pair.
    #[must_use]
    pub const fn new(first: u32, second: u32) -> Self {
        if first <= second {
            Self { first, second }
        } else {
            Self {
                first: second,
                second: first,
            }
        }
    }

    /// Return the canonical ascending antenna pair.
    #[must_use]
    pub const fn antennas(self) -> [u32; 2] {
        [self.first, self.second]
    }
}

/// Exact resolved antenna/baseline selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AntennaSelection {
    /// Select every baseline.
    All,
    /// Select exactly these unordered antenna pairs.
    Only(Vec<AntennaBaseline>),
}

/// One intent pattern resolved to an exact `STATE_ID` and `OBS_MODE` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedIntent {
    state_id: u32,
    observation_mode: String,
}

impl ResolvedIntent {
    /// Construct one resolved intent.
    #[must_use]
    pub const fn new(state_id: u32, observation_mode: String) -> Self {
        Self {
            state_id,
            observation_mode,
        }
    }

    /// Return the selected `STATE_ID`.
    #[must_use]
    pub const fn state_id(&self) -> u32 {
        self.state_id
    }

    /// Return the exact selected `OBS_MODE` metadata value.
    #[must_use]
    pub fn observation_mode(&self) -> &str {
        &self.observation_mode
    }
}

/// Exact resolved scan-intent selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntentSelection {
    /// Select every state/intent.
    All,
    /// Select exactly these resolved state rows.
    Only(Vec<ResolvedIntent>),
}

/// Exact row-level predicates applied conjunctively to one MeasurementSet.
#[derive(Debug, Clone, PartialEq)]
pub struct RowSelection {
    fields: IdSelection,
    times: TimeSelection,
    uv_distances: UvSelection,
    antennas: AntennaSelection,
    scans: IdSelection,
    observations: IdSelection,
    intents: IntentSelection,
    arrays: IdSelection,
}

impl RowSelection {
    /// Construct exact resolved row predicates.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        fields: IdSelection,
        times: TimeSelection,
        uv_distances: UvSelection,
        antennas: AntennaSelection,
        scans: IdSelection,
        observations: IdSelection,
        intents: IntentSelection,
        arrays: IdSelection,
    ) -> Self {
        Self {
            fields,
            times,
            uv_distances,
            antennas,
            scans,
            observations,
            intents,
            arrays,
        }
    }

    /// Return resolved field identifiers.
    #[must_use]
    pub const fn fields(&self) -> &IdSelection {
        &self.fields
    }

    /// Return exact MJD-second interval semantics.
    #[must_use]
    pub const fn times(&self) -> &TimeSelection {
        &self.times
    }

    /// Return exact UV-distance interval semantics.
    #[must_use]
    pub const fn uv_distances(&self) -> &UvSelection {
        &self.uv_distances
    }

    /// Return exact resolved baselines.
    #[must_use]
    pub const fn antennas(&self) -> &AntennaSelection {
        &self.antennas
    }

    /// Return resolved scan identifiers.
    #[must_use]
    pub const fn scans(&self) -> &IdSelection {
        &self.scans
    }

    /// Return resolved observation identifiers.
    #[must_use]
    pub const fn observations(&self) -> &IdSelection {
        &self.observations
    }

    /// Return resolved scan intents.
    #[must_use]
    pub const fn intents(&self) -> &IntentSelection {
        &self.intents
    }

    /// Return resolved array identifiers.
    #[must_use]
    pub const fn arrays(&self) -> &IdSelection {
        &self.arrays
    }

    fn canonicalize(&mut self) -> Result<(), CompileObservationError> {
        self.fields.canonicalize("field")?;
        self.scans.canonicalize("scan")?;
        self.observations.canonicalize("observation")?;
        self.arrays.canonicalize("array")?;
        canonicalize_time_selection(&mut self.times)?;
        canonicalize_uv_selection(&mut self.uv_distances)?;
        match &mut self.antennas {
            AntennaSelection::All => {}
            AntennaSelection::Only(baselines) => {
                baselines.sort_unstable();
                baselines.dedup();
                if baselines.is_empty() {
                    return Err(CompileObservationError::EmptyBaselineSelection);
                }
            }
        }
        match &mut self.intents {
            IntentSelection::All => {}
            IntentSelection::Only(intents) => {
                for intent in intents.iter() {
                    if intent.observation_mode.trim().is_empty() {
                        return Err(CompileObservationError::InvalidIntent);
                    }
                }
                intents.sort_unstable_by_key(|intent| intent.state_id);
                if intents.is_empty() {
                    return Err(CompileObservationError::InvalidIntent);
                }
                if intents
                    .windows(2)
                    .any(|pair| pair[0].state_id == pair[1].state_id)
                {
                    return Err(CompileObservationError::DuplicateIntentState);
                }
            }
        }
        Ok(())
    }
}

/// Stable storage-owner-reproducible identity of selected MAIN row coordinates.
///
/// There is deliberately no constructor from raw digest bytes. The identity
/// can only be minted by validating and hashing the exact ordered row sequence.
///
/// ```compile_fail
/// use casa_imaging_model::SelectedRowSequenceId;
///
/// let _ = SelectedRowSequenceId::from_sha256([0; 32]);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SelectedRowSequenceId(LogicalIdentity);

impl SelectedRowSequenceId {
    /// Identity schema version used by the canonical encoder.
    pub const SCHEMA_VERSION: u32 = SELECTED_ROW_SEQUENCE_IDENTITY_VERSION;

    /// Return the exact SHA-256 digest.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0.as_bytes()
    }
}

/// One selected physical MAIN row and its resolved `DATA_DESC_ID`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SelectedMainRow {
    physical_row: u64,
    data_description_id: u32,
}

impl SelectedMainRow {
    /// Construct one resolved MAIN row coordinate.
    #[must_use]
    pub const fn new(physical_row: u64, data_description_id: u32) -> Self {
        Self {
            physical_row,
            data_description_id,
        }
    }

    /// Return the physical MAIN row index.
    #[must_use]
    pub const fn physical_row(self) -> u64 {
        self.physical_row
    }

    /// Return the resolved `DATA_DESC_ID` read from the MAIN row.
    #[must_use]
    pub const fn data_description_id(self) -> u32 {
        self.data_description_id
    }
}

impl fmt::Debug for SelectedRowSequenceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "SelectedRowSequenceId({})", self.0)
    }
}

impl fmt::Display for SelectedRowSequenceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Failure to create a canonical selected-row sequence manifest.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SelectedRowSequenceError {
    /// The iterator's declared or observed row count cannot be represented canonically.
    #[error("selected physical MAIN row count exceeds the canonical u64 domain")]
    RowCountOverflow,
    /// An iterator yielded a different number of rows than its exact-size contract declared.
    #[error(
        "selected physical MAIN row iterator declared {declared_row_count} rows but yielded {observed_row_count}"
    )]
    DeclaredRowCountMismatch {
        /// Count declared by the exact-size iterator.
        declared_row_count: u64,
        /// Count actually observed while hashing rows.
        observed_row_count: u64,
    },
    /// A physical row lies outside the captured MAIN row population.
    #[error("physical MAIN row {row} lies outside source row count {source_row_count}")]
    PhysicalRowOutOfRange {
        /// Invalid physical row index.
        row: u64,
        /// Captured MAIN row population.
        source_row_count: u64,
    },
    /// A physical row appeared more than once.
    #[error("physical MAIN row {row} appears more than once")]
    DuplicatePhysicalRow {
        /// Duplicate physical row index.
        row: u64,
    },
    /// Physical rows were not supplied in ascending MAIN order.
    #[error("physical MAIN row {row} follows later row {previous_row}")]
    DescendingPhysicalRow {
        /// Prior physical row index.
        previous_row: u64,
        /// Descending physical row index.
        row: u64,
    },
}

/// Failure to reproduce a compiled selected-row manifest from a storage replay.
#[derive(Debug, Error)]
pub enum SelectedRowManifestValidationError<E>
where
    E: std::error::Error + 'static,
{
    /// The retained storage source could not produce the next row coordinate.
    #[error("selected MAIN row replay failed")]
    Source(#[source] E),
    /// The replay produced an intrinsically invalid row sequence.
    #[error(transparent)]
    InvalidSequence(#[from] SelectedRowSequenceError),
    /// The replay was valid but did not reproduce the compiled compact manifest.
    #[error(
        "selected MAIN row replay produced {observed_row_count} rows with sequence {observed_sequence_id}, expected {expected_row_count} rows with sequence {expected_sequence_id}"
    )]
    ManifestMismatch {
        /// Compiled selected-row count.
        expected_row_count: u64,
        /// Replayed selected-row count.
        observed_row_count: u64,
        /// Compiled row/DDID sequence identity.
        expected_sequence_id: SelectedRowSequenceId,
        /// Replayed row/DDID sequence identity.
        observed_sequence_id: SelectedRowSequenceId,
    },
}

/// Exact manifest of selected MAIN row/DDID coordinates in physical row order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedRows {
    source_row_count: u64,
    ordered_main_rows: Arc<[SelectedMainRow]>,
    sequence_id: SelectedRowSequenceId,
    used_data_description_ids: Arc<[u32]>,
}

impl SelectedRows {
    /// Validate, retain, and identify the canonical MAIN row/DDID coordinates.
    ///
    /// Validation reports the first encountered invalid row.
    /// Each row is checked for range, then adjacent duplication, then descending
    /// order. A non-adjacent repetition necessarily encounters descending order
    /// first, so validation needs no separate selection-sized duplicate set.
    pub fn from_ordered_main_rows<I>(
        source_row_count: u64,
        rows: I,
    ) -> Result<Self, SelectedRowSequenceError>
    where
        I: IntoIterator<Item = SelectedMainRow>,
        I::IntoIter: ExactSizeIterator,
    {
        let rows = rows.into_iter();
        let selected_row_count =
            u64::try_from(rows.len()).map_err(|_| SelectedRowSequenceError::RowCountOverflow)?;
        let mut accumulator =
            SelectedRowSequenceAccumulator::new(source_row_count, selected_row_count);
        let mut ordered_main_rows = Vec::with_capacity(rows.len());
        for row in rows {
            accumulator.push(row)?;
            ordered_main_rows.push(row);
        }
        let (observed_row_count, sequence_id, used_data_description_ids) = accumulator.finish();
        if observed_row_count != selected_row_count {
            return Err(SelectedRowSequenceError::DeclaredRowCountMismatch {
                declared_row_count: selected_row_count,
                observed_row_count,
            });
        }
        Ok(Self {
            source_row_count,
            ordered_main_rows: ordered_main_rows.into(),
            sequence_id,
            used_data_description_ids: used_data_description_ids.into(),
        })
    }

    /// Validate a fallible storage replay against this exact row/DDID manifest.
    ///
    /// The replay is inspected in one pass and is not retained. This operation
    /// establishes only equality with the compiled row manifest; it does not
    /// mint selected-observation completion or storage-consistency authority.
    pub fn validate_ordered_main_rows<I, E>(
        &self,
        rows: I,
    ) -> Result<(), SelectedRowManifestValidationError<E>>
    where
        I: IntoIterator<Item = Result<SelectedMainRow, E>>,
        E: std::error::Error + 'static,
    {
        let mut accumulator =
            SelectedRowSequenceAccumulator::new(self.source_row_count, self.selected_row_count());
        for row in rows {
            accumulator
                .push(row.map_err(SelectedRowManifestValidationError::Source)?)
                .map_err(SelectedRowManifestValidationError::InvalidSequence)?;
        }
        let (observed_row_count, observed_sequence_id, _) = accumulator.finish();
        if observed_row_count != self.selected_row_count()
            || observed_sequence_id != self.sequence_id
        {
            return Err(SelectedRowManifestValidationError::ManifestMismatch {
                expected_row_count: self.selected_row_count(),
                observed_row_count,
                expected_sequence_id: self.sequence_id,
                observed_sequence_id,
            });
        }
        Ok(())
    }

    /// Return the source MAIN row count at capture.
    #[must_use]
    pub const fn source_row_count(&self) -> u64 {
        self.source_row_count
    }

    /// Return the number of selected MAIN rows.
    #[must_use]
    pub fn selected_row_count(&self) -> u64 {
        u64::try_from(self.ordered_main_rows.len())
            .expect("validated selected MAIN row count fits u64")
    }

    /// Return exact selected MAIN row/DDID coordinates in canonical physical order.
    #[must_use]
    pub fn ordered_main_rows(&self) -> &[SelectedMainRow] {
        &self.ordered_main_rows
    }

    /// Return heap bytes owned by the shared canonical row/DDID manifest.
    ///
    /// Cloning [`SelectedRows`] shares these immutable allocations. A retained
    /// storage owner can therefore charge this value once instead of assuming
    /// every transaction and commitment clone owns another full row vector.
    #[must_use]
    pub fn retained_manifest_bytes(&self) -> Option<usize> {
        self.additional_retained_manifest_bytes(std::iter::empty::<&Self>())
    }

    fn additional_retained_manifest_bytes<'a>(
        &self,
        already_accounted: impl IntoIterator<Item = &'a Self>,
    ) -> Option<usize> {
        let mut ordered_main_rows_accounted = false;
        let mut used_data_description_ids_accounted = false;
        for rows in already_accounted {
            ordered_main_rows_accounted |=
                Arc::ptr_eq(&self.ordered_main_rows, &rows.ordered_main_rows);
            used_data_description_ids_accounted |= Arc::ptr_eq(
                &self.used_data_description_ids,
                &rows.used_data_description_ids,
            );
        }
        let mut bytes = 0_usize;
        if !ordered_main_rows_accounted {
            bytes = bytes.checked_add(2 * size_of::<usize>())?.checked_add(
                self.ordered_main_rows
                    .len()
                    .checked_mul(size_of::<SelectedMainRow>())?,
            )?;
        }
        if !used_data_description_ids_accounted {
            bytes = bytes.checked_add(2 * size_of::<usize>())?.checked_add(
                self.used_data_description_ids
                    .len()
                    .checked_mul(size_of::<u32>())?,
            )?;
        }
        Some(bytes)
    }

    /// Return the canonical identity of selected row/DDID coordinates in MAIN order.
    #[must_use]
    pub const fn sequence_id(&self) -> SelectedRowSequenceId {
        self.sequence_id
    }

    fn used_data_description_ids(&self) -> &[u32] {
        &self.used_data_description_ids
    }
}

pub(crate) struct SelectedRowSequenceAccumulator {
    source_row_count: u64,
    encoder: CanonicalEncoder,
    observed_row_count: u64,
    previous_row: Option<u64>,
    used_data_description_ids: BTreeSet<u32>,
}

impl SelectedRowSequenceAccumulator {
    pub(crate) fn new(source_row_count: u64, expected_row_count: u64) -> Self {
        let mut encoder = CanonicalEncoder::new();
        encoder.bytes(SELECTED_ROW_SEQUENCE_IDENTITY_DOMAIN);
        encoder.u32(SELECTED_ROW_SEQUENCE_IDENTITY_VERSION);
        encoder.u64(expected_row_count);
        Self {
            source_row_count,
            encoder,
            observed_row_count: 0,
            previous_row: None,
            used_data_description_ids: BTreeSet::new(),
        }
    }

    pub(crate) fn push(
        &mut self,
        selected: SelectedMainRow,
    ) -> Result<(), SelectedRowSequenceError> {
        let row = selected.physical_row;
        self.observed_row_count = self
            .observed_row_count
            .checked_add(1)
            .ok_or(SelectedRowSequenceError::RowCountOverflow)?;
        if row >= self.source_row_count {
            return Err(SelectedRowSequenceError::PhysicalRowOutOfRange {
                row,
                source_row_count: self.source_row_count,
            });
        }
        if self.previous_row == Some(row) {
            return Err(SelectedRowSequenceError::DuplicatePhysicalRow { row });
        }
        if let Some(previous_row) = self.previous_row
            && row < previous_row
        {
            return Err(SelectedRowSequenceError::DescendingPhysicalRow { previous_row, row });
        }
        self.encoder.u64(row);
        self.encoder.u32(selected.data_description_id);
        self.used_data_description_ids
            .insert(selected.data_description_id);
        self.previous_row = Some(row);
        Ok(())
    }

    pub(crate) fn finish(self) -> (u64, SelectedRowSequenceId, Vec<u32>) {
        (
            self.observed_row_count,
            SelectedRowSequenceId(LogicalIdentity::from_sha256(self.encoder.finish())),
            self.used_data_description_ids.into_iter().collect(),
        )
    }

    pub(crate) const fn observed_row_count(&self) -> u64 {
        self.observed_row_count
    }
}

/// One selected `DATA_DESCRIPTION` row and its exact coordinate pairing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DataDescriptionSelection {
    data_description_id: u32,
    spectral_window_id: u32,
    polarization_id: u32,
}

impl DataDescriptionSelection {
    /// Construct one resolved `DATA_DESCRIPTION` member.
    #[must_use]
    pub const fn new(
        data_description_id: u32,
        spectral_window_id: u32,
        polarization_id: u32,
    ) -> Self {
        Self {
            data_description_id,
            spectral_window_id,
            polarization_id,
        }
    }

    /// Return the MAIN `DATA_DESC_ID` and `DATA_DESCRIPTION` row index.
    #[must_use]
    pub const fn data_description_id(self) -> u32 {
        self.data_description_id
    }

    /// Return the referenced `SPECTRAL_WINDOW_ID`.
    #[must_use]
    pub const fn spectral_window_id(self) -> u32 {
        self.spectral_window_id
    }

    /// Return the referenced `POLARIZATION_ID`.
    #[must_use]
    pub const fn polarization_id(self) -> u32 {
        self.polarization_id
    }
}

/// Exact channels selected from one spectral window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpectralWindowSelection {
    spectral_window_id: u32,
    channel_indices: Vec<u32>,
}

impl SpectralWindowSelection {
    /// Construct a resolved spectral-window/channel selection.
    #[must_use]
    pub const fn new(spectral_window_id: u32, channel_indices: Vec<u32>) -> Self {
        Self {
            spectral_window_id,
            channel_indices,
        }
    }

    /// Return the `SPECTRAL_WINDOW_ID`.
    #[must_use]
    pub const fn spectral_window_id(&self) -> u32 {
        self.spectral_window_id
    }

    /// Return exact selected native channel indices in canonical order.
    #[must_use]
    pub fn channel_indices(&self) -> &[u32] {
        &self.channel_indices
    }
}

/// Standard MeasurementSet correlation coordinate.
///
/// This covers every defined non-`Undefined` casacore `StokesTypes` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CorrelationType {
    /// Stokes I.
    StokesI,
    /// Stokes Q.
    StokesQ,
    /// Stokes U.
    StokesU,
    /// Stokes V.
    StokesV,
    /// Circular-feed RR.
    CircularRr,
    /// Circular-feed RL.
    CircularRl,
    /// Circular-feed LR.
    CircularLr,
    /// Circular-feed LL.
    CircularLl,
    /// Linear-feed XX.
    LinearXx,
    /// Linear-feed XY.
    LinearXy,
    /// Linear-feed YX.
    LinearYx,
    /// Linear-feed YY.
    LinearYy,
    /// Mixed-feed RX.
    MixedRx,
    /// Mixed-feed RY.
    MixedRy,
    /// Mixed-feed LX.
    MixedLx,
    /// Mixed-feed LY.
    MixedLy,
    /// Mixed-feed XR.
    MixedXr,
    /// Mixed-feed XL.
    MixedXl,
    /// Mixed-feed YR.
    MixedYr,
    /// Mixed-feed YL.
    MixedYl,
    /// General quasi-orthogonal PP.
    QuasiOrthogonalPp,
    /// General quasi-orthogonal PQ.
    QuasiOrthogonalPq,
    /// General quasi-orthogonal QP.
    QuasiOrthogonalQp,
    /// General quasi-orthogonal QQ.
    QuasiOrthogonalQq,
    /// Single-dish right-circular polarization.
    RightCircular,
    /// Single-dish left-circular polarization.
    LeftCircular,
    /// Single-dish linear polarization.
    Linear,
    /// Total polarized intensity.
    PolarizedIntensity,
    /// Linearly polarized intensity.
    LinearPolarizedIntensity,
    /// Total polarized fraction.
    FractionalPolarizedIntensity,
    /// Linear polarized fraction.
    FractionalLinearPolarizedIntensity,
    /// Linear polarization angle in radians.
    PolarizationAngle,
}

/// One selected correlation array coordinate and its MeasurementSet meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CorrelationProduct {
    correlation_index: u32,
    correlation_type: CorrelationType,
}

impl CorrelationProduct {
    /// Construct one selected correlation coordinate.
    #[must_use]
    pub const fn new(correlation_index: u32, correlation_type: CorrelationType) -> Self {
        Self {
            correlation_index,
            correlation_type,
        }
    }

    /// Return the zero-based array coordinate.
    #[must_use]
    pub const fn correlation_index(self) -> u32 {
        self.correlation_index
    }

    /// Return the exact MeasurementSet correlation meaning.
    #[must_use]
    pub const fn correlation_type(self) -> CorrelationType {
        self.correlation_type
    }
}

/// Exact selected products for one `POLARIZATION_ID`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelationSelection {
    polarization_id: u32,
    products: Vec<CorrelationProduct>,
}

impl CorrelationSelection {
    /// Construct one resolved polarization/correlation selection.
    #[must_use]
    pub const fn new(polarization_id: u32, products: Vec<CorrelationProduct>) -> Self {
        Self {
            polarization_id,
            products,
        }
    }

    /// Return the selected `POLARIZATION_ID`.
    #[must_use]
    pub const fn polarization_id(&self) -> u32 {
        self.polarization_id
    }

    /// Return exact selected correlation coordinates in canonical array order.
    #[must_use]
    pub fn products(&self) -> &[CorrelationProduct] {
        &self.products
    }
}

/// Complete resolved logical selection for one MeasurementSet.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationSelection {
    rows: SelectedRows,
    rows_filter: RowSelection,
    data_descriptions: Vec<DataDescriptionSelection>,
    spectral_windows: Vec<SpectralWindowSelection>,
    correlations: Vec<CorrelationSelection>,
}

impl ObservationSelection {
    /// Construct exact row, channel, and correlation semantics.
    #[must_use]
    pub const fn new(
        rows: SelectedRows,
        rows_filter: RowSelection,
        data_descriptions: Vec<DataDescriptionSelection>,
        spectral_windows: Vec<SpectralWindowSelection>,
        correlations: Vec<CorrelationSelection>,
    ) -> Self {
        Self {
            rows,
            rows_filter,
            data_descriptions,
            spectral_windows,
            correlations,
        }
    }

    /// Return the compact selected-row manifest.
    #[must_use]
    pub const fn rows(&self) -> &SelectedRows {
        &self.rows
    }

    /// Return exact row-level selection predicates.
    #[must_use]
    pub const fn rows_filter(&self) -> &RowSelection {
        &self.rows_filter
    }

    /// Return the exact selected `DATA_DESCRIPTION` coordinate catalog.
    #[must_use]
    pub fn data_descriptions(&self) -> &[DataDescriptionSelection] {
        &self.data_descriptions
    }

    /// Return exact channel selections in canonical spectral-window order.
    #[must_use]
    pub fn spectral_windows(&self) -> &[SpectralWindowSelection] {
        &self.spectral_windows
    }

    /// Return exact selected correlation coordinates by polarization setup.
    #[must_use]
    pub fn correlations(&self) -> &[CorrelationSelection] {
        &self.correlations
    }

    /// Return bytes owned by this shared immutable selection manifest.
    ///
    /// The projection includes the Arc allocation, exact row/DDID manifest,
    /// resolved predicate vectors and strings, and selected coordinate catalogs.
    #[must_use]
    pub fn retained_manifest_bytes(&self) -> Option<usize> {
        let mut bytes = size_of::<Self>().checked_add(2 * size_of::<usize>())?;
        bytes = bytes.checked_add(self.rows.retained_manifest_bytes()?)?;
        let id_selection_bytes = |selection: &IdSelection| match selection {
            IdSelection::All => Some(0),
            IdSelection::Only(ids) => ids.capacity().checked_mul(size_of::<u32>()),
        };
        bytes = bytes
            .checked_add(id_selection_bytes(&self.rows_filter.fields)?)?
            .checked_add(id_selection_bytes(&self.rows_filter.scans)?)?
            .checked_add(id_selection_bytes(&self.rows_filter.observations)?)?
            .checked_add(id_selection_bytes(&self.rows_filter.arrays)?)?;
        if let TimeSelection::Ranges(ranges) = &self.rows_filter.times {
            bytes = bytes.checked_add(ranges.capacity().checked_mul(size_of::<TimeRange>())?)?;
        }
        if let UvSelection::Ranges(ranges) = &self.rows_filter.uv_distances {
            bytes = bytes.checked_add(
                ranges
                    .capacity()
                    .checked_mul(size_of::<UvDistanceRange>())?,
            )?;
        }
        if let AntennaSelection::Only(baselines) = &self.rows_filter.antennas {
            bytes = bytes.checked_add(
                baselines
                    .capacity()
                    .checked_mul(size_of::<AntennaBaseline>())?,
            )?;
        }
        if let IntentSelection::Only(intents) = &self.rows_filter.intents {
            bytes = bytes.checked_add(
                intents
                    .capacity()
                    .checked_mul(size_of::<ResolvedIntent>())?,
            )?;
            for intent in intents {
                bytes = bytes.checked_add(intent.observation_mode.capacity())?;
            }
        }
        bytes = bytes.checked_add(
            self.data_descriptions
                .capacity()
                .checked_mul(size_of::<DataDescriptionSelection>())?,
        )?;
        bytes = bytes.checked_add(
            self.spectral_windows
                .capacity()
                .checked_mul(size_of::<SpectralWindowSelection>())?,
        )?;
        for selection in &self.spectral_windows {
            bytes = bytes.checked_add(
                selection
                    .channel_indices
                    .capacity()
                    .checked_mul(size_of::<u32>())?,
            )?;
        }
        bytes = bytes.checked_add(
            self.correlations
                .capacity()
                .checked_mul(size_of::<CorrelationSelection>())?,
        )?;
        for selection in &self.correlations {
            bytes = bytes.checked_add(
                selection
                    .products
                    .capacity()
                    .checked_mul(size_of::<CorrelationProduct>())?,
            )?;
        }
        Some(bytes)
    }

    fn canonicalize(&mut self) -> Result<(), CompileObservationError> {
        self.rows_filter.canonicalize()?;
        self.data_descriptions
            .sort_unstable_by_key(|selection| selection.data_description_id);
        if let Some(data_description_id) = self.data_descriptions.windows(2).find_map(|pair| {
            (pair[0].data_description_id == pair[1].data_description_id)
                .then_some(pair[0].data_description_id)
        }) {
            return Err(CompileObservationError::DuplicateDataDescription {
                data_description_id,
            });
        }
        if let Some(data_description_id) = self
            .data_descriptions
            .iter()
            .map(|selection| selection.data_description_id)
            .find(|&data_description_id| i32::try_from(data_description_id).is_err())
        {
            return Err(
                CompileObservationError::DataDescriptionIdOutsideMainDomain {
                    data_description_id,
                },
            );
        }
        if self.rows.selected_row_count() > 0 && self.data_descriptions.is_empty() {
            return Err(CompileObservationError::NoDataDescriptionSelection);
        }
        if let Some(&data_description_id) =
            self.rows.used_data_description_ids().iter().find(|&&used| {
                !self
                    .data_descriptions
                    .iter()
                    .any(|selection| selection.data_description_id == used)
            })
        {
            return Err(CompileObservationError::SelectedRowDataDescriptionMissing {
                data_description_id,
            });
        }
        for selection in &mut self.spectral_windows {
            selection.channel_indices.sort_unstable();
            selection.channel_indices.dedup();
            if selection.channel_indices.is_empty() {
                return Err(CompileObservationError::EmptySpectralWindowSelection {
                    spectral_window_id: selection.spectral_window_id,
                });
            }
        }
        self.spectral_windows
            .sort_unstable_by_key(|selection| selection.spectral_window_id);
        if self.rows.selected_row_count() > 0 && self.spectral_windows.is_empty() {
            return Err(CompileObservationError::NoSpectralWindowSelection);
        }
        if let Some(spectral_window_id) = self.spectral_windows.windows(2).find_map(|pair| {
            (pair[0].spectral_window_id == pair[1].spectral_window_id)
                .then_some(pair[0].spectral_window_id)
        }) {
            return Err(CompileObservationError::DuplicateSpectralWindow { spectral_window_id });
        }

        for selection in &mut self.correlations {
            selection.products.sort_unstable();
            if selection.products.is_empty() {
                return Err(CompileObservationError::EmptyCorrelationSelection {
                    polarization_id: selection.polarization_id,
                });
            }
            if selection
                .products
                .windows(2)
                .any(|pair| pair[0].correlation_index == pair[1].correlation_index)
            {
                return Err(CompileObservationError::DuplicateCorrelationIndex {
                    polarization_id: selection.polarization_id,
                });
            }
            let types = selection
                .products
                .iter()
                .map(|product| product.correlation_type)
                .collect::<BTreeSet<_>>();
            if types.len() != selection.products.len() {
                return Err(CompileObservationError::DuplicateCorrelationType {
                    polarization_id: selection.polarization_id,
                });
            }
        }
        self.correlations
            .sort_unstable_by_key(|selection| selection.polarization_id);
        if self.rows.selected_row_count() > 0 && self.correlations.is_empty() {
            return Err(CompileObservationError::NoCorrelationSelection);
        }
        if let Some(polarization_id) = self.correlations.windows(2).find_map(|pair| {
            (pair[0].polarization_id == pair[1].polarization_id).then_some(pair[0].polarization_id)
        }) {
            return Err(CompileObservationError::DuplicatePolarization { polarization_id });
        }
        for data_description in &self.data_descriptions {
            if self
                .spectral_windows
                .binary_search_by_key(&data_description.spectral_window_id, |selection| {
                    selection.spectral_window_id
                })
                .is_err()
            {
                return Err(
                    CompileObservationError::UnknownDataDescriptionSpectralWindow {
                        data_description_id: data_description.data_description_id,
                        spectral_window_id: data_description.spectral_window_id,
                    },
                );
            }
            if self
                .correlations
                .binary_search_by_key(&data_description.polarization_id, |selection| {
                    selection.polarization_id
                })
                .is_err()
            {
                return Err(
                    CompileObservationError::UnknownDataDescriptionPolarization {
                        data_description_id: data_description.data_description_id,
                        polarization_id: data_description.polarization_id,
                    },
                );
            }
        }
        if let Some(spectral_window_id) = self.spectral_windows.iter().find_map(|selection| {
            (!self.data_descriptions.iter().any(|data_description| {
                data_description.spectral_window_id == selection.spectral_window_id
            }))
            .then_some(selection.spectral_window_id)
        }) {
            return Err(CompileObservationError::OrphanSpectralWindowSelection {
                spectral_window_id,
            });
        }
        if let Some(polarization_id) = self.correlations.iter().find_map(|selection| {
            (!self.data_descriptions.iter().any(|data_description| {
                data_description.polarization_id == selection.polarization_id
            }))
            .then_some(selection.polarization_id)
        }) {
            return Err(CompileObservationError::OrphanCorrelationSelection { polarization_id });
        }
        Ok(())
    }
}

/// MeasurementSet MAIN column whose generation is snapshot-bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MsColumnKind {
    /// Raw observed complex visibility data.
    Data,
    /// Calibrated complex visibility data.
    CorrectedData,
    /// Real-valued visibility data.
    FloatData,
    /// Per-correlation/channel flags.
    Flag,
    /// Whole-row flag.
    FlagRow,
    /// Per-correlation row weight.
    Weight,
    /// Per-correlation/channel weight spectrum.
    WeightSpectrum,
    /// UVW coordinates.
    Uvw,
    /// Visibility time.
    Time,
    /// Visibility time centroid.
    TimeCentroid,
    /// Integration interval.
    Interval,
    /// Effective exposure.
    Exposure,
    /// Field foreign key.
    FieldId,
    /// Data-description foreign key.
    DataDescriptionId,
    /// First antenna foreign key.
    Antenna1,
    /// Second antenna foreign key.
    Antenna2,
    /// First feed foreign key.
    Feed1,
    /// Second feed foreign key.
    Feed2,
    /// Scan identifier.
    ScanNumber,
    /// State/intent foreign key.
    StateId,
    /// Observation foreign key.
    ObservationId,
    /// Array identifier.
    ArrayId,
    /// Input model visibilities when the initial model is column-backed.
    ModelData,
}

/// Exact visibility source column; no fallback is implied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VisibilityColumn {
    /// MAIN `DATA`.
    Data,
    /// MAIN `CORRECTED_DATA`.
    CorrectedData,
    /// MAIN `FLOAT_DATA`.
    FloatData,
}

impl VisibilityColumn {
    const fn column_kind(self) -> MsColumnKind {
        match self {
            Self::Data => MsColumnKind::Data,
            Self::CorrectedData => MsColumnKind::CorrectedData,
            Self::FloatData => MsColumnKind::FloatData,
        }
    }
}

/// Exact flag combination applied to every selected sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FlagPolicy {
    /// A sample is excluded when either its `FLAG` cell or `FLAG_ROW` is true.
    FlagOrFlagRow,
}

/// Exact input weight column; no existence-based fallback is implied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WeightColumn {
    /// MAIN `WEIGHT`, broadcast over selected channels.
    Weight,
    /// MAIN `WEIGHT_SPECTRUM`, evaluated per selected channel.
    WeightSpectrum,
}

impl WeightColumn {
    const fn column_kind(self) -> MsColumnKind {
        match self {
            Self::Weight => MsColumnKind::Weight,
            Self::WeightSpectrum => MsColumnKind::WeightSpectrum,
        }
    }
}

/// Consistency generation of one selected MAIN column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnGeneration {
    kind: MsColumnKind,
    identity: LogicalIdentity,
}

/// Exact existence and generation of the optional MAIN `MODEL_DATA` column.
///
/// This state is captured independently of [`SelectedColumns`] because an
/// existing output-only model column is a write precondition without being an
/// observation input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelColumnState {
    /// `MODEL_DATA` did not exist when the snapshot was captured.
    Absent,
    /// `MODEL_DATA` existed with this storage-owner generation.
    Present(LogicalIdentity),
}

impl ColumnGeneration {
    /// Construct one column generation binding.
    #[must_use]
    pub const fn new(kind: MsColumnKind, identity: LogicalIdentity) -> Self {
        Self { kind, identity }
    }

    /// Return the bound MAIN column.
    #[must_use]
    pub const fn kind(self) -> MsColumnKind {
        self.kind
    }

    /// Return the storage-owner generation identity.
    #[must_use]
    pub const fn identity(self) -> LogicalIdentity {
        self.identity
    }
}

/// Exact data, flag, weight, and coordinate-column contract for one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedColumns {
    visibility: VisibilityColumn,
    flags: FlagPolicy,
    weights: WeightColumn,
    generations: Vec<ColumnGeneration>,
}

impl SelectedColumns {
    /// Construct selected column semantics and their captured generations.
    #[must_use]
    pub const fn new(
        visibility: VisibilityColumn,
        flags: FlagPolicy,
        weights: WeightColumn,
        generations: Vec<ColumnGeneration>,
    ) -> Self {
        Self {
            visibility,
            flags,
            weights,
            generations,
        }
    }

    /// Return the exact visibility source column.
    #[must_use]
    pub const fn visibility(&self) -> VisibilityColumn {
        self.visibility
    }

    /// Return the exact flag combination.
    #[must_use]
    pub const fn flags(&self) -> FlagPolicy {
        self.flags
    }

    /// Return the exact weight source column.
    #[must_use]
    pub const fn weights(&self) -> WeightColumn {
        self.weights
    }

    /// Return all captured column generations in canonical column order.
    #[must_use]
    pub fn generations(&self) -> &[ColumnGeneration] {
        &self.generations
    }

    /// Return the captured generation for one column.
    #[must_use]
    pub fn generation(&self, kind: MsColumnKind) -> Option<LogicalIdentity> {
        self.generations
            .iter()
            .find_map(|generation| (generation.kind == kind).then_some(generation.identity))
    }

    fn canonicalize(&mut self) -> Result<(), CompileObservationError> {
        self.generations
            .sort_unstable_by_key(|generation| generation.kind);
        if let Some(column) = self
            .generations
            .windows(2)
            .find_map(|pair| (pair[0].kind == pair[1].kind).then_some(pair[0].kind))
        {
            return Err(CompileObservationError::DuplicateColumnGeneration { column });
        }
        for generation in &self.generations {
            require_identity(generation.identity, "column generation")?;
        }
        let required = [
            self.visibility.column_kind(),
            MsColumnKind::Flag,
            MsColumnKind::FlagRow,
            self.weights.column_kind(),
        ];
        for column in required.into_iter().chain(REQUIRED_COORDINATE_COLUMNS) {
            if self.generation(column).is_none() {
                return Err(CompileObservationError::MissingColumnGeneration { column });
            }
        }
        Ok(())
    }
}

/// MeasurementSet metadata table whose generation affects selected samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetadataTableKind {
    /// `ANTENNA`.
    Antenna,
    /// `DATA_DESCRIPTION`.
    DataDescription,
    /// `DOPPLER`.
    Doppler,
    /// `FEED`.
    Feed,
    /// `FIELD`.
    Field,
    /// `FREQ_OFFSET`.
    FrequencyOffset,
    /// `OBSERVATION`.
    Observation,
    /// `POINTING`.
    Pointing,
    /// `POLARIZATION`.
    Polarization,
    /// `SOURCE`.
    Source,
    /// `SPECTRAL_WINDOW`.
    SpectralWindow,
    /// `STATE`.
    State,
    /// `SYSCAL`.
    SysCal,
    /// `WEATHER`.
    Weather,
}

/// Consistency generation of one relevant MeasurementSet metadata table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataGeneration {
    kind: MetadataTableKind,
    identity: LogicalIdentity,
}

impl MetadataGeneration {
    /// Construct one metadata generation binding.
    #[must_use]
    pub const fn new(kind: MetadataTableKind, identity: LogicalIdentity) -> Self {
        Self { kind, identity }
    }

    /// Return the metadata table family.
    #[must_use]
    pub const fn kind(self) -> MetadataTableKind {
        self.kind
    }

    /// Return the storage-owner generation identity.
    #[must_use]
    pub const fn identity(self) -> LogicalIdentity {
        self.identity
    }
}

/// All source-local generations required to consume a snapshot consistently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceGenerations {
    consistency_token: ConsistencyToken,
    columns: SelectedColumns,
    metadata: Vec<MetadataGeneration>,
    model_column: ModelColumnState,
}

impl SourceGenerations {
    /// Construct source consistency, column, metadata, and optional model-column generations.
    #[must_use]
    pub const fn new(
        consistency_token: ConsistencyToken,
        columns: SelectedColumns,
        metadata: Vec<MetadataGeneration>,
        model_column: ModelColumnState,
    ) -> Self {
        Self {
            consistency_token,
            columns,
            metadata,
            model_column,
        }
    }

    /// Return the atomic source consistency token.
    #[must_use]
    pub const fn consistency_token(&self) -> ConsistencyToken {
        self.consistency_token
    }

    /// Return selected column semantics and generations.
    #[must_use]
    pub const fn columns(&self) -> &SelectedColumns {
        &self.columns
    }

    /// Return metadata generations in canonical table-family order.
    #[must_use]
    pub fn metadata_generations(&self) -> &[MetadataGeneration] {
        &self.metadata
    }

    /// Return the captured generation for one metadata table.
    #[must_use]
    pub fn metadata(&self, kind: MetadataTableKind) -> Option<LogicalIdentity> {
        self.metadata
            .iter()
            .find_map(|generation| (generation.kind == kind).then_some(generation.identity))
    }

    /// Return the captured existence and generation of `MODEL_DATA`.
    #[must_use]
    pub const fn model_column(&self) -> ModelColumnState {
        self.model_column
    }

    /// Return bytes owned by this shared immutable generation manifest.
    #[must_use]
    pub fn retained_manifest_bytes(&self) -> Option<usize> {
        size_of::<Self>()
            .checked_add(2 * size_of::<usize>())?
            .checked_add(self.retained_owned_heap_bytes()?)
    }

    fn retained_owned_heap_bytes(&self) -> Option<usize> {
        self.columns
            .generations
            .capacity()
            .checked_mul(size_of::<ColumnGeneration>())?
            .checked_add(
                self.metadata
                    .capacity()
                    .checked_mul(size_of::<MetadataGeneration>())?,
            )
    }

    fn canonicalize(&mut self) -> Result<(), CompileObservationError> {
        require_identity(self.consistency_token.0, "source consistency token")?;
        self.columns.canonicalize()?;
        self.metadata
            .sort_unstable_by_key(|generation| generation.kind);
        if let Some(table) = self
            .metadata
            .windows(2)
            .find_map(|pair| (pair[0].kind == pair[1].kind).then_some(pair[0].kind))
        {
            return Err(CompileObservationError::DuplicateMetadataGeneration { table });
        }
        for generation in &self.metadata {
            require_identity(generation.identity, "metadata generation")?;
        }
        match self.model_column {
            ModelColumnState::Absent => {
                if self.columns.generation(MsColumnKind::ModelData).is_some() {
                    return Err(CompileObservationError::InconsistentModelColumnState);
                }
            }
            ModelColumnState::Present(generation) => {
                require_identity(generation, "MODEL_DATA generation")?;
                if self
                    .columns
                    .generation(MsColumnKind::ModelData)
                    .is_some_and(|selected| selected != generation)
                {
                    return Err(CompileObservationError::InconsistentModelColumnState);
                }
            }
        }
        for table in REQUIRED_METADATA_TABLES {
            if self.metadata(table).is_none() {
                return Err(CompileObservationError::MissingMetadataGeneration { table });
            }
        }
        Ok(())
    }
}

/// Non-scientific origin facts retained separately from snapshot content identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationSourceProvenance {
    locator: String,
    selection_request: LogicalIdentity,
}

impl ObservationSourceProvenance {
    /// Construct source-location and original-request provenance.
    #[must_use]
    pub const fn new(locator: String, selection_request: LogicalIdentity) -> Self {
        Self {
            locator,
            selection_request,
        }
    }

    /// Return the source locator captured for diagnostics and reproduction.
    #[must_use]
    pub fn locator(&self) -> &str {
        &self.locator
    }

    /// Return the identity of the original selection request.
    #[must_use]
    pub const fn selection_request_identity(&self) -> LogicalIdentity {
        self.selection_request
    }

    /// Return heap bytes retained by the source locator string.
    #[must_use]
    pub fn retained_locator_bytes(&self) -> usize {
        self.locator.capacity()
    }

    fn validate(&self) -> Result<(), CompileObservationError> {
        if self.locator.trim().is_empty() {
            return Err(CompileObservationError::EmptySourceLocator);
        }
        require_identity(self.selection_request, "selection request")
    }
}

/// Uncompiled source manifest supplied by the observation adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationSourceInput {
    identity: MeasurementSetIdentity,
    provenance: ObservationSourceProvenance,
    selection: ObservationSelection,
    generations: SourceGenerations,
}

impl ObservationSourceInput {
    /// Construct one logical MeasurementSet source manifest.
    #[must_use]
    pub const fn new(
        identity: MeasurementSetIdentity,
        provenance: ObservationSourceProvenance,
        selection: ObservationSelection,
        generations: SourceGenerations,
    ) -> Self {
        Self {
            identity,
            provenance,
            selection,
            generations,
        }
    }
}

/// One validated immutable MeasurementSet source in a compiled snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationSource {
    identity: MeasurementSetIdentity,
    provenance: ObservationSourceProvenance,
    input_ordinal: usize,
    selection: Arc<ObservationSelection>,
    generations: Arc<SourceGenerations>,
}

impl ObservationSource {
    /// Return the location-independent source identity.
    #[must_use]
    pub const fn identity(&self) -> MeasurementSetIdentity {
        self.identity
    }

    /// Return source origin and original selection-request provenance.
    #[must_use]
    pub const fn provenance(&self) -> &ObservationSourceProvenance {
        &self.provenance
    }

    /// Return this source's ordinal in the original multi-MS request.
    #[must_use]
    pub const fn input_ordinal(&self) -> usize {
        self.input_ordinal
    }

    /// Return exact resolved selection semantics.
    #[must_use]
    pub fn selection(&self) -> &ObservationSelection {
        &self.selection
    }

    /// Return all bound source generations.
    #[must_use]
    pub fn generations(&self) -> &SourceGenerations {
        &self.generations
    }

    pub(crate) fn selection_arc(&self) -> Arc<ObservationSelection> {
        Arc::clone(&self.selection)
    }

    pub(crate) fn generations_arc(&self) -> Arc<SourceGenerations> {
        Arc::clone(&self.generations)
    }
}

/// Uncompiled manifest for one immutable logical observation snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationSnapshotInput {
    sources: Vec<ObservationSourceInput>,
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
    model: ModelStateIdentity,
}

impl ObservationSnapshotInput {
    /// Construct a multi-MS snapshot manifest and its external input identities.
    #[must_use]
    pub const fn new(
        sources: Vec<ObservationSourceInput>,
        reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
        model: ModelStateIdentity,
    ) -> Self {
        Self {
            sources,
            reference_data,
            model,
        }
    }
}

/// Immutable logical identity and consistency generation of selected observation data.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationSnapshot {
    snapshot_id: ObservationSnapshotId,
    provenance_id: ObservationProvenanceId,
    sources: Vec<ObservationSource>,
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
    model: ModelStateIdentity,
}

// Compilation rejects non-finite ranges, so snapshot equality is reflexive.
impl Eq for ObservationSnapshot {}

impl ObservationSnapshot {
    /// Return the canonical content identity of the complete logical manifest.
    #[must_use]
    pub const fn snapshot_id(&self) -> ObservationSnapshotId {
        self.snapshot_id
    }

    /// Return the identity of source locators and original request order.
    #[must_use]
    pub const fn provenance_id(&self) -> ObservationProvenanceId {
        self.provenance_id
    }

    /// Return sources in canonical content-identity order.
    #[must_use]
    pub fn sources(&self) -> &[ObservationSource] {
        &self.sources
    }

    /// Return external reference identities in canonical family order.
    #[must_use]
    pub fn reference_data(&self) -> &[(ReferenceDataKind, LogicalIdentity)] {
        &self.reference_data
    }

    /// Return the exact aggregate initial-model identity.
    #[must_use]
    pub const fn model(&self) -> ModelStateIdentity {
        self.model
    }

    /// Reject any current state that could mix generations with this snapshot.
    pub fn validate_consistency(
        &self,
        mut current: ObservationState,
    ) -> Result<(), ObservationConsistencyError> {
        current.canonicalize()?;
        if self.sources.len() != current.sources.len()
            || self
                .sources
                .iter()
                .zip(&current.sources)
                .any(|(expected, actual)| expected.identity != actual.identity)
        {
            return Err(ObservationConsistencyError::SourceSetChanged);
        }
        for (expected, actual) in self.sources.iter().zip(&current.sources) {
            let source = expected.identity;
            if expected.selection.rows != actual.selected_rows {
                return Err(ObservationConsistencyError::SelectedRowsChanged {
                    measurement_set: source,
                });
            }
            if expected.generations.columns.visibility != actual.generations.columns.visibility
                || expected.generations.columns.flags != actual.generations.columns.flags
                || expected.generations.columns.weights != actual.generations.columns.weights
            {
                return Err(ObservationConsistencyError::SelectedColumnContractChanged {
                    measurement_set: source,
                });
            }
            compare_columns(
                source,
                &expected.generations.columns,
                &actual.generations.columns,
            )?;
            if expected.generations.model_column != actual.generations.model_column {
                return Err(ObservationConsistencyError::ModelColumnStateChanged {
                    measurement_set: source,
                });
            }
            compare_metadata(source, &expected.generations, &actual.generations)?;
            if expected.generations.consistency_token != actual.generations.consistency_token {
                return Err(ObservationConsistencyError::ConsistencyTokenChanged {
                    measurement_set: source,
                });
            }
        }
        if self.reference_data != current.reference_data {
            return Err(ObservationConsistencyError::ReferenceDataChanged);
        }
        if self.model != current.model {
            return Err(ObservationConsistencyError::ModelStateChanged);
        }
        Ok(())
    }
}

/// Current source facts probed before or at a declared safe streaming boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationSourceState {
    identity: MeasurementSetIdentity,
    selected_rows: SelectedRows,
    generations: SourceGenerations,
}

impl ObservationSourceState {
    /// Construct one current source-state probe.
    #[must_use]
    pub const fn new(
        identity: MeasurementSetIdentity,
        selected_rows: SelectedRows,
        generations: SourceGenerations,
    ) -> Self {
        Self {
            identity,
            selected_rows,
            generations,
        }
    }

    /// Return the source identity.
    #[must_use]
    pub const fn identity(&self) -> MeasurementSetIdentity {
        self.identity
    }

    /// Return the current selected-row manifest.
    #[must_use]
    pub const fn selected_rows(&self) -> &SelectedRows {
        &self.selected_rows
    }

    /// Return current column, metadata, and consistency generations.
    #[must_use]
    pub const fn generations(&self) -> &SourceGenerations {
        &self.generations
    }

    /// Return heap bytes additionally retained by this current-state graph.
    ///
    /// Inline state belongs to its outer owner allocation. Generation vectors
    /// are owned uniquely by this value, while selected-row manifests can be
    /// shared with compiled sources or earlier state probes and are omitted
    /// when their allocation is already accounted by one of those roots.
    #[must_use]
    pub fn additional_retained_heap_bytes<'a>(
        &self,
        already_accounted_rows: impl IntoIterator<Item = &'a SelectedRows>,
    ) -> Option<usize> {
        self.selected_rows
            .additional_retained_manifest_bytes(already_accounted_rows)?
            .checked_add(self.generations.retained_owned_heap_bytes()?)
    }
}

/// Current input state supplied for fail-closed snapshot consistency validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationState {
    sources: Vec<ObservationSourceState>,
    reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
    model: ModelStateIdentity,
}

impl ObservationState {
    /// Construct a current input-state probe.
    #[must_use]
    pub const fn new(
        sources: Vec<ObservationSourceState>,
        reference_data: Vec<(ReferenceDataKind, LogicalIdentity)>,
        model: ModelStateIdentity,
    ) -> Self {
        Self {
            sources,
            reference_data,
            model,
        }
    }

    fn canonicalize(&mut self) -> Result<(), ObservationConsistencyError> {
        self.sources.sort_unstable_by_key(|source| source.identity);
        if self
            .sources
            .windows(2)
            .any(|pair| pair[0].identity == pair[1].identity)
        {
            return Err(ObservationConsistencyError::InvalidCurrentState {
                reason: "duplicate MeasurementSet identity",
            });
        }
        for source in &mut self.sources {
            canonicalize_current_generations(&mut source.generations)?;
        }
        self.reference_data.sort_unstable_by_key(|(kind, _)| *kind);
        if self
            .reference_data
            .windows(2)
            .any(|pair| pair[0].0 == pair[1].0)
        {
            return Err(ObservationConsistencyError::InvalidCurrentState {
                reason: "duplicate reference-data family",
            });
        }
        Ok(())
    }
}

/// Failure to compile an authoritative observation snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CompileObservationError {
    /// No MeasurementSet source was supplied.
    #[error("an observation snapshot requires at least one MeasurementSet source")]
    NoSources,
    /// No source contributed any selected row.
    #[error("observation selection contains no rows")]
    EmptySelection,
    /// A caller-supplied identity used the reserved all-zero sentinel.
    #[error("{scope} identity is not established")]
    UnidentifiedInput {
        /// Stable identity scope.
        scope: &'static str,
    },
    /// One logical MeasurementSet was supplied more than once.
    #[error("duplicate MeasurementSet identity {measurement_set}")]
    DuplicateSource {
        /// Duplicate source identity.
        measurement_set: MeasurementSetIdentity,
    },
    /// Source provenance did not name an origin.
    #[error("observation source locator is empty")]
    EmptySourceLocator,
    /// An explicit resolved identifier set was empty.
    #[error("explicit {selector} selection is empty")]
    EmptyIdSelection {
        /// Selector family.
        selector: &'static str,
    },
    /// A time or UV range was unbounded, non-finite, or inverted.
    #[error("selection range is unbounded, non-finite, or inverted")]
    InvalidScalarRange,
    /// A UV-distance range used a negative bound.
    #[error("UV-distance selection bounds must be non-negative")]
    NegativeUvDistance,
    /// Explicit baseline semantics contained no baseline.
    #[error("explicit antenna selection contains no baseline")]
    EmptyBaselineSelection,
    /// Resolved intent metadata was empty.
    #[error("resolved intent must contain a non-empty OBS_MODE value")]
    InvalidIntent,
    /// More than one intent mapping was supplied for one state.
    #[error("more than one resolved intent was supplied for one STATE_ID")]
    DuplicateIntentState,
    /// One `DATA_DESC_ID` appeared more than once.
    #[error("duplicate DATA_DESCRIPTION selection for {data_description_id}")]
    DuplicateDataDescription {
        /// Duplicate `DATA_DESC_ID`.
        data_description_id: u32,
    },
    /// A selected `DATA_DESC_ID` cannot be represented by the MeasurementSet MAIN `Int` column.
    #[error("DATA_DESCRIPTION selection {data_description_id} exceeds the MAIN Int domain")]
    DataDescriptionIdOutsideMainDomain {
        /// Unrepresentable `DATA_DESC_ID`.
        data_description_id: u32,
    },
    /// A selected MAIN row named a `DATA_DESC_ID` absent from the compiled catalog.
    #[error("selected MAIN row references uncatalogued DATA_DESCRIPTION {data_description_id}")]
    SelectedRowDataDescriptionMissing {
        /// Uncatalogued `DATA_DESC_ID`.
        data_description_id: u32,
    },
    /// Selected rows had no exact `DATA_DESCRIPTION` coordinate catalog.
    #[error("selected rows require at least one DATA_DESCRIPTION selection")]
    NoDataDescriptionSelection,
    /// A selected `DATA_DESCRIPTION` row referenced an unselected spectral window.
    #[error(
        "DATA_DESCRIPTION {data_description_id} references unselected spectral window {spectral_window_id}"
    )]
    UnknownDataDescriptionSpectralWindow {
        /// Selected `DATA_DESC_ID`.
        data_description_id: u32,
        /// Unselected `SPECTRAL_WINDOW_ID`.
        spectral_window_id: u32,
    },
    /// A selected `DATA_DESCRIPTION` row referenced an unselected polarization setup.
    #[error(
        "DATA_DESCRIPTION {data_description_id} references unselected polarization {polarization_id}"
    )]
    UnknownDataDescriptionPolarization {
        /// Selected `DATA_DESC_ID`.
        data_description_id: u32,
        /// Unselected `POLARIZATION_ID`.
        polarization_id: u32,
    },
    /// No spectral-window/channel semantics were supplied.
    #[error("selected rows require at least one spectral-window/channel selection")]
    NoSpectralWindowSelection,
    /// One selected spectral window had no DDID or channel coordinate.
    #[error("spectral window {spectral_window_id} has an empty DDID or channel selection")]
    EmptySpectralWindowSelection {
        /// Spectral-window identifier.
        spectral_window_id: u32,
    },
    /// One spectral window appeared more than once instead of being resolved once.
    #[error("duplicate spectral-window selection for {spectral_window_id}")]
    DuplicateSpectralWindow {
        /// Spectral-window identifier.
        spectral_window_id: u32,
    },
    /// A selected spectral-window projection had no selected `DATA_DESCRIPTION` member.
    #[error("spectral window {spectral_window_id} is not referenced by DATA_DESCRIPTION")]
    OrphanSpectralWindowSelection {
        /// Orphan `SPECTRAL_WINDOW_ID`.
        spectral_window_id: u32,
    },
    /// No correlation-coordinate semantics were supplied.
    #[error("selected rows require at least one polarization/correlation selection")]
    NoCorrelationSelection,
    /// One polarization setup selected no correlations.
    #[error("polarization {polarization_id} has an empty correlation selection")]
    EmptyCorrelationSelection {
        /// Polarization identifier.
        polarization_id: u32,
    },
    /// One array coordinate had conflicting correlation meanings.
    #[error("polarization {polarization_id} repeats a correlation array index")]
    DuplicateCorrelationIndex {
        /// Polarization identifier.
        polarization_id: u32,
    },
    /// One correlation meaning appeared at multiple coordinates.
    #[error("polarization {polarization_id} repeats a correlation type")]
    DuplicateCorrelationType {
        /// Polarization identifier.
        polarization_id: u32,
    },
    /// One polarization setup appeared more than once.
    #[error("duplicate correlation selection for polarization {polarization_id}")]
    DuplicatePolarization {
        /// Polarization identifier.
        polarization_id: u32,
    },
    /// A selected correlation projection had no selected `DATA_DESCRIPTION` member.
    #[error("polarization {polarization_id} is not referenced by DATA_DESCRIPTION")]
    OrphanCorrelationSelection {
        /// Orphan `POLARIZATION_ID`.
        polarization_id: u32,
    },
    /// More than one generation was supplied for a MAIN column.
    #[error("duplicate generation for MAIN column {column:?}")]
    DuplicateColumnGeneration {
        /// Duplicated column.
        column: MsColumnKind,
    },
    /// A required selected or coordinate column generation was absent.
    #[error("missing generation for MAIN column {column:?}")]
    MissingColumnGeneration {
        /// Missing column.
        column: MsColumnKind,
    },
    /// The optional `MODEL_DATA` state contradicted its selected generation.
    #[error("MODEL_DATA state conflicts with its selected column generation")]
    InconsistentModelColumnState,
    /// More than one generation was supplied for a metadata table.
    #[error("duplicate generation for metadata table {table:?}")]
    DuplicateMetadataGeneration {
        /// Duplicated table.
        table: MetadataTableKind,
    },
    /// A required coordinate/selection metadata generation was absent.
    #[error("missing generation for metadata table {table:?}")]
    MissingMetadataGeneration {
        /// Missing table.
        table: MetadataTableKind,
    },
    /// More than one identity was supplied for one external reference-data family.
    #[error("duplicate reference-data identity for {kind:?}")]
    DuplicateReferenceData {
        /// Duplicated reference-data family.
        kind: ReferenceDataKind,
    },
}

/// Failure to prove that current inputs still match a compiled snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ObservationConsistencyError {
    /// A current state probe was internally ambiguous.
    #[error("invalid current observation state: {reason}")]
    InvalidCurrentState {
        /// Stable reason.
        reason: &'static str,
    },
    /// The MeasurementSet source set changed.
    #[error("MeasurementSet source set changed")]
    SourceSetChanged,
    /// Re-evaluating selection changed selected rows or their canonical order.
    #[error("selected rows changed for source {measurement_set}")]
    SelectedRowsChanged {
        /// Mutated source.
        measurement_set: MeasurementSetIdentity,
    },
    /// Visibility, flag, or weight interpretation changed.
    #[error("selected column contract changed for source {measurement_set}")]
    SelectedColumnContractChanged {
        /// Mutated source.
        measurement_set: MeasurementSetIdentity,
    },
    /// The set of generation-tracked MAIN columns changed.
    #[error("tracked MAIN column set changed for source {measurement_set}")]
    ColumnSetChanged {
        /// Mutated source.
        measurement_set: MeasurementSetIdentity,
    },
    /// One selected MAIN column changed generation.
    #[error("MAIN column {column:?} changed generation for source {measurement_set}")]
    ColumnGenerationChanged {
        /// Mutated source.
        measurement_set: MeasurementSetIdentity,
        /// Mutated column.
        column: MsColumnKind,
    },
    /// The optional `MODEL_DATA` column appeared, disappeared, or changed generation.
    #[error("MODEL_DATA state changed for source {measurement_set}")]
    ModelColumnStateChanged {
        /// Mutated source.
        measurement_set: MeasurementSetIdentity,
    },
    /// The set of generation-tracked metadata tables changed.
    #[error("tracked metadata table set changed for source {measurement_set}")]
    MetadataSetChanged {
        /// Mutated source.
        measurement_set: MeasurementSetIdentity,
    },
    /// One metadata table changed generation.
    #[error("metadata table {table:?} changed generation for source {measurement_set}")]
    MetadataGenerationChanged {
        /// Mutated source.
        measurement_set: MeasurementSetIdentity,
        /// Mutated metadata table.
        table: MetadataTableKind,
    },
    /// The storage owner's atomic consistency token changed.
    #[error("consistency token changed for source {measurement_set}")]
    ConsistencyTokenChanged {
        /// Mutated source.
        measurement_set: MeasurementSetIdentity,
    },
    /// External measures, observatory, ephemeris, or instrument data changed.
    #[error("external reference-data generation changed")]
    ReferenceDataChanged,
    /// The aggregate input-model generation changed.
    #[error("input model state changed")]
    ModelStateChanged,
}

/// Compile and validate one immutable logical observation snapshot.
///
/// The compiler hashes only compact manifests and owner-supplied generations;
/// it never reads or retains visibility, flag, weight, or coordinate arrays.
pub fn compile_observation(
    input: ObservationSnapshotInput,
) -> Result<ObservationSnapshot, CompileObservationError> {
    if input.sources.is_empty() {
        return Err(CompileObservationError::NoSources);
    }
    let mut sources = Vec::with_capacity(input.sources.len());
    let mut has_selected_rows = false;
    for (input_ordinal, source) in input.sources.into_iter().enumerate() {
        require_identity(source.identity.0, "MeasurementSet")?;
        let provenance = source.provenance;
        provenance.validate()?;
        let mut selection = source.selection;
        selection.canonicalize()?;
        has_selected_rows |= selection.rows.selected_row_count() > 0;
        let mut generations = source.generations;
        generations.canonicalize()?;
        sources.push(ObservationSource {
            identity: source.identity,
            provenance,
            input_ordinal,
            selection: Arc::new(selection),
            generations: Arc::new(generations),
        });
    }
    if !has_selected_rows {
        return Err(CompileObservationError::EmptySelection);
    }
    sources.sort_unstable_by_key(|source| source.identity);
    if let Some(source) = sources
        .windows(2)
        .find_map(|pair| (pair[0].identity == pair[1].identity).then_some(pair[0].identity))
    {
        return Err(CompileObservationError::DuplicateSource {
            measurement_set: source,
        });
    }

    let mut reference_data = input.reference_data;
    reference_data.sort_unstable_by_key(|(kind, _)| *kind);
    if let Some(kind) = reference_data
        .windows(2)
        .find_map(|pair| (pair[0].0 == pair[1].0).then_some(pair[0].0))
    {
        return Err(CompileObservationError::DuplicateReferenceData { kind });
    }
    for (_, identity) in &reference_data {
        require_identity(*identity, "reference data")?;
    }
    match input.model {
        ModelStateIdentity::Empty => {}
        ModelStateIdentity::Seed(identity) | ModelStateIdentity::Generation(identity) => {
            require_identity(identity, "input model")?;
        }
    }

    let snapshot_id = canonical_snapshot_id(&sources, &reference_data, input.model);
    let provenance_id = canonical_provenance_id(snapshot_id, &sources);
    Ok(ObservationSnapshot {
        snapshot_id,
        provenance_id,
        sources,
        reference_data,
        model: input.model,
    })
}

fn compare_columns(
    source: MeasurementSetIdentity,
    expected: &SelectedColumns,
    actual: &SelectedColumns,
) -> Result<(), ObservationConsistencyError> {
    if expected.generations.len() != actual.generations.len()
        || expected
            .generations
            .iter()
            .zip(&actual.generations)
            .any(|(left, right)| left.kind != right.kind)
    {
        return Err(ObservationConsistencyError::ColumnSetChanged {
            measurement_set: source,
        });
    }
    for (left, right) in expected.generations.iter().zip(&actual.generations) {
        if left.identity != right.identity {
            return Err(ObservationConsistencyError::ColumnGenerationChanged {
                measurement_set: source,
                column: left.kind,
            });
        }
    }
    Ok(())
}

fn compare_metadata(
    source: MeasurementSetIdentity,
    expected: &SourceGenerations,
    actual: &SourceGenerations,
) -> Result<(), ObservationConsistencyError> {
    if expected.metadata.len() != actual.metadata.len()
        || expected
            .metadata
            .iter()
            .zip(&actual.metadata)
            .any(|(left, right)| left.kind != right.kind)
    {
        return Err(ObservationConsistencyError::MetadataSetChanged {
            measurement_set: source,
        });
    }
    for (left, right) in expected.metadata.iter().zip(&actual.metadata) {
        if left.identity != right.identity {
            return Err(ObservationConsistencyError::MetadataGenerationChanged {
                measurement_set: source,
                table: left.kind,
            });
        }
    }
    Ok(())
}

fn canonicalize_current_generations(
    generations: &mut SourceGenerations,
) -> Result<(), ObservationConsistencyError> {
    generations
        .columns
        .generations
        .sort_unstable_by_key(|generation| generation.kind);
    if generations
        .columns
        .generations
        .windows(2)
        .any(|pair| pair[0].kind == pair[1].kind)
    {
        return Err(ObservationConsistencyError::InvalidCurrentState {
            reason: "duplicate MAIN column generation",
        });
    }
    generations
        .metadata
        .sort_unstable_by_key(|generation| generation.kind);
    if generations
        .metadata
        .windows(2)
        .any(|pair| pair[0].kind == pair[1].kind)
    {
        return Err(ObservationConsistencyError::InvalidCurrentState {
            reason: "duplicate metadata generation",
        });
    }
    Ok(())
}

fn canonicalize_time_selection(
    selection: &mut TimeSelection,
) -> Result<(), CompileObservationError> {
    if let TimeSelection::Ranges(ranges) = selection {
        if ranges.is_empty() {
            return Err(CompileObservationError::InvalidScalarRange);
        }
        for range in ranges.iter_mut() {
            canonicalize_bounds(&mut range.lower, &mut range.upper, false)?;
        }
        merge_time_ranges(ranges);
    }
    Ok(())
}

fn canonicalize_uv_selection(selection: &mut UvSelection) -> Result<(), CompileObservationError> {
    if let UvSelection::Ranges(ranges) = selection {
        if ranges.is_empty() {
            return Err(CompileObservationError::InvalidScalarRange);
        }
        for range in ranges.iter_mut() {
            canonicalize_bounds(&mut range.lower, &mut range.upper, true)?;
        }
        ranges.sort_unstable_by(compare_uv_ranges);
        let mut merged: Vec<UvDistanceRange> = Vec::with_capacity(ranges.len());
        for range in ranges.drain(..) {
            if let Some(last) = merged.last_mut()
                && last.unit == range.unit
                && ranges_overlap(last.upper, range.lower)
            {
                last.upper = union_upper(last.upper, range.upper);
            } else {
                merged.push(range);
            }
        }
        *ranges = merged;
    }
    Ok(())
}

fn canonicalize_bounds(
    lower: &mut Option<SelectionBound>,
    upper: &mut Option<SelectionBound>,
    non_negative: bool,
) -> Result<(), CompileObservationError> {
    if lower.is_none() && upper.is_none() {
        return Err(CompileObservationError::InvalidScalarRange);
    }
    if let Some(bound) = lower {
        bound.canonicalize()?;
        if non_negative && bound.value < 0.0 {
            return Err(CompileObservationError::NegativeUvDistance);
        }
    }
    if let Some(bound) = upper {
        bound.canonicalize()?;
        if non_negative && bound.value < 0.0 {
            return Err(CompileObservationError::NegativeUvDistance);
        }
    }
    if let (Some(lower), Some(upper)) = (*lower, *upper)
        && (lower.value > upper.value
            || (lower.value == upper.value && !(lower.inclusive && upper.inclusive)))
    {
        return Err(CompileObservationError::InvalidScalarRange);
    }
    Ok(())
}

fn merge_time_ranges(ranges: &mut Vec<TimeRange>) {
    ranges.sort_unstable_by(|left, right| compare_lower(left.lower, right.lower));
    let mut merged: Vec<TimeRange> = Vec::with_capacity(ranges.len());
    for range in ranges.drain(..) {
        if let Some(last) = merged.last_mut()
            && ranges_overlap(last.upper, range.lower)
        {
            last.upper = union_upper(last.upper, range.upper);
        } else {
            merged.push(range);
        }
    }
    *ranges = merged;
}

fn compare_uv_ranges(left: &UvDistanceRange, right: &UvDistanceRange) -> Ordering {
    left.unit
        .cmp(&right.unit)
        .then_with(|| compare_lower(left.lower, right.lower))
}

fn compare_lower(left: Option<SelectionBound>, right: Option<SelectionBound>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => left
            .value
            .total_cmp(&right.value)
            .then_with(|| right.inclusive.cmp(&left.inclusive)),
    }
}

fn ranges_overlap(upper: Option<SelectionBound>, lower: Option<SelectionBound>) -> bool {
    match (upper, lower) {
        (None, _) | (_, None) => true,
        (Some(upper), Some(lower)) => {
            upper.value > lower.value
                || (upper.value == lower.value && (upper.inclusive || lower.inclusive))
        }
    }
}

fn union_upper(
    left: Option<SelectionBound>,
    right: Option<SelectionBound>,
) -> Option<SelectionBound> {
    match (left, right) {
        (None, _) | (_, None) => None,
        (Some(left), Some(right)) => match left.value.total_cmp(&right.value) {
            Ordering::Less => Some(right),
            Ordering::Greater => Some(left),
            Ordering::Equal => Some(SelectionBound {
                value: left.value,
                inclusive: left.inclusive || right.inclusive,
            }),
        },
    }
}

fn require_identity(
    identity: LogicalIdentity,
    scope: &'static str,
) -> Result<(), CompileObservationError> {
    if identity.as_bytes() == [0; 32] {
        Err(CompileObservationError::UnidentifiedInput { scope })
    } else {
        Ok(())
    }
}

const fn canonical_zero(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}

fn canonical_snapshot_id(
    sources: &[ObservationSource],
    reference_data: &[(ReferenceDataKind, LogicalIdentity)],
    model: ModelStateIdentity,
) -> ObservationSnapshotId {
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(OBSERVATION_SNAPSHOT_IDENTITY_DOMAIN);
    encoder.u32(OBSERVATION_SNAPSHOT_IDENTITY_VERSION);
    encoder.usize(sources.len());
    for source in sources {
        encoder.identity(source.identity.0);
        encode_selection(&mut encoder, &source.selection);
        encode_generations(&mut encoder, &source.generations);
    }
    encoder.usize(reference_data.len());
    for (kind, identity) in reference_data {
        encoder.u8(reference_data_tag(*kind));
        encoder.identity(*identity);
    }
    encode_model(&mut encoder, model);
    ObservationSnapshotId(LogicalIdentity::from_sha256(encoder.finish()))
}

fn canonical_provenance_id(
    snapshot_id: ObservationSnapshotId,
    sources: &[ObservationSource],
) -> ObservationProvenanceId {
    let mut by_request_order = sources.iter().collect::<Vec<_>>();
    by_request_order.sort_unstable_by_key(|source| source.input_ordinal);
    let mut encoder = CanonicalEncoder::new();
    encoder.bytes(OBSERVATION_PROVENANCE_IDENTITY_DOMAIN);
    encoder.u32(OBSERVATION_PROVENANCE_IDENTITY_VERSION);
    encoder.identity(snapshot_id.0);
    encoder.usize(by_request_order.len());
    for source in by_request_order {
        encoder.usize(source.input_ordinal);
        encoder.identity(source.identity.0);
        encoder.bytes(source.provenance.locator.as_bytes());
        encoder.identity(source.provenance.selection_request);
    }
    ObservationProvenanceId(LogicalIdentity::from_sha256(encoder.finish()))
}

fn encode_selection(encoder: &mut CanonicalEncoder, selection: &ObservationSelection) {
    encoder.u64(selection.rows.source_row_count);
    encoder.u64(selection.rows.selected_row_count());
    encoder.digest(selection.rows.sequence_id.as_bytes());
    encode_id_selection(encoder, &selection.rows_filter.fields);
    encode_time_selection(encoder, &selection.rows_filter.times);
    encode_uv_selection(encoder, &selection.rows_filter.uv_distances);
    match &selection.rows_filter.antennas {
        AntennaSelection::All => encoder.u8(0),
        AntennaSelection::Only(baselines) => {
            encoder.u8(1);
            encoder.usize(baselines.len());
            for baseline in baselines {
                encoder.u32(baseline.first);
                encoder.u32(baseline.second);
            }
        }
    }
    encode_id_selection(encoder, &selection.rows_filter.scans);
    encode_id_selection(encoder, &selection.rows_filter.observations);
    match &selection.rows_filter.intents {
        IntentSelection::All => encoder.u8(0),
        IntentSelection::Only(intents) => {
            encoder.u8(1);
            encoder.usize(intents.len());
            for intent in intents {
                encoder.u32(intent.state_id);
                encoder.bytes(intent.observation_mode.as_bytes());
            }
        }
    }
    encode_id_selection(encoder, &selection.rows_filter.arrays);

    encoder.usize(selection.data_descriptions.len());
    for data_description in &selection.data_descriptions {
        encoder.u32(data_description.data_description_id);
        encoder.u32(data_description.spectral_window_id);
        encoder.u32(data_description.polarization_id);
    }
    encoder.usize(selection.spectral_windows.len());
    for spectral_window in &selection.spectral_windows {
        encoder.u32(spectral_window.spectral_window_id);
        encoder.usize(spectral_window.channel_indices.len());
        for channel in &spectral_window.channel_indices {
            encoder.u32(*channel);
        }
    }
    encoder.usize(selection.correlations.len());
    for correlation in &selection.correlations {
        encoder.u32(correlation.polarization_id);
        encoder.usize(correlation.products.len());
        for product in &correlation.products {
            encoder.u32(product.correlation_index);
            encoder.u8(correlation_type_tag(product.correlation_type));
        }
    }
}

fn encode_generations(encoder: &mut CanonicalEncoder, generations: &SourceGenerations) {
    encoder.identity(generations.consistency_token.0);
    encoder.u8(visibility_column_tag(generations.columns.visibility));
    encoder.u8(flag_policy_tag(generations.columns.flags));
    encoder.u8(weight_column_tag(generations.columns.weights));
    encoder.usize(generations.columns.generations.len());
    for generation in &generations.columns.generations {
        encoder.u8(ms_column_tag(generation.kind));
        encoder.identity(generation.identity);
    }
    encoder.usize(generations.metadata.len());
    for generation in &generations.metadata {
        encoder.u8(metadata_table_tag(generation.kind));
        encoder.identity(generation.identity);
    }
    match generations.model_column {
        ModelColumnState::Absent => encoder.u8(0),
        ModelColumnState::Present(generation) => {
            encoder.u8(1);
            encoder.identity(generation);
        }
    }
}

fn encode_id_selection(encoder: &mut CanonicalEncoder, selection: &IdSelection) {
    match selection {
        IdSelection::All => encoder.u8(0),
        IdSelection::Only(ids) => {
            encoder.u8(1);
            encoder.usize(ids.len());
            for id in ids {
                encoder.u32(*id);
            }
        }
    }
}

fn encode_time_selection(encoder: &mut CanonicalEncoder, selection: &TimeSelection) {
    match selection {
        TimeSelection::All => encoder.u8(0),
        TimeSelection::Ranges(ranges) => {
            encoder.u8(1);
            encoder.usize(ranges.len());
            for range in ranges {
                encode_bound(encoder, range.lower);
                encode_bound(encoder, range.upper);
            }
        }
    }
}

fn encode_uv_selection(encoder: &mut CanonicalEncoder, selection: &UvSelection) {
    match selection {
        UvSelection::All => encoder.u8(0),
        UvSelection::Ranges(ranges) => {
            encoder.u8(1);
            encoder.usize(ranges.len());
            for range in ranges {
                encoder.u8(match range.unit {
                    UvDistanceUnit::Meters => 0,
                    UvDistanceUnit::Wavelengths => 1,
                });
                encode_bound(encoder, range.lower);
                encode_bound(encoder, range.upper);
            }
        }
    }
}

fn encode_bound(encoder: &mut CanonicalEncoder, bound: Option<SelectionBound>) {
    match bound {
        None => encoder.u8(0),
        Some(bound) => {
            encoder.u8(1);
            encoder.f64(bound.value);
            encoder.u8(u8::from(bound.inclusive));
        }
    }
}

fn encode_model(encoder: &mut CanonicalEncoder, model: ModelStateIdentity) {
    match model {
        ModelStateIdentity::Empty => encoder.u8(0),
        ModelStateIdentity::Seed(identity) => {
            encoder.u8(1);
            encoder.identity(identity);
        }
        ModelStateIdentity::Generation(identity) => {
            encoder.u8(2);
            encoder.identity(identity);
        }
    }
}

const fn reference_data_tag(kind: ReferenceDataKind) -> u8 {
    match kind {
        ReferenceDataKind::Measures => 0,
        ReferenceDataKind::Ephemeris => 1,
        ReferenceDataKind::Observatory => 2,
        ReferenceDataKind::SpectralLines => 3,
        ReferenceDataKind::Instrument => 4,
    }
}

const fn visibility_column_tag(column: VisibilityColumn) -> u8 {
    match column {
        VisibilityColumn::Data => 0,
        VisibilityColumn::CorrectedData => 1,
        VisibilityColumn::FloatData => 2,
    }
}

const fn flag_policy_tag(policy: FlagPolicy) -> u8 {
    match policy {
        FlagPolicy::FlagOrFlagRow => 0,
    }
}

const fn weight_column_tag(column: WeightColumn) -> u8 {
    match column {
        WeightColumn::Weight => 0,
        WeightColumn::WeightSpectrum => 1,
    }
}

const fn ms_column_tag(column: MsColumnKind) -> u8 {
    match column {
        MsColumnKind::Data => 0,
        MsColumnKind::CorrectedData => 1,
        MsColumnKind::FloatData => 2,
        MsColumnKind::Flag => 3,
        MsColumnKind::FlagRow => 4,
        MsColumnKind::Weight => 5,
        MsColumnKind::WeightSpectrum => 6,
        MsColumnKind::Uvw => 7,
        MsColumnKind::Time => 8,
        MsColumnKind::TimeCentroid => 9,
        MsColumnKind::Interval => 10,
        MsColumnKind::Exposure => 11,
        MsColumnKind::FieldId => 12,
        MsColumnKind::DataDescriptionId => 13,
        MsColumnKind::Antenna1 => 14,
        MsColumnKind::Antenna2 => 15,
        MsColumnKind::Feed1 => 16,
        MsColumnKind::Feed2 => 17,
        MsColumnKind::ScanNumber => 18,
        MsColumnKind::StateId => 19,
        MsColumnKind::ObservationId => 20,
        MsColumnKind::ArrayId => 21,
        MsColumnKind::ModelData => 22,
    }
}

const fn metadata_table_tag(table: MetadataTableKind) -> u8 {
    match table {
        MetadataTableKind::Antenna => 0,
        MetadataTableKind::DataDescription => 1,
        MetadataTableKind::Doppler => 2,
        MetadataTableKind::Feed => 3,
        MetadataTableKind::Field => 4,
        MetadataTableKind::FrequencyOffset => 5,
        MetadataTableKind::Observation => 6,
        MetadataTableKind::Pointing => 7,
        MetadataTableKind::Polarization => 8,
        MetadataTableKind::Source => 9,
        MetadataTableKind::SpectralWindow => 10,
        MetadataTableKind::State => 11,
        MetadataTableKind::SysCal => 12,
        MetadataTableKind::Weather => 13,
    }
}

pub(crate) const fn correlation_type_tag(correlation: CorrelationType) -> u8 {
    match correlation {
        CorrelationType::StokesI => 0,
        CorrelationType::StokesQ => 1,
        CorrelationType::StokesU => 2,
        CorrelationType::StokesV => 3,
        CorrelationType::CircularRr => 4,
        CorrelationType::CircularRl => 5,
        CorrelationType::CircularLr => 6,
        CorrelationType::CircularLl => 7,
        CorrelationType::LinearXx => 8,
        CorrelationType::LinearXy => 9,
        CorrelationType::LinearYx => 10,
        CorrelationType::LinearYy => 11,
        CorrelationType::MixedRx => 12,
        CorrelationType::MixedRy => 13,
        CorrelationType::MixedLx => 14,
        CorrelationType::MixedLy => 15,
        CorrelationType::MixedXr => 16,
        CorrelationType::MixedXl => 17,
        CorrelationType::MixedYr => 18,
        CorrelationType::MixedYl => 19,
        CorrelationType::QuasiOrthogonalPp => 20,
        CorrelationType::QuasiOrthogonalPq => 21,
        CorrelationType::QuasiOrthogonalQp => 22,
        CorrelationType::QuasiOrthogonalQq => 23,
        CorrelationType::RightCircular => 24,
        CorrelationType::LeftCircular => 25,
        CorrelationType::Linear => 26,
        CorrelationType::PolarizedIntensity => 27,
        CorrelationType::LinearPolarizedIntensity => 28,
        CorrelationType::FractionalPolarizedIntensity => 29,
        CorrelationType::FractionalLinearPolarizedIntensity => 30,
        CorrelationType::PolarizationAngle => 31,
    }
}
