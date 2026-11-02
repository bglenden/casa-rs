// SPDX-License-Identifier: LGPL-3.0-or-later

//! Immutable observation manifests and exact resolved selection semantics.

use std::{collections::BTreeSet, sync::Arc};

use casa_observation::PartitionId;
use casa_types::Correlation;
use thiserror::Error;

mod uv;

use uv::canonicalize_uv_selection;
pub use uv::{SelectionBound, UvDistanceRange, UvDistanceUnit, UvSelection};

/// A resolved non-negative MeasurementSet identifier selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdSelection {
    /// Select every identifier present in the source generation.
    All,
    /// Select exactly these identifiers.
    Only(Vec<u32>),
    /// Inclusive identifier intervals, stored in space proportional to their count.
    /// Compilation normalizes endpoints and merges overlapping or adjacent intervals.
    Ranges(Vec<(u32, u32)>),
}

impl IdSelection {
    /// Return an explicit identifier list, or `None` for all IDs or interval storage.
    #[must_use]
    pub fn explicit_ids(&self) -> Option<&[u32]> {
        match self {
            Self::All | Self::Ranges(_) => None,
            Self::Only(ids) => Some(ids),
        }
    }

    /// Whether every identifier is selected without a predicate.
    #[must_use]
    pub const fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    /// Bytes in owned vector allocations; the enclosing owner charges the enum itself.
    #[must_use]
    pub fn allocated_bytes(&self) -> Option<usize> {
        match self {
            Self::All => Some(0),
            Self::Only(ids) => ids.capacity().checked_mul(size_of::<u32>()),
            Self::Ranges(ranges) => ranges.capacity().checked_mul(size_of::<(u32, u32)>()),
        }
    }

    /// Whether the selection holds identifier `value`; a negative one is
    /// never selected by a list or interval. Lists must be sorted and intervals
    /// canonical (sorted and disjoint), as guaranteed after compilation.
    #[must_use]
    pub fn holds(&self, value: i64) -> bool {
        match self {
            Self::All => true,
            Self::Only(ids) => {
                u32::try_from(value).is_ok_and(|value| ids.binary_search(&value).is_ok())
            }
            Self::Ranges(ranges) => u32::try_from(value).is_ok_and(|value| {
                let index = ranges.partition_point(|&(start, _)| start <= value);
                index > 0 && value <= ranges[index - 1].1
            }),
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
        if let Self::Ranges(ranges) = self {
            if ranges.is_empty() {
                return Err(CompileObservationError::EmptyIdSelection { selector });
            }
            for (start, end) in ranges.iter_mut() {
                if *start > *end {
                    std::mem::swap(start, end);
                }
            }
            ranges.sort_unstable();
            let mut count = 0;
            for index in 0..ranges.len() {
                let (start, end) = ranges[index];
                if count > 0 && start <= ranges[count - 1].1.saturating_add(1) {
                    ranges[count - 1].1 = ranges[count - 1].1.max(end);
                } else {
                    ranges[count] = (start, end);
                    count += 1;
                }
            }
            ranges.truncate(count);
        }
        Ok(())
    }
}

/// Exact resolved scan-intent selection: the observing modes of the
/// selected partitions.
///
/// An observing mode is a whole MSv2 `STATE::OBS_MODE`, a comma-separated
/// list of intents, or an MSv4 partition's intents joined by commas, which
/// is how XRADIO splits the `OBS_MODE` it converts. A partition's samples
/// share one mode, so the partitions the selection reads are those of the
/// selected modes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntentSelection {
    /// Select every observing mode.
    All,
    /// Select exactly these observing modes, in canonical (sorted) order.
    Only(Vec<String>),
}

/// Exact row-level predicates applied conjunctively to one MeasurementSet.
///
/// These are the MeasurementSet selections the imager exposes: field, UV
/// distance, scan intent, scan number and observation identifier.
#[derive(Debug, Clone, PartialEq)]
pub struct RowSelection {
    fields: IdSelection,
    scans: IdSelection,
    observations: IdSelection,
    uv_distances: UvSelection,
    intents: IntentSelection,
}

impl RowSelection {
    /// Construct exact resolved row predicates.
    #[must_use]
    pub const fn new(
        fields: IdSelection,
        uv_distances: UvSelection,
        intents: IntentSelection,
    ) -> Self {
        Self {
            fields,
            scans: IdSelection::All,
            observations: IdSelection::All,
            uv_distances,
            intents,
        }
    }

    /// Constrain scan numbers and observation identifiers, conjunctively with
    /// the other row predicates. Compilation canonicalizes both ID sets.
    #[must_use]
    pub fn with_scan_observation(mut self, scans: IdSelection, observations: IdSelection) -> Self {
        self.scans = scans;
        self.observations = observations;
        self
    }

    /// Return resolved scan numbers.
    #[must_use]
    pub const fn scans(&self) -> &IdSelection {
        &self.scans
    }

    /// Return resolved observation identifiers.
    #[must_use]
    pub const fn observations(&self) -> &IdSelection {
        &self.observations
    }

    /// Return resolved field identifiers.
    #[must_use]
    pub const fn fields(&self) -> &IdSelection {
        &self.fields
    }

    /// Return exact UV-distance interval semantics.
    #[must_use]
    pub const fn uv_distances(&self) -> &UvSelection {
        &self.uv_distances
    }

    /// Return resolved scan intents.
    #[must_use]
    pub const fn intents(&self) -> &IntentSelection {
        &self.intents
    }

    fn canonicalize(&mut self) -> Result<(), CompileObservationError> {
        self.fields.canonicalize("field")?;
        self.scans.canonicalize("scan")?;
        self.observations.canonicalize("observation")?;
        canonicalize_uv_selection(&mut self.uv_distances)?;
        match &mut self.intents {
            IntentSelection::All => {}
            IntentSelection::Only(modes) => {
                if modes.is_empty() || modes.iter().any(|mode| mode.trim().is_empty()) {
                    return Err(CompileObservationError::InvalidIntent);
                }
                modes.sort_unstable();
                modes.dedup();
            }
        }
        Ok(())
    }
}

/// One selected partition of the observation
/// ([`casa_observation::PartitionId`]): the spectral window its samples are
/// in and the correlation products it holds, at their stored positions. A
/// partition's samples share one window, one correlation axis and one
/// intent; [`RowSelection`] and the read select its fields, times and
/// samples.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSelection {
    partition: PartitionId,
    spectral_window_id: u32,
    products: Vec<CorrelationProduct>,
}

impl PartitionSelection {
    /// Select `partition`, whose samples are in spectral window
    /// `spectral_window_id`, reading `products`.
    #[must_use]
    pub const fn new(
        partition: PartitionId,
        spectral_window_id: u32,
        products: Vec<CorrelationProduct>,
    ) -> Self {
        Self {
            partition,
            spectral_window_id,
            products,
        }
    }

    /// The partition.
    #[must_use]
    pub const fn partition(&self) -> PartitionId {
        self.partition
    }

    /// The spectral window number its samples are in, which tasks select
    /// by and kernel sets and continuum rules are keyed on.
    #[must_use]
    pub const fn spectral_window_id(&self) -> u32 {
        self.spectral_window_id
    }

    /// The correlation products read, in canonical (stored-position) order.
    #[must_use]
    pub fn products(&self) -> &[CorrelationProduct] {
        &self.products
    }
}

/// Storage-owner-certified physical coordinates for one complete spectral window.
///
/// CASA's mosaic convolution-function selection uses the complete physical
/// `CHAN_FREQ` vector and `abs(CHAN_WIDTH[0])`, even when the imaging selection
/// contains only a subset of channels. Keeping that catalog beside the logical
/// selection lets downstream science owners reproduce those semantics without
/// reopening the MeasurementSet or inferring unselected coordinates.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralWindowCoordinateCatalog {
    channel_frequencies_hz: Arc<[f64]>,
    first_channel_width_hz: f64,
}

// Construction rejects every non-finite value, so IEEE equality is reflexive
// for all values this type can contain.
impl Eq for SpectralWindowCoordinateCatalog {}

impl SpectralWindowCoordinateCatalog {
    /// Construct one exact physical `SPECTRAL_WINDOW` coordinate catalog.
    #[must_use]
    pub fn new(
        channel_frequencies_hz: impl Into<Arc<[f64]>>,
        first_channel_width_hz: f64,
    ) -> Option<Self> {
        let channel_frequencies_hz = channel_frequencies_hz.into();
        (!channel_frequencies_hz.is_empty()
            && channel_frequencies_hz
                .iter()
                .all(|frequency| frequency.is_finite() && *frequency > 0.0)
            && first_channel_width_hz.is_finite()
            && first_channel_width_hz != 0.0)
            .then_some(Self {
                channel_frequencies_hz,
                first_channel_width_hz,
            })
    }

    /// Return the number of physical channels, including unselected channels.
    #[must_use]
    pub fn channel_count(&self) -> usize {
        self.channel_frequencies_hz.len()
    }

    /// Return the exact physical `CHAN_FREQ` value at one native channel index.
    #[must_use]
    pub fn channel_frequency_hz(&self, channel: usize) -> Option<f64> {
        self.channel_frequencies_hz.get(channel).copied()
    }

    /// Return the complete physical `CHAN_FREQ` vector in storage order.
    #[must_use]
    pub fn channel_frequencies_hz(&self) -> &[f64] {
        &self.channel_frequencies_hz
    }

    /// Return the exact physical `CHAN_WIDTH[0]` value.
    #[must_use]
    pub const fn first_channel_width_hz(&self) -> f64 {
        self.first_channel_width_hz
    }
}

/// Exact channels selected from one spectral window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpectralWindowSelection {
    spectral_window_id: u32,
    channel_indices: Vec<u32>,
    coordinate_catalog: Option<SpectralWindowCoordinateCatalog>,
}

impl SpectralWindowSelection {
    /// Construct a resolved spectral-window/channel selection.
    #[must_use]
    pub const fn new(spectral_window_id: u32, channel_indices: Vec<u32>) -> Self {
        Self {
            spectral_window_id,
            channel_indices,
            coordinate_catalog: None,
        }
    }

    /// Bind the storage owner's complete physical spectral coordinate catalog.
    #[must_use]
    pub fn with_coordinate_catalog(
        mut self,
        coordinate_catalog: SpectralWindowCoordinateCatalog,
    ) -> Self {
        self.coordinate_catalog = Some(coordinate_catalog);
        self
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

    /// Return the storage-owner-certified complete physical SPW coordinates.
    #[must_use]
    pub const fn coordinate_catalog(&self) -> Option<&SpectralWindowCoordinateCatalog> {
        self.coordinate_catalog.as_ref()
    }
}

/// One selected correlation array coordinate and its MeasurementSet meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CorrelationProduct {
    correlation_index: u32,
    correlation_type: Correlation,
}

impl CorrelationProduct {
    /// Construct one selected correlation coordinate.
    #[must_use]
    pub const fn new(correlation_index: u32, correlation_type: Correlation) -> Self {
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
    pub const fn correlation_type(self) -> Correlation {
        self.correlation_type
    }
}

/// Complete resolved logical selection of one observation: the partitions
/// read, the channels of their spectral windows, the per-sample predicates,
/// and how many samples they select.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationSelection {
    samples: u64,
    rows_filter: RowSelection,
    partitions: Vec<PartitionSelection>,
    spectral_windows: Vec<SpectralWindowSelection>,
}

impl ObservationSelection {
    /// Select `partitions`, the `spectral_windows`' channels and the samples
    /// `rows_filter` holds: `samples` of them, each a (time, baseline) of a
    /// selected partition that the read delivers.
    #[must_use]
    pub const fn new(
        samples: u64,
        rows_filter: RowSelection,
        partitions: Vec<PartitionSelection>,
        spectral_windows: Vec<SpectralWindowSelection>,
    ) -> Self {
        Self {
            samples,
            rows_filter,
            partitions,
            spectral_windows,
        }
    }

    /// The selected samples: the (time, baseline)s of the selected
    /// partitions that the per-sample predicates hold, padding excluded.
    #[must_use]
    pub const fn samples(&self) -> u64 {
        self.samples
    }

    /// Return exact per-sample selection predicates.
    #[must_use]
    pub const fn rows_filter(&self) -> &RowSelection {
        &self.rows_filter
    }

    /// The selected partitions, in canonical (identifier) order.
    #[must_use]
    pub fn partitions(&self) -> &[PartitionSelection] {
        &self.partitions
    }

    /// Return exact channel selections in canonical spectral-window order.
    #[must_use]
    pub fn spectral_windows(&self) -> &[SpectralWindowSelection] {
        &self.spectral_windows
    }

    fn canonicalize(&mut self) -> Result<(), CompileObservationError> {
        self.rows_filter.canonicalize()?;
        for selection in &mut self.spectral_windows {
            selection.channel_indices.sort_unstable();
            selection.channel_indices.dedup();
            if selection.channel_indices.is_empty() {
                return Err(CompileObservationError::EmptySpectralWindowSelection {
                    spectral_window_id: selection.spectral_window_id,
                });
            }
            if let Some(catalog) = &selection.coordinate_catalog
                && selection.channel_indices.iter().any(|channel| {
                    usize::try_from(*channel)
                        .ok()
                        .is_none_or(|channel| channel >= catalog.channel_count())
                })
            {
                return Err(
                    CompileObservationError::SpectralWindowCoordinateCatalogMismatch {
                        spectral_window_id: selection.spectral_window_id,
                    },
                );
            }
        }
        self.spectral_windows
            .sort_unstable_by_key(|selection| selection.spectral_window_id);
        if self.samples > 0 && self.spectral_windows.is_empty() {
            return Err(CompileObservationError::NoSpectralWindowSelection);
        }
        if let Some(spectral_window_id) = self.spectral_windows.windows(2).find_map(|pair| {
            (pair[0].spectral_window_id == pair[1].spectral_window_id)
                .then_some(pair[0].spectral_window_id)
        }) {
            return Err(CompileObservationError::DuplicateSpectralWindow { spectral_window_id });
        }
        self.partitions
            .sort_unstable_by_key(|selection| selection.partition);
        if self.samples > 0 && self.partitions.is_empty() {
            return Err(CompileObservationError::NoPartitionSelection);
        }
        if let Some(partition) = self
            .partitions
            .windows(2)
            .find_map(|pair| (pair[0].partition == pair[1].partition).then_some(pair[0].partition))
        {
            return Err(CompileObservationError::DuplicatePartition {
                partition: partition.0,
            });
        }
        for selection in &mut self.partitions {
            let partition = selection.partition.0;
            selection.products.sort_unstable();
            if selection.products.is_empty() {
                return Err(CompileObservationError::EmptyCorrelationSelection { partition });
            }
            if selection
                .products
                .windows(2)
                .any(|pair| pair[0].correlation_index == pair[1].correlation_index)
            {
                return Err(CompileObservationError::DuplicateCorrelationIndex { partition });
            }
            let types = selection
                .products
                .iter()
                .map(|product| product.correlation_type)
                .collect::<BTreeSet<_>>();
            if types.len() != selection.products.len() {
                return Err(CompileObservationError::DuplicateCorrelationType { partition });
            }
            if self
                .spectral_windows
                .binary_search_by_key(&selection.spectral_window_id, |window| {
                    window.spectral_window_id
                })
                .is_err()
            {
                return Err(CompileObservationError::UnknownPartitionSpectralWindow {
                    partition,
                    spectral_window_id: selection.spectral_window_id,
                });
            }
        }
        if let Some(spectral_window_id) = self.spectral_windows.iter().find_map(|window| {
            (!self
                .partitions
                .iter()
                .any(|partition| partition.spectral_window_id == window.spectral_window_id))
            .then_some(window.spectral_window_id)
        }) {
            return Err(CompileObservationError::OrphanSpectralWindowSelection {
                spectral_window_id,
            });
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

/// Exact data, flag, and weight columns read from one source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedColumns {
    visibility: VisibilityColumn,
    flags: FlagPolicy,
    weights: WeightColumn,
}

impl SelectedColumns {
    /// Construct the selected column semantics.
    #[must_use]
    pub const fn new(
        visibility: VisibilityColumn,
        flags: FlagPolicy,
        weights: WeightColumn,
    ) -> Self {
        Self {
            visibility,
            flags,
            weights,
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
}

/// Where one MeasurementSet source lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationSourceProvenance {
    locator: String,
}

impl ObservationSourceProvenance {
    /// Construct the source location.
    #[must_use]
    pub const fn new(locator: String) -> Self {
        Self { locator }
    }

    /// Return the source locator.
    #[must_use]
    pub fn locator(&self) -> &str {
        &self.locator
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
        Ok(())
    }
}

/// Uncompiled source manifest supplied by the observation adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationSourceInput {
    provenance: ObservationSourceProvenance,
    selection: ObservationSelection,
    columns: SelectedColumns,
    corrected_data_present: bool,
}

impl ObservationSourceInput {
    /// Construct one MeasurementSet source manifest: where it is, what is
    /// selected, which columns are read, and whether MAIN has `CORRECTED_DATA`.
    #[must_use]
    pub const fn new(
        provenance: ObservationSourceProvenance,
        selection: ObservationSelection,
        columns: SelectedColumns,
        corrected_data_present: bool,
    ) -> Self {
        Self {
            provenance,
            selection,
            columns,
            corrected_data_present,
        }
    }
}

/// One validated immutable MeasurementSet source in a compiled snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationSource {
    provenance: ObservationSourceProvenance,
    input_ordinal: usize,
    selection: Arc<ObservationSelection>,
    columns: SelectedColumns,
    corrected_data_present: bool,
}

impl ObservationSource {
    /// Return source origin and original selection-request provenance.
    #[must_use]
    pub const fn provenance(&self) -> &ObservationSourceProvenance {
        &self.provenance
    }

    /// Return this source's ordinal in the original multi-MS request, which
    /// is also its position in the snapshot.
    #[must_use]
    pub const fn input_ordinal(&self) -> usize {
        self.input_ordinal
    }

    /// Return exact resolved selection semantics.
    #[must_use]
    pub fn selection(&self) -> &ObservationSelection {
        &self.selection
    }

    /// Return the data, flag, and weight columns read.
    #[must_use]
    pub const fn columns(&self) -> SelectedColumns {
        self.columns
    }

    /// Return whether MAIN had a `CORRECTED_DATA` column when resolved.
    #[must_use]
    pub const fn corrected_data_present(&self) -> bool {
        self.corrected_data_present
    }

    pub(crate) fn selection_arc(&self) -> Arc<ObservationSelection> {
        Arc::clone(&self.selection)
    }
}

/// Uncompiled manifest for one immutable logical observation snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationSnapshotInput {
    sources: Vec<ObservationSourceInput>,
}

impl ObservationSnapshotInput {
    /// Construct a multi-MS snapshot manifest.
    #[must_use]
    pub const fn new(sources: Vec<ObservationSourceInput>) -> Self {
        Self { sources }
    }
}

/// The selected observation data of one imaging problem.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationSnapshot {
    sources: Vec<ObservationSource>,
}

// Compilation rejects non-finite ranges, so snapshot equality is reflexive.
impl Eq for ObservationSnapshot {}

impl ObservationSnapshot {
    /// Return sources in request order.
    #[must_use]
    pub fn sources(&self) -> &[ObservationSource] {
        &self.sources
    }
}

/// Failure to compile an authoritative observation snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CompileObservationError {
    /// No MeasurementSet source was supplied.
    #[error("an observation snapshot requires at least one MeasurementSet source")]
    NoSources,
    /// No source contributed any selected sample.
    #[error("observation selection contains no samples")]
    EmptySelection,
    /// Source provenance did not name an origin.
    #[error("observation source locator is empty")]
    EmptySourceLocator,
    /// An explicit resolved identifier set was empty.
    #[error("explicit {selector} selection is empty")]
    EmptyIdSelection {
        /// Selector family.
        selector: &'static str,
    },
    /// A UV range was unbounded, non-finite, or inverted.
    #[error("selection range is unbounded, non-finite, or inverted")]
    InvalidScalarRange,
    /// A UV-distance range used a negative bound.
    #[error("UV-distance selection bounds must be non-negative")]
    NegativeUvDistance,
    /// An intent selection named no observing mode, or an empty one.
    #[error("an intent selection names a non-empty observing mode, at least one")]
    InvalidIntent,
    /// One partition appeared more than once.
    #[error("duplicate selection of partition {partition}")]
    DuplicatePartition {
        /// The partition's identifier.
        partition: u32,
    },
    /// Selected samples had no partition to be read from.
    #[error("selected samples require at least one partition selection")]
    NoPartitionSelection,
    /// A selected partition's spectral window was not selected.
    #[error("partition {partition} is in unselected spectral window {spectral_window_id}")]
    UnknownPartitionSpectralWindow {
        /// The partition's identifier.
        partition: u32,
        /// Unselected spectral-window identifier.
        spectral_window_id: u32,
    },
    /// No spectral-window/channel semantics were supplied.
    #[error("selected samples require at least one spectral-window/channel selection")]
    NoSpectralWindowSelection,
    /// One selected spectral window had no channel.
    #[error("spectral window {spectral_window_id} has an empty channel selection")]
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
    /// A selected native channel was absent from the storage-owner coordinate catalog.
    #[error(
        "spectral window {spectral_window_id} selection exceeds its physical coordinate catalog"
    )]
    SpectralWindowCoordinateCatalogMismatch {
        /// Spectral-window identifier.
        spectral_window_id: u32,
    },
    /// A selected spectral window held no selected partition.
    #[error("spectral window {spectral_window_id} holds no selected partition")]
    OrphanSpectralWindowSelection {
        /// Orphan spectral-window identifier.
        spectral_window_id: u32,
    },
    /// One partition selected no correlations.
    #[error("partition {partition} has an empty correlation selection")]
    EmptyCorrelationSelection {
        /// The partition's identifier.
        partition: u32,
    },
    /// One stored correlation position appeared more than once.
    #[error("partition {partition} repeats a correlation position")]
    DuplicateCorrelationIndex {
        /// The partition's identifier.
        partition: u32,
    },
    /// One correlation meaning appeared at several positions.
    #[error("partition {partition} repeats a correlation type")]
    DuplicateCorrelationType {
        /// The partition's identifier.
        partition: u32,
    },
}

/// Compile and validate one immutable logical observation snapshot.
///
/// The compiler reads only compact manifests; it never reads or retains
/// visibility, flag, weight, or coordinate arrays. Sources keep request order.
pub fn compile_observation(
    input: ObservationSnapshotInput,
) -> Result<ObservationSnapshot, CompileObservationError> {
    if input.sources.is_empty() {
        return Err(CompileObservationError::NoSources);
    }
    let mut sources = Vec::with_capacity(input.sources.len());
    let mut has_selected_samples = false;
    for (input_ordinal, source) in input.sources.into_iter().enumerate() {
        let provenance = source.provenance;
        provenance.validate()?;
        let mut selection = source.selection;
        selection.canonicalize()?;
        has_selected_samples |= selection.samples > 0;
        sources.push(ObservationSource {
            provenance,
            input_ordinal,
            selection: Arc::new(selection),
            columns: source.columns,
            corrected_data_present: source.corrected_data_present,
        });
    }
    if !has_selected_samples {
        return Err(CompileObservationError::EmptySelection);
    }
    Ok(ObservationSnapshot { sources })
}
