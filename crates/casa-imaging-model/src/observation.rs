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

/// Exact resolved baseline selection: CASA's `antenna` expression evaluated
/// over the observation's antennas.
///
/// The table answers, for each ordered `(antenna1, antenna2)` pair of
/// antenna identifiers, whether a sample on that baseline is selected. CASA's
/// expressions are not symmetric in general (`/A&B/` matches only that
/// order), and whether autocorrelations are selected depends on the form, so
/// the table holds every ordered pair, autocorrelations included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BaselineSelection {
    /// Select every baseline.
    All,
    /// Select the ordered pairs marked in a row-major `antennas × antennas`
    /// table indexed by `antenna1 * antennas + antenna2`.
    Pairs {
        /// Antennas along each side of the table.
        antennas: usize,
        /// Whether each ordered pair is selected.
        selected: Vec<bool>,
    },
}

impl BaselineSelection {
    /// The selection `holds` states for every ordered pair of `antennas`
    /// antennas.
    #[must_use]
    pub fn from_pairs(antennas: usize, holds: impl Fn(usize, usize) -> bool) -> Self {
        let mut selected = Vec::with_capacity(antennas * antennas);
        for antenna1 in 0..antennas {
            selected.extend((0..antennas).map(|antenna2| holds(antenna1, antenna2)));
        }
        Self::Pairs { antennas, selected }
    }

    /// Whether the selection is unconstrained.
    #[must_use]
    pub const fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    /// Whether a sample on the ordered baseline `(antenna1, antenna2)` is
    /// selected. An antenna outside the table selects nothing.
    #[must_use]
    pub fn holds(&self, antenna1: u32, antenna2: u32) -> bool {
        match self {
            Self::All => true,
            Self::Pairs { antennas, selected } => {
                let (antenna1, antenna2) = (antenna1 as usize, antenna2 as usize);
                antenna1 < *antennas
                    && antenna2 < *antennas
                    && selected[antenna1 * antennas + antenna2]
            }
        }
    }

    /// Heap bytes the selection owns.
    #[must_use]
    pub fn allocated_bytes(&self) -> usize {
        match self {
            Self::All => 0,
            Self::Pairs { selected, .. } => selected.capacity() * size_of::<bool>(),
        }
    }
}

/// One condition CASA's `timerange` puts on a sample's `TIME`, in seconds
/// in the `TIME` column's scale, evaluated with the same double-precision
/// operations as CASA's TaQL (`MSTimeParse`, casacore `aaf72eb7`). The
/// MeasurementSet owner's port of CASA's time grammar
/// (`casa_ms::TimeCondition`) resolves them; this is its data, so imaging
/// tests each time as CASA does.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TimeCondition {
    /// `abs(TIME - time_s) <= tolerance_s`: a single time.
    Near {
        /// The time.
        time_s: f64,
        /// Half the default row's `EXPOSURE`.
        tolerance_s: f64,
    },
    /// `TIME >= time_s`, from `>T`.
    AtOrAfter {
        /// The bound.
        time_s: f64,
    },
    /// `TIME <= time_s`, from `<T`.
    AtOrBefore {
        /// The bound.
        time_s: f64,
    },
    /// `TIME >= lower_s && TIME <= upper_s`, from `T0~T1` or `T0+dT`.
    Within {
        /// The lower bound.
        lower_s: f64,
        /// The upper bound.
        upper_s: f64,
    },
    /// `(TIME > lower_s || abs(TIME - lower_s) < edge_s) && (TIME < upper_s
    /// || abs(TIME - upper_s) < edge_s)`, from a bracketed range.
    EdgeInclusive {
        /// The lower bound.
        lower_s: f64,
        /// The upper bound.
        upper_s: f64,
        /// The edge width, a C `Float` in CASA.
        edge_s: f32,
    },
}

impl TimeCondition {
    /// Whether a sample whose `TIME` is `time_s` meets the condition.
    #[must_use]
    pub fn holds(&self, time_s: f64) -> bool {
        match *self {
            Self::Near {
                time_s: centre,
                tolerance_s,
            } => (time_s - centre).abs() <= tolerance_s,
            Self::AtOrAfter { time_s: bound } => time_s >= bound,
            Self::AtOrBefore { time_s: bound } => time_s <= bound,
            Self::Within { lower_s, upper_s } => time_s >= lower_s && time_s <= upper_s,
            Self::EdgeInclusive {
                lower_s,
                upper_s,
                edge_s,
            } => {
                let edge = f64::from(edge_s);
                (time_s > lower_s || (time_s - lower_s).abs() < edge)
                    && (time_s < upper_s || (time_s - upper_s).abs() < edge)
            }
        }
    }
}

/// Exact resolved time selection: CASA's `timerange`, the OR of its terms.
#[derive(Debug, Clone, PartialEq)]
pub enum TimeSelection {
    /// Select every time.
    All,
    /// Select a time that meets any of these conditions.
    AnyOf(Vec<TimeCondition>),
}

impl TimeSelection {
    /// Whether the selection is unconstrained.
    #[must_use]
    pub const fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    /// Whether a sample whose `TIME` is `time_s` is selected.
    #[must_use]
    pub fn holds(&self, time_s: f64) -> bool {
        match self {
            Self::All => true,
            Self::AnyOf(conditions) => conditions.iter().any(|condition| condition.holds(time_s)),
        }
    }

    /// Heap bytes the selection owns.
    #[must_use]
    pub fn allocated_bytes(&self) -> usize {
        match self {
            Self::All => 0,
            Self::AnyOf(conditions) => conditions.capacity() * size_of::<TimeCondition>(),
        }
    }
}

/// Exact row-level predicates applied conjunctively to one MeasurementSet.
///
/// These are the MeasurementSet selections the imager exposes: field, UV
/// distance, scan intent, scan number, observation identifier, baseline and
/// time.
#[derive(Debug, Clone, PartialEq)]
pub struct RowSelection {
    fields: IdSelection,
    scans: IdSelection,
    observations: IdSelection,
    uv_distances: UvSelection,
    intents: IntentSelection,
    baselines: BaselineSelection,
    times: TimeSelection,
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
            baselines: BaselineSelection::All,
            times: TimeSelection::All,
        }
    }

    /// Constrain times, conjunctively with the other row predicates.
    #[must_use]
    pub fn with_times(mut self, times: TimeSelection) -> Self {
        self.times = times;
        self
    }

    /// Return the resolved time selection.
    #[must_use]
    pub const fn times(&self) -> &TimeSelection {
        &self.times
    }

    /// Constrain baselines, conjunctively with the other row predicates.
    #[must_use]
    pub fn with_baselines(mut self, baselines: BaselineSelection) -> Self {
        self.baselines = baselines;
        self
    }

    /// Return the resolved baseline selection.
    #[must_use]
    pub const fn baselines(&self) -> &BaselineSelection {
        &self.baselines
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

/// CASA's frame-based frequency selection of one spectral window
/// (`vi::FrequencySelectionUsingFrame`): frequency bands in one frame that
/// each visibility time selects channels by.
///
/// At each time the bands are converted from their frame to the window's
/// at that time, towards the time's field, and the time reads the channels
/// whose extent, centre less and plus half its width, meets a band at
/// either end (`SpectralWindowChannels::getIntersection`). So a Doppler
/// shift across an observation selects different channels at different
/// times, and a channel never selected at some time is read as if it were
/// not in the window. The bands are `SynthesisImagerVi2::selectData`'s,
/// narrowed to the image's band where `SynthesisImager::tuneSelectData`
/// tunes it.
#[derive(Debug, Clone, PartialEq)]
pub struct FrequencyBands {
    frame: crate::FrequencyFrame,
    bands_hz: Vec<[f64; 2]>,
}

impl FrequencyBands {
    /// Bands `bands_hz`, each `[lower, upper]` in hertz in `frame`;
    /// `None` unless there is a band and each is finite and positive with
    /// its lower end at most its upper.
    #[must_use]
    pub fn new(frame: crate::FrequencyFrame, bands_hz: Vec<[f64; 2]>) -> Option<Self> {
        (!bands_hz.is_empty()
            && bands_hz.iter().all(|[lower, upper]| {
                lower.is_finite() && upper.is_finite() && *lower > 0.0 && lower <= upper
            }))
        .then_some(Self { frame, bands_hz })
    }

    /// The frame the bands are stated in.
    #[must_use]
    pub const fn frame(&self) -> crate::FrequencyFrame {
        self.frame
    }

    /// The bands, each `[lower, upper]` in hertz.
    #[must_use]
    pub fn bands_hz(&self) -> &[[f64; 2]] {
        &self.bands_hz
    }

    /// Whether a channel whose extent is `[lower_hz, upper_hz]`, in the
    /// bands' frame scaled by `factor`, meets a band at either end.
    #[must_use]
    pub fn meets(&self, factor: f64, [lower_hz, upper_hz]: [f64; 2]) -> bool {
        self.bands_hz
            .iter()
            .any(|[lower, upper]| upper_hz >= lower * factor && lower_hz <= upper * factor)
    }
}

/// Exact channels selected from one spectral window.
#[derive(Debug, Clone, PartialEq)]
pub struct SpectralWindowSelection {
    spectral_window_id: u32,
    channel_indices: Vec<u32>,
    coordinate_catalog: Option<SpectralWindowCoordinateCatalog>,
    first_buffer_lsrk_factor: Option<f64>,
    frequency_bands: Option<FrequencyBands>,
}

// Construction rejects every non-finite factor, so IEEE equality is
// reflexive for all values this type can contain.
impl Eq for SpectralWindowSelection {}

impl SpectralWindowSelection {
    /// Construct a resolved spectral-window/channel selection.
    #[must_use]
    pub const fn new(spectral_window_id: u32, channel_indices: Vec<u32>) -> Self {
        Self {
            spectral_window_id,
            channel_indices,
            coordinate_catalog: None,
            first_buffer_lsrk_factor: None,
            frequency_bands: None,
        }
    }

    /// Bind CASA's per-time frequency selection of the window
    /// ([`FrequencyBands`]): each time reads only those of the selected
    /// channels the bands select then.
    #[must_use]
    pub fn with_frequency_bands(mut self, bands: FrequencyBands) -> Self {
        self.frequency_bands = Some(bands);
        self
    }

    /// The per-time frequency selection [`Self::with_frequency_bands`]
    /// bound, if any; without one every time reads every selected channel.
    #[must_use]
    pub const fn frequency_bands(&self) -> Option<&FrequencyBands> {
        self.frequency_bands.as_ref()
    }

    /// Bind the factor that takes the window's frequencies to LSRK at the
    /// first visibility buffer CASA's tclean iterates of it, where
    /// `FTMachine::matchChannel` converts them and a mosaic evaluates its
    /// beams (`HetArrayConvFunc::findConvFunction`); `None` unless the
    /// factor is finite and positive.
    #[must_use]
    pub fn with_first_buffer_lsrk_factor(mut self, factor: f64) -> Option<Self> {
        (factor.is_finite() && factor > 0.0).then(|| {
            self.first_buffer_lsrk_factor = Some(factor);
            self
        })
    }

    /// The factor [`Self::with_first_buffer_lsrk_factor`] bound, if any.
    #[must_use]
    pub const fn first_buffer_lsrk_factor(&self) -> Option<f64> {
        self.first_buffer_lsrk_factor
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
