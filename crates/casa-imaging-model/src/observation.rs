// SPDX-License-Identifier: LGPL-3.0-or-later

//! Immutable observation manifests and exact resolved selection semantics.

use std::{cmp::Ordering, collections::BTreeSet, sync::Arc};

use thiserror::Error;

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
///
/// These are the MeasurementSet selections the imager exposes: field, UV
/// distance and scan intent.
#[derive(Debug, Clone, PartialEq)]
pub struct RowSelection {
    fields: IdSelection,
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
            uv_distances,
            intents,
        }
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
        canonicalize_uv_selection(&mut self.uv_distances)?;
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

/// Failure to record a canonical selected-row manifest.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SelectedRowSequenceError {
    /// The observed row count cannot be represented canonically.
    #[error("selected physical MAIN row count exceeds the canonical u64 domain")]
    RowCountOverflow,
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

/// Count and DATA_DESCRIPTION identifiers of the selected MAIN rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedRows {
    source_row_count: u64,
    selected_row_count: u64,
    used_data_description_ids: Arc<[u32]>,
}

impl SelectedRows {
    /// Record canonical MAIN row/DDID coordinates without retaining them.
    ///
    /// Each row is checked for range, then adjacent duplication, then descending
    /// order. A non-adjacent repetition necessarily encounters descending order
    /// first, so validation needs no separate selection-sized duplicate set.
    pub fn from_ordered_main_rows(
        source_row_count: u64,
        rows: impl IntoIterator<Item = SelectedMainRow>,
    ) -> Result<Self, SelectedRowSequenceError> {
        let mut builder = SelectedRowsBuilder::new(source_row_count);
        for row in rows {
            builder.push(row)?;
        }
        Ok(builder.finish())
    }

    /// Return the source MAIN row count at capture.
    #[must_use]
    pub const fn source_row_count(&self) -> u64 {
        self.source_row_count
    }

    /// Return the number of selected MAIN rows.
    #[must_use]
    pub const fn selected_row_count(&self) -> u64 {
        self.selected_row_count
    }

    /// Return heap bytes owned by the shared DATA_DESCRIPTION identifiers.
    ///
    /// Cloning [`SelectedRows`] shares this immutable allocation, so a retained
    /// storage owner charges it once.
    #[must_use]
    pub fn retained_manifest_bytes(&self) -> Option<usize> {
        (2 * size_of::<usize>()).checked_add(
            self.used_data_description_ids
                .len()
                .checked_mul(size_of::<u32>())?,
        )
    }

    fn used_data_description_ids(&self) -> &[u32] {
        &self.used_data_description_ids
    }
}

/// Streaming builder for one compact selected-row manifest.
pub struct SelectedRowsBuilder {
    source_row_count: u64,
    selected_row_count: u64,
    previous_row: Option<u64>,
    used_data_description_ids: Vec<u32>,
}

impl SelectedRowsBuilder {
    /// Begin one physical-order selected-row capture for a fixed MAIN cardinality.
    #[must_use]
    pub fn new(source_row_count: u64) -> Self {
        Self::with_data_description_capacity(source_row_count, 0)
    }

    /// Begin one capture with bounded storage for the selected DATA_DESCRIPTION identifiers.
    #[must_use]
    pub fn with_data_description_capacity(
        source_row_count: u64,
        data_description_capacity: usize,
    ) -> Self {
        Self {
            source_row_count,
            selected_row_count: 0,
            previous_row: None,
            used_data_description_ids: Vec::with_capacity(data_description_capacity),
        }
    }

    /// Add one selected MAIN row in canonical physical order.
    pub fn push(&mut self, selected: SelectedMainRow) -> Result<(), SelectedRowSequenceError> {
        let row = selected.physical_row;
        self.selected_row_count = self
            .selected_row_count
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
        if !self
            .used_data_description_ids
            .contains(&selected.data_description_id)
        {
            self.used_data_description_ids
                .push(selected.data_description_id);
        }
        self.previous_row = Some(row);
        Ok(())
    }

    /// Finish the compact manifest without retaining the captured row corpus.
    #[must_use]
    pub fn finish(mut self) -> SelectedRows {
        self.used_data_description_ids.sort_unstable();
        SelectedRows {
            source_row_count: self.source_row_count,
            selected_row_count: self.selected_row_count,
            used_data_description_ids: self.used_data_description_ids.into(),
        }
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

    fn retained_manifest_bytes(&self) -> Option<usize> {
        self.channel_frequencies_hz
            .len()
            .checked_mul(size_of::<f64>())?
            .checked_add(2 * size_of::<usize>())
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

impl CorrelationType {
    /// Return whether this stored correlation participates directly in a Stokes-I solve.
    #[must_use]
    pub const fn contributes_to_stokes_i(self) -> bool {
        matches!(
            self,
            Self::StokesI | Self::CircularRr | Self::CircularLl | Self::LinearXx | Self::LinearYy
        )
    }
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
        bytes = bytes.checked_add(id_selection_bytes(&self.rows_filter.fields)?)?;
        if let UvSelection::Ranges(ranges) = &self.rows_filter.uv_distances {
            bytes = bytes.checked_add(
                ranges
                    .capacity()
                    .checked_mul(size_of::<UvDistanceRange>())?,
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
            if let Some(catalog) = &selection.coordinate_catalog {
                bytes = bytes.checked_add(catalog.retained_manifest_bytes()?)?;
            }
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
    /// No source contributed any selected row.
    #[error("observation selection contains no rows")]
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
    /// A selected native channel was absent from the storage-owner coordinate catalog.
    #[error(
        "spectral window {spectral_window_id} selection exceeds its physical coordinate catalog"
    )]
    SpectralWindowCoordinateCatalogMismatch {
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
    let mut has_selected_rows = false;
    for (input_ordinal, source) in input.sources.into_iter().enumerate() {
        let provenance = source.provenance;
        provenance.validate()?;
        let mut selection = source.selection;
        selection.canonicalize()?;
        has_selected_rows |= selection.rows.selected_row_count() > 0;
        sources.push(ObservationSource {
            provenance,
            input_ordinal,
            selection: Arc::new(selection),
            columns: source.columns,
            corrected_data_present: source.corrected_data_present,
        });
    }
    if !has_selected_rows {
        return Err(CompileObservationError::EmptySelection);
    }
    Ok(ObservationSnapshot { sources })
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

const fn canonical_zero(value: f64) -> f64 {
    if value == 0.0 { 0.0 } else { value }
}
