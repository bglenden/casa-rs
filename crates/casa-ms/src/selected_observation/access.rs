// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{collections::VecDeque, mem::size_of};

use crate::derived::engine::MsCalEngine;
use crate::subtables::SubTable;
use crate::{
    MeasurementSet, MsError, PointingDirectionBracket,
    PointingDirectionColumn as StoredPointingDirectionColumn, PointingDirectionQuery,
    PointingReadPlan, SelectedObservationBuffer, SelectedObservationBufferRequest,
    SelectedStoredSample, SelectedStoredVisibility, SelectedVisibilityColumn, SelectedWeightColumn,
    VisibilityChannelReadRange,
};
use casa_imaging_model::{
    CompiledProblem, CorrelationProduct, CorrelationType, DataDescriptionSelection, DelayCentreLaw,
    DirectionFrame, Epoch, FrequencyFrame, MeasurementSetReadAccess, MissingPointingPolicy,
    ObservationSelection, ObservationSource, ObservationSourceState, PhaseCentreLaw,
    PointingCentreLaw, PointingDirectionColumn, PointingExtrapolation, PointingInterpolation,
    PointingTimeSampling, SelectedMainRow, SelectedObservationSample, SelectedPointingDirections,
    SelectedPredictionTarget, SelectedSampleAddress, SelectedSampleCoordinates,
    SelectedSampleMetadata, SelectedVisibilitySample, SkyDirection, TimeScale, VisibilityColumn,
    WeightColumn,
};
use thiserror::Error;

use super::{
    SelectedObservationContentBudget, SelectedObservationContentPlan,
    SelectedObservationContentPlanError, SelectedObservationMeasures,
    SelectedObservationMeasuresError,
    content_plan::{SelectedObservationSharedBytes, selected_content_plan},
    row_selection::{CompiledRowPredicate, RowSelectionEvaluationError, StoredMainRow},
};

const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;

/// A retained, read-locked MeasurementSet bound to one compiled source selection.
///
/// Construction opens the retained storage capability and metadata needed to plan the bounded
/// content buffers. The sole MAIN traversal happens when samples are consumed: that pass both
/// validates the compact compiler-owned manifest and produces the selected values.
pub(crate) struct BoundObservationSource {
    source_identity: casa_imaging_model::MeasurementSetIdentity,
    measurement_set: MeasurementSet,
    geometry_engine: MsCalEngine,
    row_predicate: CompiledRowPredicate,
    coordinates: Box<[SelectedCoordinates]>,
    content_plan: SelectedObservationContentPlan,
    source_row_count_matches: bool,
}

impl BoundObservationSource {
    pub(super) const fn source_identity(&self) -> casa_imaging_model::MeasurementSetIdentity {
        self.source_identity
    }

    pub(super) const fn geometry_engine(&self) -> &MsCalEngine {
        &self.geometry_engine
    }

    pub(crate) const fn retained_source_slot_bytes() -> usize {
        size_of::<Self>()
    }

    /// Derive the smallest owner-coherent budget that can retain one selected row.
    #[cfg(unix)]
    pub(crate) fn minimum_content_budget_with_measures(
        problem: &CompiledProblem,
        source: &ObservationSource,
        current_state: &ObservationSourceState,
        measures: &SelectedObservationMeasures,
        shared_bytes: SelectedObservationSharedBytes,
        maximum_budget: SelectedObservationContentBudget,
    ) -> Result<SelectedObservationContentBudget, BoundObservationSourceError> {
        measures.validate_problem(problem)?;
        let measurement_set = MeasurementSet::open_retained_read(source.provenance().locator())?;
        validate_current_state(source, current_state)?;
        selected_content_plan(
            &measurement_set,
            problem,
            source,
            shared_bytes,
            maximum_budget,
        )?;

        let mut lower = 0_usize;
        let mut upper = maximum_budget.available_bytes();
        while lower + 1 < upper {
            let candidate_bytes = lower + (upper - lower) / 2;
            let candidate = SelectedObservationContentBudget::new(
                candidate_bytes,
                maximum_budget.maximum_live_blocks(),
                maximum_budget.maximum_pointing_polynomial_terms(),
            );
            match selected_content_plan(&measurement_set, problem, source, shared_bytes, candidate)
            {
                Ok(_) => upper = candidate_bytes,
                Err(
                    SelectedObservationContentPlanError::InvalidBudget
                    | SelectedObservationContentPlanError::InsufficientBudget { .. }
                    | SelectedObservationContentPlanError::InsufficientRetainedBudget { .. },
                ) => lower = candidate_bytes,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(SelectedObservationContentBudget::new(
            upper,
            maximum_budget.maximum_live_blocks(),
            maximum_budget.maximum_pointing_polynomial_terms(),
        ))
    }

    /// Open the source locator under retained read locks without traversing MAIN rows.
    #[cfg(unix)]
    pub(crate) fn open_with_measures(
        problem: &CompiledProblem,
        source: &ObservationSource,
        current_state: &ObservationSourceState,
        measures: &SelectedObservationMeasures,
        shared_bytes: SelectedObservationSharedBytes,
        content_budget: SelectedObservationContentBudget,
    ) -> Result<Self, BoundObservationSourceError> {
        measures.validate_problem(problem)?;
        let measurement_set = MeasurementSet::open_retained_read(source.provenance().locator())?;
        validate_current_state(source, current_state)?;
        let content_plan = selected_content_plan(
            &measurement_set,
            problem,
            source,
            shared_bytes,
            content_budget,
        )?;
        let row_predicate = selected_row_predicate(&measurement_set, source)?;
        let coordinates = selected_coordinates(&measurement_set, source.selection())?;
        let data_description_count = measurement_set.data_description()?.row_count();
        if row_predicate.requires_every_source_row(data_description_count)
            && source.selection().rows().selected_row_count()
                != u64::try_from(measurement_set.row_count())
                    .map_err(|_| BoundObservationSourceError::PhysicalRowIndexOverflow)?
        {
            return Err(BoundObservationSourceError::IncompleteUnconditionalRowManifest);
        }
        let geometry_engine = MsCalEngine::new_selected_observation(
            &measurement_set,
            measures.provider(),
            measures.provider_state(),
        )?;
        geometry_engine.verify_selected_observation_measures()?;
        Ok(Self {
            source_identity: source.identity(),
            source_row_count_matches: usize::try_from(source.selection().rows().source_row_count())
                .ok()
                == Some(measurement_set.row_count()),
            measurement_set,
            geometry_engine,
            row_predicate,
            coordinates,
            content_plan,
        })
    }

    #[cfg(all(test, unix))]
    pub(crate) fn open(
        problem: &CompiledProblem,
        source: &ObservationSource,
        current_state: &ObservationSourceState,
        content_budget: SelectedObservationContentBudget,
    ) -> Result<Self, BoundObservationSourceError> {
        let measures = super::measures::test_selected_observation_measures(problem)?;
        let current_state_heap_bytes = current_state
            .additional_retained_heap_bytes([source.selection().rows()])
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        Self::open_with_measures(
            problem,
            source,
            current_state,
            &measures,
            SelectedObservationSharedBytes::new(
                measures.retained_bytes(),
                Self::retained_source_slot_bytes(),
                current_state_heap_bytes,
            ),
            content_budget,
        )
    }

    #[cfg(test)]
    pub(crate) const fn content_plan(&self) -> SelectedObservationContentPlan {
        self.content_plan
    }

    #[cfg(test)]
    pub(crate) fn retained_storage_metadata_bytes(&self) -> Option<usize> {
        self.measurement_set.retained_read_metadata_bytes()
    }

    #[cfg(test)]
    pub(crate) fn predicate_row_manifest_ptr(
        &self,
    ) -> Option<*const casa_imaging_model::SelectedMainRow> {
        self.row_predicate.shared_row_manifest_ptr()
    }

    /// Stream the exact selected samples for this source under the retained read locks.
    ///
    /// Samples are emitted in canonical physical-row, channel, and correlation order. Physical
    /// row blocking comes only from the admitted content plan and is absent from scientific identity.
    pub(crate) fn selected_samples<'a>(
        &'a self,
        problem: &'a CompiledProblem,
    ) -> Result<BoundObservationSamples<'a>, BoundObservationSourceError> {
        self.geometry_engine
            .verify_selected_observation_measures()?;
        let expected = problem
            .selected_observation()
            .read_set()
            .sources()
            .iter()
            .find(|candidate| candidate.measurement_set() == self.source_identity)
            .ok_or(BoundObservationSourceError::ProblemSourceMismatch)?;
        let rows = expected
            .selection()
            .rows()
            .ordered_main_rows()
            .iter()
            .copied();
        Ok(BoundObservationSamples {
            source: self,
            logical_source: expected,
            problem,
            rows,
            pending_row: None,
            active_block: None,
            ready_blocks: VecDeque::with_capacity(self.content_plan.maximum_live_blocks()),
            next_buffer_slot: 0,
            source_exhausted: false,
            terminal_manifest_checked: false,
            pending_error: None,
            live_block_high_water: 0,
            row_offset: 0,
            channel_ordinal: 0,
            correlation_ordinal: 0,
            finished: false,
        })
    }
}

/// One fallible bounded stream of canonical samples from a retained source.
pub(crate) struct BoundObservationSamples<'a> {
    source: &'a BoundObservationSource,
    logical_source: &'a MeasurementSetReadAccess,
    problem: &'a CompiledProblem,
    rows: std::iter::Copied<std::slice::Iter<'a, SelectedMainRow>>,
    pending_row: Option<SelectedMainRow>,
    active_block: Option<BufferedObservationBlock>,
    ready_blocks: VecDeque<BufferedObservationBlock>,
    next_buffer_slot: usize,
    source_exhausted: bool,
    terminal_manifest_checked: bool,
    pending_error: Option<BoundObservationSourceError>,
    live_block_high_water: usize,
    row_offset: usize,
    channel_ordinal: usize,
    correlation_ordinal: usize,
    finished: bool,
}

impl Iterator for BoundObservationSamples<'_> {
    type Item = Result<SelectedObservationSample, BoundObservationSourceError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        loop {
            if let Some(block) = &self.active_block {
                let coordinate_index = block.coordinate_index;
                let coordinates = &self.source.coordinates[coordinate_index];
                if self.row_offset < block.buffer.row_count() {
                    let channel_index = coordinates.channels[self.channel_ordinal].channel_index;
                    let product = coordinates.products[self.correlation_ordinal];
                    let channel_offset = usize::try_from(channel_index)
                        .ok()
                        .and_then(|channel| channel.checked_sub(coordinates.channel_start));
                    let correlation_offset = usize::try_from(product.correlation_index()).ok();
                    let stored = match (channel_offset, correlation_offset) {
                        (Some(channel), Some(correlation)) => {
                            block.buffer.sample(channel, self.row_offset, correlation)
                        }
                        _ => None,
                    };
                    let Some(stored) = stored else {
                        self.finished = true;
                        return Some(Err(BoundObservationSourceError::StoredSampleShapeMismatch));
                    };
                    let sample = self.project_sample(
                        coordinates,
                        coordinates.channels[self.channel_ordinal],
                        product,
                        stored,
                        block.row_geometry[self.row_offset],
                    );
                    self.advance(coordinates.channels.len(), coordinates.products.len());
                    if sample.is_err() {
                        self.finished = true;
                    }
                    return Some(sample);
                }
            }
            self.recycle_active_block();
            self.prefetch_to_capacity();
            if let Some(block) = self.ready_blocks.pop_front() {
                self.active_block = Some(block);
                self.row_offset = 0;
                self.channel_ordinal = 0;
                self.correlation_ordinal = 0;
                self.record_live_block_high_water();
                continue;
            }
            if let Some(error) = self.pending_error.take() {
                self.finished = true;
                return Some(Err(error));
            }
            if let Err(error) = self
                .source
                .geometry_engine
                .verify_selected_observation_measures()
            {
                self.finished = true;
                return Some(Err(error.into()));
            }
            self.finished = true;
            return None;
        }
    }
}

impl BoundObservationSamples<'_> {
    fn advance(&mut self, channel_count: usize, product_count: usize) {
        self.correlation_ordinal += 1;
        if self.correlation_ordinal == product_count {
            self.correlation_ordinal = 0;
            self.channel_ordinal += 1;
            if self.channel_ordinal == channel_count {
                self.channel_ordinal = 0;
                self.row_offset += 1;
            }
        }
    }

    fn recycle_active_block(&mut self) {
        let Some(mut block) = self.active_block.take() else {
            return;
        };
        if self.source_exhausted || self.pending_error.is_some() {
            return;
        }
        debug_assert!(block.slot < self.source.content_plan.maximum_live_blocks());
        match self.fill_next_block(&mut block) {
            Ok(true) => {
                self.ready_blocks.push_back(block);
                self.record_live_block_high_water();
            }
            Ok(false) => self.source_exhausted = true,
            Err(error) => {
                self.source_exhausted = true;
                self.pending_error = Some(error);
            }
        }
    }

    fn prefetch_to_capacity(&mut self) {
        let maximum_live_blocks = self.source.content_plan.maximum_live_blocks();
        while !self.source_exhausted
            && self.pending_error.is_none()
            && self.active_block.is_some() as usize + self.ready_blocks.len() < maximum_live_blocks
        {
            debug_assert!(self.next_buffer_slot < maximum_live_blocks);
            let mut block = BufferedObservationBlock::new(
                self.next_buffer_slot,
                self.source.content_plan.rows_per_block(),
            );
            self.next_buffer_slot += 1;
            match self.fill_next_block(&mut block) {
                Ok(true) => {
                    self.ready_blocks.push_back(block);
                    self.record_live_block_high_water();
                }
                Ok(false) => self.source_exhausted = true,
                Err(error) => {
                    self.source_exhausted = true;
                    self.pending_error = Some(error);
                }
            }
        }
    }

    fn record_live_block_high_water(&mut self) {
        self.live_block_high_water = self
            .live_block_high_water
            .max(self.active_block.is_some() as usize + self.ready_blocks.len());
    }

    fn project_sample(
        &self,
        coordinates: &SelectedCoordinates,
        channel: SelectedChannel,
        product: CorrelationProduct,
        stored: SelectedStoredSample,
        geometry: EvaluatedRowGeometry,
    ) -> Result<SelectedObservationSample, BoundObservationSourceError> {
        let time_scale = time_scale(self.source.geometry_engine.time_reference().as_str())?;
        let physical_row = u64::try_from(stored.physical_row())
            .map_err(|_| BoundObservationSourceError::PhysicalRowIndexOverflow)?;
        let visibility = match stored.visibility() {
            SelectedStoredVisibility::Float32(value) => SelectedVisibilitySample::Float32(value),
            SelectedStoredVisibility::Complex32(value) => {
                SelectedVisibilitySample::Complex32(value)
            }
        };
        let prediction_target = if self
            .problem
            .observation_transaction()
            .write_set()
            .model_columns()
            .iter()
            .any(|write| write.measurement_set() == self.logical_source.measurement_set())
        {
            SelectedPredictionTarget::ModelData
        } else {
            SelectedPredictionTarget::NotRequested
        };
        Ok(SelectedObservationSample {
            address: SelectedSampleAddress {
                measurement_set: self.logical_source.measurement_set(),
                physical_row,
                data_description_id: stored.data_description_id(),
                spectral_window_id: coordinates.data_description.spectral_window_id(),
                channel_index: channel.channel_index,
                frequency_centre_hz: channel.centre_hz,
                frequency_lower_hz: channel.lower_hz,
                frequency_upper_hz: channel.upper_hz,
                channel_width_hz: channel.width_hz,
                frequency_frame: channel.frame,
                polarization_id: coordinates.data_description.polarization_id(),
                correlation_index: product.correlation_index(),
                correlation_type: product.correlation_type(),
            },
            visibility,
            prediction_target,
            channel_flag: stored.channel_flag(),
            row_flag: stored.row_flag(),
            input_weight: stored.input_weight(),
            coordinates: SelectedSampleCoordinates {
                raw_uvw_m: stored.uvw_m(),
                density_uvw_m: geometry.density_uvw_m,
                transformed_uvw_m: geometry.transformed_uvw_m,
                phase_shift_m: geometry.phase_shift_m,
                uvw_law: self.problem.geometry().uvw(),
                time: Epoch::new(stored.time_mjd_seconds() / 86_400.0, time_scale),
                time_centroid: Epoch::new(
                    stored.time_centroid_mjd_seconds() / 86_400.0,
                    time_scale,
                ),
                interval_seconds: stored.interval_seconds(),
                exposure_seconds: stored.exposure_seconds(),
                phase_direction: geometry.phase_direction,
                delay_direction: geometry.delay_direction,
                pointing_directions: geometry.pointing_directions,
            },
            metadata: SelectedSampleMetadata {
                field_id: stored.field_id(),
                antenna1: stored.antenna1(),
                antenna2: stored.antenna2(),
                feed1: stored.feed1(),
                feed2: stored.feed2(),
                scan_number: stored.scan_number(),
                state_id: stored.state_id(),
                observation_id: stored.observation_id(),
                array_id: stored.array_id(),
            },
        })
    }

    #[cfg(test)]
    pub(super) fn scheduling_state(&self) -> (Option<usize>, Vec<(usize, usize)>, usize) {
        (
            self.active_block.as_ref().map(|block| block.slot),
            self.ready_blocks
                .iter()
                .map(|block| {
                    let physical_row = block
                        .buffer
                        .sample(0, 0, 0)
                        .expect("ready selected-observation block contains one sample")
                        .physical_row();
                    (block.slot, physical_row)
                })
                .collect(),
            self.live_block_high_water,
        )
    }

    fn fill_next_block(
        &mut self,
        block: &mut BufferedObservationBlock,
    ) -> Result<bool, BoundObservationSourceError> {
        let first = match self.pending_row.take() {
            Some(row) => row,
            None => match self.rows.next() {
                Some(row) => row,
                None => {
                    if !self.terminal_manifest_checked {
                        self.terminal_manifest_checked = true;
                        if !self.source.source_row_count_matches {
                            return Err(BoundObservationSourceError::SourceRowCountMismatch);
                        }
                    }
                    return Ok(false);
                }
            },
        };
        let coordinate_index = self
            .source
            .coordinates
            .iter()
            .position(|coordinates| {
                coordinates.data_description.data_description_id() == first.data_description_id()
            })
            .ok_or(
                BoundObservationSourceError::DataDescriptionCoordinateMismatch {
                    data_description_id: first.data_description_id(),
                },
            )?;
        let maximum_rows = self.source.content_plan.rows_per_block();
        let mut physical_rows = Vec::with_capacity(maximum_rows);
        physical_rows.push(
            usize::try_from(first.physical_row())
                .map_err(|_| BoundObservationSourceError::PhysicalRowIndexOverflow)?,
        );
        while physical_rows.len() < maximum_rows {
            let Some(row) = self.rows.next() else {
                break;
            };
            if row.data_description_id()
                != self.source.coordinates[coordinate_index]
                    .data_description
                    .data_description_id()
            {
                self.pending_row = Some(row);
                break;
            }
            physical_rows.push(
                usize::try_from(row.physical_row())
                    .map_err(|_| BoundObservationSourceError::PhysicalRowIndexOverflow)?,
            );
        }
        let coordinates = &self.source.coordinates[coordinate_index];
        self.source
            .measurement_set
            .fill_selected_observation_buffer(
                &SelectedObservationBufferRequest::new(
                    selected_visibility(self.logical_source.selected_columns().visibility()),
                    selected_weight(self.logical_source.selected_columns().weights()),
                    physical_rows,
                    VisibilityChannelReadRange::new(
                        coordinates.channel_start,
                        coordinates.channel_count,
                    ),
                ),
                &mut block.buffer,
            )?;
        for row in 0..block.buffer.row_count() {
            let stored = block
                .buffer
                .sample(0, row, 0)
                .ok_or(BoundObservationSourceError::StoredSampleShapeMismatch)?;
            if u32::try_from(stored.data_description_id()).ok()
                != Some(coordinates.data_description.data_description_id())
            {
                return Err(
                    BoundObservationSourceError::DataDescriptionCoordinateMismatch {
                        data_description_id: coordinates.data_description.data_description_id(),
                    },
                );
            }
            if !self
                .source
                .row_predicate
                .matches(StoredMainRow::from(stored))
            {
                return Err(BoundObservationSourceError::SelectedRowPredicateMismatch {
                    physical_row: u64::try_from(stored.physical_row())
                        .map_err(|_| BoundObservationSourceError::PhysicalRowIndexOverflow)?,
                });
            }
        }
        let observation_pointings = evaluate_observation_pointings(
            self.source,
            self.problem,
            &block.buffer,
            self.source.content_plan.rows_per_block(),
            self.source.content_plan.maximum_pointing_polynomial_terms(),
        )?;
        block.row_geometry.clear();
        for row in 0..block.buffer.row_count() {
            let stored = block
                .buffer
                .sample(0, row, 0)
                .ok_or(BoundObservationSourceError::StoredSampleShapeMismatch)?;
            block.row_geometry.push(evaluate_row_geometry(
                self.source,
                self.problem,
                stored,
                observation_pointings
                    .as_ref()
                    .map(|pointings| pointings[row]),
            )?);
        }
        block.coordinate_index = coordinate_index;
        Ok(true)
    }
}

pub(super) struct BufferedObservationBlock {
    slot: usize,
    coordinate_index: usize,
    buffer: SelectedObservationBuffer,
    row_geometry: Vec<EvaluatedRowGeometry>,
}

impl BufferedObservationBlock {
    fn new(slot: usize, rows_per_block: usize) -> Self {
        Self {
            slot,
            coordinate_index: 0,
            buffer: SelectedObservationBuffer::default(),
            row_geometry: Vec::with_capacity(rows_per_block),
        }
    }
}

/// Failure to bind a retained MeasurementSet to one compiled observation source.
#[derive(Debug, Error)]
pub enum BoundObservationSourceError {
    /// The injected Measures provider is missing, stale, or unaccounted.
    #[error(transparent)]
    Measures(#[from] SelectedObservationMeasuresError),
    /// The MeasurementSet could not be opened or read under the admitted content budget.
    #[error(transparent)]
    Storage(#[from] MsError),
    /// Stored DATA_DESCRIPTION metadata contradicted the compiler-owned coordinate catalog.
    #[error(
        "stored DATA_DESCRIPTION row {data_description_id} does not match the compiled coordinate catalog"
    )]
    DataDescriptionCoordinateMismatch {
        /// DATA_DESCRIPTION row that differed.
        data_description_id: u32,
    },
    /// A wavelength UV predicate lacked a positive finite reference wavelength.
    #[error(
        "selected DATA_DESC_ID {data_description_id} has no positive finite reference wavelength"
    )]
    MissingReferenceWavelength {
        /// Selected DATA_DESCRIPTION row without usable spectral metadata.
        data_description_id: u32,
    },
    /// The explicit selected-content memory budget could not realize this source.
    #[error(transparent)]
    ContentPlan(#[from] SelectedObservationContentPlanError),
    /// The retained source is not one exact member of the supplied compiled problem.
    #[error("retained observation source does not match the compiled selected observation")]
    ProblemSourceMismatch,
    /// The fresh source-state probe names a different logical MeasurementSet.
    #[error("current source-state probe names a different MeasurementSet")]
    CurrentSourceIdentityMismatch,
    /// Re-evaluated selected rows differ from the compiled row/DDID manifest.
    #[error("current selected rows differ from the compiled source manifest")]
    StaleSelectedRows,
    /// A selected column, metadata, model-column, or consistency generation changed.
    #[error("current source generations differ from the compiled source snapshot")]
    StaleSourceGenerations,
    /// This first native slice does not yet implement the compiled centre laws.
    #[error(
        "compiled centre laws require a selected-observation geometry evaluator not yet migrated"
    )]
    UnsupportedCentreLaw,
    /// A fixed centre uses a celestial frame not yet migrated into the native tracer.
    #[error("fixed selected-observation centres currently require J2000, found {frame:?}")]
    UnsupportedFixedCentreFrame {
        /// Unsupported fixed celestial frame.
        frame: DirectionFrame,
    },
    /// A stored frequency reference cannot be represented by the selected-sample schema.
    #[error("MeasurementSet frequency reference code {code} is unsupported")]
    UnsupportedFrequencyFrame {
        /// Standard casacore `MFrequency` reference code.
        code: i32,
    },
    /// Compiled sampling and selected coordinates could not form a bounded contribution set.
    #[error("selected sample has no valid bounded spectral contribution mapping")]
    SpectralContributionMismatch,
    /// A stored epoch reference cannot be represented by the selected-sample schema.
    #[error("MeasurementSet epoch reference {name} is unsupported")]
    UnsupportedTimeScale {
        /// Canonical casacore epoch-reference name.
        name: String,
    },
    /// Stored spectral-window coordinates contradicted the compiled channel selection.
    #[error(
        "stored spectral coordinates do not match selected SPECTRAL_WINDOW_ID {spectral_window_id}"
    )]
    SpectralCoordinateMismatch {
        /// Spectral window whose coordinate vectors differed.
        spectral_window_id: u32,
    },
    /// Stored correlation coordinates contradicted the compiled polarization selection.
    #[error(
        "stored correlation coordinates do not match selected POLARIZATION_ID {polarization_id}"
    )]
    CorrelationCoordinateMismatch {
        /// Polarization row whose correlation products differed.
        polarization_id: u32,
    },
    /// A physical MAIN row index did not fit the host storage index domain.
    #[error("selected physical MAIN row index exceeds the host storage index domain")]
    PhysicalRowIndexOverflow,
    /// Retained MAIN cardinality differs from the compiler-owned source manifest.
    #[error("retained MAIN row count differs from the compiled source manifest")]
    SourceRowCountMismatch,
    /// An unconditional resolved selector omitted one or more retained MAIN rows.
    #[error("compiled unconditional row selection omits retained MAIN rows")]
    IncompleteUnconditionalRowManifest,
    /// The bounded storage block did not contain one compiled sample coordinate.
    #[error("bounded selected-observation storage block has an inconsistent sample shape")]
    StoredSampleShapeMismatch,
    /// Stored row geometry could not be represented by the compiled sample schema.
    #[error("stored selected-observation row geometry is invalid")]
    InvalidRowGeometry,
    /// One manifest-listed row no longer satisfies the compiled resolved selector.
    #[error(
        "selected physical MAIN row {physical_row} no longer satisfies the compiled row selection"
    )]
    SelectedRowPredicateMismatch {
        /// Manifest-listed physical MAIN row.
        physical_row: u64,
    },
    /// Observation POINTING evaluation was required but no per-antenna result was supplied.
    #[error("selected-observation row is missing evaluated POINTING directions")]
    MissingEvaluatedPointingDirections,
    /// No POINTING row exists for one required antenna and epoch.
    #[error("POINTING has no direction for antenna {antenna_id} at MJD seconds {time_mjd_seconds}")]
    MissingPointingDirection {
        /// Required antenna.
        antenna_id: i32,
        /// Required MAIN epoch in MJD seconds.
        time_mjd_seconds: f64,
    },
    /// The query epoch lies outside POINTING coverage and extrapolation is forbidden.
    #[error("POINTING coverage excludes antenna {antenna_id} at MJD seconds {time_mjd_seconds}")]
    PointingOutsideCoverage {
        /// Required antenna.
        antenna_id: i32,
        /// Required MAIN epoch in MJD seconds.
        time_mjd_seconds: f64,
    },
    /// Great-circle POINTING interpolation produced an invalid direction.
    #[error("POINTING great-circle interpolation is undefined")]
    InvalidPointingInterpolation,
}

pub(super) struct SelectedCoordinates {
    data_description: DataDescriptionSelection,
    channels: Box<[SelectedChannel]>,
    products: Box<[CorrelationProduct]>,
    channel_start: usize,
    channel_count: usize,
}

#[derive(Clone, Copy)]
pub(super) struct SelectedChannel {
    channel_index: u32,
    centre_hz: f64,
    lower_hz: f64,
    upper_hz: f64,
    width_hz: f64,
    frame: FrequencyFrame,
}

fn selected_coordinates(
    measurement_set: &MeasurementSet,
    selection: &ObservationSelection,
) -> Result<Box<[SelectedCoordinates]>, BoundObservationSourceError> {
    let spectral_windows = measurement_set.spectral_window()?;
    let polarizations = measurement_set.polarization()?;
    let mut coordinates = Vec::with_capacity(selection.data_descriptions().len());
    for data_description in selection.data_descriptions().iter().copied() {
        let spectral_window = selection
            .spectral_windows()
            .iter()
            .find(|candidate| {
                candidate.spectral_window_id() == data_description.spectral_window_id()
            })
            .expect("compiled DATA_DESCRIPTION has one spectral-window selection");
        let spw_row = usize::try_from(data_description.spectral_window_id()).map_err(|_| {
            BoundObservationSourceError::SpectralCoordinateMismatch {
                spectral_window_id: data_description.spectral_window_id(),
            }
        })?;
        let Some(casa_types::ArrayValue::Float64(centres)) = spectral_windows
            .table()
            .column_accessor("CHAN_FREQ")
            .map_err(MsError::from)?
            .array_cells_owned_uncached(&[spw_row])
            .map_err(MsError::from)?
            .pop()
            .flatten()
        else {
            return Err(BoundObservationSourceError::SpectralCoordinateMismatch {
                spectral_window_id: data_description.spectral_window_id(),
            });
        };
        let Some(casa_types::ArrayValue::Float64(widths)) = spectral_windows
            .table()
            .column_accessor("CHAN_WIDTH")
            .map_err(MsError::from)?
            .array_cells_owned_uncached(&[spw_row])
            .map_err(MsError::from)?
            .pop()
            .flatten()
        else {
            return Err(BoundObservationSourceError::SpectralCoordinateMismatch {
                spectral_window_id: data_description.spectral_window_id(),
            });
        };
        let frame = frequency_frame(selected_i32_scalar(
            spectral_windows.table(),
            "MEAS_FREQ_REF",
            spw_row,
        )?)?;
        let mut channels = Vec::with_capacity(spectral_window.channel_indices().len());
        for &channel_index in spectral_window.channel_indices() {
            let channel = usize::try_from(channel_index).map_err(|_| {
                BoundObservationSourceError::SpectralCoordinateMismatch {
                    spectral_window_id: data_description.spectral_window_id(),
                }
            })?;
            let centre_hz = *centres.get(channel).ok_or(
                BoundObservationSourceError::SpectralCoordinateMismatch {
                    spectral_window_id: data_description.spectral_window_id(),
                },
            )?;
            let width_hz = *widths.get(channel).ok_or(
                BoundObservationSourceError::SpectralCoordinateMismatch {
                    spectral_window_id: data_description.spectral_window_id(),
                },
            )?;
            let first_edge = centre_hz - 0.5 * width_hz;
            let second_edge = centre_hz + 0.5 * width_hz;
            if ![centre_hz, width_hz, first_edge, second_edge]
                .into_iter()
                .all(f64::is_finite)
            {
                return Err(BoundObservationSourceError::SpectralCoordinateMismatch {
                    spectral_window_id: data_description.spectral_window_id(),
                });
            }
            channels.push(SelectedChannel {
                channel_index,
                centre_hz,
                lower_hz: first_edge.min(second_edge),
                upper_hz: first_edge.max(second_edge),
                width_hz,
                frame,
            });
        }
        let channel_start = channels
            .first()
            .and_then(|channel| usize::try_from(channel.channel_index).ok())
            .expect("compiled spectral-window selection is nonempty");
        let channel_end = channels
            .last()
            .and_then(|channel| usize::try_from(channel.channel_index).ok())
            .and_then(|channel| channel.checked_add(1))
            .ok_or(BoundObservationSourceError::SpectralCoordinateMismatch {
                spectral_window_id: data_description.spectral_window_id(),
            })?;
        drop(centres);
        drop(widths);
        let polarization = selection
            .correlations()
            .iter()
            .find(|candidate| candidate.polarization_id() == data_description.polarization_id())
            .expect("compiled DATA_DESCRIPTION has one polarization selection");
        let polarization_row =
            usize::try_from(data_description.polarization_id()).map_err(|_| {
                BoundObservationSourceError::CorrelationCoordinateMismatch {
                    polarization_id: data_description.polarization_id(),
                }
            })?;
        let Some(casa_types::ArrayValue::Int32(stored_products)) = polarizations
            .table()
            .column_accessor("CORR_TYPE")
            .map_err(MsError::from)?
            .array_cells_owned_uncached(&[polarization_row])
            .map_err(MsError::from)?
            .pop()
            .flatten()
        else {
            return Err(BoundObservationSourceError::CorrelationCoordinateMismatch {
                polarization_id: data_description.polarization_id(),
            });
        };
        for product in polarization.products() {
            let product_index = usize::try_from(product.correlation_index()).map_err(|_| {
                BoundObservationSourceError::CorrelationCoordinateMismatch {
                    polarization_id: data_description.polarization_id(),
                }
            })?;
            if stored_products
                .get(product_index)
                .and_then(|code| correlation_type(*code))
                != Some(product.correlation_type())
            {
                return Err(BoundObservationSourceError::CorrelationCoordinateMismatch {
                    polarization_id: data_description.polarization_id(),
                });
            }
        }
        coordinates.push(SelectedCoordinates {
            data_description,
            channels: channels.into_boxed_slice(),
            products: polarization.products().into(),
            channel_start,
            channel_count: channel_end - channel_start,
        });
    }
    Ok(coordinates.into_boxed_slice())
}

pub(crate) fn validate_selected_coordinates(
    measurement_set: &MeasurementSet,
    selection: &ObservationSelection,
) -> Result<(), BoundObservationSourceError> {
    selected_coordinates(measurement_set, selection).map(drop)
}

#[derive(Clone, Copy)]
pub(super) struct EvaluatedRowGeometry {
    density_uvw_m: [f64; 3],
    transformed_uvw_m: [f64; 3],
    phase_shift_m: f64,
    phase_direction: SkyDirection,
    delay_direction: SkyDirection,
    pointing_directions: SelectedPointingDirections,
}

fn evaluate_row_geometry(
    source: &BoundObservationSource,
    problem: &CompiledProblem,
    stored: SelectedStoredSample,
    observation_pointing: Option<SelectedPointingDirections>,
) -> Result<EvaluatedRowGeometry, BoundObservationSourceError> {
    let field_id = usize::try_from(stored.field_id())
        .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
    let (longitude_rad, latitude_rad) = source
        .geometry_engine
        .field_direction_j2000(field_id)?
        .as_angles();
    let observation_direction =
        SkyDirection::new(DirectionFrame::J2000, longitude_rad, latitude_rad);
    let centres = problem.geometry().centres();
    let phase_direction = match centres.phase_tracking() {
        PhaseCentreLaw::Observation => observation_direction,
        PhaseCentreLaw::Fixed(direction) => require_fixed_j2000(*direction)?,
        PhaseCentreLaw::Ephemeris(_) => {
            return Err(BoundObservationSourceError::UnsupportedCentreLaw);
        }
    };
    let delay_direction = match centres.delay() {
        DelayCentreLaw::PhaseTrackingCentre => phase_direction,
        DelayCentreLaw::Observation => observation_direction,
        DelayCentreLaw::Fixed(direction) => require_fixed_j2000(*direction)?,
    };
    let pointing_directions = match centres.pointing() {
        PointingCentreLaw::PhaseTrackingCentre => SelectedPointingDirections {
            antenna1: phase_direction,
            antenna2: phase_direction,
        },
        PointingCentreLaw::Fixed(direction) => {
            let direction = require_fixed_j2000(*direction)?;
            SelectedPointingDirections {
                antenna1: direction,
                antenna2: direction,
            }
        }
        PointingCentreLaw::Observation(_) => observation_pointing
            .ok_or(BoundObservationSourceError::MissingEvaluatedPointingDirections)?,
    };
    let (density_uvw_m, transformed_uvw_m, phase_shift_m) =
        if matches!(centres.phase_tracking(), PhaseCentreLaw::Observation) {
            (stored.uvw_m(), stored.uvw_m(), 0.0)
        } else {
            let target = [
                phase_direction.longitude_rad(),
                phase_direction.latitude_rad(),
            ];
            let density_uvw_m = source
                .geometry_engine
                .reproject_raw_uvw_for_density_to_j2000(stored.uvw_m(), field_id, target)?;
            let (transformed_uvw_m, phase_shift_m) = source
                .geometry_engine
                .reproject_raw_uvw_to_j2000(stored.uvw_m(), field_id, target)?;
            (density_uvw_m, transformed_uvw_m, phase_shift_m)
        };
    Ok(EvaluatedRowGeometry {
        density_uvw_m,
        transformed_uvw_m,
        phase_shift_m,
        phase_direction,
        delay_direction,
        pointing_directions,
    })
}

fn evaluate_observation_pointings(
    source: &BoundObservationSource,
    problem: &CompiledProblem,
    buffer: &SelectedObservationBuffer,
    scan_rows_per_block: usize,
    maximum_polynomial_terms: usize,
) -> Result<Option<Vec<SelectedPointingDirections>>, BoundObservationSourceError> {
    let PointingCentreLaw::Observation(law) = problem.geometry().centres().pointing() else {
        return Ok(None);
    };
    let mut queries = Vec::with_capacity(buffer.row_count().saturating_mul(2));
    let mut phase_directions = Vec::with_capacity(buffer.row_count());
    for row in 0..buffer.row_count() {
        let stored = buffer
            .sample(0, row, 0)
            .ok_or(BoundObservationSourceError::StoredSampleShapeMismatch)?;
        let time_mjd_seconds = match law.time_sampling() {
            PointingTimeSampling::VisibilityTime => stored.time_mjd_seconds(),
            PointingTimeSampling::VisibilityTimeCentroid => stored.time_centroid_mjd_seconds(),
        };
        queries.push(PointingDirectionQuery::new(
            stored.antenna1(),
            time_mjd_seconds,
        )?);
        queries.push(PointingDirectionQuery::new(
            stored.antenna2(),
            time_mjd_seconds,
        )?);
        phase_directions.push(evaluate_phase_direction(source, problem, stored)?);
    }
    let column = match law.direction_column() {
        PointingDirectionColumn::Direction => StoredPointingDirectionColumn::Direction,
        PointingDirectionColumn::Target => StoredPointingDirectionColumn::Target,
    };
    let brackets = source.measurement_set.pointing_direction_brackets(
        &source.geometry_engine,
        column,
        &queries,
        PointingReadPlan::new(scan_rows_per_block, maximum_polynomial_terms)?,
    )?;
    let mut pointings = Vec::with_capacity(buffer.row_count());
    for ((row, antenna_brackets), fallback) in
        brackets.chunks_exact(2).enumerate().zip(phase_directions)
    {
        pointings.push(SelectedPointingDirections {
            antenna1: resolve_pointing_direction(
                antenna_brackets[0],
                queries[2 * row],
                *law,
                fallback,
            )?,
            antenna2: resolve_pointing_direction(
                antenna_brackets[1],
                queries[2 * row + 1],
                *law,
                fallback,
            )?,
        });
    }
    Ok(Some(pointings))
}

fn evaluate_phase_direction(
    source: &BoundObservationSource,
    problem: &CompiledProblem,
    stored: SelectedStoredSample,
) -> Result<SkyDirection, BoundObservationSourceError> {
    let field_id = usize::try_from(stored.field_id())
        .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
    let (longitude_rad, latitude_rad) = source
        .geometry_engine
        .field_direction_j2000(field_id)?
        .as_angles();
    let observation = SkyDirection::new(DirectionFrame::J2000, longitude_rad, latitude_rad);
    match problem.geometry().centres().phase_tracking() {
        PhaseCentreLaw::Observation => Ok(observation),
        PhaseCentreLaw::Fixed(direction) => require_fixed_j2000(*direction),
        PhaseCentreLaw::Ephemeris(_) => Err(BoundObservationSourceError::UnsupportedCentreLaw),
    }
}

fn resolve_pointing_direction(
    bracket: PointingDirectionBracket,
    query: PointingDirectionQuery,
    law: casa_imaging_model::ObservationPointingLaw,
    fallback: SkyDirection,
) -> Result<SkyDirection, BoundObservationSourceError> {
    if let Some(covering) = bracket.covering() {
        return Ok(j2000_direction(covering.direction_j2000_rad()));
    }
    let direction = match (bracket.before(), bracket.after()) {
        (Some(before), Some(after)) if before.row_index() == after.row_index() => {
            before.direction_j2000_rad()
        }
        (Some(before), Some(after)) => match law.interpolation() {
            PointingInterpolation::Nearest => {
                let before_distance = query.time_mjd_seconds() - before.row_time_mjd_seconds();
                let after_distance = after.row_time_mjd_seconds() - query.time_mjd_seconds();
                if before_distance <= after_distance {
                    before.direction_j2000_rad()
                } else {
                    after.direction_j2000_rad()
                }
            }
            PointingInterpolation::GreatCircleShortestArc => interpolate_direction(
                before.direction_j2000_rad(),
                after.direction_j2000_rad(),
                (query.time_mjd_seconds() - before.row_time_mjd_seconds())
                    / (after.row_time_mjd_seconds() - before.row_time_mjd_seconds()),
            )?,
        },
        (Some(endpoint), None) | (None, Some(endpoint))
            if matches!(law.extrapolation(), PointingExtrapolation::HoldNearest) =>
        {
            endpoint.direction_j2000_rad()
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(BoundObservationSourceError::PointingOutsideCoverage {
                antenna_id: query.antenna_id(),
                time_mjd_seconds: query.time_mjd_seconds(),
            });
        }
        (None, None) if matches!(law.missing(), MissingPointingPolicy::UsePhaseTrackingCentre) => {
            return Ok(fallback);
        }
        (None, None) => {
            return Err(BoundObservationSourceError::MissingPointingDirection {
                antenna_id: query.antenna_id(),
                time_mjd_seconds: query.time_mjd_seconds(),
            });
        }
    };
    Ok(j2000_direction(direction))
}

fn j2000_direction(direction_rad: [f64; 2]) -> SkyDirection {
    SkyDirection::new(DirectionFrame::J2000, direction_rad[0], direction_rad[1])
}

fn interpolate_direction(
    before: [f64; 2],
    after: [f64; 2],
    fraction: f64,
) -> Result<[f64; 2], BoundObservationSourceError> {
    if !(fraction.is_finite() && (0.0..=1.0).contains(&fraction)) {
        return Err(BoundObservationSourceError::InvalidPointingInterpolation);
    }
    let left = unit_direction(before);
    let right = unit_direction(after);
    let cosine = left
        .iter()
        .zip(right)
        .map(|(left, right)| left * right)
        .sum::<f64>()
        .clamp(-1.0, 1.0);
    let angle = cosine.acos();
    let vector = if angle.abs() < 1.0e-15 {
        left
    } else {
        let denominator = angle.sin();
        let before_weight = ((1.0 - fraction) * angle).sin() / denominator;
        let after_weight = (fraction * angle).sin() / denominator;
        [
            before_weight * left[0] + after_weight * right[0],
            before_weight * left[1] + after_weight * right[1],
            before_weight * left[2] + after_weight * right[2],
        ]
    };
    let norm = vector.iter().map(|value| value * value).sum::<f64>().sqrt();
    if !(norm.is_finite() && norm > 0.0) {
        return Err(BoundObservationSourceError::InvalidPointingInterpolation);
    }
    let [x, y, z] = vector.map(|value| value / norm);
    Ok([y.atan2(x).rem_euclid(std::f64::consts::TAU), z.asin()])
}

fn unit_direction(direction: [f64; 2]) -> [f64; 3] {
    [
        direction[1].cos() * direction[0].cos(),
        direction[1].cos() * direction[0].sin(),
        direction[1].sin(),
    ]
}

fn require_fixed_j2000(
    direction: SkyDirection,
) -> Result<SkyDirection, BoundObservationSourceError> {
    if direction.frame() != DirectionFrame::J2000 {
        return Err(BoundObservationSourceError::UnsupportedFixedCentreFrame {
            frame: direction.frame(),
        });
    }
    Ok(direction)
}

const fn selected_visibility(visibility: VisibilityColumn) -> SelectedVisibilityColumn {
    match visibility {
        VisibilityColumn::Data => SelectedVisibilityColumn::Data,
        VisibilityColumn::CorrectedData => SelectedVisibilityColumn::CorrectedData,
        VisibilityColumn::FloatData => SelectedVisibilityColumn::FloatData,
    }
}

const fn selected_weight(weight: WeightColumn) -> SelectedWeightColumn {
    match weight {
        WeightColumn::Weight => SelectedWeightColumn::Weight,
        WeightColumn::WeightSpectrum => SelectedWeightColumn::WeightSpectrum,
    }
}

const fn frequency_frame(code: i32) -> Result<FrequencyFrame, BoundObservationSourceError> {
    match code {
        1 => Ok(FrequencyFrame::Lsrk),
        3 => Ok(FrequencyFrame::Barycentric),
        5 => Ok(FrequencyFrame::Topocentric),
        _ => Err(BoundObservationSourceError::UnsupportedFrequencyFrame { code }),
    }
}

fn time_scale(name: &str) -> Result<TimeScale, BoundObservationSourceError> {
    match name {
        "UTC" => Ok(TimeScale::Utc),
        "TAI" => Ok(TimeScale::Tai),
        "TT" | "TDT" => Ok(TimeScale::Tt),
        "TDB" => Ok(TimeScale::Tdb),
        _ => Err(BoundObservationSourceError::UnsupportedTimeScale {
            name: name.to_string(),
        }),
    }
}

const fn correlation_type(code: i32) -> Option<CorrelationType> {
    Some(match code {
        1 => CorrelationType::StokesI,
        2 => CorrelationType::StokesQ,
        3 => CorrelationType::StokesU,
        4 => CorrelationType::StokesV,
        5 => CorrelationType::CircularRr,
        6 => CorrelationType::CircularRl,
        7 => CorrelationType::CircularLr,
        8 => CorrelationType::CircularLl,
        9 => CorrelationType::LinearXx,
        10 => CorrelationType::LinearXy,
        11 => CorrelationType::LinearYx,
        12 => CorrelationType::LinearYy,
        13 => CorrelationType::MixedRx,
        14 => CorrelationType::MixedRy,
        15 => CorrelationType::MixedLx,
        16 => CorrelationType::MixedLy,
        17 => CorrelationType::MixedXr,
        18 => CorrelationType::MixedXl,
        19 => CorrelationType::MixedYr,
        20 => CorrelationType::MixedYl,
        21 => CorrelationType::QuasiOrthogonalPp,
        22 => CorrelationType::QuasiOrthogonalPq,
        23 => CorrelationType::QuasiOrthogonalQp,
        24 => CorrelationType::QuasiOrthogonalQq,
        25 => CorrelationType::RightCircular,
        26 => CorrelationType::LeftCircular,
        27 => CorrelationType::Linear,
        28 => CorrelationType::PolarizedIntensity,
        29 => CorrelationType::LinearPolarizedIntensity,
        30 => CorrelationType::FractionalPolarizedIntensity,
        31 => CorrelationType::FractionalLinearPolarizedIntensity,
        32 => CorrelationType::PolarizationAngle,
        _ => return None,
    })
}

fn validate_current_state(
    expected: &ObservationSource,
    current: &ObservationSourceState,
) -> Result<(), BoundObservationSourceError> {
    if current.identity() != expected.identity() {
        return Err(BoundObservationSourceError::CurrentSourceIdentityMismatch);
    }
    if current.selected_rows() != expected.selection().rows() {
        return Err(BoundObservationSourceError::StaleSelectedRows);
    }
    if current.generations() != expected.generations() {
        return Err(BoundObservationSourceError::StaleSourceGenerations);
    }
    Ok(())
}

fn selected_row_predicate(
    measurement_set: &MeasurementSet,
    source: &ObservationSource,
) -> Result<CompiledRowPredicate, BoundObservationSourceError> {
    let selection = source.selection();
    let wavelengths = validate_data_descriptions(measurement_set, selection)?;
    CompiledRowPredicate::new_shared(source, |data_description_id| {
        wavelengths
            .iter()
            .find(|(candidate, _)| *candidate == data_description_id)
            .map(|(_, wavelength_m)| *wavelength_m)
    })
    .map_err(|error| match error {
        RowSelectionEvaluationError::MissingReferenceWavelength {
            data_description_id,
        } => BoundObservationSourceError::MissingReferenceWavelength {
            data_description_id,
        },
    })
}

fn validate_data_descriptions(
    measurement_set: &MeasurementSet,
    selection: &ObservationSelection,
) -> Result<Vec<(u32, f64)>, BoundObservationSourceError> {
    let data_descriptions = measurement_set.data_description()?;
    let spectral_windows = measurement_set.spectral_window()?;
    let mut wavelengths = Vec::with_capacity(selection.data_descriptions().len());
    for expected in selection.data_descriptions() {
        let data_description_id = expected.data_description_id();
        let row = usize::try_from(data_description_id).map_err(|_| {
            BoundObservationSourceError::DataDescriptionCoordinateMismatch {
                data_description_id,
            }
        })?;
        let spectral_window_id =
            selected_i32_scalar(data_descriptions.table(), "SPECTRAL_WINDOW_ID", row)?;
        let polarization_id =
            selected_i32_scalar(data_descriptions.table(), "POLARIZATION_ID", row)?;
        if u32::try_from(spectral_window_id).ok() != Some(expected.spectral_window_id())
            || u32::try_from(polarization_id).ok() != Some(expected.polarization_id())
        {
            return Err(
                BoundObservationSourceError::DataDescriptionCoordinateMismatch {
                    data_description_id,
                },
            );
        }
        let spectral_window_row = usize::try_from(spectral_window_id).map_err(|_| {
            BoundObservationSourceError::DataDescriptionCoordinateMismatch {
                data_description_id,
            }
        })?;
        let reference_frequency_hz = selected_f64_scalar(
            spectral_windows.table(),
            "REF_FREQUENCY",
            spectral_window_row,
        )?;
        wavelengths.push((
            data_description_id,
            SPEED_OF_LIGHT_M_PER_S / reference_frequency_hz,
        ));
    }
    Ok(wavelengths)
}

fn selected_i32_scalar(
    table: &casa_tables::Table,
    column: &str,
    row: usize,
) -> Result<i32, BoundObservationSourceError> {
    match table
        .column_accessor(column)
        .map_err(MsError::from)?
        .scalar_cells_owned_for_rows(&[row])
        .map_err(MsError::from)?
        .pop()
        .flatten()
    {
        Some(casa_types::ScalarValue::Int32(value)) => Ok(value),
        value => Err(MsError::InvalidInput(format!(
            "required selected metadata {column} row {row} is not Int32: {value:?}"
        ))
        .into()),
    }
}

fn selected_f64_scalar(
    table: &casa_tables::Table,
    column: &str,
    row: usize,
) -> Result<f64, BoundObservationSourceError> {
    match table
        .column_accessor(column)
        .map_err(MsError::from)?
        .scalar_cells_owned_for_rows(&[row])
        .map_err(MsError::from)?
        .pop()
        .flatten()
    {
        Some(casa_types::ScalarValue::Float64(value)) => Ok(value),
        value => Err(MsError::InvalidInput(format!(
            "required selected metadata {column} row {row} is not Float64: {value:?}"
        ))
        .into()),
    }
}
