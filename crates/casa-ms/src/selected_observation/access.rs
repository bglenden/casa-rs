// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
    sync::Arc,
};

use crate::derived::engine::{MsCalEngine, polarization_operator_angle};
use crate::spectral_selection::PreparedFrequencyFrameConversion;
use crate::subtables::SubTable;
use crate::{
    MainRowSelectionCursor, MeasurementSet, MsError, MsReadPlan, MsSelectionIoBudget,
    ObservationOwnerError, PointingDirectionBracket,
    PointingDirectionColumn as StoredPointingDirectionColumn, PointingDirectionQuery,
    PointingReadPlan, PreparedSelectedPointingCatalog, SelectedObservationBuffer,
    SelectedObservationBufferRequest, SelectedObservationRow, SelectedPointingQueryDomain,
    SelectedStoredRow, SelectedVisibilityColumn, SelectedWeightColumn, VisibilityChannelReadRange,
};
use casa_coordinates::{
    Coordinate, DirectionCoordinate, Projection as CoordinateProjection, ProjectionType,
};
use casa_imaging_model::{
    CompiledProblem, CorrelationProduct, CorrelationType, DataDescriptionSelection, DirectionFrame,
    Epoch, FrequencyFrame, InstrumentModel, MeasurementSetReadAccess, MissingPointingPolicy,
    ObservationSelection, ObservationSource, PhaseCentreLaw, PointingCentreLaw,
    PointingDirectionColumn, PointingExtrapolation, PointingInterpolation, PointingTimeSampling,
    Projection as ModelProjection, SelectedAntennaResponses, SelectedImageDomainProjection,
    SelectedImageDomainProjections, SelectedObservationRunChannel, SelectedObservationRunRow,
    SelectedPhaseCentreProjection, SelectedPointingDirections, SelectedSampleCoordinates,
    SelectedSampleMetadata, SkyDirection, TimeScale, VisibilityColumn, WeightColumn,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use ndarray::arr2;
use thiserror::Error;

use super::spectral_evaluation::prepare_row_frequency_conversion;
use super::{
    SelectedObservationContentBudget, SelectedObservationContentPlan,
    SelectedObservationContentPlanError, SelectedObservationMeasures,
    SelectedObservationMeasuresError,
    content_plan::{
        SelectedObservationContentRequirements, SelectedObservationSharedBytes,
        construction_scratch_fits_slack, selected_content_plan_with_pointing_catalog,
        selected_content_requirements, selected_pointing_catalog_budget,
    },
    row_selection::{CompiledRowPredicate, RowSelectionEvaluationError},
};

const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;
const AW_POINTING_PLAN_BYTES_PER_SELECTED_ROW: usize = 384;

fn aw_pointing_plan_retained_byte_ceiling(
    problem: &CompiledProblem,
    source: &ObservationSource,
) -> Result<usize, BoundObservationSourceError> {
    let enabled = problem
        .science()
        .measurement_equation()
        .aw_projection()
        .is_some_and(|contract| contract.use_pointing());
    if !enabled {
        return Ok(0);
    }
    usize::try_from(source.selection().rows().selected_row_count())
        .ok()
        .and_then(|rows| rows.checked_mul(AW_POINTING_PLAN_BYTES_PER_SELECTED_ROW))
        .and_then(|bytes| bytes.checked_add(size_of::<AwPointingEpochPlan>()))
        .ok_or(BoundObservationSourceError::MeasurementOverflow)
}

pub(crate) struct BoundObservationReferenceData<'a> {
    ephemeris: Option<&'a Arc<crate::SelectedObservationEphemeris>>,
    pointing_query_domain: Option<&'a SelectedPointingQueryDomain>,
}

impl<'a> BoundObservationReferenceData<'a> {
    pub(crate) const fn new(
        ephemeris: Option<&'a Arc<crate::SelectedObservationEphemeris>>,
        pointing_query_domain: Option<&'a SelectedPointingQueryDomain>,
    ) -> Self {
        Self {
            ephemeris,
            pointing_query_domain,
        }
    }
}

/// A retained, read-locked MeasurementSet bound to one compiled source selection.
///
/// Construction opens the retained storage capability and metadata needed to plan the bounded
/// content buffers. Each pass walks MAIN once, in physical order, selecting rows with the
/// compiled row predicate and reading their values.
pub(crate) struct BoundObservationSource {
    measurement_set: MeasurementSet,
    geometry_engine: Arc<MsCalEngine>,
    row_predicate: CompiledRowPredicate,
    coordinates: Arc<[SelectedCoordinates]>,
    pointing_catalog: Option<PreparedSelectedPointingCatalog>,
    aw_pointing_plan: Option<AwPointingEpochPlan>,
    content_plan: SelectedObservationContentPlan,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AwPointingEpochKey {
    data_description_id: i32,
    field_id: i32,
    time_bits: u64,
}

#[derive(Debug)]
struct AwPointingEpochInput {
    key: AwPointingEpochKey,
    row_count: usize,
    antennas: BTreeSet<i32>,
}

/// Antenna pointing-centre pixels by field for one grouped pointing epoch.
type AwPointingChartPixels = Arc<[BTreeMap<i32, [f64; 2]>]>;

#[derive(Debug)]
struct AwPointingEpochPlan {
    chart_pixels_by_epoch: BTreeMap<AwPointingEpochKey, AwPointingChartPixels>,
    retained_byte_ceiling: usize,
}

impl BoundObservationSource {
    pub(super) const fn row_replay_fixed_bytes() -> usize {
        size_of::<SelectedRowReplay>()
    }

    pub(super) const fn row_replay_bytes_per_row() -> usize {
        MainRowSelectionCursor::retained_bytes_per_row()
    }

    pub(super) fn geometry_engine(&self) -> &MsCalEngine {
        self.geometry_engine.as_ref()
    }

    /// The CASA aperture classes of this source's antennas (`None` for a
    /// dish outside the ALMA and ACA classes), in ANTENNA row order.
    pub(crate) fn antenna_response_classes(
        &self,
    ) -> &[Option<casa_imaging_model::AntennaResponseClass>] {
        self.geometry_engine.antenna_response_classes()
    }

    pub(super) const fn rows_per_block(&self) -> usize {
        self.content_plan.rows_per_block()
    }

    /// Open the source locator under retained read locks without traversing MAIN rows.
    #[cfg(unix)]
    pub(crate) fn open_with_measures(
        problem: &CompiledProblem,
        source: &ObservationSource,
        measures: &SelectedObservationMeasures,
        shared_bytes: SelectedObservationSharedBytes,
        content_budget: SelectedObservationContentBudget,
        reference_data: BoundObservationReferenceData<'_>,
    ) -> Result<Self, BoundObservationSourceError> {
        let measurement_set = MeasurementSet::open_retained_read(source.provenance().locator())?;
        Self::from_locked_measurement_set(
            problem,
            source,
            measures,
            shared_bytes,
            content_budget,
            measurement_set,
            reference_data.ephemeris,
            reference_data.pointing_query_domain,
        )
    }

    #[cfg(unix)]
    pub(crate) fn content_requirements(
        problem: &CompiledProblem,
        source: &ObservationSource,
        binding: &super::ObservationSourceBinding,
        shared_bytes: SelectedObservationSharedBytes,
    ) -> Result<SelectedObservationContentRequirements, BoundObservationSourceError> {
        let measurement_set = MeasurementSet::open_retained_read(source.provenance().locator())?;
        Self::requirements_for_locked_source(
            &measurement_set,
            problem,
            source,
            shared_bytes,
            binding.content_budget().maximum_pointing_polynomial_terms(),
            binding.pointing_query_domain(),
        )
    }

    pub(super) fn requirements_for_locked_source(
        measurement_set: &MeasurementSet,
        problem: &CompiledProblem,
        source: &ObservationSource,
        shared_bytes: SelectedObservationSharedBytes,
        maximum_pointing_polynomial_terms: usize,
        pointing_query_domain: Option<&SelectedPointingQueryDomain>,
    ) -> Result<SelectedObservationContentRequirements, BoundObservationSourceError> {
        let (catalog, scan_bytes_per_row) = if matches!(
            problem.geometry().centres().pointing(),
            PointingCentreLaw::Observation(_)
        ) {
            let domain = pointing_query_domain
                .ok_or(BoundObservationSourceError::MissingPointingQueryDomain)?;
            let (catalog, scan) = measurement_set.selected_pointing_catalog_requirements(
                domain,
                maximum_pointing_polynomial_terms,
            )?;
            (Some(catalog), scan)
        } else {
            (None, 0)
        };
        Ok(selected_content_requirements(
            measurement_set,
            problem,
            source,
            shared_bytes.with_source_plan_retained_bytes(aw_pointing_plan_retained_byte_ceiling(
                problem, source,
            )?),
            maximum_pointing_polynomial_terms,
            catalog,
            scan_bytes_per_row,
        )?)
    }

    #[allow(clippy::too_many_arguments)]
    fn from_locked_measurement_set(
        problem: &CompiledProblem,
        source: &ObservationSource,
        measures: &SelectedObservationMeasures,
        shared_bytes: SelectedObservationSharedBytes,
        content_budget: SelectedObservationContentBudget,
        measurement_set: MeasurementSet,
        ephemeris: Option<&Arc<crate::SelectedObservationEphemeris>>,
        pointing_query_domain: Option<&SelectedPointingQueryDomain>,
    ) -> Result<Self, BoundObservationSourceError> {
        debug_assert!(
            construction_scratch_fits_slack(source, pointing_query_domain),
            "the POINTING query domain and DATA_DESCRIPTION table fit the construction slack"
        );
        let aw_pointing_plan_ceiling = aw_pointing_plan_retained_byte_ceiling(problem, source)?;
        let shared_bytes = shared_bytes.with_source_plan_retained_bytes(aw_pointing_plan_ceiling);
        let preliminary_content_plan = Self::requirements_for_locked_source(
            &measurement_set,
            problem,
            source,
            shared_bytes,
            content_budget.maximum_pointing_polynomial_terms(),
            pointing_query_domain,
        )?
        .plan(content_budget)?;
        let pointing_catalog = if let PointingCentreLaw::Observation(law) =
            problem.geometry().centres().pointing()
        {
            let domain = pointing_query_domain
                .ok_or(BoundObservationSourceError::MissingPointingQueryDomain)?;
            let column = match law.direction_column() {
                PointingDirectionColumn::Direction => StoredPointingDirectionColumn::Direction,
                PointingDirectionColumn::Target => StoredPointingDirectionColumn::Target,
            };
            let catalog_budget = selected_pointing_catalog_budget(
                &measurement_set,
                source,
                shared_bytes,
                content_budget,
            )?;
            let catalog = measurement_set.prepare_selected_pointing_catalog(
                column,
                domain,
                law.time_sampling(),
                PointingReadPlan::new(
                    preliminary_content_plan.rows_per_block(),
                    preliminary_content_plan.maximum_pointing_polynomial_terms(),
                    catalog_budget,
                )?,
            )?;
            let catalog_measurements = catalog.measurements();
            tracing::info!(
                target: "casa_ms::selected_pointing",
                "prepared selected POINTING catalog source_rows_scanned={} retained_rows={} retained_bytes={} construction_peak_bytes={} build_nanos={}",
                catalog_measurements.source_rows_scanned(),
                catalog_measurements.retained_rows(),
                catalog_measurements.retained_bytes(),
                catalog_measurements.construction_peak_bytes(),
                catalog_measurements.build_nanos(),
            );
            Some(catalog)
        } else {
            None
        };
        let content_plan = selected_content_plan_with_pointing_catalog(
            &measurement_set,
            problem,
            source,
            shared_bytes,
            content_budget,
            pointing_catalog
                .as_ref()
                .map(|catalog| catalog.measurements()),
        )?;
        let mut bound = Self::from_planned_locked_measurement_set(
            source,
            measures,
            content_plan,
            measurement_set,
            ephemeris,
            pointing_catalog,
        )?;
        if aw_pointing_plan_ceiling != 0 {
            let plan = build_aw_pointing_epoch_plan(problem, &bound)?;
            if plan.retained_byte_ceiling > aw_pointing_plan_ceiling {
                return Err(BoundObservationSourceError::MeasurementOverflow);
            }
            tracing::info!(
                target: "casa_ms::selected_pointing",
                "prepared CASA AW pointing epoch plan epochs={} retained_byte_ceiling={}",
                plan.chart_pixels_by_epoch.len(),
                plan.retained_byte_ceiling,
            );
            bound.aw_pointing_plan = Some(plan);
        }
        Ok(bound)
    }

    fn from_planned_locked_measurement_set(
        source: &ObservationSource,
        measures: &SelectedObservationMeasures,
        content_plan: SelectedObservationContentPlan,
        measurement_set: MeasurementSet,
        ephemeris: Option<&Arc<crate::SelectedObservationEphemeris>>,
        pointing_catalog: Option<PreparedSelectedPointingCatalog>,
    ) -> Result<Self, BoundObservationSourceError> {
        let row_predicate = selected_row_predicate(&measurement_set, source)?;
        let coordinates = Arc::from(selected_coordinates(&measurement_set, source.selection())?);
        let geometry_engine = Arc::new(MsCalEngine::new_selected_observation(
            &measurement_set,
            measures.provider(),
            ephemeris.cloned(),
        )?);
        Ok(Self {
            measurement_set,
            geometry_engine,
            row_predicate,
            coordinates,
            pointing_catalog,
            aw_pointing_plan: None,
            content_plan,
        })
    }

    #[cfg(all(test, unix))]
    pub(crate) fn open(
        problem: &CompiledProblem,
        source: &ObservationSource,
        content_budget: SelectedObservationContentBudget,
    ) -> Result<Self, BoundObservationSourceError> {
        let measures = super::measures::test_selected_observation_measures()?;
        let pointing_query_domain = if matches!(
            problem.geometry().centres().pointing(),
            PointingCentreLaw::Observation(_)
        ) {
            let measurement_set =
                MeasurementSet::open_retained_read(source.provenance().locator())?;
            let domain = crate::observation_owner::test_pointing_query_domain(
                &measurement_set,
                source.selection(),
                content_budget,
            )
            .map_err(|error| BoundObservationSourceError::OwnerState(Box::new(error)))?;
            Some(domain)
        } else {
            None
        };
        Self::open_with_measures(
            problem,
            source,
            &measures,
            SelectedObservationSharedBytes::new(measures.retained_bytes(), 0),
            content_budget,
            BoundObservationReferenceData::new(None, pointing_query_domain.as_ref()),
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

    /// Fill `block` with this source's next row group and return what the
    /// fill bound it to; `None` once the source's selected rows are spent.
    pub(super) fn fill_next_selected_block(
        &self,
        problem: &CompiledProblem,
        logical_source: &MeasurementSetReadAccess,
        replay: &mut SelectedRowReplay,
        block: &mut SelectedObservationBlock,
        window: Option<[f64; 2]>,
    ) -> Result<Option<BlockBinding>, BoundObservationSourceError> {
        loop {
            let Some(coordinate_index) = self.fill_selected_row_group(
                replay,
                &mut block.request_rows,
                &mut block.row_contexts,
            )?
            else {
                return Ok(None);
            };
            let coordinates = &self.coordinates[coordinate_index];
            let (channel_range, channels) = match window {
                Some(bounds) => {
                    let Some(window) = selected_channel_window(
                        self,
                        problem,
                        coordinates,
                        &block.row_contexts,
                        bounds,
                    )?
                    else {
                        // No channel payload is needed for a disjoint source block.
                        continue;
                    };
                    window
                }
                None => (
                    VisibilityChannelReadRange::new(
                        coordinates.channel_start,
                        coordinates.channel_count,
                    ),
                    0..coordinates.channels.len(),
                ),
            };
            block.row_contexts.clear();
            self.measurement_set.fill_selected_observation_buffer(
                &SelectedObservationBufferRequest::new(
                    selected_visibility(logical_source.selected_columns().visibility()),
                    selected_weight(logical_source.selected_columns().weights()),
                    &block.request_rows,
                    channel_range,
                ),
                &mut block.buffer,
            )?;
            let observation_pointings =
                evaluate_observation_pointings(self, problem, &block.buffer)?;
            block.row_geometry.clear();
            for row in 0..block.buffer.row_count() {
                let stored = block
                    .buffer
                    .row(row)
                    .ok_or(BoundObservationSourceError::StoredSampleShapeMismatch)?;
                block.row_geometry.push(evaluate_row_geometry(
                    self,
                    problem,
                    stored,
                    observation_pointings
                        .as_ref()
                        .map(|pointings| pointings[row]),
                )?);
            }
            return Ok(Some(BlockBinding {
                coordinates: Arc::clone(&self.coordinates),
                coordinate_index,
                channels,
                geometry_engine: Arc::clone(&self.geometry_engine),
            }));
        }
    }

    pub(super) fn selected_row_replay(
        &self,
    ) -> Result<SelectedRowReplay, BoundObservationSourceError> {
        let rows_per_block = self.content_plan.rows_per_block();
        let available_bytes = rows_per_block
            .checked_mul(crate::SelectedObservationRow::STORAGE_BYTES_PER_ROW)
            .ok_or(BoundObservationSourceError::MeasurementOverflow)?;
        let plan = MsReadPlan::new(
            self.measurement_set.row_count(),
            MsSelectionIoBudget {
                available_bytes,
                maximum_live_blocks: 1,
                requested_bytes_per_row: crate::SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                storage_alignment_rows: Some(rows_per_block),
            },
        )
        .map_err(|error| MsError::InvalidInput(error.to_string()))?;
        Ok(SelectedRowReplay {
            cursor: self.measurement_set.main_row_selection_cursor(plan)?,
            pending: None,
            selected_rows: 0,
        })
    }

    fn fill_selected_row_group(
        &self,
        replay: &mut SelectedRowReplay,
        physical_rows: &mut Vec<usize>,
        row_contexts: &mut Vec<SelectedReplayRow>,
    ) -> Result<Option<usize>, BoundObservationSourceError> {
        let Some(first) = replay.next_selected(self)? else {
            return Ok(None);
        };
        let coordinate_index = self
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
        physical_rows.clear();
        row_contexts.clear();
        physical_rows.push(
            usize::try_from(first.physical_row())
                .map_err(|_| BoundObservationSourceError::PhysicalRowIndexOverflow)?,
        );
        row_contexts.push(first);
        while physical_rows.len() < self.content_plan.rows_per_block() {
            let Some(row) = replay.next_selected(self)? else {
                break;
            };
            if row.data_description_id() != first.data_description_id() {
                replay.pending = Some(row);
                break;
            }
            physical_rows.push(
                usize::try_from(row.physical_row())
                    .map_err(|_| BoundObservationSourceError::PhysicalRowIndexOverflow)?,
            );
            row_contexts.push(row);
        }
        Ok(Some(coordinate_index))
    }
}

#[derive(Clone, Copy)]
pub(super) struct SelectedReplayRow {
    physical_row: u64,
    data_description_id: u32,
    field_id: i32,
    time_mjd_seconds: f64,
}

impl SelectedReplayRow {
    fn new(physical_row: u64, data_description_id: u32, fact: SelectedObservationRow) -> Self {
        Self {
            physical_row,
            data_description_id,
            field_id: fact.field_id(),
            time_mjd_seconds: fact.time_mjd_seconds(),
        }
    }

    const fn physical_row(self) -> u64 {
        self.physical_row
    }

    const fn data_description_id(self) -> u32 {
        self.data_description_id
    }
}

fn selected_channel_window(
    source: &BoundObservationSource,
    problem: &CompiledProblem,
    coordinates: &SelectedCoordinates,
    rows: &[SelectedReplayRow],
    frequency_bounds_hz: [f64; 2],
) -> Result<Option<(VisibilityChannelReadRange, std::ops::Range<usize>)>, BoundObservationSourceError>
{
    let lower_bound = frequency_bounds_hz[0].min(frequency_bounds_hz[1]);
    let upper_bound = frequency_bounds_hz[0].max(frequency_bounds_hz[1]);
    if !lower_bound.is_finite() || !upper_bound.is_finite() || lower_bound == upper_bound {
        return Err(BoundObservationSourceError::SpectralContributionMismatch);
    }
    let output_frame = problem.geometry().spectral().output_frame();
    let source_frame = coordinates
        .channels
        .first()
        .ok_or(BoundObservationSourceError::StoredSampleShapeMismatch)?
        .frame;
    let mut admitted_first: Option<usize> = None;
    let mut admitted_last: Option<usize> = None;
    let mut cached_conversion: Option<((i32, u64), PreparedFrequencyFrameConversion)> = None;
    for row in rows.iter().copied() {
        let normalized_time = row.time_mjd_seconds / 86_400.0 * 86_400.0;
        let context_key = (row.field_id, normalized_time.to_bits());
        let conversion = if let Some((cached_key, conversion)) = cached_conversion
            && cached_key == context_key
        {
            conversion
        } else {
            let conversion = prepare_row_frequency_conversion(
                source.geometry_engine(),
                row.field_id,
                normalized_time,
                source_frame,
                output_frame,
            )?;
            cached_conversion = Some((context_key, conversion));
            conversion
        };
        let mut row_window = SelectedChannelWindowSelection::new(lower_bound, upper_bound);
        for (ordinal, channel) in coordinates.channels.iter().copied().enumerate() {
            let centre = conversion.convert_hz(channel.centre_hz);
            if !centre.is_finite() || centre <= 0.0 {
                return Err(BoundObservationSourceError::SpectralContributionMismatch);
            }
            row_window.observe(ordinal, centre);
        }
        if let Some(window) = row_window.finish() {
            let first = window.start;
            let last = window.end - 1;
            admitted_first = Some(admitted_first.map_or(first, |current| current.min(first)));
            admitted_last = Some(admitted_last.map_or(last, |current| current.max(last)));
        }
    }
    let (Some(admitted_first), Some(admitted_last)) = (admitted_first, admitted_last) else {
        return Ok(None);
    };
    let mut physical_start = usize::MAX;
    let mut physical_end = 0_usize;
    for ordinal in admitted_first..=admitted_last {
        let channel = coordinates
            .channels
            .get(ordinal)
            .ok_or(BoundObservationSourceError::StoredSampleShapeMismatch)?;
        let index = usize::try_from(channel.channel_index)
            .map_err(|_| BoundObservationSourceError::SpectralContributionMismatch)?;
        physical_start = physical_start.min(index);
        physical_end = physical_end.max(
            index
                .checked_add(1)
                .ok_or(BoundObservationSourceError::MeasurementOverflow)?,
        );
    }
    Ok(Some((
        VisibilityChannelReadRange::new(physical_start, physical_end - physical_start),
        admitted_first..admitted_last + 1,
    )))
}

#[derive(Clone, Copy)]
struct SelectedChannelWindowSelection {
    lower_bound: f64,
    upper_bound: f64,
    first: Option<usize>,
    last: Option<usize>,
    previous: Option<(usize, f64)>,
}

impl SelectedChannelWindowSelection {
    const fn new(lower_bound: f64, upper_bound: f64) -> Self {
        Self {
            lower_bound,
            upper_bound,
            first: None,
            last: None,
            previous: None,
        }
    }

    fn observe(&mut self, ordinal: usize, centre: f64) {
        if (self.lower_bound..=self.upper_bound).contains(&centre) {
            self.include(ordinal, ordinal);
        }
        if let Some((previous_ordinal, previous_centre)) = self.previous
            && previous_centre.min(centre) <= self.upper_bound
            && previous_centre.max(centre) >= self.lower_bound
        {
            self.include(previous_ordinal.min(ordinal), previous_ordinal.max(ordinal));
        }
        self.previous = Some((ordinal, centre));
    }

    fn include(&mut self, first: usize, last: usize) {
        self.first = Some(self.first.map_or(first, |current| current.min(first)));
        self.last = Some(self.last.map_or(last, |current| current.max(last)));
    }

    fn finish(self) -> Option<std::ops::Range<usize>> {
        let first = self.first?;
        let last = self.last?;
        Some(first..last + 1)
    }
}

#[cfg(test)]
mod selected_channel_window_tests {
    use super::SelectedChannelWindowSelection;

    fn select(centres: &[f64], bounds: [f64; 2]) -> Option<std::ops::Range<usize>> {
        let lower = bounds[0].min(bounds[1]);
        let upper = bounds[0].max(bounds[1]);
        let mut selection = SelectedChannelWindowSelection::new(lower, upper);
        for (ordinal, centre) in centres.iter().copied().enumerate() {
            selection.observe(ordinal, centre);
        }
        selection.finish()
    }

    #[test]
    fn adjacent_pair_support_handles_descending_and_gapped_centres() {
        assert_eq!(select(&[4.0, 3.0, 2.0, 1.0], [2.4, 3.6]), Some(0..3));
        assert_eq!(
            select(&[100.0, 111.0, 150.0, 220.0], [120.0, 145.0]),
            Some(1..3)
        );
    }

    #[test]
    fn selection_does_not_add_a_channel_beyond_the_intersecting_pair() {
        assert_eq!(select(&[0.0, 10.0, 20.0], [1.0, 9.0]), Some(0..2));
        assert_eq!(select(&[0.0, 10.0, 20.0], [9.0, 11.0]), Some(0..3));
        assert_eq!(select(&[0.0, 10.0, 20.0], [30.0, 40.0]), None);
        assert_eq!(select(&[10.0], [9.0, 11.0]), Some(0..1));
    }
}

/// The selected MAIN rows of one source, in physical order: a cursor over
/// MAIN filtered by the compiled row predicate.
pub(super) struct SelectedRowReplay {
    cursor: MainRowSelectionCursor,
    pending: Option<SelectedReplayRow>,
    selected_rows: u64,
}

impl SelectedRowReplay {
    /// Rows the predicate has selected so far, whether or not a channel
    /// window then skipped their block.
    pub(super) const fn selected_rows(&self) -> u64 {
        self.selected_rows
    }

    fn next_selected(
        &mut self,
        source: &BoundObservationSource,
    ) -> Result<Option<SelectedReplayRow>, BoundObservationSourceError> {
        if let Some(row) = self.pending.take() {
            return Ok(Some(row));
        }
        while let Some(fact) = self.cursor.next(&source.measurement_set)? {
            if !source.row_predicate.matches(fact) {
                continue;
            }
            self.selected_rows += 1;
            return Ok(Some(SelectedReplayRow::new(
                u64::try_from(fact.physical_row())
                    .map_err(|_| BoundObservationSourceError::PhysicalRowIndexOverflow)?,
                u32::try_from(fact.data_description_id()).map_err(|_| {
                    BoundObservationSourceError::DataDescriptionCoordinateMismatch {
                        data_description_id: u32::MAX,
                    }
                })?,
                fact,
            )));
        }
        Ok(None)
    }
}

fn project_stored_run_row(
    problem: &CompiledProblem,
    geometry_engine: &MsCalEngine,
    coordinates: &SelectedCoordinates,
    stored: SelectedStoredRow,
    geometry: &EvaluatedRowGeometry,
) -> Result<SelectedObservationRunRow, BoundObservationSourceError> {
    let time_scale = time_scale(geometry_engine.time_reference().as_str())?;
    let field_id = usize::try_from(stored.field_id())
        .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
    let antenna1 = usize::try_from(stored.antenna1())
        .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
    let antenna2 = usize::try_from(stored.antenna2())
        .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
    let parallactic_angles_rad = if problem.requires_parallactic_angles() {
        Some([
            polarization_operator_angle(geometry_engine.parallactic_angle(
                stored.time_mjd_seconds(),
                field_id,
                antenna1,
            )?),
            polarization_operator_angle(geometry_engine.parallactic_angle(
                stored.time_mjd_seconds(),
                field_id,
                antenna2,
            )?),
        ])
    } else {
        None
    };
    let antenna_responses = match problem.science().instrument_model() {
        None => None,
        Some(InstrumentModel::CasaAca7mInterferometricDirectPbV1) => {
            Some(SelectedAntennaResponses {
                antenna1: casa_imaging_model::AntennaResponseClass::CasaAca7m,
                antenna2: casa_imaging_model::AntennaResponseClass::CasaAca7m,
                family_envelope: casa_imaging_model::AntennaResponseClass::CasaAca7m,
            })
        }
        Some(InstrumentModel::CasaAlmaAcaHeterogeneousInterferometricResponseV1) => {
            Some(SelectedAntennaResponses {
                antenna1: geometry_engine
                    .antenna_response_class(antenna1)
                    .ok_or(BoundObservationSourceError::InvalidRowGeometry)?,
                antenna2: geometry_engine
                    .antenna_response_class(antenna2)
                    .ok_or(BoundObservationSourceError::InvalidRowGeometry)?,
                family_envelope: geometry_engine
                    .antenna_response_family_envelope()
                    .ok_or(BoundObservationSourceError::InvalidRowGeometry)?,
            })
        }
        Some(InstrumentModel::CasaEvlaWidebandAwV1) => None,
    };
    let physical_row = u64::try_from(stored.physical_row())
        .map_err(|_| BoundObservationSourceError::PhysicalRowIndexOverflow)?;
    Ok(SelectedObservationRunRow {
        physical_row,
        data_description_id: stored.data_description_id(),
        spectral_window_id: coordinates.data_description.spectral_window_id(),
        row_flag: stored.row_flag(),
        coordinates: SelectedSampleCoordinates {
            raw_uvw_m: stored.uvw_m(),
            time: Epoch::new(stored.time_mjd_seconds() / 86_400.0, time_scale),
            parallactic_angles_rad,
            pointing_directions: geometry.pointing_directions,
        },
        domain_projections: geometry.domain_projections.clone(),
        metadata: SelectedSampleMetadata {
            field_id: stored.field_id(),
            antenna1: stored.antenna1(),
            antenna2: stored.antenna2(),
            antenna_responses,
        },
    })
}

const fn project_run_channel(channel: SelectedChannel) -> SelectedObservationRunChannel {
    SelectedObservationRunChannel {
        channel_index: channel.channel_index,
        frequency_centre_hz: channel.centre_hz,
    }
}

/// Opaque caller-owned storage for one selected-observation block.
///
/// Only [`SelectedObservationBlockSource::fill_next`] reads or writes it: the
/// fill returns a [`FilledObservationBlock`] that borrows it, so an unfilled
/// block cannot be read and a block cannot be refilled while it is read.
///
/// [`SelectedObservationBlockSource::fill_next`]: super::SelectedObservationBlockSource::fill_next
pub struct SelectedObservationBlock {
    buffer: SelectedObservationBuffer,
    row_geometry: Vec<EvaluatedRowGeometry>,
    request_rows: Vec<usize>,
    row_contexts: Vec<SelectedReplayRow>,
}

/// What one fill bound its block to: the source's coordinate catalog and
/// geometry engine, and the channel ordinals read.
pub(super) struct BlockBinding {
    coordinates: Arc<[SelectedCoordinates]>,
    coordinate_index: usize,
    channels: std::ops::Range<usize>,
    geometry_engine: Arc<MsCalEngine>,
}

impl BlockBinding {
    fn coordinates(&self) -> &SelectedCoordinates {
        &self.coordinates[self.coordinate_index]
    }
}

/// The block [`SelectedObservationBlockSource::fill_next`] just filled.
///
/// It borrows the block's storage, so the stream cannot refill that storage
/// until this and every projection of it are dropped.
///
/// [`SelectedObservationBlockSource::fill_next`]: super::SelectedObservationBlockSource::fill_next
pub struct FilledObservationBlock<'a> {
    block: &'a SelectedObservationBlock,
    binding: BlockBinding,
}

/// Reusable row-scope geometry storage, sized from the source's admitted
/// row and channel bounds. A projection borrows it; no visibility is copied.
pub struct SelectedObservationNumericGeometry {
    rows: Vec<Option<SelectedObservationRunRow>>,
    channels: Vec<SelectedObservationRunChannel>,
    frequencies_hz: Vec<f64>,
    maximum_rows: usize,
    maximum_channels: usize,
}

impl SelectedObservationNumericGeometry {
    /// Allocate bounded, reusable row and frequency storage after admission.
    pub fn new(
        maximum_rows: usize,
        maximum_channels: usize,
    ) -> Result<Self, BoundObservationSourceError> {
        if maximum_rows == 0 || maximum_channels == 0 {
            return Err(BoundObservationSourceError::StoredSampleShapeMismatch);
        }
        Ok(Self {
            rows: Vec::with_capacity(maximum_rows),
            channels: Vec::with_capacity(maximum_channels),
            frequencies_hz: Vec::with_capacity(maximum_rows * maximum_channels),
            maximum_rows,
            maximum_channels,
        })
    }
}

/// One filled block with its row geometry and output-frame frequencies.
///
/// It borrows both the block and the geometry storage, so neither can be
/// refilled or reprojected while it lives.
///
/// The stream cannot refill a block whose projection is still read:
///
/// ```compile_fail,E0499
/// use casa_ms::{
///     SelectedObservationBlock, SelectedObservationBlockSource,
///     SelectedObservationNumericGeometry,
/// };
///
/// fn stale(
///     problem: &casa_imaging_model::CompiledProblem,
///     source: &mut SelectedObservationBlockSource<'_>,
///     block: &mut SelectedObservationBlock,
///     geometry: &mut SelectedObservationNumericGeometry,
/// ) {
///     let filled = source.fill_next(block).unwrap().unwrap();
///     let projected = filled.project_numeric_geometry(problem, geometry).unwrap();
///     source.fill_next(block).unwrap();
///     let _ = projected.row_count();
/// }
/// ```
pub struct ProjectedObservationBlock<'a> {
    columns: crate::SelectedObservationNumericColumns<'a>,
    coordinates: &'a SelectedCoordinates,
    first_stored_channel: u32,
    geometry: &'a SelectedObservationNumericGeometry,
}

/// A disjoint portion of the reusable numeric geometry. The caller executes
/// these borrowed jobs on its already admitted worker team and joins them
/// before consuming the complete block.
pub struct SelectedObservationNumericGeometryChunk<'a> {
    block: &'a SelectedObservationBlock,
    problem: &'a CompiledProblem,
    engine: &'a MsCalEngine,
    coordinates: &'a SelectedCoordinates,
    channels: &'a [SelectedObservationRunChannel],
    frame: FrequencyFrame,
    first_row: usize,
    rows: &'a mut [Option<SelectedObservationRunRow>],
    frequencies_hz: &'a mut [f64],
}

impl SelectedObservationNumericGeometryChunk<'_> {
    /// Project this row partition without opening or sharing an MS table handle.
    pub fn project(&mut self) -> Result<(), BoundObservationSourceError> {
        let mut previous_conversion = None;
        let channels = self.channels.len();
        for local in 0..self.rows.len() {
            let row = self.first_row + local;
            let stored = self
                .block
                .buffer
                .row(row)
                .ok_or(BoundObservationSourceError::StoredSampleShapeMismatch)?;
            let projected = project_stored_run_row(
                self.problem,
                self.engine,
                self.coordinates,
                stored,
                &self.block.row_geometry[row],
            )?;
            let key = (stored.field_id(), stored.time_mjd_seconds().to_bits());
            if previous_conversion
                .as_ref()
                .is_none_or(|(previous, _)| *previous != key)
            {
                previous_conversion = Some((
                    key,
                    prepare_row_frequency_conversion(
                        self.engine,
                        stored.field_id(),
                        stored.time_mjd_seconds(),
                        self.frame,
                        self.problem.geometry().spectral().output_frame(),
                    )?,
                ));
            }
            let conversion = &previous_conversion.as_ref().expect("conversion prepared").1;
            for (channel, coordinate) in self
                .channels
                .iter()
                .zip(&mut self.frequencies_hz[local * channels..(local + 1) * channels])
            {
                *coordinate = conversion.convert_hz(channel.frequency_centre_hz);
            }
            self.rows[local] = Some(projected);
        }
        Ok(())
    }
}

impl SelectedObservationBlock {
    pub(super) fn new(rows_per_block: usize) -> Self {
        Self {
            buffer: SelectedObservationBuffer::default(),
            row_geometry: Vec::with_capacity(rows_per_block),
            request_rows: Vec::with_capacity(rows_per_block),
            row_contexts: Vec::with_capacity(rows_per_block),
        }
    }
}

impl<'a> FilledObservationBlock<'a> {
    pub(super) fn new(block: &'a SelectedObservationBlock, binding: BlockBinding) -> Self {
        Self { block, binding }
    }

    #[cfg(test)]
    pub(super) fn parallactic_angle_cache_entries(&self) -> usize {
        self.binding
            .geometry_engine
            .parallactic_angle_cache_entries()
    }

    /// Borrow the block's flat numeric columns without allocating sample
    /// records.
    #[must_use]
    pub fn numeric_columns(&self) -> crate::SelectedObservationNumericColumns<'a> {
        self.block
            .buffer
            .numeric_columns()
            .expect("a filled block holds its visibilities and weights")
    }

    /// Evaluate row geometry and frequency conversion once per row into
    /// `geometry`. Numeric columns stay with the block, and are not turned
    /// into sample records.
    pub fn project_numeric_geometry<'p>(
        &'p self,
        problem: &CompiledProblem,
        geometry: &'p mut SelectedObservationNumericGeometry,
    ) -> Result<ProjectedObservationBlock<'p>, BoundObservationSourceError> {
        let chunk_rows = geometry.maximum_rows;
        self.project_numeric_geometry_with(problem, geometry, chunk_rows, |chunks| {
            chunks[0].project()
        })
    }

    /// Project disjoint coarse row chunks through a caller-supplied, joined
    /// executor, which must project every chunk before it returns `Ok`.
    pub fn project_numeric_geometry_with<'p>(
        &'p self,
        problem: &CompiledProblem,
        geometry: &'p mut SelectedObservationNumericGeometry,
        chunk_rows: usize,
        execute: impl FnOnce(
            &mut [SelectedObservationNumericGeometryChunk<'_>],
        ) -> Result<(), BoundObservationSourceError>,
    ) -> Result<ProjectedObservationBlock<'p>, BoundObservationSourceError> {
        if chunk_rows == 0 {
            return Err(BoundObservationSourceError::StoredSampleShapeMismatch);
        }
        geometry.rows.clear();
        geometry.channels.clear();
        geometry.frequencies_hz.clear();
        let coordinates = self.binding.coordinates();
        let window = self.binding.channels.clone();
        let rows = self.block.buffer.row_count();
        if rows > geometry.maximum_rows || window.len() > geometry.maximum_channels {
            return Err(BoundObservationSourceError::StoredSampleShapeMismatch);
        }
        let columns = self.numeric_columns();
        let first_stored_channel = u32::try_from(columns.channel_range.start)
            .map_err(|_| BoundObservationSourceError::StoredSampleShapeMismatch)?;
        geometry.channels.extend(
            coordinates.channels[window]
                .iter()
                .copied()
                .map(project_run_channel),
        );
        let frame = coordinates
            .channels
            .first()
            .ok_or(BoundObservationSourceError::StoredSampleShapeMismatch)?
            .frame;
        let channels = geometry.channels.len();
        geometry.rows.resize_with(rows, || None);
        geometry.frequencies_hz.resize(rows * channels, 0.0);
        let (mut row_slots, mut frequencies_hz) = (
            geometry.rows.as_mut_slice(),
            geometry.frequencies_hz.as_mut_slice(),
        );
        let mut chunks = Vec::with_capacity(rows.div_ceil(chunk_rows));
        let mut first_row = 0;
        while first_row < rows {
            let take = (rows - first_row).min(chunk_rows);
            let (head_rows, tail_rows) = row_slots.split_at_mut(take);
            let (head_frequencies, tail_frequencies) = frequencies_hz.split_at_mut(take * channels);
            chunks.push(SelectedObservationNumericGeometryChunk {
                block: self.block,
                problem,
                engine: &self.binding.geometry_engine,
                coordinates,
                channels: &geometry.channels,
                frame,
                first_row,
                rows: head_rows,
                frequencies_hz: head_frequencies,
            });
            (row_slots, frequencies_hz) = (tail_rows, tail_frequencies);
            first_row += take;
        }
        execute(&mut chunks)?;
        assert!(
            geometry.rows.iter().all(Option::is_some),
            "a projection executor that returns Ok has projected every chunk"
        );
        Ok(ProjectedObservationBlock {
            columns,
            coordinates,
            first_stored_channel,
            geometry,
        })
    }
}

impl<'a> ProjectedObservationBlock<'a> {
    /// Rows in the block; geometry is evaluated at row scope, never per
    /// correlation.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.geometry.rows.len()
    }

    /// The channels read, as selected native channel descriptors.
    #[must_use]
    pub fn channels(&self) -> &'a [SelectedObservationRunChannel] {
        &self.geometry.channels
    }

    /// Row-major channel centres of every row in the compiled output frame.
    #[must_use]
    pub fn frequencies_hz(&self) -> &'a [f64] {
        &self.geometry.frequencies_hz
    }

    /// Borrow one whole row with source-issued axes and projected metadata.
    ///
    /// # Panics
    ///
    /// When `row` is not below [`Self::row_count`].
    #[must_use]
    pub fn numeric_row(&self, row: usize) -> casa_imaging_model::SelectedNumericRow<'a> {
        let columns = self.columns;
        let projected = self.geometry.rows[row]
            .as_ref()
            .expect("a projection holds every row of its block");
        let channels = columns.channel_range.count;
        let width = channels * columns.correlation_count;
        let start = row * width;
        let visibility = match columns.visibility {
            crate::SelectedNumericVisibility::Float32(values) => {
                crate::SelectedNumericVisibility::Float32(&values[start..start + width])
            }
            crate::SelectedNumericVisibility::Complex32(values) => {
                crate::SelectedNumericVisibility::Complex32(&values[start..start + width])
            }
        };
        let weights = match columns.weights {
            crate::SelectedNumericWeights::PerRow(values) => crate::SelectedNumericWeights::PerRow(
                &values[row * columns.correlation_count..(row + 1) * columns.correlation_count],
            ),
            crate::SelectedNumericWeights::PerChannel(values) => {
                crate::SelectedNumericWeights::PerChannel(&values[start..start + width])
            }
        };
        casa_imaging_model::SelectedNumericRow {
            row: projected,
            channels: &self.geometry.channels,
            correlations: &self.coordinates.products,
            first_stored_channel: self.first_stored_channel,
            stored_channels: channels,
            stored_correlations: columns.correlation_count,
            visibility,
            flags: &columns.flags[start..start + width],
            weights,
        }
    }
}

pub(super) type BufferedObservationBlock = SelectedObservationBlock;

/// Failure to bind a retained MeasurementSet to one compiled observation source.
#[derive(Debug, Error)]
pub enum BoundObservationSourceError {
    /// The injected Measures provider is missing, stale, or unaccounted.
    #[error(transparent)]
    Measures(#[from] SelectedObservationMeasuresError),
    /// The MeasurementSet could not be opened or read under the admitted content budget.
    #[error(transparent)]
    Storage(#[from] MsError),
    /// The physical selected rows could not be resolved.
    #[error("selected-observation resolution failed: {0}")]
    OwnerState(#[source] Box<ObservationOwnerError>),
    /// Physical traversal counters exceeded their diagnostics domain.
    #[error("selected-observation traversal measurements overflowed")]
    MeasurementOverflow,
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
    /// Observation-pointing evaluation lacks its owner-derived selected-time domain.
    #[error("observation-pointing evaluation requires an owner-derived selected-time domain")]
    MissingPointingQueryDomain,
    /// The retained source is not one exact member of the supplied compiled problem.
    #[error("retained observation source does not match the compiled selected observation")]
    ProblemSourceMismatch,
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
    /// Stored spectral-window coordinates do not cover the compiled channel selection.
    #[error(
        "stored spectral coordinates do not cover selected SPECTRAL_WINDOW_ID {spectral_window_id}"
    )]
    SpectralCoordinateMismatch {
        /// Spectral window whose coordinate vectors were short or not finite.
        spectral_window_id: u32,
    },
    /// Selected correlations do not define CASA's one unpolarized imaging-weight group.
    #[error(
        "selected POLARIZATION_ID {polarization_id} is not one correlation or a canonical circular/linear parallel-hand group"
    )]
    UnsupportedImagingWeightCorrelationGroup {
        /// Polarization row whose selected products were ambiguous.
        polarization_id: u32,
    },
    /// A physical MAIN row index did not fit the host storage index domain.
    #[error("selected physical MAIN row index exceeds the host storage index domain")]
    PhysicalRowIndexOverflow,
    /// The block stream was completed before its last block was read.
    #[error("selected-observation block stream completed before it was exhausted")]
    IncompleteBlockTraversal,
    /// The walk of MAIN selected a different number of rows than the compiled
    /// selection counted: the MeasurementSet changed after compile.
    #[error(
        "the compiled selection counted {expected} selected MAIN rows but the stream selected {delivered}"
    )]
    SelectedRowCountMismatch {
        /// Selected rows the compiled selection counted.
        expected: u64,
        /// Selected rows the stream's walk of MAIN delivered, read or skipped
        /// by a channel window.
        delivered: u64,
    },
    /// The bounded storage block did not contain one compiled sample coordinate.
    #[error("bounded selected-observation storage block has an inconsistent sample shape")]
    StoredSampleShapeMismatch,
    /// Stored row geometry could not be represented by the compiled sample schema.
    #[error("stored selected-observation row geometry is invalid")]
    InvalidRowGeometry,
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
    frame: FrequencyFrame,
}

fn selected_coordinates(
    measurement_set: &MeasurementSet,
    selection: &ObservationSelection,
) -> Result<Box<[SelectedCoordinates]>, BoundObservationSourceError> {
    let spectral_windows = measurement_set.spectral_window()?;
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
            if !centre_hz.is_finite() {
                return Err(BoundObservationSourceError::SpectralCoordinateMismatch {
                    spectral_window_id: data_description.spectral_window_id(),
                });
            }
            channels.push(SelectedChannel {
                channel_index,
                centre_hz,
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
        let polarization = selection
            .correlations()
            .iter()
            .find(|candidate| candidate.polarization_id() == data_description.polarization_id())
            .expect("compiled DATA_DESCRIPTION has one polarization selection");
        validate_input_weight_group(polarization.products(), data_description.polarization_id())?;
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

pub(super) fn validate_input_weight_group(
    products: &[CorrelationProduct],
    polarization_id: u32,
) -> Result<(), BoundObservationSourceError> {
    if products.len() == 1 {
        return Ok(());
    }
    let valid = match (products.first(), products.last()) {
        (Some(first), Some(last))
            if first.correlation_type() == CorrelationType::CircularRr
                && last.correlation_type() == CorrelationType::CircularLl =>
        {
            matches_canonical_correlation_order(
                products,
                &[
                    CorrelationType::CircularRr,
                    CorrelationType::CircularRl,
                    CorrelationType::CircularLr,
                    CorrelationType::CircularLl,
                ],
            )
        }
        (Some(first), Some(last))
            if first.correlation_type() == CorrelationType::LinearXx
                && last.correlation_type() == CorrelationType::LinearYy =>
        {
            matches_canonical_correlation_order(
                products,
                &[
                    CorrelationType::LinearXx,
                    CorrelationType::LinearXy,
                    CorrelationType::LinearYx,
                    CorrelationType::LinearYy,
                ],
            )
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(
            BoundObservationSourceError::UnsupportedImagingWeightCorrelationGroup {
                polarization_id,
            },
        )
    }
}

fn matches_canonical_correlation_order(
    products: &[CorrelationProduct],
    canonical: &[CorrelationType; 4],
) -> bool {
    let mut remaining = canonical.iter();
    products.iter().all(|product| {
        remaining
            .by_ref()
            .any(|candidate| *candidate == product.correlation_type())
    })
}

#[derive(Clone)]
pub(super) struct EvaluatedRowGeometry {
    domain_projections: SelectedImageDomainProjections,
    pointing_directions: SelectedPointingDirections,
}

fn evaluate_row_geometry(
    source: &BoundObservationSource,
    problem: &CompiledProblem,
    stored: SelectedStoredRow,
    observation_pointing: Option<SelectedPointingDirections>,
) -> Result<EvaluatedRowGeometry, BoundObservationSourceError> {
    let field_id = usize::try_from(stored.field_id())
        .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
    let (longitude_rad, latitude_rad) = source
        .geometry_engine
        .observation_direction_j2000(stored.time_mjd_seconds(), field_id)?
        .as_angles();
    let observation_direction =
        SkyDirection::new(DirectionFrame::J2000, longitude_rad, latitude_rad);
    let centres = problem.geometry().centres();
    let phase_direction = match centres.phase_tracking() {
        PhaseCentreLaw::Observation => observation_direction,
        PhaseCentreLaw::Fixed(direction) => require_fixed_j2000(*direction)?,
        PhaseCentreLaw::Ephemeris(target) => {
            let direction = source.geometry_engine.moving_direction_j2000(
                stored.time_mjd_seconds(),
                field_id,
                target,
            )?;
            let (longitude_rad, latitude_rad) = direction.as_angles();
            SkyDirection::new(DirectionFrame::J2000, longitude_rad, latitude_rad)
        }
    };
    let pointing_directions = match centres.pointing() {
        PointingCentreLaw::PhaseTrackingCentre => SelectedPointingDirections {
            antenna1: phase_direction,
            antenna2: phase_direction,
        },
        PointingCentreLaw::FieldCentre => SelectedPointingDirections {
            antenna1: observation_direction,
            antenna2: observation_direction,
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
    let domains = problem.geometry().domains();
    let moving_direction_shift = if matches!(centres.phase_tracking(), PhaseCentreLaw::Ephemeris(_))
    {
        let anchor = domains
            .first()
            .ok_or(BoundObservationSourceError::InvalidRowGeometry)?
            .model_phase_centre();
        let anchor_j2000 = source.geometry_engine.direction_angles_j2000(
            stored.time_mjd_seconds(),
            [anchor.longitude_rad(), anchor.latitude_rad()],
            direction_ref(anchor.frame()),
        )?;
        Some([
            phase_direction.longitude_rad() - anchor_j2000[0],
            phase_direction.latitude_rad() - anchor_j2000[1],
        ])
    } else {
        None
    };
    let chart_count = domains.iter().try_fold(0_usize, |count, domain| {
        count.checked_add(domain.facets().len())
    });
    let mut domain_projections = Vec::new();
    domain_projections
        .try_reserve_exact(chart_count.ok_or(BoundObservationSourceError::InvalidRowGeometry)?)
        .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
    for (domain_ordinal, domain) in domains.iter().enumerate() {
        let domain_ordinal = u32::try_from(domain_ordinal)
            .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
        let shared_psf = domain.psf_phase_centre() == domain.model_phase_centre();
        let distinct_psf = if shared_psf {
            None
        } else {
            Some(evaluate_phase_centre_projection(
                source,
                stored,
                observation_direction,
                PhaseCentreProjectionRequest {
                    field_id,
                    target_direction: domain.psf_phase_centre(),
                    project_to_observation_plane: domain.facets().len() > 1,
                    moving_direction_shift,
                    uvw_law: problem.geometry().uvw(),
                },
            )?)
        };
        for (facet_ordinal, facet) in domain.facets().iter().enumerate() {
            let facet_ordinal = u32::try_from(facet_ordinal)
                .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
            let model_phase_centre = if domain.facets().len() == 1 {
                domain.model_phase_centre()
            } else {
                facet.phase_centre()
            };
            let model = evaluate_phase_centre_projection(
                source,
                stored,
                observation_direction,
                PhaseCentreProjectionRequest {
                    field_id,
                    target_direction: model_phase_centre,
                    project_to_observation_plane: domain.facets().len() > 1,
                    moving_direction_shift,
                    uvw_law: problem.geometry().uvw(),
                },
            )?;
            let projection = match distinct_psf {
                Some(psf) => SelectedImageDomainProjection::new_facet(
                    domain_ordinal,
                    facet_ordinal,
                    model,
                    psf,
                ),
                None => SelectedImageDomainProjection::facet_with_shared_psf(
                    domain_ordinal,
                    facet_ordinal,
                    model,
                ),
            };
            let projection = if let Some(plan) = &source.aw_pointing_plan {
                let epoch = AwPointingEpochKey {
                    data_description_id: stored.data_description_id(),
                    field_id: stored.field_id(),
                    time_bits: stored.time_mjd_seconds().to_bits(),
                };
                let chart = domain_projections.len();
                let pixels = plan
                    .chart_pixels_by_epoch
                    .get(&epoch)
                    .and_then(|charts| charts.get(chart))
                    .ok_or(BoundObservationSourceError::InvalidRowGeometry)?;
                let antenna1 = pixels
                    .get(&stored.antenna1())
                    .ok_or(BoundObservationSourceError::InvalidRowGeometry)?;
                let antenna2 = pixels
                    .get(&stored.antenna2())
                    .ok_or(BoundObservationSourceError::InvalidRowGeometry)?;
                projection
                    .with_aw_pointing_pixel([
                        0.5 * (antenna1[0] + antenna2[0]),
                        0.5 * (antenna1[1] + antenna2[1]),
                    ])
                    .ok_or(BoundObservationSourceError::InvalidRowGeometry)?
            } else {
                projection
            };
            domain_projections.push(projection);
        }
    }
    let domain_projections = SelectedImageDomainProjections::new(domain_projections)
        .ok_or(BoundObservationSourceError::InvalidRowGeometry)?;
    Ok(EvaluatedRowGeometry {
        domain_projections,
        pointing_directions,
    })
}

struct PhaseCentreProjectionRequest {
    field_id: usize,
    target_direction: SkyDirection,
    project_to_observation_plane: bool,
    moving_direction_shift: Option<[f64; 2]>,
    uvw_law: casa_imaging_model::UvwCoordinateLaw,
}

fn evaluate_phase_centre_projection(
    source: &BoundObservationSource,
    stored: SelectedStoredRow,
    observation_direction: SkyDirection,
    request: PhaseCentreProjectionRequest,
) -> Result<SelectedPhaseCentreProjection, BoundObservationSourceError> {
    let mut target_angles = source.geometry_engine.direction_angles_j2000(
        stored.time_mjd_seconds(),
        [
            request.target_direction.longitude_rad(),
            request.target_direction.latitude_rad(),
        ],
        direction_ref(request.target_direction.frame()),
    )?;
    if let Some([longitude, latitude]) = request.moving_direction_shift {
        target_angles[0] += longitude;
        target_angles[1] += latitude;
    }
    let target_j2000 = SkyDirection::new(DirectionFrame::J2000, target_angles[0], target_angles[1]);
    let (transformed_uvw_m, phase_shift_m) = if matches!(
        request.uvw_law,
        casa_imaging_model::UvwCoordinateLaw::MosaicPhaseTrackingCentre
    ) {
        if request.project_to_observation_plane {
            return Err(BoundObservationSourceError::InvalidRowGeometry);
        }
        let target = MDirection::from_angles(
            target_j2000.longitude_rad(),
            target_j2000.latitude_rad(),
            DirectionRef::J2000,
        );
        let (uvw_m, casa_dphase_m) = source
            .geometry_engine
            .reproject_raw_uvw_for_mosaic_to_direction(stored.uvw_m(), request.field_id, &target)?;
        (uvw_m, -casa_dphase_m)
    } else if target_j2000 == observation_direction && !request.project_to_observation_plane {
        (stored.uvw_m(), 0.0)
    } else if request.project_to_observation_plane {
        source
            .geometry_engine
            .reproject_raw_uvw_for_faceted_gridft_between_j2000_directions(
                stored.uvw_m(),
                [
                    observation_direction.longitude_rad(),
                    observation_direction.latitude_rad(),
                ],
                target_angles,
            )?
    } else {
        source
            .geometry_engine
            .reproject_raw_uvw_for_gridft_between_j2000_directions(
                stored.uvw_m(),
                [
                    observation_direction.longitude_rad(),
                    observation_direction.latitude_rad(),
                ],
                target_angles,
            )?
    };
    SelectedPhaseCentreProjection::new(transformed_uvw_m, phase_shift_m)
        .ok_or(BoundObservationSourceError::InvalidRowGeometry)
}

const fn direction_ref(frame: DirectionFrame) -> DirectionRef {
    match frame {
        DirectionFrame::Icrs => DirectionRef::ICRS,
        DirectionFrame::J2000 => DirectionRef::J2000,
        DirectionFrame::B1950 => DirectionRef::B1950,
        DirectionFrame::Galactic => DirectionRef::GALACTIC,
    }
}

fn evaluate_observation_pointings(
    source: &BoundObservationSource,
    problem: &CompiledProblem,
    buffer: &SelectedObservationBuffer,
) -> Result<Option<Vec<SelectedPointingDirections>>, BoundObservationSourceError> {
    let PointingCentreLaw::Observation(law) = problem.geometry().centres().pointing() else {
        return Ok(None);
    };
    let mut queries = Vec::with_capacity(buffer.row_count().saturating_mul(2));
    let mut phase_directions = Vec::with_capacity(buffer.row_count());
    for row in 0..buffer.row_count() {
        let stored = buffer
            .row(row)
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
    let brackets = source
        .pointing_catalog
        .as_ref()
        .ok_or(BoundObservationSourceError::MissingPointingQueryDomain)?
        .direction_brackets(&source.geometry_engine, &queries)?;
    let mut pointings = Vec::with_capacity(buffer.row_count());
    for ((row, antenna_brackets), fallback) in brackets
        .as_chunks::<2>()
        .0
        .iter()
        .enumerate()
        .zip(phase_directions)
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

fn build_aw_pointing_epoch_plan(
    problem: &CompiledProblem,
    source: &BoundObservationSource,
) -> Result<AwPointingEpochPlan, BoundObservationSourceError> {
    let contract = problem
        .science()
        .measurement_equation()
        .aw_projection()
        .filter(|contract| contract.use_pointing())
        .ok_or(BoundObservationSourceError::UnsupportedCentreLaw)?;
    let law = match problem.geometry().centres().pointing() {
        PointingCentreLaw::Observation(law) => *law,
        _ => return Err(BoundObservationSourceError::UnsupportedCentreLaw),
    };
    let catalog = source
        .pointing_catalog
        .as_ref()
        .ok_or(BoundObservationSourceError::MissingPointingQueryDomain)?;
    let available_bytes = source
        .content_plan
        .rows_per_block()
        .checked_mul(crate::SelectedObservationRow::STORAGE_BYTES_PER_ROW)
        .ok_or(BoundObservationSourceError::MeasurementOverflow)?;
    let scan_plan = MsReadPlan::new(
        source.measurement_set.row_count(),
        MsSelectionIoBudget {
            available_bytes,
            maximum_live_blocks: 1,
            requested_bytes_per_row: crate::SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: Some(source.content_plan.rows_per_block()),
        },
    )
    .map_err(|error| MsError::InvalidInput(error.to_string()))?;
    let mut cursor = source
        .measurement_set
        .main_row_selection_cursor(scan_plan)?;
    let mut epochs = Vec::<AwPointingEpochInput>::new();
    while let Some(row) = cursor.next(&source.measurement_set)? {
        if !source.row_predicate.matches(row) {
            continue;
        }
        let key = AwPointingEpochKey {
            data_description_id: row.data_description_id(),
            field_id: row.field_id(),
            time_bits: row.time_mjd_seconds().to_bits(),
        };
        if epochs.last().is_none_or(|epoch| epoch.key != key) {
            epochs.push(AwPointingEpochInput {
                key,
                row_count: 0,
                antennas: BTreeSet::new(),
            });
        }
        let epoch = epochs.last_mut().expect("AW pointing epoch was appended");
        epoch.row_count = epoch
            .row_count
            .checked_add(1)
            .ok_or(BoundObservationSourceError::MeasurementOverflow)?;
        epoch.antennas.insert(row.antenna1());
        epoch.antennas.insert(row.antenna2());
    }

    let charts = problem
        .geometry()
        .domains()
        .iter()
        .flat_map(|domain| domain.facets().iter().map(|facet| facet.direction()))
        .map(aw_direction_coordinate)
        .collect::<Result<Vec<_>, _>>()?;
    let [grouping_arcsec, refresh_arcsec] = contract.pointing_offset_sigdev_arcsec();
    let grouping_arcsec = if grouping_arcsec > 0.0 {
        grouping_arcsec
    } else {
        1.0e-3
    };
    let mut chart_pixels_by_epoch = BTreeMap::new();
    let mut cached_field = None;
    let mut cached_row_count = 0_usize;
    let mut cached_antenna_pixels = BTreeMap::<i32, [f64; 2]>::new();
    let mut cached_grouped = AwPointingChartPixels::from([]);
    let primary_increment = problem
        .geometry()
        .domains()
        .first()
        .ok_or(BoundObservationSourceError::InvalidRowGeometry)?
        .direction()
        .increment_rad();
    let primary_increment_norm = primary_increment[0].hypot(primary_increment[1]);
    let refresh_threshold_rad = refresh_arcsec.to_radians() / 3600.0;

    for epoch in epochs {
        let time = f64::from_bits(epoch.key.time_bits);
        let fallback = pointing_epoch_phase_direction(source, problem, epoch.key.field_id, time)?;
        let queries = epoch
            .antennas
            .iter()
            .copied()
            .map(|antenna| PointingDirectionQuery::new(antenna, time))
            .collect::<Result<Vec<_>, _>>()?;
        let brackets = catalog.direction_brackets(&source.geometry_engine, &queries)?;
        let directions = epoch
            .antennas
            .iter()
            .copied()
            .zip(brackets)
            .zip(queries.iter().copied())
            .map(|((antenna, bracket), query)| {
                resolve_pointing_direction(bracket, query, law, fallback)
                    .map(|direction| (antenna, direction))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let pixels_by_chart = charts
            .iter()
            .map(|coordinate| {
                directions
                    .iter()
                    .map(|(&antenna, direction)| {
                        coordinate
                            .to_pixel(&[direction.longitude_rad(), direction.latitude_rad()])
                            .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)
                            .and_then(|pixel| {
                                pixel
                                    .get(0..2)
                                    .filter(|pixel| pixel.iter().all(|value| value.is_finite()))
                                    .map(|pixel| (antenna, [pixel[0], pixel[1]]))
                                    .ok_or(BoundObservationSourceError::InvalidRowGeometry)
                            })
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let current_primary = pixels_by_chart
            .first()
            .ok_or(BoundObservationSourceError::InvalidRowGeometry)?;
        let same_antennas = cached_antenna_pixels.len() == current_primary.len()
            && cached_antenna_pixels.keys().eq(current_primary.keys());
        let refresh = if cached_field != Some(epoch.key.field_id)
            || cached_row_count != epoch.row_count
            || !same_antennas
        {
            true
        } else {
            let residual = cached_antenna_pixels.iter().zip(current_primary).fold(
                [0.0, 0.0],
                |[sum_x, sum_y], ((_, cached), (_, current))| {
                    [
                        sum_x + cached[0] - current[0],
                        sum_y + cached[1] - current[1],
                    ]
                },
            );
            residual[0].hypot(residual[1]) / current_primary.len().max(1) as f64
                * primary_increment_norm
                >= refresh_threshold_rad
        };
        if refresh {
            let grouped =
                pixels_by_chart
                    .iter()
                    .zip(
                        problem.geometry().domains().iter().flat_map(|domain| {
                            domain.facets().iter().map(|facet| facet.direction())
                        }),
                    )
                    .map(|(pixels, direction)| {
                        casa_aw_grouped_pixels(pixels, grouping_arcsec, direction.increment_rad())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
            cached_grouped = Arc::from(grouped);
        }
        cached_field = Some(epoch.key.field_id);
        cached_row_count = epoch.row_count;
        cached_antenna_pixels = current_primary.clone();
        if chart_pixels_by_epoch
            .insert(epoch.key, Arc::clone(&cached_grouped))
            .is_some()
        {
            return Err(BoundObservationSourceError::InvalidRowGeometry);
        }
    }
    let retained_byte_ceiling = size_of::<AwPointingEpochPlan>()
        .checked_add(
            chart_pixels_by_epoch
                .values()
                .try_fold(0_usize, |bytes, charts| {
                    charts.iter().try_fold(bytes, |bytes, chart| {
                        chart
                            .len()
                            .checked_mul(usize::BITS as usize)
                            .and_then(|entries| bytes.checked_add(entries))
                    })
                })
                .ok_or(BoundObservationSourceError::MeasurementOverflow)?,
        )
        .ok_or(BoundObservationSourceError::MeasurementOverflow)?;
    Ok(AwPointingEpochPlan {
        chart_pixels_by_epoch,
        retained_byte_ceiling,
    })
}

fn aw_direction_coordinate(
    spec: casa_imaging_model::DirectionCoordinateSpec,
) -> Result<DirectionCoordinate, BoundObservationSourceError> {
    if spec.projection() != ModelProjection::Sin {
        return Err(BoundObservationSourceError::InvalidRowGeometry);
    }
    let reference = spec.reference_direction();
    let pc = spec.pc();
    Ok(DirectionCoordinate::new(
        direction_ref(reference.frame()),
        CoordinateProjection::new(ProjectionType::SIN),
        [reference.longitude_rad(), reference.latitude_rad()],
        spec.increment_rad(),
        spec.reference_pixel(),
    )
    .with_pc_matrix(arr2(&pc))
    .with_longpole(spec.pole_deg()[0].to_radians())
    .with_latpole(spec.pole_deg()[1].to_radians()))
}

fn casa_aw_grouped_pixels(
    pixels: &BTreeMap<i32, [f64; 2]>,
    grouping_arcsec: f64,
    increment_rad: [f64; 2],
) -> Result<BTreeMap<i32, [f64; 2]>, BoundObservationSourceError> {
    let bin_width =
        grouping_arcsec.to_radians() / 3600.0 / increment_rad[0].hypot(increment_rad[1]);
    if !(bin_width.is_finite() && bin_width > 0.0) || pixels.is_empty() {
        return Err(BoundObservationSourceError::InvalidRowGeometry);
    }
    let min_x = pixels
        .values()
        .map(|pixel| pixel[0])
        .fold(f64::INFINITY, f64::min);
    let min_y = pixels
        .values()
        .map(|pixel| pixel[1])
        .fold(f64::INFINITY, f64::min);
    let mut bins = BTreeMap::<i32, (i64, i64)>::new();
    let mut sums = BTreeMap::<(i64, i64), (f32, f32, usize)>::new();
    for (&antenna, &pixel) in pixels {
        let bin = (
            ((pixel[0] - min_x) / bin_width).floor() as i64,
            ((pixel[1] - min_y) / bin_width).floor() as i64,
        );
        bins.insert(antenna, bin);
        let sum = sums.entry(bin).or_insert((0.0, 0.0, 0));
        sum.0 = (f64::from(sum.0) + pixel[0]) as f32;
        sum.1 = (f64::from(sum.1) + pixel[1]) as f32;
        sum.2 = sum
            .2
            .checked_add(1)
            .ok_or(BoundObservationSourceError::MeasurementOverflow)?;
    }
    let means = sums
        .into_iter()
        .map(|(bin, (x, y, count))| (bin, [(x / count as f32) as f64, (y / count as f32) as f64]))
        .collect::<BTreeMap<_, _>>();
    Ok(bins
        .into_iter()
        .map(|(antenna, bin)| (antenna, means[&bin]))
        .collect())
}

fn pointing_epoch_phase_direction(
    source: &BoundObservationSource,
    problem: &CompiledProblem,
    field_id: i32,
    time_mjd_seconds: f64,
) -> Result<SkyDirection, BoundObservationSourceError> {
    let field =
        usize::try_from(field_id).map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
    let observation = source
        .geometry_engine
        .observation_direction_j2000(time_mjd_seconds, field)?;
    let (longitude, latitude) = observation.as_angles();
    let observation = SkyDirection::new(DirectionFrame::J2000, longitude, latitude);
    match problem.geometry().centres().phase_tracking() {
        PhaseCentreLaw::Observation => Ok(observation),
        PhaseCentreLaw::Fixed(direction) => require_fixed_j2000(*direction),
        PhaseCentreLaw::Ephemeris(target) => {
            let direction =
                source
                    .geometry_engine
                    .moving_direction_j2000(time_mjd_seconds, field, target)?;
            let (longitude, latitude) = direction.as_angles();
            Ok(SkyDirection::new(
                DirectionFrame::J2000,
                longitude,
                latitude,
            ))
        }
    }
}

fn evaluate_phase_direction(
    source: &BoundObservationSource,
    problem: &CompiledProblem,
    stored: SelectedStoredRow,
) -> Result<SkyDirection, BoundObservationSourceError> {
    let field_id = usize::try_from(stored.field_id())
        .map_err(|_| BoundObservationSourceError::InvalidRowGeometry)?;
    let (longitude_rad, latitude_rad) = source
        .geometry_engine
        .observation_direction_j2000(stored.time_mjd_seconds(), field_id)?
        .as_angles();
    let observation = SkyDirection::new(DirectionFrame::J2000, longitude_rad, latitude_rad);
    match problem.geometry().centres().phase_tracking() {
        PhaseCentreLaw::Observation => Ok(observation),
        PhaseCentreLaw::Fixed(direction) => require_fixed_j2000(*direction),
        PhaseCentreLaw::Ephemeris(target) => {
            let direction = source.geometry_engine.moving_direction_j2000(
                stored.time_mjd_seconds(),
                field_id,
                target,
            )?;
            let (longitude_rad, latitude_rad) = direction.as_angles();
            Ok(SkyDirection::new(
                DirectionFrame::J2000,
                longitude_rad,
                latitude_rad,
            ))
        }
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

pub(super) const fn selected_visibility(visibility: VisibilityColumn) -> SelectedVisibilityColumn {
    match visibility {
        VisibilityColumn::Data => SelectedVisibilityColumn::Data,
        VisibilityColumn::CorrectedData => SelectedVisibilityColumn::CorrectedData,
        VisibilityColumn::FloatData => SelectedVisibilityColumn::FloatData,
    }
}

pub(super) const fn selected_weight(weight: WeightColumn) -> SelectedWeightColumn {
    match weight {
        WeightColumn::Weight => SelectedWeightColumn::Weight,
        WeightColumn::WeightSpectrum => SelectedWeightColumn::WeightSpectrum,
    }
}

const fn frequency_frame(code: i32) -> Result<FrequencyFrame, BoundObservationSourceError> {
    match code {
        0 => Ok(FrequencyFrame::Rest),
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

#[cfg(test)]
mod aw_pointing_tests {
    use super::*;

    #[test]
    #[ignore = "requires local MeasurementSet and native source-group pointing trace"]
    fn t51_native_source_group_pointing_matches_selected_catalog() {
        use casa_imaging_model::{ObservationPointingLaw, PointingDirectionSemantic};
        use casa_types::measures::MeasuresProvider;
        let ms =
            MeasurementSet::open(std::env::var_os("CASA_RS_T51_POINTING_MS").unwrap()).unwrap();
        let native =
            std::fs::read_to_string(std::env::var_os("CASA_RS_T51_NATIVE_POINTING").unwrap())
                .unwrap();
        let centre = std::env::var("CASA_RS_T51_POINTING_CENTRE")
            .unwrap()
            .split(',')
            .map(|v| v.parse::<f64>().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(centre.len(), 2);
        let records = native
            .lines()
            .map(|line| line.split('\t').collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let group = records.iter().find(|r| r[0] == "source_group").unwrap();
        let time = group[3].parse::<f64>().unwrap();
        let expected = records
            .iter()
            .filter(|r| r[0] == "antenna_pixel")
            .map(|r| {
                (
                    r[1].parse::<i32>().unwrap(),
                    [r[2].parse::<f64>().unwrap(), r[3].parse::<f64>().unwrap()],
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert!(!expected.is_empty());
        let mut domain = crate::selected_pointing::SelectedPointingQueryDomainBuilder::default();
        for &antenna in expected.keys() {
            domain.observe_row(antenna, antenna, time, time).unwrap();
        }
        let domain = domain.finish().unwrap().unwrap();
        let catalog = ms
            .prepare_selected_pointing_catalog(
                StoredPointingDirectionColumn::Direction,
                &domain,
                PointingTimeSampling::VisibilityTime,
                PointingReadPlan::new(1024, 16, 16 * 1024 * 1024).unwrap(),
            )
            .unwrap();
        let measures_root = records
            .iter()
            .find(|r| r[0] == "native_measures_root")
            .unwrap()[1];
        let runtime = crate::test_helpers::production_measures_runtime_at(measures_root).unwrap();
        eprintln!("rust_measures_root\t{}", runtime.root().display());
        let measures: std::sync::Arc<dyn MeasuresProvider> = runtime;
        let state = measures.prepare_bounded_state().unwrap().unwrap();
        eprintln!("rust_measures_identity\t{:?}", state.identity_sha256());
        eprintln!(
            "rust_eop\t{:?}\ttai_minus_utc={}",
            measures.eop_values(time / 86_400.0).unwrap(),
            measures.tai_minus_utc_seconds(time / 86_400.0).unwrap()
        );
        let engine = MsCalEngine::new_selected_observation(&ms, measures, None).unwrap();
        let queries = expected
            .keys()
            .map(|&antenna| PointingDirectionQuery::new(antenna, time).unwrap())
            .collect::<Vec<_>>();
        let brackets = catalog.direction_brackets(&engine, &queries).unwrap();
        let law = ObservationPointingLaw::new(
            PointingDirectionColumn::Direction,
            PointingDirectionSemantic::AntennaBoresight,
            PointingTimeSampling::VisibilityTime,
            PointingInterpolation::Nearest,
            PointingExtrapolation::HoldNearest,
            MissingPointingPolicy::UsePhaseTrackingCentre,
        );
        let field_direction = engine
            .field_direction_j2000(group[4].parse().unwrap())
            .unwrap();
        let fallback = SkyDirection::new(
            DirectionFrame::J2000,
            field_direction.longitude_rad(),
            field_direction.latitude_rad(),
        );
        let cell = (0.6_f64 / 3600.0).to_radians();
        let coordinate = DirectionCoordinate::new(
            DirectionRef::J2000,
            CoordinateProjection::new(ProjectionType::SIN),
            [centre[0], centre[1]],
            [-cell, cell],
            [256.0, 256.0],
        )
        .with_longpole(std::f64::consts::PI)
        .with_latpole(centre[1]);
        let mut pixels = BTreeMap::new();
        let mut maximum_error = 0.0_f64;
        for ((&antenna, query), bracket) in expected.keys().zip(queries).zip(brackets) {
            let direction = resolve_pointing_direction(bracket, query, law, fallback).unwrap();
            let pixel = coordinate
                .to_pixel(&[direction.longitude_rad(), direction.latitude_rad()])
                .unwrap();
            for axis in 0..2 {
                maximum_error = maximum_error.max((pixel[axis] - expected[&antenna][axis]).abs());
            }
            pixels.insert(antenna, [pixel[0], pixel[1]]);
        }
        eprintln!(
            "pointing_pixel_comparison antennas={} maximum_error_pixels={maximum_error:.17e}",
            pixels.len()
        );
        assert!(maximum_error < 1.0e-5, "native POINTING projection differs");
        let grouped = casa_aw_grouped_pixels(&pixels, 600.0, [-cell, cell]).unwrap();
        let baseline = records
            .iter()
            .find(|r| r[0] == "selected_baseline")
            .unwrap();
        let midpoint = records.iter().find(|r| r[0] == "grouped_midpoint").unwrap();
        let native_midpoint = [
            midpoint[1].parse::<f64>().unwrap(),
            midpoint[2].parse::<f64>().unwrap(),
        ];
        let left = grouped[&baseline[1].parse::<i32>().unwrap()];
        let right = grouped[&baseline[2].parse::<i32>().unwrap()];
        assert_eq!(
            [(left[0] + right[0]) / 2.0, (left[1] + right[1]) / 2.0],
            native_midpoint,
            "native grouped POINTING midpoint differs"
        );
    }

    #[test]
    fn casa_default_pointing_group_collapses_compact_array_to_f32_mean() {
        let pixels = BTreeMap::from([
            (0, [256.125_000_01, 255.875_000_01]),
            (1, [257.250_000_02, 256.500_000_02]),
            (2, [255.750_000_03, 257.125_000_03]),
        ]);

        let grouped = casa_aw_grouped_pixels(
            &pixels,
            600.0,
            [0.6_f64.to_radians() / 3600.0, 0.6_f64.to_radians() / 3600.0],
        )
        .expect("CASA default pointing grouping");
        let casa_mean = |values: [f64; 3]| {
            let sum = values
                .into_iter()
                .fold(0.0_f32, |sum, value| (f64::from(sum) + value) as f32);
            f64::from(sum / 3.0)
        };
        let expected_x = casa_mean([256.125_000_01, 257.250_000_02, 255.750_000_03]);
        let expected_y = casa_mean([255.875_000_01, 256.500_000_02, 257.125_000_03]);

        assert_eq!(grouped.len(), pixels.len());
        assert!(
            grouped
                .values()
                .all(|pixel| *pixel == [expected_x, expected_y])
        );
    }

    #[test]
    fn casa_pointing_group_keeps_separated_bins_independent() {
        let pixels = BTreeMap::from([(0, [10.0, 20.0]), (1, [10.2, 20.2]), (2, [30.0, 40.0])]);

        let grouped = casa_aw_grouped_pixels(&pixels, 1.0, [1.0_f64.to_radians() / 3600.0, 0.0])
            .expect("separated CASA pointing groups");

        assert_eq!(
            grouped[&0],
            [10.100_000_381_469_727, 20.100_000_381_469_727]
        );
        assert_eq!(grouped[&1], grouped[&0]);
        assert_eq!(grouped[&2], [30.0, 40.0]);
    }
}
