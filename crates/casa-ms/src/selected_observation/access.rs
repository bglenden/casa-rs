// SPDX-License-Identifier: LGPL-3.0-or-later

use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
    sync::Arc,
};

use crate::derived::engine::{MsCalEngine, polarization_operator_angle};
use crate::subtables::SubTable;
use crate::{
    MainRowSelectionCursor, MainRowWalk, MeasurementSet, MsError, MsReadPlan, MsSelectionIoBudget,
    ObservationOwnerError, PointingDirectionColumn as StoredPointingDirectionColumn,
    PointingReadPlan, PreparedSelectedPointingCatalog, SelectedObservationBuffer,
    SelectedObservationBufferCapacity, SelectedObservationBufferRequest, SelectedObservationRow,
    SelectedPointingQueryDomain, SelectedStoredRow, SelectedVisibilityColumn, SelectedWeightColumn,
    VisibilityChannelReadRange,
};
use casa_imaging_model::{
    CompiledProblem, CorrelationProduct, DataDescriptionSelection, DirectionFrame, Epoch,
    FrequencyFrame, InstrumentModel, MeasurementSetReadAccess, ObservationSelection,
    ObservationSource, PointingCentreLaw, PointingDirectionColumn, SelectedAntennaResponses,
    SelectedObservationRunChannel, SelectedObservationRunRow, SelectedSampleCoordinates,
    SelectedSampleMetadata, TimeScale, VisibilityColumn, WeightColumn,
};
use casa_types::Correlation;
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

mod channel_window;
mod geometry;
mod walk;

use channel_window::selected_channel_window;
pub(super) use geometry::EvaluatedRowGeometry;
use geometry::{
    build_aw_pointing_epoch_plan, evaluate_observation_pointings, evaluate_row_geometry,
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
/// content buffers. Each pass walks MAIN once, selecting rows with the compiled row predicate
/// and reading their values: in physical order with one DATA_DESCRIPTION selected, and with
/// several one DATA_DESCRIPTION after another, physical order within each, so a pass meets
/// each spectral window's convolution functions together instead of cycling through every
/// window's (#659). The first traversal records that walk ([`MainRowWalk`]).
pub(crate) struct BoundObservationSource {
    measurement_set: MeasurementSet,
    geometry_engine: Arc<MsCalEngine>,
    row_predicate: CompiledRowPredicate,
    coordinates: Arc<[SelectedCoordinates]>,
    walk: std::sync::OnceLock<Arc<MainRowWalk>>,
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

    /// The storage one block of this source needs.
    pub(super) const fn buffer_capacity(&self) -> SelectedObservationBufferCapacity {
        self.content_plan.buffer_capacity()
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
            walk: std::sync::OnceLock::new(),
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

    /// Fill `block` with this source's next row group, its visibilities only
    /// when `visibilities` asks for them, and return what the fill bound it
    /// to; `None` once the source's selected rows are spent.
    pub(super) fn fill_next_selected_block(
        &self,
        problem: &CompiledProblem,
        logical_source: &MeasurementSetReadAccess,
        replay: &mut SelectedRowReplay,
        block: &mut SelectedObservationBlock,
        window: Option<[f64; 2]>,
        visibilities: bool,
    ) -> Result<Option<BlockBinding>, BoundObservationSourceError> {
        loop {
            let Some(coordinate_index) =
                self.fill_selected_row_group(replay, &mut block.selected_rows)?
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
                        &block.selected_rows,
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
            self.measurement_set.fill_selected_observation_buffer(
                &SelectedObservationBufferRequest::new(
                    visibilities.then(|| {
                        selected_visibility(logical_source.selected_columns().visibility())
                    }),
                    selected_weight(logical_source.selected_columns().weights()),
                    &block.selected_rows,
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
        let cursor = if self.coordinates.len() > 1 {
            let walk = match self.walk.get() {
                Some(walk) => Arc::clone(walk),
                None => {
                    let walk = Arc::new(self.row_walk(plan)?);
                    Arc::clone(self.walk.get_or_init(|| walk))
                }
            };
            self.measurement_set.main_row_walk_cursor(plan, walk)?
        } else {
            self.measurement_set.main_row_selection_cursor(plan)?
        };
        Ok(SelectedRowReplay {
            cursor,
            pending: None,
            selected_rows: 0,
        })
    }

    /// Collect into `rows` the next consecutive selected rows of one
    /// DATA_DESCRIPTION, at most a block's worth, and return the index of
    /// their coordinates.
    fn fill_selected_row_group(
        &self,
        replay: &mut SelectedRowReplay,
        rows: &mut Vec<SelectedObservationRow>,
    ) -> Result<Option<usize>, BoundObservationSourceError> {
        let Some(first) = replay.next_selected(self)? else {
            return Ok(None);
        };
        let data_description_id = u32::try_from(first.data_description_id()).map_err(|_| {
            BoundObservationSourceError::DataDescriptionCoordinateMismatch {
                data_description_id: u32::MAX,
            }
        })?;
        let coordinate_index = self
            .coordinates
            .iter()
            .position(|coordinates| {
                coordinates.data_description.data_description_id() == data_description_id
            })
            .ok_or(
                BoundObservationSourceError::DataDescriptionCoordinateMismatch {
                    data_description_id,
                },
            )?;
        rows.clear();
        rows.push(first);
        while rows.len() < self.content_plan.rows_per_block() {
            let Some(row) = replay.next_selected(self)? else {
                break;
            };
            if row.data_description_id() != first.data_description_id() {
                replay.pending = Some(row);
                break;
            }
            rows.push(row);
        }
        Ok(Some(coordinate_index))
    }
}

/// The selected MAIN rows of one source, in physical order: a cursor over
/// MAIN filtered by the compiled row predicate.
pub(super) struct SelectedRowReplay {
    cursor: MainRowSelectionCursor,
    pending: Option<SelectedObservationRow>,
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
    ) -> Result<Option<SelectedObservationRow>, BoundObservationSourceError> {
        if let Some(row) = self.pending.take() {
            return Ok(Some(row));
        }
        while let Some(row) = self.cursor.next(&source.measurement_set)? {
            if source.row_predicate.matches(row) {
                self.selected_rows += 1;
                return Ok(Some(row));
            }
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
    /// The stored rows of the group being filled, as the row walk read
    /// them; the buffer copies their metadata rather than reading it again.
    selected_rows: Vec<SelectedObservationRow>,
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
    /// Storage allocated once for blocks within `capacity`, which no fill
    /// grows.
    pub(super) fn new(capacity: SelectedObservationBufferCapacity) -> Self {
        Self {
            buffer: SelectedObservationBuffer::with_capacity(capacity),
            row_geometry: Vec::with_capacity(capacity.rows()),
            selected_rows: Vec::with_capacity(capacity.rows()),
        }
    }

    /// The bytes the buffer's vectors hold allocated.
    #[cfg(test)]
    pub(super) fn buffer_allocated_bytes(&self) -> usize {
        self.buffer.allocated_bytes()
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
            unread @ crate::SelectedNumericVisibility::Unread => unread,
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
            if first.correlation_type() == Correlation::RR
                && last.correlation_type() == Correlation::LL =>
        {
            matches_canonical_correlation_order(
                products,
                &[
                    Correlation::RR,
                    Correlation::RL,
                    Correlation::LR,
                    Correlation::LL,
                ],
            )
        }
        (Some(first), Some(last))
            if first.correlation_type() == Correlation::XX
                && last.correlation_type() == Correlation::YY =>
        {
            matches_canonical_correlation_order(
                products,
                &[
                    Correlation::XX,
                    Correlation::XY,
                    Correlation::YX,
                    Correlation::YY,
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
    canonical: &[Correlation; 4],
) -> bool {
    let mut remaining = canonical.iter();
    products.iter().all(|product| {
        remaining
            .by_ref()
            .any(|candidate| *candidate == product.correlation_type())
    })
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
