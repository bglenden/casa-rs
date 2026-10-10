// SPDX-License-Identifier: LGPL-3.0-or-later

use crate::{
    MeasurementSet, MsError, PointingDirectionBracket, PointingDirectionQuery,
    SelectedPointingCatalogMeasurements, derived::engine::MsCalEngine,
};
use crate::{
    selected_observation_buffer::selected_observation_buffer_residency,
    selected_pointing::selected_pointing_preparation_peak_bytes, subtables::SubTable,
};
use casa_imaging_model::{
    CompiledProblem, CorrelationProduct, ObservationSource, PointingCentreLaw,
    SelectedImageDomainProjections, SelectedPointingDirections, SkyDirection, VisibilityColumn,
    WeightColumn,
};
use thiserror::Error;

use super::access::{
    BoundObservationSource, BufferedObservationBlock, EvaluatedRowGeometry, SelectedChannel,
    SelectedCoordinates, SelectedReplayRow,
};
use super::row_selection::CompiledRowPredicate;

/// Transient scratch while a source is constructed, beyond the spectral
/// coordinate arrays charged by size: one subtable cell at a time from
/// ANTENNA, FIELD, OBSERVATION and POINTING, the predicate's DATA_DESCRIPTION
/// table, the binding and source-slot vectors and the POINTING query domain.
/// Each is at most a few kilobytes; the slack covers them without reading
/// every subtable row to size them. It is charged to traversal as well, so
/// it never decides which phase bounds a block.
const CONSTRUCTION_SLACK_BYTES: usize = 64 << 10;

mod requirements;
pub use requirements::SelectedObservationContentRequirements;

/// Once-only allocations shared by one bound selected-observation owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SelectedObservationSharedBytes {
    shared_measures_retained_bytes: usize,
    shared_reference_data_retained_bytes: usize,
    shared_source_plan_retained_bytes: usize,
}

impl SelectedObservationSharedBytes {
    pub(crate) const NONE: Self = Self::new(0, 0);

    pub(crate) const fn new(
        shared_measures_retained_bytes: usize,
        shared_reference_data_retained_bytes: usize,
    ) -> Self {
        Self {
            shared_measures_retained_bytes,
            shared_reference_data_retained_bytes,
            shared_source_plan_retained_bytes: 0,
        }
    }

    pub(crate) const fn with_source_plan_retained_bytes(mut self, bytes: usize) -> Self {
        self.shared_source_plan_retained_bytes = bytes;
        self
    }
}

/// Explicit memory available to one selected source: its retained metadata
/// and the one content block its stream holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedObservationContentBudget {
    available_bytes: usize,
    maximum_live_blocks: usize,
    maximum_pointing_polynomial_terms: usize,
}

/// Explicit retained-byte ceiling for selected-observation reference data.
///
/// This capability can only be derived from a selected-content budget, keeping
/// reference-data loading under the same resource authority as later source
/// planning and traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedObservationReferenceDataBudget {
    available_bytes: usize,
}

impl SelectedObservationContentBudget {
    /// Construct a content budget from explicit resource-authority values.
    #[must_use]
    pub const fn new(
        available_bytes: usize,
        maximum_live_blocks: usize,
        maximum_pointing_polynomial_terms: usize,
    ) -> Self {
        Self {
            available_bytes,
            maximum_live_blocks,
            maximum_pointing_polynomial_terms,
        }
    }

    /// Return the bytes available to the source.
    #[must_use]
    pub const fn available_bytes(self) -> usize {
        self.available_bytes
    }

    /// Derive the reference-data admission ceiling from this content authority.
    #[must_use]
    pub const fn reference_data_budget(self) -> SelectedObservationReferenceDataBudget {
        SelectedObservationReferenceDataBudget {
            available_bytes: self.available_bytes,
        }
    }

    /// Return the live-block allowance of metadata row walks
    /// ([`Self::row_io_budget`]); the content stream holds one block.
    #[must_use]
    pub const fn maximum_live_blocks(self) -> usize {
        self.maximum_live_blocks
    }

    /// Return the maximum accepted POINTING polynomial coefficient count per axis.
    #[must_use]
    pub const fn maximum_pointing_polynomial_terms(self) -> usize {
        self.maximum_pointing_polynomial_terms
    }

    /// The read budget of one pass over the selected MAIN rows: this
    /// budget's bytes and live blocks, at a row's stored size.
    #[must_use]
    pub const fn row_io_budget(self) -> crate::MsSelectionIoBudget {
        crate::MsSelectionIoBudget {
            available_bytes: self.available_bytes,
            maximum_live_blocks: self.maximum_live_blocks,
            requested_bytes_per_row: crate::SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: None,
        }
    }
}

impl SelectedObservationReferenceDataBudget {
    /// Return the retained bytes available for immutable reference data.
    #[must_use]
    pub const fn available_bytes(self) -> usize {
        self.available_bytes
    }
}

/// Checked logical payload plan for one bounded selected-content block.
///
/// The projection charges every owner-visible heap capacity in the casa-ms selected-content
/// buffer, the bounded POINTING scalar scan and candidate set, evaluated row geometry, shared
/// compiler manifests, and retained metadata. Physical row indices, selected visibility
/// precision, flags, exact input weights, raw UVW, time coordinates, and MAIN provenance are all
/// included. Boolean vectors are conservatively charged as one byte per value. Platform allocator
/// bookkeeping outside those Rust allocations remains the allocator's responsibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedObservationContentPlan {
    retained_bytes: usize,
    initialization_scratch_bytes: usize,
    resident_bytes_per_row: usize,
    preparation_bytes_per_row: usize,
    rows_per_block: usize,
    resident_bytes_per_block: usize,
    preparation_bytes_per_block: usize,
    maximum_resident_bytes: usize,
    maximum_pointing_polynomial_terms: usize,
}

impl SelectedObservationContentPlan {
    /// Logical metadata bytes retained for the lifetime of the bound source.
    #[must_use]
    #[cfg(test)]
    pub const fn retained_bytes(self) -> usize {
        self.retained_bytes
    }

    /// Maximum transient coordinate-catalog construction scratch.
    #[must_use]
    #[cfg(test)]
    pub const fn initialization_scratch_bytes(self) -> usize {
        self.initialization_scratch_bytes
    }

    /// Maximum bytes retained by one selected MAIN row after preparation.
    #[must_use]
    #[cfg(test)]
    pub const fn bytes_per_row(self) -> usize {
        self.resident_bytes_per_row
    }

    /// Maximum bytes live per row while one block is being prepared.
    #[must_use]
    #[cfg(test)]
    pub const fn preparation_bytes_per_row(self) -> usize {
        self.preparation_bytes_per_row
    }

    /// Maximum selected MAIN rows retained by one content block.
    #[must_use]
    pub const fn rows_per_block(self) -> usize {
        self.rows_per_block
    }

    /// Maximum payload bytes retained by one prepared content block.
    #[must_use]
    #[cfg(test)]
    pub const fn bytes_per_block(self) -> usize {
        self.resident_bytes_per_block
    }

    /// Maximum payload bytes live while filling and evaluating one full block.
    #[must_use]
    #[cfg(test)]
    pub const fn preparation_bytes_per_block(self) -> usize {
        self.preparation_bytes_per_block
    }

    /// Maximum modeled owner-resident bytes across initialization and traversal.
    #[must_use]
    pub const fn maximum_resident_bytes(self) -> usize {
        self.maximum_resident_bytes
    }

    /// Return the maximum accepted POINTING polynomial coefficient count per axis.
    #[must_use]
    pub const fn maximum_pointing_polynomial_terms(self) -> usize {
        self.maximum_pointing_polynomial_terms
    }
}

/// Failure to derive an exact bounded selected-content plan.
#[derive(Debug, Error)]
pub enum SelectedObservationContentPlanError {
    /// Storage metadata required by the logical payload projection was unreadable.
    #[error(transparent)]
    Storage(#[from] MsError),
    /// The explicit resource budget is empty or otherwise inconsistent.
    #[error("selected-observation content budget must have positive bytes and live blocks")]
    InvalidBudget,
    /// One selected row cannot fit in the per-block budget.
    #[error(
        "one selected-observation row requires {required_bytes} bytes but one content block has {available_bytes} bytes"
    )]
    InsufficientBudget {
        /// Maximum logical bytes required by one selected row.
        required_bytes: usize,
        /// Bytes available to one simultaneously live block.
        available_bytes: usize,
    },
    /// Retained metadata and bounded coordinate-construction scratch exceed the budget.
    #[error(
        "selected-observation retained metadata requires {required_bytes} bytes but the content budget has {available_bytes} bytes"
    )]
    InsufficientRetainedBudget {
        /// Retained metadata plus its maximum initialization scratch.
        required_bytes: usize,
        /// Total source content budget.
        available_bytes: usize,
    },
    /// Checked logical byte arithmetic overflowed.
    #[error("selected-observation content byte projection overflowed")]
    ByteOverflow,
    /// A compiled selected spectral or polarization coordinate was empty or invalid.
    #[error("compiled selected-observation coordinate shape is invalid")]
    InvalidCoordinateShape,
}

#[cfg(test)]
pub(crate) fn selected_content_plan(
    measurement_set: &MeasurementSet,
    problem: &CompiledProblem,
    source: &ObservationSource,
    shared_bytes: SelectedObservationSharedBytes,
    budget: SelectedObservationContentBudget,
) -> Result<SelectedObservationContentPlan, SelectedObservationContentPlanError> {
    selected_content_plan_with_pointing_catalog(
        measurement_set,
        problem,
        source,
        shared_bytes,
        budget,
        None,
    )
}

pub(crate) fn selected_pointing_catalog_budget(
    measurement_set: &MeasurementSet,
    source: &ObservationSource,
    shared_bytes: SelectedObservationSharedBytes,
    budget: SelectedObservationContentBudget,
) -> Result<usize, SelectedObservationContentPlanError> {
    let (retained_bytes, _) = retained_source_bytes(measurement_set, source, shared_bytes)?;
    budget
        .available_bytes
        .checked_sub(retained_bytes)
        .and_then(|bytes| bytes.checked_sub(CONSTRUCTION_SLACK_BYTES))
        .ok_or(
            SelectedObservationContentPlanError::InsufficientRetainedBudget {
                required_bytes: retained_bytes
                    .checked_add(CONSTRUCTION_SLACK_BYTES)
                    .ok_or(SelectedObservationContentPlanError::ByteOverflow)?,
                available_bytes: budget.available_bytes,
            },
        )
}

pub(crate) fn selected_content_plan_with_pointing_catalog(
    measurement_set: &MeasurementSet,
    problem: &CompiledProblem,
    source: &ObservationSource,
    shared_bytes: SelectedObservationSharedBytes,
    budget: SelectedObservationContentBudget,
    pointing_catalog: Option<SelectedPointingCatalogMeasurements>,
) -> Result<SelectedObservationContentPlan, SelectedObservationContentPlanError> {
    selected_content_requirements(
        measurement_set,
        problem,
        source,
        shared_bytes,
        budget.maximum_pointing_polynomial_terms,
        pointing_catalog,
        0,
    )?
    .plan(budget)
}

pub(crate) fn selected_content_requirements(
    measurement_set: &MeasurementSet,
    problem: &CompiledProblem,
    source: &ObservationSource,
    shared_bytes: SelectedObservationSharedBytes,
    maximum_pointing_polynomial_terms: usize,
    pointing_catalog: Option<SelectedPointingCatalogMeasurements>,
    initialization_scan_bytes_per_row: usize,
) -> Result<SelectedObservationContentRequirements, SelectedObservationContentPlanError> {
    if maximum_pointing_polynomial_terms == 0 {
        return Err(SelectedObservationContentPlanError::InvalidBudget);
    }
    let (noncatalog_retained_bytes, coordinate_construction_scratch_bytes) =
        retained_source_bytes(measurement_set, source, shared_bytes)?;
    let retained_bytes = noncatalog_retained_bytes
        .checked_add(pointing_catalog.map_or(0, |catalog| catalog.retained_bytes()))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let catalog_initialization_scratch_bytes = pointing_catalog
        .map(|catalog| {
            catalog
                .construction_peak_bytes()
                .checked_sub(catalog.retained_bytes())
                .ok_or(SelectedObservationContentPlanError::ByteOverflow)
        })
        .transpose()?
        .unwrap_or(0);
    let initialization_scratch_bytes = coordinate_construction_scratch_bytes
        .max(catalog_initialization_scratch_bytes)
        .checked_add(CONSTRUCTION_SLACK_BYTES)
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let domain_projection_payload_bytes =
        SelectedImageDomainProjections::retained_heap_bytes_for_len(
            problem
                .geometry()
                .domains()
                .iter()
                .try_fold(0_usize, |count, domain| {
                    count.checked_add(domain.facets().len())
                })
                .ok_or(SelectedObservationContentPlanError::ByteOverflow)?,
        )
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let row_replay_fixed_bytes = BoundObservationSource::row_replay_fixed_bytes();
    let traversal_base_bytes = retained_bytes
        // The generation encoder may retain the final row's shared projection
        // payload while its source block is recycled.
        .checked_add(domain_projection_payload_bytes)
        .and_then(|bytes| bytes.checked_add(row_replay_fixed_bytes))
        .and_then(|bytes| bytes.checked_add(CONSTRUCTION_SLACK_BYTES))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let polarization = measurement_set.polarization()?;
    let mut resident_bytes_per_row = 0_usize;
    let mut fill_bytes_per_row = 0_usize;
    let mut preparation_bytes_per_row = 0_usize;
    let empty_fill = selected_observation_buffer_residency(0, 0, 0, 0)
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let fill_fixed_bytes = empty_fill.fill_peak_bytes;
    let pointing_direction_column = match problem.geometry().centres().pointing() {
        PointingCentreLaw::Observation(law) => Some(match law.direction_column() {
            casa_imaging_model::PointingDirectionColumn::Direction => {
                crate::PointingDirectionColumn::Direction
            }
            casa_imaging_model::PointingDirectionColumn::Target => {
                crate::PointingDirectionColumn::Target
            }
        }),
        PointingCentreLaw::PhaseTrackingCentre
        | PointingCentreLaw::FieldCentre
        | PointingCentreLaw::Fixed(_) => None,
    };
    for description in source.selection().data_descriptions() {
        let channels = source
            .selection()
            .spectral_windows()
            .iter()
            .find(|selection| selection.spectral_window_id() == description.spectral_window_id())
            .expect("compiled DATA_DESCRIPTION has one spectral-window selection")
            .channel_indices();
        let (Some(first_channel), Some(last_channel)) = (channels.first(), channels.last()) else {
            return Err(SelectedObservationContentPlanError::InvalidCoordinateShape);
        };
        let covering_channels = usize::try_from(
            last_channel
                .checked_sub(*first_channel)
                .and_then(|span| span.checked_add(1))
                .ok_or(SelectedObservationContentPlanError::ByteOverflow)?,
        )
        .map_err(|_| SelectedObservationContentPlanError::ByteOverflow)?;
        let polarization_row = usize::try_from(description.polarization_id())
            .map_err(|_| SelectedObservationContentPlanError::InvalidCoordinateShape)?;
        let correlations =
            selected_i32_array_len(polarization.table(), "CORR_TYPE", polarization_row)?
                .filter(|count| *count > 0)
                .ok_or(SelectedObservationContentPlanError::InvalidCoordinateShape)?;
        let sample_count = covering_channels
            .checked_mul(correlations)
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let visibility_bytes = match source.columns().visibility() {
            VisibilityColumn::Data | VisibilityColumn::CorrectedData => 8,
            VisibilityColumn::FloatData => 4,
        };
        let weight_values = match source.columns().weights() {
            WeightColumn::Weight => correlations,
            WeightColumn::WeightSpectrum => sample_count,
        };
        let buffer =
            selected_observation_buffer_residency(1, sample_count, weight_values, visibility_bytes)
                .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let resident = buffer
            .resident_bytes
            .checked_add(size_of::<EvaluatedRowGeometry>())
            .and_then(|bytes| {
                bytes.checked_add(size_of::<SelectedReplayRow>() + size_of::<usize>())
            })
            .and_then(|bytes| bytes.checked_add(domain_projection_payload_bytes))
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        // A recycled block keeps its row geometry, request indices and frequency-window
        // metadata allocations while refilling storage and preparing POINTING output.
        // Charge their capacity during preparation as well as completed handoff.
        let retained_geometry = size_of::<EvaluatedRowGeometry>()
            .checked_add(domain_projection_payload_bytes)
            .and_then(|bytes| {
                bytes.checked_add(size_of::<SelectedReplayRow>() + size_of::<usize>())
            })
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let fill = buffer
            .fill_peak_bytes
            .checked_sub(fill_fixed_bytes)
            .and_then(|bytes| bytes.checked_add(retained_geometry))
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let geometry_build = buffer
            .resident_bytes
            .checked_add(size_of::<EvaluatedRowGeometry>())
            .and_then(|bytes| {
                bytes.checked_add(size_of::<SelectedReplayRow>() + size_of::<usize>())
            })
            // Vec-to-Arc construction can hold source and destination payloads
            // simultaneously for the row currently being arranged.
            .and_then(|bytes| {
                domain_projection_payload_bytes
                    .checked_mul(2)
                    .and_then(|payload| bytes.checked_add(payload))
            })
            .and_then(|bytes| {
                let pointing_output = if pointing_direction_column.is_some() {
                    size_of::<SelectedPointingDirections>()
                } else {
                    0
                };
                pointing_output.checked_add(bytes)
            })
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let pointing = if let Some(direction_column) = pointing_direction_column {
            let pointing_scratch = if pointing_catalog.is_some() {
                2 * size_of::<PointingDirectionQuery>()
                    + size_of::<SkyDirection>()
                    + 2 * size_of::<PointingDirectionBracket>()
                    + size_of::<SelectedPointingDirections>()
            } else {
                selected_pointing_preparation_peak_bytes(
                    1,
                    1,
                    maximum_pointing_polynomial_terms,
                    direction_column,
                )
                .ok_or(SelectedObservationContentPlanError::ByteOverflow)?
            };
            buffer
                .resident_bytes
                .checked_add(retained_geometry)
                .and_then(|bytes| bytes.checked_add(pointing_scratch))
                .ok_or(SelectedObservationContentPlanError::ByteOverflow)?
        } else {
            0
        };
        resident_bytes_per_row = resident_bytes_per_row.max(resident);
        fill_bytes_per_row = fill_bytes_per_row.max(fill);
        preparation_bytes_per_row = preparation_bytes_per_row
            .max(fill)
            .max(pointing)
            .max(geometry_build);
    }
    if resident_bytes_per_row == 0 || preparation_bytes_per_row == 0 {
        return Err(SelectedObservationContentPlanError::InvalidCoordinateShape);
    }
    let selected_rows = usize::try_from(source.selection().rows().selected_row_count())
        .map_err(|_| SelectedObservationContentPlanError::ByteOverflow)?;
    if selected_rows == 0 {
        return Err(SelectedObservationContentPlanError::InvalidCoordinateShape);
    }
    Ok(SelectedObservationContentRequirements {
        retained_bytes,
        initialization_scratch_bytes,
        initialization_scan_bytes_per_row,
        traversal_base_bytes,
        resident_bytes_per_row,
        fill_bytes_per_row,
        preparation_bytes_per_row,
        fill_fixed_bytes,
        selected_rows,
        maximum_pointing_polynomial_terms,
    })
}

/// The bytes a bound source retains, and the largest spectral-coordinate
/// scratch of its construction (one SPW's CHAN_FREQ and CHAN_WIDTH).
fn retained_source_bytes(
    measurement_set: &MeasurementSet,
    source: &ObservationSource,
    shared_bytes: SelectedObservationSharedBytes,
) -> Result<(usize, usize), SelectedObservationContentPlanError> {
    let storage_bytes = measurement_set
        .retained_read_metadata_heap_bytes()
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let geometry_bytes = MsCalEngine::selected_observation_retained_heap_bytes(measurement_set)?;
    let manifest_bytes = source
        .selection()
        .retained_manifest_bytes()
        .and_then(|bytes| bytes.checked_add(source.provenance().retained_locator_bytes()))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let predicate_bytes = CompiledRowPredicate::shared_retained_heap_bytes(source)
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let spectral_windows = measurement_set.spectral_window()?;
    let mut coordinate_bytes = source
        .selection()
        .data_descriptions()
        .len()
        .checked_mul(size_of::<SelectedCoordinates>())
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let mut spectral_scratch_bytes = 0_usize;
    for description in source.selection().data_descriptions() {
        let spectral_window = source
            .selection()
            .spectral_windows()
            .iter()
            .find(|selection| selection.spectral_window_id() == description.spectral_window_id())
            .expect("compiled DATA_DESCRIPTION has one spectral-window selection");
        let polarization = source
            .selection()
            .correlations()
            .iter()
            .find(|selection| selection.polarization_id() == description.polarization_id())
            .expect("compiled DATA_DESCRIPTION has one polarization selection");
        coordinate_bytes = coordinate_bytes
            .checked_add(
                spectral_window
                    .channel_indices()
                    .len()
                    .checked_mul(size_of::<SelectedChannel>())
                    .ok_or(SelectedObservationContentPlanError::ByteOverflow)?,
            )
            .and_then(|bytes| {
                polarization
                    .products()
                    .len()
                    .checked_mul(size_of::<CorrelationProduct>())
                    .and_then(|products| bytes.checked_add(products))
            })
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;

        let spectral_window_row = usize::try_from(description.spectral_window_id())
            .map_err(|_| SelectedObservationContentPlanError::InvalidCoordinateShape)?;
        let frequency_count =
            selected_f64_array_len(spectral_windows.table(), "CHAN_FREQ", spectral_window_row)?
                .filter(|count| *count > 0)
                .ok_or(SelectedObservationContentPlanError::InvalidCoordinateShape)?;
        let width_count =
            selected_f64_array_len(spectral_windows.table(), "CHAN_WIDTH", spectral_window_row)?
                .filter(|count| *count > 0)
                .ok_or(SelectedObservationContentPlanError::InvalidCoordinateShape)?;
        // CHAN_FREQ and CHAN_WIDTH of one SPW are live together.
        spectral_scratch_bytes = spectral_scratch_bytes.max(
            frequency_count
                .checked_add(width_count)
                .and_then(|values| values.checked_mul(size_of::<f64>()))
                .ok_or(SelectedObservationContentPlanError::ByteOverflow)?,
        );
    }
    let retained_bytes = shared_bytes
        .shared_measures_retained_bytes
        .checked_add(shared_bytes.shared_reference_data_retained_bytes)
        .and_then(|bytes| bytes.checked_add(shared_bytes.shared_source_plan_retained_bytes))
        .and_then(|bytes| bytes.checked_add(storage_bytes))
        .and_then(|bytes| bytes.checked_add(geometry_bytes))
        .and_then(|bytes| bytes.checked_add(manifest_bytes))
        .and_then(|bytes| bytes.checked_add(predicate_bytes))
        .and_then(|bytes| bytes.checked_add(coordinate_bytes))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    Ok((retained_bytes, spectral_scratch_bytes))
}

fn selected_f64_array_len(
    table: &casa_tables::Table,
    column: &str,
    row: usize,
) -> Result<Option<usize>, SelectedObservationContentPlanError> {
    Ok(
        match table
            .column_accessor(column)
            .map_err(MsError::from)?
            .array_cells_owned_uncached(&[row])
            .map_err(MsError::from)?
            .pop()
            .flatten()
        {
            Some(casa_types::ArrayValue::Float64(values)) => Some(values.len()),
            _ => None,
        },
    )
}

fn selected_i32_array_len(
    table: &casa_tables::Table,
    column: &str,
    row: usize,
) -> Result<Option<usize>, SelectedObservationContentPlanError> {
    Ok(
        match table
            .column_accessor(column)
            .map_err(MsError::from)?
            .array_cells_owned_uncached(&[row])
            .map_err(MsError::from)?
            .pop()
            .flatten()
        {
            Some(casa_types::ArrayValue::Int32(values)) => Some(values.len()),
            _ => None,
        },
    )
}
