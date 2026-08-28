// SPDX-License-Identifier: LGPL-3.0-or-later

use crate::{
    MeasurementSet, MsError,
    derived::engine::{MsCalEngine, selected_direction_reference_column},
};
use crate::{
    selected_observation_buffer::selected_observation_buffer_residency,
    selected_pointing::selected_pointing_preparation_peak_bytes, subtables::SubTable,
};
use casa_imaging_model::{
    CompiledProblem, CorrelationProduct, ObservationSource, PointingCentreLaw,
    SelectedPointingDirections, VisibilityColumn, WeightColumn,
};
use thiserror::Error;

use super::access::{
    BoundObservationSource, BufferedObservationBlock, EvaluatedRowGeometry, SelectedChannel,
    SelectedCoordinates,
};
use super::row_selection::CompiledRowPredicate;

/// Once-only allocations shared by one bound selected-observation owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SelectedObservationSharedBytes {
    shared_measures_retained_bytes: usize,
    shared_source_slots_retained_bytes: usize,
    shared_binding_graph_initialization_bytes: usize,
}

impl SelectedObservationSharedBytes {
    pub(crate) const NONE: Self = Self::new(0, 0, 0);

    pub(crate) const fn new(
        shared_measures_retained_bytes: usize,
        shared_source_slots_retained_bytes: usize,
        shared_binding_graph_initialization_bytes: usize,
    ) -> Self {
        Self {
            shared_measures_retained_bytes,
            shared_source_slots_retained_bytes,
            shared_binding_graph_initialization_bytes,
        }
    }
}

/// Explicit memory available to simultaneously live selected-content blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedObservationContentBudget {
    available_bytes: usize,
    maximum_live_blocks: usize,
    maximum_pointing_polynomial_terms: usize,
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

    /// Return bytes available across every simultaneously live content block.
    #[must_use]
    pub const fn available_bytes(self) -> usize {
        self.available_bytes
    }

    /// Return the exact maximum simultaneously live content blocks.
    #[must_use]
    pub const fn maximum_live_blocks(self) -> usize {
        self.maximum_live_blocks
    }

    /// Return the maximum accepted POINTING polynomial coefficient count per axis.
    #[must_use]
    pub const fn maximum_pointing_polynomial_terms(self) -> usize {
        self.maximum_pointing_polynomial_terms
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
    pointing_reference_scratch_bytes: usize,
    resident_bytes_per_row: usize,
    preparation_bytes_per_row: usize,
    rows_per_block: usize,
    resident_bytes_per_block: usize,
    preparation_bytes_per_block: usize,
    maximum_resident_bytes: usize,
    maximum_live_blocks: usize,
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

    /// Maximum one-at-a-time variable POINTING reference read scratch.
    #[must_use]
    #[cfg(test)]
    pub const fn pointing_reference_scratch_bytes(self) -> usize {
        self.pointing_reference_scratch_bytes
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
    #[cfg(test)]
    pub const fn maximum_resident_bytes(self) -> usize {
        self.maximum_resident_bytes
    }

    /// Exact maximum simultaneously live content blocks.
    #[must_use]
    pub const fn maximum_live_blocks(self) -> usize {
        self.maximum_live_blocks
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

pub(crate) fn selected_content_plan(
    measurement_set: &MeasurementSet,
    problem: &CompiledProblem,
    source: &ObservationSource,
    shared_bytes: SelectedObservationSharedBytes,
    budget: SelectedObservationContentBudget,
) -> Result<SelectedObservationContentPlan, SelectedObservationContentPlanError> {
    if budget.available_bytes == 0
        || budget.maximum_live_blocks == 0
        || budget.maximum_pointing_polynomial_terms == 0
    {
        return Err(SelectedObservationContentPlanError::InvalidBudget);
    }
    let (retained_bytes, coordinate_construction_scratch_bytes, pointing_reference_scratch_bytes) =
        retained_metadata_bytes(
            measurement_set,
            problem,
            source,
            shared_bytes.shared_measures_retained_bytes,
            shared_bytes.shared_source_slots_retained_bytes,
        )?;
    let initialization_scratch_bytes = coordinate_construction_scratch_bytes
        .checked_add(shared_bytes.shared_binding_graph_initialization_bytes)
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let initialization_peak_bytes = retained_bytes
        .checked_add(initialization_scratch_bytes)
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    if initialization_peak_bytes > budget.available_bytes {
        return Err(
            SelectedObservationContentPlanError::InsufficientRetainedBudget {
                required_bytes: initialization_peak_bytes,
                available_bytes: budget.available_bytes,
            },
        );
    }
    let inspection_bytes = problem
        .selected_observation()
        .inspection_scratch_bytes()
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let row_replay_fixed_bytes = BoundObservationSource::row_replay_fixed_bytes(
        source.selection().data_descriptions().len(),
    )
    .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    // VecDeque retains storage for every ready block while Option retains the
    // active block inline. Heap payloads are charged separately below.
    let block_container_bytes = budget
        .maximum_live_blocks
        .checked_add(1)
        .and_then(|blocks| blocks.checked_mul(size_of::<BufferedObservationBlock>()))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let traversal_base_bytes = retained_bytes
        .checked_add(inspection_bytes)
        .and_then(|bytes| bytes.checked_add(block_container_bytes))
        .and_then(|bytes| bytes.checked_add(row_replay_fixed_bytes))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    if traversal_base_bytes >= budget.available_bytes {
        return Err(
            SelectedObservationContentPlanError::InsufficientRetainedBudget {
                required_bytes: traversal_base_bytes,
                available_bytes: budget.available_bytes,
            },
        );
    }
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
        PointingCentreLaw::PhaseTrackingCentre | PointingCentreLaw::Fixed(_) => None,
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
        let visibility_bytes = match source.generations().columns().visibility() {
            VisibilityColumn::Data | VisibilityColumn::CorrectedData => 8,
            VisibilityColumn::FloatData => 4,
        };
        let weight_values = match source.generations().columns().weights() {
            WeightColumn::Weight => correlations,
            WeightColumn::WeightSpectrum => sample_count,
        };
        let buffer =
            selected_observation_buffer_residency(1, sample_count, weight_values, visibility_bytes)
                .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let resident = buffer
            .resident_bytes
            .checked_add(size_of::<EvaluatedRowGeometry>())
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        // A recycled block keeps its row-geometry allocation until the new
        // storage buffer and POINTING output are ready. Charge that allocation
        // during both preparation phases, not only after the block is complete.
        let retained_geometry = size_of::<EvaluatedRowGeometry>();
        let fill = buffer
            .fill_peak_bytes
            .checked_sub(fill_fixed_bytes)
            .and_then(|bytes| bytes.checked_add(retained_geometry))
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let geometry_build = buffer
            .resident_bytes
            .checked_add(size_of::<EvaluatedRowGeometry>())
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
            let pointing_scratch = selected_pointing_preparation_peak_bytes(
                1,
                1,
                budget.maximum_pointing_polynomial_terms,
                direction_column,
            )
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
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
    let prior_live_blocks = budget.maximum_live_blocks - 1;
    let prior_resident_bytes_per_row = resident_bytes_per_row
        .checked_mul(prior_live_blocks)
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let traversal_bytes = budget.available_bytes - traversal_base_bytes;
    let fill_denominator = prior_resident_bytes_per_row
        .checked_add(fill_bytes_per_row)
        .and_then(|bytes| bytes.checked_add(BoundObservationSource::row_replay_bytes_per_row()))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let preparation_denominator = prior_resident_bytes_per_row
        .checked_add(preparation_bytes_per_row)
        .and_then(|bytes| bytes.checked_add(BoundObservationSource::row_replay_bytes_per_row()))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let rows_by_fill = traversal_bytes
        .checked_sub(fill_fixed_bytes)
        .map_or(0, |bytes| bytes / fill_denominator);
    let rows_by_preparation = traversal_bytes
        .checked_sub(pointing_reference_scratch_bytes)
        .map_or(0, |bytes| bytes / preparation_denominator);
    let rows_per_block = rows_by_fill.min(rows_by_preparation).min(selected_rows);
    if rows_per_block == 0 {
        return Err(SelectedObservationContentPlanError::InsufficientBudget {
            required_bytes: fill_fixed_bytes
                .checked_add(fill_denominator)
                .and_then(|fill| {
                    pointing_reference_scratch_bytes
                        .checked_add(preparation_denominator)
                        .map(|preparation| fill.max(preparation))
                })
                .ok_or(SelectedObservationContentPlanError::ByteOverflow)?,
            available_bytes: traversal_bytes,
        });
    }
    let resident_bytes_per_block = rows_per_block
        .checked_mul(resident_bytes_per_row)
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let preparation_bytes_per_block = rows_per_block
        .checked_mul(preparation_bytes_per_row)
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let prior_blocks = resident_bytes_per_block
        .checked_mul(prior_live_blocks)
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let fill_payload = rows_per_block
        .checked_mul(fill_bytes_per_row)
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let fill_peak = traversal_base_bytes
        .checked_add(prior_blocks)
        .and_then(|bytes| bytes.checked_add(fill_fixed_bytes))
        .and_then(|bytes| bytes.checked_add(fill_payload))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let preparation_peak = traversal_base_bytes
        .checked_add(prior_blocks)
        .and_then(|bytes| bytes.checked_add(preparation_bytes_per_block))
        .and_then(|bytes| bytes.checked_add(pointing_reference_scratch_bytes))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let traversal_peak_bytes = fill_peak.max(preparation_peak);
    Ok(SelectedObservationContentPlan {
        retained_bytes,
        initialization_scratch_bytes,
        pointing_reference_scratch_bytes,
        resident_bytes_per_row,
        preparation_bytes_per_row,
        rows_per_block,
        resident_bytes_per_block,
        preparation_bytes_per_block,
        maximum_resident_bytes: initialization_peak_bytes.max(traversal_peak_bytes),
        maximum_live_blocks: budget.maximum_live_blocks,
        maximum_pointing_polynomial_terms: budget.maximum_pointing_polynomial_terms,
    })
}

fn retained_metadata_bytes(
    measurement_set: &MeasurementSet,
    problem: &CompiledProblem,
    source: &ObservationSource,
    shared_measures_retained_bytes: usize,
    shared_source_slots_retained_bytes: usize,
) -> Result<(usize, usize, usize), SelectedObservationContentPlanError> {
    let storage_bytes = measurement_set
        .retained_read_metadata_heap_bytes()
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let geometry_bytes = MsCalEngine::selected_observation_retained_heap_bytes(measurement_set)?;
    let manifest_bytes = source
        .selection()
        .retained_manifest_bytes()
        .and_then(|bytes| {
            source
                .generations()
                .retained_manifest_bytes()
                .and_then(|generations| bytes.checked_add(generations))
        })
        .and_then(|bytes| bytes.checked_add(source.provenance().retained_locator_bytes()))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let predicate_bytes = CompiledRowPredicate::shared_retained_heap_bytes(source)
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let spectral_windows = measurement_set.spectral_window()?;
    let polarizations = measurement_set.polarization()?;
    let mut coordinate_bytes = source
        .selection()
        .data_descriptions()
        .len()
        .checked_mul(size_of::<SelectedCoordinates>())
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let pointing_reference_scratch_bytes =
        selected_pointing_reference_scratch_bytes(measurement_set, problem)?;
    let mut maximum_scratch_bytes = selected_geometry_construction_scratch_bytes(measurement_set)?
        .max(pointing_reference_scratch_bytes);
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
        let spectral_scratch_bytes = frequency_count
            .checked_add(width_count)
            .and_then(|values| values.checked_mul(size_of::<f64>()))
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let polarization_row = usize::try_from(description.polarization_id())
            .map_err(|_| SelectedObservationContentPlanError::InvalidCoordinateShape)?;
        let full_correlation_count =
            selected_i32_array_len(polarizations.table(), "CORR_TYPE", polarization_row)?
                .filter(|count| *count > 0)
                .ok_or(SelectedObservationContentPlanError::InvalidCoordinateShape)?;
        let correlation_scratch_bytes = full_correlation_count
            .checked_mul(size_of::<i32>())
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        // Frequency and width arrays are released before CORR_TYPE is loaded.
        // Charge the larger payload plus the transient dynamic-array metadata.
        let coordinate_scratch_bytes = spectral_scratch_bytes
            .max(correlation_scratch_bytes)
            // Three dynamic 1-D ndarrays retain one shape and one stride word
            // each; CHAN_FREQ and CHAN_WIDTH are the simultaneous pair.
            .checked_add(4 * size_of::<usize>())
            // One selected-cell wrapper and its accessor name are transient
            // while the already extracted array payloads remain live.
            .and_then(|bytes| bytes.checked_add(size_of::<Option<casa_types::ArrayValue>>()))
            .and_then(|bytes| {
                ["CHAN_FREQ", "CHAN_WIDTH", "CORR_TYPE"]
                    .iter()
                    .map(|name| name.len())
                    .max()
                    .and_then(|name| bytes.checked_add(name))
            })
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        maximum_scratch_bytes = maximum_scratch_bytes.max(coordinate_scratch_bytes);
    }
    // validate_data_descriptions retains one temporary DDID/wavelength vector
    // while CompiledRowPredicate shares the compiler-owned predicate catalog.
    let predicate_construction_scratch = source
        .selection()
        .data_descriptions()
        .len()
        .checked_mul(size_of::<(u32, f64)>())
        .and_then(|bytes| bytes.checked_add(size_of::<Option<casa_types::ScalarValue>>()))
        .and_then(|bytes| {
            ["SPECTRAL_WINDOW_ID", "POLARIZATION_ID", "REF_FREQUENCY"]
                .iter()
                .map(|name| name.len())
                .max()
                .and_then(|name| bytes.checked_add(name))
        })
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    maximum_scratch_bytes = maximum_scratch_bytes.max(predicate_construction_scratch);
    let retained_bytes = shared_source_slots_retained_bytes
        .checked_add(shared_measures_retained_bytes)
        .and_then(|bytes| bytes.checked_add(storage_bytes))
        .and_then(|bytes| bytes.checked_add(geometry_bytes))
        .and_then(|bytes| bytes.checked_add(manifest_bytes))
        .and_then(|bytes| bytes.checked_add(predicate_bytes))
        .and_then(|bytes| bytes.checked_add(coordinate_bytes))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    Ok((
        retained_bytes,
        maximum_scratch_bytes,
        pointing_reference_scratch_bytes,
    ))
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

fn selected_geometry_construction_scratch_bytes(
    measurement_set: &MeasurementSet,
) -> Result<usize, SelectedObservationContentPlanError> {
    let antenna = measurement_set.antenna()?;
    let mut maximum = 0_usize;
    for row in 0..antenna.row_count() {
        maximum = maximum
            .max(selected_f64_array_scratch_bytes(
                antenna.table(),
                "POSITION",
                row,
            )?)
            .max(selected_string_scratch_bytes(
                antenna.table(),
                "MOUNT",
                row,
            )?);
    }
    let field = measurement_set.field()?;
    let direction_reference_column =
        selected_direction_reference_column(field.table(), "PHASE_DIR");
    for row in 0..field.row_count() {
        maximum = maximum.max(selected_f64_array_scratch_bytes(
            field.table(),
            "PHASE_DIR",
            row,
        )?);
        if let Some(reference_column) = direction_reference_column {
            maximum = maximum.max(selected_scalar_scratch_bytes(
                field.table(),
                reference_column,
                row,
            )?);
        }
    }
    if let Ok(observation) = measurement_set.observation() {
        for row in 0..observation.row_count() {
            if let Ok(bytes) =
                selected_string_scratch_bytes(observation.table(), "TELESCOPE_NAME", row)
            {
                maximum = maximum.max(bytes);
            }
        }
    }
    Ok(maximum)
}

fn selected_pointing_reference_scratch_bytes(
    measurement_set: &MeasurementSet,
    problem: &CompiledProblem,
) -> Result<usize, SelectedObservationContentPlanError> {
    let PointingCentreLaw::Observation(law) = problem.geometry().centres().pointing() else {
        return Ok(0);
    };
    let Ok(pointing) = measurement_set.pointing() else {
        return Ok(0);
    };
    let column = match law.direction_column() {
        casa_imaging_model::PointingDirectionColumn::Direction => "DIRECTION",
        casa_imaging_model::PointingDirectionColumn::Target => "TARGET",
    };
    let Some(reference_column) = selected_direction_reference_column(pointing.table(), column)
    else {
        return Ok(0);
    };
    (0..pointing.row_count()).try_fold(0_usize, |maximum, row| {
        Ok(maximum.max(selected_scalar_scratch_bytes(
            pointing.table(),
            reference_column,
            row,
        )?))
    })
}

fn selected_f64_array_scratch_bytes(
    table: &casa_tables::Table,
    column: &str,
    row: usize,
) -> Result<usize, SelectedObservationContentPlanError> {
    let value = table
        .column_accessor(column)
        .map_err(MsError::from)?
        .array_cells_owned_uncached(&[row])
        .map_err(MsError::from)?
        .pop()
        .flatten()
        .ok_or(SelectedObservationContentPlanError::InvalidCoordinateShape)?;
    let casa_types::ArrayValue::Float64(values) = value else {
        return Err(SelectedObservationContentPlanError::InvalidCoordinateShape);
    };
    values
        .len()
        .checked_mul(size_of::<f64>())
        .and_then(|bytes| {
            values
                .ndim()
                .checked_mul(2 * size_of::<usize>())
                .and_then(|dimensions| bytes.checked_add(dimensions))
        })
        .and_then(|bytes| bytes.checked_add(size_of::<Option<casa_types::ArrayValue>>()))
        .and_then(|bytes| bytes.checked_add(column.len()))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)
}

fn selected_string_scratch_bytes(
    table: &casa_tables::Table,
    column: &str,
    row: usize,
) -> Result<usize, SelectedObservationContentPlanError> {
    let value = selected_scalar_scratch_value(table, column, row)?;
    if !matches!(value, casa_types::ScalarValue::String(_)) {
        return Err(SelectedObservationContentPlanError::InvalidCoordinateShape);
    }
    scalar_scratch_bytes(&value, column)
}

fn selected_scalar_scratch_bytes(
    table: &casa_tables::Table,
    column: &str,
    row: usize,
) -> Result<usize, SelectedObservationContentPlanError> {
    let value = selected_scalar_scratch_value(table, column, row)?;
    scalar_scratch_bytes(&value, column)
}

fn selected_scalar_scratch_value(
    table: &casa_tables::Table,
    column: &str,
    row: usize,
) -> Result<casa_types::ScalarValue, SelectedObservationContentPlanError> {
    let value = table
        .column_accessor(column)
        .map_err(MsError::from)?
        .scalar_cells_owned_for_rows(&[row])
        .map_err(MsError::from)?
        .pop()
        .flatten()
        .ok_or(SelectedObservationContentPlanError::InvalidCoordinateShape)?;
    Ok(value)
}

fn scalar_scratch_bytes(
    value: &casa_types::ScalarValue,
    column: &str,
) -> Result<usize, SelectedObservationContentPlanError> {
    let dynamic_bytes = match value {
        casa_types::ScalarValue::String(value) => value.capacity(),
        _ => 0,
    };
    dynamic_bytes
        .checked_add(size_of::<Option<casa_types::ScalarValue>>())
        .and_then(|bytes| bytes.checked_add(column.len()))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)
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
