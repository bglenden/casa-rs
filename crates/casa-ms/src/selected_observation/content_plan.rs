// SPDX-License-Identifier: LGPL-3.0-or-later

use crate::{MeasurementSet, MsError, derived::engine::MsCalEngine};
use casa_imaging_model::{CorrelationProduct, ObservationSource, VisibilityColumn, WeightColumn};
use thiserror::Error;

use super::access::{SelectedChannel, SelectedCoordinates};
use super::row_selection::CompiledRowPredicate;

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
/// The projection charges every retained vector element in the casa-ms selected-content buffer,
/// the bounded POINTING scalar scan and candidate set, and evaluated row geometry. Physical row
/// indices, selected visibility precision, flags, exact input weights, raw UVW, time coordinates,
/// and MAIN provenance are all included. Boolean vectors are conservatively charged as one byte
/// per value. Allocation descriptors and allocator bookkeeping belong to the execution plan's
/// allocation records rather than this science-payload projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedObservationContentPlan {
    retained_bytes: usize,
    initialization_scratch_bytes: usize,
    bytes_per_row: usize,
    rows_per_block: usize,
    bytes_per_block: usize,
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

    /// Maximum logical bytes retained by one selected MAIN row.
    #[must_use]
    #[cfg(test)]
    pub const fn bytes_per_row(self) -> usize {
        self.bytes_per_row
    }

    /// Maximum selected MAIN rows retained by one content block.
    #[must_use]
    pub const fn rows_per_block(self) -> usize {
        self.rows_per_block
    }

    /// Maximum logical payload bytes retained by one full content block.
    #[must_use]
    #[cfg(test)]
    pub const fn bytes_per_block(self) -> usize {
        self.bytes_per_block
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
    source: &ObservationSource,
    budget: SelectedObservationContentBudget,
) -> Result<SelectedObservationContentPlan, SelectedObservationContentPlanError> {
    if budget.available_bytes == 0
        || budget.maximum_live_blocks == 0
        || budget.maximum_pointing_polynomial_terms == 0
    {
        return Err(SelectedObservationContentPlanError::InvalidBudget);
    }
    let (retained_bytes, initialization_scratch_bytes) =
        retained_metadata_bytes(measurement_set, source)?;
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
    let traversal_bytes = budget.available_bytes - retained_bytes;
    let per_block_budget = traversal_bytes / budget.maximum_live_blocks;
    if per_block_budget == 0 {
        return Err(SelectedObservationContentPlanError::InvalidBudget);
    }
    let polarization = measurement_set.polarization()?;
    let mut bytes_per_row = 0_usize;
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
        let correlations = usize::try_from(polarization.num_corr(polarization_row)?)
            .ok()
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
        // The fill request and retained content block simultaneously own the selected row index.
        // Add ten Int32 provenance values, UVW, four Float64 time values, and FLAG_ROW.
        let fixed_row_bytes = (2 * size_of::<usize>())
            .checked_add(10 * size_of::<i32>())
            .and_then(|bytes| bytes.checked_add(3 * size_of::<f64>()))
            .and_then(|bytes| bytes.checked_add(4 * size_of::<f64>()))
            .and_then(|bytes| bytes.checked_add(1))
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let selected_content_bytes = sample_count
            .checked_mul(visibility_bytes)
            .and_then(|bytes| bytes.checked_add(sample_count))
            .and_then(|bytes| {
                weight_values
                    .checked_mul(size_of::<f32>())
                    .and_then(|weights| bytes.checked_add(weights))
            })
            .and_then(|bytes| bytes.checked_add(fixed_row_bytes))
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        // Two antenna/time queries retain at most a covering, before, and after POINTING row.
        // Each candidate carries two Float64 polynomial axes plus fixed row metadata. The scalar
        // scan block and evaluated row geometry are charged in the same row-shaped working set,
        // so simultaneous residency is independent of total MAIN and POINTING table rows.
        let pointing_candidate_bytes = 2_usize
            .checked_mul(3)
            .and_then(|candidates| {
                candidates.checked_mul(
                    2_usize
                        .checked_mul(budget.maximum_pointing_polynomial_terms)?
                        .checked_mul(size_of::<f64>())?
                        .checked_add(
                            size_of::<usize>() + 3 * size_of::<f64>() + size_of::<i32>(),
                        )?,
                )
            })
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let pointing_query_bytes = 2_usize
            .checked_mul(size_of::<i32>() + size_of::<f64>())
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let pointing_scan_bytes = size_of::<usize>() + 2 * size_of::<i32>() + 3 * size_of::<f64>();
        let evaluated_geometry_bytes =
            7 * size_of::<f64>() + 4 * (2 * size_of::<f64>() + size_of::<u8>());
        let projected = selected_content_bytes
            .checked_add(pointing_candidate_bytes)
            .and_then(|bytes| bytes.checked_add(pointing_query_bytes))
            .and_then(|bytes| bytes.checked_add(pointing_scan_bytes))
            .and_then(|bytes| bytes.checked_add(evaluated_geometry_bytes))
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        bytes_per_row = bytes_per_row.max(projected);
    }
    if bytes_per_row == 0 {
        return Err(SelectedObservationContentPlanError::InvalidCoordinateShape);
    }
    let selected_rows = usize::try_from(source.selection().rows().selected_row_count())
        .map_err(|_| SelectedObservationContentPlanError::ByteOverflow)?;
    let rows_per_block = (per_block_budget / bytes_per_row).min(selected_rows);
    if rows_per_block == 0 {
        return Err(SelectedObservationContentPlanError::InsufficientBudget {
            required_bytes: bytes_per_row,
            available_bytes: per_block_budget,
        });
    }
    let bytes_per_block = rows_per_block
        .checked_mul(bytes_per_row)
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let traversal_peak_bytes = bytes_per_block
        .checked_mul(budget.maximum_live_blocks)
        .and_then(|bytes| bytes.checked_add(retained_bytes))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    Ok(SelectedObservationContentPlan {
        retained_bytes,
        initialization_scratch_bytes,
        bytes_per_row,
        rows_per_block,
        bytes_per_block,
        maximum_resident_bytes: initialization_peak_bytes.max(traversal_peak_bytes),
        maximum_live_blocks: budget.maximum_live_blocks,
        maximum_pointing_polynomial_terms: budget.maximum_pointing_polynomial_terms,
    })
}

fn retained_metadata_bytes(
    measurement_set: &MeasurementSet,
    source: &ObservationSource,
) -> Result<(usize, usize), SelectedObservationContentPlanError> {
    let geometry_bytes = MsCalEngine::selected_observation_retained_bytes(measurement_set)?;
    let predicate_bytes = CompiledRowPredicate::retained_bytes(
        source.selection().rows_filter(),
        source.selection().data_descriptions(),
    )
    .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let spectral_windows = measurement_set.spectral_window()?;
    let polarizations = measurement_set.polarization()?;
    let mut coordinate_bytes = source
        .selection()
        .data_descriptions()
        .len()
        .checked_mul(size_of::<SelectedCoordinates>())
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    let mut maximum_scratch_bytes = 0_usize;
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
        let full_channel_count = usize::try_from(spectral_windows.num_chan(spectral_window_row)?)
            .ok()
            .filter(|count| *count > 0)
            .ok_or(SelectedObservationContentPlanError::InvalidCoordinateShape)?;
        let spectral_scratch_bytes = full_channel_count
            .checked_mul(2 * size_of::<f64>())
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let polarization_row = usize::try_from(description.polarization_id())
            .map_err(|_| SelectedObservationContentPlanError::InvalidCoordinateShape)?;
        let full_correlation_count = usize::try_from(polarizations.num_corr(polarization_row)?)
            .ok()
            .filter(|count| *count > 0)
            .ok_or(SelectedObservationContentPlanError::InvalidCoordinateShape)?;
        let correlation_scratch_bytes = full_correlation_count
            .checked_mul(size_of::<i32>())
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        maximum_scratch_bytes = maximum_scratch_bytes
            .max(spectral_scratch_bytes)
            .max(correlation_scratch_bytes);
    }
    let retained_bytes = geometry_bytes
        .checked_add(predicate_bytes)
        .and_then(|bytes| bytes.checked_add(coordinate_bytes))
        .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
    Ok((retained_bytes, maximum_scratch_bytes))
}
