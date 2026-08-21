// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{ObservationSource, VisibilityColumn, WeightColumn};
use casa_ms::{MeasurementSet, MsError};
use thiserror::Error;

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
    bytes_per_row: usize,
    rows_per_block: usize,
    bytes_per_block: usize,
    maximum_live_blocks: usize,
    maximum_pointing_polynomial_terms: usize,
}

impl SelectedObservationContentPlan {
    /// Maximum logical bytes retained by one selected MAIN row.
    #[must_use]
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
    pub const fn bytes_per_block(self) -> usize {
        self.bytes_per_block
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
    let per_block_budget = budget.available_bytes / budget.maximum_live_blocks;
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
        // row index + ten Int32 provenance values + UVW + four Float64 time values + FLAG_ROW
        let fixed_row_bytes = size_of::<usize>()
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
    Ok(SelectedObservationContentPlan {
        bytes_per_row,
        rows_per_block,
        bytes_per_block,
        maximum_live_blocks: budget.maximum_live_blocks,
        maximum_pointing_polynomial_terms: budget.maximum_pointing_polynomial_terms,
    })
}
