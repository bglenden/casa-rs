// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    CompiledProblem, CompiledProblemId, MeasurementSetIdentity, ObservationProvenanceId,
};

use super::{
    BufferedObservationBlock, SelectedObservationContentBudget, SelectedObservationContentPlan,
    SelectedObservationContentPlanError,
};

/// Payload-free, identity-bound memory requirements for one selected source.
///
/// The storage owner derives this curve under a short-lived read lock. It includes
/// retained metadata, source-specific plans, POINTING construction, and overlapping
/// block preparation. It retains neither table handles nor a prepared catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedObservationContentRequirements {
    pub(super) problem: CompiledProblemId,
    pub(super) provenance: ObservationProvenanceId,
    pub(super) source: MeasurementSetIdentity,
    pub(super) retained_bytes: usize,
    pub(super) initialization_scratch_bytes: usize,
    pub(super) initialization_scan_bytes_per_row: usize,
    pub(super) pointing_reference_scratch_bytes: usize,
    pub(super) traversal_base_bytes: usize,
    pub(super) traversal_pointing_reference_scratch_bytes: usize,
    pub(super) resident_bytes_per_row: usize,
    pub(super) fill_bytes_per_row: usize,
    pub(super) preparation_bytes_per_row: usize,
    pub(super) fill_fixed_bytes: usize,
    pub(super) selected_rows: usize,
    pub(super) maximum_pointing_polynomial_terms: usize,
}

impl SelectedObservationContentRequirements {
    pub(crate) fn matches(
        &self,
        problem: &CompiledProblem,
        source: MeasurementSetIdentity,
    ) -> bool {
        self.problem == problem.problem_id()
            && self.provenance == problem.inputs().observation_snapshot().provenance_id()
            && self.source == source
    }

    /// Return the smallest complete envelope admitting one selected row.
    pub fn minimum_bytes(
        self,
        maximum_live_blocks: usize,
    ) -> Result<usize, SelectedObservationContentPlanError> {
        self.bytes_for_rows(1, maximum_live_blocks)
    }

    /// Return the full initialization/traversal envelope for a bounded block size.
    ///
    /// Rows beyond the selected source's row count do not enlarge the block.
    pub fn bytes_for_rows(
        self,
        rows: usize,
        maximum_live_blocks: usize,
    ) -> Result<usize, SelectedObservationContentPlanError> {
        if rows == 0 || self.selected_rows == 0 || maximum_live_blocks == 0 {
            return Err(SelectedObservationContentPlanError::InvalidBudget);
        }
        let rows = rows.min(self.selected_rows);
        let overflow = SelectedObservationContentPlanError::ByteOverflow;
        let prior_resident = self
            .resident_bytes_per_row
            .checked_mul(maximum_live_blocks - 1)
            .ok_or(overflow)?;
        let replay = super::BoundObservationSource::row_replay_bytes_per_row();
        let fill_per_row = prior_resident
            .checked_add(self.fill_bytes_per_row)
            .and_then(|bytes| bytes.checked_add(replay))
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let prepare_per_row = prior_resident
            .checked_add(self.preparation_bytes_per_row)
            .and_then(|bytes| bytes.checked_add(replay))
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let base = self.traversal_base(maximum_live_blocks)?;
        let fill = rows
            .checked_mul(fill_per_row)
            .and_then(|bytes| bytes.checked_add(self.fill_fixed_bytes))
            .and_then(|bytes| bytes.checked_add(base))
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let prepare = rows
            .checked_mul(prepare_per_row)
            .and_then(|bytes| bytes.checked_add(self.traversal_pointing_reference_scratch_bytes))
            .and_then(|bytes| bytes.checked_add(base))
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        let initialization = rows
            .checked_mul(self.initialization_scan_bytes_per_row)
            .and_then(|bytes| bytes.checked_add(self.initialization_scratch_bytes))
            .and_then(|bytes| bytes.checked_add(self.retained_bytes))
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?;
        Ok(initialization.max(fill).max(prepare))
    }

    fn traversal_base(
        self,
        maximum_live_blocks: usize,
    ) -> Result<usize, SelectedObservationContentPlanError> {
        maximum_live_blocks
            .checked_add(1)
            .and_then(|blocks| blocks.checked_mul(size_of::<BufferedObservationBlock>()))
            .and_then(|bytes| bytes.checked_add(self.traversal_base_bytes))
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)
    }

    /// Return the POINTING polynomial bound used to derive this curve.
    #[must_use]
    pub const fn maximum_pointing_polynomial_terms(self) -> usize {
        self.maximum_pointing_polynomial_terms
    }

    /// Select the largest bounded block that fits an explicit physical budget.
    pub fn plan(
        self,
        budget: SelectedObservationContentBudget,
    ) -> Result<SelectedObservationContentPlan, SelectedObservationContentPlanError> {
        if budget.available_bytes() == 0
            || budget.maximum_live_blocks() == 0
            || budget.maximum_pointing_polynomial_terms() != self.maximum_pointing_polynomial_terms
            || self.maximum_pointing_polynomial_terms == 0
        {
            return Err(SelectedObservationContentPlanError::InvalidBudget);
        }
        let fixed = self
            .retained_bytes
            .checked_add(self.initialization_scratch_bytes)
            .ok_or(SelectedObservationContentPlanError::ByteOverflow)?
            .max(self.traversal_base(budget.maximum_live_blocks())?);
        if fixed > budget.available_bytes() {
            return Err(
                SelectedObservationContentPlanError::InsufficientRetainedBudget {
                    required_bytes: fixed,
                    available_bytes: budget.available_bytes(),
                },
            );
        }
        let minimum = self.minimum_bytes(budget.maximum_live_blocks())?;
        if minimum > budget.available_bytes() {
            return Err(SelectedObservationContentPlanError::InsufficientBudget {
                required_bytes: minimum,
                available_bytes: budget.available_bytes(),
            });
        }
        let mut low = 1;
        let mut high = self.selected_rows;
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if self.bytes_for_rows(middle, budget.maximum_live_blocks())?
                <= budget.available_bytes()
            {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        let rows = low;
        Ok(SelectedObservationContentPlan {
            retained_bytes: self.retained_bytes,
            initialization_scratch_bytes: self
                .initialization_scan_bytes_per_row
                .checked_mul(rows)
                .and_then(|bytes| bytes.checked_add(self.initialization_scratch_bytes))
                .ok_or(SelectedObservationContentPlanError::ByteOverflow)?,
            pointing_reference_scratch_bytes: self.pointing_reference_scratch_bytes,
            resident_bytes_per_row: self.resident_bytes_per_row,
            preparation_bytes_per_row: self.preparation_bytes_per_row,
            rows_per_block: rows,
            resident_bytes_per_block: rows
                .checked_mul(self.resident_bytes_per_row)
                .ok_or(SelectedObservationContentPlanError::ByteOverflow)?,
            preparation_bytes_per_block: rows
                .checked_mul(self.preparation_bytes_per_row)
                .ok_or(SelectedObservationContentPlanError::ByteOverflow)?,
            maximum_resident_bytes: self.bytes_for_rows(rows, budget.maximum_live_blocks())?,
            maximum_live_blocks: budget.maximum_live_blocks(),
            maximum_pointing_polynomial_terms: self.maximum_pointing_polynomial_terms,
        })
    }
}
