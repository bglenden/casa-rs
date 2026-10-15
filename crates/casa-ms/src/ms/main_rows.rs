// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded reads of the MAIN columns row selection needs: contiguous blocks
//! in storage order, or a recorded walk of selected rows.

use std::mem::size_of;
use std::sync::Arc;

use casa_tables::{
    RequiredScalarColumnDestination, RequiredScalarColumnValuesMut, SelectedArray1DCellsMut, Table,
};

use super::{MeasurementSet, SelectedObservationRow};
use crate::error::{MsError, MsResult};

/// One bounded block of stored MAIN row-selection facts.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MainRowSelectionBlock {
    first_row: usize,
    pub(super) data_description_ids: Vec<i32>,
    pub(super) field_ids: Vec<i32>,
    antenna1: Vec<i32>,
    antenna2: Vec<i32>,
    times_mjd_seconds: Vec<f64>,
    pub(super) time_centroids_mjd_seconds: Vec<f64>,
    state_ids: Vec<i32>,
    observation_ids: Vec<i32>,
    flag_rows: Vec<bool>,
    pub(super) uvw_m: Vec<f64>,
}

impl MainRowSelectionBlock {
    fn with_capacity(rows: usize) -> MsResult<Self> {
        let uvw_values = rows.checked_mul(3).ok_or_else(|| {
            MsError::InvalidInput("MAIN row-selection UVW capacity overflow".to_string())
        })?;
        Ok(Self {
            first_row: 0,
            data_description_ids: Vec::with_capacity(rows),
            field_ids: Vec::with_capacity(rows),
            antenna1: Vec::with_capacity(rows),
            antenna2: Vec::with_capacity(rows),
            times_mjd_seconds: Vec::with_capacity(rows),
            time_centroids_mjd_seconds: Vec::with_capacity(rows),
            state_ids: Vec::with_capacity(rows),
            observation_ids: Vec::with_capacity(rows),
            flag_rows: Vec::with_capacity(rows),
            uvw_m: Vec::with_capacity(uvw_values),
        })
    }

    /// Number of MAIN rows represented by this block.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.data_description_ids.len()
    }

    /// Return one row by its block-local offset, numbered from the block's
    /// first row as a contiguous block's are.
    #[must_use]
    pub(crate) fn row(&self, offset: usize) -> Option<SelectedObservationRow> {
        (offset < self.len()).then(|| SelectedObservationRow {
            physical_row: self.first_row + offset,
            data_description_id: self.data_description_ids[offset],
            field_id: self.field_ids[offset],
            antenna1: self.antenna1[offset],
            antenna2: self.antenna2[offset],
            time_mjd_seconds: self.times_mjd_seconds[offset],
            time_centroid_mjd_seconds: self.time_centroids_mjd_seconds[offset],
            state_id: self.state_ids[offset],
            observation_id: self.observation_ids[offset],
            flag_row: self.flag_rows[offset],
            uvw_m: [
                self.uvw_m[offset * 3],
                self.uvw_m[offset * 3 + 1],
                self.uvw_m[offset * 3 + 2],
            ],
        })
    }

    /// Iterate the stored facts in physical MAIN row order.
    #[cfg(test)]
    pub(crate) fn rows(&self) -> impl ExactSizeIterator<Item = SelectedObservationRow> + '_ {
        (0..self.len()).map(|offset| self.row(offset).expect("offset is bounded by block length"))
    }
}

/// Pull cursor over bounded reusable MAIN row-selection storage.
pub(crate) struct MainRowSelectionCursor {
    plan: crate::MsReadPlan,
    /// The next MAIN row, or with a walk the next position in it.
    next_row: usize,
    offset: usize,
    row_indices: Vec<usize>,
    block: MainRowSelectionBlock,
    walk: Option<Arc<MainRowWalk>>,
    /// The walk's group holding `next_row`.
    group: usize,
}

/// Selected MAIN rows in the order a traversal reads them: grouped, each
/// group ascending; `ends[g]` closes group `g`. A selection's
/// DATA_DESCRIPTIONs are its groups, so a traversal delivers each spectral
/// window's rows together.
#[derive(Debug)]
pub(crate) struct MainRowWalk {
    rows: Vec<u32>,
    ends: Vec<usize>,
}

impl MainRowWalk {
    /// The walk over `rows`, whose groups end at `ends` (ascending, the
    /// last at `rows.len()`).
    pub(crate) fn new(rows: Vec<u32>, ends: Vec<usize>) -> Self {
        debug_assert!(
            ends.windows(2).all(|pair| pair[0] <= pair[1])
                && ends.last().is_none_or(|&end| end == rows.len()),
            "walk groups end in order at the last row"
        );
        Self { rows, ends }
    }

    /// Bytes a walk of `rows` rows in `groups` groups holds.
    pub(crate) const fn bytes(rows: usize, groups: usize) -> Option<usize> {
        match (
            rows.checked_mul(size_of::<u32>()),
            groups.checked_mul(size_of::<usize>()),
        ) {
            (Some(rows), Some(groups)) => rows.checked_add(groups),
            _ => None,
        }
    }
}

impl MainRowSelectionCursor {
    pub(crate) const fn retained_bytes_per_row() -> usize {
        SelectedObservationRow::STORAGE_BYTES_PER_ROW + size_of::<usize>()
    }

    pub(crate) fn next(
        &mut self,
        measurement_set: &MeasurementSet,
    ) -> MsResult<Option<SelectedObservationRow>> {
        if self.offset >= self.block.len() {
            self.row_indices.clear();
            match &self.walk {
                None => {
                    if self.next_row == self.plan.row_count {
                        return Ok(None);
                    }
                    let end = self
                        .next_row
                        .saturating_add(self.plan.rows_per_block)
                        .min(self.plan.row_count);
                    self.row_indices.extend(self.next_row..end);
                    self.next_row = end;
                }
                Some(walk) => {
                    // A block never crosses a group, so every block reads
                    // ascending rows.
                    while walk.ends.get(self.group) == Some(&self.next_row) {
                        self.group += 1;
                    }
                    let Some(&group_end) = walk.ends.get(self.group) else {
                        return Ok(None);
                    };
                    let end = self
                        .next_row
                        .saturating_add(self.plan.rows_per_block)
                        .min(group_end);
                    self.row_indices.extend(
                        walk.rows[self.next_row..end]
                            .iter()
                            .map(|&row| row as usize),
                    );
                    self.next_row = end;
                }
            }
            fill_main_row_selection_block(
                &measurement_set.main,
                self.row_indices[0],
                &self.row_indices,
                &mut self.block,
            )?;
            self.offset = 0;
        }
        let mut fact = self
            .block
            .row(self.offset)
            .expect("offset is bounded by MAIN selection block length");
        // A walk's block need not be contiguous.
        fact.physical_row = self.row_indices[self.offset];
        self.offset += 1;
        Ok(Some(fact))
    }
}

fn fill_main_row_selection_block(
    table: &Table,
    first_row: usize,
    row_indices: &[usize],
    block: &mut MainRowSelectionBlock,
) -> MsResult<()> {
    block.first_row = first_row;
    {
        let mut scalar_destinations = [
            RequiredScalarColumnDestination::new(
                "DATA_DESC_ID",
                RequiredScalarColumnValuesMut::Int32(&mut block.data_description_ids),
            ),
            RequiredScalarColumnDestination::new(
                "FIELD_ID",
                RequiredScalarColumnValuesMut::Int32(&mut block.field_ids),
            ),
            RequiredScalarColumnDestination::new(
                "ANTENNA1",
                RequiredScalarColumnValuesMut::Int32(&mut block.antenna1),
            ),
            RequiredScalarColumnDestination::new(
                "ANTENNA2",
                RequiredScalarColumnValuesMut::Int32(&mut block.antenna2),
            ),
            RequiredScalarColumnDestination::new(
                "TIME",
                RequiredScalarColumnValuesMut::Float64(&mut block.times_mjd_seconds),
            ),
            RequiredScalarColumnDestination::new(
                "TIME_CENTROID",
                RequiredScalarColumnValuesMut::Float64(&mut block.time_centroids_mjd_seconds),
            ),
            RequiredScalarColumnDestination::new(
                "STATE_ID",
                RequiredScalarColumnValuesMut::Int32(&mut block.state_ids),
            ),
            RequiredScalarColumnDestination::new(
                "OBSERVATION_ID",
                RequiredScalarColumnValuesMut::Int32(&mut block.observation_ids),
            ),
            RequiredScalarColumnDestination::new(
                "FLAG_ROW",
                RequiredScalarColumnValuesMut::Bool(&mut block.flag_rows),
            ),
        ];
        table.required_scalar_columns_for_rows_into(row_indices, &mut scalar_destinations)?;
    }
    let uvw_shape = table
        .column_accessor("UVW")?
        .fill_array_cells_1d_typed_uncached(
            row_indices,
            SelectedArray1DCellsMut::Float64(&mut block.uvw_m),
        )?;
    if uvw_shape.row_count != row_indices.len() || uvw_shape.axis0_count != 3 {
        return Err(MsError::ColumnTypeMismatch {
            column: "UVW".to_string(),
            table: "MAIN".to_string(),
            expected: "f64[3]".to_string(),
            found: format!("f64[{}]", uvw_shape.axis0_count),
        });
    }
    let expected = row_indices.len();
    let lengths = [
        block.data_description_ids.len(),
        block.field_ids.len(),
        block.antenna1.len(),
        block.antenna2.len(),
        block.times_mjd_seconds.len(),
        block.time_centroids_mjd_seconds.len(),
        block.state_ids.len(),
        block.observation_ids.len(),
        block.flag_rows.len(),
        block.uvw_m.len() / 3,
    ];
    if lengths.into_iter().any(|actual| actual != expected) {
        return Err(MsError::InvalidInput(
            "MAIN row-selection column lengths do not match the requested block".to_string(),
        ));
    }
    Ok(())
}

impl MeasurementSet {
    /// Visit the stored MAIN facts needed by row selection in bounded reusable blocks.
    pub(crate) fn visit_main_row_selection_blocks(
        &self,
        plan: crate::MsReadPlan,
        mut visit: impl FnMut(&MainRowSelectionBlock),
    ) -> MsResult<()> {
        let mut cursor = self.main_row_selection_cursor(plan)?;
        while cursor.next_row < cursor.plan.row_count {
            let first_row = cursor.next_row;
            let end = first_row
                .saturating_add(cursor.plan.rows_per_block)
                .min(cursor.plan.row_count);
            cursor.row_indices.clear();
            cursor.row_indices.extend(first_row..end);
            fill_main_row_selection_block(
                &self.main,
                first_row,
                &cursor.row_indices,
                &mut cursor.block,
            )?;
            cursor.next_row = end;
            visit(&cursor.block);
        }
        Ok(())
    }

    pub(crate) fn main_row_selection_cursor(
        &self,
        plan: crate::MsReadPlan,
    ) -> MsResult<MainRowSelectionCursor> {
        self.main_row_cursor(plan, None)
    }

    /// A cursor over `walk`'s rows in its order, in blocks of the plan's
    /// rows that never cross one of its groups.
    pub(crate) fn main_row_walk_cursor(
        &self,
        plan: crate::MsReadPlan,
        walk: Arc<MainRowWalk>,
    ) -> MsResult<MainRowSelectionCursor> {
        if walk
            .rows
            .iter()
            .any(|&row| row as usize >= self.row_count())
        {
            return Err(MsError::InvalidInput(
                "MAIN row walk names a row beyond the table".to_string(),
            ));
        }
        self.main_row_cursor(plan, Some(walk))
    }

    fn main_row_cursor(
        &self,
        plan: crate::MsReadPlan,
        walk: Option<Arc<MainRowWalk>>,
    ) -> MsResult<MainRowSelectionCursor> {
        if plan.row_count != self.row_count() {
            return Err(MsError::InvalidInput(format!(
                "MAIN row plan covers {} rows but the table has {}",
                plan.row_count,
                self.row_count()
            )));
        }
        if plan.row_count == 0 {
            return Ok(MainRowSelectionCursor {
                plan,
                next_row: 0,
                offset: 0,
                row_indices: Vec::new(),
                block: MainRowSelectionBlock::with_capacity(0)?,
                walk: None,
                group: 0,
            });
        }
        if plan.rows_per_block == 0 {
            return Err(MsError::InvalidInput(
                "nonempty MAIN row plan has zero rows per block".to_string(),
            ));
        }
        let required_bytes = plan
            .rows_per_block
            .checked_mul(SelectedObservationRow::STORAGE_BYTES_PER_ROW)
            .ok_or_else(|| {
                MsError::InvalidInput("MAIN row block byte count overflow".to_string())
            })?;
        if plan.bytes_per_block < required_bytes {
            return Err(MsError::InvalidInput(format!(
                "MAIN row plan reserves {} bytes per block but row selection requires at least {required_bytes}",
                plan.bytes_per_block
            )));
        }
        Ok(MainRowSelectionCursor {
            plan,
            next_row: 0,
            offset: 0,
            row_indices: Vec::with_capacity(plan.rows_per_block),
            block: MainRowSelectionBlock::with_capacity(plan.rows_per_block)?,
            walk,
            group: 0,
        })
    }
}
