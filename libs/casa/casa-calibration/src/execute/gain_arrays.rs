// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded array reads for ordinary antenna-based gain tables.

use super::{ApplyExecutionError, ArrayValue, COL_CPARAM, COL_FLAG, MsError, Path, Table};

pub(super) struct GainArrayBatch {
    flags: Vec<Option<ArrayValue>>,
    gains: Vec<Option<ArrayValue>>,
}

impl GainArrayBatch {
    pub(super) const ROWS: usize = 512;

    pub(super) fn read(table: &Table, first: usize) -> Option<Self> {
        let end = first.saturating_add(Self::ROWS).min(table.row_count());
        let rows: Vec<_> = (first..end).collect();
        let read = |column| {
            table
                .column_accessor(column)
                .and_then(|accessor| accessor.array_cells_owned_uncached(&rows))
                .ok()
        };
        Some(Self {
            flags: read(COL_FLAG)?,
            gains: read(COL_CPARAM)?,
        })
    }

    pub(super) fn cells(&self, row: usize) -> (Option<&ArrayValue>, Option<&ArrayValue>) {
        let offset = row % Self::ROWS;
        (
            self.flags.get(offset).and_then(Option::as_ref),
            self.gains.get(offset).and_then(Option::as_ref),
        )
    }
}

pub(super) fn load_array<'a>(
    table: &'a Table,
    path: &Path,
    row: usize,
    column: &str,
    prefetched: Option<&'a ArrayValue>,
) -> Result<&'a ArrayValue, ApplyExecutionError> {
    if let Some(value) = prefetched {
        return Ok(value);
    }
    // Let the normal cell read report missing data and read failures in row order.
    table
        .cell_accessor(row, column)
        .and_then(|cell| cell.array())
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: path.display().to_string(),
            source: MsError::from(source),
        })
}
