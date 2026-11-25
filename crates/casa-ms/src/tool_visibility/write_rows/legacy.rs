// SPDX-License-Identifier: LGPL-3.0-or-later
//! Write layouts for the row and baseline axes returned by the legacy selector.

use casa_tables::Table;
use casa_types::RecordValue;

use super::{BaselineGrid, VisibilityWriteRecord, WriteColumn, WriteGrid, input, write_columns};
use crate::MsResult;

/// The sampled row order and baseline layout returned by the last legacy read.
#[derive(Clone, Copy)]
pub struct LegacyVisibilityWriteGrid<'a> {
    /// Logical MAIN rows represented in the returned record, in read order.
    pub rows: &'a [usize],
    /// The explicitly requested antenna pairs, including their order and duplicates.
    pub baseline_pairs: Option<&'a [(i32, i32)]>,
    /// Gap slots between ascending first-antenna groups.
    pub gap: i32,
}

impl WriteGrid {
    fn from_legacy_read(
        main: &Table,
        source: LegacyVisibilityWriteGrid<'_>,
    ) -> MsResult<(Vec<usize>, Self)> {
        let columns = main.required_scalar_columns_owned_for_rows(
            &["TIME", "ANTENNA1", "ANTENNA2", "DATA_DESC_ID"],
            source.rows,
        )?;
        let grid = BaselineGrid::legacy(&columns, source.baseline_pairs, source.gap)?;
        let baselines = grid.baseline_count();
        let (rows, positions) = grid
            .occupied_slots()
            .iter()
            .map(|&(slot, source_row)| {
                (
                    source.rows[source_row],
                    [slot % baselines, slot / baselines],
                )
            })
            .unzip();
        Ok((
            rows,
            Self {
                shape: [baselines, grid.time_count()],
                positions,
                collapsed: false,
            },
        ))
    }
}

impl VisibilityWriteRecord<'_> {
    /// Resolve legacy row-axis and baseline-axis fields against the rows they represent.
    ///
    /// # Errors
    /// A recognized field cannot be converted, or its layout cannot be mapped safely.
    pub fn new_legacy(
        main: &Table,
        rows: Vec<usize>,
        items: RecordValue,
        baseline: Option<LegacyVisibilityWriteGrid<'_>>,
    ) -> MsResult<Vec<Self>> {
        let mut row_columns = Vec::new();
        let mut baseline_columns = Vec::new();
        for field in write_columns(main, items)? {
            let rank = field.column.cell_rank();
            match field.values.ndim().checked_sub(rank) {
                Some(1) => row_columns.push(field),
                Some(2) => baseline_columns.push(field),
                _ => {}
            }
        }
        let mut records = Vec::with_capacity(2);
        if !row_columns.is_empty() {
            validate_shape(&row_columns, &[rows.len()])?;
            records.push(Self {
                rows,
                columns: row_columns,
                grid: None,
                polarization: None,
            });
        }
        if !baseline_columns.is_empty() {
            let saved = baseline.ok_or_else(|| {
                input("call getdataold with ifraxis=True before writing a baseline-axis record")
            })?;
            let (rows, grid) = WriteGrid::from_legacy_read(main, saved)?;
            validate_shape(&baseline_columns, &grid.shape)?;
            if !rows.is_empty() {
                records.push(Self {
                    rows,
                    columns: baseline_columns,
                    grid: Some(grid),
                    polarization: None,
                });
            }
        }
        Ok(records)
    }
}

fn validate_shape(columns: &[WriteColumn], expected: &[usize]) -> MsResult<()> {
    for field in columns {
        let shape = field.values.shape();
        if shape.get(field.column.cell_rank()..) != Some(expected) {
            return Err(input(format!(
                "putdataold {} shape {shape:?} does not match the selected axes {expected:?}",
                field.column.name()
            )));
        }
    }
    Ok(())
}
