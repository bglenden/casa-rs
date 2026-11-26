// SPDX-License-Identifier: LGPL-3.0-or-later
//! Write layouts for the row and baseline axes returned by the legacy selector.

use casa_core::RecordValue;
use casa_tables::Table;

use super::super::{VisibilityChannels, VisibilityPolarization};
use super::{
    BaselineGrid, Column, VisibilityWriteRecord, WriteColumn, WriteGrid, input, write_columns,
};
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

/// The current channel and polarization write selection and last baseline read.
#[derive(Clone, Copy)]
pub struct LegacyVisibilityWriteOptions<'a, 'grid> {
    /// The baseline-axis mapping used only by input fields with both baseline and time axes.
    pub baseline: Option<LegacyVisibilityWriteGrid<'grid>>,
    /// Selected physical channels, or the bins to expand for FLAG.
    pub channels: Option<VisibilityChannels>,
    /// The most recent stored correlation selection or Stokes conversion.
    pub polarization: Option<&'a VisibilityPolarization>,
    /// Whether complex data needs an unsupported correlation permutation.
    pub noncanonical_complex_polarization: bool,
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

impl<'a> VisibilityWriteRecord<'a> {
    /// Resolve legacy row-axis and baseline-axis fields against the rows they represent.
    ///
    /// # Errors
    /// A recognized field cannot be converted, or its layout cannot be mapped safely.
    pub fn new_legacy(
        main: &Table,
        rows: Vec<usize>,
        items: RecordValue,
        options: LegacyVisibilityWriteOptions<'a, '_>,
    ) -> MsResult<Option<Vec<Self>>> {
        let binned = options
            .channels
            .is_some_and(|selection| selection.width() > 1);
        let derived = options
            .polarization
            .is_some_and(VisibilityPolarization::is_conversion);
        let mut fields = Vec::with_capacity(items.fields().len());
        for field in items.into_fields() {
            let Some(column) = Column::from_legacy_name(&field.name) else {
                continue;
            };
            if binned && column.is_visibility_data() {
                continue;
            }
            if (derived && column.is_visibility_data())
                || (options.noncanonical_complex_polarization && column.is_complex_data())
            {
                return Ok(None);
            }
            fields.push(field);
        }
        let mut row_columns = Vec::new();
        let mut baseline_columns = Vec::new();
        for mut field in write_columns(main, RecordValue::new(fields), true)? {
            let rank = field.column.cell_rank();
            field.select_polarization = rank == 2;
            field.channels = options.channels.filter(|_| rank == 2);
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
                polarization: options.polarization,
            });
        }
        if !baseline_columns.is_empty() {
            let saved = options.baseline.ok_or_else(|| {
                input("call getdataold with ifraxis=True before writing a baseline-axis record")
            })?;
            let (rows, grid) = WriteGrid::from_legacy_read(main, saved)?;
            // MSSelector ignores a visibility or FLAG cube with mismatched
            // trailing grid dimensions. Valid fields in its record still write.
            baseline_columns.retain(|field| {
                field.column.cell_rank() != 2 || field.values.shape()[2..] == grid.shape
            });
            validate_shape(&baseline_columns, &grid.shape)?;
            if !rows.is_empty() && !baseline_columns.is_empty() {
                records.push(Self {
                    rows,
                    columns: baseline_columns,
                    grid: Some(grid),
                    polarization: options.polarization,
                });
            }
        }
        Ok(Some(records))
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
