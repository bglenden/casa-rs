// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded projection of visibility array cells for a transform input block.

use std::path::Path;

use casa_core::ArrayValue;
use casa_tables::{Table, TableError};

use super::visibility_columns::VisibilityColumns;
use super::{MsTransformError, PreparedTransformInputView, TransformInputColumns};

pub(super) type ArrayCells = Vec<Option<ArrayValue>>;

pub(super) struct MainArrayBlock {
    pub(super) visibility: Vec<ArrayCells>,
    pub(super) flags: ArrayCells,
    pub(super) categories: ArrayCells,
    pub(super) uvw: ArrayCells,
    pub(super) weights: ArrayCells,
    pub(super) sigmas: ArrayCells,
    pub(super) spectra: Vec<ArrayCells>,
}

impl MainArrayBlock {
    /// CASA chooses whether to read spectra from the first row in a
    /// visibility buffer. Retain the decision across transform batch edges.
    pub(super) fn resolve_buffer_spectra(
        &mut self,
        names: &[&'static str],
        availability: &mut [bool],
        starts_buffer: &[bool],
        physical_rows: &[usize],
        path: &Path,
    ) -> Result<(), MsTransformError> {
        for ((name, cells), available) in names.iter().zip(&mut self.spectra).zip(availability) {
            for ((cell, &first), &row) in cells.iter_mut().zip(starts_buffer).zip(physical_rows) {
                if first {
                    *available = cell.is_some();
                }
                if !*available {
                    *cell = None;
                } else if cell.is_none() {
                    return Err(MsTransformError::MissingSpectrumCell {
                        path: path.display().to_string(),
                        row,
                        column: name,
                    });
                }
            }
        }
        Ok(())
    }

    pub(super) fn for_capacity(rows: usize, visibility: usize, spectra: usize) -> Self {
        Self {
            visibility: (0..visibility).map(|_| Vec::with_capacity(rows)).collect(),
            flags: Vec::with_capacity(rows),
            categories: Vec::with_capacity(rows),
            uvw: Vec::with_capacity(rows),
            weights: Vec::with_capacity(rows),
            sigmas: Vec::with_capacity(rows),
            spectra: (0..spectra).map(|_| Vec::with_capacity(rows)).collect(),
        }
    }

    pub(super) fn read(
        table: &Table,
        rows: &[usize],
        visibility: &VisibilityColumns,
        spectra: &[&str],
        view: Option<&dyn PreparedTransformInputView>,
    ) -> Result<Self, TableError> {
        let mut computed = if let Some(view) = view {
            let mut columns = Vec::from(["FLAG", "FLAG_CATEGORY", "UVW", "WEIGHT", "SIGMA"]);
            columns.extend(spectra.iter().copied());
            columns.extend(visibility.copies().iter().map(|copy| copy.source.name()));
            view.read(table, rows, &columns)?
        } else {
            TransformInputColumns::new()
        };
        let mut read = |name| {
            if let Some(cells) = computed.remove(name) {
                if cells.len() != rows.len() {
                    return Err(TableError::Schema(format!(
                        "computed {name} supplied {} cells for {} rows",
                        cells.len(),
                        rows.len()
                    )));
                }
                return Ok(cells);
            }
            table
                .column_accessor(name)?
                .array_cells_owned_uncached(rows)
        };
        let visibility = visibility
            .copies()
            .iter()
            .map(|copy| read(copy.source.name()))
            .collect::<Result<Vec<_>, _>>()?;
        let spectra = spectra
            .iter()
            .map(|name| read(name))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            visibility,
            flags: read("FLAG")?,
            categories: read("FLAG_CATEGORY")?,
            uvw: read("UVW")?,
            weights: read("WEIGHT")?,
            sigmas: read("SIGMA")?,
            spectra,
        })
    }
}
