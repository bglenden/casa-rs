// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bounded projection of visibility array cells for a transform input block.

use casa_tables::{Table, TableError};
use casa_types::ArrayValue;

use super::visibility_columns::VisibilityColumns;

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
    ) -> Result<Self, TableError> {
        let read = |name| {
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
