// SPDX-License-Identifier: LGPL-3.0-or-later
//! Inspect an IncrementalStMan's stored row indexes.

use super::{Table, TableError};

pub use crate::storage::incremental_stman::IncrementalCorruption;

/// Result of examining the manager bound to a column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrementalLayoutCheck {
    /// The column does not use IncrementalStMan.
    NotIncremental,
    /// No non-ascending row index was found in the manager's buckets.
    Valid,
    /// A bucket contains a non-ascending row index.
    Corrupt(IncrementalCorruption),
}

impl Table {
    /// Check all bucket row indexes in the IncrementalStMan bound to `column`.
    ///
    /// A non-incremental manager returns `NotIncremental`. A missing column,
    /// unreadable storage, or a table whose manager binding cannot be resolved
    /// returns an error rather than asserting that its layout is valid. This
    /// checks the same ascending-row property as casacore's
    /// `ROIncrementalStManAccessor::checkBucketLayout`, not cell contents.
    pub fn check_incremental_layout(
        &self,
        column: &str,
    ) -> Result<IncrementalLayoutCheck, TableError> {
        if !self
            .schema()
            .is_some_and(|schema| schema.columns().iter().any(|col| col.name() == column))
        {
            return Err(TableError::SchemaColumnUnknown {
                column: column.into(),
            });
        }
        if self.is_memory() {
            return Ok(IncrementalLayoutCheck::NotIncremental);
        }
        let manager = self
            .data_manager_info()
            .iter()
            .find(|manager| manager.columns.iter().any(|name| name == column))
            .ok_or_else(|| {
                TableError::Storage(format!("cannot resolve data manager for column {column}"))
            })?;
        if manager.dm_type != "IncrementalStMan" {
            return Ok(IncrementalLayoutCheck::NotIncremental);
        }
        let path = self
            .source_path
            .as_ref()
            .ok_or_else(|| TableError::Storage("incremental manager has no storage path".into()))?
            .join(format!("table.f{}", manager.seq_nr));
        Ok(
            match crate::storage::incremental_stman::check_incremental_layout(
                &path,
                manager.columns.len(),
            )? {
                Some(corruption) => IncrementalLayoutCheck::Corrupt(corruption),
                None => IncrementalLayoutCheck::Valid,
            },
        )
    }
}
