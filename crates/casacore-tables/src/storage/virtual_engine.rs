// SPDX-License-Identifier: LGPL-3.0-or-later
//! Virtual column engine trait and registry.
//!
//! Virtual columns are computed from other data rather than stored on disk.
//! During loading, stored columns are populated first (pass 1), then virtual
//! engines materialize their columns from the already-loaded data (pass 2).
//!
//! # C++ equivalent
//!
//! `VirtualColumnEngine` / `DataManager::isVirtualColumn()` in
//! `casacore/tables/DataMan/VirtualColEngine.h`.

use std::fmt;
use std::path::{Path, PathBuf};

use casacore_types::{Complex64, RecordValue};

use super::StorageError;
use super::table_control::{ColumnDescContents, PlainColumnEntry};

/// Context provided to virtual engines during materialization.
///
/// Contains the column descriptors, already-loaded row data (from pass 1),
/// the table's filesystem path, and the total row count.
pub(crate) struct VirtualContext<'a> {
    /// All column descriptors from the table descriptor.
    pub col_descs: &'a [ColumnDescContents],
    /// Already-loaded rows from storage managers (pass 1).
    /// Virtual engines read stored column values from here.
    pub rows: &'a [RecordValue],
    /// Filesystem path of the table being loaded.
    pub table_path: &'a Path,
    /// Total number of rows in the table.
    pub nrrow: usize,
}

/// Trait for virtual column engines that compute column values from other data.
///
/// Implementors materialize virtual column values into the row records during
/// table loading. The engine reads configuration from column keywords and
/// computes values based on stored columns or external tables.
///
/// # C++ equivalent
///
/// `VirtualColumnEngine` in `casacore/tables/DataMan/VirtualColEngine.h`.
pub(crate) trait VirtualColumnEngine: fmt::Debug {
    /// Returns the C++ data manager type name (e.g. `"ForwardColumnEngine"`).
    fn type_name(&self) -> &str;

    /// Materialize virtual column values into the row records.
    ///
    /// `bound_cols` contains `(desc_index, PlainColumnEntry)` pairs for the
    /// columns bound to this engine instance. The engine should insert
    /// computed values into each row of `rows` for these columns.
    fn materialize(
        &self,
        ctx: &VirtualContext,
        bound_cols: &[(usize, &PlainColumnEntry)],
        rows: &mut [RecordValue],
    ) -> Result<(), StorageError>;
}

/// Look up a virtual engine implementation by its C++ type name.
///
/// Returns `Some(engine)` for supported virtual engine types, `None` otherwise.
/// Uses prefix matching for parameterized engines like `ScaledArrayEngine<...>`.
pub(crate) fn lookup_engine(type_name: &str) -> Option<Box<dyn VirtualColumnEngine>> {
    use super::virtual_scaled_array::{ScaledColumnEngine, ScaledVariant};
    if type_name == "ForwardColumnEngine" {
        Some(Box::new(super::virtual_forward::ForwardColumnEngine))
    } else if type_name.starts_with("ScaledArrayEngine") {
        Some(Box::new(ScaledColumnEngine {
            variant: ScaledVariant::Array,
        }))
    } else if type_name.starts_with("ScaledComplexData") {
        Some(Box::new(ScaledColumnEngine {
            variant: ScaledVariant::ComplexData,
        }))
    } else {
        None
    }
}

/// Returns `true` if the given DM type name is a recognized virtual engine.
pub(crate) fn is_virtual_engine(type_name: &str) -> bool {
    type_name == "ForwardColumnEngine"
        || type_name.starts_with("ScaledArrayEngine")
        || type_name.starts_with("ScaledComplexData")
}

/// Metadata for a virtual column binding, used during save to produce
/// the correct DM entries and column keywords in `table.dat`.
#[derive(Debug, Clone)]
pub(crate) enum VirtualColumnBinding {
    /// A ForwardColumnEngine binding: column `col_name` reads from `ref_table`.
    Forward {
        col_name: String,
        ref_table: PathBuf,
    },
    /// A ScaledArrayEngine binding: column `virtual_col` = `stored_col * scale + offset`.
    ScaledArray {
        virtual_col: String,
        stored_col: String,
        scale: f64,
        offset: f64,
    },
    /// A ScaledComplexData binding: stored `[2, ...]` → virtual Complex with
    /// complex-valued scale/offset applied per re/im component.
    ///
    /// # C++ equivalent
    ///
    /// `ScaledComplexData<Complex,Short>(virtualCol, storedCol, scale, offset)`.
    ScaledComplexData {
        virtual_col: String,
        stored_col: String,
        scale: Complex64,
        offset: Complex64,
    },
}
