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

use casa_types::{Complex64, RecordValue};

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
    use super::virtual_compress::{
        CompressComplexEngine, CompressComplexVariant, CompressFloatEngine,
    };
    use super::virtual_scaled_array::{ScaledColumnEngine, ScaledVariant};
    if type_name == "ForwardColumnEngine" {
        Some(Box::new(super::virtual_forward::ForwardColumnEngine))
    } else if type_name == "ForwardColumnIndexedRowEngine" {
        Some(Box::new(
            super::virtual_forward::ForwardColumnIndexedRowEngine,
        ))
    } else if type_name.starts_with("ScaledArrayEngine") {
        Some(Box::new(ScaledColumnEngine {
            variant: ScaledVariant::Array,
        }))
    } else if type_name.starts_with("ScaledComplexData") {
        Some(Box::new(ScaledColumnEngine {
            variant: ScaledVariant::ComplexData,
        }))
    } else if type_name.starts_with("BitFlagsEngine") {
        Some(Box::new(super::virtual_bitflags::BitFlagsEngine))
    } else if type_name == "CompressFloat" {
        Some(Box::new(CompressFloatEngine))
    } else if type_name == "CompressComplex" {
        Some(Box::new(CompressComplexEngine {
            variant: CompressComplexVariant::Standard,
        }))
    } else if type_name == "CompressComplexSD" {
        Some(Box::new(CompressComplexEngine {
            variant: CompressComplexVariant::SingleDish,
        }))
    } else if type_name == "VirtualTaQLColumn" {
        Some(Box::new(
            super::virtual_taql_column::VirtualTaQLColumnEngine,
        ))
    } else {
        None
    }
}

/// Returns `true` if the given DM type name is a recognized virtual engine.
pub(crate) fn is_virtual_engine(type_name: &str) -> bool {
    type_name == "ForwardColumnEngine"
        || type_name == "ForwardColumnIndexedRowEngine"
        || type_name.starts_with("ScaledArrayEngine")
        || type_name.starts_with("ScaledComplexData")
        || type_name.starts_with("BitFlagsEngine")
        || type_name == "CompressFloat"
        || type_name == "CompressComplex"
        || type_name == "CompressComplexSD"
        || type_name == "VirtualTaQLColumn"
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
    /// A BitFlagsEngine binding: stored integer → virtual Bool via bitmask.
    ///
    /// # C++ equivalent
    ///
    /// `BitFlagsEngine<uChar|Short|Int>`.
    BitFlags {
        virtual_col: String,
        stored_col: String,
        read_mask: u32,
        write_mask: u32,
    },
    /// A CompressFloat binding: stored Int16 → virtual Float via FITS-style scaling.
    ///
    /// # C++ equivalent
    ///
    /// `CompressFloat` in `casacore/tables/DataMan/CompressFloat.h`.
    CompressFloat {
        virtual_col: String,
        stored_col: String,
        scale: f32,
        offset: f32,
    },
    /// A CompressComplex binding: stored Int32 → virtual Complex.
    ///
    /// # C++ equivalent
    ///
    /// `CompressComplex` / `CompressComplexSD`.
    CompressComplex {
        virtual_col: String,
        stored_col: String,
        scale: f32,
        offset: f32,
        single_dish: bool,
    },
    /// A ForwardColumnIndexedRowEngine binding: forwarding with row remapping.
    ///
    /// # C++ equivalent
    ///
    /// `ForwardColumnIndexedRowEngine`.
    ForwardIndexedRow {
        col_name: String,
        ref_table: PathBuf,
        row_column: String,
    },
    /// A VirtualTaQLColumn binding: column computed from a TaQL expression.
    ///
    /// # C++ equivalent
    ///
    /// `VirtualTaQLColumn` in `casacore/tables/DataMan/VirtualTaQLColumn.h`.
    TaQLColumn {
        col_name: String,
        expression: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_engine_matches_all_supported_virtual_engines() {
        let cases = [
            (
                "ForwardColumnEngine",
                "ForwardColumnEngine",
                "ForwardColumnEngine",
            ),
            (
                "ForwardColumnIndexedRowEngine",
                "ForwardColumnIndexedRowEngine",
                "ForwardColumnIndexedRowEngine",
            ),
            (
                "ScaledArrayEngine<Float,Short>",
                "ScaledArrayEngine",
                "ScaledColumnEngine",
            ),
            (
                "ScaledComplexData<Complex,Short>",
                "ScaledComplexData",
                "ScaledColumnEngine",
            ),
            ("BitFlagsEngine<Int>", "BitFlagsEngine", "BitFlagsEngine"),
            ("CompressFloat", "CompressFloat", "CompressFloatEngine"),
            (
                "CompressComplex",
                "CompressComplex",
                "CompressComplexEngine",
            ),
            (
                "CompressComplexSD",
                "CompressComplexSD",
                "CompressComplexEngine",
            ),
            (
                "VirtualTaQLColumn",
                "VirtualTaQLColumn",
                "VirtualTaQLColumnEngine",
            ),
        ];

        for (type_name, expected_type_name, expected_debug_name) in cases {
            let engine = lookup_engine(type_name)
                .unwrap_or_else(|| panic!("expected virtual engine for {type_name}"));
            assert!(
                is_virtual_engine(type_name),
                "{type_name} should be recognized"
            );
            assert_eq!(engine.type_name(), expected_type_name);
            assert!(
                format!("{engine:?}").contains(expected_debug_name),
                "debug output for {type_name} should identify {expected_debug_name}"
            );
        }
    }

    #[test]
    fn lookup_engine_rejects_unknown_or_similar_names() {
        let cases = [
            "",
            "ForwardColumnEngineExtra",
            "CompressComplexSingleDish",
            "VirtualTaQLColumnV2",
            "UnknownEngine",
        ];

        for type_name in cases {
            assert!(lookup_engine(type_name).is_none());
            assert!(!is_virtual_engine(type_name));
        }
    }

    #[test]
    fn virtual_column_bindings_clone_and_debug_with_expected_fields() {
        let bindings = [
            VirtualColumnBinding::Forward {
                col_name: "DATA".to_string(),
                ref_table: PathBuf::from("ref-table"),
            },
            VirtualColumnBinding::ScaledArray {
                virtual_col: "FLOAT_DATA".to_string(),
                stored_col: "DATA_SHORT".to_string(),
                scale: 2.0,
                offset: -1.0,
            },
            VirtualColumnBinding::ScaledComplexData {
                virtual_col: "DATA".to_string(),
                stored_col: "DATA_SHORT".to_string(),
                scale: Complex64::new(1.0, -1.0),
                offset: Complex64::new(0.5, 0.25),
            },
            VirtualColumnBinding::BitFlags {
                virtual_col: "FLAG".to_string(),
                stored_col: "FLAG_BITS".to_string(),
                read_mask: 0x1,
                write_mask: 0x2,
            },
            VirtualColumnBinding::CompressFloat {
                virtual_col: "FLOAT_DATA".to_string(),
                stored_col: "DATA_SHORT".to_string(),
                scale: 0.5,
                offset: 10.0,
            },
            VirtualColumnBinding::CompressComplex {
                virtual_col: "DATA".to_string(),
                stored_col: "DATA_INT".to_string(),
                scale: 0.25,
                offset: -3.0,
                single_dish: true,
            },
            VirtualColumnBinding::ForwardIndexedRow {
                col_name: "MODEL_DATA".to_string(),
                ref_table: PathBuf::from("model-table"),
                row_column: "MODEL_ROW".to_string(),
            },
            VirtualColumnBinding::TaQLColumn {
                col_name: "DERIVED".to_string(),
                expression: "DATA + MODEL_DATA".to_string(),
            },
        ];

        for binding in bindings {
            let cloned = binding.clone();
            let debug = format!("{cloned:?}");
            assert!(debug.contains("col") || debug.contains("DATA") || debug.contains("FLAG"));
            assert!(
                debug.contains("Forward")
                    || debug.contains("Scaled")
                    || debug.contains("Compress")
                    || debug.contains("TaQL")
                    || debug.contains("BitFlags")
            );
        }
    }
}
