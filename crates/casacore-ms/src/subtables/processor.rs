// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the PROCESSOR subtable.
//!
//! Cf. C++ `MSProcessorColumns`.

use crate::schema::{self, SubtableId};

crate::subtables::define_generic_subtable!(
    read = MsProcessor,
    write = MsProcessorMut,
    id = SubtableId::Processor,
    required = schema::processor::REQUIRED_COLUMNS,
    optional = schema::processor::OPTIONAL_COLUMNS,
    table_name = "PROCESSOR",
    read_doc = "Read-only typed wrapper for the PROCESSOR subtable.",
    write_doc = "Mutable typed wrapper for the PROCESSOR subtable."
);
