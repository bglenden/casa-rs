// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the SOURCE subtable.
//!
//! Cf. C++ `MSSourceColumns`.

use crate::schema::{self, SubtableId};

crate::subtables::define_generic_subtable!(
    read = MsSource,
    write = MsSourceMut,
    id = SubtableId::Source,
    required = schema::source::REQUIRED_COLUMNS,
    optional = schema::source::OPTIONAL_COLUMNS,
    table_name = "SOURCE",
    read_doc = "Read-only typed wrapper for the SOURCE subtable.",
    write_doc = "Mutable typed wrapper for the SOURCE subtable."
);
