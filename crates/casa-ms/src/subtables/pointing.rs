// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the POINTING subtable.
//!
//! Cf. C++ `MSPointingColumns`.

use crate::schema::{self, SubtableId};

crate::subtables::define_generic_subtable!(
    read = MsPointing,
    write = MsPointingMut,
    id = SubtableId::Pointing,
    required = schema::pointing::REQUIRED_COLUMNS,
    optional = schema::pointing::OPTIONAL_COLUMNS,
    table_name = "POINTING",
    read_doc = "Read-only typed wrapper for the POINTING subtable.",
    write_doc = "Mutable typed wrapper for the POINTING subtable."
);
