// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the STATE subtable.
//!
//! Cf. C++ `MSStateColumns`.

use crate::schema::{self, SubtableId};

crate::subtables::define_generic_subtable!(
    read = MsState,
    write = MsStateMut,
    id = SubtableId::State,
    required = schema::state::REQUIRED_COLUMNS,
    optional = schema::state::OPTIONAL_COLUMNS,
    table_name = "STATE",
    read_doc = "Read-only typed wrapper for the STATE subtable.",
    write_doc = "Mutable typed wrapper for the STATE subtable."
);
