// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the HISTORY subtable.
//!
//! Cf. C++ `MSHistoryColumns`.

use crate::schema::{self, SubtableId};

crate::subtables::define_generic_subtable!(
    read = MsHistory,
    write = MsHistoryMut,
    id = SubtableId::History,
    required = schema::history::REQUIRED_COLUMNS,
    optional = schema::history::OPTIONAL_COLUMNS,
    table_name = "HISTORY",
    read_doc = "Read-only typed wrapper for the HISTORY subtable.",
    write_doc = "Mutable typed wrapper for the HISTORY subtable."
);
