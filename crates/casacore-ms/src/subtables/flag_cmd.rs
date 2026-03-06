// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the FLAG_CMD subtable.
//!
//! Cf. C++ `MSFlagCmdColumns`.

use crate::schema::{self, SubtableId};

crate::subtables::define_generic_subtable!(
    read = MsFlagCmd,
    write = MsFlagCmdMut,
    id = SubtableId::FlagCmd,
    required = schema::flag_cmd::REQUIRED_COLUMNS,
    optional = schema::flag_cmd::OPTIONAL_COLUMNS,
    table_name = "FLAG_CMD",
    read_doc = "Read-only typed wrapper for the FLAG_CMD subtable.",
    write_doc = "Mutable typed wrapper for the FLAG_CMD subtable."
);
