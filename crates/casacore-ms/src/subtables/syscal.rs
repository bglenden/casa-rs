// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the SYSCAL subtable.
//!
//! Cf. C++ `MSSysCalColumns`.

use crate::schema::{self, SubtableId};

crate::subtables::define_generic_subtable!(
    read = MsSysCal,
    write = MsSysCalMut,
    id = SubtableId::SysCal,
    required = schema::syscal::REQUIRED_COLUMNS,
    optional = schema::syscal::OPTIONAL_COLUMNS,
    table_name = "SYSCAL",
    read_doc = "Read-only typed wrapper for the SYSCAL subtable.",
    write_doc = "Mutable typed wrapper for the SYSCAL subtable."
);
