// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the FREQ_OFFSET subtable.
//!
//! Cf. C++ `MSFreqOffsetColumns`.

use crate::schema::{self, SubtableId};

crate::subtables::define_generic_subtable!(
    read = MsFreqOffset,
    write = MsFreqOffsetMut,
    id = SubtableId::FreqOffset,
    required = schema::freq_offset::REQUIRED_COLUMNS,
    optional = schema::freq_offset::OPTIONAL_COLUMNS,
    table_name = "FREQ_OFFSET",
    read_doc = "Read-only typed wrapper for the FREQ_OFFSET subtable.",
    write_doc = "Mutable typed wrapper for the FREQ_OFFSET subtable."
);
