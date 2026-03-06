// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the FEED subtable.
//!
//! Cf. C++ `MSFeedColumns`.

use crate::schema::{self, SubtableId};

crate::subtables::define_generic_subtable!(
    read = MsFeed,
    write = MsFeedMut,
    id = SubtableId::Feed,
    required = schema::feed::REQUIRED_COLUMNS,
    optional = schema::feed::OPTIONAL_COLUMNS,
    table_name = "FEED",
    read_doc = "Read-only typed wrapper for the FEED subtable.",
    write_doc = "Mutable typed wrapper for the FEED subtable."
);
