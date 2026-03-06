// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the DOPPLER subtable.
//!
//! This subtable uses the generic typed accessors because its API surface is
//! comparatively small and mostly row-oriented.
//!
//! Cf. C++ `MSDopplerColumns`.

use crate::schema::{self, SubtableId};

crate::subtables::define_generic_subtable!(
    read = MsDoppler,
    write = MsDopplerMut,
    id = SubtableId::Doppler,
    required = schema::doppler::REQUIRED_COLUMNS,
    optional = schema::doppler::OPTIONAL_COLUMNS,
    table_name = "DOPPLER",
    read_doc = "Read-only typed wrapper for the DOPPLER subtable.",
    write_doc = "Mutable typed wrapper for the DOPPLER subtable."
);
