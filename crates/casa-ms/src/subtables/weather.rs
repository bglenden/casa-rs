// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the WEATHER subtable.
//!
//! Cf. C++ `MSWeatherColumns`.

use crate::schema::{self, SubtableId};

crate::subtables::define_generic_subtable!(
    read = MsWeather,
    write = MsWeatherMut,
    id = SubtableId::Weather,
    required = schema::weather::REQUIRED_COLUMNS,
    optional = schema::weather::OPTIONAL_COLUMNS,
    table_name = "WEATHER",
    read_doc = "Read-only typed wrapper for the WEATHER subtable.",
    write_doc = "Mutable typed wrapper for the WEATHER subtable."
);
