// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the OBSERVATION subtable.
//!
//! Cf. C++ `MSObservationColumns`.

use crate::schema::{self, SubtableId};

crate::subtables::define_generic_subtable!(
    read = MsObservation,
    write = MsObservationMut,
    id = SubtableId::Observation,
    required = schema::observation::REQUIRED_COLUMNS,
    optional = schema::observation::OPTIONAL_COLUMNS,
    table_name = "OBSERVATION",
    read_doc = "Read-only typed wrapper for the OBSERVATION subtable.",
    write_doc = "Mutable typed wrapper for the OBSERVATION subtable."
);
