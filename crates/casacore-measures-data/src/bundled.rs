// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bundled IERS EOP data snapshot.
//!
//! The `finals2000A.data` file is included at compile time via [`include_str!`],
//! parsed on first access, and cached as a `&'static EopTable` for the
//! lifetime of the process.
//!
//! This ensures that EOP data is always available even without network access
//! or local data files. The bundled snapshot is updated periodically by
//! maintainers.

use std::sync::LazyLock;

use super::EopTable;

/// The bundled finals2000A.data file, included at compile time.
const BUNDLED_DATA: &str = include_str!("../data/finals2000A.data");

/// Lazily-parsed bundled EOP table.
static BUNDLED_TABLE: LazyLock<EopTable> = LazyLock::new(|| {
    EopTable::from_finals2000a(BUNDLED_DATA).expect("bundled finals2000A.data must be valid")
});

/// Returns a reference to the bundled EOP table.
///
/// The table is parsed on first access and cached for the lifetime of the
/// process. This is an O(1) operation after the first call.
pub fn bundled_eop_table() -> &'static EopTable {
    &BUNDLED_TABLE
}
