// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bundled observatory catalog snapshot.

use std::sync::LazyLock;

use super::ObservatoryCatalog;

/// The bundled observatory snapshot, included at compile time.
const BUNDLED_DATA: &str = include_str!("../data/observatories.json");

/// Lazily-parsed bundled observatory catalog.
static BUNDLED_CATALOG: LazyLock<ObservatoryCatalog> = LazyLock::new(|| {
    ObservatoryCatalog::from_json_str(BUNDLED_DATA)
        .expect("bundled observatories.json must be valid")
});

/// Returns the bundled observatory catalog.
pub fn bundled_observatory_catalog() -> &'static ObservatoryCatalog {
    &BUNDLED_CATALOG
}
