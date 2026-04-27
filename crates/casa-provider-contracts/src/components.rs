// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;

use schemars::schema::{RootSchema, Schema};

/// Merged reusable component schemas extracted from one or more root schemas.
pub type ProviderComponentSchemas = BTreeMap<String, Schema>;

/// Merge reusable component definitions from one or more root schemas.
pub fn merged_components<'a>(
    schemas: impl IntoIterator<Item = &'a RootSchema>,
) -> ProviderComponentSchemas {
    let mut merged = ProviderComponentSchemas::new();
    for root in schemas {
        for (name, schema) in &root.definitions {
            merged.insert(name.clone(), schema.clone());
        }
    }
    merged
}
