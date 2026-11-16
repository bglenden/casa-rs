// SPDX-License-Identifier: LGPL-3.0-or-later
//! Parse the bundled task catalog without changing its representation.

use serde::Deserialize;

use crate::{ParameterCatalog, SurfaceCatalogBundle, SurfaceDefinition};

#[derive(Deserialize)]
struct SurfaceIndex {
    schema_version: u32,
    surfaces: Vec<SurfaceDefinition>,
}

pub(crate) fn parse(
    catalog_json: &str,
    surfaces_json: &str,
) -> Result<SurfaceCatalogBundle, String> {
    let catalog = serde_json::from_str::<ParameterCatalog>(catalog_json)
        .map_err(|error| format!("parse provider parameter-catalog.json: {error}"))?;
    let index = serde_json::from_str::<SurfaceIndex>(surfaces_json)
        .map_err(|error| format!("parse provider parameter-surfaces.json: {error}"))?;
    Ok(SurfaceCatalogBundle {
        schema_version: index.schema_version,
        catalog,
        surfaces: index.surfaces,
    })
}
