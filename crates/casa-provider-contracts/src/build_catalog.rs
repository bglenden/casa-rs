// SPDX-License-Identifier: LGPL-3.0-or-later
//! Parse and validate the immutable task catalog, at build time and on demand.

use crate::{ContractValidationError, SurfaceCatalogBundle};

pub(crate) fn parse_and_validate(
    catalog_json: &str,
    surfaces_json: &str,
) -> Result<SurfaceCatalogBundle, String> {
    let bundle = crate::catalog_parse::parse(catalog_json, surfaces_json)?;
    bundle.validate().map_err(format_errors)?;
    Ok(bundle)
}

pub(crate) fn format_errors(errors: Vec<ContractValidationError>) -> String {
    errors
        .into_iter()
        .map(|error| {
            format!(
                "{}{}{}: {}",
                error.code,
                error
                    .surface
                    .as_deref()
                    .map_or_else(String::new, |surface| format!(" [{surface}]")),
                error
                    .parameter
                    .as_deref()
                    .map_or_else(String::new, |parameter| format!(".{parameter}")),
                error.message
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::parse_and_validate;

    const CATALOG: &str = include_str!("../resources/parameter-catalog.json");
    const SURFACES: &str = include_str!("../resources/parameter-surfaces.json");

    #[test]
    fn build_validation_rejects_invalid_aggregate_catalog() {
        let mut catalog: serde_json::Value = serde_json::from_str(CATALOG).unwrap();
        catalog["schema_version"] = 999.into();
        let error = parse_and_validate(&catalog.to_string(), SURFACES).unwrap_err();
        assert!(error.contains("schema_version"), "{error}");
        assert!(
            parse_and_validate("invalid json", SURFACES)
                .unwrap_err()
                .contains("parameter-catalog.json")
        );
    }
}
