// SPDX-License-Identifier: LGPL-3.0-or-later
//! `imexplore` command schema projected from its canonical surface contract.

use casa_provider_contracts::{builtin_surface_bundle, project_ui_schema};

/// Return the `imexplore` UI schema as formatted JSON.
pub fn ui_schema_json(invocation_name: &str) -> Result<String, String> {
    let bundle = builtin_surface_bundle("imexplore")?;
    let canonical_invocation = bundle.surface.execution().invocation_name.as_str();
    if invocation_name != canonical_invocation {
        return Err(format!(
            "imexplore UI schema invocation {invocation_name:?} does not match canonical invocation {canonical_invocation:?}"
        ));
    }
    serde_json::to_string_pretty(&project_ui_schema(&bundle))
        .map_err(|error| format!("serialize imexplore UI schema: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ui_schema_is_exact_canonical_projection() {
        let actual: serde_json::Value =
            serde_json::from_str(&ui_schema_json("imexplore").unwrap()).unwrap();
        let expected = project_ui_schema(&builtin_surface_bundle("imexplore").unwrap());
        assert_eq!(actual, expected);
    }

    #[test]
    fn ui_schema_rejects_noncanonical_invocation_alias() {
        assert!(ui_schema_json("image-browser").is_err());
    }
}
