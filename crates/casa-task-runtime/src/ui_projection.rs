// SPDX-License-Identifier: LGPL-3.0-or-later
//! Derived compatibility/presentation projection from canonical definitions.

pub use casa_provider_contracts::project_ui_schema;

#[cfg(test)]
mod tests {
    use casa_provider_contracts::builtin_surface_bundle;

    use super::*;

    #[test]
    fn projection_keeps_canonical_names_and_private_flags_separate() {
        let schema = project_ui_schema(&builtin_surface_bundle("imager").unwrap());
        let arguments = schema["arguments"].as_array().unwrap();
        let cell = arguments
            .iter()
            .find(|argument| argument["id"] == "cell")
            .unwrap();
        assert_eq!(cell["concept_id"], "image.geometry.cell");
        assert_eq!(cell["parser"]["flags"][0], "--cell-arcsec");
        assert_eq!(cell["value_kind"], "string");
    }
}
