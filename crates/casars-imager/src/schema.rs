// SPDX-License-Identifier: LGPL-3.0-or-later
//! Machine-readable UI schema for `casars-imager`.

use casa_ms::ui_schema::UiCommandSchema;

/// Build the launcher-facing UI schema for the standalone imager.
pub fn command_schema(program_name: &str) -> UiCommandSchema {
    let bundle = casa_provider_contracts::builtin_surface_bundle("imager")
        .expect("built-in imager parameter surface must remain valid");
    let mut schema: UiCommandSchema =
        serde_json::from_value(casa_provider_contracts::project_ui_schema(&bundle))
            .expect("canonical imager UI projection must match UiCommandSchema");
    schema.invocation_name = program_name.to_string();
    schema.usage = format!("{program_name} [parameters]");
    schema
}

#[cfg(test)]
mod tests {
    use super::command_schema;
    use casa_ms::ui_schema::{UiArgumentParser, UiValueKind};

    #[test]
    fn schema_exposes_workflow_surface_for_casars() {
        let schema = command_schema("casars-imager");
        assert_eq!(schema.command_id, "imager");
        assert_eq!(schema.display_name, "Imager");
        assert_eq!(schema.category, "Imaging");
        assert_eq!(
            schema
                .managed_output
                .as_ref()
                .map(|output| output.renderer.as_str()),
            Some("imager-run-v1")
        );

        let specmode = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "specmode")
            .expect("specmode argument");
        assert_eq!(specmode.group, "Stages");
        assert!(matches!(specmode.value_kind, UiValueKind::Choice));

        let usepointing = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "usepointing")
            .expect("usepointing argument");
        assert_eq!(usepointing.default.as_deref(), Some("false"));
        assert!(matches!(usepointing.value_kind, UiValueKind::Bool));
        let UiArgumentParser::Toggle { true_flags, .. } = &usepointing.parser else {
            panic!("usepointing should use a toggle parser");
        };
        assert!(true_flags.contains(&"--usepointing".to_string()));

        let savemodel = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "savemodel")
            .expect("savemodel argument");
        assert_eq!(savemodel.default.as_deref(), Some("none"));
        let UiArgumentParser::Option { choices, .. } = &savemodel.parser else {
            panic!("savemodel should use an option parser");
        };
        assert!(choices.contains(&"modelcolumn".to_string()));

        let startmodel = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "startmodel")
            .expect("startmodel argument");
        assert_eq!(startmodel.value_kind, UiValueKind::Path);
        let UiArgumentParser::Option { flags, .. } = &startmodel.parser else {
            panic!("startmodel should use an option parser");
        };
        assert!(flags.contains(&"--startmodel".to_string()));

        let outlierfile = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "outlierfile")
            .expect("outlierfile argument");
        assert_eq!(outlierfile.value_kind, UiValueKind::Path);
        let UiArgumentParser::Option { flags, .. } = &outlierfile.parser else {
            panic!("outlierfile should use an option parser");
        };
        assert!(flags.contains(&"--outlierfile".to_string()));

        let standard_mfs_acceleration = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "standard_mfs_acceleration")
            .expect("standard_mfs_acceleration argument");
        assert_eq!(standard_mfs_acceleration.default.as_deref(), Some("auto"));
        assert!(standard_mfs_acceleration.advanced);
        let UiArgumentParser::Option { flags, choices, .. } = &standard_mfs_acceleration.parser
        else {
            panic!("standard_mfs_acceleration should use an option parser");
        };
        assert!(flags.contains(&"--standard-mfs-acceleration".to_string()));
        assert!(choices.contains(&"metal".to_string()));
        assert!(choices.contains(&"multi-cpu".to_string()));

        let polarization = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "polarization")
            .expect("polarization argument");
        let UiArgumentParser::Option { choices, .. } = &polarization.parser else {
            panic!("polarization should use an option parser");
        };
        assert!(choices.contains(&"I".to_string()));
        assert!(choices.contains(&"XX".to_string()));
    }

    #[test]
    fn schema_defaults_match_profile_defaults_for_advanced_controls() {
        let schema = command_schema("casars-imager");
        let default_for = |id: &str| {
            schema
                .arguments
                .iter()
                .find(|argument| argument.id == id)
                .unwrap_or_else(|| panic!("missing {id}"))
                .default
                .as_deref()
                .unwrap_or_default()
                .to_string()
        };

        assert_eq!(default_for("gain"), "0.1");
        assert_eq!(default_for("pblimit"), "0.2");
        assert_eq!(default_for("minor_cycle_length"), "1000");
        assert_eq!(default_for("minpsffraction"), "0.05");
        assert_eq!(default_for("chanchunks"), "none");
        assert_eq!(default_for("parallel"), "none");
        assert_eq!(default_for("imaging_read_ahead_blocks"), "none");
        assert_eq!(default_for("imaging_fft_backend"), "auto");
        for id in [
            "chanchunks",
            "parallel",
            "imaging_read_ahead_blocks",
            "imaging_fft_backend",
        ] {
            let argument = schema
                .arguments
                .iter()
                .find(|argument| argument.id == id)
                .unwrap_or_else(|| panic!("missing {id}"));
            assert!(argument.advanced, "{id} should remain an advanced control");
        }
        assert!(
            schema
                .arguments
                .iter()
                .all(|argument| argument.id != "progress_detail"),
            "runtime telemetry controls must not enter parameter profiles"
        );
    }
}
