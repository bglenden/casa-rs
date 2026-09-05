// SPDX-License-Identifier: LGPL-3.0-or-later
//! Machine-readable UI schema for `casars-imager`.

use casa_ms::presentation::UiCommandSchema;

/// Build the launcher-facing UI schema for the standalone imager.
pub fn command_schema(program_name: &str) -> UiCommandSchema {
    let bundle = casa_provider_contracts::builtin_surface_bundle("imager")
        .expect("built-in imager parameter surface must remain valid");
    let mut schema: UiCommandSchema =
        serde_json::from_value(casa_provider_contracts::project_ui_form(&bundle))
            .expect("canonical imager UI projection must match UiCommandSchema");
    schema.invocation_name = program_name.to_string();
    schema.usage = format!("{program_name} [parameters]");
    schema
}

#[cfg(test)]
mod tests {
    use super::command_schema;
    use casa_ms::presentation::{UiArgumentParser, UiValueKind};

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

        let save_continuum_residual = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "save_continuum_residual")
            .expect("save_continuum_residual argument");
        assert_eq!(save_continuum_residual.default.as_deref(), Some("false"));
        assert!(matches!(
            save_continuum_residual.value_kind,
            UiValueKind::Bool
        ));
        let UiArgumentParser::Toggle { true_flags, .. } = &save_continuum_residual.parser else {
            panic!("save_continuum_residual should use a toggle parser");
        };
        assert!(true_flags.contains(&"--save-continuum-residual".to_string()));
        assert!(
            save_continuum_residual
                .help
                .contains("existing CORRECTED_DATA")
        );
        assert!(save_continuum_residual.help.contains("in place"));
        assert!(schema.render_help().contains("--save-continuum-residual"));

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
        assert_eq!(standard_mfs_acceleration.default.as_deref(), Some("cpu"));
        assert!(standard_mfs_acceleration.advanced);
        let UiArgumentParser::Option { flags, choices, .. } = &standard_mfs_acceleration.parser
        else {
            panic!("standard_mfs_acceleration should use an option parser");
        };
        assert!(flags.contains(&"--standard-mfs-acceleration".to_string()));
        assert!(choices.contains(&"metal".to_string()));
        assert!(choices.contains(&"multi-cpu".to_string()));

        let stokes = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "stokes")
            .expect("stokes argument");
        let UiArgumentParser::Option { flags, choices, .. } = &stokes.parser else {
            panic!("stokes should use an option parser");
        };
        assert!(flags.contains(&"--stokes".to_string()));
        assert!(flags.contains(&"--corr".to_string()));
        assert!(
            choices.is_empty(),
            "stokes accepts the full CASA selector grammar"
        );
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
        assert_eq!(default_for("imaging_fft_backend"), "rustfft");
        assert_eq!(default_for("imaging_memory_target_mb"), "none");
        assert_eq!(default_for("imaging_memory_pressure_policy"), "auto");
        for id in [
            "chanchunks",
            "parallel",
            "imaging_read_ahead_blocks",
            "imaging_fft_backend",
            "imaging_memory_target_mb",
            "imaging_memory_pressure_policy",
        ] {
            let argument = schema
                .arguments
                .iter()
                .find(|argument| argument.id == id)
                .unwrap_or_else(|| panic!("missing {id}"));
            assert!(argument.advanced, "{id} should remain an advanced control");
        }
        let memory_policy = schema
            .arguments
            .iter()
            .find(|argument| argument.id == "imaging_memory_pressure_policy")
            .expect("imaging_memory_pressure_policy");
        let UiArgumentParser::Option { choices, .. } = &memory_policy.parser else {
            panic!("imaging_memory_pressure_policy should use an option parser");
        };
        assert_eq!(
            choices,
            &vec![
                "auto".to_string(),
                "conservative-no-swap".to_string(),
                "aggressive".to_string(),
                "oversubscribe".to_string(),
            ]
        );
        assert!(
            schema
                .arguments
                .iter()
                .all(|argument| argument.id != "progress_detail"),
            "runtime telemetry controls must not enter parameter profiles"
        );
    }
}
