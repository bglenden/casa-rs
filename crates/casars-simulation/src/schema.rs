// SPDX-License-Identifier: LGPL-3.0-or-later
//! Machine-readable UI schema and help for `simobserve`.

use casa_ms::presentation::UiCommandSchema;

/// The launcher-facing UI schema of `simobserve`.
pub fn command_schema(program_name: &str) -> UiCommandSchema {
    let bundle = casa_provider_contracts::builtin_surface_bundle("simobserve")
        .expect("built-in simobserve parameter surface must remain valid");
    let mut schema: UiCommandSchema =
        serde_json::from_value(casa_provider_contracts::project_ui_form(&bundle))
            .expect("canonical simobserve UI projection must match UiCommandSchema");
    schema.invocation_name = program_name.to_string();
    schema.usage = format!("{program_name} [parameters]");
    schema
}

/// `--help`: the parameter surface followed by the shared task-host actions.
pub(crate) fn render_help(program_name: &str) -> String {
    format!(
        "{}\n\n{}",
        command_schema(program_name).render_help(),
        casa_task_runtime::task_cli_machine_help("SimobserveTaskRequest")
    )
}

#[cfg(test)]
mod tests {
    use super::{command_schema, render_help};

    #[test]
    fn the_schema_is_casas_simobserve() {
        let schema = command_schema("simobserve");
        assert_eq!(schema.command_id, "simobserve");
        assert_eq!(schema.category, "Simulation");
        assert_eq!(
            schema
                .managed_output
                .as_ref()
                .map(|output| output.renderer.as_str()),
            Some("simobserve-run-v1")
        );
        let surface = casa_provider_contracts::builtin_surface_bundle("simobserve").unwrap();
        for binding in surface.surface.bindings() {
            assert!(
                schema
                    .arguments
                    .iter()
                    .any(|argument| argument.id == binding.name),
                "{}",
                binding.name
            );
        }
        let help = render_help("simobserve");
        assert!(help.contains("--antennalist"), "{help}");
        assert!(help.contains("--json-run"), "{help}");
    }
}
