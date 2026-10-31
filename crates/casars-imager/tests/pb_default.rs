// SPDX-License-Identifier: LGPL-3.0-or-later

//! Contract 30 preserves explicit PB overrides while changing the default.

use casa_provider_contracts::{ParameterValue, builtin_surface_bundle};
use casa_task_runtime::{DiagnosticCode, parse_profile, resolve_profile};

#[test]
fn contract_29_warns_for_an_omitted_pb_default_and_preserves_explicit_false() {
    let bundle = builtin_surface_bundle("tclean").unwrap();
    for (line, expected, warning) in [("", true, true), ("write_pb = false", false, false)] {
        let text = format!(
            "[casars]\nformat = 1\nsurface = \"tclean\"\nkind = \"task\"\ncontract = 29\n\n[parameters]\nvis = \"input.ms\"\nimagename = \"image\"\ncell = \"1arcsec\"\n{line}\n"
        );
        let profile = parse_profile(&text).unwrap();
        let migrated = resolve_profile(&profile, &bundle).unwrap();
        assert_eq!(migrated.values["write_pb"], ParameterValue::Bool(expected));
        assert_eq!(migrated.diagnostics.iter().any(|d|
            d.code == DiagnosticCode::DefaultChanged && d.message.contains("write_pb")), warning);
    }
}
