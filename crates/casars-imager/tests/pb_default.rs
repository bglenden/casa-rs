// SPDX-License-Identifier: LGPL-3.0-or-later

//! Current PB defaults and explicit overrides.

use casa_provider_contracts::{ParameterValue, builtin_surface_bundle};
use casa_task_runtime::{parse_profile, resolve_profile};

#[test]
fn current_profile_defaults_to_writing_pb_but_preserves_explicit_false() {
    let bundle = builtin_surface_bundle("tclean").unwrap();
    let version = bundle.surface.contract_version();
    for (line, expected) in [("", true), ("write_pb = false", false)] {
        let text = format!(
            "[casars]\nformat = 1\nsurface = \"tclean\"\nkind = \"task\"\ncontract = {version}\n\n[parameters]\nvis = \"input.ms\"\nimagename = \"image\"\ncell = \"1arcsec\"\n{line}\n"
        );
        let profile = parse_profile(&text).unwrap();
        let resolved = resolve_profile(&profile, &bundle).unwrap();
        assert_eq!(resolved.values["write_pb"], ParameterValue::Bool(expected));
    }
}
