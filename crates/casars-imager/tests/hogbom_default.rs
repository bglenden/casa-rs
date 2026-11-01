// SPDX-License-Identifier: LGPL-3.0-or-later

//! Contract 31 adopts CASA's accounting only for omitted Hogbom defaults.

use casa_imaging_application::{HogbomIterationAccounting, ImagingRequest};
use casa_provider_contracts::{ParameterValue, builtin_surface_bundle};
use casa_task_runtime::{DiagnosticCode, parse_profile, resolve_profile};

#[test]
fn old_hogbom_defaults_warn_and_explicit_policies_survive() {
    let bundle = builtin_surface_bundle("tclean").unwrap();
    for version in [29, 30, 31, 32] {
        for explicit in [None, Some("strict"), Some("casa-inclusive")] {
            let line = explicit.map_or_else(String::new, |mode| {
                format!("hogbom_iteration_mode = {mode:?}")
            });
            let text = format!(
                "[casars]\nformat = 1\nsurface = \"tclean\"\nkind = \"task\"\ncontract = {version}\n\n[parameters]\nvis = \"input.ms\"\nimagename = \"image\"\ncell = \"1arcsec\"\n{line}\n"
            );
            let profile = parse_profile(&text).unwrap();
            let resolved = resolve_profile(&profile, &bundle).unwrap();
            let expected = explicit.unwrap_or("casa-inclusive");
            assert_eq!(
                resolved.values["hogbom_iteration_mode"],
                ParameterValue::String(expected.into())
            );
            assert_eq!(
                resolved
                    .diagnostics
                    .iter()
                    .any(|d| d.code == DiagnosticCode::DefaultChanged
                        && d.parameter.as_deref() == Some("hogbom_iteration_mode")),
                version < 31 && explicit.is_none()
            );
            let values = resolved
                .values
                .iter()
                .map(|(name, value)| (name.clone(), value.to_plain_json()))
                .collect();
            let request = ImagingRequest::from_tclean(&values).unwrap();
            assert_eq!(
                request.hogbom_iteration_mode,
                if expected == "strict" {
                    HogbomIterationAccounting::Strict
                } else {
                    HogbomIterationAccounting::CasaInclusive
                }
            );
        }
    }
    assert_eq!(
        HogbomIterationAccounting::default(),
        HogbomIterationAccounting::CasaInclusive
    );
}
