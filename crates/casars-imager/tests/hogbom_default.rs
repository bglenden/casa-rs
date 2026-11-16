// SPDX-License-Identifier: LGPL-3.0-or-later

//! Current Hogbom iteration accounting defaults and explicit overrides.

use casa_imaging_application::{HogbomIterationAccounting, ImagingRequest};
use casa_provider_contracts::{ParameterValue, builtin_surface_bundle};
use casa_task_runtime::{parse_profile, resolve_profile};

#[test]
fn current_hogbom_defaults_and_explicit_policies_reach_imaging() {
    let bundle = builtin_surface_bundle("tclean").unwrap();
    let version = bundle.surface.contract_version();
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
    assert_eq!(
        HogbomIterationAccounting::default(),
        HogbomIterationAccounting::CasaInclusive
    );
}
