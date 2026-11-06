// SPDX-License-Identifier: LGPL-3.0-or-later
//! The image task binaries on the shared task front end (#931).
//!
//! Each binary's `main` builds its front end here, passing the surfaces it
//! hosts and a callback that reads the resolved run into its typed request.
//! The front end owns the command line, `--json-run`, Last and logging.

use casa_provider_contracts::CasaValue;
use casa_task_runtime::{TaskFailure, TaskFrontEnd, TaskInvocation, TaskSuccess};

use crate::{ImageAnalysisTaskResult, ImageError, image_analysis_task_schema_bundle};

#[cfg(casa_prebuilt_image_provider)]
include!(concat!(env!("OUT_DIR"), "/image-provider-probes.rs"));

#[cfg(casa_prebuilt_image_provider)]
fn compiled_provider_matches_target() -> bool {
    static MATCHES: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *MATCHES
        .get_or_init(|| provider_matches_target(IMAGE_PROVIDER_SCHEMA_ORDER, IMAGE_PROVIDER_PROBES))
}

#[cfg(casa_prebuilt_image_provider)]
fn provider_matches_target(order: bool, probes: &[(&str, u64, u64, u64)]) -> bool {
    crate::analysis::schema_maps_preserve_order() == order
        && casa_provider_contracts::builtin_provider_parser_matches(probes)
}

/// The front end of the image binary `program`, which hosts `surfaces`.
pub fn image_task_front_end<F>(program: &str, surfaces: &[&str], execute: F) -> TaskFrontEnd<F>
where
    F: Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>,
{
    #[cfg(casa_prebuilt_image_provider)]
    let compiled = compiled_provider_matches_target();
    #[cfg(not(casa_prebuilt_image_provider))]
    let compiled = false;
    image_task_front_end_using(program, surfaces, execute, compiled)
}

fn image_task_front_end_using<F>(
    program: &str,
    surfaces: &[&str],
    execute: F,
    compiled: bool,
) -> TaskFrontEnd<F>
where
    F: Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>,
{
    #[cfg(not(casa_prebuilt_image_provider))]
    let _ = compiled;
    #[cfg(casa_prebuilt_image_provider)]
    if compiled {
        let hosted = surfaces
            .iter()
            .map(|id| {
                if !crate::analysis::IMAGE_ANALYSIS_TASK_SURFACES.contains(id) {
                    return Err(format!("the provider contract carries no surface {id:?}"));
                }
                casa_provider_contracts::builtin_surface_bundle(id).map_err(|error| {
                    format!("built-in image-analysis parameter surface {id:?}: {error}")
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .expect("the image-analysis contract carries every image surface");
        return TaskFrontEnd::new_with_compiled_contract(
            program,
            crate::image_analysis_protocol_descriptor(),
            hosted,
            include_str!(concat!(env!("OUT_DIR"), "/image-provider.json")),
            execute,
        )
        .expect("the image-analysis contract carries every image surface");
    }
    TaskFrontEnd::new_with_deferred_schema(
        program,
        image_analysis_task_schema_bundle(),
        surfaces,
        execute,
    )
    .expect("the image-analysis contract carries every image surface")
}

/// A product-writing image task's reply: CASA's return value `None`, and
/// its summary as the result, which a command line prints as JSON.
pub fn image_task_success(
    result: Result<ImageAnalysisTaskResult, ImageError>,
) -> Result<TaskSuccess, TaskFailure> {
    image_task_returning(CasaValue::None, result)
}

/// A product-writing image task's reply that returns CASA's `value`, as
/// `immath` returns `True`.
pub fn image_task_returning(
    value: CasaValue,
    result: Result<ImageAnalysisTaskResult, ImageError>,
) -> Result<TaskSuccess, TaskFailure> {
    let result = result.map_err(|error| TaskFailure::execution(error.to_string()))?;
    let text = serde_json::to_string_pretty(&result)
        .map_err(|error| TaskFailure::execution(format!("serialize the result: {error}")))?;
    Ok(TaskSuccess::new(value, &result)?.with_text(text + "\n"))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use casa_provider_contracts::CasaValue;
    use casa_task_runtime::{TaskFrontEnd, TaskSuccess};

    use super::{image_task_front_end, image_task_front_end_using};
    use crate::image_analysis_task_schema_bundle;

    #[test]
    fn compiled_image_provider_matches_the_runtime_contract() {
        let old = TaskFrontEnd::new_with_deferred_schema(
            "imexplore",
            image_analysis_task_schema_bundle(),
            &["imhead", "imstat"],
            |_| TaskSuccess::new(CasaValue::None, &()),
        )
        .unwrap();
        let current = image_task_front_end("imexplore", &["imhead", "imstat"], |_| {
            TaskSuccess::new(CasaValue::None, &())
        });
        let fallback = image_task_front_end_using(
            "imexplore",
            &["imhead", "imstat"],
            |_| TaskSuccess::new(CasaValue::None, &()),
            false,
        );
        #[cfg(casa_prebuilt_image_provider)]
        let forced_compiled = super::compiled_provider_matches_target().then(|| {
            image_task_front_end_using(
                "imexplore",
                &["imhead", "imstat"],
                |_| TaskSuccess::new(CasaValue::None, &()),
                true,
            )
        });
        for option in ["--help", "--json-schema", "--protocol-info", "--show"] {
            let args = [OsString::from("imhead"), OsString::from(option)];
            assert_eq!(current.dispatch(&args), old.dispatch(&args), "{option}");
            assert_eq!(fallback.dispatch(&args), old.dispatch(&args), "{option}");
            #[cfg(casa_prebuilt_image_provider)]
            if let Some(compiled) = &forced_compiled {
                assert_eq!(compiled.dispatch(&args), old.dispatch(&args), "{option}");
            }
        }
        let workspace = tempfile::tempdir().unwrap();
        let request = workspace.path().join("invalid.json");
        std::fs::write(
            &request,
            r#"{"surface":"imhead","values":{"imagename":123}}"#,
        )
        .unwrap();
        let args = [
            OsString::from("imhead"),
            OsString::from("--json-run"),
            request.into_os_string(),
        ];
        let refused = current.dispatch(&args);
        assert_ne!(refused.code, 0);
        assert_eq!(refused, old.dispatch(&args));
        assert_eq!(fallback.dispatch(&args), refused);
        #[cfg(casa_prebuilt_image_provider)]
        if let Some(compiled) = &forced_compiled {
            assert_eq!(compiled.dispatch(&args), refused);
        }
    }

    #[cfg(casa_prebuilt_image_provider)]
    #[test]
    fn incompatible_target_parser_requires_runtime_contract() {
        let order = super::IMAGE_PROVIDER_SCHEMA_ORDER;
        let (token, direct, value, casa) = super::IMAGE_PROVIDER_PROBES[0];
        assert!(!super::provider_matches_target(!order, &[]));
        assert!(!super::provider_matches_target(
            order,
            &[(token, direct ^ 1, value, casa)]
        ));
    }
}
