// SPDX-License-Identifier: LGPL-3.0-or-later

//! msexplore and plotms task completions decoded from the real provider.
//!
//! The provider arguments come from the runner's projection of a typed
//! parameter session, the path `casars run` and the Python tasks take, and
//! the provider's stdout goes through the completion decoder (#835).

mod common;

use std::path::Path;
use std::process::Command;

use casa_provider_contracts::{
    ParameterValue, ProviderInvocationAdaptation, builtin_surface_bundle,
};
use casa_task_runtime::{
    ParameterSession, TaskCompletion, TaskOutputValue, decode_task_completion,
    project_provider_invocation,
};
use common::create_msexplore_fixture_ms;
use tempfile::tempdir;

/// Run `surface` through the real msexplore provider with `parameters` and
/// decode its successful completion.
fn complete(surface: &str, workspace: &Path, parameters: &[(&str, &str)]) -> TaskCompletion {
    let bundle = builtin_surface_bundle(surface).expect("surface bundle");
    let mut session = ParameterSession::defaults(bundle).expect("default session");
    for (name, value) in parameters {
        session
            .set(*name, ParameterValue::String((*value).to_string()))
            .expect("set parameter");
    }
    let invocation = project_provider_invocation(&session, |_family, _values, direct| {
        Ok(ProviderInvocationAdaptation::direct(direct))
    })
    .expect("provider invocation");
    assert!(invocation.stdin.is_none(), "{invocation:?}");

    let output = Command::new(env!("CARGO_BIN_EXE_msexplore"))
        .args(&invocation.args)
        .current_dir(workspace)
        .output()
        .expect("run msexplore");
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    let values = parameters
        .iter()
        .map(|(name, value)| TaskOutputValue {
            name: (*name).to_string(),
            value: (*value).to_string(),
        })
        .collect::<Vec<_>>();
    decode_task_completion(session.bundle(), &stdout, workspace, &values)
        .unwrap_or_else(|error| panic!("{surface} completion: {error}\nstdout: {stdout}"))
}

#[test]
fn summary_on_stdout_decodes_with_the_plot_product() {
    for surface in ["msexplore", "plotms"] {
        let workspace = tempdir().expect("tempdir");
        let ms_path = create_msexplore_fixture_ms(workspace.path());
        let completion = complete(
            surface,
            workspace.path(),
            &[
                ("vis", ms_path.to_str().unwrap()),
                ("preset", "amplitude_vs_time"),
                ("plot_output", "amp.txt"),
            ],
        );

        assert!(
            completion
                .summary
                .starts_with("MeasurementSet summary for ")
                && completion.summary.ends_with("msexplore_fixture.ms"),
            "{surface}: {}",
            completion.summary
        );
        assert_eq!(completion.products.len(), 1, "{surface}");
        let product = &completion.products[0];
        assert_eq!(product.id, "plot_output", "{surface}");
        assert_eq!(product.path, workspace.path().join("amp.txt"), "{surface}");
        assert!(product.exists, "{surface}: {product:?}");
    }
}

#[test]
fn summary_written_to_output_decodes_from_empty_stdout() {
    for surface in ["msexplore", "plotms"] {
        let workspace = tempdir().expect("tempdir");
        let ms_path = create_msexplore_fixture_ms(workspace.path());
        let completion = complete(
            surface,
            workspace.path(),
            &[
                ("vis", ms_path.to_str().unwrap()),
                ("output", "summary.txt"),
            ],
        );

        let summary = workspace.path().join("summary.txt");
        assert_eq!(
            completion.summary,
            format!("MeasurementSet summary written to {}", summary.display()),
            "{surface}"
        );
        assert_eq!(completion.products.len(), 1, "{surface}");
        let product = &completion.products[0];
        assert_eq!(product.id, "output", "{surface}");
        assert_eq!(product.path, summary, "{surface}");
        assert!(product.exists, "{surface}: {product:?}");
    }
}
